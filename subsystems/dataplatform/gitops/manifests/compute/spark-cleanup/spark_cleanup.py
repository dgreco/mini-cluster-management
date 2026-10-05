#!/usr/bin/env python3
"""Purge finished Spark jobs in compute (CronJob compute/spark-cleanup, hourly).

Deletes every SparkApplication that ended (phase Succeeded or Failed) more than
RETENTION_HOURS ago; its ConfigMaps go with it (owner references). Then deletes
every Secret spark-token-<job> (dp-spark-submit.sh writes one per job, just
before the job) that no unfinished SparkApplication mounts, once it is
TOKEN_GRACE_MINUTES old (the job may not exist yet): a Keycloak token stays in
the cluster only while its job runs. The jobs' logs stay in VictoriaLogs and
their event logs in the Spark History Server.

A SparkApplication records no end time: the latest write of its status
subresource (managedFields; the operator's, when the driver ended) stands for
it. Secrets are listed as metadata only (PartialObjectMetadataList): their data
is never fetched.

Standard library only. Locally, through `kubectl proxy`:
  K8S_API=http://127.0.0.1:8001 DRY_RUN=true python3 spark_cleanup.py
"""
import datetime
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

NAMESPACE = os.environ.get("NAMESPACE", "compute")
RETENTION_HOURS = float(os.environ.get("RETENTION_HOURS", "24"))
TOKEN_GRACE_MINUTES = float(os.environ.get("TOKEN_GRACE_MINUTES", "30"))
DRY_RUN = os.environ.get("DRY_RUN", "") == "true"
K8S = os.environ.get("K8S_API", "https://kubernetes.default.svc")
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
FINISHED = {"Succeeded", "Failed"}
TOKEN_PREFIX = "spark-token-"
APPS = f"/apis/spark.stackable.tech/v1alpha1/namespaces/{NAMESPACE}/sparkapplications"
SECRETS = f"/api/v1/namespaces/{NAMESPACE}/secrets"
METADATA_ONLY = "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1"


def log(msg: str) -> None:
    print(msg, flush=True)


def request(method: str, path: str, body=None, accept="application/json"):
    headers = {"Accept": accept, "Content-Type": "application/json"}
    ctx = None
    if K8S.startswith("https://"):
        with open(f"{SA_DIR}/token") as f:
            headers["Authorization"] = f"Bearer {f.read().strip()}"
        ctx = ssl.create_default_context(cafile=f"{SA_DIR}/ca.crt")
    req = urllib.request.Request(f"{K8S}{path}", method=method, headers=headers,
                                 data=None if body is None else json.dumps(body).encode())
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        if e.code == 404 and method == "DELETE":
            return None  # gone meanwhile
        raise RuntimeError(f"{method} {path}: HTTP {e.code} {e.read().decode(errors='replace')[:300]}") from None


def list_all(path: str, accept="application/json") -> list:
    items, cont = [], ""
    while True:
        query = urllib.parse.urlencode({"limit": 200, **({"continue": cont} if cont else {})})
        page = request("GET", f"{path}?{query}", accept=accept)
        items += page.get("items") or []
        cont = (page.get("metadata") or {}).get("continue") or ""
        if not cont:
            return items


def when(stamp: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))


def ended_at(app: dict) -> datetime.datetime:
    meta = app["metadata"]
    times = [when(f["time"]) for f in meta.get("managedFields") or []
             if f.get("subresource") == "status" and f.get("time")]
    return max(times) if times else when(meta["creationTimestamp"])


def secret_names(obj) -> set:
    """Every secretName anywhere in a SparkApplication's spec (its volumes)."""
    found = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "secretName" and isinstance(v, str):
                found.add(v)
            else:
                found |= secret_names(v)
    elif isinstance(obj, list):
        for v in obj:
            found |= secret_names(v)
    return found


def delete(path: str, what: str) -> None:
    if DRY_RUN:
        log(f"would delete {what}")
        return
    request("DELETE", path, {"propagationPolicy": "Background"})
    log(f"deleted {what}")


def main() -> int:
    now = datetime.datetime.now(datetime.timezone.utc)
    apps = list_all(APPS)
    in_use, purged, kept = set(), 0, 0
    for app in apps:
        name = app["metadata"]["name"]
        phase = (app.get("status") or {}).get("phase", "")
        if phase not in FINISHED:
            in_use |= secret_names(app.get("spec") or {})
            continue
        age = (now - ended_at(app)).total_seconds() / 3600
        if age < RETENTION_HOURS:
            kept += 1
            continue
        delete(f"{APPS}/{name}", f"SparkApplication {name} ({phase}, ended {age:.0f} h ago)")
        purged += 1

    tokens = 0
    for secret in list_all(SECRETS, accept=METADATA_ONLY):
        meta = secret["metadata"]
        name = meta["name"]
        if not name.startswith(TOKEN_PREFIX) or name in in_use:
            continue
        age = (now - when(meta["creationTimestamp"])).total_seconds() / 60
        if age >= TOKEN_GRACE_MINUTES:
            delete(f"{SECRETS}/{name}", f"Secret {name} ({age:.0f} min old, no running job mounts it)")
            tokens += 1

    log(f"{purged} finished jobs purged, {kept} kept (ended less than {RETENTION_HOURS:g} h ago), "
        f"{len(apps) - purged - kept} unfinished; {tokens} token Secrets of ended jobs removed"
        + (" (dry run)" if DRY_RUN else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
