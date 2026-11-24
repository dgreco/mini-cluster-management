"""The pipeline's identity on the platform, and what it does with it.

Every task runs as the service account of the Keycloak client
lakehouse-pipeline (service-account-lakehouse-pipeline): a client-credentials
token, asked for at the start of each task. Its rights are the grants of the
tables in Meta Catalog (the SERVICE principal lakehouse-pipeline of
metacatalog/tables/tables.yaml), nothing else:

  Trino   the token is the login (its audience includes trino); the Open
          Policy Agent decides each statement from the tables' grants, and
          Trino reaches the files with the pipeline's STS role
  Ceph    the token is exchanged at Ceph's STS (AssumeRoleWithWebIdentity)
          for the role usr-service-account-lakehouse-pipeline, which reaches
          the folders the tables grant it
  Spark   the token logs in to the Kubernetes API (its audience includes
          kubernetes) as oidc:service-account-lakehouse-pipeline, who may
          submit jobs in compute like any person (RoleBinding
          spark-submitter-lakehouse-pipeline; the admission policies of
          compute name the jobs after it and pin its label); the job's pods
          get the token and take the same role, and the catalog gate lets
          them see and commit what the grants allow

Settings come from the environment, which the AirflowCluster sets
(gitops/manifests/tools/airflow/airflow.yaml): KEYCLOAK_TOKEN_URL,
LAKEHOUSE_PIPELINE_CLIENT_ID, LAKEHOUSE_PIPELINE_SECRET_FILE, TRINO_URL,
S3_ENDPOINT, S3_REGION, ROLE_ARN_PREFIX, USER_ROLE_PREFIX, LAKEHOUSE_BUCKET,
LAKEHOUSE_PREFIX, LAKEHOUSE_CATALOG, LABEL_DOMAIN, SPARK_NAMESPACE,
SPARK_IMAGE, SPARK_PRODUCT_VERSION. Third-party modules (trino, boto3, kubernetes, all
in Stackable's Airflow image) are imported where they are used.
"""
import base64
import json
import os
import re
import secrets
import threading
import time
import urllib.parse
import urllib.request

# ---------------------------------------------------------------- settings ----
def env(name: str, default: str = None) -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        raise RuntimeError(f"{name} is not set (the AirflowCluster's executor environment)")
    return v


def bucket() -> str:
    return env("LAKEHOUSE_BUCKET")


def prefix() -> str:
    return env("LAKEHOUSE_PREFIX", "lakehouse")


def catalog() -> str:
    return env("LAKEHOUSE_CATALOG", "lakehouse")


# ---------------------------------------------------------------- the token ----
def claims(token: str) -> dict:
    body = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))


def pipeline_token() -> str:
    """A fresh client-credentials token of lakehouse-pipeline."""
    with open(env("LAKEHOUSE_PIPELINE_SECRET_FILE")) as f:
        secret = f.read().strip()
    data = urllib.parse.urlencode({"grant_type": "client_credentials",
                                   "client_id": env("LAKEHOUSE_PIPELINE_CLIENT_ID", "lakehouse-pipeline"),
                                   "client_secret": secret}).encode()
    with urllib.request.urlopen(urllib.request.Request(env("KEYCLOAK_TOKEN_URL"), data=data), timeout=30) as r:
        return json.load(r)["access_token"]


# ---------------------------------------------------------------- Trino ----
def trino_connection(token: str):
    from trino.auth import JWTAuthentication
    from trino.dbapi import connect
    u = urllib.parse.urlparse(env("TRINO_URL"))
    return connect(host=u.hostname, port=u.port or (443 if u.scheme == "https" else 80), http_scheme=u.scheme,
                   user=claims(token)["preferred_username"], auth=JWTAuthentication(token),
                   catalog=catalog(), source="airflow-lakehouse")


def trino_run(statements: list, log=print) -> list:
    """Runs the statements one after the other, as the pipeline; returns
    what each answered (an INSERT: its row count)."""
    token = pipeline_token()
    conn = trino_connection(token)
    out = []
    try:
        cur = conn.cursor()
        for sql in statements:
            first = " ".join(sql.split())[:160]
            started = time.time()
            log(f"trino: {first}")
            cur.execute(sql)
            rows = cur.fetchall()
            log(f"trino: done in {time.time() - started:.0f} s: {rows[:3]}")
            out.append(rows)
    finally:
        conn.close()
    return out


# ---------------------------------------------------------------- Ceph ----
def pipeline_role_arn(token: str) -> str:
    return env("ROLE_ARN_PREFIX") + env("USER_ROLE_PREFIX", "usr-") + claims(token)["preferred_username"]


def s3_client(token: str = ""):
    """An S3 client with the pipeline's STS role (an hour)."""
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    token = token or pipeline_token()
    region = env("S3_REGION")
    # AssumeRoleWithWebIdentity is not signed: the token is the credential
    sts = boto3.client("sts", endpoint_url=env("S3_ENDPOINT"), region_name=region,
                       config=Config(signature_version=UNSIGNED))
    c = sts.assume_role_with_web_identity(RoleArn=pipeline_role_arn(token),
                                          RoleSessionName=claims(token)["preferred_username"][:64],
                                          WebIdentityToken=token)["Credentials"]
    return boto3.client("s3", endpoint_url=env("S3_ENDPOINT"), region_name=region,
                        aws_access_key_id=c["AccessKeyId"], aws_secret_access_key=c["SecretAccessKey"],
                        aws_session_token=c["SessionToken"],
                        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}))


def list_keys(s3, folder: str) -> list:
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket(), "Prefix": folder.rstrip("/") + "/"}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in r.get("Contents") or []]
        if not r.get("IsTruncated"):
            return keys
        token = r["NextContinuationToken"]


def empty_folder(s3, folder: str, keep: set = frozenset()) -> int:
    """Deletes what is in the folder but its directory object and keep."""
    marker = folder.rstrip("/") + "/"
    gone = [k for k in list_keys(s3, folder) if k != marker and k not in keep]
    for i in range(0, len(gone), 1000):
        s3.delete_objects(Bucket=bucket(), Delete={"Objects": [{"Key": k} for k in gone[i:i + 1000]], "Quiet": True})
    return len(gone)


# ---------------------------------------------------------------- Spark ----
NAME = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


# Stackable's limit on a SparkApplication's name (ClusterName in operator-rs
# 0.113.3, which spark-k8s-operator 26.7.0 uses): a longer one is never
# started, the operator only reports "failed to set the cluster name:
# maximum length exceeded" in its events.
JOB_NAME_MAX = 40
KEY_MIN = 4                 # random base-36 digits that keep a name new


def job_name(user: str, step: str, key: str) -> str:
    """<user>-<step>-<key>, or <user>-<key> (its tail) where the step does
    not fit: the admission policies of compute want the user in front, and
    Stackable takes at most JOB_NAME_MAX characters (the pipeline's user,
    service-account-lakehouse-pipeline, leaves five)."""
    def clean(n):
        return re.sub(r"[^a-z0-9-]", "-", n.lower()).strip("-")
    user = clean(user)
    room = JOB_NAME_MAX - len(user) - 1
    for name in (clean(f"{user}-{step}-{key}"), clean(f"{user}-{key[-room:]}") if room >= KEY_MIN else ""):
        if name and len(name) <= JOB_NAME_MAX and NAME.match(name):
            return name
    raise ValueError(f"no Spark job name for {user!r} fits in {JOB_NAME_MAX} characters with a key of "
                     f"{KEY_MIN}: the user name may be at most {JOB_NAME_MAX - KEY_MIN - 1} characters")


def run_key() -> str:
    """Six random base-36 digits: a job name that is new at each try, also
    for tasks that start at the same moment."""
    return "".join(secrets.choice("0123456789abcdefghijklmnopqrstuvwxyz") for _ in range(6))


def spark_application(name: str, user: str, token_secret: str, role_arn: str, job_file: str, args: list,
                      template: str = "lakehouse", executors: int = 2, driver_memory: str = "",
                      executor_memory: str = "") -> dict:
    """The SparkApplication of a job, as files/dp-spark-submit.sh writes it:
    the template holds the rest (catalog, S3, logging)."""
    overrides = {"podOverrides": {"spec": {
        "volumes": [{"name": "keycloak-token", "secret": {"secretName": token_secret,
                                                          "items": [{"key": "token", "path": "token"}]}}],
        "containers": [{"name": "spark",
                        "env": [{"name": "AWS_WEB_IDENTITY_TOKEN_FILE", "value": "/stackable/keycloak/token"},
                                {"name": "AWS_ROLE_ARN", "value": role_arn},
                                {"name": "AWS_ROLE_SESSION_NAME", "value": user[:64]},
                                {"name": "AWS_REGION", "value": env("S3_REGION")},
                                {"name": "AWS_ENDPOINT_URL_STS", "value": env("S3_ENDPOINT")}],
                        "volumeMounts": [{"name": "keycloak-token", "mountPath": "/stackable/keycloak", "readOnly": True}]}]}}}
    driver = {"config": {"volumeMounts": [{"name": "job", "mountPath": "/stackable/jobs"}]}, **overrides}
    if driver_memory:
        driver["config"]["resources"] = {"memory": {"limit": driver_memory}}
    executor = {"replicas": executors, **json.loads(json.dumps(overrides))}
    if executor_memory:
        executor["config"] = {"resources": {"memory": {"limit": executor_memory}}}
    return {
        "apiVersion": "spark.stackable.tech/v1alpha1",
        "kind": "SparkApplication",
        "metadata": {"name": name, "namespace": env("SPARK_NAMESPACE", "compute"),
                     "labels": {f"{env('LABEL_DOMAIN')}/user": user},
                     "annotations": {"spark-application.template.merge": "true",
                                     "spark-application.template.0.name": template}},
        "spec": {"mode": "cluster",
                 # the platform's image (Iceberg in it); it replaces the template's whole
                 "sparkImage": {"custom": env("SPARK_IMAGE"), "productVersion": env("SPARK_PRODUCT_VERSION"),
                                "pullPolicy": "IfNotPresent"},
                 "mainApplicationFile": f"local:///stackable/jobs/{os.path.basename(job_file)}",
                 "args": args,
                 "volumes": [{"name": "job", "configMap": {"name": f"job-{name}"}}],
                 "driver": driver,
                 "executor": executor},
    }


class LogFollower(threading.Thread):
    """Follows the spark container's log of a driver pod while it runs and
    keeps all of it: the driver pod is removed when the job ends, so a log
    read afterwards finds nothing (measured: every Spark step of the first
    run failed with the job's success line unread). A follow that breaks
    (an API or kubelet hiccup) starts again from the beginning; the longest
    copy read wins. read(pod, namespace, container, follow) returns an
    object whose stream() yields bytes, as the kubernetes client's
    read_namespaced_pod_log(..., _preload_content=False) does."""

    def __init__(self, read, pod: str, namespace: str, patience: float = 1800, pause: float = 5):
        super().__init__(daemon=True)
        self.read, self.pod, self.namespace, self.patience, self.pause = read, pod, namespace, patience, pause
        self.text, self.error = "", ""

    def run(self):
        # The API refuses the log of a container that has not started yet
        # (its image may take minutes to pull), and a pod that is gone (404)
        # has no more to give: retry until the first, stop at the second.
        deadline = time.time() + self.patience
        while time.time() < deadline:
            chunks = []
            try:
                for chunk in self.read(self.pod, self.namespace, "spark", True).stream(65536):
                    chunks.append(chunk)
                self._keep(chunks)
                if self.text:
                    return                  # the container ended: the whole log
            except Exception as e:  # noqa: BLE001 - kept and retried; the verdict says what is missing
                self._keep(chunks)
                self.error = f"{type(e).__name__}: {e}"[:300]
                if getattr(e, "status", None) == 404 and self.text:
                    return
            time.sleep(self.pause)

    def _keep(self, chunks):
        text = b"".join(chunks).decode("utf-8", "replace")
        if len(text) >= len(self.text):
            self.text = text


FAILED_LOG = re.compile(r"Traceback \(most recent call last\)|py4j\.protocol\.Py4J[A-Za-z]*Error")
DONE = "LAKEHOUSE_JOB_DONE"


def judge(phase: str, log_text: str, container_reason: str = "") -> str:
    """Succeeded, or why not: a Python error still ends a job with exit 0,
    and a driver whose spark container died stays Running on its sidecar."""
    if container_reason and container_reason != "Completed":
        return f"Failed (driver {container_reason})"
    if phase != "Succeeded":
        return phase or "unknown"
    if FAILED_LOG.search(log_text or ""):
        return "Failed (Python error)"
    if DONE not in (log_text or ""):
        return f"Failed (the job did not print {DONE})"
    return "Succeeded"


def kubernetes_api(token: str):
    from kubernetes import client
    c = client.Configuration()
    c.host = env("KUBERNETES_URL", "https://kubernetes.default.svc")
    c.api_key = {"authorization": token}
    c.api_key_prefix = {"authorization": "Bearer"}
    c.ssl_ca_cert = env("KUBERNETES_CA_FILE", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
    return client.ApiClient(c)


SAFE_ARG = re.compile(r"^[A-Za-z0-9._/=:-]+$")


def submit_spark(step: str, job_file: str, args: list, template: str = "lakehouse", executors: int = 2,
                 driver_memory: str = "", executor_memory: str = "", files: dict = None,
                 timeout: int = 4 * 3600, start_timeout: int = 900, log=print) -> str:
    """Runs job_file as a Spark job in compute, as the pipeline, and waits
    for it; fails unless it succeeded. Returns the job's name. files go into
    the job's ConfigMap next to the code (/stackable/jobs/<name>): the
    operator hands spark-submit the arguments through bash -c without
    quoting them, so anything with a space or a quote travels as a file.
    A job with no phase and no driver after start_timeout seconds is one
    the operator refuses (its events say why): it is deleted and fails
    the task, which would otherwise wait the whole timeout."""
    bad = [a for a in args if not SAFE_ARG.match(str(a))]
    if bad:
        raise ValueError(f"Spark job arguments {bad}: the operator passes them through a shell unquoted; "
                         "put such values into files")
    from kubernetes import client
    from kubernetes.client.rest import ApiException
    token = pipeline_token()
    user = claims(token)["preferred_username"]
    ns = env("SPARK_NAMESPACE", "compute")
    api = kubernetes_api(token)
    core, custom = client.CoreV1Api(api), client.CustomObjectsApi(api)
    for attempt in range(3):            # a name another job holds: another key
        name = job_name(user, step, run_key())
        secret = f"spark-token-{name}"
        try:
            core.create_namespaced_secret(ns, {"metadata": {"name": secret}, "type": "Opaque",
                                               "stringData": {"token": token}})
            break
        except ApiException as e:
            if e.status != 409 or attempt == 2:
                raise
    try:
        app = custom.create_namespaced_custom_object("spark.stackable.tech", "v1alpha1", ns, "sparkapplications",
                                                     spark_application(name, user, secret, pipeline_role_arn(token),
                                                                       job_file, args, template, executors,
                                                                       driver_memory, executor_memory))
        with open(job_file) as f:
            code = f.read()
        core.create_namespaced_config_map(ns, {
            "metadata": {"name": f"job-{name}", "ownerReferences": [{
                "apiVersion": "spark.stackable.tech/v1alpha1", "kind": "SparkApplication",
                "name": name, "uid": app["metadata"]["uid"]}]},
            "data": {os.path.basename(job_file): code, **(files or {})}})
        log(f"spark: submitted {ns}/{name} as {user} ({step}, template {template})")
        started = time.time()
        deadline, driver, phase, reason, follower = started + timeout, "", "", "", None
        while time.time() < deadline:
            time.sleep(10)
            phase = (custom.get_namespaced_custom_object("spark.stackable.tech", "v1alpha1", ns, "sparkapplications",
                                                         name).get("status") or {}).get("phase", "")
            if not driver:
                driver = next((p.metadata.name for p in core.list_namespaced_pod(ns).items
                               if p.metadata.name.startswith(name + "-") and p.metadata.name.endswith("-driver")), "")
                if driver:
                    follower = LogFollower(lambda pod, n, c, f: core.read_namespaced_pod_log(
                        pod, n, container=c, follow=f, _preload_content=False), driver, ns)
                    follower.start()
            if not phase and not driver and time.time() - started > start_timeout:
                custom.delete_namespaced_custom_object("spark.stackable.tech", "v1alpha1", ns, "sparkapplications", name)
                raise RuntimeError(f"Spark job {ns}/{name}: not started by the operator in {start_timeout} s, "
                                   f"deleted (the reason was in its events: kubectl -n {ns} get events "
                                   f"--field-selector involvedObject.name={name})")
            if driver:
                try:
                    pod = core.read_namespaced_pod(driver, ns)
                    for s in pod.status.container_statuses or []:
                        if s.name == "spark" and s.state and s.state.terminated:
                            reason = s.state.terminated.reason or ""
                except ApiException:
                    pass
            if phase in ("Succeeded", "Failed") or (reason and reason != "Completed"):
                break
        text = ""
        if follower:
            follower.join(120)              # the container has ended: its log stream ends too
            text = follower.text or f"(the driver's log could not be followed: {follower.error or 'empty'})"
        for line in text.splitlines():
            if not re.match(r"^([0-9/]+ [0-9:]+|[0-9-]+T[0-9:,.]+) (INFO|DEBUG) ", line) and line.strip():
                log(line)
        verdict = judge(phase if time.time() < deadline else "timed out", text, reason)
        log(f"spark: {ns}/{name}: {verdict}")
        if verdict != "Succeeded":
            raise RuntimeError(f"Spark job {ns}/{name}: {verdict}")
        return name
    finally:
        # the token is the job's only: gone with it (spark-cleanup would take it later)
        try:
            core.delete_namespaced_secret(secret, ns)
        except ApiException:
            pass
