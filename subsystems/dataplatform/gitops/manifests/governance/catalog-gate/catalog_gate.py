#!/usr/bin/env python3
"""catalog-gate: what a Spark job may ask of the Hive Metastore's Iceberg REST
catalog.

The catalog has no login, a table is Meta Catalog's to make, to shape and to
drop, and who reads or writes it is decided by the grants given there. The
jobs reach the catalog only through this gate (network policy), which

1. finds out whose job is asking, from the caller's address: the one running
   pod of the jobs' namespace with that address, its service account (the
   operator names it after the SparkApplication), that SparkApplication's
   label <LABEL_DOMAIN>/user (the admission policy
   dataplatform-spark-submitter-jobs makes it the submitter), and the user's
   groups in Trino's group file (ConfigMap trino-iam, iam-sync's). A caller
   that is no job's pod gets nothing;

2. asks the Open Policy Agent, with the question Trino would ask for the same
   user (package trino, manifests/governance/opa/trino.rego), so a job sees
   and writes what its user sees and writes in Trino:

     GET, HEAD   /iceberg/v1/config                        every job
                 a schema, a table that is not there       every job: "not
                                                           found", as in Trino
                 .../namespaces, .../<schema>/tables       the list, without
                                                           what the user may
                                                           not read
                 .../namespaces/<schema>                   a schema with a
                                                           table they may read
                 .../<schema>/tables/<table>               READ on the table
     POST        .../<schema>/tables/<table>               WRITE on the table,
                 a commit that changes the table's data and nothing else (new
                 snapshots, the branch they are on, expired snapshots,
                 statistics), to a table that exists
     POST        .../tables/<table>/metrics                READ: the client's
                                                           report of a scan

(a schema of several levels answers "not found" here: the metastore has
none, and Iceberg's client asks for one before it reads a metadata table
such as <table>.snapshots) and answers 403, in the catalog's own error
format so that the job's exception says why, to everything else: creating a table or a namespace,
dropping, renaming, registering, a commit that changes a table's columns,
partitioning, properties or place (ALTER TABLE, CREATE OR REPLACE TABLE),
and a commit to a table that is not there (which would create it: Iceberg's
"create transaction"). It answers 503 while it cannot ask (the Kubernetes
API, the Agent): never yes without an answer.

Without the first two steps a job of anybody could move any table's branch
to an older snapshot or to files of its own: the metastore writes a table's
metadata itself, with its own key, so Ceph never sees who asked (measured).

A path is taken as written and must be plain (letters, digits, '_', '-',
'.'): nothing is decoded or normalised here, so the catalog cannot read a
path differently from the rule that let it through.

CATALOG_URL is the catalog and OPA_URL the Agent (http://host:port, no
path). Standard library only; the decisions are in decide() and the small
functions after it, tested by files/tests/test_catalog_gate.py.
"""
import http.client
import http.server
import ipaddress
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

CATALOG_URL = os.environ.get("CATALOG_URL", "")
OPA_URL = os.environ.get("OPA_URL", "").rstrip("/")
OPA_PACKAGE = os.environ.get("OPA_PACKAGE", "trino")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "lakehouse")     # the catalog's name in Trino's rules
JOBS_NAMESPACE = os.environ.get("JOBS_NAMESPACE", "compute")
GROUPS_CONFIGMAP = os.environ.get("GROUPS_CONFIGMAP", "trino-iam")
GROUPS_KEY = os.environ.get("GROUPS_KEY", "groups.txt")
# the label of a SparkApplication that names its user: <LABEL_DOMAIN>/user (required)
USER_LABEL = os.environ.get("LABEL_DOMAIN", "") + "/user"
PORT = int(os.environ.get("PORT", "8080"))
TIMEOUT = int(os.environ.get("CATALOG_TIMEOUT", "120"))
MAX_BODY = 16 * 1024 * 1024

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
K8S = os.environ.get("K8S_API", "https://kubernetes.default.svc")
APPS = f"/apis/spark.stackable.tech/v1alpha1/namespaces/{JOBS_NAMESPACE}/sparkapplications"

NAME = r"[A-Za-z0-9_][A-Za-z0-9_.-]*"
PLAIN = re.compile(rf"^/iceberg/v1(/{NAME})*$")
CONFIG = "/iceberg/v1/config"
SCHEMAS = "/iceberg/v1/namespaces"
SCHEMA = re.compile(rf"^/iceberg/v1/namespaces/({NAME})$")
TABLES = re.compile(rf"^/iceberg/v1/namespaces/({NAME})/(?:tables|views)$")
TABLE = re.compile(rf"^/iceberg/v1/namespaces/({NAME})/tables/({NAME})$")
VIEW = re.compile(rf"^/iceberg/v1/namespaces/({NAME})/views/({NAME})$")
METRICS = re.compile(rf"^/iceberg/v1/namespaces/({NAME})/tables/({NAME})/metrics$")
# a schema of several levels (a%1Fb): the metastore has none, but Iceberg's
# client asks for one whenever a job reads a table's metadata table
# (<schema>.<table>.snapshots is first looked up as the table "snapshots" of
# the schema <schema>.<table>), and goes on only when told "not found"
DEEP = re.compile(rf"^/iceberg/v1/namespaces/{NAME}(?:%1[Ff]{NAME})+(?:/(?:tables|views)(?:/{NAME})?)?$")
# what a write, a row change, a compaction or a snapshot expiry sends
DATA_UPDATES = {"add-snapshot", "set-snapshot-ref", "remove-snapshots", "remove-snapshot-ref",
                "set-statistics", "remove-statistics", "set-partition-statistics", "remove-partition-statistics"}
ONLY_META_CATALOG = "tables are made, changed and dropped in Meta Catalog, not by a job"
HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers",
       "transfer-encoding", "upgrade", "host", "content-length"}
# what the catalog is not sent: its lists are read here, so no compression
NOT_SENT = HOP | {"accept-encoding"}
USER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,50}[a-z0-9]$")
OBJECT_NAME = re.compile(r"^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$")
ERRORS = {400: "BadRequestException", 403: "ForbiddenException", 503: "ServiceUnavailableException"}


def error_type(status: int, path: str) -> str:
    """The name Iceberg's client knows the refusal by. "Not found" must say of
    what: the client looks for a metadata table's own table only after a
    NoSuchTableException."""
    if status == 404:
        kind = path.rsplit("/", 2)[-2:][0]
        return {"tables": "NoSuchTableException", "views": "NoSuchViewException"}.get(kind, "NoSuchNamespaceException")
    return ERRORS.get(status, "ServiceFailureException")


class Refused(Exception):
    """The caller may not: 403 with this message."""


class Unavailable(Exception):
    """Somebody the gate must ask does not answer: 503 with this message."""


# ---------------------------------------------------------------- what is asked ----
def decide(method: str, path: str, body: bytes = b"") -> tuple:
    """(status, message, ask): status 0 lets the request go on, anything else
    is answered with that status and message. ask is what the policy must
    allow the caller before it passes: None (every job), ("read", schema,
    table), ("write", schema, table), ("schema", schema), or a list whose
    answer is filtered: ("tables", schema), ("schemas",)."""
    if method in ("GET", "HEAD") and DEEP.match(path):
        return 404, "the catalog's schemas have one level: there is nothing at " + path, None
    if not PLAIN.match(path) or "/../" in path + "/" or "/./" in path + "/":
        return 400, f"the path {path!r} is not a plain catalog path", None
    if method in ("GET", "HEAD"):
        if path == CONFIG:
            return 0, "", None
        if path == SCHEMAS:
            return 0, "", ("schemas",)
        for pattern, kind in ((SCHEMA, "schema"), (TABLES, "tables"), (TABLE, "read"), (VIEW, "read")):
            found = pattern.match(path)
            if found:
                return 0, "", (kind, *found.groups())
        return 403, f"{method} {path}: not something a job asks of the catalog", None
    if method != "POST":
        return 403, f"{method} {path}: {ONLY_META_CATALOG}", None
    found = METRICS.match(path)
    if found:
        return 0, "", ("read", *found.groups())
    found = TABLE.match(path)
    if not found:
        return 403, f"POST {path}: {ONLY_META_CATALOG}", None
    try:
        commit = json.loads(body or b"")
    except ValueError:
        return 400, "the commit is not JSON", None
    if not isinstance(commit, dict) or not isinstance(commit.get("updates", []), list) \
            or not isinstance(commit.get("requirements", []), list):
        return 400, "the commit is not an object with updates and requirements", None
    for requirement in commit.get("requirements", []):
        if not isinstance(requirement, dict) or requirement.get("type") == "assert-create":
            return 403, f"a commit that creates the table: {ONLY_META_CATALOG}", None
    for update in commit.get("updates", []):
        action = update.get("action") if isinstance(update, dict) else None
        if action not in DATA_UPDATES:
            return 403, (f"a commit that changes the table itself ({action}), not only its data: "
                         f"{ONLY_META_CATALOG}"), None
    return 0, "", ("write", *found.groups())


# ---------------------------------------------------------------- who asks ----
def job_of(pods: list, address: str) -> str:
    """The service account of the one running pod with this address: a job's
    name. An address is given again once its pod is gone, and a finished pod
    keeps it in its status, so only a running pod counts, and only one."""
    mine = [p for p in pods
            if (p.get("status") or {}).get("phase") == "Running"
            and not (p.get("spec") or {}).get("hostNetwork")
            and address in {(p.get("status") or {}).get("podIP")}
            | {a.get("ip") for a in (p.get("status") or {}).get("podIPs") or []}]
    if len(mine) != 1:
        raise Refused(f"the catalog is for Spark jobs: {'no' if not mine else 'more than one'} running pod of "
                      f"{JOBS_NAMESPACE} has the address {address}")
    name = (mine[0].get("spec") or {}).get("serviceAccountName") or "default"
    if not OBJECT_NAME.match(name):
        raise Refused(f"the catalog is for Spark jobs: {name!r} is no job's service account")
    return name


def user_of(job: str, app) -> str:
    """The user a SparkApplication runs for: its label."""
    if not app:
        raise Refused(f"the catalog is for Spark jobs: the service account {job} is no SparkApplication's")
    user = ((app.get("metadata") or {}).get("labels") or {}).get(USER_LABEL)
    if not isinstance(user, str) or not USER_PATTERN.match(user):
        raise Refused(f"the Spark job {job} names no user (label {USER_LABEL})")
    return user


def groups_of(text: str, user: str) -> list:
    """The user's groups in Trino's group file (group:user1,user2 per line)."""
    groups = []
    for line in text.splitlines():
        group, colon, members = line.partition(":")
        if colon and user in [m.strip() for m in members.split(",")]:
            groups.append(group.strip())
    return sorted(groups)


# ---------------------------------------------------------------- may they ----
def table_of(schema: str, table: str) -> dict:
    return {"table": {"catalogName": TRINO_CATALOG, "schemaName": schema, "tableName": table}}


def schema_of(schema: str) -> dict:
    return {"schema": {"catalogName": TRINO_CATALOG, "schemaName": schema}}


def question(who: dict, ask: tuple) -> tuple:
    """(rule, input, what is missing): the question for the Agent, shaped as
    Trino's. A read is Trino's SELECT, a commit its INSERT (the policy gives
    every change of a table's rows to the same grant), a schema its SHOW
    TABLES; a list is filtered as Trino filters its own."""
    context = {"identity": {"user": who["user"], "groups": who["groups"]}}
    kind = ask[0]
    if kind in ("read", "write"):
        operation = "SelectFromColumns" if kind == "read" else "InsertIntoTable"
        missing = (f"{who['user']} has no {'READ' if kind == 'read' else 'WRITE'} grant on the table "
                   f"{TRINO_CATALOG}.{ask[1]}.{ask[2]} in Meta Catalog")
        return "allow", {"context": context, "action": {"operation": operation, "resource": table_of(*ask[1:])}}, missing
    if kind == "schema":
        missing = f"{who['user']} has no grant on a table of {TRINO_CATALOG}.{ask[1]} in Meta Catalog"
        return "allow", {"context": context, "action": {"operation": "ShowTables", "resource": schema_of(ask[1])}}, missing
    raise ValueError(kind)


def listed(listing, ask: tuple) -> tuple:
    """(key, items, resources) of a list the catalog answered: its entries
    and the object each one is for the policy (None: not one of ours)."""
    if not isinstance(listing, dict):
        raise Unavailable("the catalog's list is not one")
    if ask[0] == "schemas":
        items = listing.get("namespaces")
        resources = [schema_of(i[0]) if isinstance(i, list) and len(i) == 1 and isinstance(i[0], str) else None
                     for i in items or []]
        key = "namespaces"
    else:
        items = listing.get("identifiers")
        resources = [table_of(i["namespace"][0], i["name"])
                     if isinstance(i, dict) and isinstance(i.get("name"), str) and isinstance(i.get("namespace"), list)
                     and len(i["namespace"]) == 1 and i["namespace"][0] == ask[1] else None
                     for i in items or []]
        key = "identifiers"
    if not isinstance(items, list):
        raise Unavailable("the catalog's list is not one")
    return key, items, resources


def filtered(listing: dict, ask: tuple, who: dict, batch) -> dict:
    """The list with only what the user may read. batch(input) is the Agent's
    answer to a filter request: the positions it allows."""
    key, items, resources = listed(listing, ask)
    known = [i for i, r in enumerate(resources) if r is not None]
    allowed = set()
    if known:
        answer = batch({"context": {"identity": {"user": who["user"], "groups": who["groups"]}},
                        "action": {"operation": "FilterSchemas" if ask[0] == "schemas" else "FilterTables",
                                   "filterResources": [resources[i] for i in known]}})
        if not isinstance(answer, list):
            raise Unavailable("the Open Policy Agent's answer to a list is not one")
        allowed = {known[n] for n in answer if isinstance(n, int) and not isinstance(n, bool) and 0 <= n < len(known)}
    return {**listing, key: [item for i, item in enumerate(items) if i in allowed]}


# ---------------------------------------------------------------- the others ----
def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%dT%H:%M:%S ") + msg, flush=True)


_k8s_ctx = None


def k8s(path: str):
    """A GET of the Kubernetes API; None when the object is not there."""
    global _k8s_ctx
    headers = {}
    if K8S.startswith("https://"):
        if _k8s_ctx is None:
            _k8s_ctx = ssl.create_default_context(cafile=f"{SA_DIR}/ca.crt")
        with open(f"{SA_DIR}/token") as f:      # read each time: the kubelet renews it
            headers["Authorization"] = f"Bearer {f.read().strip()}"
    try:
        with urllib.request.urlopen(urllib.request.Request(f"{K8S}{path}", headers=headers),
                                    context=_k8s_ctx, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise Unavailable(f"the Kubernetes API answers {e.code} to GET {path.partition('?')[0]}") from None
    except (OSError, ValueError) as e:
        raise Unavailable(f"the Kubernetes API does not answer: {e}") from None


def who_is(address: str) -> dict:
    """{user, groups, job} of the Spark job whose pod has this address."""
    try:
        ipaddress.ip_address(address)
    except ValueError:
        raise Refused(f"the catalog is for Spark jobs: {address!r} is no pod's address") from None
    selector = urllib.parse.quote(f"status.podIP={address},status.phase=Running")
    pods = k8s(f"/api/v1/namespaces/{JOBS_NAMESPACE}/pods?fieldSelector={selector}")
    job = job_of((pods or {}).get("items") or [], address)
    user = user_of(job, k8s(f"{APPS}/{job}"))
    groups = (k8s(f"/api/v1/namespaces/{JOBS_NAMESPACE}/configmaps/{GROUPS_CONFIGMAP}") or {}).get("data") or {}
    if not isinstance(groups.get(GROUPS_KEY), str):
        raise Unavailable(f"the ConfigMap {JOBS_NAMESPACE}/{GROUPS_CONFIGMAP} has no {GROUPS_KEY} (iam-sync writes it)")
    return {"user": user, "groups": groups_of(groups[GROUPS_KEY], user), "job": job}


def opa(rule: str, question_input: dict):
    """The Agent's answer to one question: the rule's value, None when it has none."""
    req = urllib.request.Request(f"{OPA_URL}/v1/data/{OPA_PACKAGE}/{rule}", method="POST",
                                 data=json.dumps({"input": question_input}).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read()).get("result")
    except urllib.error.HTTPError as e:
        raise Unavailable(f"the Open Policy Agent answers {e.code}") from None
    except (OSError, ValueError, AttributeError) as e:
        raise Unavailable(f"the Open Policy Agent does not answer: {e}") from None


class Gate(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "catalog-gate"

    def log_message(self, *args) -> None:       # one line per request, below
        pass

    def answer(self, status: int, headers: list, body: bytes) -> None:
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        if self.close_connection:               # said, or the client sends its next request into a closed one
            self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def refuse(self, status: int, message: str) -> None:
        """An answer in the catalog's error format: the client shows the message."""
        kind = error_type(status, self.path.partition("?")[0])
        body = json.dumps({"error": {"message": message, "type": kind, "code": status}}).encode()
        self.answer(status, [("Content-Type", "application/json")], body)

    def catalog(self, method: str, target: str, body: bytes = None) -> tuple:
        """(status, headers, body) of the catalog's answer"""
        url = urllib.parse.urlsplit(CATALOG_URL)
        conn = http.client.HTTPConnection(url.hostname, url.port or 80, timeout=TIMEOUT)
        try:
            headers = {k: v for k, v in self.headers.items() if k.lower() not in NOT_SENT}
            conn.request(method, target, body=body, headers=headers)
            reply = conn.getresponse()
            data = reply.read()
            return reply.status, [(k, v) for k, v in reply.getheaders() if k.lower() not in HOP], data
        except (OSError, http.client.HTTPException) as e:
            raise Unavailable(f"the catalog does not answer: {e}") from None
        finally:
            conn.close()

    def handle_any(self) -> None:
        path = self.path.partition("?")[0]
        if self.command == "GET" and path == "/healthz":
            return self.answer(200, [("Content-Type", "text/plain")], b"ok\n")
        body = b""
        if self.command == "POST":
            length = self.headers.get("Content-Length")
            if length is None or not length.isdigit() or int(length) > MAX_BODY:
                self.close_connection = True
                return self.refuse(400, "a request body needs a Content-Length (at most 16 MiB)")
            body = self.rfile.read(int(length))
        elif self.command not in ("GET", "HEAD"):
            self.close_connection = True        # whatever body it has is not read
        who = {"user": "?", "job": self.client_address[0]}
        status, message, ask = decide(self.command, path, body)
        if status:
            if status == 403:                   # who tried, for the log
                try:
                    who = who_is(self.client_address[0])
                except (Refused, Unavailable):
                    pass
            if status != 404:
                log(f"refused {who['user']} ({who['job']}) {self.command} {path}: {message}")
            return self.refuse(status, message)
        try:
            who = who_is(self.client_address[0])
            if ask and ask[0] in ("read", "write", "schema"):
                rule, question_input, missing = question(who, ask)
                # what is not there is "not found" for everybody, as in Trino: a
                # job that looks for a table before it tries to make one is then
                # told where tables are made, not that it lacks a grant
                if opa(rule, question_input) is not True and not (
                        self.command in ("GET", "HEAD") and self.catalog("HEAD", path)[0] == 404):
                    raise Refused(missing)
            if ask and ask[0] == "write" and self.catalog("HEAD", path)[0] not in (200, 204):
                raise Refused(f"a commit to a table that is not there: {ONLY_META_CATALOG}")
            code, headers, data = self.catalog(self.command, self.path, body if self.command == "POST" else None)
            if ask and ask[0] in ("tables", "schemas") and self.command == "GET" and code == 200:
                try:
                    listing = json.loads(data)
                except ValueError:
                    raise Unavailable("the catalog's list is not JSON") from None
                data = json.dumps(filtered(listing, ask, who, lambda i: opa("batch", i))).encode()
        except Refused as e:
            log(f"refused {who['user']} ({who['job']}) {self.command} {path}: {e}")
            return self.refuse(403, str(e))
        except Unavailable as e:
            log(f"cannot answer {who['user']} ({who['job']}) {self.command} {path}: {e}")
            return self.refuse(503, str(e))
        if ask and ask[0] == "write":
            log(f"{who['user']} ({who['job']}) commit {path}: {code}")
        self.answer(code, headers, data)

    do_GET = do_HEAD = do_POST = do_PUT = do_DELETE = do_PATCH = do_OPTIONS = handle_any


def main() -> None:
    for name, value in (("CATALOG_URL", CATALOG_URL), ("OPA_URL", OPA_URL)):
        if not re.match(r"^http://[^/]+$", value):
            sys.exit(f"{name} is not set (http://host:port)")
    if not os.environ.get("LABEL_DOMAIN"):
        sys.exit("LABEL_DOMAIN is not set")
    log(f"catalog-gate: {CATALOG_URL} behind port {PORT}, for the Spark jobs of {JOBS_NAMESPACE} (user: label "
        f"{USER_LABEL}, groups: ConfigMap {GROUPS_CONFIGMAP}), decided by {OPA_URL} (package {OPA_PACKAGE}, "
        f"catalog {TRINO_CATALOG})")
    http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Gate).serve_forever()


if __name__ == "__main__":
    main()
