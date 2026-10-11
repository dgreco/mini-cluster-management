#!/usr/bin/env python3
"""iam-sync: Keycloak groups and Meta Catalog's grants -> what the platform
derives from them.

Every SYNC_INTERVAL seconds it reads the groups of the realm and their members
(service account of the Keycloak client iam-sync: view-users, query-groups,
query-users; both come from the directory, Samba AD, through the realm's user
federation, and a member the realm holds itself does not count), ACCESS_FILE (the ConfigMap iam-access: gitops/iam/access.yaml,
the platform's own settings) and the record of grants (the ConfigMap
GRANTS_CONFIGMAP of its namespace, which Meta Catalog's authorize and reject
write, and which they apply to Ceph's STS roles; only grants under
access.yaml's metacatalog.prefix count, the rest is ignored), and
reconciles, in TRINO_NAMESPACE (compute):

1. ConfigMap trino-iam:
   - groups.txt: Trino's file group provider (group:user1,user2), from Keycloak
   - s3-security-mapping.json: for every Keycloak user who has a role (their
     own usr-<user>, or a group's grp-<group>), a rule per directory granted
     to them or to one of their groups, the most specific first (Trino takes
     the first rule that matches a file's location): the role of theirs that
     reaches it, rw before ro, then their own before their groups'
     (alphabetical); the admin role only where a grant names the admin
     group, never for its bucket-wide rights. So one query may read and
     write every directory the user has, each with its own role. Then the
     user's default rule, for other locations: their own role, else their
     first group's. Every role of theirs is allowed in each rule, so the
     extra credential s3_role (trino --extra-credential s3_role=<arn>) pins
     one role for a whole session instead. Then a rule per group with a
     role, for users iam-sync has not seen yet. Session name = the Trino
     user. No catch-all rule: a user nothing grants gets no S3 credentials
     from Trino.
   - rules.json: Trino's file-based access control. Meta Catalog creates
     every schema and table: only its provisioner (access.yaml
     metacatalog.provisioner) may create, alter, comment or drop schemas and
     tables, nobody else, admins included. Every user may SELECT, INSERT,
     DELETE and UPDATE: the files decide, at Ceph, with the user's roles.
2. RoleBinding spark-submitter: every Keycloak group (Kubernetes group
   oidc:<group>) may use the ClusterRole SPARK_SUBMITTER_ROLE in that
   namespace, where every Spark job runs. The data itself is Ceph's to
   refuse: a job holds only the submitter's Keycloak token, exchanged at STS
   for the chosen role.

The RGW roles come from the same record, through Meta Catalog
(provisioning/lib-iam.sh); the admin groups' and the provisioner's are
Ansible's (make dataplatform-iam).

Safety: an answer with no group at all, with no member in any group, or
without a group seen earlier in this process, is not applied (it would empty
Trino's group file and revoke every user), unless IAM_SYNC_ALLOW_SHRINK=true.
Removing a group on purpose: set that, or restart iam-sync. While the
directory is down Keycloak still answers, 200, with every group and no
member (Keycloak 26.7): that is the answer with no member, whatever was seen
before (a process that starts during the outage has seen nothing), and a
directory that goes away in the middle of a read is caught by reading one
group with members a second time.
Standard library only (access.yaml is read by parse_access, a reader of the
YAML subset the file uses; make dataplatform-iam-check proves it agrees with
PyYAML); objects are written with server-side apply.
"""
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

KEYCLOAK_URL = os.environ.get("KEYCLOAK_URL", "").rstrip("/")   # required (checked in main)
REALM = os.environ.get("KEYCLOAK_REALM", "")
CLIENT_ID = os.environ.get("KEYCLOAK_CLIENT_ID", "iam-sync")
CLIENT_SECRET = os.environ.get("KEYCLOAK_CLIENT_SECRET", "")
ACCESS_FILE = os.environ.get("ACCESS_FILE", "/etc/iam-sync/access/access.yaml")
GRANTS_CONFIGMAP = os.environ.get("GRANTS_CONFIGMAP", "metacatalog-grants")
ROLE_ARN_PREFIX = os.environ.get("ROLE_ARN_PREFIX", "")              # arn:aws:iam::<account>:role/ (required)
USER_ROLE_PREFIX = os.environ.get("USER_ROLE_PREFIX", "usr-")
GROUP_ROLE_PREFIX = os.environ.get("GROUP_ROLE_PREFIX", "grp-")
GROUP_PATTERN = re.compile(os.environ.get("GROUP_PATTERN", r"^[a-z0-9][a-z0-9-]{0,40}[a-z0-9]$"))
USER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,50}[a-z0-9]$")
TRINO_NAMESPACE = os.environ.get("TRINO_NAMESPACE", "compute")
SPARK_SUBMITTER_ROLE = os.environ.get("SPARK_SUBMITTER_ROLE", "dataplatform-spark-submitter")
OIDC_PREFIX = os.environ.get("OIDC_PREFIX", "oidc:")      # the API server's prefix of Keycloak groups
INTERVAL = int(os.environ.get("SYNC_INTERVAL", "30"))
ALLOW_SHRINK = os.environ.get("IAM_SYNC_ALLOW_SHRINK", "false") == "true"
PAGE = 200
MANAGER = "iam-sync"
# the label it stamps on what it writes: <LABEL_DOMAIN>/managed-by (required)
LABEL = os.environ.get("LABEL_DOMAIN", "") + "/managed-by"

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
K8S = "https://kubernetes.default.svc"
_k8s_ctx = None


def k8s_ctx() -> ssl.SSLContext:
    global _k8s_ctx
    if _k8s_ctx is None:
        _k8s_ctx = ssl.create_default_context(cafile=f"{SA_DIR}/ca.crt")
    return _k8s_ctx


def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%dT%H:%M:%S ") + msg, flush=True)


# ---------------------------------------------------------------- access.yaml ----
def _scalar(text: str):
    text = text.strip()
    if text.startswith(("'", '"')) and text.endswith(text[0]) and len(text) >= 2:
        return text[1:-1]
    if text in ("true", "false"):
        return text == "true"
    if text in ("null", "~", ""):
        return None
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    return text


def _value(text: str):
    """A scalar or a flow list ([a, b]) as PyYAML would read it."""
    text = text.strip()
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        return [_scalar(v) for v in inner.split(",")] if inner else []
    if text.startswith("{") and text.endswith("}") and text == "{}":
        return {}
    return _scalar(text)


def parse_access(text: str):
    """The YAML subset access.yaml is written in: block mappings, block lists
    of mappings or scalars, flow lists of scalars, comments. Returns what
    yaml.safe_load returns for such a file."""
    lines = []
    for raw in text.splitlines():
        stripped = raw.split(" #", 1)[0] if " #" in raw else raw
        if stripped.lstrip().startswith("#") or not stripped.strip():
            continue
        lines.append(stripped.rstrip())

    def block(i: int, indent: int):
        """Parses the block starting at line i whose items are indented by
        `indent`; returns (value, next line index)."""
        if lines[i][indent:].startswith("- "):
            out = []
            while i < len(lines) and len(lines[i]) - len(lines[i].lstrip()) == indent and lines[i][indent:].startswith("- "):
                item = lines[i][indent + 2:]
                if ":" in item and not item.strip().startswith(("[", "'", '"')):     # a mapping starts on the dash line
                    lines[i] = " " * (indent + 2) + item                              # re-read it as the first key
                    value, i = block(i, indent + 2)
                    out.append(value)
                else:
                    out.append(_value(item))
                    i += 1
            return out, i
        out = {}
        while i < len(lines) and len(lines[i]) - len(lines[i].lstrip()) == indent and not lines[i][indent:].startswith("- "):
            key, _, rest = lines[i][indent:].partition(":")
            key = key.strip()
            if rest.strip():
                out[key] = _value(rest)
                i += 1
            else:
                i += 1
                if i < len(lines) and len(lines[i]) - len(lines[i].lstrip()) > indent:
                    out[key], i = block(i, len(lines[i]) - len(lines[i].lstrip()))
                elif i < len(lines) and len(lines[i]) - len(lines[i].lstrip()) == indent and lines[i][indent:].startswith("- "):
                    out[key], i = block(i, indent)          # a list at the key's own indent
                else:
                    out[key] = None
        return out, i

    if not lines:
        return None
    value, _ = block(0, len(lines[0]) - len(lines[0].lstrip()))
    return value


def read_access() -> dict:
    """{bucket, grants: [{path, users, groups, access}], admins: [groups],
    provisioner: the one user with DDL in Trino}. The grants: the
    provisioner's rw on metacatalog.prefix (it creates every schema and table
    there), then those of the record under the prefix. A `grants` list in
    access.yaml is ignored: Meta Catalog is the only source of grants
    (check_access.py refuses one)."""
    with open(ACCESS_FILE) as f:
        doc = parse_access(f.read()) or {}
    admins = [a for a in ((doc.get("admins") or {}).get("groups") or []) if GROUP_PATTERN.match(str(a))]
    mc = doc.get("metacatalog") or {}
    prefix = str(mc.get("prefix") or "").strip("/")
    provisioner = str(mc.get("provisioner") or "")
    provisioner = provisioner if USER_PATTERN.match(provisioner) else ""
    grants = []
    if prefix and provisioner:
        grants.append({"path": prefix, "access": "rw", "users": [provisioner], "groups": []})
    if prefix:
        grants += metacatalog_grants(prefix)
    return {"bucket": doc.get("bucket", ""), "grants": grants, "admins": admins, "provisioner": provisioner}


_record = None


def read_record() -> dict:
    """{resource: entry} of the record of grants. When it cannot be read, the
    last record read stands in (an API hiccup must not revoke every grant
    from Trino's mapping); before a first read the round fails, and nothing
    is applied."""
    global _record
    try:
        with open(f"{SA_DIR}/namespace") as f:
            ns = f.read().strip()
        cm = k8s("GET", f"/api/v1/namespaces/{ns}/configmaps/{GRANTS_CONFIGMAP}")
        if cm is None:
            raise RuntimeError(f"ConfigMap {ns}/{GRANTS_CONFIGMAP} does not exist")
        out = {}
        for key, raw in sorted((cm.get("data") or {}).items()):
            try:
                out[key] = json.loads(raw)
            except ValueError:
                log(f"record of grants: {key}: not JSON, ignored")
        _record = out
    except Exception as e:  # noqa: BLE001 - any failure: the last record, or none at all
        if _record is None:
            raise RuntimeError(f"the record of grants: {e}") from None
        log(f"the record of grants: {e}: the last one read stands in")
    return _record


def metacatalog_grants(prefix: str) -> list:
    """The grants of the record ({resource: {"path", "grants": [{access,
    users, groups}]}}), those under the prefix only: the scripts write no
    others, and anything else (edited by hand) is dropped here (no grant is
    the safe side)."""
    out = []
    for name, r in sorted(read_record().items()):
        path = str((r or {}).get("path") or "").strip("/") if isinstance(r, dict) else ""
        if not path.startswith(prefix + "/") or ".." in path.split("/"):
            log(f"record of grants: {name}: {path!r} is not under {prefix}/, ignored")
            continue
        for g in r.get("grants") or []:
            if not isinstance(g, dict) or g.get("access") not in ("rw", "ro"):
                continue
            out.append({"path": path, "access": g["access"],
                        "users": [u for u in (g.get("users") or []) if USER_PATTERN.match(str(u))],
                        "groups": [x for x in (g.get("groups") or []) if GROUP_PATTERN.match(str(x))]})
    return out


# ---------------------------------------------------------------- Keycloak ----
def keycloak_token() -> str:
    data = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": CLIENT_ID,
                                   "client_secret": CLIENT_SECRET}).encode()
    url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/token"
    with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=20) as r:
        return json.load(r)["access_token"]


def keycloak_get(token: str, path: str):
    req = urllib.request.Request(f"{KEYCLOAK_URL}/admin/realms/{REALM}{path}",
                                 headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def keycloak_all(token: str, path: str) -> list:
    """Every item of a paged listing (first/max)."""
    out, first = [], 0
    while True:
        page = keycloak_get(token, f"{path}{'&' if '?' in path else '?'}first={first}&max={PAGE}")
        out.extend(page)
        if len(page) < PAGE:
            return out
        first += PAGE


class DirectoryDown(Exception):
    """Keycloak answered, but not from the directory."""


_ignored_members: set = set()


def group_members(token: str, group: dict) -> list:
    """Sorted names of the group's enabled members that come from the
    directory (federationLink): a user the realm holds itself is nobody here."""
    out = []
    for m in keycloak_all(token, f"/groups/{group['id']}/members?briefRepresentation=true"):
        if not m.get("federationLink"):
            if (group["name"], m["username"]) not in _ignored_members:
                _ignored_members.add((group["name"], m["username"]))
                log(f"{m['username']} in {group['name']} is not a user of the directory: ignored")
        elif m.get("enabled", True):
            out.append(m["username"])
    return sorted(out)


def read_groups() -> dict:
    """{group name: sorted usernames} of the top-level groups with usable names."""
    token = keycloak_token()
    out, by_name = {}, {}
    for g in keycloak_all(token, "/groups?briefRepresentation=true"):
        if not GROUP_PATTERN.match(g["name"]):
            continue
        out[g["name"]] = group_members(token, g)
        by_name[g["name"]] = g
    # the directory may have gone away between two of the answers above
    witness = next((name for name, users in out.items() if users), None)
    if witness is not None and not group_members(token, by_name[witness]):
        raise DirectoryDown(f"{witness} had members a moment ago and has none now: the directory went away "
                            "during the read, nothing applied")
    return out


_seen_groups: set = set()


def plausible(groups: dict) -> bool:
    """False for an answer that would revoke everyone: no group, no member in
    any group (the directory is down), or a group seen earlier in this process
    is gone (a half-imported realm, a wrong realm)."""
    if ALLOW_SHRINK:
        return True
    if not groups:
        log("Keycloak lists no group: not applied (IAM_SYNC_ALLOW_SHRINK=true to apply)")
        return False
    if not any(groups.values()):
        log("Keycloak lists no member in any group, as it does while the directory is down: not applied "
            "(IAM_SYNC_ALLOW_SHRINK=true to apply)")
        return False
    gone = _seen_groups - set(groups)
    if gone:
        log(f"groups seen before are gone from Keycloak: {sorted(gone)}: not applied "
            "(IAM_SYNC_ALLOW_SHRINK=true, or restart iam-sync, to apply)")
        return False
    _seen_groups.update(groups)
    return True


# ---------------------------------------------------------------- Kubernetes ----
def k8s(method: str, path: str, body=None, content_type="application/json"):
    with open(f"{SA_DIR}/token") as f:
        token = f.read().strip()
    req = urllib.request.Request(f"{K8S}{path}", method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Authorization": f"Bearer {token}", "Content-Type": content_type})
    try:
        with urllib.request.urlopen(req, context=k8s_ctx(), timeout=30) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        if e.code == 404 and method in ("GET", "DELETE"):
            return None
        raise RuntimeError(f"{method} {path}: HTTP {e.code} {e.read().decode(errors='replace')[:400]}") from None


def api_path(obj: dict) -> str:
    """REST path of a namespaced or cluster-scoped object (the kinds used here)."""
    kind, name, ns = obj["kind"], obj["metadata"]["name"], obj["metadata"].get("namespace")
    group_version = obj["apiVersion"]
    plural = {"ConfigMap": "configmaps", "RoleBinding": "rolebindings"}[kind]
    base = f"/api/{group_version}" if "/" not in group_version else f"/apis/{group_version}"
    return f"{base}/namespaces/{ns}/{plural}/{name}" if ns else f"{base}/{plural}/{name}"


_applied: dict = {}


def apply(obj: dict) -> None:
    """Server-side apply, skipped while the object is unchanged since the last apply."""
    obj.setdefault("metadata", {}).setdefault("labels", {})[LABEL] = MANAGER
    path = api_path(obj)
    digest = json.dumps(obj, sort_keys=True)
    if _applied.get(path) == digest:
        return
    k8s("PATCH", f"{path}?fieldManager={MANAGER}&force=true", obj, "application/apply-patch+yaml")
    _applied[path] = digest
    log(f"applied {obj['kind']} {obj['metadata'].get('namespace', '')}/{obj['metadata']['name']}")


# ---------------------------------------------------------------- Trino ----
def principals(access: dict) -> tuple:
    """(users with a grant, groups with a grant or admin), sorted."""
    users = sorted({u for g in access["grants"] for u in g["users"]})
    groups = sorted({x for g in access["grants"] for x in g["groups"]} | set(access["admins"]))
    return users, groups


def trino_rules(access: dict) -> dict:
    """Schema and table DDL for Meta Catalog's provisioner only; data
    statements for everyone (Ceph refuses the files of a role that lacks
    them)."""
    data = ["SELECT", "INSERT", "DELETE", "UPDATE"]
    schemas, tables = [], []
    if access.get("provisioner"):
        who = f"^{re.escape(access['provisioner'])}$"
        schemas.append({"user": who, "owner": True})
        tables.append({"user": who, "privileges": data + ["OWNERSHIP"]})
    schemas.append({"owner": False})
    tables.append({"privileges": data})
    return {"catalogs": [{"allow": "all"}], "schemas": schemas, "tables": tables}


def memberships(groups: dict) -> dict:
    """{user: {groups}} from {group: [users]}."""
    member_of: dict = {}
    for g, members in groups.items():
        for m in members:
            member_of.setdefault(m, set()).add(g)
    return member_of


def user_roles(groups: dict, users: list, role_groups: list) -> dict:
    """{user: [role ARNs]} of every usable user with at least one role: their
    own first, then their groups' in alphabetical order."""
    member_of = memberships(groups)
    out = {}
    for u in sorted(set(member_of) | set(users)):
        if not USER_PATTERN.match(u):
            continue
        roles = [f"{ROLE_ARN_PREFIX}{USER_ROLE_PREFIX}{u}"] if u in users else []
        roles += [f"{ROLE_ARN_PREFIX}{GROUP_ROLE_PREFIX}{g}" for g in sorted(member_of.get(u, ())) if g in role_groups]
        if roles:
            out[u] = roles
    return out


def directory_roles(access: dict, user: str, member_of: set, roles: list) -> list:
    """[(directory, role ARN)] for every directory granted to the user or to
    one of their groups, the most specific first: the role Trino uses for a
    file there. Of the user's roles that reach the directory (granted it or
    a directory above it), rw before ro, then in the order of roles (their
    own first). The admin groups' bucket-wide rights are not a grant here:
    their role is only picked where a grant names the group."""
    own = f"{ROLE_ARN_PREFIX}{USER_ROLE_PREFIX}{user}"
    reach = []                                  # (directory, access, role)
    for g in access["grants"]:
        if user in g["users"] and own in roles:
            reach.append((g["path"], g["access"], own))
        for x in g["groups"]:
            arn = f"{ROLE_ARN_PREFIX}{GROUP_ROLE_PREFIX}{x}"
            if x in member_of and arn in roles:
                reach.append((g["path"], g["access"], arn))

    def covers(above: str, path: str) -> bool:
        return above == "" or path == above or path.startswith(above + "/")

    out = []
    # every directory that matches a location is a prefix of it: the longest
    # is the most specific
    for path in sorted({p for p, _, _ in reach}, key=lambda p: (-len(p), p)):
        best = min(((a, r) for above, a, r in reach if covers(above, path)),
                   key=lambda ar: (ar[0] != "rw", roles.index(ar[1])))
        out.append((path, best[1]))
    return out


def trino_objects(groups: dict, access: dict) -> dict:
    # only usable usernames: a ',' or ':' in one would break another group's line
    lines = [f"{g}:{','.join(u for u in users if USER_PATTERN.match(u))}"
             for g, users in sorted(groups.items()) if any(USER_PATTERN.match(u) for u in users)]
    users, role_groups = principals(access)
    member_of = memberships(groups)
    rules = []
    for u, roles in user_roles(groups, users, role_groups).items():
        # the trailing slash: projects/data must not match projects/data-x
        for path, role in directory_roles(access, u, member_of.get(u, set()), roles):
            rules.append({"user": f"^{re.escape(u)}$", "prefix": f"s3://{access['bucket']}/{path + '/' if path else ''}",
                          "iamRole": role, "allowedIamRoles": roles, "roleSessionName": "${USER}"})
        rules.append({"user": f"^{re.escape(u)}$", "iamRole": roles[0], "allowedIamRoles": roles,
                      "roleSessionName": "${USER}"})
    rules += [{"group": f"^{re.escape(g)}$", "iamRole": f"{ROLE_ARN_PREFIX}{GROUP_ROLE_PREFIX}{g}",
               "allowedIamRoles": [f"{ROLE_ARN_PREFIX}{GROUP_ROLE_PREFIX}{g}"], "roleSessionName": "${USER}"}
              for g in role_groups]
    # restarter ignore: Trino re-reads both files by itself (every 10 s / 30 s);
    # without it Stackable's restart controller would restart Trino (and kill
    # every running query) at each membership change.
    return {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "trino-iam", "namespace": TRINO_NAMESPACE,
                         "labels": {"restarter.stackable.tech/ignore": "true"}},
            "data": {"groups.txt": "\n".join(lines) + "\n",
                     "s3-security-mapping.json": json.dumps({"mappings": rules}, indent=2) + "\n",
                     "rules.json": json.dumps(trino_rules(access), indent=2) + "\n"}}


# ---------------------------------------------------------------- Spark ----
def spark_objects(groups: dict) -> list:
    """Who may submit jobs in the jobs' namespace: every Keycloak group."""
    return [
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
         "metadata": {"name": "spark-submitter", "namespace": TRINO_NAMESPACE},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": SPARK_SUBMITTER_ROLE},
         "subjects": [{"apiGroup": "rbac.authorization.k8s.io", "kind": "Group", "name": f"{OIDC_PREFIX}{g}"}
                      for g in sorted(groups)]},
    ]


# ---------------------------------------------------------------- loop ----
def sync_once() -> None:
    groups = read_groups()
    if not plausible(groups):
        return
    access = read_access()
    apply(trino_objects(groups, access))
    for obj in spark_objects(groups):
        apply(obj)


def main() -> None:
    for name in ("KEYCLOAK_URL", "KEYCLOAK_REALM", "KEYCLOAK_CLIENT_SECRET", "ROLE_ARN_PREFIX", "LABEL_DOMAIN"):
        if not os.environ.get(name):
            sys.exit(f"{name} is not set")
    log(f"iam-sync: realm {REALM} at {KEYCLOAK_URL}, settings from {ACCESS_FILE}, grants from the ConfigMap "
        f"{GRANTS_CONFIGMAP}, every {INTERVAL} s, into {TRINO_NAMESPACE}")
    full_resync = 0
    while True:
        try:
            if time.time() >= full_resync:      # re-apply everything every 10 min (drift, deletions)
                _applied.clear()
                full_resync = time.time() + 600
            sync_once()
        except Exception as e:  # noqa: BLE001 - keep running; the next round retries
            log(f"error: {e}")
        sys.stdout.flush()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
