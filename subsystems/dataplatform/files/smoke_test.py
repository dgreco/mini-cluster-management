#!/usr/bin/env python3
"""End-to-end check of the IAM model (make dataplatform-smoke-test).

Run by smoke-test.yml with Ansible's Python (boto3), after it created the
temporary users of SMOKE_USERS in the directory (Samba AD; JSON {name: {group,
groups}}, password SMOKE_PASSWORD): smoke-eng (the engineer group), smoke-ana (the analyst
group), smoke-both (both), smoke-nogroup (none), smoke-adm (analyst,
engineer and the admin group, last: the gate must find it among
several groups; it authorizes the tables).

Everything happens in the smoke test's own tables, the domain smoke
(metacatalog/domains.yaml, metacatalog/tables/smoke.yaml), which only
Meta Catalog creates and whose policies grant the engineer group the bronze
tables (the analyst group may read one of them) and the analyst group the
gold one (the site's groups: ADMIN_GROUP, ENGINEER_GROUP, ANALYST_GROUP). 0
loads the model (metacatalog/table-model.yaml) when Meta Catalog lacks it,
and the tables as make dataplatform-tables does (with its script,
DP_TABLES), and provisions them: a platform built from scratch needs no step
before this test. Z takes them out again at the end of every run, passed or
failed, so no grant of the test stays in the record.

0  Meta Catalog lets the engineer in and refuses the analyst (only
   the admin and engineer groups); the tables: in Meta Catalog, provisioned
   in their three formats; a domain outside metacatalog/domains.yaml is
   refused by Meta Catalog, a subdomain of another domain (which passes its
   two enums) by make dataplatform-tables at load and by the provisioning
   script, which makes nothing; the engineer may not
   authorize one (only the admin group: the gate, also against tricks with
   the path and a forged groups header), the administrator may, and their
   grants are in force within minutes.
A  direct STS: each user gets their group's role only; the role reaches the
   folders of the tables granted to it, not the other group's, nor the
   bucket's root; a role granted READ on a table reads its files and writes
   none; make dataplatform-s3-login writes working keys for the user's own
   group only.
B  Trino (bearer tokens; the Open Policy Agent decides the statements, Ceph
   the files): the engineer writes and reads the bronze tables, iceberg and
   avro; the analyst reads the table granted READ and may not write it, and
   is refused the others, as is a user in no group, by Trino itself; a user
   sees only the tables granted to them; nobody may create, alter, comment
   or drop a schema or a table (only Meta Catalog's provisioner), an
   administrator included; a member of both groups reads and writes both
   layers in one query, each with the role granted it, and is refused by
   Ceph when s3_role pins the other role.
C  Spark (web identity; the catalog gate knows the job's user and asks the
   Open Policy Agent, as Trino does): the engineer's job reads Trino's table
   and writes another that Trino reads back, and may not create, alter or
   drop a table (only Meta Catalog); the analyst's job reads the table
   granted READ, is refused the others, and may not move a table back to an
   older snapshot; the job's service account reads no Secret, starts no pod
   mounting another one, changes neither Trino's group file, its pods, its
   labels nor its Services, and reaches neither the Hive Metastore nor the
   Open Policy Agent.
D  Kubernetes (Keycloak login): the engineer may run jobs in compute, not
   read the platform's secrets, nor write objects the Spark fence refuses.
F  the directory is the only source of people, and only its administrators
   change it: a user it disabled cannot log in; a membership it gives after
   a user's first login is in a later token; Keycloak may not set a
   directory user's password; the realm holds no user of its own. The
   playbook prepares these (SMOKE_DISABLED_USER, SMOKE_LATE_USER and _GROUP,
   SMOKE_KEYCLOAK_WRITE, SMOKE_LOCAL_USERS). The directory answers LDAP
   clients outside the cluster, on ldaps://DIRECTORY_LDAPS_HOST:636 through
   the Gateway, with a certificate of its own authority (DIRECTORY_CA) that
   names that host: the engineer binds with its password, a wrong one is
   refused. There the administrator may change an entry of the people's unit
   (DIRECTORY_PEOPLE_DN), the engineer and the account Keycloak reads the
   directory as (DIRECTORY_BIND_USER, _PASSWORD) may not. And its web
   interface (DIRECTORY_UI_URL, LDAP Account Manager) is reached through a
   Keycloak login by the administrator alone, who then logs in to it with
   the same directory account.
Z  the tables of the domain smoke go out of Meta Catalog: the administrator
   rejects them (their keys leave the record of grants), the engineer
   unprovisions them (the metastore's tables and the files go) and deletes
   them. Before that, one more is authorized and deleted without a reject, as
   Meta Catalog's UI lets anyone: the playbook then runs iam-reconcile, which
   must take its grants out of the record.
Reads use sum(), not count(*) (answered from Iceberg statistics, no data read).
Prints one line per check and exits 1 when any check fails.
"""
import base64
import html
import http.cookiejar
import json
import os
import re
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3
import botocore

KEYCLOAK = os.environ["KEYCLOAK_ISSUER"]
S3 = os.environ["S3_ENDPOINT"]
TRINO = os.environ["TRINO_URL"]
SUBMIT = os.environ["SPARK_SUBMIT"]
S3_LOGIN = os.environ["S3_LOGIN"]
PREFIX = os.environ.get("GROUP_ROLE_PREFIX", "grp-")
ROLE_ARN = os.environ["ROLE_ARN_PREFIX"]
BUCKET = os.environ["S3_BUCKET"]
REGION = os.environ["S3_REGION"]
ADMIN, ENGINEER, ANALYST = os.environ["ADMIN_GROUP"], os.environ["ENGINEER_GROUP"], os.environ["ANALYST_GROUP"]
LABEL_DOMAIN = os.environ["LABEL_DOMAIN"]               # the platform's label keys: <it>/user
METACATALOG = os.environ["METACATALOG_URL"].rstrip("/") + "/metacatalog/v1"
TABLES = os.environ["DP_TABLES"]                       # files/dp-tables.sh
DOMAIN, SUBDOMAIN = "smoke", "test"                    # metacatalog/domains.yaml
DOMAINS_FILE = os.environ["SMOKE_DOMAINS_FILE"]        # its pairs, as make configure renders them (domains.json)
TABLES_FILE = os.environ["SMOKE_TABLES_FILE"]          # metacatalog/tables/smoke.yaml
MODEL_FILE = os.environ["SMOKE_MODEL_FILE"]            # metacatalog/table-model.yaml
ROOT = f"lakehouse/{DOMAIN}/{SUBDOMAIN}"               # where its tables' folders are, under the bucket
BRONZE, GOLD = f"{ROOT}/bronze", f"{ROOT}/gold"
# the tables (metacatalog/tables/smoke.yaml): the engineers' in bronze (the
# analysts may read eng_t), the analysts' in gold
SCHEMA_B, SCHEMA_G = f"lakehouse.{DOMAIN}_{SUBDOMAIN}_bronze", f"lakehouse.{DOMAIN}_{SUBDOMAIN}_gold"
ENG_T, SPARK_T, AVRO_T, ANA_T = f"{SCHEMA_B}.eng_t", f"{SCHEMA_B}.spark_t", f"{SCHEMA_B}.avro_t", f"{SCHEMA_G}.ana_t"
EXPECTED = ["eng_t", "spark_t", "avro_t", "drop_zone", "ana_t"]
ENG_DIR, ANA_DIR = f"{BRONZE}/drop_zone", f"{GOLD}/ana_t"     # a folder of each group, for the S3 checks
USERS = json.loads(os.environ["SMOKE_USERS"])          # {user: {"group": first group, "groups": [...]}}
PASSWORD = os.environ["SMOKE_PASSWORD"]
results = []


def check(label: str, ok: bool, detail: str = "") -> None:
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {label}{('  (' + detail + ')') if detail and not ok else ''}", flush=True)


CLIENT = os.environ.get("SMOKE_CLIENT", "smoke-test")               # the test's own confidential client:
CLIENT_SECRET = os.environ["SMOKE_CLIENT_SECRET"]                    # the only one with the password grant


def token(user: str, client: str = CLIENT, id_token: bool = False) -> str:
    """A token of the temporary user from the smoke-test client (audiences trino
    and kubernetes, groups; RGW accepts the client); `client` is kept for the
    call sites' readability."""
    data = urllib.parse.urlencode({"grant_type": "password", "client_id": CLIENT, "client_secret": CLIENT_SECRET,
                                   "username": user, "password": PASSWORD, "scope": "openid profile"}).encode()
    with urllib.request.urlopen(f"{KEYCLOAK}/protocol/openid-connect/token", data, timeout=30) as r:
        return json.load(r)["id_token" if id_token else "access_token"]


def ber(tag: int, body: bytes) -> bytes:
    """One BER element (LDAP's encoding): tag, length, content."""
    n = len(body)
    size = bytes([n]) if n < 128 else bytes([0x80 | ((n.bit_length() + 7) // 8)]) + n.to_bytes((n.bit_length() + 7) // 8, "big")
    return bytes([tag]) + size + body


def ldap_result(answer: bytes, tag: int) -> int:
    """The result code of an LDAPMessage {messageID, [APPLICATION tag] {resultCode ENUMERATED, ...}}."""
    def skip(at: int) -> int:                           # past an element's tag and length
        return at + (2 if answer[at + 1] < 128 else 2 + (answer[at + 1] & 0x7F))
    at = skip(0)                                        # the message
    at = skip(at) + answer[at + 1]                      # its id
    if answer[at] != tag or answer[skip(at)] != 0x0A:
        raise OSError(f"not the expected LDAP response: {answer[:24].hex()}")
    return answer[skip(at) + 2]


def ldap_bind(host: str, who: str, password: str, ca_pem: str, port: int = 636, change: tuple = ()) -> int:
    """A simple LDAP bind over TLS, as an LDAP client outside the cluster does
    it: the result code (0 accepted, 49 wrong credentials). The certificate
    must come from ca_pem's authority and name host. With change = (dn,
    attribute, value) the bind must succeed, and the result is that of
    replacing the attribute of that entry (0 done, 50 no right to)."""
    ctx = ssl.create_default_context(cadata=ca_pem)
    bind = ber(0x60, ber(0x02, b"\x03") + ber(0x04, who.encode()) + ber(0x80, password.encode()))
    with socket.create_connection((host, port), timeout=20) as raw, ctx.wrap_socket(raw, server_hostname=host) as tls:
        tls.sendall(ber(0x30, ber(0x02, b"\x01") + bind))
        code = ldap_result(tls.recv(4096), 0x61)        # BindResponse
        if not change:
            return code
        if code:
            raise OSError(f"{who} cannot bind: {code}")
        dn, attribute, value = change
        replace = ber(0x30, ber(0x0A, b"\x02") + ber(0x30, ber(0x04, attribute.encode()) + ber(0x31, ber(0x04, value.encode()))))
        tls.sendall(ber(0x30, ber(0x02, b"\x02") + ber(0x66, ber(0x04, dn.encode()) + ber(0x30, replace))))
        return ldap_result(tls.recv(4096), 0x67)        # ModifyResponse


def directory_ui(url: str, user: str) -> tuple:
    """A browser's way into the directory's web interface: the proxy in front
    of it sends the visitor to Keycloak, whose form takes the user's password,
    and lets the administrators through to LDAP Account Manager, whose own
    login takes the same account. Returns (the proxy's answer after the
    Keycloak login: an HTTP status, 0 when it never got that far; where LAM's
    login led, or what went wrong)."""
    browser = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    try:
        with browser.open(f"{url}/", timeout=30) as r:
            page, at = r.read().decode(errors="replace"), r.geturl()
    except urllib.error.HTTPError as e:
        return 0, f"{url}/ answers {e.code}"
    form = re.search(r'<form[^>]*id="kc-form-login"[^>]*>', page)
    action = re.search(r'action="([^"]+)"', form.group(0)) if form else None
    if not at.startswith(f"{KEYCLOAK}/protocol/openid-connect/auth") or "client_id=directory" not in at or not action:
        return 0, f"no Keycloak login form of the client directory at {at[:160]}"
    login = urllib.parse.urlencode({"username": user, "password": PASSWORD, "credentialId": ""}).encode()
    try:
        with browser.open(html.unescape(action.group(1)), login, timeout=30) as r:
            page, at = r.read().decode(errors="replace"), r.geturl()
    except urllib.error.HTTPError as e:
        return e.code, ""
    if not at.startswith(f"{url}/lam/"):
        return 200, f"after the Keycloak login: {at[:160]}"
    form = dict(re.findall(r'<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"', page))
    form.update({"username": user, "passwd": PASSWORD, "profile": "lam", "language": "en_GB.utf8", "checklogin": "Login"})
    try:
        with browser.open(f"{url}/lam/templates/login.php", urllib.parse.urlencode(form).encode(), timeout=30) as r:
            return 200, urllib.parse.urlsplit(r.geturl()).path
    except urllib.error.HTTPError as e:
        return 200, f"LDAP Account Manager refuses the login: {e.code}"


def sts(user: str, group: str):
    c = boto3.client("sts", endpoint_url=S3, region_name=REGION,
                     aws_access_key_id="x", aws_secret_access_key="x")
    try:
        cred = c.assume_role_with_web_identity(RoleArn=f"{ROLE_ARN}{PREFIX}{group}", RoleSessionName=user,
                                               WebIdentityToken=token(user, "data-cli"))["Credentials"]
        return boto3.client("s3", endpoint_url=S3, region_name=REGION, aws_access_key_id=cred["AccessKeyId"],
                            aws_secret_access_key=cred["SecretAccessKey"], aws_session_token=cred["SessionToken"])
    except botocore.exceptions.ClientError as e:
        return e.response["Error"]["Code"]


def s3_ok(fn) -> tuple:
    try:
        fn()
        return True, ""
    except botocore.exceptions.ClientError as e:
        return False, e.response["Error"]["Code"]


def trino(user: str, sql: str, role: str = "") -> tuple:
    """(ok, rows or error message); role: pin that STS role (extra credential s3_role)"""
    tok = token(user, "data-cli")
    hdr = {"Authorization": f"Bearer {tok}", "X-Trino-Source": "smoke-test"}
    if role:
        hdr["X-Trino-Extra-Credential"] = f"s3_role={urllib.parse.quote_plus(role)}"
    try:
        req = urllib.request.Request(f"{TRINO}/v1/statement", sql.encode(), hdr, method="POST")
        with urllib.request.urlopen(req, timeout=60) as r:
            res = json.load(r)
        rows = []
        while True:
            rows += res.get("data", [])
            if "error" in res:
                # the whole cause chain: Ceph's refusal is often a cause of a generic error
                msgs, info = [res["error"].get("message", "")], res["error"].get("failureInfo")
                while info:
                    msgs.append(info.get("message") or "")
                    info = info.get("cause")
                return False, " <- ".join(m for m in msgs if m)[:800]
            if "nextUri" not in res:
                return True, rows
            time.sleep(0.3)
            with urllib.request.urlopen(urllib.request.Request(res["nextUri"], headers=hdr), timeout=60) as r:
                res = json.load(r)
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}"


def spark(user: str, group: str, code: str) -> tuple:
    """(ok, driver output) of a PySpark job run as user with the group's role"""
    with tempfile.TemporaryDirectory() as d:
        job, tok = os.path.join(d, "smoke_job.py"), os.path.join(d, "token")
        with open(job, "w") as f:
            f.write(code)
        with open(tok, "w") as f:
            f.write(token(user, "spark"))
        p = subprocess.run([SUBMIT, "-g", group, "-n", f"smoke-{int(time.time())}", "-w", job],
                           env=dict(os.environ, SPARK_TOKEN_FILE=tok), capture_output=True, text=True, timeout=1800)
    return p.returncode == 0, p.stdout + p.stderr


def wait_for_trino(user: str, group: str) -> None:
    """Trino answers and knows the user's group. The group file reaches the pods
    through a ConfigMap volume (kubelet sync, up to a minute or two), then
    Trino re-reads it every 10 s."""
    end = time.time() + 600
    while time.time() < end:
        ok, rows = trino(user, "SELECT contains(current_groups(), '%s')" % group)
        if ok and rows == [[True]]:
            return
        time.sleep(10)


def metacatalog(user: str, method: str, path: str, body: bytes = None, ctype: str = "application/json",
                headers: dict = None, base: str = None):
    """(HTTP status, parsed answer) of Meta Catalog's API, with the user's token
    (base: another root than the API's, e.g. the site's for /ui)"""
    req = urllib.request.Request((METACATALOG if base is None else base) + path, body,
                                 {"Authorization": f"Bearer {token(user)}", "Content-Type": ctype, **(headers or {})},
                                 method=method)
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:500]


def entities(user: str, entity_type: str, where: str) -> tuple:
    """(HTTP status, [{id, values}]) of the entities of a type that match"""
    q = urllib.parse.urlencode({"entityTypeName": entity_type, "queryPath": f"$ ? ({where})"})
    code, rows = metacatalog(user, "GET", f"/entity?{q}")
    if code != 200:
        return code, rows
    return code, [{"id": r["id"], "values": json.loads(r["values"]) if isinstance(r["values"], str) else r["values"]}
                  for r in rows]


def tables_tool(user: str, *args) -> tuple:
    """(ok, output) of dp-tables.sh run as the user (make dataplatform-tables)"""
    with tempfile.TemporaryDirectory() as d:
        tok = os.path.join(d, "token")
        with open(tok, "w") as f:
            f.write(token(user))
        p = subprocess.run([TABLES, "-y", *args], capture_output=True, text=True, timeout=1800,
                           env=dict(os.environ, TABLES_TOKEN_FILE=tok, PYTHON=sys.executable))
    return p.returncode == 0, (p.stdout + p.stderr).strip()


def model_types() -> list:
    """the entity types the model file defines (its EntityTypes section, read
    like dp-tables.sh reads it)"""
    names, section = [], False
    for line in open(MODEL_FILE):
        if line.startswith("EntityTypes:"):
            section = True
        elif re.match(r"^[A-Za-z]", line):
            section = False
        elif section and line.startswith("  - name:"):
            names.append(line.split()[2].strip("\"'"))
    return names


def fixture(eng: str) -> bool:
    """The smoke test's tables exist, are provisioned and their grants are in
    force. Missing pieces are made through Meta Catalog: the engineer loads
    the model, the domain and the tables and provisions, the administrator
    authorizes (every run: unchanged grants change nothing)."""
    names = model_types()
    code, rows = entities(eng, "TableType", f'@.domain == "{DOMAIN}"')
    check(f"0a Meta Catalog answers {eng} ({METACATALOG})", code in (200, 400, 404), str(rows)[:200])
    if code not in (200, 400, 404):     # 400/404: the model is not there yet
        return False
    # only the admin and engineer groups get past the proxy in front of it
    ana = "smoke-ana"
    code_ana, out_ana = metacatalog(ana, "GET", "/entity-type")
    check(f"0b Meta Catalog refuses {ana} ({ANALYST}: not {ADMIN} nor {ENGINEER})",
          code_ana in (401, 403), f"HTTP {code_ana} {str(out_ana)[:200]}")
    have = sorted(r["values"]["name"] for r in rows) if code == 200 else []
    if have != sorted(EXPECTED):
        if have:
            check(f"0c the tables of the domain {DOMAIN} are in Meta Catalog", False,
                  f"{have} of {EXPECTED} only: finish or undo that by hand (make dataplatform-tables)")
            return False
        ok, out = tables_tool(eng, "load", TABLES_FILE)
        check(f"0c Meta Catalog loads the model ({len(names)} entity types) and the {len(EXPECTED)} tables "
              f"of the domain {DOMAIN}", ok, out[-400:])
        if not ok:
            return False
        code, rows = entities(eng, "TableType", f'@.domain == "{DOMAIN}"')
    by_name = {r["values"]["name"]: r["id"] for r in rows}
    ok, out = tables_tool(eng, "provision", DOMAIN)
    check(f"0c Meta Catalog provisions them ({len(EXPECTED)} folders, {len(EXPECTED) - 1} tables in the metastore: "
          "iceberg, avro, parquet)", ok, out[-600:])
    if not ok:
        return False
    tid = by_name["eng_t"]
    gate(eng, tid)
    adm = "smoke-adm"

    def in_force():
        s3e = sts(eng, USERS[eng]["group"])
        return (False, s3e) if isinstance(s3e, str) else s3_ok(lambda: s3e.list_objects_v2(Bucket=BUCKET, Prefix=f"{ENG_DIR}/"))
    before, _ = in_force()
    code, out = metacatalog(eng, "POST", f"/aggregate/{tid}/authorize")
    check(f"0d {eng} may not authorize a table (only {ADMIN})", code == 403, f"HTTP {code} {str(out)[:200]}")
    ok, out = tables_tool(adm, "authorize", DOMAIN)
    check(f"0d {adm} ({ADMIN}) authorizes them", ok, out[-600:])
    # Ceph's roles change during the call; iam-sync's next round (30 s) gives
    # Trino the mapping and the Open Policy Agent the tables' grants
    end, ok, err = time.time() + 180, False, ""
    while time.time() < end:
        ok, err = in_force()
        if ok:
            break
        time.sleep(10)
    check(f"0e the grants on the files are in force (Ceph)", ok, f"s3: {err}")
    if not ok:
        return False
    # ... and in Trino: the Agent has the grants (it loads them within half
    # a minute of iam-sync's round), and every Trino pod the mapping. The
    # authorize writes the record entry by entry, iam-sync may publish it in
    # two rounds, and each pod reads the mounted ConfigMap at its own
    # kubelet's refresh: so each user's own rules are waited for (the
    # engineer's default role passes on a first half of the record), on the
    # coordinator, and then the workers get the time a refresh takes (a
    # minute at most, and Trino's 30 s).
    probes = ((eng, ENG_T), ("smoke-ana", ENG_T), ("smoke-both", AVRO_T), ("smoke-both", ANA_T))
    end, missing = time.time() + (420 if not before else 180), []
    while time.time() < end:
        missing = []
        for user, table in probes:
            ok, out = trino(user, f"SELECT count(*) FROM {table}")
            if not ok:
                missing.append(f"{user} {table}: {str(out)[:150]}")
        if not missing:
            break
        time.sleep(10)
    check(f"0e the grants on the tables are in force (Trino), for each user", not missing, "; ".join(missing)[:400])
    if missing:
        return False
    started = time.time()
    domain_rules(eng, adm)
    time.sleep(max(0, 90 - (time.time() - started)))
    return True


WRONG_PAIR = "wrong_pair"       # 0i's table: in Meta Catalog, never provisioned (Z1 deletes it)


def table_doc(domain: str, subdomain: str, name: str) -> bytes:
    """A table of the smoke test's shape without a schema (a folder only), as
    an aggregate document"""
    return (f'ref: "{name}"\nentityType: "TableType"\nvalues:\n  name: "{name}"\n'
            f'  description: "A check of the domains"\n  domain: "{domain}"\n  subdomain: "{subdomain}"\n'
            f'  layer: "bronze"\n  format: "parquet"\n'
            f'  principals:\n    - {{ id: "{ENGINEER}", type: "GROUP" }}\n'
            f'  permissions:\n    - {{ name: "READ", description: "Read its files" }}\n'
            f'  grants:\n    - {{ principals: ["{ENGINEER}"], permissions: ["READ"] }}\n').encode()


def domain_rules(eng: str, adm: str) -> None:
    """A table's domain and subdomain are a pair of metacatalog/domains.yaml.
    Meta Catalog refuses a domain outside its enum; a subdomain of another
    domain passes its two enums (it keeps no rule across two properties), so
    make dataplatform-tables refuses it at load, and the provisioning script
    before it makes anything."""
    code, out = metacatalog(eng, "POST", "/aggregate/yaml", table_doc("nosuchdomain", SUBDOMAIN, "bad_domain"),
                            ctype="application/octet-stream")
    found, rows = entities(eng, "TableType", '@.name == "bad_domain"')
    check("0g Meta Catalog refuses a table whose domain is not one of metacatalog/domains.yaml, and keeps nothing",
          400 <= code < 500 and found == 200 and not rows, f"HTTP {code} {str(out)[:200]}; kept: {rows if found == 200 else found}")
    pairs = json.load(open(DOMAINS_FILE))
    other = next((sub for d, subs in pairs.items() if d != DOMAIN for sub in subs if sub not in pairs.get(DOMAIN, [])), None)
    if other is None:
        print(f"0h, 0i not run: metacatalog/domains.yaml has no subdomain outside the domain {DOMAIN}'s")
        return
    with tempfile.NamedTemporaryFile("wb", suffix=".yaml", delete=False) as f:
        f.write(table_doc(DOMAIN, other, WRONG_PAIR))
    try:
        ok, out = tables_tool(eng, "load", f.name)
    finally:
        os.unlink(f.name)
    found, rows = entities(eng, "TableType", f'@.name == "{WRONG_PAIR}"')
    check(f"0h make dataplatform-tables refuses to load {DOMAIN}/{other} (a subdomain of another domain)",
          not ok and "is not one of the domain" in out and found == 200 and not rows, out[-300:])
    # straight through the API, where Meta Catalog's two enums let it in
    code, out = metacatalog(eng, "POST", "/aggregate/yaml", table_doc(DOMAIN, other, WRONG_PAIR),
                            ctype="application/octet-stream")
    if code != 204:
        check(f"0i Meta Catalog takes {DOMAIN}/{other} through its API (each enum on its own)", False,
              f"HTTP {code} {str(out)[:200]}")
        return
    end, folders = time.time() + 60, []            # derivation is asynchronous
    while time.time() < end:
        found, folders = entities(eng, "S3FolderType", f'@.domain == "{DOMAIN}" && @.subdomain == "{other}"'
                                                         f' && @.table == "{WRONG_PAIR}"')
        if found == 200 and folders:
            break
        time.sleep(2)
    if not folders:
        check(f"0i Meta Catalog derives the folder of {DOMAIN}/{other}/bronze/{WRONG_PAIR}", False, f"HTTP {found}")
        return
    ok, out = tables_tool(eng, "provision", f"{DOMAIN}/{other}/bronze/{WRONG_PAIR}")
    s3 = sts(adm, ADMIN)
    made = s3 if isinstance(s3, str) else s3.list_objects_v2(Bucket=BUCKET, Prefix=f"lakehouse/{DOMAIN}/{other}/").get("KeyCount", 0)
    check(f"0i the provisioning refuses {DOMAIN}/{other}, and makes nothing",
          not ok and "is not one of the domain" in out and made == 0, f"{out[-300:]}; objects there: {made}")


def gate(eng: str, lid: str) -> None:
    """Only the admin group may authorize or reject, in the API and the UI:
    every way of writing those paths, and a groups header the client made
    up, end at the gate (403) for the engineer."""
    site = METACATALOG[:-len("/metacatalog/v1")]
    tries = [(f"/aggregate/{lid}/authorize", {}, None), (f"/aggregate/{lid}/reject", {}, None),
             (f"/aggregate/{lid}/authorize?async=true", {}, None), (f"/aggregate/{lid}/authorize/", {}, None),
             (f"/aggregate/{lid}/authorize;x", {}, None), (f"/aggregate/{lid}/AUTHORIZE", {}, None),
             (f"/aggregate/{lid}/%61uthorize", {}, None), (f"//aggregate//{lid}//reject", {}, None),
             (f"/aggregate/{lid}/authorize", {"X-Forwarded-Groups": ADMIN}, None),
             (f"/ui/instances/{lid}/procedure/authorize", {}, site), (f"/ui/instances/{lid}/procedure/reject", {}, site)]
    let_through = []
    for path, headers, base in tries:
        code, _ = metacatalog(eng, "POST", path, headers=headers, base=base)
        if code not in (403, 400):        # the gate, or the Gateway refusing an odd path first
            let_through.append(f"{path}{' (forged groups)' if headers else ''}: HTTP {code}")
    check(f"0f the gate refuses {eng} authorize and reject however written ({len(tries)} ways)", not let_through,
          "; ".join(let_through))


def empty_folder(user: str, group: str, folder: str) -> None:
    """Deletes the files of a folder with the group's role, not the folder
    itself (the object <folder>/ the table was made on)."""
    s3 = sts(user, group)
    if isinstance(s3, str):
        return
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{folder}/"):
        for o in page.get("Contents", []):
            if o["Key"] != f"{folder}/":
                s3.delete_object(Bucket=BUCKET, Key=o["Key"])


def empty_tables(eng: str, both: str) -> None:
    """The fixture's tables hold only what this run writes. An Iceberg table
    is emptied with DELETE; a table of plain files (avro, parquet) takes no
    DELETE in Trino: its files are deleted."""
    for t in (ENG_T, SPARK_T):
        trino(eng, f"DELETE FROM {t}")
    try:
        empty_folder(eng, USERS[eng]["group"], f"{BRONZE}/avro_t")
        empty_folder(eng, USERS[eng]["group"], ENG_DIR)
        empty_folder("smoke-ana", USERS["smoke-ana"]["group"], ANA_DIR)
    except Exception as e:  # noqa: BLE001 - best effort, the checks report what is left
        print(f"emptying the smoke test's folders: {e}")


def remove_domain(eng: str, adm: str) -> None:
    """The tables of the domain smoke go out of Meta Catalog and the record of
    grants: rejected (the administrator), unprovisioned (their entries in the
    metastore and their files go) and deleted, 0i's among them. The first
    step that fails stops it, and the check says what is left. Nothing to do
    when Meta Catalog lacks the model."""
    code, rows = entities(eng, "TableType", f'@.domain == "{DOMAIN}"')
    if code in (400, 404):
        return
    steps = []
    if code != 200:
        check(f"Z1 the tables of the domain {DOMAIN} go out of Meta Catalog", False,
              f"listing them: HTTP {code} {str(rows)[:200]}")
        return
    if rows:
        for who, step in ((adm, "reject"), (eng, "unprovision"), (eng, "delete")):
            ok, out = tables_tool(who, step, DOMAIN)
            if not ok:
                check(f"Z1 the tables of the domain {DOMAIN} go out of Meta Catalog", False,
                      f"{step} failed, done: {', '.join(steps) or 'nothing'}: {out[-400:]}")
                return
            steps.append(step)
    code, rows = entities(eng, "TableType", f'@.domain == "{DOMAIN}"')
    check(f"Z1 the tables of the domain {DOMAIN} go out of Meta Catalog: rejected, unprovisioned and deleted",
          code == 200 and not rows, f"still there: HTTP {code} {len(rows) if code == 200 else '?'}")


ORPHAN = "orphan_t"             # Z2's table: authorized, then deleted without a reject


def orphan(eng: str, adm: str) -> None:
    """A table deleted in Meta Catalog without a reject (its UI's delete, its
    API's) leaves its grants in the record of grants: iam-reconcile takes
    them out. Here one is made, authorized and deleted that way; the
    playbook then runs iam-reconcile and fails if anything of the domain
    smoke is left in the record. A folder only, never provisioned: nothing
    in S3."""
    label = f"Z2 a table authorized, then deleted in Meta Catalog without a reject (as its UI's delete does)"
    ref = f"{DOMAIN}/{SUBDOMAIN}/bronze/{ORPHAN}"
    code, out = metacatalog(eng, "POST", "/aggregate/yaml", table_doc(DOMAIN, SUBDOMAIN, ORPHAN),
                            ctype="application/octet-stream")
    if code != 204:
        check(label, False, f"loading it: HTTP {code} {str(out)[:200]}")
        return
    end, folders = time.time() + 60, []            # derivation is asynchronous
    while time.time() < end:
        found, folders = entities(eng, "S3FolderType", f'@.domain == "{DOMAIN}" && @.table == "{ORPHAN}"')
        if found == 200 and folders:
            break
        time.sleep(2)
    ok, out = tables_tool(adm, "authorize", ref) if folders else (False, "its folder was not derived")
    found, rows = entities(eng, "TableType", f'@.domain == "{DOMAIN}" && @.name == "{ORPHAN}"')
    code = 0
    if found == 200 and rows:
        code, _ = metacatalog(eng, "DELETE", f"/aggregate/{rows[0]['id']}")
    found, rows = entities(eng, "TableType", f'@.domain == "{DOMAIN}" && @.name == "{ORPHAN}"')
    check(label, ok and code in (200, 204) and found == 200 and not rows,
          f"authorize: {out[-200:]}; delete: HTTP {code}; still there: {rows if found == 200 else found}")


def cleanup(eng: str, both: str) -> None:
    """Whatever the run left behind: the rows, the S3 checks' objects, then
    the domain smoke itself (the next run loads it again)."""
    empty_tables(eng, both)
    try:
        orphan(eng, "smoke-adm")
    except Exception as e:  # noqa: BLE001 - reported as a failed check
        check("Z2 a table authorized, then deleted in Meta Catalog without a reject", False, repr(e)[:400])
    try:
        remove_domain(eng, "smoke-adm")
    except Exception as e:  # noqa: BLE001 - reported as a failed check
        check(f"Z1 the tables of the domain {DOMAIN} go out of Meta Catalog", False, repr(e)[:400])


def main() -> None:
    eng, both = "smoke-eng", "smoke-both"
    try:
        if fixture(eng):
            run()
    finally:
        cleanup(eng, both)
    passed = sum(results)
    print(f"\n{passed}/{len(results)} checks passed")
    sys.exit(0 if passed == len(results) else 1)


def denied(out) -> bool:
    """Trino refused the statement (its access control, not Ceph)"""
    return "Access Denied" in str(out) and "403" not in str(out)


def ceph_refused(out) -> bool:
    return any(x in str(out) for x in ("Access Denied", "AccessDenied", "403"))


def denied_or_unread(out) -> bool:
    """Trino refused the statement, or could not load the Iceberg table at
    all: its metadata is read with the user's role before Trino's rules are
    asked, Ceph refuses a role with no grant there, and Trino 481's lakehouse
    connector then fails on its own stand-in for an unreadable table"""
    return denied(out) or "Unsupported table handle: io.trino.plugin.iceberg.CorruptedIcebergTableHandle" in str(out)


def run() -> None:
    eng, ana, nog, both, adm = "smoke-eng", "smoke-ana", "smoke-nogroup", "smoke-both", "smoke-adm"
    ge, ga = USERS[eng]["group"], USERS[ana]["group"]
    de, da = ENG_DIR, ANA_DIR      # a folder the tables' policies grant each of the two groups

    # ---------------------------------------------------------------- A ----
    s3e = sts(eng, ge)
    check(f"A1 {eng} assumes {PREFIX}{ge}", not isinstance(s3e, str), str(s3e))
    check(f"A2 {eng} may not assume {PREFIX}{ga}", sts(eng, ga) == "AccessDenied")
    check(f"A3 {nog} may not assume {PREFIX}{ge}", sts(nog, ge) == "AccessDenied")
    if not isinstance(s3e, str):
        ok, err = s3_ok(lambda: s3e.put_object(Bucket=BUCKET, Key=f"{de}/smoke/hello.txt", Body=b"hello"))
        check(f"A4 {PREFIX}{ge} writes s3://{BUCKET}/{de}/", ok, err)
        ok, err = s3_ok(lambda: s3e.list_objects_v2(Bucket=BUCKET, Prefix=f"{da}/"))
        check(f"A5 {PREFIX}{ge} is refused on s3://{BUCKET}/{da}/", not ok and err == "AccessDenied", err or "allowed")
        ok, err = s3_ok(lambda: s3e.put_object(Bucket=BUCKET, Key=f"{da}/smoke/hello.txt", Body=b"hello"))
        check(f"A6 {PREFIX}{ge} may not write s3://{BUCKET}/{da}/", not ok and err == "AccessDenied", err or "allowed")
        ok, err = s3_ok(lambda: s3e.list_objects_v2(Bucket=BUCKET))
        check(f"A7 {PREFIX}{ge} may not list the whole bucket", not ok and err == "AccessDenied", err or "allowed")
        ok, err = s3_ok(lambda: s3e.list_objects_v2(Bucket=BUCKET, Prefix=f"{BRONZE}/"))
        check(f"A7 {PREFIX}{ge} may not list the layer either: a grant is a table's", not ok and err == "AccessDenied",
              err or "allowed")
        s3_ok(lambda: s3e.delete_object(Bucket=BUCKET, Key=f"{de}/smoke/hello.txt"))

    # make dataplatform-s3-login (dp-s3-login.sh) writes working keys, and only
    # for the user's own group
    with tempfile.TemporaryDirectory() as d:
        tok, env_file = os.path.join(d, "token"), os.path.join(d, "sso.env")
        with open(tok, "w") as f:
            f.write(token(eng, "data-cli"))
        p = subprocess.run([S3_LOGIN, "-g", ge, "-o", env_file], env=dict(os.environ, S3_LOGIN_TOKEN_FILE=tok),
                           capture_output=True, text=True, timeout=120)
        keys = {}
        if p.returncode == 0:
            for line in open(env_file):
                if line.startswith("export ") and "=" in line:
                    k, v = line[len("export "):].strip().split("=", 1)
                    keys[k] = v
        ok, err = (False, p.stderr.strip()[-300:]) if not keys else s3_ok(lambda: boto3.client(
            "s3", endpoint_url=S3, region_name=REGION, aws_access_key_id=keys["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=keys["AWS_SECRET_ACCESS_KEY"], aws_session_token=keys["AWS_SESSION_TOKEN"]
        ).put_object(Bucket=BUCKET, Key=f"{de}/smoke/s3-login.txt", Body=b"hello"))
        check(f"A8 dataplatform-s3-login gives {eng} working keys for s3://{BUCKET}/{de}/", ok, err)
        with open(tok, "w") as f:
            f.write(token(ana, "data-cli"))
        p = subprocess.run([S3_LOGIN, "-g", ge, "-o", env_file + ".ana"], env=dict(os.environ, S3_LOGIN_TOKEN_FILE=tok),
                           capture_output=True, text=True, timeout=120)
        check(f"A9 dataplatform-s3-login refuses {ana} the role of {ge}", p.returncode != 0 and "AccessDenied" in p.stderr,
              p.stderr.strip()[-300:])
        if not isinstance(s3e, str):
            s3_ok(lambda: s3e.delete_object(Bucket=BUCKET, Key=f"{de}/smoke/s3-login.txt"))

    # ---------------------------------------------------------------- B ----
    wait_for_trino(eng, ge)
    wait_for_trino(ana, ga)
    wait_for_trino(both, ge)
    empty_tables(eng, both)                  # what an earlier, failed run left
    ok, out = trino(eng, f"INSERT INTO {ENG_T} VALUES 1, 2, 3, 4")
    check(f"B1 {eng} writes {ENG_T} (bronze, iceberg) through Trino", ok, str(out))
    ok, out = trino(eng, f"SELECT sum(x) FROM {ENG_T}")
    check(f"B2 {eng} reads it back (sum = 10)", ok and out == [[10]], str(out))
    ok, out = trino(eng, f"INSERT INTO {AVRO_T} VALUES (5, TIMESTAMP '2026-01-01 10:00:00'), (6, NULL)")
    if ok:
        ok, out = trino(eng, f"SELECT sum(x) FROM {AVRO_T}")
    check(f"B2 {eng} writes and reads {AVRO_T} (avro: sum = 11)", ok and out == [[11]], str(out))
    # READ on a table: Trino lets the analyst select and nothing else, and
    # their role reads the table's files and writes none
    ok, out = trino(ana, f"SELECT sum(x) FROM {ENG_T}")
    check(f"B3 {ana}, granted READ, reads {ENG_T} (sum = 10)", ok and out == [[10]], str(out))
    ok, out = trino(ana, f"INSERT INTO {ENG_T} VALUES 9")
    check(f"B3 {ana} may not write it: Trino refuses", not ok and denied(out), str(out))
    s3a = sts(ana, ga)
    if not isinstance(s3a, str):
        ok, err = s3_ok(lambda: s3a.list_objects_v2(Bucket=BUCKET, Prefix=f"{BRONZE}/eng_t/"))
        check(f"B3 {PREFIX}{ga} lists the table's files", ok, err)
        ok, err = s3_ok(lambda: s3a.put_object(Bucket=BUCKET, Key=f"{BRONZE}/eng_t/smoke.txt", Body=b"x"))
        check(f"B3 {PREFIX}{ga} may not write one: Ceph refuses", not ok and err == "AccessDenied", err or "allowed")
    else:
        check(f"B3 {ana} assumes {PREFIX}{ga}", False, s3a)
    # Ceph refuses the analyst's role the table's metadata, which Trino reads
    # before it asks its rules: the refusal is Ceph's (README, Known limits)
    ok, out = trino(ana, f"SELECT sum(id) FROM {SPARK_T}")
    check(f"B4 {ana} is refused {SPARK_T}, not granted: Ceph or Trino refuses", not ok and denied_or_unread(out), str(out))
    ok, out = trino(nog, f"SELECT sum(x) FROM {ENG_T}")
    check(f"B4 {nog}, in no group, is refused by Trino", not ok and denied(out), str(out))
    ok, out = trino(ana, f"SHOW TABLES FROM {SCHEMA_B}")
    check(f"B4 {ana} sees only the table granted to them in {SCHEMA_B}", ok and out == [["eng_t"]], str(out))
    ok, out = trino(nog, "SHOW SCHEMAS FROM lakehouse")
    check(f"B4 {nog} sees no schema of the tables", ok and out == [["information_schema"]], str(out))
    # Only Meta Catalog creates, changes or drops a schema or a table: Trino
    # gives DDL to its provisioner alone, even where a user may write the data.
    for n, (label, sql) in enumerate((("create a schema", "CREATE SCHEMA lakehouse.smoke_by_hand"),
                       ("create a table", f"CREATE TABLE {SCHEMA_B}.by_hand (x int)"),
                       ("create a table from a query", f"CREATE TABLE {SCHEMA_B}.by_hand AS SELECT 1 AS x"),
                       ("comment a table", f"COMMENT ON TABLE {ENG_T} IS 'smoke'"),
                       ("add a column", f"ALTER TABLE {ENG_T} ADD COLUMN z int"),
                       ("drop a table", f"DROP TABLE {ENG_T}"))):
        ok, out = trino(eng, sql)
        check(f"B5{'abcdef'[n]} {eng} may not {label}, though the table is theirs", not ok and denied(out), str(out))
    ok, out = trino(adm, f"DROP TABLE {ENG_T}")
    check(f"B5g {adm} ({ADMIN}) may not drop a table either", not ok and denied(out), str(out))
    # A member of both groups: Trino picks, for each file, the role granted
    # its directory (iam-sync's per-directory rules). Their default role is
    # the analysts' (first alphabetically), which Ceph refuses in the
    # engineers' folders, so reading there proves the per-directory rule.
    # The mapping file is re-read every 30 s, after the group file: retry.
    end = time.time() + 90
    while True:
        ok, out = trino(both, f"SELECT sum(x) FROM {AVRO_T}")
        if ok or time.time() > end:
            break
        time.sleep(10)
    check(f"B6 {both} reads {AVRO_T} with no role chosen", ok and out == [[11]], str(out))
    ok, out = trino(both, f"INSERT INTO {ANA_T} VALUES 5")
    if ok:
        ok, out = trino(both, f"SELECT (SELECT sum(x) FROM {ENG_T}) + (SELECT sum(y) FROM {ANA_T})")
    check(f"B7 {both} writes {ANA_T} (gold, parquet) and reads both layers in one query (sum = 15)",
          ok and out == [[15]], str(out))
    # Trino's rules let a member of the engineer group read the table; with
    # the analysts' role pinned, the files are refused all the same
    ok, out = trino(both, f"SELECT sum(x) FROM {AVRO_T}", role=f"{ROLE_ARN}{PREFIX}{ga}")
    check(f"B8 with s3_role pinned to {PREFIX}{ga}, {both} is refused {AVRO_T} by Ceph",
          not ok and ceph_refused(out), str(out))

    # ---------------------------------------------------------------- C ----
    # A Spark job reaches the catalog through the catalog gate, which knows
    # the job's user and asks the Open Policy Agent what Trino asks: the
    # engineer's job reads and writes the engineers' tables.
    ok, out = spark(eng, ge, f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print("SMOKE trino-table", spark.sql("SELECT sum(x) FROM {ENG_T}").collect()[0][0])
spark.sql("INSERT INTO {SPARK_T} SELECT id FROM range(5)")
print("SMOKE spark-table", spark.sql("SELECT sum(id) FROM {SPARK_T}").collect()[0][0])
print("SMOKE path", spark.sql("SELECT file_path FROM {SPARK_T}.files LIMIT 1").collect()[0][0])
# Tables are Meta Catalog's to make, change and drop: the gate refuses a job,
# also on a table its user may write.
def tried(label, fn):
    try:
        fn()
        print("SMOKE", label, "DONE")
    except Exception as e:
        said = [l for l in str(e).splitlines() if "Meta Catalog" in l]
        print("SMOKE", label, "refused:", (said[0] if said else str(e).splitlines()[0])[:240])
tried("ddl-create", lambda: spark.sql("CREATE TABLE {SCHEMA_B}.by_job (id bigint) USING iceberg"))
tried("ddl-alter", lambda: spark.sql("ALTER TABLE {SPARK_T} ADD COLUMN z int"))
tried("ddl-drop", lambda: spark.sql("DROP TABLE {SPARK_T}"))
# The fence (spark-fence.yaml, the job ClusterRole): the job's own service
# account may neither read a Secret of compute nor start a pod that mounts
# one, and changes nothing Trino lives by: its group file, its pods, the
# labels it is known by, its Services (dry runs).
import base64, json, socket, ssl, urllib.error, urllib.request
sa = "/var/run/secrets/kubernetes.io/serviceaccount"
ctx = ssl.create_default_context(cafile=sa + "/ca.crt")
bearer = "Bearer " + open(sa + "/token").read().strip()
def k8s(method, path, body=None, ctype="application/json"):
    req = urllib.request.Request("https://kubernetes.default.svc" + path, body,
                                 {{"Authorization": bearer, "Content-Type": ctype}}, method=method)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            return r.status, r.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:400]
ns = "/api/v1/namespaces/compute"
print("SMOKE secret-read", k8s("GET", ns + "/secrets/trino-s3")[0])
claims = json.loads(base64.urlsafe_b64decode(bearer.split(".")[1] + "=="))
me = claims["sub"].rsplit(":", 1)[1]                  # the job's own service account
mine = claims["kubernetes.io"]["pod"]["name"]         # and this pod
pod = {{"apiVersion": "v1", "kind": "Pod", "metadata": {{"generateName": "smoke-fence-", "namespace": "compute"}},
       "spec": {{"serviceAccountName": me, "containers": [{{"name": "c", "image": "busybox"}}],
                "volumes": [{{"name": "s", "secret": {{"secretName": "trino-s3"}}}}]}}}}
code, msg = k8s("POST", ns + "/pods?dryRun=All", json.dumps(pod).encode())
print("SMOKE pod-mount", code, "fence" if "dataplatform-spark-pods" in msg else msg[:120].replace(chr(10), " "))
def fence(label, path, patch, policy):
    code, msg = k8s("PATCH", path + "?dryRun=All", json.dumps(patch).encode(), "application/merge-patch+json")
    print("SMOKE fence", label, code, "refused" if policy in msg else msg[:160].replace(chr(10), " "))
fence("groups", ns + "/configmaps/trino-iam", {{"data": {{"groups.txt": "{ADMIN}:" + me}}}}, "dataplatform-compute-configmaps")
code, pods = k8s("GET", ns + "/pods?labelSelector=app.kubernetes.io/name%3Dtrino")
theirs = json.loads(pods)["items"][0]["metadata"]["name"] if code == 200 else "trino-coordinator-default-0"
fence("trino-pod", ns + "/pods/" + theirs, {{"metadata": {{"labels": {{"smoke": "x"}}}}}}, "dataplatform-compute-pods")
fence("trino-label", ns + "/pods/" + mine, {{"metadata": {{"labels": {{"app.kubernetes.io/name": "trino"}}}}}},
      "dataplatform-compute-pod-labels")
fence("trino-service", ns + "/services/trino-coordinator", {{"spec": {{"selector": {{"spark-role": "driver"}}}}}},
      "dataplatform-compute-services")
# The doors of governance that are Trino's alone (network policies): a job's
# pod reaches neither the metastore nor the Open Policy Agent, only the gate.
def door(label, host, port):
    s = socket.socket()
    s.settimeout(5)
    try:
        s.connect((host, port))
        print("SMOKE door", label, "OPEN")
    except (ConnectionRefusedError, TimeoutError) as e:
        print("SMOKE door", label, "closed")
    except OSError as e:
        print("SMOKE door", label, "not tried:", e)
    finally:
        s.close()
door("thrift", "hive-metastore.governance.svc.cluster.local", 9083)
door("rest", "hive-metastore-iceberg.governance.svc.cluster.local", 9001)
door("agent", "opa-server.governance.svc.cluster.local", 8081)
spark.stop()
""")
    tail = out[-900:]
    check(f"C1 {eng}'s Spark job reads Trino's table", ok and "SMOKE trino-table 10" in out, tail)
    check(f"C2 {eng}'s Spark job writes {SPARK_T}", ok and "SMOKE spark-table 10" in out, tail)
    check(f"C3 the table's files are in s3://{BUCKET}/{BRONZE}/spark_t/", f"SMOKE path s3://{BUCKET}/{BRONZE}/spark_t/" in out, tail)

    def said(label: str) -> str:
        """the line a probe of the job printed"""
        return next((line for line in out.splitlines() if line.startswith(f"SMOKE {label} ")), f"no line SMOKE {label}")
    for n, (label, what) in enumerate((("ddl-create", "create a table"), ("ddl-alter", "add a column"),
                                       ("ddl-drop", "drop a table"))):
        line = said(label)
        check(f"C4{'abc'[n]} {eng}'s Spark job may not {what}, though the table is theirs: the catalog gate refuses",
              " refused: " in line and "Meta Catalog" in line, line)
    check(f"C5 {eng}'s Spark job may not read a Secret of compute", "SMOKE secret-read 403" in out, tail)
    check(f"C6 {eng}'s Spark job may not start a pod mounting another Secret",
          re.search(r"SMOKE pod-mount 4(03|22) fence", out) is not None, said("pod-mount"))
    for n, (label, what) in enumerate((("groups", "rewrite Trino's group file"), ("trino-pod", "change a pod of Trino"),
                                       ("trino-label", "give its own pod Trino's label"),
                                       ("trino-service", "change Trino's Service"))):
        line = said(f"fence {label}")
        check(f"C6{'abcd'[n]} {eng}'s Spark job may not {what}", re.search(r" 4(03|22) refused$", line) is not None, line)
    for n, (label, what) in enumerate((("thrift", "the Hive Metastore's Thrift port"), ("rest", "its REST catalog"),
                                       ("agent", "the Open Policy Agent"))):
        line = said(f"door {label}")
        check(f"C6{'efg'[n]} {eng}'s Spark job cannot reach {what}", line.endswith(" closed"), line)
    ok2, out2 = trino(eng, f"SELECT sum(id) FROM {SPARK_T}")
    check(f"C7 Trino reads the table Spark wrote (sum = 10)", ok2 and out2 == [[10]], str(out2))
    # The analyst's job: the gate lets it read the table the analysts were
    # granted READ on and no other, and commit to neither. A commit that
    # moves a table back to an older snapshot writes no file, so Ceph would
    # not see it: the gate is what refuses it.
    schema = SCHEMA_B.split(".", 1)[1]
    ok, out = spark(ana, ga, f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
def tried(label, fn):
    try:
        print("SMOKE", label, "DONE", fn())
    except Exception as e:
        said = [l for l in str(e).splitlines() if "Meta Catalog" in l]
        print("SMOKE", label, "refused:", (said[0] if said else str(e).splitlines()[0])[:240])
print("SMOKE granted-read", spark.sql("SELECT sum(x) FROM {ENG_T}").collect()[0][0])
tried("not-granted", lambda: spark.sql("SELECT sum(id) FROM {SPARK_T}").collect()[0][0])
ids = [r[0] for r in spark.sql("SELECT snapshot_id FROM {ENG_T}.snapshots ORDER BY committed_at").collect()]
if len(ids) > 1:
    tried("rollback", lambda: spark.sql(f"CALL lakehouse.system.rollback_to_snapshot('{schema}.eng_t', {{ids[0]}})").collect())
else:
    print("SMOKE rollback not tried: the table has", len(ids), "snapshot")
spark.stop()
""")
    check(f"C8 {ana}'s Spark job, granted READ, reads {ENG_T} (sum = 10)", "SMOKE granted-read 10" in out, out[-900:])
    line = said("not-granted")
    check(f"C8 {ana}'s Spark job is refused {SPARK_T}, not granted: the catalog gate refuses",
          " refused: " in line and "no READ grant" in line, line)
    line = said("rollback")
    check(f"C9 {ana}'s Spark job may not move {ENG_T} back to an older snapshot: the catalog gate refuses",
          " refused: " in line and "no WRITE grant" in line, line)
    ok2, out2 = trino(eng, f"SELECT sum(x) FROM {ENG_T}")
    check(f"C9 {ENG_T} is as it was (sum = 10)", ok2 and out2 == [[10]], str(out2))

    # ---------------------------------------------------------------- D ----
    # Kubernetes API with the engineer's Keycloak login (client kubernetes)
    if os.environ.get("K8S_SERVER"):
        with tempfile.TemporaryDirectory() as d:
            cfg = os.path.join(d, "config")
            with open(cfg, "w") as f:
                json.dump({"apiVersion": "v1", "kind": "Config",
                           "clusters": [{"name": "c", "cluster": {"server": os.environ["K8S_SERVER"],
                                                                  "certificate-authority-data": os.environ["K8S_CA"]}}],
                           "users": [{"name": "u", "user": {"token": token(eng, "kubernetes", id_token=True)}}],
                           "contexts": [{"name": "x", "context": {"cluster": "c", "user": "u"}}],
                           "current-context": "x"}, f)

            def can(verb: str, resource: str, ns: str) -> str:
                p = subprocess.run(["kubectl", "--kubeconfig", cfg, "auth", "can-i", verb, resource, "-n", ns],
                                   capture_output=True, text=True, timeout=60)
                return p.stdout.strip() or p.stderr.strip()[:200]

            who = subprocess.run(["kubectl", "--kubeconfig", cfg, "auth", "whoami", "-o", "jsonpath={.status.userInfo.username}"],
                                 capture_output=True, text=True, timeout=60).stdout
            check(f"D1 Kubernetes knows the Keycloak login as oidc:{eng}", who == f"oidc:{eng}", who)
            r = can("create", "sparkapplications.spark.stackable.tech", "compute")
            check(f"D2 {eng} may run Spark jobs in compute", r == "yes", r)
            r = can("get", "secrets", "compute")
            check(f"D3 {eng} may not read secrets of compute (the other users' tokens)", r == "no", r)
            r = can("get", "secrets", "governance")
            check(f"D4 {eng} may not read secrets of governance", r == "no", r)

            def refused(manifest: dict) -> str:
                """'' when the API server refuses the object (dry run), else why not"""
                p = subprocess.run(["kubectl", "--kubeconfig", cfg, "create", "--dry-run=server", "-f", "-"],
                                   input=json.dumps(manifest), capture_output=True, text=True, timeout=60)
                return "" if p.returncode != 0 and "ValidatingAdmissionPolicy" in p.stderr else (p.stdout + p.stderr)[:200]
            meta = {"namespace": "compute", "labels": {f"{LABEL_DOMAIN}/user": eng}}
            app = {"apiVersion": "spark.stackable.tech/v1alpha1", "kind": "SparkApplication",
                   "metadata": dict(meta, name="someone-else-job"),
                   "spec": {"mode": "cluster", "sparkImage": {"productVersion": "4.1.2"}, "mainApplicationFile": "local:///x.py"}}
            r = refused(app)
            check(f"D5 {eng} may not name a job after someone else", r == "", r)
            r = refused({"apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/service-account-token",
                         "metadata": {"name": f"spark-token-{eng}-x", "namespace": "compute",
                                      "annotations": {"kubernetes.io/service-account.name": "default"}}})
            check(f"D6 {eng} may not write a service-account token Secret", r == "", r)
            r = refused({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "spark-token-someone-else-x", "namespace": "compute"},
                         "stringData": {"token": "x"}})
            check(f"D7 {eng} may not write a token Secret of someone else's job", r == "", r)

    # ---------------------------------------------------------------- E ----
    # JupyterHub answers and sends its logins to Keycloak (client jupyterhub)
    if os.environ.get("JUPYTER_URL"):
        jup = os.environ["JUPYTER_URL"]

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *a, **k):
                return None

        try:
            with urllib.request.urlopen(f"{jup}/hub/login", timeout=30) as r:
                page = r.read().decode(errors="replace")
            check("E1 JupyterHub serves its login page", "/hub/oauth_login" in page, page[:200])
        except (urllib.error.URLError, OSError) as e:
            check("E1 JupyterHub serves its login page", False, str(e)[:200])
        try:
            urllib.request.build_opener(NoRedirect).open(f"{jup}/hub/oauth_login", timeout=30)
            check("E2 JupyterHub logs in through Keycloak", False, "no redirect")
        except urllib.error.HTTPError as e:
            loc = e.headers.get("Location", "")
            check("E2 JupyterHub logs in through Keycloak",
                  e.code == 302 and loc.startswith(f"{KEYCLOAK}/protocol/openid-connect/auth") and "client_id=jupyterhub" in loc,
                  f"{e.code} {loc[:200]}")
        except (urllib.error.URLError, OSError) as e:
            check("E2 JupyterHub logs in through Keycloak", False, str(e)[:200])

    # ---------------------------------------------------------------- F ----
    # The directory (Samba AD) is the only source of people and groups
    off = os.environ.get("SMOKE_DISABLED_USER")
    if off:
        try:
            token(off)
            check(f"F1 {off}, disabled in the directory, cannot log in", False, "got a token")
        except urllib.error.HTTPError as e:
            check(f"F1 {off}, disabled in the directory, cannot log in", e.code in (400, 401), str(e.code))
    late, late_group = os.environ.get("SMOKE_LATE_USER"), os.environ.get("SMOKE_LATE_GROUP")
    if late and late_group:
        end, seen = time.time() + int(os.environ.get("SMOKE_LATE_WAIT", "390")), []
        while True:
            try:
                body = token(late).split(".")[1]
                seen = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))).get("groups") or []
            except (urllib.error.URLError, OSError) as e:
                seen = [f"no token: {e}"]
            if late_group in seen or time.time() >= end:
                break
            time.sleep(10)
        check(f"F2 {late}'s token has the group the directory gave it after its first login",
              late_group in seen, f"groups {seen}")
    if os.environ.get("SMOKE_KEYCLOAK_WRITE"):
        status = os.environ["SMOKE_KEYCLOAK_WRITE"]
        check("F3 Keycloak may not set the password of a user of the directory", status in ("400", "403"), status)
    if "SMOKE_LOCAL_USERS" in os.environ:
        local = json.loads(os.environ["SMOKE_LOCAL_USERS"])
        check("F4 every user of the realm comes from the directory", not local, ", ".join(local))
    ldaps = os.environ.get("DIRECTORY_LDAPS_HOST")
    if ldaps:
        domain, ca = os.environ["DIRECTORY_DOMAIN"], os.environ["DIRECTORY_CA"]
        who = f"{eng}@{domain}"
        try:
            right, wrong = ldap_bind(ldaps, who, PASSWORD, ca), ldap_bind(ldaps, who, PASSWORD + "x", ca)
            detail = f"right password: {right}, wrong password: {wrong}"
        except (OSError, ValueError, IndexError) as e:
            right, wrong, detail = -1, -1, str(e)
        check(f"F5 the directory answers LDAP clients on ldaps://{ldaps}:636: {eng} binds, a wrong password is refused",
              right == 0 and wrong == 49, detail)
        # who may change the directory: its administrators, in the people's and the groups' units
        if late and os.environ.get("DIRECTORY_PEOPLE_DN"):
            change = (f"CN={late},{os.environ['DIRECTORY_PEOPLE_DN']}", "description", "smoke test")
            reader = os.environ["DIRECTORY_BIND_USER"]
            got = {}
            for name, password in (("smoke-adm", PASSWORD), (eng, PASSWORD), (reader, os.environ["DIRECTORY_BIND_PASSWORD"])):
                try:
                    got[name] = ldap_bind(ldaps, f"{name}@{domain}", password, ca, change=change)
                except (OSError, ValueError, IndexError) as e:
                    got[name] = str(e)
            check(f"F6 smoke-adm ({ADMIN}) may change an entry of the directory", got["smoke-adm"] == 0, f"LDAP result {got['smoke-adm']}")
            check(f"F7 {eng} and {reader} (Keycloak's account) may not", got[eng] == 50 and got[reader] == 50,
                  f"LDAP results {got[eng]}, {got[reader]}")
    ui = os.environ.get("DIRECTORY_UI_URL")
    if ui:
        try:
            proxy, lam = directory_ui(ui, "smoke-adm")
        except (urllib.error.URLError, OSError) as e:
            proxy, lam = 0, str(e)[:200]
        check(f"F8 smoke-adm ({ADMIN}) reaches LDAP Account Manager through Keycloak and logs in to it",
              proxy == 200 and lam.endswith("/lam/templates/lists/list.php"), f"proxy {proxy}; {lam}")
        try:
            proxy, lam = directory_ui(ui, eng)
        except (urllib.error.URLError, OSError) as e:
            proxy, lam = 0, str(e)[:200]
        check(f"F9 {eng} ({ENGINEER}) is refused there after its Keycloak login", proxy == 403, f"proxy {proxy}; {lam}")


if __name__ == "__main__":
    main()
