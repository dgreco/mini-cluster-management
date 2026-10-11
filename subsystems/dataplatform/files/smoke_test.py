#!/usr/bin/env python3
"""End-to-end check of the IAM model (make dataplatform-smoke-test).

Run by smoke-test.yml with Ansible's Python (boto3), after it created the
temporary users of SMOKE_USERS in the directory (Samba AD; JSON {name: {group,
groups}}, password SMOKE_PASSWORD): smoke-eng (the engineer group), smoke-ana (the analyst
group), smoke-both (both), smoke-nogroup (none), smoke-adm (analyst,
engineer and the admin group, last: the gate must find it among
several groups; it authorizes the lakehouse).

Everything happens in the smoke test's own lakehouse, smoke_test
(metacatalog/lakehouses/smoke_test.yaml), whose schemas and tables only
Meta Catalog creates and whose policy grants the engineer group the bronze
layer and the analyst group the gold layer (the site's groups: ADMIN_GROUP,
ENGINEER_GROUP, ANALYST_GROUP). 0 loads the medallion model
(metacatalog/medallion-model.yaml, as make dataplatform-medallion does) when
Meta Catalog has none of it, and loads, provisions and authorizes the
lakehouse when it is missing: a platform built from scratch needs no step
before this test.

0  Meta Catalog lets the engineer in and refuses the analyst (only
   the admin and engineer groups); the lakehouse: in Meta Catalog,
   provisioned; the engineer may not authorize it (only the admin group: the
   gate, also against tricks with the path and a forged groups header), the
   administrator may, and its grants are in force within minutes.
A  direct STS: each user gets their group's role only; the role reaches its
   layer's directory, not the other group's, nor the bucket's root; make
   dataplatform-s3-login writes working keys for the user's own group only.
B  Trino (bearer tokens): the engineer writes and reads the bronze table; the
   analyst is refused by Ceph; a user in no group is refused by Trino (no S3
   security mapping); nobody may create, alter, comment or drop a schema or
   a table (only Meta Catalog's provisioner); a member of both groups reads
   and writes both layers in one query, each with the role granted it, and
   is refused when s3_role pins the other role.
C  Spark (web identity): the engineer's job reads Trino's table and writes
   another that Trino reads back, but may not create a table (its catalog
   login has no DDL); the analyst's job is refused by Ceph; the job's service
   account reads no Secret and starts no pod mounting another one.
D  Kubernetes (Keycloak login): the engineer may run jobs in compute, not
   read the platform's secrets, nor write objects the Spark fence refuses.
F  the directory is the only place people live, and Keycloak manages them
   there: a user the directory disabled cannot log in; a membership it gives
   after a user's first login is in a later token; a password Keycloak set
   is the directory's (the user logs in with it); the realm holds no user of
   its own; a user made in Keycloak is in the directory with the group
   Keycloak gave it, and logs in. The playbook prepares these
   (SMOKE_DISABLED_USER, SMOKE_LATE_USER, _GROUP and _PASSWORD,
   SMOKE_KEYCLOAK_WRITE, SMOKE_LOCAL_USERS, SMOKE_KC_USER, _GROUP and
   _IN_DIRECTORY).
Reads use sum(), not count(*) (answered from Iceberg statistics, no data read).
Prints one line per check and exits 1 when any check fails.
"""
import base64
import json
import os
import re
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
LAKEHOUSE = os.environ.get("SMOKE_LAKEHOUSE", "smoke_test")
LAKEHOUSE_FILE = os.environ["SMOKE_LAKEHOUSE_FILE"]
MODEL_FILE = os.environ["SMOKE_MODEL_FILE"]            # metacatalog/medallion-model.yaml
ROOT = f"lakehouse/{LAKEHOUSE}"                      # its storageRoot, under the bucket
BRONZE, GOLD = f"{ROOT}/bronze", f"{ROOT}/gold"     # the engineers', the analysts'
ENG_T, SPARK_T = f"lakehouse.{LAKEHOUSE}_bronze.eng_t", f"lakehouse.{LAKEHOUSE}_bronze.spark_t"
ANA_T = f"lakehouse.{LAKEHOUSE}_gold.ana_t"
USERS = json.loads(os.environ["SMOKE_USERS"])          # {user: {"group": first group, "groups": [...]}}
PASSWORD = os.environ["SMOKE_PASSWORD"]
results = []


def check(label: str, ok: bool, detail: str = "") -> None:
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {label}{('  (' + detail + ')') if detail and not ok else ''}", flush=True)


CLIENT = os.environ.get("SMOKE_CLIENT", "smoke-test")               # the test's own confidential client:
CLIENT_SECRET = os.environ["SMOKE_CLIENT_SECRET"]                    # the only one with the password grant


def token(user: str, client: str = CLIENT, id_token: bool = False, password: str = "") -> str:
    """A token of the temporary user from the smoke-test client (audiences trino
    and kubernetes, groups; RGW accepts the client); `client` is kept for the
    call sites' readability."""
    data = urllib.parse.urlencode({"grant_type": "password", "client_id": CLIENT, "client_secret": CLIENT_SECRET,
                                   "username": user, "password": password or PASSWORD,
                                   "scope": "openid profile"}).encode()
    with urllib.request.urlopen(f"{KEYCLOAK}/protocol/openid-connect/token", data, timeout=30) as r:
        return json.load(r)["id_token" if id_token else "access_token"]


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


def resources(user: str) -> list:
    """the lakehouse's schemas and tables (values) as Meta Catalog has them"""
    out = []
    for t in ("MedallionSchemaType", "MedallionTableType"):
        q = urllib.parse.urlencode({"entityTypeName": t, "queryPath": f'$ ? (@.lakehouse == "{LAKEHOUSE}")'})
        code, rows = metacatalog(user, "GET", f"/entity?{q}")
        for r in rows if code == 200 else []:
            v = r["values"]
            out.append(json.loads(v) if isinstance(v, str) else v)
    return out


def model_types() -> list:
    """the entity types the model file defines (its EntityTypes section, read
    like dp-medallion.sh reads it)"""
    names, section = [], False
    for line in open(MODEL_FILE):
        if line.startswith("EntityTypes:"):
            section = True
        elif re.match(r"^[A-Za-z]", line):
            section = False
        elif section and line.startswith("  - name:"):
            names.append(line.split()[2].strip("\"'"))
    return names


def model(eng: str) -> bool:
    """The medallion model is in Meta Catalog: loaded by the engineer when none
    of its entity types is there (a fresh platform); a partial model is left
    to a person, as dp-medallion.sh leaves it."""
    names = model_types()
    found = {n: metacatalog(eng, "GET", f"/entity-type/{n}")[0] for n in names}
    other = {n: c for n, c in found.items() if c not in (200, 404)}
    present = [n for n, c in found.items() if c == 200]
    if other or not names:
        check(f"0 Meta Catalog answers {eng} about the model's entity types", False,
              f"{other or 'no entity type in ' + MODEL_FILE}")
        return False
    if len(present) == len(names):
        check(f"0 the medallion model is in Meta Catalog ({len(names)} entity types)", True)
        return True
    if present:
        check("0 the medallion model is in Meta Catalog", False,
              f"{len(present)} of {len(names)} entity types only: finish or undo that by hand (Meta Catalog UI)")
        return False
    code, out = metacatalog(eng, "POST", "/bulk-creation", open(MODEL_FILE, "rb").read(), "application/octet-stream")
    check(f"0 Meta Catalog loads the medallion model ({len(names)} entity types)", code == 204, f"HTTP {code} {out}")
    return code == 204


def fixture(eng: str) -> bool:
    """The smoke lakehouse exists, is provisioned and its grants are in force.
    Missing pieces are made through Meta Catalog: the engineer loads the model
    and the lakehouse and provisions, the administrator authorizes (every run:
    unchanged grants change nothing)."""
    if not model(eng):
        return False
    q = urllib.parse.urlencode({"entityTypeName": "LakehouseType", "queryPath": f'$ ? (@.name == "{LAKEHOUSE}")'})
    code, rows = metacatalog(eng, "GET", f"/entity?{q}")
    check(f"0a Meta Catalog answers {eng} ({METACATALOG})", code == 200, str(rows)[:200])
    if code != 200:
        return False
    # only the admin and engineer groups get past the proxy in front of it
    ana = "smoke-ana"
    code_ana, out_ana = metacatalog(ana, "GET", "/entity-type")
    check(f"0b Meta Catalog refuses {ana} ({ANALYST}: not {ADMIN} nor {ENGINEER})",
          code_ana in (401, 403), f"HTTP {code_ana} {str(out_ana)[:200]}")
    if not rows:
        code, out = metacatalog(eng, "POST", "/aggregate/yaml", open(LAKEHOUSE_FILE, "rb").read(), "application/octet-stream")
        if code != 204:
            check(f"0c the lakehouse {LAKEHOUSE} loads into Meta Catalog", False, str(out))
            return False
        code, rows = metacatalog(eng, "GET", f"/entity?{q}")
    lid = rows[0]["id"]
    end = time.time() + 120                   # the mappings derive the schemas and tables
    while len(resources(eng)) < 6 and time.time() < end:
        time.sleep(5)
    code, out = metacatalog(eng, "POST", f"/aggregate/{lid}/provision")
    check(f"0c Meta Catalog provisions {LAKEHOUSE} (3 schemas, 3 tables)", code == 204, str(out)[:300])
    if code != 204:
        return False
    gate(eng, lid)
    adm = "smoke-adm"

    def in_force():
        s3e = sts(eng, USERS[eng]["group"])
        return (False, s3e) if isinstance(s3e, str) else s3_ok(lambda: s3e.list_objects_v2(Bucket=BUCKET, Prefix=f"{BRONZE}/"))
    before, _ = in_force()
    code, out = metacatalog(eng, "POST", f"/aggregate/{lid}/authorize")
    check(f"0d {eng} may not authorize {LAKEHOUSE} (only {ADMIN})", code == 403, f"HTTP {code} {str(out)[:200]}")
    code, out = metacatalog(adm, "POST", f"/aggregate/{lid}/authorize")
    check(f"0d {adm} ({ADMIN}) authorizes {LAKEHOUSE}", code == 204, f"HTTP {code} {str(out)[:300]}")
    # Ceph's roles change during the call; iam-sync's next round (30 s) gives
    # Trino the mapping
    end, ok, err = time.time() + 180, False, ""
    while time.time() < end:
        ok, err = in_force()
        if ok:
            break
        time.sleep(10)
    check(f"0e the grants of {LAKEHOUSE} are in force", ok, f"s3: {err}")
    if ok and not before:
        # new grants: Trino's mapping follows (iam-sync's round, then the
        # kubelet's refresh of the mounted ConfigMap)
        time.sleep(120)
    return ok


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


def empty_tables(eng: str, both: str) -> None:
    """The fixture's tables hold only what this run writes."""
    for t in (ENG_T, SPARK_T):
        trino(eng, f"DELETE FROM {t}")
    trino(both, f"DELETE FROM {ANA_T}")


def cleanup(eng: str, both: str) -> None:
    """Whatever the run left behind: the rows, the smoke/ objects."""
    empty_tables(eng, both)
    try:
        s3e = sts(eng, USERS[eng]["group"])
        if not isinstance(s3e, str):
            # smoke/: the S3 checks' objects; by_spark/: the metadata file of
            # the table the catalog refused Spark
            for prefix in (f"{BRONZE}/smoke/", f"{BRONZE}/by_spark/"):
                for page in s3e.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
                    for o in page.get("Contents", []):
                        s3e.delete_object(Bucket=BUCKET, Key=o["Key"])
    except Exception as e:  # noqa: BLE001 - best effort, the result is reported anyway
        print(f"cleanup of s3://{BUCKET}/{BRONZE}/: {e}")


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


def run() -> None:
    eng, ana, nog, both = "smoke-eng", "smoke-ana", "smoke-nogroup", "smoke-both"
    ge, ga = USERS[eng]["group"], USERS[ana]["group"]
    de, da = BRONZE, GOLD          # the layers the lakehouse's policy grants the two groups

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
    check(f"B1 {eng} writes {ENG_T} (bronze) through Trino", ok, str(out))
    ok, out = trino(eng, f"SELECT sum(x) FROM {ENG_T}")
    check(f"B2 {eng} reads it back (sum = 10)", ok and out == [[10]], str(out))
    ok, out = trino(ana, f"SELECT sum(x) FROM {ENG_T}")
    check(f"B3 {ana} is refused by Ceph", not ok and ceph_refused(out), str(out))
    ok, out = trino(nog, f"SELECT sum(x) FROM {ENG_T}")
    check(f"B4 {nog} is refused by Trino (no mapping)", not ok and "No matching S3 security mapping" in out, str(out))
    # Only Meta Catalog creates, changes or drops a schema or a table: Trino
    # gives DDL to its provisioner alone, even where a user may write the data.
    for n, (label, sql) in enumerate((("create a schema", f"CREATE SCHEMA lakehouse.smoke_by_hand WITH (location = 's3://{BUCKET}/{de}/by_hand')"),
                       ("create a table", f"CREATE TABLE lakehouse.{LAKEHOUSE}_bronze.by_hand (x int)"),
                       ("create a table from a query", f"CREATE TABLE lakehouse.{LAKEHOUSE}_bronze.by_hand AS SELECT 1 AS x"),
                       ("comment a table", f"COMMENT ON TABLE {ENG_T} IS 'smoke'"),
                       ("add a column", f"ALTER TABLE {ENG_T} ADD COLUMN z int"),
                       ("drop a table", f"DROP TABLE {ENG_T}"))):
        ok, out = trino(eng, sql)
        check(f"B5{'abcdef'[n]} {eng} may not {label}, though the layer is theirs", not ok and denied(out), str(out))
    # A member of both groups: Trino picks, for each file, the role granted
    # its directory (iam-sync's per-directory rules). Their default role is
    # the analysts' (first alphabetically), which Ceph refuses in bronze, so
    # reading there proves the per-directory rule. The mapping file is
    # re-read every 30 s, after the group file: retry.
    end = time.time() + 90
    while True:
        ok, out = trino(both, f"SELECT sum(x) FROM {ENG_T}")
        if ok or time.time() > end:
            break
        time.sleep(10)
    check(f"B6 {both} reads {ENG_T} in {de} with no role chosen", ok and out == [[10]], str(out))
    ok, out = trino(both, f"INSERT INTO {ANA_T} VALUES 5")
    if ok:
        ok, out = trino(both, f"SELECT (SELECT sum(x) FROM {ENG_T}) + (SELECT sum(y) FROM {ANA_T})")
    check(f"B7 {both} writes in {da} and reads both layers in one query (sum = 15)", ok and out == [[15]], str(out))
    ok, out = trino(both, f"SELECT sum(x) FROM {ENG_T}", role=f"{ROLE_ARN}{PREFIX}{ga}")
    check(f"B8 with s3_role pinned to {PREFIX}{ga}, {both} is refused {ENG_T} by Ceph",
          not ok and ceph_refused(out), str(out))

    # ---------------------------------------------------------------- C ----
    ok, out = spark(eng, ge, f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print("SMOKE trino-table", spark.sql("SELECT sum(x) FROM {ENG_T}").collect()[0][0])
spark.sql("INSERT INTO {SPARK_T} SELECT id FROM range(5)")
print("SMOKE spark-table", spark.sql("SELECT sum(id) FROM {SPARK_T}").collect()[0][0])
print("SMOKE path", spark.sql("SELECT file_path FROM {SPARK_T}.files LIMIT 1").collect()[0][0])
# Only Meta Catalog makes tables: the job's catalog login (spark_writer) may not.
# In the engineers' own layer, so that Ceph lets the metadata file be written
# and the catalog is what refuses (outside a grant Ceph would refuse first).
try:
    spark.sql("CREATE TABLE lakehouse.{LAKEHOUSE}_bronze.by_spark (id bigint) USING iceberg "
              "LOCATION 's3://{BUCKET}/{BRONZE}/by_spark'")
    print("SMOKE create allowed")
except Exception as e:
    # PostgreSQL's refusal sits deep in the Java exception's text
    text = str(e)
    why = [l.strip() for l in text.splitlines() if "permission denied" in l.lower()]
    print("SMOKE create refused", (why[0] if why else text.splitlines()[0] if text else type(e).__name__)[:200])
# The fence (spark-fence.yaml, the job ClusterRole): the job's own service
# account may neither read a Secret of compute nor start a pod that mounts one.
import json, ssl, urllib.error, urllib.request
sa = "/var/run/secrets/kubernetes.io/serviceaccount"
ctx = ssl.create_default_context(cafile=sa + "/ca.crt")
hdr = {{"Authorization": "Bearer " + open(sa + "/token").read().strip(), "Content-Type": "application/json"}}
def k8s(method, path, body=None):
    req = urllib.request.Request("https://kubernetes.default.svc" + path, body, hdr, method=method)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            return r.status, ""
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:300]
print("SMOKE secret-read", k8s("GET", "/api/v1/namespaces/compute/secrets/iceberg-catalog")[0])
import base64
claims = json.loads(base64.urlsafe_b64decode(hdr["Authorization"].split(".")[1] + "=="))
me = claims["sub"].rsplit(":", 1)[1]                  # the job's own service account
pod = {{"apiVersion": "v1", "kind": "Pod", "metadata": {{"generateName": "smoke-fence-", "namespace": "compute"}},
       "spec": {{"serviceAccountName": me, "containers": [{{"name": "c", "image": "busybox"}}],
                "volumes": [{{"name": "s", "secret": {{"secretName": "iceberg-catalog"}}}}]}}}}
code, msg = k8s("POST", "/api/v1/namespaces/compute/pods?dryRun=All", json.dumps(pod).encode())
print("SMOKE pod-mount", code, "fence" if "dataplatform-spark-pods" in msg else msg[:120].replace(chr(10), " "))
spark.stop()
""")
    check(f"C1 {eng}'s Spark job reads Trino's table", ok and "SMOKE trino-table 10" in out, out[-600:])
    check(f"C2 {eng}'s Spark job writes {SPARK_T}", ok and "SMOKE spark-table 10" in out, out[-600:])
    check(f"C3 the table's files are in s3://{BUCKET}/{de}/spark_t/", f"SMOKE path s3://{BUCKET}/{de}/spark_t/" in out, out[-600:])
    said = next((l for l in out.splitlines() if "SMOKE create" in l), "no SMOKE create line: " + out[-400:])
    check(f"C4 {eng}'s Spark job may not create a table (its catalog login has no DDL)",
          re.search(r"SMOKE create refused.*permission denied", said, re.I) is not None, said[:400])
    check(f"C5 {eng}'s Spark job may not read a Secret of compute", "SMOKE secret-read 403" in out, out[-600:])
    check(f"C6 {eng}'s Spark job may not start a pod mounting another Secret",
          re.search(r"SMOKE pod-mount 4(03|22) fence", out) is not None, out[-600:])
    ok2, out2 = trino(eng, f"SELECT sum(id) FROM {SPARK_T}")
    check(f"C7 Trino reads the table Spark wrote (sum = 10)", ok2 and out2 == [[10]], str(out2))
    # The analyst's job sees the catalog entry (the catalog is no boundary)
    # but Ceph refuses the files with the analysts' role.
    ok, out = spark(ana, ga, f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print("SMOKE leaked", spark.sql("SELECT sum(x) FROM {ENG_T}").collect()[0][0])
""")
    reasons = [l.strip() for l in out.splitlines() if re.search(r"Exception|Denied|Forbidden|403", l)][:4]
    # refused = no data came back and Ceph answered 403 (the job's own exit
    # status is unreliable: a PySpark error can still exit 0; a traceback
    # echoes the failing source line, so only a printed value is a leak)
    check(f"C8 {ana}'s Spark job is refused {ENG_T} by Ceph", re.search(r"SMOKE leaked \d", out) is None
          and re.search(r"Status Code: 403|AccessDenied|Access Denied", out) is not None,
          " | ".join(reasons)[:900])

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
    # smoke-late's password is the one Keycloak set after the directory made the user
    late, late_group = os.environ.get("SMOKE_LATE_USER"), os.environ.get("SMOKE_LATE_GROUP")
    late_password = os.environ.get("SMOKE_LATE_PASSWORD", "")

    def groups_of(user: str, password: str = "") -> list:
        body = token(user, password=password).split(".")[1]
        return json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))).get("groups") or []

    if late and late_group:
        end, seen = time.time() + int(os.environ.get("SMOKE_LATE_WAIT", "390")), []
        while True:
            try:
                seen = groups_of(late, late_password)
            except (urllib.error.URLError, OSError) as e:
                seen = [f"no token: {e}"]
            if late_group in seen or time.time() >= end:
                break
            time.sleep(10)
        check(f"F2 {late}'s token has the group the directory gave it after its first login",
              late_group in seen, f"groups {seen}")
    if os.environ.get("SMOKE_KEYCLOAK_WRITE") and late:
        status = os.environ["SMOKE_KEYCLOAK_WRITE"]
        try:
            groups_of(late, late_password)
            took, detail = True, ""
        except (urllib.error.URLError, OSError) as e:
            took, detail = False, str(e)
        check(f"F3 a password Keycloak set for {late} is the directory's", status == "204" and took,
              f"Keycloak answered {status}; login: {detail or 'ok'}")
    if "SMOKE_LOCAL_USERS" in os.environ:
        local = json.loads(os.environ["SMOKE_LOCAL_USERS"])
        check("F4 every user of the realm comes from the directory", not local, ", ".join(local))
    made, made_group = os.environ.get("SMOKE_KC_USER"), os.environ.get("SMOKE_KC_GROUP")
    if made:
        there = json.loads(os.environ.get("SMOKE_KC_IN_DIRECTORY", "[]"))
        in_group = bool(there) and made_group in there[0].get("groups", [])
        try:
            seen = groups_of(made)
        except (urllib.error.URLError, OSError) as e:
            seen = [f"no token: {e}"]
        check(f"F5 {made}, made in Keycloak, is in the directory with its group and logs in",
              in_group and made_group in seen, f"directory {there}; token groups {seen}")


if __name__ == "__main__":
    main()
