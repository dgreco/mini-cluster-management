"""Unit tests of iam-sync (make test): the pure functions, no Keycloak, no
Kubernetes. The script is loaded from the gitops tree."""
import importlib.util
import json
import os
import pathlib
import re
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parents[2] / "gitops" / "manifests" / "governance" / "iam-sync" / "iam_sync.py"
ACCESS = """bucket: dataplatform
admins:
  groups: [platform-admins]
metacatalog:
  prefix: lake
"""
# the record of grants (ConfigMap metacatalog-grants), as Meta Catalog writes it
RECORD = {
    "lake.projects_eng": {"path": "lake/projects/eng", "grants": [
        {"access": "rw", "users": [], "groups": ["data-engineers"]}]},
    "lake.projects_ana": {"path": "lake/projects/ana", "grants": [
        {"access": "rw", "users": ["alice"], "groups": ["data-analysts"]}]},
    "lake.shared": {"path": "lake/shared", "grants": [
        {"access": "ro", "users": [], "groups": ["data-engineers", "data-analysts"]}]},
}


def load(access=ACCESS, record=RECORD):
    """The module with access.yaml pointed at a temporary copy and the record
    of grants answered by a stand-in for the Kubernetes API (record: a dict,
    or an exception to raise)."""
    d = tempfile.mkdtemp()
    with open(os.path.join(d, "access.yaml"), "w") as f:
        f.write(access)
    with open(os.path.join(d, "namespace"), "w") as f:
        f.write("governance")
    os.environ["ACCESS_FILE"] = os.path.join(d, "access.yaml")
    os.environ["ROLE_ARN_PREFIX"] = "arn:aws:iam::RGW00000000000000042:role/"
    os.environ["LABEL_DOMAIN"] = "data.example.org"
    spec = importlib.util.spec_from_file_location("iam_sync", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.SA_DIR = d
    mod.answer = record

    def k8s(method, path, body=None, content_type=None):
        assert (method, path) == ("GET", "/api/v1/namespaces/governance/configmaps/metacatalog-grants"), path
        if isinstance(mod.answer, Exception):
            raise mod.answer
        return {"data": {k: v if isinstance(v, str) else json.dumps(v) for k, v in mod.answer.items()}}
    mod.k8s = k8s
    return mod


class Plausibility(unittest.TestCase):
    def test_empty_answer_is_refused(self):
        m = load()
        self.assertFalse(m.plausible({}))

    def test_a_group_that_vanished_is_refused_unless_allowed(self):
        m = load()
        self.assertTrue(m.plausible({"a": ["u"], "b": []}))
        self.assertFalse(m.plausible({"a": ["u"]}))
        m.ALLOW_SHRINK = True
        self.assertTrue(m.plausible({"a": ["u"]}))

    def test_no_member_anywhere_is_refused_even_by_a_process_that_saw_nothing(self):
        # Keycloak 26.7 while the directory is down: every group, 200, no member
        m = load()
        self.assertFalse(m.plausible({"platform-admins": [], "data-engineers": [], "data-analysts": []}))
        self.assertTrue(m.plausible({"platform-admins": ["admin"], "data-engineers": [], "data-analysts": []}))
        self.assertFalse(m.plausible({"platform-admins": [], "data-engineers": [], "data-analysts": []}))
        m.ALLOW_SHRINK = True
        self.assertTrue(m.plausible({"platform-admins": [], "data-engineers": [], "data-analysts": []}))


class KeycloakAnswers(unittest.TestCase):
    """read_groups against a stand-in for Keycloak's admin API: answers is a
    list of {group: [members]} dicts, one per reading of that group's members."""
    FED = "4_HzvuIATHOsC-Q4dcZ55Q"

    def keycloak(self, m, groups, readings):
        calls = {}

        def keycloak_all(token, path):
            if path.startswith("/groups?"):
                return [{"id": "id-" + g, "name": g} for g in groups]
            g = path.split("/")[2][3:]
            calls[g] = calls.get(g, 0) + 1
            return readings[min(calls[g], len(readings)) - 1].get(g, [])
        m.keycloak_all = keycloak_all
        m.keycloak_token = lambda: "token"
        return calls

    def user(self, name, federated=True, enabled=True):
        u = {"id": "id-" + name, "username": name, "enabled": enabled}
        return {**u, "federationLink": self.FED, "origin": self.FED} if federated else u

    def test_only_enabled_members_from_the_directory_count(self):
        m = load()
        self.keycloak(m, ["platform-admins", "data-engineers"], [{
            "platform-admins": [self.user("admin"), self.user("mallory", federated=False)],
            "data-engineers": [self.user("alice"), self.user("bob", enabled=False)]}])
        self.assertEqual(m.read_groups(), {"platform-admins": ["admin"], "data-engineers": ["alice"]})

    def test_a_directory_that_is_down_gives_no_member_and_nothing_is_applied(self):
        m = load()
        self.keycloak(m, ["platform-admins", "data-engineers"], [{}])
        answer = m.read_groups()
        self.assertEqual(answer, {"platform-admins": [], "data-engineers": []})
        self.assertFalse(m.plausible(answer))

    def test_a_directory_that_goes_away_during_the_read_is_an_error(self):
        m = load()
        # the first group is read while the directory answers, the rest is not
        self.keycloak(m, ["platform-admins", "data-engineers"],
                      [{"platform-admins": [self.user("admin")]}, {}])
        with self.assertRaises(m.DirectoryDown):
            m.read_groups()

    def test_groups_with_unusable_names_are_left_out(self):
        m = load()
        self.keycloak(m, ["platform-admins", "Bad Name"], [{"platform-admins": [self.user("admin")]}])
        self.assertEqual(m.read_groups(), {"platform-admins": ["admin"]})


class AccessFile(unittest.TestCase):
    def test_the_reader_gives_the_grants(self):
        a = load().read_access()
        self.assertEqual(a["bucket"], "dataplatform")
        # in the order of the record's keys
        self.assertEqual([g["path"] for g in a["grants"]], ["lake/projects/ana", "lake/projects/eng", "lake/shared"])
        self.assertEqual(a["grants"][0]["users"], ["alice"])
        self.assertEqual(a["admins"], ["platform-admins"])

    def test_a_grants_list_in_access_yaml_is_ignored(self):
        a = load(access=ACCESS + "grants:\n  - path: lake/x\n    users: [mallory]\n    access: rw\n").read_access()
        self.assertNotIn("mallory", [u for g in a["grants"] for u in g["users"]])

    def test_principals_are_users_with_a_grant_and_groups_with_a_grant_or_admin(self):
        m = load()
        self.assertEqual(m.principals(m.read_access()), (["alice"], ["data-analysts", "data-engineers", "platform-admins"]))


def pick(rules, user, place):
    """The role Trino takes for a user's access to s3://dataplatform/<place>:
    the first rule, in file order, whose user pattern matches and whose
    prefix the location starts with (a rule without one matches all)."""
    location = f"s3://dataplatform/{place}"
    for r in rules:
        if "user" in r and re.fullmatch(r["user"].strip("^$"), user) and location.startswith(r.get("prefix", "")):
            return r["iamRole"].rsplit("/", 1)[1]
    return None


class Trino(unittest.TestCase):
    def test_group_file_takes_only_usable_usernames(self):
        m = load()
        cm = m.trino_objects({"eng": ["alice", "bad,name", "bob"], "empty": ["x:y"]}, m.read_access())
        self.assertEqual(cm["data"]["groups.txt"], "eng:alice,bob\n")

    def test_mapping_has_user_rules_first_then_group_rules(self):
        m = load()
        groups = {"data-analysts": ["alice", "carol"], "data-engineers": ["carol"], "nogrant": ["dave"]}
        rules = json.loads(m.trino_objects(groups, m.read_access())["data"]["s3-security-mapping.json"])["mappings"]
        users = {r["user"]: r for r in rules if "user" in r and "prefix" not in r}     # the default rules
        # alice: her own role by default, her group's allowed too (s3_role switches)
        self.assertTrue(users["^alice$"]["iamRole"].endswith(":role/usr-alice"))
        self.assertEqual([a.rsplit("/", 1)[1] for a in users["^alice$"]["allowedIamRoles"]], ["usr-alice", "grp-data-analysts"])
        # carol: no own role, first group alphabetically, both groups allowed
        self.assertTrue(users["^carol$"]["iamRole"].endswith(":role/grp-data-analysts"))
        self.assertEqual(len(users["^carol$"]["allowedIamRoles"]), 2)
        # dave: his only group has no grant, so no rule at all
        self.assertNotIn("^dave$", users)
        self.assertEqual([r["group"] for r in rules if "group" in r],
                         ["^data\\-analysts$", "^data\\-engineers$", "^platform\\-admins$"])
        self.assertTrue(all("user" in r or "group" in r for r in rules))

    def test_each_directory_gets_the_role_granted_it(self):
        m = load()
        groups = {"data-analysts": ["alice", "carol"], "data-engineers": ["carol"]}
        rules = json.loads(m.trino_objects(groups, m.read_access())["data"]["s3-security-mapping.json"])["mappings"]

        def of(user):
            return [(r.get("prefix"), r["iamRole"].rsplit("/", 1)[1]) for r in rules if r.get("user") == f"^{user}$"]

        # alice: lake/projects/ana is granted rw to her and to her group: her own
        # role wins the tie; shared only through her group; then her default.
        # Each directory twice: what is in it, and itself (no slash)
        self.assertEqual(of("alice"), [("s3://dataplatform/lake/projects/ana/", "usr-alice"),
                                       ("s3://dataplatform/lake/projects/ana", "usr-alice"),
                                       ("s3://dataplatform/lake/shared/", "grp-data-analysts"),
                                       ("s3://dataplatform/lake/shared", "grp-data-analysts"),
                                       (None, "usr-alice")])
        # carol: each project directory with its group's role, the most
        # specific (longest) first; shared is ro to both groups: alphabetical
        self.assertEqual(of("carol"), [("s3://dataplatform/lake/projects/ana/", "grp-data-analysts"),
                                       ("s3://dataplatform/lake/projects/eng/", "grp-data-engineers"),
                                       ("s3://dataplatform/lake/projects/ana", "grp-data-analysts"),
                                       ("s3://dataplatform/lake/projects/eng", "grp-data-engineers"),
                                       ("s3://dataplatform/lake/shared/", "grp-data-analysts"),
                                       ("s3://dataplatform/lake/shared", "grp-data-analysts"),
                                       (None, "grp-data-analysts")])
        # every rule of a user allows all their roles: s3_role still pins one
        for r in rules:
            if r.get("user") == "^carol$":
                self.assertEqual(len(r["allowedIamRoles"]), 2)

    def test_nested_directories_prefer_rw_and_never_the_admin_rights(self):
        def entry(path, access, users=(), groups=()):
            return {"path": path, "grants": [{"access": access, "users": list(users), "groups": list(groups)}]}
        m = load(record={"a": entry("lake/data", "rw", users=["bob"]),
                         "b": entry("lake/data/archive", "ro", groups=["readers"]),
                         "c": entry("lake/data/archive/2025", "rw", groups=["writers"]),
                         "d": entry("lake/data-x", "ro", groups=["readers"]),
                         "e": entry("lake/shared", "ro", groups=["platform-admins"])})
        groups = {"readers": ["bob", "zed"], "writers": ["zed"], "platform-admins": ["zed"]}
        rules = json.loads(m.trino_objects(groups, m.read_access())["data"]["s3-security-mapping.json"])["mappings"]

        def of(user):
            return [(r.get("prefix"), r["iamRole"].rsplit("/", 1)[1]) for r in rules if r.get("user") == f"^{user}$"]

        # bob: data/archive is granted ro to his group, but his own rw grant
        # on data covers it: the rw role wins
        self.assertEqual(of("bob"), [("s3://dataplatform/lake/data/archive/", "usr-bob"),
                                     ("s3://dataplatform/lake/data/archive", "usr-bob"),
                                     ("s3://dataplatform/lake/data-x/", "grp-readers"),
                                     ("s3://dataplatform/lake/data-x", "grp-readers"),
                                     ("s3://dataplatform/lake/data/", "usr-bob"),
                                     ("s3://dataplatform/lake/data", "usr-bob"),
                                     (None, "usr-bob")])
        # zed: the rw grant below the ro one; the admin role only where a grant
        # names the admin group (shared, ro), never for data/ (its s3:* is not
        # a grant); default: the first group alphabetically
        self.assertEqual([p for p in of("zed") if p[0] is None or p[0].endswith("/")],
                         [("s3://dataplatform/lake/data/archive/2025/", "grp-writers"),
                          ("s3://dataplatform/lake/data/archive/", "grp-readers"),
                          ("s3://dataplatform/lake/data-x/", "grp-readers"),
                          ("s3://dataplatform/lake/shared/", "grp-platform-admins"),
                          (None, "grp-platform-admins")])
        # data must not match data-x: Trino's pick for each place
        self.assertEqual(pick(rules, "bob", "lake/data-x/f"), "grp-readers")
        self.assertEqual(pick(rules, "bob", "lake/data/f"), "usr-bob")
        self.assertEqual(pick(rules, "zed", "lake/data/archive/2025"), "grp-writers")
        self.assertEqual(pick(rules, "zed", "lake/data/archive/2026/f"), "grp-readers")
        # a user's directory rules come before their default rule
        for user in ("bob", "zed"):
            mine = [r for r in rules if r.get("user") == f"^{user}$"]
            self.assertTrue(all("prefix" in r for r in mine[:-1]) and "prefix" not in mine[-1])

    def test_a_table_directory_itself_gets_the_role_granted_it(self):
        # Trino lists a parquet or avro table at its location, which has no
        # trailing slash: a member of two groups must not fall back to the
        # default role there (smoke check B6). A sibling whose name starts
        # with the table's keeps what it had.
        def entry(path, group):
            return {"path": path, "grants": [{"access": "rw", "users": [], "groups": [group]}]}
        m = load(access=LAKE_ACCESS, record={
            "folder.d.s.bronze.t": entry("lake/d/s/bronze/t", "data-engineers"),
            "folder.d.s.bronze.t_raw": entry("lake/d/s/bronze/t_raw", "data-stewards"),
            "folder.d.s.bronze.t_raw2": entry("lake/d/s/bronze/t_raw2", "data-engineers"),
            "folder.d.s.gold.g": entry("lake/d/s/gold/g", "data-analysts"),
            "folder.d.s.gold.g_old": entry("lake/d/s/gold/g_old", "data-stewards")})
        groups = {"data-analysts": ["both", "ana"], "data-engineers": ["both"], "data-stewards": ["sam"]}
        rules = json.loads(m.trino_objects(groups, m.read_access())["data"]["s3-security-mapping.json"])["mappings"]
        # both: default is the analysts' role (first alphabetically)
        for place, role in (("lake/d/s/bronze/t", "grp-data-engineers"),           # the listing
                            ("lake/d/s/bronze/t/part-0.avro", "grp-data-engineers"),
                            ("lake/d/s/bronze/t_raw", "grp-data-analysts"),        # not theirs: default
                            ("lake/d/s/bronze/t_raw/part-0.avro", "grp-data-analysts"),
                            ("lake/d/s/bronze/t_raw2", "grp-data-engineers"),      # theirs again
                            ("lake/d/s/bronze/t_raw2/f", "grp-data-engineers"),
                            ("lake/d/s/gold/g", "grp-data-analysts"),
                            ("lake/d/s/gold/g_old/f", "grp-data-analysts"),
                            ("lake/d/s/silver/x/f", "grp-data-analysts")):
            self.assertEqual(pick(rules, "both", place), role, place)
        # ana: only g, and g_old is not hers
        self.assertEqual(pick(rules, "ana", "lake/d/s/gold/g"), "grp-data-analysts")
        self.assertEqual(pick(rules, "ana", "lake/d/s/gold/g_old/f"), "grp-data-analysts")
        # every rule of a user names one of that user's own roles
        for r in rules:
            if "user" in r:
                self.assertIn(r["iamRole"], r["allowedIamRoles"])


LAKE_ACCESS = ACCESS + """  provisioner: service-account-metacatalog-provisioner
"""
LAKE_RECORD = {
    "folder.acme.sales.gold.daily": {"path": "lake/acme/sales/gold/daily", "grants": [
        {"access": "rw", "users": [], "groups": ["data-engineers"]},
        {"access": "ro", "users": ["bob"], "groups": ["data-analysts"]}]},
    "table.acme_sales_gold.daily": {"catalog": "lakehouse", "schema": "acme_sales_gold", "table": "daily", "grants": [
        {"access": "rw", "users": [], "groups": ["data-engineers"]},
        {"access": "ro", "users": ["bob"], "groups": ["data-analysts"]}]},
    "folder.acme.sales.silver.orders": {"path": "lake/acme/sales/silver/orders", "grants": [
        {"access": "rw", "users": [], "groups": ["data-engineers"]}]},
    "table.acme_sales_silver.orders": {"catalog": "lakehouse", "schema": "acme_sales_silver", "table": "orders", "grants": [
        {"access": "rw", "users": [], "groups": ["data-engineers"]}]},
    "folder.evil": {"path": "projects/eng/stolen", "grants": [
        {"access": "rw", "users": ["mallory"], "groups": []}]},
    "folder.dots": {"path": "lake/../projects/eng", "grants": [
        {"access": "rw", "users": ["mallory"], "groups": []}]},
    "folder.odd": {"path": "lake/acme/sales/gold/odd", "grants": [
        {"access": "admin", "users": ["mallory"], "groups": []}]},
    "table.other_catalog": {"catalog": "system", "schema": "runtime", "table": "queries", "grants": [
        {"access": "rw", "users": ["mallory"], "groups": []}]},
    "table.bad_name": {"catalog": "lakehouse", "schema": "acme_sales_gold", "table": "x\" OR 1=1", "grants": [
        {"access": "rw", "users": ["mallory"], "groups": []}]},
    "neither": {"grants": [{"access": "rw", "users": ["mallory"], "groups": []}]},
    "notjson": "{not json"}


class RecordOfGrants(unittest.TestCase):
    def test_folders_under_the_prefix_count_after_the_provisioners(self):
        a = load(access=LAKE_ACCESS, record=LAKE_RECORD).read_access()
        self.assertEqual(a["grants"], [
            {"path": "lake", "access": "rw", "users": ["service-account-metacatalog-provisioner"], "groups": []},
            {"path": "lake/acme/sales/gold/daily", "access": "rw", "users": [], "groups": ["data-engineers"]},
            {"path": "lake/acme/sales/gold/daily", "access": "ro", "users": ["bob"], "groups": ["data-analysts"]},
            {"path": "lake/acme/sales/silver/orders", "access": "rw", "users": [], "groups": ["data-engineers"]}])

    def test_tables_of_the_catalog_count(self):
        a = load(access=LAKE_ACCESS, record=LAKE_RECORD).read_access()
        self.assertEqual(a["tables"], [
            {"schema": "acme_sales_gold", "table": "daily", "access": "rw", "users": [], "groups": ["data-engineers"]},
            {"schema": "acme_sales_gold", "table": "daily", "access": "ro", "users": ["bob"], "groups": ["data-analysts"]},
            {"schema": "acme_sales_silver", "table": "orders", "access": "rw", "users": [], "groups": ["data-engineers"]}])

    def test_without_a_prefix_there_are_none(self):
        a = load(access="bucket: dataplatform\n", record=LAKE_RECORD).read_access()
        self.assertEqual((a["grants"], a["tables"]), ([], []))

    def test_an_unreadable_record_keeps_the_last_one_and_fails_before_a_first(self):
        m = load(record=RuntimeError("API down"))
        with self.assertRaises(RuntimeError):
            m.read_access()
        m.answer = RECORD
        self.assertEqual(len(m.read_access()["grants"]), 3)
        m.answer = RuntimeError("API down")
        self.assertEqual(len(m.read_access()["grants"]), 3)

    def test_trino_reads_a_granted_folder_with_the_granted_role(self):
        m = load(access=LAKE_ACCESS, record=LAKE_RECORD)
        rules = json.loads(m.trino_objects({"data-analysts": ["ann"], "data-engineers": ["eve"]},
                                           m.read_access())["data"]["s3-security-mapping.json"])["mappings"]
        ann = {r["prefix"]: r["iamRole"].rsplit("/", 1)[1] for r in rules if r.get("user") == "^ann$" and "prefix" in r}
        self.assertEqual(ann["s3://dataplatform/lake/acme/sales/gold/daily/"], "grp-data-analysts")
        self.assertNotIn("s3://dataplatform/lake/acme/sales/silver/orders/", ann)
        bot = [r for r in rules if r.get("user") == "^service\\-account\\-metacatalog\\-provisioner$"]
        self.assertEqual(bot[0]["prefix"], "s3://dataplatform/lake/")

    def test_the_mapping_no_longer_carries_trinos_rules(self):
        m = load(access=LAKE_ACCESS, record=LAKE_RECORD)
        data = m.trino_objects({"data-engineers": ["eve"]}, m.read_access())["data"]
        self.assertEqual(sorted(data), ["groups.txt", "s3-security-mapping.json"])


def rego_values(text: str) -> dict:
    """The values of the generated module: `name := <JSON>` statements."""
    out = {}
    for block in text.split("\n\n"):
        lines = [line for line in block.splitlines() if not line.startswith("#")]
        if lines and " := " in lines[0]:
            name, _, first = lines[0].partition(" := ")
            out[name] = json.loads("\n".join([first] + lines[1:]))
    return out


class OpenPolicyAgent(unittest.TestCase):
    def test_the_module_is_the_package_the_policy_imports(self):
        m = load(access=LAKE_ACCESS, record=LAKE_RECORD)
        text = m.opa_grants(m.read_access())
        self.assertIn("\npackage dataplatform\n", text)
        self.assertEqual(sorted(rego_values(text)), ["admin_groups", "catalog", "provisioner", "tables"])

    def test_only_meta_catalogs_provisioner_is_named_for_ddl(self):
        m = load(access=LAKE_ACCESS, record=LAKE_RECORD)
        v = rego_values(m.opa_grants(m.read_access()))
        self.assertEqual(v["provisioner"], "service-account-metacatalog-provisioner")
        self.assertEqual(v["admin_groups"], ["platform-admins"])
        self.assertEqual(v["catalog"], "lakehouse")

    def test_without_a_provisioner_nobody_is(self):
        m = load()
        self.assertEqual(rego_values(m.opa_grants(m.read_access()))["provisioner"], "")

    def test_each_table_names_who_reads_and_who_writes(self):
        m = load(access=LAKE_ACCESS, record=LAKE_RECORD)
        tables = rego_values(m.opa_grants(m.read_access()))["tables"]
        self.assertEqual(tables, {
            "acme_sales_gold": {"daily": {"rw": {"users": [], "groups": ["data-engineers"]},
                                          "ro": {"users": ["bob"], "groups": ["data-analysts"]}}},
            "acme_sales_silver": {"orders": {"rw": {"users": [], "groups": ["data-engineers"]}}}})

    def test_what_is_not_a_table_of_the_catalog_grants_nothing(self):
        m = load(access=LAKE_ACCESS, record=LAKE_RECORD)
        text = m.opa_grants(m.read_access())
        self.assertNotIn("mallory", text)
        self.assertNotIn("runtime", text)

    def test_an_empty_record_grants_no_table(self):
        m = load(access=LAKE_ACCESS, record={})
        self.assertEqual(rego_values(m.opa_grants(m.read_access()))["tables"], {})

    def test_the_configmap_is_a_bundle_of_the_agents_namespace(self):
        m = load(access=LAKE_ACCESS, record=LAKE_RECORD)
        cm = m.opa_objects(m.read_access())
        self.assertEqual((cm["metadata"]["namespace"], cm["metadata"]["name"]), ("governance", "trino-grants"))
        self.assertEqual(cm["metadata"]["labels"], {"opa.stackable.tech/bundle": "true"})
        self.assertEqual(list(cm["data"]), ["grants.rego"])


class Spark(unittest.TestCase):
    def test_every_keycloak_group_may_submit(self):
        m = load()
        rb = m.spark_objects({"b": [], "a": []})[0]
        self.assertEqual([s["name"] for s in rb["subjects"]], ["oidc:a", "oidc:b"])


class Reader(unittest.TestCase):
    def test_the_subset_reader_handles_the_shapes_the_file_uses(self):
        m = load()
        self.assertEqual(m.parse_access("a: 1\nb:\n  - x: [p, q]\n    y: rw\n  - z\nc: {}\nd: []\n# c\n"),
                         {"a": 1, "b": [{"x": ["p", "q"], "y": "rw"}, "z"], "c": {}, "d": []})
        self.assertEqual(m.parse_access("k: 'quoted'   # a comment\n"), {"k": "quoted"})


if __name__ == "__main__":
    unittest.main()
