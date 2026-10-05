"""Unit tests of iam-sync (make test): the pure functions, no Keycloak, no
Kubernetes. The script is loaded from the gitops tree."""
import importlib.util
import json
import os
import pathlib
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
        self.assertTrue(m.plausible({"a": [], "b": []}))
        self.assertFalse(m.plausible({"a": []}))
        m.ALLOW_SHRINK = True
        self.assertTrue(m.plausible({"a": []}))


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
        # role wins the tie; shared only through her group; then her default
        self.assertEqual(of("alice"), [("s3://dataplatform/lake/projects/ana/", "usr-alice"),
                                       ("s3://dataplatform/lake/shared/", "grp-data-analysts"),
                                       (None, "usr-alice")])
        # carol: each project directory with its group's role, the most
        # specific (longest) first; shared is ro to both groups: alphabetical
        self.assertEqual(of("carol"), [("s3://dataplatform/lake/projects/ana/", "grp-data-analysts"),
                                       ("s3://dataplatform/lake/projects/eng/", "grp-data-engineers"),
                                       ("s3://dataplatform/lake/shared/", "grp-data-analysts"),
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
                                     ("s3://dataplatform/lake/data-x/", "grp-readers"),
                                     ("s3://dataplatform/lake/data/", "usr-bob"),
                                     (None, "usr-bob")])
        # zed: the rw grant below the ro one; the admin role only where a grant
        # names the admin group (shared, ro), never for data/ (its s3:* is not
        # a grant); default: the first group alphabetically
        self.assertEqual(of("zed"), [("s3://dataplatform/lake/data/archive/2025/", "grp-writers"),
                                     ("s3://dataplatform/lake/data/archive/", "grp-readers"),
                                     ("s3://dataplatform/lake/data-x/", "grp-readers"),
                                     ("s3://dataplatform/lake/shared/", "grp-platform-admins"),
                                     (None, "grp-platform-admins")])
        # a prefix always ends with a slash (data must not match data-x), and a
        # user's directory rules come before their default rule
        for r in rules:
            if "prefix" in r:
                self.assertRegex(r["prefix"], r"^s3://dataplatform/.+/$")
        for user in ("bob", "zed"):
            mine = [r for r in rules if r.get("user") == f"^{user}$"]
            self.assertTrue(all("prefix" in r for r in mine[:-1]) and "prefix" not in mine[-1])

    def test_only_meta_catalogs_provisioner_has_ddl(self):
        m = load(access=LAKE_ACCESS)
        rules = m.trino_rules(m.read_access())
        prov = "^service\\-account\\-metacatalog\\-provisioner$"
        self.assertEqual(rules["schemas"], [{"user": prov, "owner": True}, {"owner": False}])
        self.assertEqual(rules["tables"], [
            {"user": prov, "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]},
            {"privileges": ["SELECT", "INSERT", "DELETE", "UPDATE"]}])
        self.assertNotIn("group", json.dumps(rules))       # admins included: no DDL by group

    def test_without_a_provisioner_nobody_has_ddl(self):
        m = load()
        rules = m.trino_rules(m.read_access())
        self.assertEqual(rules["schemas"], [{"owner": False}])
        self.assertEqual(rules["tables"], [{"privileges": ["SELECT", "INSERT", "DELETE", "UPDATE"]}])


LAKE_ACCESS = ACCESS + """  provisioner: service-account-metacatalog-provisioner
"""
LAKE_RECORD = {
    "lake.acme_gold": {"path": "lake/acme/gold", "grants": [
        {"access": "rw", "users": [], "groups": ["data-engineers"]},
        {"access": "ro", "users": ["bob"], "groups": ["data-analysts"]}]},
    "lake.acme_silver": {"path": "lake/acme/silver", "grants": [
        {"access": "rw", "users": [], "groups": ["data-engineers"]}]},
    "lake.evil": {"path": "projects/eng/stolen", "grants": [
        {"access": "rw", "users": ["mallory"], "groups": []}]},
    "lake.dots": {"path": "lake/../projects/eng", "grants": [
        {"access": "rw", "users": ["mallory"], "groups": []}]},
    "lake.odd": {"path": "lake/acme/odd", "grants": [
        {"access": "admin", "users": ["mallory"], "groups": []}]},
    "lake.notjson": "{not json"}


class RecordOfGrants(unittest.TestCase):
    def test_grants_under_the_prefix_count_after_the_provisioners(self):
        a = load(access=LAKE_ACCESS, record=LAKE_RECORD).read_access()
        self.assertEqual(a["grants"], [
            {"path": "lake", "access": "rw", "users": ["service-account-metacatalog-provisioner"], "groups": []},
            {"path": "lake/acme/gold", "access": "rw", "users": [], "groups": ["data-engineers"]},
            {"path": "lake/acme/gold", "access": "ro", "users": ["bob"], "groups": ["data-analysts"]},
            {"path": "lake/acme/silver", "access": "rw", "users": [], "groups": ["data-engineers"]}])

    def test_without_a_prefix_there_are_none(self):
        self.assertEqual(load(access="bucket: dataplatform\n").read_access()["grants"], [])

    def test_an_unreadable_record_keeps_the_last_one_and_fails_before_a_first(self):
        m = load(record=RuntimeError("API down"))
        with self.assertRaises(RuntimeError):
            m.read_access()
        m.answer = RECORD
        self.assertEqual(len(m.read_access()["grants"]), 3)
        m.answer = RuntimeError("API down")
        self.assertEqual(len(m.read_access()["grants"]), 3)

    def test_trino_reads_a_granted_layer_with_the_granted_role(self):
        m = load(access=LAKE_ACCESS, record=LAKE_RECORD)
        rules = json.loads(m.trino_objects({"data-analysts": ["ann"], "data-engineers": ["eve"]},
                                           m.read_access())["data"]["s3-security-mapping.json"])["mappings"]
        ann = {r["prefix"]: r["iamRole"].rsplit("/", 1)[1] for r in rules if r.get("user") == "^ann$" and "prefix" in r}
        self.assertEqual(ann["s3://dataplatform/lake/acme/gold/"], "grp-data-analysts")
        self.assertNotIn("s3://dataplatform/lake/acme/silver/", ann)
        bot = [r for r in rules if r.get("user") == "^service\\-account\\-metacatalog\\-provisioner$"]
        self.assertEqual(bot[0]["prefix"], "s3://dataplatform/lake/")


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
