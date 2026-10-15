"""Unit tests of tables_plan.py (make test): how a file of tables becomes the
steps of dp-tables.sh sync. What is made, changed, provisioned, authorized,
rejected and removed, in which order, and what is refused before anything
is done. No network."""
import copy
import importlib.util
import pathlib
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "tables_plan.py"
spec = importlib.util.spec_from_file_location("tables_plan", SCRIPT)
tp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tp)

SCOPE = {"except": ["smoke"]}
GRANTS = {"principals": [{"id": "engineers", "type": "GROUP"}],
          "permissions": [{"name": "READ"}, {"name": "WRITE"}],
          "grants": [{"principals": ["engineers"], "permissions": ["READ", "WRITE"]}]}


def entry(name, layer="bronze", fmt="iceberg", columns=(("id", "long"),), depends=None, **values):
    e = {"ref": name, "entityType": "TableType",
         "values": {"name": name, "domain": "shop", "subdomain": "orders", "layer": layer, "format": fmt, **GRANTS, **values}}
    if columns is not None:
        e["parts"] = [{"entityType": "TableSchemaType", "values": {"columns": [{"name": n, "type": t} for n, t in columns]}}]
    if depends:
        e["dependsOn"] = depends
    return e


def live(e, depends=(), provisioned=True, authorized=True, status=True):
    """What Meta Catalog has for a table loaded from e, provisioned and authorized"""
    values = dict(e["values"])
    if status:
        values.update({"provisioningStatus": "PROVISIONED", "provisioningResult": "ok",
                       "authorizationStatus": "AUTHORIZED", "authorizationResult": "ok"})
    part = {"id": e["ref"] + "-schema", "values": e["parts"][0]["values"]} if e.get("parts") else None
    kinds = ["folder"] + (["table"] if part else [])
    res = [{"kind": k, "provisioningStatus": "PROVISIONED" if provisioned else None,
            "authorizationStatus": "AUTHORIZED" if authorized else None} for k in kinds]
    return {"id": e["ref"] + "-id", "values": values, "part": part, "depends": list(depends), "resources": res}


RAW, CLEAN = entry("raw"), entry("clean", layer="silver", depends=["raw"])
FILE = [RAW, CLEAN]
LIVE = [live(RAW), live(CLEAN, depends=["shop/orders/bronze/raw"])]


def plan(entries=None, have=None, scope=None):
    return tp.plan(copy.deepcopy(FILE if entries is None else entries), copy.deepcopy(LIVE if have is None else have),
                   scope or SCOPE)


def ops(steps):
    return [(s["op"], s.get("table", ",".join(s.get("tables", [])))) for s in steps if "op" in s]


def notes(steps):
    return [s["note"] for s in steps if "note" in s]


class Unchanged(unittest.TestCase):
    def test_nothing_for_the_tables_meta_catalog_has(self):
        self.assertEqual(plan(), ([], []))

    def test_tables_outside_the_scope_are_left_alone(self):
        smoke = live(entry("eng_t", domain="smoke", subdomain="test"))
        self.assertEqual(plan(have=LIVE + [smoke]), ([], []))


class New(unittest.TestCase):
    def test_a_new_table_is_made_provisioned_and_authorized(self):
        new = entry("audit", layer="gold", depends=["clean"])
        steps, refused = plan(entries=FILE + [new])
        self.assertEqual(refused, [])
        self.assertEqual(ops(steps), [("create", "shop/orders/gold/audit"),
                                      ("link", "shop/orders/gold/audit"),
                                      ("provision", "shop/orders/gold/audit"),
                                      ("authorize", "shop/orders/gold/audit")])
        # the link to a table Meta Catalog has is made afterwards, not in the load
        self.assertNotIn("dependsOn", steps[0]["documents"][0])
        self.assertEqual(steps[1]["target"], "shop/orders/silver/clean")

    def test_lineage_between_new_tables_goes_into_the_load(self):
        a, b = entry("a"), entry("b", layer="silver", depends=["a"])
        steps, _ = plan(entries=FILE + [a, b])
        self.assertEqual(steps[0]["documents"][1]["dependsOn"], ["a"])
        self.assertNotIn("link", [op for op, _ in ops(steps)])

    def test_a_new_table_without_grants_is_not_authorized(self):
        bare = entry("bare", grants=[])
        self.assertEqual([op for op, t in ops(plan(entries=FILE + [bare])[0]) if t.endswith("/bare")],
                         ["create", "provision"])


class Changed(unittest.TestCase):
    def test_changed_grants_are_put_back_then_provisioned_and_authorized(self):
        f = copy.deepcopy(FILE)
        f[0]["values"]["grants"] = [{"principals": ["engineers"], "permissions": ["READ"]}]
        steps, _ = plan(entries=f)
        self.assertEqual(ops(steps), [("update", "shop/orders/bronze/raw"), ("provision", "shop/orders/bronze/raw"),
                                      ("authorize", "shop/orders/bronze/raw")])
        update = steps[0]
        self.assertEqual((update["entityType"], update["entity"], update["wait"]), ("TableType", "raw-id", "folder"))
        # the status fields Meta Catalog wrote go back with the file's values
        self.assertEqual(update["values"]["provisioningStatus"], "PROVISIONED")
        self.assertEqual(update["values"]["grants"], f[0]["values"]["grants"])

    def test_a_value_set_in_the_ui_only_is_taken_out(self):
        have = copy.deepcopy(LIVE)
        have[0]["values"]["owner"] = "someone"
        update = plan(have=have)[0][0]
        self.assertNotIn("owner", update["values"])
        self.assertIn("owner", update["say"])

    def test_a_column_added(self):
        f = copy.deepcopy(FILE)
        f[0]["parts"][0]["values"]["columns"].append({"name": "note", "type": "string"})
        steps, _ = plan(entries=f)
        self.assertEqual(ops(steps), [("update", "shop/orders/bronze/raw"), ("provision", "shop/orders/bronze/raw"),
                                      ("authorize", "shop/orders/bronze/raw")])
        self.assertEqual((steps[0]["entityType"], steps[0]["entity"], steps[0]["wait"]),
                         ("TableSchemaType", "raw-schema", "table"))

    def test_a_column_taken_out_stays_in_the_table(self):
        have = copy.deepcopy(LIVE)
        have[0]["part"]["values"]["columns"].append({"name": "old", "type": "string"})
        steps, _ = plan(have=have)
        self.assertIn("update", [op for op, _ in ops(steps)])
        self.assertTrue(any("old" in n and "stay" in n for n in notes(steps)))

    def test_grants_taken_away_reject_the_table(self):
        f = copy.deepcopy(FILE)
        f[0]["values"]["grants"] = []
        self.assertEqual(ops(plan(entries=f)[0]), [("update", "shop/orders/bronze/raw"),
                                                   ("provision", "shop/orders/bronze/raw"),
                                                   ("reject", "shop/orders/bronze/raw")])

    def test_a_table_not_provisioned_or_authorized_is_done_again(self):
        have = copy.deepcopy(LIVE)
        have[0] = live(RAW, provisioned=False, authorized=False)
        self.assertEqual(ops(plan(have=have)[0]), [("provision", "shop/orders/bronze/raw"),
                                                   ("authorize", "shop/orders/bronze/raw")])

    def test_lineage_taken_out_is_unlinked(self):
        f = copy.deepcopy(FILE)
        del f[1]["dependsOn"]
        steps, _ = plan(entries=f)
        self.assertEqual(ops(steps), [("unlink", "shop/orders/silver/clean")])
        self.assertEqual(steps[0]["target"], "shop/orders/bronze/raw")


class Removed(unittest.TestCase):
    def test_a_table_no_longer_in_the_file_is_removed(self):
        steps, _ = plan(entries=[RAW])
        self.assertEqual(ops(steps), [("remove", "shop/orders/silver/clean")])
        self.assertIn("DROPS", steps[0]["say"])

    def test_a_removed_table_s_lineage_goes_with_it(self):
        clean = copy.deepcopy(CLEAN)
        del clean["dependsOn"]
        self.assertEqual(ops(plan(entries=[clean])[0]), [("remove", "shop/orders/bronze/raw")])

    def test_a_new_name_is_a_removal_and_a_new_table(self):
        renamed = entry("clean2", layer="silver", depends=["raw"])
        got = ops(plan(entries=[RAW, renamed])[0])
        self.assertEqual(got[0], ("create", "shop/orders/silver/clean2"))
        self.assertEqual(got[-1], ("remove", "shop/orders/silver/clean"))


class Refused(unittest.TestCase):
    def refused(self, entries=None, have=None, scope=None):
        steps, refusals = plan(entries=entries, have=have, scope=scope)
        self.assertEqual(steps, [])
        return " | ".join(refusals)

    def test_a_file_without_tables(self):
        self.assertIn("holds no table", self.refused(entries=[]))

    def test_a_table_of_the_smoke_test_s_domain(self):
        self.assertIn("smoke", self.refused(entries=FILE + [entry("x", domain="smoke", subdomain="test")]))

    def test_one_table_twice_and_one_ref_twice(self):
        self.assertIn("two documents", self.refused(entries=FILE + [entry("raw")]))
        other = entry("raw", layer="gold")
        self.assertIn("the ref 'raw'", self.refused(entries=FILE + [other]))

    def test_a_dependency_the_file_does_not_have(self):
        self.assertIn("names no table", self.refused(entries=[RAW, entry("clean", layer="silver", depends=["gone"])]))

    def test_another_format(self):
        f = copy.deepcopy(FILE)
        f[0]["values"]["format"] = "parquet"
        self.assertIn("format does not change", self.refused(entries=f))

    def test_a_retyped_column(self):
        f = copy.deepcopy(FILE)
        f[0]["parts"][0]["values"]["columns"][0]["type"] = "string"
        self.assertIn("type does not change", self.refused(entries=f))

    def test_a_schema_added_or_taken_away(self):
        f = copy.deepcopy(FILE)
        del f[0]["parts"]
        self.assertIn("takes the schema off", self.refused(entries=f))

    def test_resources_missing(self):
        have = copy.deepcopy(LIVE)
        have[0]["resources"] = have[0]["resources"][:1]
        self.assertIn("derivation that failed", self.refused(have=have))

    def test_a_document_of_another_type(self):
        self.assertIn("only TableType", self.refused(entries=FILE + [{"entityType": "Other", "values": {}}]))


class Scope(unittest.TestCase):
    def test_only_the_named_domains(self):
        smoke = entry("eng_t", domain="smoke", subdomain="test")
        steps, refused = plan(entries=[smoke], have=LIVE, scope={"only": ["smoke"]})
        self.assertEqual(refused, [])
        # the shop tables are outside this scope: made is the smoke table, nothing removed
        self.assertEqual([op for op, _ in ops(steps)], ["create", "provision", "authorize"])


if __name__ == "__main__":
    unittest.main()
