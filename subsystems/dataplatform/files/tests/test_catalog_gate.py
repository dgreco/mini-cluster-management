"""Unit tests of catalog-gate (make test): what a Spark job may ask of the
Hive Metastore's Iceberg REST catalog, whose job is asking, and the question
for the Open Policy Agent. The decisions only, no network. The script is
loaded from the gitops tree."""
import importlib.util
import json
import pathlib
import unittest

SCRIPT = (pathlib.Path(__file__).resolve().parents[2] / "gitops" / "manifests" / "governance" / "catalog-gate"
          / "catalog_gate.py")
spec = importlib.util.spec_from_file_location("catalog_gate", SCRIPT)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
gate.USER_LABEL = "data.test/user"

S = "/iceberg/v1/namespaces/retail_orders_silver"
T = S + "/tables/orders"
# what Spark sends for an INSERT (Iceberg's REST client)
APPEND = {"requirements": [{"type": "assert-table-uuid", "uuid": "u"}, {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1}],
          "updates": [{"action": "add-snapshot", "snapshot": {"snapshot-id": 2}},
                      {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 2}]}
EVE = {"user": "eve", "groups": ["engineers"], "job": "eve-load"}


def status(method, path, body=None):
    return gate.decide(method, path, b"" if body is None else json.dumps(body).encode())[0]


def pod(address, account="eve-load", phase="Running", **spec):
    return {"spec": {"serviceAccountName": account, **spec}, "status": {"phase": phase, "podIP": address,
                                                                       "podIPs": [{"ip": address}]}}


class Reads(unittest.TestCase):
    def test_a_read_passes_with_the_question_it_needs(self):
        for path, ask in (("/iceberg/v1/config", None), ("/iceberg/v1/namespaces", ("schemas",)),
                          (S, ("schema", "retail_orders_silver")), (S + "/tables", ("tables", "retail_orders_silver")),
                          (S + "/views", ("tables", "retail_orders_silver")),
                          (T, ("read", "retail_orders_silver", "orders")),
                          (S + "/views/orders", ("read", "retail_orders_silver", "orders"))):
            for method in ("GET", "HEAD"):
                self.assertEqual(gate.decide(method, path), (0, "", ask), f"{method} {path}")

    def test_what_the_catalog_has_besides_is_not_a_jobs(self):
        for path in (T + "/credentials", T + "/metrics", "/iceberg/v1/oauth/tokens", "/iceberg/v1",
                     "/iceberg/v1/namespaces/a/b/tables", "/iceberg/v1/tables"):
            self.assertEqual(status("GET", path), 403, path)


class MetadataTables(unittest.TestCase):
    """<schema>.<table>.snapshots: Iceberg's client first asks for the table
    "snapshots" of the two-level schema <schema>%1F<table>, and reads the
    metadata table only when that is "not found"."""
    DEEP = "/iceberg/v1/namespaces/retail_orders_silver%1Forders"

    def test_a_schema_of_several_levels_is_not_found(self):
        for path in (self.DEEP + "/tables/snapshots", self.DEEP + "/tables/files", self.DEEP, self.DEEP + "/tables",
                     self.DEEP.replace("%1F", "%1f") + "/views/history", self.DEEP + "%1Fmore/tables/x"):
            for method in ("GET", "HEAD"):
                self.assertEqual(gate.decide(method, path)[::2], (404, None), f"{method} {path}")

    def test_the_client_is_told_what_was_not_found(self):
        self.assertEqual(gate.error_type(404, self.DEEP + "/tables/snapshots"), "NoSuchTableException")
        self.assertEqual(gate.error_type(404, self.DEEP + "/views/snapshots"), "NoSuchViewException")
        self.assertEqual(gate.error_type(404, self.DEEP), "NoSuchNamespaceException")
        self.assertEqual(gate.error_type(404, self.DEEP + "/tables"), "NoSuchNamespaceException")
        self.assertEqual(gate.error_type(403, T), "ForbiddenException")
        self.assertEqual(gate.error_type(502, T), "ServiceFailureException")

    def test_nothing_is_written_there_and_nothing_else_is_decoded(self):
        for method in ("POST", "DELETE", "PUT"):
            self.assertEqual(status(method, self.DEEP + "/tables/snapshots", APPEND), 400, method)
        for path in ("/iceberg/v1/namespaces/a%2Fb/tables/x", "/iceberg/v1/namespaces/a%1F/tables/x",
                     "/iceberg/v1/namespaces/%1Fa/tables/x", "/iceberg/v1/namespaces/a%1Fb/tables/x%1Fy",
                     "/iceberg/v1/namespaces/a%1Fb/tables/x/metrics", "/iceberg/v1/namespaces/a%1F..%1Fb/tables/x"):
            self.assertEqual(status("GET", path), 400, path)


class Commits(unittest.TestCase):
    def test_a_commit_of_data_needs_a_write_grant(self):
        self.assertEqual(gate.decide("POST", T, json.dumps(APPEND).encode()), (0, "", ("write", "retail_orders_silver", "orders")))

    def test_expiring_snapshots_and_statistics_are_data_too(self):
        for action in ("remove-snapshots", "remove-snapshot-ref", "set-statistics", "remove-statistics",
                       "set-partition-statistics", "remove-partition-statistics"):
            self.assertEqual(status("POST", T, {"requirements": [], "updates": [{"action": action}]}), 0, action)

    def test_a_commit_that_changes_the_table_itself_is_refused(self):
        for action in ("add-schema", "set-current-schema", "add-spec", "set-default-spec", "add-sort-order",
                       "set-default-sort-order", "set-location", "set-properties", "remove-properties",
                       "upgrade-format-version", "assign-uuid", "remove-schemas", "an-action-of-tomorrow", None):
            body = {"requirements": [], "updates": APPEND["updates"] + [{"action": action}]}
            self.assertEqual(status("POST", T, body), 403, action)

    def test_a_commit_that_would_create_the_table_is_refused(self):
        self.assertEqual(status("POST", T, {"requirements": [{"type": "assert-create"}], "updates": []}), 403)
        self.assertEqual(status("POST", T, {"requirements": ["assert-create"], "updates": []}), 403)

    def test_a_commit_that_is_not_one_is_refused(self):
        self.assertEqual(gate.decide("POST", T, b"{not json")[0], 400)
        self.assertEqual(gate.decide("POST", T, b"")[0], 400)
        self.assertEqual(status("POST", T, [APPEND]), 400)
        self.assertEqual(status("POST", T, {"updates": {"action": "add-snapshot"}}), 400)

    def test_the_clients_report_needs_a_read_grant(self):
        self.assertEqual(gate.decide("POST", T + "/metrics", b"{}"), (0, "", ("read", "retail_orders_silver", "orders")))


class OnlyMetaCatalog(unittest.TestCase):
    def test_creating_dropping_renaming_and_registering_are_refused(self):
        for method, path in (("POST", S + "/tables"),      # create a table
                             ("DELETE", T), ("PUT", T), ("PATCH", T),
                             ("POST", "/iceberg/v1/tables/rename"),
                             ("POST", "/iceberg/v1/namespaces"), ("DELETE", S),
                             ("POST", S + "/properties"), ("POST", S + "/register"),
                             ("POST", "/iceberg/v1/transactions/commit"),
                             ("POST", S + "/views"), ("POST", S + "/views/v"),
                             ("POST", "/iceberg/v1/views/rename"), ("POST", "/iceberg/v1/oauth/tokens"),
                             ("OPTIONS", T)):
            self.assertEqual(status(method, path, APPEND), 403, f"{method} {path}")

    def test_the_refusal_says_where_tables_are_made(self):
        self.assertIn("Meta Catalog", gate.decide("DELETE", T)[1])


class Paths(unittest.TestCase):
    def test_a_path_that_is_not_plain_is_refused_whatever_the_method(self):
        for path in (T + "/", T + "//metrics", "/iceberg/v1/namespaces/a/tables/x/../orders",
                     "/iceberg/v1/namespaces/a/tables/./orders", "/iceberg/v1/namespaces/a/%74ables/orders",
                     "/iceberg/v1/namespaces/a/tables/orders%2F..%2F..%2Ftables", "/iceberg/v1/namespaces/a/tables;x",
                     "/ICEBERG/v1/namespaces", "/iceberg/v2/config", "/v1/config", "/", "/iceberg/v1/namespaces/a b/tables",
                     "/iceberg/v1/namespaces/.hidden/tables", "//iceberg/v1/config"):
            for method in ("GET", "POST", "DELETE"):
                self.assertEqual(status(method, path, APPEND), 400, f"{method} {path}")

    def test_what_looks_like_a_table_but_is_not_one_cannot_be_committed_to(self):
        for path in ("/iceberg/v1/namespaces/a/tables/b/c", "/iceberg/v1/namespaces/a/b/tables/c",
                     "/iceberg/v1/namespaces/a/views/b", "/iceberg/v1/namespaces/a/tables/b/metrics/x"):
            self.assertEqual(status("POST", path, APPEND), 403, path)


class WhoAsks(unittest.TestCase):
    def test_the_job_is_the_service_account_of_the_running_pod_with_the_address(self):
        pods = [pod("10.0.0.7"), pod("10.0.0.8", "ann-report")]
        self.assertEqual(gate.job_of(pods, "10.0.0.7"), "eve-load")
        self.assertEqual(gate.job_of(pods, "10.0.0.8"), "ann-report")

    def test_a_finished_pod_that_kept_the_address_is_nobody(self):
        self.assertEqual(gate.job_of([pod("10.0.0.7", "old-job", "Succeeded"), pod("10.0.0.7")], "10.0.0.7"), "eve-load")
        for phase in ("Succeeded", "Failed", "Pending", "Unknown"):
            with self.assertRaises(gate.Refused, msg=phase):
                gate.job_of([pod("10.0.0.7", phase=phase)], "10.0.0.7")

    def test_no_pod_another_pods_address_or_two_pods_are_refused(self):
        for pods in ([], [pod("10.0.0.8")], [pod("10.0.0.7"), pod("10.0.0.7", "ann-report")],
                     [pod("10.0.0.7", hostNetwork=True)], [{"status": {"phase": "Running"}}], [{}]):
            with self.assertRaises(gate.Refused, msg=pods):
                gate.job_of(pods, "10.0.0.7")

    def test_a_server_that_ignored_the_selector_misleads_nobody(self):
        # every pod of the namespace comes back: still only the address counts
        self.assertEqual(gate.job_of([pod("10.0.0.1", "trino-serviceaccount"), pod("10.0.0.7")], "10.0.0.7"), "eve-load")

    def test_the_user_is_the_applications_label(self):
        self.assertEqual(gate.user_of("eve-load", {"metadata": {"labels": {"data.test/user": "eve"}}}), "eve")

    def test_an_account_without_application_or_a_job_without_user_is_refused(self):
        for app in (None, {}, {"metadata": {}}, {"metadata": {"labels": {"user": "eve"}}},
                    {"metadata": {"labels": {"data.test/user": "Eve Adams"}}},
                    {"metadata": {"labels": {"data.test/user": ""}}}, {"metadata": {"labels": {"data.test/user": 7}}}):
            with self.assertRaises(gate.Refused, msg=app):
                gate.user_of("trino-serviceaccount", app)

    def test_the_groups_are_those_of_trinos_group_file(self):
        text = "analysts:ann,eve\nengineers:eve\nadmins:adam\nempty:\nnot a line\n"
        self.assertEqual(gate.groups_of(text, "eve"), ["analysts", "engineers"])
        self.assertEqual(gate.groups_of(text, "adam"), ["admins"])
        self.assertEqual(gate.groups_of(text, "nick"), [])
        self.assertEqual(gate.groups_of("engineers:evelyn,steve\n", "eve"), [])      # a part of a name is no member
        self.assertEqual(gate.groups_of("", "eve"), [])


class MayThey(unittest.TestCase):
    def test_a_read_is_trinos_select_and_a_commit_its_insert(self):
        for kind, operation, word in (("read", "SelectFromColumns", "READ"), ("write", "InsertIntoTable", "WRITE")):
            rule, asked, missing = gate.question(EVE, (kind, "retail_orders_silver", "orders"))
            self.assertEqual(rule, "allow")
            self.assertEqual(asked, {"context": {"identity": {"user": "eve", "groups": ["engineers"]}},
                                     "action": {"operation": operation, "resource": {"table": {
                                         "catalogName": "lakehouse", "schemaName": "retail_orders_silver", "tableName": "orders"}}}})
            self.assertIn(f"eve has no {word} grant on the table lakehouse.retail_orders_silver.orders", missing)

    def test_a_schema_is_trinos_show_tables(self):
        rule, asked, missing = gate.question(EVE, ("schema", "retail_orders_silver"))
        self.assertEqual((rule, asked["action"]), ("allow", {"operation": "ShowTables", "resource": {"schema": {
            "catalogName": "lakehouse", "schemaName": "retail_orders_silver"}}}))
        self.assertIn("Meta Catalog", missing)

    def test_a_list_of_tables_keeps_what_the_agent_allows(self):
        listing = {"identifiers": [{"namespace": ["s"], "name": "a"}, {"namespace": ["s"], "name": "b"},
                                   {"namespace": ["s"], "name": "c"}], "next-page-token": "n"}
        asked = []

        def batch(question_input):
            asked.append(question_input)
            return [0, 2]
        self.assertEqual(gate.filtered(listing, ("tables", "s"), EVE, batch),
                         {"identifiers": [{"namespace": ["s"], "name": "a"}, {"namespace": ["s"], "name": "c"}],
                          "next-page-token": "n"})
        self.assertEqual(asked[0]["action"]["operation"], "FilterTables")
        self.assertEqual(asked[0]["context"]["identity"], {"user": "eve", "groups": ["engineers"]})
        self.assertEqual([r["table"]["tableName"] for r in asked[0]["action"]["filterResources"]], ["a", "b", "c"])

    def test_a_list_of_schemas_keeps_what_the_agent_allows(self):
        listing = {"namespaces": [["default"], ["retail_orders_silver"], ["a", "b"], "odd"]}
        asked = []

        def batch(question_input):
            asked.append(question_input)
            return [1]          # of the two it was asked about: the second
        self.assertEqual(gate.filtered(listing, ("schemas",), EVE, batch), {"namespaces": [["retail_orders_silver"]]})
        self.assertEqual(asked[0]["action"], {"operation": "FilterSchemas", "filterResources": [
            {"schema": {"catalogName": "lakehouse", "schemaName": "default"}},
            {"schema": {"catalogName": "lakehouse", "schemaName": "retail_orders_silver"}}]})

    def test_what_the_agent_does_not_name_is_dropped(self):
        listing = {"identifiers": [{"namespace": ["s"], "name": "a"}, {"namespace": ["other"], "name": "b"}, {"name": "c"}]}
        self.assertEqual(gate.filtered(listing, ("tables", "s"), EVE, lambda i: [])["identifiers"], [])
        # positions it was never asked about, and answers that are no positions
        self.assertEqual(gate.filtered(listing, ("tables", "s"), EVE, lambda i: [1, 2, 7, -1, True, "0"])["identifiers"], [])
        self.assertEqual(gate.filtered(listing, ("tables", "s"), EVE, lambda i: [0])["identifiers"],
                         [{"namespace": ["s"], "name": "a"}])

    def test_an_empty_list_asks_nothing(self):
        def batch(question_input):
            raise AssertionError("asked")
        self.assertEqual(gate.filtered({"identifiers": []}, ("tables", "s"), EVE, batch), {"identifiers": []})

    def test_a_list_or_an_answer_that_is_not_one_stops_the_request(self):
        for listing in ([], {"identifiers": {"a": 1}}, {"tables": []}, None):
            with self.assertRaises(gate.Unavailable, msg=listing):
                gate.filtered(listing, ("tables", "s"), EVE, lambda i: [0])
        for answer in (None, True, {"0": True}):
            with self.assertRaises(gate.Unavailable, msg=answer):
                gate.filtered({"identifiers": [{"namespace": ["s"], "name": "a"}]}, ("tables", "s"), EVE, lambda i: answer)


if __name__ == "__main__":
    unittest.main()
