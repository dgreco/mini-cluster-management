"""Unit tests of the platform's table tool (files/tables.py, behind
dp-tables.sh: the file's checks and what sync plans: making, changing,
removing, its scope) and of the lakehouse pipelines' helpers
(examples/lakehouse/dags/lakehouse_pipeline: the tables of tables.yaml, the
SQL, the Spark jobs' names and verdicts, the generator).
The generator's tests need numpy and pyarrow (Airflow's image has both) and
are skipped without them."""
import copy
import importlib.util
import json
import os
import sys
import unittest

import yaml

EXAMPLE = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "examples", "lakehouse"))
METACATALOG = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "metacatalog"))
FILE = os.path.join(METACATALOG, "tables", "tables.yaml")
SMOKE_FILE = os.path.join(METACATALOG, "tables", "smoke.yaml")
sys.path.insert(0, os.path.join(EXAMPLE, "dags"))

spec = importlib.util.spec_from_file_location("tables_tool", os.path.join(os.path.dirname(__file__), "..", "tables.py"))
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)

from lakehouse_pipeline import catalog, platform  # noqa: E402

try:
    import numpy  # noqa: F401
    import pyarrow  # noqa: F401
    from lakehouse_pipeline import generator
except ImportError:                                  # make test: standard library and PyYAML only
    generator = None


def docs():
    return copy.deepcopy(tool.read_docs(FILE))


def live_of(f, provisioned=True):
    """Meta Catalog as it is once apply has run: every table and domain there."""
    tables = {}
    for tref, d in f["tables"].items():
        st = {"provisioningStatus": "PROVISIONED", "authorizationStatus": "AUTHORIZED"} if provisioned else {}
        tables[tref] = {"id": "id-" + tref, "values": {**d["values"], **st}, "part": "p-" + tref,
                        "columns": copy.deepcopy(tool.columns_of(d)), "lineage": list(f["lineage"][tref]),
                        "resources": [dict(st), dict(st)]}
    domains = {name: {"id": "id-" + name, "values": dict(d["values"]),
                      "dependencies": tool.domain_tables(f)[name]} for name, d in f["domains"].items()}
    return {"domains": domains, "tables": tables}


class TheFile(unittest.TestCase):
    def test_the_example_is_right(self):
        f = tool.check(docs())
        self.assertEqual(sorted(f["domains"]), ["logistics", "sales"])
        self.assertEqual(len(f["tables"]), 10)
        self.assertEqual(f["lineage"]["logistics/gold/delivery_performance_v1"],
                         ["logistics/silver/shipments_v1", "sales/silver/orders_v1", "sales/silver/customers_v1"])

    def test_ownership(self):
        """bronze and silver: engineers and the pipeline; gold: analysts may read."""
        for tref, d in tool.check(docs())["tables"].items():
            grants = {(p, tuple(g["permissions"])) for g in d["values"]["grants"] for p in g["principals"]}
            self.assertIn(("data-engineers", ("READ", "WRITE")), grants, tref)
            self.assertIn(("lakehouse-pipeline", ("READ", "WRITE")), grants, tref)
            if "/gold/" in tref:
                self.assertIn(("data-analysts", ("READ",)), grants, tref)
            else:
                self.assertFalse(any(p == "data-analysts" for p, _ in grants), tref)
            if "/bronze/" in tref:
                self.assertEqual(d["values"]["format"], "parquet", tref)

    def refused(self, d, text):
        with self.assertRaises(tool.Refused) as e:
            tool.check(d)
        self.assertIn(text, str(e.exception))

    def test_refusals(self):
        d = docs()
        d[2]["values"]["layer"] = "PLATINUM"
        self.refused(d, "layer 'PLATINUM'")
        d = docs()
        d[2]["values"]["format"] = "avro"
        self.refused(d, "format 'avro'")
        d = docs()
        d[2]["values"]["version"] = 0
        self.refused(d, "version 0")
        d = docs()
        d[2]["values"]["domain"] = "hr"
        self.refused(d, "domain 'hr' is not a domain of this file")
        d = docs()
        d[0]["values"]["name"] = "sales_eu"
        self.refused(d, "domain name 'sales_eu'")
        d = docs()
        d[6]["dependsOn"] = ["nothing"]
        self.refused(d, "dependsOn 'nothing' is no table")
        d = docs()
        d[3]["ref"] = d[2]["ref"]
        self.refused(d, "twice")
        d = docs()
        d[2]["values"]["grants"].append({"principals": ["somebody"], "permissions": ["READ"]})
        self.refused(d, "not among the principals")
        d = docs()
        d[2]["parts"][0]["values"]["columns"].append(dict(d[2]["parts"][0]["values"]["columns"][0]))
        self.refused(d, "a column named twice")

    def test_a_cycle(self):
        d = docs()
        bronze = next(x for x in d if x["ref"] == "sales_bronze_orders_v1")
        bronze["dependsOn"] = ["sales_silver_orders_v1"]
        self.refused(d, "goes round")

    def test_a_new_version_is_another_table(self):
        d = docs()
        v2 = copy.deepcopy(next(x for x in d if x["ref"] == "sales_silver_orders_v1"))
        v2["ref"], v2["values"]["version"] = "sales_silver_orders_v2", 2
        f = tool.check(d + [v2])
        self.assertIn("sales/silver/orders_v1", f["tables"])
        self.assertIn("sales/silver/orders_v2", f["tables"])


class ThePlan(unittest.TestCase):
    def test_from_nothing(self):
        f = tool.check(docs())
        p = tool.plan(f, {"domains": {}, "tables": {}})
        self.assertEqual(len(p["create"]), 12)
        self.assertEqual(len(p["provision"]), 10)
        self.assertEqual(len(p["authorize"]), 10)
        self.assertEqual(p["update"], [])

    def test_nothing_to_do(self):
        f = tool.check(docs())
        p = tool.plan(f, live_of(f))
        self.assertEqual(tool.describe(p), [])

    def test_changes(self):
        f = tool.check(docs())
        live = live_of(f)
        live["tables"]["sales/gold/daily_sales_v1"]["values"]["description"] = "old"
        live["tables"]["sales/silver/orders_v1"]["columns"].pop()           # the file added a column
        live["tables"]["sales/bronze/orders_v1"]["resources"][1]["authorizationStatus"] = None
        live["domains"]["logistics"]["dependencies"].pop()
        p = tool.plan(f, live)
        self.assertEqual(dict(p["update"]), {"sales/gold/daily_sales_v1": ["description"],
                                              "sales/silver/orders_v1": ["columns"]})
        self.assertEqual(p["domains"], ["logistics"])
        self.assertEqual(sorted(p["provision"]), ["sales/gold/daily_sales_v1", "sales/silver/orders_v1"])
        self.assertEqual(sorted(p["authorize"]), ["sales/bronze/orders_v1", "sales/gold/daily_sales_v1",
                                                  "sales/silver/orders_v1"])

    def test_what_cannot_change(self):
        f = tool.check(docs())
        for change in ("format", "drop", "retype"):
            live = live_of(f)
            t = live["tables"]["sales/silver/orders_v1"]
            if change == "format":
                t["values"]["format"] = "parquet"
            elif change == "drop":
                t["columns"].append({"name": "gone", "type": "string", "description": "x"})
            else:
                t["columns"][0]["type"] = "string"
            with self.assertRaises(tool.Refused, msg=change):
                tool.plan(f, live)

    def test_what_the_file_lost_is_removed(self):
        """sync removes a table of its scope that the file no longer has
        (dependants first), and a domain without tables; never a table of the
        smoke test's domains, nor anything outside TABLES_SYNC_DOMAINS."""
        f = tool.check(docs())
        live = live_of(f)
        old = copy.deepcopy(live["tables"]["sales/silver/orders_v1"])
        old["values"]["version"] = 0
        live["tables"]["sales/silver/orders_v0"] = old
        gold = copy.deepcopy(live["tables"]["sales/gold/daily_sales_v1"])
        gold["values"]["name"], gold["lineage"] = "old_gold", ["sales/silver/orders_v0"]
        live["tables"]["sales/gold/old_gold_v1"] = gold
        smoke = copy.deepcopy(live["tables"]["sales/bronze/orders_v1"])
        smoke["values"]["domain"] = "smoke"
        live["tables"]["smoke/bronze/orders_v1"] = smoke
        live["domains"]["retired"] = {"id": "id-retired", "values": {"name": "retired", "description": "x"}, "dependencies": []}
        live["domains"]["smoke"] = {"id": "id-smoke", "values": {"name": "smoke", "description": "x"}, "dependencies": []}
        p = tool.plan(f, live)
        self.assertEqual(p["remove"], ["sales/gold/old_gold_v1", "sales/silver/orders_v0"])
        self.assertEqual(p["remove_domains"], ["retired"])
        with self.assertRaises(tool.Refused):           # a domain of the file outside the scope
            tool.plan(f, live, scope=["logistics"])

    def test_scope_and_refusals(self):
        f = tool.check(docs())
        with self.assertRaises(tool.Refused):           # a file with no table would remove them all
            tool.plan({**f, "tables": {}}, live_of(f))
        smoke = tool.check(copy.deepcopy(tool.read_docs(SMOKE_FILE)))
        with self.assertRaises(tool.Refused):           # the smoke test's domain is outside the default scope
            tool.plan(smoke, {"domains": {}, "tables": {}})
        p = tool.plan(smoke, {"domains": {}, "tables": {}}, scope=["smoke"])
        self.assertEqual(len(p["create"]), 6)
        self.assertIn("smoke/bronze/drop_zone_v1", p["provision"])       # a folder only, no schema
        self.assertTrue(tool.in_scope("sales", []) and not tool.in_scope("smoke", []) and not tool.in_scope("smokeabc", []))
        live = live_of(f)
        live["tables"]["sales/gold/daily_sales_v1"]["values"]["grants"] = []   # Meta Catalog's, so the file's differ
        no_grants = copy.deepcopy(docs())
        for d in no_grants:
            if d.get("ref") == "sales_gold_daily_sales_v1":
                d["values"]["grants"] = []
        p = tool.plan(tool.check(no_grants), live_of(tool.check(no_grants)))
        self.assertEqual(p["reject"], ["sales/gold/daily_sales_v1"])          # authorized, the file grants nothing

    def test_a_schema_added_or_taken_away_is_refused(self):
        f = tool.check(docs())
        live = live_of(f)
        live["tables"]["sales/silver/orders_v1"]["part"] = None
        with self.assertRaises(tool.Refused):
            tool.plan(f, live)

    def test_upload_names_existing_tables_by_id(self):
        f = tool.check(docs())
        text = tool.upload_document(f, ["logistics", "logistics_gold_delivery_performance_v1"],
                                    {"sales/silver/orders_v1": "ID1", "sales/silver/customers_v1": "ID2",
                                     "logistics/silver/shipments_v1": "ID3", "logistics/bronze/carriers_v1": "ID4",
                                     "logistics/bronze/shipments_v1": "ID5"})
        up = list(yaml.safe_load_all(text))
        dom = next(u for u in up if u["entityType"] == tool.DOMAIN_TYPE)
        gold = next(u for u in up if u["entityType"] == tool.TABLE_TYPE)
        self.assertIn("logistics_gold_delivery_performance_v1", dom["dependsOn"])   # in the upload: by ref
        self.assertIn("ID4", dom["dependsOn"])                                       # there already: by id
        self.assertEqual(sorted(gold["dependsOn"]), ["ID1", "ID2", "ID3"])

    def test_update_keeps_the_part(self):
        d = tool.check(docs())["tables"]["sales/silver/orders_v1"]
        u = yaml.safe_load(tool.update_document(d, "E", "P", ["B"]))
        self.assertEqual((u["id"], u["parts"][0]["id"], u["dependencies"]), ("E", "P", ["B"]))

    def test_removal_order(self):
        f = tool.check(docs())
        live = live_of(f)["tables"]
        domain = [t for t in live if t.startswith("sales/")]
        with self.assertRaises(tool.Refused):     # logistics' gold reads sales' silver
            tool.removal_order(live, domain)
        order = tool.removal_order(live, [t for t in live if t.startswith("logistics/")])
        self.assertLess(order.index("logistics/gold/delivery_performance_v1"), order.index("logistics/silver/shipments_v1"))
        self.assertLess(order.index("logistics/silver/shipments_v1"), order.index("logistics/bronze/carriers_v1"))


class TheDagsHelpers(unittest.TestCase):
    def test_tables(self):
        t = catalog.read_tables(FILE)
        o = t["sales.silver.orders"]
        self.assertEqual(o.trino(), "lakehouse.sales_silver.orders_v1")
        self.assertEqual(o.location("bkt"), "s3://bkt/lakehouse/sales/silver/orders_v1")
        self.assertEqual(o.depends, ("sales.bronze.orders",))

    def test_the_names_are_the_models(self):
        """catalog.py says what the model's mappings say."""
        model = list(yaml.safe_load_all(open(os.path.join(METACATALOG, "table-model.yaml"))))
        maps = {(m["sourceEntityType"], m["targetEntityType"]): m["mappingValues"] for m in model[3]["Mappings"]}
        folder = maps[("TableType", "S3FolderType")]
        self.assertIn("'lakehouse/'", folder["path"])
        self.assertIn("'_v'", folder["table"])
        table = maps[("TableSchemaType", "HiveTableType")]
        self.assertIn("+ '_'", table["schema"])

    def test_highest_version_wins(self):
        d = docs()
        v2 = copy.deepcopy(next(x for x in d if x["ref"] == "sales_silver_orders_v1"))
        v2["ref"], v2["values"]["version"] = "sales_silver_orders_v2", 2
        path = os.path.join(os.environ.get("TMPDIR", "/tmp"), "lakehouse-test.yaml")
        with open(path, "w") as fh:
            yaml.safe_dump_all(d + [v2], fh, sort_keys=False)
        try:
            self.assertEqual(catalog.read_tables(path)["sales.silver.orders"].versioned, "orders_v2")
        finally:
            os.unlink(path)

    def test_steps_follow_the_lineage(self):
        order = catalog.built(catalog.read_tables(FILE))
        self.assertEqual(len(order), 6)
        self.assertLess(order.index("sales.silver.orders"), order.index("logistics.gold.delivery_performance"))
        self.assertLess(order.index("logistics.silver.shipments"), order.index("logistics.gold.delivery_performance"))

    def test_every_step_has_its_sql_and_job(self):
        from lakehouse_pipeline import steps
        tables = catalog.read_tables(FILE)
        for key in catalog.built(tables):
            with open(steps.sql_file(key)) as fh:
                sql = "\n".join(catalog.statements(catalog.render(fh.read(), tables)))
            self.assertNotIn("{", sql, key)
            self.assertIn(tables[key].trino(), sql, key)
            self.assertTrue(os.path.isfile(steps.job_file(key)), key)
            with open(steps.job_file(key)) as fh:
                self.assertIn(f'"{key}"', fh.read(), key)

    def test_render_and_statements(self):
        t = catalog.read_tables(FILE)
        sql = "-- a; comment\nDELETE FROM {sales.gold.daily_sales};\nINSERT INTO {sales.gold.daily_sales}\nSELECT 1;\n"
        self.assertEqual(catalog.statements(catalog.render(sql, t)),
                         ["DELETE FROM lakehouse.sales_gold.daily_sales_v1",
                          "INSERT INTO lakehouse.sales_gold.daily_sales_v1\nSELECT 1"])
        with self.assertRaises(KeyError):
            catalog.render("{sales.gold.nothing}", t)

    def test_job_names(self):
        # Stackable starts no SparkApplication whose name has more than 40
        # characters (measured: the pipeline's first Spark run, 49)
        self.assertEqual(platform.JOB_NAME_MAX, 40)
        self.assertEqual(platform.job_name("alice", "b2s-sal", "0abc12"), "alice-b2s-sal-0abc12")
        user = "service-account-lakehouse-pipeline"
        self.assertEqual(platform.job_name(user, "b2s-sal", "0abc12"), f"{user}-abc12")
        for _ in range(200):
            name = platform.job_name(user, "s2g-log", platform.run_key())
            self.assertLessEqual(len(name), 40)
            self.assertRegex(name, platform.NAME)
            self.assertTrue(name.startswith(user + "-"))
        with self.assertRaises(ValueError):
            platform.job_name("u" * 36, "b2s-sal", "0abc12")
        self.assertEqual(len({platform.run_key() for _ in range(100)}), 100)

    def test_log_follower(self):
        # the driver pod is removed when its job ends: the log is followed
        # while it runs, through a container that has not started yet and a
        # follow that breaks, and kept when the pod is gone
        class Status(Exception):
            def __init__(self, status):
                super().__init__(f"HTTP {status}")
                self.status = status

        class Stream:
            def __init__(self, chunks, fail=None):
                self.chunks, self.fail = chunks, fail

            def stream(self, size):
                yield from self.chunks
                if self.fail:
                    raise self.fail

        def follow(answers):
            calls = iter(answers)

            def read(pod, ns, container, follow_):
                self.assertEqual((container, follow_), ("spark", True))
                a = next(calls)
                if isinstance(a, Exception):
                    raise a
                return a
            f = platform.LogFollower(read, "job-driver", "compute", patience=5, pause=0)
            f.start()
            f.join(5)
            self.assertFalse(f.is_alive())
            return f

        done = b"line\n" + platform.DONE.encode() + b" {}\n"
        f = follow([Status(400), Status(400), Stream([b"line\n", platform.DONE.encode() + b" {}\n"])])
        self.assertEqual(platform.judge("Succeeded", f.text), "Succeeded")
        f = follow([Stream([b"line\n"], fail=ConnectionError("reset")), Stream([done])])
        self.assertEqual(f.text, done.decode())
        f = follow([Stream([b"line\n", done[5:]], fail=ConnectionError("reset")), Status(404)])
        self.assertEqual(platform.judge("Succeeded", f.text), "Succeeded")
        self.assertIn("404", f.error)

    def test_judge(self):
        ok = "...\nLAKEHOUSE_JOB_DONE {}\n"
        self.assertEqual(platform.judge("Succeeded", ok), "Succeeded")
        self.assertEqual(platform.judge("Succeeded", "Traceback (most recent call last):\n" + ok), "Failed (Python error)")
        self.assertTrue(platform.judge("Succeeded", "no marker").startswith("Failed"))
        self.assertEqual(platform.judge("Running", ok, "OOMKilled"), "Failed (driver OOMKilled)")
        self.assertEqual(platform.judge("Failed", ok), "Failed")

    def test_spark_application(self):
        os.environ.update({"S3_REGION": "r", "S3_ENDPOINT": "https://s3", "LABEL_DOMAIN": "data.example.org",
                           "SPARK_IMAGE": "ghcr.io/x/spark:1@sha256:0", "SPARK_PRODUCT_VERSION": "4.1.2"})
        app = platform.spark_application("u-b2s-sal-1", "u", "spark-token-u-b2s-sal-1", "arn:r", "/x/job.py",
                                         ["--target", "t"], "lakehouse-files", 3)
        self.assertEqual(app["metadata"]["labels"], {"data.example.org/user": "u"})
        self.assertEqual(app["metadata"]["annotations"]["spark-application.template.0.name"], "lakehouse-files")
        self.assertEqual(app["spec"]["volumes"][0]["configMap"]["name"], "job-u-b2s-sal-1")
        self.assertEqual(app["spec"]["executor"]["replicas"], 3)
        # the job's sparkImage replaces the template's whole: it names the image itself
        self.assertEqual(app["spec"]["sparkImage"], {"custom": "ghcr.io/x/spark:1@sha256:0", "productVersion": "4.1.2",
                                                     "pullPolicy": "IfNotPresent"})
        self.assertNotIn("sparkConf", app["spec"])            # spark.kubernetes.* is refused anyway
        env = {e["name"]: e["value"] for e in app["spec"]["driver"]["podOverrides"]["spec"]["containers"][0]["env"]}
        self.assertEqual(env["AWS_ROLE_ARN"], "arn:r")

    def test_spark_arguments_are_shell_safe(self):
        """The operator hands spark-submit the arguments through bash -c,
        unquoted: what the DAG passes must need no quoting; the tables'
        JSON goes as a file."""
        from lakehouse_pipeline import steps
        os.environ.update({"LAKEHOUSE_BUCKET": "bkt"})
        seen = {}
        real = platform.submit_spark
        platform.submit_spark = lambda step, job, args, **kw: seen.update(step=step, args=args, **kw) or "x"
        try:
            steps.spark_step("logistics.gold.delivery_performance")
        finally:
            platform.submit_spark = real
        self.assertTrue(all(platform.SAFE_ARG.match(a) for a in seen["args"]), seen["args"])
        self.assertEqual(seen["template"], "lakehouse")
        tables = json.loads(seen["files"]["tables.json"])
        self.assertEqual(sorted(tables), ["logistics.gold.delivery_performance", "logistics.silver.shipments",
                                          "sales.silver.customers", "sales.silver.orders"])
        with self.assertRaises(ValueError):
            real("s", "/x.py", ["--tables", '{"a": 1}'])

    def test_claims(self):
        import base64
        body = base64.urlsafe_b64encode(json.dumps({"preferred_username": "x"}).encode()).decode().rstrip("=")
        self.assertEqual(platform.claims(f"h.{body}.s")["preferred_username"], "x")


@unittest.skipIf(generator is None, "numpy and pyarrow (Airflow's image has them)")
class TheGenerator(unittest.TestCase):
    def test_plan(self):
        p = generator.plan(1_000_000, 300_000)
        by = {}
        for s in p:
            by.setdefault(s["table"], []).append(s)
        self.assertEqual(sum(s["count"] for s in by["orders"]), 1_000_000)
        self.assertEqual(sum(s["count"] for s in by["customers"]), 50_000)
        self.assertEqual([s["part"] for s in by["shipments"]], [s["part"] for s in by["orders"]])
        self.assertEqual(len(by["carriers"]), 1)

    def test_columns_are_the_tables(self):
        tables = catalog.read_tables(FILE)
        for table, key in generator.BRONZE.items():
            data = generator.make({"table": table, "part": 0, "start": 0, "count": 2000}, 1, 100_000, 0)
            generator.check_columns(data, tables[key].columns)

    def test_deterministic_and_consistent(self):
        spec = {"table": "orders", "part": 2, "start": 4000, "count": 2000}
        a = generator.make(spec, 7, 100_000, 0)
        self.assertTrue(a.equals(generator.make(spec, 7, 100_000, 0)))
        s = generator.make({**spec, "table": "shipments"}, 7, 100_000, 0)
        ids = set(a.column("order_id").to_pylist())
        self.assertTrue(set(s.column("order_id").to_pylist()) <= ids)


if __name__ == "__main__":
    unittest.main()
