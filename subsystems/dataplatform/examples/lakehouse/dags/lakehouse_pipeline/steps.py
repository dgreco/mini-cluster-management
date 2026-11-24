"""What the DAGs' tasks do, one function per kind of task (the DAG files only
wire them). Every function runs in a task pod of Airflow's KubernetesExecutor,
as the pipeline (platform.py).
"""
import json
import os
import time

from . import catalog, platform

EXAMPLE = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
SQL_DIR = os.path.join(EXAMPLE, "sql")
JOBS_DIR = os.path.join(EXAMPLE, "jobs")


def sql_file(key: str) -> str:
    domain, layer, name = key.split(".")
    return os.path.join(SQL_DIR, layer, f"{domain}_{name}.sql")


def job_file(key: str) -> str:
    return os.path.join(JOBS_DIR, "bronze_to_silver.py" if key.split(".")[1] == "silver" else "silver_to_gold.py")


def task_id(key: str) -> str:
    return key.replace(".", "_")


# ---------------------------------------------------------------- bronze ----
def generate_plan(orders: int, rows_per_file: int, seed: int) -> list:
    """Every part to write, with what writing it needs."""
    from . import generator     # numpy, pyarrow: only where data is made
    ingested_ms = int(time.time() * 1000)
    return [{**p, "seed": int(seed), "orders": int(orders), "ingested_ms": ingested_ms}
            for p in generator.plan(int(orders), int(rows_per_file))]


def prepare_bronze(log=print) -> dict:
    """Empties the bronze tables' folders (but their directory objects): the
    generator writes them whole, and a run with fewer parts must not leave
    the last run's other files behind."""
    from . import generator     # numpy, pyarrow: only where data is made
    tables = catalog.read_tables()
    s3 = platform.s3_client()
    out = {}
    for key in generator.BRONZE.values():
        t = tables[key]
        n = platform.empty_folder(s3, t.folder(platform.prefix()))
        log(f"{t.trino(platform.catalog())}: {n} file(s) deleted from {t.location(platform.bucket(), platform.prefix())}")
        out[key] = n
    return out


def generate_part(spec: dict, log=print) -> dict:
    from . import generator     # numpy, pyarrow: only where data is made
    tables = catalog.read_tables()
    t = tables[generator.BRONZE[spec["table"]]]
    started = time.time()
    data = generator.make(spec, spec["seed"], spec["orders"], spec["ingested_ms"])
    generator.check_columns(data, t.columns)
    key = f"{t.folder(platform.prefix())}/{generator.file_name(spec['part'])}"
    size = generator.write(platform.s3_client(), platform.bucket(), key, data)
    log(f"{t.trino(platform.catalog())}: {data.num_rows} rows, {size // 1024} KiB to {key} "
        f"in {time.time() - started:.1f} s")
    return {"table": t.key, "rows": data.num_rows, "bytes": size}


def count_bronze(results: list, log=print) -> dict:
    """What each bronze table holds now, as Trino sees it: the generator's
    rows must all be there."""
    tables = catalog.read_tables()
    written = {}
    for r in results:
        written[r["table"]] = written.get(r["table"], 0) + r["rows"]
    keys = sorted(written)
    answers = platform.trino_run([f"SELECT count(*) FROM {tables[k].trino(platform.catalog())}" for k in keys], log)
    seen = {k: a[0][0] for k, a in zip(keys, answers)}
    for k in keys:
        log(f"{k}: {seen[k]} rows in Trino, {written[k]} written")
    wrong = [k for k in keys if seen[k] != written[k]]
    if wrong:
        raise RuntimeError(f"Trino does not see what was written in {', '.join(wrong)}")
    return seen


# ---------------------------------------------------------------- Trino ----
def trino_step(key: str, log=print) -> list:
    tables = catalog.read_tables()
    with open(sql_file(key)) as f:
        statements = catalog.statements(catalog.render(f.read(), tables, platform.catalog()))
    answers = platform.trino_run(statements, log)
    rows = platform.trino_run([f"SELECT count(*) FROM {tables[key].trino(platform.catalog())}"], log)[0][0][0]
    log(f"{key}: {rows} rows")
    return [a[:3] for a in answers]


# ---------------------------------------------------------------- Spark ----
def spark_tables(key: str) -> dict:
    """The target and what it is built from: Iceberg name and folder (s3a)."""
    tables = catalog.read_tables()
    out = {}
    for k in (key,) + tables[key].depends:
        t = tables[k]
        out[k] = {"name": t.trino(platform.catalog()),
                  "location": t.location(platform.bucket(), platform.prefix(), scheme="s3a")}
    return out


def spark_step(key: str, executors: int = 2, executor_memory: str = "", log=print) -> str:
    """A silver table reads bronze parquet through S3A, hence the template
    lakehouse-files (manifests/compute/spark/spark-pipeline.yaml); a gold one
    reads and writes Iceberg only (the template lakehouse)."""
    layer = key.split(".")[1]
    step = ("b2s-" if layer == "silver" else "s2g-") + key.split(".")[0][:3]
    return platform.submit_spark(step, job_file(key), ["--target", key, "--tables-file", "/stackable/jobs/tables.json"],
                                 template="lakehouse-files" if layer == "silver" else "lakehouse",
                                 executors=executors, executor_memory=executor_memory,
                                 files={"tables.json": json.dumps(spark_tables(key), indent=1)}, log=log)
