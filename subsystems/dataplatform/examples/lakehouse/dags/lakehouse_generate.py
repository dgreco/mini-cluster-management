"""The DAG lakehouse_generate: synthetic data into the lakehouse example's
bronze tables (parquet), and nothing else.

  plan      the parts: one parquet file of at most rows_per_file rows each
  prepare   empties the four bronze folders (sales.bronze.customers, .orders,
            logistics.bronze.shipments, .carriers)
  generate  one task per part, side by side (at most four at a time),
            each in a pod of its own, writing its file with the pipeline's
            STS role
  count     Trino must count every row that was written

orders sets the size: N orders, N/20 customers, about 0.74 N shipments, so
200 million orders write some 360 million rows (lakehouse_pipeline/
generator.py). The same seed gives the same data.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task
from kubernetes.client import models as k8s

# the helper package next to this file (git-sync's checkout of the example)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_pipeline import steps

# a part of two million orders takes some hundred MB while it is written
PART_POD = {"pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(
    name="base", resources=k8s.V1ResourceRequirements(requests={"cpu": "500m", "memory": "1Gi"},
                                                      limits={"memory": "3Gi"}))]))}


@dag(dag_id="lakehouse_generate", schedule=None, start_date=datetime(2026, 1, 1), catchup=False,
     # a task pod may fail to start (an image pull, the network policy of a new pod): once more
     default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
     max_active_runs=1, tags=["lakehouse", "bronze"], doc_md=__doc__,
     params={
         "orders": Param(2_000_000, type="integer", minimum=1_000, maximum=2_000_000_000,
                         description="Orders to make (customers: a twentieth; shipments: about 74%)"),
         "rows_per_file": Param(2_000_000, type="integer", minimum=10_000, maximum=10_000_000,
                                description="Rows of one parquet file, and of one task"),
         "seed": Param(42, type="integer", description="The same seed makes the same data"),
     })
def lakehouse_generate():
    @task
    def plan() -> list:
        p = get_current_context()["params"]
        parts = steps.generate_plan(p["orders"], p["rows_per_file"], p["seed"])
        print(f"{len(parts)} part(s) for {p['orders']} orders")
        return parts

    @task
    def prepare() -> dict:
        return steps.prepare_bronze()

    @task(executor_config=PART_POD, max_active_tis_per_dag=4, retries=2)
    def generate(spec: dict) -> dict:
        return steps.generate_part(spec)

    @task
    def count(results: list) -> dict:
        return steps.count_bronze(list(results))

    parts = plan()
    emptied = prepare()
    written = generate.expand(spec=parts)
    emptied >> written
    count(written)


lakehouse_generate()
