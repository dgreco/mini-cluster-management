"""The DAG lakehouse_spark: the lakehouse example's silver and gold tables
from bronze, with Python on Spark (jobs/bronze_to_silver.py,
jobs/silver_to_gold.py), the same as lakehouse_trino does with SQL.

One task per silver and gold table, in the tables' lineage like
lakehouse_trino; each submits a Spark job in compute as the pipeline
(SparkApplication service-account-lakehouse-pipeline-<step>-<key>) and waits
for it. A silver job reads its bronze parquet files with the template
lakehouse-files, a gold one reads and writes Iceberg with the template
lakehouse.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task

# the helper package next to this file (git-sync's checkout of the example)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_pipeline import catalog, steps


def run(key: str):
    p = get_current_context()["params"]
    return steps.spark_step(key, executors=int(p["executors"]), executor_memory=p["executor_memory"])


@dag(dag_id="lakehouse_spark", schedule=None, start_date=datetime(2026, 1, 1), catchup=False,
     # a task pod may fail to start (an image pull, the network policy of a new pod): once more
     default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
     max_active_runs=1, tags=["lakehouse", "silver", "gold", "spark"], doc_md=__doc__,
     params={
         "executors": Param(2, type="integer", minimum=1, maximum=16, description="Executors of each Spark job"),
         "executor_memory": Param("", type="string",
                                  description="Memory limit of an executor (empty: the template's, 2Gi)"),
     })
def lakehouse_spark():
    tables = catalog.read_tables()
    made = {}
    for key in catalog.built(tables):
        made[key] = task(task_id=steps.task_id(key),
                         doc_md=f"{key}: jobs/{steps.job_file(key).rsplit('/', 1)[1]}")(run)(key)
        for dep in tables[key].depends:
            if dep in made:
                made[dep] >> made[key]


lakehouse_spark()
