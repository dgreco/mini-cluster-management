"""The DAG lakehouse_trino: the lakehouse example's silver and gold tables
from bronze, with Trino.

One task per silver and gold table of the platform's tables.yaml, each
running its SQL (sql/<layer>/<domain>_<name>.sql) as the pipeline; a task
starts once the tables it is built from are done: the task graph is the
tables' lineage in Meta Catalog (dependsOn). Bronze comes from the DAG
lakehouse_generate.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task

# the helper package next to this file (git-sync's checkout of the example)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_pipeline import catalog, steps


@dag(dag_id="lakehouse_trino", schedule=None, start_date=datetime(2026, 1, 1), catchup=False,
     # a task pod may fail to start (an image pull, the network policy of a new pod): once more
     default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
     max_active_runs=1, tags=["lakehouse", "silver", "gold", "trino"], doc_md=__doc__)
def lakehouse_trino():
    tables = catalog.read_tables()
    made = {}
    for key in catalog.built(tables):
        made[key] = task(task_id=steps.task_id(key), retries=1, doc_md=f"{key}: sql/{key.split('.')[1]}/"
                         f"{key.split('.')[0]}_{key.split('.')[2]}.sql")(steps.trino_step)(key)
        for dep in tables[key].depends:
            if dep in made:
                made[dep] >> made[key]


lakehouse_trino()
