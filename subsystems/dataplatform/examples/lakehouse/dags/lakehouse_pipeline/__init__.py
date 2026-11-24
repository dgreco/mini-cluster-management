"""What the lakehouse example's DAGs share (subsystems/dataplatform/examples/
lakehouse/dags, which Airflow reads from Git through git-sync):

  catalog    the tables, as metacatalog/tables/tables.yaml defines them: their
             Trino names and folders, at the highest version the file has
  platform   the pipeline's identity (the Keycloak client lakehouse-pipeline)
             and what it does with it: Trino, Ceph's STS, Spark jobs in
             compute
  generator  the synthetic data of the bronze tables

Nothing here imports Airflow: the DAG files do, and the unit tests
(subsystems/dataplatform/files/tests/test_lakehouse_pipeline.py) run without
it.
"""
