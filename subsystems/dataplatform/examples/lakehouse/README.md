# A lakehouse, bronze to gold — the platform's worked example

Two business domains, ten tables in three layers, a data generator that writes
hundreds of millions of rows, and pipelines that refine them twice: once with
**Trino** (SQL), once with **Python on Spark**. Airflow schedules everything.
Every step is authenticated and authorized like any other user of the
platform:
- Meta Catalog describes and builds every table: these are the platform's own
  domains and tables (`subsystems/dataplatform/metacatalog/tables/tables.yaml`,
  in the platform's model `metacatalog/table-model.yaml`), made with
  `make dataplatform-tables`;
- the tables' grants decide every read and write (Ceph for the files, the Open
  Policy Agent for Trino and for Spark's catalog);
- Keycloak is the only login.

```
                 sales                                   logistics
  BRONZE  customers_v1   orders_v1                 carriers_v1   shipments_v1      parquet, from the generator
             │              │                          │             │
  SILVER  customers_v1   orders_v1  ──────────┐        └── shipments_v1             iceberg
             │      ╲       │                 │                │
  GOLD    customer_value_v1  daily_sales_v1   └──── delivery_performance_v1         iceberg (across domains)
```

## What is in here

| Path | What it is |
|---|---|
| `../../metacatalog/table-model.yaml` | The platform's model in Meta Catalog: a **domain** (name, description) that depends on all its tables, and a **table**, a *versioned asset* (Meta Catalog's built-in `Asset` and `Versionable`) with a format (`parquet` or `iceberg`), a description, a layer (`BRONZE`, `SILVER`, `GOLD`), a version and its columns (name, type, description) |
| `../../metacatalog/tables/tables.yaml` | The two domains and ten tables, with their lineage and their grants: the one definition, which `make dataplatform-tables ARGS=sync` brings Meta Catalog to |
| `dags/` | Airflow's DAGs (read from Git by Airflow's git-sync) and their helper package `lakehouse_pipeline` |
| `sql/silver/`, `sql/gold/` | The Trino pipeline: one file per silver and gold table |
| `jobs/` | The Python pipeline: `bronze_to_silver.py`, `silver_to_gold.py` (PySpark) |
| `reset.sh` | `make dataplatform-lakehouse-reset`: the example from scratch (below) |

## Where a table lives

A table's place is built from its domain, layer, name and version, by the
model's mappings (Meta Catalog derives it, the provisioning script checks it):

| | |
|---|---|
| folder | `s3://dataplatform/lakehouse/<domain>/<layer>/<name>_v<version>` |
| Trino | `lakehouse.<domain>_<layer>.<name>_v<version>` |

with the layer in lower case: `sales/BRONZE/orders` version 1 is
`s3://dataplatform/lakehouse/sales/bronze/orders_v1` and
`lakehouse.sales_bronze.orders_v1`.

**The version is part of the table's identity.** Version 2 of a table is
another table, next to version 1, with its own folder and its own entry in
the metastore. To bring out a new version, copy the table's document in
`tables.yaml`, raise `version` (and the `ref`), change what the new version
changes, and sync. Version 1 stays as long as its document stays in the file:
take it out and sync drops it, with its data. The DAGs always use the highest
version the file has. Meta Catalog's own history (*Versions*
on a table's page) records every change of a table's description, columns or
grants as a numbered version, whatever its `version` says.

> A past version in Meta Catalog's history can be run (Provision, Unprovision,
> Authorize, Reject on the history page). Its resources are the **live**
> table's: unprovisioning a past version drops the live table and deletes its
> files, and authorizing one puts its old grants in force. Run functions on the
> live table only.

## Who owns what

| Layer | `data-engineers` | `data-analysts` | `lakehouse-pipeline` (Airflow) |
|---|---|---|---|
| BRONZE | read, write | – | read, write |
| SILVER | read, write | – | read, write |
| GOLD | read, write | read | read, write |

Read means querying the table and reading its files; write means also
changing its rows and writing its files. Nobody but Meta Catalog makes, alters
or drops a table. `platform-admins` reads and writes every table anyway.

The pipeline is a principal of its own: the Keycloak client
`lakehouse-pipeline`, whose service account
(`service-account-lakehouse-pipeline`) every Airflow task runs as. It reaches
exactly what the grants above give it:
- Trino judges its statements;
- Ceph gives its files to the STS role `usr-service-account-lakehouse-pipeline`;
- its Spark jobs run in `compute` under its own name, like a person's.

Who may *start* a pipeline is Airflow's business (below); what a run may
*touch* is always the pipeline's grants, never those of the person who pressed
the button.

## Run it

The tables are the platform's, so they are there once `make dataplatform-tables`
has synced `tables.yaml` (it sends the model when Meta Catalog has none). You
need a login in the admin group for authorizing, and once, Airflow's read
access to the repository (*Airflow* below):

```bash
make dataplatform-tables ARGS=check                # the model and tables.yaml, offline
make dataplatform-tables ARGS="-l admin -n sync"   # what sync would do
make dataplatform-tables ARGS="-l admin sync"      # domains and tables: made, provisioned, authorized
make dataplatform-tables ARGS=list
```

Then open https://airflow.data.example.org (Keycloak login) and trigger, in
this order:

1. **`lakehouse_generate`**: synthetic data into the four bronze tables.
   - Its parameter `orders` sets the size: N orders, N/20 customers, about
     0.74 N shipments. The default, 2 million, takes a few minutes; 200
     million writes some 360 million rows.
   - The data is cut into parquet files of `rows_per_file` rows (2 million by
     default). One Airflow task writes each file, four at a time, as the
     pipeline. The last task checks that Trino counts every row.
   - The same `seed` gives the same data. A run replaces all bronze data.
2. **`lakehouse_trino`**: silver and gold with Trino. One task per table, in
   the tables' lineage: a gold task waits for the silver tables it reads.
   - A dimension (`customers`) is merged.
   - Facts and aggregates are rebuilt whole (`DELETE`, then
     `INSERT ... SELECT`; a reader in between sees the table empty).
3. **`lakehouse_spark`** does the same with PySpark, as Spark jobs in
   `compute` (parameters: `executors`, `executor_memory`). Run either
   pipeline: both write the same tables, the same rows.

Then query, for example as an analyst in Trino or DBeaver:

```sql
SELECT order_date, sum(revenue_eur) FROM lakehouse.sales_gold.daily_sales_v1 GROUP BY 1 ORDER BY 1 DESC LIMIT 7;
SELECT carrier_name, service_level, sum(on_time) * 1.0 / sum(delivered) AS on_time_rate
FROM lakehouse.logistics_gold.delivery_performance_v1 GROUP BY 1, 2 ORDER BY 3;
```

An analyst gets *Access Denied* on bronze and silver, and on any `INSERT`
into gold: that is the policy, held by Trino and by Ceph.

### Sizes and time

On the four boards, a run of `orders=2000000` takes minutes; Trino's heaviest
statement is the join of every parcel to its order
(`logistics.gold.delivery_performance`). At hundreds of millions of rows,
watch Trino's memory (Grafana) and Ceph's free space (`make ceph-status`):
- bronze takes about 40 MiB per million orders, shipments and customers
  included (measured: 2 million orders make a 41 MiB file set of orders);
- each silver and gold layer takes a little less, in Iceberg.

The Spark pipeline spreads the same work over `executors` pods.

## Airflow

https://airflow.data.example.org runs Stackable's Airflow 3.0 (`tools`, app
`tools-airflow`, `gitops/manifests/tools/airflow`). Its roles come from your
Keycloak groups at every login:

| Group | Airflow role | May |
|---|---|---|
| `platform-admins` | Admin | everything, users and roles included |
| `data-engineers` | Op | trigger, pause, clear and mark DAGs; connections, variables, pools |
| `data-analysts` | Viewer | see the DAGs, their runs and logs |
| anybody else | Public | nothing |

Airflow reads the DAGs from this repository, with git-sync, on the branch
Argo CD follows: a pushed change of `dags/`, `sql/`, `jobs/` or of the
platform's `metacatalog/tables/tables.yaml` reaches Airflow within a minute. Every task runs
in a pod of its own (KubernetesExecutor) in `tools`.

Airflow reads the repository with a deploy key of its own, never Argo CD's,
made once:

```bash
ssh-keygen -t ed25519 -N '' -C airflow-git-sync -f .secrets/airflow-git
# add .secrets/airflow-git.pub to the project's deploy keys (read-only), then:
make dataplatform-openbao
```

## Changing or taking out a table

Edit `metacatalog/tables/tables.yaml` and sync: a changed table gets the
change as a new version in Meta Catalog, and a table taken out of the file is
rejected (its grants go), unprovisioned (the table is dropped, its files
deleted) and deleted, the tables built from it first, then a domain left
without tables. Sync shows that and asks first.

## Starting again from scratch

```bash
make dataplatform-lakehouse-reset ARGS="-l admin -n"   # what it would do
make dataplatform-lakehouse-reset ARGS="-l admin"      # do it (asks first)
```

The reset (`reset.sh`, a login in the admin group):

1. pauses the three DAGs and stops their running tasks;
2. deletes the pipeline's Spark jobs in `compute`;
3. deletes the DAGs' history in Airflow (runs, task instances, their
   results);
4. takes every domain and table of `tables.yaml` out of Meta Catalog:
   each table is rejected, unprovisioned (**dropped, its files deleted**)
   and deleted;
5. makes them all again with `make dataplatform-tables ARGS=sync`: empty,
   provisioned, authorized, with a new history in Meta Catalog.

Airflow shows the DAGs again within a minute, paused. Unpause and trigger
`lakehouse_generate`, then `lakehouse_trino`, then `lakehouse_spark`.

## How it is checked

- `make test` runs the unit tests of the tool, the DAGs' helpers and the
  generator (`files/tests/test_lakehouse.py`; the generator's need numpy and
  pyarrow, which Airflow's image has).
- Every SQL file and both Spark jobs were run on a local Trino 481 and Spark
  4.1.2 with Iceberg 1.11 (the platform's versions), twice over the same
  generated data: they agree row for row and come out the same the second
  time.
- The model and `make dataplatform-tables` were run against a local Meta
  Catalog 2.1.1, with a stand-in provisioning script: from an empty catalog
  to the file, a second sync with nothing to do, a table removed and made
  again through the file.
