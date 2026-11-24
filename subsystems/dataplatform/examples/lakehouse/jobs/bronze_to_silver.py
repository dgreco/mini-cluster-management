"""bronze_to_silver.py: a silver table of the lakehouse example from its
bronze ones, with Spark (the DAG lakehouse_spark submits it in compute, as the
pipeline; the same as sql/silver/*.sql does with Trino).

  bronze_to_silver.py --target <domain>.silver.<name> --tables-file <file.json>
  bronze_to_silver.py --target <domain>.silver.<name> --tables '<json>'

The JSON names, for the target and every table it is built from, its Iceberg
table in the catalog lakehouse and its folder (the DAG puts it into the
job's ConfigMap: the operator passes arguments through a shell, unquoted):
{"sales.bronze.orders": {"name": "lakehouse.sales_bronze.orders_v1",
"location": "s3a://<bucket>/lakehouse/sales/bronze/orders_v1"}, ...}.

The bronze tables are plain parquet in their folders, which Spark reads with
S3A as the job's role (the template lakehouse-files gives the platform's
bucket web identity). A role reaches only the inside of a folder: the files
are listed (a LIST under the folder's prefix) and read by name, so that
nothing asks for the folder's own key, which the role may not read. The
silver tables are Iceberg, written through the catalog gate with data
changes only (MERGE, a whole overwrite): their schema is Meta Catalog's.

A Python error would still end the job with exit 0: the last line,
LAKEHOUSE_JOB_DONE, is what tells the DAG it went through.
"""
import argparse
import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

RATES = {"EUR": 1.0, "USD": 0.92, "GBP": 1.17, "CHF": 1.04}


def parquet_files(spark, location: str) -> list:
    """The parquet files directly in a folder (FileSystem.listStatus lists
    the prefix and never stats the folder itself)."""
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(location)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return sorted(s.getPath().toString() for s in fs.listStatus(path)
                  if s.isFile() and s.getPath().getName().endswith(".parquet"))


def bronze(spark, tables: dict, key: str):
    files = parquet_files(spark, tables[key]["location"])
    if not files:
        raise SystemExit(f"{key}: no parquet file in {tables[key]['location']} (run the DAG lakehouse_generate)")
    print(f"{key}: {len(files)} file(s)")
    return spark.read.parquet(*files)


def ntz(column: str):
    """A timestamp as the Iceberg tables hold it (timestamp(6) in Trino:
    without a time zone), whichever way Spark read the parquet one."""
    return F.col(column).cast("timestamp_ntz").alias(column)


def epoch(column: str):
    """Seconds since 1970 (a timestamp without a time zone has no cast to a
    number of its own)."""
    return F.col(column).cast("timestamp").cast("long")


def latest(df, key: str):
    """One row per key: the one loaded last."""
    w = Window.partitionBy(key).orderBy(F.col("ingested_at").desc())
    return df.withColumn("_latest", F.row_number().over(w)).where("_latest = 1").drop("_latest")


def sales_customers(spark, tables):
    src = latest(bronze(spark, tables, "sales.bronze.customers").where("customer_id IS NOT NULL"), "customer_id")
    src = src.select(
        "customer_id",
        F.trim("full_name").alias("full_name"),
        F.lower(F.trim("email")).alias("email"),
        F.split(F.lower(F.trim("email")), "@").getItem(1).alias("email_domain"),
        F.upper(F.trim("country")).alias("country"),
        F.lower("segment").alias("segment"),
        "signup_date")
    src.createOrReplaceTempView("src")
    target = tables["sales.silver.customers"]["name"]
    # a dimension: changed rows are rewritten, new ones added
    spark.sql(f"""
        MERGE INTO {target} t USING src s ON t.customer_id = s.customer_id
        WHEN MATCHED AND NOT (t.full_name <=> s.full_name AND t.email <=> s.email AND t.country <=> s.country
                              AND t.segment <=> s.segment AND t.signup_date <=> s.signup_date)
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
    return target


def sales_orders(spark, tables):
    df = bronze(spark, tables, "sales.bronze.orders").where(
        "customer_id IS NOT NULL AND amount > 0 AND currency IN ('EUR', 'USD', 'GBP', 'CHF')")
    rate = F.create_map(*[x for k, v in RATES.items() for x in (F.lit(k), F.lit(v))])
    out = latest(df, "order_id").select(
        "order_id", "customer_id",
        F.to_date("order_ts").alias("order_date"),
        ntz("order_ts"), "status", "currency",
        F.round("amount", 2).cast(DecimalType(12, 2)).alias("amount"),
        F.round(F.col("amount") * rate[F.col("currency")], 2).cast(DecimalType(12, 2)).alias("amount_eur"),
        "items", "channel")
    target = tables["sales.silver.orders"]["name"]
    out.writeTo(target).overwritePartitions()      # unpartitioned: the whole table, one commit
    return target


def logistics_shipments(spark, tables):
    s = latest(bronze(spark, tables, "logistics.bronze.shipments"), "shipment_id")
    c = latest(bronze(spark, tables, "logistics.bronze.carriers"), "carrier_code").select(
        "carrier_code", "carrier_name", "service_level", "promised_hours")
    seconds = epoch("delivered_ts") - epoch("shipped_ts")
    out = s.join(F.broadcast(c), "carrier_code", "left").select(
        "shipment_id", "order_id", "carrier_code", "carrier_name", "service_level", "warehouse",
        F.to_date("shipped_ts").alias("shipped_date"),
        ntz("shipped_ts"), ntz("delivered_ts"),
        F.when(F.col("delivered_ts").isNotNull(), seconds / 3600.0).alias("transit_hours"),
        "promised_hours",
        F.coalesce(F.col("delivered_ts").isNotNull() & (seconds <= F.col("promised_hours") * 3600), F.lit(False)).alias("on_time"),
        "status", "weight_kg")
    target = tables["logistics.silver.shipments"]["name"]
    out.writeTo(target).overwritePartitions()
    return target


STEPS = {"sales.silver.customers": sales_customers, "sales.silver.orders": sales_orders,
         "logistics.silver.shipments": logistics_shipments}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True, choices=sorted(STEPS))
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--tables-file", help="the JSON in a file (the DAG's way: /stackable/jobs/tables.json)")
    src.add_argument("--tables", help="the JSON itself (by hand)")
    a = ap.parse_args()
    if a.tables_file:
        with open(a.tables_file) as f:
            tables = json.load(f)
    else:
        tables = json.loads(a.tables)
    spark = SparkSession.builder.appName(f"lakehouse {a.target}").getOrCreate()
    target = STEPS[a.target](spark, tables)
    rows = spark.table(target).count()
    print("LAKEHOUSE_JOB_DONE " + json.dumps({"target": a.target, "table": target, "rows": rows}))


if __name__ == "__main__":
    main()
