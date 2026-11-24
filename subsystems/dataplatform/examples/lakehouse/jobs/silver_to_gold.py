"""silver_to_gold.py: a gold table of the lakehouse example from silver ones,
with Spark (the DAG lakehouse_spark submits it in compute, as the pipeline;
the same as sql/gold/*.sql does with Trino).

  silver_to_gold.py --target <domain>.gold.<name> --tables-file <file.json>

The tables' JSON as for bronze_to_silver.py. Every table read and written is
Iceberg, through the catalog lakehouse (the template lakehouse: the Hive
Metastore's REST catalog behind the catalog gate, which shows the job what
the pipeline may read and lets it commit data to what it may write). A gold
table is rebuilt whole in one commit.
"""
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

PAID = ("paid", "shipped", "delivered")


def sales_daily_sales(spark, t):
    o = spark.table(t["sales.silver.orders"]["name"]).where(F.col("status").isin(*PAID))
    c = spark.table(t["sales.silver.customers"]["name"]).select("customer_id", "country")
    out = (o.join(c, "customer_id", "left")
           .withColumn("country", F.coalesce("country", F.lit("unknown")))
           .groupBy("order_date", "country", "channel")
           .agg(F.count(F.lit(1)).alias("orders"),
                F.countDistinct("customer_id").alias("customers"),
                F.sum("items").cast("long").alias("items"),
                F.sum("amount_eur").cast(DecimalType(18, 2)).alias("revenue_eur"),
                F.avg("amount_eur").cast(DecimalType(12, 2)).alias("avg_order_eur")))
    return out, "sales.gold.daily_sales"


def sales_customer_value(spark, t):
    o = spark.table(t["sales.silver.orders"]["name"]).where(F.col("status").isin(*PAID))
    c = spark.table(t["sales.silver.customers"]["name"]).select("customer_id", "country", "segment")
    v = o.groupBy("customer_id").agg(F.min("order_date").alias("first_order_date"),
                                     F.max("order_date").alias("last_order_date"),
                                     F.count(F.lit(1)).alias("orders"),
                                     F.sum("amount_eur").alias("revenue"))
    band = (F.when(F.col("revenue") >= 2500, "top").when(F.col("revenue") >= 1500, "high")
            .when(F.col("revenue") >= 700, "mid").otherwise("low"))
    out = v.join(c, "customer_id", "left").select(
        "customer_id", "country", "segment", "first_order_date", "last_order_date", "orders",
        F.col("revenue").cast(DecimalType(18, 2)).alias("revenue_eur"), band.alias("value_band"))
    return out, "sales.gold.customer_value"


def logistics_delivery_performance(spark, t):
    s = spark.table(t["logistics.silver.shipments"]["name"])
    o = spark.table(t["sales.silver.orders"]["name"]).select("order_id", "customer_id")
    c = spark.table(t["sales.silver.customers"]["name"]).select("customer_id", F.col("country").alias("destination_country"))
    j = s.join(o, "order_id", "left").join(c, "customer_id", "left")
    out = (j.select(
               "shipped_date",
               F.coalesce("carrier_name", F.lit("unknown")).alias("carrier_name"),
               F.coalesce("service_level", F.lit("unknown")).alias("service_level"),
               F.coalesce("destination_country", F.lit("unknown")).alias("destination_country"),
               "delivered_ts", "on_time", "transit_hours")
           .groupBy("shipped_date", "carrier_name", "service_level", "destination_country")
           .agg(F.count(F.lit(1)).alias("shipments"),
                F.count("delivered_ts").alias("delivered"),
                F.sum(F.col("on_time").cast("long")).alias("on_time"),
                F.avg("transit_hours").alias("avg_transit_hours"),
                F.percentile_approx("transit_hours", 0.95).alias("p95_transit_hours"))
           .withColumn("on_time_rate", F.col("on_time") / F.when(F.col("delivered") > 0, F.col("delivered")))
           .select("shipped_date", "carrier_name", "service_level", "destination_country", "shipments",
                   "delivered", "on_time", "on_time_rate", "avg_transit_hours", "p95_transit_hours"))
    return out, "logistics.gold.delivery_performance"


STEPS = {"sales.gold.daily_sales": sales_daily_sales, "sales.gold.customer_value": sales_customer_value,
         "logistics.gold.delivery_performance": logistics_delivery_performance}


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
    out, key = STEPS[a.target](spark, tables)
    target = tables[key]["name"]
    out.writeTo(target).overwritePartitions()      # unpartitioned: the whole table, one commit
    rows = spark.table(target).count()
    print("LAKEHOUSE_JOB_DONE " + json.dumps({"target": a.target, "table": target, "rows": rows}))


if __name__ == "__main__":
    main()
