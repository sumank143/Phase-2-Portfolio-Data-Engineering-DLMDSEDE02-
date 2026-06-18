"""
job1_accident_kpi_geo.py — Aggregate accident KPIs by date and geographic grid.

Output table: accident_kpi_geo
Key: (event_date, lat_grid, lon_grid)
Metrics: total_accidents, fatal, serious, slight, total_casualties, total_vehicles
"""

import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_date, round as spark_round,
    count, sum as spark_sum, when, lit
)

from awsglue.utils import getResolvedOptions

from schemas import ACCIDENT_ENVELOPE
from common import get_kafka_options, merge_then_upsert

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("job1")

# ── Glue arguments ───────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "kafka_secret_name",
    "pg_secret_name",
    "checkpoint_path",
    "aws_region",
])

spark = (
    SparkSession.builder
    .appName(args["JOB_NAME"])
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Read stream ──────────────────────────────────────────
kafka_opts = get_kafka_options(
    topic="accident-raw",
    secret_name=args["kafka_secret_name"],
    region=args["aws_region"],
)

raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_opts)
    .load()
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), ACCIDENT_ENVELOPE).alias("data"))
    .select("data.payload.*", "data.event_time")
)

# ── Parse and derive columns ─────────────────────────────
parsed = (
    raw
    .withColumn("event_date", to_date(col("Date"), "dd/MM/yyyy"))
    .withColumn("lat_grid", spark_round(col("Latitude").cast("double"), 1))
    .withColumn("lon_grid", spark_round(col("Longitude").cast("double"), 1))
    .withColumn("severity_int", col("Accident_Severity").cast("int"))
    .withColumn("fatal",   when(col("severity_int") == 1, 1).otherwise(0))
    .withColumn("serious", when(col("severity_int") == 2, 1).otherwise(0))
    .withColumn("slight",  when(col("severity_int") == 3, 1).otherwise(0))
    .withColumn("casualties", col("Number_of_Casualties").cast("int"))
    .withColumn("vehicles",   col("Number_of_Vehicles").cast("int"))
    .filter(col("event_date").isNotNull() & col("lat_grid").isNotNull())
)


# ── foreachBatch handler ─────────────────────────────────
def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    agg = (
        batch_df
        .groupBy("event_date", "lat_grid", "lon_grid")
        .agg(
            count("*").alias("total_accidents"),
            spark_sum("fatal").alias("fatal"),
            spark_sum("serious").alias("serious"),
            spark_sum("slight").alias("slight"),
            spark_sum("casualties").alias("total_casualties"),
            spark_sum("vehicles").alias("total_vehicles"),
        )
    )

    merge_then_upsert(
        batch_df=agg,
        batch_id=batch_id,
        table="accident_kpi_geo",
        key_cols=["event_date", "lat_grid", "lon_grid"],
        value_cols=["total_accidents", "fatal", "serious", "slight",
                    "total_casualties", "total_vehicles"],
        secret_name=args["pg_secret_name"],
        region=args["aws_region"],
    )


# ── Start streaming query ────────────────────────────────
query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", args["checkpoint_path"])
    .trigger(processingTime="1 minute")
    .start()
)

query.awaitTermination()
