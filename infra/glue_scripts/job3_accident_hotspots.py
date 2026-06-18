"""
job3_accident_hotspots.py — Weighted severity scores by district and road type.

Output table: accident_hotspots
Key: (event_date, local_authority_district, road_type, urban_or_rural)
Metrics: total_accidents, weighted_severity (fatal*3 + serious*2 + slight*1)
"""

import sys, logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_date, count, sum as spark_sum, when
from awsglue.utils import getResolvedOptions
from schemas import ACCIDENT_ENVELOPE
from common import get_kafka_options, merge_then_upsert

logging.basicConfig(level=logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "kafka_secret_name", "pg_secret_name",
    "checkpoint_path", "aws_region",
])

spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_opts = get_kafka_options("accident-raw", args["kafka_secret_name"], args["aws_region"])

raw = (
    spark.readStream.format("kafka").options(**kafka_opts).load()
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), ACCIDENT_ENVELOPE).alias("data"))
    .select("data.payload.*")
)

parsed = (
    raw
    .withColumn("event_date", to_date(col("Date"), "dd/MM/yyyy"))
    .withColumn("local_authority_district", col("`Local_Authority_(District)`").cast("int"))
    .withColumn("road_type", col("Road_Type").cast("int"))
    .withColumn("urban_or_rural", col("Urban_or_Rural_Area").cast("int"))
    .withColumn("severity_int", col("Accident_Severity").cast("int"))
    .withColumn("weight",
        when(col("severity_int") == 1, 3)
        .when(col("severity_int") == 2, 2)
        .otherwise(1)
    )
    .filter(col("event_date").isNotNull())
)


def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    agg = (
        batch_df
        .groupBy("event_date", "local_authority_district", "road_type", "urban_or_rural")
        .agg(
            count("*").alias("total_accidents"),
            spark_sum("weight").alias("weighted_severity"),
        )
    )
    merge_then_upsert(
        agg, batch_id, "accident_hotspots",
        ["event_date", "local_authority_district", "road_type", "urban_or_rural"],
        ["total_accidents", "weighted_severity"],
        args["pg_secret_name"], args["aws_region"],
    )


query = (
    parsed.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", args["checkpoint_path"])
    .trigger(processingTime="1 minute").start()
)
query.awaitTermination()
