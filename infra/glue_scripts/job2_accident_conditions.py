"""
job2_accident_conditions.py — Aggregate accidents by environmental conditions.

Output table: accident_conditions
Key: (event_date, weather, light, road_surface, speed_limit)
Metrics: total_accidents, fatal, severity_sum
"""

import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_date, count, sum as spark_sum, when

from awsglue.utils import getResolvedOptions
from schemas import ACCIDENT_ENVELOPE
from common import get_kafka_options, merge_then_upsert

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("job2")

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
    .select("data.payload.*", "data.event_time")
)

parsed = (
    raw
    .withColumn("event_date", to_date(col("Date"), "dd/MM/yyyy"))
    .withColumn("weather", col("Weather_Conditions").cast("int"))
    .withColumn("light", col("Light_Conditions").cast("int"))
    .withColumn("road_surface", col("Road_Surface_Conditions").cast("int"))
    .withColumn("speed_limit", col("Speed_limit").cast("int"))
    .withColumn("severity_int", col("Accident_Severity").cast("int"))
    .withColumn("fatal", when(col("severity_int") == 1, 1).otherwise(0))
    .filter(col("event_date").isNotNull())
)


def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    agg = (
        batch_df
        .groupBy("event_date", "weather", "light", "road_surface", "speed_limit")
        .agg(
            count("*").alias("total_accidents"),
            spark_sum("fatal").alias("fatal"),
            spark_sum("severity_int").alias("severity_sum"),
        )
    )

    merge_then_upsert(
        batch_df=agg, batch_id=batch_id,
        table="accident_conditions",
        key_cols=["event_date", "weather", "light", "road_surface", "speed_limit"],
        value_cols=["total_accidents", "fatal", "severity_sum"],
        secret_name=args["pg_secret_name"], region=args["aws_region"],
    )


query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", args["checkpoint_path"])
    .trigger(processingTime="1 minute")
    .start()
)
query.awaitTermination()
