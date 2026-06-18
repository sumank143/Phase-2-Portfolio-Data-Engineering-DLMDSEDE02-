"""
job4_vehicle_profile.py — Aggregate vehicle/driver demographics by year.

Output table: vehicle_profile
Key: (year, age_band_of_driver, sex_of_driver, vehicle_type)
Metrics: vehicle_count, age_of_vehicle_sum
"""

import sys, logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, count, sum as spark_sum, coalesce, lit
from awsglue.utils import getResolvedOptions
from schemas import VEHICLE_ENVELOPE
from common import get_kafka_options, merge_then_upsert

logging.basicConfig(level=logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "kafka_secret_name", "pg_secret_name",
    "checkpoint_path", "aws_region",
])

spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_opts = get_kafka_options("vehicles-raw", args["kafka_secret_name"], args["aws_region"])

raw = (
    spark.readStream.format("kafka").options(**kafka_opts).load()
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), VEHICLE_ENVELOPE).alias("data"))
    .select("data.payload.*")
)

parsed = (
    raw
    .withColumn("year", col("Year").cast("int"))
    .withColumn("age_band_of_driver", col("Age_Band_of_Driver"))
    .withColumn("sex_of_driver", col("Sex_of_Driver"))
    .withColumn("vehicle_type", col("Vehicle_Type"))
    .withColumn("age_of_vehicle", coalesce(col("Age_of_Vehicle").cast("int"), lit(0)))
    .filter(col("year").isNotNull())
)


def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    agg = (
        batch_df
        .groupBy("year", "age_band_of_driver", "sex_of_driver", "vehicle_type")
        .agg(
            count("*").alias("vehicle_count"),
            spark_sum("age_of_vehicle").alias("age_of_vehicle_sum"),
        )
    )
    merge_then_upsert(
        agg, batch_id, "vehicle_profile",
        ["year", "age_band_of_driver", "sex_of_driver", "vehicle_type"],
        ["vehicle_count", "age_of_vehicle_sum"],
        args["pg_secret_name"], args["aws_region"],
    )


query = (
    parsed.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", args["checkpoint_path"])
    .trigger(processingTime="1 minute").start()
)
query.awaitTermination()
