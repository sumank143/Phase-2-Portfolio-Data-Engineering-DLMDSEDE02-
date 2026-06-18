"""
job5_accident_vehicle_extract.py — Extract raw streams to S3 Parquet for ML training.

Reads from both accident-raw and vehicles-raw topics independently.
Writes:
  - s3://<bucket>/processed/accidents/  (Accident_Index + Accident_Severity)
  - s3://<bucket>/processed/vehicles/   (Accident_Index + driver/vehicle columns)

No aggregation, no Postgres write — pure extraction.
"""

import sys, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from awsglue.utils import getResolvedOptions
from schemas import ACCIDENT_ENVELOPE, VEHICLE_ENVELOPE
from common import get_kafka_options

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("job5")

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "kafka_secret_name",
    "checkpoint_path_accidents", "checkpoint_path_vehicles",
    "output_path_accidents", "output_path_vehicles",
    "aws_region",
])

spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Accident stream → Parquet ────────────────────────────
acc_opts = get_kafka_options("accident-raw", args["kafka_secret_name"], args["aws_region"])

acc_stream = (
    spark.readStream.format("kafka").options(**acc_opts).load()
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), ACCIDENT_ENVELOPE).alias("data"))
    .select(
        col("data.payload.Accident_Index").alias("Accident_Index"),
        col("data.payload.Accident_Severity").cast("int").alias("Accident_Severity"),
    )
    .filter(col("Accident_Index").isNotNull())
)

acc_query = (
    acc_stream.writeStream
    .format("parquet")
    .option("path", args["output_path_accidents"])
    .option("checkpointLocation", args["checkpoint_path_accidents"])
    .trigger(processingTime="1 minute")
    .start()
)

# ── Vehicle stream → Parquet ─────────────────────────────
veh_opts = get_kafka_options("vehicles-raw", args["kafka_secret_name"], args["aws_region"])

veh_stream = (
    spark.readStream.format("kafka").options(**veh_opts).load()
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), VEHICLE_ENVELOPE).alias("data"))
    .select(
        col("data.payload.Accident_Index").alias("Accident_Index"),
        col("data.payload.Age_Band_of_Driver").alias("Age_Band_of_Driver"),
        col("data.payload.Sex_of_Driver").alias("Sex_of_Driver"),
        col("data.payload.Vehicle_Type").alias("Vehicle_Type"),
    )
    .filter(col("Accident_Index").isNotNull())
)

veh_query = (
    veh_stream.writeStream
    .format("parquet")
    .option("path", args["output_path_vehicles"])
    .option("checkpointLocation", args["checkpoint_path_vehicles"])
    .trigger(processingTime="1 minute")
    .start()
)

# ── Await both ───────────────────────────────────────────
spark.streams.awaitAnyTermination()
