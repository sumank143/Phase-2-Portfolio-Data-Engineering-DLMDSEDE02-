"""
schemas.py — Kafka message schema definitions shared across all Glue streaming jobs.
Uploaded to S3 as --extra-py-files.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

# ── Accident envelope schema (from producer) ─────────────
ACCIDENT_ENVELOPE = StructType([
    StructField("seq", IntegerType()),
    StructField("stream", StringType()),
    StructField("event_time", StringType()),
    StructField("ingest_time", StringType()),
    StructField("payload", StructType([
        StructField("Accident_Index", StringType()),
        StructField("Accident_Severity", StringType()),
        StructField("Number_of_Vehicles", StringType()),
        StructField("Number_of_Casualties", StringType()),
        StructField("Date", StringType()),
        StructField("Time", StringType()),
        StructField("Latitude", StringType()),
        StructField("Longitude", StringType()),
        StructField("Light_Conditions", StringType()),
        StructField("Weather_Conditions", StringType()),
        StructField("Road_Surface_Conditions", StringType()),
        StructField("Road_Type", StringType()),
        StructField("Speed_limit", StringType()),
        StructField("Urban_or_Rural_Area", StringType()),
        StructField("Local_Authority_(District)", StringType()),
    ])),
])

# ── Vehicle envelope schema (from producer) ──────────────
VEHICLE_ENVELOPE = StructType([
    StructField("seq", IntegerType()),
    StructField("stream", StringType()),
    StructField("event_time", StringType()),
    StructField("ingest_time", StringType()),
    StructField("payload", StructType([
        StructField("Accident_Index", StringType()),
        StructField("Age_Band_of_Driver", StringType()),
        StructField("Sex_of_Driver", StringType()),
        StructField("Vehicle_Type", StringType()),
        StructField("Age_of_Vehicle", StringType()),
        StructField("Engine_Capacity_(CC)", StringType()),
        StructField("make", StringType()),
        StructField("model", StringType()),
        StructField("Year", StringType()),
    ])),
])
