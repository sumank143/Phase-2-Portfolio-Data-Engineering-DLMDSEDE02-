#!/usr/bin/env python3
"""
Producer — streams two CSV datasets from S3 into Azure Event Hubs (Kafka surface).

Runs two child processes (one per stream) using multiprocessing. Each process:
  1. Opens an S3 object as a streaming HTTP body (iter_lines).
  2. Parses CSV rows as dicts, wraps each in a typed envelope.
  3. Produces to an Azure Event Hubs topic via the Kafka protocol.

Environment variables:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
  S3_BUCKET, S3_ACCIDENT_KEY, S3_VEHICLE_KEY
  KAFKA_BROKERS, KAFKA_SASL_PASSWORD
  KAFKA_TOPIC_ACCIDENTS, KAFKA_TOPIC_VEHICLES
  PRODUCER_RATE          — messages per second per stream (default 100)
  PRODUCER_MAX_RECORDS   — 0 = unlimited (default 0)
"""

import argparse
import csv
import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from multiprocessing import Process

import boto3
from kafka import KafkaProducer

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(processName)s] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────
S3_BUCKET          = os.environ.get("S3_BUCKET", "accident-severity-data")
S3_ACCIDENT_KEY    = os.environ.get("S3_ACCIDENT_KEY", "raw/Accident_Information.csv")
S3_VEHICLE_KEY     = os.environ.get("S3_VEHICLE_KEY", "raw/Vehicle_Information.csv")

KAFKA_BROKERS      = os.environ.get("KAFKA_BROKERS", "").split(",")
KAFKA_SASL_PASS    = os.environ.get("KAFKA_SASL_PASSWORD", "")
TOPIC_ACCIDENTS    = os.environ.get("KAFKA_TOPIC_ACCIDENTS", "accident-raw")
TOPIC_VEHICLES     = os.environ.get("KAFKA_TOPIC_VEHICLES", "vehicles-raw")

DEFAULT_RATE       = int(os.environ.get("PRODUCER_RATE", "100"))
DEFAULT_MAX        = int(os.environ.get("PRODUCER_MAX_RECORDS", "0"))


def _build_producer() -> KafkaProducer:
    """Create a KafkaProducer configured for Azure Event Hubs SASL_SSL."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",   # literal — Event Hubs requirement
        sasl_plain_password=KAFKA_SASL_PASS,
        api_version=(2, 0, 0),                      # Event Hubs rejects newer protocols
        acks="all",
        retries=5,
        linger_ms=50,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def _iter_s3_csv(bucket: str, key: str):
    """Yield rows from an S3 CSV object as dicts (streaming, no full-file load)."""
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]

    # Wrap the streaming body in a text-mode line iterator
    line_iter = io.TextIOWrapper(body, encoding="utf-8")
    reader = csv.DictReader(line_iter)
    for row in reader:
        yield row


def stream_one(
    stream_name: str,
    s3_key: str,
    topic: str,
    rate: int,
    max_records: int,
):
    """Stream a single CSV from S3 → Kafka at the configured rate."""
    log.info("Starting stream=%s  topic=%s  rate=%d msg/s  max=%s",
             stream_name, topic, rate, max_records or "unlimited")

    producer = _build_producer()
    delay = 1.0 / rate if rate > 0 else 0.0
    sent = 0

    try:
        for row in _iter_s3_csv(S3_BUCKET, s3_key):
            key = row.get("Accident_Index", "")
            envelope = {
                "seq": sent,
                "stream": stream_name,
                "event_time": _extract_event_time(row),
                "ingest_time": datetime.now(timezone.utc).isoformat(),
                "payload": row,
            }
            producer.send(topic, key=key, value=envelope)
            sent += 1

            if sent % 5000 == 0:
                log.info("[%s] Sent %d messages", stream_name, sent)
                producer.flush()

            if 0 < max_records <= sent:
                log.info("[%s] Reached max_records=%d, stopping.", stream_name, max_records)
                break

            if delay > 0:
                time.sleep(delay)

    except Exception:
        log.exception("[%s] Fatal error after %d messages", stream_name, sent)
        raise
    finally:
        producer.flush()
        producer.close()
        log.info("[%s] Finished. Total sent: %d", stream_name, sent)


def _extract_event_time(row: dict) -> str:
    """Build an ISO timestamp from Date + Time columns; fall back to ingest time."""
    date_str = row.get("Date", "")
    time_str = row.get("Time", "")
    if date_str and time_str:
        try:
            # Date is DD/MM/YYYY in the DfT dataset
            dt = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            pass
    return datetime.now(timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description="Stream CSVs from S3 to Kafka")
    parser.add_argument("--only", choices=["accidents", "vehicles"],
                        help="Stream only one dataset (default: both)")
    parser.add_argument("--rate", type=int, default=DEFAULT_RATE,
                        help=f"Messages per second per stream (default {DEFAULT_RATE})")
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX,
                        help=f"Max records per stream, 0=unlimited (default {DEFAULT_MAX})")
    args = parser.parse_args()

    streams = []

    if args.only != "vehicles":
        streams.append(
            Process(
                target=stream_one,
                name="accidents",
                args=("accidents", S3_ACCIDENT_KEY, TOPIC_ACCIDENTS,
                      args.rate, args.max_records),
            )
        )
    if args.only != "accidents":
        streams.append(
            Process(
                target=stream_one,
                name="vehicles",
                args=("vehicles", S3_VEHICLE_KEY, TOPIC_VEHICLES,
                      args.rate, args.max_records),
            )
        )

    for p in streams:
        p.start()
    for p in streams:
        p.join()

    # Exit non-zero if any child failed
    if any(p.exitcode != 0 for p in streams):
        log.error("One or more streams failed.")
        sys.exit(1)
    log.info("All streams completed successfully.")


if __name__ == "__main__":
    main()
