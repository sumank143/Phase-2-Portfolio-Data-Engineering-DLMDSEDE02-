"""
common.py — Shared helpers for all Glue streaming jobs.
Uploaded to S3 as --extra-py-files.

Provides:
  - get_secret()           : fetch a secret from AWS Secrets Manager
  - get_kafka_options()    : build Kafka readStream options for Event Hubs
  - get_pg_connection()    : return a psycopg2 connection to RDS
  - merge_then_upsert()    : READ-MERGE-WRITE UPSERT pattern for foreachBatch
"""

import json
import logging
import os

import boto3
import psycopg2

log = logging.getLogger(__name__)

# ── Secrets ──────────────────────────────────────────────

_secret_cache: dict = {}


def get_secret(secret_name: str, region: str = None) -> dict:
    """Retrieve a JSON secret from AWS Secrets Manager (cached in-process)."""
    if secret_name in _secret_cache:
        return _secret_cache[secret_name]

    region = region or os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(resp["SecretString"])
    _secret_cache[secret_name] = secret
    return secret


# ── Kafka / Event Hubs ───────────────────────────────────

def get_kafka_options(topic: str, secret_name: str, region: str = None) -> dict:
    """Build the readStream options dict for Kafka on Azure Event Hubs."""
    secret = get_secret(secret_name, region)
    conn_str = secret.get("connection_string", secret.get("password", ""))
    brokers  = secret.get("brokers", os.environ.get("KAFKA_BROKERS", ""))

    jaas = (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn_str}";'
    )
    return {
        "kafka.bootstrap.servers": brokers,
        "subscribe": topic,
        "startingOffsets": "latest",
        "maxOffsetsPerTrigger": "10000",
        "failOnDataLoss": "false",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "kafka.request.timeout.ms": "60000",
        "kafka.session.timeout.ms": "60000",
    }


# ── PostgreSQL ───────────────────────────────────────────

def get_pg_connection(secret_name: str, region: str = None):
    """Return a psycopg2 connection to RDS Postgres using Secrets Manager creds."""
    secret = get_secret(secret_name, region)
    return psycopg2.connect(
        host=secret["host"],
        port=int(secret.get("port", 5432)),
        dbname=secret.get("dbname", "accidentdb"),
        user=secret["username"],
        password=secret["password"],
        sslmode="require",
    )


# ── UPSERT ───────────────────────────────────────────────

def merge_then_upsert(
    batch_df,
    batch_id: int,
    table: str,
    key_cols: list,
    value_cols: list,
    secret_name: str,
    region: str = None,
):
    """
    READ-MERGE-WRITE UPSERT pattern.

    1. Collect aggregated rows from the Spark micro-batch.
    2. Read matching rows from Postgres (chunked IN-clause).
    3. Merge counts: merged = batch + existing.
    4. INSERT ... ON CONFLICT DO UPDATE.
    """
    rows = batch_df.collect()
    if not rows:
        log.info("Batch %d: empty, skipping.", batch_id)
        return

    log.info("Batch %d: %d aggregated rows to upsert into %s", batch_id, len(rows), table)
    conn = get_pg_connection(secret_name, region)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # ── Step 1: Build lookup of existing rows ────────
        existing = {}
        # Chunk into groups of 500 to avoid SQL length limits
        for i in range(0, len(rows), 500):
            chunk = rows[i : i + 500]
            where_clauses = []
            params = []
            for r in chunk:
                conds = " AND ".join(f"{k} = %s" for k in key_cols)
                where_clauses.append(f"({conds})")
                for k in key_cols:
                    params.append(r[k])

            if not where_clauses:
                continue

            sql = f"SELECT * FROM {table} WHERE {' OR '.join(where_clauses)}"
            cur.execute(sql, params)
            col_names = [desc[0] for desc in cur.description]
            for db_row in cur.fetchall():
                db_dict = dict(zip(col_names, db_row))
                key = tuple(str(db_dict[k]) for k in key_cols)
                existing[key] = db_dict

        # ── Step 2: Merge and upsert ────────────────────
        all_cols = key_cols + value_cols
        placeholders = ", ".join(["%s"] * len(all_cols))
        col_list = ", ".join(all_cols)
        conflict_cols = ", ".join(key_cols)
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in value_cols)

        upsert_sql = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
        )

        for r in rows:
            key = tuple(str(r[k]) for k in key_cols)
            merged_vals = []
            for c in all_cols:
                batch_val = r[c]
                if c in value_cols and key in existing:
                    old_val = existing[key].get(c, 0)
                    # Additive merge for numeric columns
                    if isinstance(batch_val, (int, float)) and isinstance(old_val, (int, float)):
                        batch_val = batch_val + old_val
                merged_vals.append(batch_val)

            cur.execute(upsert_sql, merged_vals)

        conn.commit()
        log.info("Batch %d: upserted %d rows into %s", batch_id, len(rows), table)

    except Exception:
        conn.rollback()
        log.exception("Batch %d: upsert failed for %s", batch_id, table)
        raise
    finally:
        cur.close()
        conn.close()
