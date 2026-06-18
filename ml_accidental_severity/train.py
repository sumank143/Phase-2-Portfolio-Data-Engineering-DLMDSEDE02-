"""
train.py — Glue Python Shell job for ML severity classifier training.

Scheduled every 15 minutes by a Glue Trigger.
  1. Reads accident + vehicle Parquet from S3 (written by job5).
  2. Joins on Accident_Index in Pandas.
  3. Cleans missing/unknown feature values.
  4. Trains OneHotEncoder → LogisticRegression(class_weight="balanced").
  5. Serialises pipeline + metadata with joblib and writes to S3.
"""

import logging
import os
import tempfile

import boto3
import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import accuracy_score

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("train")

# ── Configuration ────────────────────────────────────────
S3_BUCKET    = os.environ.get("S3_BUCKET", "accident-severity-data")
ACC_PREFIX   = os.environ.get("ACC_PREFIX", "processed/accidents/")
VEH_PREFIX   = os.environ.get("VEH_PREFIX", "processed/vehicles/")
MODEL_KEY    = os.environ.get("MODEL_KEY", "models/severity_classifier.joblib")
REGION       = os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")

FEATURES = ["Age_Band_of_Driver", "Sex_of_Driver", "Vehicle_Type"]
TARGET   = "Accident_Severity"
UNKNOWN_VALUES = {"-1", "0", "", "Data missing or out of range", "Not known"}


def read_parquet_prefix(bucket: str, prefix: str) -> pd.DataFrame:
    """Read all Parquet files under an S3 prefix into a single DataFrame."""
    s3 = boto3.client("s3", region_name=REGION)
    paginator = s3.get_paginator("list_objects_v2")
    frames = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                s3.download_file(bucket, key, tmp.name)
                frames.append(pd.read_parquet(tmp.name))

    if not frames:
        raise ValueError(f"No parquet files found under s3://{bucket}/{prefix}")
    return pd.concat(frames, ignore_index=True)


def main():
    log.info("=== Reading accident parquet from s3://%s/%s ===", S3_BUCKET, ACC_PREFIX)
    acc_df = read_parquet_prefix(S3_BUCKET, ACC_PREFIX)
    log.info("Accident records: %d", len(acc_df))

    log.info("=== Reading vehicle parquet from s3://%s/%s ===", S3_BUCKET, VEH_PREFIX)
    veh_df = read_parquet_prefix(S3_BUCKET, VEH_PREFIX)
    log.info("Vehicle records: %d", len(veh_df))

    # ── Join ─────────────────────────────────────────────
    log.info("Joining on Accident_Index...")
    df = veh_df.merge(acc_df, on="Accident_Index", how="inner")
    log.info("Joined records: %d", len(df))

    # ── Clean ────────────────────────────────────────────
    for feat in FEATURES:
        df = df[~df[feat].astype(str).isin(UNKNOWN_VALUES)]
    df = df[df[TARGET].notna()]
    df[TARGET] = df[TARGET].astype(int)
    log.info("Clean records: %d", len(df))

    if len(df) < 100:
        log.warning("Insufficient data for training (%d rows). Skipping.", len(df))
        return

    # ── Train ────────────────────────────────────────────
    X = df[FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    pipeline = Pipeline([
        ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ("clf", LogisticRegression(
            class_weight="balanced", max_iter=500, random_state=42
        )),
    ])

    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    log.info("Test accuracy: %.4f", acc)

    # ── Collect feature options (for /features endpoint) ─
    feature_options = {}
    for feat in FEATURES:
        feature_options[feat] = sorted(df[feat].astype(str).unique().tolist())

    # ── Serialise and upload ─────────────────────────────
    artefact = {
        "pipeline": pipeline,
        "features": FEATURES,
        "feature_options": feature_options,
        "accuracy": acc,
        "n_train": len(X_train),
        "n_test": len(X_test),
    }

    with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as tmp:
        joblib.dump(artefact, tmp.name)
        s3 = boto3.client("s3", region_name=REGION)
        s3.upload_file(tmp.name, S3_BUCKET, MODEL_KEY)
        log.info("Model uploaded to s3://%s/%s", S3_BUCKET, MODEL_KEY)


if __name__ == "__main__":
    main()
