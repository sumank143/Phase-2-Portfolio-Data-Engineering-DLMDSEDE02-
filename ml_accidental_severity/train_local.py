"""
train_local.py — Train the severity classifier from local CSV files.

Use this to bootstrap the model before the Glue pipeline produces enough Parquet data.

Usage:
    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...
    python ml_accidental_severity/train_local.py
    docker compose restart ml-predict
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
log = logging.getLogger("train_local")

S3_BUCKET  = os.environ.get("S3_BUCKET", "accident-severity-data")
MODEL_KEY  = os.environ.get("MODEL_KEY", "models/severity_classifier.joblib")
REGION     = os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")

ACC_CSV = "data/Accident_Information.csv"
VEH_CSV = "data/Vehicle_Information.csv"

FEATURES = ["Age_Band_of_Driver", "Sex_of_Driver", "Vehicle_Type"]
TARGET   = "Accident_Severity"
UNKNOWN_VALUES = {"-1", "0", "", "Data missing or out of range", "Not known"}


def main():
    log.info("Reading %s ...", ACC_CSV)
    acc_df = pd.read_csv(ACC_CSV, usecols=["Accident_Index", "Accident_Severity"],
                         low_memory=False)
    log.info("Accident rows: %d", len(acc_df))

    log.info("Reading %s ...", VEH_CSV)
    veh_df = pd.read_csv(VEH_CSV, usecols=["Accident_Index"] + FEATURES,
                         low_memory=False)
    log.info("Vehicle rows: %d", len(veh_df))

    df = veh_df.merge(acc_df, on="Accident_Index", how="inner")
    log.info("Joined rows: %d", len(df))

    for feat in FEATURES:
        df = df[~df[feat].astype(str).isin(UNKNOWN_VALUES)]
    df = df[df[TARGET].notna()]
    df[TARGET] = df[TARGET].astype(int)
    log.info("Clean rows: %d", len(df))

    X = df[FEATURES]
    y = df[TARGET]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    pipeline = Pipeline([
        ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ("clf", LogisticRegression(class_weight="balanced", max_iter=500, random_state=42)),
    ])
    pipeline.fit(X_train, y_train)

    acc = accuracy_score(y_test, pipeline.predict(X_test))
    log.info("Test accuracy: %.4f", acc)

    feature_options = {}
    for feat in FEATURES:
        feature_options[feat] = sorted(df[feat].astype(str).unique().tolist())

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
