"""
predict.py — Model loading, caching, and inference logic.
"""

import logging
import os
import tempfile
import time
from typing import Optional

import boto3
import joblib
import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger("predict")

router = APIRouter()

# ── Configuration ────────────────────────────────────────
MODEL_S3_BUCKET = os.environ.get("MODEL_S3_BUCKET", os.environ.get("S3_BUCKET", "accident-severity-data"))
MODEL_S3_KEY    = os.environ.get("MODEL_S3_KEY", "models/severity_classifier.joblib")
CACHE_TTL       = int(os.environ.get("MODEL_CACHE_TTL_SEC", "300"))
REGION          = os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")

# ── In-process model cache ───────────────────────────────
_cache: dict = {"artefact": None, "loaded_at": 0.0}


def _load_model() -> dict:
    """Fetch model from S3 if cache is stale."""
    now = time.time()
    if _cache["artefact"] and (now - _cache["loaded_at"]) < CACHE_TTL:
        return _cache["artefact"]

    log.info("Fetching model from s3://%s/%s", MODEL_S3_BUCKET, MODEL_S3_KEY)
    s3 = boto3.client("s3", region_name=REGION)

    with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as tmp:
        s3.download_file(MODEL_S3_BUCKET, MODEL_S3_KEY, tmp.name)
        artefact = joblib.load(tmp.name)

    _cache["artefact"] = artefact
    _cache["loaded_at"] = now
    log.info("Model loaded. Accuracy=%.4f, n_train=%d",
             artefact.get("accuracy", 0), artefact.get("n_train", 0))
    return artefact


# ── Request / Response models ────────────────────────────
class PredictRequest(BaseModel):
    age_band_of_driver: str
    sex_of_driver: str
    vehicle_type: str


class SeverityProb(BaseModel):
    severity: int
    label: str
    probability: float


class PredictResponse(BaseModel):
    predictions: list[SeverityProb]
    model_accuracy: Optional[float] = None


SEVERITY_LABELS = {1: "Fatal", 2: "Serious", 3: "Slight"}


# ── Endpoints ────────────────────────────────────────────
@router.get("/features")
async def get_features():
    """Return the feature options from the trained model's OHE categories."""
    try:
        artefact = _load_model()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Model not available: {e}")
    return artefact.get("feature_options", {})


@router.post("/predict", response_model=PredictResponse)
async def predict(req: PredictRequest):
    """Run severity prediction for the given features."""
    try:
        artefact = _load_model()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Model not available: {e}")

    pipeline = artefact["pipeline"]
    features = artefact["features"]

    input_df = pd.DataFrame([{
        "Age_Band_of_Driver": req.age_band_of_driver,
        "Sex_of_Driver": req.sex_of_driver,
        "Vehicle_Type": req.vehicle_type,
    }], columns=features)

    probas = pipeline.predict_proba(input_df)[0]
    classes = pipeline.classes_

    predictions = sorted(
        [
            SeverityProb(
                severity=int(cls),
                label=SEVERITY_LABELS.get(int(cls), f"Unknown ({cls})"),
                probability=round(float(prob), 4),
            )
            for cls, prob in zip(classes, probas)
        ],
        key=lambda x: x.probability,
        reverse=True,
    )

    return PredictResponse(
        predictions=predictions,
        model_accuracy=artefact.get("accuracy"),
    )
