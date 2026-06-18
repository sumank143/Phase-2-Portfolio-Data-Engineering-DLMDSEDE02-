"""
ML Predict — Stateless FastAPI inference microservice.

Endpoints:
  GET  /health      — liveness check
  GET  /features    — dropdown options from the trained OHE categories
  POST /predict     — severity prediction for given driver/vehicle features
"""

from fastapi import FastAPI
from .predict import router

app = FastAPI(
    title="Accident Severity ML Predict",
    version="1.0.0",
    docs_url="/docs",
)

app.include_router(router)


@app.get("/health")
async def health():
    return {"status": "ok"}
