"""
Dashboard — FastAPI service for data delivery.

Endpoints:
  GET  /              — Single-page HTML dashboard
  GET  /health        — Liveness check
  GET  /api/kpi-geo   — KPI by date and geo grid
  GET  /api/conditions— Accidents by weather/light/road
  GET  /api/hotspots  — Weighted severity by district
  GET  /api/vehicle-profile — Driver/vehicle demographics
  GET  /api/top-weather     — Top weather conditions
  GET  /api/top-districts   — Top districts by severity
  POST /api/predict         — Reverse proxy to ml-predict
  GET  /api/predict/features— Reverse proxy to ml-predict
  WS   /ws                  — Live push every 10 seconds
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

log = logging.getLogger("dashboard")
logging.basicConfig(level=logging.INFO)

# ── Configuration ────────────────────────────────────────
DB_HOST     = os.environ.get("DB_HOST", "localhost")
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))
DB_NAME     = os.environ.get("DB_NAME", "accidentdb")
DB_USER     = os.environ.get("DB_USER", "postgres")
DB_PASS     = os.environ.get("DB_PASS", "")
ML_URL      = os.environ.get("ML_PREDICT_URL", "http://ml-predict:8001")
WS_INTERVAL = int(os.environ.get("WS_PUSH_INTERVAL_SEC", "10"))

# ── Global state ─────────────────────────────────────────
pool: Optional[asyncpg.Pool] = None
ws_clients: set[WebSocket] = set()


# ── Lifespan ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    dsn = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    pool = await asyncpg.create_pool(dsn=dsn, min_size=2, max_size=10)
    log.info("Database pool created.")

    # Start WebSocket broadcaster
    task = asyncio.create_task(_ws_broadcaster())
    yield
    task.cancel()
    await pool.close()


app = FastAPI(title="Accident Severity Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


# ── Health ───────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}


# ── Dashboard HTML ───────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html", "r") as f:
        return HTMLResponse(content=f.read())


# ── REST API Endpoints ───────────────────────────────────
@app.get("/api/kpi-geo")
async def api_kpi_geo():
    rows = await pool.fetch(
        "SELECT * FROM accident_kpi_geo ORDER BY event_date DESC LIMIT 500"
    )
    return [dict(r) for r in rows]


@app.get("/api/conditions")
async def api_conditions():
    rows = await pool.fetch(
        "SELECT * FROM accident_conditions ORDER BY total_accidents DESC LIMIT 200"
    )
    return [dict(r) for r in rows]


@app.get("/api/hotspots")
async def api_hotspots():
    rows = await pool.fetch(
        "SELECT * FROM accident_hotspots ORDER BY weighted_severity DESC LIMIT 100"
    )
    return [dict(r) for r in rows]


@app.get("/api/vehicle-profile")
async def api_vehicle_profile():
    rows = await pool.fetch(
        "SELECT * FROM vehicle_profile ORDER BY vehicle_count DESC LIMIT 200"
    )
    return [dict(r) for r in rows]


@app.get("/api/top-weather")
async def api_top_weather():
    rows = await pool.fetch("""
        SELECT weather, SUM(total_accidents) as total
        FROM accident_conditions
        GROUP BY weather
        ORDER BY total DESC
        LIMIT 10
    """)
    return [dict(r) for r in rows]


@app.get("/api/top-districts")
async def api_top_districts():
    rows = await pool.fetch("""
        SELECT local_authority_district, SUM(weighted_severity) as total_severity
        FROM accident_hotspots
        GROUP BY local_authority_district
        ORDER BY total_severity DESC
        LIMIT 10
    """)
    return [dict(r) for r in rows]


# ── ML Predict Reverse Proxy ─────────────────────────────
@app.post("/api/predict")
async def api_predict(request: Request):
    body = await request.json()
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{ML_URL}/predict", json=body, timeout=10.0)
    return JSONResponse(content=resp.json(), status_code=resp.status_code)


@app.get("/api/predict/features")
async def api_predict_features():
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{ML_URL}/features", timeout=10.0)
    return JSONResponse(content=resp.json(), status_code=resp.status_code)


# ── WebSocket ────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    log.info("WebSocket client connected. Total: %d", len(ws_clients))
    try:
        while True:
            # Keep connection alive; client may send pings
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_clients.discard(ws)
        log.info("WebSocket client disconnected. Total: %d", len(ws_clients))


async def _ws_broadcaster():
    """Background task: push dashboard snapshot to all WS clients every N seconds."""
    while True:
        await asyncio.sleep(WS_INTERVAL)
        if not ws_clients or pool is None:
            continue

        try:
            snapshot = await _build_snapshot()
            payload = json.dumps(snapshot, default=str)

            disconnected = set()
            for ws in ws_clients:
                try:
                    await ws.send_text(payload)
                except Exception:
                    disconnected.add(ws)
            ws_clients.difference_update(disconnected)

        except Exception:
            log.exception("WebSocket broadcast error")


async def _build_snapshot() -> dict:
    """Build a dashboard snapshot from the four aggregation tables."""
    stats = await pool.fetchrow("""
        SELECT
            COALESCE(SUM(total_accidents), 0) AS total_accidents,
            COALESCE(SUM(fatal), 0) AS total_fatal,
            COALESCE(SUM(serious), 0) AS total_serious,
            COALESCE(SUM(slight), 0) AS total_slight,
            COALESCE(SUM(total_casualties), 0) AS total_casualties
        FROM accident_kpi_geo
    """)

    recent_geo = await pool.fetch("""
        SELECT event_date, lat_grid, lon_grid, total_accidents, fatal
        FROM accident_kpi_geo
        ORDER BY event_date DESC
        LIMIT 50
    """)

    top_districts = await pool.fetch("""
        SELECT local_authority_district, SUM(weighted_severity) AS severity
        FROM accident_hotspots
        GROUP BY local_authority_district
        ORDER BY severity DESC
        LIMIT 5
    """)

    top_weather = await pool.fetch("""
        SELECT weather, SUM(total_accidents) AS total
        FROM accident_conditions
        GROUP BY weather
        ORDER BY total DESC
        LIMIT 5
    """)

    age_bands = await pool.fetch("""
        SELECT age_band_of_driver, SUM(vehicle_count) AS total
        FROM vehicle_profile
        GROUP BY age_band_of_driver
        ORDER BY total DESC
        LIMIT 10
    """)

    return {
        "stats": dict(stats) if stats else {},
        "recent_geo": [dict(r) for r in recent_geo],
        "top_districts": [dict(r) for r in top_districts],
        "top_weather": [dict(r) for r in top_weather],
        "age_bands": [dict(r) for r in age_bands],
    }
