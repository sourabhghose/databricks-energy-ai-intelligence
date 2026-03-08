# ============================================================
# AUS Energy Copilot — FastAPI Backend (Databricks Apps)
# ============================================================
# Thin entry point: FastAPI init, middleware, router includes,
# and SPA static serving. All route logic lives in routers/*.
#
# Run locally:  cd app && uvicorn main:app --reload --port 8000
# ============================================================

from __future__ import annotations

import os
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from routers.shared import (
    ALLOW_ORIGINS,
    RATE_LIMIT_REQUESTS,
    RATE_LIMIT_WINDOW_SECONDS,
    _rate_limit_store,
    _get_last_source,
    _get_last_elapsed_ms,
    logger,
)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="AUS Energy Copilot API",
    description=(
        "## AUS Energy Copilot — NEM Market Intelligence API\n\n"
        "Real-time Australian electricity market data, ML forecasts, and AI-powered analytics.\n\n"
        "This is the slim deployment build for Databricks Apps."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {"name": "Health", "description": "Service health and readiness checks"},
        {"name": "Market Data", "description": "Real-time NEM prices, generation, and interconnector data"},
        {"name": "Grid Operations", "description": "Power system security, frequency, and inertia data"},
        {"name": "Renewable Energy", "description": "Curtailment, DER, and sustainability analytics"},
        {"name": "Battery Storage", "description": "Battery economics and arbitrage analytics"},
        {"name": "Gas Market", "description": "Eastern Australian gas market data"},
        {"name": "Retail Market", "description": "Retail competition and customer analytics"},
        {"name": "Demand Response", "description": "RERT contracts and demand response activations"},
        {"name": "Carbon & Emissions", "description": "NEM carbon intensity and emissions tracking"},
        {"name": "Cyber Security", "description": "Energy sector cyber security analytics"},
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Data-Source", "X-Query-Ms", "X-Request-ID"],
)

# ---------------------------------------------------------------------------
# Middleware — rate limiting
# ---------------------------------------------------------------------------

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"
    now = time.monotonic()
    window_start = now - RATE_LIMIT_WINDOW_SECONDS
    _rate_limit_store[client_ip] = [t for t in _rate_limit_store[client_ip] if t > window_start]
    if len(_rate_limit_store[client_ip]) >= RATE_LIMIT_REQUESTS:
        return JSONResponse(
            status_code=429,
            content={"error": "rate_limit_exceeded", "retry_after_seconds": RATE_LIMIT_WINDOW_SECONDS},
        )
    _rate_limit_store[client_ip].append(now)
    return await call_next(request)

# ---------------------------------------------------------------------------
# Middleware — structured request logging + request ID
# ---------------------------------------------------------------------------

@app.middleware("http")
async def _logging_middleware(request: Request, call_next):
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    t0 = time.monotonic()
    response = await call_next(request)
    duration_ms = round((time.monotonic() - t0) * 1000, 1)
    logger.info(
        "http_request",
        extra={
            "request_id":  request_id,
            "method":      request.method,
            "path":        request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
        },
    )
    response.headers["X-Request-ID"] = request_id
    # Attach data source info so callers can see which backend served the request
    source = _get_last_source()
    if source != "unknown":
        response.headers["X-Data-Source"] = source
        response.headers["X-Query-Ms"] = str(round(_get_last_elapsed_ms(), 1))
    return response

# ---------------------------------------------------------------------------
# Global exception handler
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def _global_exc_handler(request: Request, exc: Exception):
    logger.exception("unhandled_exception", extra={"path": request.url.path})
    return JSONResponse(status_code=500, content={"error": "internal_server_error"})


# ---------------------------------------------------------------------------
# Include all routers (order matters — catch-alls must be last)
# ---------------------------------------------------------------------------
from routers.health import router as health_router  # noqa: E402
from routers.dashboards import router as dashboards_router  # noqa: E402
from routers.home import router as home_router  # noqa: E402
from routers.sidebar import router as sidebar_router  # noqa: E402
from routers.copilot import router as copilot_router  # noqa: E402
from routers.stubs import router as stubs_router  # noqa: E402
from routers.market_events import router as market_events_router  # noqa: E402
from routers.spike_analysis import router as spike_analysis_router  # noqa: E402
from routers.genie import router as genie_router  # noqa: E402
from routers.batch_forecasting import router as batch_forecasting_router  # noqa: E402
from routers.batch_futures_hedging import router as batch_futures_hedging_router  # noqa: E402
from routers.batch_bidding import router as batch_bidding_router  # noqa: E402
from routers.alerts import router as anomaly_router  # noqa: E402
from routers.market_briefs import router as market_briefs_router  # noqa: E402
from routers.replay import router as replay_router  # noqa: E402
from routers.auto_stubs import router as auto_stubs_router  # noqa: E402
from routers.deals import router as deals_router  # noqa: E402
from routers.curves import router as curves_router  # noqa: E402
from routers.risk import router as risk_router
from routers.constraints import router as constraints_router  # noqa: E402  # noqa: E402

app.include_router(health_router)
app.include_router(dashboards_router)
app.include_router(home_router)
app.include_router(sidebar_router)
app.include_router(copilot_router)
app.include_router(stubs_router)
app.include_router(market_events_router)
app.include_router(spike_analysis_router)
app.include_router(genie_router)
app.include_router(batch_forecasting_router)
app.include_router(batch_futures_hedging_router)
app.include_router(batch_bidding_router)
app.include_router(anomaly_router)
app.include_router(market_briefs_router)
app.include_router(replay_router)
app.include_router(deals_router)
app.include_router(curves_router)
app.include_router(risk_router)
app.include_router(constraints_router)
app.include_router(auto_stubs_router)

# ---------------------------------------------------------------------------
# API 404 catch-all (must be BEFORE the SPA catch-all)
# ---------------------------------------------------------------------------

@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def api_fallback(path: str):
    """Return 501 for any unregistered /api/* route."""
    label = path.replace("/", " ").replace("-", " ").replace("_", " ").title()
    return JSONResponse(
        status_code=501,
        content={
            "error": f"Endpoint /api/{path} is not yet implemented",
            "message": f"{label} data is coming soon. This endpoint has not been wired up yet.",
        },
    )

# ---------------------------------------------------------------------------
# STATIC FRONTEND SERVING (Databricks Apps deployment)
# ---------------------------------------------------------------------------

from fastapi.staticfiles import StaticFiles  # noqa: E402
from fastapi.responses import FileResponse    # noqa: E402

_frontend_dist = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.isdir(_frontend_dist):
    # Serve static assets (JS, CSS, images)
    _assets_dir = os.path.join(_frontend_dist, "assets")
    if os.path.isdir(_assets_dir):
        app.mount("/assets", StaticFiles(directory=_assets_dir), name="static-assets")

    # Catch-all: serve index.html for any non-API route (SPA routing)
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve React SPA for all non-API routes."""
        if full_path.startswith("api/"):
            return JSONResponse(status_code=404, content={"error": "Not found"})
        index = os.path.join(_frontend_dist, "index.html")
        if os.path.exists(index):
            return FileResponse(index)
        return {"error": "Frontend not built. Run npm run build in app/frontend/"}
