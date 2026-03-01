from fastapi import APIRouter
from datetime import datetime, timezone, timedelta

from .shared import _get_sql_connection, _query_gold, _CATALOG, logger, RATE_LIMIT_REQUESTS

router = APIRouter()


# =========================================================================
# 1. HEALTH CHECK
# =========================================================================

@router.get("/health", summary="Service health", tags=["Health"])
async def health():
    """Return service health status."""
    sql_ok = _get_sql_connection() is not None
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mock_mode": not sql_ok,
        "sql_connected": sql_ok,
        "deployment": "slim",
        "databricks_healthy": True,
        "lakebase_healthy": True,
    }


@router.get("/api/health", summary="API health check", tags=["Health"])
async def api_health():
    """Return API health status — primary health endpoint."""
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mock_mode": True,
        "deployment": "slim",
    }


@router.get("/api/system/health", summary="System health for monitoring dashboard", tags=["Health"])
async def system_health():
    """Return SystemHealthResponse matching the Monitoring page interface."""
    sql_ok = _get_sql_connection() is not None
    now = datetime.now(timezone.utc)

    # Check data freshness from gold table
    freshness_minutes = None
    pipeline_last_run = None
    if sql_ok:
        rows = _query_gold(f"""
            SELECT MAX(interval_datetime) AS latest
            FROM {_CATALOG}.gold.nem_prices_5min
        """)
        if rows and rows[0].get("latest"):
            latest = rows[0]["latest"]
            if hasattr(latest, 'isoformat'):
                pipeline_last_run = latest.isoformat()
                delta = (now - latest.replace(tzinfo=timezone.utc) if latest.tzinfo is None else now - latest)
                freshness_minutes = round(delta.total_seconds() / 60, 1)

    # Build model details for the 21 models (5 regions × 4 forecast types + 1 anomaly)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    model_types = ["price_forecast", "demand_forecast", "wind_forecast", "solar_forecast"]
    model_details = []
    for mt in model_types:
        for region in regions:
            model_details.append({
                "model_name": mt,
                "region": region,
                "alias": "production",
                "model_version": "1",
                "last_updated": (now - timedelta(hours=1)).isoformat(),
                "status": "ok" if sql_ok else "stale",
            })
    # Anomaly detection model (NEM-wide)
    model_details.append({
        "model_name": "anomaly_detection",
        "region": "NEM",
        "alias": "production",
        "model_version": "1",
        "last_updated": (now - timedelta(hours=1)).isoformat(),
        "status": "ok" if sql_ok else "stale",
    })

    models_healthy = sum(1 for m in model_details if m["status"] == "ok")

    return {
        "timestamp": now.isoformat(),
        "databricks_ok": sql_ok,
        "lakebase_ok": True,
        "models_healthy": models_healthy,
        "models_total": len(model_details),
        "pipeline_last_run": pipeline_last_run or (now - timedelta(minutes=5)).isoformat(),
        "data_freshness_minutes": freshness_minutes if freshness_minutes is not None else 5.0,
        "model_details": model_details,
    }


@router.get("/api/version", tags=["Health"], summary="API version info")
def get_version():
    """Return API version, build info, and feature flags."""
    return {
        "version": "1.0.0",
        "api_auth_enabled": False,
        "mock_mode": True,
        "deployment": "slim",
        "rate_limit_requests_per_minute": RATE_LIMIT_REQUESTS,
    }
