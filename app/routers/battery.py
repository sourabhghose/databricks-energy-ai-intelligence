"""WS3: Battery Optimisation & Dispatch.

Fleet management, dispatch scheduling, revenue optimisation, and performance analytics.
"""
from __future__ import annotations

import math
import random
import uuid
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from .shared import (
    _CATALOG,
    _NEM_REGIONS,
    _query_gold,
    _execute_gold,
    _sql_escape,
    logger,
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"


# ---------------------------------------------------------------------------
# Core functions (exported for Copilot tools)
# ---------------------------------------------------------------------------

def _optimize_battery_core(asset_id: str, horizon_hours: int = 24) -> Dict[str, Any]:
    """Generate an optimised dispatch schedule for a battery asset."""
    # Get asset details
    assets = _query_gold(
        f"SELECT * FROM {_SCHEMA}.battery_assets WHERE asset_id = '{_sql_escape(asset_id)}'"
    )
    if not assets:
        return {"error": f"Battery asset {asset_id} not found"}
    asset = assets[0]
    region = asset["region"]
    capacity = float(asset.get("capacity_mw", 100))
    storage = float(asset.get("storage_mwh", 200))
    efficiency = float(asset.get("efficiency_pct", 88)) / 100

    # Get price forecast for the region
    prices = _query_gold(
        f"SELECT HOUR(interval_datetime) as hour, AVG(rrp) as avg_price, "
        f"MIN(rrp) as min_price, MAX(rrp) as max_price "
        f"FROM {_SCHEMA}.nem_prices_5min "
        f"WHERE region_id = '{_sql_escape(region)}' "
        f"AND interval_datetime >= current_timestamp() - INTERVAL 7 DAYS "
        f"GROUP BY HOUR(interval_datetime) ORDER BY hour"
    )
    price_profile = {int(p["hour"]): float(p["avg_price"]) for p in (prices or [])}
    if not price_profile:
        price_profile = {h: 50 + 30 * math.sin((h - 6) * math.pi / 12) for h in range(24)}

    # Simple optimisation: charge at lowest prices, discharge at highest
    sorted_hours = sorted(price_profile.items(), key=lambda x: x[1])
    charge_hours = {h for h, _ in sorted_hours[:6]}
    discharge_hours = {h for h, _ in sorted_hours[-6:]}

    schedule = []
    soc = 50.0  # Start at 50%
    total_revenue = 0

    for h in range(min(horizon_hours, 48)):
        hour = h % 24
        price = price_profile.get(hour, 75)

        if hour in charge_hours and soc < 90:
            action = "CHARGE"
            power = round(capacity * 0.8, 1)
            soc = min(95, soc + (power / storage) * 100 * efficiency)
            revenue = -power * price / 2  # Half hour cost
        elif hour in discharge_hours and soc > 15:
            action = "DISCHARGE"
            power = round(capacity * 0.9, 1)
            soc = max(5, soc - (power / storage) * 100 / efficiency)
            revenue = power * price / 2
        else:
            action = "IDLE"
            power = 0
            revenue = 0

        total_revenue += revenue
        schedule.append({
            "hour": h,
            "hour_of_day": hour,
            "action": action,
            "power_mw": power,
            "soc_pct": round(soc, 1),
            "price": round(price, 2),
            "revenue": round(revenue, 2),
        })

    return {
        "asset_id": asset_id,
        "asset_name": asset.get("name", asset_id),
        "region": region,
        "capacity_mw": capacity,
        "storage_mwh": storage,
        "horizon_hours": min(horizon_hours, 48),
        "schedule": schedule,
        "total_revenue": round(total_revenue, 2),
        "charge_hours": sorted(charge_hours),
        "discharge_hours": sorted(discharge_hours),
        "avg_charge_price": round(sum(price_profile[h] for h in charge_hours) / max(len(charge_hours), 1), 2),
        "avg_discharge_price": round(sum(price_profile[h] for h in discharge_hours) / max(len(discharge_hours), 1), 2),
        "spread": round(
            sum(price_profile[h] for h in discharge_hours) / max(len(discharge_hours), 1)
            - sum(price_profile[h] for h in charge_hours) / max(len(charge_hours), 1), 2
        ),
    }


def _get_battery_performance_core(asset_id: Optional[str] = None,
                                   days: int = 7) -> Dict[str, Any]:
    """Get battery fleet performance metrics."""
    # Try computing performance from real price spreads (charge 1-5am, discharge 4-9pm)
    real = _query_gold(
        f"SELECT DATE(interval_datetime) as perf_date, "
        f"AVG(CASE WHEN HOUR(interval_datetime) BETWEEN 1 AND 5 THEN rrp END) as avg_charge, "
        f"AVG(CASE WHEN HOUR(interval_datetime) BETWEEN 16 AND 21 THEN rrp END) as avg_discharge, "
        f"AVG(CASE WHEN HOUR(interval_datetime) BETWEEN 16 AND 21 THEN rrp END) "
        f"- AVG(CASE WHEN HOUR(interval_datetime) BETWEEN 1 AND 5 THEN rrp END) as spread "
        f"FROM {_SCHEMA}.nem_prices_5min "
        f"WHERE interval_datetime >= current_timestamp() - INTERVAL {days} DAYS "
        f"GROUP BY DATE(interval_datetime) "
        f"ORDER BY perf_date DESC"
    )
    if real:
        avg_charge = sum(float(r.get("avg_charge", 0) or 0) for r in real) / max(len(real), 1)
        avg_discharge = sum(float(r.get("avg_discharge", 0) or 0) for r in real) / max(len(real), 1)
        avg_spread = sum(float(r.get("spread", 0) or 0) for r in real) / max(len(real), 1)
        # Assume 100MW fleet doing 1 cycle/day
        daily_revenue = avg_spread * 100 * 2  # 100MW × 2hr discharge
        return {
            "period_days": days,
            "assets": [{
                "asset_id": asset_id or "fleet",
                "total_cycles": len(real),
                "total_throughput": round(len(real) * 200, 0),
                "arb_rev": round(daily_revenue * len(real), 2),
                "fcas_rev": 0,
                "total_rev": round(daily_revenue * len(real), 2),
                "avg_charge": round(avg_charge, 2),
                "avg_discharge": round(avg_discharge, 2),
                "avg_eff": 88.0,
            }],
            "fleet_total_revenue": round(daily_revenue * len(real), 2),
        }

    # Seed fallback
    where = f"WHERE asset_id = '{_sql_escape(asset_id)}'" if asset_id else ""
    if where:
        where += f" AND perf_date >= current_date() - INTERVAL {days} DAYS"
    else:
        where = f"WHERE perf_date >= current_date() - INTERVAL {days} DAYS"

    rows = _query_gold(
        f"SELECT asset_id, "
        f"SUM(cycles_count) as total_cycles, SUM(throughput_mwh) as total_throughput, "
        f"SUM(arbitrage_revenue) as arb_rev, SUM(fcas_revenue) as fcas_rev, "
        f"SUM(total_revenue) as total_rev, "
        f"AVG(avg_charge_price) as avg_charge, AVG(avg_discharge_price) as avg_discharge, "
        f"AVG(efficiency_actual) as avg_eff "
        f"FROM {_SCHEMA}.battery_performance {where} "
        f"GROUP BY asset_id"
    )
    return {
        "period_days": days,
        "assets": rows or [],
        "fleet_total_revenue": round(sum(float(r.get("total_rev", 0) or 0) for r in (rows or [])), 2),
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/battery/dashboard")
async def battery_dashboard():
    """Battery fleet overview: assets, KPIs, recent performance."""
    # Try real battery facilities from nem_facilities
    real_assets = _query_gold(
        f"SELECT duid as asset_id, station_name as name, region_id as region, "
        f"fuel_type, capacity_mw, capacity_mw * 2 as storage_mwh "
        f"FROM {_SCHEMA}.nem_facilities "
        f"WHERE fuel_type = 'battery' AND capacity_mw > 0 "
        f"ORDER BY capacity_mw DESC"
    )

    assets = real_assets if real_assets else _query_gold(
        f"SELECT * FROM {_SCHEMA}.battery_assets ORDER BY capacity_mw DESC"
    )
    perf = _get_battery_performance_core(days=7)

    total_mw = sum(float(a.get("capacity_mw", 0)) for a in (assets or []))
    total_mwh = sum(float(a.get("storage_mwh", 0)) for a in (assets or []))

    return {
        "fleet_summary": {
            "total_assets": len(assets or []),
            "total_capacity_mw": round(total_mw, 1),
            "total_storage_mwh": round(total_mwh, 1),
            "total_revenue_7d": perf["fleet_total_revenue"],
        },
        "assets": [{**a, "commissioning_date": str(a.get("commissioning_date", ""))}
                    for a in (assets or [])],
        "performance": perf,
    }


@router.get("/api/battery/assets")
async def list_battery_assets():
    """List all battery assets."""
    # Try real battery facilities
    real = _query_gold(
        f"SELECT duid as asset_id, station_name as name, region_id as region, "
        f"fuel_type, capacity_mw, capacity_mw * 2 as storage_mwh, "
        f"88 as efficiency_pct, 'ONLINE' as status "
        f"FROM {_SCHEMA}.nem_facilities "
        f"WHERE fuel_type = 'battery' AND capacity_mw > 0 "
        f"ORDER BY capacity_mw DESC"
    )
    if real:
        return {"assets": [{**r, "commissioning_date": ""} for r in real]}

    rows = _query_gold(f"SELECT * FROM {_SCHEMA}.battery_assets ORDER BY capacity_mw DESC")
    return {"assets": [{**r, "commissioning_date": str(r.get("commissioning_date", ""))}
                        for r in (rows or [])]}


@router.get("/api/battery/schedule")
async def battery_schedule(asset_id: str = Query("HPR1"), days: int = Query(1)):
    """Get dispatch schedule for a battery asset."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.battery_dispatch_schedule "
        f"WHERE asset_id = '{_sql_escape(asset_id)}' "
        f"AND interval_datetime >= current_timestamp() - INTERVAL {days} DAYS "
        f"ORDER BY interval_datetime LIMIT 200"
    )
    return {
        "asset_id": asset_id,
        "schedule": [{**r, "interval_datetime": str(r.get("interval_datetime", ""))}
                      for r in (rows or [])],
    }


@router.post("/api/battery/optimize")
async def optimize_battery(asset_id: str = Query("HPR1"),
                            horizon_hours: int = Query(24)):
    """Generate optimised dispatch schedule."""
    return _optimize_battery_core(asset_id, horizon_hours)


@router.get("/api/battery/performance")
async def battery_performance(asset_id: Optional[str] = None, days: int = Query(7)):
    """Get battery performance metrics."""
    return _get_battery_performance_core(asset_id, days)


@router.get("/api/battery/revenue")
async def battery_revenue():
    """Get revenue breakdown across all battery assets."""
    rows = _query_gold(
        f"SELECT ba.asset_id, ba.name, ba.region, ba.capacity_mw, "
        f"SUM(bp.arbitrage_revenue) as arb_rev, SUM(bp.fcas_revenue) as fcas_rev, "
        f"SUM(bp.total_revenue) as total_rev "
        f"FROM {_SCHEMA}.battery_assets ba "
        f"LEFT JOIN {_SCHEMA}.battery_performance bp ON ba.asset_id = bp.asset_id "
        f"GROUP BY ba.asset_id, ba.name, ba.region, ba.capacity_mw "
        f"ORDER BY total_rev DESC"
    )
    return {"revenue": rows or []}
