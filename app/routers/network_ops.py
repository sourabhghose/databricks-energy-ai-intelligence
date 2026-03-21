"""Network Operations Monitoring — real-time loading, voltage, power quality.

Serves distribution network operational dashboards for DNSPs including
zone substation loading, voltage excursion detection, and power quality metrics.
"""
from __future__ import annotations

import random as _r
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

from .shared import (
    _CATALOG,
    _NEM_REGIONS,
    _cache_get,
    _cache_set,
    _query_gold,
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"


# =========================================================================
# Core helpers (importable by copilot.py)
# =========================================================================

def _core_network_loading(region: Optional[str] = None, min_utilization: float = 0) -> Dict[str, Any]:
    """Zone substation loading dashboard."""
    where = []
    if region:
        where.append(f"a.region = '{region}'")
    if min_utilization > 0:
        where.append(f"l.utilization_pct >= {min_utilization}")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT a.asset_id, a.asset_name, a.nameplate_rating_mva, a.region, a.dnsp,
               l.mw, l.mvar, l.utilization_pct, l.interval_datetime
        FROM {_SCHEMA}.network_assets a
        JOIN (
            SELECT asset_id, mw, mvar, utilization_pct, interval_datetime,
                   ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY interval_datetime DESC) AS rn
            FROM {_SCHEMA}.asset_loading_5min
        ) l ON a.asset_id = l.asset_id AND l.rn = 1
        {where_clause}
        ORDER BY l.utilization_pct DESC
        LIMIT 100
    """)
    if rows:
        for r in rows:
            r["interval_datetime"] = str(r.get("interval_datetime", ""))
        return {"assets": rows, "count": len(rows)}

    # Mock fallback
    _r.seed(101)
    mock = []
    subs = ["Sydney North", "Parramatta", "Melbourne CBD", "Adelaide CBD", "Brisbane CBD",
            "Gold Coast", "Dandenong", "Newcastle", "Geelong", "Hobart"]
    for s in subs:
        util = round(_r.uniform(max(min_utilization, 30), 98), 1)
        rating = _r.choice([30, 40, 60, 80])
        mock.append({
            "asset_id": f"ZS-{s.replace(' ', '_').upper()}",
            "asset_name": f"{s} Zone Substation",
            "nameplate_rating_mva": rating,
            "region": _r.choice(_NEM_REGIONS),
            "dnsp": "Mock DNSP",
            "mw": round(rating * util / 100, 1),
            "mvar": round(rating * util / 100 * 0.3, 1),
            "utilization_pct": util,
            "interval_datetime": datetime.now(timezone.utc).isoformat(),
        })
    return {"assets": mock, "count": len(mock)}


def _core_loading_history(asset_id: str) -> Dict[str, Any]:
    """24-hour loading history for a single asset."""
    rows = _query_gold(f"""
        SELECT interval_datetime, mw, mvar, utilization_pct
        FROM {_SCHEMA}.asset_loading_5min
        WHERE asset_id = '{asset_id}'
        ORDER BY interval_datetime DESC
        LIMIT 288
    """)
    if rows:
        for r in rows:
            r["interval_datetime"] = str(r["interval_datetime"])
        return {"asset_id": asset_id, "history": list(reversed(rows))}

    # Mock fallback
    _r.seed(hash(asset_id) % 10000)
    now = datetime.now(timezone.utc)
    history = []
    for i in range(288):
        dt = now - timedelta(minutes=5 * (287 - i))
        hour = dt.hour
        base = 0.35 + 0.25 * (1 if 7 <= hour <= 21 else 0)
        util = max(0.1, min(0.99, base + _r.gauss(0, 0.05)))
        history.append({
            "interval_datetime": dt.isoformat(),
            "mw": round(40 * util, 2),
            "mvar": round(40 * util * 0.3, 2),
            "utilization_pct": round(util * 100, 1),
        })
    return {"asset_id": asset_id, "history": history}


def _core_loading_alerts(threshold: float = 80) -> Dict[str, Any]:
    """Assets currently above utilization threshold."""
    rows = _query_gold(f"""
        SELECT a.asset_id, a.asset_name, a.region, a.dnsp, a.nameplate_rating_mva,
               l.mw, l.utilization_pct, l.interval_datetime
        FROM {_SCHEMA}.network_assets a
        JOIN (
            SELECT asset_id, mw, utilization_pct, interval_datetime,
                   ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY interval_datetime DESC) AS rn
            FROM {_SCHEMA}.asset_loading_5min
        ) l ON a.asset_id = l.asset_id AND l.rn = 1
        WHERE l.utilization_pct >= {threshold}
        ORDER BY l.utilization_pct DESC
    """)
    if rows:
        for r in rows:
            r["interval_datetime"] = str(r.get("interval_datetime", ""))
        return {"alerts": rows, "threshold_pct": threshold, "count": len(rows)}

    _r.seed(102)
    alerts = []
    for name in ["Sydney North", "Melbourne CBD", "Adelaide CBD"]:
        util = round(_r.uniform(threshold, 98), 1)
        alerts.append({
            "asset_id": f"ZS-{name.replace(' ', '_').upper()}",
            "asset_name": f"{name} Zone Substation",
            "region": "NSW1",
            "dnsp": "Mock DNSP",
            "nameplate_rating_mva": 60,
            "mw": round(60 * util / 100, 1),
            "utilization_pct": util,
            "interval_datetime": datetime.now(timezone.utc).isoformat(),
        })
    return {"alerts": alerts, "threshold_pct": threshold, "count": len(alerts)}


def _core_voltage_events(region: Optional[str] = None) -> Dict[str, Any]:
    """Voltage excursion events."""
    where = "AND v.excursion_flag = true" + (f" AND a.region = '{region}'" if region else "")
    rows = _query_gold(f"""
        SELECT v.monitoring_point_id, v.interval_datetime, v.voltage_kv,
               v.nominal_kv, v.excursion_type
        FROM {_SCHEMA}.voltage_monitoring v
        WHERE v.excursion_flag = true
        ORDER BY v.interval_datetime DESC
        LIMIT 100
    """)
    if rows:
        for r in rows:
            r["interval_datetime"] = str(r["interval_datetime"])
        return {"excursions": rows, "count": len(rows)}

    _r.seed(103)
    mock = []
    now = datetime.now(timezone.utc)
    for i in range(15):
        mock.append({
            "monitoring_point_id": f"VM-FDR-SYDNEY_NORTH-{i+1:02d}",
            "interval_datetime": (now - timedelta(hours=_r.randint(0, 24))).isoformat(),
            "voltage_kv": round(11 * _r.uniform(1.06, 1.10), 3),
            "nominal_kv": 11.0,
            "excursion_type": "over",
        })
    return {"excursions": mock, "count": len(mock)}


def _core_power_quality() -> Dict[str, Any]:
    """Power quality metrics."""
    rows = _query_gold(f"""
        SELECT monitoring_point_id, interval_datetime, thd_pct,
               flicker_pst, voltage_unbalance_pct
        FROM {_SCHEMA}.power_quality
        ORDER BY interval_datetime DESC
        LIMIT 100
    """)
    if rows:
        for r in rows:
            r["interval_datetime"] = str(r["interval_datetime"])
        return {"metrics": rows, "count": len(rows)}

    _r.seed(104)
    now = datetime.now(timezone.utc)
    mock = []
    for i in range(20):
        mock.append({
            "monitoring_point_id": f"VM-FDR-MOCK-{i+1:02d}",
            "interval_datetime": (now - timedelta(hours=i)).isoformat(),
            "thd_pct": round(_r.uniform(2, 7), 2),
            "flicker_pst": round(_r.uniform(0.3, 1.2), 2),
            "voltage_unbalance_pct": round(_r.uniform(0.5, 2.5), 2),
        })
    return {"metrics": mock, "count": len(mock)}


def _core_network_summary() -> Dict[str, Any]:
    """Network operations KPI summary."""
    cache_key = "network_ops_summary"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    total_assets = _query_gold(f"SELECT COUNT(*) AS cnt FROM {_SCHEMA}.network_assets")
    overloaded = _query_gold(f"""
        SELECT COUNT(DISTINCT asset_id) AS cnt
        FROM {_SCHEMA}.asset_loading_5min
        WHERE utilization_pct >= 80
        AND interval_datetime >= current_timestamp() - INTERVAL 1 HOUR
    """)
    excursions = _query_gold(f"""
        SELECT COUNT(*) AS cnt FROM {_SCHEMA}.voltage_monitoring
        WHERE excursion_flag = true
        AND interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
    """)
    der_mw = _query_gold(f"SELECT ROUND(SUM(capacity_kw) / 1000, 1) AS total_mw FROM {_SCHEMA}.der_fleet")

    result = {
        "total_assets": (total_assets[0]["cnt"] if total_assets else 200),
        "overloaded_count": (overloaded[0]["cnt"] if overloaded else 8),
        "excursions_today": (excursions[0]["cnt"] if excursions else 23),
        "total_der_mw": (der_mw[0]["total_mw"] if der_mw and der_mw[0].get("total_mw") else 1250),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(cache_key, result, ttl_seconds=30)
    return result


# =========================================================================
# API Endpoints
# =========================================================================

@router.get("/api/network/summary")
async def network_summary():
    """KPI dashboard: total assets, overloaded count, excursions today, DER MW."""
    return _core_network_summary()


@router.get("/api/network/loading/alerts")
async def loading_alerts(threshold: float = Query(default=80, ge=0, le=100)):
    """Assets above utilization threshold."""
    return _core_loading_alerts(threshold)


@router.get("/api/network/loading")
async def network_loading(
    region: Optional[str] = Query(default=None),
    min_utilization: float = Query(default=0, ge=0, le=100),
):
    """Zone substation loading dashboard."""
    return _core_network_loading(region, min_utilization)


@router.get("/api/network/loading/{asset_id}")
async def loading_history(asset_id: str):
    """Single asset loading history (24h)."""
    return _core_loading_history(asset_id)


@router.get("/api/network/voltage")
async def voltage_events(region: Optional[str] = Query(default=None)):
    """Voltage excursion events."""
    return _core_voltage_events(region)


@router.get("/api/network/voltage/{monitoring_point_id}")
async def voltage_profile(monitoring_point_id: str):
    """Voltage profile for a monitoring point."""
    rows = _query_gold(f"""
        SELECT interval_datetime, voltage_kv, nominal_kv, excursion_flag, excursion_type
        FROM {_SCHEMA}.voltage_monitoring
        WHERE monitoring_point_id = '{monitoring_point_id}'
        ORDER BY interval_datetime DESC
        LIMIT 96
    """)
    if rows:
        for r in rows:
            r["interval_datetime"] = str(r["interval_datetime"])
        return {"monitoring_point_id": monitoring_point_id, "profile": list(reversed(rows))}

    _r.seed(hash(monitoring_point_id) % 10000)
    now = datetime.now(timezone.utc)
    profile = []
    for i in range(48):
        dt = now - timedelta(minutes=30 * (47 - i))
        v = 11.0 * (1.0 + _r.gauss(0, 0.015))
        profile.append({
            "interval_datetime": dt.isoformat(),
            "voltage_kv": round(v, 3),
            "nominal_kv": 11.0,
            "excursion_flag": abs(v - 11.0) > 0.66,
            "excursion_type": "over" if v > 11.66 else ("under" if v < 10.34 else ""),
        })
    return {"monitoring_point_id": monitoring_point_id, "profile": profile}


@router.get("/api/network/power-quality")
async def power_quality():
    """Power quality metrics (THD, flicker, unbalance)."""
    return _core_power_quality()
