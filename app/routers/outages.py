"""Outage Management & Reliability KPIs.

Distribution outage events, SAIDI/SAIFI/CAIDI reliability tracking vs AER targets,
cause analysis, worst-performing feeders, and GSL payment liability.
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

def _core_outage_list(region: Optional[str] = None, cause: Optional[str] = None,
                      days: int = 90) -> Dict[str, Any]:
    """Outage events list."""
    where = [f"start_time >= current_timestamp() - INTERVAL {days} DAYS"]
    if region:
        where.append(f"region = '{region}'")
    if cause:
        where.append(f"cause_code = '{cause}'")

    rows = _query_gold(f"""
        SELECT event_id, start_time, end_time, feeder_id, zone_substation,
               affected_customers, cause_code, region, status, etr_minutes
        FROM {_SCHEMA}.outage_events
        WHERE {' AND '.join(where)}
        ORDER BY start_time DESC
        LIMIT 100
    """)
    if rows:
        for r in rows:
            r["start_time"] = str(r.get("start_time", ""))
            r["end_time"] = str(r.get("end_time", ""))
        return {"outages": rows, "count": len(rows)}

    _r.seed(301)
    mock = []
    causes = ["vegetation", "equipment_failure", "weather", "animal", "third_party", "unknown"]
    now = datetime.now(timezone.utc)
    for i in range(20):
        start = now - timedelta(days=_r.randint(0, days))
        mock.append({
            "event_id": f"OUT-MOCK-{i+1:04d}",
            "start_time": start.isoformat(),
            "end_time": (start + timedelta(hours=_r.randint(1, 8))).isoformat(),
            "feeder_id": f"FDR-MOCK-{i+1:02d}",
            "zone_substation": f"ZS-MOCK-{i+1:02d}",
            "affected_customers": _r.randint(50, 5000),
            "cause_code": _r.choice(causes),
            "region": _r.choice(_NEM_REGIONS),
            "status": "restored",
            "etr_minutes": 0,
        })
    return {"outages": mock, "count": len(mock)}


def _core_active_outages() -> Dict[str, Any]:
    """Currently active outages."""
    rows = _query_gold(f"""
        SELECT event_id, start_time, feeder_id, zone_substation,
               affected_customers, cause_code, region, status, etr_minutes
        FROM {_SCHEMA}.outage_events
        WHERE status = 'active'
        ORDER BY affected_customers DESC
    """)
    if rows:
        for r in rows:
            r["start_time"] = str(r.get("start_time", ""))
        return {"active_outages": rows, "count": len(rows)}

    _r.seed(302)
    now = datetime.now(timezone.utc)
    mock = []
    for i in range(3):
        mock.append({
            "event_id": f"OUT-ACTIVE-{i+1:04d}",
            "start_time": (now - timedelta(hours=_r.randint(1, 6))).isoformat(),
            "feeder_id": f"FDR-MOCK-{i+1:02d}",
            "zone_substation": f"ZS-MOCK-{i+1:02d}",
            "affected_customers": _r.randint(200, 5000),
            "cause_code": _r.choice(["equipment_failure", "weather", "vegetation"]),
            "region": _r.choice(_NEM_REGIONS),
            "status": "active",
            "etr_minutes": _r.randint(30, 180),
        })
    return {"active_outages": mock, "count": len(mock)}


def _core_reliability(region: Optional[str] = None) -> Dict[str, Any]:
    """SAIDI/SAIFI/CAIDI KPIs by region with AER targets."""
    where = f"WHERE region = '{region}'" if region else ""
    rows = _query_gold(f"""
        SELECT region, period_type, period_start, saidi_minutes, saifi_count,
               caidi_minutes, maifi_count, aer_target_saidi, aer_target_saifi
        FROM {_SCHEMA}.reliability_kpis
        {where}
        ORDER BY region, period_start
    """)
    if rows:
        for r in rows:
            r["period_start"] = str(r.get("period_start", ""))
        return {"kpis": rows, "count": len(rows)}

    _r.seed(303)
    mock = []
    targets = {"NSW1": (85, 1.1), "QLD1": (140, 1.6), "VIC1": (75, 1.0), "SA1": (120, 1.3), "TAS1": (180, 2.0)}
    for reg, (target_saidi, target_saifi) in targets.items():
        if region and reg != region:
            continue
        mock.append({
            "region": reg,
            "period_type": "yearly",
            "period_start": "2026-01-01",
            "saidi_minutes": round(target_saidi * _r.uniform(0.6, 1.2), 1),
            "saifi_count": round(target_saifi * _r.uniform(0.6, 1.2), 2),
            "caidi_minutes": round(target_saidi * _r.uniform(0.6, 1.2) / max(target_saifi * _r.uniform(0.6, 1.2), 0.01), 1),
            "maifi_count": round(_r.uniform(1, 4), 1),
            "aer_target_saidi": target_saidi,
            "aer_target_saifi": target_saifi,
        })
    return {"kpis": mock, "count": len(mock)}


def _core_cause_breakdown(days: int = 90) -> Dict[str, Any]:
    """Outage cause breakdown."""
    rows = _query_gold(f"""
        SELECT cause_code, COUNT(*) AS event_count,
               SUM(affected_customers) AS total_affected,
               ROUND(AVG(CASE WHEN end_time IS NOT NULL AND end_time != ''
                   THEN TIMESTAMPDIFF(MINUTE, start_time, end_time) ELSE 60 END), 0) AS avg_duration_min
        FROM {_SCHEMA}.outage_events
        WHERE start_time >= current_timestamp() - INTERVAL {days} DAYS
        GROUP BY cause_code
        ORDER BY event_count DESC
    """)
    if rows:
        return {"causes": rows}

    return {"causes": [
        {"cause_code": "vegetation", "event_count": 45, "total_affected": 28000, "avg_duration_min": 95},
        {"cause_code": "equipment_failure", "event_count": 38, "total_affected": 22000, "avg_duration_min": 120},
        {"cause_code": "weather", "event_count": 30, "total_affected": 35000, "avg_duration_min": 180},
        {"cause_code": "animal", "event_count": 15, "total_affected": 8000, "avg_duration_min": 45},
        {"cause_code": "third_party", "event_count": 15, "total_affected": 10000, "avg_duration_min": 60},
        {"cause_code": "unknown", "event_count": 7, "total_affected": 4000, "avg_duration_min": 75},
    ]}


def _core_worst_feeders() -> Dict[str, Any]:
    """Bottom 5% feeders by reliability."""
    rows = _query_gold(f"""
        SELECT feeder_id, zone_substation, region,
               COUNT(*) AS outage_count,
               SUM(affected_customers) AS total_affected,
               ROUND(SUM(CASE WHEN end_time IS NOT NULL AND end_time != ''
                   THEN TIMESTAMPDIFF(MINUTE, start_time, end_time) ELSE 60 END), 0) AS total_minutes
        FROM {_SCHEMA}.outage_events
        GROUP BY feeder_id, zone_substation, region
        ORDER BY outage_count DESC
        LIMIT 20
    """)
    if rows:
        return {"worst_feeders": rows, "count": len(rows)}

    _r.seed(305)
    mock = []
    for i in range(10):
        mock.append({
            "feeder_id": f"FDR-MOCK-{i+1:02d}",
            "zone_substation": f"ZS-MOCK-{i+1:02d}",
            "region": _r.choice(_NEM_REGIONS),
            "outage_count": _r.randint(5, 15),
            "total_affected": _r.randint(5000, 30000),
            "total_minutes": _r.randint(300, 2000),
        })
    return {"worst_feeders": mock, "count": len(mock)}


def _core_gsl() -> Dict[str, Any]:
    """GSL tracking — eligible customers, projected payments."""
    rows = _query_gold(f"""
        SELECT feeder_id,
               COUNT(*) AS total_customers,
               SUM(CASE WHEN gsl_eligible_flag = true THEN 1 ELSE 0 END) AS eligible_count,
               ROUND(SUM(projected_payment_aud), 2) AS total_projected_aud
        FROM {_SCHEMA}.gsl_tracking
        GROUP BY feeder_id
        HAVING eligible_count > 0
        ORDER BY total_projected_aud DESC
        LIMIT 50
    """)
    total = _query_gold(f"""
        SELECT COUNT(*) AS eligible_total,
               ROUND(SUM(projected_payment_aud), 2) AS total_aud
        FROM {_SCHEMA}.gsl_tracking
        WHERE gsl_eligible_flag = true
    """)
    if rows:
        result = {"feeders": rows, "count": len(rows)}
        if total:
            result["total_eligible_customers"] = total[0].get("eligible_total", 0)
            result["total_projected_payment_aud"] = total[0].get("total_aud", 0)
        return result

    _r.seed(306)
    return {
        "feeders": [
            {"feeder_id": f"FDR-MOCK-{i+1:02d}", "total_customers": _r.randint(100, 500),
             "eligible_count": _r.randint(2, 15), "total_projected_aud": round(_r.uniform(500, 5000), 2)}
            for i in range(10)
        ],
        "count": 10,
        "total_eligible_customers": 85,
        "total_projected_payment_aud": 18500.00,
    }


def _core_outage_summary() -> Dict[str, Any]:
    """Outage summary: active count, SAIDI YTD, worst region."""
    cache_key = "outage_summary"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    active = _query_gold(f"SELECT COUNT(*) AS cnt FROM {_SCHEMA}.outage_events WHERE status = 'active'")
    ytd = _query_gold(f"""
        SELECT region, saidi_minutes, aer_target_saidi
        FROM {_SCHEMA}.reliability_kpis
        WHERE period_type = 'yearly'
        ORDER BY saidi_minutes DESC
    """)

    active_count = active[0]["cnt"] if active else 3
    worst_region = ytd[0]["region"] if ytd else "QLD1"
    ytd_saidi = ytd[0]["saidi_minutes"] if ytd else 95.0
    target_saidi = ytd[0]["aer_target_saidi"] if ytd else 140.0

    result = {
        "active_outages": active_count,
        "saidi_ytd": ytd_saidi,
        "saidi_target": target_saidi,
        "saidi_pct_of_target": round(ytd_saidi / target_saidi * 100, 1) if target_saidi else 0,
        "worst_region": worst_region,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(cache_key, result, ttl_seconds=30)
    return result


# =========================================================================
# API Endpoints
# =========================================================================

@router.get("/api/outages/summary")
async def outage_summary():
    """Active outages count, SAIDI YTD vs target, worst region."""
    return _core_outage_summary()


@router.get("/api/outages/active")
async def active_outages():
    """Currently active outages."""
    return _core_active_outages()


@router.get("/api/outages/reliability")
async def reliability(region: Optional[str] = Query(default=None)):
    """SAIDI/SAIFI/CAIDI KPIs by region with AER targets."""
    return _core_reliability(region)


@router.get("/api/outages/causes")
async def outage_causes(days: int = Query(default=90, ge=1, le=365)):
    """Outage cause breakdown."""
    return _core_cause_breakdown(days)


@router.get("/api/outages/worst-feeders")
async def worst_feeders():
    """Bottom 5% feeders by reliability."""
    return _core_worst_feeders()


@router.get("/api/outages/gsl")
async def gsl_tracking():
    """GSL tracking — eligible customers, projected payments."""
    return _core_gsl()


@router.get("/api/outages")
async def outage_list(
    region: Optional[str] = Query(default=None),
    cause: Optional[str] = Query(default=None),
    days: int = Query(default=90, ge=1, le=365),
):
    """Outage events list (filter: region, cause, period)."""
    return _core_outage_list(region, cause, days)


@router.get("/api/outages/{event_id}")
async def outage_detail(event_id: str):
    """Outage event detail with timeline."""
    rows = _query_gold(f"""
        SELECT event_id, start_time, end_time, feeder_id, zone_substation,
               affected_customers, cause_code, region, status, etr_minutes
        FROM {_SCHEMA}.outage_events
        WHERE event_id = '{event_id}'
    """)
    if rows:
        r = rows[0]
        r["start_time"] = str(r.get("start_time", ""))
        r["end_time"] = str(r.get("end_time", ""))
        return r

    _r.seed(hash(event_id) % 10000)
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=_r.randint(2, 48))
    return {
        "event_id": event_id,
        "start_time": start.isoformat(),
        "end_time": (start + timedelta(hours=3)).isoformat(),
        "feeder_id": "FDR-MOCK-01",
        "zone_substation": "ZS-MOCK-01",
        "affected_customers": _r.randint(200, 5000),
        "cause_code": "equipment_failure",
        "region": "NSW1",
        "status": "restored",
        "etr_minutes": 0,
    }
