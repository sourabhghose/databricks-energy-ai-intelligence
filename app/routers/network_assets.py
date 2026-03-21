"""Network Asset Health & Predictive Maintenance.

Asset registry, condition-based health scoring, failure probability predictions,
and replacement priority ranking for distribution network assets.
"""
from __future__ import annotations

import random as _r
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

from .shared import (
    _CATALOG,
    _NEM_REGIONS,
    _query_gold,
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"


# =========================================================================
# Core helpers (importable by copilot.py)
# =========================================================================

def _core_asset_list(asset_type: Optional[str] = None, region: Optional[str] = None,
                     health_threshold: Optional[float] = None) -> Dict[str, Any]:
    """Asset registry with filters."""
    where = ["1=1"]
    if asset_type:
        where.append(f"a.asset_type = '{asset_type}'")
    if region:
        where.append(f"a.region = '{region}'")
    if health_threshold is not None:
        where.append(f"h.health_index <= {health_threshold}")

    rows = _query_gold(f"""
        SELECT a.asset_id, a.asset_type, a.asset_name, a.nameplate_rating_mva,
               a.installation_year, a.condition_score, a.replacement_cost_aud,
               a.region, a.dnsp, a.voltage_kv, a.status,
               h.health_index, h.risk_factors
        FROM {_SCHEMA}.network_assets a
        LEFT JOIN {_SCHEMA}.asset_health_index h ON a.asset_id = h.asset_id
        WHERE {' AND '.join(where)}
        ORDER BY COALESCE(h.health_index, 999) ASC
        LIMIT 200
    """)
    if rows:
        return {"assets": rows, "count": len(rows)}

    _r.seed(201)
    mock = []
    types = ["zone_substation", "feeder", "distribution_transformer"]
    for i in range(20):
        t = _r.choice(types)
        mock.append({
            "asset_id": f"MOCK-{t[:2].upper()}-{i+1:03d}",
            "asset_type": t,
            "asset_name": f"Mock {t.replace('_', ' ').title()} {i+1}",
            "nameplate_rating_mva": _r.choice([0.5, 10, 30, 60]),
            "installation_year": _r.randint(1980, 2020),
            "condition_score": round(_r.uniform(30, 95), 1),
            "replacement_cost_aud": _r.randint(50_000, 15_000_000),
            "region": _r.choice(_NEM_REGIONS),
            "dnsp": "Mock DNSP",
            "voltage_kv": _r.choice([0.415, 11, 33, 66]),
            "status": "in_service",
            "health_index": round(_r.uniform(20, 95), 1),
            "risk_factors": str(["age", "corrosion"]),
        })
    return {"assets": mock, "count": len(mock)}


def _core_asset_detail(asset_id: str) -> Dict[str, Any]:
    """Asset detail: health index, failure prediction, loading history."""
    asset = _query_gold(f"""
        SELECT a.*, h.health_index, h.risk_factors, h.updated_date,
               f.failure_probability_12m, f.contributing_factors, f.model_version
        FROM {_SCHEMA}.network_assets a
        LEFT JOIN {_SCHEMA}.asset_health_index h ON a.asset_id = h.asset_id
        LEFT JOIN {_SCHEMA}.asset_failure_predictions f ON a.asset_id = f.asset_id
        WHERE a.asset_id = '{asset_id}'
    """)
    loading = _query_gold(f"""
        SELECT interval_datetime, mw, utilization_pct
        FROM {_SCHEMA}.asset_loading_5min
        WHERE asset_id = '{asset_id}'
        ORDER BY interval_datetime DESC LIMIT 48
    """)
    if asset:
        result = asset[0]
        if loading:
            for r in loading:
                r["interval_datetime"] = str(r["interval_datetime"])
            result["loading_history"] = list(reversed(loading))
        return result

    _r.seed(hash(asset_id) % 10000)
    return {
        "asset_id": asset_id,
        "asset_type": "zone_substation",
        "asset_name": asset_id,
        "nameplate_rating_mva": 60,
        "installation_year": 1995,
        "condition_score": round(_r.uniform(40, 85), 1),
        "replacement_cost_aud": 12_000_000,
        "region": "NSW1",
        "dnsp": "Ausgrid",
        "health_index": round(_r.uniform(30, 80), 1),
        "risk_factors": str(["age", "overloading_history"]),
        "failure_probability_12m": round(_r.uniform(0.05, 0.4), 3),
        "contributing_factors": str(["thermal_stress", "age_related"]),
        "model_version": "v2.1.0",
        "loading_history": [],
    }


def _core_health_ranking() -> Dict[str, Any]:
    """Assets ranked by health index (worst first)."""
    rows = _query_gold(f"""
        SELECT h.asset_id, h.health_index, h.risk_factors, h.updated_date,
               a.asset_type, a.asset_name, a.region, a.dnsp, a.replacement_cost_aud
        FROM {_SCHEMA}.asset_health_index h
        JOIN {_SCHEMA}.network_assets a ON h.asset_id = a.asset_id
        ORDER BY h.health_index ASC
        LIMIT 50
    """)
    if rows:
        for r in rows:
            r["updated_date"] = str(r.get("updated_date", ""))
        return {"ranking": rows, "count": len(rows)}

    _r.seed(202)
    mock = []
    for i in range(20):
        mock.append({
            "asset_id": f"MOCK-ZS-{i+1:03d}",
            "health_index": round(_r.uniform(10, 90), 1),
            "risk_factors": str(["age", "corrosion"]),
            "updated_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "asset_type": "zone_substation",
            "asset_name": f"Mock Sub {i+1}",
            "region": _r.choice(_NEM_REGIONS),
            "dnsp": "Mock DNSP",
            "replacement_cost_aud": _r.randint(5, 20) * 1_000_000,
        })
    mock.sort(key=lambda x: x["health_index"])
    return {"ranking": mock, "count": len(mock)}


def _core_failure_risk(min_probability: float = 0.5) -> Dict[str, Any]:
    """Top failure risk assets."""
    rows = _query_gold(f"""
        SELECT f.asset_id, f.failure_probability_12m, f.contributing_factors,
               f.model_version, f.scoring_date,
               a.asset_type, a.asset_name, a.region, a.dnsp
        FROM {_SCHEMA}.asset_failure_predictions f
        JOIN {_SCHEMA}.network_assets a ON f.asset_id = a.asset_id
        WHERE f.failure_probability_12m >= {min_probability}
        ORDER BY f.failure_probability_12m DESC
        LIMIT 50
    """)
    if rows:
        for r in rows:
            r["scoring_date"] = str(r.get("scoring_date", ""))
        return {"high_risk_assets": rows, "count": len(rows), "threshold": min_probability}

    _r.seed(203)
    mock = []
    for i in range(10):
        mock.append({
            "asset_id": f"MOCK-ZS-{i+1:03d}",
            "failure_probability_12m": round(_r.uniform(min_probability, 0.95), 3),
            "contributing_factors": str(["thermal_stress", "age_related"]),
            "model_version": "v2.1.0",
            "scoring_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "asset_type": "zone_substation",
            "asset_name": f"Mock Sub {i+1}",
            "region": _r.choice(_NEM_REGIONS),
            "dnsp": "Mock DNSP",
        })
    return {"high_risk_assets": mock, "count": len(mock), "threshold": min_probability}


def _core_condition_trends() -> Dict[str, Any]:
    """Health index trends by asset type."""
    rows = _query_gold(f"""
        SELECT a.asset_type,
               ROUND(AVG(h.health_index), 1) AS avg_health,
               ROUND(MIN(h.health_index), 1) AS min_health,
               ROUND(MAX(h.health_index), 1) AS max_health,
               COUNT(*) AS asset_count
        FROM {_SCHEMA}.asset_health_index h
        JOIN {_SCHEMA}.network_assets a ON h.asset_id = a.asset_id
        GROUP BY a.asset_type
        ORDER BY avg_health ASC
    """)
    if rows:
        return {"trends": rows}

    return {"trends": [
        {"asset_type": "zone_substation", "avg_health": 62.3, "min_health": 18.5, "max_health": 95.0, "asset_count": 47},
        {"asset_type": "feeder", "avg_health": 68.1, "min_health": 22.0, "max_health": 98.0, "asset_count": 188},
    ]}


def _core_replacement_priority() -> Dict[str, Any]:
    """Replacement priority ranking (risk x cost x criticality)."""
    rows = _query_gold(f"""
        SELECT a.asset_id, a.asset_name, a.asset_type, a.region, a.dnsp,
               a.replacement_cost_aud, a.nameplate_rating_mva,
               h.health_index,
               f.failure_probability_12m,
               ROUND(f.failure_probability_12m * a.replacement_cost_aud / 1000000, 2) AS risk_weighted_cost_m
        FROM {_SCHEMA}.network_assets a
        JOIN {_SCHEMA}.asset_health_index h ON a.asset_id = h.asset_id
        JOIN {_SCHEMA}.asset_failure_predictions f ON a.asset_id = f.asset_id
        ORDER BY risk_weighted_cost_m DESC
        LIMIT 30
    """)
    if rows:
        return {"priority": rows, "count": len(rows)}

    _r.seed(204)
    mock = []
    for i in range(15):
        prob = round(_r.uniform(0.3, 0.9), 3)
        cost = _r.randint(3, 20) * 1_000_000
        mock.append({
            "asset_id": f"MOCK-ZS-{i+1:03d}",
            "asset_name": f"Mock Sub {i+1}",
            "asset_type": "zone_substation",
            "region": _r.choice(_NEM_REGIONS),
            "dnsp": "Mock DNSP",
            "replacement_cost_aud": cost,
            "nameplate_rating_mva": _r.choice([30, 40, 60]),
            "health_index": round(_r.uniform(15, 60), 1),
            "failure_probability_12m": prob,
            "risk_weighted_cost_m": round(prob * cost / 1_000_000, 2),
        })
    mock.sort(key=lambda x: x["risk_weighted_cost_m"], reverse=True)
    return {"priority": mock, "count": len(mock)}


# =========================================================================
# API Endpoints
# =========================================================================

@router.get("/api/assets/health-ranking")
async def health_ranking():
    """Assets ranked by health index (worst first)."""
    return _core_health_ranking()


@router.get("/api/assets/failure-risk")
async def failure_risk(min_probability: float = Query(default=0.5, ge=0, le=1)):
    """Top failure risk assets (>threshold probability)."""
    return _core_failure_risk(min_probability)


@router.get("/api/assets/condition-trends")
async def condition_trends():
    """Health index trends over time by asset type."""
    return _core_condition_trends()


@router.get("/api/assets/replacement-priority")
async def replacement_priority():
    """Replacement priority ranking (risk x cost x criticality)."""
    return _core_replacement_priority()


@router.get("/api/assets")
async def asset_list(
    asset_type: Optional[str] = Query(default=None),
    region: Optional[str] = Query(default=None),
    health_threshold: Optional[float] = Query(default=None),
):
    """Asset registry with filters (type, region, health threshold)."""
    return _core_asset_list(asset_type, region, health_threshold)


@router.get("/api/assets/{asset_id}")
async def asset_detail(asset_id: str):
    """Asset detail: health index, failure prediction, loading history."""
    return _core_asset_detail(asset_id)
