"""Network Planning — Spatial demand forecasting, constraints, EV impact.

10-year spatial demand forecasts, network constraint register with augmentation
options, EV charging impact projections, and investment prioritisation.
"""
from __future__ import annotations

import json
import random as _r
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

from .shared import (
    _CATALOG,
    _cache_get,
    _cache_set,
    _query_gold,
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"


# =========================================================================
# Core helpers (importable by copilot.py)
# =========================================================================

def _core_demand_forecast(zone_substation: Optional[str] = None,
                          scenario: Optional[str] = None) -> Dict[str, Any]:
    """10-year spatial demand forecast by zone sub + scenario."""
    where = ["1=1"]
    if zone_substation:
        where.append(f"zone_substation = '{zone_substation}'")
    if scenario:
        where.append(f"scenario = '{scenario}'")

    rows = _query_gold(f"""
        SELECT zone_substation, scenario, forecast_year, peak_demand_mw,
               underlying_growth_mw, solar_impact_mw, battery_impact_mw, ev_impact_mw
        FROM {_SCHEMA}.demand_forecast_spatial
        WHERE {' AND '.join(where)}
        ORDER BY zone_substation, scenario, forecast_year
        LIMIT 500
    """)
    if rows:
        return {"forecasts": rows, "count": len(rows)}

    _r.seed(501)
    mock = []
    scenarios = ["bau", "high_solar", "high_ev", "combined"]
    for zs in [f"ZS-MOCK-{i+1:02d}" for i in range(5)]:
        for sc in scenarios:
            if scenario and sc != scenario:
                continue
            base = _r.uniform(20, 60)
            for year in range(2026, 2036):
                offset = year - 2026
                mock.append({
                    "zone_substation": zs,
                    "scenario": sc,
                    "forecast_year": year,
                    "peak_demand_mw": round(base + offset * 0.8, 2),
                    "underlying_growth_mw": round(offset * 0.5, 2),
                    "solar_impact_mw": round(-offset * 0.3, 2),
                    "battery_impact_mw": round(-offset * 0.15, 2),
                    "ev_impact_mw": round(offset * 0.4, 2),
                })
    return {"forecasts": mock, "count": len(mock)}


def _core_demand_forecast_detail(zone_substation: str) -> Dict[str, Any]:
    """Single zone sub multi-scenario forecast."""
    rows = _query_gold(f"""
        SELECT scenario, forecast_year, peak_demand_mw,
               underlying_growth_mw, solar_impact_mw, battery_impact_mw, ev_impact_mw
        FROM {_SCHEMA}.demand_forecast_spatial
        WHERE zone_substation = '{zone_substation}'
        ORDER BY scenario, forecast_year
    """)
    if rows:
        return {"zone_substation": zone_substation, "forecasts": rows}

    _r.seed(hash(zone_substation) % 10000)
    mock = []
    base = _r.uniform(25, 55)
    for sc in ["bau", "high_solar", "high_ev", "combined"]:
        for year in range(2026, 2036):
            offset = year - 2026
            mock.append({
                "scenario": sc,
                "forecast_year": year,
                "peak_demand_mw": round(base + offset * _r.uniform(0.3, 1.0), 2),
                "underlying_growth_mw": round(offset * 0.5, 2),
                "solar_impact_mw": round(-offset * 0.3, 2),
                "battery_impact_mw": round(-offset * 0.15, 2),
                "ev_impact_mw": round(offset * 0.4, 2),
            })
    return {"zone_substation": zone_substation, "forecasts": mock}


def _core_constraints(constraint_type: Optional[str] = None) -> Dict[str, Any]:
    """Network constraint register."""
    where = f"WHERE constraint_type = '{constraint_type}'" if constraint_type else ""
    rows = _query_gold(f"""
        SELECT constraint_id, zone_substation, constraint_type, limiting_asset,
               current_utilization_pct, breach_year_bau, breach_year_high, options_json
        FROM {_SCHEMA}.network_constraints_register
        {where}
        ORDER BY breach_year_bau ASC
        LIMIT 100
    """)
    if rows:
        for r in rows:
            try:
                r["options"] = json.loads(r.get("options_json", "[]"))
            except (json.JSONDecodeError, TypeError):
                r["options"] = []
        return {"constraints": rows, "count": len(rows)}

    _r.seed(502)
    types = ["thermal", "voltage", "fault_level"]
    mock = []
    for i in range(15):
        ct = _r.choice(types)
        if constraint_type and ct != constraint_type:
            continue
        breach = _r.randint(2027, 2034)
        mock.append({
            "constraint_id": f"CON-MOCK-{i+1:04d}",
            "zone_substation": f"ZS-MOCK-{i+1:02d}",
            "constraint_type": ct,
            "limiting_asset": f"Mock {ct} asset {i+1}",
            "current_utilization_pct": round(_r.uniform(65, 105), 1),
            "breach_year_bau": breach,
            "breach_year_high": max(2026, breach - _r.randint(1, 3)),
            "options_json": "[]",
            "options": [
                {"option": "Network augmentation", "type": "network", "npv_aud": _r.randint(2, 12) * 1_000_000},
                {"option": "Demand management", "type": "non_network", "npv_aud": _r.randint(500_000, 4_000_000)},
            ],
        })
    return {"constraints": mock, "count": len(mock)}


def _core_constraint_detail(constraint_id: str) -> Dict[str, Any]:
    """Constraint detail with options (network vs non-network NPV)."""
    rows = _query_gold(f"""
        SELECT constraint_id, zone_substation, constraint_type, limiting_asset,
               current_utilization_pct, breach_year_bau, breach_year_high, options_json
        FROM {_SCHEMA}.network_constraints_register
        WHERE constraint_id = '{constraint_id}'
    """)
    if rows:
        r = rows[0]
        try:
            r["options"] = json.loads(r.get("options_json", "[]"))
        except (json.JSONDecodeError, TypeError):
            r["options"] = []
        return r

    _r.seed(hash(constraint_id) % 10000)
    breach = _r.randint(2028, 2033)
    return {
        "constraint_id": constraint_id,
        "zone_substation": "ZS-MOCK-01",
        "constraint_type": "thermal",
        "limiting_asset": "Mock transformer",
        "current_utilization_pct": round(_r.uniform(80, 100), 1),
        "breach_year_bau": breach,
        "breach_year_high": breach - 2,
        "options": [
            {"option": "Network augmentation", "type": "network", "npv_aud": 8_000_000},
            {"option": "Demand management", "type": "non_network", "npv_aud": 3_000_000},
            {"option": "Battery storage", "type": "non_network", "npv_aud": 5_000_000},
        ],
    }


def _core_ev_impact(scenario: Optional[str] = None) -> Dict[str, Any]:
    """EV charging impact projections by scenario."""
    where = f"WHERE scenario = '{scenario}'" if scenario else ""
    rows = _query_gold(f"""
        SELECT feeder_id, scenario, target_year, peak_load_delta_mw,
               assets_at_risk, upgrade_required_flag
        FROM {_SCHEMA}.ev_network_impact
        {where}
        ORDER BY peak_load_delta_mw DESC
        LIMIT 200
    """)
    if rows:
        return {"impacts": rows, "count": len(rows)}

    _r.seed(503)
    scenarios = [scenario] if scenario else ["low", "medium", "high"]
    mock = []
    for sc in scenarios:
        mult = {"low": 0.3, "medium": 0.6, "high": 1.0}[sc]
        for i in range(10):
            for year in [2028, 2030, 2033]:
                delta = round(_r.uniform(0.2, 2.5) * mult * ((year - 2026) / 4), 2)
                mock.append({
                    "feeder_id": f"FDR-MOCK-{i+1:02d}",
                    "scenario": sc,
                    "target_year": year,
                    "peak_load_delta_mw": delta,
                    "assets_at_risk": _r.randint(0, 3) if delta > 1.0 else 0,
                    "upgrade_required_flag": delta > 1.0,
                })
    return {"impacts": mock, "count": len(mock)}


def _core_ev_impact_detail(feeder_id: str) -> Dict[str, Any]:
    """Single feeder EV impact detail."""
    rows = _query_gold(f"""
        SELECT scenario, target_year, peak_load_delta_mw,
               assets_at_risk, upgrade_required_flag
        FROM {_SCHEMA}.ev_network_impact
        WHERE feeder_id = '{feeder_id}'
        ORDER BY scenario, target_year
    """)
    if rows:
        return {"feeder_id": feeder_id, "projections": rows}

    _r.seed(hash(feeder_id) % 10000)
    mock = []
    for sc in ["low", "medium", "high"]:
        mult = {"low": 0.3, "medium": 0.6, "high": 1.0}[sc]
        for year in [2028, 2030, 2033, 2035]:
            delta = round(_r.uniform(0.2, 2.0) * mult * ((year - 2026) / 4), 2)
            mock.append({
                "scenario": sc, "target_year": year,
                "peak_load_delta_mw": delta,
                "assets_at_risk": _r.randint(0, 2) if delta > 0.8 else 0,
                "upgrade_required_flag": delta > 0.8,
            })
    return {"feeder_id": feeder_id, "projections": mock}


def _core_ev_profiles() -> Dict[str, Any]:
    """EV charging load profiles by type."""
    rows = _query_gold(f"""
        SELECT charge_point_type, hour_of_day,
               ROUND(AVG(load_kw_per_ev), 2) AS avg_load_kw
        FROM {_SCHEMA}.ev_charging_profiles
        GROUP BY charge_point_type, hour_of_day
        ORDER BY charge_point_type, hour_of_day
    """)
    if rows:
        return {"profiles": rows}

    mock = []
    for ctype in ["residential", "commercial", "fast_charger"]:
        for hour in range(24):
            if ctype == "residential":
                load = 3.5 if 17 <= hour <= 23 else (1.0 if 0 <= hour <= 6 else 0.5)
            elif ctype == "commercial":
                load = 11 if 8 <= hour <= 17 else 2.0
            else:
                load = _r.uniform(20, 50)
            mock.append({
                "charge_point_type": ctype,
                "hour_of_day": hour,
                "avg_load_kw": round(load, 2),
            })
    return {"profiles": mock}


def _core_augmentation_options() -> Dict[str, Any]:
    """Augmentation options ranked by NPV."""
    rows = _query_gold(f"""
        SELECT constraint_id, zone_substation, constraint_type,
               current_utilization_pct, breach_year_bau, options_json
        FROM {_SCHEMA}.network_constraints_register
        WHERE current_utilization_pct >= 75
        ORDER BY breach_year_bau ASC
        LIMIT 30
    """)
    if rows:
        options = []
        for r in rows:
            try:
                opts = json.loads(r.get("options_json", "[]"))
            except (json.JSONDecodeError, TypeError):
                opts = []
            for opt in opts:
                options.append({
                    "constraint_id": r["constraint_id"],
                    "zone_substation": r["zone_substation"],
                    "constraint_type": r["constraint_type"],
                    "breach_year": r["breach_year_bau"],
                    **opt,
                })
        options.sort(key=lambda x: x.get("npv_aud", 0))
        return {"options": options, "count": len(options)}

    _r.seed(505)
    mock = []
    for i in range(10):
        for opt_type, label in [("network", "Network augmentation"), ("non_network", "Demand management")]:
            mock.append({
                "constraint_id": f"CON-MOCK-{i+1:04d}",
                "zone_substation": f"ZS-MOCK-{i+1:02d}",
                "constraint_type": _r.choice(["thermal", "voltage"]),
                "breach_year": _r.randint(2027, 2033),
                "option": label,
                "type": opt_type,
                "npv_aud": _r.randint(1, 15) * 1_000_000,
            })
    mock.sort(key=lambda x: x["npv_aud"])
    return {"options": mock, "count": len(mock)}


def _core_planning_summary() -> Dict[str, Any]:
    """Planning summary KPIs."""
    cache_key = "planning_summary"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    constraints = _query_gold(f"SELECT COUNT(*) AS cnt FROM {_SCHEMA}.network_constraints_register")
    breach_2030 = _query_gold(f"""
        SELECT COUNT(*) AS cnt FROM {_SCHEMA}.network_constraints_register
        WHERE breach_year_bau <= 2030
    """)
    ev_growth = _query_gold(f"""
        SELECT ROUND(AVG(peak_load_delta_mw), 2) AS avg_delta
        FROM {_SCHEMA}.ev_network_impact
        WHERE scenario = 'high' AND target_year = 2030
    """)

    result = {
        "total_constraints": (constraints[0]["cnt"] if constraints else 50),
        "breach_by_2030": (breach_2030[0]["cnt"] if breach_2030 else 18),
        "avg_ev_impact_2030_mw": (ev_growth[0]["avg_delta"] if ev_growth and ev_growth[0].get("avg_delta") else 1.2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(cache_key, result, ttl_seconds=30)
    return result


# =========================================================================
# API Endpoints
# =========================================================================

@router.get("/api/planning/summary")
async def planning_summary():
    """Constraints count, breach-by-2030 count, EV growth rate."""
    return _core_planning_summary()


@router.get("/api/planning/demand-forecast")
async def demand_forecast(
    zone_substation: Optional[str] = Query(default=None),
    scenario: Optional[str] = Query(default=None),
):
    """10-year spatial demand forecast by zone sub + scenario."""
    return _core_demand_forecast(zone_substation, scenario)


@router.get("/api/planning/demand-forecast/{zone_substation}")
async def demand_forecast_detail(zone_substation: str):
    """Single zone sub multi-scenario forecast."""
    return _core_demand_forecast_detail(zone_substation)


@router.get("/api/planning/constraints")
async def constraints(constraint_type: Optional[str] = Query(default=None)):
    """Network constraint register."""
    return _core_constraints(constraint_type)


@router.get("/api/planning/constraints/{constraint_id}")
async def constraint_detail(constraint_id: str):
    """Constraint detail with options (network vs non-network NPV)."""
    return _core_constraint_detail(constraint_id)


@router.get("/api/planning/ev-impact")
async def ev_impact(scenario: Optional[str] = Query(default=None)):
    """EV charging impact projections by scenario."""
    return _core_ev_impact(scenario)


@router.get("/api/planning/ev-impact/{feeder_id}")
async def ev_impact_detail(feeder_id: str):
    """Single feeder EV impact detail."""
    return _core_ev_impact_detail(feeder_id)


@router.get("/api/planning/ev-profiles")
async def ev_profiles():
    """EV charging load profiles by type."""
    return _core_ev_profiles()


@router.get("/api/planning/augmentation-options")
async def augmentation_options():
    """Augmentation options ranked by NPV."""
    return _core_augmentation_options()
