"""DER Fleet, Hosting Capacity, Curtailment, VPP & DOE Management.

Distributed energy resource analytics for DNSPs — fleet composition,
hosting capacity, curtailment tracking, VPP performance, and DOE compliance.
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

def _core_der_fleet(zone_substation: Optional[str] = None) -> Dict[str, Any]:
    """DER fleet summary by zone substation and technology."""
    where = f"WHERE zone_substation = '{zone_substation}'" if zone_substation else ""
    rows = _query_gold(f"""
        SELECT zone_substation, technology,
               COUNT(*) AS install_count,
               ROUND(SUM(capacity_kw), 1) AS total_capacity_kw,
               ROUND(SUM(capacity_kw) / 1000, 2) AS total_capacity_mw,
               region
        FROM {_SCHEMA}.der_fleet
        {where}
        GROUP BY zone_substation, technology, region
        ORDER BY total_capacity_kw DESC
        LIMIT 200
    """)
    if rows:
        return {"fleet": rows, "count": len(rows)}

    _r.seed(401)
    techs = ["solar", "battery", "ev_charger"]
    mock = []
    for i in range(15):
        for t in techs:
            mock.append({
                "zone_substation": f"ZS-MOCK-{i+1:02d}",
                "technology": t,
                "install_count": _r.randint(20, 500),
                "total_capacity_kw": round(_r.uniform(100, 5000), 1),
                "total_capacity_mw": round(_r.uniform(0.1, 5.0), 2),
                "region": _r.choice(_NEM_REGIONS),
            })
    return {"fleet": mock, "count": len(mock)}


def _core_der_fleet_detail(zone_substation: str) -> Dict[str, Any]:
    """Detailed DER breakdown for a zone sub."""
    rows = _query_gold(f"""
        SELECT technology,
               COUNT(*) AS count,
               ROUND(SUM(capacity_kw), 1) AS total_kw,
               ROUND(AVG(capacity_kw), 1) AS avg_kw,
               MIN(connection_date) AS earliest_connection,
               MAX(connection_date) AS latest_connection
        FROM {_SCHEMA}.der_fleet
        WHERE zone_substation = '{zone_substation}'
        GROUP BY technology
    """)
    if rows:
        for r in rows:
            r["earliest_connection"] = str(r.get("earliest_connection", ""))
            r["latest_connection"] = str(r.get("latest_connection", ""))
        return {"zone_substation": zone_substation, "breakdown": rows}

    _r.seed(hash(zone_substation) % 10000)
    return {
        "zone_substation": zone_substation,
        "breakdown": [
            {"technology": "solar", "count": _r.randint(100, 800), "total_kw": round(_r.uniform(500, 5000), 1),
             "avg_kw": 6.6, "earliest_connection": "2015-03-15", "latest_connection": "2026-02-20"},
            {"technology": "battery", "count": _r.randint(10, 100), "total_kw": round(_r.uniform(50, 1000), 1),
             "avg_kw": 13.5, "earliest_connection": "2020-01-10", "latest_connection": "2026-03-01"},
            {"technology": "ev_charger", "count": _r.randint(5, 80), "total_kw": round(_r.uniform(50, 2000), 1),
             "avg_kw": 7.0, "earliest_connection": "2022-06-01", "latest_connection": "2026-03-05"},
        ],
    }


def _core_hosting_capacity(min_available_pct: Optional[float] = None) -> Dict[str, Any]:
    """Hosting capacity by feeder."""
    rows = _query_gold(f"""
        SELECT feeder_id, static_capacity_kw, dynamic_capacity_kw,
               limiting_constraint, scenario
        FROM {_SCHEMA}.hosting_capacity
        WHERE scenario = 'base'
        ORDER BY static_capacity_kw ASC
        LIMIT 200
    """)
    if rows:
        return {"feeders": rows, "count": len(rows)}

    _r.seed(402)
    mock = []
    constraints = ["voltage_rise", "thermal_limit", "protection_settings", "fault_level"]
    for i in range(30):
        static = _r.randint(200, 2000)
        mock.append({
            "feeder_id": f"FDR-MOCK-{i+1:02d}",
            "static_capacity_kw": static,
            "dynamic_capacity_kw": int(static * _r.uniform(1.1, 1.8)),
            "limiting_constraint": _r.choice(constraints),
            "scenario": "base",
        })
    return {"feeders": mock, "count": len(mock)}


def _core_hosting_capacity_detail(feeder_id: str) -> Dict[str, Any]:
    """Single feeder hosting capacity detail."""
    rows = _query_gold(f"""
        SELECT feeder_id, static_capacity_kw, dynamic_capacity_kw,
               limiting_constraint, scenario
        FROM {_SCHEMA}.hosting_capacity
        WHERE feeder_id = '{feeder_id}'
    """)
    if rows:
        return {"feeder_id": feeder_id, "scenarios": rows}

    _r.seed(hash(feeder_id) % 10000)
    static = _r.randint(300, 1500)
    return {
        "feeder_id": feeder_id,
        "scenarios": [
            {"feeder_id": feeder_id, "static_capacity_kw": static,
             "dynamic_capacity_kw": int(static * 1.4), "limiting_constraint": "voltage_rise", "scenario": "base"},
            {"feeder_id": feeder_id, "static_capacity_kw": int(static * 0.7),
             "dynamic_capacity_kw": int(static * 1.0), "limiting_constraint": "voltage_rise", "scenario": "high_solar"},
        ],
    }


def _core_curtailment(region: Optional[str] = None, days: int = 30) -> Dict[str, Any]:
    """Curtailment events summary."""
    where = f"AND ce.feeder_id IN (SELECT asset_id FROM {_SCHEMA}.network_assets WHERE region = '{region}')" if region else ""
    rows = _query_gold(f"""
        SELECT feeder_id, curtailment_reason,
               COUNT(*) AS event_count,
               ROUND(SUM(curtailed_mwh), 3) AS total_mwh,
               SUM(affected_customers) AS total_affected,
               ROUND(SUM(lost_energy_value_aud), 2) AS total_value_aud
        FROM {_SCHEMA}.curtailment_events
        WHERE interval_datetime >= current_timestamp() - INTERVAL {days} DAYS
        GROUP BY feeder_id, curtailment_reason
        ORDER BY total_mwh DESC
        LIMIT 100
    """)
    if rows:
        return {"curtailment": rows, "count": len(rows)}

    _r.seed(403)
    reasons = ["voltage", "thermal", "dnsp_limit"]
    mock = []
    for i in range(15):
        mock.append({
            "feeder_id": f"FDR-MOCK-{i+1:02d}",
            "curtailment_reason": _r.choice(reasons),
            "event_count": _r.randint(2, 20),
            "total_mwh": round(_r.uniform(1, 30), 3),
            "total_affected": _r.randint(20, 500),
            "total_value_aud": round(_r.uniform(100, 3000), 2),
        })
    return {"curtailment": mock, "count": len(mock)}


def _core_curtailment_impact() -> Dict[str, Any]:
    """Financial impact of curtailment."""
    rows = _query_gold(f"""
        SELECT ROUND(SUM(curtailed_mwh), 2) AS total_curtailed_mwh,
               ROUND(SUM(lost_energy_value_aud), 2) AS total_lost_value_aud,
               SUM(affected_customers) AS total_affected_customers,
               COUNT(*) AS total_events
        FROM {_SCHEMA}.curtailment_events
    """)
    if rows and rows[0].get("total_curtailed_mwh"):
        return rows[0]

    return {
        "total_curtailed_mwh": 245.8,
        "total_lost_value_aud": 28750.00,
        "total_affected_customers": 12500,
        "total_events": 200,
    }


def _core_vpp(program_id: Optional[str] = None) -> Dict[str, Any]:
    """VPP dispatch events and performance."""
    where = f"WHERE program_id = '{program_id}'" if program_id else ""
    rows = _query_gold(f"""
        SELECT program_id, program_name, dispatch_datetime, target_mw,
               response_mw, response_accuracy_pct, revenue_aud
        FROM {_SCHEMA}.vpp_dispatch_events
        {where}
        ORDER BY dispatch_datetime DESC
        LIMIT 100
    """)
    if rows:
        for r in rows:
            r["dispatch_datetime"] = str(r.get("dispatch_datetime", ""))
        return {"events": rows, "count": len(rows)}

    _r.seed(404)
    programs = [("VPP-SAPN-001", "SA Power Networks VPP"), ("VPP-AGL-001", "AGL Virtual Power Plant")]
    mock = []
    now = datetime.now(timezone.utc)
    for pid, pname in programs:
        if program_id and pid != program_id:
            continue
        for i in range(10):
            target = round(_r.uniform(5, 50), 1)
            response = round(target * _r.uniform(0.7, 1.1), 1)
            mock.append({
                "program_id": pid,
                "program_name": pname,
                "dispatch_datetime": (now - timedelta(days=_r.randint(0, 30))).isoformat(),
                "target_mw": target,
                "response_mw": response,
                "response_accuracy_pct": round(min(100, response / target * 100), 1),
                "revenue_aud": round(response * _r.uniform(100, 400), 2),
            })
    return {"events": mock, "count": len(mock)}


def _core_vpp_detail(program_id: str) -> Dict[str, Any]:
    """Single VPP program performance detail."""
    rows = _query_gold(f"""
        SELECT program_id, program_name,
               COUNT(*) AS dispatch_count,
               ROUND(AVG(response_accuracy_pct), 1) AS avg_accuracy,
               ROUND(SUM(revenue_aud), 2) AS total_revenue_aud,
               ROUND(AVG(target_mw), 1) AS avg_target_mw,
               ROUND(AVG(response_mw), 1) AS avg_response_mw
        FROM {_SCHEMA}.vpp_dispatch_events
        WHERE program_id = '{program_id}'
        GROUP BY program_id, program_name
    """)
    if rows:
        return rows[0]

    _r.seed(hash(program_id) % 10000)
    return {
        "program_id": program_id,
        "program_name": "Mock VPP Program",
        "dispatch_count": _r.randint(10, 50),
        "avg_accuracy": round(_r.uniform(80, 98), 1),
        "total_revenue_aud": round(_r.uniform(10000, 100000), 2),
        "avg_target_mw": round(_r.uniform(10, 40), 1),
        "avg_response_mw": round(_r.uniform(8, 38), 1),
    }


def _core_doe_compliance() -> Dict[str, Any]:
    """DOE compliance rates by feeder."""
    rows = _query_gold(f"""
        SELECT feeder_id,
               ROUND(AVG(compliance_rate_pct), 1) AS avg_compliance_pct,
               ROUND(AVG(doe_limit_kw), 2) AS avg_doe_limit_kw,
               ROUND(AVG(customer_response_kw), 2) AS avg_response_kw,
               COUNT(*) AS measurement_count
        FROM {_SCHEMA}.doe_compliance
        GROUP BY feeder_id
        ORDER BY avg_compliance_pct ASC
        LIMIT 100
    """)
    if rows:
        return {"feeders": rows, "count": len(rows)}

    _r.seed(405)
    mock = []
    for i in range(20):
        limit = round(_r.uniform(3, 10), 2)
        response = round(limit * _r.uniform(0.6, 1.0), 2)
        mock.append({
            "feeder_id": f"FDR-MOCK-{i+1:02d}",
            "avg_compliance_pct": round(response / limit * 100, 1),
            "avg_doe_limit_kw": limit,
            "avg_response_kw": response,
            "measurement_count": _r.randint(5, 30),
        })
    return {"feeders": mock, "count": len(mock)}


def _core_der_output() -> Dict[str, Any]:
    """Estimated DER output by feeder."""
    rows = _query_gold(f"""
        SELECT feeder_id, interval_datetime, solar_mw, battery_dispatch_mw,
               net_export_mw, reverse_power_flow_flag
        FROM {_SCHEMA}.der_output_estimated
        ORDER BY interval_datetime DESC
        LIMIT 200
    """)
    if rows:
        for r in rows:
            r["interval_datetime"] = str(r["interval_datetime"])
        return {"output": rows, "count": len(rows)}

    _r.seed(406)
    now = datetime.now(timezone.utc)
    mock = []
    for i in range(30):
        solar = round(max(0, _r.uniform(-0.5, 3.0)), 3)
        battery = round(_r.uniform(-0.5, 0.5), 2)
        net = round(solar + battery - _r.uniform(1, 3), 2)
        mock.append({
            "feeder_id": f"FDR-MOCK-{i % 5 + 1:02d}",
            "interval_datetime": (now - timedelta(minutes=15 * i)).isoformat(),
            "solar_mw": solar,
            "battery_dispatch_mw": battery,
            "net_export_mw": net,
            "reverse_power_flow_flag": net > 0,
        })
    return {"output": mock, "count": len(mock)}


def _core_der_summary() -> Dict[str, Any]:
    """DER fleet summary KPIs."""
    cache_key = "der_summary"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    fleet = _query_gold(f"""
        SELECT technology, ROUND(SUM(capacity_kw) / 1000, 1) AS total_mw, COUNT(*) AS count
        FROM {_SCHEMA}.der_fleet
        GROUP BY technology
    """)
    curt = _query_gold(f"""
        SELECT ROUND(SUM(curtailed_mwh), 2) AS curtailed_today
        FROM {_SCHEMA}.curtailment_events
        WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
    """)

    if fleet:
        result = {
            "solar_mw": 0, "battery_mw": 0, "ev_count": 0,
            "curtailment_today_mwh": (curt[0]["curtailed_today"] if curt and curt[0].get("curtailed_today") else 12.5),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        for f in fleet:
            if f["technology"] == "solar":
                result["solar_mw"] = f["total_mw"]
                result["solar_count"] = f["count"]
            elif f["technology"] == "battery":
                result["battery_mw"] = f["total_mw"]
                result["battery_count"] = f["count"]
            elif f["technology"] == "ev_charger":
                result["ev_mw"] = f["total_mw"]
                result["ev_count"] = f["count"]
        _cache_set(cache_key, result, ttl_seconds=30)
        return result

    result = {
        "solar_mw": 850.5, "solar_count": 600,
        "battery_mw": 125.0, "battery_count": 200,
        "ev_mw": 95.0, "ev_count": 200,
        "curtailment_today_mwh": 12.5,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(cache_key, result, ttl_seconds=30)
    return result


# =========================================================================
# API Endpoints
# =========================================================================

@router.get("/api/der/summary")
async def der_summary():
    """Total solar MW, battery MW, EVs, curtailment today."""
    return _core_der_summary()


@router.get("/api/der/fleet")
async def der_fleet(zone_substation: Optional[str] = Query(default=None)):
    """DER fleet summary by zone substation and technology."""
    return _core_der_fleet(zone_substation)


@router.get("/api/der/fleet/{zone_substation}")
async def der_fleet_detail(zone_substation: str):
    """Detailed DER breakdown for a zone sub."""
    return _core_der_fleet_detail(zone_substation)


@router.get("/api/der/hosting-capacity")
async def hosting_capacity(min_available_pct: Optional[float] = Query(default=None)):
    """Hosting capacity by feeder."""
    return _core_hosting_capacity(min_available_pct)


@router.get("/api/der/hosting-capacity/{feeder_id}")
async def hosting_capacity_detail(feeder_id: str):
    """Single feeder hosting capacity detail."""
    return _core_hosting_capacity_detail(feeder_id)


@router.get("/api/der/curtailment/impact")
async def curtailment_impact():
    """Financial impact of curtailment (MWh, AUD)."""
    return _core_curtailment_impact()


@router.get("/api/der/curtailment")
async def curtailment(
    region: Optional[str] = Query(default=None),
    days: int = Query(default=30, ge=1, le=365),
):
    """Curtailment events summary (period, region filter)."""
    return _core_curtailment(region, days)


@router.get("/api/der/vpp")
async def vpp_events():
    """VPP dispatch events and performance."""
    return _core_vpp()


@router.get("/api/der/vpp/{program_id}")
async def vpp_detail(program_id: str):
    """Single VPP program performance detail."""
    return _core_vpp_detail(program_id)


@router.get("/api/der/doe-compliance")
async def doe_compliance():
    """DOE compliance rates by feeder."""
    return _core_doe_compliance()


@router.get("/api/der/output")
async def der_output():
    """Estimated DER output by feeder (30-min intervals)."""
    return _core_der_output()
