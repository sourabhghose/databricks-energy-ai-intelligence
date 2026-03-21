"""Large Load Connection Pipeline — Data Centres, EVs, Green Hydrogen.

Tracks large-scale load connection applications, capacity outlook,
and revenue opportunities across Australian DNSP networks.
"""
from __future__ import annotations

import random as _r
from datetime import date, timedelta

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy"]
_LOAD_TYPES = ["Data Centre", "EV Charging Hub", "Battery Storage", "Green Hydrogen", "Industrial"]
_STATUSES = ["Enquiry", "Application Received", "Assessment", "Offer Issued", "Accepted", "In Construction", "Connected"]
_CONNECTION_VOLTAGES = [11.0, 22.0, 33.0, 66.0, 110.0, 132.0]

_CUSTOMER_NAMES = [
    "NextGen Data Centres Pty Ltd", "AGL Energy", "Origin Energy Storage",
    "Transgrid Partners", "H2 Green Ventures", "Tesla Energy Australia",
    "ENGIE Australia", "Ørsted Pacific", "Amazon AWS Infrastructure",
    "Microsoft Azure ANZ", "Rio Tinto Industrial", "BHP Energy Ops",
    "BP Australia EV Fleet", "Ampol Charge Network", "Fortescue Future Industries",
]


@router.get("/api/large-load/summary")
async def large_load_summary() -> JSONResponse:
    """Large load connection pipeline summary by technology type."""
    _r.seed(640)
    dc_mw = round(_r.uniform(400.0, 1800.0), 1)
    ev_mw = round(_r.uniform(200.0, 900.0), 1)
    bess_mw = round(_r.uniform(150.0, 600.0), 1)
    h2_mw = round(_r.uniform(50.0, 400.0), 1)
    other_mw = round(_r.uniform(100.0, 350.0), 1)
    total_mw = round(dc_mw + ev_mw + bess_mw + h2_mw + other_mw, 1)
    approved_mw = round(total_mw * _r.uniform(0.30, 0.55), 1)
    pending_mw = round(total_mw - approved_mw, 1)
    return JSONResponse({
        "total_applications": _r.randint(35, 120),
        "total_mw_requested": total_mw,
        "approved_mw": approved_mw,
        "pending_mw": pending_mw,
        "data_centre_mw": dc_mw,
        "ev_charging_mw": ev_mw,
        "battery_storage_mw": bess_mw,
        "green_hydrogen_mw": h2_mw,
        "data_source": "synthetic",
    })


@router.get("/api/large-load/pipeline")
async def large_load_pipeline() -> JSONResponse:
    """Large load connection application pipeline."""
    _r.seed(641)
    pipeline = []
    for i in range(30):
        load_type = _r.choice(_LOAD_TYPES)
        mw = round(_r.uniform(5.0, 200.0), 1)
        capex = round(mw * _r.uniform(0.3, 1.8), 2)
        conn_date = date.today() + timedelta(days=_r.randint(180, 1460))
        status = _r.choice(_STATUSES)
        pipeline.append({
            "application_id": f"LLC-{2026}-{i+1:04d}",
            "customer_name": _r.choice(_CUSTOMER_NAMES),
            "load_type": load_type,
            "dnsp": _r.choice(_DNSPS),
            "zone_substation": f"ZSS-{_r.randint(1, 50):02d}",
            "mw_requested": mw,
            "connection_voltage_kv": _r.choice(_CONNECTION_VOLTAGES),
            "status": status,
            "estimated_connection_date": conn_date.isoformat(),
            "capex_required_m_aud": capex,
        })
    return JSONResponse({"pipeline": pipeline, "count": len(pipeline)})


@router.get("/api/large-load/capacity-outlook")
async def large_load_capacity_outlook() -> JSONResponse:
    """Zone substation capacity outlook for large load uptake."""
    _r.seed(642)
    outlook = []
    for i in range(20):
        current_loading = round(_r.uniform(45.0, 105.0), 1)
        committed = round(_r.uniform(5.0, 80.0), 1)
        available = round(max(0.0, (100.0 - current_loading) / 100 * _r.uniform(50.0, 300.0) - committed), 1)
        aug_required = current_loading + committed / 10 > 90.0
        outlook.append({
            "zone_substation": f"ZSS-{i+1:02d}",
            "current_loading_pct": current_loading,
            "committed_load_mw": committed,
            "available_capacity_mw": available,
            "augmentation_required": aug_required,
            "augmentation_year": _r.randint(2027, 2031) if aug_required else None,
        })
    return JSONResponse({"outlook": outlook, "count": len(outlook)})


@router.get("/api/large-load/revenue-opportunity")
async def large_load_revenue_opportunity() -> JSONResponse:
    """Revenue opportunity by large load type."""
    _r.seed(643)
    by_type = []
    for load_type in _LOAD_TYPES:
        count = _r.randint(3, 25)
        total_mw = round(_r.uniform(50.0, 600.0), 1)
        annual_rev = round(total_mw * _r.uniform(0.08, 0.25), 2)
        by_type.append({
            "load_type": load_type,
            "count": count,
            "total_mw": total_mw,
            "annual_revenue_m_aud": annual_rev,
        })
    return JSONResponse({"by_type": by_type})
