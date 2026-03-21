"""Distributed Energy Resource Hosting Capacity Analysis.

Provides feeder-level hosting capacity assessments, DER penetration scenario
modelling, and curtailment event tracking to support network planning and
regulatory reporting for Australian distribution networks.
"""
from __future__ import annotations

import random as _r
from datetime import date, timedelta

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy", "SA Power Networks"]

_ZONE_SUBSTATIONS = [
    "Ringwood ZSS", "Lilydale ZSS", "Mooroolbark ZSS", "Croydon ZSS",
    "Warrandyte ZSS", "Eltham ZSS", "Doncaster ZSS", "Box Hill ZSS",
    "Blackburn ZSS", "Nunawading ZSS", "Mitcham ZSS", "Vermont ZSS",
]

_CONSTRAINT_TYPES = ["Thermal", "Voltage", "Protection", "None"]
_RISK_LEVELS = ["Critical", "High", "Medium", "Low"]
_CURTAILMENT_CAUSES = ["Voltage", "Thermal", "Protection"]


# =========================================================================
# GET /api/hosting-capacity/summary
# =========================================================================

@router.get("/api/hosting-capacity/summary")
async def hosting_capacity_summary() -> JSONResponse:
    """Network-wide DER hosting capacity summary and curtailment statistics."""
    _r.seed(780)
    total_feeders = 1_840
    constrained = _r.randint(220, 480)
    return JSONResponse({
        "total_feeders": total_feeders,
        "feeders_constrained": constrained,
        "constrained_pct": round(constrained / total_feeders * 100, 1),
        "total_hosting_capacity_mw": round(_r.uniform(2_400.0, 4_200.0), 1),
        "avg_capacity_utilisation_pct": round(_r.uniform(52.0, 78.0), 1),
        "der_registered_mw": round(_r.uniform(1_800.0, 3_100.0), 1),
        "der_queue_mw": round(_r.uniform(380.0, 920.0), 1),
        "curtailment_events_ytd": _r.randint(420, 1_800),
        "estimated_curtailed_mwh_ytd": round(_r.uniform(1_200.0, 6_800.0), 1),
        "data_source": "synthetic",
    })


# =========================================================================
# GET /api/hosting-capacity/feeders
# =========================================================================

@router.get("/api/hosting-capacity/feeders")
async def hosting_capacity_feeders() -> JSONResponse:
    """Feeder-level hosting capacity and DER constraint data."""
    _r.seed(781)
    feeders = []
    for i in range(30):
        zss = _r.choice(_ZONE_SUBSTATIONS)
        hc_kw = round(_r.uniform(800.0, 8_500.0), 1)
        der_kw = round(hc_kw * _r.uniform(0.15, 1.10), 1)
        util_pct = round(min(der_kw / hc_kw * 100, 115.0), 1)
        constraint = (
            "Thermal" if util_pct > 90 else
            "Voltage" if util_pct > 75 else
            "Protection" if util_pct > 60 else
            "None"
        )
        risk = (
            "Critical" if util_pct > 100 else
            "High" if util_pct > 85 else
            "Medium" if util_pct > 65 else
            "Low"
        )
        feeders.append({
            "feeder_id": f"FDR-{i+1:03d}",
            "zone_substation": zss,
            "hosting_capacity_kw": hc_kw,
            "current_der_kw": der_kw,
            "utilisation_pct": util_pct,
            "constraint_type": constraint,
            "risk_level": risk,
            "dynamic_oe_enabled": _r.random() > 0.55,
        })
    feeders.sort(key=lambda x: x["utilisation_pct"], reverse=True)
    return JSONResponse({"feeders": feeders, "count": len(feeders)})


# =========================================================================
# GET /api/hosting-capacity/scenarios
# =========================================================================

@router.get("/api/hosting-capacity/scenarios")
async def hosting_capacity_scenarios() -> JSONResponse:
    """DER penetration scenarios and network impact modelling."""
    _r.seed(782)
    scenarios = [
        ("Base Case",    12.0, 5.0,  8.0,  1),
        ("Moderate DER", 32.0, 18.0, 22.0, 3),
        ("High DER",     55.0, 38.0, 42.0, 5),
        ("Extreme DER",  80.0, 65.0, 68.0, 8),
    ]
    result = []
    for name, solar_pct, ev_pct, batt_pct, timeline in scenarios:
        constrained = int(_r.uniform(80, 160) * (1 + (solar_pct / 50)))
        curtailment = round(_r.uniform(500, 1_500) * (solar_pct / 30), 1)
        augmentation = round(_r.uniform(25.0, 80.0) * (solar_pct / 40), 1)
        result.append({
            "scenario_name": name,
            "solar_penetration_pct": solar_pct,
            "ev_penetration_pct": ev_pct,
            "battery_penetration_pct": batt_pct,
            "constrained_feeders": constrained,
            "curtailment_mwh_annual": curtailment,
            "network_augmentation_m": augmentation,
            "timeline_years": timeline,
        })
    return JSONResponse({"scenarios": result, "count": len(result)})


# =========================================================================
# GET /api/hosting-capacity/curtailment
# =========================================================================

@router.get("/api/hosting-capacity/curtailment")
async def hosting_capacity_curtailment() -> JSONResponse:
    """Recent DER curtailment events with cause and customer impact."""
    _r.seed(783)
    today = date.today()
    events = []
    for i in range(25):
        event_date = today - timedelta(days=_r.randint(1, 180))
        duration = _r.randint(8, 240)
        energy = round(duration * _r.uniform(8.0, 45.0), 1)
        revenue_loss = round(energy * _r.uniform(0.08, 0.22), 2)
        feeder_num = _r.randint(1, 30)
        events.append({
            "date": event_date.isoformat(),
            "feeder_id": f"FDR-{feeder_num:03d}",
            "duration_mins": duration,
            "energy_curtailed_kwh": energy,
            "cause": _r.choice(_CURTAILMENT_CAUSES),
            "customer_impact": _r.randint(12, 420),
            "revenue_loss_aud": revenue_loss,
        })
    events.sort(key=lambda x: x["date"], reverse=True)
    return JSONResponse({"events": events, "count": len(events)})
