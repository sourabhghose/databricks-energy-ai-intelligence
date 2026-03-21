"""Fleet Electrification — EV Transition Program Tracking.

Monitors DNSP fleet electrification progress, charging infrastructure,
vehicle category breakdowns, and the transition roadmap to 2030.
"""
from __future__ import annotations

import random as _r

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy"]
_VEHICLE_CATEGORIES = [
    "Utes & Vans", "Trucks & HGV", "Field Crew Vehicles",
    "Management Cars", "Specialist Equipment",
]
_CHARGER_TYPES = ["AC Level 2 (7kW)", "AC Level 2 (22kW)", "DC Fast (50kW)", "DC Rapid (150kW)"]


@router.get("/api/fleet/summary")
async def fleet_summary() -> JSONResponse:
    """Fleet electrification headline KPIs."""
    _r.seed(700)
    total = _r.randint(1800, 5500)
    ev_count = int(total * _r.uniform(0.06, 0.22))
    return JSONResponse({
        "total_vehicles": total,
        "ev_count": ev_count,
        "ev_pct_target_2030": round(_r.uniform(65.0, 100.0), 1),
        "charging_sites": _r.randint(20, 85),
        "annual_fuel_cost_m_aud_saved": round(ev_count * _r.uniform(0.003, 0.012), 3),
        "co2e_saved_t_ytd": round(ev_count * _r.uniform(1.2, 5.5), 1),
        "data_source": "synthetic",
    })


@router.get("/api/fleet/vehicles")
async def fleet_vehicles() -> JSONResponse:
    """Fleet breakdown by vehicle category."""
    _r.seed(701)
    vehicles = []
    for category in _VEHICLE_CATEGORIES:
        total = _r.randint(80, 1200)
        ev = int(total * _r.uniform(0.02, 0.30))
        hybrid = int(total * _r.uniform(0.05, 0.20))
        ice = total - ev - hybrid
        repl_3yr = int(total * _r.uniform(0.10, 0.35))
        vehicles.append({
            "category": category,
            "total": total,
            "ev": ev,
            "hybrid": hybrid,
            "ice": ice,
            "ev_pct": round(ev / total * 100, 1),
            "avg_age_years": round(_r.uniform(3.0, 9.5), 1),
            "replacement_due_3yr": repl_3yr,
        })
    return JSONResponse({"vehicles": vehicles})


@router.get("/api/fleet/charging-network")
async def fleet_charging_network() -> JSONResponse:
    """EV charging site infrastructure details."""
    _r.seed(702)
    site_names = [
        "Melbourne Depot — Sunshine", "Brisbane Depot — Eagle Farm", "Sydney Depot — Rydalmere",
        "Adelaide Depot — Regency Park", "Perth Depot — Welshpool", "Geelong Field Base",
        "Toowoomba Regional Depot", "Newcastle Operations Centre", "Ballarat Zone Depot",
        "Gold Coast Service Centre", "Bendigo Field Depot", "Townsville Depot",
    ]
    sites = []
    for i, site in enumerate(site_names):
        chargers = _r.randint(4, 32)
        capacity_kw = chargers * _r.choice([7, 22, 50, 150])
        sites.append({
            "site_name": site,
            "dnsp": _r.choice(_DNSPS),
            "chargers": chargers,
            "capacity_kw": capacity_kw,
            "utilisation_pct": round(_r.uniform(25.0, 88.0), 1),
            "vehicles_supported": _r.randint(chargers * 2, chargers * 6),
        })
    return JSONResponse({"sites": sites, "count": len(sites)})


@router.get("/api/fleet/transition-plan")
async def fleet_transition_plan() -> JSONResponse:
    """EV fleet transition roadmap from current year to 2030."""
    _r.seed(703)
    plan = []
    cumulative_ev = _r.randint(150, 400)
    total_fleet = _r.randint(2800, 4500)
    for yr in range(2026, 2031):
        ev_additions = _r.randint(80, 350)
        ice_retirements = _r.randint(60, 250)
        cumulative_ev += ev_additions
        ev_pct = round(cumulative_ev / total_fleet * 100, 1)
        capex = round(ev_additions * _r.uniform(0.045, 0.120), 2)
        opex_saving = round(ev_additions * _r.uniform(0.003, 0.010), 3)
        plan.append({
            "year": yr,
            "ev_additions": ev_additions,
            "ice_retirements": ice_retirements,
            "cumulative_ev_count": cumulative_ev,
            "ev_pct": min(ev_pct, 100.0),
            "capex_m_aud": capex,
            "opex_saving_m_aud": opex_saving,
        })
    return JSONResponse({"plan": plan})
