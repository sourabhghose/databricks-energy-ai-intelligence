"""SF6 Gas Management & Environment, Safety & Quality (ESQ).

Tracks SF6 gas leakage rates, Scope 1/2 emissions, fleet electrification,
and net-zero progress for Australian DNSPs.
"""
from __future__ import annotations

import random as _r
from datetime import date, timedelta

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy"]
_SF6_ASSET_TYPES = [
    "Gas-Insulated Switchgear (GIS)", "SF6 Circuit Breaker", "SF6 Ring Main Unit",
    "Gas-Insulated Transformer", "SF6 Disconnector",
]
_RISK_LEVELS = ["Critical", "High", "Medium", "Low"]
_VEHICLE_TYPES = [
    "Utes & Light Commercial", "Dual-Cab 4WDs", "Heavy Line Trucks",
    "Management Sedans", "Specialist Plant",
]


@router.get("/api/esq/summary")
async def esq_summary() -> JSONResponse:
    """ESQ headline — SF6 inventory, emissions, fleet EV %, and net-zero target."""
    _r.seed(680)
    total_sf6 = round(_r.uniform(15000.0, 60000.0), 1)
    leakage = round(total_sf6 * _r.uniform(0.005, 0.025), 1)
    return JSONResponse({
        "sf6_total_kg": total_sf6,
        "sf6_leakage_kg_ytd": leakage,
        "leakage_rate_pct": round(leakage / total_sf6 * 100, 3),
        "scope1_emissions_tco2e": round(_r.uniform(8000.0, 35000.0), 1),
        "scope2_emissions_tco2e": round(_r.uniform(15000.0, 75000.0), 1),
        "fleet_ev_pct": round(_r.uniform(4.0, 22.0), 1),
        "renewable_office_pct": round(_r.uniform(45.0, 100.0), 1),
        "net_zero_target_year": _r.choice([2040, 2045, 2050]),
        "data_source": "synthetic",
    })


@router.get("/api/esq/sf6")
async def esq_sf6() -> JSONResponse:
    """SF6 asset inventory and leakage rates by asset type and DNSP."""
    _r.seed(681)
    today_dt = date.today()
    sf6_assets = []
    for d in _DNSPS:
        for atype in _SF6_ASSET_TYPES:
            count = _r.randint(5, 120)
            total_gas = round(count * _r.uniform(2.0, 80.0), 1)
            leakage = round(total_gas * _r.uniform(0.002, 0.030), 2)
            last_test = today_dt - timedelta(days=_r.randint(30, 400))
            risk = "Critical" if leakage / total_gas > 0.025 else (
                "High" if leakage / total_gas > 0.015 else (
                    "Medium" if leakage / total_gas > 0.008 else "Low"
                )
            )
            sf6_assets.append({
                "asset_type": atype,
                "dnsp": d,
                "count": count,
                "total_gas_kg": total_gas,
                "leakage_ytd_kg": leakage,
                "leakage_rate_pct": round(leakage / total_gas * 100, 3),
                "last_test_date": last_test.isoformat(),
                "risk_level": risk,
            })
    return JSONResponse({"sf6_assets": sf6_assets, "count": len(sf6_assets)})


@router.get("/api/esq/emissions-trend")
async def esq_emissions_trend() -> JSONResponse:
    """5-year Scope 1 and Scope 2 emissions trend."""
    _r.seed(682)
    trend = []
    customers = 1_200_000
    for yr in range(2022, 2027):
        scope1 = round(_r.uniform(12000.0, 28000.0), 1)
        scope2 = round(_r.uniform(20000.0, 65000.0), 1)
        total = round(scope1 + scope2, 1)
        trend.append({
            "year": yr,
            "scope1_tco2e": scope1,
            "scope2_tco2e": scope2,
            "total_tco2e": total,
            "intensity_tco2e_per_customer": round(total / customers, 4),
        })
    return JSONResponse({"trend": trend})


@router.get("/api/esq/fleet")
async def esq_fleet() -> JSONResponse:
    """Fleet electrification status and CO2e savings."""
    _r.seed(683)
    fleet = []
    for vtype in _VEHICLE_TYPES:
        total = _r.randint(50, 800)
        ev_count = int(total * _r.uniform(0.02, 0.25))
        avg_age = round(_r.uniform(3.5, 9.0), 1)
        co2e_saved = round(ev_count * _r.uniform(1.5, 6.0), 1)
        repl_year = _r.randint(2027, 2032)
        fleet.append({
            "vehicle_type": vtype,
            "total_count": total,
            "ev_count": ev_count,
            "ev_pct": round(ev_count / total * 100, 1),
            "avg_age_years": avg_age,
            "annual_co2e_saved_t": co2e_saved,
            "replacement_plan_year": repl_year,
        })
    return JSONResponse({"fleet": fleet})
