"""DAPR (Distribution Annual Planning Report) Assembly & Submission Tracking.

Manages the assembly, validation, and progress tracking of the DAPR submission
for the 2025-2030 regulatory period, including demand forecasting, network
capability statements, bulk supply point headroom, and augmentation pipelines.
"""
from __future__ import annotations

import random as _r
from datetime import date

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy", "SA Power Networks"]

_BSP_NAMES = [
    "Ringwood BSP", "Lilydale BSP", "Box Hill BSP", "Doncaster BSP",
    "Nunawading BSP", "Croydon BSP", "Belgrave BSP", "Ferntree Gully BSP",
    "Bayswater BSP", "Mooroolbark BSP", "Healesville BSP", "Warburton BSP",
    "Yarra Glen BSP", "Diamond Creek BSP", "Eltham BSP", "Greensborough BSP",
    "Heidelberg BSP", "Preston BSP", "Reservoir BSP", "Thomastown BSP",
]

_PROJECT_NAMES = [
    "Ringwood 220/66kV Transformer Augmentation",
    "Lilydale 66/22kV Switchgear Replacement",
    "Box Hill Cable Augmentation Stage 2",
    "Doncaster Zone Substation Upgrade",
    "Nunawading DER Export Management System",
    "Bayswater 66kV Feeder Reinforcement",
    "Eastern Corridor Cable Upgrade",
    "Yarra Valley Bushfire Hardening Program",
    "Metro DER Integration Pilot",
    "Belgrave Voltage Management Augmentation",
]

_PROJECT_TYPES = ["Augmentation", "Replacement", "DER Integration"]
_TRIGGERS = ["Demand Growth", "N-1 Compliance", "DER Hosting"]
_STATUSES = ["Committed", "Identified", "Under Review"]
_ZONES = ["Metro", "Inner Regional", "Outer Regional"]


# =========================================================================
# GET /api/dapr-assembly/summary
# =========================================================================

@router.get("/api/dapr-assembly/summary")
async def dapr_assembly_summary() -> JSONResponse:
    """DAPR submission progress summary for the 2025-2030 regulatory period."""
    _r.seed(840)
    complete = _r.randint(4, 6)
    total = 8
    overall_pct = round(complete / total * 100 * _r.uniform(0.90, 1.05), 1)
    overall_pct = min(overall_pct, 100.0)

    def _status() -> str:
        return _r.choice(["Complete", "In Progress"])

    return JSONResponse({
        "dnsp_name": "AusNet Services",
        "regulatory_period": "2025-2030",
        "submission_due_date": "2026-06-30",
        "sections_complete": complete,
        "sections_total": total,
        "overall_completion_pct": overall_pct,
        "demand_forecast_status": "Complete",
        "network_capability_status": _status(),
        "augmentation_pipeline_status": _status(),
        "der_integration_status": "In Progress",
        "last_updated": date.today().isoformat(),
        "data_source": "synthetic",
    })


# =========================================================================
# GET /api/dapr-assembly/demand-forecast
# =========================================================================

@router.get("/api/dapr-assembly/demand-forecast")
async def dapr_assembly_demand_forecast() -> JSONResponse:
    """Five-year demand forecast by zone for DAPR submission."""
    _r.seed(841)
    base_year = 2025
    zone_params = [
        ("Metro",          8_200.0, 28.4, 1.8),
        ("Inner Regional", 3_400.0, 11.2, 1.2),
        ("Outer Regional", 1_800.0,  5.8, 0.9),
        ("Metro",          8_400.0, 29.0, 2.1),
        ("Inner Regional", 3_500.0, 11.5, 1.4),
    ]
    forecasts = []
    for i, (zone, base_peak, base_energy, growth) in enumerate(zone_params):
        year = base_year + i
        peak = round(base_peak * (1 + growth / 100) ** i * _r.uniform(0.97, 1.03), 1)
        energy = round(base_energy * (1 + growth / 100) ** i * _r.uniform(0.97, 1.03), 2)
        der_offset = round(peak * _r.uniform(0.06, 0.18), 1)
        net_peak = round(peak - der_offset, 1)
        confidence = round(_r.uniform(80.0, 95.0), 1)
        forecasts.append({
            "year": year,
            "zone_name": zone,
            "peak_demand_mw": peak,
            "energy_twh": energy,
            "growth_pct": round(growth * _r.uniform(0.8, 1.2), 2),
            "der_offset_mw": der_offset,
            "net_peak_demand_mw": net_peak,
            "confidence_interval_pct": confidence,
        })
    return JSONResponse({"demand_forecast": forecasts, "count": len(forecasts)})


# =========================================================================
# GET /api/dapr-assembly/network-capability
# =========================================================================

@router.get("/api/dapr-assembly/network-capability")
async def dapr_assembly_network_capability() -> JSONResponse:
    """Bulk supply point network capability and N-1 headroom assessment."""
    _r.seed(842)
    bsps = []
    for i, bsp_name in enumerate(_BSP_NAMES):
        zone = _r.choice(_ZONES)
        capacity_mva = _r.choice([60.0, 120.0, 180.0, 240.0, 320.0])
        peak_demand = round(capacity_mva * _r.uniform(0.50, 0.98), 1)
        headroom = round(capacity_mva - peak_demand, 1)
        headroom_pct = round(headroom / capacity_mva * 100, 1)
        n1_compliant = headroom_pct > 15.0
        aug_required = headroom_pct < 12.0
        aug_year = (date.today().year + _r.randint(1, 5)) if aug_required else None
        bsps.append({
            "bsp_name": bsp_name,
            "zone": zone,
            "current_capacity_mva": capacity_mva,
            "peak_demand_mva": peak_demand,
            "headroom_mva": headroom,
            "headroom_pct": headroom_pct,
            "n1_compliant": n1_compliant,
            "augmentation_required": aug_required,
            "augmentation_year": aug_year,
        })
    bsps.sort(key=lambda x: x["headroom_pct"])
    return JSONResponse({"bulk_supply_points": bsps, "count": len(bsps)})


# =========================================================================
# GET /api/dapr-assembly/augmentation-pipeline
# =========================================================================

@router.get("/api/dapr-assembly/augmentation-pipeline")
async def dapr_assembly_augmentation_pipeline() -> JSONResponse:
    """Capital augmentation project pipeline for the DAPR regulatory period."""
    _r.seed(843)
    projects = []
    for i, name in enumerate(_PROJECT_NAMES):
        proj_type = _r.choice(_PROJECT_TYPES)
        trigger = _r.choice(_TRIGGERS)
        capex = round(_r.uniform(8.0, 145.0), 1)
        status = (
            "Committed" if capex > 80 else
            "Identified" if capex > 30 else
            "Under Review"
        )
        in_service = date.today().year + _r.randint(1, 5)
        aer_notifiable = capex > 50.0 or trigger == "N-1 Compliance"
        projects.append({
            "project_name": name,
            "project_type": proj_type,
            "trigger": trigger,
            "capex_m": capex,
            "status": status,
            "in_service_year": in_service,
            "aer_notifiable": aer_notifiable,
        })
    projects.sort(key=lambda x: x["capex_m"], reverse=True)
    return JSONResponse({"projects": projects, "count": len(projects)})
