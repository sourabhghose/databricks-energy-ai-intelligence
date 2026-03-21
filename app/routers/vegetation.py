"""Vegetation Management — Line Clearance & Contractor Performance.

Tracking vegetation clearance compliance, contractor utilisation,
and scheduled works across DNSP distribution networks.
"""
from __future__ import annotations

import random as _r
from datetime import date, timedelta

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()

_CONTRACTORS = [
    "Ventia", "Programmed Maintenance", "Downer EDI", "Delta Utilities", "Arborgen",
]
_PRIORITIES = ["P1 - Urgent", "P2 - High", "P3 - Routine", "P4 - Scheduled"]
_STATUSES = ["Completed", "In Progress", "Scheduled", "Overdue"]
_RISK_LEVELS = ["Critical", "High", "Medium", "Low"]


@router.get("/api/vegetation/summary")
async def vegetation_summary() -> JSONResponse:
    """Vegetation clearance compliance summary."""
    _r.seed(520)
    total_spans = round(_r.uniform(18000.0, 32000.0), 1)
    compliant_pct = _r.uniform(76.0, 94.0)
    compliant_km = round(total_spans * compliant_pct / 100, 1)
    non_compliant = _r.randint(200, 1200)
    return JSONResponse({
        "total_spans_km": total_spans,
        "compliant_spans_km": compliant_km,
        "non_compliant_spans": non_compliant,
        "overdue_inspections": _r.randint(50, 350),
        "contractor_utilisation_pct": round(_r.uniform(72.0, 96.0), 1),
        "forecast_completion_date": (date.today() + timedelta(days=_r.randint(30, 180))).isoformat(),
        "data_source": "synthetic",
    })


@router.get("/api/vegetation/by-zone")
async def vegetation_by_zone(
    dnsp: str = Query("AusNet Services", description="DNSP filter"),
) -> JSONResponse:
    """Vegetation compliance by zone."""
    _r.seed(521)
    zones = []
    zone_names = [
        "Northern Metro", "Eastern Suburbs", "Southern Rural", "Western Plains",
        "Coastal Zone", "Highland Region", "CBD & Inner", "Outer Growth",
        "Industrial Belt", "Peri-Urban Fringe",
    ]
    for zone in zone_names:
        total_km = round(_r.uniform(500.0, 4500.0), 1)
        comp_pct = round(_r.uniform(65.0, 98.0), 1)
        compliant_km = round(total_km * comp_pct / 100, 1)
        next_due = date.today() + timedelta(days=_r.randint(-14, 90))
        zones.append({
            "zone": zone,
            "total_km": total_km,
            "compliant_km": compliant_km,
            "compliance_pct": comp_pct,
            "next_inspection_due": next_due.isoformat(),
            "contractor": _r.choice(_CONTRACTORS),
            "priority": _r.choice(_PRIORITIES),
        })
    return JSONResponse({"zones": zones, "dnsp": dnsp})


@router.get("/api/vegetation/contractor-performance")
async def vegetation_contractor_performance() -> JSONResponse:
    """Contractor KPI dashboard — month-to-date clearance performance."""
    _r.seed(522)
    contractors = []
    regions = ["VIC1", "QLD1", "NSW1", "SA1", "TAS1"]
    for i, contractor in enumerate(_CONTRACTORS):
        target = round(_r.uniform(200.0, 800.0), 1)
        achieved = round(target * _r.uniform(0.70, 1.10), 1)
        contractors.append({
            "contractor_name": contractor,
            "contract_region": regions[i % len(regions)],
            "km_cleared_mtd": achieved,
            "km_target_mtd": target,
            "utilisation_pct": round(_r.uniform(68.0, 98.0), 1),
            "compliance_rate": round(_r.uniform(82.0, 99.0), 1),
            "defects_found": _r.randint(5, 120),
        })
    return JSONResponse({"contractors": contractors})


@router.get("/api/vegetation/clearance-schedule")
async def vegetation_clearance_schedule() -> JSONResponse:
    """12-week vegetation clearance schedule."""
    _r.seed(523)
    schedule = []
    zone_names = [
        "Northern Metro", "Eastern Suburbs", "Southern Rural", "Western Plains",
        "Coastal Zone", "Highland Region", "CBD & Inner", "Outer Growth",
        "Industrial Belt", "Peri-Urban Fringe", "Bushfire Risk Zone A", "Fire Risk Zone B",
    ]
    today_dt = date.today()
    for i, zone in enumerate(zone_names):
        planned = today_dt + timedelta(weeks=i)
        status = "Completed" if planned < today_dt else (
            "In Progress" if i == 0 else "Scheduled"
        )
        schedule.append({
            "zone": zone,
            "planned_date": planned.isoformat(),
            "contractor": _r.choice(_CONTRACTORS),
            "km_planned": round(_r.uniform(50.0, 400.0), 1),
            "status": status,
            "risk_level": _r.choice(_RISK_LEVELS),
        })
    return JSONResponse({"schedule": schedule})
