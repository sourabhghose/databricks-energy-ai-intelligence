"""DNSP Reliability Performance — SAIDI, SAIFI, CAIDI & MED Compliance.

Tracks distribution reliability KPIs against AER targets, worst-performing
feeders, interruption cause analysis, and 12-month trend data.
"""
from __future__ import annotations

import random as _r
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy"]
_INTERRUPTION_CAUSES = [
    "Vegetation Contact", "Equipment Failure", "Lightning", "Animals",
    "Vehicle Damage", "Planned Maintenance", "Overloading", "Unknown", "Third Party",
]
_MONTHS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


@router.get("/api/reliability-dnsp/summary")
async def reliability_dnsp_summary() -> JSONResponse:
    """Aggregate reliability KPI summary across all DNSPs."""
    _r.seed(540)
    return JSONResponse({
        "saidi_ytd": round(_r.uniform(55.0, 130.0), 2),
        "saidi_target": round(_r.uniform(65.0, 110.0), 2),
        "saifi_ytd": round(_r.uniform(0.8, 1.8), 3),
        "saifi_target": round(_r.uniform(0.9, 1.5), 3),
        "caidi_minutes": round(_r.uniform(55.0, 95.0), 1),
        "med_compliance_pct": round(_r.uniform(82.0, 97.0), 1),
        "worst_feeders_count": _r.randint(8, 30),
        "data_source": "synthetic",
    })


@router.get("/api/reliability-dnsp/kpis")
async def reliability_dnsp_kpis() -> JSONResponse:
    """Per-DNSP reliability KPI breakdown."""
    _r.seed(541)
    kpi_data = [
        ("AusNet Services", "VIC1", 75.0, 1.00, 900),
        ("Ergon Energy",    "QLD1", 140.0, 1.60, 1150),
        ("Energex",         "QLD1", 80.0,  1.10, 1750),
        ("Ausgrid",         "NSW1", 70.0,  0.95, 1600),
        ("Essential Energy","NSW1", 160.0, 1.75, 750),
    ]
    kpis = []
    for dnsp, region, saidi_t, saifi_t, _ in kpi_data:
        saidi_a = round(saidi_t * _r.uniform(0.75, 1.25), 2)
        saifi_a = round(saifi_t * _r.uniform(0.75, 1.25), 3)
        saidi_var = round((saidi_a - saidi_t) / saidi_t * 100, 1)
        caidi = round(saidi_a / saifi_a * 60, 1) if saifi_a > 0 else 0
        kpis.append({
            "dnsp": dnsp,
            "region": region,
            "saidi_ytd": saidi_a,
            "saidi_target": saidi_t,
            "saidi_variance_pct": saidi_var,
            "saifi_ytd": saifi_a,
            "saifi_target": saifi_t,
            "caidi_minutes": caidi,
            "med_compliance_pct": round(_r.uniform(80.0, 97.0), 1),
            "exclusions_applied": _r.randint(0, 8),
        })
    return JSONResponse({"kpis": kpis})


@router.get("/api/reliability-dnsp/worst-feeders")
async def reliability_dnsp_worst_feeders(
    dnsp: Optional[str] = Query(None, description="Filter by DNSP"),
    limit: int = Query(20, description="Max feeders to return"),
) -> JSONResponse:
    """Worst-performing feeders by SAIDI YTD."""
    _r.seed(542)
    causes = _INTERRUPTION_CAUSES
    feeders = []
    target_dnsps = [dnsp] if dnsp else _DNSPS
    per_dnsp = max(1, limit // len(target_dnsps))
    for d in target_dnsps:
        for i in range(per_dnsp):
            feeders.append({
                "feeder_id": f"FDR-{d[:3].upper()}-{_r.randint(1, 99):02d}",
                "dnsp": d,
                "zone_substation": f"ZSS-{_r.randint(1, 50):02d}",
                "saidi_ytd": round(_r.uniform(80.0, 600.0), 2),
                "saifi_ytd": round(_r.uniform(1.0, 8.0), 3),
                "customers_affected": _r.randint(200, 5000),
                "interruption_count": _r.randint(5, 40),
                "primary_cause": _r.choice(causes),
            })
    feeders.sort(key=lambda x: x["saidi_ytd"], reverse=True)
    return JSONResponse({"feeders": feeders[:limit], "count": len(feeders[:limit])})


@router.get("/api/reliability-dnsp/interruption-causes")
async def reliability_dnsp_interruption_causes() -> JSONResponse:
    """SAIDI contribution breakdown by interruption cause."""
    _r.seed(543)
    total_count = 0
    causes_raw = []
    for cause in _INTERRUPTION_CAUSES:
        count = _r.randint(20, 400)
        total_count += count
        causes_raw.append({
            "cause": cause,
            "count": count,
            "avg_duration_min": round(_r.uniform(25.0, 180.0), 1),
        })
    for c in causes_raw:
        c["saidi_contribution_pct"] = round(c["count"] / total_count * 100, 1)
    causes_raw.sort(key=lambda x: x["saidi_contribution_pct"], reverse=True)
    return JSONResponse({"causes": causes_raw})


@router.get("/api/reliability-dnsp/trend")
async def reliability_dnsp_trend() -> JSONResponse:
    """12-month rolling reliability trend."""
    _r.seed(544)
    trend = []
    for month in _MONTHS:
        saidi = round(_r.uniform(4.0, 18.0), 2)
        saifi = round(_r.uniform(0.05, 0.20), 4)
        caidi = round(saidi / saifi * 60, 1) if saifi > 0 else 0
        trend.append({
            "month": month,
            "saidi": saidi,
            "saifi": saifi,
            "caidi": caidi,
        })
    return JSONResponse({"trend": trend})
