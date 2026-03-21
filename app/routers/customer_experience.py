"""Customer Experience — NPS, CSAT, Complaints & Digital Channels.

Tracks customer satisfaction, complaint volumes, and digital channel
adoption across Australian DNSP operations.
"""
from __future__ import annotations

import random as _r

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

_MONTHS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]
_COMPLAINT_CATEGORIES = [
    "Connection Delays", "Billing Errors", "Outage Communication",
    "Voltage Issues", "Meter Issues", "Tree Trimming", "Other",
]
_CHANNELS = [
    "MyAccount Portal", "Mobile App", "Outage Map",
    "Online Connection", "Live Chat", "Virtual Assistant",
]
_TRENDS = ["Up", "Stable", "Down"]


@router.get("/api/cx/summary")
async def cx_summary() -> JSONResponse:
    """Customer experience headline KPIs."""
    _r.seed(660)
    return JSONResponse({
        "nps_score": round(_r.uniform(18.0, 52.0), 1),
        "csat_score": round(_r.uniform(72.0, 88.0), 1),
        "connection_satisfaction": round(_r.uniform(65.0, 85.0), 1),
        "complaints_per_1000": round(_r.uniform(3.5, 12.0), 2),
        "resolution_rate_pct": round(_r.uniform(82.0, 97.0), 1),
        "digital_adoption_pct": round(_r.uniform(45.0, 72.0), 1),
        "data_source": "synthetic",
    })


@router.get("/api/cx/nps-trend")
async def cx_nps_trend() -> JSONResponse:
    """12-month NPS and CSAT trend."""
    _r.seed(661)
    trend = []
    for month in _MONTHS:
        nps = round(_r.uniform(15.0, 55.0), 1)
        csat = round(_r.uniform(70.0, 90.0), 1)
        promoters = round(_r.uniform(30.0, 65.0), 1)
        detractors = round(_r.uniform(10.0, 35.0), 1)
        trend.append({
            "month": month,
            "nps": nps,
            "csat": csat,
            "promoters_pct": promoters,
            "detractors_pct": detractors,
        })
    return JSONResponse({"trend": trend})


@router.get("/api/cx/complaints")
async def cx_complaints() -> JSONResponse:
    """Complaint volume by category — MTD, YTD, and benchmark comparison."""
    _r.seed(662)
    complaints = []
    benchmarks = {
        "Connection Delays": 2.5,
        "Billing Errors": 1.8,
        "Outage Communication": 1.2,
        "Voltage Issues": 0.8,
        "Meter Issues": 0.9,
        "Tree Trimming": 0.6,
        "Other": 1.5,
    }
    for cat in _COMPLAINT_CATEGORIES:
        benchmark = benchmarks[cat]
        mtd = round(_r.uniform(benchmark * 0.4, benchmark * 2.2), 2)
        ytd = round(mtd * _r.uniform(10.0, 13.0), 2)
        trend = "Up" if mtd > benchmark else ("Down" if mtd < benchmark * 0.8 else "Stable")
        complaints.append({
            "category": cat,
            "count": _r.randint(10, 500),
            "mtd": mtd,
            "ytd": ytd,
            "benchmark": benchmark,
            "trend": trend,
        })
    return JSONResponse({"complaints": complaints})


@router.get("/api/cx/digital-channels")
async def cx_digital_channels() -> JSONResponse:
    """Digital channel usage and satisfaction metrics."""
    _r.seed(663)
    channels = []
    base_users = {
        "MyAccount Portal": 85000,
        "Mobile App": 62000,
        "Outage Map": 45000,
        "Online Connection": 12000,
        "Live Chat": 8000,
        "Virtual Assistant": 15000,
    }
    for channel in _CHANNELS:
        base = base_users[channel]
        active = int(base * _r.uniform(0.85, 1.15))
        transactions = _r.randint(int(active * 0.3), int(active * 2.5))
        channels.append({
            "channel": channel,
            "active_users": active,
            "transactions_mtd": transactions,
            "satisfaction_score": round(_r.uniform(3.2, 4.8), 2),
            "adoption_pct": round(_r.uniform(12.0, 78.0), 1),
        })
    return JSONResponse({"channels": channels})
