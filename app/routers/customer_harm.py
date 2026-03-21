"""Customer Harm Reduction — AER Service Performance Framework.

Tracks customer harm index, unplanned interruptions, voltage complaints,
connection delays, hardship cases, and improvement actions.
"""
from __future__ import annotations

import random as _r
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()

_MONTHS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]
_METRICS = [
    "unplanned interruptions",
    "voltage excursions",
    "connection delays",
    "billing complaints",
    "hardship enrollment",
    "NPS score",
]
_TRENDS = ["Improving", "Stable", "Deteriorating"]
_STATUSES = ["On Track", "At Risk", "Exceeds Target", "Below Target"]


@router.get("/api/customer-harm/summary")
async def customer_harm_summary() -> JSONResponse:
    """Customer harm index and key harm metrics summary."""
    _r.seed(600)
    return JSONResponse({
        "harm_index_score": round(_r.uniform(3.2, 7.8), 2),
        "unplanned_interruptions_per_customer": round(_r.uniform(0.8, 2.4), 3),
        "voltage_complaints": _r.randint(120, 850),
        "connection_delay_rate_pct": round(_r.uniform(4.5, 18.0), 1),
        "affordability_hardship_cases": _r.randint(800, 4500),
        "overall_rating": _r.choice(["Good", "Satisfactory", "Needs Improvement", "Poor"]),
        "data_source": "synthetic",
    })


@router.get("/api/customer-harm/metrics")
async def customer_harm_metrics(
    dnsp: Optional[str] = Query(None, description="Filter by DNSP"),
) -> JSONResponse:
    """Per-metric performance vs benchmark for each DNSP."""
    _r.seed(601)
    dnsps = [dnsp] if dnsp else ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy"]
    units = {
        "unplanned interruptions": "per customer",
        "voltage excursions": "per 100 customers",
        "connection delays": "% delayed",
        "billing complaints": "per 1000",
        "hardship enrollment": "% enrolled",
        "NPS score": "score (-100 to 100)",
    }
    benchmarks = {
        "unplanned interruptions": 1.2,
        "voltage excursions": 2.5,
        "connection delays": 8.0,
        "billing complaints": 5.0,
        "hardship enrollment": 2.5,
        "NPS score": 30.0,
    }
    metrics = []
    for d in dnsps:
        for metric in _METRICS:
            benchmark = benchmarks[metric]
            if metric == "NPS score":
                value = round(_r.uniform(15.0, 55.0), 1)
                better = value >= benchmark
            else:
                value = round(_r.uniform(benchmark * 0.5, benchmark * 1.8), 2)
                better = value <= benchmark
            status = "Exceeds Target" if better and abs(value - benchmark) / benchmark > 0.1 else (
                "On Track" if better else (
                    "At Risk" if abs(value - benchmark) / benchmark < 0.2 else "Below Target"
                )
            )
            metrics.append({
                "metric": metric,
                "dnsp": d,
                "value": value,
                "benchmark": benchmark,
                "status": status,
                "trend": _r.choice(_TRENDS),
            })
    return JSONResponse({"metrics": metrics})


@router.get("/api/customer-harm/trend")
async def customer_harm_trend() -> JSONResponse:
    """12-month customer harm trend."""
    _r.seed(602)
    trend = []
    for month in _MONTHS:
        trend.append({
            "month": month,
            "harm_index": round(_r.uniform(3.0, 8.5), 2),
            "interruptions_per_customer": round(_r.uniform(0.05, 0.25), 4),
            "complaints_per_1000": round(_r.uniform(2.0, 12.0), 2),
        })
    return JSONResponse({"trend": trend})


@router.get("/api/customer-harm/improvement-actions")
async def customer_harm_improvement_actions() -> JSONResponse:
    """Improvement actions targeting customer harm reduction."""
    _r.seed(603)
    today_dt = date.today()
    action_templates = [
        ("Proactive feeder maintenance program", "unplanned interruptions"),
        ("LV voltage monitoring rollout", "voltage excursions"),
        ("Connection application portal upgrade", "connection delays"),
        ("Billing accuracy audit and remediation", "billing complaints"),
        ("Hardship customer outreach campaign", "hardship enrollment"),
        ("Customer satisfaction survey improvements", "NPS score"),
        ("Field crew first-time-fix rate initiative", "unplanned interruptions"),
        ("Power quality monitoring expansion", "voltage excursions"),
        ("Same-day connection guarantee pilot", "connection delays"),
        ("Automated billing validation engine", "billing complaints"),
    ]
    owners = ["Customer Operations", "Network Operations", "Digital", "Finance", "Field Services"]
    statuses = ["On Track", "In Progress", "Complete", "At Risk", "Not Started"]
    actions = []
    for action, metric in action_templates:
        deadline = today_dt + timedelta(days=_r.randint(30, 365))
        actions.append({
            "action": action,
            "metric_targeted": metric,
            "target_improvement_pct": round(_r.uniform(5.0, 30.0), 1),
            "deadline": deadline.isoformat(),
            "status": _r.choice(statuses),
            "owner": _r.choice(owners),
        })
    return JSONResponse({"actions": actions, "count": len(actions)})
