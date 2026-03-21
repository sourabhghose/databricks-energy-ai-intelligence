"""Distribution Annual Planning Report (DAPR) — AER Obligation.

Network constraint identification, augmentation vs non-network alternatives,
and DAPR regulatory obligation tracking for DNSPs.
"""
from __future__ import annotations

import random as _r
from datetime import date, timedelta

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

_CONSTRAINT_TYPES = [
    "Thermal Overload", "Voltage Excursion", "Fault Level", "N-1 Security",
    "Power Quality", "Reliability Shortfall",
]
_SOLUTION_STATUSES = ["Approved", "Under Assessment", "Deferred", "In Construction", "Complete"]
_TECHNOLOGIES = [
    "Battery Energy Storage", "Demand Response", "Solar + Storage",
    "Virtual Power Plant", "EV Smart Charging", "Network Tariff Reform",
]


@router.get("/api/dapr/summary")
async def dapr_summary() -> JSONResponse:
    """DAPR high-level summary — constraints, projects, compliance."""
    _r.seed(580)
    return JSONResponse({
        "constraints_identified": _r.randint(12, 45),
        "augmentation_projects": _r.randint(6, 20),
        "non_network_alternatives": _r.randint(4, 15),
        "next_submission_due": (date.today() + timedelta(days=_r.randint(30, 270))).isoformat(),
        "compliance_status": _r.choice(["On Track", "At Risk", "Compliant"]),
        "data_source": "synthetic",
    })


@router.get("/api/dapr/constraints")
async def dapr_constraints() -> JSONResponse:
    """Network constraints — type, breach year, and proposed solution."""
    _r.seed(581)
    constraints = []
    zone_substations = [f"ZSS-{i:02d}" for i in range(1, 21)]
    for i in range(20):
        solution_type = _r.choice(["Augmentation", "Non-Network"])
        cost = round(_r.uniform(2.0, 85.0), 2)
        constraints.append({
            "constraint_id": f"CON-{2026}-{i+1:03d}",
            "zone_substation": _r.choice(zone_substations),
            "constraint_type": _r.choice(_CONSTRAINT_TYPES),
            "utilisation_pct": round(_r.uniform(85.0, 115.0), 1),
            "breach_year": _r.randint(2026, 2031),
            "proposed_solution": f"{solution_type} solution {i+1}",
            "solution_type": solution_type,
            "estimated_cost_m_aud": cost,
            "status": _r.choice(_SOLUTION_STATUSES),
        })
    return JSONResponse({"constraints": constraints, "count": len(constraints)})


@router.get("/api/dapr/obligations")
async def dapr_obligations() -> JSONResponse:
    """DAPR regulatory obligations and compliance status."""
    _r.seed(582)
    today_dt = date.today()
    obligation_list = [
        ("Identify and publish network constraints", "Network Planning"),
        ("Assess non-network alternatives", "Strategy & Innovation"),
        ("Submit DAPR to AER", "Regulatory Affairs"),
        ("Publish augmentation options", "Network Planning"),
        ("Consult with non-network providers", "Market Development"),
        ("Update constraint forecasts", "Network Planning"),
        ("Publish connection opportunity register", "Customer Connections"),
        ("Annual DAPR review and update", "Regulatory Affairs"),
    ]
    obligations = []
    statuses = ["Complete", "In Progress", "Upcoming", "Overdue"]
    for oblig, team in obligation_list:
        due = today_dt + timedelta(days=_r.randint(-30, 300))
        is_past = due < today_dt
        status = "Complete" if is_past and _r.random() > 0.2 else (
            "Overdue" if is_past else (
                "In Progress" if (due - today_dt).days < 60 else "Upcoming"
            )
        )
        obligations.append({
            "obligation": oblig,
            "due_date": due.isoformat(),
            "status": status,
            "responsible_team": team,
            "notes": "" if status == "Complete" else _r.choice([
                "Awaiting data from field teams",
                "Stakeholder consultation in progress",
                "Pending AER guidance",
                "",
            ]),
        })
    return JSONResponse({"obligations": obligations, "count": len(obligations)})


@router.get("/api/dapr/non-network-alternatives")
async def dapr_non_network_alternatives() -> JSONResponse:
    """Non-network alternative projects assessed under DAPR."""
    _r.seed(583)
    alternatives = []
    zone_substations = [f"ZSS-{i:02d}" for i in range(1, 21)]
    project_statuses = ["Under Assessment", "Approved", "Tendered", "In Delivery", "Complete"]
    for i in range(12):
        tech = _r.choice(_TECHNOLOGIES)
        capacity = round(_r.uniform(1.0, 50.0), 1)
        cost = round(_r.uniform(1.5, 40.0), 2)
        augmentation_equiv = round(cost * _r.uniform(1.3, 3.5), 2)
        alternatives.append({
            "project_name": f"NNA-{i+1:03d} {tech.split()[0]} {_r.choice(zone_substations)}",
            "constraint_addressed": f"CON-2026-{_r.randint(1, 20):03d}",
            "technology": tech,
            "capacity_mw": capacity,
            "cost_m_aud": cost,
            "savings_vs_augmentation_m_aud": round(augmentation_equiv - cost, 2),
            "status": _r.choice(project_statuses),
        })
    return JSONResponse({"alternatives": alternatives, "count": len(alternatives)})
