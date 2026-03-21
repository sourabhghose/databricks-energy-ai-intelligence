"""AER Regulatory Compliance & RIN Management.

Endpoints for AER regulatory information notices, STPIS performance incentive
scheme tracking, revenue cap monitoring, and regulatory calendar.
Serves AusNet (VIC1) and Energy Queensland (Ergon/Energex, QLD1).
"""
from __future__ import annotations

import random as _r
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from .shared import (
    _CATALOG,
    _cache_get,
    _cache_set,
    _query_gold,
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"
_DNSPS = ["AusNet Services", "Ergon Energy", "Energex"]


# =========================================================================
# GET /api/aer/summary
# =========================================================================

@router.get("/api/aer/summary")
async def aer_summary() -> JSONResponse:
    """KPI summary — total RINs submitted, STPIS exposure, revenue variance."""
    cache_key = "aer:summary"
    if cached := _cache_get(cache_key):
        return JSONResponse(cached)

    rows = _query_gold(f"""
        SELECT
            COUNT(*) AS total_rins,
            SUM(CASE WHEN aer_accepted = TRUE THEN 1 ELSE 0 END) AS accepted_rins,
            MAX(submission_year) AS latest_submission_year
        FROM {_SCHEMA}.rin_submissions
    """)

    stpis = _query_gold(f"""
        SELECT
            SUM(revenue_at_risk_aud) AS total_revenue_at_risk,
            COUNT(CASE WHEN performance_band IN ('C','D') THEN 1 END) AS underperformers
        FROM {_SCHEMA}.stpis_performance
        WHERE period_year = (SELECT MAX(period_year) FROM {_SCHEMA}.stpis_performance)
    """)

    rev = _query_gold(f"""
        SELECT
            ROUND(SUM(variance_m_aud), 2) AS total_variance_m_aud,
            ROUND(AVG(smoothing_account_balance_m_aud), 2) AS avg_smoothing_balance
        FROM {_SCHEMA}.revenue_monitoring
        WHERE period_type = 'monthly'
          AND period_start >= DATE_TRUNC('year', CURRENT_DATE())
    """)

    if rows and stpis and rev:
        result = {
            "total_rins": rows[0].get("total_rins", 0),
            "accepted_rins": rows[0].get("accepted_rins", 0),
            "latest_year": rows[0].get("latest_submission_year", 2026),
            "total_revenue_at_risk_aud": stpis[0].get("total_revenue_at_risk", 0) or 0,
            "stpis_underperformers": stpis[0].get("underperformers", 0) or 0,
            "ytd_revenue_variance_m": rev[0].get("total_variance_m_aud", 0) or 0,
            "avg_smoothing_balance_m": rev[0].get("avg_smoothing_balance", 0) or 0,
        }
        _cache_set(cache_key, result, ttl=300)
        return JSONResponse(result)

    # Mock fallback
    _r.seed(201)
    result = {
        "total_rins": 90,
        "accepted_rins": 86,
        "latest_year": 2026,
        "total_revenue_at_risk_aud": round(_r.uniform(5_000_000, 45_000_000), 2),
        "stpis_underperformers": _r.randint(0, 2),
        "ytd_revenue_variance_m": round(_r.uniform(-15.0, 20.0), 2),
        "avg_smoothing_balance_m": round(_r.uniform(-30.0, 30.0), 2),
        "data_source": "synthetic",
    }
    return JSONResponse(result)


# =========================================================================
# GET /api/aer/rin
# =========================================================================

@router.get("/api/aer/rin")
async def aer_rin(
    dnsp: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    category: Optional[str] = Query(None),
) -> JSONResponse:
    """RIN submissions list."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if year:
        where.append(f"submission_year = {year}")
    if category:
        where.append(f"category = '{category}'")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT rin_id, dnsp, submission_year, category, data_value, unit,
               submission_date, aer_accepted, notes
        FROM {_SCHEMA}.rin_submissions
        {where_clause}
        ORDER BY submission_year DESC, dnsp, category
        LIMIT 200
    """)
    if rows:
        for r in rows:
            r["submission_date"] = str(r.get("submission_date", ""))
        return JSONResponse({"submissions": rows, "count": len(rows)})

    _r.seed(202)
    mock = []
    categories = ["SAIDI", "SAIFI", "Opex", "Capex", "Customer Connections",
                  "Asset Age Profile", "Network Length", "DER Connection"]
    target_dnsps = [dnsp] if dnsp else _DNSPS
    target_years = [year] if year else [2024, 2025, 2026]
    for d in target_dnsps:
        for y in target_years:
            for cat in (categories if not category else [category]):
                mock.append({
                    "rin_id": f"RIN-{d[:3].upper()}-{y}-{cat[:4].upper()}-{_r.randint(1,9):02d}",
                    "dnsp": d,
                    "submission_year": y,
                    "category": cat,
                    "data_value": round(_r.uniform(1.0, 500.0), 2),
                    "unit": _r.choice(["minutes", "count", "$M", "km", "%"]),
                    "submission_date": f"{y}-10-{_r.randint(1,28):02d}",
                    "aer_accepted": _r.random() > 0.05,
                    "notes": "",
                })
    return JSONResponse({"submissions": mock[:50], "count": len(mock)})


# =========================================================================
# GET /api/aer/stpis
# =========================================================================

@router.get("/api/aer/stpis")
async def aer_stpis(
    dnsp: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
) -> JSONResponse:
    """STPIS performance bands."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if year:
        where.append(f"period_year = {year}")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT dnsp, period_year, saidi_actual, saidi_target, saifi_actual, saifi_target,
               s_factor_saidi, s_factor_saifi, revenue_at_risk_aud, performance_band
        FROM {_SCHEMA}.stpis_performance
        {where_clause}
        ORDER BY period_year DESC, dnsp
    """)
    if rows:
        return JSONResponse({"stpis": rows, "count": len(rows)})

    _r.seed(203)
    mock = []
    for d, saidi_t, saifi_t, rev_base in [
        ("AusNet Services", 75.0, 1.0, 950.0),
        ("Ergon Energy", 140.0, 1.6, 1200.0),
        ("Energex", 80.0, 1.1, 1800.0),
    ]:
        if dnsp and d != dnsp:
            continue
        for y in ([year] if year else [2024, 2025, 2026]):
            saidi_a = round(saidi_t * _r.uniform(0.7, 1.3), 2)
            saifi_a = round(saifi_t * _r.uniform(0.7, 1.3), 3)
            s_saidi = round((saidi_t - saidi_a) / saidi_t * 0.5, 4)
            s_saifi = round((saifi_t - saifi_a) / saifi_t * 0.5, 4)
            combined = s_saidi + s_saifi
            band = "A" if combined > 0.05 else ("B" if combined > 0 else ("C" if combined > -0.05 else "D"))
            mock.append({
                "dnsp": d,
                "period_year": y,
                "saidi_actual": saidi_a,
                "saidi_target": saidi_t,
                "saifi_actual": saifi_a,
                "saifi_target": saifi_t,
                "s_factor_saidi": s_saidi,
                "s_factor_saifi": s_saifi,
                "revenue_at_risk_aud": round(rev_base * abs(min(combined, 0)) * 1_000_000 * 10, 2),
                "performance_band": band,
            })
    return JSONResponse({"stpis": mock, "count": len(mock)})


# =========================================================================
# GET /api/aer/stpis/risk
# =========================================================================

@router.get("/api/aer/stpis/risk")
async def aer_stpis_risk() -> JSONResponse:
    """Revenue at risk breakdown by DNSP."""
    rows = _query_gold(f"""
        SELECT dnsp, SUM(revenue_at_risk_aud) AS total_at_risk,
               AVG(s_factor_saidi + s_factor_saifi) AS avg_combined_s_factor,
               COUNT(CASE WHEN performance_band IN ('C','D') THEN 1 END) AS poor_years
        FROM {_SCHEMA}.stpis_performance
        GROUP BY dnsp
        ORDER BY total_at_risk DESC
    """)
    if rows:
        return JSONResponse({"risk": rows})

    _r.seed(204)
    mock = [
        {"dnsp": "AusNet Services", "total_at_risk": round(_r.uniform(5e6, 20e6), 2),
         "avg_combined_s_factor": round(_r.uniform(-0.05, 0.05), 4), "poor_years": _r.randint(0, 2)},
        {"dnsp": "Ergon Energy", "total_at_risk": round(_r.uniform(8e6, 35e6), 2),
         "avg_combined_s_factor": round(_r.uniform(-0.08, 0.03), 4), "poor_years": _r.randint(0, 3)},
        {"dnsp": "Energex", "total_at_risk": round(_r.uniform(3e6, 15e6), 2),
         "avg_combined_s_factor": round(_r.uniform(-0.03, 0.06), 4), "poor_years": _r.randint(0, 1)},
    ]
    return JSONResponse({"risk": mock})


# =========================================================================
# GET /api/aer/revenue
# =========================================================================

@router.get("/api/aer/revenue")
async def aer_revenue(dnsp: Optional[str] = Query(None)) -> JSONResponse:
    """Revenue cap monitoring."""
    where = f"WHERE dnsp = '{dnsp}'" if dnsp else ""
    rows = _query_gold(f"""
        SELECT dnsp, period_type, period_start, allowed_revenue_m_aud,
               actual_revenue_m_aud, variance_m_aud, smoothing_account_balance_m_aud
        FROM {_SCHEMA}.revenue_monitoring
        {where}
        ORDER BY dnsp, period_start DESC
        LIMIT 100
    """)
    if rows:
        for r in rows:
            r["period_start"] = str(r.get("period_start", ""))
        return JSONResponse({"revenue": rows, "count": len(rows)})

    _r.seed(205)
    mock = []
    today_dt = datetime.now(timezone.utc)
    for d, base in ([("AusNet Services", 950.0)] if dnsp == "AusNet Services" else
                    [("Ergon Energy", 1200.0)] if dnsp == "Ergon Energy" else
                    [("Energex", 1800.0)] if dnsp == "Energex" else
                    [("AusNet Services", 950.0), ("Ergon Energy", 1200.0), ("Energex", 1800.0)]):
        for m in range(12):
            dt = today_dt.replace(day=1) - timedelta(days=m * 30)
            allowed = round(base / 12 * _r.uniform(0.97, 1.03), 2)
            actual = round(allowed * _r.uniform(0.95, 1.05), 2)
            mock.append({
                "dnsp": d,
                "period_type": "monthly",
                "period_start": dt.strftime("%Y-%m-01"),
                "allowed_revenue_m_aud": allowed,
                "actual_revenue_m_aud": actual,
                "variance_m_aud": round(actual - allowed, 3),
                "smoothing_account_balance_m_aud": round(_r.uniform(-50, 50), 2),
            })
    return JSONResponse({"revenue": mock, "count": len(mock)})


# =========================================================================
# GET /api/aer/pricing-proposals
# =========================================================================

@router.get("/api/aer/pricing-proposals")
async def aer_pricing_proposals() -> JSONResponse:
    """Pricing proposal status by DNSP and regulatory year."""
    rows = _query_gold(f"""
        SELECT dnsp, regulatory_year, proposal_date, aer_decision_date, status,
               proposed_revenue_m_aud, aer_approved_revenue_m_aud
        FROM {_SCHEMA}.pricing_proposals
        ORDER BY regulatory_year DESC, dnsp
    """)
    if rows:
        for r in rows:
            r["proposal_date"] = str(r.get("proposal_date", ""))
            r["aer_decision_date"] = str(r.get("aer_decision_date", ""))
        return JSONResponse({"proposals": rows, "count": len(rows)})

    _r.seed(206)
    mock = []
    for d, base in [("AusNet Services", 950.0), ("Ergon Energy", 1200.0), ("Energex", 1800.0)]:
        for y in range(2023, 2028):
            proposed = round(base * _r.uniform(1.02, 1.12), 2)
            approved = round(proposed * _r.uniform(0.92, 0.99), 2)
            status = "Approved" if y <= 2025 else ("Under Review" if y == 2026 else "Draft")
            mock.append({
                "dnsp": d,
                "regulatory_year": y,
                "proposal_date": f"{y-1}-10-15",
                "aer_decision_date": f"{y-1}-12-31" if y <= 2025 else "",
                "status": status,
                "proposed_revenue_m_aud": proposed,
                "aer_approved_revenue_m_aud": approved if y <= 2025 else 0.0,
            })
    return JSONResponse({"proposals": mock, "count": len(mock)})


# =========================================================================
# GET /api/aer/milestones
# =========================================================================

@router.get("/api/aer/milestones")
async def aer_milestones(
    dnsp: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
) -> JSONResponse:
    """Regulatory calendar milestones."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if status:
        where.append(f"status = '{status}'")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT milestone_id, dnsp, milestone_type, description,
               due_date, completed_date, status, responsible_team
        FROM {_SCHEMA}.regulatory_milestones
        {where_clause}
        ORDER BY due_date ASC
    """)
    if rows:
        for r in rows:
            r["due_date"] = str(r.get("due_date", ""))
            r["completed_date"] = str(r.get("completed_date", ""))
        return JSONResponse({"milestones": rows, "count": len(rows)})

    _r.seed(207)
    today_dt = date.today()
    milestone_types = ["RIN Submission", "Annual Pricing Proposal", "STPIS Assessment",
                       "AER Audit", "Annual Compliance Report", "Revenue Reset"]
    mock = []
    for d in ([dnsp] if dnsp else _DNSPS):
        for i, mtype in enumerate(milestone_types):
            due = today_dt + timedelta(days=_r.randint(-60, 365))
            is_complete = due < today_dt and _r.random() > 0.2
            ms_status = "COMPLETE" if is_complete else ("OVERDUE" if due < today_dt else ("IN PROGRESS" if (due - today_dt).days < 30 else "UPCOMING"))
            if status and ms_status != status:
                continue
            mock.append({
                "milestone_id": f"MS-{d[:3].upper()}-{i+1:03d}",
                "dnsp": d,
                "milestone_type": mtype,
                "description": f"{mtype} for {d}",
                "due_date": due.isoformat(),
                "completed_date": (due - timedelta(days=3)).isoformat() if is_complete else "",
                "status": ms_status,
                "responsible_team": _r.choice(["Regulatory Affairs", "Finance", "Legal"]),
            })
    return JSONResponse({"milestones": mock, "count": len(mock)})


# =========================================================================
# GET /api/aer/milestones/upcoming
# =========================================================================

@router.get("/api/aer/milestones/upcoming")
async def aer_milestones_upcoming() -> JSONResponse:
    """Next 90 days milestones."""
    today_dt = date.today()
    cutoff = today_dt + timedelta(days=90)
    rows = _query_gold(f"""
        SELECT milestone_id, dnsp, milestone_type, description,
               due_date, status, responsible_team
        FROM {_SCHEMA}.regulatory_milestones
        WHERE due_date BETWEEN '{today_dt}' AND '{cutoff}'
          AND status != 'COMPLETE'
        ORDER BY due_date ASC
        LIMIT 20
    """)
    if rows:
        for r in rows:
            r["due_date"] = str(r.get("due_date", ""))
        return JSONResponse({"upcoming": rows, "count": len(rows), "window_days": 90})

    _r.seed(208)
    mock = []
    for d in _DNSPS:
        for i in range(3):
            due = today_dt + timedelta(days=_r.randint(1, 90))
            days_remaining = (due - today_dt).days
            mock.append({
                "milestone_id": f"UP-{d[:3].upper()}-{i+1:02d}",
                "dnsp": d,
                "milestone_type": _r.choice(["RIN Submission", "Pricing Proposal", "STPIS Review"]),
                "description": f"Upcoming milestone for {d}",
                "due_date": due.isoformat(),
                "status": "UPCOMING" if days_remaining > 14 else "IN PROGRESS",
                "responsible_team": "Regulatory Affairs",
                "days_remaining": days_remaining,
            })
    mock.sort(key=lambda x: x["due_date"])
    return JSONResponse({"upcoming": mock, "count": len(mock), "window_days": 90})
