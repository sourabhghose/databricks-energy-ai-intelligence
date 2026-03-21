"""Field Force & Capital Program Delivery.

Capital project register, maintenance orders, fault response KPIs,
contractor performance, capex/opex analysis, and delivery trend.
"""
from __future__ import annotations

import random as _r
from datetime import date, timedelta
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


@router.get("/api/capex/summary")
async def capex_summary() -> JSONResponse:
    """Capital program KPI summary."""
    cache_key = "capex:summary"
    if cached := _cache_get(cache_key):
        return JSONResponse(cached)

    rows = _query_gold(f"""
        SELECT
            ROUND(SUM(budget_m_aud), 2) AS total_budget,
            ROUND(SUM(actual_spend_m_aud), 2) AS total_spend,
            ROUND(AVG(completion_pct), 1) AS avg_completion_pct,
            COUNT(*) AS total_projects,
            COUNT(CASE WHEN status = 'Complete' THEN 1 END) AS complete_projects,
            COUNT(CASE WHEN status = 'On Hold' THEN 1 END) AS on_hold_projects
        FROM {_SCHEMA}.capital_projects
    """)
    maint = _query_gold(f"""
        SELECT
            COUNT(*) AS total_orders,
            COUNT(CASE WHEN priority = 'P1 Emergency' THEN 1 END) AS p1_orders,
            COUNT(CASE WHEN status = 'In Progress' THEN 1 END) AS active_orders
        FROM {_SCHEMA}.maintenance_orders
        WHERE scheduled_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    """)

    if rows and maint:
        result = {
            "total_budget_m": rows[0].get("total_budget", 0) or 0,
            "total_spend_m": rows[0].get("total_spend", 0) or 0,
            "avg_completion_pct": rows[0].get("avg_completion_pct", 0) or 0,
            "total_projects": rows[0].get("total_projects", 0),
            "complete_projects": rows[0].get("complete_projects", 0),
            "on_hold_projects": rows[0].get("on_hold_projects", 0),
            "maintenance_orders_30d": maint[0].get("total_orders", 0),
            "p1_emergency_orders": maint[0].get("p1_orders", 0),
            "active_maintenance_orders": maint[0].get("active_orders", 0),
        }
        _cache_set(cache_key, result, ttl=300)
        return JSONResponse(result)

    _r.seed(250)
    return JSONResponse({
        "total_budget_m": round(_r.uniform(800, 1500), 2),
        "total_spend_m": round(_r.uniform(600, 1200), 2),
        "avg_completion_pct": round(_r.uniform(45, 75), 1),
        "total_projects": 80,
        "complete_projects": _r.randint(20, 40),
        "on_hold_projects": _r.randint(3, 12),
        "maintenance_orders_30d": _r.randint(80, 200),
        "p1_emergency_orders": _r.randint(2, 15),
        "active_maintenance_orders": _r.randint(20, 60),
        "data_source": "synthetic",
    })


@router.get("/api/capex/projects")
async def capex_projects(
    dnsp: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
) -> JSONResponse:
    """Capital project register."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if category:
        where.append(f"category = '{category}'")
    if status:
        where.append(f"status = '{status}'")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT project_id, dnsp, project_name, category, region, budget_m_aud,
               actual_spend_m_aud, completion_pct, start_date, target_completion_date,
               status, aer_approved
        FROM {_SCHEMA}.capital_projects
        {where_clause}
        ORDER BY budget_m_aud DESC
        LIMIT 100
    """)
    if rows:
        for r in rows:
            r["start_date"] = str(r.get("start_date", ""))
            r["target_completion_date"] = str(r.get("target_completion_date", ""))
        return JSONResponse({"projects": rows, "count": len(rows)})

    _r.seed(251)
    proj_cats = ["Reliability Augmentation", "Bushfire Mitigation", "DER Integration",
                 "Asset Replacement", "EV Network Readiness", "Zone Substation Upgrade"]
    proj_statuses = ["In Progress", "Complete", "On Hold", "Pending Approval"]
    today_dt = date.today()
    mock = []
    for i in range(40):
        d = dnsp or _r.choice(_DNSPS)
        cat = category or _r.choice(proj_cats)
        budget = round(_r.uniform(0.5, 50.0), 2)
        completion = round(_r.uniform(0, 100), 1)
        actual = round(budget * completion / 100 * _r.uniform(0.9, 1.15), 2)
        start = today_dt - timedelta(days=_r.randint(30, 730))
        target_end = start + timedelta(days=_r.randint(180, 1095))
        s = status or ("Complete" if completion >= 100 else _r.choice(proj_statuses[:3]))
        mock.append({
            "project_id": f"PROJ-{d[:3].upper()}-{i+1:04d}",
            "dnsp": d,
            "project_name": f"{cat} — Site {i+1}",
            "category": cat,
            "region": "VIC1" if d == "AusNet Services" else "QLD1",
            "budget_m_aud": budget,
            "actual_spend_m_aud": actual,
            "completion_pct": completion,
            "start_date": start.isoformat(),
            "target_completion_date": target_end.isoformat(),
            "status": s,
            "aer_approved": _r.random() > 0.1,
        })
    return JSONResponse({"projects": mock, "count": len(mock)})


@router.get("/api/capex/maintenance")
async def capex_maintenance(
    dnsp: Optional[str] = Query(None),
    priority: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
) -> JSONResponse:
    """Maintenance orders."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if priority:
        where.append(f"priority = '{priority}'")
    if status:
        where.append(f"status = '{status}'")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT order_id, dnsp, asset_id, order_type, priority, scheduled_date,
               completed_date, crew_id, contractor, status, cost_aud
        FROM {_SCHEMA}.maintenance_orders
        {where_clause}
        ORDER BY priority, scheduled_date ASC
        LIMIT 200
    """)
    if rows:
        for r in rows:
            r["scheduled_date"] = str(r.get("scheduled_date", ""))
            r["completed_date"] = str(r.get("completed_date", ""))
        return JSONResponse({"orders": rows, "count": len(rows)})

    _r.seed(252)
    order_types = ["Planned Inspection", "Corrective Maintenance", "Condition Based Maintenance",
                   "Emergency Response", "Vegetation Management"]
    priorities = ["P1 Emergency", "P2 Urgent", "P3 Routine", "P4 Planned"]
    priority_weights = [5, 15, 40, 40]
    today_dt = date.today()
    mock = []
    for i in range(60):
        d = dnsp or _r.choice(_DNSPS)
        p = priority or _r.choices(priorities, weights=priority_weights)[0]
        sched = today_dt - timedelta(days=_r.randint(-30, 180))
        complete = sched + timedelta(hours=_r.randint(1, 48)) if sched < today_dt and _r.random() > 0.2 else None
        s = status or ("Complete" if complete else ("In Progress" if sched <= today_dt else "Scheduled"))
        mock.append({
            "order_id": f"MO-{d[:3].upper()}-{i+1:05d}",
            "dnsp": d,
            "asset_id": f"ZS-MOCK-{i+1:02d}",
            "order_type": _r.choice(order_types),
            "priority": p,
            "scheduled_date": sched.isoformat(),
            "completed_date": complete.isoformat() if complete else "",
            "crew_id": f"CREW-{_r.randint(1,30):02d}",
            "contractor": _r.choice(["", "Silcar", "Downer EDI", "UGL", "Nexus"]),
            "status": s,
            "cost_aud": round(_r.uniform(500, 150000), 2),
        })
    return JSONResponse({"orders": mock, "count": len(mock)})


@router.get("/api/capex/fault-response")
async def capex_fault_response(
    dnsp: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
) -> JSONResponse:
    """Fault response KPIs."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if region:
        where.append(f"region = '{region}'")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT dnsp, region, period_month, fault_type, avg_response_time_min,
               avg_restoration_time_min, sla_target_min, sla_compliance_pct, total_faults
        FROM {_SCHEMA}.fault_response_kpis
        {where_clause}
        ORDER BY period_month DESC, fault_type
        LIMIT 120
    """)
    if rows:
        return JSONResponse({"kpis": rows, "count": len(rows)})

    _r.seed(253)
    fault_types = ["HV Overhead", "LV Overhead", "Underground Cable", "Transformer", "Switchgear"]
    sla_map = {"HV Overhead": 240, "LV Overhead": 300, "Underground Cable": 360,
               "Transformer": 480, "Switchgear": 300}
    today_dt = date.today()
    mock = []
    for d in ([dnsp] if dnsp else _DNSPS):
        r = region or ("VIC1" if d == "AusNet Services" else "QLD1")
        for m in range(12):
            month_date = today_dt.replace(day=1) - timedelta(days=m * 30)
            pm = month_date.strftime("%Y-%m")
            for ft in fault_types:
                sla = sla_map[ft]
                avg_resp = round(_r.uniform(20, 90), 1)
                avg_rest = round(_r.uniform(sla * 0.4, sla * 1.1), 0)
                mock.append({
                    "dnsp": d,
                    "region": r,
                    "period_month": pm,
                    "fault_type": ft,
                    "avg_response_time_min": avg_resp,
                    "avg_restoration_time_min": avg_rest,
                    "sla_target_min": float(sla),
                    "sla_compliance_pct": round(min(100, sla / max(avg_rest, 1) * 100 * _r.uniform(0.85, 1.0)), 1),
                    "total_faults": _r.randint(5, 80),
                })
    return JSONResponse({"kpis": mock, "count": len(mock)})


@router.get("/api/capex/contractors")
async def capex_contractors(dnsp: Optional[str] = Query(None)) -> JSONResponse:
    """Contractor performance scorecard."""
    where = f"WHERE dnsp = '{dnsp}'" if dnsp else ""
    rows = _query_gold(f"""
        SELECT contractor_id, dnsp, contractor_name, period_year, total_jobs,
               completed_on_time, defect_rate_pct, avg_cost_variance_pct, quality_score
        FROM {_SCHEMA}.contractor_performance
        {where}
        ORDER BY period_year DESC, quality_score DESC
    """)
    if rows:
        return JSONResponse({"contractors": rows, "count": len(rows)})

    _r.seed(254)
    contractors = ["Silcar", "Downer EDI", "UGL", "CPB Contractors", "Nexus Infrastructure"]
    mock = []
    for d in ([dnsp] if dnsp else _DNSPS):
        for c in contractors:
            for y in [2024, 2025, 2026]:
                total = _r.randint(50, 800)
                on_time = int(total * _r.uniform(0.75, 0.98))
                mock.append({
                    "contractor_id": f"CONTR-{c[:4].upper()}-{d[:3].upper()}",
                    "dnsp": d,
                    "contractor_name": c,
                    "period_year": y,
                    "total_jobs": total,
                    "completed_on_time": on_time,
                    "on_time_pct": round(on_time / total * 100, 1),
                    "defect_rate_pct": round(_r.uniform(0.5, 8.0), 2),
                    "avg_cost_variance_pct": round(_r.uniform(-5.0, 15.0), 2),
                    "quality_score": round(_r.uniform(65, 98), 1),
                })
    return JSONResponse({"contractors": mock, "count": len(mock)})


@router.get("/api/capex/capex-opex")
async def capex_opex(dnsp: Optional[str] = Query(None)) -> JSONResponse:
    """Capex vs opex analysis by asset."""
    where = f"WHERE dnsp = '{dnsp}'" if dnsp else ""
    rows = _query_gold(f"""
        SELECT dnsp, asset_id, asset_type, period_year, capex_aud, opex_aud,
               health_index, recommended_action
        FROM {_SCHEMA}.capex_opex_analysis
        {where}
        ORDER BY health_index ASC
        LIMIT 100
    """)
    if rows:
        return JSONResponse({"analysis": rows, "count": len(rows)})

    _r.seed(255)
    asset_types = ["zone_substation", "feeder", "distribution_transformer", "switchgear", "conductor"]
    actions = ["Monitor and review", "Opex maintenance", "Defer capex", "Schedule replacement",
               "Urgent replacement", "Refurbishment", "Technology upgrade"]
    mock = []
    for d in ([dnsp] if dnsp else _DNSPS):
        for i in range(30):
            hi = round(_r.uniform(20, 95), 1)
            capex = round(_r.uniform(0, 5000000) if hi < 50 else _r.uniform(0, 500000), 2)
            action = actions[0] if hi > 75 else (actions[-2] if hi < 35 else _r.choice(actions[1:5]))
            mock.append({
                "dnsp": d,
                "asset_id": f"ASSET-{d[:3].upper()}-{i+1:04d}",
                "asset_type": _r.choice(asset_types),
                "period_year": 2025,
                "capex_aud": capex,
                "opex_aud": round(_r.uniform(5000, 500000), 2),
                "health_index": hi,
                "recommended_action": action,
            })
    return JSONResponse({"analysis": mock, "count": len(mock)})


@router.get("/api/capex/delivery-trend")
async def capex_delivery_trend() -> JSONResponse:
    """Monthly capex delivery trend — last 12 months."""
    rows = _query_gold(f"""
        SELECT
            DATE_FORMAT(start_date, 'yyyy-MM') AS month,
            dnsp,
            ROUND(SUM(actual_spend_m_aud * completion_pct / 100), 2) AS monthly_spend_m
        FROM {_SCHEMA}.capital_projects
        WHERE start_date >= CURRENT_DATE() - INTERVAL 12 MONTHS
        GROUP BY DATE_FORMAT(start_date, 'yyyy-MM'), dnsp
        ORDER BY month ASC, dnsp
    """)
    if rows:
        return JSONResponse({"trend": rows, "count": len(rows)})

    _r.seed(256)
    today_dt = date.today()
    mock = []
    for m in range(12):
        month_date = today_dt.replace(day=1) - timedelta(days=m * 30)
        pm = month_date.strftime("%Y-%m")
        for d, base in [("AusNet Services", 12.0), ("Ergon Energy", 15.0), ("Energex", 18.0)]:
            mock.append({
                "month": pm,
                "dnsp": d,
                "monthly_spend_m": round(base * _r.uniform(0.6, 1.4), 2),
            })
    mock.sort(key=lambda x: x["month"])
    return JSONResponse({"trend": mock, "count": len(mock)})
