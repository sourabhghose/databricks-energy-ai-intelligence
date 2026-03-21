"""WS6: Compliance Dashboard.

Regulatory obligations, AER enforcement register, ESV incidents & regulations,
AEMO procedures, and market notices — all from real data.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from .shared import (
    _CATALOG,
    _insert_gold,
    _invalidate_cache,
    _query_gold,
    _sql_escape,
    _update_gold,
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"


# ---------------------------------------------------------------------------
# Core functions (exported for Copilot tools)
# ---------------------------------------------------------------------------

def _get_compliance_status_core(region: Optional[str] = None) -> Dict[str, Any]:
    """Get compliance status summary across all obligations."""
    where = f"WHERE region = '{_sql_escape(region)}'" if region else ""
    rows = _query_gold(
        f"SELECT status, COUNT(*) as cnt, "
        f"SUM(CASE WHEN priority = 'CRITICAL' THEN 1 ELSE 0 END) as critical, "
        f"SUM(CASE WHEN priority = 'HIGH' THEN 1 ELSE 0 END) as high "
        f"FROM {_SCHEMA}.compliance_obligations {where} "
        f"GROUP BY status"
    )

    overdue = _query_gold(
        f"SELECT COUNT(*) as cnt FROM {_SCHEMA}.compliance_obligations "
        f"{'WHERE' if not where else where + ' AND'} due_date < current_date() "
        f"AND status NOT IN ('COMPLIANT', 'COMPLETED')"
    )

    upcoming = _query_gold(
        f"SELECT obligation_id, obligation_type, title, due_date, status, priority, region "
        f"FROM {_SCHEMA}.compliance_obligations "
        f"{'WHERE' if not where else where + ' AND'} due_date >= current_date() "
        f"AND due_date <= current_date() + INTERVAL 30 DAYS "
        f"AND status NOT IN ('COMPLIANT', 'COMPLETED') "
        f"ORDER BY due_date LIMIT 10"
    )

    total = sum(int(r.get("cnt", 0)) for r in (rows or []))
    compliant = sum(int(r["cnt"]) for r in (rows or []) if r["status"] in ("COMPLIANT", "COMPLETED"))

    return {
        "total_obligations": total,
        "compliant_count": compliant,
        "compliance_rate": round(compliant / max(total, 1) * 100, 1),
        "overdue_count": int((overdue or [{}])[0].get("cnt", 0)),
        "status_breakdown": rows or [],
        "upcoming_deadlines": [{**r, "due_date": str(r.get("due_date", ""))}
                                for r in (upcoming or [])],
    }


def _get_enforcement_summary_core() -> Dict[str, Any]:
    """Get AER enforcement register summary."""
    actions = _query_gold(
        f"SELECT action_id, action_type, company_name, title, penalty_aud, "
        f"action_date, sector, status "
        f"FROM {_SCHEMA}.aer_enforcement_actions "
        f"ORDER BY action_date DESC LIMIT 50"
    )
    stats = _query_gold(
        f"SELECT action_type, COUNT(*) as cnt, SUM(penalty_aud) as total_penalties "
        f"FROM {_SCHEMA}.aer_enforcement_actions GROUP BY action_type"
    )
    total_penalties = sum(float(s.get("total_penalties", 0) or 0) for s in (stats or []))
    return {
        "total_actions": sum(int(s.get("cnt", 0)) for s in (stats or [])),
        "total_penalties_aud": round(total_penalties, 0),
        "by_type": stats or [],
        "recent_actions": [{**a, "action_date": str(a.get("action_date", ""))}
                           for a in (actions or [])],
    }


def _get_esv_summary_core() -> Dict[str, Any]:
    """Get ESV incidents and safety performance summary."""
    incidents = _query_gold(
        f"SELECT incident_id, incident_date, incident_type, severity, location, "
        f"distribution_business, description, injuries, fatalities, fire_ignitions "
        f"FROM {_SCHEMA}.esv_incidents ORDER BY incident_date DESC LIMIT 20"
    )
    performance = _query_gold(
        f"SELECT report_year, distribution_business, total_incidents, "
        f"serious_electrical_incidents, fire_incidents, fatalities, "
        f"powerline_bushfire_ignitions, esms_compliance_pct "
        f"FROM {_SCHEMA}.esv_safety_performance ORDER BY report_year DESC, distribution_business"
    )
    regulations = _query_gold(
        f"SELECT regulation_id, regulation_type, short_title, title, status, "
        f"effective_date, expiry_date, penalty_provisions "
        f"FROM {_SCHEMA}.esv_regulations ORDER BY regulation_type, title"
    )
    return {
        "incidents": [{**i, "incident_date": str(i.get("incident_date", ""))}
                      for i in (incidents or [])],
        "safety_performance": performance or [],
        "regulations": [{**r, "effective_date": str(r.get("effective_date", "")),
                         "expiry_date": str(r.get("expiry_date", ""))}
                        for r in (regulations or [])],
    }


def _get_aemo_procedures_core(category: Optional[str] = None) -> Dict[str, Any]:
    """Get AEMO procedures register."""
    where = f"WHERE category = '{_sql_escape(category)}'" if category else ""
    procedures = _query_gold(
        f"SELECT procedure_id, procedure_code, title, category, document_type, "
        f"description, effective_date, version, ner_clause, applicable_market, status "
        f"FROM {_SCHEMA}.aemo_procedures {where} "
        f"ORDER BY category, procedure_code"
    )
    categories = _query_gold(
        f"SELECT category, COUNT(*) as cnt "
        f"FROM {_SCHEMA}.aemo_procedures GROUP BY category ORDER BY cnt DESC"
    )
    return {
        "total_procedures": sum(int(c.get("cnt", 0)) for c in (categories or [])),
        "by_category": categories or [],
        "procedures": [{**p, "effective_date": str(p.get("effective_date", ""))}
                       for p in (procedures or [])],
    }


def _get_market_notices_core(region: Optional[str] = None,
                              notice_type: Optional[str] = None) -> Dict[str, Any]:
    """Get AEMO market notices."""
    parts = []
    if region:
        parts.append(f"region = '{_sql_escape(region)}'")
    if notice_type:
        parts.append(f"notice_type = '{_sql_escape(notice_type)}'")
    where = f"WHERE {' AND '.join(parts)}" if parts else ""

    notices = _query_gold(
        f"SELECT notice_id, notice_type, reason, region, "
        f"effective_date, external_reference "
        f"FROM {_SCHEMA}.nem_market_notices {where} "
        f"ORDER BY effective_date DESC LIMIT 50"
    )
    type_counts = _query_gold(
        f"SELECT notice_type, COUNT(*) as cnt "
        f"FROM {_SCHEMA}.nem_market_notices GROUP BY notice_type ORDER BY cnt DESC"
    )
    total = _query_gold(
        f"SELECT COUNT(*) as cnt FROM {_SCHEMA}.nem_market_notices"
    )
    return {
        "total_notices": int((total or [{}])[0].get("cnt", 0)),
        "by_type": type_counts or [],
        "recent_notices": [{**n, "effective_date": str(n.get("effective_date", ""))}
                           for n in (notices or [])],
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/compliance/dashboard")
async def compliance_dashboard(region: Optional[str] = None):
    """Compliance overview: obligations, enforcement, ESV, procedures, notices."""
    status = _get_compliance_status_core(region)

    timeline = _query_gold(
        f"SELECT obligation_type, title, due_date, status, priority, region "
        f"FROM {_SCHEMA}.compliance_obligations "
        f"ORDER BY due_date LIMIT 20"
    )

    # Get summary counts from all compliance data sources
    enforcement_count = _query_gold(
        f"SELECT COUNT(*) as cnt FROM {_SCHEMA}.aer_enforcement_actions"
    )
    esv_incident_count = _query_gold(
        f"SELECT COUNT(*) as cnt FROM {_SCHEMA}.esv_incidents"
    )
    regulation_count = _query_gold(
        f"SELECT COUNT(*) as cnt FROM {_SCHEMA}.esv_regulations"
    )
    procedure_count = _query_gold(
        f"SELECT COUNT(*) as cnt FROM {_SCHEMA}.aemo_procedures"
    )
    notice_count = _query_gold(
        f"SELECT COUNT(*) as cnt FROM {_SCHEMA}.nem_market_notices"
    )

    return {
        **status,
        "timeline": [{**r, "due_date": str(r.get("due_date", ""))} for r in (timeline or [])],
        "data_sources": {
            "enforcement_actions": int((enforcement_count or [{}])[0].get("cnt", 0)),
            "esv_incidents": int((esv_incident_count or [{}])[0].get("cnt", 0)),
            "esv_regulations": int((regulation_count or [{}])[0].get("cnt", 0)),
            "aemo_procedures": int((procedure_count or [{}])[0].get("cnt", 0)),
            "market_notices": int((notice_count or [{}])[0].get("cnt", 0)),
        },
    }


@router.get("/api/compliance/obligations")
async def list_obligations(region: Optional[str] = None,
                            status: Optional[str] = None,
                            obligation_type: Optional[str] = None,
                            limit: int = Query(50)):
    """List compliance obligations with filters."""
    parts = []
    if region:
        parts.append(f"region = '{_sql_escape(region)}'")
    if status:
        parts.append(f"status = '{_sql_escape(status)}'")
    if obligation_type:
        parts.append(f"obligation_type = '{_sql_escape(obligation_type)}'")
    where = f"WHERE {' AND '.join(parts)}" if parts else ""

    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.compliance_obligations {where} "
        f"ORDER BY due_date LIMIT {limit}"
    )
    return {
        "obligations": [
            {**r, "due_date": str(r.get("due_date", "")),
             "created_at": str(r.get("created_at", "")),
             "updated_at": str(r.get("updated_at", ""))}
            for r in (rows or [])
        ]
    }


@router.post("/api/compliance/obligations")
async def create_obligation(request: Request):
    """Create a new compliance obligation."""
    body = await request.json()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    data = {
        "obligation_id": str(uuid.uuid4()),
        "obligation_type": body.get("obligation_type", "NER_COMPLIANCE"),
        "title": body.get("title", "New Obligation"),
        "description": body.get("description", ""),
        "due_date": body.get("due_date", "2026-06-30"),
        "status": "PENDING",
        "region": body.get("region", "NSW1"),
        "responsible_party": body.get("responsible_party", "Compliance Team"),
        "priority": body.get("priority", "MEDIUM"),
        "created_at": now,
        "updated_at": now,
    }
    ok = _insert_gold(f"{_SCHEMA}.compliance_obligations", data)
    if ok:
        _invalidate_cache("sql:")
        return {"status": "created", "obligation_id": data["obligation_id"]}
    return JSONResponse(status_code=500, content={"error": "Failed to create obligation"})


@router.put("/api/compliance/obligations")
async def update_obligation(request: Request):
    """Update an existing compliance obligation."""
    body = await request.json()
    obligation_id = body.get("obligation_id")
    if not obligation_id:
        return JSONResponse(status_code=400, content={"error": "obligation_id required"})

    updates = {}
    for field in ["status", "priority", "title", "description", "due_date", "responsible_party"]:
        if field in body:
            updates[field] = body[field]
    updates["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    ok = _update_gold(
        f"{_SCHEMA}.compliance_obligations",
        updates,
        f"obligation_id = '{_sql_escape(obligation_id)}'"
    )
    if ok:
        _invalidate_cache("sql:")
        return {"status": "updated", "obligation_id": obligation_id}
    return JSONResponse(status_code=500, content={"error": "Failed to update obligation"})


@router.get("/api/compliance/timeline")
async def compliance_timeline(months_ahead: int = Query(6)):
    """Get compliance timeline for upcoming deadlines."""
    rows = _query_gold(
        f"SELECT obligation_id, obligation_type, title, due_date, status, priority, region "
        f"FROM {_SCHEMA}.compliance_obligations "
        f"WHERE due_date >= current_date() "
        f"AND due_date <= current_date() + INTERVAL {months_ahead} MONTHS "
        f"ORDER BY due_date LIMIT 50"
    )
    return {
        "months_ahead": months_ahead,
        "deadlines": [{**r, "due_date": str(r.get("due_date", ""))} for r in (rows or [])],
    }


# ---------------------------------------------------------------------------
# AER Enforcement Register
# ---------------------------------------------------------------------------

@router.get("/api/compliance/enforcement")
async def enforcement_register(sector: Optional[str] = None,
                                action_type: Optional[str] = None):
    """AER enforcement actions — real fines, companies, breaches."""
    parts = []
    if sector:
        parts.append(f"sector = '{_sql_escape(sector)}'")
    if action_type:
        parts.append(f"action_type = '{_sql_escape(action_type)}'")
    where = f"WHERE {' AND '.join(parts)}" if parts else ""

    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.aer_enforcement_actions {where} "
        f"ORDER BY action_date DESC LIMIT 100"
    )
    stats = _query_gold(
        f"SELECT action_type, COUNT(*) as cnt, SUM(penalty_aud) as total_penalties, "
        f"AVG(penalty_aud) as avg_penalty "
        f"FROM {_SCHEMA}.aer_enforcement_actions GROUP BY action_type"
    )
    sector_stats = _query_gold(
        f"SELECT sector, COUNT(*) as cnt, SUM(penalty_aud) as total_penalties "
        f"FROM {_SCHEMA}.aer_enforcement_actions GROUP BY sector"
    )
    return {
        "actions": [{**r, "action_date": str(r.get("action_date", ""))}
                    for r in (rows or [])],
        "by_type": stats or [],
        "by_sector": sector_stats or [],
    }


# ---------------------------------------------------------------------------
# ESV Incidents & Safety
# ---------------------------------------------------------------------------

@router.get("/api/compliance/esv/incidents")
async def esv_incidents(severity: Optional[str] = None,
                         distribution_business: Optional[str] = None):
    """ESV electrical incident reports."""
    parts = []
    if severity:
        parts.append(f"severity = '{_sql_escape(severity)}'")
    if distribution_business:
        parts.append(f"distribution_business = '{_sql_escape(distribution_business)}'")
    where = f"WHERE {' AND '.join(parts)}" if parts else ""

    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.esv_incidents {where} "
        f"ORDER BY incident_date DESC LIMIT 50"
    )
    return {
        "incidents": [{**r, "incident_date": str(r.get("incident_date", ""))}
                      for r in (rows or [])]
    }


@router.get("/api/compliance/esv/safety-performance")
async def esv_safety_performance(year: Optional[int] = None):
    """ESV annual safety performance by Victorian DNSP."""
    where = f"WHERE report_year = {year}" if year else ""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.esv_safety_performance {where} "
        f"ORDER BY report_year DESC, distribution_business"
    )
    return {"performance": rows or []}


@router.get("/api/compliance/esv/regulations")
async def esv_regulations(regulation_type: Optional[str] = None):
    """Victorian electrical safety regulations."""
    where = f"WHERE regulation_type = '{_sql_escape(regulation_type)}'" if regulation_type else ""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.esv_regulations {where} "
        f"ORDER BY regulation_type, title"
    )
    return {
        "regulations": [{**r, "effective_date": str(r.get("effective_date", "")),
                         "expiry_date": str(r.get("expiry_date", "")),
                         "last_amended": str(r.get("last_amended", ""))}
                        for r in (rows or [])]
    }


# ---------------------------------------------------------------------------
# AEMO Procedures
# ---------------------------------------------------------------------------

@router.get("/api/compliance/aemo/procedures")
async def aemo_procedures(category: Optional[str] = None,
                           market: Optional[str] = None):
    """AEMO NEM/WEM procedures and guidelines register."""
    parts = []
    if category:
        parts.append(f"category = '{_sql_escape(category)}'")
    if market:
        parts.append(f"applicable_market = '{_sql_escape(market)}'")
    where = f"WHERE {' AND '.join(parts)}" if parts else ""

    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.aemo_procedures {where} "
        f"ORDER BY category, procedure_code"
    )
    cats = _query_gold(
        f"SELECT category, COUNT(*) as cnt FROM {_SCHEMA}.aemo_procedures GROUP BY category"
    )
    return {
        "procedures": [{**r, "effective_date": str(r.get("effective_date", "")),
                        "last_reviewed": str(r.get("last_reviewed", "")),
                        "next_review_date": str(r.get("next_review_date", ""))}
                       for r in (rows or [])],
        "categories": cats or [],
    }


# ---------------------------------------------------------------------------
# Market Notices
# ---------------------------------------------------------------------------

@router.get("/api/compliance/market-notices")
async def market_notices(region: Optional[str] = None,
                          notice_type: Optional[str] = None,
                          limit: int = Query(50)):
    """AEMO market notices from NEMWEB."""
    parts = []
    if region:
        parts.append(f"region = '{_sql_escape(region)}'")
    if notice_type:
        parts.append(f"notice_type = '{_sql_escape(notice_type)}'")
    where = f"WHERE {' AND '.join(parts)}" if parts else ""

    rows = _query_gold(
        f"SELECT notice_id, notice_type, reason, region, effective_date, "
        f"external_reference "
        f"FROM {_SCHEMA}.nem_market_notices {where} "
        f"ORDER BY effective_date DESC LIMIT {limit}"
    )
    types = _query_gold(
        f"SELECT notice_type, COUNT(*) as cnt "
        f"FROM {_SCHEMA}.nem_market_notices GROUP BY notice_type ORDER BY cnt DESC"
    )
    return {
        "notices": [{**r, "effective_date": str(r.get("effective_date", ""))}
                    for r in (rows or [])],
        "by_type": types or [],
        "total": sum(int(t.get("cnt", 0)) for t in (types or [])),
    }
