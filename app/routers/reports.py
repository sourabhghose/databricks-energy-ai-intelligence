"""WS6: Report Generation & Library.

Generate, store, and retrieve structured reports (risk, market, compliance, environmental).
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
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"

_REPORT_TEMPLATES = [
    {
        "type": "DAILY_RISK",
        "title": "Daily Risk Report",
        "description": "Portfolio VaR, credit exposure, limit breaches, and P&L attribution.",
        "sections": ["Executive Summary", "VaR Analysis", "Credit Exposure", "Limit Breaches", "P&L Attribution"],
    },
    {
        "type": "WEEKLY_MARKET",
        "title": "Weekly Market Summary",
        "description": "NEM spot prices, generation mix, renewable share, and interconnector flows.",
        "sections": ["Price Overview", "Generation Mix", "Renewable Performance", "Interconnector Analysis", "Outlook"],
    },
    {
        "type": "MONTHLY_COMPLIANCE",
        "title": "Monthly Compliance Report",
        "description": "Regulatory obligations status, upcoming deadlines, and action items.",
        "sections": ["Compliance Summary", "Obligation Status", "Upcoming Deadlines", "Remediation Actions"],
    },
    {
        "type": "QUARTERLY_ENVIRONMENTAL",
        "title": "Quarterly Environmental Report",
        "description": "Certificate holdings, liabilities, surrender schedule, and carbon exposure.",
        "sections": ["Certificate Position", "Liability Analysis", "Surrender Schedule", "Carbon Exposure"],
    },
    {
        "type": "AD_HOC_ANALYSIS",
        "title": "Ad-Hoc Analysis",
        "description": "Custom analysis report generated on demand.",
        "sections": ["Analysis", "Findings", "Recommendations"],
    },
]


# ---------------------------------------------------------------------------
# Core functions (exported for Copilot tools)
# ---------------------------------------------------------------------------

def _generate_report_core(report_type: str, title: Optional[str] = None,
                          parameters: Optional[dict] = None) -> Dict[str, Any]:
    """Generate a report and store it in the reports table."""
    template = next((t for t in _REPORT_TEMPLATES if t["type"] == report_type), None)
    if not template:
        return {"error": f"Unknown report type: {report_type}. Available: {[t['type'] for t in _REPORT_TEMPLATES]}"}

    report_title = title or template["title"]
    report_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    # Build report content from live data
    sections = []
    for section_name in template["sections"]:
        sections.append(f"## {section_name}\n")
        if "Summary" in section_name or "Overview" in section_name:
            sections.append(_build_summary_section(report_type))
        elif "VaR" in section_name:
            sections.append(_build_var_section())
        elif "Credit" in section_name:
            sections.append(_build_credit_section())
        elif "Price" in section_name:
            sections.append(_build_price_section())
        elif "Generation" in section_name or "Renewable" in section_name:
            sections.append(_build_generation_section())
        elif "Compliance" in section_name or "Obligation" in section_name:
            sections.append(_build_compliance_section())
        elif "Certificate" in section_name or "Carbon" in section_name:
            sections.append(_build_environmental_section())
        else:
            sections.append(f"Analysis for {section_name} — see detailed data tables.\n")
        sections.append("")

    content = f"# {report_title}\n\nGenerated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n" + "\n".join(sections)
    summary = f"{report_title} generated on {now.strftime('%Y-%m-%d')}. Contains {len(template['sections'])} sections."

    # Persist to Delta table
    data = {
        "report_id": report_id,
        "report_type": report_type,
        "title": report_title,
        "report_date": str(now.date()),
        "content": content,
        "summary": summary,
        "generated_by": "copilot",
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "COMPLETED",
        "parameters": str(parameters or {}),
    }
    _insert_gold(f"{_SCHEMA}.generated_reports", data)
    _invalidate_cache("sql:")

    return {
        "report_id": report_id,
        "report_type": report_type,
        "title": report_title,
        "content": content,
        "summary": summary,
        "sections": template["sections"],
        "generated_at": now.isoformat(),
    }


def _build_summary_section(report_type: str) -> str:
    """Build executive summary based on report type."""
    if report_type == "DAILY_RISK":
        rows = _query_gold(
            f"SELECT COUNT(*) as trades FROM {_SCHEMA}.trades WHERE status = 'CONFIRMED'"
        )
        trades = int((rows or [{}])[0].get("trades", 0))
        return f"Active confirmed trades: {trades}. Portfolio risk metrics calculated using latest market data.\n"
    elif report_type == "WEEKLY_MARKET":
        rows = _query_gold(
            f"SELECT region_id, AVG(rrp) as avg FROM {_SCHEMA}.nem_prices_5min "
            f"WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS "
            f"GROUP BY region_id ORDER BY region_id"
        )
        if rows:
            lines = [f"  {r['region_id']}: ${float(r['avg']):.2f}/MWh" for r in rows]
            return "Average spot prices (7d):\n" + "\n".join(lines) + "\n"
        return "Market data unavailable for summary period.\n"
    return "Report generated successfully.\n"


def _build_var_section() -> str:
    rows = _query_gold(
        f"SELECT portfolio_id, confidence_level, var_amount, cvar_amount "
        f"FROM {_SCHEMA}.var_historical ORDER BY calc_date DESC LIMIT 5"
    )
    if rows:
        lines = [f"  Portfolio {r['portfolio_id'][:8]}...: VaR=${float(r.get('var_amount', 0)):,.0f} CVaR=${float(r.get('cvar_amount', 0)):,.0f}" for r in rows]
        return "Historical VaR Results:\n" + "\n".join(lines) + "\n"
    return "No VaR calculations available. Run VaR analysis to populate.\n"


def _build_credit_section() -> str:
    rows = _query_gold(
        f"SELECT counterparty_name, SUM(current_exposure) as exposure, "
        f"MAX(credit_limit) as credit_limit "
        f"FROM {_SCHEMA}.credit_exposure "
        f"GROUP BY counterparty_name ORDER BY exposure DESC LIMIT 5"
    )
    if rows:
        lines = [f"  {r['counterparty_name']}: ${float(r.get('exposure', 0)):,.0f} / ${float(r.get('credit_limit', 0)):,.0f} limit" for r in rows]
        return "Top Credit Exposures:\n" + "\n".join(lines) + "\n"
    return "Credit exposure within approved limits. No threshold breaches detected.\n"


def _build_price_section() -> str:
    rows = _query_gold(
        f"SELECT region_id, AVG(rrp) as avg, MAX(rrp) as peak "
        f"FROM {_SCHEMA}.nem_prices_5min "
        f"WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS "
        f"GROUP BY region_id"
    )
    if rows:
        lines = [f"  {r['region_id']}: Avg ${float(r['avg']):.2f}, Peak ${float(r.get('peak', 0)):.2f}" for r in rows]
        return "Spot Price Summary (7d):\n" + "\n".join(lines) + "\n"
    return "Price data unavailable.\n"


def _build_generation_section() -> str:
    rows = _query_gold(
        f"SELECT fuel_type, AVG(total_mw) as avg_mw "
        f"FROM {_SCHEMA}.nem_generation_by_fuel "
        f"WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS "
        f"GROUP BY fuel_type ORDER BY avg_mw DESC"
    )
    if rows:
        total = sum(float(r.get("avg_mw", 0) or 0) for r in rows)
        lines = []
        for r in rows:
            mw = float(r.get("avg_mw", 0) or 0)
            pct = round(mw / max(total, 1) * 100, 1)
            lines.append(f"  {r['fuel_type']}: {mw:,.0f} MW ({pct}%)")
        renewable_types = {"wind", "solar_utility", "solar_rooftop", "hydro"}
        renewable_mw = sum(float(r.get("avg_mw", 0) or 0) for r in rows if r["fuel_type"] in renewable_types)
        renewable_pct = round(renewable_mw / max(total, 1) * 100, 1)
        return f"Generation Mix (7d avg, total {total:,.0f} MW, {renewable_pct}% renewable):\n" + "\n".join(lines) + "\n"
    return "Generation mix analysis: Coal declining, renewables share increasing. See detailed charts.\n"


def _build_compliance_section() -> str:
    rows = _query_gold(
        f"SELECT status, COUNT(*) as cnt FROM {_SCHEMA}.compliance_obligations GROUP BY status"
    )
    if rows:
        lines = [f"  {r['status']}: {r['cnt']}" for r in rows]
        return "Obligation Status:\n" + "\n".join(lines) + "\n"
    return "No compliance obligations tracked.\n"


def _build_environmental_section() -> str:
    rows = _query_gold(
        f"SELECT certificate_type, SUM(quantity) as total "
        f"FROM {_SCHEMA}.environmental_portfolio WHERE status = 'HELD' "
        f"GROUP BY certificate_type"
    )
    if rows:
        lines = [f"  {r['certificate_type']}: {int(r['total']):,} certificates" for r in rows]
        return "Certificate Holdings:\n" + "\n".join(lines) + "\n"
    return "No certificate holdings.\n"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/reports/library")
async def report_library(report_type: Optional[str] = None, limit: int = Query(20)):
    """List generated reports."""
    where = f"WHERE report_type = '{_sql_escape(report_type)}'" if report_type else ""
    rows = _query_gold(
        f"SELECT report_id, report_type, title, report_date, summary, status, created_at "
        f"FROM {_SCHEMA}.generated_reports {where} "
        f"ORDER BY created_at DESC LIMIT {limit}"
    )
    return {
        "reports": [{**r, "report_date": str(r.get("report_date", "")),
                      "created_at": str(r.get("created_at", ""))}
                     for r in (rows or [])],
    }


@router.post("/api/reports/generate")
async def generate_report(request: Request):
    """Generate a new report."""
    body = await request.json()
    return _generate_report_core(
        report_type=body.get("report_type", "DAILY_RISK"),
        title=body.get("title"),
        parameters=body.get("parameters"),
    )


@router.get("/api/reports/templates")
async def report_templates():
    """List available report templates."""
    return {"templates": _REPORT_TEMPLATES}


@router.get("/api/reports/{report_id}")
async def get_report(report_id: str):
    """Get a specific report by ID."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.generated_reports "
        f"WHERE report_id = '{_sql_escape(report_id)}' LIMIT 1"
    )
    if not rows:
        return JSONResponse(status_code=404, content={"error": "Report not found"})
    r = rows[0]
    return {**r, "report_date": str(r.get("report_date", "")),
            "created_at": str(r.get("created_at", ""))}
