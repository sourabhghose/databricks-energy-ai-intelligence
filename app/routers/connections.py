"""Connections & NER Compliance.

Connection application queue, NER timeframe compliance tracking,
large customer pipeline, and connection capacity by zone substation.
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


@router.get("/api/connections/summary")
async def connections_summary() -> JSONResponse:
    """Connection queue KPIs."""
    cache_key = "connections:summary"
    if cached := _cache_get(cache_key):
        return JSONResponse(cached)

    rows = _query_gold(f"""
        SELECT
            COUNT(*) AS total_applications,
            COUNT(CASE WHEN status = 'Pending' OR status = 'Under Assessment' THEN 1 END) AS pending,
            COUNT(CASE WHEN ner_compliant = TRUE THEN 1 END) AS ner_compliant,
            ROUND(100.0 * COUNT(CASE WHEN ner_compliant = TRUE THEN 1 END) / COUNT(*), 1) AS compliance_rate,
            ROUND(AVG(offer_price_aud), 0) AS avg_offer_price
        FROM {_SCHEMA}.connection_applications
    """)
    pipeline = _query_gold(f"""
        SELECT
            COUNT(*) AS large_customer_count,
            ROUND(SUM(capacity_mw), 1) AS total_pipeline_mw
        FROM {_SCHEMA}.large_customer_pipeline
        WHERE assessment_status NOT IN ('Accepted', 'Withdrawn')
    """)

    if rows and pipeline:
        result = {
            "total_applications": rows[0].get("total_applications", 0),
            "pending_applications": rows[0].get("pending", 0),
            "ner_compliant": rows[0].get("ner_compliant", 0),
            "ner_compliance_rate_pct": rows[0].get("compliance_rate", 0) or 0,
            "avg_offer_price_aud": rows[0].get("avg_offer_price", 0) or 0,
            "large_customer_pipeline_count": pipeline[0].get("large_customer_count", 0),
            "large_customer_pipeline_mw": pipeline[0].get("total_pipeline_mw", 0) or 0,
        }
        _cache_set(cache_key, result, ttl=300)
        return JSONResponse(result)

    _r.seed(240)
    return JSONResponse({
        "total_applications": 150,
        "pending_applications": _r.randint(20, 60),
        "ner_compliant": _r.randint(110, 145),
        "ner_compliance_rate_pct": round(_r.uniform(82, 97), 1),
        "avg_offer_price_aud": round(_r.uniform(3000, 15000), 0),
        "large_customer_pipeline_count": _r.randint(15, 35),
        "large_customer_pipeline_mw": round(_r.uniform(200, 800), 1),
        "data_source": "synthetic",
    })


@router.get("/api/connections/queue")
async def connections_queue(
    dnsp: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    application_type: Optional[str] = Query(None),
) -> JSONResponse:
    """Connection applications queue."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if status:
        where.append(f"status = '{status}'")
    if application_type:
        where.append(f"application_type = '{application_type}'")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT application_id, dnsp, application_date, application_type, customer_type,
               zone_substation, capacity_kw, status, offer_date, ner_deadline_date,
               ner_compliant, offer_price_aud
        FROM {_SCHEMA}.connection_applications
        {where_clause}
        ORDER BY application_date DESC
        LIMIT 150
    """)
    if rows:
        for r in rows:
            for col in ("application_date", "offer_date", "ner_deadline_date"):
                r[col] = str(r.get(col, ""))
        today_dt = date.today()
        for r in rows:
            if r.get("ner_deadline_date"):
                try:
                    ner_dt = date.fromisoformat(r["ner_deadline_date"][:10])
                    r["days_to_ner_deadline"] = (ner_dt - today_dt).days
                except Exception:
                    r["days_to_ner_deadline"] = None
        return JSONResponse({"applications": rows, "count": len(rows)})

    _r.seed(241)
    app_types = ["Standard Connection", "Large Customer", "EV Charger", "Solar Export", "Battery Storage"]
    cust_types = ["residential", "small_business", "commercial", "industrial"]
    statuses = ["Pending", "Offer Issued", "Accepted", "Rejected", "Under Assessment"]
    today_dt = date.today()
    mock = []
    for i in range(50):
        d = dnsp or _r.choice(_DNSPS)
        atype = application_type or _r.choice(app_types)
        app_date = today_dt - timedelta(days=_r.randint(1, 365))
        ner_days = {"Standard Connection": 65, "Large Customer": 120, "EV Charger": 30, "Solar Export": 45, "Battery Storage": 65}[atype]
        ner_deadline = app_date + timedelta(days=ner_days)
        s = status or _r.choice(statuses)
        mock.append({
            "application_id": f"CA-{d[:3].upper()}-{i+1:05d}",
            "dnsp": d,
            "application_date": app_date.isoformat(),
            "application_type": atype,
            "customer_type": _r.choice(cust_types),
            "zone_substation": f"ZS-MOCK-{i+1:02d}",
            "capacity_kw": round(_r.choice([6.6, 10, 30, 100, 500, 1000]), 1),
            "status": s,
            "offer_date": (app_date + timedelta(days=_r.randint(10, ner_days - 5))).isoformat() if s in ("Offer Issued", "Accepted") else "",
            "ner_deadline_date": ner_deadline.isoformat(),
            "ner_compliant": _r.random() > 0.1,
            "offer_price_aud": round(_r.uniform(500, 50000), 2),
            "days_to_ner_deadline": (ner_deadline - today_dt).days,
        })
    return JSONResponse({"applications": mock, "count": len(mock)})


@router.get("/api/connections/compliance")
async def connections_compliance(
    dnsp: Optional[str] = Query(None),
) -> JSONResponse:
    """NER timeframe compliance by application type and quarter."""
    where = f"WHERE dnsp = '{dnsp}'" if dnsp else ""
    rows = _query_gold(f"""
        SELECT dnsp, period_quarter, period_year, application_type,
               total_applications, compliant_count, compliance_rate_pct, ner_threshold_days
        FROM {_SCHEMA}.timely_connections_kpi
        {where}
        ORDER BY period_year DESC, period_quarter DESC, dnsp, application_type
    """)
    if rows:
        return JSONResponse({"compliance": rows, "count": len(rows)})

    _r.seed(242)
    mock = []
    for d in ([dnsp] if dnsp else _DNSPS):
        for y in [2025, 2026]:
            for q in ["Q1", "Q2", "Q3", "Q4"]:
                for atype, ner in [("Standard Connection", 65), ("Large Customer", 120), ("Solar Export", 45)]:
                    total = _r.randint(20, 200)
                    cr = round(_r.uniform(75, 99), 1)
                    mock.append({
                        "dnsp": d,
                        "period_quarter": q,
                        "period_year": y,
                        "application_type": atype,
                        "total_applications": total,
                        "compliant_count": int(total * cr / 100),
                        "compliance_rate_pct": cr,
                        "ner_threshold_days": ner,
                    })
    return JSONResponse({"compliance": mock, "count": len(mock)})


@router.get("/api/connections/large-customers")
async def connections_large_customers() -> JSONResponse:
    """Large customer connection pipeline."""
    rows = _query_gold(f"""
        SELECT application_id, dnsp, customer_name, industry, capacity_mw,
               zone_substation, network_impact_mw, assessment_status,
               application_date, target_energisation_date
        FROM {_SCHEMA}.large_customer_pipeline
        ORDER BY capacity_mw DESC
        LIMIT 50
    """)
    if rows:
        for r in rows:
            r["application_date"] = str(r.get("application_date", ""))
            r["target_energisation_date"] = str(r.get("target_energisation_date", ""))
        return JSONResponse({"pipeline": rows, "count": len(rows)})

    _r.seed(243)
    industries = ["Mining", "Data Centre", "Manufacturing", "Green Hydrogen", "EV Charging Hub",
                  "Cold Storage", "Agriculture Processing", "Water Treatment"]
    statuses = ["Pre-Application", "Under Assessment", "Offer Issued", "Accepted", "Withdrawn"]
    today_dt = date.today()
    mock = []
    for i in range(25):
        cap_mw = float(_r.choice([1, 2, 5, 10, 20, 50, 100]))
        app_date = today_dt - timedelta(days=_r.randint(30, 548))
        mock.append({
            "application_id": f"LCA-{i+1:04d}",
            "dnsp": _r.choice(_DNSPS),
            "customer_name": f"Customer {chr(65 + i % 26)}{i // 26 + 1} Pty Ltd",
            "industry": _r.choice(industries),
            "capacity_mw": cap_mw,
            "zone_substation": f"ZS-MOCK-{i+1:02d}",
            "network_impact_mw": round(cap_mw * _r.uniform(0.7, 1.0), 1),
            "assessment_status": _r.choice(statuses),
            "application_date": app_date.isoformat(),
            "target_energisation_date": (app_date + timedelta(days=_r.randint(365, 1095))).isoformat(),
        })
    return JSONResponse({"pipeline": mock, "count": len(mock)})


@router.get("/api/connections/capacity")
async def connections_capacity() -> JSONResponse:
    """Connection capacity summary by zone substation."""
    rows = _query_gold(f"""
        SELECT zone_substation, dnsp,
               COUNT(*) AS application_count,
               ROUND(SUM(capacity_kw), 0) AS total_applied_kw,
               ROUND(SUM(CASE WHEN status IN ('Accepted','Offer Issued') THEN capacity_kw ELSE 0 END), 0) AS accepted_kw,
               COUNT(CASE WHEN status = 'Pending' OR status = 'Under Assessment' THEN 1 END) AS pending_count
        FROM {_SCHEMA}.connection_applications
        GROUP BY zone_substation, dnsp
        ORDER BY total_applied_kw DESC
        LIMIT 30
    """)
    if rows:
        return JSONResponse({"capacity": rows, "count": len(rows)})

    _r.seed(244)
    zone_subs = [f"ZS-MOCK-{i:02d}" for i in range(1, 16)]
    mock = []
    for i, zs in enumerate(zone_subs):
        apps = _r.randint(5, 30)
        total_kw = round(_r.uniform(500, 50000), 0)
        mock.append({
            "zone_substation": zs,
            "dnsp": _r.choice(_DNSPS),
            "application_count": apps,
            "total_applied_kw": total_kw,
            "accepted_kw": round(total_kw * _r.uniform(0.3, 0.7), 0),
            "pending_count": _r.randint(1, int(apps * 0.5)),
        })
    return JSONResponse({"capacity": mock, "count": len(mock)})
