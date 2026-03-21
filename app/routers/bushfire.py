"""Bushfire Mitigation Program (BMP) — AusNet Critical.

ELC inspections, BMP asset register, fire risk incidents,
seasonal readiness checklist, and BMP capex tracking.
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
_BMO_ZONES = [
    "BMO-1 Dandenong Ranges", "BMO-2 Kinglake", "BMO-3 Yarra Valley",
    "BMO-4 Mornington Peninsula", "BMO-5 Macedon Ranges",
]


@router.get("/api/bushfire/summary")
async def bushfire_summary() -> JSONResponse:
    """BMP compliance KPI summary."""
    cache_key = "bushfire:summary"
    if cached := _cache_get(cache_key):
        return JSONResponse(cached)

    asset_rows = _query_gold(f"""
        SELECT
            COUNT(*) AS total_assets,
            COUNT(CASE WHEN fire_risk_rating IN ('EXTREME','HIGH') THEN 1 END) AS high_risk_assets,
            COUNT(CASE WHEN clearance_status = 'COMPLIANT' THEN 1 END) AS compliant_assets,
            COUNT(CASE WHEN clearance_status = 'OVERDUE' THEN 1 END) AS overdue_assets
        FROM {_SCHEMA}.bmp_asset_register
        WHERE dnsp = 'AusNet Services'
    """)
    elc_rows = _query_gold(f"""
        SELECT
            COUNT(*) AS total_inspections,
            COUNT(CASE WHEN clearance_status = 'NON_COMPLIANT' THEN 1 END) AS non_compliant_spans,
            ROUND(100.0 * COUNT(CASE WHEN clearance_status = 'COMPLIANT' THEN 1 END) / COUNT(*), 1) AS compliance_pct
        FROM {_SCHEMA}.elc_inspections
        WHERE dnsp = 'AusNet Services'
          AND inspection_date >= CURRENT_DATE() - INTERVAL 90 DAYS
    """)
    spend_rows = _query_gold(f"""
        SELECT
            ROUND(SUM(approved_allowance_m_aud), 2) AS total_allowance,
            ROUND(SUM(actual_spend_m_aud), 2) AS total_spend
        FROM {_SCHEMA}.bmp_spend
        WHERE dnsp = 'AusNet Services'
          AND period_year = (SELECT MAX(period_year) FROM {_SCHEMA}.bmp_spend)
    """)

    if asset_rows and elc_rows and spend_rows:
        total = asset_rows[0].get("total_assets", 0) or 1
        compliant = asset_rows[0].get("compliant_assets", 0) or 0
        result = {
            "total_assets": total,
            "high_risk_assets": asset_rows[0].get("high_risk_assets", 0),
            "asset_compliance_pct": round(compliant / total * 100, 1),
            "overdue_assets": asset_rows[0].get("overdue_assets", 0),
            "elc_inspections_90d": elc_rows[0].get("total_inspections", 0),
            "non_compliant_spans": elc_rows[0].get("non_compliant_spans", 0),
            "elc_compliance_pct": elc_rows[0].get("compliance_pct", 0) or 0,
            "bmp_allowance_m_aud": spend_rows[0].get("total_allowance", 0),
            "bmp_spend_m_aud": spend_rows[0].get("total_spend", 0),
        }
        _cache_set(cache_key, result, ttl=300)
        return JSONResponse(result)

    _r.seed(220)
    return JSONResponse({
        "total_assets": 200,
        "high_risk_assets": _r.randint(40, 80),
        "asset_compliance_pct": round(_r.uniform(72, 92), 1),
        "overdue_assets": _r.randint(5, 30),
        "elc_inspections_90d": _r.randint(150, 350),
        "non_compliant_spans": _r.randint(10, 60),
        "elc_compliance_pct": round(_r.uniform(75, 95), 1),
        "bmp_allowance_m_aud": round(_r.uniform(280, 380), 2),
        "bmp_spend_m_aud": round(_r.uniform(250, 360), 2),
        "data_source": "synthetic",
    })


@router.get("/api/bushfire/assets")
async def bushfire_assets(
    dnsp: Optional[str] = Query(None, description="DNSP filter"),
    risk_rating: Optional[str] = Query(None, description="EXTREME/HIGH/MEDIUM/LOW"),
    clearance_status: Optional[str] = Query(None),
) -> JSONResponse:
    """BMP asset register."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if risk_rating:
        where.append(f"fire_risk_rating = '{risk_rating}'")
    if clearance_status:
        where.append(f"clearance_status = '{clearance_status}'")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT asset_id, dnsp, asset_type, bmo_zone, fire_risk_rating,
               last_inspection_date, next_inspection_date, clearance_status,
               action_required, region
        FROM {_SCHEMA}.bmp_asset_register
        {where_clause}
        ORDER BY fire_risk_rating, last_inspection_date ASC
        LIMIT 200
    """)
    if rows:
        for r in rows:
            r["last_inspection_date"] = str(r.get("last_inspection_date", ""))
            r["next_inspection_date"] = str(r.get("next_inspection_date", ""))
        return JSONResponse({"assets": rows, "count": len(rows)})

    _r.seed(221)
    fire_ratings = ["EXTREME", "HIGH", "MEDIUM", "LOW"]
    fire_weights = [15, 35, 35, 15]
    clearance_options = ["COMPLIANT", "ACTION_REQUIRED", "OVERDUE", "UNDER_REVIEW"]
    asset_types = ["aerial_conductor", "underground_cable", "distribution_transformer",
                   "zone_substation_feeder", "overhead_span"]
    mock = []
    today_dt = date.today()
    for i in range(50):
        fr = risk_rating or _r.choices(fire_ratings, weights=fire_weights)[0]
        cl = clearance_status or _r.choices(clearance_options, weights=[50, 25, 15, 10])[0]
        last_insp = today_dt - timedelta(days=_r.randint(10, 400))
        mock.append({
            "asset_id": f"BMP-AUSNET-{i+1:04d}",
            "dnsp": dnsp or "AusNet Services",
            "asset_type": _r.choice(asset_types),
            "bmo_zone": _r.choice(_BMO_ZONES),
            "fire_risk_rating": fr,
            "last_inspection_date": last_insp.isoformat(),
            "next_inspection_date": (last_insp + timedelta(days=180 if fr in ("EXTREME","HIGH") else 365)).isoformat(),
            "clearance_status": cl,
            "action_required": "" if cl == "COMPLIANT" else _r.choice(["Vegetation trim", "Conductor replacement", "Clearance audit"]),
            "region": "VIC1",
        })
    return JSONResponse({"assets": mock, "count": len(mock)})


@router.get("/api/bushfire/elc")
async def bushfire_elc(
    dnsp: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
) -> JSONResponse:
    """ELC inspection status."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if status:
        where.append(f"clearance_status = '{status}'")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT inspection_id, dnsp, feeder_id, span_id, inspection_date,
               inspector, clearance_status, vegetation_species, encroachment_m, action_taken
        FROM {_SCHEMA}.elc_inspections
        {where_clause}
        ORDER BY inspection_date DESC
        LIMIT 200
    """)
    if rows:
        for r in rows:
            r["inspection_date"] = str(r.get("inspection_date", ""))
        return JSONResponse({"inspections": rows, "count": len(rows)})

    _r.seed(222)
    species = ["Eucalyptus", "Acacia", "Melaleuca", "Callitris", "Pinus radiata"]
    today_dt = date.today()
    mock = []
    for i in range(60):
        enc = round(_r.uniform(0.0, 4.5), 2)
        cl = status or ("NON_COMPLIANT" if enc > 3.0 else ("WARNING" if enc > 1.5 else "COMPLIANT"))
        mock.append({
            "inspection_id": f"ELC-{i+1:05d}",
            "dnsp": dnsp or "AusNet Services",
            "feeder_id": f"FDR-AUSNET-VIC1-{_r.randint(1,20):02d}",
            "span_id": f"SPAN-FDR-AUSNET-{_r.randint(1,50):03d}",
            "inspection_date": (today_dt - timedelta(days=_r.randint(1, 180))).isoformat(),
            "inspector": f"INSP-{_r.randint(1,20):02d}",
            "clearance_status": cl,
            "vegetation_species": _r.choice(species),
            "encroachment_m": enc,
            "action_taken": "None required" if cl == "COMPLIANT" else _r.choice(["Trim scheduled", "Emergency trim completed", "Referral to contractor"]),
        })
    return JSONResponse({"inspections": mock, "count": len(mock)})


@router.get("/api/bushfire/incidents")
async def bushfire_incidents(
    dnsp: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
) -> JSONResponse:
    """Fire incidents."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if year:
        where.append(f"YEAR(incident_date) = {year}")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT incident_id, dnsp, incident_date, feeder_id, region,
               fire_origin_electrical, esv_notified, esv_notification_date,
               customers_affected, fire_ha, corrective_action
        FROM {_SCHEMA}.fire_risk_incidents
        {where_clause}
        ORDER BY incident_date DESC
        LIMIT 100
    """)
    if rows:
        for r in rows:
            r["incident_date"] = str(r.get("incident_date", ""))
            r["esv_notification_date"] = str(r.get("esv_notification_date", ""))
        return JSONResponse({"incidents": rows, "count": len(rows)})

    _r.seed(223)
    today_dt = date.today()
    actions = ["Asset replacement", "Enhanced inspection schedule", "Vegetation management", "No further action"]
    mock = []
    for i in range(25):
        electrical = _r.random() > 0.4
        esv = electrical and _r.random() > 0.1
        inc_date = today_dt - timedelta(days=_r.randint(1, 730))
        if year and inc_date.year != year:
            continue
        mock.append({
            "incident_id": f"FRI-AUSNET-{i+1:04d}",
            "dnsp": dnsp or "AusNet Services",
            "incident_date": inc_date.isoformat(),
            "feeder_id": f"FDR-AUSNET-VIC1-{_r.randint(1,20):02d}",
            "region": "VIC1",
            "fire_origin_electrical": electrical,
            "esv_notified": esv,
            "esv_notification_date": (inc_date + timedelta(days=_r.randint(0, 2))).isoformat() if esv else "",
            "customers_affected": _r.randint(0, 2000) if electrical else 0,
            "fire_ha": round(_r.uniform(0.1, 500.0), 1) if electrical else 0.0,
            "corrective_action": _r.choice(actions),
        })
    return JSONResponse({"incidents": mock, "count": len(mock)})


@router.get("/api/bushfire/spend")
async def bushfire_spend(dnsp: Optional[str] = Query(None)) -> JSONResponse:
    """BMP capex vs AER allowance."""
    where = f"WHERE dnsp = '{dnsp}'" if dnsp else "WHERE dnsp = 'AusNet Services'"
    rows = _query_gold(f"""
        SELECT dnsp, period_year, category, approved_allowance_m_aud,
               actual_spend_m_aud, projects_complete, projects_total
        FROM {_SCHEMA}.bmp_spend
        {where}
        ORDER BY period_year DESC, category
    """)
    if rows:
        return JSONResponse({"spend": rows, "count": len(rows)})

    _r.seed(224)
    bmp_cats = ["ELC Inspections", "Conductor Replacement", "Insulation Works",
                "Vegetation Management", "Asset Monitoring", "Technology Upgrades"]
    mock = []
    for y in [2024, 2025, 2026]:
        for cat in bmp_cats:
            allowance = round(_r.uniform(10.0, 85.0), 2)
            actual = round(allowance * _r.uniform(0.75, 1.10), 2)
            total_p = _r.randint(8, 40)
            complete = int(total_p * _r.uniform(0.5, 1.0)) if y < 2026 else int(total_p * _r.uniform(0.2, 0.8))
            mock.append({
                "dnsp": dnsp or "AusNet Services",
                "period_year": y,
                "category": cat,
                "approved_allowance_m_aud": allowance,
                "actual_spend_m_aud": actual,
                "projects_complete": complete,
                "projects_total": total_p,
            })
    return JSONResponse({"spend": mock, "count": len(mock)})


@router.get("/api/bushfire/seasonal-readiness")
async def bushfire_seasonal_readiness(dnsp: Optional[str] = Query(None)) -> JSONResponse:
    """Pre-summer seasonal readiness checklist."""
    where = f"WHERE dnsp = '{dnsp}'" if dnsp else "WHERE dnsp = 'AusNet Services'"
    rows = _query_gold(f"""
        SELECT dnsp, season_year, check_category, check_item,
               target_completion_date, actual_completion_date, status, completion_pct
        FROM {_SCHEMA}.seasonal_readiness
        {where}
        ORDER BY season_year DESC, check_category, check_item
    """)
    if rows:
        for r in rows:
            r["target_completion_date"] = str(r.get("target_completion_date", ""))
            r["actual_completion_date"] = str(r.get("actual_completion_date", ""))
        return JSONResponse({"readiness": rows, "count": len(rows)})

    _r.seed(225)
    items = [
        ("ELC Inspections", "Complete all BMO zone ELC inspections"),
        ("ELC Inspections", "Clear non-compliant vegetation from high-risk spans"),
        ("Asset Condition", "Complete thermal imaging of zone substations"),
        ("Operational", "Update emergency response plans"),
        ("Operational", "Complete field crew summer fire training"),
        ("Technology", "Test and calibrate rapidearth fault indicators"),
        ("Customer Comms", "Issue pre-summer customer advisories"),
    ]
    today_dt = date.today()
    mock = []
    for y in [2025, 2026]:
        for cat, item in items:
            target = date(y - 1, 11, 30)
            pct = round(_r.uniform(60, 100), 1)
            actual = target - timedelta(days=_r.randint(0, 14)) if pct >= 100 else None
            status = "COMPLETE" if pct >= 100 else ("IN PROGRESS" if pct > 0 else "NOT STARTED")
            mock.append({
                "dnsp": dnsp or "AusNet Services",
                "season_year": y,
                "check_category": cat,
                "check_item": item,
                "target_completion_date": target.isoformat(),
                "actual_completion_date": actual.isoformat() if actual else "",
                "status": status,
                "completion_pct": pct,
            })
    return JSONResponse({"readiness": mock, "count": len(mock)})


@router.get("/api/bushfire/risk-zones")
async def bushfire_risk_zones() -> JSONResponse:
    """Zone risk summary by BMO region."""
    rows = _query_gold(f"""
        SELECT bmo_zone, region,
               COUNT(*) AS total_assets,
               COUNT(CASE WHEN fire_risk_rating = 'EXTREME' THEN 1 END) AS extreme_count,
               COUNT(CASE WHEN fire_risk_rating = 'HIGH' THEN 1 END) AS high_count,
               COUNT(CASE WHEN clearance_status = 'OVERDUE' THEN 1 END) AS overdue_count,
               ROUND(100.0 * COUNT(CASE WHEN clearance_status = 'COMPLIANT' THEN 1 END) / COUNT(*), 1) AS compliance_pct
        FROM {_SCHEMA}.bmp_asset_register
        WHERE dnsp = 'AusNet Services'
        GROUP BY bmo_zone, region
        ORDER BY extreme_count DESC
    """)
    if rows:
        return JSONResponse({"zones": rows, "count": len(rows)})

    _r.seed(226)
    mock = []
    for zone in _BMO_ZONES:
        total = _r.randint(20, 60)
        extreme = _r.randint(2, 15)
        high = _r.randint(5, 25)
        overdue = _r.randint(0, 10)
        mock.append({
            "bmo_zone": zone,
            "region": "VIC1",
            "total_assets": total,
            "extreme_count": extreme,
            "high_count": high,
            "overdue_count": overdue,
            "compliance_pct": round((total - overdue - extreme * 0.3) / total * 100, 1),
        })
    return JSONResponse({"zones": mock, "count": len(mock)})
