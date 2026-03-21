"""Rural Network & CSO — Energy Queensland Critical.

CSO payment history, rural feeder SAIDI by length tier,
RAPS fleet status, non-network alternatives, and Ergon vs Energex comparison.
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


@router.get("/api/rural/summary")
async def rural_summary() -> JSONResponse:
    """Rural/CSO KPI summary."""
    cache_key = "rural:summary"
    if cached := _cache_get(cache_key):
        return JSONResponse(cached)

    cso = _query_gold(f"""
        SELECT
            ROUND(SUM(cso_payment_m_aud), 2) AS total_cso_ytd,
            ROUND(AVG(cso_payment_m_aud / uneconomic_cost_m_aud * 100), 1) AS avg_cso_coverage_pct
        FROM {_SCHEMA}.cso_payments
        WHERE period_year = (SELECT MAX(period_year) FROM {_SCHEMA}.cso_payments)
    """)
    raps = _query_gold(f"""
        SELECT
            COUNT(*) AS total_raps_sites,
            COUNT(CASE WHEN site_status = 'OPERATIONAL' THEN 1 END) AS operational_sites,
            ROUND(SUM(solar_kw), 0) AS total_solar_kw,
            ROUND(SUM(battery_kwh), 0) AS total_battery_kwh
        FROM {_SCHEMA}.raps_fleet
    """)
    feeder = _query_gold(f"""
        SELECT
            ROUND(AVG(saidi_minutes), 1) AS avg_saidi,
            ROUND(AVG(CASE WHEN length_tier = '>200km' THEN saidi_minutes END), 1) AS avg_saidi_long_rural
        FROM {_SCHEMA}.rural_feeder_performance
    """)

    if cso and raps and feeder:
        result = {
            "total_cso_ytd_m": cso[0].get("total_cso_ytd", 0) or 0,
            "avg_cso_coverage_pct": cso[0].get("avg_cso_coverage_pct", 0) or 0,
            "total_raps_sites": raps[0].get("total_raps_sites", 0),
            "operational_raps_sites": raps[0].get("operational_sites", 0),
            "total_raps_solar_kw": raps[0].get("total_solar_kw", 0) or 0,
            "total_raps_battery_kwh": raps[0].get("total_battery_kwh", 0) or 0,
            "avg_saidi_all_feeders": feeder[0].get("avg_saidi", 0) or 0,
            "avg_saidi_long_rural": feeder[0].get("avg_saidi_long_rural", 0) or 0,
        }
        _cache_set(cache_key, result, ttl=300)
        return JSONResponse(result)

    _r.seed(230)
    return JSONResponse({
        "total_cso_ytd_m": round(_r.uniform(400, 600), 2),
        "avg_cso_coverage_pct": round(_r.uniform(80, 95), 1),
        "total_raps_sites": 80,
        "operational_raps_sites": _r.randint(68, 78),
        "total_raps_solar_kw": _r.randint(800, 1600),
        "total_raps_battery_kwh": _r.randint(2000, 4000),
        "avg_saidi_all_feeders": round(_r.uniform(180, 380), 1),
        "avg_saidi_long_rural": round(_r.uniform(400, 900), 1),
        "data_source": "synthetic",
    })


@router.get("/api/rural/cso")
async def rural_cso(
    dnsp: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
) -> JSONResponse:
    """CSO payment history."""
    where = ["dnsp = 'Ergon Energy'"]
    if dnsp:
        where[0] = f"dnsp = '{dnsp}'"
    if year:
        where.append(f"period_year = {year}")
    where_clause = "WHERE " + " AND ".join(where)

    rows = _query_gold(f"""
        SELECT dnsp, period_quarter, period_year, uneconomic_cost_m_aud,
               cso_payment_m_aud, payment_date, qld_govt_approved
        FROM {_SCHEMA}.cso_payments
        {where_clause}
        ORDER BY period_year DESC, period_quarter DESC
    """)
    if rows:
        for r in rows:
            r["payment_date"] = str(r.get("payment_date", ""))
        return JSONResponse({"cso": rows, "count": len(rows)})

    _r.seed(231)
    mock = []
    for y in ([year] if year else [2023, 2024, 2025, 2026]):
        for q in ["Q1", "Q2", "Q3", "Q4"]:
            month_map = {"Q1": 3, "Q2": 6, "Q3": 9, "Q4": 12}
            uneconomic = round(_r.uniform(120.0, 180.0), 2)
            cso_pay = round(uneconomic * _r.uniform(0.85, 0.95), 2)
            payment_date = date(y, month_map[q], 28)
            mock.append({
                "dnsp": dnsp or "Ergon Energy",
                "period_quarter": q,
                "period_year": y,
                "uneconomic_cost_m_aud": uneconomic,
                "cso_payment_m_aud": cso_pay,
                "payment_date": payment_date.isoformat(),
                "qld_govt_approved": payment_date < date.today(),
            })
    return JSONResponse({"cso": mock, "count": len(mock)})


@router.get("/api/rural/feeder-performance")
async def rural_feeder_performance(dnsp: Optional[str] = Query(None)) -> JSONResponse:
    """SAIDI by feeder length tier."""
    where = f"WHERE dnsp = '{dnsp}'" if dnsp else "WHERE dnsp = 'Ergon Energy'"
    rows = _query_gold(f"""
        SELECT dnsp, feeder_id, feeder_length_km, length_tier, saidi_minutes,
               saifi_count, customers_affected, period_year
        FROM {_SCHEMA}.rural_feeder_performance
        {where}
        ORDER BY length_tier, saidi_minutes DESC
    """)
    if rows:
        return JSONResponse({"feeders": rows, "count": len(rows)})

    _r.seed(232)
    length_tiers = [("<50km", 25), ("50-100km", 75), ("100-200km", 150), (">200km", 250)]
    mock = []
    for i in range(40):
        tier_name, tier_km = _r.choice(length_tiers)
        length = round(tier_km * _r.uniform(0.8, 1.2), 1)
        saidi = round(_r.uniform(50, 600) * (tier_km / 100), 1)
        mock.append({
            "dnsp": dnsp or "Ergon Energy",
            "feeder_id": f"FDR-ERGON-QLD1-{i+1:02d}",
            "feeder_length_km": length,
            "length_tier": tier_name,
            "saidi_minutes": saidi,
            "saifi_count": round(_r.uniform(0.5, 5.0), 2),
            "customers_affected": _r.randint(10, 500),
            "period_year": 2025,
        })
    return JSONResponse({"feeders": mock, "count": len(mock)})


@router.get("/api/rural/raps")
async def rural_raps(
    dnsp: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
) -> JSONResponse:
    """RAPS fleet status."""
    where = ["dnsp = 'Ergon Energy'"]
    if dnsp:
        where[0] = f"dnsp = '{dnsp}'"
    if region:
        where.append(f"region = '{region}'")
    where_clause = "WHERE " + " AND ".join(where)

    rows = _query_gold(f"""
        SELECT site_id, dnsp, region, customer_nmi, solar_kw, battery_kwh,
               diesel_kva, site_status, last_service_date
        FROM {_SCHEMA}.raps_fleet
        {where_clause}
        ORDER BY region, site_id
    """)
    if rows:
        for r in rows:
            r["last_service_date"] = str(r.get("last_service_date", ""))
        return JSONResponse({"raps": rows, "count": len(rows)})

    _r.seed(233)
    qld_regions = ["Cape York", "Gulf Country", "Torres Strait", "Central West",
                   "South West", "North West", "Outback QLD"]
    today_dt = date.today()
    mock = []
    for i in range(40):
        solar = _r.choice([5.0, 10.0, 15.0, 20.0, 30.0])
        mock.append({
            "site_id": f"RAPS-ERGON-{i+1:04d}",
            "dnsp": dnsp or "Ergon Energy",
            "region": region or _r.choice(qld_regions),
            "customer_nmi": f"NMI5{_r.randint(100000000, 999999999)}",
            "solar_kw": solar,
            "battery_kwh": round(solar * _r.uniform(2.0, 4.0), 1),
            "diesel_kva": float(_r.choice([5, 10, 15, 20, 30])),
            "site_status": _r.choices(["OPERATIONAL", "MAINTENANCE", "FAULT"], weights=[85, 10, 5])[0],
            "last_service_date": (today_dt - timedelta(days=_r.randint(30, 730))).isoformat(),
        })
    return JSONResponse({"raps": mock, "count": len(mock)})


@router.get("/api/rural/non-network")
async def rural_non_network() -> JSONResponse:
    """Non-network alternatives register."""
    rows = _query_gold(f"""
        SELECT constraint_id, dnsp, zone_substation, augmentation_cost_m_aud,
               non_network_cost_m_aud, recommended_solution, assessment_date, rit_d_required
        FROM {_SCHEMA}.non_network_alternatives
        ORDER BY augmentation_cost_m_aud DESC
    """)
    if rows:
        for r in rows:
            r["assessment_date"] = str(r.get("assessment_date", ""))
        return JSONResponse({"alternatives": rows, "count": len(rows)})

    _r.seed(234)
    solutions = ["Battery storage + demand management", "Solar + battery microgrid",
                 "RAPS hybrid system", "Demand response program", "Non-firm connection offer"]
    qld_subs = ["Toowoomba", "Rockhampton", "Townsville", "Cairns", "Mackay",
                "Bundaberg", "Mount Isa", "Longreach"]
    today_dt = date.today()
    mock = []
    for i in range(20):
        dnsp = _r.choice(["Ergon Energy", "AusNet Services"])
        aug = round(_r.uniform(5.0, 80.0), 2)
        nna = round(aug * _r.uniform(0.3, 0.8), 2)
        mock.append({
            "constraint_id": f"NNA-{i+1:03d}",
            "dnsp": dnsp,
            "zone_substation": f"ZS-{_r.choice(qld_subs).replace(' ','_').upper()}",
            "augmentation_cost_m_aud": aug,
            "non_network_cost_m_aud": nna,
            "recommended_solution": _r.choice(solutions),
            "assessment_date": (today_dt - timedelta(days=_r.randint(30, 365))).isoformat(),
            "rit_d_required": aug > 20.0,
        })
    return JSONResponse({"alternatives": mock, "count": len(mock)})


@router.get("/api/rural/ergon-energex")
async def rural_ergon_energex() -> JSONResponse:
    """Ergon vs Energex operational and financial comparison."""
    rows = _query_gold(f"""
        SELECT business_unit, period_year, saidi_minutes, saifi_count,
               network_km, customers, capex_m_aud, opex_m_aud, rab_m_aud
        FROM {_SCHEMA}.ergon_energex_split
        ORDER BY period_year DESC, business_unit
    """)
    if rows:
        return JSONResponse({"comparison": rows, "count": len(rows)})

    _r.seed(235)
    mock = []
    for y in [2023, 2024, 2025, 2026]:
        mock.append({
            "business_unit": "Ergon Energy",
            "period_year": y,
            "saidi_minutes": round(_r.uniform(280, 380), 1),
            "saifi_count": round(_r.uniform(3.2, 4.5), 2),
            "network_km": round(_r.uniform(163000, 165000), 0),
            "customers": _r.randint(730000, 750000),
            "capex_m_aud": round(_r.uniform(480, 560), 2),
            "opex_m_aud": round(_r.uniform(380, 420), 2),
            "rab_m_aud": round(_r.uniform(8500, 9200), 2),
        })
        mock.append({
            "business_unit": "Energex",
            "period_year": y,
            "saidi_minutes": round(_r.uniform(60, 90), 1),
            "saifi_count": round(_r.uniform(0.9, 1.3), 2),
            "network_km": round(_r.uniform(52000, 54000), 0),
            "customers": _r.randint(1430000, 1470000),
            "capex_m_aud": round(_r.uniform(550, 680), 2),
            "opex_m_aud": round(_r.uniform(420, 480), 2),
            "rab_m_aud": round(_r.uniform(10800, 11500), 2),
        })
    return JSONResponse({"comparison": mock, "count": len(mock)})
