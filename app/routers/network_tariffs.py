"""Network Tariff Analytics.

Tariff structure management, customer migration progress tracking,
revenue by customer class, and demand tariff performance.
"""
from __future__ import annotations

import random as _r
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


@router.get("/api/tariffs/summary")
async def tariff_summary() -> JSONResponse:
    """Tariff KPI summary."""
    cache_key = "tariffs:summary"
    if cached := _cache_get(cache_key):
        return JSONResponse(cached)

    rows = _query_gold(f"""
        SELECT COUNT(DISTINCT tariff_id) AS total_tariffs,
               COUNT(DISTINCT dnsp) AS dnsp_count,
               COUNT(DISTINCT customer_class) AS customer_class_count
        FROM {_SCHEMA}.network_tariff_structures
    """)
    migration = _query_gold(f"""
        SELECT AVG(tou_pct + demand_pct) AS avg_cost_reflective_pct,
               AVG(aer_target_cost_reflective_pct) AS avg_aer_target_pct
        FROM {_SCHEMA}.tariff_migration_progress
        WHERE period_year = (SELECT MAX(period_year) FROM {_SCHEMA}.tariff_migration_progress)
          AND customer_class = 'residential'
    """)
    if rows and migration:
        result = {
            "total_tariffs": rows[0].get("total_tariffs", 0),
            "dnsp_count": rows[0].get("dnsp_count", 0),
            "customer_class_count": rows[0].get("customer_class_count", 0),
            "avg_cost_reflective_pct": round(migration[0].get("avg_cost_reflective_pct", 0) or 0, 1),
            "avg_aer_target_pct": round(migration[0].get("avg_aer_target_pct", 0) or 0, 1),
        }
        _cache_set(cache_key, result, ttl=300)
        return JSONResponse(result)

    _r.seed(210)
    return JSONResponse({
        "total_tariffs": 36,
        "dnsp_count": 3,
        "customer_class_count": 4,
        "avg_cost_reflective_pct": round(_r.uniform(25, 55), 1),
        "avg_aer_target_pct": 65.0,
        "data_source": "synthetic",
    })


@router.get("/api/tariffs/structures")
async def tariff_structures(
    dnsp: Optional[str] = Query(None),
    customer_class: Optional[str] = Query(None),
) -> JSONResponse:
    """Tariff structure list."""
    where = []
    if dnsp:
        where.append(f"dnsp = '{dnsp}'")
    if customer_class:
        where.append(f"customer_class = '{customer_class}'")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    rows = _query_gold(f"""
        SELECT tariff_id, dnsp, tariff_name, tariff_type, customer_class,
               fixed_charge_aud_day, energy_charge_aud_kwh, demand_charge_aud_kw,
               peak_hours, effective_date
        FROM {_SCHEMA}.network_tariff_structures
        {where_clause}
        ORDER BY dnsp, customer_class, tariff_type
    """)
    if rows:
        for r in rows:
            r["effective_date"] = str(r.get("effective_date", ""))
        return JSONResponse({"structures": rows, "count": len(rows)})

    _r.seed(211)
    mock = []
    tariff_types = ["flat_rate", "tou", "demand", "inclining_block"]
    classes = ["residential", "small_business", "large_business", "industrial"]
    for d in ([dnsp] if dnsp else _DNSPS):
        for cc in ([customer_class] if customer_class else classes):
            for tt in tariff_types:
                mock.append({
                    "tariff_id": f"TAR-{d[:3].upper()}-{cc[:3].upper()}-{tt[:3].upper()}",
                    "dnsp": d,
                    "tariff_name": f"{d} {cc.replace('_',' ').title()} {tt.upper()}",
                    "tariff_type": tt,
                    "customer_class": cc,
                    "fixed_charge_aud_day": round(_r.uniform(0.20, 0.50), 4),
                    "energy_charge_aud_kwh": round(_r.uniform(0.04, 0.15), 5),
                    "demand_charge_aud_kw": round(_r.uniform(8.0, 25.0), 2) if tt == "demand" else 0.0,
                    "peak_hours": "7am-9am, 5pm-8pm" if tt in ("tou", "demand") else "all_day",
                    "effective_date": "2025-07-01",
                })
    return JSONResponse({"structures": mock, "count": len(mock)})


@router.get("/api/tariffs/migration")
async def tariff_migration(dnsp: Optional[str] = Query(None)) -> JSONResponse:
    """Customer migration progress to cost-reflective tariffs."""
    where = f"WHERE dnsp = '{dnsp}'" if dnsp else ""
    rows = _query_gold(f"""
        SELECT dnsp, period_year, customer_class, flat_rate_pct, tou_pct,
               demand_pct, inclining_block_pct, aer_target_cost_reflective_pct
        FROM {_SCHEMA}.tariff_migration_progress
        {where}
        ORDER BY dnsp, period_year, customer_class
    """)
    if rows:
        return JSONResponse({"migration": rows, "count": len(rows)})

    _r.seed(212)
    mock = []
    classes = ["residential", "small_business", "large_business"]
    for d in ([dnsp] if dnsp else _DNSPS):
        for y in [2023, 2024, 2025, 2026]:
            for cc in classes:
                tou_base = {"residential": 0.15, "small_business": 0.30, "large_business": 0.55}[cc]
                demand_base = {"residential": 0.02, "small_business": 0.10, "large_business": 0.30}[cc]
                pf = (y - 2022) * 0.08
                tou = min(0.80, tou_base + pf + _r.uniform(-0.02, 0.02))
                demand = min(0.50, demand_base + pf * 0.5 + _r.uniform(-0.01, 0.01))
                flat = max(0.05, 1.0 - tou - demand - 0.05)
                iblock = max(0.0, 1.0 - flat - tou - demand)
                mock.append({
                    "dnsp": d,
                    "period_year": y,
                    "customer_class": cc,
                    "flat_rate_pct": round(flat * 100, 1),
                    "tou_pct": round(tou * 100, 1),
                    "demand_pct": round(demand * 100, 1),
                    "inclining_block_pct": round(iblock * 100, 1),
                    "aer_target_cost_reflective_pct": {"residential": 65.0, "small_business": 75.0, "large_business": 90.0}[cc],
                })
    return JSONResponse({"migration": mock, "count": len(mock)})


@router.get("/api/tariffs/revenue-by-class")
async def tariff_revenue_by_class() -> JSONResponse:
    """Revenue by tariff class vs allocated cost."""
    rows = _query_gold(f"""
        SELECT dnsp, period_year, customer_class, actual_revenue_m_aud,
               allocated_cost_m_aud, cost_recovery_ratio
        FROM {_SCHEMA}.tariff_revenue_by_class
        ORDER BY dnsp, period_year DESC, customer_class
    """)
    if rows:
        return JSONResponse({"revenue_by_class": rows, "count": len(rows)})

    _r.seed(213)
    mock = []
    class_splits = {"residential": 0.45, "small_business": 0.25, "large_business": 0.20, "industrial": 0.10}
    for d, total in [("AusNet Services", 950.0), ("Ergon Energy", 1200.0), ("Energex", 1800.0)]:
        for y in [2024, 2025, 2026]:
            for cc, split in class_splits.items():
                actual = round(total * split * _r.uniform(0.95, 1.05), 2)
                cost = round(total * split * _r.uniform(0.90, 1.10), 2)
                mock.append({
                    "dnsp": d,
                    "period_year": y,
                    "customer_class": cc,
                    "actual_revenue_m_aud": actual,
                    "allocated_cost_m_aud": cost,
                    "cost_recovery_ratio": round(actual / cost, 4),
                })
    return JSONResponse({"revenue_by_class": mock, "count": len(mock)})


@router.get("/api/tariffs/demand-performance")
async def tariff_demand_performance(dnsp: Optional[str] = Query(None)) -> JSONResponse:
    """Demand tariff customer performance."""
    where = f"WHERE dnsp = '{dnsp}'" if dnsp else ""
    rows = _query_gold(f"""
        SELECT dnsp, feeder_id, customer_nmi, contracted_peak_reduction_kw,
               actual_peak_reduction_kw, performance_ratio, period_year
        FROM {_SCHEMA}.demand_tariff_performance
        {where}
        ORDER BY performance_ratio ASC
        LIMIT 100
    """)
    if rows:
        return JSONResponse({"performance": rows, "count": len(rows)})

    _r.seed(214)
    mock = []
    for i in range(30):
        d = dnsp or _r.choice(_DNSPS)
        contracted = round(_r.uniform(5.0, 50.0), 1)
        actual = round(contracted * _r.uniform(0.65, 1.1), 1)
        mock.append({
            "dnsp": d,
            "feeder_id": f"FDR-{d[:3].upper()}-{i+1:02d}",
            "customer_nmi": f"NMI{_r.randint(3000000000, 3999999999)}",
            "contracted_peak_reduction_kw": contracted,
            "actual_peak_reduction_kw": actual,
            "performance_ratio": round(actual / contracted, 4),
            "period_year": 2025,
        })
    return JSONResponse({"performance": mock, "count": len(mock)})


@router.get("/api/tariffs/reform-tracker")
async def tariff_reform_tracker() -> JSONResponse:
    """Reform progress vs AER targets — latest year snapshot by DNSP."""
    rows = _query_gold(f"""
        SELECT dnsp, customer_class, flat_rate_pct, tou_pct, demand_pct,
               inclining_block_pct, aer_target_cost_reflective_pct,
               (tou_pct + demand_pct) AS current_cost_reflective_pct
        FROM {_SCHEMA}.tariff_migration_progress
        WHERE period_year = (SELECT MAX(period_year) FROM {_SCHEMA}.tariff_migration_progress)
        ORDER BY dnsp, customer_class
    """)
    if rows:
        return JSONResponse({"reform": rows, "count": len(rows)})

    _r.seed(215)
    mock = []
    for d in _DNSPS:
        for cc, target in [("residential", 65.0), ("small_business", 75.0), ("large_business", 90.0)]:
            current = round(_r.uniform(target * 0.5, target * 1.05), 1)
            mock.append({
                "dnsp": d,
                "customer_class": cc,
                "flat_rate_pct": round(100 - current - 5, 1),
                "tou_pct": round(current * 0.7, 1),
                "demand_pct": round(current * 0.3, 1),
                "inclining_block_pct": 5.0,
                "aer_target_cost_reflective_pct": target,
                "current_cost_reflective_pct": current,
                "gap_to_target_pct": round(target - current, 1),
            })
    return JSONResponse({"reform": mock, "count": len(mock)})
