"""WS5: Western Australian Electricity Market (WEM) Expansion.

Balancing prices, generation mix, demand, and NEM-WEM comparison.
"""
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Query

from .shared import (
    _CATALOG,
    _query_gold,
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"


# ---------------------------------------------------------------------------
# Core functions (exported for Copilot tools)
# ---------------------------------------------------------------------------

def _get_wem_price_core(days: int = 7) -> Dict[str, Any]:
    """Get WEM balancing prices and summary stats."""
    rows = _query_gold(
        f"SELECT trading_interval, balancing_price, total_balancing_demand_mw, "
        f"total_generation_mw, reserve_margin_mw, price_flag "
        f"FROM {_SCHEMA}.wem_balancing_prices "
        f"WHERE trading_interval >= current_timestamp() - INTERVAL {days} DAYS "
        f"ORDER BY trading_interval DESC LIMIT 500"
    )

    stats = _query_gold(
        f"SELECT AVG(balancing_price) as avg_price, MIN(balancing_price) as min_price, "
        f"MAX(balancing_price) as max_price, STDDEV(balancing_price) as vol, "
        f"AVG(total_balancing_demand_mw) as avg_demand, "
        f"SUM(CASE WHEN price_flag = 'HIGH' THEN 1 ELSE 0 END) as high_price_intervals "
        f"FROM {_SCHEMA}.wem_balancing_prices "
        f"WHERE trading_interval >= current_timestamp() - INTERVAL {days} DAYS"
    )

    return {
        "period_days": days,
        "prices": [{**r, "trading_interval": str(r.get("trading_interval", ""))}
                    for r in (rows or [])],
        "summary": stats[0] if stats else {},
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/wem/dashboard")
async def wem_dashboard(days: int = Query(7)):
    """WEM overview: prices, generation, demand."""
    prices = _get_wem_price_core(days)

    gen_mix = _query_gold(
        f"SELECT fuel_type, AVG(total_mw) as avg_mw, SUM(total_mw) as total_mw "
        f"FROM {_SCHEMA}.wem_generation "
        f"WHERE trading_interval >= current_timestamp() - INTERVAL {days} DAYS "
        f"GROUP BY fuel_type ORDER BY avg_mw DESC"
    )

    demand = _query_gold(
        f"SELECT AVG(total_demand_mw) as avg_demand, "
        f"MAX(total_demand_mw) as peak_demand, "
        f"MIN(total_demand_mw) as min_demand "
        f"FROM {_SCHEMA}.wem_demand "
        f"WHERE trading_interval >= current_timestamp() - INTERVAL {days} DAYS"
    )

    return {
        "period_days": days,
        "price_summary": prices["summary"],
        "generation_mix": gen_mix or [],
        "demand_summary": demand[0] if demand else {},
    }


@router.get("/api/wem/prices")
async def wem_prices(days: int = Query(7)):
    """Get WEM balancing prices."""
    return _get_wem_price_core(days)


@router.get("/api/wem/generation")
async def wem_generation(days: int = Query(7)):
    """Get WEM generation by fuel type."""
    rows = _query_gold(
        f"SELECT trading_interval, fuel_type, total_mw, capacity_factor "
        f"FROM {_SCHEMA}.wem_generation "
        f"WHERE trading_interval >= current_timestamp() - INTERVAL {days} DAYS "
        f"ORDER BY trading_interval DESC LIMIT 1000"
    )
    return {"generation": [{**r, "trading_interval": str(r.get("trading_interval", ""))}
                            for r in (rows or [])]}


@router.get("/api/wem/demand")
async def wem_demand(days: int = Query(7)):
    """Get WEM demand data."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.wem_demand "
        f"WHERE trading_interval >= current_timestamp() - INTERVAL {days} DAYS "
        f"ORDER BY trading_interval DESC LIMIT 500"
    )
    return {"demand": [{**r, "trading_interval": str(r.get("trading_interval", ""))}
                        for r in (rows or [])]}


@router.get("/api/wem/comparison")
async def wem_nem_comparison(days: int = Query(7)):
    """Compare WEM vs NEM (NSW1) prices and demand."""
    wem_stats = _query_gold(
        f"SELECT AVG(balancing_price) as avg_price, "
        f"MAX(balancing_price) as max_price, "
        f"AVG(total_balancing_demand_mw) as avg_demand "
        f"FROM {_SCHEMA}.wem_balancing_prices "
        f"WHERE trading_interval >= current_timestamp() - INTERVAL {days} DAYS"
    )

    nem_stats = _query_gold(
        f"SELECT AVG(rrp) as avg_price, "
        f"MAX(rrp) as max_price, "
        f"AVG(total_demand_mw) as avg_demand "
        f"FROM {_SCHEMA}.nem_prices_5min "
        f"WHERE region_id = 'NSW1' "
        f"AND interval_datetime >= current_timestamp() - INTERVAL {days} DAYS"
    )

    wem = wem_stats[0] if wem_stats else {}
    nem = nem_stats[0] if nem_stats else {}

    return {
        "period_days": days,
        "wem": {
            "avg_price": round(float(wem.get("avg_price", 0) or 0), 2),
            "max_price": round(float(wem.get("max_price", 0) or 0), 2),
            "avg_demand_mw": round(float(wem.get("avg_demand", 0) or 0), 1),
        },
        "nem_nsw1": {
            "avg_price": round(float(nem.get("avg_price", 0) or 0), 2),
            "max_price": round(float(nem.get("max_price", 0) or 0), 2),
            "avg_demand_mw": round(float(nem.get("avg_demand", 0) or 0), 1),
        },
        "price_differential": round(
            float(wem.get("avg_price", 0) or 0) - float(nem.get("avg_price", 0) or 0), 2
        ),
    }
