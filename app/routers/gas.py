"""WS4: Gas Market Analytics.

STTM prices, DWGM prices, spark spreads, and gas-electricity correlation.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

from .shared import (
    _CATALOG,
    _query_gold,
    _sql_escape,
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"

# Hub → NEM region mapping for spark spread computation
_HUB_REGION_MAP = {
    "Sydney": "NSW1", "Brisbane": "QLD1", "Adelaide": "SA1",
    "Wallumbilla": "QLD1", "Moomba": "SA1",
}
_GAS_HEAT_RATE = 7.5  # GJ/MWh typical CCGT


# ---------------------------------------------------------------------------
# Core functions (exported for Copilot tools)
# ---------------------------------------------------------------------------

def _get_gas_price_core(hub: Optional[str] = None, days: int = 30) -> Dict[str, Any]:
    """Get gas prices across STTM hubs and DWGM."""
    # Try real gas_hub_prices → STTM format
    hub_filter = f"AND hub = '{_sql_escape(hub)}'" if hub else ""
    real_sttm = _query_gold(
        f"SELECT hub, trade_date, price_aud_gj as ex_ante_price, "
        f"price_aud_gj as ex_post_price "
        f"FROM {_SCHEMA}.gas_hub_prices "
        f"WHERE trade_date >= current_date() - INTERVAL {days} DAYS {hub_filter} "
        f"ORDER BY trade_date DESC LIMIT 200"
    )
    real_summary = _query_gold(
        f"SELECT hub, AVG(price_aud_gj) as avg_price, "
        f"MIN(price_aud_gj) as min_price, MAX(price_aud_gj) as max_price "
        f"FROM {_SCHEMA}.gas_hub_prices "
        f"WHERE trade_date >= current_date() - INTERVAL {days} DAYS {hub_filter} "
        f"GROUP BY hub"
    ) if real_sttm else None

    sttm = real_sttm
    sttm_avg = real_summary

    if not sttm:
        # Seed fallback
        sttm_where = f"AND hub = '{_sql_escape(hub)}'" if hub else ""
        sttm = _query_gold(
            f"SELECT hub, trade_date, ex_ante_price, ex_post_price "
            f"FROM {_SCHEMA}.gas_sttm_prices "
            f"WHERE trade_date >= current_date() - INTERVAL {days} DAYS {sttm_where} "
            f"ORDER BY trade_date DESC LIMIT 200"
        )
        sttm_avg = _query_gold(
            f"SELECT hub, AVG(ex_ante_price) as avg_price, "
            f"MIN(ex_ante_price) as min_price, MAX(ex_ante_price) as max_price "
            f"FROM {_SCHEMA}.gas_sttm_prices "
            f"WHERE trade_date >= current_date() - INTERVAL {days} DAYS {sttm_where} "
            f"GROUP BY hub"
        )

    # DWGM prices (keep seed — separate Victorian mechanism)
    dwgm = _query_gold(
        f"SELECT trade_date, AVG(price_aud_gj) as avg_price, "
        f"MIN(price_aud_gj) as min_price, MAX(price_aud_gj) as max_price "
        f"FROM {_SCHEMA}.gas_dwgm_prices "
        f"WHERE trade_date >= current_date() - INTERVAL {days} DAYS "
        f"GROUP BY trade_date ORDER BY trade_date DESC LIMIT 90"
    )

    return {
        "period_days": days,
        "sttm_prices": [{**r, "trade_date": str(r.get("trade_date", ""))} for r in (sttm or [])],
        "dwgm_prices": [{**r, "trade_date": str(r.get("trade_date", ""))} for r in (dwgm or [])],
        "sttm_summary": sttm_avg or [],
    }


def _get_spark_spread_core(region: Optional[str] = None, days: int = 30) -> Dict[str, Any]:
    """Get spark spread data — electricity price vs gas cost of generation."""
    # Try real: JOIN nem_prices_5min daily avg × gas_hub_prices with hub→region mapping
    region_filter = f"AND p.region_id = '{_sql_escape(region)}'" if region else ""
    real = _query_gold(
        f"SELECT p.region_id as region, DATE(p.interval_datetime) as trade_date, "
        f"AVG(p.rrp) as electricity_price, AVG(g.price_aud_gj) as gas_price, "
        f"{_GAS_HEAT_RATE} as heat_rate, "
        f"AVG(p.rrp) - AVG(g.price_aud_gj) * {_GAS_HEAT_RATE} as spark_spread, "
        f"AVG(p.rrp) - AVG(g.price_aud_gj) * {_GAS_HEAT_RATE} - 5.0 as clean_spark_spread, "
        f"5.0 as carbon_cost "
        f"FROM {_SCHEMA}.nem_prices_5min p "
        f"JOIN {_SCHEMA}.gas_hub_prices g "
        f"ON DATE(p.interval_datetime) = g.trade_date "
        f"AND CASE g.hub "
        f"  WHEN 'Sydney' THEN 'NSW1' "
        f"  WHEN 'Brisbane' THEN 'QLD1' "
        f"  WHEN 'Adelaide' THEN 'SA1' "
        f"  WHEN 'Wallumbilla' THEN 'QLD1' "
        f"  WHEN 'Moomba' THEN 'SA1' "
        f"END = p.region_id "
        f"WHERE p.interval_datetime >= current_timestamp() - INTERVAL {days} DAYS "
        f"{region_filter} "
        f"GROUP BY p.region_id, DATE(p.interval_datetime) "
        f"ORDER BY trade_date DESC LIMIT 500"
    )

    if real:
        # Compute averages by region
        from collections import defaultdict
        by_region: dict = defaultdict(lambda: {"spark": [], "clean_spark": [], "elec": [], "gas": []})
        for r in real:
            rgn = r["region"]
            by_region[rgn]["spark"].append(float(r.get("spark_spread", 0) or 0))
            by_region[rgn]["clean_spark"].append(float(r.get("clean_spark_spread", 0) or 0))
            by_region[rgn]["elec"].append(float(r.get("electricity_price", 0) or 0))
            by_region[rgn]["gas"].append(float(r.get("gas_price", 0) or 0))

        avgs = []
        for rgn, vals in sorted(by_region.items()):
            n = max(len(vals["spark"]), 1)
            avgs.append({
                "region": rgn,
                "avg_spark": round(sum(vals["spark"]) / n, 2),
                "avg_clean_spark": round(sum(vals["clean_spark"]) / n, 2),
                "avg_elec": round(sum(vals["elec"]) / n, 2),
                "avg_gas": round(sum(vals["gas"]) / n, 2),
            })
        avgs.sort(key=lambda x: x["avg_spark"], reverse=True)

        return {
            "period_days": days,
            "spreads": [{**r, "trade_date": str(r.get("trade_date", ""))} for r in real],
            "summary_by_region": avgs,
        }

    # Seed fallback
    where = f"AND region = '{_sql_escape(region)}'" if region else ""
    rows = _query_gold(
        f"SELECT region, trade_date, electricity_price, gas_price, "
        f"heat_rate, spark_spread, clean_spark_spread, carbon_cost "
        f"FROM {_SCHEMA}.gas_spark_spreads "
        f"WHERE trade_date >= current_date() - INTERVAL {days} DAYS {where} "
        f"ORDER BY trade_date DESC LIMIT 500"
    )

    avgs = _query_gold(
        f"SELECT region, AVG(spark_spread) as avg_spark, "
        f"AVG(clean_spark_spread) as avg_clean_spark, "
        f"AVG(electricity_price) as avg_elec, AVG(gas_price) as avg_gas "
        f"FROM {_SCHEMA}.gas_spark_spreads "
        f"WHERE trade_date >= current_date() - INTERVAL {days} DAYS {where} "
        f"GROUP BY region ORDER BY avg_spark DESC"
    )

    return {
        "period_days": days,
        "spreads": [{**r, "trade_date": str(r.get("trade_date", ""))} for r in (rows or [])],
        "summary_by_region": avgs or [],
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/gas/dashboard")
async def gas_dashboard(days: int = Query(30)):
    """Gas market overview: STTM, DWGM, spark spreads."""
    prices = _get_gas_price_core(days=days)
    spreads = _get_spark_spread_core(days=days)

    return {
        "period_days": days,
        "sttm_summary": prices["sttm_summary"],
        "dwgm_latest": prices["dwgm_prices"][:5] if prices["dwgm_prices"] else [],
        "spark_spread_summary": spreads["summary_by_region"],
    }


@router.get("/api/gas/sttm")
async def sttm_prices(hub: Optional[str] = None, days: int = Query(30)):
    """Get STTM hub prices."""
    # Try real gas_hub_prices
    hub_filter = f"AND hub = '{_sql_escape(hub)}'" if hub else ""
    real = _query_gold(
        f"SELECT hub, trade_date, price_aud_gj as ex_ante_price, "
        f"price_aud_gj as ex_post_price "
        f"FROM {_SCHEMA}.gas_hub_prices "
        f"WHERE trade_date >= current_date() - INTERVAL {days} DAYS {hub_filter} "
        f"ORDER BY trade_date DESC, hub LIMIT 300"
    )
    if real:
        return {"prices": [{**r, "trade_date": str(r.get("trade_date", ""))} for r in real]}

    where = f"AND hub = '{_sql_escape(hub)}'" if hub else ""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.gas_sttm_prices "
        f"WHERE trade_date >= current_date() - INTERVAL {days} DAYS {where} "
        f"ORDER BY trade_date DESC, hub LIMIT 300"
    )
    return {"prices": [{**r, "trade_date": str(r.get("trade_date", ""))} for r in (rows or [])]}


@router.get("/api/gas/dwgm")
async def dwgm_prices(days: int = Query(30)):
    """Get DWGM (Victorian gas market) prices."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.gas_dwgm_prices "
        f"WHERE trade_date >= current_date() - INTERVAL {days} DAYS "
        f"ORDER BY trade_date DESC, trading_interval LIMIT 600"
    )
    return {"prices": [{**r, "trade_date": str(r.get("trade_date", ""))} for r in (rows or [])]}


@router.get("/api/gas/spark-spread")
async def spark_spreads(region: Optional[str] = None, days: int = Query(30)):
    """Get spark spread data."""
    return _get_spark_spread_core(region, days)


@router.get("/api/gas/correlation")
async def gas_electricity_correlation(region: str = Query("NSW1"), days: int = Query(30)):
    """Get gas-electricity price correlation."""
    # Try real: JOIN gas_hub_prices × nem_prices_5min
    real = _query_gold(
        f"SELECT DATE(p.interval_datetime) as trade_date, "
        f"AVG(g.price_aud_gj) as gas_price, AVG(p.rrp) as electricity_price, "
        f"AVG(p.rrp) - AVG(g.price_aud_gj) * {_GAS_HEAT_RATE} as spark_spread "
        f"FROM {_SCHEMA}.nem_prices_5min p "
        f"JOIN {_SCHEMA}.gas_hub_prices g "
        f"ON DATE(p.interval_datetime) = g.trade_date "
        f"AND CASE g.hub "
        f"  WHEN 'Sydney' THEN 'NSW1' "
        f"  WHEN 'Brisbane' THEN 'QLD1' "
        f"  WHEN 'Adelaide' THEN 'SA1' "
        f"  WHEN 'Wallumbilla' THEN 'QLD1' "
        f"  WHEN 'Moomba' THEN 'SA1' "
        f"END = p.region_id "
        f"WHERE p.region_id = '{_sql_escape(region)}' "
        f"AND p.interval_datetime >= current_timestamp() - INTERVAL {days} DAYS "
        f"GROUP BY DATE(p.interval_datetime) ORDER BY trade_date"
    )

    rows = real if real else _query_gold(
        f"SELECT gs.trade_date, gs.gas_price, gs.electricity_price, gs.spark_spread "
        f"FROM {_SCHEMA}.gas_spark_spreads gs "
        f"WHERE gs.region = '{_sql_escape(region)}' "
        f"AND gs.trade_date >= current_date() - INTERVAL {days} DAYS "
        f"ORDER BY gs.trade_date"
    )

    if rows and len(rows) > 2:
        gas_prices = [float(r.get("gas_price", 0) or 0) for r in rows]
        elec_prices = [float(r.get("electricity_price", 0) or 0) for r in rows]
        n = len(gas_prices)
        mean_g = sum(gas_prices) / n
        mean_e = sum(elec_prices) / n
        cov = sum((g - mean_g) * (e - mean_e) for g, e in zip(gas_prices, elec_prices)) / n
        std_g = (sum((g - mean_g) ** 2 for g in gas_prices) / n) ** 0.5
        std_e = (sum((e - mean_e) ** 2 for e in elec_prices) / n) ** 0.5
        corr = cov / (std_g * std_e) if std_g > 0 and std_e > 0 else 0
    else:
        corr = 0

    return {
        "region": region,
        "period_days": days,
        "correlation": round(corr, 4),
        "data": [{**r, "trade_date": str(r.get("trade_date", ""))} for r in (rows or [])],
    }
