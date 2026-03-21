"""Risk Management — MtM Valuation, VaR & Greeks, Credit Risk.

E2: Mark-to-Market valuation using forward curves
E3: VaR & portfolio Greeks
E5: Credit risk per counterparty
"""
from __future__ import annotations

import math
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from .curves import _build_forward_curve
from .shared import (
    _CATALOG,
    _NEM_REGIONS,
    _execute_gold,
    _insert_gold,
    _insert_gold_batch,
    _query_gold,
    _query_lakebase_fresh,
    _sql_escape,
    logger,
)

router = APIRouter()

_SCHEMA = f"{_CATALOG}.gold"
_RISK_FREE_RATE = 0.045  # 4.5% AUD risk-free rate
_REC_SPOT_PRICE = 40.0  # $/MWh default REC spot

# Fallback annualised volatilities by region
_DEFAULT_VOL: Dict[str, float] = {
    "NSW1": 0.45, "QLD1": 0.55, "VIC1": 0.40, "SA1": 0.65, "TAS1": 0.30,
}


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def _discount_factor(days: int) -> float:
    """Continuous discounting at risk-free rate."""
    return math.exp(-_RISK_FREE_RATE * days / 365.0)


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via erf."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _get_historical_volatility(region: str, lookback_days: int = 90) -> float:
    """Annualised log-return std dev from nem_prices_5min. Falls back to defaults."""
    try:
        rows = _query_gold(
            f"SELECT rrp FROM {_SCHEMA}.nem_prices_5min "
            f"WHERE region_id = '{_sql_escape(region)}' "
            f"AND interval_datetime >= current_timestamp() - INTERVAL {lookback_days} DAYS "
            f"AND rrp > 0 ORDER BY interval_datetime"
        )
        if rows and len(rows) > 20:
            prices = [r["rrp"] for r in rows if r["rrp"] and r["rrp"] > 0]
            if len(prices) > 20:
                log_returns = [math.log(prices[i] / prices[i - 1])
                               for i in range(1, len(prices)) if prices[i - 1] > 0]
                if log_returns:
                    mean_lr = sum(log_returns) / len(log_returns)
                    var_lr = sum((lr - mean_lr) ** 2 for lr in log_returns) / len(log_returns)
                    # 5-min intervals → annualise: sqrt(intervals_per_year)
                    intervals_per_day = 288  # 24*60/5
                    annual_vol = math.sqrt(var_lr) * math.sqrt(intervals_per_day * 252)
                    return min(max(annual_vol, 0.1), 2.0)  # clamp 10%-200%
    except Exception as exc:
        logger.debug("Vol calc failed for %s: %s", region, exc)
    return _DEFAULT_VOL.get(region, 0.45)


# ---------------------------------------------------------------------------
# Trade fetching
# ---------------------------------------------------------------------------

def _get_active_trades(portfolio_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch non-CANCELLED trades, optionally filtered by portfolio. Lakebase first."""
    if portfolio_id:
        lb_sql = (
            "SELECT t.*, c.name as counterparty_name "
            "FROM gold.trades_synced t "
            "LEFT JOIN gold.counterparties_synced c ON t.counterparty_id = c.counterparty_id "
            "INNER JOIN gold.portfolio_trades_synced pt ON t.trade_id = pt.trade_id "
            f"WHERE pt.portfolio_id = '{_sql_escape(portfolio_id)}' AND t.status != 'CANCELLED'"
        )
        wh_sql = (
            f"SELECT t.*, c.name as counterparty_name "
            f"FROM {_SCHEMA}.trades t "
            f"LEFT JOIN {_SCHEMA}.counterparties c ON t.counterparty_id = c.counterparty_id "
            f"INNER JOIN {_SCHEMA}.portfolio_trades pt ON t.trade_id = pt.trade_id "
            f"WHERE pt.portfolio_id = '{_sql_escape(portfolio_id)}' AND t.status != 'CANCELLED'"
        )
    else:
        lb_sql = (
            "SELECT t.*, c.name as counterparty_name "
            "FROM gold.trades_synced t "
            "LEFT JOIN gold.counterparties_synced c ON t.counterparty_id = c.counterparty_id "
            "WHERE t.status != 'CANCELLED'"
        )
        wh_sql = (
            f"SELECT t.*, c.name as counterparty_name "
            f"FROM {_SCHEMA}.trades t "
            f"LEFT JOIN {_SCHEMA}.counterparties c ON t.counterparty_id = c.counterparty_id "
            f"WHERE t.status != 'CANCELLED'"
        )

    result = _query_lakebase_fresh(lb_sql)
    if result is not None:
        return result
    return _query_gold(wh_sql) or []


# ---------------------------------------------------------------------------
# MtM Valuation Engine (E2)
# ---------------------------------------------------------------------------

def _value_trade(trade: Dict[str, Any], valuation_date: date) -> Dict[str, Any]:
    """Value a single trade. Returns mtm dict ready for DB insert."""
    trade_type = str(trade.get("trade_type", "FORWARD")).upper()
    region = str(trade.get("region", "NSW1"))
    buy_sell = str(trade.get("buy_sell", "BUY")).upper()
    volume_mw = float(trade.get("volume_mw", 0) or 0)
    contract_price = float(trade.get("price", 0) or 0)
    profile = str(trade.get("profile", "FLAT")).upper()

    start_date = trade.get("start_date")
    end_date = trade.get("end_date")
    if isinstance(start_date, str):
        start_date = date.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = date.fromisoformat(end_date)
    if not start_date or not end_date:
        start_date = start_date or valuation_date
        end_date = end_date or valuation_date

    direction = 1.0 if buy_sell == "BUY" else -1.0
    remaining_days = max((end_date - valuation_date).days, 0)
    df = _discount_factor(remaining_days)

    # Get forward prices for the region+profile
    curve_points = _build_forward_curve(
        region=region, profile=profile if profile in ("FLAT", "PEAK", "OFF_PEAK") else "FLAT",
        curve_date=valuation_date.isoformat(), num_quarters=8,
    )
    # Average forward price for the trade period
    relevant_months = set()
    cur = start_date.replace(day=1) if start_date > valuation_date else valuation_date.replace(day=1)
    while cur <= end_date:
        relevant_months.add(f"{cur.year}-{cur.month:02d}")
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)

    forward_prices = [pt["price_mwh"] for pt in curve_points if pt["month"] in relevant_months]
    avg_forward_price = sum(forward_prices) / len(forward_prices) if forward_prices else contract_price

    mtm_value = 0.0
    unrealised_pnl = 0.0

    if trade_type == "SPOT":
        # Spot trades are settled — MtM = 0
        mtm_value = 0.0
        unrealised_pnl = 0.0

    elif trade_type in ("FORWARD", "SWAP", "FUTURE", "PPA"):
        # DCF: (forward_price - contract_price) * volume * direction * discount
        price_diff = avg_forward_price - contract_price
        mtm_value = price_diff * volume_mw * direction * remaining_days * df
        unrealised_pnl = mtm_value

    elif trade_type == "OPTION":
        # Black-76 model
        vol = _get_historical_volatility(region)
        T = remaining_days / 365.0
        if T > 0.001 and avg_forward_price > 0 and contract_price > 0:
            F = avg_forward_price
            K = contract_price
            vol_sqrt_T = vol * math.sqrt(T)
            d1 = (math.log(F / K) + 0.5 * vol * vol * T) / vol_sqrt_T
            d2 = d1 - vol_sqrt_T
            discount = math.exp(-_RISK_FREE_RATE * T)
            if buy_sell == "BUY":
                # Long call
                mtm_value = discount * (F * _norm_cdf(d1) - K * _norm_cdf(d2)) * volume_mw * remaining_days
            else:
                # Long put
                mtm_value = discount * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1)) * volume_mw * remaining_days
            unrealised_pnl = mtm_value
        else:
            mtm_value = 0.0

    elif trade_type == "REC":
        mtm_value = direction * volume_mw * remaining_days * _REC_SPOT_PRICE * df
        unrealised_pnl = mtm_value - direction * volume_mw * remaining_days * contract_price * df

    return {
        "mtm_id": str(uuid.uuid4()),
        "valuation_date": valuation_date.isoformat(),
        "trade_id": trade.get("trade_id", ""),
        "portfolio_id": trade.get("portfolio_id", "") or "",
        "region": region,
        "trade_type": trade_type,
        "direction": buy_sell,
        "mtm_value": round(mtm_value, 2),
        "unrealised_pnl": round(unrealised_pnl, 2),
        "market_price": round(avg_forward_price, 2),
        "contract_price": round(contract_price, 2),
        "volume_mw": volume_mw,
        "remaining_days": remaining_days,
        "discount_factor": round(df, 6),
        "model_version": "1.0",
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


def _run_mtm_core(
    portfolio_id: Optional[str] = None,
    valuation_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Run MtM valuation for all active trades. Returns summary dict."""
    if valuation_date is None:
        valuation_date = date.today()

    trades = _get_active_trades(portfolio_id)
    if not trades:
        return {"status": "no_trades", "trades_valued": 0, "total_mtm": 0.0}

    mtm_rows: List[Dict[str, Any]] = []
    for trade in trades:
        mtm = _value_trade(trade, valuation_date)
        mtm_rows.append(mtm)

    # Batch insert to portfolio_mtm
    inserted = _insert_gold_batch(f"{_SCHEMA}.portfolio_mtm", mtm_rows)

    # Compute P&L attribution (simplified: compare vs previous day's MtM if available)
    prev_date = valuation_date - timedelta(days=1)
    prev_mtm = _query_gold(
        f"SELECT portfolio_id, region, SUM(mtm_value) as prev_total "
        f"FROM {_SCHEMA}.portfolio_mtm "
        f"WHERE valuation_date = '{prev_date.isoformat()}' "
        f"GROUP BY portfolio_id, region"
    )
    prev_map: Dict[str, float] = {}
    if prev_mtm:
        for r in prev_mtm:
            key = f"{r.get('portfolio_id', '')}:{r.get('region', '')}"
            prev_map[key] = float(r.get("prev_total", 0) or 0)

    # Aggregate current by portfolio+region
    current_agg: Dict[str, Dict[str, float]] = {}
    for m in mtm_rows:
        key = f"{m['portfolio_id']}:{m['region']}"
        if key not in current_agg:
            current_agg[key] = {"mtm": 0.0, "pid": m["portfolio_id"], "region": m["region"]}
        current_agg[key]["mtm"] += m["mtm_value"]

    # Build attribution rows
    attr_rows: List[Dict[str, Any]] = []
    for key, agg in current_agg.items():
        prev_total = prev_map.get(key, 0.0)
        total_pnl = agg["mtm"] - prev_total
        # Simplified decomposition
        price_effect = total_pnl * 0.6  # approximate
        time_decay = total_pnl * -0.1
        volume_effect = total_pnl * 0.2
        new_trades_effect = total_pnl * 0.1
        residual = total_pnl - (price_effect + time_decay + volume_effect + new_trades_effect)
        price_effect += residual  # absorb rounding

        attr_rows.append({
            "attribution_id": str(uuid.uuid4()),
            "valuation_date": valuation_date.isoformat(),
            "portfolio_id": agg["pid"],
            "region": agg["region"],
            "price_effect": round(price_effect, 2),
            "volume_effect": round(volume_effect, 2),
            "new_trades_effect": round(new_trades_effect, 2),
            "time_decay": round(time_decay, 2),
            "total_pnl": round(total_pnl, 2),
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        })

    if attr_rows:
        _insert_gold_batch(f"{_SCHEMA}.pnl_attribution", attr_rows)

    # Summary
    total_mtm = sum(m["mtm_value"] for m in mtm_rows)
    total_pnl = sum(a["total_pnl"] for a in attr_rows)
    by_portfolio: Dict[str, float] = {}
    for m in mtm_rows:
        pid = m["portfolio_id"] or "unassigned"
        by_portfolio[pid] = by_portfolio.get(pid, 0.0) + m["mtm_value"]

    return {
        "status": "ok",
        "valuation_date": valuation_date.isoformat(),
        "trades_valued": len(mtm_rows),
        "rows_inserted": inserted,
        "total_mtm": round(total_mtm, 2),
        "daily_pnl": round(total_pnl, 2),
        "by_portfolio": {k: round(v, 2) for k, v in by_portfolio.items()},
    }


# ---------------------------------------------------------------------------
# VaR & Greeks (E3)
# ---------------------------------------------------------------------------

def _calculate_var_greeks(
    portfolio_id: str,
    valuation_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Calculate VaR and Greeks for a portfolio. Persists to risk_metrics."""
    if valuation_date is None:
        valuation_date = date.today()

    # Get latest MtM for this portfolio
    mtm_rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.portfolio_mtm "
        f"WHERE portfolio_id = '{_sql_escape(portfolio_id)}' "
        f"AND valuation_date = '{valuation_date.isoformat()}'"
    )
    if not mtm_rows:
        # Try running MtM first
        _run_mtm_core(portfolio_id=portfolio_id, valuation_date=valuation_date)
        mtm_rows = _query_gold(
            f"SELECT * FROM {_SCHEMA}.portfolio_mtm "
            f"WHERE portfolio_id = '{_sql_escape(portfolio_id)}' "
            f"AND valuation_date = '{valuation_date.isoformat()}'"
        )
    if not mtm_rows:
        return {"status": "no_data", "portfolio_id": portfolio_id}

    # Group by region
    by_region: Dict[str, List[Dict[str, Any]]] = {}
    for m in mtm_rows:
        r = m.get("region", "NSW1")
        if r not in by_region:
            by_region[r] = []
        by_region[r].append(m)

    z_95 = 1.645
    z_99 = 2.326
    metric_rows: List[Dict[str, Any]] = []

    for region, trades in by_region.items():
        vol = _get_historical_volatility(region)
        daily_vol = vol / math.sqrt(252)

        # Portfolio value for this region
        portfolio_value = sum(abs(float(t.get("mtm_value", 0) or 0)) for t in trades)
        if portfolio_value < 0.01:
            portfolio_value = sum(
                float(t.get("volume_mw", 0) or 0) * float(t.get("market_price", 0) or 0) * max(int(t.get("remaining_days", 0) or 0), 1)
                for t in trades
            )

        # VaR
        var_95_1d = portfolio_value * z_95 * daily_vol
        var_99_1d = portfolio_value * z_99 * daily_vol
        var_95_10d = var_95_1d * math.sqrt(10)
        var_99_10d = var_99_1d * math.sqrt(10)

        # Greeks
        delta_mw = 0.0
        gamma = 0.0
        vega = 0.0
        theta = 0.0

        for t in trades:
            tt = str(t.get("trade_type", "FORWARD")).upper()
            direction = 1.0 if str(t.get("direction", "BUY")).upper() == "BUY" else -1.0
            vol_mw = float(t.get("volume_mw", 0) or 0)
            rem_days = max(int(t.get("remaining_days", 0) or 0), 1)
            mkt_price = float(t.get("market_price", 0) or 0)
            K = float(t.get("contract_price", 0) or 0)
            T = rem_days / 365.0

            if tt in ("FORWARD", "SWAP", "FUTURE", "PPA"):
                # Linear instruments
                delta_mw += direction * vol_mw * rem_days / 365.0
                theta += -abs(float(t.get("mtm_value", 0) or 0)) * _RISK_FREE_RATE / 365.0
                # gamma = 0 for linear

            elif tt == "OPTION" and T > 0.001 and mkt_price > 0 and K > 0:
                F = mkt_price
                vol_sqrt_T = vol * math.sqrt(T)
                d1 = (math.log(F / K) + 0.5 * vol * vol * T) / vol_sqrt_T
                npd1 = _norm_pdf(d1)
                discount = math.exp(-_RISK_FREE_RATE * T)

                # Delta
                delta_mw += direction * _norm_cdf(d1) * vol_mw * rem_days / 365.0
                # Gamma
                gamma += npd1 / (F * vol_sqrt_T) * vol_mw * rem_days
                # Vega (per 1% vol move)
                vega += F * math.sqrt(T) * npd1 * discount * vol_mw * rem_days / 100.0
                # Theta
                theta += -(F * npd1 * vol / (2.0 * math.sqrt(T)) * discount
                           + _RISK_FREE_RATE * K * discount * _norm_cdf(d1 - vol_sqrt_T)) * vol_mw / 365.0

        metric_rows.append({
            "metric_id": str(uuid.uuid4()),
            "valuation_date": valuation_date.isoformat(),
            "portfolio_id": portfolio_id,
            "region": region,
            "var_95_1d": round(var_95_1d, 2),
            "var_99_1d": round(var_99_1d, 2),
            "var_95_10d": round(var_95_10d, 2),
            "var_99_10d": round(var_99_10d, 2),
            "delta_mw": round(delta_mw, 2),
            "gamma": round(gamma, 4),
            "vega": round(vega, 2),
            "theta": round(theta, 2),
            "volatility_annual": round(vol, 4),
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        })

    if metric_rows:
        _insert_gold_batch(f"{_SCHEMA}.risk_metrics", metric_rows)

    return {
        "status": "ok",
        "portfolio_id": portfolio_id,
        "valuation_date": valuation_date.isoformat(),
        "metrics": metric_rows,
    }


# ---------------------------------------------------------------------------
# Credit Risk (E5)
# ---------------------------------------------------------------------------

def _calculate_credit_exposure(valuation_date: Optional[date] = None) -> Dict[str, Any]:
    """Calculate credit exposures for all counterparties."""
    if valuation_date is None:
        valuation_date = date.today()

    # Get counterparties
    cp_rows = _query_lakebase_fresh(
        "SELECT * FROM gold.counterparties_synced WHERE status = 'ACTIVE'"
    )
    if cp_rows is None:
        cp_rows = _query_gold(
            f"SELECT * FROM {_SCHEMA}.counterparties WHERE status = 'ACTIVE'"
        )
    if not cp_rows:
        return {"status": "no_counterparties", "exposures": []}

    # Get latest MtM grouped by counterparty
    mtm_sql = (
        f"SELECT t.counterparty_id, m.mtm_value, m.remaining_days "
        f"FROM {_SCHEMA}.portfolio_mtm m "
        f"INNER JOIN {_SCHEMA}.trades t ON m.trade_id = t.trade_id "
        f"WHERE m.valuation_date = '{valuation_date.isoformat()}' "
        f"AND t.counterparty_id IS NOT NULL AND t.counterparty_id != ''"
    )
    mtm_rows = _query_gold(mtm_sql)
    if not mtm_rows:
        # Run MtM first
        _run_mtm_core(valuation_date=valuation_date)
        mtm_rows = _query_gold(mtm_sql)

    # Group by counterparty
    cp_mtm: Dict[str, List[Dict[str, Any]]] = {}
    for r in (mtm_rows or []):
        cpid = r.get("counterparty_id", "")
        if cpid:
            if cpid not in cp_mtm:
                cp_mtm[cpid] = []
            cp_mtm[cpid].append(r)

    exposure_rows: List[Dict[str, Any]] = []
    for cp in cp_rows:
        cpid = cp.get("counterparty_id", "")
        credit_limit = float(cp.get("credit_limit_aud", 0) or 0)
        if credit_limit <= 0:
            credit_limit = 10_000_000.0  # default $10M

        trades = cp_mtm.get(cpid, [])

        # Current exposure = sum of positive MtM
        current_exposure = sum(
            max(float(t.get("mtm_value", 0) or 0), 0) for t in trades
        )

        # Exposure aging buckets
        bucket_0_30 = sum(
            max(float(t.get("mtm_value", 0) or 0), 0)
            for t in trades if int(t.get("remaining_days", 0) or 0) <= 30
        )
        bucket_30_90 = sum(
            max(float(t.get("mtm_value", 0) or 0), 0)
            for t in trades if 30 < int(t.get("remaining_days", 0) or 0) <= 90
        )
        bucket_90plus = sum(
            max(float(t.get("mtm_value", 0) or 0), 0)
            for t in trades if int(t.get("remaining_days", 0) or 0) > 90
        )

        # PFE = current * (1 + 0.15 * sqrt(avg_years))
        avg_days = (
            sum(int(t.get("remaining_days", 0) or 0) for t in trades) / len(trades)
            if trades else 180
        )
        pfe = current_exposure * (1.0 + 0.15 * math.sqrt(avg_days / 365.0))

        utilization = current_exposure / credit_limit if credit_limit > 0 else 0.0

        if utilization >= 0.95:
            alert = "CRITICAL"
        elif utilization >= 0.80:
            alert = "WARNING"
        else:
            alert = "NORMAL"

        exposure_rows.append({
            "exposure_id": str(uuid.uuid4()),
            "valuation_date": valuation_date.isoformat(),
            "counterparty_id": cpid,
            "counterparty_name": cp.get("name", ""),
            "current_exposure": round(current_exposure, 2),
            "potential_future_exposure": round(pfe, 2),
            "credit_limit": round(credit_limit, 2),
            "credit_utilization": round(utilization, 4),
            "credit_rating": cp.get("credit_rating", ""),
            "exposure_current_bucket": round(bucket_0_30, 2),
            "exposure_30_90_bucket": round(bucket_30_90, 2),
            "exposure_90plus_bucket": round(bucket_90plus, 2),
            "alert_level": alert,
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        })

    if exposure_rows:
        _insert_gold_batch(f"{_SCHEMA}.credit_exposure", exposure_rows)

    return {
        "status": "ok",
        "valuation_date": valuation_date.isoformat(),
        "counterparties": len(exposure_rows),
        "exposures": exposure_rows,
        "alerts": [e for e in exposure_rows if e["alert_level"] != "NORMAL"],
    }


# =========================================================================
# MtM Endpoints (E2)
# =========================================================================

@router.post("/api/risk/mtm/run")
async def run_mtm(
    portfolio_id: Optional[str] = Query(None),
    valuation_date: Optional[str] = Query(None),
):
    """Trigger MtM valuation for all trades or a specific portfolio."""
    vd = date.fromisoformat(valuation_date) if valuation_date else date.today()
    result = _run_mtm_core(portfolio_id=portfolio_id, valuation_date=vd)
    return result


@router.get("/api/risk/mtm/latest")
async def mtm_latest(portfolio_id: Optional[str] = Query(None)):
    """Latest MtM results, optionally filtered by portfolio."""
    where = ""
    if portfolio_id:
        where = f"AND portfolio_id = '{_sql_escape(portfolio_id)}'"

    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.portfolio_mtm "
        f"WHERE valuation_date = (SELECT MAX(valuation_date) FROM {_SCHEMA}.portfolio_mtm) "
        f"{where} ORDER BY ABS(mtm_value) DESC LIMIT 200"
    )
    if not rows:
        return {"results": [], "total_mtm": 0.0}

    for r in rows:
        for k in ("valuation_date", "created_at"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])

    total = sum(float(r.get("mtm_value", 0) or 0) for r in rows)
    return {
        "results": rows,
        "total_mtm": round(total, 2),
        "trades_valued": len(rows),
        "valuation_date": rows[0].get("valuation_date", ""),
    }


@router.get("/api/risk/mtm/history")
async def mtm_history(
    portfolio_id: str = Query(...),
    days: int = Query(30, le=365),
):
    """MtM over time for a portfolio."""
    rows = _query_gold(
        f"SELECT valuation_date, SUM(mtm_value) as total_mtm, COUNT(*) as trade_count "
        f"FROM {_SCHEMA}.portfolio_mtm "
        f"WHERE portfolio_id = '{_sql_escape(portfolio_id)}' "
        f"AND valuation_date >= current_date() - INTERVAL {days} DAYS "
        f"GROUP BY valuation_date ORDER BY valuation_date"
    )
    if not rows:
        return {"history": [], "portfolio_id": portfolio_id}

    for r in rows:
        if "valuation_date" in r and r["valuation_date"] is not None:
            r["valuation_date"] = str(r["valuation_date"])
    return {"history": rows, "portfolio_id": portfolio_id}


@router.get("/api/risk/pnl/attribution")
async def pnl_attribution(
    portfolio_id: Optional[str] = Query(None),
    valuation_date: Optional[str] = Query(None),
):
    """P&L breakdown by effect type."""
    where_parts = ["1=1"]
    if portfolio_id:
        where_parts.append(f"portfolio_id = '{_sql_escape(portfolio_id)}'")
    if valuation_date:
        where_parts.append(f"valuation_date = '{_sql_escape(valuation_date)}'")
    else:
        where_parts.append(
            f"valuation_date = (SELECT MAX(valuation_date) FROM {_SCHEMA}.pnl_attribution)"
        )

    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.pnl_attribution "
        f"WHERE {' AND '.join(where_parts)} ORDER BY ABS(total_pnl) DESC"
    )
    if not rows:
        return {"attribution": []}

    for r in rows:
        for k in ("valuation_date", "created_at"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])
    return {"attribution": rows}


@router.get("/api/risk/pnl/summary")
async def pnl_summary():
    """Aggregate P&L across all portfolios."""
    rows = _query_gold(
        f"SELECT portfolio_id, "
        f"SUM(price_effect) as total_price_effect, "
        f"SUM(volume_effect) as total_volume_effect, "
        f"SUM(new_trades_effect) as total_new_trades, "
        f"SUM(time_decay) as total_time_decay, "
        f"SUM(total_pnl) as total_pnl "
        f"FROM {_SCHEMA}.pnl_attribution "
        f"WHERE valuation_date = (SELECT MAX(valuation_date) FROM {_SCHEMA}.pnl_attribution) "
        f"GROUP BY portfolio_id"
    )
    if not rows:
        return {"portfolios": [], "grand_total_pnl": 0.0}

    grand_total = sum(float(r.get("total_pnl", 0) or 0) for r in rows)
    return {"portfolios": rows, "grand_total_pnl": round(grand_total, 2)}


# =========================================================================
# VaR & Greeks Endpoints (E3)
# =========================================================================

@router.post("/api/risk/var/calculate")
async def calculate_var(
    portfolio_id: str = Query(...),
    valuation_date: Optional[str] = Query(None),
):
    """Calculate VaR and Greeks for a portfolio."""
    vd = date.fromisoformat(valuation_date) if valuation_date else date.today()
    result = _calculate_var_greeks(portfolio_id=portfolio_id, valuation_date=vd)
    return result


@router.get("/api/risk/var/latest")
async def var_latest(portfolio_id: Optional[str] = Query(None)):
    """Latest VaR results."""
    where = ""
    if portfolio_id:
        where = f"AND portfolio_id = '{_sql_escape(portfolio_id)}'"

    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.risk_metrics "
        f"WHERE valuation_date = (SELECT MAX(valuation_date) FROM {_SCHEMA}.risk_metrics) "
        f"{where} ORDER BY var_95_1d DESC"
    )
    if not rows:
        return {"metrics": []}

    for r in rows:
        for k in ("valuation_date", "created_at"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])
    return {"metrics": rows}


@router.get("/api/risk/var/history")
async def var_history(
    portfolio_id: str = Query(...),
    days: int = Query(30, le=365),
):
    """VaR over time for a portfolio."""
    rows = _query_gold(
        f"SELECT valuation_date, SUM(var_95_1d) as var_95_1d, SUM(var_99_1d) as var_99_1d, "
        f"SUM(var_95_10d) as var_95_10d, SUM(var_99_10d) as var_99_10d "
        f"FROM {_SCHEMA}.risk_metrics "
        f"WHERE portfolio_id = '{_sql_escape(portfolio_id)}' "
        f"AND valuation_date >= current_date() - INTERVAL {days} DAYS "
        f"GROUP BY valuation_date ORDER BY valuation_date"
    )
    if not rows:
        return {"history": [], "portfolio_id": portfolio_id}

    for r in rows:
        if "valuation_date" in r and r["valuation_date"] is not None:
            r["valuation_date"] = str(r["valuation_date"])
    return {"history": rows, "portfolio_id": portfolio_id}


@router.get("/api/risk/greeks")
async def greeks(
    portfolio_id: str = Query(...),
):
    """Greeks by region for a portfolio."""
    rows = _query_gold(
        f"SELECT region, delta_mw, gamma, vega, theta, volatility_annual "
        f"FROM {_SCHEMA}.risk_metrics "
        f"WHERE portfolio_id = '{_sql_escape(portfolio_id)}' "
        f"AND valuation_date = (SELECT MAX(valuation_date) FROM {_SCHEMA}.risk_metrics "
        f"WHERE portfolio_id = '{_sql_escape(portfolio_id)}')"
    )
    if not rows:
        return {"greeks": [], "portfolio_id": portfolio_id}
    return {"greeks": rows, "portfolio_id": portfolio_id}


# =========================================================================
# Credit Risk Endpoints (E5)
# =========================================================================

@router.post("/api/risk/credit/calculate")
async def calculate_credit(
    valuation_date: Optional[str] = Query(None),
):
    """Calculate credit exposures for all counterparties."""
    vd = date.fromisoformat(valuation_date) if valuation_date else date.today()
    return _calculate_credit_exposure(valuation_date=vd)


@router.get("/api/risk/credit/summary")
async def credit_summary():
    """Credit exposure summary — latest valuations."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.credit_exposure "
        f"WHERE valuation_date = (SELECT MAX(valuation_date) FROM {_SCHEMA}.credit_exposure) "
        f"ORDER BY current_exposure DESC"
    )
    if not rows:
        return {"exposures": []}

    for r in rows:
        for k in ("valuation_date", "created_at"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])
    return {"exposures": rows}


@router.get("/api/risk/credit/counterparty/{counterparty_id}")
async def credit_counterparty(counterparty_id: str):
    """Detailed credit exposure for one counterparty."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.credit_exposure "
        f"WHERE counterparty_id = '{_sql_escape(counterparty_id)}' "
        f"ORDER BY valuation_date DESC LIMIT 30"
    )
    if not rows:
        return JSONResponse(status_code=404, content={"error": "No exposure data"})

    for r in rows:
        for k in ("valuation_date", "created_at"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])
    return {"counterparty_id": counterparty_id, "history": rows}


@router.get("/api/risk/credit/alerts")
async def credit_alerts():
    """WARNING and CRITICAL credit alerts only."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.credit_exposure "
        f"WHERE valuation_date = (SELECT MAX(valuation_date) FROM {_SCHEMA}.credit_exposure) "
        f"AND alert_level IN ('WARNING', 'CRITICAL') "
        f"ORDER BY credit_utilization DESC"
    )
    if not rows:
        return {"alerts": [], "message": "No active alerts"}

    for r in rows:
        for k in ("valuation_date", "created_at"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])
    return {"alerts": rows}


# =========================================================================
# PPA Valuation Engine (E6)
# =========================================================================

# Typical capacity factors by region and technology
_CAPACITY_FACTORS: Dict[str, Dict[str, float]] = {
    "solar_utility": {"NSW1": 0.24, "QLD1": 0.27, "VIC1": 0.20, "SA1": 0.26, "TAS1": 0.17},
    "wind":          {"NSW1": 0.32, "QLD1": 0.30, "VIC1": 0.35, "SA1": 0.38, "TAS1": 0.40},
}

# Capture price discount vs flat price (solar gets less during middle-of-day oversupply)
_CAPTURE_DISCOUNT: Dict[str, float] = {"solar_utility": 0.82, "wind": 0.92}


def _value_ppa_core(
    strike_price: float,
    term_years: int,
    technology: str,
    region: str,
    volume_mw: float,
    escalation: float = 0.025,
    n_simulations: int = 1000,
) -> Dict[str, Any]:
    """Monte Carlo PPA valuation with generation and price uncertainty."""
    tech = technology.lower().replace(" ", "_")
    if tech == "solar":
        tech = "solar_utility"

    cap_factor = _CAPACITY_FACTORS.get(tech, {}).get(region, 0.25)
    capture_disc = _CAPTURE_DISCOUNT.get(tech, 0.90)
    hours_per_year = 8760

    # Get forward curve for price baseline
    curve_points = _build_forward_curve(
        region=region, profile="FLAT",
        num_quarters=term_years * 4,
    )
    # Average annual forward prices
    annual_forwards: List[float] = []
    for yr in range(term_years):
        year_months = set()
        base_date = date.today()
        for m in range(yr * 12, (yr + 1) * 12):
            d = base_date + timedelta(days=m * 30)
            year_months.add(f"{d.year}-{d.month:02d}")
        yr_prices = [pt["price_mwh"] for pt in curve_points if pt["month"] in year_months]
        if yr_prices:
            annual_forwards.append(sum(yr_prices) / len(yr_prices))
        elif annual_forwards:
            annual_forwards.append(annual_forwards[-1] * (1 + escalation))
        else:
            annual_forwards.append(strike_price * 1.05)

    vol = _get_historical_volatility(region)
    wacc = _RISK_FREE_RATE + 0.03  # risk-adjusted discount

    rng = random.Random(42)
    npv_samples: List[float] = []
    annual_cashflows_sum: List[Dict[str, float]] = [
        {"cashflow": 0.0, "generation_mwh": 0.0, "revenue": 0.0, "cost": 0.0}
        for _ in range(term_years)
    ]

    for _ in range(n_simulations):
        sim_npv = 0.0
        for yr in range(term_years):
            # Price uncertainty
            price_shock = math.exp(rng.gauss(0, vol * 0.5))
            merchant_price = annual_forwards[yr] * price_shock * capture_disc

            # Generation uncertainty (15% std dev)
            gen_shock = 1.0 + rng.gauss(0, 0.15)
            gen_mwh = volume_mw * cap_factor * hours_per_year * max(gen_shock, 0.3)

            # Escalated strike
            esc_strike = strike_price * ((1 + escalation) ** yr)

            # Annual cashflow = (strike - merchant) * generation for seller PPA
            annual_cf = (esc_strike - merchant_price) * gen_mwh
            df = 1.0 / ((1 + wacc) ** (yr + 1))
            sim_npv += annual_cf * df

            annual_cashflows_sum[yr]["cashflow"] += annual_cf
            annual_cashflows_sum[yr]["generation_mwh"] += gen_mwh
            annual_cashflows_sum[yr]["revenue"] += esc_strike * gen_mwh
            annual_cashflows_sum[yr]["cost"] += merchant_price * gen_mwh

        npv_samples.append(sim_npv)

    npv_samples.sort()
    n = len(npv_samples)
    expected_npv = sum(npv_samples) / n
    p10_npv = npv_samples[int(n * 0.10)]
    p50_npv = npv_samples[int(n * 0.50)]
    p90_npv = npv_samples[int(n * 0.90)]

    # Breakeven strike: find strike where expected NPV = 0
    # Approximate: adjust strike proportionally
    avg_gen_mwh = volume_mw * cap_factor * hours_per_year
    avg_merchant = sum(af * capture_disc for af in annual_forwards) / len(annual_forwards) if annual_forwards else strike_price
    breakeven_strike = avg_merchant  # simplified

    annual_cashflows = []
    for yr in range(term_years):
        annual_cashflows.append({
            "year": yr + 1,
            "cashflow": round(annual_cashflows_sum[yr]["cashflow"] / n, 2),
            "generation_mwh": round(annual_cashflows_sum[yr]["generation_mwh"] / n, 0),
            "revenue": round(annual_cashflows_sum[yr]["revenue"] / n, 2),
            "cost": round(annual_cashflows_sum[yr]["cost"] / n, 2),
        })

    return {
        "region": region,
        "technology": tech,
        "volume_mw": volume_mw,
        "strike_price": strike_price,
        "term_years": term_years,
        "escalation": escalation,
        "capacity_factor": cap_factor,
        "capture_price_discount": capture_disc,
        "expected_npv": round(expected_npv, 2),
        "p10_npv": round(p10_npv, 2),
        "p50_npv": round(p50_npv, 2),
        "p90_npv": round(p90_npv, 2),
        "breakeven_strike": round(breakeven_strike, 2),
        "simulations": n_simulations,
        "annual_cashflows": annual_cashflows,
    }


@router.post("/api/risk/ppa/value")
async def value_ppa(
    strike_price: float = Query(...),
    term_years: int = Query(..., ge=1, le=30),
    technology: str = Query(..., description="solar_utility or wind"),
    region: str = Query("NSW1"),
    volume_mw: float = Query(100),
    escalation: float = Query(0.025),
    n_simulations: int = Query(1000, le=5000),
):
    """Monte Carlo PPA valuation. Returns NPV distribution and annual cashflows."""
    if region not in _NEM_REGIONS:
        return JSONResponse(status_code=400, content={"error": f"Invalid region: {region}"})
    result = _value_ppa_core(
        strike_price=strike_price,
        term_years=term_years,
        technology=technology,
        region=region,
        volume_mw=volume_mw,
        escalation=escalation,
        n_simulations=n_simulations,
    )
    return result


# =========================================================================
# Pre-Trade Credit Check (E12)
# =========================================================================

def _credit_check_core(
    counterparty_id: str,
    notional_aud: float,
    tenor_days: int = 365,
) -> Dict[str, Any]:
    """Check credit availability for a proposed trade."""
    # Get counterparty
    cp_rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.counterparties "
        f"WHERE counterparty_id = '{_sql_escape(counterparty_id)}'"
    )
    if not cp_rows:
        return {"status": "error", "message": f"Counterparty {counterparty_id} not found"}

    cp = cp_rows[0]
    credit_limit = float(cp.get("credit_limit_aud", 0) or 0)
    if credit_limit <= 0:
        credit_limit = 10_000_000.0

    # Get current exposure
    exp_rows = _query_gold(
        f"SELECT current_exposure FROM {_SCHEMA}.credit_exposure "
        f"WHERE counterparty_id = '{_sql_escape(counterparty_id)}' "
        f"AND valuation_date = (SELECT MAX(valuation_date) FROM {_SCHEMA}.credit_exposure "
        f"WHERE counterparty_id = '{_sql_escape(counterparty_id)}')"
    )
    current_exposure = float(exp_rows[0]["current_exposure"]) if exp_rows else 0.0

    # Proposed increment scaled by tenor and volatility
    vol_multiplier = 1.0 + 0.1 * math.sqrt(tenor_days / 365.0)
    proposed_increment = notional_aud * (tenor_days / 365.0) * vol_multiplier

    new_total = current_exposure + proposed_increment
    utilization = new_total / credit_limit if credit_limit > 0 else 1.0

    if utilization > 1.0:
        status = "block"
        message = f"Exceeds credit limit. Utilization would be {utilization:.0%}."
    elif utilization >= 0.90:
        status = "warn"
        message = f"Near credit limit. Utilization would be {utilization:.0%}. Override available."
    elif utilization >= 0.80:
        status = "warn"
        message = f"Approaching credit limit. Utilization would be {utilization:.0%}."
    else:
        status = "pass"
        message = f"Within credit limits. Utilization would be {utilization:.0%}."

    return {
        "status": status,
        "counterparty_id": counterparty_id,
        "counterparty_name": cp.get("name", ""),
        "utilization_pct": round(utilization * 100, 1),
        "current_exposure": round(current_exposure, 2),
        "proposed_increment": round(proposed_increment, 2),
        "new_total_exposure": round(new_total, 2),
        "credit_limit": round(credit_limit, 2),
        "message": message,
    }


@router.get("/api/credit/check")
async def credit_check(
    counterparty_id: str = Query(...),
    notional_aud: float = Query(...),
    tenor_days: int = Query(365, ge=1),
):
    """Pre-trade credit check. Returns pass/warn/block status."""
    return _credit_check_core(counterparty_id, notional_aud, tenor_days)


# ---------------------------------------------------------------------------
# E4 — Stress Testing
# ---------------------------------------------------------------------------

_STRESS_SCENARIOS: Dict[str, Dict[str, Any]] = {
    "sa_heatwave": {
        "name": "SA Heatwave +300%",
        "description": "Extreme heatwave in South Australia drives prices to 3x normal across SA1 and VIC1",
        "price_shocks": {"SA1": 3.0, "VIC1": 1.8, "NSW1": 1.3, "QLD1": 1.1, "TAS1": 1.0},
        "volume_shocks": {"SA1": 1.15, "VIC1": 1.10},
        "generation_shocks": {"wind": 0.4, "solar_utility": 1.3},
    },
    "wind_drought": {
        "name": "Wind Drought -80%",
        "description": "Extended low-wind period across southern states, wind generation drops 80%",
        "price_shocks": {"SA1": 2.5, "VIC1": 2.0, "NSW1": 1.5, "QLD1": 1.2, "TAS1": 1.8},
        "volume_shocks": {},
        "generation_shocks": {"wind": 0.2, "solar_utility": 1.0},
    },
    "coal_trip_2gw": {
        "name": "Coal Plant Trip 2GW",
        "description": "Major coal plant trip in NSW (e.g. Eraring 2GW offline), supply shortage",
        "price_shocks": {"NSW1": 2.8, "QLD1": 1.6, "VIC1": 1.4, "SA1": 1.2, "TAS1": 1.1},
        "volume_shocks": {"NSW1": 0.85},
        "generation_shocks": {"coal_black": 0.5, "coal_brown": 0.8},
    },
    "interconnector_failure": {
        "name": "QNI Interconnector Failure",
        "description": "QLD-NSW interconnector failure isolates QLD, price divergence",
        "price_shocks": {"QLD1": 2.2, "NSW1": 0.7, "VIC1": 0.8, "SA1": 0.9, "TAS1": 1.0},
        "volume_shocks": {},
        "generation_shocks": {},
    },
}


def _run_stress_test_core(
    portfolio_id: str, scenario_id: str, custom_shocks: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Run a stress test against a portfolio."""

    if scenario_id == "custom" and custom_shocks:
        scenario = {
            "name": "Custom Scenario",
            "description": "User-defined stress scenario",
            "price_shocks": custom_shocks.get("price_shocks", {}),
            "volume_shocks": custom_shocks.get("volume_shocks", {}),
            "generation_shocks": custom_shocks.get("generation_shocks", {}),
        }
    elif scenario_id in _STRESS_SCENARIOS:
        scenario = _STRESS_SCENARIOS[scenario_id]
    else:
        return {"error": f"Unknown scenario: {scenario_id}. Available: {list(_STRESS_SCENARIOS.keys())}"}

    # Fetch portfolio trades
    trades = _query_gold(
        f"SELECT t.trade_id, t.region, t.trade_type, t.buy_sell, t.volume_mw, "
        f"t.price, t.start_date, t.end_date, t.profile "
        f"FROM {_CATALOG}.gold.trades t "
        f"LEFT JOIN {_CATALOG}.gold.portfolio_trades pt ON t.trade_id = pt.trade_id "
        f"WHERE (pt.portfolio_id = '{_sql_escape(portfolio_id)}' OR '{_sql_escape(portfolio_id)}' = 'all') "
        f"AND t.status IN ('CONFIRMED','SETTLED') "
    )
    if not trades:
        trades = _query_gold(
            f"SELECT trade_id, region, trade_type, buy_sell, volume_mw, price, "
            f"start_date, end_date, profile "
            f"FROM {_CATALOG}.gold.trades WHERE status IN ('CONFIRMED','SETTLED')"
        )

    # Get current forward curves for base case
    base_curves: Dict[str, float] = {}
    for region in _NEM_REGIONS:
        try:
            fc = _build_forward_curve(region, "FLAT", num_quarters=8)
            if fc:
                base_curves[region] = sum(p["price_mwh"] for p in fc) / len(fc)
        except Exception:
            base_curves[region] = 80.0  # fallback

    # Calculate base MtM and stressed MtM per trade
    trade_impacts = []
    total_base_mtm = 0.0
    total_stressed_mtm = 0.0

    for t in (trades or []):
        region = t.get("region", "NSW1")
        volume = float(t.get("volume_mw", 0))
        price = float(t.get("price", 0))
        direction = 1.0 if t.get("buy_sell") == "BUY" else -1.0

        start_d = t.get("start_date")
        end_d = t.get("end_date")
        try:
            if isinstance(start_d, str):
                start_d = datetime.fromisoformat(start_d.replace("Z", "+00:00"))
            if isinstance(end_d, str):
                end_d = datetime.fromisoformat(end_d.replace("Z", "+00:00"))
            days = max((end_d - start_d).days, 1)
        except Exception:
            days = 90

        market_price = base_curves.get(region, 80.0)
        base_mtm = direction * volume * (market_price - price) * days * 24 / 365

        # Apply stress shocks
        price_shock = scenario["price_shocks"].get(region, 1.0)
        stressed_price = market_price * price_shock
        stressed_mtm = direction * volume * (stressed_price - price) * days * 24 / 365

        impact = stressed_mtm - base_mtm
        total_base_mtm += base_mtm
        total_stressed_mtm += stressed_mtm

        if abs(impact) > 1000:  # only report material impacts
            trade_impacts.append({
                "trade_id": t.get("trade_id", "")[:8],
                "region": region,
                "type": t.get("trade_type", ""),
                "direction": t.get("buy_sell", ""),
                "volume_mw": volume,
                "base_mtm": round(base_mtm, 0),
                "stressed_mtm": round(stressed_mtm, 0),
                "impact": round(impact, 0),
            })

    trade_impacts.sort(key=lambda x: abs(x.get("impact", 0)), reverse=True)

    # Regional breakdown
    region_impacts: Dict[str, float] = {}
    for ti in trade_impacts:
        r = ti["region"]
        region_impacts[r] = region_impacts.get(r, 0) + ti["impact"]

    total_impact = total_stressed_mtm - total_base_mtm

    result = {
        "scenario_id": scenario_id,
        "scenario_name": scenario["name"],
        "scenario_description": scenario["description"],
        "portfolio_id": portfolio_id,
        "trades_analysed": len(trades or []),
        "base_mtm": round(total_base_mtm, 0),
        "stressed_mtm": round(total_stressed_mtm, 0),
        "total_impact": round(total_impact, 0),
        "impact_pct": round(total_impact / max(abs(total_base_mtm), 1) * 100, 1),
        "region_impacts": {k: round(v, 0) for k, v in region_impacts.items()},
        "top_trade_impacts": trade_impacts[:10],
        "price_shocks_applied": scenario["price_shocks"],
    }

    # Persist stress test result
    try:
        import json as _json
        _insert_gold(f"{_SCHEMA}.stress_test_results", {
            "run_id": str(uuid.uuid4()),
            "portfolio_id": portfolio_id,
            "scenario_id": scenario_id,
            "scenario_name": _sql_escape(scenario["name"]),
            "base_mtm_aud": round(total_base_mtm, 2),
            "stressed_mtm_aud": round(total_stressed_mtm, 2),
            "pnl_change_aud": round(total_impact, 2),
            "pnl_change_pct": round(total_impact / max(abs(total_base_mtm), 1) * 100, 2),
            "var_95_aud": 0,
            "max_loss_aud": round(min(ti.get("impact", 0) for ti in trade_impacts) if trade_impacts else 0, 2),
            "results_json": _sql_escape(_json.dumps(result, default=str)[:4000]),
            "run_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        })
    except Exception as e:
        logger.warning("Failed to persist stress test result: %s", e)

    return result


@router.post("/api/risk/stress-test")
async def stress_test(
    portfolio_id: str = Query("all"),
    scenario_id: str = Query(..., description="Scenario: sa_heatwave, wind_drought, coal_trip_2gw, interconnector_failure, custom"),
):
    """Run a stress test on a portfolio."""
    return _run_stress_test_core(portfolio_id, scenario_id)


@router.get("/api/risk/stress-test/scenarios")
async def stress_test_scenarios():
    """List available stress test scenarios."""
    return [
        {"id": k, "name": v["name"], "description": v["description"]}
        for k, v in _STRESS_SCENARIOS.items()
    ]


# ---------------------------------------------------------------------------
# E14 — Portfolio What-If Analysis
# ---------------------------------------------------------------------------

def _what_if_core(
    scenario_description: str, portfolio_id: str = "all",
    price_change_pct: Optional[float] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """Run a what-if scenario analysis on the portfolio."""

    # Parse scenario: if explicit price change given, use it
    if price_change_pct is not None:
        target_regions = [region] if region else list(_NEM_REGIONS)
        shock_factor = 1.0 + (price_change_pct / 100.0)
        price_shocks = {r: shock_factor for r in target_regions}
        for r in _NEM_REGIONS:
            if r not in price_shocks:
                price_shocks[r] = 1.0
        scenario_name = f"Price {'increase' if price_change_pct > 0 else 'decrease'} {abs(price_change_pct):.0f}%"
        if region:
            scenario_name += f" in {region}"
    else:
        # Interpret NL scenario description
        desc_lower = scenario_description.lower()
        price_shocks = {r: 1.0 for r in _NEM_REGIONS}

        if "heatwave" in desc_lower or "heat" in desc_lower:
            price_shocks = {"SA1": 3.0, "VIC1": 1.8, "NSW1": 1.3, "QLD1": 1.1, "TAS1": 1.0}
            scenario_name = "Heatwave scenario"
        elif "wind drought" in desc_lower or "low wind" in desc_lower:
            price_shocks = {"SA1": 2.5, "VIC1": 2.0, "NSW1": 1.5, "QLD1": 1.2, "TAS1": 1.8}
            scenario_name = "Wind drought scenario"
        elif "coal" in desc_lower and ("trip" in desc_lower or "outage" in desc_lower):
            price_shocks = {"NSW1": 2.8, "QLD1": 1.6, "VIC1": 1.4, "SA1": 1.2, "TAS1": 1.1}
            scenario_name = "Coal plant trip scenario"
        elif "interconnector" in desc_lower or "qni" in desc_lower:
            price_shocks = {"QLD1": 2.2, "NSW1": 0.7, "VIC1": 0.8, "SA1": 0.9, "TAS1": 1.0}
            scenario_name = "Interconnector failure scenario"
        elif any(w in desc_lower for w in ["increase", "rise", "up", "higher", "spike"]):
            # Extract percentage if present
            import re
            pct_match = re.search(r'(\d+)\s*%', desc_lower)
            pct = float(pct_match.group(1)) if pct_match else 20.0
            factor = 1.0 + pct / 100.0
            # Check for specific region
            for r in _NEM_REGIONS:
                if r.lower() in desc_lower or r[:3].lower() in desc_lower:
                    price_shocks[r] = factor
                    break
            else:
                price_shocks = {r: factor for r in _NEM_REGIONS}
            scenario_name = f"Price increase {pct:.0f}%"
        elif any(w in desc_lower for w in ["decrease", "drop", "fall", "down", "lower"]):
            import re
            pct_match = re.search(r'(\d+)\s*%', desc_lower)
            pct = float(pct_match.group(1)) if pct_match else 20.0
            factor = 1.0 - pct / 100.0
            for r in _NEM_REGIONS:
                if r.lower() in desc_lower or r[:3].lower() in desc_lower:
                    price_shocks[r] = factor
                    break
            else:
                price_shocks = {r: factor for r in _NEM_REGIONS}
            scenario_name = f"Price decrease {pct:.0f}%"
        else:
            price_shocks = {r: 1.2 for r in _NEM_REGIONS}
            scenario_name = f"Custom: {scenario_description[:60]}"

    # Run as stress test with custom shocks
    result = _run_stress_test_core(
        portfolio_id, "custom",
        custom_shocks={"price_shocks": price_shocks},
    )
    result["scenario_name"] = scenario_name
    result["scenario_description"] = scenario_description
    result["scenario_id"] = "what_if"
    return result


@router.post("/api/risk/what-if")
async def what_if_analysis(
    scenario: str = Query(..., description="Natural language scenario description"),
    portfolio_id: str = Query("all"),
    price_change_pct: Optional[float] = Query(None, description="Explicit price change percentage"),
    region: Optional[str] = Query(None, description="Target region for price change"),
):
    """Run a what-if scenario analysis on the portfolio."""
    return _what_if_core(scenario, portfolio_id, price_change_pct, region)


# =========================================================================
# Risk Limits CRUD + Monitor
# =========================================================================

@router.get("/api/risk/limits")
async def list_risk_limits(
    portfolio_id: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
):
    """List risk limits, optionally filtered by portfolio/active status."""
    where_parts = []
    if portfolio_id:
        where_parts.append(f"portfolio_id = '{_sql_escape(portfolio_id)}'")
    if is_active is not None:
        where_parts.append(f"is_active = {'true' if is_active else 'false'}")
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.risk_limits {where} ORDER BY limit_type, region"
    )
    if rows:
        for r in rows:
            for k in ("created_at", "updated_at"):
                if k in r and r[k] is not None:
                    r[k] = str(r[k])
        return {"limits": rows, "count": len(rows)}
    return {"limits": [], "count": 0}


@router.post("/api/risk/limits")
async def create_risk_limit(
    portfolio_id: str = Query(...),
    limit_type: str = Query(..., description="POSITION_MW, VAR_AUD, NOTIONAL_AUD, CREDIT_AUD"),
    region: str = Query("ALL"),
    limit_value: float = Query(...),
    warning_pct: float = Query(0.80),
    breach_pct: float = Query(1.0),
    created_by: str = Query("app_user"),
):
    """Create a new risk limit."""
    limit_id = str(uuid.uuid4())
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _insert_gold(f"{_SCHEMA}.risk_limits", {
        "limit_id": limit_id,
        "portfolio_id": _sql_escape(portfolio_id),
        "limit_type": _sql_escape(limit_type),
        "region": _sql_escape(region),
        "limit_value": limit_value,
        "warning_pct": warning_pct,
        "breach_pct": breach_pct,
        "is_active": 1,
        "created_by": _sql_escape(created_by),
        "created_at": now_str,
        "updated_at": now_str,
    })
    return {"status": "created", "limit_id": limit_id}


@router.put("/api/risk/limits/{limit_id}")
async def update_risk_limit(
    limit_id: str,
    limit_value: Optional[float] = Query(None),
    warning_pct: Optional[float] = Query(None),
    breach_pct: Optional[float] = Query(None),
    is_active: Optional[bool] = Query(None),
):
    """Update a risk limit."""
    set_parts = []
    if limit_value is not None:
        set_parts.append(f"limit_value = {limit_value}")
    if warning_pct is not None:
        set_parts.append(f"warning_pct = {warning_pct}")
    if breach_pct is not None:
        set_parts.append(f"breach_pct = {breach_pct}")
    if is_active is not None:
        set_parts.append(f"is_active = {'true' if is_active else 'false'}")
    if not set_parts:
        return {"status": "no_changes"}
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    set_parts.append(f"updated_at = '{now_str}'")
    _execute_gold(
        f"UPDATE {_SCHEMA}.risk_limits SET {', '.join(set_parts)} "
        f"WHERE limit_id = '{_sql_escape(limit_id)}'"
    )
    return {"status": "updated", "limit_id": limit_id}


@router.delete("/api/risk/limits/{limit_id}")
async def delete_risk_limit(limit_id: str):
    """Soft-delete a risk limit (set is_active=false)."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _execute_gold(
        f"UPDATE {_SCHEMA}.risk_limits SET is_active = false, updated_at = '{now_str}' "
        f"WHERE limit_id = '{_sql_escape(limit_id)}'"
    )
    return {"status": "deleted", "limit_id": limit_id}


@router.get("/api/risk/limits/monitor")
async def monitor_risk_limits(portfolio_id: Optional[str] = Query(None)):
    """Check current exposure vs risk limits. Returns utilization + status per limit."""
    # Get active limits
    where = ""
    if portfolio_id:
        where = f"AND portfolio_id = '{_sql_escape(portfolio_id)}'"
    limits = _query_gold(
        f"SELECT * FROM {_SCHEMA}.risk_limits WHERE is_active = true {where}"
    )
    if not limits:
        return {"monitors": [], "count": 0}

    # Get latest MtM per portfolio/region for current values
    mtm_rows = _query_gold(
        f"SELECT portfolio_id, region, SUM(mtm_value) AS total_mtm, "
        f"SUM(ABS(volume_mw * remaining_days / 365.0)) AS position_mw_yrs "
        f"FROM {_SCHEMA}.portfolio_mtm "
        f"WHERE valuation_date = (SELECT MAX(valuation_date) FROM {_SCHEMA}.portfolio_mtm) "
        f"GROUP BY portfolio_id, region"
    )
    mtm_by_pr: Dict[str, Dict[str, Any]] = {}
    for r in (mtm_rows or []):
        key = f"{r.get('portfolio_id', '')}|{r.get('region', 'ALL')}"
        mtm_by_pr[key] = r

    # Get credit exposure
    credit_rows = _query_gold(
        f"SELECT SUM(current_exposure) AS total_credit "
        f"FROM {_SCHEMA}.credit_exposure "
        f"WHERE valuation_date = (SELECT MAX(valuation_date) FROM {_SCHEMA}.credit_exposure)"
    )
    total_credit = float(credit_rows[0]["total_credit"] or 0) if credit_rows else 0.0

    monitors = []
    for lim in limits:
        lt = lim.get("limit_type", "")
        pid = lim.get("portfolio_id", "")
        reg = lim.get("region", "ALL")
        limit_val = float(lim.get("limit_value", 0))
        warn_pct = float(lim.get("warning_pct", 0.80))
        breach_pct = float(lim.get("breach_pct", 1.0))

        # Look up current value based on limit type
        current = 0.0
        key = f"{pid}|{reg}"
        key_all = f"{pid}|ALL"
        mtm_data = mtm_by_pr.get(key) or mtm_by_pr.get(key_all)

        if lt == "POSITION_MW":
            current = float(mtm_data.get("position_mw_yrs", 0)) if mtm_data else 0.0
        elif lt == "VAR_AUD":
            # Use latest VaR from risk_metrics
            var_row = _query_gold(
                f"SELECT var_95_1d FROM {_SCHEMA}.risk_metrics "
                f"WHERE portfolio_id = '{_sql_escape(pid)}' "
                f"ORDER BY created_at DESC LIMIT 1"
            )
            current = abs(float(var_row[0]["var_95_1d"])) if var_row else 0.0
        elif lt == "NOTIONAL_AUD":
            current = abs(float(mtm_data.get("total_mtm", 0))) if mtm_data else 0.0
        elif lt == "CREDIT_AUD":
            current = total_credit

        utilization = current / limit_val if limit_val > 0 else 0.0
        if utilization >= breach_pct:
            status = "BREACH"
        elif utilization >= warn_pct:
            status = "WARNING"
        else:
            status = "OK"

        monitors.append({
            "limit_id": lim.get("limit_id", ""),
            "portfolio_id": pid,
            "limit_type": lt,
            "region": reg,
            "limit_value": limit_val,
            "current_value": round(current, 2),
            "utilization_pct": round(utilization * 100, 1),
            "warning_pct": round(warn_pct * 100, 1),
            "breach_pct": round(breach_pct * 100, 1),
            "status": status,
        })

    return {"monitors": monitors, "count": len(monitors)}


# =========================================================================
# Phase 3 WS2: Advanced Risk Analytics
# =========================================================================

def _historical_var_core(portfolio_id: str = "all",
                         horizon_days: int = 1,
                         confidence: float = 0.95) -> Dict[str, Any]:
    """Calculate Historical VaR from stored results or simulate."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.var_historical "
        f"WHERE portfolio_id = '{_sql_escape(portfolio_id)}' "
        f"AND horizon_days = {horizon_days} "
        f"AND confidence_level = {confidence} "
        f"ORDER BY calc_date DESC LIMIT 10"
    )
    if rows:
        return {
            "method": "historical",
            "portfolio_id": portfolio_id,
            "horizon_days": horizon_days,
            "confidence": confidence,
            "results": [{**r, "calc_date": str(r.get("calc_date", ""))} for r in rows],
        }
    # Fallback: compute from price data
    price_rows = _query_gold(
        f"SELECT rrp FROM {_SCHEMA}.nem_prices_5min "
        f"WHERE interval_datetime >= current_timestamp() - INTERVAL 90 DAYS "
        f"ORDER BY interval_datetime LIMIT 2000"
    )
    if not price_rows:
        return {"error": "No price data for VaR calculation"}
    prices = [float(r["rrp"]) for r in price_rows if r.get("rrp") is not None]
    if len(prices) < 20:
        return {"error": "Insufficient price data"}
    returns = [(prices[i] - prices[i-1]) / max(abs(prices[i-1]), 0.01) for i in range(1, len(prices))]
    returns.sort()
    idx = int(len(returns) * (1 - confidence))
    var_pct = abs(returns[max(idx, 0)])
    cvar_pct = abs(sum(returns[:max(idx, 1)]) / max(idx, 1))
    notional = 10_000_000  # assumed portfolio notional
    return {
        "method": "historical_computed",
        "portfolio_id": portfolio_id,
        "horizon_days": horizon_days,
        "confidence": confidence,
        "var_pct": round(var_pct, 6),
        "cvar_pct": round(cvar_pct, 6),
        "var_amount": round(var_pct * notional * math.sqrt(horizon_days), 2),
        "cvar_amount": round(cvar_pct * notional * math.sqrt(horizon_days), 2),
        "observations": len(returns),
    }


def _monte_carlo_var_core(portfolio_id: str = "all",
                          horizon_days: int = 1,
                          confidence: float = 0.95,
                          num_sims: int = 10000) -> Dict[str, Any]:
    """Monte Carlo VaR simulation."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.var_monte_carlo "
        f"WHERE portfolio_id = '{_sql_escape(portfolio_id)}' "
        f"ORDER BY calc_date DESC LIMIT 5"
    )
    if rows:
        return {
            "method": "monte_carlo",
            "portfolio_id": portfolio_id,
            "results": [{**r, "calc_date": str(r.get("calc_date", ""))} for r in rows],
        }
    # Simulate — use real historical vol where available
    rng = random.Random(42)
    annual_vol = _get_historical_volatility("NSW1")
    vol = annual_vol / math.sqrt(252)  # annualised → daily
    notional = 10_000_000
    pnls = sorted([notional * rng.gauss(0, vol * math.sqrt(horizon_days)) for _ in range(num_sims)])
    idx = int(num_sims * (1 - confidence))
    var_amount = abs(pnls[idx])
    cvar_amount = abs(sum(pnls[:idx]) / max(idx, 1))
    return {
        "method": "monte_carlo_computed",
        "portfolio_id": portfolio_id,
        "horizon_days": horizon_days,
        "confidence": confidence,
        "num_simulations": num_sims,
        "var_amount": round(var_amount, 2),
        "cvar_amount": round(cvar_amount, 2),
        "mean_pnl": round(sum(pnls) / len(pnls), 2),
        "std_pnl": round((sum((p - sum(pnls)/len(pnls))**2 for p in pnls) / len(pnls))**0.5, 2),
    }


def _vol_surface_core(region: str = "NSW1") -> Dict[str, Any]:
    """Get volatility surface data for a region."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.vol_surface "
        f"WHERE region = '{_sql_escape(region)}' "
        f"ORDER BY tenor_days, strike_pct"
    )
    if rows:
        return {
            "region": region,
            "surface": [{**r, "calc_date": str(r.get("calc_date", ""))} for r in rows],
        }

    # Computed fallback: build vol surface from historical vol with tenor/smile adjustments
    base_vol = _get_historical_volatility(region)
    today = str(date.today())
    tenors = [7, 30, 90, 180, 365]
    strikes = [0.8, 0.9, 1.0, 1.1, 1.2]
    surface = []
    for tenor in tenors:
        # Longer tenors → lower vol (mean reversion), sqrt scaling
        tenor_adj = math.sqrt(30 / max(tenor, 1))
        for strike in strikes:
            # Smile: OTM options have higher vol
            smile_adj = 1.0 + 0.15 * (strike - 1.0) ** 2
            vol = round(base_vol * tenor_adj * smile_adj, 4)
            surface.append({
                "region": region,
                "tenor_days": tenor,
                "strike_pct": strike,
                "implied_vol": vol,
                "calc_date": today,
            })

    return {
        "region": region,
        "surface": surface,
    }


def _reverse_stress_test_core(target_loss: float = -1_000_000,
                               portfolio_id: str = "all") -> Dict[str, Any]:
    """Find scenarios that produce a given loss level."""
    scenarios = _query_gold(
        f"SELECT * FROM {_SCHEMA}.stress_test_library WHERE is_active = true"
    )
    if not scenarios:
        return {"error": "No stress scenarios in library"}

    results = []
    for s in scenarios:
        # Estimate portfolio impact from shock percentages
        price_shock = float(s.get("price_shock_pct", 0) or 0) / 100
        estimated_loss = -abs(price_shock) * 5_000_000  # rough scaling
        if estimated_loss <= target_loss:
            results.append({
                "scenario_id": s.get("scenario_id", ""),
                "scenario_name": s.get("scenario_name", ""),
                "category": s.get("category", ""),
                "severity": s.get("severity", ""),
                "price_shock_pct": float(s.get("price_shock_pct", 0) or 0),
                "estimated_loss": round(estimated_loss, 0),
                "description": s.get("description", ""),
            })
    results.sort(key=lambda x: x["estimated_loss"])

    return {
        "target_loss": target_loss,
        "portfolio_id": portfolio_id,
        "matching_scenarios": results,
        "total_scenarios_checked": len(scenarios),
    }


@router.get("/api/risk/var/historical")
async def historical_var(
    portfolio_id: str = Query("all"),
    horizon_days: int = Query(1, ge=1, le=30),
    confidence: float = Query(0.95),
):
    """Historical VaR calculation."""
    return _historical_var_core(portfolio_id, horizon_days, confidence)


@router.get("/api/risk/var/monte-carlo")
async def monte_carlo_var(
    portfolio_id: str = Query("all"),
    horizon_days: int = Query(1, ge=1, le=30),
    confidence: float = Query(0.95),
    num_sims: int = Query(10000, le=50000),
):
    """Monte Carlo VaR simulation."""
    return _monte_carlo_var_core(portfolio_id, horizon_days, confidence, num_sims)


@router.get("/api/risk/vol-surface")
async def vol_surface(region: str = Query("NSW1")):
    """Get volatility surface for a region."""
    return _vol_surface_core(region)


@router.post("/api/risk/reverse-stress-test")
async def reverse_stress_test(
    target_loss: float = Query(-1_000_000),
    portfolio_id: str = Query("all"),
):
    """Find scenarios that produce a target loss."""
    return _reverse_stress_test_core(target_loss, portfolio_id)


@router.get("/api/risk/stress-scenarios")
async def stress_scenario_library():
    """List all stress test scenarios."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.stress_test_library ORDER BY category, severity"
    )
    return {"scenarios": rows or []}
