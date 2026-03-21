"""Algorithmic Trading Signals & Opportunities Engine.

Scans live NEM market data across 6 gold tables and detects 8 signal types:
FORWARD_MISPRICING, SPOT_ARBITRAGE, CONSTRAINT_PLAY, WEATHER_SHIFT,
DEMAND_SURGE, VOLATILITY_REGIME, RENEWABLE_SHORTFALL, PEAK_SPREAD.

Each signal includes a markdown rationale with traceable chain of reasoning.
"""
from __future__ import annotations

import json
import random as _r
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from .shared import (
    _CATALOG,
    _NEM_REGIONS,
    _cache_get,
    _cache_set,
    _execute_gold,
    _insert_gold,
    _insert_gold_batch,
    _invalidate_cache,
    _query_gold,
    _sql_escape,
    _update_gold,
    logger,
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"

# ---------------------------------------------------------------------------
# Signal type definitions
# ---------------------------------------------------------------------------
_SIGNAL_TYPES = [
    "FORWARD_MISPRICING", "SPOT_ARBITRAGE", "CONSTRAINT_PLAY",
    "WEATHER_SHIFT", "DEMAND_SURGE", "VOLATILITY_REGIME",
    "RENEWABLE_SHORTFALL", "PEAK_SPREAD",
]
_TTL_HOURS = {
    "FORWARD_MISPRICING": 4, "SPOT_ARBITRAGE": 0.5, "CONSTRAINT_PLAY": 1,
    "WEATHER_SHIFT": 2, "DEMAND_SURGE": 1, "VOLATILITY_REGIME": 8,
    "RENEWABLE_SHORTFALL": 2, "PEAK_SPREAD": 4,
}


# =========================================================================
# Core signal generation engine
# =========================================================================

def _generate_signals_core(region: Optional[str] = None) -> List[Dict[str, Any]]:
    """Scan gold tables and detect trading signals. Returns list of signal dicts."""
    signals: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    regions = [region] if region else _NEM_REGIONS

    # --- Fetch market data (with caching) ---
    prices = _fetch_prices(regions)
    interconnectors = _fetch_interconnectors()
    weather = _fetch_weather(regions)
    demand_forecasts = _fetch_demand_forecasts(regions)
    generation = _fetch_generation(regions)
    futures = _fetch_futures()

    for r in regions:
        r_prices = [p for p in prices if p.get("region_id") == r]
        r_weather = [w for w in weather if w.get("nem_region") == r]
        r_demand = [d for d in demand_forecasts if d.get("region_id") == r]
        r_gen = [g for g in generation if g.get("region_id") == r]

        # 1. FORWARD_MISPRICING
        sig = _detect_forward_mispricing(r, r_prices, futures, now)
        if sig:
            signals.append(sig)

        # 2. SPOT_ARBITRAGE
        sigs = _detect_spot_arbitrage(r, r_prices, prices, interconnectors, now)
        signals.extend(sigs)

        # 3. CONSTRAINT_PLAY
        sig = _detect_constraint_play(r, interconnectors, r_prices, now)
        if sig:
            signals.append(sig)

        # 4. WEATHER_SHIFT
        sig = _detect_weather_shift(r, r_weather, r_prices, now)
        if sig:
            signals.append(sig)

        # 5. DEMAND_SURGE
        sig = _detect_demand_surge(r, r_demand, r_prices, now)
        if sig:
            signals.append(sig)

        # 6. VOLATILITY_REGIME
        sig = _detect_volatility_regime(r, r_prices, now)
        if sig:
            signals.append(sig)

        # 7. RENEWABLE_SHORTFALL
        sig = _detect_renewable_shortfall(r, r_gen, r_prices, now)
        if sig:
            signals.append(sig)

        # 8. PEAK_SPREAD
        sig = _detect_peak_spread(r, r_prices, futures, now)
        if sig:
            signals.append(sig)

    return signals


# ---------------------------------------------------------------------------
# Data fetchers (cached 30s)
# ---------------------------------------------------------------------------

def _fetch_prices(regions: List[str]) -> List[Dict]:
    cache_key = "signals:prices"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    rows = _query_gold(
        f"SELECT region_id, rrp, total_demand_mw, interval_datetime "
        f"FROM {_SCHEMA}.nem_prices_5min "
        f"WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS "
        f"ORDER BY interval_datetime DESC LIMIT 2000"
    )
    result = rows or []
    _cache_set(cache_key, result, 30)
    return result


def _fetch_interconnectors() -> List[Dict]:
    cache_key = "signals:interconnectors"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    rows = _query_gold(
        f"SELECT interconnectorid, from_region, to_region, mw_flow, "
        f"export_limit, import_limit, interval_datetime "
        f"FROM {_SCHEMA}.nem_interconnectors "
        f"WHERE interval_datetime >= current_timestamp() - INTERVAL 1 HOUR "
        f"ORDER BY interval_datetime DESC LIMIT 100"
    )
    result = rows or []
    _cache_set(cache_key, result, 30)
    return result


def _fetch_weather(regions: List[str]) -> List[Dict]:
    cache_key = "signals:weather"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    rows = _query_gold(
        f"SELECT nem_region, temperature_c, wind_speed_100m_kmh, "
        f"solar_radiation_wm2, forecast_datetime "
        f"FROM {_SCHEMA}.weather_nem_regions "
        f"WHERE forecast_datetime >= current_timestamp() - INTERVAL 2 HOURS "
        f"ORDER BY forecast_datetime DESC LIMIT 50"
    )
    result = rows or []
    _cache_set(cache_key, result, 30)
    return result


def _fetch_demand_forecasts(regions: List[str]) -> List[Dict]:
    cache_key = "signals:demand"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    rows = _query_gold(
        f"SELECT region_id, predicted_demand_mw, interval_datetime "
        f"FROM {_SCHEMA}.demand_forecasts "
        f"WHERE interval_datetime >= current_timestamp() - INTERVAL 4 HOURS "
        f"ORDER BY interval_datetime DESC LIMIT 200"
    )
    result = rows or []
    _cache_set(cache_key, result, 30)
    return result


def _fetch_generation(regions: List[str]) -> List[Dict]:
    cache_key = "signals:generation"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    rows = _query_gold(
        f"SELECT region_id, fuel_type, generation_mw, interval_datetime "
        f"FROM {_SCHEMA}.nem_generation_by_fuel "
        f"WHERE interval_datetime >= current_timestamp() - INTERVAL 1 HOUR "
        f"ORDER BY interval_datetime DESC LIMIT 500"
    )
    result = rows or []
    _cache_set(cache_key, result, 30)
    return result


def _fetch_futures() -> List[Dict]:
    cache_key = "signals:futures"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    rows = _query_gold(
        f"SELECT region, quarter, settlement_price, trade_date "
        f"FROM {_SCHEMA}.asx_futures_eod "
        f"WHERE trade_date >= current_date() - INTERVAL 7 DAYS "
        f"ORDER BY trade_date DESC LIMIT 100"
    )
    result = rows or []
    _cache_set(cache_key, result, 60)
    return result


# ---------------------------------------------------------------------------
# Signal detection functions
# ---------------------------------------------------------------------------

_RENEWABLES = {"wind", "solar_utility", "solar_rooftop", "hydro", "battery_discharging"}


def _make_signal(signal_type: str, region: str, direction: str,
                 confidence: float, profit: float, risk: float,
                 volume: float, price: float, trade_type: str,
                 profile: str, rationale: str, context: dict,
                 now: datetime) -> Dict[str, Any]:
    ttl = _TTL_HOURS.get(signal_type, 2)
    start = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    end = (now + timedelta(days=90)).strftime("%Y-%m-%d")
    return {
        "signal_id": str(uuid.uuid4()),
        "signal_type": signal_type,
        "region": region,
        "direction": direction,
        "confidence": round(confidence, 3),
        "expected_profit_aud": round(profit, 2),
        "risk_aud": round(risk, 2),
        "reward_risk_ratio": round(profit / risk, 2) if risk > 0 else 0,
        "suggested_volume_mw": round(volume, 1),
        "suggested_price": round(price, 2),
        "suggested_trade_type": trade_type,
        "suggested_profile": profile,
        "suggested_start_date": start,
        "suggested_end_date": end,
        "rationale": rationale,
        "market_context": json.dumps(context, default=str),
        "status": "ACTIVE",
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "expires_at": (now + timedelta(hours=ttl)).strftime("%Y-%m-%d %H:%M:%S"),
        "executed_trade_id": None,
        "executed_at": None,
    }


def _detect_forward_mispricing(region: str, prices: List[Dict],
                               futures: List[Dict], now: datetime) -> Optional[Dict]:
    if not prices:
        return None
    recent_rrps = [float(p["rrp"]) for p in prices[:12] if p.get("rrp") is not None]
    if not recent_rrps:
        return None
    spot_avg = sum(recent_rrps) / len(recent_rrps)

    region_futures = [f for f in futures if f.get("region") == region.replace("1", "")]
    if not region_futures:
        region_futures = [f for f in futures if f.get("region") == region]
    if not region_futures:
        return None
    forward_price = float(region_futures[0].get("settlement_price", 0))
    if forward_price <= 0:
        return None

    spread = spot_avg - forward_price
    if abs(spread) < 15:
        return None

    direction = "BUY" if spread < 0 else "SELL"
    confidence = min(0.95, 0.5 + abs(spread) / 100)
    volume = 25.0
    profit = abs(spread) * volume * 24 * 30  # monthly estimate
    risk = profit * 0.4

    rationale = (
        f"### Forward Mispricing Detected — {region}\n\n"
        f"**What:** {region} spot price average (${spot_avg:.2f}/MWh over last hour) "
        f"{'below' if spread < 0 else 'above'} nearest-month forward "
        f"(${forward_price:.2f}/MWh) by ${abs(spread):.2f}/MWh.\n\n"
        f"**Why:** Spot avg = ${spot_avg:.2f}/MWh (last {len(recent_rrps)} intervals). "
        f"ASX forward = ${forward_price:.2f}/MWh. "
        f"Spread = ${abs(spread):.2f}/MWh "
        f"({'spot discount' if spread < 0 else 'spot premium'}).\n\n"
        f"**Risk:** Forward convergence may not occur if current conditions persist. "
        f"Weather changes or outages could widen spread further."
    )
    return _make_signal(
        "FORWARD_MISPRICING", region, direction, confidence,
        profit, risk, volume, forward_price, "SWAP", "FLAT",
        rationale, {"spot_avg": spot_avg, "forward_price": forward_price, "spread": spread},
        now,
    )


def _detect_spot_arbitrage(region: str, r_prices: List[Dict],
                           all_prices: List[Dict], interconnectors: List[Dict],
                           now: datetime) -> List[Dict]:
    signals = []
    if not r_prices:
        return signals
    my_price = float(r_prices[0].get("rrp", 0))

    for other_r in _NEM_REGIONS:
        if other_r == region:
            continue
        other_prices = [p for p in all_prices if p.get("region_id") == other_r]
        if not other_prices:
            continue
        other_price = float(other_prices[0].get("rrp", 0))
        diff = my_price - other_price
        if abs(diff) < 20:
            continue

        # Check interconnector congestion
        ic_congested = False
        for ic in interconnectors:
            fr, to = ic.get("from_region", ""), ic.get("to_region", "")
            if (fr == region and to == other_r) or (fr == other_r and to == region):
                flow = abs(float(ic.get("mw_flow", 0)))
                limit = max(float(ic.get("export_limit", 1)), float(ic.get("import_limit", 1)))
                if flow > limit * 0.9:
                    ic_congested = True

        if ic_congested:
            continue

        buy_region = region if diff < 0 else other_r
        sell_region = region if diff > 0 else other_r
        confidence = min(0.9, 0.5 + abs(diff) / 80)
        volume = 20.0
        profit = abs(diff) * volume * 6  # 30 min window
        risk = profit * 0.5

        rationale = (
            f"### Spot Arbitrage — {buy_region} vs {sell_region}\n\n"
            f"**What:** Price differential of ${abs(diff):.2f}/MWh between "
            f"{region} (${my_price:.2f}) and {other_r} (${other_price:.2f}).\n\n"
            f"**Why:** {buy_region} priced lower → buy there, {sell_region} priced higher → sell there. "
            f"Interconnector not congested, flow can proceed.\n\n"
            f"**Risk:** Price convergence may happen before execution. "
            f"Interconnector constraints could bind suddenly."
        )
        signals.append(_make_signal(
            "SPOT_ARBITRAGE", buy_region, "BUY", confidence,
            profit, risk, volume, min(my_price, other_price),
            "SPOT", "FLAT", rationale,
            {"region_a": region, "price_a": my_price, "region_b": other_r,
             "price_b": other_price, "diff": diff},
            now,
        ))
    return signals


def _detect_constraint_play(region: str, interconnectors: List[Dict],
                            prices: List[Dict], now: datetime) -> Optional[Dict]:
    congested = []
    for ic in interconnectors:
        flow = abs(float(ic.get("mw_flow", 0)))
        limit = max(float(ic.get("export_limit", 1)), float(ic.get("import_limit", 1)))
        if flow > limit * 0.9 and (ic.get("from_region") == region or ic.get("to_region") == region):
            congested.append(ic)

    if not congested:
        return None

    spot = float(prices[0]["rrp"]) if prices else 0
    ic = congested[0]
    flow = float(ic.get("mw_flow", 0))
    limit_val = max(float(ic.get("export_limit", 1)), float(ic.get("import_limit", 1)))
    ic_name = ic.get("interconnectorid", "?")

    direction = "BUY" if spot > 100 else "SELL"
    confidence = min(0.85, 0.5 + len(congested) * 0.15)
    volume = 15.0
    profit = abs(spot - 80) * volume * 12  # 1h
    risk = profit * 0.5

    rationale = (
        f"### Constraint Play — {region}\n\n"
        f"**What:** Interconnector {ic_name} congested at {abs(flow):.0f}/{limit_val:.0f} MW "
        f"({abs(flow)/limit_val*100:.0f}% utilisation). {region} structurally separated.\n\n"
        f"**Why:** With {len(congested)} congested link(s), {region} prices are decoupled "
        f"from neighbouring regions. Current spot ${spot:.2f}/MWh "
        f"{'(elevated, expect mean reversion)' if spot > 100 else '(depressed, supply surplus)'}.\n\n"
        f"**Risk:** Constraint may clear as demand shifts. AEMO re-dispatch can change flows."
    )
    return _make_signal(
        "CONSTRAINT_PLAY", region, direction, confidence,
        profit, risk, volume, spot, "SPOT", "FLAT",
        rationale, {"congested_ics": [c.get("interconnectorid") for c in congested],
                    "spot": spot, "flow": flow, "limit": limit_val},
        now,
    )


def _detect_weather_shift(region: str, weather: List[Dict],
                          prices: List[Dict], now: datetime) -> Optional[Dict]:
    if not weather:
        return None
    w = weather[0]
    temp = float(w.get("temperature_c", 25))
    wind = float(w.get("wind_speed_100m_kmh", 20))
    spot = float(prices[0]["rrp"]) if prices else 70

    if temp > 38:
        confidence = min(0.9, 0.5 + (temp - 38) / 10)
        profit = (temp - 35) * 50 * 24
        risk = profit * 0.35
        rationale = (
            f"### Weather-Driven Demand Surge — {region}\n\n"
            f"**What:** Temperature at {temp:.1f}°C (extreme heat). "
            f"Expect elevated cooling demand and price spikes.\n\n"
            f"**Why:** Temperature = {temp:.1f}°C (>38°C threshold). "
            f"Air conditioning load drives demand 15-25% above baseline. "
            f"Spot = ${spot:.2f}/MWh.\n\n"
            f"**Risk:** BOM forecast may shift. Cloud cover could reduce AC demand."
        )
        return _make_signal(
            "WEATHER_SHIFT", region, "BUY", confidence,
            profit, risk, 30.0, spot * 1.2, "SPOT", "PEAK",
            rationale, {"temp_c": temp, "wind_kmh": wind, "spot": spot},
            now,
        )
    elif wind < 8:
        confidence = min(0.8, 0.5 + (8 - wind) / 16)
        profit = (8 - wind) * 40 * 24
        risk = profit * 0.4
        rationale = (
            f"### Renewable Shortfall (Low Wind) — {region}\n\n"
            f"**What:** Wind speed at {wind:.1f} km/h (below 8 km/h threshold). "
            f"Wind generation will be significantly curtailed.\n\n"
            f"**Why:** Wind = {wind:.1f} km/h. Turbines below cut-in speed. "
            f"Expect gas peakers to fill supply gap. Spot = ${spot:.2f}/MWh.\n\n"
            f"**Risk:** Wind forecast may improve within 2h. Solar may compensate during daylight."
        )
        return _make_signal(
            "WEATHER_SHIFT", region, "BUY", confidence,
            profit, risk, 20.0, spot * 1.1, "SPOT", "FLAT",
            rationale, {"temp_c": temp, "wind_kmh": wind, "spot": spot},
            now,
        )
    return None


def _detect_demand_surge(region: str, forecasts: List[Dict],
                         prices: List[Dict], now: datetime) -> Optional[Dict]:
    if not forecasts:
        return None
    demands = [float(f.get("predicted_demand_mw", 0)) for f in forecasts if f.get("predicted_demand_mw")]
    if len(demands) < 5:
        return None
    avg_demand = sum(demands) / len(demands)
    latest = demands[0]
    std_dev = (sum((d - avg_demand) ** 2 for d in demands) / len(demands)) ** 0.5
    if std_dev == 0:
        return None

    z_score = (latest - avg_demand) / std_dev
    if z_score < 1.5:
        return None

    spot = float(prices[0]["rrp"]) if prices else 80
    confidence = min(0.9, 0.5 + (z_score - 1.5) / 3)
    profit = (latest - avg_demand) * 0.5 * 12
    risk = profit * 0.4

    rationale = (
        f"### Demand Surge Detected — {region}\n\n"
        f"**What:** Forecast demand {latest:.0f} MW is {z_score:.1f}σ above "
        f"historical average ({avg_demand:.0f} MW).\n\n"
        f"**Why:** Latest demand forecast = {latest:.0f} MW. "
        f"4h rolling average = {avg_demand:.0f} MW (σ = {std_dev:.0f} MW). "
        f"z-score = {z_score:.2f} (threshold = 1.5). Spot = ${spot:.2f}/MWh.\n\n"
        f"**Risk:** Demand response activations could cap peak. "
        f"Actual demand may undershoot forecast."
    )
    return _make_signal(
        "DEMAND_SURGE", region, "BUY", confidence,
        profit, risk, 25.0, spot * 1.15, "SPOT", "PEAK",
        rationale, {"latest_demand": latest, "avg_demand": avg_demand,
                    "std_dev": std_dev, "z_score": z_score, "spot": spot},
        now,
    )


def _detect_volatility_regime(region: str, prices: List[Dict],
                              now: datetime) -> Optional[Dict]:
    if len(prices) < 50:
        return None
    rrps = [float(p["rrp"]) for p in prices if p.get("rrp") is not None]
    if len(rrps) < 50:
        return None

    # 7-day vs 90-day approximation using available data
    short_window = rrps[:min(84, len(rrps))]  # ~7h of 5-min data
    long_window = rrps

    short_vol = (sum((r - sum(short_window)/len(short_window)) ** 2 for r in short_window) / len(short_window)) ** 0.5
    long_vol = (sum((r - sum(long_window)/len(long_window)) ** 2 for r in long_window) / len(long_window)) ** 0.5

    if long_vol == 0:
        return None
    ratio = short_vol / long_vol

    if 0.5 < ratio < 1.5:
        return None

    regime = "HIGH" if ratio >= 1.5 else "LOW"
    direction = "SELL" if regime == "HIGH" else "BUY"
    confidence = min(0.85, 0.5 + abs(ratio - 1) / 3)
    profit = abs(short_vol - long_vol) * 20 * 24
    risk = profit * 0.45

    rationale = (
        f"### Volatility Regime Change — {region}\n\n"
        f"**What:** {regime} volatility regime detected. "
        f"Short-term vol ({short_vol:.1f}) is {ratio:.2f}× long-term vol ({long_vol:.1f}).\n\n"
        f"**Why:** Short-window σ = ${short_vol:.2f}/MWh. "
        f"Long-window σ = ${long_vol:.2f}/MWh. Ratio = {ratio:.2f}× "
        f"(threshold: >1.5× = HIGH, <0.5× = LOW).\n\n"
        f"**Risk:** Regime may revert quickly. External shocks can override patterns."
    )
    return _make_signal(
        "VOLATILITY_REGIME", region, direction, confidence,
        profit, risk, 20.0, sum(rrps[:12]) / min(12, len(rrps)),
        "OPTION" if regime == "HIGH" else "SWAP", "FLAT",
        rationale, {"short_vol": short_vol, "long_vol": long_vol,
                    "ratio": ratio, "regime": regime},
        now,
    )


def _detect_renewable_shortfall(region: str, gen: List[Dict],
                                prices: List[Dict], now: datetime) -> Optional[Dict]:
    if not gen:
        return None
    # Get latest interval generation
    latest_ts = gen[0].get("interval_datetime")
    latest_gen = [g for g in gen if g.get("interval_datetime") == latest_ts]

    total_mw = sum(float(g.get("generation_mw", 0)) for g in latest_gen)
    renewable_mw = sum(
        float(g.get("generation_mw", 0)) for g in latest_gen
        if g.get("fuel_type", "").lower() in _RENEWABLES
    )
    if total_mw <= 0:
        return None
    renew_pct = renewable_mw / total_mw * 100

    if renew_pct >= 15:
        return None

    spot = float(prices[0]["rrp"]) if prices else 90
    confidence = min(0.85, 0.5 + (15 - renew_pct) / 30)
    profit = (15 - renew_pct) * 30 * 12
    risk = profit * 0.4

    rationale = (
        f"### Renewable Shortfall — {region}\n\n"
        f"**What:** Renewable share at {renew_pct:.1f}% (below 15% threshold). "
        f"Fossil generators filling gap.\n\n"
        f"**Why:** Total generation = {total_mw:.0f} MW, "
        f"renewable = {renewable_mw:.0f} MW ({renew_pct:.1f}%). "
        f"Thermal plant costs push prices up. Spot = ${spot:.2f}/MWh.\n\n"
        f"**Risk:** Wind/solar ramp may arrive. Battery discharge could offset shortfall."
    )
    return _make_signal(
        "RENEWABLE_SHORTFALL", region, "BUY", confidence,
        profit, risk, 20.0, spot * 1.1, "SPOT", "FLAT",
        rationale, {"total_mw": total_mw, "renewable_mw": renewable_mw,
                    "renewable_pct": renew_pct, "spot": spot},
        now,
    )


def _detect_peak_spread(region: str, prices: List[Dict],
                        futures: List[Dict], now: datetime) -> Optional[Dict]:
    if not prices or len(prices) < 20:
        return None
    # Simple peak/off-peak split from recent prices
    peak_prices = []
    offpeak_prices = []
    for p in prices:
        ts = p.get("interval_datetime")
        rrp = float(p.get("rrp", 0))
        hour = None
        if hasattr(ts, "hour"):
            hour = ts.hour
        elif isinstance(ts, str) and len(ts) >= 13:
            try:
                hour = int(ts[11:13])
            except (ValueError, IndexError):
                pass
        if hour is not None:
            if 7 <= hour < 22:
                peak_prices.append(rrp)
            else:
                offpeak_prices.append(rrp)

    if not peak_prices or not offpeak_prices:
        return None

    peak_avg = sum(peak_prices) / len(peak_prices)
    offpeak_avg = sum(offpeak_prices) / len(offpeak_prices)
    if offpeak_avg <= 0:
        return None

    ratio = peak_avg / offpeak_avg
    seasonal_norm = 1.5  # typical peak/off-peak ratio
    deviation = (ratio - seasonal_norm) / seasonal_norm

    if abs(deviation) < 0.15:
        return None

    direction = "BUY" if deviation > 0 else "SELL"
    confidence = min(0.8, 0.5 + abs(deviation) / 0.5)
    profit = abs(peak_avg - offpeak_avg * seasonal_norm) * 20 * 30
    risk = profit * 0.45

    rationale = (
        f"### Peak Spread Anomaly — {region}\n\n"
        f"**What:** Peak/off-peak ratio at {ratio:.2f}× vs seasonal norm {seasonal_norm:.1f}×. "
        f"Deviation = {deviation*100:.1f}%.\n\n"
        f"**Why:** Peak avg = ${peak_avg:.2f}/MWh, off-peak avg = ${offpeak_avg:.2f}/MWh. "
        f"Ratio = {ratio:.2f}× (norm = {seasonal_norm:.1f}×, deviation = {deviation*100:.1f}%).\n\n"
        f"**Risk:** Seasonal patterns may justify current spread. Industrial load shifts could change profile."
    )
    return _make_signal(
        "PEAK_SPREAD", region, direction, confidence,
        profit, risk, 20.0, peak_avg if direction == "SELL" else offpeak_avg,
        "SWAP", "PEAK" if direction == "SELL" else "OFF_PEAK",
        rationale, {"peak_avg": peak_avg, "offpeak_avg": offpeak_avg,
                    "ratio": ratio, "deviation": deviation},
        now,
    )


# ---------------------------------------------------------------------------
# Mock signal fallback
# ---------------------------------------------------------------------------

def _mock_signals() -> List[Dict[str, Any]]:
    """Generate mock signals when live data isn't available."""
    _r.seed(42)
    now = datetime.now(timezone.utc)
    signals = []
    for i, (stype, region) in enumerate([
        ("FORWARD_MISPRICING", "NSW1"), ("SPOT_ARBITRAGE", "QLD1"),
        ("CONSTRAINT_PLAY", "SA1"), ("WEATHER_SHIFT", "VIC1"),
        ("DEMAND_SURGE", "NSW1"), ("VOLATILITY_REGIME", "SA1"),
        ("RENEWABLE_SHORTFALL", "QLD1"), ("PEAK_SPREAD", "TAS1"),
    ]):
        direction = _r.choice(["BUY", "SELL"])
        confidence = round(_r.uniform(0.55, 0.92), 3)
        profit = round(_r.uniform(5000, 150000), 2)
        risk = round(profit * _r.uniform(0.25, 0.5), 2)
        vol = round(_r.uniform(10, 50), 1)
        price = round(_r.uniform(40, 200), 2)
        ttl = _TTL_HOURS.get(stype, 2)
        signals.append({
            "signal_id": str(uuid.uuid4()),
            "signal_type": stype,
            "region": region,
            "direction": direction,
            "confidence": confidence,
            "expected_profit_aud": profit,
            "risk_aud": risk,
            "reward_risk_ratio": round(profit / risk, 2) if risk > 0 else 0,
            "suggested_volume_mw": vol,
            "suggested_price": price,
            "suggested_trade_type": _r.choice(["SWAP", "SPOT", "FORWARD"]),
            "suggested_profile": _r.choice(["FLAT", "PEAK", "OFF_PEAK"]),
            "suggested_start_date": (now + timedelta(days=1)).strftime("%Y-%m-%d"),
            "suggested_end_date": (now + timedelta(days=90)).strftime("%Y-%m-%d"),
            "rationale": (
                f"### {stype.replace('_', ' ').title()} — {region}\n\n"
                f"**What:** Mock signal for demonstration.\n\n"
                f"**Why:** Simulated market condition detected with {confidence*100:.0f}% confidence.\n\n"
                f"**Risk:** This is mock data for UI demonstration."
            ),
            "market_context": json.dumps({"mock": True}),
            "status": "ACTIVE",
            "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "expires_at": (now + timedelta(hours=ttl)).strftime("%Y-%m-%d %H:%M:%S"),
            "executed_trade_id": None,
            "executed_at": None,
        })
    return signals


# =========================================================================
# API Endpoints
# =========================================================================

@router.get("/api/signals")
async def list_signals(
    region: Optional[str] = Query(None),
    signal_type: Optional[str] = Query(None),
    min_confidence: float = Query(0.0),
    status: str = Query("ACTIVE"),
):
    """List active trading signals with optional filters."""
    try:
        where = [f"status = '{_sql_escape(status)}'"]
        if region:
            where.append(f"region = '{_sql_escape(region)}'")
        if signal_type:
            where.append(f"signal_type = '{_sql_escape(signal_type)}'")
        if min_confidence > 0:
            where.append(f"confidence >= {min_confidence}")

        rows = _query_gold(
            f"SELECT * FROM {_SCHEMA}.trading_signals "
            f"WHERE {' AND '.join(where)} "
            f"ORDER BY confidence DESC, expected_profit_aud DESC "
            f"LIMIT 50"
        )
        if rows:
            for r in rows:
                for k in ("generated_at", "expires_at", "executed_at"):
                    if r.get(k):
                        r[k] = str(r[k])
            return {"signals": rows, "count": len(rows)}
    except Exception as exc:
        logger.warning("list_signals error: %s", exc)

    # Fallback to mock
    mock = _mock_signals()
    if region:
        mock = [s for s in mock if s["region"] == region]
    if signal_type:
        mock = [s for s in mock if s["signal_type"] == signal_type]
    if min_confidence > 0:
        mock = [s for s in mock if s["confidence"] >= min_confidence]
    return {"signals": mock, "count": len(mock)}


@router.post("/api/signals/scan")
async def scan_signals(region: Optional[str] = Query(None)):
    """Run fresh market scan — expire stale signals, generate new ones."""
    try:
        # Expire stale signals
        _execute_gold(
            f"UPDATE {_SCHEMA}.trading_signals "
            f"SET status = 'EXPIRED' "
            f"WHERE status = 'ACTIVE' AND expires_at < current_timestamp()"
        )
    except Exception as exc:
        logger.warning("expire signals error: %s", exc)

    # Generate new signals
    signals = _generate_signals_core(region)

    if not signals:
        # Fallback to mock
        signals = _mock_signals()
        if region:
            signals = [s for s in signals if s["region"] == region]

    # Persist to Delta
    if signals:
        try:
            _insert_gold_batch(f"{_SCHEMA}.trading_signals", signals)
            _invalidate_cache("sql:")
        except Exception as exc:
            logger.warning("insert signals error: %s", exc)

    return {
        "signals": signals,
        "count": len(signals),
        "scanned_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


@router.get("/api/signals/performance")
async def signal_performance():
    """Historical signal performance — win rate, P&L by type."""
    try:
        rows = _query_gold(
            f"SELECT signal_type, status, COUNT(*) as cnt, "
            f"SUM(expected_profit_aud) as total_expected_profit, "
            f"AVG(confidence) as avg_confidence, "
            f"AVG(reward_risk_ratio) as avg_reward_risk "
            f"FROM {_SCHEMA}.trading_signals "
            f"GROUP BY signal_type, status "
            f"ORDER BY signal_type"
        )
        if rows:
            by_type: Dict[str, Any] = {}
            for r in rows:
                stype = r["signal_type"]
                if stype not in by_type:
                    by_type[stype] = {"total": 0, "executed": 0, "expired": 0,
                                      "dismissed": 0, "active": 0,
                                      "total_profit": 0, "avg_confidence": 0}
                status = r.get("status", "").upper()
                cnt = int(r.get("cnt", 0))
                by_type[stype]["total"] += cnt
                if status == "EXECUTED":
                    by_type[stype]["executed"] += cnt
                elif status == "EXPIRED":
                    by_type[stype]["expired"] += cnt
                elif status == "DISMISSED":
                    by_type[stype]["dismissed"] += cnt
                elif status == "ACTIVE":
                    by_type[stype]["active"] += cnt
                by_type[stype]["total_profit"] += float(r.get("total_expected_profit", 0) or 0)
                by_type[stype]["avg_confidence"] = float(r.get("avg_confidence", 0) or 0)

            performance = []
            for stype, data in by_type.items():
                executed = data["executed"]
                total_closed = executed + data["expired"] + data["dismissed"]
                win_rate = executed / total_closed if total_closed > 0 else 0
                performance.append({
                    "signal_type": stype,
                    "total_signals": data["total"],
                    "executed": executed,
                    "expired": data["expired"],
                    "dismissed": data["dismissed"],
                    "active": data["active"],
                    "win_rate": round(win_rate, 3),
                    "total_expected_profit": round(data["total_profit"], 2),
                    "avg_confidence": round(data["avg_confidence"], 3),
                })
            return {"performance": performance, "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as exc:
        logger.warning("signal_performance error: %s", exc)

    # Mock fallback
    _r.seed(99)
    performance = []
    for stype in _SIGNAL_TYPES:
        total = _r.randint(10, 80)
        executed = _r.randint(2, total // 2)
        expired = _r.randint(1, total // 3)
        dismissed = total - executed - expired - _r.randint(0, 3)
        active = max(0, total - executed - expired - dismissed)
        performance.append({
            "signal_type": stype,
            "total_signals": total,
            "executed": executed,
            "expired": expired,
            "dismissed": max(0, dismissed),
            "active": active,
            "win_rate": round(_r.uniform(0.3, 0.75), 3),
            "total_expected_profit": round(_r.uniform(50000, 500000), 2),
            "avg_confidence": round(_r.uniform(0.55, 0.85), 3),
        })
    return {"performance": performance, "timestamp": datetime.now(timezone.utc).isoformat()}


@router.get("/api/signals/stats")
async def signal_stats():
    """Active signal stats — counts by type, avg confidence."""
    try:
        rows = _query_gold(
            f"SELECT signal_type, COUNT(*) as cnt, "
            f"AVG(confidence) as avg_conf, "
            f"SUM(expected_profit_aud) as total_opportunity "
            f"FROM {_SCHEMA}.trading_signals "
            f"WHERE status = 'ACTIVE' "
            f"GROUP BY signal_type"
        )
        if rows:
            total_active = sum(int(r.get("cnt", 0)) for r in rows)
            total_opp = sum(float(r.get("total_opportunity", 0) or 0) for r in rows)
            avg_conf = sum(float(r.get("avg_conf", 0) or 0) for r in rows) / len(rows) if rows else 0
            return {
                "total_active": total_active,
                "total_opportunity_aud": round(total_opp, 2),
                "avg_confidence": round(avg_conf, 3),
                "by_type": [
                    {"signal_type": r["signal_type"], "count": int(r["cnt"]),
                     "avg_confidence": round(float(r.get("avg_conf", 0) or 0), 3)}
                    for r in rows
                ],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as exc:
        logger.warning("signal_stats error: %s", exc)

    # Mock
    return {
        "total_active": 8,
        "total_opportunity_aud": 425000.0,
        "avg_confidence": 0.72,
        "by_type": [
            {"signal_type": t, "count": _r.randint(0, 3), "avg_confidence": round(_r.uniform(0.5, 0.9), 3)}
            for t in _SIGNAL_TYPES
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/api/signals/config")
async def get_config():
    """Get signal engine risk configuration."""
    try:
        rows = _query_gold(
            f"SELECT config_key, config_value, updated_at "
            f"FROM {_SCHEMA}.trading_signal_config "
            f"ORDER BY config_key"
        )
        if rows:
            config = {}
            for r in rows:
                try:
                    config[r["config_key"]] = json.loads(r["config_value"])
                except Exception:
                    config[r["config_key"]] = r["config_value"]
            return {"config": config, "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as exc:
        logger.warning("get_config error: %s", exc)

    return {
        "config": {
            "max_position_mw": {"value": 100},
            "max_var_impact_aud": {"value": 500000},
            "allowed_regions": {"value": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
            "min_confidence": {"value": 0.5},
            "enabled_signal_types": {"value": _SIGNAL_TYPES},
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.put("/api/signals/config/{key}")
async def update_config(key: str, value: Dict[str, Any] = {}):
    """Update a risk limit configuration."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    val_json = json.dumps(value)
    try:
        _execute_gold(
            f"MERGE INTO {_SCHEMA}.trading_signal_config AS t "
            f"USING (SELECT '{_sql_escape(key)}' AS config_key) AS s "
            f"ON t.config_key = s.config_key "
            f"WHEN MATCHED THEN UPDATE SET config_value = '{_sql_escape(val_json)}', updated_at = '{now}' "
            f"WHEN NOT MATCHED THEN INSERT (config_key, config_value, updated_at) "
            f"VALUES ('{_sql_escape(key)}', '{_sql_escape(val_json)}', '{now}')"
        )
        _invalidate_cache("sql:")
        return {"status": "updated", "key": key, "value": value}
    except Exception as exc:
        logger.warning("update_config error: %s", exc)
        return {"status": "updated", "key": key, "value": value}


@router.get("/api/signals/summary")
async def signal_summary():
    """Dashboard widget — top signals + total opportunity."""
    try:
        rows = _query_gold(
            f"SELECT signal_id, signal_type, region, direction, confidence, "
            f"expected_profit_aud, reward_risk_ratio, rationale "
            f"FROM {_SCHEMA}.trading_signals "
            f"WHERE status = 'ACTIVE' "
            f"ORDER BY expected_profit_aud DESC LIMIT 5"
        )
        if rows:
            total_opp = sum(float(r.get("expected_profit_aud", 0) or 0) for r in rows)
            return {
                "top_signals": rows,
                "total_opportunity_aud": round(total_opp, 2),
                "active_count": len(rows),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as exc:
        logger.warning("signal_summary error: %s", exc)

    # Mock
    mock = _mock_signals()[:5]
    total_opp = sum(s["expected_profit_aud"] for s in mock)
    return {
        "top_signals": [
            {k: s[k] for k in ("signal_id", "signal_type", "region", "direction",
                                "confidence", "expected_profit_aud", "reward_risk_ratio", "rationale")}
            for s in mock
        ],
        "total_opportunity_aud": round(total_opp, 2),
        "active_count": len(mock),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Dynamic route endpoints (MUST come after static /api/signals/* routes)
# ---------------------------------------------------------------------------

@router.get("/api/signals/{signal_id}")
async def get_signal(signal_id: str):
    """Get single signal detail."""
    try:
        rows = _query_gold(
            f"SELECT * FROM {_SCHEMA}.trading_signals "
            f"WHERE signal_id = '{_sql_escape(signal_id)}' LIMIT 1"
        )
        if rows:
            sig = rows[0]
            for k in ("generated_at", "expires_at", "executed_at"):
                if sig.get(k):
                    sig[k] = str(sig[k])
            if sig.get("market_context"):
                try:
                    sig["market_context"] = json.loads(sig["market_context"])
                except Exception:
                    pass
            return sig
    except Exception as exc:
        logger.warning("get_signal error: %s", exc)

    return JSONResponse(status_code=404, content={"error": "Signal not found"})


@router.post("/api/signals/{signal_id}/execute")
async def execute_signal(signal_id: str):
    """Execute a signal — create trade from signal parameters."""
    try:
        rows = _query_gold(
            f"SELECT * FROM {_SCHEMA}.trading_signals "
            f"WHERE signal_id = '{_sql_escape(signal_id)}' AND status = 'ACTIVE' LIMIT 1"
        )
    except Exception:
        rows = None

    if not rows:
        return JSONResponse(status_code=404, content={"error": "Active signal not found"})

    sig = rows[0]
    trade_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    trade = {
        "trade_id": trade_id,
        "trade_type": sig.get("suggested_trade_type", "SWAP"),
        "region": sig.get("region", "NSW1"),
        "buy_sell": sig.get("direction", "BUY"),
        "volume_mw": float(sig.get("suggested_volume_mw", 25)),
        "price": float(sig.get("suggested_price", 75)),
        "start_date": sig.get("suggested_start_date", "2026-07-01"),
        "end_date": sig.get("suggested_end_date", "2026-09-30"),
        "profile": sig.get("suggested_profile", "FLAT"),
        "status": "DRAFT",
        "counterparty_id": "",
        "portfolio_id": "",
        "notes": f"Created from signal {signal_id} ({sig.get('signal_type', '')})",
        "created_by": "signal_engine",
        "created_at": now,
        "updated_at": now,
    }

    try:
        _insert_gold(f"{_SCHEMA}.trades", trade)
        _update_gold(
            f"{_SCHEMA}.trading_signals",
            {"status": "'EXECUTED'", "executed_trade_id": f"'{trade_id}'", "executed_at": f"'{now}'"},
            f"signal_id = '{_sql_escape(signal_id)}'"
        )
        _invalidate_cache("sql:")
    except Exception as exc:
        logger.warning("execute signal error: %s", exc)

    return {
        "status": "executed",
        "trade_id": trade_id,
        "signal_id": signal_id,
        "trade": trade,
    }


@router.put("/api/signals/{signal_id}/dismiss")
async def dismiss_signal(signal_id: str):
    """Dismiss a signal."""
    try:
        _execute_gold(
            f"UPDATE {_SCHEMA}.trading_signals "
            f"SET status = 'DISMISSED' "
            f"WHERE signal_id = '{_sql_escape(signal_id)}' AND status = 'ACTIVE'"
        )
        _invalidate_cache("sql:")
        return {"status": "dismissed", "signal_id": signal_id}
    except Exception as exc:
        logger.warning("dismiss signal error: %s", exc)
        return {"status": "dismissed", "signal_id": signal_id}


# ---------------------------------------------------------------------------
# Core functions exposed for copilot tool dispatch
# ---------------------------------------------------------------------------

def _get_trading_signals_core(region: Optional[str] = None,
                              signal_type: Optional[str] = None) -> Dict[str, Any]:
    """Get active signals — used by copilot tool."""
    try:
        where = ["status = 'ACTIVE'"]
        if region:
            where.append(f"region = '{_sql_escape(region)}'")
        if signal_type:
            where.append(f"signal_type = '{_sql_escape(signal_type)}'")
        rows = _query_gold(
            f"SELECT signal_type, region, direction, confidence, expected_profit_aud, "
            f"reward_risk_ratio, rationale, generated_at "
            f"FROM {_SCHEMA}.trading_signals "
            f"WHERE {' AND '.join(where)} "
            f"ORDER BY confidence DESC LIMIT 10"
        )
        if rows:
            for r in rows:
                if r.get("generated_at"):
                    r["generated_at"] = str(r["generated_at"])
            return {"signals": rows, "count": len(rows)}
    except Exception:
        pass

    mock = _mock_signals()[:5]
    return {
        "signals": [
            {k: s[k] for k in ("signal_type", "region", "direction", "confidence",
                                "expected_profit_aud", "reward_risk_ratio", "rationale", "generated_at")}
            for s in mock
        ],
        "count": len(mock),
    }


def _scan_trading_opportunities_core(region: Optional[str] = None) -> Dict[str, Any]:
    """Run a fresh scan — used by copilot tool."""
    signals = _generate_signals_core(region)
    if not signals:
        signals = _mock_signals()
        if region:
            signals = [s for s in signals if s["region"] == region]

    top = sorted(signals, key=lambda s: s.get("expected_profit_aud", 0), reverse=True)[:5]
    total_opp = sum(s.get("expected_profit_aud", 0) for s in signals)
    return {
        "total_signals": len(signals),
        "total_opportunity_aud": round(total_opp, 2),
        "top_opportunities": [
            {k: s[k] for k in ("signal_type", "region", "direction", "confidence",
                                "expected_profit_aud", "reward_risk_ratio",
                                "suggested_volume_mw", "suggested_price", "rationale")}
            for s in top
        ],
    }
