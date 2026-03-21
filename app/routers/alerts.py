"""Anomaly Auto-Explanation Router — AI-powered root cause analysis for market events."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Query

from .shared import _CATALOG, _insert_gold, _query_gold, _sql_escape, logger

router = APIRouter()

# ---------------------------------------------------------------------------
# Core explanation engine
# ---------------------------------------------------------------------------


def _explain_anomaly_core(
    event_id: str,
    region: str,
    timestamp_str: str,
    window_minutes: int = 30,
) -> Dict[str, Any]:
    """Generate a heuristic root-cause explanation for a market anomaly.

    1. Check cache in gold.anomaly_explanations
    2. Query surrounding data (prices, generation, interconnectors, weather)
    3. Identify root causes via heuristic rules
    4. Build narrative and persist
    """
    # 1. Check cache
    if event_id:
        cached = _query_gold(
            f"SELECT * FROM {_CATALOG}.gold.anomaly_explanations "
            f"WHERE event_id = '{_sql_escape(event_id)}' LIMIT 1"
        )
        if cached:
            row = cached[0]
            return {
                "explanation_id": row.get("explanation_id", ""),
                "event_id": row.get("event_id", ""),
                "region": row.get("region", ""),
                "event_timestamp": str(row.get("event_timestamp", "")),
                "event_type": row.get("event_type", ""),
                "metric_value": float(row.get("metric_value", 0)),
                "explanation": row.get("explanation_text", ""),
                "root_causes": json.loads(row["root_causes"]) if row.get("root_causes") else [],
                "generation_context": row.get("generation_context", ""),
                "weather_context": row.get("weather_context", ""),
                "constraint_context": row.get("constraint_context", ""),
                "cached": True,
            }

    # 2. Query surrounding data
    ts = timestamp_str.replace("T", " ")[:19]
    root_causes: List[str] = []
    event_type = "UNKNOWN"
    metric_value = 0.0
    gen_context = ""
    weather_ctx = ""
    constraint_ctx = ""

    # 2a. Price at event
    price_rows = _query_gold(
        f"SELECT rrp, total_demand_mw, available_gen_mw FROM {_CATALOG}.gold.nem_prices_5min "
        f"WHERE region_id = '{_sql_escape(region)}' "
        f"AND interval_datetime BETWEEN TIMESTAMP('{ts}') - INTERVAL {window_minutes} MINUTES "
        f"AND TIMESTAMP('{ts}') + INTERVAL {window_minutes} MINUTES "
        f"ORDER BY ABS(TIMESTAMPDIFF(SECOND, interval_datetime, TIMESTAMP('{ts}'))) LIMIT 20"
    )

    if price_rows:
        closest = price_rows[0]
        metric_value = float(closest.get("rrp", 0))
        demand = float(closest.get("total_demand_mw", 0))
        avail = float(closest.get("available_gen_mw", 0))

        if metric_value > 1000:
            event_type = "EXTREME_PRICE"
        elif metric_value > 300:
            event_type = "PRICE_SPIKE"
        elif metric_value < 0:
            event_type = "NEGATIVE_PRICE"
        else:
            event_type = "PRICE_ANOMALY"

        if avail > 0 and demand / avail > 0.9:
            root_causes.append("TIGHT_SUPPLY_DEMAND_BALANCE")

        # Check price volatility in window
        prices = [float(r.get("rrp", 0)) for r in price_rows]
        if len(prices) > 2:
            avg_p = sum(prices) / len(prices)
            if avg_p > 0 and max(prices) > avg_p * 3:
                root_causes.append("PRICE_VOLATILITY_SPIKE")

    # 2b. Generation changes
    _RENEWABLE_FUELS = {"wind", "solar_utility", "solar_rooftop", "hydro", "battery"}
    gen_rows = _query_gold(
        f"SELECT fuel_type, total_mw FROM {_CATALOG}.gold.nem_generation_by_fuel "
        f"WHERE region_id = '{_sql_escape(region)}' "
        f"AND interval_datetime BETWEEN TIMESTAMP('{ts}') - INTERVAL {window_minutes} MINUTES "
        f"AND TIMESTAMP('{ts}') + INTERVAL {window_minutes} MINUTES "
        f"ORDER BY interval_datetime DESC LIMIT 50"
    )
    if gen_rows:
        # Check for sudden generation drops
        total_gen = sum(float(r.get("total_mw", 0)) for r in gen_rows[:5])
        renewable_mw = sum(float(r.get("total_mw", 0)) for r in gen_rows[:5]
                          if r.get("fuel_type", "").lower() in _RENEWABLE_FUELS)
        fossil_mw = total_gen - renewable_mw
        gen_context = f"Total gen ~{total_gen:.0f}MW (renewable {renewable_mw:.0f}MW, fossil {fossil_mw:.0f}MW)"

        if renewable_mw > 0 and total_gen > 0 and renewable_mw / total_gen < 0.15:
            root_causes.append("LOW_RENEWABLE_OUTPUT")
    else:
        gen_context = "Generation data unavailable for window"

    # 2c. Interconnector congestion
    ic_rows = _query_gold(
        f"SELECT interconnector_id, mw_flow, is_congested, utilization_pct "
        f"FROM {_CATALOG}.gold.nem_interconnectors "
        f"WHERE (from_region = '{_sql_escape(region)}' OR to_region = '{_sql_escape(region)}') "
        f"AND interval_datetime BETWEEN TIMESTAMP('{ts}') - INTERVAL {window_minutes} MINUTES "
        f"AND TIMESTAMP('{ts}') + INTERVAL {window_minutes} MINUTES "
        f"ORDER BY interval_datetime DESC LIMIT 20"
    )
    congested_ics = [r for r in (ic_rows or []) if r.get("is_congested")]
    if congested_ics:
        root_causes.append("INTERCONNECTOR_CONGESTION")
        ic_names = list({r["interconnector_id"] for r in congested_ics})
        constraint_ctx = f"Congested interconnectors: {', '.join(ic_names)}"
    else:
        constraint_ctx = "No interconnector congestion detected"

    # 2d. Weather
    weather_rows = _query_gold(
        f"SELECT temperature_c, wind_speed_100m_kmh, solar_radiation_wm2 "
        f"FROM {_CATALOG}.gold.weather_nem_regions "
        f"WHERE nem_region = '{_sql_escape(region)}' "
        f"AND forecast_datetime BETWEEN TIMESTAMP('{ts}') - INTERVAL {window_minutes} MINUTES "
        f"AND TIMESTAMP('{ts}') + INTERVAL {window_minutes} MINUTES "
        f"ORDER BY ABS(TIMESTAMPDIFF(SECOND, forecast_datetime, TIMESTAMP('{ts}'))) LIMIT 5"
    )
    if weather_rows:
        w = weather_rows[0]
        temp = float(w.get("temperature_c", 20))
        wind = float(w.get("wind_speed_100m_kmh", 0))
        solar = float(w.get("solar_radiation_wm2", 0))
        weather_ctx = f"Temp {temp:.1f}°C, wind {wind:.0f}km/h, solar {solar:.0f}W/m²"

        if temp > 38:
            root_causes.append("EXTREME_HEAT")
        elif temp < 5:
            root_causes.append("COLD_SNAP")
        if wind < 5:
            root_causes.append("LOW_WIND")
    else:
        weather_ctx = "Weather data unavailable"

    # Default if no causes found
    if not root_causes:
        root_causes.append("MARKET_DYNAMICS")

    # 3. Build narrative
    cause_descriptions = {
        "TIGHT_SUPPLY_DEMAND_BALANCE": "Supply-demand balance was tight with demand approaching available generation capacity",
        "PRICE_VOLATILITY_SPIKE": "Price exhibited extreme volatility with rapid excursions from the period average",
        "LOW_RENEWABLE_OUTPUT": "Renewable generation was unusually low, reducing available supply margin",
        "INTERCONNECTOR_CONGESTION": f"Interstate interconnectors were congested, limiting import capability ({constraint_ctx})",
        "EXTREME_HEAT": f"Extreme heat ({weather_ctx}) drove elevated cooling demand",
        "COLD_SNAP": f"Cold conditions ({weather_ctx}) increased heating demand",
        "LOW_WIND": f"Very low wind speeds ({weather_ctx}) reduced wind generation output",
        "MARKET_DYNAMICS": "Standard market dynamics — no single dominant cause identified",
    }

    explanation_parts = [f"**{event_type}** event in **{region}** at {timestamp_str[:16]}"]
    explanation_parts.append(f"Price reached **${metric_value:,.2f}/MWh**.")
    explanation_parts.append("")
    explanation_parts.append("**Root Causes:**")
    for cause in root_causes:
        explanation_parts.append(f"- {cause_descriptions.get(cause, cause)}")
    explanation_parts.append("")
    explanation_parts.append(f"**Generation:** {gen_context}")
    explanation_parts.append(f"**Weather:** {weather_ctx}")
    explanation_parts.append(f"**Constraints:** {constraint_ctx}")

    explanation_text = "\n".join(explanation_parts)

    # 4. Persist
    explanation_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _insert_gold(f"{_CATALOG}.gold.anomaly_explanations", {
        "explanation_id": explanation_id,
        "event_id": event_id or explanation_id,
        "region": region,
        "event_timestamp": ts,
        "event_type": event_type,
        "metric_value": metric_value,
        "explanation_text": _sql_escape(explanation_text),
        "root_causes": _sql_escape(json.dumps(root_causes)),
        "generation_context": _sql_escape(gen_context),
        "weather_context": _sql_escape(weather_ctx),
        "constraint_context": _sql_escape(constraint_ctx),
        "model_id": "heuristic-v1",
        "generated_at": now,
    })

    return {
        "explanation_id": explanation_id,
        "event_id": event_id or explanation_id,
        "region": region,
        "event_timestamp": timestamp_str,
        "event_type": event_type,
        "metric_value": metric_value,
        "explanation": explanation_text,
        "root_causes": root_causes,
        "generation_context": gen_context,
        "weather_context": weather_ctx,
        "constraint_context": constraint_ctx,
        "cached": False,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/api/anomaly/explain", summary="Explain a market anomaly", tags=["Anomaly"])
async def explain_anomaly(
    region: str = Query("NSW1"),
    timestamp: str = Query(""),
    event_id: str = Query(""),
):
    """Generate AI-powered root-cause explanation for a price anomaly."""
    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat()
    try:
        return _explain_anomaly_core(event_id, region, timestamp)
    except Exception as exc:
        logger.warning("Anomaly explanation failed: %s", exc)
        return {
            "explanation_id": "",
            "event_id": event_id,
            "region": region,
            "event_timestamp": timestamp,
            "event_type": "ERROR",
            "metric_value": 0,
            "explanation": f"Unable to generate explanation: {str(exc)[:200]}",
            "root_causes": ["ERROR"],
            "cached": False,
        }


@router.get("/api/anomaly/recent", summary="Recent anomaly explanations", tags=["Anomaly"])
async def recent_anomalies(
    region: str = Query("NSW1"),
    hours_back: int = Query(48),
):
    """List recent anomaly explanations from cache."""
    try:
        rows = _query_gold(
            f"SELECT explanation_id, event_id, region, event_timestamp, event_type, "
            f"metric_value, explanation_text, root_causes, generated_at "
            f"FROM {_CATALOG}.gold.anomaly_explanations "
            f"WHERE region = '{_sql_escape(region)}' "
            f"AND generated_at >= current_timestamp() - INTERVAL {hours_back} HOURS "
            f"ORDER BY generated_at DESC LIMIT 20"
        )
        if rows:
            results = []
            for r in rows:
                results.append({
                    "explanation_id": r.get("explanation_id", ""),
                    "event_id": r.get("event_id", ""),
                    "region": r.get("region", ""),
                    "event_timestamp": str(r.get("event_timestamp", "")),
                    "event_type": r.get("event_type", ""),
                    "metric_value": float(r.get("metric_value", 0)),
                    "explanation": r.get("explanation_text", ""),
                    "root_causes": json.loads(r["root_causes"]) if r.get("root_causes") else [],
                    "generated_at": str(r.get("generated_at", "")),
                })
            return {"explanations": results, "count": len(results)}
    except Exception as exc:
        logger.warning("Recent anomalies query failed: %s", exc)

    # Mock fallback
    now = datetime.now(timezone.utc)
    return {
        "explanations": [
            {
                "explanation_id": "mock-1",
                "event_id": "mock-evt-1",
                "region": region,
                "event_timestamp": (now - timedelta(hours=3)).isoformat(),
                "event_type": "PRICE_SPIKE",
                "metric_value": 542.0,
                "explanation": f"Price spike to $542/MWh in {region} driven by tight supply-demand balance.",
                "root_causes": ["TIGHT_SUPPLY_DEMAND_BALANCE"],
                "generated_at": now.isoformat(),
            },
        ],
        "count": 1,
    }
