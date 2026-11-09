"""Data tools for the AI Market Intelligence assistant.

Each tool runs a validated query against real gold tables and returns JSON with an `as_of` timestamp. Inputs from
the LLM are whitelisted (regions) or clamped (hours) before they reach SQL. A tool whose source is stale returns an
explicit `not_available` error with the last data time, so the model reports unavailability instead of quoting old
or simulated values.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .shared import _CATALOG, _NEM_REGIONS, _query_gold

GOLD = f"{_CATALOG}.gold"
MAX_AGE_HOURS = 2.0


def _region(value: Any, default: str | None = None) -> str | None:
    v = str(value or "").upper().strip()
    return v if v in _NEM_REGIONS else default


def _hours(value: Any, default: int, cap: int) -> int:
    try:
        return max(1, min(int(value), cap))
    except (TypeError, ValueError):
        return default


def _fresh(table: str, ts_col: str) -> tuple[bool, str | None]:
    rows = _query_gold(f"SELECT max({ts_col}) AS ts FROM {GOLD}.{table}") or [{}]
    ts = rows[0].get("ts")
    if not ts:
        return False, None
    last = datetime.fromisoformat(str(ts).replace("Z", "+00:00").replace(" ", "T"))
    last = last if last.tzinfo else last.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).total_seconds() / 3600 <= MAX_AGE_HOURS, str(ts)


def _not_available(what: str, last: str | None) -> str:
    return json.dumps({"error": "not_available", "detail": f"{what} is not ingested in this build "
                       f"(last data: {last or 'none'}). Do not quote values for it."})


def spot_prices(arguments: dict) -> str:
    region, hours = _region(arguments.get("region")), _hours(arguments.get("hours"), 1, 72)
    where = f"AND region_id = '{region}'" if region else ""
    rows = _query_gold(f"""
        WITH w AS (SELECT * FROM {GOLD}.nem_prices_5min
                   WHERE interval_datetime >= (SELECT max(interval_datetime) FROM {GOLD}.nem_prices_5min)
                         - INTERVAL {hours} HOURS {where})
        SELECT region_id, max_by(rrp, interval_datetime) AS latest_rrp, max(interval_datetime) AS as_of_utc,
               round(avg(rrp), 2) AS avg_rrp, round(min(rrp), 2) AS min_rrp, round(max(rrp), 2) AS max_rrp,
               count_if(rrp > 300) AS intervals_over_300, round(max_by(total_demand_mw, interval_datetime)) AS latest_demand_mw
        FROM w GROUP BY region_id ORDER BY region_id
    """)
    if not rows:
        return json.dumps({"error": "No price data available"})
    return json.dumps({"source": "gold.nem_prices_5min (AEMO NEMWEB DispatchIS)", "window_hours": hours,
                       "regions": rows}, default=str)


def _forecasts(table: str, value_col: str, arguments: dict, extra: str = "") -> str:
    region, hours = _region(arguments.get("region"), "NSW1"), _hours(arguments.get("hours"), 4, 4)
    rows = _query_gold(f"""
        WITH latest AS (SELECT max(forecast_run_at) AS run FROM {GOLD}.{table}
                        WHERE region_id = '{region}' AND model_name LIKE '%.ml.nem_%')
        SELECT f.interval_datetime, f.horizon_intervals * 5 AS horizon_minutes, round(f.{value_col}, 2) AS forecast,
               round(f.prediction_lower_80, 2) AS lower_80, round(f.prediction_upper_80, 2) AS upper_80 {extra},
               f.forecast_run_at, f.model_name, f.model_version
        FROM {GOLD}.{table} f JOIN latest ON f.forecast_run_at = latest.run
        WHERE f.region_id = '{region}' AND f.horizon_intervals * 5 <= {hours * 60}
        ORDER BY f.horizon_intervals
    """)
    if not rows:
        return json.dumps({"error": f"No model forecasts for {region}"})
    target = "price" if table == "price_forecasts" else "demand"
    skill = _query_gold(f"""
        SELECT horizon_intervals * 5 AS horizon_minutes, round(mae, 2) AS backtest_mae,
               round(persistence_mae, 2) AS persistence_mae, round(skill_vs_persistence, 3) AS skill_vs_persistence
        FROM {GOLD}.forecast_model_metrics
        WHERE target = '{target}' AND region = '{region}'
          AND trained_at = (SELECT max(trained_at) FROM {GOLD}.forecast_model_metrics)
        ORDER BY horizon_intervals
    """) or []
    return json.dumps({"source": f"gold.{table} (UC model {rows[0]['model_name']} v{rows[0]['model_version']})",
                       "region": region, "forecasts": rows, "held_out_backtest": skill,
                       "note": "Spike probabilities are a weak signal: the spike classifier has low precision on "
                               "held-out data." if target == "price" else ""}, default=str)


def price_forecasts(arguments: dict) -> str:
    return _forecasts("price_forecasts", "predicted_rrp", arguments, ", round(f.spike_probability, 4) AS spike_probability")


def demand_forecasts(arguments: dict) -> str:
    return _forecasts("demand_forecasts", "predicted_demand_mw", arguments)


def generation_mix(arguments: dict) -> str:
    ok, last = _fresh("nem_generation_by_fuel", "interval_datetime")
    return _not_available("Generation mix by fuel", last) if not ok else _latest_generation(_region(arguments.get("region"), "NSW1"))


def _latest_generation(region: str) -> str:
    rows = _query_gold(f"""
        SELECT fuel_type, round(total_mw, 1) AS mw, is_renewable, interval_datetime AS as_of_utc
        FROM {GOLD}.nem_generation_by_fuel
        WHERE region_id = '{region}' AND interval_datetime = (SELECT max(interval_datetime) FROM {GOLD}.nem_generation_by_fuel)
    """) or []
    return json.dumps({"region": region, "fuel_mix": rows}, default=str)


def interconnectors(arguments: dict) -> str:
    ok, last = _fresh("nem_interconnectors", "interval_datetime")
    if not ok:
        return _not_available("Interconnector flows", last)
    rows = _query_gold(f"""
        SELECT interconnector_id, from_region, to_region, mw_flow, export_limit_mw, import_limit_mw, interval_datetime
        FROM {GOLD}.nem_interconnectors
        WHERE interval_datetime = (SELECT max(interval_datetime) FROM {GOLD}.nem_interconnectors)
    """) or []
    return json.dumps({"interconnectors": rows}, default=str)


def weather(arguments: dict) -> str:
    ok, last = _fresh("weather_nem_regions", "forecast_datetime")
    if not ok:
        return _not_available("Weather", last)
    region = _region(arguments.get("region"))
    where = f"AND nem_region = '{region}'" if region else ""
    rows = _query_gold(f"""
        SELECT nem_region, temperature_c, wind_speed_100m_kmh, solar_radiation_wm2, cloud_cover_pct, forecast_datetime
        FROM {GOLD}.weather_nem_regions
        WHERE forecast_datetime = (SELECT max(forecast_datetime) FROM {GOLD}.weather_nem_regions) {where}
    """) or []
    return json.dumps({"weather": rows}, default=str)


TOOLS = {
    "query_spot_prices": spot_prices,
    "query_price_forecasts": price_forecasts,
    "query_demand_forecasts": demand_forecasts,
    "query_generation_mix": generation_mix,
    "query_interconnectors": interconnectors,
    "query_weather": weather,
}


def stale_sources() -> dict[str, str | None]:
    """Sources used by the market context that are too old to present as current (name -> last data time)."""
    checks = {"interconnector flows": ("nem_interconnectors", "interval_datetime"),
              "generation mix": ("nem_generation_by_fuel", "interval_datetime"),
              "weather": ("weather_nem_regions", "forecast_datetime")}
    out = {}
    for name, (table, col) in checks.items():
        ok, last = _fresh(table, col)
        if not ok:
            out[name] = last
    return out
