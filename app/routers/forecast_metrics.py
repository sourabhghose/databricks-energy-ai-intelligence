"""Real forecast-accuracy queries for the forecasting dashboards.

All numbers come from tables written by `models/nem_forecast/train.py` (held-out back-test on real AEMO
NEMWEB data) and from `gold.nem_prices_5min`. Nothing here is simulated: when the tables are empty the
endpoints return HTTP 503 instead of illustrative numbers.
"""
from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from .shared import _CATALOG, _query_gold, logger

GOLD = f"{_CATALOG}.gold"
HORIZON_LABEL = {6: "30min", 12: "1h", 48: "4h"}
HORIZON_MIN = {6: 30, 12: 60, 48: 240}
AEST = "'Australia/Brisbane'"
SEASON_SQL = (f"CASE WHEN month(from_utc_timestamp(interval_datetime, {AEST})) IN (12,1,2) THEN 'Summer' "
              f"WHEN month(from_utc_timestamp(interval_datetime, {AEST})) IN (3,4,5) THEN 'Autumn' "
              f"WHEN month(from_utc_timestamp(interval_datetime, {AEST})) IN (6,7,8) THEN 'Winter' ELSE 'Spring' END")
SOURCE_NOTE = ("Held-out back-test of LightGBM models trained on real AEMO NEMWEB data "
               "(models/nem_forecast/train.py); persistence = latest observed value.")


def rows(sql: str, what: str) -> list[dict[str, Any]]:
    """Run a query; raise 503 (never fabricate) when the real table is missing or empty."""
    result = _query_gold(sql)
    if not result:
        logger.warning("forecast metrics unavailable: %s", what)
        raise HTTPException(status_code=503, detail=f"{what} unavailable — run the NEM forecast training job")
    return result


def model_metrics(target: str) -> list[dict[str, Any]]:
    return rows(f"""
        SELECT * FROM {GOLD}.forecast_model_metrics
        WHERE target = '{target}' AND trained_at = (SELECT max(trained_at) FROM {GOLD}.forecast_model_metrics)
        ORDER BY region, horizon_intervals
    """, f"{target} model metrics")


def _error_aggregates(mape_floor: float) -> str:
    return f"""
        avg(actual) AS avg_actual, avg(predicted) AS avg_forecast,
        avg(abs(predicted - actual)) AS mae, sqrt(avg(pow(predicted - actual, 2))) AS rmse,
        avg(predicted - actual) AS bias,
        avg(abs(persistence - actual)) AS persistence_mae,
        avg(CASE WHEN abs(actual) >= {mape_floor} THEN abs(predicted - actual) / abs(actual) END) * 100 AS mape_pct,
        avg(CASE WHEN abs(actual) >= {mape_floor} THEN CAST(abs(predicted - actual) <= 0.1 * abs(actual) AS INT) END)
            * 100 AS within_10pct,
        avg(CASE WHEN actual <> persistence
                 THEN CAST(sign(predicted - persistence) = sign(actual - persistence) AS INT) END) * 100 AS directional_pct,
        percentile_approx(abs(predicted - actual), 0.9) AS p90_abs_error,
        count(*) AS n"""


def backtest_grouped(target: str, groups: list[tuple[str, str]], horizon: int | None = None,
                     extra_where: str = "") -> list[dict]:
    """Back-test errors grouped by (SQL expression, alias) pairs."""
    mape_floor = 20.0 if target == "price" else 1.0
    where = f"target = '{target}'" + (f" AND horizon_intervals = {horizon}" if horizon else "") + extra_where
    select = ", ".join(f"{expr} AS {alias}" for expr, alias in groups)
    keys = ", ".join(str(i + 1) for i in range(len(groups)))
    return rows(f"""
        SELECT {select}, {_error_aggregates(mape_floor)}
        FROM {GOLD}.forecast_backtest WHERE {where}
        GROUP BY {keys} ORDER BY {keys}
    """, f"{target} back-test")


def spike_events(limit: int = 12) -> list[dict]:
    """Largest real price spikes in the test window, what the 1-hour model forecast for them, and the
    energy cost above $300/MWh in that interval (excess price x actual regional demand x 5 minutes)."""
    return rows(f"""
        SELECT b.region_id, b.interval_datetime, b.actual, b.predicted, b.spike_probability, b.spike_flag, b.model_name,
               b.model_version, (b.actual - 300) * g.total_demand_mw / 12 / 1e6 AS cost_above_300_m_aud
        FROM {GOLD}.forecast_backtest b
        JOIN {GOLD}.nem_prices_5min g ON g.region_id = b.region_id AND g.interval_datetime = b.interval_datetime
        WHERE b.target = 'price' AND b.horizon_intervals = 12 AND b.actual > 300
        ORDER BY b.actual DESC LIMIT {limit}
    """, "spike events")


def negative_price_events(limit: int = 6) -> list[dict]:
    return _query_gold(f"""
        SELECT region_id, interval_datetime, actual, predicted, model_name
        FROM {GOLD}.forecast_backtest
        WHERE target = 'price' AND horizon_intervals = 12 AND actual < 0
        ORDER BY actual ASC LIMIT {limit}
    """) or []


def feature_importance(target: str, top_n: int = 10) -> list[dict]:
    return rows(f"""
        SELECT region, feature, importance_share, model_name, model_version,
               row_number() OVER (PARTITION BY region ORDER BY importance_gain DESC) AS rank
        FROM {GOLD}.forecast_feature_importance
        WHERE target = '{target}'
        QUALIFY rank <= {top_n}
        ORDER BY region, rank
    """, f"{target} feature importance")


def spike_calibration() -> list[dict]:
    """Reliability of the spike classifier: predicted probability decile vs observed spike frequency."""
    return _query_gold(f"""
        SELECT region_id, LEAST(CAST(floor(spike_probability * 10) AS INT), 9) + 1 AS decile,
               avg(spike_probability) AS predicted_probability,
               avg(CAST(actual > 300 AS INT)) AS actual_frequency, count(*) AS n
        FROM {GOLD}.forecast_backtest
        WHERE target = 'price' AND horizon_intervals = 12 AND spike_probability IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 2
    """) or []


FEATURE_CATEGORY = (("rrp_lag", "Price history"), ("rrp_mean", "Price history"), ("rrp_std", "Price volatility"),
                    ("rrp_max", "Price volatility"), ("demand", "Demand"), ("supply_margin", "Supply"),
                    ("net_interchange", "Network"), ("spread", "Cross-region"), ("rrp_", "Cross-region"),
                    ("horizon", "Horizon"))


def feature_category(name: str) -> str:
    return next((cat for prefix, cat in FEATURE_CATEGORY if name.startswith(prefix)), "Calendar")


def short_model_id(model_name: str, version: str | int) -> str:
    return f"{model_name.split('.')[-1]}@v{version}"


def daily_spike_flags(horizon: int) -> list[dict]:
    """Per region and market day: did a >$300 interval occur, and did the model flag one (validation-tuned threshold)?"""
    return rows(f"""
        SELECT to_date(from_utc_timestamp(interval_datetime, {AEST})) AS d, region_id,
               max(actual) > 300 AS spike_occurred,
               max(spike_flag) AS spike_predicted
        FROM {GOLD}.forecast_backtest WHERE target = 'price' AND horizon_intervals = {horizon}
        GROUP BY 1, 2
    """, "daily spike flags")


def demand_peak_days(per_region: int = 3) -> list[dict]:
    """Highest-demand market days per region in the test window: actual vs 1-hour-ahead forecast peak."""
    return rows(f"""
        SELECT region_id, d, actual_peak, forecast_peak FROM (
          SELECT region_id, to_date(from_utc_timestamp(interval_datetime, {AEST})) AS d,
                 max(actual) AS actual_peak, max(predicted) AS forecast_peak,
                 row_number() OVER (PARTITION BY region_id ORDER BY max(actual) DESC) AS rn
          FROM {GOLD}.forecast_backtest WHERE target = 'demand' AND horizon_intervals = 12
          GROUP BY 1, 2)
        WHERE rn <= {per_region} ORDER BY region_id, d
    """, "demand peak days")


def performance_drift(target: str, horizon: int = 12, recent_days: int = 3) -> list[dict]:
    """MAE in the most recent days of the held-out window vs the earlier days (concept-drift signal)."""
    return rows(f"""
        WITH b AS (SELECT *, max(interval_datetime) OVER () AS end_ts FROM {GOLD}.forecast_backtest
                   WHERE target = '{target}' AND horizon_intervals = {horizon})
        SELECT region_id, max(model_name) AS model_name, max(model_version) AS model_version,
               avg(CASE WHEN interval_datetime >= end_ts - INTERVAL {recent_days} DAYS
                        THEN abs(predicted - actual) END) AS recent_mae,
               avg(CASE WHEN interval_datetime < end_ts - INTERVAL {recent_days} DAYS
                        THEN abs(predicted - actual) END) AS earlier_mae,
               to_date(max(end_ts)) AS monitor_date
        FROM b GROUP BY 1 ORDER BY 1
    """, f"{target} drift")


def monthly_volatility() -> list[dict]:
    """Monthly price statistics per region from real 5-minute prices."""
    return rows(f"""
        SELECT region_id, date_format(from_utc_timestamp(interval_datetime, {AEST}), 'yyyy-MM') AS month,
               avg(rrp) AS avg_price, stddev(rrp) AS price_std, count_if(rrp > 300) AS spike_count,
               count_if(rrp < 0) / 12.0 AS negative_price_hours,
               count(DISTINCT to_date(from_utc_timestamp(interval_datetime, {AEST}))) AS days
        FROM {GOLD}.nem_prices_5min GROUP BY 1, 2 ORDER BY 1, 2
    """, "monthly price volatility")


def daily_price_extremes() -> list[dict]:
    """Daily max/avg price and energy cost above $300/MWh per region (input to spike-cluster detection)."""
    return rows(f"""
        SELECT region_id, to_date(from_utc_timestamp(interval_datetime, {AEST})) AS d,
               max(rrp) AS max_price, avg(rrp) AS avg_price,
               sum(greatest(rrp - 300, 0) * total_demand_mw / 12) / 1e6 AS cost_above_300_m_aud
        FROM {GOLD}.nem_prices_5min GROUP BY 1, 2 ORDER BY 1, 2
    """, "daily price extremes")


def seasonal_demand_patterns() -> list[dict]:
    """Peak / average / minimum demand and the peak hour per region and season, from real actuals."""
    return rows(f"""
        WITH h AS (
          SELECT region_id, {SEASON_SQL} AS season, hour(from_utc_timestamp(interval_datetime, {AEST})) AS hr,
                 total_demand_mw FROM {GOLD}.nem_prices_5min WHERE total_demand_mw IS NOT NULL),
        by_hour AS (SELECT region_id, season, hr, avg(total_demand_mw) AS hr_avg FROM h GROUP BY 1, 2, 3)
        SELECT s.region_id, s.season, s.peak, s.avg_d, s.min_d,
               max_by(b.hr, b.hr_avg) AS peak_hour
        FROM (SELECT region_id, season, max(total_demand_mw) AS peak, avg(total_demand_mw) AS avg_d,
                     min(total_demand_mw) AS min_d FROM h GROUP BY 1, 2) s
        JOIN by_hour b USING (region_id, season)
        GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2
    """, "seasonal demand patterns")


def latest_backtest_day_hourly() -> list[dict]:
    """Hourly demand forecast vs actual (1-hour horizon) for the most recent complete back-test day."""
    return rows(f"""
        WITH b AS (SELECT *, to_date(from_utc_timestamp(interval_datetime, {AEST})) AS d,
                          hour(from_utc_timestamp(interval_datetime, {AEST})) AS hr
                   FROM {GOLD}.forecast_backtest WHERE target = 'demand' AND horizon_intervals = 12),
        day AS (SELECT max(d) - 1 AS d FROM b)
        SELECT b.region_id, b.d, b.hr, avg(b.predicted) AS forecast, avg(b.actual) AS actual,
               avg(b.lower_80) AS lower_80, avg(b.upper_80) AS upper_80, avg(b.persistence) AS persistence
        FROM b JOIN day USING (d) GROUP BY 1, 2, 3 ORDER BY 1, 3
    """, "latest back-test day")


def training_window_years() -> float:
    r = _query_gold(f"SELECT datediff(max(test_start), min(to_timestamp(train_start))) / 365.0 AS y "
                    f"FROM {GOLD}.forecast_model_metrics") or [{}]
    return round(float(r[0].get("y") or 0), 2)
