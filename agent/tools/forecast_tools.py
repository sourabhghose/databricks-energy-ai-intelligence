"""
agent/tools/forecast_tools.py
===============================
Python stubs for the 3 forecast tools used by the AUS Energy Copilot agent.

These tools query the gold.price_forecasts and gold.demand_forecasts tables
in the Databricks Unity Catalog, and call the BOM weather API respectively.
"""

from __future__ import annotations

import logging
import os

from databricks import sql as dbsql
from langchain_core.tools import tool

logger = logging.getLogger(__name__)

_CATALOG = os.getenv("DATABRICKS_CATALOG", "energy_copilot")


def _connection():
    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def _query(sql: str) -> list[dict]:
    conn = _connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 1: get_price_forecast
# ---------------------------------------------------------------------------

@tool
def get_price_forecast(region: str, horizon: str) -> list:
    """
    Retrieve ML model price forecasts for a NEM region and forecast horizon.

    Forecasts are produced by an XGBoost model trained on historical NEM data,
    weather features, and scheduled outage information. They are refreshed every
    30 minutes and stored in the gold layer.

    Args:
        region:  NEM region code (NSW1, QLD1, VIC1, SA1, TAS1).
        horizon: Forecast horizon. One of:
                   "30min" — next 6 dispatch intervals (30 min ahead)
                   "1h"    — next 12 intervals (1 hour ahead)
                   "4h"    — next 48 intervals (4 hours ahead)
                   "24h"   — next 288 intervals (day-ahead)
                   "7d"    — next 2016 intervals (week-ahead, lower accuracy)

    Returns:
        A list of forecast records ordered by forecast_time, each containing:
          - "forecast_time"     (str)   ISO-8601 target dispatch interval timestamp
          - "horizon_minutes"   (int)   Minutes from model run to this interval
          - "predicted_rrp"     (float) Point forecast RRP in AUD/MWh
          - "lower_bound"       (float) 10th percentile (P10) forecast
          - "upper_bound"       (float) 90th percentile (P90) forecast
          - "model_version"     (str)   MLflow run ID of the model that produced this forecast
          - "model_run_at"      (str)   ISO-8601 timestamp when model was last run

    Note:
        Always caveat forecasts as model outputs. Price spikes (>$500/MWh) are
        difficult to predict more than 30 minutes ahead due to their stochastic nature.

    Example:
        >>> get_price_forecast("NSW1", "4h")
        [{"forecast_time": "2026-02-19T11:00:00", "predicted_rrp": 95.2,
          "lower_bound": 62.1, "upper_bound": 187.4, ...}, ...]
    """
    valid_regions = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}
    valid_horizons = {"30min", "1h", "4h", "24h", "7d"}
    if region not in valid_regions:
        raise ValueError(f"Invalid region '{region}'. Must be one of {valid_regions}.")
    if horizon not in valid_horizons:
        raise ValueError(f"Invalid horizon '{horizon}'. Must be one of {valid_horizons}.")

    rows = _query(
        f"""
        SELECT forecast_time,
               horizon_minutes,
               predicted_rrp,
               lower_bound,
               upper_bound,
               model_version,
               model_run_at
        FROM {_CATALOG}.gold.price_forecasts
        WHERE regionid = '{region}'
          AND horizon = '{horizon}'
          AND model_run_at = (
              SELECT MAX(model_run_at)
              FROM {_CATALOG}.gold.price_forecasts
              WHERE regionid = '{region}' AND horizon = '{horizon}'
          )
        ORDER BY forecast_time
        """
    )
    return rows


# ---------------------------------------------------------------------------
# Tool 2: get_demand_forecast
# ---------------------------------------------------------------------------

@tool
def get_demand_forecast(region: str, horizon: str) -> list:
    """
    Retrieve demand (operational consumption) forecasts for a NEM region.

    Demand forecasts are produced by AEMO's STPASA and MTPASA processes as
    ingested into the gold layer, supplemented by an internal ML model for
    short-term horizons.

    Args:
        region:  NEM region code (NSW1, QLD1, VIC1, SA1, TAS1).
        horizon: Forecast horizon. One of:
                   "30min" — 30 minutes ahead
                   "1h"    — 1 hour ahead
                   "4h"    — 4 hours ahead
                   "24h"   — Day-ahead (STPASA)
                   "7d"    — 7-day ahead (MTPASA weekly)

    Returns:
        A list of demand forecast records ordered by forecast_time, each
        containing:
          - "forecast_time"     (str)   ISO-8601 target interval timestamp
          - "horizon_minutes"   (int)   Minutes from forecast run to interval
          - "predicted_demand_mw" (float) Point forecast in MW
          - "low_reserve_mw"    (float) Low-probability reserve (LOR1 threshold)
          - "high_reserve_mw"   (float) High-probability reserve (LOR3 threshold)
          - "source"            (str)   "STPASA", "MTPASA", or "ML_MODEL"
          - "run_datetime"      (str)   ISO-8601 timestamp of the forecast run

    Note:
        Demand forecasts are generally more accurate than price forecasts.
        Demand above LOR2 threshold may trigger AEMO intervention notices.

    Example:
        >>> get_demand_forecast("QLD1", "24h")
        [{"forecast_time": "2026-02-20T00:00:00", "predicted_demand_mw": 7820.5,
          "source": "STPASA", ...}, ...]
    """
    valid_regions = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}
    valid_horizons = {"30min", "1h", "4h", "24h", "7d"}
    if region not in valid_regions:
        raise ValueError(f"Invalid region '{region}'. Must be one of {valid_regions}.")
    if horizon not in valid_horizons:
        raise ValueError(f"Invalid horizon '{horizon}'. Must be one of {valid_horizons}.")

    rows = _query(
        f"""
        SELECT forecast_time,
               horizon_minutes,
               predicted_demand_mw,
               low_reserve_mw,
               high_reserve_mw,
               source,
               run_datetime
        FROM {_CATALOG}.gold.demand_forecasts
        WHERE regionid = '{region}'
          AND horizon = '{horizon}'
          AND run_datetime = (
              SELECT MAX(run_datetime)
              FROM {_CATALOG}.gold.demand_forecasts
              WHERE regionid = '{region}' AND horizon = '{horizon}'
          )
        ORDER BY forecast_time
        """
    )
    return rows


# ---------------------------------------------------------------------------
# Tool 3: get_weather_forecast
# ---------------------------------------------------------------------------

@tool
def get_weather_forecast(region: str, hours_ahead: int) -> dict:
    """
    Retrieve weather forecast data for the primary weather station associated
    with a NEM region.

    Weather data drives rooftop solar output, air-conditioner demand, and wind
    generation forecasts. Bureau of Meteorology (BOM) gridded forecast data is
    ingested into the platform and pre-joined to NEM regions.

    Region-to-BOM station mapping:
      NSW1 -> Sydney Observatory Hill (066062) + weighted average for region
      QLD1 -> Brisbane Airport (040842)
      VIC1 -> Melbourne Airport (086282)
      SA1  -> Adelaide Airport (023034)
      TAS1 -> Hobart Airport (094008)

    Args:
        region:      NEM region code (NSW1, QLD1, VIC1, SA1, TAS1).
        hours_ahead: Number of hours of forecast to retrieve (1–168).
                     Maximum 168 hours (7 days). Typical use: 24 or 48.

    Returns:
        A dict containing:
          - "region"         (str)  Requested region
          - "bom_station_id" (str)  Primary BOM station ID
          - "forecast_run"   (str)  ISO-8601 timestamp of BOM forecast run
          - "forecasts"      (list) Hourly records:
              - "valid_time"        (str)   ISO-8601 forecast valid time (AEST)
              - "temp_c"            (float) Air temperature in Celsius
              - "apparent_temp_c"   (float) Feels-like temperature
              - "wind_speed_kph"    (float) Wind speed at 10m hub height equivalent
              - "wind_dir_deg"      (int)   Wind direction in degrees (0=N)
              - "solar_exposure_mj" (float) Expected solar exposure in MJ/m²
              - "precip_mm"         (float) Precipitation in mm
              - "cloud_oktas"       (int)   Cloud cover in oktas (0-8)

    Example:
        >>> get_weather_forecast("SA1", 24)
        {"region": "SA1", "bom_station_id": "023034",
         "forecasts": [{"valid_time": "2026-02-19T11:00:00", "temp_c": 31.2,
                        "wind_speed_kph": 18.4, "solar_exposure_mj": 2.1, ...}, ...]}
    """
    valid_regions = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}
    if region not in valid_regions:
        raise ValueError(f"Invalid region '{region}'. Must be one of {valid_regions}.")
    if not (1 <= hours_ahead <= 168):
        raise ValueError(f"hours_ahead must be between 1 and 168, got {hours_ahead}.")

    rows = _query(
        f"""
        SELECT w.valid_time,
               w.temp_c,
               w.apparent_temp_c,
               w.wind_speed_kph,
               w.wind_dir_deg,
               w.solar_exposure_mj,
               w.precip_mm,
               w.cloud_oktas,
               m.bom_station_id,
               m.forecast_run_at AS forecast_run
        FROM {_CATALOG}.gold.weather_forecasts w
        JOIN {_CATALOG}.gold.weather_station_region_map m
          ON w.station_id = m.station_id
        WHERE m.regionid = '{region}'
          AND m.is_primary = TRUE
          AND w.valid_time BETWEEN CURRENT_TIMESTAMP
              AND CURRENT_TIMESTAMP + INTERVAL {hours_ahead} HOURS
        ORDER BY w.valid_time
        """
    )

    bom_station_id = rows[0]["bom_station_id"] if rows else "unknown"
    forecast_run = rows[0]["forecast_run"] if rows else None

    return {
        "region": region,
        "bom_station_id": bom_station_id,
        "forecast_run": str(forecast_run) if forecast_run else None,
        "forecasts": rows,
    }
