# Databricks notebook source
# =============================================================================
# 06_register_uc_functions.py
# AUS Energy Copilot -- Register Unity Catalog SQL/Python functions as AI agent tools
#
# Registers all 14 Unity Catalog functions in the energy_copilot.tools schema.
# These functions are the tool-calling interface for the Mosaic AI Copilot agent.
#
# Tool categories (14 total):
#   Market data (6):  get_latest_prices, get_price_history, get_generation_mix,
#                     get_interconnector_flows, get_active_constraints, get_fcas_prices
#   Forecasting (3):  get_price_forecast, get_demand_forecast, get_weather_forecast
#   Analysis (3):     explain_price_event, compare_regions, get_market_summary
#   RAG (1):          search_market_rules
#   Utility (1):      get_generator_info
#
# Prerequisites:
#   - Unity Catalog energy_copilot.tools schema exists (run 01_create_schemas.sql)
#   - energy_copilot.gold tables populated (run 02_create_tables.sql)
#   - Databricks SDK: pip install databricks-sdk>=0.25.0
#   - Caller has CREATE FUNCTION on energy_copilot.tools schema
#
# Each function is registered as a Unity Catalog SQL function.
# The agent references functions as: energy_copilot.tools.<function_name>
# Functions are implemented as SQL queries over Gold tables for maximum
# portability; complex analysis tools fall back to PYTHON language.
# =============================================================================

from __future__ import annotations

import logging

from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

CATALOG = 'energy_copilot'
SCHEMA  = 'tools'
GOLD    = 'energy_copilot.gold'
SILVER  = 'energy_copilot.silver'

w = WorkspaceClient()


# ---------------------------------------------------------------------------
# Helper: create or replace a Unity Catalog function via Databricks SQL
# ---------------------------------------------------------------------------

def run_sql(statement: str) -> None:
    '''Execute a SQL statement on the default SQL warehouse.',
'    Used to CREATE OR REPLACE FUNCTION statements in Unity Catalog.
    '''
    result = w.statement_execution.execute_statement(
        warehouse_id=_get_warehouse_id(),
        statement=statement,
        wait_timeout='120s',
    )
    status = result.status
    if status and status.state and status.state.value not in ('SUCCEEDED',):
        raise RuntimeError(
            f'SQL execution failed: {status.state} -- {getattr(status.error, "message", "")}',
        )
    log.info('SQL executed successfully (%d chars)', len(statement))


def _get_warehouse_id() -> str:
    '''Return an active SQL warehouse ID.',
'    Prefers warehouses named energy_copilot_* for isolation.
    '''
    for wh in w.warehouses.list():
        if wh.name and 'energy_copilot' in wh.name.lower() and wh.state:
            if wh.state.value in ('RUNNING', 'IDLE'):
                return wh.id
    # Fall back to first available warehouse
    warehouses = list(w.warehouses.list())
    if warehouses:
        return warehouses[0].id
    raise RuntimeError('No active SQL warehouse found. Start a warehouse first.')


# ===========================================================================
# MARKET DATA TOOLS (6)
# ===========================================================================

def register_get_latest_prices() -> None:
    '''
    Tool: get_latest_prices
    Returns the most recent spot electricity price for one or all NEM regions.

    Agent use cases:
      - 'What is the current electricity price in NSW?'
      - 'Show me the current price in all regions'

    Returns: TABLE(region_id, rrp, interval_datetime, total_demand_mw,
                   net_interchange_mw, apc_flag)
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_latest_prices(
    region STRING COMMENT 'NEM region code: NSW1, QLD1, VIC1, SA1, TAS1, or NULL for all regions'
)
RETURNS TABLE (
    region_id           STRING  COMMENT 'NEM region identifier',
    rrp                 DOUBLE  COMMENT 'Current Regional Reference Price ($/MWh)',
    interval_datetime   TIMESTAMP COMMENT 'Interval end time (AEST/AEDT) for this price observation',
    total_demand_mw     DOUBLE  COMMENT 'Total scheduled demand in region (MW)',
    net_interchange_mw  DOUBLE  COMMENT 'Net interconnector import into region (MW; positive=importing)',
    apc_flag            BOOLEAN COMMENT 'True if Administered Price Cap is active',
    market_suspended    BOOLEAN COMMENT 'True if NEM market is suspended for this region'
)
COMMENT 'Returns the most recent 5-minute spot electricity price for the specified NEM region
         (or all regions if region is NULL). Data sourced from gold.nem_prices_5min.
         Typical latency from AEMO publish: < 2 minutes.'
RETURN
    SELECT
        p.region_id,
        p.rrp,
        p.interval_datetime,
        p.total_demand_mw,
        p.net_interchange_mw,
        p.apc_flag,
        p.market_suspended
    FROM energy_copilot.gold.nem_prices_5min p
    WHERE p.interval_datetime = (
            SELECT MAX(interval_datetime) FROM energy_copilot.gold.nem_prices_5min)
      AND (region IS NULL OR p.region_id = region)
    ORDER BY p.region_id
'''
    run_sql(sql)
    log.info('Registered: get_latest_prices')


def register_get_price_history() -> None:
    '''
    Tool: get_price_history
    Returns historical spot prices for a region over a date range.

    Agent use cases:
      - 'Show me NSW prices over the last 7 days'
      - 'What was the average price in VIC yesterday?'
      - 'Show me the price chart for SA from Dec 1 to Dec 7'

    Returns: TABLE(interval_datetime, region_id, rrp, total_demand_mw)
    Interval parameter: '5min' | '30min' | '1hr' | 'daily'
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_price_history(
    region    STRING    COMMENT 'NEM region code: NSW1, QLD1, VIC1, SA1, or TAS1',
    start_dt  TIMESTAMP COMMENT 'Start of date range (AEST/AEDT)',
    end_dt    TIMESTAMP COMMENT 'End of date range (AEST/AEDT)',
    interval  STRING    COMMENT 'Resolution: 5min, 30min, 1hr, or daily'
)
RETURNS TABLE (
    interval_datetime   TIMESTAMP COMMENT 'Interval end time at the requested resolution',
    region_id           STRING    COMMENT 'NEM region identifier',
    rrp                 DOUBLE    COMMENT 'Regional Reference Price ($/MWh)',
    rrp_min             DOUBLE    COMMENT 'Minimum price within the aggregation period ($/MWh)',
    rrp_max             DOUBLE    COMMENT 'Maximum price within the aggregation period ($/MWh)',
    total_demand_mw     DOUBLE    COMMENT 'Average demand in region over aggregation period (MW)'
)
COMMENT 'Returns historical NEM spot prices for a region over a specified date range and interval.
         Source: gold.nem_prices_5min (5min) or gold.nem_prices_30min (30min, 1hr, daily).
         Max range: 90 days at 5min resolution; 365 days at daily resolution.'
RETURN
    SELECT
        CASE interval
            WHEN '5min'  THEN p.interval_datetime
            WHEN '30min' THEN DATE_TRUNC('MINUTE', p.interval_datetime -
                              INTERVAL (MINUTE(p.interval_datetime) MOD 30) MINUTES)
            WHEN '1hr'   THEN DATE_TRUNC('HOUR', p.interval_datetime)
            WHEN 'daily' THEN CAST(DATE_TRUNC('DAY', p.interval_datetime) AS TIMESTAMP)
            ELSE p.interval_datetime
        END                    AS interval_datetime,
        p.region_id,
        AVG(p.rrp)             AS rrp,
        MIN(p.rrp)             AS rrp_min,
        MAX(p.rrp)             AS rrp_max,
        AVG(p.total_demand_mw) AS total_demand_mw
    FROM energy_copilot.gold.nem_prices_5min p
    WHERE p.region_id = region
      AND p.interval_datetime BETWEEN start_dt AND end_dt
    GROUP BY 1, p.region_id
    ORDER BY 1
'''
    run_sql(sql)
    log.info('Registered: get_price_history')


def register_get_generation_mix() -> None:
    '''
    Tool: get_generation_mix
    Returns the generation mix by fuel type for a region over a date range.

    Agent use cases:
      - 'What is the current generation mix in QLD?'
      - 'Show me wind vs solar generation in SA this week'
      - 'How much coal is VIC generating today?'

    Returns: TABLE(fuel_type, total_mw, pct_of_total, avg_capacity_factor)
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_generation_mix(
    region    STRING    COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
    start_dt  TIMESTAMP COMMENT 'Start of date range (AEST/AEDT)',
    end_dt    TIMESTAMP COMMENT 'End of date range (AEST/AEDT)'
)
RETURNS TABLE (
    fuel_type              STRING  COMMENT 'Fuel/technology type',
    avg_output_mw          DOUBLE  COMMENT 'Average output for the fuel type over date range (MW)',
    total_energy_gwh       DOUBLE  COMMENT 'Total energy generated over date range (GWh)',
    pct_of_total           DOUBLE  COMMENT 'Share of total regional generation (%)',
    avg_capacity_factor    DOUBLE  COMMENT 'Average capacity factor (0–1)',
    total_emissions_tco2e  DOUBLE  COMMENT 'Total CO2-equivalent emissions (tonnes)'
)
COMMENT 'Returns NEM generation breakdown by fuel type for a region and date range.
         Source: gold.nem_generation_by_fuel.
         Fuel types: coal_black, coal_brown, gas_ccgt, gas_ocgt, gas_steam, hydro,
         wind, solar_utility, solar_rooftop, battery_charging, battery_discharging, other.'
RETURN
    SELECT
        g.fuel_type,
        ROUND(AVG(g.total_mw), 2)              AS avg_output_mw,
        ROUND(SUM(g.total_mw * 5.0 / 60.0 / 1000.0), 3)  AS total_energy_gwh,
        ROUND(AVG(g.total_mw) / NULLIF(
            SUM(AVG(g.total_mw)) OVER (), 0) * 100.0, 2)  AS pct_of_total,
        ROUND(AVG(g.capacity_factor), 4)       AS avg_capacity_factor,
        ROUND(SUM(g.emissions_tco2e), 2)       AS total_emissions_tco2e
    FROM energy_copilot.gold.nem_generation_by_fuel g
    WHERE g.region_id = region
      AND g.interval_datetime BETWEEN start_dt AND end_dt
    GROUP BY g.fuel_type
    ORDER BY avg_output_mw DESC
'''
    run_sql(sql)
    log.info('Registered: get_generation_mix')


def register_get_interconnector_flows() -> None:
    '''
    Tool: get_interconnector_flows
    Returns current or recent interconnector flows and utilisation.

    Agent use cases:
      - 'What is the current Basslink flow?'
      - 'Is Heywood congested right now?'
      - 'Show me all interconnector flows'

    Returns: TABLE(interconnector_id, mw_flow, utilization_pct, is_congested, marginal_value)
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_interconnector_flows(
    interconnector_id STRING COMMENT 'Interconnector ID (e.g. VIC1-TAS1) or NULL for all interconnectors'
)
RETURNS TABLE (
    interconnector_id   STRING    COMMENT 'Interconnector identifier',
    from_region         STRING    COMMENT 'Region exporting in forward direction',
    to_region           STRING    COMMENT 'Region receiving in forward direction',
    mw_flow             DOUBLE    COMMENT 'Current MW flow (positive=forward, negative=reverse)',
    export_limit_mw     DOUBLE    COMMENT 'Forward direction capacity limit (MW)',
    import_limit_mw     DOUBLE    COMMENT 'Reverse direction capacity limit (MW)',
    utilization_pct     DOUBLE    COMMENT 'Utilisation percentage (0–100). Above 95% = congested.',
    is_congested        BOOLEAN   COMMENT 'True if interconnector is congested (>95% utilised)',
    marginal_value      DOUBLE    COMMENT 'Shadow price of congestion ($/MWh). Non-zero when congested.',
    interval_datetime   TIMESTAMP COMMENT 'Interval time for this reading (AEST/AEDT)'
)
COMMENT 'Returns the latest interconnector flow data for the specified interconnector (or all).
         Source: gold.nem_interconnectors.
         Key interconnectors: NSW1-QLD1 (QNI), VIC1-NSW1 (VNI), VIC1-SA1 (Heywood),
         VIC1-TAS1 (Basslink), SA1-VIC1 (Murraylink).'
RETURN
    SELECT
        ic.interconnector_id,
        ic.from_region,
        ic.to_region,
        ROUND(ic.mw_flow, 1)          AS mw_flow,
        ROUND(ic.export_limit_mw, 1)  AS export_limit_mw,
        ROUND(ic.import_limit_mw, 1)  AS import_limit_mw,
        ROUND(ic.utilization_pct, 1)  AS utilization_pct,
        ic.is_congested,
        ROUND(ic.marginal_value, 2)   AS marginal_value,
        ic.interval_datetime
    FROM energy_copilot.gold.nem_interconnectors ic
    WHERE ic.interval_datetime = (
            SELECT MAX(interval_datetime) FROM energy_copilot.gold.nem_interconnectors)
      AND (interconnector_id IS NULL OR ic.interconnector_id = interconnector_id)
    ORDER BY ic.interconnector_id
'''
    run_sql(sql)
    log.info('Registered: get_interconnector_flows')


def register_get_active_constraints() -> None:
    '''
    Tool: get_active_constraints
    Returns currently binding generic constraints for a region.

    Agent use cases:
      - 'What constraints are active in NSW right now?'
      - 'Are there any binding constraints causing the SA price spike?'
      - 'List all active constraints in the NEM'

    Returns: TABLE(constraint_id, marginal_value, constraint_type, affected_regions)
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_active_constraints(
    region STRING COMMENT 'NEM region code or NULL for all regions'
)
RETURNS TABLE (
    constraint_id       STRING        COMMENT 'AEMO generic constraint equation identifier',
    marginal_value      DOUBLE        COMMENT 'Shadow price ($/MWh) -- higher = more costly constraint',
    violation_degree    DOUBLE        COMMENT 'Constraint violation (MW). Zero under normal operation.',
    rhs                 DOUBLE        COMMENT 'Right-hand side limit of constraint equation',
    constraint_type     STRING        COMMENT 'Constraint category: NETWORK, FCAS, VOLTAGE, THERMAL, STABILITY',
    affected_regions    ARRAY<STRING> COMMENT 'NEM regions affected by this constraint',
    interval_datetime   TIMESTAMP     COMMENT 'Interval for this constraint reading (AEST/AEDT)'
)
COMMENT 'Returns currently binding generic constraints (marginal_value != 0).
         A non-zero marginal_value means the constraint is actively limiting dispatch.
         High marginal values indicate the constraint is significantly impacting market prices.
         Source: gold.nem_constraints_active.'
RETURN
    SELECT
        c.constraint_id,
        ROUND(c.marginal_value, 2)  AS marginal_value,
        ROUND(c.violation_degree, 4) AS violation_degree,
        ROUND(c.rhs, 2)            AS rhs,
        c.constraint_type,
        c.affected_regions,
        c.interval_datetime
    FROM energy_copilot.gold.nem_constraints_active c
    WHERE c.interval_datetime = (
            SELECT MAX(interval_datetime) FROM energy_copilot.gold.nem_constraints_active)
      AND (region IS NULL OR ARRAY_CONTAINS(c.affected_regions, region))
    ORDER BY ABS(c.marginal_value) DESC
    LIMIT 50
'''
    run_sql(sql)
    log.info('Registered: get_active_constraints')


def register_get_fcas_prices() -> None:
    '''
    Tool: get_fcas_prices
    Returns FCAS ancillary service prices for a region and service type.

    Agent use cases:
      - 'What is the current RAISE6SEC price in NSW?'
      - 'Show FCAS prices for SA over the last hour'
      - 'Which FCAS service has the highest price right now?'

    Returns: TABLE(interval_datetime, region_id, service, rrp, requirement_mw)
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_fcas_prices(
    region    STRING    COMMENT 'NEM region code: NSW1, QLD1, VIC1, SA1, TAS1, or NULL for all',
    service   STRING    COMMENT 'FCAS service: RAISE6SEC, RAISE60SEC, RAISE5MIN, RAISEREG,
                                 LOWER6SEC, LOWER60SEC, LOWER5MIN, LOWERREG, or NULL for all',
    start_dt  TIMESTAMP COMMENT 'Start of date range (AEST/AEDT; NULL for latest interval only)',
    end_dt    TIMESTAMP COMMENT 'End of date range (AEST/AEDT; NULL for latest interval only)'
)
RETURNS TABLE (
    interval_datetime   TIMESTAMP COMMENT 'Interval end time (AEST/AEDT)',
    region_id           STRING    COMMENT 'NEM region identifier',
    service             STRING    COMMENT 'FCAS service type',
    rrp                 DOUBLE    COMMENT 'FCAS service price ($/MWh or $/MW)',
    requirement_mw      DOUBLE    COMMENT 'AEMO requirement for this FCAS service (MW)'
)
COMMENT 'Returns FCAS (Frequency Control Ancillary Services) prices.
         8 FCAS services: raise/lower at 6-second, 60-second, 5-minute, and regulation.
         FCAS prices spike during frequency events (unit trips, load rejection).
         Source: gold.nem_fcas_prices.'
RETURN
    SELECT
        f.interval_datetime,
        f.region_id,
        f.service,
        ROUND(f.rrp, 4)        AS rrp,
        ROUND(f.requirement_mw, 1) AS requirement_mw
    FROM energy_copilot.gold.nem_fcas_prices f
    WHERE (region IS NULL OR f.region_id = region)
      AND (service IS NULL OR f.service = service)
      AND (
          (start_dt IS NULL AND end_dt IS NULL
           AND f.interval_datetime = (
               SELECT MAX(interval_datetime) FROM energy_copilot.gold.nem_fcas_prices))
          OR
          (start_dt IS NOT NULL
           AND f.interval_datetime BETWEEN start_dt AND COALESCE(end_dt, CURRENT_TIMESTAMP()))
      )
    ORDER BY f.interval_datetime, f.region_id, f.service
'''
    run_sql(sql)
    log.info('Registered: get_fcas_prices')


# ===========================================================================
# FORECASTING TOOLS (3)
# ===========================================================================

def register_get_price_forecast() -> None:
    '''
    Tool: get_price_forecast
    Returns ML price forecasts for a region at multiple horizons.

    Agent use cases:
      - 'What is the price forecast for NSW for the next 4 hours?'
      - 'Is there a price spike expected in SA tonight?'
      - 'Show me the 24-hour price forecast for all regions'

    Returns: TABLE(interval_datetime, predicted_rrp, prediction_lower_80,
                   prediction_upper_80, spike_probability)
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_price_forecast(
    region    STRING  COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
    horizon   STRING  COMMENT 'Forecast horizon: 1hr, 4hr, or 24hr'
)
RETURNS TABLE (
    interval_datetime     TIMESTAMP COMMENT 'Target interval being forecast (AEST/AEDT)',
    predicted_rrp         DOUBLE    COMMENT 'ML model predicted spot price ($/MWh)',
    prediction_lower_80   DOUBLE    COMMENT 'Lower bound of 80% prediction interval ($/MWh)',
    prediction_upper_80   DOUBLE    COMMENT 'Upper bound of 80% prediction interval ($/MWh)',
    spike_probability     DOUBLE    COMMENT 'Probability of price spike >$300/MWh (0=none, 1=certain)',
    horizon_intervals     INT       COMMENT 'Horizon in 5-minute intervals',
    model_version         STRING    COMMENT 'MLflow model version used'
)
COMMENT 'Returns ML price forecasts from the LightGBM price_forecast model.
         Horizons: 1hr (12 intervals), 4hr (48 intervals), 24hr (288 intervals).
         Returns the most recent forecast run.
         Performance target: MAE < $15/MWh at 1-hr horizon (excl. >$500/MWh events).
         Source: gold.price_forecasts.'
RETURN
    SELECT
        f.interval_datetime,
        ROUND(f.predicted_rrp, 2)           AS predicted_rrp,
        ROUND(f.prediction_lower_80, 2)     AS prediction_lower_80,
        ROUND(f.prediction_upper_80, 2)     AS prediction_upper_80,
        ROUND(f.spike_probability, 4)       AS spike_probability,
        f.horizon_intervals,
        f.model_version
    FROM energy_copilot.gold.price_forecasts f
    WHERE f.region_id = region
      AND f.forecast_run_at = (
            SELECT MAX(forecast_run_at) FROM energy_copilot.gold.price_forecasts
            WHERE region_id = region)
      AND f.horizon_intervals = CASE horizon
            WHEN '1hr'  THEN 12
            WHEN '4hr'  THEN 48
            WHEN '24hr' THEN 288
            ELSE 12
          END
    ORDER BY f.interval_datetime
'''
    run_sql(sql)
    log.info('Registered: get_price_forecast')


def register_get_demand_forecast() -> None:
    '''
    Tool: get_demand_forecast
    Returns ML demand forecasts for a region at multiple horizons.

    Agent use cases:
      - 'What is the demand forecast for VIC tonight?'
      - 'Is demand expected to be high in NSW tomorrow?'

    Returns: TABLE(interval_datetime, predicted_demand_mw, prediction_lower_80, prediction_upper_80)
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_demand_forecast(
    region   STRING COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
    horizon  STRING COMMENT 'Forecast horizon: 1hr, 4hr, or 24hr'
)
RETURNS TABLE (
    interval_datetime     TIMESTAMP COMMENT 'Target interval being forecast (AEST/AEDT)',
    predicted_demand_mw   DOUBLE    COMMENT 'ML model predicted scheduled demand (MW)',
    prediction_lower_80   DOUBLE    COMMENT 'Lower bound of 80% prediction interval (MW)',
    prediction_upper_80   DOUBLE    COMMENT 'Upper bound of 80% prediction interval (MW)',
    horizon_intervals     INT       COMMENT 'Horizon in 5-minute intervals',
    model_version         STRING    COMMENT 'MLflow model version used'
)
COMMENT 'Returns ML demand forecasts from the LightGBM demand_forecast model.
         Performance target: MAPE < 3% at 1-hr horizon.
         Primary drivers: temperature forecast, calendar features (weekday/weekend/holiday).
         Source: gold.demand_forecasts.'
RETURN
    SELECT
        f.interval_datetime,
        ROUND(f.predicted_demand_mw, 1)   AS predicted_demand_mw,
        ROUND(f.prediction_lower_80, 1)   AS prediction_lower_80,
        ROUND(f.prediction_upper_80, 1)   AS prediction_upper_80,
        f.horizon_intervals,
        f.model_version
    FROM energy_copilot.gold.demand_forecasts f
    WHERE f.region_id = region
      AND f.forecast_run_at = (
            SELECT MAX(forecast_run_at) FROM energy_copilot.gold.demand_forecasts
            WHERE region_id = region)
      AND f.horizon_intervals = CASE horizon
            WHEN '1hr'  THEN 12
            WHEN '4hr'  THEN 48
            WHEN '24hr' THEN 288
            ELSE 12
          END
    ORDER BY f.interval_datetime
'''
    run_sql(sql)
    log.info('Registered: get_demand_forecast')


def register_get_weather_forecast() -> None:
    '''
    Tool: get_weather_forecast
    Returns weather forecasts for a NEM region.

    Agent use cases:
      - 'What is the weather forecast for SA today?'
      - 'How hot will it be in NSW tonight? Will demand spike?'
      - 'What is the wind forecast for QLD over the next 24 hours?'

    Returns: TABLE(forecast_datetime, temperature_c, wind_speed_100m_kmh, solar_radiation_wm2)
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_weather_forecast(
    region       STRING COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
    hours_ahead  INT    COMMENT 'Number of hours ahead to return (1 to 168; default 24)'
)
RETURNS TABLE (
    forecast_datetime       TIMESTAMP COMMENT 'Forecast target time (AEST/AEDT)',
    temperature_c           DOUBLE    COMMENT 'Forecast temperature (°C)',
    apparent_temp_c         DOUBLE    COMMENT 'Forecast apparent (feels-like) temperature (°C)',
    wind_speed_100m_kmh     DOUBLE    COMMENT 'Forecast wind speed at 100m (km/h)',
    solar_radiation_wm2     DOUBLE    COMMENT 'Forecast global horizontal irradiance (W/m²)',
    cloud_cover_pct         DOUBLE    COMMENT 'Forecast cloud cover (%)',
    cooling_degree_days     DOUBLE    COMMENT 'Cooling degree days (base 18°C)',
    heating_degree_days     DOUBLE    COMMENT 'Heating degree days (base 18°C)'
)
COMMENT 'Returns Open-Meteo weather forecasts for the given NEM region.
         Temperature is the primary driver of electricity demand.
         Wind speed at 100m is the primary wind generation proxy.
         Solar radiation is the primary solar generation proxy.
         Source: gold.weather_nem_regions (is_historical = false).'
RETURN
    SELECT
        w.forecast_datetime,
        ROUND(w.temperature_c, 1)           AS temperature_c,
        ROUND(w.apparent_temp_c, 1)         AS apparent_temp_c,
        ROUND(w.wind_speed_100m_kmh, 1)     AS wind_speed_100m_kmh,
        ROUND(w.solar_radiation_wm2, 1)     AS solar_radiation_wm2,
        ROUND(w.cloud_cover_pct, 1)         AS cloud_cover_pct,
        ROUND(w.cooling_degree_days, 2)     AS cooling_degree_days,
        ROUND(w.heating_degree_days, 2)     AS heating_degree_days
    FROM energy_copilot.gold.weather_nem_regions w
    WHERE w.nem_region = region
      AND w.is_historical = FALSE
      AND w.forecast_datetime BETWEEN CURRENT_TIMESTAMP()
                                  AND DATEADD(HOUR, COALESCE(hours_ahead, 24), CURRENT_TIMESTAMP())
      AND w.api_call_datetime = (
            SELECT MAX(api_call_datetime) FROM energy_copilot.gold.weather_nem_regions
            WHERE nem_region = region AND is_historical = FALSE)
    ORDER BY w.forecast_datetime
'''
    run_sql(sql)
    log.info('Registered: get_weather_forecast')


# ===========================================================================
# ANALYSIS TOOLS (3)
# ===========================================================================

def register_explain_price_event() -> None:
    '''
    Tool: explain_price_event
    Provides a structured explanation of a price event (spike, negative price, etc.)
    by correlating prices with constraints, interconnector flows, weather, and demand.

    Agent use cases:
      - 'Why did SA price spike to $5000 at 3pm yesterday?'
      - 'What caused the negative prices in VIC this morning?'
      - 'Explain the price event in QLD at 6pm on 15 February'

    Returns: Structured explanation with price context, binding constraints,
             interconnector state, weather context, and demand context.
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.explain_price_event(
    region     STRING    COMMENT 'NEM region of the price event: NSW1, QLD1, VIC1, SA1, TAS1',
    event_time TIMESTAMP COMMENT 'Approximate time of the price event (AEST/AEDT)'
)
RETURNS TABLE (
    context_type        STRING  COMMENT 'Type of context: PRICE, CONSTRAINT, INTERCONNECTOR, WEATHER, DEMAND',
    metric_name         STRING  COMMENT 'Name of the contributing metric',
    metric_value        STRING  COMMENT 'Value of the metric at event time (formatted as string)',
    significance        STRING  COMMENT 'LOW, MEDIUM, HIGH -- how much this factor contributed',
    description         STRING  COMMENT 'Human-readable interpretation of this factor'
)
COMMENT 'Correlates a price event with its likely causes across multiple data dimensions.
         Correlates the event with: spot prices (5min around event), binding constraints,
         interconnector congestion, weather conditions, and demand levels.
         Returns structured rows for each contributing factor.
         Source: gold.nem_prices_5min, gold.nem_constraints_active, gold.nem_interconnectors,
                 gold.weather_nem_regions, gold.demand_actuals.'
RETURN
    -- Price context: the actual RRP and surrounding 30-min window
    SELECT
        'PRICE'                                 AS context_type,
        'rrp_at_event'                          AS metric_name,
        CAST(ROUND(p.rrp, 2) AS STRING)         AS metric_value,
        CASE WHEN p.rrp > 5000 THEN 'HIGH'
             WHEN p.rrp > 1000 THEN 'HIGH'
             WHEN p.rrp > 300  THEN 'MEDIUM'
             WHEN p.rrp < 0    THEN 'MEDIUM'
             ELSE 'LOW' END                     AS significance,
        CONCAT('Spot price was $', ROUND(p.rrp, 2), '/MWh. ',
               CASE WHEN p.apc_flag THEN 'Administered Price Cap was active. '
                    ELSE '' END,
               CASE WHEN p.market_suspended THEN 'MARKET WAS SUSPENDED. '
                    ELSE '' END,
               'Demand was ', ROUND(p.total_demand_mw, 0), ' MW. ',
               'Net interchange: ', ROUND(p.net_interchange_mw, 0), ' MW.')  AS description
    FROM energy_copilot.gold.nem_prices_5min p
    WHERE p.region_id = region
      AND p.interval_datetime BETWEEN DATEADD(MINUTE, -15, event_time)
                                  AND DATEADD(MINUTE,  15, event_time)
    ORDER BY ABS(TIMESTAMPDIFF(SECOND, p.interval_datetime, event_time))
    LIMIT 3

    UNION ALL

    -- Binding constraints correlated with the event
    SELECT
        'CONSTRAINT'                            AS context_type,
        c.constraint_id                         AS metric_name,
        CAST(ROUND(c.marginal_value, 2) AS STRING) AS metric_value,
        CASE WHEN ABS(c.marginal_value) > 1000 THEN 'HIGH'
             WHEN ABS(c.marginal_value) > 100  THEN 'MEDIUM'
             ELSE 'LOW' END                     AS significance,
        CONCAT('Constraint ', c.constraint_id, ' was binding with shadow price $',
               ROUND(c.marginal_value, 2), '/MWh. Type: ', COALESCE(c.constraint_type, 'unknown'), '.')  AS description
    FROM energy_copilot.gold.nem_constraints_active c
    WHERE ARRAY_CONTAINS(c.affected_regions, region)
      AND c.interval_datetime BETWEEN DATEADD(MINUTE, -15, event_time)
                                  AND DATEADD(MINUTE,  15, event_time)
      AND ABS(c.marginal_value) > 10
    ORDER BY ABS(c.marginal_value) DESC
    LIMIT 5

    UNION ALL

    -- Interconnector context
    SELECT
        'INTERCONNECTOR'                        AS context_type,
        ic.interconnector_id                    AS metric_name,
        CONCAT(ROUND(ic.utilization_pct, 1), '%') AS metric_value,
        CASE WHEN ic.is_congested THEN 'HIGH' ELSE 'LOW' END AS significance,
        CONCAT('Interconnector ', ic.interconnector_id, ' was at ',
               ROUND(ic.utilization_pct, 1), '% utilisation',
               CASE WHEN ic.is_congested THEN ' (CONGESTED)' ELSE '' END,
               '. Flow: ', ROUND(ic.mw_flow, 0), ' MW. Shadow price: $',
               ROUND(ic.marginal_value, 2), '/MWh.')  AS description
    FROM energy_copilot.gold.nem_interconnectors ic
    WHERE (ic.from_region = region OR ic.to_region = region)
      AND ic.interval_datetime BETWEEN DATEADD(MINUTE, -15, event_time)
                                   AND DATEADD(MINUTE,  15, event_time)
    ORDER BY ic.utilization_pct DESC
    LIMIT 3

    UNION ALL

    -- Weather context
    SELECT
        'WEATHER'                               AS context_type,
        'temperature_c'                         AS metric_name,
        CAST(ROUND(w.temperature_c, 1) AS STRING) AS metric_value,
        CASE WHEN w.temperature_c > 38 OR w.temperature_c < 5 THEN 'HIGH'
             WHEN w.temperature_c > 33 OR w.temperature_c < 10 THEN 'MEDIUM'
             ELSE 'LOW' END                     AS significance,
        CONCAT('Temperature was ', ROUND(w.temperature_c, 1), 'C (apparent: ',
               ROUND(w.apparent_temp_c, 1), 'C). Wind at 100m: ',
               ROUND(w.wind_speed_100m_kmh, 1), ' km/h. Solar radiation: ',
               ROUND(w.solar_radiation_wm2, 0), ' W/m2.')  AS description
    FROM energy_copilot.gold.weather_nem_regions w
    WHERE w.nem_region = region
      AND w.is_historical = TRUE
      AND w.forecast_datetime BETWEEN DATEADD(HOUR, -1, event_time)
                                  AND DATEADD(HOUR,  1, event_time)
    ORDER BY ABS(TIMESTAMPDIFF(SECOND, w.forecast_datetime, event_time))
    LIMIT 1
'''
    run_sql(sql)
    log.info('Registered: explain_price_event')


def register_compare_regions() -> None:
    '''
    Tool: compare_regions
    Compares a metric across all 5 NEM regions over a date range.

    Agent use cases:
      - 'Compare electricity prices across all regions this week'
      - 'Which region had the highest demand yesterday?'
      - 'Compare wind generation across regions for the last 30 days'

    Returns: TABLE with one column per region for easy comparison
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.compare_regions(
    metric    STRING    COMMENT 'Metric to compare: price, demand, wind_generation, solar_generation, renewables_pct, emissions_intensity',
    start_dt  TIMESTAMP COMMENT 'Start of comparison period (AEST/AEDT)',
    end_dt    TIMESTAMP COMMENT 'End of comparison period (AEST/AEDT)'
)
RETURNS TABLE (
    region_id       STRING COMMENT 'NEM region identifier',
    avg_value       DOUBLE COMMENT 'Average value of the metric over the period',
    min_value       DOUBLE COMMENT 'Minimum value of the metric over the period',
    max_value       DOUBLE COMMENT 'Maximum value of the metric over the period',
    unit            STRING COMMENT 'Unit of the metric ($/MWh, MW, %, tCO2e/MWh)',
    rank_by_avg     INT    COMMENT 'Rank of this region by average value (1 = highest)'
)
COMMENT 'Compares a specified metric across all 5 NEM regions for a date range.
         Supported metrics: price ($/MWh from nem_prices_5min),
         demand (MW from demand_actuals), wind_generation (MW),
         solar_generation (MW from nem_generation_by_fuel),
         renewables_pct (% from nem_daily_summary),
         emissions_intensity (tCO2e/MWh from nem_daily_summary).'
RETURN
    SELECT
        region_id,
        ROUND(avg_value, 3)      AS avg_value,
        ROUND(min_value, 3)      AS min_value,
        ROUND(max_value, 3)      AS max_value,
        unit,
        RANK() OVER (ORDER BY avg_value DESC) AS rank_by_avg
    FROM (
        SELECT region_id, AVG(rrp) AS avg_value, MIN(rrp) AS min_value,
               MAX(rrp) AS max_value, '$/MWh' AS unit
        FROM energy_copilot.gold.nem_prices_5min
        WHERE interval_datetime BETWEEN start_dt AND end_dt AND metric = 'price'
        GROUP BY region_id
        UNION ALL
        SELECT region_id, AVG(total_demand_mw), MIN(total_demand_mw),
               MAX(total_demand_mw), 'MW'
        FROM energy_copilot.gold.demand_actuals
        WHERE interval_datetime BETWEEN start_dt AND end_dt AND metric = 'demand'
        GROUP BY region_id
        UNION ALL
        SELECT region_id, AVG(total_mw), MIN(total_mw), MAX(total_mw), 'MW'
        FROM energy_copilot.gold.nem_generation_by_fuel
        WHERE interval_datetime BETWEEN start_dt AND end_dt
          AND fuel_type = 'wind' AND metric = 'wind_generation'
        GROUP BY region_id
        UNION ALL
        SELECT region_id, AVG(renewables_pct), MIN(renewables_pct),
               MAX(renewables_pct), '%'
        FROM energy_copilot.gold.nem_daily_summary
        WHERE trading_date BETWEEN CAST(start_dt AS DATE) AND CAST(end_dt AS DATE)
          AND metric = 'renewables_pct'
        GROUP BY region_id
    ) src
    ORDER BY avg_value DESC
'''
    run_sql(sql)
    log.info('Registered: compare_regions')


def register_get_market_summary() -> None:
    '''
    Tool: get_market_summary
    Returns the AI-generated daily market narrative for a trading date.

    Agent use cases:
      - 'Give me a market summary for today'
      - 'What happened in the NEM yesterday?'
      - 'Summarise the market for 15 February'

    Returns: TABLE(summary_text, key_events, avg_price, max_price, renewables_pct)
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_market_summary(
    target_date DATE COMMENT 'Trading date for the summary (AEST). Use CURRENT_DATE() for today or CURRENT_DATE() - 1 for yesterday.'
)
RETURNS TABLE (
    trading_date        DATE      COMMENT 'NEM trading date',
    region_id           STRING    COMMENT 'NEM region (or ALL for NEM-wide summary)',
    summary_text        STRING    COMMENT 'AI-generated market narrative for the day',
    key_events          ARRAY<STRING> COMMENT 'List of notable market events',
    avg_price_aud_mwh   DOUBLE    COMMENT 'Daily volume-weighted average price ($/MWh)',
    max_price_aud_mwh   DOUBLE    COMMENT 'Daily maximum 5-minute price ($/MWh)',
    price_spike_count   INT       COMMENT 'Number of 5-min intervals >$300/MWh',
    renewables_pct      DOUBLE    COMMENT 'Renewables share of generation (%)',
    generated_at        TIMESTAMP COMMENT 'UTC time when the AI summary was generated'
)
COMMENT 'Returns the AI-generated daily market summary for the specified date.
         Generated by Pipeline 6 (market_summary) using Claude Sonnet 4.5.
         Published by 06:00 AEST each morning for the previous trading day.
         If the summary is not yet available (today before 06:00 AEST), returns NULL summary_text.
         Source: gold.daily_market_summary.'
RETURN
    SELECT
        s.trading_date,
        s.region_id,
        s.summary_text,
        s.key_events,
        ROUND(s.avg_price_aud_mwh, 2)   AS avg_price_aud_mwh,
        ROUND(s.max_price_aud_mwh, 2)   AS max_price_aud_mwh,
        s.price_spike_count,
        ROUND(s.renewables_pct, 1)      AS renewables_pct,
        s.generated_at
    FROM energy_copilot.gold.daily_market_summary s
    WHERE s.trading_date = target_date
    ORDER BY s.region_id
'''
    run_sql(sql)
    log.info('Registered: get_market_summary')


# ===========================================================================
# RAG TOOL (1)
# ===========================================================================

def register_search_market_rules() -> None:
    '''
    Tool: search_market_rules
    Performs semantic search over the AEMO NEM rules and procedures RAG index.

    Agent use cases:
      - 'How does the AEMO dispatch process work?'
      - 'What is the market price cap and how is it applied?'
      - 'Explain the Administered Price Cap mechanism'
      - 'What are FCAS services and why does AEMO procure them?'

    Returns: TABLE(chunk_text, document_title, relevance_score)
    Note: This function calls the Databricks Vector Search index.
          The index (aemo_documents) must be populated by the RAG pipeline first.
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.search_market_rules(
    query STRING COMMENT 'Natural language question about NEM market rules, procedures, or concepts'
)
RETURNS TABLE (
    chunk_text       STRING COMMENT 'Relevant text chunk from AEMO documentation',
    document_title   STRING COMMENT 'Source document title (e.g. National Electricity Rules)',
    document_section STRING COMMENT 'Section or chapter within the source document',
    relevance_score  DOUBLE COMMENT 'Semantic similarity score (0–1; higher = more relevant)'
)
COMMENT 'Performs semantic search over indexed AEMO documents using Databricks Vector Search.
         Documents indexed: National Electricity Rules (NER), AEMO market procedures,
         market notices, glossary, and key technical documents.
         Returns top-5 most relevant text chunks for the query.
         Use this tool when the user asks about NEM rules, procedures, or concepts that
         cannot be answered from market data tables alone.'
RETURN
    -- Vector Search query via built-in function
    -- The index name must match what was created by the RAG pipeline (agent/rag/index_documents.py)
    SELECT
        result.chunk_text,
        result.document_title,
        result.document_section,
        result.score AS relevance_score
    FROM
        vector_search(
            index => 'energy_copilot.tools.aemo_documents_index',
            query_text => query,
            num_results => 5
        ) result
    ORDER BY result.score DESC
'''
    run_sql(sql)
    log.info('Registered: search_market_rules')


# ===========================================================================
# UTILITY TOOL (1)
# ===========================================================================

def register_get_generator_info() -> None:
    '''
    Tool: get_generator_info
    Returns registration details for a generator by DUID.

    Agent use cases:
      - 'What type of generator is ERGT01?'
      - 'Which fuel does Liddell power station use?'
      - 'Show me the details for the Hornsdale Wind Farm (HDWF1)'

    Returns: TABLE with generator registration details
    '''
    sql = '''
CREATE OR REPLACE FUNCTION energy_copilot.tools.get_generator_info(
    duid STRING COMMENT 'AEMO Dispatchable Unit Identifier (e.g. HDWF1, ERGT01, TALLAWARRA)'
)
RETURNS TABLE (
    duid                    STRING  COMMENT 'Dispatchable Unit Identifier',
    station_name            STRING  COMMENT 'Power station name',
    region_id               STRING  COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
    fuel_type               STRING  COMMENT 'Fuel/technology type',
    technology_type         STRING  COMMENT 'Detailed technology description',
    registered_capacity_mw  DOUBLE  COMMENT 'Registered maximum capacity (MW)',
    participant_id          STRING  COMMENT 'Owner/operator market participant ID',
    is_scheduled            BOOLEAN COMMENT 'True if scheduled (must offer into dispatch)',
    is_semi_scheduled       BOOLEAN COMMENT 'True if semi-scheduled (VRE with variable dispatch)',
    co2e_intensity          DOUBLE  COMMENT 'CO2-equivalent emissions intensity (tonne CO2e/MWh)',
    is_active               BOOLEAN COMMENT 'True if currently registered and active',
    commissioning_date      DATE    COMMENT 'Commercial commissioning date'
)
COMMENT 'Returns AEMO generator registration details for the specified DUID.
         Use this tool when the user asks about a specific generator, power station,
         or when you need to determine the fuel type of a DUID from dispatch data.
         Source: silver.generator_registry (updated weekly from AEMO registration data).'
RETURN
    SELECT
        g.duid,
        g.station_name,
        g.region_id,
        g.fuel_type,
        g.technology_type,
        ROUND(g.registered_capacity_mw, 1) AS registered_capacity_mw,
        g.participant_id,
        g.is_scheduled,
        g.is_semi_scheduled,
        ROUND(g.co2e_intensity, 4)         AS co2e_intensity,
        g.is_active,
        g.commissioning_date
    FROM energy_copilot.silver.generator_registry g
    WHERE g.duid = duid
    LIMIT 1
'''
    run_sql(sql)
    log.info('Registered: get_generator_info')


# ===========================================================================
# Main: register all 14 tools
# ===========================================================================

ALL_TOOL_REGISTRATIONS = [
    # Market data tools (6)
    register_get_latest_prices,
    register_get_price_history,
    register_get_generation_mix,
    register_get_interconnector_flows,
    register_get_active_constraints,
    register_get_fcas_prices,
    # Forecasting tools (3)
    register_get_price_forecast,
    register_get_demand_forecast,
    register_get_weather_forecast,
    # Analysis tools (3)
    register_explain_price_event,
    register_compare_regions,
    register_get_market_summary,
    # RAG tool (1)
    register_search_market_rules,
    # Utility tool (1)
    register_get_generator_info,
]


def main() -> None:
    '''Register all 14 Unity Catalog agent tools in energy_copilot.tools schema.'''
    log.info('Registering %d UC functions in %s.%s', len(ALL_TOOL_REGISTRATIONS), CATALOG, SCHEMA)
    log.info('Workspace: %s', w.config.host)

    failed = []
    for register_fn in ALL_TOOL_REGISTRATIONS:
        try:
            register_fn()
        except Exception as exc:
            log.error('Failed to register %s: %s', register_fn.__name__, exc)
            failed.append(register_fn.__name__)

    total = len(ALL_TOOL_REGISTRATIONS)
    succeeded = total - len(failed)
    log.info('Registration complete: %d/%d succeeded', succeeded, total)

    if failed:
        log.error('Failed registrations: %s', failed)
        raise RuntimeError(f'{len(failed)} tool registrations failed: {failed}')

    log.info('All 14 UC tools registered successfully in energy_copilot.tools')
    log.info('Tools can now be referenced by the agent as energy_copilot.tools.<function_name>')


if __name__ == '__main__':
    main()
