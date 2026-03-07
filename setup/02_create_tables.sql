-- =============================================================================
-- 02_create_tables.sql
-- AUS Energy Copilot — Full table DDL
--
-- Creates all Delta tables across the Bronze (11), Silver (13) and Gold (16) schemas.
-- All tables:
--   - Use Delta format (default in Unity Catalog)
--   - High-volume time-series tables partitioned by interval_datetime::date
--   - Include column-level COMMENT annotations for Genie semantic layer
--   - Use fully qualified names: energy_copilot.<schema>.<table>
--
-- Run after 01_create_schemas.sql.
-- Safe to re-run: all statements use CREATE TABLE IF NOT EXISTS.
-- =============================================================================

USE CATALOG energy_copilot;

-- =============================================================================
-- BRONZE LAYER — 11 tables
-- Raw, append-only ingestion from NEMWEB, OpenElectricity, Open-Meteo, APVI.
-- Source data preserved; types kept as STRING where source format is ambiguous.
-- Partitioned by settlement_date (cast from source string) on high-volume tables.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.nemweb_dispatch_gen
-- Source: NEMWEB Dispatch_SCADA / PUBLIC_DISPATCHSCADA_*.ZIP
-- Frequency: every 5 minutes
-- Contains: actual generation output per DUID (dispatchable unit identifier)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.nemweb_dispatch_gen (
  -- Ingestion metadata
  _source_file        STRING  COMMENT 'Source ZIP filename from NEMWEB',
  _ingested_at        TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',

  -- NEMWEB header fields
  i_type              STRING  COMMENT 'NEMWEB record type identifier (I = data)',
  report_type         STRING  COMMENT 'NEMWEB report category (DISPATCH)',
  table_name          STRING  COMMENT 'NEMWEB table name within the report',

  -- Core dispatch SCADA fields
  settlement_date     STRING  COMMENT 'Dispatch interval date, YYYY/MM/DD format from source',
  run_no              STRING  COMMENT 'Dispatch run number within the interval',
  duid                STRING  COMMENT 'Dispatchable Unit Identifier — unique generator/load ID',
  intervention        STRING  COMMENT 'Intervention flag (0 = normal dispatch, 1 = intervention)',
  dispatch_mode       STRING  COMMENT 'Dispatch mode (0 = normal, 1 = SCADA, 3 = manual)',
  agc_status          STRING  COMMENT 'AGC (Automatic Generation Control) status flag',
  initialmw           STRING  COMMENT 'Initial MW output at start of interval (string from source)',
  totalcleared        STRING  COMMENT 'Total MW cleared in dispatch solution',
  rampdown_rate       STRING  COMMENT 'Ramp-down rate limit (MW/min)',
  rampup_rate         STRING  COMMENT 'Ramp-up rate limit (MW/min)',
  lower5min           STRING  COMMENT 'Lower 5-minute FCAS enablement (MW)',
  lower60sec          STRING  COMMENT 'Lower 60-second FCAS enablement (MW)',
  lower6sec           STRING  COMMENT 'Lower 6-second FCAS enablement (MW)',
  raise5min           STRING  COMMENT 'Raise 5-minute FCAS enablement (MW)',
  raise60sec          STRING  COMMENT 'Raise 60-second FCAS enablement (MW)',
  raise6sec           STRING  COMMENT 'Raise 6-second FCAS enablement (MW)',
  lastchanged         STRING  COMMENT 'AEMO last-changed timestamp for this record'
)
USING DELTA
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw NEMWEB Dispatch SCADA data — actual generation output per DUID every 5 minutes.
         Source: NEMWEB /REPORTS/CURRENT/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_*.ZIP.
         Append-only; schema preserved from source CSV.';

-- -----------------------------------------------------------------------------
-- bronze.nemweb_dispatch_is
-- Source: NEMWEB DispatchIS_Reports / PUBLIC_DISPATCHIS_*.ZIP
-- Frequency: every 5 minutes
-- Contains: DispatchIS — dispatch interval solution (prices + enablements per region)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.nemweb_dispatch_is (
  _source_file        STRING    COMMENT 'Source ZIP filename from NEMWEB',
  _ingested_at        TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',
  i_type              STRING    COMMENT 'NEMWEB record type identifier',
  report_type         STRING    COMMENT 'NEMWEB report category',
  table_name          STRING    COMMENT 'NEMWEB sub-table name (PRICE, REGIONSUM, INTERCONNECTORSOLN, etc.)',
  settlement_date     STRING    COMMENT 'Settlement date YYYY/MM/DD',
  run_no              STRING    COMMENT 'Dispatch run number',
  intervention        STRING    COMMENT 'Intervention flag',
  regionid            STRING    COMMENT 'NEM region identifier (NSW1, QLD1, VIC1, SA1, TAS1)',
  rrp                 STRING    COMMENT 'Regional Reference Price ($/MWh) — raw string from source',
  eep                 STRING    COMMENT 'Energy Export Price ($/MWh)',
  rop                 STRING    COMMENT 'Regional Over-constrained Dispatch price ($/MWh)',
  apcflag             STRING    COMMENT 'Administered Price Cap flag (0/1)',
  marketsuspendedflag STRING    COMMENT 'Market suspended flag (0/1)',
  totaldemand         STRING    COMMENT 'Total scheduled demand for region (MW)',
  availablegeneration STRING    COMMENT 'Total available generation for region (MW)',
  dispatchableload    STRING    COMMENT 'Total dispatchable load for region (MW)',
  netinterchange      STRING    COMMENT 'Net interconnector flow into region (MW, positive = import)',
  excessgeneration    STRING    COMMENT 'Excess generation in region (MW)',
  raise6secreq        STRING    COMMENT 'Raise 6-second FCAS requirement for region (MW)',
  raise60secreq       STRING    COMMENT 'Raise 60-second FCAS requirement for region (MW)',
  raise5minreq        STRING    COMMENT 'Raise 5-minute FCAS requirement for region (MW)',
  raiseregulationreq  STRING    COMMENT 'Raise regulation FCAS requirement (MW)',
  lower6secreq        STRING    COMMENT 'Lower 6-second FCAS requirement for region (MW)',
  lower60secreq       STRING    COMMENT 'Lower 60-second FCAS requirement for region (MW)',
  lower5minreq        STRING    COMMENT 'Lower 5-minute FCAS requirement for region (MW)',
  lowerregulationreq  STRING    COMMENT 'Lower regulation FCAS requirement (MW)',
  lastchanged         STRING    COMMENT 'AEMO last-changed timestamp'
)
USING DELTA
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw NEMWEB DispatchIS solution data — regional prices and FCAS requirements every 5 minutes.
         Source: NEMWEB /REPORTS/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_*.ZIP.';

-- -----------------------------------------------------------------------------
-- bronze.nemweb_dispatch_price
-- Source: NEMWEB Dispatch_Reports / PUBLIC_DISPATCHPRICE_*.ZIP
-- Frequency: every 5 minutes
-- Contains: simple dispatch price per region
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.nemweb_dispatch_price (
  _source_file        STRING    COMMENT 'Source ZIP filename from NEMWEB',
  _ingested_at        TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',
  i_type              STRING    COMMENT 'NEMWEB record type identifier',
  settlement_date     STRING    COMMENT 'Settlement date YYYY/MM/DD',
  run_no              STRING    COMMENT 'Dispatch run number within interval',
  regionid            STRING    COMMENT 'NEM region identifier (NSW1, QLD1, VIC1, SA1, TAS1)',
  rrp                 STRING    COMMENT 'Regional Reference Price ($/MWh)',
  intervention        STRING    COMMENT 'Intervention flag',
  lastchanged         STRING    COMMENT 'AEMO last-changed timestamp'
)
USING DELTA
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw NEMWEB dispatch price per NEM region every 5 minutes.
         Source: NEMWEB /REPORTS/CURRENT/Dispatch_Reports/PUBLIC_DISPATCHPRICE_*.ZIP.';

-- -----------------------------------------------------------------------------
-- bronze.nemweb_dispatch_inter
-- Source: NEMWEB Dispatch_Reports / PUBLIC_DISPATCHINTERCONNECTORRES_*.ZIP
-- Frequency: every 5 minutes
-- Contains: interconnector flows and limits in each dispatch interval
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.nemweb_dispatch_inter (
  _source_file          STRING    COMMENT 'Source ZIP filename from NEMWEB',
  _ingested_at          TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',
  i_type                STRING    COMMENT 'NEMWEB record type identifier',
  settlement_date       STRING    COMMENT 'Settlement date YYYY/MM/DD',
  run_no                STRING    COMMENT 'Dispatch run number',
  intervention          STRING    COMMENT 'Intervention flag',
  interconnectorid      STRING    COMMENT 'Interconnector identifier (e.g. NSW1-QLD1, VIC1-SA1)',
  meteredmwflow         STRING    COMMENT 'Actual metered MW flow (positive = forward direction)',
  mwflow                STRING    COMMENT 'Dispatch solution MW flow',
  mwlosses              STRING    COMMENT 'Interconnector losses (MW)',
  exportlimit           STRING    COMMENT 'Forward direction MW export limit',
  importlimit           STRING    COMMENT 'Reverse direction MW import limit',
  marginalvalue         STRING    COMMENT 'Marginal value / shadow price of interconnector limit',
  violationdegree       STRING    COMMENT 'Violation degree if limit exceeded',
  exportgenconid        STRING    COMMENT 'Generic constraint ID binding the export limit',
  importgenconid        STRING    COMMENT 'Generic constraint ID binding the import limit',
  lastchanged           STRING    COMMENT 'AEMO last-changed timestamp'
)
USING DELTA
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw NEMWEB dispatch interconnector results — flows and limits every 5 minutes.
         Source: NEMWEB /REPORTS/CURRENT/Dispatch_Reports/PUBLIC_DISPATCHINTERCONNECTORRES_*.ZIP.';

-- -----------------------------------------------------------------------------
-- bronze.nemweb_trading_is
-- Source: NEMWEB TradingIS_Reports / PUBLIC_TRADINGIS_*.ZIP
-- Frequency: every 30 minutes
-- Contains: 30-minute trading interval settlement prices
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.nemweb_trading_is (
  _source_file        STRING    COMMENT 'Source ZIP filename from NEMWEB',
  _ingested_at        TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',
  i_type              STRING    COMMENT 'NEMWEB record type identifier',
  settlement_date     STRING    COMMENT 'Settlement date YYYY/MM/DD',
  run_no              STRING    COMMENT 'Trading interval run number',
  regionid            STRING    COMMENT 'NEM region identifier (NSW1, QLD1, VIC1, SA1, TAS1)',
  rrp                 STRING    COMMENT 'Regional Reference Price averaged over 30-min trading interval ($/MWh)',
  eep                 STRING    COMMENT 'Energy Export Price ($/MWh)',
  invalidflag         STRING    COMMENT 'Invalid flag — 1 if trading price is not valid for settlement',
  lastchanged         STRING    COMMENT 'AEMO last-changed timestamp',
  totaldemand         STRING    COMMENT 'Total scheduled demand over trading interval (MW)',
  availablegeneration STRING    COMMENT 'Total available generation over trading interval (MW)',
  netinterchange      STRING    COMMENT 'Net interconnector import into region over interval (MW)'
)
USING DELTA
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw NEMWEB TradingIS data — 30-minute settlement prices per region.
         Source: NEMWEB /REPORTS/CURRENT/TradingIS_Reports/PUBLIC_TRADINGIS_*.ZIP.';

-- -----------------------------------------------------------------------------
-- bronze.nemweb_predispatch
-- Source: NEMWEB Predispatch_Reports / PUBLIC_PREDISPATCHIS_*.ZIP
-- Frequency: every 30 minutes (covers next ~2 hours in 5-min intervals)
-- Contains: AEMO pre-dispatch price and demand forecasts
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.nemweb_predispatch (
  _source_file        STRING    COMMENT 'Source ZIP filename from NEMWEB',
  _ingested_at        TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',
  i_type              STRING    COMMENT 'NEMWEB record type identifier',
  run_datetime        STRING    COMMENT 'Pre-dispatch run datetime (when forecast was generated)',
  predispatch_seqno   STRING    COMMENT 'Pre-dispatch sequence number',
  regionid            STRING    COMMENT 'NEM region identifier (NSW1, QLD1, VIC1, SA1, TAS1)',
  period              STRING    COMMENT 'Forecast period datetime (target interval)',
  rrp                 STRING    COMMENT 'Forecast Regional Reference Price ($/MWh)',
  eep                 STRING    COMMENT 'Forecast Energy Export Price ($/MWh)',
  rop                 STRING    COMMENT 'Forecast Regional Over-constrained Dispatch price',
  totaldemand         STRING    COMMENT 'Forecast total demand for region (MW)',
  availablegeneration STRING    COMMENT 'Forecast available generation for region (MW)',
  netinterchange      STRING    COMMENT 'Forecast net interconnector import (MW)',
  lastchanged         STRING    COMMENT 'AEMO last-changed timestamp'
)
USING DELTA
PARTITIONED BY (run_datetime)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw NEMWEB pre-dispatch reports — AEMO price and demand forecasts for next ~2 hours.
         Source: NEMWEB /REPORTS/CURRENT/Predispatch_Reports/PUBLIC_PREDISPATCHIS_*.ZIP.
         Each run covers ~30 intervals ahead at 5-minute resolution.';

-- -----------------------------------------------------------------------------
-- bronze.nemweb_bids
-- Source: NEMWEB Next_Day_Dispatch / PUBLIC_NEXT_DAY_DISPATCH_*.ZIP
-- Frequency: daily (next-day dispatch bids published by AEMO)
-- Contains: generator bid stacks (price bands and volumes)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.nemweb_bids (
  _source_file        STRING    COMMENT 'Source ZIP filename from NEMWEB',
  _ingested_at        TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',
  i_type              STRING    COMMENT 'NEMWEB record type identifier',
  settlement_date     STRING    COMMENT 'Settlement date YYYY/MM/DD',
  duid                STRING    COMMENT 'Dispatchable Unit Identifier',
  bidtype             STRING    COMMENT 'Bid type (ENERGY, RAISE6SEC, LOWER6SEC, etc.)',
  priceband1          STRING    COMMENT 'Price band 1 ($/MWh) — lowest bid band',
  priceband2          STRING    COMMENT 'Price band 2 ($/MWh)',
  priceband3          STRING    COMMENT 'Price band 3 ($/MWh)',
  priceband4          STRING    COMMENT 'Price band 4 ($/MWh)',
  priceband5          STRING    COMMENT 'Price band 5 ($/MWh)',
  priceband6          STRING    COMMENT 'Price band 6 ($/MWh)',
  priceband7          STRING    COMMENT 'Price band 7 ($/MWh)',
  priceband8          STRING    COMMENT 'Price band 8 ($/MWh)',
  priceband9          STRING    COMMENT 'Price band 9 ($/MWh)',
  priceband10         STRING    COMMENT 'Price band 10 ($/MWh) — highest bid band',
  bandavail1          STRING    COMMENT 'Available MW in band 1',
  bandavail2          STRING    COMMENT 'Available MW in band 2',
  bandavail3          STRING    COMMENT 'Available MW in band 3',
  bandavail4          STRING    COMMENT 'Available MW in band 4',
  bandavail5          STRING    COMMENT 'Available MW in band 5',
  bandavail6          STRING    COMMENT 'Available MW in band 6',
  bandavail7          STRING    COMMENT 'Available MW in band 7',
  bandavail8          STRING    COMMENT 'Available MW in band 8',
  bandavail9          STRING    COMMENT 'Available MW in band 9',
  bandavail10         STRING    COMMENT 'Available MW in band 10',
  lastchanged         STRING    COMMENT 'AEMO last-changed timestamp'
)
USING DELTA
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw NEMWEB next-day dispatch bid data — generator price-band offers (published T+1).
         Source: NEMWEB /REPORTS/CURRENT/Next_Day_Dispatch/PUBLIC_NEXT_DAY_DISPATCH_*.ZIP.';

-- -----------------------------------------------------------------------------
-- bronze.nemweb_constraints
-- Source: NEMWEB DispatchIS_Reports — constraint sub-tables
-- Frequency: every 5 minutes
-- Contains: binding generic constraints in each dispatch interval
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.nemweb_constraints (
  _source_file        STRING    COMMENT 'Source ZIP filename from NEMWEB',
  _ingested_at        TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',
  i_type              STRING    COMMENT 'NEMWEB record type identifier',
  settlement_date     STRING    COMMENT 'Settlement date YYYY/MM/DD',
  run_no              STRING    COMMENT 'Dispatch run number',
  intervention        STRING    COMMENT 'Intervention flag',
  constraintid        STRING    COMMENT 'Generic constraint equation identifier',
  rhs                 STRING    COMMENT 'Right-hand side value of constraint equation',
  marginalvalue       STRING    COMMENT 'Marginal value / shadow price of binding constraint ($/MWh)',
  violationdegree     STRING    COMMENT 'Violation degree (MW) — positive if constraint is violated',
  lhs                 STRING    COMMENT 'Left-hand side value of constraint equation',
  lastchanged         STRING    COMMENT 'AEMO last-changed timestamp'
)
USING DELTA
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw NEMWEB dispatch constraint solution data — binding constraints and shadow prices every 5 minutes.
         Source: NEMWEB /REPORTS/CURRENT/DispatchIS_Reports (constraint sub-tables).';

-- -----------------------------------------------------------------------------
-- bronze.openelec_raw
-- Source: OpenElectricity API (api.openelectricity.org.au/v4)
-- Frequency: hourly batch
-- Contains: generation by fuel type and region (JSON response stored as string)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.openelec_raw (
  _ingested_at        TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',
  _api_endpoint       STRING    COMMENT 'OpenElectricity API endpoint called',
  _api_version        STRING    COMMENT 'OpenElectricity API version (v4)',
  interval_start      STRING    COMMENT 'Interval start datetime in ISO 8601 format',
  interval_end        STRING    COMMENT 'Interval end datetime in ISO 8601 format',
  network_region      STRING    COMMENT 'NEM network region code (NSW1, QLD1, VIC1, SA1, TAS1)',
  fuel_tech           STRING    COMMENT 'Fuel technology type (coal_black, coal_brown, gas_ccgt, gas_ocgt, wind, solar_utility, hydro, battery_discharging, etc.)',
  energy_gwh          STRING    COMMENT 'Energy generation for interval (GWh)',
  power_mw            STRING    COMMENT 'Average power output for interval (MW)',
  emissions_tco2e     STRING    COMMENT 'CO2-equivalent emissions (tonnes)',
  market_value_aud    STRING    COMMENT 'Market value of generation (AUD)',
  raw_json            STRING    COMMENT 'Full raw JSON response body for audit trail'
)
USING DELTA
PARTITIONED BY (interval_start)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw OpenElectricity API response data — generation by fuel tech and region (hourly).
         Used for validation against NEMWEB and for backfill where NEMWEB archives are incomplete.
         Source: OpenElectricity API v4 /energy/network endpoint.';

-- -----------------------------------------------------------------------------
-- bronze.weather_raw
-- Source: Open-Meteo API (api.open-meteo.com) — BOM ACCESS-G model
-- Frequency: hourly batch, 5 representative coordinates per NEM region
-- Contains: temperature, wind speed/direction, solar radiation forecasts
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.weather_raw (
  _ingested_at          TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',
  _api_call_datetime    TIMESTAMP COMMENT 'Datetime when the Open-Meteo API call was made',
  latitude              DOUBLE    COMMENT 'Latitude of weather station coordinate',
  longitude             DOUBLE    COMMENT 'Longitude of weather station coordinate',
  nem_region            STRING    COMMENT 'NEM region this coordinate maps to (NSW1, QLD1, VIC1, SA1, TAS1)',
  city_label            STRING    COMMENT 'Human-readable label for this coordinate (e.g. Sydney, Melbourne)',
  forecast_datetime     STRING    COMMENT 'Forecast target datetime (ISO 8601)',
  is_historical         BOOLEAN   COMMENT 'True if record is historical actuals, False if forecast',
  temperature_2m_c      DOUBLE    COMMENT 'Air temperature at 2 metres above ground (degrees Celsius)',
  apparent_temp_c       DOUBLE    COMMENT 'Apparent (feels-like) temperature (degrees Celsius)',
  wind_speed_10m_kmh    DOUBLE    COMMENT 'Wind speed at 10 metres (km/h)',
  wind_speed_100m_kmh   DOUBLE    COMMENT 'Wind speed at 100 metres — proxy for wind generation potential (km/h)',
  wind_direction_10m    DOUBLE    COMMENT 'Wind direction at 10 metres (degrees from north)',
  solar_radiation_wm2   DOUBLE    COMMENT 'Global horizontal irradiance — solar energy proxy (W/m²)',
  cloud_cover_pct       DOUBLE    COMMENT 'Total cloud cover percentage (0-100)',
  precipitation_mm      DOUBLE    COMMENT 'Precipitation amount (mm)',
  relative_humidity_pct DOUBLE    COMMENT 'Relative humidity at 2 metres (%)'
)
USING DELTA
PARTITIONED BY (nem_region)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw Open-Meteo weather data for NEM regions.
         5 coordinate pairs per region (major population centres).
         Covers both historical actuals and 7-day forecasts.
         Source: Open-Meteo API using BOM ACCESS-G model (Australian-specific NWP).';

-- -----------------------------------------------------------------------------
-- bronze.apvi_raw
-- Source: APVI API (pv-map.apvi.org.au/api)
-- Frequency: every 30 minutes
-- Contains: rooftop solar generation estimates per postcode/state
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.bronze.apvi_raw (
  _ingested_at          TIMESTAMP COMMENT 'UTC timestamp when record was written to Bronze',
  interval_datetime     STRING    COMMENT 'Interval datetime in ISO 8601 format',
  state                 STRING    COMMENT 'Australian state (NSW, QLD, VIC, SA, TAS)',
  postcode              STRING    COMMENT 'Postcode for the rooftop solar estimate (may be NULL for state aggregates)',
  estimated_output_mw   DOUBLE    COMMENT 'Estimated rooftop PV output for postcode/state (MW)',
  capacity_mw           DOUBLE    COMMENT 'Installed rooftop PV capacity for postcode/state (MW)',
  capacity_factor       DOUBLE    COMMENT 'Capacity factor — ratio of actual to installed capacity (0-1)',
  raw_json              STRING    COMMENT 'Full raw JSON response for audit trail'
)
USING DELTA
PARTITIONED BY (state)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Bronze: raw APVI rooftop solar generation estimates — 30-minute intervals per state.
         Used in Gold solar_rooftop table after mapping states to NEM regions.
         Source: APVI API pv-map.apvi.org.au/api.';


-- =============================================================================
-- SILVER LAYER — 13 tables
-- Cleaned, typed, validated, deduplicated data.
-- interval_datetime is cast to TIMESTAMP; all numeric fields cast from STRING.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- silver.dispatch_prices
-- Sourced from: bronze.nemweb_dispatch_price + bronze.nemweb_dispatch_is
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.dispatch_prices (
  interval_datetime     TIMESTAMP COMMENT 'Dispatch interval end time (AEST/AEDT — Australia/Sydney timezone)',
  region_id             STRING    COMMENT 'NEM region identifier: NSW1, QLD1, VIC1, SA1, or TAS1',
  rrp                   DOUBLE    COMMENT 'Regional Reference Price for dispatch interval ($/MWh). Range: -$1,000 to $15,500/MWh (market floor to market cap)',
  rop                   DOUBLE    COMMENT 'Regional Over-constrained Dispatch price ($/MWh) — differs from RRP when network constraints bind',
  intervention          BOOLEAN   COMMENT 'True if AEMO intervention pricing applied to this interval',
  apc_flag              BOOLEAN   COMMENT 'True if Administered Price Cap (APC) is in effect',
  market_suspended_flag BOOLEAN   COMMENT 'True if NEM market is suspended for this region',
  total_demand_mw       DOUBLE    COMMENT 'Total scheduled demand in region for this interval (MW)',
  available_gen_mw      DOUBLE    COMMENT 'Total available generation in region (MW)',
  net_interchange_mw    DOUBLE    COMMENT 'Net interconnector flow into region — positive = net import (MW)',
  run_no                INT       COMMENT 'Dispatch run number — typically 1 for normal dispatch',
  _source               STRING    COMMENT 'Source table (dispatch_price or dispatch_is)',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: cleaned 5-minute dispatch prices per NEM region.
         Deduplicated on (interval_datetime, region_id, run_no).
         Data quality: nulls rejected on rrp and region_id; price range validated.';

-- -----------------------------------------------------------------------------
-- silver.dispatch_generation
-- Sourced from: bronze.nemweb_dispatch_gen
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.dispatch_generation (
  interval_datetime     TIMESTAMP COMMENT 'Dispatch interval end time (AEST/AEDT)',
  duid                  STRING    COMMENT 'Dispatchable Unit Identifier — unique AEMO identifier for each generator or controllable load',
  initial_mw            DOUBLE    COMMENT 'Initial MW output at start of dispatch interval',
  total_cleared_mw      DOUBLE    COMMENT 'MW cleared in dispatch solution — what the unit was instructed to generate',
  ramp_down_rate        DOUBLE    COMMENT 'Physical ramp-down rate limit (MW/min)',
  ramp_up_rate          DOUBLE    COMMENT 'Physical ramp-up rate limit (MW/min)',
  lower5min_mw          DOUBLE    COMMENT 'Lower 5-minute FCAS enabled for this unit (MW)',
  lower60sec_mw         DOUBLE    COMMENT 'Lower 60-second FCAS enabled (MW)',
  lower6sec_mw          DOUBLE    COMMENT 'Lower 6-second FCAS enabled (MW)',
  raise5min_mw          DOUBLE    COMMENT 'Raise 5-minute FCAS enabled (MW)',
  raise60sec_mw         DOUBLE    COMMENT 'Raise 60-second FCAS enabled (MW)',
  raise6sec_mw          DOUBLE    COMMENT 'Raise 6-second FCAS enabled (MW)',
  dispatch_mode         INT       COMMENT 'Dispatch mode: 0=normal, 1=SCADA only, 3=manual',
  intervention          BOOLEAN   COMMENT 'True if AEMO intervention applied',
  run_no                INT       COMMENT 'Dispatch run number',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: cleaned 5-minute dispatch generation outputs per DUID.
         Deduplicated on (interval_datetime, duid, run_no).
         Join to silver.generator_registry on duid for fuel type and region.';

-- -----------------------------------------------------------------------------
-- silver.dispatch_interconnectors
-- Sourced from: bronze.nemweb_dispatch_inter
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.dispatch_interconnectors (
  interval_datetime     TIMESTAMP COMMENT 'Dispatch interval end time (AEST/AEDT)',
  interconnector_id     STRING    COMMENT 'Interconnector identifier: NSW1-QLD1, VIC1-NSW1, VIC1-SA1, VIC1-TAS1, NSW1-VIC1, SA1-VIC1',
  metered_mw_flow       DOUBLE    COMMENT 'Actual metered MW flow — positive = forward direction (as defined by AEMO)',
  mw_flow               DOUBLE    COMMENT 'Dispatch solution MW flow',
  mw_losses             DOUBLE    COMMENT 'Transmission losses on interconnector (MW)',
  export_limit_mw       DOUBLE    COMMENT 'Forward direction MW limit (export constraint)',
  import_limit_mw       DOUBLE    COMMENT 'Reverse direction MW limit (import constraint)',
  marginal_value        DOUBLE    COMMENT 'Shadow price of binding interconnector limit ($/MWh)',
  violation_degree      DOUBLE    COMMENT 'Constraint violation degree (MW) — zero in normal dispatch',
  export_gencon_id      STRING    COMMENT 'Binding constraint ID for export limit (if applicable)',
  import_gencon_id      STRING    COMMENT 'Binding constraint ID for import limit (if applicable)',
  intervention          BOOLEAN   COMMENT 'True if AEMO intervention applied',
  run_no                INT       COMMENT 'Dispatch run number',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: cleaned 5-minute dispatch interconnector results.
         Deduplicated on (interval_datetime, interconnector_id, run_no).';

-- -----------------------------------------------------------------------------
-- silver.dispatch_constraints
-- Sourced from: bronze.nemweb_constraints
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.dispatch_constraints (
  interval_datetime     TIMESTAMP COMMENT 'Dispatch interval end time (AEST/AEDT)',
  constraint_id         STRING    COMMENT 'AEMO generic constraint equation identifier (e.g. N^^NIL_FCSPS, V_MSLOAD_LIM)',
  rhs                   DOUBLE    COMMENT 'Right-hand side value of the constraint equation',
  lhs                   DOUBLE    COMMENT 'Left-hand side value (actual sum of terms)',
  marginal_value        DOUBLE    COMMENT 'Shadow price of the constraint ($/MWh) — non-zero only when binding',
  violation_degree      DOUBLE    COMMENT 'Constraint violation degree (MW) — should be zero under normal dispatch',
  intervention          BOOLEAN   COMMENT 'True if AEMO intervention applied',
  run_no                INT       COMMENT 'Dispatch run number',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: cleaned 5-minute dispatch constraint results.
         Only constraints with marginal_value != 0 are considered "binding".
         Deduplicated on (interval_datetime, constraint_id, run_no).';

-- -----------------------------------------------------------------------------
-- silver.trading_prices
-- Sourced from: bronze.nemweb_trading_is
-- 30-minute settlement prices used for actual energy billing
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.trading_prices (
  interval_datetime     TIMESTAMP COMMENT 'Trading interval end time (AEST/AEDT) — 30-minute intervals',
  region_id             STRING    COMMENT 'NEM region identifier: NSW1, QLD1, VIC1, SA1, TAS1',
  rrp                   DOUBLE    COMMENT 'Regional Reference Price averaged over the 30-minute trading interval ($/MWh). Used for settlement.',
  eep                   DOUBLE    COMMENT 'Energy Export Price ($/MWh) — applies to exported energy',
  invalid_flag          BOOLEAN   COMMENT 'True if this trading price is flagged as invalid by AEMO (will not be used for settlement)',
  total_demand_mw       DOUBLE    COMMENT 'Average scheduled demand over trading interval (MW)',
  available_gen_mw      DOUBLE    COMMENT 'Average available generation over trading interval (MW)',
  net_interchange_mw    DOUBLE    COMMENT 'Average net interconnector import into region over interval (MW)',
  run_no                INT       COMMENT 'Trading interval run number',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: cleaned 30-minute NEM trading prices (settlement prices) per region.
         These are the prices used for energy billing (average of 6 dispatch intervals).
         Deduplicated on (interval_datetime, region_id, run_no).';

-- -----------------------------------------------------------------------------
-- silver.predispatch_prices
-- Sourced from: bronze.nemweb_predispatch
-- AEMO short-term forecast prices (0 to ~2 hours ahead)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.predispatch_prices (
  run_datetime          TIMESTAMP COMMENT 'Datetime when AEMO ran the pre-dispatch solution',
  period_datetime       TIMESTAMP COMMENT 'Target forecast period (interval being forecast)',
  region_id             STRING    COMMENT 'NEM region identifier: NSW1, QLD1, VIC1, SA1, TAS1',
  forecast_rrp          DOUBLE    COMMENT 'AEMO pre-dispatch forecast Regional Reference Price ($/MWh)',
  forecast_rop          DOUBLE    COMMENT 'AEMO forecast Regional Over-constrained Dispatch price ($/MWh)',
  forecast_demand_mw    DOUBLE    COMMENT 'AEMO forecast total demand for region (MW)',
  forecast_avail_gen_mw DOUBLE    COMMENT 'AEMO forecast available generation (MW)',
  forecast_interchange  DOUBLE    COMMENT 'AEMO forecast net interconnector import (MW)',
  horizon_intervals     INT       COMMENT 'Number of 5-min intervals ahead (1 = 5 min, 12 = 1 hour, 24 = 2 hours)',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (CAST(run_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: cleaned AEMO pre-dispatch price forecasts — AEMO baseline forecast for next ~2 hours.
         Used as a benchmark for ML model accuracy comparison.
         Deduplicated on (run_datetime, period_datetime, region_id).';

-- -----------------------------------------------------------------------------
-- silver.generator_registry
-- Sourced from: AEMO generator registration CSV (one-time + periodic refresh)
-- Reference/dimension table — DUID to fuel type, capacity, region mapping
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.generator_registry (
  duid                  STRING    COMMENT 'Dispatchable Unit Identifier — primary key',
  station_name          STRING    COMMENT 'Power station name (human-readable)',
  region_id             STRING    COMMENT 'NEM region where the generator is located (NSW1, QLD1, VIC1, SA1, TAS1)',
  fuel_type             STRING    COMMENT 'Primary fuel type: coal_black, coal_brown, gas_ccgt, gas_ocgt, gas_steam, gas_recip, hydro, wind, solar_utility, battery, diesel, distillate, pumps',
  technology_type       STRING    COMMENT 'Technology description from AEMO registration data',
  registered_capacity_mw DOUBLE  COMMENT 'Registered maximum capacity (MW)',
  max_capacity_mw       DOUBLE    COMMENT 'Maximum capacity including temporary constraints (MW)',
  min_capacity_mw       DOUBLE    COMMENT 'Minimum stable generation output (MW)',
  participant_id        STRING    COMMENT 'Market participant (owner/operator) identifier',
  classification        STRING    COMMENT 'Unit classification: GENERATOR, LOAD, BIDIRECTIONAL',
  dispatch_type         STRING    COMMENT 'Dispatch type: GENERATOR or LOAD',
  co2e_intensity        DOUBLE    COMMENT 'CO2-equivalent emissions intensity (tonne CO2e / MWh) — derived from fuel type',
  is_scheduled          BOOLEAN   COMMENT 'True if unit is scheduled (must offer into dispatch).',
  is_semi_scheduled     BOOLEAN   COMMENT 'True if unit is semi-scheduled (VRE with variable dispatch)',
  is_non_scheduled      BOOLEAN   COMMENT 'True if unit is non-scheduled (embedded generation)',
  connection_point_id   STRING    COMMENT 'Network connection point identifier',
  commissioning_date    DATE      COMMENT 'Commercial commissioning date',
  deregistration_date   DATE      COMMENT 'Deregistration date (NULL if still active)',
  is_active             BOOLEAN   COMMENT 'True if unit is currently registered and active',
  _last_updated         TIMESTAMP COMMENT 'UTC timestamp when this registry record was last refreshed'
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Silver: AEMO generator registration data — dimension table mapping DUID to fuel type, region, and capacity.
         Updated when AEMO publishes new registration data (typically weekly).
         Used by generation_by_fuel rollup and agent get_generator_info tool.';

-- -----------------------------------------------------------------------------
-- silver.bids
-- Sourced from: bronze.nemweb_bids
-- Generator bid stacks (next-day published, T+1)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.bids (
  settlement_date       DATE      COMMENT 'Trading day for which bids apply',
  duid                  STRING    COMMENT 'Dispatchable Unit Identifier',
  bid_type              STRING    COMMENT 'Bid type: ENERGY, RAISE6SEC, RAISE60SEC, RAISE5MIN, RAISEREG, LOWER6SEC, LOWER60SEC, LOWER5MIN, LOWERREG',
  price_band_1          DOUBLE    COMMENT 'Price band 1 — lowest offer price ($/MWh)',
  price_band_2          DOUBLE    COMMENT 'Price band 2 ($/MWh)',
  price_band_3          DOUBLE    COMMENT 'Price band 3 ($/MWh)',
  price_band_4          DOUBLE    COMMENT 'Price band 4 ($/MWh)',
  price_band_5          DOUBLE    COMMENT 'Price band 5 ($/MWh)',
  price_band_6          DOUBLE    COMMENT 'Price band 6 ($/MWh)',
  price_band_7          DOUBLE    COMMENT 'Price band 7 ($/MWh)',
  price_band_8          DOUBLE    COMMENT 'Price band 8 ($/MWh)',
  price_band_9          DOUBLE    COMMENT 'Price band 9 ($/MWh)',
  price_band_10         DOUBLE    COMMENT 'Price band 10 — highest offer price ($/MWh)',
  avail_band_1          DOUBLE    COMMENT 'Available capacity in price band 1 (MW)',
  avail_band_2          DOUBLE    COMMENT 'Available capacity in price band 2 (MW)',
  avail_band_3          DOUBLE    COMMENT 'Available capacity in price band 3 (MW)',
  avail_band_4          DOUBLE    COMMENT 'Available capacity in price band 4 (MW)',
  avail_band_5          DOUBLE    COMMENT 'Available capacity in price band 5 (MW)',
  avail_band_6          DOUBLE    COMMENT 'Available capacity in price band 6 (MW)',
  avail_band_7          DOUBLE    COMMENT 'Available capacity in price band 7 (MW)',
  avail_band_8          DOUBLE    COMMENT 'Available capacity in price band 8 (MW)',
  avail_band_9          DOUBLE    COMMENT 'Available capacity in price band 9 (MW)',
  avail_band_10         DOUBLE    COMMENT 'Available capacity in price band 10 (MW)',
  total_avail_mw        DOUBLE    COMMENT 'Total available capacity across all bands (MW)',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: cleaned generator bid stack data (next-day published, publicly available).
         Published by AEMO with T+1 delay. Deduplicated on (settlement_date, duid, bid_type).';

-- -----------------------------------------------------------------------------
-- silver.weather_nem_regions
-- Sourced from: bronze.weather_raw — aggregated to NEM region level
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.weather_nem_regions (
  forecast_datetime       TIMESTAMP COMMENT 'Forecast target datetime (AEST/AEDT)',
  api_call_datetime       TIMESTAMP COMMENT 'Datetime when the Open-Meteo API call was made',
  nem_region              STRING    COMMENT 'NEM region identifier (NSW1, QLD1, VIC1, SA1, TAS1)',
  is_historical           BOOLEAN   COMMENT 'True if record is historical actuals; False if forecast',
  temperature_2m_c        DOUBLE    COMMENT 'Population-weighted average temperature across region (°C)',
  apparent_temp_c         DOUBLE    COMMENT 'Population-weighted apparent (feels-like) temperature (°C)',
  max_temp_c              DOUBLE    COMMENT 'Maximum temperature across all stations in region (°C)',
  wind_speed_10m_kmh      DOUBLE    COMMENT 'Average wind speed at 10m across region (km/h)',
  wind_speed_100m_kmh     DOUBLE    COMMENT 'Load-weighted average wind speed at 100m — proxy for wind generation potential (km/h)',
  max_wind_speed_100m_kmh DOUBLE    COMMENT 'Maximum wind speed at 100m across wind-farm representative stations (km/h)',
  solar_radiation_wm2     DOUBLE    COMMENT 'Average global horizontal irradiance across region (W/m²)',
  max_solar_radiation_wm2 DOUBLE    COMMENT 'Maximum solar radiation — peak solar generation indicator (W/m²)',
  cloud_cover_pct         DOUBLE    COMMENT 'Average cloud cover percentage (0-100)',
  precipitation_mm        DOUBLE    COMMENT 'Average precipitation (mm)',
  relative_humidity_pct   DOUBLE    COMMENT 'Average relative humidity (%)',
  heating_degree_days     DOUBLE    COMMENT 'Heating degree days relative to 18°C base — demand driver in winter',
  cooling_degree_days     DOUBLE    COMMENT 'Cooling degree days relative to 18°C base — demand driver in summer',
  station_count           INT       COMMENT 'Number of weather stations averaged for this region record',
  _processed_at           TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (CAST(forecast_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: Open-Meteo weather data aggregated to NEM region level.
         Multiple station coordinates per region aggregated using population/load weighting.
         Covers historical actuals and 7-day forecasts.';

-- -----------------------------------------------------------------------------
-- silver.solar_rooftop
-- Sourced from: bronze.apvi_raw — state aggregates mapped to NEM regions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.solar_rooftop (
  interval_datetime     TIMESTAMP COMMENT 'Interval end time (AEST/AEDT) — 30-minute intervals',
  nem_region            STRING    COMMENT 'NEM region identifier (NSW1, QLD1, VIC1, SA1, TAS1)',
  estimated_output_mw   DOUBLE    COMMENT 'Estimated rooftop PV output for the NEM region (MW)',
  installed_capacity_mw DOUBLE    COMMENT 'Total installed rooftop PV capacity in region (MW)',
  capacity_factor       DOUBLE    COMMENT 'Capacity factor — ratio of actual output to installed capacity (0-1)',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: APVI rooftop solar PV estimates aggregated to NEM region level (30-minute intervals).
         States mapped to NEM regions: NSW→NSW1, QLD→QLD1, VIC→VIC1, SA→SA1, TAS→TAS1.';

-- -----------------------------------------------------------------------------
-- silver.openelec_generation
-- Sourced from: bronze.openelec_raw — generation by fuel tech
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.openelec_generation (
  interval_start        TIMESTAMP COMMENT 'Interval start datetime (AEST/AEDT)',
  interval_end          TIMESTAMP COMMENT 'Interval end datetime (AEST/AEDT)',
  nem_region            STRING    COMMENT 'NEM region identifier (NSW1, QLD1, VIC1, SA1, TAS1)',
  fuel_tech             STRING    COMMENT 'OpenElectricity fuel technology label (coal_black, coal_brown, gas_ccgt, gas_ocgt, wind, solar_utility, solar_rooftop, hydro, battery_discharging, imports, etc.)',
  energy_gwh            DOUBLE    COMMENT 'Energy generated in interval (GWh)',
  power_mw              DOUBLE    COMMENT 'Average power output (MW)',
  emissions_tco2e       DOUBLE    COMMENT 'CO2-equivalent emissions in interval (tonnes)',
  market_value_aud      DOUBLE    COMMENT 'Market value of generation in interval (AUD)',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (CAST(interval_start AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: OpenElectricity API generation data by fuel technology and NEM region.
         Hourly resolution. Used as validation dataset against NEMWEB dispatch SCADA.';

-- -----------------------------------------------------------------------------
-- silver.openelec_prices
-- Sourced from: bronze.openelec_raw — price data from OpenElectricity
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.openelec_prices (
  interval_start        TIMESTAMP COMMENT 'Interval start datetime (AEST/AEDT)',
  interval_end          TIMESTAMP COMMENT 'Interval end datetime (AEST/AEDT)',
  nem_region            STRING    COMMENT 'NEM region identifier (NSW1, QLD1, VIC1, SA1, TAS1)',
  price_aud_mwh         DOUBLE    COMMENT 'Average electricity price for interval ($/MWh)',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Bronze record was processed to Silver'
)
USING DELTA
PARTITIONED BY (CAST(interval_start AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: OpenElectricity API price data per NEM region (hourly resolution).
         Used for historical backfill and cross-validation against nemweb_dispatch_price.';

-- -----------------------------------------------------------------------------
-- silver.generation_by_fuel
-- Sourced from: silver.dispatch_generation + silver.generator_registry
-- Aggregated view of generation by fuel type, region and interval
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.silver.generation_by_fuel (
  interval_datetime     TIMESTAMP COMMENT 'Dispatch interval end time (AEST/AEDT)',
  region_id             STRING    COMMENT 'NEM region identifier (NSW1, QLD1, VIC1, SA1, TAS1)',
  fuel_type             STRING    COMMENT 'Fuel type classification: coal_black, coal_brown, gas_ccgt, gas_ocgt, gas_steam, hydro, wind, solar_utility, battery, diesel, other',
  total_cleared_mw      DOUBLE    COMMENT 'Total MW cleared for this fuel type in region and interval',
  unit_count            INT       COMMENT 'Number of generating units of this fuel type dispatched in region',
  capacity_factor       DOUBLE    COMMENT 'Aggregate capacity factor for fuel type (actual / registered capacity)',
  emissions_intensity   DOUBLE    COMMENT 'Weighted average CO2e emissions intensity for fuel type (tonne CO2e / MWh)',
  total_emissions_t     DOUBLE    COMMENT 'Total CO2e emissions from this fuel type in interval (tonnes)',
  _processed_at         TIMESTAMP COMMENT 'UTC timestamp when Silver record was created'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Silver: generation output aggregated by fuel type and NEM region per 5-minute dispatch interval.
         Derived from silver.dispatch_generation joined to silver.generator_registry.
         Feeds gold.nem_generation_by_fuel after further cleaning.';


-- =============================================================================
-- GOLD LAYER — 16 tables
-- Analytics-ready tables serving the dashboard, Genie spaces, and ML models.
-- All high-volume time-series tables partitioned by CAST(interval_datetime AS DATE).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- gold.nem_prices_5min
-- Primary 5-minute price table for dashboard and agent tools
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.nem_prices_5min (
  interval_datetime     TIMESTAMP NOT NULL COMMENT 'Dispatch interval end time in AEST/AEDT (Australia/Sydney). 5-minute resolution. This is the primary timestamp for all price queries.',
  region_id             STRING    NOT NULL COMMENT 'NEM region: NSW1 (New South Wales), QLD1 (Queensland), VIC1 (Victoria), SA1 (South Australia), TAS1 (Tasmania)',
  rrp                   DOUBLE    NOT NULL COMMENT 'Regional Reference Price — spot electricity price for the dispatch interval ($/MWh). Normal range: -$1,000 to $500/MWh. Spikes can reach $15,500/MWh (market price cap).',
  rop                   DOUBLE    COMMENT 'Regional Over-constrained Dispatch price ($/MWh) — differs from RRP when the network is congested',
  total_demand_mw       DOUBLE    COMMENT 'Total scheduled demand in region (MW). NSW typically 7,000-14,000 MW; QLD 5,000-10,000 MW; VIC 4,000-9,000 MW; SA 1,000-3,000 MW; TAS 900-1,700 MW.',
  available_gen_mw      DOUBLE    COMMENT 'Total available generation in region (MW)',
  net_interchange_mw    DOUBLE    COMMENT 'Net interconnector flow into region (MW). Positive = net import. Negative = net export.',
  intervention          BOOLEAN   COMMENT 'True if AEMO Administered intervention pricing applied',
  apc_flag              BOOLEAN   COMMENT 'True if Administered Price Cap (APC) is in effect — prices cannot exceed APC threshold',
  market_suspended      BOOLEAN   COMMENT 'True if NEM market is suspended for this region',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true'
)
COMMENT 'Gold: 5-minute NEM spot electricity prices per region. Primary table for price queries, dashboards, and ML features.
         Key metrics: rrp (spot price), total_demand_mw, net_interchange_mw.
         Data freshness SLA: < 2 minutes from AEMO publish.
         Covers all 5 NEM regions at 5-minute resolution.';

-- -----------------------------------------------------------------------------
-- gold.nem_prices_30min
-- 30-minute settlement prices (used for billing and historical analysis)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.nem_prices_30min (
  interval_datetime     TIMESTAMP NOT NULL COMMENT 'Trading interval end time in AEST/AEDT (Australia/Sydney). 30-minute resolution. Used for settlement pricing.',
  region_id             STRING    NOT NULL COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
  rrp                   DOUBLE    NOT NULL COMMENT 'Settlement price — average of 6 dispatch interval RRPs ($/MWh). This is the price used for energy billing.',
  rrp_min               DOUBLE    COMMENT 'Minimum 5-min RRP within the trading interval ($/MWh)',
  rrp_max               DOUBLE    COMMENT 'Maximum 5-min RRP within the trading interval ($/MWh)',
  rrp_stddev            DOUBLE    COMMENT 'Standard deviation of 5-min RRPs within the trading interval — measure of price volatility',
  spike_count           INT       COMMENT 'Number of 5-min intervals within the trading period where RRP exceeded $300/MWh',
  total_demand_mw       DOUBLE    COMMENT 'Average demand over trading interval (MW)',
  net_interchange_mw    DOUBLE    COMMENT 'Average net interconnector import over trading interval (MW)',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true'
)
COMMENT 'Gold: 30-minute NEM settlement prices per region. Aggregated from 5-minute dispatch prices.
         rrp is the settlement price used for energy billing (average of 6 dispatch intervals).
         rrp_max and spike_count useful for identifying intra-interval price spikes.';

-- -----------------------------------------------------------------------------
-- gold.nem_generation_by_fuel
-- Generation mix by fuel type, region and 5-min interval
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.nem_generation_by_fuel (
  interval_datetime     TIMESTAMP NOT NULL COMMENT 'Dispatch interval end time in AEST/AEDT. 5-minute resolution.',
  region_id             STRING    NOT NULL COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
  fuel_type             STRING    NOT NULL COMMENT 'Fuel/technology type: coal_black, coal_brown, gas_ccgt, gas_ocgt, gas_steam, hydro, wind, solar_utility, solar_rooftop, battery_charging, battery_discharging, diesel, pumps, other',
  total_mw              DOUBLE    COMMENT 'Total generation output for fuel type in region and interval (MW)',
  unit_count            INT       COMMENT 'Number of generating units of this fuel type dispatched',
  capacity_factor       DOUBLE    COMMENT 'Capacity factor (actual MW / installed capacity MW). Range 0-1.',
  emissions_tco2e       DOUBLE    COMMENT 'CO2-equivalent emissions for this fuel type in this interval (tonnes CO2e)',
  emissions_intensity   DOUBLE    COMMENT 'Emissions intensity weighted by output (tonne CO2e / MWh)',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: NEM generation output by fuel type and region at 5-minute resolution.
         Key table for fuel mix charts and generation stack analysis.
         Rooftop solar added from APVI data (silver.solar_rooftop) as a separate fuel_type row.
         Emissions data derived from CO2 intensity coefficients in silver.generator_registry.';

-- -----------------------------------------------------------------------------
-- gold.nem_interconnectors
-- Interconnector flows at 5-min resolution
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.nem_interconnectors (
  interval_datetime     TIMESTAMP NOT NULL COMMENT 'Dispatch interval end time in AEST/AEDT. 5-minute resolution.',
  interconnector_id     STRING    NOT NULL COMMENT 'Interconnector ID: NSW1-QLD1 (QNI), VIC1-NSW1 (VNI), VIC1-SA1 (Heywood), VIC1-TAS1 (Basslink), SA1-VIC1 (Murraylink)',
  from_region           STRING    COMMENT 'Region that exports power in forward direction',
  to_region             STRING    COMMENT 'Region that receives power in forward direction',
  mw_flow               DOUBLE    COMMENT 'MW flow — positive = forward (from_region to to_region), negative = reverse',
  mw_losses             DOUBLE    COMMENT 'Transmission losses (MW)',
  export_limit_mw       DOUBLE    COMMENT 'Forward direction MW limit',
  import_limit_mw       DOUBLE    COMMENT 'Reverse direction MW limit (shown as positive)',
  utilization_pct       DOUBLE    COMMENT 'Utilisation percentage — abs(mw_flow) / applicable_limit * 100. Values near 100% indicate congestion.',
  is_congested          BOOLEAN   COMMENT 'True if interconnector is at or near its physical limit (utilization > 95%)',
  marginal_value        DOUBLE    COMMENT 'Shadow price of binding interconnector limit ($/MWh). Non-zero when congested.',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: NEM interconnector flows and utilisation at 5-minute resolution.
         Key table for interconnector flow diagrams and congestion analysis.
         is_congested flag useful for filtering to constrained periods.';

-- -----------------------------------------------------------------------------
-- gold.nem_constraints_active
-- Binding generic constraints per dispatch interval
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.nem_constraints_active (
  interval_datetime     TIMESTAMP NOT NULL COMMENT 'Dispatch interval end time in AEST/AEDT',
  constraint_id         STRING    NOT NULL COMMENT 'AEMO generic constraint equation identifier. Examples: N^^NIL_FCSPS, V_MSLOAD_LIM, S_RAMP_AVOID, Q_LEAKY',
  marginal_value        DOUBLE    COMMENT 'Shadow price of binding constraint ($/MWh). A non-zero marginal value means this constraint is actively limiting dispatch.',
  violation_degree      DOUBLE    COMMENT 'Constraint violation degree (MW). Non-zero values indicate a constraint violation (unusual under normal operation).',
  rhs                   DOUBLE    COMMENT 'Right-hand side limit of constraint equation',
  affected_regions      ARRAY<STRING> COMMENT 'NEM regions affected by this constraint (derived from constraint ID prefix)',
  constraint_type       STRING    COMMENT 'Constraint category: NETWORK, FCAS, VOLTAGE, THERMAL, STABILITY',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: binding generic constraints per dispatch interval (marginal_value != 0).
         Only truly binding constraints (marginal_value != 0) are included.
         Used by explain_price_event agent tool to correlate price spikes with network constraints.';

-- -----------------------------------------------------------------------------
-- gold.nem_fcas_prices
-- FCAS ancillary service prices — 8 services × 5 regions × 5-min
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.nem_fcas_prices (
  interval_datetime     TIMESTAMP NOT NULL COMMENT 'Dispatch interval end time in AEST/AEDT. 5-minute resolution.',
  region_id             STRING    NOT NULL COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
  service               STRING    NOT NULL COMMENT 'FCAS service type: RAISE6SEC, RAISE60SEC, RAISE5MIN, RAISEREG, LOWER6SEC, LOWER60SEC, LOWER5MIN, LOWERREG',
  rrp                   DOUBLE    COMMENT 'FCAS service price for this region and interval ($/MWh or $/MW). Often much lower than energy price but can spike during frequency events.',
  requirement_mw        DOUBLE    COMMENT 'AEMO requirement for this FCAS service in this region (MW)',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: FCAS (Frequency Control Ancillary Services) prices per service, region and 5-minute interval.
         8 FCAS services: RAISE6SEC, RAISE60SEC, RAISE5MIN, RAISEREG (frequency raise),
                          LOWER6SEC, LOWER60SEC, LOWER5MIN, LOWERREG (frequency lower).
         FCAS prices spike during frequency events or unit outages.
         Sourced from silver.dispatch_prices (DispatchIS FCAS sub-tables).';

-- -----------------------------------------------------------------------------
-- gold.demand_actuals
-- Actual demand by region — consolidated from multiple sources
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.demand_actuals (
  interval_datetime     TIMESTAMP NOT NULL COMMENT 'Dispatch interval end time in AEST/AEDT. 5-minute resolution.',
  region_id             STRING    NOT NULL COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
  total_demand_mw       DOUBLE    COMMENT 'Total scheduled + semi-scheduled demand (MW). This is the actual operational demand served by the grid.',
  scheduled_demand_mw   DOUBLE    COMMENT 'Scheduled demand component (MW) — excludes rooftop solar and non-scheduled generation',
  net_demand_mw         DOUBLE    COMMENT 'Net demand = total_demand_mw - solar_rooftop_mw. This is the "duck curve" demand that dispatchable generators must serve.',
  solar_rooftop_mw      DOUBLE    COMMENT 'Estimated rooftop solar contribution subtracted from gross demand (MW). From APVI data.',
  temperature_c         DOUBLE    COMMENT 'Region representative temperature at this interval (°C) — key demand driver',
  is_peak               BOOLEAN   COMMENT 'True if this interval is in the morning (07:00-09:00) or evening (17:00-20:00) peak period',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: actual electricity demand per NEM region at 5-minute resolution.
         Net demand (total minus rooftop solar) is the key figure for dispatchable generation requirements.
         Temperature included as primary demand driver for analysis and ML features.';

-- -----------------------------------------------------------------------------
-- gold.weather_nem_regions
-- Weather actuals and forecasts at NEM region level (Gold copy)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.weather_nem_regions (
  forecast_datetime       TIMESTAMP NOT NULL COMMENT 'Forecast target datetime in AEST/AEDT',
  api_call_datetime       TIMESTAMP COMMENT 'Datetime when the weather API call was made (freshness indicator)',
  nem_region              STRING    NOT NULL COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
  is_historical           BOOLEAN   COMMENT 'True if record is observed historical actuals; False if forecast',
  temperature_c           DOUBLE    COMMENT 'Population-weighted average temperature (°C). Primary driver of electricity demand.',
  apparent_temp_c         DOUBLE    COMMENT 'Apparent (feels-like) temperature (°C) — often more correlated with air conditioning demand than actual temperature',
  max_temp_c              DOUBLE    COMMENT 'Maximum temperature in region (°C) — used for extreme demand event prediction',
  wind_speed_100m_kmh     DOUBLE    COMMENT 'Average wind speed at 100m height (km/h) — primary wind generation proxy. Used in wind forecast model.',
  solar_radiation_wm2     DOUBLE    COMMENT 'Global horizontal irradiance (W/m²) — primary solar generation proxy',
  cloud_cover_pct         DOUBLE    COMMENT 'Cloud cover percentage (0-100%) — inverter of solar generation potential',
  heating_degree_days     DOUBLE    COMMENT 'Heating degree days (base 18°C) — proxy for heating demand',
  cooling_degree_days     DOUBLE    COMMENT 'Cooling degree days (base 18°C) — proxy for cooling/air conditioning demand',
  _updated_at             TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(forecast_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: weather data and forecasts at NEM region level.
         Covers both historical actuals and 7-day Open-Meteo forecasts.
         Key ML features: temperature_c, wind_speed_100m_kmh, solar_radiation_wm2.
         Used by get_weather_forecast agent tool and demand/generation forecast models.';

-- -----------------------------------------------------------------------------
-- gold.price_forecasts
-- ML model price forecasts — multi-horizon output
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.price_forecasts (
  forecast_run_at       TIMESTAMP NOT NULL COMMENT 'UTC datetime when the ML inference pipeline ran and produced this forecast',
  interval_datetime     TIMESTAMP NOT NULL COMMENT 'Target interval being forecast (AEST/AEDT)',
  region_id             STRING    NOT NULL COMMENT 'NEM region being forecast: NSW1, QLD1, VIC1, SA1, TAS1',
  horizon_intervals     INT       NOT NULL COMMENT 'Forecast horizon in 5-minute intervals ahead (e.g. 12 = 1 hour, 48 = 4 hours, 288 = 24 hours)',
  predicted_rrp         DOUBLE    COMMENT 'Model-predicted Regional Reference Price ($/MWh)',
  prediction_lower_80   DOUBLE    COMMENT 'Lower bound of 80% prediction interval ($/MWh)',
  prediction_upper_80   DOUBLE    COMMENT 'Upper bound of 80% prediction interval ($/MWh)',
  spike_probability     DOUBLE    COMMENT 'Probability of a price spike (>$300/MWh) in this interval (0-1)',
  model_version         STRING    COMMENT 'MLflow model version used for this forecast run',
  model_name            STRING    COMMENT 'MLflow registered model name (e.g. price_forecast_nsw1)',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true'
)
COMMENT 'Gold: ML price forecasts per NEM region across multiple horizons (1-hr, 4-hr, 24-hr).
         LightGBM models trained on 18 months of history, retrained weekly.
         spike_probability is the key field for predictive alerting.
         Performance target: MAE < $15/MWh at 1-hr horizon (excluding >$500/MWh events).';

-- -----------------------------------------------------------------------------
-- gold.demand_forecasts
-- ML model demand forecasts — multi-horizon output
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.demand_forecasts (
  forecast_run_at       TIMESTAMP NOT NULL COMMENT 'UTC datetime when the ML inference pipeline ran',
  interval_datetime     TIMESTAMP NOT NULL COMMENT 'Target interval being forecast (AEST/AEDT)',
  region_id             STRING    NOT NULL COMMENT 'NEM region being forecast: NSW1, QLD1, VIC1, SA1, TAS1',
  horizon_intervals     INT       NOT NULL COMMENT 'Forecast horizon in 5-minute intervals ahead',
  predicted_demand_mw   DOUBLE    COMMENT 'Predicted total scheduled demand (MW)',
  prediction_lower_80   DOUBLE    COMMENT 'Lower bound of 80% prediction interval (MW)',
  prediction_upper_80   DOUBLE    COMMENT 'Upper bound of 80% prediction interval (MW)',
  model_version         STRING    COMMENT 'MLflow model version used for this forecast run',
  model_name            STRING    COMMENT 'MLflow registered model name (e.g. demand_forecast_vic1)',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: ML demand forecasts per NEM region at multiple horizons.
         LightGBM models incorporating weather, calendar, and historical demand features.
         Performance target: MAPE < 3% at 1-hour horizon.';

-- -----------------------------------------------------------------------------
-- gold.generation_forecasts
-- ML model generation forecasts — wind and solar by region
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.generation_forecasts (
  forecast_run_at       TIMESTAMP NOT NULL COMMENT 'UTC datetime when the ML inference pipeline ran',
  interval_datetime     TIMESTAMP NOT NULL COMMENT 'Target interval being forecast (AEST/AEDT)',
  region_id             STRING    NOT NULL COMMENT 'NEM region being forecast: NSW1, QLD1, VIC1, SA1, TAS1',
  fuel_type             STRING    NOT NULL COMMENT 'Generation type being forecast: wind or solar_utility',
  horizon_intervals     INT       NOT NULL COMMENT 'Forecast horizon in 5-minute intervals ahead',
  predicted_mw          DOUBLE    COMMENT 'Predicted generation output (MW)',
  prediction_lower_80   DOUBLE    COMMENT 'Lower bound of 80% prediction interval (MW)',
  prediction_upper_80   DOUBLE    COMMENT 'Upper bound of 80% prediction interval (MW)',
  model_version         STRING    COMMENT 'MLflow model version used for this forecast run',
  model_name            STRING    COMMENT 'MLflow registered model name',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: ML wind and solar generation forecasts per NEM region at multiple horizons.
         LightGBM models driven primarily by Open-Meteo weather forecasts.
         Covers wind and solar_utility types; rooftop solar is handled separately via APVI.';

-- -----------------------------------------------------------------------------
-- gold.nem_daily_summary
-- Daily aggregated NEM metrics — one row per region per day
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.nem_daily_summary (
  trading_date          DATE      NOT NULL COMMENT 'NEM trading date (AEST, market day runs 04:05 to 04:00 next day)',
  region_id             STRING    NOT NULL COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
  avg_price_aud_mwh     DOUBLE    COMMENT 'Volume-weighted average price for the trading day ($/MWh)',
  min_price_aud_mwh     DOUBLE    COMMENT 'Minimum 5-min price in trading day ($/MWh)',
  max_price_aud_mwh     DOUBLE    COMMENT 'Maximum 5-min price in trading day ($/MWh) — key spike indicator',
  price_spike_count     INT       COMMENT 'Number of 5-min intervals with RRP > $300/MWh',
  price_negative_count  INT       COMMENT 'Number of 5-min intervals with RRP < $0/MWh (negative prices indicate excess supply)',
  avg_demand_mw         DOUBLE    COMMENT 'Average demand over trading day (MW)',
  peak_demand_mw        DOUBLE    COMMENT 'Maximum demand in trading day (MW) — used for capacity planning',
  total_energy_gwh      DOUBLE    COMMENT 'Total energy consumed in trading day (GWh)',
  coal_pct              DOUBLE    COMMENT 'Coal (black + brown) share of generation (%)',
  gas_pct               DOUBLE    COMMENT 'Gas (CCGT + OCGT + steam) share of generation (%)',
  wind_pct              DOUBLE    COMMENT 'Wind generation share (%)',
  solar_utility_pct     DOUBLE    COMMENT 'Utility-scale solar share (%)',
  solar_rooftop_pct     DOUBLE    COMMENT 'Rooftop solar share (%)',
  hydro_pct             DOUBLE    COMMENT 'Hydro generation share (%)',
  battery_pct           DOUBLE    COMMENT 'Battery storage share (%)',
  renewables_pct        DOUBLE    COMMENT 'Total renewables share = wind + solar_utility + solar_rooftop + hydro (%)',
  avg_emissions_intensity DOUBLE  COMMENT 'Average grid emissions intensity for the day (tonne CO2e / MWh)',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (trading_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: daily NEM market summary per region.
         One row per (trading_date, region_id). Used in daily market analysis and Genie spaces.
         renewables_pct and avg_emissions_intensity are key decarbonisation tracking metrics.';

-- -----------------------------------------------------------------------------
-- gold.feature_store_price
-- ML feature store — price forecast training and inference features
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.feature_store_price (
  interval_datetime         TIMESTAMP NOT NULL COMMENT 'Feature timestamp (AEST/AEDT) — primary key for feature lookup',
  region_id                 STRING    NOT NULL COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
  -- Temporal features
  hour_of_day               INT       COMMENT 'Hour of day 0-23 (AEST)',
  day_of_week               INT       COMMENT 'Day of week 0-6 (0=Monday, 6=Sunday)',
  month                     INT       COMMENT 'Month 1-12',
  is_weekend                BOOLEAN   COMMENT 'True if Saturday or Sunday',
  is_public_holiday         BOOLEAN   COMMENT 'True if Australian public holiday (state-specific)',
  season                    STRING    COMMENT 'Season: summer (Dec-Feb), autumn (Mar-May), winter (Jun-Aug), spring (Sep-Nov)',
  minutes_since_midnight    INT       COMMENT 'Minutes elapsed since midnight AEST (continuous temporal feature)',
  -- Price lag features
  rrp_lag_1                 DOUBLE    COMMENT 'RRP 5 minutes ago ($/MWh)',
  rrp_lag_6                 DOUBLE    COMMENT 'RRP 30 minutes ago ($/MWh)',
  rrp_lag_12                DOUBLE    COMMENT 'RRP 1 hour ago ($/MWh)',
  rrp_lag_288               DOUBLE    COMMENT 'RRP 24 hours ago ($/MWh)',
  rrp_lag_2016              DOUBLE    COMMENT 'RRP 7 days ago ($/MWh)',
  rrp_rolling_mean_1hr      DOUBLE    COMMENT 'Rolling mean RRP over last 1 hour ($/MWh)',
  rrp_rolling_mean_4hr      DOUBLE    COMMENT 'Rolling mean RRP over last 4 hours ($/MWh)',
  rrp_rolling_mean_24hr     DOUBLE    COMMENT 'Rolling mean RRP over last 24 hours ($/MWh)',
  rrp_rolling_std_1hr       DOUBLE    COMMENT 'Rolling standard deviation of RRP over last 1 hour — price volatility ($/MWh)',
  rrp_rolling_std_24hr      DOUBLE    COMMENT 'Rolling standard deviation of RRP over last 24 hours ($/MWh)',
  -- Demand features
  demand_lag_1              DOUBLE    COMMENT 'Demand 5 minutes ago (MW)',
  demand_lag_288            DOUBLE    COMMENT 'Demand 24 hours ago (MW)',
  demand_rolling_mean_1hr   DOUBLE    COMMENT 'Rolling mean demand over last 1 hour (MW)',
  -- Generation features
  wind_mw_lag_1             DOUBLE    COMMENT 'Wind generation 5 minutes ago (MW)',
  solar_mw_lag_1            DOUBLE    COMMENT 'Solar generation 5 minutes ago (MW)',
  coal_mw_lag_1             DOUBLE    COMMENT 'Coal generation 5 minutes ago (MW)',
  -- Weather features
  temperature_c             DOUBLE    COMMENT 'Region temperature at this interval (°C)',
  wind_speed_100m_kmh       DOUBLE    COMMENT 'Wind speed at 100m (km/h) — wind generation proxy',
  solar_radiation_wm2       DOUBLE    COMMENT 'Solar radiation (W/m²) — solar generation proxy',
  -- Cross-regional features
  price_spread_max          DOUBLE    COMMENT 'Maximum RRP spread between any two connected NEM regions ($/MWh)',
  nem_total_demand_mw       DOUBLE    COMMENT 'Total NEM-wide scheduled demand (sum of 5 regions, MW)',
  -- Predispatch baseline
  predispatch_rrp_1hr       DOUBLE    COMMENT 'AEMO pre-dispatch forecast price 1 hour ahead ($/MWh) — baseline comparison',
  _feature_created_at       TIMESTAMP COMMENT 'UTC timestamp when feature row was computed'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: ML feature store for price forecast models.
         One row per (interval_datetime, region_id). Used for both training and live inference.
         Features include lagged prices, demand, generation, weather, and calendar variables.
         Updated every 5 minutes by the forecast pipeline.';

-- -----------------------------------------------------------------------------
-- gold.feature_store_demand
-- ML feature store — demand forecast training and inference features
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.feature_store_demand (
  interval_datetime         TIMESTAMP NOT NULL COMMENT 'Feature timestamp (AEST/AEDT)',
  region_id                 STRING    NOT NULL COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
  -- Temporal features (same as price feature store)
  hour_of_day               INT       COMMENT 'Hour of day 0-23 (AEST)',
  day_of_week               INT       COMMENT 'Day of week 0-6 (0=Monday)',
  month                     INT       COMMENT 'Month 1-12',
  is_weekend                BOOLEAN   COMMENT 'True if Saturday or Sunday',
  is_public_holiday         BOOLEAN   COMMENT 'True if Australian public holiday (state-specific)',
  season                    STRING    COMMENT 'Season: summer, autumn, winter, spring',
  -- Demand lag features
  demand_lag_1              DOUBLE    COMMENT 'Demand 5 minutes ago (MW)',
  demand_lag_6              DOUBLE    COMMENT 'Demand 30 minutes ago (MW)',
  demand_lag_12             DOUBLE    COMMENT 'Demand 1 hour ago (MW)',
  demand_lag_288            DOUBLE    COMMENT 'Demand 24 hours ago (MW)',
  demand_lag_2016           DOUBLE    COMMENT 'Demand 7 days ago (MW)',
  demand_rolling_mean_1hr   DOUBLE    COMMENT 'Rolling mean demand over last 1 hour (MW)',
  demand_rolling_mean_4hr   DOUBLE    COMMENT 'Rolling mean demand over last 4 hours (MW)',
  demand_rolling_mean_24hr  DOUBLE    COMMENT 'Rolling mean demand over last 24 hours (MW)',
  demand_rolling_std_24hr   DOUBLE    COMMENT 'Rolling standard deviation of demand over last 24 hours (MW)',
  -- Weather features (key demand drivers)
  temperature_c             DOUBLE    COMMENT 'Region temperature at interval (°C) — strongest demand predictor',
  apparent_temp_c           DOUBLE    COMMENT 'Apparent temperature (°C) — often more correlated with demand than actual temp',
  temperature_forecast_1hr  DOUBLE    COMMENT 'Temperature forecast 1 hour ahead (°C) — from Open-Meteo',
  temperature_forecast_4hr  DOUBLE    COMMENT 'Temperature forecast 4 hours ahead (°C)',
  temperature_forecast_24hr DOUBLE    COMMENT 'Temperature forecast 24 hours ahead (°C)',
  heating_degree_days       DOUBLE    COMMENT 'Heating degree days today (base 18°C)',
  cooling_degree_days       DOUBLE    COMMENT 'Cooling degree days today (base 18°C)',
  -- Solar subtraction feature
  solar_rooftop_mw          DOUBLE    COMMENT 'Estimated rooftop solar output (MW) — reduces net demand',
  -- Cross-regional
  nem_total_demand_lag_1    DOUBLE    COMMENT 'Total NEM-wide demand 5 minutes ago (MW)',
  _feature_created_at       TIMESTAMP COMMENT 'UTC timestamp when feature row was computed'
)
USING DELTA
PARTITIONED BY (CAST(interval_datetime AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: ML feature store for demand forecast models.
         Temperature and weather forecasts are the primary drivers.
         Used for both training and live inference by the demand_forecast pipeline.';

-- -----------------------------------------------------------------------------
-- gold.daily_market_summary
-- AI-generated daily market narrative — one row per region per day
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.daily_market_summary (
  trading_date          DATE      NOT NULL COMMENT 'NEM trading date (AEST)',
  region_id             STRING    NOT NULL COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1 (or ALL for NEM-wide summary)',
  summary_text          STRING    COMMENT 'AI-generated narrative summary of market conditions for the day (generated by Claude Sonnet 4.5 using market data tools)',
  key_events            ARRAY<STRING> COMMENT 'List of notable market events identified for the day (e.g. price spike at 15:30, constraint binding, unit outage)',
  avg_price_aud_mwh     DOUBLE    COMMENT 'Daily volume-weighted average price ($/MWh) — headline metric in summary',
  max_price_aud_mwh     DOUBLE    COMMENT 'Daily maximum price ($/MWh)',
  price_spike_count     INT       COMMENT 'Number of price spike intervals (>$300/MWh)',
  renewables_pct        DOUBLE    COMMENT 'Renewables share of generation for the day (%)',
  generated_at          TIMESTAMP COMMENT 'UTC datetime when this summary was generated by the AI pipeline',
  model_used            STRING    COMMENT 'LLM model version used to generate summary (e.g. claude-sonnet-4-5)',
  _updated_at           TIMESTAMP COMMENT 'UTC timestamp when this Gold record was last updated'
)
USING DELTA
PARTITIONED BY (trading_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Gold: AI-generated daily NEM market summaries (text narratives).
         Generated by Pipeline 6 (market_summary) using Claude Sonnet 4.5 and market data tools.
         Published by 06:00 AEST each morning.
         Served by get_market_summary agent tool.';

-- -----------------------------------------------------------------------------
-- gold.anomaly_events
-- Detected price spikes, demand surges and data anomalies
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.anomaly_events (
  event_id              STRING    NOT NULL COMMENT 'Unique event identifier (UUID)',
  detected_at           TIMESTAMP NOT NULL COMMENT 'UTC datetime when anomaly was first detected',
  interval_datetime     TIMESTAMP COMMENT 'NEM interval datetime associated with the event (AEST/AEDT)',
  region_id             STRING    COMMENT 'NEM region where anomaly occurred (NULL if multi-region)',
  event_type            STRING    COMMENT 'Anomaly type: PRICE_SPIKE, PRICE_NEGATIVE, DEMAND_SURGE, DEMAND_DROP, DATA_MISSING, DATA_STALE, MODEL_DRIFT',
  severity              STRING    COMMENT 'Event severity: LOW, MEDIUM, HIGH, CRITICAL',
  metric_value          DOUBLE    COMMENT 'Observed value that triggered the anomaly ($/MWh for prices, MW for demand)',
  threshold_value       DOUBLE    COMMENT 'Threshold that was exceeded to trigger this anomaly',
  description           STRING    COMMENT 'Human-readable description of the anomaly event',
  is_resolved           BOOLEAN   COMMENT 'True if the anomaly condition has returned to normal',
  resolved_at           TIMESTAMP COMMENT 'UTC datetime when event was resolved (NULL if still active)',
  alert_fired           BOOLEAN   COMMENT 'True if a Databricks SQL Alert notification was sent for this event',
  _created_at           TIMESTAMP COMMENT 'UTC timestamp when this record was inserted'
)
USING DELTA
PARTITIONED BY (CAST(detected_at AS DATE))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true'
)
COMMENT 'Gold: detected anomaly and alert events from the anomaly detection model and rule-based monitors.
         Includes price spikes, demand surges, data quality issues and model drift signals.
         Used by the Alerts tab in the dashboard and by SQL alert queries.';

-- -----------------------------------------------------------------------------
-- gold.forward_curves
-- E1 Enhancement: Forward curve snapshots bootstrapped from ASX futures
-- Stores versioned curves for all NEM regions and profiles (FLAT/PEAK/OFF_PEAK)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_copilot.gold.forward_curves (
  curve_id        STRING    NOT NULL COMMENT 'Unique identifier for this curve build',
  curve_date      DATE      NOT NULL COMMENT 'Date the curve was built',
  region          STRING    NOT NULL COMMENT 'NEM region: NSW1, QLD1, VIC1, SA1, TAS1',
  profile         STRING    NOT NULL COMMENT 'Load profile: FLAT, PEAK, OFF_PEAK',
  quarter         STRING    NOT NULL COMMENT 'Quarter label e.g. Q1_2026',
  month           STRING    NOT NULL COMMENT 'Month label e.g. 2026-01',
  price_mwh       DOUBLE    NOT NULL COMMENT 'Forward price in $/MWh',
  source          STRING    NOT NULL COMMENT 'Data provenance: ASX_BOOTSTRAP, SHAPED, EXTRAPOLATED, ILLUSTRATIVE',
  model_version   STRING             COMMENT 'Curve engine version',
  created_at      TIMESTAMP          COMMENT 'UTC timestamp when this row was inserted'
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed'       = 'true'
)
COMMENT 'Gold: forward electricity price curves bootstrapped from ASX futures data.
         Quarterly prices decomposed to monthly with NEM seasonal shape factors and peak/off-peak shaping.
         Used by /api/curves/* endpoints, ForwardCurves.tsx dashboard, and Copilot get_forward_curve tool.';
