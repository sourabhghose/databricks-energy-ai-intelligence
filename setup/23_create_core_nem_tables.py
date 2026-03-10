# Databricks notebook source
# MAGIC %md
# MAGIC # Create Core NEM Gold Tables
# MAGIC
# MAGIC Creates the foundational NEM market data tables that the backfill script
# MAGIC (`11_historical_backfill.py`) and live pipelines write into.
# MAGIC These were originally in `02_create_tables_legacy.sql` but that file
# MAGIC used the wrong catalog name and was never added to `jobs.yml`.
# MAGIC
# MAGIC **MUST run BEFORE the backfill task.**

# COMMAND ----------

try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "energy_copilot_catalog"

SCHEMA = f"{CATALOG}.gold"
APP_SP = "67aaaa6b-778c-4c8b-b2f0-9f9b9728b3bb"

spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. nem_prices_5min — Core price table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.nem_prices_5min (
    interval_datetime   TIMESTAMP NOT NULL,
    region_id           STRING    NOT NULL,
    rrp                 DOUBLE    NOT NULL,
    rop                 DOUBLE,
    total_demand_mw     DOUBLE,
    available_gen_mw    DOUBLE,
    net_interchange_mw  DOUBLE,
    intervention        BOOLEAN,
    apc_flag            BOOLEAN,
    market_suspended    BOOLEAN,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
""")
print(f"Created {SCHEMA}.nem_prices_5min")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. nem_generation_by_fuel

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.nem_generation_by_fuel (
    interval_datetime   TIMESTAMP NOT NULL,
    region_id           STRING    NOT NULL,
    fuel_type           STRING    NOT NULL,
    total_mw            DOUBLE,
    unit_count          INT,
    capacity_factor     DOUBLE,
    emissions_tco2e     DOUBLE,
    emissions_intensity DOUBLE,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.nem_generation_by_fuel")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. nem_interconnectors

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.nem_interconnectors (
    interval_datetime   TIMESTAMP NOT NULL,
    interconnector_id   STRING    NOT NULL,
    from_region         STRING,
    to_region           STRING,
    mw_flow             DOUBLE,
    mw_losses           DOUBLE,
    export_limit_mw     DOUBLE,
    import_limit_mw     DOUBLE,
    utilization_pct     DOUBLE,
    is_congested        BOOLEAN,
    marginal_value      DOUBLE,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.nem_interconnectors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. demand_actuals

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.demand_actuals (
    interval_datetime   TIMESTAMP NOT NULL,
    region_id           STRING    NOT NULL,
    total_demand_mw     DOUBLE,
    scheduled_demand_mw DOUBLE,
    net_demand_mw       DOUBLE,
    solar_rooftop_mw    DOUBLE,
    temperature_c       DOUBLE,
    is_peak             BOOLEAN,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.demand_actuals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. weather_nem_regions

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.weather_nem_regions (
    forecast_datetime     TIMESTAMP NOT NULL,
    api_call_datetime     TIMESTAMP,
    nem_region            STRING    NOT NULL,
    is_historical         BOOLEAN,
    temperature_c         DOUBLE,
    apparent_temp_c       DOUBLE,
    max_temp_c            DOUBLE,
    wind_speed_100m_kmh   DOUBLE,
    solar_radiation_wm2   DOUBLE,
    cloud_cover_pct       DOUBLE,
    heating_degree_days   DOUBLE,
    cooling_degree_days   DOUBLE,
    _updated_at           TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.weather_nem_regions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. anomaly_events

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.anomaly_events (
    event_id            STRING    NOT NULL,
    detected_at         TIMESTAMP NOT NULL,
    interval_datetime   TIMESTAMP,
    region_id           STRING,
    event_type          STRING,
    severity            STRING,
    metric_value        DOUBLE,
    threshold_value     DOUBLE,
    description         STRING,
    is_resolved         BOOLEAN,
    resolved_at         TIMESTAMP,
    alert_fired         BOOLEAN,
    _created_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
""")
print(f"Created {SCHEMA}.anomaly_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. price_forecasts

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.price_forecasts (
    forecast_run_at     TIMESTAMP NOT NULL,
    interval_datetime   TIMESTAMP NOT NULL,
    region_id           STRING    NOT NULL,
    horizon_intervals   INT       NOT NULL,
    predicted_rrp       DOUBLE,
    prediction_lower_80 DOUBLE,
    prediction_upper_80 DOUBLE,
    spike_probability   DOUBLE,
    model_version       STRING,
    model_name          STRING,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
""")
print(f"Created {SCHEMA}.price_forecasts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. demand_forecasts

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.demand_forecasts (
    forecast_run_at     TIMESTAMP NOT NULL,
    interval_datetime   TIMESTAMP NOT NULL,
    region_id           STRING    NOT NULL,
    horizon_intervals   INT       NOT NULL,
    predicted_demand_mw DOUBLE,
    prediction_lower_80 DOUBLE,
    prediction_upper_80 DOUBLE,
    model_version       STRING,
    model_name          STRING,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.demand_forecasts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. nem_daily_summary

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.nem_daily_summary (
    trading_date          DATE      NOT NULL,
    region_id             STRING    NOT NULL,
    avg_price_aud_mwh     DOUBLE,
    min_price_aud_mwh     DOUBLE,
    max_price_aud_mwh     DOUBLE,
    price_spike_count     INT,
    price_negative_count  INT,
    avg_demand_mw         DOUBLE,
    peak_demand_mw        DOUBLE,
    total_energy_gwh      DOUBLE,
    renewables_pct        DOUBLE,
    avg_emissions_intensity DOUBLE,
    _updated_at           TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.nem_daily_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. nem_facilities

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.nem_facilities (
    duid                STRING NOT NULL,
    station_name        STRING,
    region_id           STRING,
    fuel_type           STRING,
    technology_type     STRING,
    max_capacity_mw     DOUBLE,
    registered_capacity_mw DOUBLE,
    dispatch_type       STRING,
    classification      STRING,
    connection_point_id STRING,
    participant_id      STRING,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.nem_facilities")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. nem_bid_stack

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.nem_bid_stack (
    interval_datetime   TIMESTAMP NOT NULL,
    duid                STRING    NOT NULL,
    region_id           STRING,
    fuel_type           STRING,
    band_no             INT,
    band_avail_mw       DOUBLE,
    price_band          DOUBLE,
    total_cleared_mw    DOUBLE,
    max_avail_mw        DOUBLE,
    rebid_category      STRING,
    rebid_reason        STRING,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.nem_bid_stack")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. dashboard_snapshots

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.dashboard_snapshots (
    endpoint_path       STRING NOT NULL,
    region              STRING NOT NULL,
    payload_json        STRING,
    generated_at        TIMESTAMP,
    ttl_seconds         INT,
    CONSTRAINT dashboard_snapshots_pk PRIMARY KEY (endpoint_path, region)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.dashboard_snapshots")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. asx_futures_eod

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.asx_futures_eod (
    trade_date          DATE      NOT NULL,
    region              STRING    NOT NULL,
    quarter             STRING    NOT NULL,
    product_type        STRING,
    settlement_price    DOUBLE,
    volume_contracts    INT,
    open_interest       INT,
    change_aud          DOUBLE,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
""")
print(f"Created {SCHEMA}.asx_futures_eod")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. gas_hub_prices

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.gas_hub_prices (
    trade_date          DATE      NOT NULL,
    hub                 STRING    NOT NULL,
    price_aud_gj        DOUBLE,
    volume_tj           DOUBLE,
    source              STRING,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
""")
print(f"Created {SCHEMA}.gas_hub_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. emissions_factors

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.emissions_factors (
    fuel_type           STRING NOT NULL,
    scope               STRING,
    factor_tco2e_mwh    DOUBLE,
    source              STRING,
    effective_year      INT,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.emissions_factors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. daily_market_summary (AI-generated narratives)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.daily_market_summary (
    trading_date        DATE      NOT NULL,
    region_id           STRING    NOT NULL,
    summary_text        STRING,
    key_events          ARRAY<STRING>,
    avg_price_aud_mwh   DOUBLE,
    max_price_aud_mwh   DOUBLE,
    price_spike_count   INT,
    renewables_pct      DOUBLE,
    generated_at        TIMESTAMP,
    model_used          STRING,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.daily_market_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. nem_market_notices

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.nem_market_notices (
    notice_id           STRING NOT NULL,
    notice_type         STRING,
    creation_date       TIMESTAMP,
    issue_date          TIMESTAMP,
    external_reference  STRING,
    reason              STRING,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.nem_market_notices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. nem_region_summary (derived view-like table)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.nem_region_summary (
    interval_datetime   TIMESTAMP NOT NULL,
    region_id           STRING    NOT NULL,
    rrp                 DOUBLE,
    total_demand_mw     DOUBLE,
    net_interchange_mw  DOUBLE,
    renewable_mw        DOUBLE,
    fossil_mw           DOUBLE,
    renewable_pct       DOUBLE,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.nem_region_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19. nem_settlement_summary

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.nem_settlement_summary (
    settlement_date     DATE      NOT NULL,
    region_id           STRING    NOT NULL,
    trading_interval    STRING,
    energy_mwh          DOUBLE,
    settlement_aud      DOUBLE,
    avg_price_aud_mwh   DOUBLE,
    residue_aud         DOUBLE,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.nem_settlement_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 20. nem_sra_residues (Settlement Residue Auction)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.nem_sra_residues (
    quarter             STRING    NOT NULL,
    interconnector_id   STRING    NOT NULL,
    direction           STRING,
    units_offered       INT,
    units_sold          INT,
    clearing_price_aud  DOUBLE,
    total_proceeds_aud  DOUBLE,
    auction_date        DATE,
    _updated_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.nem_sra_residues")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify all tables

# COMMAND ----------

all_tables = [
    "nem_prices_5min", "nem_generation_by_fuel", "nem_interconnectors",
    "demand_actuals", "weather_nem_regions", "anomaly_events",
    "price_forecasts", "demand_forecasts", "nem_daily_summary",
    "nem_facilities", "nem_bid_stack", "dashboard_snapshots",
    "asx_futures_eod", "gas_hub_prices", "emissions_factors",
    "daily_market_summary", "nem_market_notices", "nem_region_summary",
    "nem_settlement_summary", "nem_sra_residues",
]
for t in all_tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {SCHEMA}.{t}").collect()[0].cnt
    print(f"  {SCHEMA}.{t}: {count} rows")

print(f"\nAll {len(all_tables)} core NEM tables verified!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant app service principal access

# COMMAND ----------

# SELECT on all gold tables is granted via schema-level grant in 02_create_tables.py
# MODIFY needed for tables the app writes to
modify_tables = [
    "anomaly_events", "dashboard_snapshots", "daily_market_summary",
    "market_briefs", "nem_region_summary", "nem_settlement_summary",
]
for t in modify_tables:
    try:
        spark.sql(f"GRANT MODIFY ON TABLE {SCHEMA}.{t} TO `{APP_SP}`")
        print(f"  MODIFY granted on {t}")
    except Exception as e:
        print(f"  SKIP: {t} — {e}")

print("\nGrants complete.")
