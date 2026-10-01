# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Create Deal Capture Tables
# MAGIC Creates 6 tables for Deal Capture & Portfolio Management (PRD 15.1):
# MAGIC - `gold.trades` — Trade records
# MAGIC - `gold.trade_legs` — Settlement interval legs
# MAGIC - `gold.trade_amendments` — Audit trail
# MAGIC - `gold.counterparties` — Counterparty registry
# MAGIC - `gold.portfolios` — Portfolio definitions
# MAGIC - `gold.portfolio_trades` — Portfolio↔trade mapping

# COMMAND ----------

try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "energy_copilot_catalog"

SCHEMA = f"{CATALOG}.gold"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Trades

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.trades (
    trade_id        STRING      NOT NULL,
    trade_type      STRING      NOT NULL  COMMENT 'SPOT|FORWARD|SWAP|FUTURE|OPTION|PPA|REC',
    region          STRING      NOT NULL  COMMENT 'NEM region: NSW1|QLD1|VIC1|SA1|TAS1',
    buy_sell        STRING      NOT NULL  COMMENT 'BUY or SELL',
    volume_mw       DOUBLE      NOT NULL,
    price           DOUBLE      NOT NULL  COMMENT '$/MWh',
    start_date      DATE        NOT NULL,
    end_date        DATE        NOT NULL,
    profile         STRING      NOT NULL  COMMENT 'FLAT|PEAK|OFF_PEAK|SUPER_PEAK',
    status          STRING      NOT NULL  COMMENT 'DRAFT|CONFIRMED|SETTLED|CANCELLED',
    counterparty_id STRING,
    portfolio_id    STRING,
    notes           STRING,
    created_by      STRING      NOT NULL,
    created_at      TIMESTAMP   NOT NULL,
    updated_at      TIMESTAMP   NOT NULL,
    CONSTRAINT trades_pk PRIMARY KEY (trade_id)
)
USING DELTA
COMMENT 'Deal capture trades — Phase 2 PRD 15.1'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.trades")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Trade Legs

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.trade_legs (
    leg_id           STRING      NOT NULL,
    trade_id         STRING      NOT NULL,
    settlement_date  DATE        NOT NULL,
    interval_start   TIMESTAMP   NOT NULL,
    interval_end     TIMESTAMP   NOT NULL,
    volume_mw        DOUBLE      NOT NULL,
    price            DOUBLE      NOT NULL,
    profile_factor   DOUBLE      NOT NULL  COMMENT '1.0=active, 0.0=inactive for profile',
    CONSTRAINT trade_legs_pk PRIMARY KEY (leg_id)
)
USING DELTA
COMMENT 'Trade settlement interval legs'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.trade_legs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Trade Amendments (audit trail)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.trade_amendments (
    amendment_id    STRING      NOT NULL,
    trade_id        STRING      NOT NULL,
    field_changed   STRING      NOT NULL,
    old_value       STRING,
    new_value       STRING,
    amended_by      STRING      NOT NULL,
    amended_at      TIMESTAMP   NOT NULL,
    CONSTRAINT trade_amendments_pk PRIMARY KEY (amendment_id)
)
USING DELTA
COMMENT 'Append-only audit log for trade amendments'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.trade_amendments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Counterparties

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.counterparties (
    counterparty_id   STRING      NOT NULL,
    name              STRING      NOT NULL,
    credit_rating     STRING,
    credit_limit_aud  DOUBLE,
    status            STRING      NOT NULL  COMMENT 'ACTIVE|SUSPENDED|INACTIVE',
    created_at        TIMESTAMP   NOT NULL,
    CONSTRAINT counterparties_pk PRIMARY KEY (counterparty_id)
)
USING DELTA
COMMENT 'Counterparty registry for deal capture'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.counterparties")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Portfolios

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.portfolios (
    portfolio_id    STRING      NOT NULL,
    name            STRING      NOT NULL,
    owner           STRING,
    description     STRING,
    created_at      TIMESTAMP   NOT NULL,
    CONSTRAINT portfolios_pk PRIMARY KEY (portfolio_id)
)
USING DELTA
COMMENT 'Portfolio definitions for trade grouping'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.portfolios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Portfolio↔Trade Mapping

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.portfolio_trades (
    portfolio_id    STRING      NOT NULL,
    trade_id        STRING      NOT NULL,
    CONSTRAINT portfolio_trades_pk PRIMARY KEY (portfolio_id, trade_id)
)
USING DELTA
COMMENT 'Many-to-many mapping between portfolios and trades'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.portfolio_trades")

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Forward Curves

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.forward_curves (
    curve_id        STRING      NOT NULL,
    curve_date      DATE        NOT NULL  COMMENT 'Date the curve was built',
    region          STRING      NOT NULL  COMMENT 'NEM region: NSW1|QLD1|VIC1|SA1|TAS1',
    profile         STRING      NOT NULL  COMMENT 'FLAT|PEAK|OFF_PEAK',
    quarter         STRING      NOT NULL  COMMENT 'Q1_2026, Q2_2026, etc.',
    month           STRING      NOT NULL  COMMENT '2026-01, 2026-02, etc.',
    price_mwh       DOUBLE      NOT NULL  COMMENT '$/MWh',
    source          STRING      NOT NULL  COMMENT 'ASX_BOOTSTRAP|SHAPED|EXTRAPOLATED',
    model_version   STRING      DEFAULT '1.0',
    created_at      TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Forward curve snapshots bootstrapped from ASX futures — E1 enhancement'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.forward_curves")

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Portfolio MtM (Mark-to-Market)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.portfolio_mtm (
    mtm_id          STRING      NOT NULL,
    valuation_date  DATE        NOT NULL,
    trade_id        STRING      NOT NULL,
    portfolio_id    STRING,
    region          STRING      NOT NULL,
    trade_type      STRING      NOT NULL,
    direction       STRING      NOT NULL  COMMENT 'BUY or SELL',
    mtm_value       DOUBLE      NOT NULL  COMMENT 'Current mark-to-market value AUD',
    unrealised_pnl  DOUBLE      NOT NULL  COMMENT 'Unrealised P&L AUD',
    market_price    DOUBLE      NOT NULL  COMMENT 'Current forward/spot price $/MWh',
    contract_price  DOUBLE      NOT NULL  COMMENT 'Original trade price $/MWh',
    volume_mw       DOUBLE      NOT NULL,
    remaining_days  INT         NOT NULL,
    discount_factor DOUBLE      NOT NULL,
    model_version   STRING,
    created_at      TIMESTAMP,
    CONSTRAINT portfolio_mtm_pk PRIMARY KEY (mtm_id)
)
USING DELTA
COMMENT 'Mark-to-market valuations per trade — E2 enhancement'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Created {SCHEMA}.portfolio_mtm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. P&L Attribution

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.pnl_attribution (
    attribution_id  STRING      NOT NULL,
    valuation_date  DATE        NOT NULL,
    portfolio_id    STRING      NOT NULL,
    region          STRING      NOT NULL,
    price_effect    DOUBLE      NOT NULL  COMMENT 'P&L from price changes',
    volume_effect   DOUBLE      NOT NULL  COMMENT 'P&L from volume changes',
    new_trades_effect DOUBLE    NOT NULL  COMMENT 'P&L from new trades',
    time_decay      DOUBLE      NOT NULL  COMMENT 'P&L from time decay',
    total_pnl       DOUBLE      NOT NULL  COMMENT 'Total daily P&L',
    created_at      TIMESTAMP,
    CONSTRAINT pnl_attribution_pk PRIMARY KEY (attribution_id)
)
USING DELTA
COMMENT 'P&L attribution breakdown by effect type — E2 enhancement'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.pnl_attribution")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Risk Metrics (VaR & Greeks)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.risk_metrics (
    metric_id       STRING      NOT NULL,
    valuation_date  DATE        NOT NULL,
    portfolio_id    STRING      NOT NULL,
    region          STRING      NOT NULL,
    var_95_1d       DOUBLE      NOT NULL  COMMENT 'Value at Risk 95% 1-day',
    var_99_1d       DOUBLE      NOT NULL  COMMENT 'Value at Risk 99% 1-day',
    var_95_10d      DOUBLE      NOT NULL  COMMENT 'Value at Risk 95% 10-day',
    var_99_10d      DOUBLE      NOT NULL  COMMENT 'Value at Risk 99% 10-day',
    delta_mw        DOUBLE      NOT NULL  COMMENT 'Portfolio delta in MW',
    gamma           DOUBLE      NOT NULL  COMMENT 'Portfolio gamma',
    vega            DOUBLE      NOT NULL  COMMENT 'Portfolio vega',
    theta           DOUBLE      NOT NULL  COMMENT 'Portfolio theta (daily)',
    volatility_annual DOUBLE    NOT NULL  COMMENT 'Annualised volatility',
    created_at      TIMESTAMP,
    CONSTRAINT risk_metrics_pk PRIMARY KEY (metric_id)
)
USING DELTA
COMMENT 'VaR and portfolio Greeks — E3 enhancement'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.risk_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Credit Exposure

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.credit_exposure (
    exposure_id              STRING      NOT NULL,
    valuation_date           DATE        NOT NULL,
    counterparty_id          STRING      NOT NULL,
    counterparty_name        STRING      NOT NULL,
    current_exposure         DOUBLE      NOT NULL  COMMENT 'Current credit exposure AUD',
    potential_future_exposure DOUBLE     NOT NULL  COMMENT 'PFE AUD',
    credit_limit             DOUBLE      NOT NULL  COMMENT 'Credit limit AUD',
    credit_utilization       DOUBLE      NOT NULL  COMMENT 'Utilisation 0-1',
    credit_rating            STRING,
    exposure_current_bucket  DOUBLE      NOT NULL  COMMENT '0-30 day exposure',
    exposure_30_90_bucket    DOUBLE      NOT NULL  COMMENT '30-90 day exposure',
    exposure_90plus_bucket   DOUBLE      NOT NULL  COMMENT '90+ day exposure',
    alert_level              STRING      NOT NULL  COMMENT 'NORMAL|WARNING|CRITICAL',
    created_at               TIMESTAMP,
    CONSTRAINT credit_exposure_pk PRIMARY KEY (exposure_id)
)
USING DELTA
COMMENT 'Counterparty credit exposure — E5 enhancement'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.credit_exposure")

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Retail Tariffs (AER CDR)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.retail_tariffs (
    plan_id         STRING      NOT NULL,
    retailer        STRING,
    state           STRING,
    fuel_type       STRING,
    plan_type       STRING      COMMENT 'STANDING or MARKET',
    effective_from  STRING,
    effective_to    STRING,
    ingested_at     STRING
)
USING DELTA
COMMENT 'AER CDR retail energy plans'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.retail_tariffs")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.tariff_components (
    plan_id             STRING      NOT NULL,
    component_type      STRING      NOT NULL  COMMENT 'USAGE|SUPPLY|DEMAND|FIT',
    rate_aud_kwh        DOUBLE,
    daily_supply_charge_aud DOUBLE,
    tou_period          STRING      COMMENT 'PEAK|OFFPEAK|SHOULDER',
    unit                STRING,
    ingested_at         STRING
)
USING DELTA
COMMENT 'Tariff rate components per plan'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.tariff_components")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Facility Generation Timeseries (OpenNEM)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.facility_generation_ts (
    facility_id         STRING      NOT NULL,
    network_region      STRING      NOT NULL,
    fuel_type           STRING,
    interval_datetime   TIMESTAMP   NOT NULL,
    power_mw            DOUBLE,
    energy_mwh          DOUBLE
)
USING DELTA
COMMENT 'Facility-level generation timeseries from OpenElectricity API'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.facility_generation_ts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. LGC Registry & Spot Prices (CER)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.lgc_registry (
    power_station       STRING      NOT NULL,
    state               STRING,
    fuel_type           STRING,
    capacity_mw         DOUBLE,
    lgc_created_mwh     DOUBLE,
    year                STRING,
    quarter             STRING,
    ingested_at         STRING
)
USING DELTA
COMMENT 'CER LGC creation volumes by power station'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.lgc_registry")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.lgc_spot_prices (
    trade_date          STRING      NOT NULL,
    price_aud_mwh       DOUBLE      NOT NULL,
    source              STRING,
    ingested_at         STRING
)
USING DELTA
COMMENT 'LGC spot price history from CER quarterly reports'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.lgc_spot_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. ISP Projects & REZ Assessments (AEMO)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.isp_projects (
    project_name        STRING      NOT NULL,
    type                STRING      COMMENT 'transmission|interconnector|generation',
    status              STRING,
    expected_completion STRING,
    capex_m_aud         DOUBLE,
    tnsp                STRING,
    route_km            INT,
    capacity_mw         INT,
    ingested_at         STRING
)
USING DELTA
COMMENT 'AEMO ISP 2024 actionable and future projects'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.isp_projects")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.isp_capacity_outlook (
    scenario            STRING      NOT NULL,
    year                INT         NOT NULL,
    region              STRING      NOT NULL,
    fuel_type           STRING      NOT NULL,
    capacity_mw         DOUBLE,
    generation_twh      DOUBLE,
    ingested_at         STRING
)
USING DELTA
COMMENT 'ISP 2024 capacity outlook by scenario/year/region/fuel'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.isp_capacity_outlook")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.rez_assessments (
    rez_name            STRING      NOT NULL,
    region              STRING      NOT NULL,
    solar_capacity_mw   DOUBLE,
    wind_capacity_mw    DOUBLE,
    network_capacity_mw DOUBLE,
    development_status  STRING      COMMENT 'Declared|Active|Candidate|Planned',
    score               INT,
    ingested_at         STRING
)
USING DELTA
COMMENT 'ISP 2024 Renewable Energy Zone assessments'
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.rez_assessments")

# COMMAND ----------

# Verify all tables
all_tables = [
    "trades", "trade_legs", "trade_amendments", "counterparties",
    "portfolios", "portfolio_trades", "forward_curves",
    "portfolio_mtm", "pnl_attribution", "risk_metrics", "credit_exposure",
    "retail_tariffs", "tariff_components", "facility_generation_ts",
    "lgc_registry", "lgc_spot_prices",
    "isp_projects", "isp_capacity_outlook", "rez_assessments",
]
for t in all_tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {SCHEMA}.{t}").collect()[0].cnt
    print(f"  {SCHEMA}.{t}: {count} rows")

print(f"\nAll {len(all_tables)} tables created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant app service principal access
# MAGIC The energy-copilot Databricks App runs as a service principal that needs
# MAGIC SELECT on gold tables and USAGE on schemas. MODIFY is needed for deal tables.

# COMMAND ----------

APP_SP = "67aaaa6b-778c-4c8b-b2f0-9f9b9728b3bb"

# Grant access to gold schema (app's own tables)
_grant_stmts = [
    f"GRANT USAGE ON SCHEMA {SCHEMA} TO `{APP_SP}`",
    f"GRANT SELECT ON SCHEMA {SCHEMA} TO `{APP_SP}`",
    # MODIFY needed for deal capture + forward curves
    f"GRANT MODIFY ON TABLE {SCHEMA}.trades TO `{APP_SP}`",
    f"GRANT MODIFY ON TABLE {SCHEMA}.trade_legs TO `{APP_SP}`",
    f"GRANT MODIFY ON TABLE {SCHEMA}.trade_amendments TO `{APP_SP}`",
    f"GRANT MODIFY ON TABLE {SCHEMA}.counterparties TO `{APP_SP}`",
    f"GRANT MODIFY ON TABLE {SCHEMA}.portfolios TO `{APP_SP}`",
    f"GRANT MODIFY ON TABLE {SCHEMA}.portfolio_trades TO `{APP_SP}`",
    f"GRANT MODIFY ON TABLE {SCHEMA}.forward_curves TO `{APP_SP}`",
    f"GRANT MODIFY ON TABLE {SCHEMA}.portfolio_mtm TO `{APP_SP}`",
    f"GRANT MODIFY ON TABLE {SCHEMA}.pnl_attribution TO `{APP_SP}`",
    f"GRANT MODIFY ON TABLE {SCHEMA}.risk_metrics TO `{APP_SP}`",
    f"GRANT MODIFY ON TABLE {SCHEMA}.credit_exposure TO `{APP_SP}`",
    # Grant access to nemweb_analytics schema (DLT-produced tables from nemweb-accelerator)
    f"GRANT USAGE ON SCHEMA {CATALOG}.nemweb_analytics TO `{APP_SP}`",
    f"GRANT SELECT ON SCHEMA {CATALOG}.nemweb_analytics TO `{APP_SP}`",
]

for stmt in _grant_stmts:
    try:
        spark.sql(stmt)
        print(f"  OK: {stmt}")
    except Exception as e:
        print(f"  SKIP: {stmt} — {e}")

print("\nService principal grants complete.")
