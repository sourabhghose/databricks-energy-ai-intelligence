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
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print(f"Created {SCHEMA}.portfolio_trades")

# COMMAND ----------

# Verify all tables
for t in ["trades", "trade_legs", "trade_amendments", "counterparties", "portfolios", "portfolio_trades"]:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {SCHEMA}.{t}").collect()[0].cnt
    print(f"  {SCHEMA}.{t}: {count} rows")

print("\nAll 6 deal capture tables created successfully!")
