# Databricks notebook source
# MAGIC %md
# MAGIC # NEMWEB Bronze → Gold with data-quality enforcement
# MAGIC
# MAGIC Reads new rows from `{catalog}.bronze.nemweb_dispatch_price` (landed by Pipeline 01),
# MAGIC applies **data-quality rules**, and:
# MAGIC
# MAGIC | Outcome | Destination |
# MAGIC |---------|-------------|
# MAGIC | Passes every rule | MERGE into `{catalog}.gold.nem_prices_5min` |
# MAGIC | Fails any rule | `{catalog}.gold.nem_prices_quarantine` with the list of failed rules |
# MAGIC | Per-rule pass/fail counts | `{catalog}.gold.dq_results` (one row per rule per run) |
# MAGIC
# MAGIC A **freshness rule** fails the job when the newest real interval is older than
# MAGIC `max_staleness_minutes`, so a broken upstream feed can no longer "succeed" silently.
# MAGIC
# MAGIC Market times from NEMWEB are AEST (UTC+10, no daylight saving); gold stores UTC.

# COMMAND ----------

import json
import re
from datetime import datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "energy_copilot_catalog")
dbutils.widgets.text("market_price_floor", "-1000")  # AEMO market floor price ($/MWh)
dbutils.widgets.text("market_price_cap", "23200")  # AEMC market price cap FY2026-27 (CPI-indexed; update each 1 July)
dbutils.widgets.text("max_staleness_minutes", "30")
dbutils.widgets.text("fail_on_stale", "true")
dbutils.widgets.text("job_run_id", "")

CATALOG = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"Invalid catalog name: {CATALOG!r}")
PRICE_FLOOR = float(dbutils.widgets.get("market_price_floor"))
PRICE_CAP = float(dbutils.widgets.get("market_price_cap"))
MAX_STALENESS = timedelta(minutes=int(dbutils.widgets.get("max_staleness_minutes")))
FAIL_ON_STALE = dbutils.widgets.get("fail_on_stale").lower() == "true"
JOB_RUN_ID = dbutils.widgets.get("job_run_id") or "interactive"

BRONZE = f"{CATALOG}.bronze.nemweb_dispatch_price"
GOLD = f"{CATALOG}.gold.nem_prices_5min"
QUARANTINE = f"{CATALOG}.gold.nem_prices_quarantine"
DQ_RESULTS = f"{CATALOG}.gold.dq_results"
WATERMARKS = f"{CATALOG}.gold.pipeline_watermarks"
PIPELINE_NAME = "nemweb_bronze_to_gold"
NEM_REGIONS = ("NSW1", "QLD1", "VIC1", "SA1", "TAS1")
RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)

# Data-quality rules: (name, SQL boolean expression that must be TRUE for a valid row)
DQ_RULES: tuple[tuple[str, str], ...] = (
    ("interval_not_null", "interval_datetime IS NOT NULL"),
    ("interval_on_5min_grid", "minute(interval_datetime) % 5 = 0 AND second(interval_datetime) = 0"),
    ("valid_nem_region", f"region_id IN {NEM_REGIONS}"),
    ("rrp_is_numeric", "rrp IS NOT NULL"),
    ("rrp_within_market_limits", f"rrp BETWEEN {PRICE_FLOOR} AND {PRICE_CAP}"),
    # SA1 operational demand goes negative at midday under high rooftop solar, so the rule is a physical
    # plausibility bound, not ">= 0" (a ">= 0" rule wrongly quarantined 102 real SA1 intervals on 2026-11-09).
    ("demand_within_physical_bounds", "total_demand_mw IS NULL OR total_demand_mw BETWEEN -2000 AND 20000"),
)

# COMMAND ----------
# MAGIC %md ## Step 0 — Governance tables (idempotent)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DQ_RESULTS} (
  run_ts TIMESTAMP COMMENT 'UTC time the rule was evaluated',
  job_run_id STRING COMMENT 'Databricks job run id ({{{{job.run_id}}}})',
  pipeline STRING, target_table STRING,
  rule_name STRING, rule_expression STRING,
  rows_checked BIGINT, rows_failed BIGINT, pass_rate DOUBLE,
  status STRING COMMENT 'PASS | FAIL'
) COMMENT 'Data-quality rule results, one row per rule per pipeline run'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE} (
  interval_datetime TIMESTAMP, region_id STRING, rrp DOUBLE, total_demand_mw DOUBLE,
  _source_file STRING, failed_rules ARRAY<STRING>, quarantined_at TIMESTAMP, job_run_id STRING
) COMMENT 'Rows from bronze.nemweb_dispatch_price that failed data-quality rules'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {WATERMARKS} (
  pipeline STRING, watermark TIMESTAMP, updated_at TIMESTAMP
) COMMENT 'Incremental-processing watermarks per pipeline'
""")

# COMMAND ----------
# MAGIC %md ## Step 1 — Read new bronze rows since the last watermark

# COMMAND ----------

wm_rows = spark.sql(f"SELECT max(watermark) AS wm FROM {WATERMARKS} WHERE pipeline = '{PIPELINE_NAME}'").collect()
watermark = wm_rows[0]["wm"] if wm_rows else None
print(f"Watermark (_ingested_at): {watermark}")

bronze = spark.table(BRONZE)
if watermark is not None:
    bronze = bronze.filter(F.col("_ingested_at") > F.lit(watermark))
bronze = bronze.filter(F.col("settlement_datetime").isNotNull())
batch_max_ingested = bronze.agg(F.max("_ingested_at")).collect()[0][0]

dedup = Window.partitionBy("settlement_datetime", "regionid").orderBy(F.col("_ingested_at").desc())
parsed = (
    bronze
    .filter(F.col("intervention") == "0")  # business rule: exclude intervention pricing runs
    .withColumn("rn", F.row_number().over(dedup))
    .filter("rn = 1")
    .select(
        F.to_utc_timestamp(
            F.to_timestamp("settlement_datetime", "yyyy/MM/dd HH:mm:ss"), "Australia/Brisbane"
        ).alias("interval_datetime"),
        F.col("regionid").alias("region_id"),
        F.col("rrp").cast(DoubleType()).alias("rrp"),
        F.col("total_demand").cast(DoubleType()).alias("total_demand_mw"),
        F.col("available_generation").cast(DoubleType()).alias("available_gen_mw"),
        F.col("net_interchange").cast(DoubleType()).alias("net_interchange_mw"),
        "_source_file",
    )
)

# COMMAND ----------
# MAGIC %md ## Step 2 — Evaluate data-quality rules

# COMMAND ----------

failed_rules_col = F.array_compact(F.array(*[
    F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(name)) for name, expr in DQ_RULES
]))
evaluated = parsed.withColumn("failed_rules", failed_rules_col)
rows_checked = evaluated.count()

rule_counts = evaluated.select(*[
    F.sum(F.array_contains("failed_rules", name).cast("int")).alias(name) for name, _ in DQ_RULES
]).collect()[0].asDict() if rows_checked else {name: 0 for name, _ in DQ_RULES}

dq_rows = [
    (RUN_TS, JOB_RUN_ID, PIPELINE_NAME, GOLD, name, expr, rows_checked, int(rule_counts[name] or 0),
     (1.0 - (rule_counts[name] or 0) / rows_checked) if rows_checked else 1.0,
     "PASS" if not rule_counts[name] else "FAIL")
    for name, expr in DQ_RULES
]

valid = evaluated.filter(F.size("failed_rules") == 0).drop("failed_rules", "_source_file")
invalid = evaluated.filter(F.size("failed_rules") > 0)
invalid_count = invalid.count()
if invalid_count:
    (invalid.select("interval_datetime", "region_id", "rrp", "total_demand_mw", "_source_file", "failed_rules",
                    F.lit(RUN_TS).alias("quarantined_at"), F.lit(JOB_RUN_ID).alias("job_run_id"))
     .write.mode("append").saveAsTable(QUARANTINE))
print(f"Checked {rows_checked} rows | quarantined {invalid_count} | rule failures: {rule_counts}")

# COMMAND ----------
# MAGIC %md ## Step 3 — MERGE valid rows into gold

# COMMAND ----------

valid_count = rows_checked - invalid_count
if valid_count:
    src = valid.select(
        "interval_datetime", "region_id", "rrp",
        F.lit(None).cast(DoubleType()).alias("rop"),
        "total_demand_mw", "available_gen_mw", "net_interchange_mw",
        F.lit(False).alias("intervention"), F.lit(False).alias("apc_flag"), F.lit(False).alias("market_suspended"),
        F.current_timestamp().alias("_updated_at"),
    )
    (DeltaTable.forName(spark, GOLD).alias("tgt")
     .merge(src.alias("src"), "tgt.interval_datetime = src.interval_datetime AND tgt.region_id = src.region_id")
     .whenMatchedUpdate(set={c: f"src.{c}" for c in
                             ("rrp", "rop", "total_demand_mw", "available_gen_mw", "net_interchange_mw",
                              "intervention", "apc_flag", "market_suspended", "_updated_at")})
     .whenNotMatchedInsertAll()
     .execute())
print(f"Merged {valid_count} valid rows into {GOLD}")

# COMMAND ----------
# MAGIC %md ## Step 4 — Freshness rule, results log, watermark

# COMMAND ----------

latest_real = spark.sql(f"""
  SELECT max(to_utc_timestamp(to_timestamp(settlement_datetime, 'yyyy/MM/dd HH:mm:ss'), 'Australia/Brisbane'))
  FROM {BRONZE}
""").collect()[0][0]
staleness = (RUN_TS - latest_real) if latest_real else None
is_stale = staleness is None or staleness > MAX_STALENESS
dq_rows.append((
    RUN_TS, JOB_RUN_ID, PIPELINE_NAME, GOLD, "freshness",
    f"now() - max(interval_datetime) <= {int(MAX_STALENESS.total_seconds() // 60)} min",
    1, int(is_stale), 0.0 if is_stale else 1.0, "FAIL" if is_stale else "PASS",
))

spark.createDataFrame(dq_rows, schema=spark.table(DQ_RESULTS).schema).write.mode("append").saveAsTable(DQ_RESULTS)
if batch_max_ingested is not None:
    spark.createDataFrame([(PIPELINE_NAME, batch_max_ingested, RUN_TS)], schema=spark.table(WATERMARKS).schema) \
        .write.mode("append").saveAsTable(WATERMARKS)

summary = {
    "rows_checked": rows_checked,
    "rows_merged": valid_count,
    "rows_quarantined": invalid_count,
    "rule_failures": {k: int(v or 0) for k, v in rule_counts.items()},
    "latest_real_interval_utc": str(latest_real),
    "staleness_minutes": round(staleness.total_seconds() / 60, 1) if staleness else None,
    "freshness": "FAIL" if is_stale else "PASS",
}
print(json.dumps(summary, indent=2))
if is_stale and FAIL_ON_STALE:
    raise RuntimeError(f"Freshness rule failed: latest real interval {latest_real} is older than {MAX_STALENESS}")
dbutils.notebook.exit(json.dumps(summary))
