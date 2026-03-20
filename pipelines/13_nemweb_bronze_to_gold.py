# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 13 — NEMWEB Bronze → Gold (Real Price Data)
# MAGIC
# MAGIC Incremental pipeline: promotes real AEMO NEMWEB dispatch price data from
# MAGIC `bronze.nemweb_dispatch_price` into `gold.nem_prices_5min`, filling any gaps
# MAGIC left by the NEM Simulator with actual market prices.
# MAGIC
# MAGIC **Run schedule:** every 5 minutes via job 154845545310728.
# MAGIC
# MAGIC **Source:** `energy_copilot_catalog.bronze.nemweb_dispatch_price`
# MAGIC   - `lastchanged`  — interval timestamp string `'YYYY/MM/DD HH:mm:ss'`
# MAGIC   - `regionid`     — NEM region (NSW1, QLD1, VIC1, SA1, TAS1)
# MAGIC   - `rrp`          — spot price $/MWh (string)
# MAGIC   - `intervention` — '0' = normal, '1' = intervention pricing (exclude)
# MAGIC   - `run_no`       — dispatch run number (keep lowest per interval+region)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import timezone, datetime

CATALOG     = "energy_copilot_catalog"
NEM_REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
GOLD        = f"{CATALOG}.gold.nem_prices_5min"

# COMMAND ----------
# MAGIC %md ## Step 1 — Watermark: latest real-data row already in gold

# COMMAND ----------

# We tag real rows with apc_flag = false and intervention = false (simulator rows have various values).
# Simpler approach: use MAX(interval_datetime) from gold as watermark, since we only insert
# rows that are NOT already present (MERGE on interval_datetime + region_id handles dedup).
try:
    wm_row = spark.sql(f"SELECT MAX(interval_datetime) AS wm FROM {GOLD}").collect()[0]
    gold_watermark = wm_row["wm"]
except Exception:
    gold_watermark = None

if gold_watermark:
    print(f"Gold watermark: {gold_watermark}")
else:
    print("Gold table empty or not found — full load")

# COMMAND ----------
# MAGIC %md ## Step 2 — Read & clean bronze

# COMMAND ----------

bronze_raw = spark.table(f"{CATALOG}.bronze.nemweb_dispatch_price")

# Only process rows newer than current gold max (incremental)
if gold_watermark is not None:
    bronze_raw = bronze_raw.filter(
        F.to_timestamp(F.col("lastchanged"), "yyyy/MM/dd HH:mm:ss") > F.lit(gold_watermark)
    )

# Parse and clean
window = Window.partitionBy("interval_datetime", "regionid").orderBy(F.col("run_no").cast("int"))

bronze_clean = (
    bronze_raw
    .withColumn("interval_datetime", F.to_timestamp(F.col("lastchanged"), "yyyy/MM/dd HH:mm:ss"))
    .filter(F.col("interval_datetime").isNotNull())
    .filter(F.col("intervention").cast("string") == "0")
    .filter(F.col("regionid").isin(NEM_REGIONS))
    .withColumn("rrp_val", F.col("rrp").cast(DoubleType()))
    .filter(F.col("rrp_val").between(-1000.0, 17000.0))
    # Keep only initial dispatch run_no per (interval, region)
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .select(
        F.col("interval_datetime"),
        F.col("regionid").alias("region_id"),
        F.col("rrp_val").alias("rrp"),
        # Columns in gold schema that we don't have in bronze — leave NULL
        F.lit(None).cast(DoubleType()).alias("rop"),
        F.lit(None).cast(DoubleType()).alias("total_demand_mw"),
        F.lit(None).cast(DoubleType()).alias("available_gen_mw"),
        F.lit(None).cast(DoubleType()).alias("net_interchange_mw"),
        # intervention='0' means non-intervention
        F.lit(False).alias("intervention"),
        F.lit(False).alias("apc_flag"),
        F.lit(False).alias("market_suspended"),
        F.current_timestamp().alias("_updated_at"),
    )
)

new_count = bronze_clean.count()
print(f"New bronze rows to merge into gold: {new_count}")

# COMMAND ----------
# MAGIC %md ## Step 3 — MERGE into gold.nem_prices_5min

# COMMAND ----------

if new_count > 0:
    gold_dt = DeltaTable.forName(spark, GOLD)
    (
        gold_dt.alias("tgt")
        .merge(
            bronze_clean.alias("src"),
            "tgt.interval_datetime = src.interval_datetime AND tgt.region_id = src.region_id",
        )
        # Update existing simulator rows with real rrp (preserve other columns)
        .whenMatchedUpdate(set={
            "rrp":        "src.rrp",
            "_updated_at": "src._updated_at",
        })
        # Insert new intervals not yet in gold
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Gold MERGE complete — {new_count} rows processed")
else:
    print("Gold already up to date — nothing to merge")

# COMMAND ----------
# MAGIC %md ## Step 4 — Summary

# COMMAND ----------

summary = spark.sql(f"""
SELECT
    MIN(interval_datetime) AS earliest,
    MAX(interval_datetime) AS latest,
    COUNT(*)               AS total_rows,
    COUNT(DISTINCT region_id) AS regions
FROM {GOLD}
""").collect()[0]

print(
    f"\n=== {GOLD} ===\n"
    f"  Earliest:   {summary['earliest']}\n"
    f"  Latest:     {summary['latest']}\n"
    f"  Total rows: {summary['total_rows']}\n"
    f"  Regions:    {summary['regions']}"
)
