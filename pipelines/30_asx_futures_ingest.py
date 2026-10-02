# Databricks notebook source
# MAGIC %md
# MAGIC # 30 — ASX Energy Futures Ingest
# MAGIC
# MAGIC Builds a realistic forward electricity curve for the NEM by:
# MAGIC 1. Querying 90 days of historical spot prices from `gold.nem_prices_5min`
# MAGIC 2. Computing quarterly average prices with seasonal adjustment
# MAGIC 3. Applying a term-structure premium (contango) for future quarters
# MAGIC 4. Writing quarterly + calendar forward prices to `gold.asx_futures_eod`
# MAGIC
# MAGIC Runs daily. Produces data in the format expected by `app/routers/curves.py`.

# COMMAND ----------

import math
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = SparkSession.builder.getOrCreate()

CATALOG = "energy_copilot_catalog"
TABLE = f"{CATALOG}.gold.asx_futures_eod"
REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

# Seasonal adjustment factors by quarter (relative to annual average)
# Q1 Jan-Mar: Summer demand spike (hot weather, high AC load)
# Q2 Apr-Jun: Autumn transition, mild
# Q3 Jul-Sep: Winter heating demand
# Q4 Oct-Dec: Spring transition, shoulder
SEASONAL = {"Q1": 1.12, "Q2": 0.94, "Q3": 1.08, "Q4": 0.96}

# Term structure: contango slope per quarter (% premium per quarter out)
CONTANGO_PER_QUARTER = 0.018  # 1.8% per quarter

# COMMAND ----------

print("Querying 90-day historical spot price averages...")

# Get 90-day spot price stats per region
spot_df = spark.sql(f"""
    SELECT
        region_id,
        AVG(rrp) AS avg_price,
        STDDEV(rrp) AS std_price,
        PERCENTILE_APPROX(rrp, 0.5) AS median_price,
        MAX(rrp) AS max_price,
        COUNT(*) AS interval_count
    FROM {CATALOG}.gold.nem_prices_5min
    WHERE interval_datetime >= current_timestamp() - INTERVAL 90 DAYS
      AND rrp BETWEEN -1000 AND 15000
    GROUP BY region_id
""")

region_stats = {row["region_id"]: row.asDict() for row in spot_df.collect()}

if not region_stats:
    print("No spot price data found — using base prices")
    region_stats = {
        r: {"avg_price": p, "std_price": p * 0.3, "median_price": p, "max_price": p * 10, "interval_count": 0}
        for r, p in [("NSW1", 72.5), ("QLD1", 65.3), ("VIC1", 55.8), ("SA1", 88.1), ("TAS1", 42.0)]
    }

for r, s in region_stats.items():
    print(f"  {r}: avg=${float(s['avg_price']):.2f} median=${float(s.get('median_price', s['avg_price']) or s['avg_price']):.2f} n={s['interval_count']}")

# COMMAND ----------

print("Building forward curve records...")

today = date.today()
records = []

# Generate 8 quarters out from current quarter + 3 calendar years
current_year = today.year
current_quarter = (today.month - 1) // 3 + 1

def quarter_start_month(q: int) -> int:
    return (q - 1) * 3 + 1

quarters_to_generate = []
yr, qtr = current_year, current_quarter
for _ in range(8):
    quarters_to_generate.append((yr, qtr))
    qtr += 1
    if qtr > 4:
        qtr = 1
        yr += 1

calendar_years = [current_year, current_year + 1, current_year + 2]

for region in REGIONS:
    stats = region_stats.get(region)
    if not stats:
        continue
    base_price = float(stats["avg_price"] or 70.0)
    if base_price <= 0:
        base_price = 70.0
    std_price = float(stats.get("std_price") or base_price * 0.3)

    # --- Quarterly contracts ---
    for offset, (yr, qtr) in enumerate(quarters_to_generate):
        q_label = f"Q{qtr}"
        seasonal_factor = SEASONAL[q_label]
        contango_factor = 1 + CONTANGO_PER_QUARTER * offset

        flat_price = round(base_price * seasonal_factor * contango_factor, 2)
        peak_price = round(flat_price * 1.28, 2)   # peak hours premium ~28%
        offpeak_price = round(flat_price * 0.82, 2)  # off-peak discount ~18%

        # Volume and OI decay with time (near contracts more liquid)
        volume = max(10, int(500 * math.exp(-offset * 0.25)))
        oi = max(50, int(3000 * math.exp(-offset * 0.2)))
        change = round((flat_price - base_price * seasonal_factor) * 0.1, 2)

        quarter_str = f"{q_label}-{yr}"

        for product_type, price in [("Base", flat_price), ("Peak", peak_price), ("Off-Peak", offpeak_price)]:
            records.append({
                "trade_date": today,
                "region": region,
                "quarter": quarter_str,
                "year": yr,
                "product_type": product_type,
                "contract_type": product_type,  # alias for router compatibility
                "contract_code": f"{region[:2]}{q_label}{str(yr)[-2:]}-{product_type[:1]}",
                "settlement_price": price,
                "volume_contracts": volume,
                "open_interest": oi,
                "change_aud": change,
                "_updated_at": F.current_timestamp(),
            })

    # --- Calendar year contracts ---
    for yr_offset, cal_year in enumerate(calendar_years):
        # Annual average = weighted average of 4 quarters
        annual_factors = [SEASONAL[f"Q{q}"] for q in range(1, 5)]
        avg_seasonal = sum(annual_factors) / 4
        contango_factor = 1 + CONTANGO_PER_QUARTER * (4 + yr_offset * 4)

        cal_flat = round(base_price * avg_seasonal * contango_factor, 2)
        cal_peak = round(cal_flat * 1.25, 2)
        cal_offpeak = round(cal_flat * 0.84, 2)

        volume = max(5, int(200 * math.exp(-yr_offset * 0.4)))
        oi = max(30, int(1500 * math.exp(-yr_offset * 0.35)))
        change = round((cal_flat - base_price) * 0.05, 2)

        for product_type, price in [("Base", cal_flat), ("Peak", cal_peak), ("Off-Peak", cal_offpeak)]:
            records.append({
                "trade_date": today,
                "region": region,
                "quarter": f"CAL{str(cal_year)[-2:]}",
                "year": cal_year,
                "product_type": product_type,
                "contract_type": product_type,
                "contract_code": f"{region[:2]}CAL{str(cal_year)[-2:]}-{product_type[:1]}",
                "settlement_price": price,
                "volume_contracts": volume,
                "open_interest": oi,
                "change_aud": change,
                "_updated_at": None,  # will be set in DataFrame
            })

print(f"Generated {len(records)} forward curve records across {len(REGIONS)} regions")

# COMMAND ----------

print("Writing to gold.asx_futures_eod...")

# Build DataFrame (drop the _updated_at placeholder since we use current_timestamp)
schema = StructType([
    StructField("trade_date", DateType(), False),
    StructField("region", StringType(), False),
    StructField("quarter", StringType(), False),
    StructField("year", IntegerType(), True),
    StructField("product_type", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("contract_code", StringType(), True),
    StructField("settlement_price", DoubleType(), True),
    StructField("volume_contracts", IntegerType(), True),
    StructField("open_interest", IntegerType(), True),
    StructField("change_aud", DoubleType(), True),
])

rows = [
    (
        r["trade_date"],
        r["region"],
        r["quarter"],
        r["year"],
        r["product_type"],
        r["contract_type"],
        r["contract_code"],
        r["settlement_price"],
        r["volume_contracts"],
        r["open_interest"],
        r["change_aud"],
    )
    for r in records
]

df = (
    spark.createDataFrame(rows, schema=schema)
    .withColumn("_updated_at", F.current_timestamp())
)

# Create table if not exists (with extended schema including router-expected columns)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        trade_date DATE NOT NULL,
        region STRING NOT NULL,
        quarter STRING NOT NULL,
        year INT,
        product_type STRING,
        contract_type STRING,
        contract_code STRING,
        settlement_price DOUBLE,
        volume_contracts INT,
        open_interest INT,
        change_aud DOUBLE,
        _updated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'ASX Energy quarterly and calendar electricity futures (synthetic forward curve)'
    PARTITIONED BY (region)
""")

# Add missing columns if table already existed with old schema
for col, dtype in [("year", "INT"), ("contract_type", "STRING"), ("contract_code", "STRING")]:
    try:
        spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN {col} {dtype}")
        print(f"  Added column {col}")
    except Exception:
        pass  # Column already exists

# Delete today's existing records then insert fresh
spark.sql(f"DELETE FROM {TABLE} WHERE trade_date = '{today}'")

df.write.format("delta").mode("append").saveAsTable(TABLE)

count = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE} WHERE trade_date = '{today}'").collect()[0]["n"]
print(f"✓ Wrote {count} records to {TABLE} for {today}")

# COMMAND ----------

# Verify
print("\nSample records:")
spark.sql(f"""
    SELECT region, quarter, year, product_type, settlement_price, volume_contracts
    FROM {TABLE}
    WHERE trade_date = '{today}' AND product_type = 'Base'
    ORDER BY region, year, quarter
    LIMIT 20
""").show(20, truncate=False)
