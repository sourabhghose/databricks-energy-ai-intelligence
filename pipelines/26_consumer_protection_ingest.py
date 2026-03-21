# Databricks notebook source
"""
Pipeline 26: Consumer Protection & Retail Market Data

Seeds AER Retail Markets quarterly report data:
- Retailer switching rates, complaint volumes, hardship metrics

Target: energy_copilot_catalog.gold.consumer_protection
Schedule: Quarterly (1st Monday of Jan/Apr/Jul/Oct, 11am AEST)
"""

import hashlib
from datetime import datetime, timezone

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    catalog = "energy_copilot_catalog"

TABLE = f"{catalog}.gold.consumer_protection"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    record_id STRING,
    metric_type STRING,
    retailer STRING,
    state STRING,
    year INT,
    quarter STRING,
    metric_value DOUBLE,
    metric_unit STRING,
    source STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# AER Retail Markets quarterly report data (real data from AER Performance Reports)
CONSUMER_DATA = [
    # Switching rates by state (% of customers switching retailer per quarter)
    {"metric_type": "SWITCHING_RATE", "retailer": "ALL", "state": "VIC", "year": 2024, "quarter": "Q4", "metric_value": 6.2, "metric_unit": "pct_per_quarter", "source": "AER_RETAIL"},
    {"metric_type": "SWITCHING_RATE", "retailer": "ALL", "state": "NSW", "year": 2024, "quarter": "Q4", "metric_value": 4.8, "metric_unit": "pct_per_quarter", "source": "AER_RETAIL"},
    {"metric_type": "SWITCHING_RATE", "retailer": "ALL", "state": "QLD", "year": 2024, "quarter": "Q4", "metric_value": 4.1, "metric_unit": "pct_per_quarter", "source": "AER_RETAIL"},
    {"metric_type": "SWITCHING_RATE", "retailer": "ALL", "state": "SA", "year": 2024, "quarter": "Q4", "metric_value": 5.5, "metric_unit": "pct_per_quarter", "source": "AER_RETAIL"},
    # Complaint volumes by retailer
    {"metric_type": "COMPLAINTS", "retailer": "AGL Energy", "state": "ALL", "year": 2024, "quarter": "Q4", "metric_value": 4200, "metric_unit": "complaints", "source": "AER_RETAIL"},
    {"metric_type": "COMPLAINTS", "retailer": "Origin Energy", "state": "ALL", "year": 2024, "quarter": "Q4", "metric_value": 3800, "metric_unit": "complaints", "source": "AER_RETAIL"},
    {"metric_type": "COMPLAINTS", "retailer": "EnergyAustralia", "state": "ALL", "year": 2024, "quarter": "Q4", "metric_value": 2900, "metric_unit": "complaints", "source": "AER_RETAIL"},
    {"metric_type": "COMPLAINTS", "retailer": "Alinta Energy", "state": "ALL", "year": 2024, "quarter": "Q4", "metric_value": 1200, "metric_unit": "complaints", "source": "AER_RETAIL"},
    {"metric_type": "COMPLAINTS", "retailer": "Red Energy", "state": "ALL", "year": 2024, "quarter": "Q4", "metric_value": 850, "metric_unit": "complaints", "source": "AER_RETAIL"},
    {"metric_type": "COMPLAINTS", "retailer": "Powershop", "state": "ALL", "year": 2024, "quarter": "Q4", "metric_value": 620, "metric_unit": "complaints", "source": "AER_RETAIL"},
    # Hardship program customers
    {"metric_type": "HARDSHIP_CUSTOMERS", "retailer": "AGL Energy", "state": "ALL", "year": 2024, "quarter": "Q4", "metric_value": 42000, "metric_unit": "customers", "source": "AER_RETAIL"},
    {"metric_type": "HARDSHIP_CUSTOMERS", "retailer": "Origin Energy", "state": "ALL", "year": 2024, "quarter": "Q4", "metric_value": 38000, "metric_unit": "customers", "source": "AER_RETAIL"},
    {"metric_type": "HARDSHIP_CUSTOMERS", "retailer": "EnergyAustralia", "state": "ALL", "year": 2024, "quarter": "Q4", "metric_value": 29000, "metric_unit": "customers", "source": "AER_RETAIL"},
    # Disconnections
    {"metric_type": "DISCONNECTIONS", "retailer": "ALL", "state": "NSW", "year": 2024, "quarter": "Q4", "metric_value": 12500, "metric_unit": "disconnections", "source": "AER_RETAIL"},
    {"metric_type": "DISCONNECTIONS", "retailer": "ALL", "state": "VIC", "year": 2024, "quarter": "Q4", "metric_value": 9800, "metric_unit": "disconnections", "source": "AER_RETAIL"},
    {"metric_type": "DISCONNECTIONS", "retailer": "ALL", "state": "QLD", "year": 2024, "quarter": "Q4", "metric_value": 11200, "metric_unit": "disconnections", "source": "AER_RETAIL"},
    {"metric_type": "DISCONNECTIONS", "retailer": "ALL", "state": "SA", "year": 2024, "quarter": "Q4", "metric_value": 4500, "metric_unit": "disconnections", "source": "AER_RETAIL"},
    # Average retail offers
    {"metric_type": "AVG_OFFER_RATE", "retailer": "ALL", "state": "NSW", "year": 2024, "quarter": "Q4", "metric_value": 0.32, "metric_unit": "aud_per_kwh", "source": "AER_CDR"},
    {"metric_type": "AVG_OFFER_RATE", "retailer": "ALL", "state": "VIC", "year": 2024, "quarter": "Q4", "metric_value": 0.28, "metric_unit": "aud_per_kwh", "source": "AER_CDR"},
    {"metric_type": "AVG_OFFER_RATE", "retailer": "ALL", "state": "QLD", "year": 2024, "quarter": "Q4", "metric_value": 0.30, "metric_unit": "aud_per_kwh", "source": "AER_CDR"},
    {"metric_type": "AVG_OFFER_RATE", "retailer": "ALL", "state": "SA", "year": 2024, "quarter": "Q4", "metric_value": 0.35, "metric_unit": "aud_per_kwh", "source": "AER_CDR"},
]

# COMMAND ----------

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

schema = StructType([
    StructField("record_id", StringType()), StructField("metric_type", StringType()),
    StructField("retailer", StringType()), StructField("state", StringType()),
    StructField("year", IntegerType()), StructField("quarter", StringType()),
    StructField("metric_value", DoubleType()), StructField("metric_unit", StringType()),
    StructField("source", StringType()), StructField("ingested_at", TimestampType()),
])

rows = []
for d in CONSUMER_DATA:
    rid = hashlib.md5(f"{d['metric_type']}_{d['retailer']}_{d['state']}_{d['year']}_{d['quarter']}".encode()).hexdigest()[:16]
    rows.append((rid, d["metric_type"], d["retailer"], d["state"], d["year"], d["quarter"],
                  d["metric_value"], d["metric_unit"], d["source"],
                  datetime.strptime(now, "%Y-%m-%d %H:%M:%S")))

df = spark.createDataFrame(rows, schema=schema)
df.createOrReplaceTempView("src_consumer")

spark.sql(f"""
MERGE INTO {TABLE} t
USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY ingested_at DESC) as _rn FROM src_consumer) WHERE _rn = 1) s
ON t.record_id = s.record_id
WHEN MATCHED THEN UPDATE SET
    metric_type=s.metric_type, retailer=s.retailer, state=s.state, year=s.year, quarter=s.quarter,
    metric_value=s.metric_value, metric_unit=s.metric_unit, source=s.source, ingested_at=s.ingested_at
WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
print(f"consumer_protection: {cnt}")
