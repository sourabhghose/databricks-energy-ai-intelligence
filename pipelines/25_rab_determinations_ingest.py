# Databricks notebook source
"""
Pipeline 25: AER RAB & Network Determinations

Seeds AER network regulatory determinations (RAB values, WACC, capex allowances).

Target: energy_copilot_catalog.gold.rab_determinations
Schedule: Quarterly (1st Monday of Jan/Apr/Jul/Oct, 10:30am AEST)
"""

import hashlib
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    catalog = "energy_copilot_catalog"

TABLE = f"{catalog}.gold.rab_determinations"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    determination_id STRING,
    network_business STRING,
    network_type STRING,
    state STRING,
    regulatory_period STRING,
    period_start STRING,
    period_end STRING,
    rab_opening_m_aud DOUBLE,
    rab_closing_m_aud DOUBLE,
    allowed_revenue_m_aud DOUBLE,
    capex_allowance_m_aud DOUBLE,
    opex_allowance_m_aud DOUBLE,
    wacc_nominal_pct DOUBLE,
    status STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

DETERMINATIONS = [
    {"network_business": "Ausgrid", "network_type": "DNSP", "state": "NSW", "regulatory_period": "2024-2029", "period_start": "2024-07-01", "period_end": "2029-06-30", "rab_opening_m_aud": 12800, "rab_closing_m_aud": 14200, "allowed_revenue_m_aud": 8950, "capex_allowance_m_aud": 4200, "opex_allowance_m_aud": 3100, "wacc_nominal_pct": 6.2, "status": "FINAL"},
    {"network_business": "Endeavour Energy", "network_type": "DNSP", "state": "NSW", "regulatory_period": "2024-2029", "period_start": "2024-07-01", "period_end": "2029-06-30", "rab_opening_m_aud": 6100, "rab_closing_m_aud": 7000, "allowed_revenue_m_aud": 4350, "capex_allowance_m_aud": 2100, "opex_allowance_m_aud": 1500, "wacc_nominal_pct": 6.2, "status": "FINAL"},
    {"network_business": "Essential Energy", "network_type": "DNSP", "state": "NSW", "regulatory_period": "2024-2029", "period_start": "2024-07-01", "period_end": "2029-06-30", "rab_opening_m_aud": 7200, "rab_closing_m_aud": 8100, "allowed_revenue_m_aud": 5100, "capex_allowance_m_aud": 2500, "opex_allowance_m_aud": 1800, "wacc_nominal_pct": 6.2, "status": "FINAL"},
    {"network_business": "CitiPower", "network_type": "DNSP", "state": "VIC", "regulatory_period": "2021-2026", "period_start": "2021-01-01", "period_end": "2025-12-31", "rab_opening_m_aud": 2800, "rab_closing_m_aud": 3100, "allowed_revenue_m_aud": 2050, "capex_allowance_m_aud": 750, "opex_allowance_m_aud": 550, "wacc_nominal_pct": 5.8, "status": "FINAL"},
    {"network_business": "Powercor", "network_type": "DNSP", "state": "VIC", "regulatory_period": "2021-2026", "period_start": "2021-01-01", "period_end": "2025-12-31", "rab_opening_m_aud": 5900, "rab_closing_m_aud": 6600, "allowed_revenue_m_aud": 4200, "capex_allowance_m_aud": 1800, "opex_allowance_m_aud": 1200, "wacc_nominal_pct": 5.8, "status": "FINAL"},
    {"network_business": "AusNet Services", "network_type": "DNSP", "state": "VIC", "regulatory_period": "2021-2026", "period_start": "2021-01-01", "period_end": "2025-12-31", "rab_opening_m_aud": 5100, "rab_closing_m_aud": 5700, "allowed_revenue_m_aud": 3800, "capex_allowance_m_aud": 1600, "opex_allowance_m_aud": 1100, "wacc_nominal_pct": 5.8, "status": "FINAL"},
    {"network_business": "Jemena", "network_type": "DNSP", "state": "VIC", "regulatory_period": "2021-2026", "period_start": "2021-01-01", "period_end": "2025-12-31", "rab_opening_m_aud": 2200, "rab_closing_m_aud": 2500, "allowed_revenue_m_aud": 1650, "capex_allowance_m_aud": 600, "opex_allowance_m_aud": 450, "wacc_nominal_pct": 5.8, "status": "FINAL"},
    {"network_business": "United Energy", "network_type": "DNSP", "state": "VIC", "regulatory_period": "2021-2026", "period_start": "2021-01-01", "period_end": "2025-12-31", "rab_opening_m_aud": 2600, "rab_closing_m_aud": 2900, "allowed_revenue_m_aud": 1900, "capex_allowance_m_aud": 700, "opex_allowance_m_aud": 500, "wacc_nominal_pct": 5.8, "status": "FINAL"},
    {"network_business": "Energex", "network_type": "DNSP", "state": "QLD", "regulatory_period": "2020-2025", "period_start": "2020-07-01", "period_end": "2025-06-30", "rab_opening_m_aud": 10200, "rab_closing_m_aud": 11000, "allowed_revenue_m_aud": 6800, "capex_allowance_m_aud": 2800, "opex_allowance_m_aud": 2000, "wacc_nominal_pct": 5.5, "status": "FINAL"},
    {"network_business": "Ergon Energy", "network_type": "DNSP", "state": "QLD", "regulatory_period": "2020-2025", "period_start": "2020-07-01", "period_end": "2025-06-30", "rab_opening_m_aud": 11100, "rab_closing_m_aud": 11900, "allowed_revenue_m_aud": 7500, "capex_allowance_m_aud": 3200, "opex_allowance_m_aud": 2400, "wacc_nominal_pct": 5.5, "status": "FINAL"},
    {"network_business": "SA Power Networks", "network_type": "DNSP", "state": "SA", "regulatory_period": "2020-2025", "period_start": "2020-07-01", "period_end": "2025-06-30", "rab_opening_m_aud": 4200, "rab_closing_m_aud": 4600, "allowed_revenue_m_aud": 3000, "capex_allowance_m_aud": 1100, "opex_allowance_m_aud": 850, "wacc_nominal_pct": 5.5, "status": "FINAL"},
    {"network_business": "TasNetworks", "network_type": "DNSP", "state": "TAS", "regulatory_period": "2024-2029", "period_start": "2024-07-01", "period_end": "2029-06-30", "rab_opening_m_aud": 1800, "rab_closing_m_aud": 2100, "allowed_revenue_m_aud": 1350, "capex_allowance_m_aud": 650, "opex_allowance_m_aud": 400, "wacc_nominal_pct": 6.2, "status": "FINAL"},
    # TNSPs
    {"network_business": "TransGrid", "network_type": "TNSP", "state": "NSW", "regulatory_period": "2023-2028", "period_start": "2023-07-01", "period_end": "2028-06-30", "rab_opening_m_aud": 8100, "rab_closing_m_aud": 11500, "allowed_revenue_m_aud": 7200, "capex_allowance_m_aud": 5800, "opex_allowance_m_aud": 1200, "wacc_nominal_pct": 6.0, "status": "FINAL"},
    {"network_business": "AusNet Transmission", "network_type": "TNSP", "state": "VIC", "regulatory_period": "2022-2027", "period_start": "2022-04-01", "period_end": "2027-03-31", "rab_opening_m_aud": 3400, "rab_closing_m_aud": 4000, "allowed_revenue_m_aud": 2500, "capex_allowance_m_aud": 1200, "opex_allowance_m_aud": 600, "wacc_nominal_pct": 5.9, "status": "FINAL"},
    {"network_business": "Powerlink", "network_type": "TNSP", "state": "QLD", "regulatory_period": "2022-2027", "period_start": "2022-07-01", "period_end": "2027-06-30", "rab_opening_m_aud": 6800, "rab_closing_m_aud": 7500, "allowed_revenue_m_aud": 4500, "capex_allowance_m_aud": 2000, "opex_allowance_m_aud": 800, "wacc_nominal_pct": 5.9, "status": "FINAL"},
    {"network_business": "ElectraNet", "network_type": "TNSP", "state": "SA", "regulatory_period": "2023-2028", "period_start": "2023-07-01", "period_end": "2028-06-30", "rab_opening_m_aud": 2100, "rab_closing_m_aud": 2800, "allowed_revenue_m_aud": 1800, "capex_allowance_m_aud": 1100, "opex_allowance_m_aud": 400, "wacc_nominal_pct": 6.0, "status": "FINAL"},
]

# COMMAND ----------

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

schema = StructType([
    StructField("determination_id", StringType()), StructField("network_business", StringType()),
    StructField("network_type", StringType()), StructField("state", StringType()),
    StructField("regulatory_period", StringType()), StructField("period_start", StringType()),
    StructField("period_end", StringType()), StructField("rab_opening_m_aud", DoubleType()),
    StructField("rab_closing_m_aud", DoubleType()), StructField("allowed_revenue_m_aud", DoubleType()),
    StructField("capex_allowance_m_aud", DoubleType()), StructField("opex_allowance_m_aud", DoubleType()),
    StructField("wacc_nominal_pct", DoubleType()), StructField("status", StringType()),
    StructField("ingested_at", TimestampType()),
])

rows = []
for d in DETERMINATIONS:
    did = hashlib.md5(f"{d['network_business']}_{d['regulatory_period']}".encode()).hexdigest()[:16]
    rows.append((did, d["network_business"], d["network_type"], d["state"],
                  d["regulatory_period"], d["period_start"], d["period_end"],
                  d["rab_opening_m_aud"], d["rab_closing_m_aud"], d["allowed_revenue_m_aud"],
                  d["capex_allowance_m_aud"], d["opex_allowance_m_aud"], d["wacc_nominal_pct"],
                  d["status"], datetime.strptime(now, "%Y-%m-%d %H:%M:%S")))

df = spark.createDataFrame(rows, schema=schema)
df.createOrReplaceTempView("src_rab")

spark.sql(f"""
MERGE INTO {TABLE} t
USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY determination_id ORDER BY ingested_at DESC) as _rn FROM src_rab) WHERE _rn = 1) s
ON t.determination_id = s.determination_id
WHEN MATCHED THEN UPDATE SET
    network_business=s.network_business, network_type=s.network_type, state=s.state,
    regulatory_period=s.regulatory_period, period_start=s.period_start, period_end=s.period_end,
    rab_opening_m_aud=s.rab_opening_m_aud, rab_closing_m_aud=s.rab_closing_m_aud,
    allowed_revenue_m_aud=s.allowed_revenue_m_aud, capex_allowance_m_aud=s.capex_allowance_m_aud,
    opex_allowance_m_aud=s.opex_allowance_m_aud, wacc_nominal_pct=s.wacc_nominal_pct,
    status=s.status, ingested_at=s.ingested_at
WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
print(f"rab_determinations: {cnt}")
