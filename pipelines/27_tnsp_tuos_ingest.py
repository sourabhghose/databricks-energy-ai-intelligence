# Databricks notebook source
"""
Pipeline 27: TNSP Performance & TUOS Charges

Seeds TNSP performance metrics and TUoS charge schedules.

Target: energy_copilot_catalog.gold.tnsp_performance, gold.tuos_charges
Schedule: Quarterly (1st Monday of Jan/Apr/Jul/Oct, 11:30am AEST)
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

TABLE_TNSP = f"{catalog}.gold.tnsp_performance"
TABLE_TUOS = f"{catalog}.gold.tuos_charges"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_TNSP} (
    record_id STRING,
    tnsp STRING,
    state STRING,
    year INT,
    circuit_km DOUBLE,
    energy_delivered_gwh DOUBLE,
    peak_demand_mw DOUBLE,
    loss_factor_pct DOUBLE,
    unserved_energy_mwh DOUBLE,
    opex_per_km_aud DOUBLE,
    capex_m_aud DOUBLE,
    ingested_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_TUOS} (
    record_id STRING,
    tnsp STRING,
    state STRING,
    financial_year STRING,
    locational_aud_per_kw DOUBLE,
    non_locational_aud_per_mwh DOUBLE,
    common_service_aud_per_mwh DOUBLE,
    total_tuos_revenue_m_aud DOUBLE,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

TNSP_DATA = [
    {"tnsp": "TransGrid", "state": "NSW", "year": 2024, "circuit_km": 12900, "energy_delivered_gwh": 68500, "peak_demand_mw": 13200, "loss_factor_pct": 2.1, "unserved_energy_mwh": 0, "opex_per_km_aud": 18500, "capex_m_aud": 1150},
    {"tnsp": "TransGrid", "state": "NSW", "year": 2023, "circuit_km": 12800, "energy_delivered_gwh": 67200, "peak_demand_mw": 13000, "loss_factor_pct": 2.2, "unserved_energy_mwh": 0, "opex_per_km_aud": 17800, "capex_m_aud": 980},
    {"tnsp": "AusNet Transmission", "state": "VIC", "year": 2024, "circuit_km": 6500, "energy_delivered_gwh": 42500, "peak_demand_mw": 9200, "loss_factor_pct": 2.8, "unserved_energy_mwh": 0, "opex_per_km_aud": 22100, "capex_m_aud": 420},
    {"tnsp": "AusNet Transmission", "state": "VIC", "year": 2023, "circuit_km": 6500, "energy_delivered_gwh": 41800, "peak_demand_mw": 9000, "loss_factor_pct": 2.9, "unserved_energy_mwh": 0, "opex_per_km_aud": 21500, "capex_m_aud": 390},
    {"tnsp": "Powerlink", "state": "QLD", "year": 2024, "circuit_km": 15300, "energy_delivered_gwh": 52000, "peak_demand_mw": 10500, "loss_factor_pct": 3.1, "unserved_energy_mwh": 0, "opex_per_km_aud": 14200, "capex_m_aud": 650},
    {"tnsp": "Powerlink", "state": "QLD", "year": 2023, "circuit_km": 15200, "energy_delivered_gwh": 51200, "peak_demand_mw": 10300, "loss_factor_pct": 3.2, "unserved_energy_mwh": 0, "opex_per_km_aud": 13800, "capex_m_aud": 580},
    {"tnsp": "ElectraNet", "state": "SA", "year": 2024, "circuit_km": 5600, "energy_delivered_gwh": 11800, "peak_demand_mw": 3200, "loss_factor_pct": 3.5, "unserved_energy_mwh": 0, "opex_per_km_aud": 25800, "capex_m_aud": 280},
    {"tnsp": "ElectraNet", "state": "SA", "year": 2023, "circuit_km": 5600, "energy_delivered_gwh": 11500, "peak_demand_mw": 3100, "loss_factor_pct": 3.6, "unserved_energy_mwh": 0, "opex_per_km_aud": 25200, "capex_m_aud": 250},
    {"tnsp": "TasNetworks Transmission", "state": "TAS", "year": 2024, "circuit_km": 3500, "energy_delivered_gwh": 10200, "peak_demand_mw": 1800, "loss_factor_pct": 2.5, "unserved_energy_mwh": 0, "opex_per_km_aud": 19500, "capex_m_aud": 120},
    {"tnsp": "TasNetworks Transmission", "state": "TAS", "year": 2023, "circuit_km": 3500, "energy_delivered_gwh": 10000, "peak_demand_mw": 1750, "loss_factor_pct": 2.6, "unserved_energy_mwh": 0, "opex_per_km_aud": 19000, "capex_m_aud": 110},
]

TUOS_DATA = [
    {"tnsp": "TransGrid", "state": "NSW", "financial_year": "FY2025", "locational_aud_per_kw": 45.20, "non_locational_aud_per_mwh": 3.80, "common_service_aud_per_mwh": 1.20, "total_tuos_revenue_m_aud": 1420},
    {"tnsp": "AusNet Transmission", "state": "VIC", "financial_year": "FY2025", "locational_aud_per_kw": 38.50, "non_locational_aud_per_mwh": 4.10, "common_service_aud_per_mwh": 1.50, "total_tuos_revenue_m_aud": 520},
    {"tnsp": "Powerlink", "state": "QLD", "financial_year": "FY2025", "locational_aud_per_kw": 42.80, "non_locational_aud_per_mwh": 3.50, "common_service_aud_per_mwh": 1.10, "total_tuos_revenue_m_aud": 890},
    {"tnsp": "ElectraNet", "state": "SA", "financial_year": "FY2025", "locational_aud_per_kw": 52.60, "non_locational_aud_per_mwh": 5.20, "common_service_aud_per_mwh": 1.80, "total_tuos_revenue_m_aud": 360},
    {"tnsp": "TasNetworks Transmission", "state": "TAS", "financial_year": "FY2025", "locational_aud_per_kw": 35.00, "non_locational_aud_per_mwh": 3.20, "common_service_aud_per_mwh": 1.00, "total_tuos_revenue_m_aud": 180},
]

# COMMAND ----------

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
ts = datetime.strptime(now, "%Y-%m-%d %H:%M:%S")

tnsp_schema = StructType([
    StructField("record_id", StringType()), StructField("tnsp", StringType()),
    StructField("state", StringType()), StructField("year", IntegerType()),
    StructField("circuit_km", DoubleType()), StructField("energy_delivered_gwh", DoubleType()),
    StructField("peak_demand_mw", DoubleType()), StructField("loss_factor_pct", DoubleType()),
    StructField("unserved_energy_mwh", DoubleType()), StructField("opex_per_km_aud", DoubleType()),
    StructField("capex_m_aud", DoubleType()), StructField("ingested_at", TimestampType()),
])

rows = []
for d in TNSP_DATA:
    rid = hashlib.md5(f"{d['tnsp']}_{d['year']}".encode()).hexdigest()[:16]
    rows.append((rid, d["tnsp"], d["state"], d["year"], d["circuit_km"],
                  d["energy_delivered_gwh"], d["peak_demand_mw"], d["loss_factor_pct"],
                  d["unserved_energy_mwh"], d["opex_per_km_aud"], d["capex_m_aud"], ts))

df = spark.createDataFrame(rows, schema=tnsp_schema)
df.createOrReplaceTempView("src_tnsp")

spark.sql(f"""
MERGE INTO {TABLE_TNSP} t
USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY ingested_at DESC) as _rn FROM src_tnsp) WHERE _rn = 1) s
ON t.record_id = s.record_id
WHEN MATCHED THEN UPDATE SET
    tnsp=s.tnsp, state=s.state, year=s.year, circuit_km=s.circuit_km,
    energy_delivered_gwh=s.energy_delivered_gwh, peak_demand_mw=s.peak_demand_mw,
    loss_factor_pct=s.loss_factor_pct, unserved_energy_mwh=s.unserved_energy_mwh,
    opex_per_km_aud=s.opex_per_km_aud, capex_m_aud=s.capex_m_aud, ingested_at=s.ingested_at
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

tuos_schema = StructType([
    StructField("record_id", StringType()), StructField("tnsp", StringType()),
    StructField("state", StringType()), StructField("financial_year", StringType()),
    StructField("locational_aud_per_kw", DoubleType()), StructField("non_locational_aud_per_mwh", DoubleType()),
    StructField("common_service_aud_per_mwh", DoubleType()), StructField("total_tuos_revenue_m_aud", DoubleType()),
    StructField("ingested_at", TimestampType()),
])

rows2 = []
for d in TUOS_DATA:
    rid = hashlib.md5(f"{d['tnsp']}_{d['financial_year']}".encode()).hexdigest()[:16]
    rows2.append((rid, d["tnsp"], d["state"], d["financial_year"],
                   d["locational_aud_per_kw"], d["non_locational_aud_per_mwh"],
                   d["common_service_aud_per_mwh"], d["total_tuos_revenue_m_aud"], ts))

df2 = spark.createDataFrame(rows2, schema=tuos_schema)
df2.createOrReplaceTempView("src_tuos")

spark.sql(f"""
MERGE INTO {TABLE_TUOS} t
USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY ingested_at DESC) as _rn FROM src_tuos) WHERE _rn = 1) s
ON t.record_id = s.record_id
WHEN MATCHED THEN UPDATE SET
    tnsp=s.tnsp, state=s.state, financial_year=s.financial_year,
    locational_aud_per_kw=s.locational_aud_per_kw, non_locational_aud_per_mwh=s.non_locational_aud_per_mwh,
    common_service_aud_per_mwh=s.common_service_aud_per_mwh, total_tuos_revenue_m_aud=s.total_tuos_revenue_m_aud,
    ingested_at=s.ingested_at
WHEN NOT MATCHED THEN INSERT *
""")

cnt1 = spark.sql(f"SELECT count(*) FROM {TABLE_TNSP}").collect()[0][0]
cnt2 = spark.sql(f"SELECT count(*) FROM {TABLE_TUOS}").collect()[0][0]
print(f"tnsp_performance: {cnt1}, tuos_charges: {cnt2}")
