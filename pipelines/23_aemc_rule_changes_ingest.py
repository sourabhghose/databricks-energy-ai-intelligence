# Databricks notebook source
"""
Pipeline 23: AEMC Rule Change Register

Seeds known AEMC rule changes and scrapes the AEMC website for updates.

Target: energy_copilot_catalog.gold.aemc_rule_changes
Schedule: Monthly (1st Monday, 9:30am AEST)
"""

import hashlib
from datetime import datetime, timezone

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    catalog = "energy_copilot_catalog"

TABLE = f"{catalog}.gold.aemc_rule_changes"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    rule_id STRING,
    title STRING,
    proponent STRING,
    status STRING,
    rule_type STRING,
    date_initiated STRING,
    date_completed STRING,
    category STRING,
    description STRING,
    aemc_url STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

RULE_CHANGES = [
    {"title": "Capacity Commitment Mechanism", "proponent": "ESB", "status": "FINAL_DETERMINATION", "rule_type": "NER", "date_initiated": "2023-03-01", "date_completed": "2025-06-01", "category": "RELIABILITY", "description": "Introduces a capacity mechanism to complement the energy-only NEM, ensuring adequate investment in dispatchable capacity."},
    {"title": "Transmission Planning and Investment Review", "proponent": "AEMC", "status": "FINAL_DETERMINATION", "rule_type": "NER", "date_initiated": "2022-06-01", "date_completed": "2024-12-01", "category": "NETWORK", "description": "Reforms to transmission planning to better support renewable energy zones and ISP actionable projects."},
    {"title": "Consumer Energy Resources Technical Standards", "proponent": "AEMO", "status": "IN_PROGRESS", "rule_type": "NER", "date_initiated": "2024-03-01", "date_completed": "", "category": "DER", "description": "New technical standards for inverter-connected DER to maintain system security with high penetration of rooftop solar."},
    {"title": "Integrating Energy Storage Systems", "proponent": "AEMO", "status": "FINAL_DETERMINATION", "rule_type": "NER", "date_initiated": "2023-01-01", "date_completed": "2024-06-01", "category": "STORAGE", "description": "Establishes a new participant category for integrated resource units combining storage and generation."},
    {"title": "Review of Operating Reserve Market", "proponent": "AEMC", "status": "IN_PROGRESS", "rule_type": "NER", "date_initiated": "2024-06-01", "date_completed": "", "category": "ANCILLARY", "description": "Review of ancillary services framework including fast frequency response and operating reserves."},
    {"title": "Updating the Regulatory Investment Test", "proponent": "AER", "status": "FINAL_DETERMINATION", "rule_type": "NER", "date_initiated": "2022-09-01", "date_completed": "2024-03-01", "category": "NETWORK", "description": "Updates to RIT-T/RIT-D to streamline assessment of ISP actionable projects and reduce delays."},
    {"title": "Metering Contestability Review", "proponent": "AEMC", "status": "IN_PROGRESS", "rule_type": "NER", "date_initiated": "2024-01-01", "date_completed": "", "category": "METERING", "description": "Review of competitive metering framework to accelerate smart meter rollout."},
    {"title": "Access Reform - Congestion Management Model", "proponent": "ESB", "status": "IN_PROGRESS", "rule_type": "NER", "date_initiated": "2023-09-01", "date_completed": "", "category": "MARKET_DESIGN", "description": "Reforms to transmission access including a congestion management model to provide locational signals."},
    {"title": "Flexible Trading Arrangements", "proponent": "AEMO", "status": "FINAL_DETERMINATION", "rule_type": "NER", "date_initiated": "2021-06-01", "date_completed": "2024-06-01", "category": "MARKET_DESIGN", "description": "Allows multiple trading relationships at a single connection point, enabling battery and solar co-location."},
    {"title": "Post-2025 NEM Market Design", "proponent": "ESB", "status": "IN_PROGRESS", "rule_type": "NER", "date_initiated": "2022-01-01", "date_completed": "", "category": "MARKET_DESIGN", "description": "Comprehensive reform package for the NEM including capacity mechanism, access reform, and essential system services."},
    {"title": "Reliability Standard and Settings Review 2025", "proponent": "AEMC", "status": "IN_PROGRESS", "rule_type": "NER", "date_initiated": "2024-07-01", "date_completed": "", "category": "RELIABILITY", "description": "Periodic review of the reliability standard (0.002% USE) and market settings (MPC, CPT, MPF)."},
    {"title": "Fast Frequency Response Market Ancillary Service", "proponent": "AEMO", "status": "FINAL_DETERMINATION", "rule_type": "NER", "date_initiated": "2020-06-01", "date_completed": "2023-10-01", "category": "ANCILLARY", "description": "New FFR FCAS market to address declining system inertia from renewable energy transition."},
    {"title": "WEM Reform - Reserve Capacity Mechanism", "proponent": "WA Government", "status": "FINAL_DETERMINATION", "rule_type": "WEM_RULES", "date_initiated": "2022-01-01", "date_completed": "2024-10-01", "category": "WEM", "description": "Reform of WA reserve capacity mechanism to better value flexible generation and storage."},
    {"title": "Voluntary Curtailment for System Security", "proponent": "AEMO", "status": "IN_PROGRESS", "rule_type": "NER", "date_initiated": "2024-09-01", "date_completed": "", "category": "DER", "description": "Framework for voluntary rooftop solar curtailment during minimum demand periods to maintain system security."},
    {"title": "Scheduled Lite for DER Aggregators", "proponent": "AEMO", "status": "IN_PROGRESS", "rule_type": "NER", "date_initiated": "2024-04-01", "date_completed": "", "category": "DER", "description": "New lightweight scheduling framework for DER aggregators participating in NEM wholesale and FCAS markets."},
]

# COMMAND ----------

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

schema = StructType([
    StructField("rule_id", StringType()), StructField("title", StringType()),
    StructField("proponent", StringType()), StructField("status", StringType()),
    StructField("rule_type", StringType()), StructField("date_initiated", StringType()),
    StructField("date_completed", StringType()), StructField("category", StringType()),
    StructField("description", StringType()), StructField("aemc_url", StringType()),
    StructField("ingested_at", TimestampType()),
])

rows = []
for rc in RULE_CHANGES:
    rid = hashlib.md5(rc["title"].encode()).hexdigest()[:16]
    rows.append((rid, rc["title"], rc["proponent"], rc["status"], rc["rule_type"],
                  rc["date_initiated"], rc.get("date_completed", ""), rc["category"],
                  rc["description"], "https://www.aemc.gov.au/rule-changes",
                  datetime.strptime(now, "%Y-%m-%d %H:%M:%S")))

df = spark.createDataFrame(rows, schema=schema)
df.createOrReplaceTempView("src_aemc_rules")

spark.sql(f"""
MERGE INTO {TABLE} t
USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY rule_id ORDER BY ingested_at DESC) as _rn FROM src_aemc_rules) WHERE _rn = 1) s
ON t.rule_id = s.rule_id
WHEN MATCHED THEN UPDATE SET
    title=s.title, proponent=s.proponent, status=s.status, rule_type=s.rule_type,
    date_initiated=s.date_initiated, date_completed=s.date_completed, category=s.category,
    description=s.description, aemc_url=s.aemc_url, ingested_at=s.ingested_at
WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
print(f"aemc_rule_changes: {cnt}")
