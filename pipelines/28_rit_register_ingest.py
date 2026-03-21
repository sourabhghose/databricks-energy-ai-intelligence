# Databricks notebook source
"""
Pipeline 28: AER Regulatory Investment Test Register

Seeds the AER RIT-T/RIT-D register with active and completed projects.

Target: energy_copilot_catalog.gold.rit_register
Schedule: Quarterly (1st Monday of Jan/Apr/Jul/Oct, 12pm AEST)
"""

import hashlib
from datetime import datetime, timezone

from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    catalog = "energy_copilot_catalog"

TABLE = f"{catalog}.gold.rit_register"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    project_id STRING,
    project_name STRING,
    rit_type STRING,
    proponent STRING,
    state STRING,
    status STRING,
    estimated_cost_m_aud DOUBLE,
    net_benefit_m_aud DOUBLE,
    date_published STRING,
    date_completed STRING,
    preferred_option STRING,
    description STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

RIT_PROJECTS = [
    {"project_name": "HumeLink", "rit_type": "RIT-T", "proponent": "TransGrid", "state": "NSW", "status": "COMPLETED", "estimated_cost_m_aud": 3300, "net_benefit_m_aud": 2800, "date_published": "2021-03-01", "date_completed": "2023-06-01", "preferred_option": "500kV AC double circuit", "description": "330km transmission link connecting Snowy 2.0 to Sydney/Melbourne load centres via Bannaby and Wagga Wagga."},
    {"project_name": "VNI West (KerangLink)", "rit_type": "RIT-T", "proponent": "AEMO/TransGrid/AusNet", "state": "NSW/VIC", "status": "COMPLETED", "estimated_cost_m_aud": 2800, "net_benefit_m_aud": 1900, "date_published": "2020-06-01", "date_completed": "2023-03-01", "preferred_option": "Western Victorian route via Kerang", "description": "New 500kV interconnector between NSW and VIC via western corridor, enabling REZ access."},
    {"project_name": "Marinus Link", "rit_type": "RIT-T", "proponent": "TasNetworks/Marinus Link Pty Ltd", "state": "TAS/VIC", "status": "COMPLETED", "estimated_cost_m_aud": 3500, "net_benefit_m_aud": 1200, "date_published": "2019-01-01", "date_completed": "2022-12-01", "preferred_option": "2 x 750MW HVDC undersea cables", "description": "Second Bass Strait interconnector doubling Tasmania-Victoria transfer capacity."},
    {"project_name": "Sydney Ring Reinforcement", "rit_type": "RIT-T", "proponent": "TransGrid", "state": "NSW", "status": "IN_PROGRESS", "estimated_cost_m_aud": 1500, "net_benefit_m_aud": 800, "date_published": "2023-09-01", "date_completed": "", "preferred_option": "", "description": "Reinforcement of transmission supply into Greater Sydney to address load growth and coal retirement."},
    {"project_name": "QNI Connect (QNI Medium)", "rit_type": "RIT-T", "proponent": "Powerlink/TransGrid", "state": "QLD/NSW", "status": "IN_PROGRESS", "estimated_cost_m_aud": 2100, "net_benefit_m_aud": 1500, "date_published": "2022-06-01", "date_completed": "", "preferred_option": "", "description": "Upgrade of QLD-NSW interconnector capacity to enable renewable energy flows."},
    {"project_name": "Central West Orana REZ Network Infrastructure", "rit_type": "RIT-T", "proponent": "EnergyCo/TransGrid", "state": "NSW", "status": "COMPLETED", "estimated_cost_m_aud": 1200, "net_benefit_m_aud": 900, "date_published": "2021-06-01", "date_completed": "2023-09-01", "preferred_option": "500kV backbone with REZ connection", "description": "Transmission backbone for CWO REZ enabling 3GW of renewable generation connection."},
    {"project_name": "Western Renewables Link", "rit_type": "RIT-T", "proponent": "AusNet Services", "state": "VIC", "status": "COMPLETED", "estimated_cost_m_aud": 900, "net_benefit_m_aud": 600, "date_published": "2020-01-01", "date_completed": "2022-06-01", "preferred_option": "220kV double circuit Sydenham to Bulgana", "description": "New transmission link to unlock western Victorian wind and solar resources."},
    {"project_name": "Project EnergyConnect", "rit_type": "RIT-T", "proponent": "ElectraNet/TransGrid", "state": "SA/NSW", "status": "COMPLETED", "estimated_cost_m_aud": 2400, "net_benefit_m_aud": 1800, "date_published": "2018-06-01", "date_completed": "2021-03-01", "preferred_option": "330kV SA-NSW interconnector via Broken Hill", "description": "New SA-NSW interconnector improving SA reliability and enabling renewable export."},
    {"project_name": "Gladstone Grid Reinforcement", "rit_type": "RIT-T", "proponent": "Powerlink", "state": "QLD", "status": "IN_PROGRESS", "estimated_cost_m_aud": 800, "net_benefit_m_aud": 500, "date_published": "2024-01-01", "date_completed": "", "preferred_option": "", "description": "Grid reinforcement around Gladstone industrial zone to support hydrogen and green manufacturing."},
    {"project_name": "Melbourne Inner Ring Capacity", "rit_type": "RIT-T", "proponent": "AusNet Services", "state": "VIC", "status": "IN_PROGRESS", "estimated_cost_m_aud": 650, "net_benefit_m_aud": 400, "date_published": "2023-06-01", "date_completed": "", "preferred_option": "", "description": "Augmentation of inner Melbourne transmission network for EV charging and electrification load growth."},
    # RIT-D examples
    {"project_name": "Ausgrid Northern Beaches Supply", "rit_type": "RIT-D", "proponent": "Ausgrid", "state": "NSW", "status": "COMPLETED", "estimated_cost_m_aud": 85, "net_benefit_m_aud": 45, "date_published": "2022-06-01", "date_completed": "2023-12-01", "preferred_option": "Non-network + targeted 132kV augmentation", "description": "Supply augmentation for Northern Beaches area addressing peak demand growth."},
    {"project_name": "Powercor Ballarat Supply", "rit_type": "RIT-D", "proponent": "Powercor", "state": "VIC", "status": "COMPLETED", "estimated_cost_m_aud": 42, "net_benefit_m_aud": 28, "date_published": "2021-09-01", "date_completed": "2023-03-01", "preferred_option": "66kV line augmentation", "description": "Supply augmentation for Ballarat growth corridor."},
]

# COMMAND ----------

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

schema = StructType([
    StructField("project_id", StringType()), StructField("project_name", StringType()),
    StructField("rit_type", StringType()), StructField("proponent", StringType()),
    StructField("state", StringType()), StructField("status", StringType()),
    StructField("estimated_cost_m_aud", DoubleType()), StructField("net_benefit_m_aud", DoubleType()),
    StructField("date_published", StringType()), StructField("date_completed", StringType()),
    StructField("preferred_option", StringType()), StructField("description", StringType()),
    StructField("ingested_at", TimestampType()),
])

rows = []
for p in RIT_PROJECTS:
    pid = hashlib.md5(p["project_name"].encode()).hexdigest()[:16]
    rows.append((pid, p["project_name"], p["rit_type"], p["proponent"], p["state"],
                  p["status"], p["estimated_cost_m_aud"], p["net_benefit_m_aud"],
                  p["date_published"], p.get("date_completed", ""), p.get("preferred_option", ""),
                  p["description"], datetime.strptime(now, "%Y-%m-%d %H:%M:%S")))

df = spark.createDataFrame(rows, schema=schema)
df.createOrReplaceTempView("src_rit")

spark.sql(f"""
MERGE INTO {TABLE} t
USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY ingested_at DESC) as _rn FROM src_rit) WHERE _rn = 1) s
ON t.project_id = s.project_id
WHEN MATCHED THEN UPDATE SET
    project_name=s.project_name, rit_type=s.rit_type, proponent=s.proponent, state=s.state,
    status=s.status, estimated_cost_m_aud=s.estimated_cost_m_aud, net_benefit_m_aud=s.net_benefit_m_aud,
    date_published=s.date_published, date_completed=s.date_completed, preferred_option=s.preferred_option,
    description=s.description, ingested_at=s.ingested_at
WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
print(f"rit_register: {cnt}")
