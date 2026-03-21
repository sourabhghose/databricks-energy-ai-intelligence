# Databricks notebook source
"""
Pipeline 29: Offshore Wind Declared Areas Register

Seeds DCCEEW offshore wind declared areas and licence applications.

Target: energy_copilot_catalog.gold.offshore_wind_areas
Schedule: Quarterly (1st Monday of Jan/Apr/Jul/Oct, 12:30pm AEST)
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

TABLE = f"{catalog}.gold.offshore_wind_areas"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    area_id STRING,
    area_name STRING,
    state STRING,
    declared_date STRING,
    area_sq_km DOUBLE,
    estimated_capacity_gw DOUBLE,
    water_depth_m STRING,
    distance_from_shore_km DOUBLE,
    licence_applicants INT,
    status STRING,
    key_projects STRING,
    description STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# Real DCCEEW Offshore Wind Declared Areas (as of 2024)
AREAS = [
    {"area_name": "Gippsland", "state": "VIC", "declared_date": "2022-12-16", "area_sq_km": 15742, "estimated_capacity_gw": 10.0, "water_depth_m": "20-60", "distance_from_shore_km": 10, "licence_applicants": 8, "status": "FEASIBILITY_LICENCES_ISSUED", "key_projects": "Star of the South (2.2GW), Flotation Energy (1.5GW), Oceanex (2GW)", "description": "Australia's first declared offshore wind area. Gippsland offers excellent wind resources and proximity to Melbourne load centre. Star of the South is the most advanced project."},
    {"area_name": "Hunter", "state": "NSW", "declared_date": "2023-07-14", "area_sq_km": 1854, "estimated_capacity_gw": 5.0, "water_depth_m": "30-80", "distance_from_shore_km": 20, "licence_applicants": 5, "status": "LICENCE_APPLICATIONS_OPEN", "key_projects": "Novocastrian Wind (2GW), Hunter Wind Holdings (1.5GW)", "description": "Located off the Hunter coast near Newcastle. Strategic location to replace retiring coal generation in the Hunter Valley."},
    {"area_name": "Illawarra", "state": "NSW", "declared_date": "2023-12-08", "area_sq_km": 1461, "estimated_capacity_gw": 4.0, "water_depth_m": "40-100", "distance_from_shore_km": 25, "licence_applicants": 3, "status": "LICENCE_APPLICATIONS_OPEN", "key_projects": "BlueFloat Energy Illawarra", "description": "Off the Illawarra coast south of Sydney. Deeper waters suitable for floating offshore wind technology."},
    {"area_name": "Southern Ocean (Portland)", "state": "VIC", "declared_date": "2024-03-15", "area_sq_km": 2100, "estimated_capacity_gw": 3.0, "water_depth_m": "40-80", "distance_from_shore_km": 15, "licence_applicants": 2, "status": "DECLARED", "key_projects": "Spinifex Offshore Wind", "description": "Off the western Victorian coast near Portland. Leverages existing aluminium smelter industrial load."},
    {"area_name": "Bass Strait", "state": "VIC/TAS", "declared_date": "2024-06-01", "area_sq_km": 3200, "estimated_capacity_gw": 5.0, "water_depth_m": "30-70", "distance_from_shore_km": 30, "licence_applicants": 1, "status": "DECLARED", "key_projects": "TBC", "description": "Central Bass Strait area with excellent wind resources. Potential to complement Marinus Link capacity."},
    {"area_name": "Pacific Ocean (North QLD)", "state": "QLD", "declared_date": "2024-09-01", "area_sq_km": 1800, "estimated_capacity_gw": 2.5, "water_depth_m": "30-60", "distance_from_shore_km": 20, "licence_applicants": 0, "status": "UNDER_ASSESSMENT", "key_projects": "TBC", "description": "Proposed area off north Queensland coast. Under assessment for declaration."},
]

# COMMAND ----------

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

schema = StructType([
    StructField("area_id", StringType()), StructField("area_name", StringType()),
    StructField("state", StringType()), StructField("declared_date", StringType()),
    StructField("area_sq_km", DoubleType()), StructField("estimated_capacity_gw", DoubleType()),
    StructField("water_depth_m", StringType()), StructField("distance_from_shore_km", DoubleType()),
    StructField("licence_applicants", IntegerType()), StructField("status", StringType()),
    StructField("key_projects", StringType()), StructField("description", StringType()),
    StructField("ingested_at", TimestampType()),
])

rows = []
for a in AREAS:
    aid = hashlib.md5(a["area_name"].encode()).hexdigest()[:16]
    rows.append((aid, a["area_name"], a["state"], a["declared_date"],
                  a["area_sq_km"], a["estimated_capacity_gw"], a["water_depth_m"],
                  a["distance_from_shore_km"], a["licence_applicants"], a["status"],
                  a["key_projects"], a["description"],
                  datetime.strptime(now, "%Y-%m-%d %H:%M:%S")))

df = spark.createDataFrame(rows, schema=schema)
df.createOrReplaceTempView("src_offshore_wind")

spark.sql(f"""
MERGE INTO {TABLE} t
USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY area_id ORDER BY ingested_at DESC) as _rn FROM src_offshore_wind) WHERE _rn = 1) s
ON t.area_id = s.area_id
WHEN MATCHED THEN UPDATE SET
    area_name=s.area_name, state=s.state, declared_date=s.declared_date, area_sq_km=s.area_sq_km,
    estimated_capacity_gw=s.estimated_capacity_gw, water_depth_m=s.water_depth_m,
    distance_from_shore_km=s.distance_from_shore_km, licence_applicants=s.licence_applicants,
    status=s.status, key_projects=s.key_projects, description=s.description, ingested_at=s.ingested_at
WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
print(f"offshore_wind_areas: {cnt}")
