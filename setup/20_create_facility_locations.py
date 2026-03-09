# Databricks notebook source
# ============================================================
# Create facility_locations Gold Table — NEM Infrastructure Map
# ============================================================
# Fetches OpenNEM stations.json for generator coordinates,
# supplements with hardcoded REZ, ISP, gas hub, and region
# centroid locations.
# ============================================================

# COMMAND ----------

catalog = "energy_copilot_catalog"
sp_id = "67aaaa6b-778c-4c8b-b2f0-9f9b9728b3bb"

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

# --- Create table ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.facility_locations (
    duid STRING,
    station_name STRING,
    lat DOUBLE,
    lng DOUBLE,
    state STRING,
    region_id STRING,
    fuel_type STRING,
    capacity_mw DOUBLE,
    status STRING,
    layer_type STRING
)
USING DELTA
COMMENT 'NEM facility locations with lat/lng for infrastructure map'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')
""")

# COMMAND ----------

# --- Fetch OpenNEM stations.json ---
import requests
import json

url = "https://raw.githubusercontent.com/opennem/opennem/main/opennem/data/stations.json"
try:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    stations_raw = resp.json()
    print(f"Fetched {len(stations_raw)} stations from OpenNEM")
except Exception as e:
    print(f"Failed to fetch OpenNEM stations: {e}")
    stations_raw = []

# COMMAND ----------

# --- OpenNEM fueltech → our fuel_type mapping ---
FUELTECH_MAP = {
    "coal_black": "coal_black",
    "coal_brown": "coal_brown",
    "gas_ccgt": "gas_ccgt",
    "gas_ocgt": "gas_ocgt",
    "gas_steam": "gas_steam",
    "gas_recip": "gas_recip",
    "gas_wcmg": "gas_wcgt",
    "hydro": "hydro",
    "pumps": "pumped_hydro",
    "wind": "wind",
    "solar_utility": "solar_utility",
    "solar_rooftop": "solar_rooftop",
    "battery_charging": "battery",
    "battery_discharging": "battery",
    "bioenergy_biomass": "bioenergy",
    "bioenergy_biogas": "bioenergy",
    "distillate": "distillate",
    "nuclear": "nuclear",
}

STATE_TO_REGION = {
    "NSW": "NSW1",
    "QLD": "QLD1",
    "VIC": "VIC1",
    "SA": "SA1",
    "TAS": "TAS1",
    "WA": "WA1",
    "NT": "NT1",
    "ACT": "NSW1",
}

# COMMAND ----------

# --- Parse stations into flat rows ---
# OpenNEM stations.json is a LIST of station objects.
# Each station has: code, name, location: {lat, lng, state}, facilities: [{code, fueltech_id, status_id, capacity_registered, network_region}]
rows = []
seen_duids = set()

for station_data in stations_raw:
    if not isinstance(station_data, dict):
        continue
    station_name = station_data.get("name", station_data.get("code", "unknown"))
    location = station_data.get("location") or {}
    lat = location.get("lat")
    lng = location.get("lng")
    state = location.get("state", "")

    if lat is None or lng is None:
        continue

    # Sanity check: must be in Australia roughly
    if lat > 0 or lat < -45 or lng < 110 or lng > 160:
        continue

    region_id = STATE_TO_REGION.get(state, "")

    facilities = station_data.get("facilities") or []
    if not facilities:
        station_code = station_data.get("code", station_name)
        rows.append({
            "duid": station_code,
            "station_name": station_name,
            "lat": float(lat),
            "lng": float(lng),
            "state": state,
            "region_id": region_id,
            "fuel_type": "unknown",
            "capacity_mw": 0.0,
            "status": "operating",
            "layer_type": "generator",
        })
        seen_duids.add(station_code)
        continue

    for fac_data in facilities:
        if not isinstance(fac_data, dict):
            continue
        duid = fac_data.get("code", "")
        if not duid or duid in seen_duids:
            continue
        seen_duids.add(duid)

        fueltech = fac_data.get("fueltech_id", "")
        fuel_type = FUELTECH_MAP.get(fueltech, fueltech if fueltech else "unknown")
        capacity = fac_data.get("capacity_registered", 0) or 0
        status = fac_data.get("status_id", "operating")
        fac_region = fac_data.get("network_region", region_id)

        rows.append({
            "duid": duid,
            "station_name": station_name,
            "lat": float(lat),
            "lng": float(lng),
            "state": state,
            "region_id": fac_region if fac_region else region_id,
            "fuel_type": fuel_type,
            "capacity_mw": float(capacity),
            "status": str(status),
            "layer_type": "generator",
        })

print(f"Parsed {len(rows)} generator facility rows")

# COMMAND ----------

# --- Hardcoded supplemental locations ---

# NEM region centroids (for interconnector lines)
REGION_CENTROIDS = [
    {"duid": "REGION_NSW1", "station_name": "NSW Region Centroid", "lat": -33.0, "lng": 149.0, "state": "NSW", "region_id": "NSW1", "fuel_type": "region_centroid", "capacity_mw": 0.0, "status": "active", "layer_type": "region"},
    {"duid": "REGION_QLD1", "station_name": "QLD Region Centroid", "lat": -24.5, "lng": 150.5, "state": "QLD", "region_id": "QLD1", "fuel_type": "region_centroid", "capacity_mw": 0.0, "status": "active", "layer_type": "region"},
    {"duid": "REGION_VIC1", "station_name": "VIC Region Centroid", "lat": -37.0, "lng": 145.0, "state": "VIC", "region_id": "VIC1", "fuel_type": "region_centroid", "capacity_mw": 0.0, "status": "active", "layer_type": "region"},
    {"duid": "REGION_SA1",  "station_name": "SA Region Centroid",  "lat": -34.0, "lng": 138.5, "state": "SA",  "region_id": "SA1",  "fuel_type": "region_centroid", "capacity_mw": 0.0, "status": "active", "layer_type": "region"},
    {"duid": "REGION_TAS1", "station_name": "TAS Region Centroid", "lat": -42.0, "lng": 146.5, "state": "TAS", "region_id": "TAS1", "fuel_type": "region_centroid", "capacity_mw": 0.0, "status": "active", "layer_type": "region"},
]

# Gas hubs
GAS_HUBS = [
    {"duid": "GAS_WALLUMBILLA", "station_name": "Wallumbilla Gas Hub", "lat": -26.59, "lng": 149.19, "state": "QLD", "region_id": "QLD1", "fuel_type": "gas_hub", "capacity_mw": 0.0, "status": "active", "layer_type": "gas_hub"},
    {"duid": "GAS_MOOMBA",      "station_name": "Moomba Gas Hub",      "lat": -28.10, "lng": 140.20, "state": "SA",  "region_id": "SA1",  "fuel_type": "gas_hub", "capacity_mw": 0.0, "status": "active", "layer_type": "gas_hub"},
    {"duid": "GAS_LONGFORD",    "station_name": "Longford Gas Hub",    "lat": -38.17, "lng": 147.07, "state": "VIC", "region_id": "VIC1", "fuel_type": "gas_hub", "capacity_mw": 0.0, "status": "active", "layer_type": "gas_hub"},
    {"duid": "GAS_GLADSTONE",   "station_name": "Gladstone LNG Hub",   "lat": -23.85, "lng": 151.27, "state": "QLD", "region_id": "QLD1", "fuel_type": "gas_hub", "capacity_mw": 0.0, "status": "active", "layer_type": "gas_hub"},
    {"duid": "GAS_OTWAY",       "station_name": "Otway Gas Hub",       "lat": -38.75, "lng": 143.52, "state": "VIC", "region_id": "VIC1", "fuel_type": "gas_hub", "capacity_mw": 0.0, "status": "active", "layer_type": "gas_hub"},
]

# REZ zones (approximate centroid coordinates)
REZ_ZONES = [
    {"duid": "REZ_NQ",     "station_name": "North Queensland REZ",      "lat": -19.5, "lng": 146.8, "state": "QLD", "region_id": "QLD1", "fuel_type": "rez", "capacity_mw": 3500.0, "status": "planned", "layer_type": "rez"},
    {"duid": "REZ_IQ",     "station_name": "Isaac-Connors REZ",         "lat": -21.5, "lng": 148.5, "state": "QLD", "region_id": "QLD1", "fuel_type": "rez", "capacity_mw": 2500.0, "status": "planned", "layer_type": "rez"},
    {"duid": "REZ_FNQ",    "station_name": "Far North Queensland REZ",  "lat": -16.9, "lng": 145.8, "state": "QLD", "region_id": "QLD1", "fuel_type": "rez", "capacity_mw": 1500.0, "status": "planned", "layer_type": "rez"},
    {"duid": "REZ_DARLING", "station_name": "Darling Downs REZ",        "lat": -27.5, "lng": 151.0, "state": "QLD", "region_id": "QLD1", "fuel_type": "rez", "capacity_mw": 2000.0, "status": "planned", "layer_type": "rez"},
    {"duid": "REZ_CWO",    "station_name": "Central-West Orana REZ",    "lat": -32.0, "lng": 148.5, "state": "NSW", "region_id": "NSW1", "fuel_type": "rez", "capacity_mw": 3000.0, "status": "under_construction", "layer_type": "rez"},
    {"duid": "REZ_NE",     "station_name": "New England REZ",           "lat": -30.0, "lng": 151.5, "state": "NSW", "region_id": "NSW1", "fuel_type": "rez", "capacity_mw": 8000.0, "status": "planned", "layer_type": "rez"},
    {"duid": "REZ_HUN",    "station_name": "Hunter-Central Coast REZ",  "lat": -32.7, "lng": 151.5, "state": "NSW", "region_id": "NSW1", "fuel_type": "rez", "capacity_mw": 1200.0, "status": "planned", "layer_type": "rez"},
    {"duid": "REZ_SW",     "station_name": "South-West NSW REZ",        "lat": -34.5, "lng": 146.5, "state": "NSW", "region_id": "NSW1", "fuel_type": "rez", "capacity_mw": 2500.0, "status": "planned", "layer_type": "rez"},
    {"duid": "REZ_MUR",    "station_name": "Murray River REZ",          "lat": -36.0, "lng": 145.0, "state": "VIC", "region_id": "VIC1", "fuel_type": "rez", "capacity_mw": 3500.0, "status": "planned", "layer_type": "rez"},
    {"duid": "REZ_WV",     "station_name": "Western Victoria REZ",      "lat": -37.0, "lng": 142.5, "state": "VIC", "region_id": "VIC1", "fuel_type": "rez", "capacity_mw": 3000.0, "status": "declared", "layer_type": "rez"},
    {"duid": "REZ_GIP",    "station_name": "Gippsland REZ",             "lat": -38.2, "lng": 146.0, "state": "VIC", "region_id": "VIC1", "fuel_type": "rez", "capacity_mw": 1500.0, "status": "planned", "layer_type": "rez"},
    {"duid": "REZ_MN",     "station_name": "Mid-North SA REZ",          "lat": -33.0, "lng": 138.5, "state": "SA",  "region_id": "SA1",  "fuel_type": "rez", "capacity_mw": 2000.0, "status": "active", "layer_type": "rez"},
    {"duid": "REZ_SE_SA",  "station_name": "South East SA REZ",         "lat": -36.5, "lng": 140.0, "state": "SA",  "region_id": "SA1",  "fuel_type": "rez", "capacity_mw": 1800.0, "status": "planned", "layer_type": "rez"},
    {"duid": "REZ_NW_TAS", "station_name": "North-West Tasmania REZ",   "lat": -41.2, "lng": 145.5, "state": "TAS", "region_id": "TAS1", "fuel_type": "rez", "capacity_mw": 1200.0, "status": "planned", "layer_type": "rez"},
]

# ISP Projects (key transmission/generation projects from AEMO ISP)
ISP_PROJECTS = [
    {"duid": "ISP_MARINUS",    "station_name": "Marinus Link",              "lat": -41.0, "lng": 145.5, "state": "TAS", "region_id": "TAS1", "fuel_type": "isp_transmission", "capacity_mw": 1500.0, "status": "committed", "layer_type": "isp"},
    {"duid": "ISP_HUMELINK",   "station_name": "HumeLink",                  "lat": -35.5, "lng": 148.0, "state": "NSW", "region_id": "NSW1", "fuel_type": "isp_transmission", "capacity_mw": 2200.0, "status": "under_construction", "layer_type": "isp"},
    {"duid": "ISP_SYDNEY_RING","station_name": "Sydney Ring Reinforcement", "lat": -33.8, "lng": 150.8, "state": "NSW", "region_id": "NSW1", "fuel_type": "isp_transmission", "capacity_mw": 0.0, "status": "planned", "layer_type": "isp"},
    {"duid": "ISP_VNI_WEST",  "station_name": "VNI West",                   "lat": -36.0, "lng": 144.0, "state": "VIC", "region_id": "VIC1", "fuel_type": "isp_transmission", "capacity_mw": 1800.0, "status": "committed", "layer_type": "isp"},
    {"duid": "ISP_QNSW",      "station_name": "QNI Medium",                 "lat": -29.0, "lng": 152.0, "state": "NSW", "region_id": "NSW1", "fuel_type": "isp_transmission", "capacity_mw": 1200.0, "status": "anticipated", "layer_type": "isp"},
    {"duid": "ISP_CWO_TX",    "station_name": "CWO REZ Transmission",       "lat": -32.2, "lng": 148.8, "state": "NSW", "region_id": "NSW1", "fuel_type": "isp_transmission", "capacity_mw": 0.0, "status": "under_construction", "layer_type": "isp"},
    {"duid": "ISP_NE_TX",     "station_name": "New England REZ Transmission","lat": -29.5, "lng": 151.5, "state": "NSW", "region_id": "NSW1", "fuel_type": "isp_transmission", "capacity_mw": 0.0, "status": "planned", "layer_type": "isp"},
    {"duid": "ISP_SNOWY2",    "station_name": "Snowy 2.0",                  "lat": -36.0, "lng": 148.4, "state": "NSW", "region_id": "NSW1", "fuel_type": "pumped_hydro", "capacity_mw": 2000.0, "status": "under_construction", "layer_type": "isp"},
    {"duid": "ISP_BORUMBA",   "station_name": "Borumba Pumped Hydro",       "lat": -26.5, "lng": 152.6, "state": "QLD", "region_id": "QLD1", "fuel_type": "pumped_hydro", "capacity_mw": 2000.0, "status": "committed", "layer_type": "isp"},
    {"duid": "ISP_PIONEER",   "station_name": "Pioneer-Burdekin Pumped Hydro","lat": -20.0, "lng": 146.5, "state": "QLD", "region_id": "QLD1", "fuel_type": "pumped_hydro", "capacity_mw": 5000.0, "status": "anticipated", "layer_type": "isp"},
]

# Combine all supplemental rows
supplemental = REGION_CENTROIDS + GAS_HUBS + REZ_ZONES + ISP_PROJECTS
for r in supplemental:
    if r["duid"] not in seen_duids:
        rows.append(r)
        seen_duids.add(r["duid"])

print(f"Total rows after supplemental: {len(rows)}")

# COMMAND ----------

# --- Write to Delta table ---
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("duid", StringType(), False),
    StructField("station_name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("state", StringType(), True),
    StructField("region_id", StringType(), True),
    StructField("fuel_type", StringType(), True),
    StructField("capacity_mw", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("layer_type", StringType(), True),
])

df = spark.createDataFrame(rows, schema=schema)

# MERGE on duid (upsert)
df.createOrReplaceTempView("new_locations")

spark.sql(f"""
MERGE INTO {catalog}.gold.facility_locations AS target
USING new_locations AS source
ON target.duid = source.duid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

final_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.facility_locations").collect()[0]["cnt"]
print(f"facility_locations table now has {final_count} rows")

# COMMAND ----------

# --- Grant SELECT to app SP ---
spark.sql(f"GRANT SELECT ON TABLE {catalog}.gold.facility_locations TO `{sp_id}`")
print("Granted SELECT to app service principal")
