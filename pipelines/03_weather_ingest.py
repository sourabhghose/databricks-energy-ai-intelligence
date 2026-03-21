# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 03 — Weather Ingest (Open-Meteo -> Bronze -> Silver -> Gold)
# MAGIC Batch pipeline, hourly. API: api.open-meteo.com (BOM ACCESS-G, no key required)
# MAGIC Region coords: NSW1(-33.87,151.21) QLD1(-27.47,153.03) VIC1(-37.81,144.96) SA1(-34.93,138.60) TAS1(-42.88,147.33)

from datetime import datetime, timezone

import dlt
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

NEM_REGION_COORDS = {
    "NSW1": {"lat": -33.87, "lon": 151.21},
    "QLD1": {"lat": -27.47, "lon": 153.03},
    "VIC1": {"lat": -37.81, "lon": 144.96},
    "SA1":  {"lat": -34.93, "lon": 138.60},
    "TAS1": {"lat": -42.88, "lon": 147.33},
}

HOURLY_VARS = [
    "temperature_2m", "apparent_temperature", "relative_humidity_2m", "precipitation",
    "wind_speed_10m", "wind_speed_100m", "wind_direction_100m",
    "shortwave_radiation", "direct_normal_irradiance", "cloud_cover",
]


def _fetch_region(region_id: str, coords: dict) -> list:
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": coords["lat"], "longitude": coords["lon"],
            "hourly": ",".join(HOURLY_VARS),
            "timezone": "Australia/Sydney", "past_days": 2, "forecast_days": 7,
            "models": "bom_access_global",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    times = data.get("hourly", {}).get("time", [])
    rows = []
    for i, ts_str in enumerate(times):
        row = {"region_id": region_id, "valid_time": ts_str, "fetched_at": datetime.now(timezone.utc).isoformat()}
        for var in HOURLY_VARS:
            vals = data.get("hourly", {}).get(var, [])
            row[var] = vals[i] if i < len(vals) else None
        rows.append(row)
    return rows


@dlt.table(name="weather_raw", comment="Bronze: raw Open-Meteo hourly weather 5 NEM regions",
           table_properties={"quality": "bronze"}, partition_cols=["_ingest_date"])
def bronze_weather_raw():
    all_rows = []
    for region_id, coords in NEM_REGION_COORDS.items():
        try: all_rows.extend(_fetch_region(region_id, coords))
        except Exception as exc: print(f"Weather fetch failed for {region_id}: {exc}")
    if not all_rows:
        return spark.createDataFrame([], schema=StructType(
            [StructField("region_id", StringType()), StructField("valid_time", StringType())] +
            [StructField(v, DoubleType()) for v in HOURLY_VARS] +
            [StructField("_ingested_at", TimestampType()), StructField("_ingest_date", StringType())]
        ))
    return (spark.createDataFrame(all_rows)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingest_date", F.to_date(F.current_timestamp())))


@dlt.table(name="weather_nem_raw", comment="Silver: typed hourly weather per NEM region",
           table_properties={"quality": "silver"}, partition_cols=["interval_date"])
@dlt.expect_or_drop("valid_region", "region_id IN ('NSW1','QLD1','VIC1','SA1','TAS1')")
@dlt.expect_or_drop("valid_timestamp", "valid_time_ts IS NOT NULL")
@dlt.expect("temperature_in_range", "temperature_2m BETWEEN -10 AND 55")
def silver_weather():
    return (
        dlt.read_stream("weather_raw")
        .select(
            F.col("region_id"),
            F.to_timestamp("valid_time", "yyyy-MM-dd'T'HH:mm").alias("valid_time_ts"),
            F.col("temperature_2m").cast(DoubleType()),
            F.col("apparent_temperature").cast(DoubleType()),
            F.col("wind_speed_10m").cast(DoubleType()),
            F.col("wind_speed_100m").cast(DoubleType()),
            F.col("wind_direction_100m").cast(DoubleType()),
            F.col("shortwave_radiation").cast(DoubleType()),
            F.col("direct_normal_irradiance").cast(DoubleType()),
            F.col("cloud_cover").cast(DoubleType()),
            "_ingested_at",
        )
        .filter(F.col("valid_time_ts").isNotNull())
        .dropDuplicates(["region_id", "valid_time_ts"])
        .withColumn("interval_date", F.to_date("valid_time_ts"))
    )


@dlt.table(name="weather_nem_regions", comment="Gold: hourly weather per NEM region with energy demand derived features",
           table_properties={"quality": "gold"}, partition_cols=["interval_date"])
def gold_weather_nem_regions():
    return (
        dlt.read("weather_nem_raw")
        .select(
            F.col("region_id"),
            F.col("valid_time_ts").alias("interval_datetime"),
            F.col("interval_date"),
            F.col("temperature_2m").alias("temperature_c"),
            F.col("apparent_temperature").alias("feels_like_c"),
            F.col("wind_speed_10m").alias("wind_speed_10m_ms"),
            F.col("wind_speed_100m").alias("wind_speed_100m_ms"),
            F.col("shortwave_radiation").alias("solar_radiation_wm2"),
            F.col("direct_normal_irradiance").alias("dni_wm2"),
            F.col("cloud_cover").alias("cloud_cover_pct"),
            F.greatest(F.col("temperature_2m") - 18.0, F.lit(0.0)).alias("cooling_degree_hours"),
            F.greatest(18.0 - F.col("temperature_2m"), F.lit(0.0)).alias("heating_degree_hours"),
            (F.pow(F.col("wind_speed_100m"), 3) * F.lit(0.5) * F.lit(1.225)).alias("wind_power_density_wm2"),
        )
    )
