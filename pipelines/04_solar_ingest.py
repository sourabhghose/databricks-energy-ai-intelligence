# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 04 — APVI Rooftop Solar Ingest (Bronze -> Silver -> Gold)
# MAGIC Batch pipeline, 30-min. API: pv-map.apvi.org.au/api

import os
from datetime import datetime, timedelta, timezone

import dlt
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

SECRET_SCOPE = "energy_copilot"
APVI_BASE_URL = "https://pv-map.apvi.org.au/api"
STATE_TO_NEM = {"NSW": "NSW1", "ACT": "NSW1", "QLD": "QLD1", "VIC": "VIC1", "SA": "SA1", "TAS": "TAS1"}


def _api_key() -> str:
    try: return dbutils.secrets.get(scope=SECRET_SCOPE, key="apvi_api_key")
    except Exception: return os.environ.get("APVI_API_KEY", "")


def _fetch_generation(since: datetime) -> list:
    resp = requests.get(
        f"{APVI_BASE_URL}/v1/generation",
        headers={"Authorization": f"Token {_api_key()}", "Accept": "application/json"},
        params={"since": since.strftime("%Y-%m-%dT%H:%M:%SZ"), "format": "json"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", data) if isinstance(data, dict) else data


@dlt.table(name="apvi_raw", comment="Bronze: raw APVI rooftop solar generation 30min incremental",
           table_properties={"quality": "bronze"}, partition_cols=["_ingest_date"])
def bronze_apvi_raw():
    since = datetime.now(timezone.utc) - timedelta(hours=2)
    records = _fetch_generation(since)
    if not records:
        return spark.createDataFrame([], schema=StructType([
            StructField("ts", StringType()), StructField("state", StringType()),
            StructField("ac_power_mw", DoubleType()), StructField("_ingested_at", TimestampType()),
        ]))
    return (spark.createDataFrame(records)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingest_date", F.to_date(F.current_timestamp())))


@dlt.table(name="apvi_solar_generation", comment="Silver: APVI rooftop solar typed NEM-region mapped deduped",
           table_properties={"quality": "silver"}, partition_cols=["interval_date"])
@dlt.expect_or_drop("valid_timestamp", "interval_datetime IS NOT NULL")
@dlt.expect("non_negative", "ac_power_mw >= 0 OR ac_power_mw IS NULL")
def silver_apvi_generation():
    state_map = F.create_map(*[i for p in STATE_TO_NEM.items() for i in (F.lit(p[0]), F.lit(p[1]))])
    return (
        dlt.read_stream("apvi_raw")
        .select(
            F.coalesce(
                F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            ).alias("interval_datetime"),
            F.upper(F.col("state")).alias("state"),
            F.col("ac_power_mw").cast(DoubleType()),
            "_ingested_at",
        )
        .filter(F.col("interval_datetime").isNotNull())
        .withColumn("region_id", state_map[F.col("state")])
        .dropDuplicates(["interval_datetime", "state"])
        .withColumn("interval_date", F.to_date("interval_datetime"))
    )


@dlt.table(name="nem_rooftop_solar", comment="Gold: rooftop solar by NEM region 30min",
           table_properties={"quality": "gold"}, partition_cols=["interval_date"])
def gold_nem_rooftop_solar():
    return (
        dlt.read("apvi_solar_generation")
        .filter(F.col("region_id").isNotNull())
        .groupBy("interval_datetime", "interval_date", "region_id")
        .agg(F.sum("ac_power_mw").alias("total_ac_mw"), F.count("state").alias("state_count"))
    )
