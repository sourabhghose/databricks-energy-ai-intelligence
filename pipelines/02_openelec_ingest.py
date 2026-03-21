# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 02 — OpenElectricity API Ingest (Bronze -> Silver -> Gold)
# MAGIC Batch pipeline, hourly. API: https://api.openelectricity.org.au/v4

import os
from datetime import datetime, timedelta, timezone

import dlt
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

OPENELEC_BASE_URL = "https://api.openelectricity.org.au/v4"
SECRET_SCOPE = "energy_copilot"
LAST_FETCH_TABLE = "energy_copilot.bronze.openelec_fetch_state"


def _get_api_key() -> str:
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key="openelec_api_key")
    except Exception:
        return os.environ.get("OPENELEC_API_KEY", "")


def _last_fetch(endpoint: str) -> datetime:
    try:
        rows = (spark.table(LAST_FETCH_TABLE).filter(F.col("endpoint") == endpoint)
                .orderBy(F.col("fetched_at").desc()).limit(1).collect())
        if rows: return rows[0]["last_data_timestamp"].replace(tzinfo=timezone.utc)
    except Exception: pass
    return datetime.now(timezone.utc) - timedelta(days=7)


def _fetch(endpoint_path: str, since: datetime) -> list:
    resp = requests.get(
        f"{OPENELEC_BASE_URL}{endpoint_path}",
        headers={"Authorization": f"Bearer {_get_api_key()}", "Accept": "application/json"},
        params={"interval": "5m", "since": since.strftime("%Y-%m-%dT%H:%M:%SZ"), "limit": 2000},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", data) if isinstance(data, dict) else data


@dlt.table(name="openelec_generation_raw", comment="Bronze: raw OpenElectricity generation API",
           table_properties={"quality": "bronze"}, partition_cols=["_ingest_date"])
def bronze_openelec_generation():
    records = _fetch("/stats/au/generation", _last_fetch("generation"))
    if not records:
        return spark.createDataFrame([], schema=StructType([
            StructField("interval", StringType()), StructField("network_region", StringType()),
            StructField("fuel_tech", StringType()), StructField("generated_mw", DoubleType()),
            StructField("_ingested_at", TimestampType()), StructField("_ingest_date", StringType()),
        ]))
    return (spark.createDataFrame(records)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingest_date", F.to_date(F.current_timestamp())))


@dlt.table(name="openelec_prices_raw", comment="Bronze: raw OpenElectricity wholesale prices",
           table_properties={"quality": "bronze"}, partition_cols=["_ingest_date"])
def bronze_openelec_prices():
    records = _fetch("/stats/au/price", _last_fetch("prices"))
    if not records:
        return spark.createDataFrame([], schema=StructType([
            StructField("interval", StringType()), StructField("network_region", StringType()),
            StructField("price", DoubleType()), StructField("_ingested_at", TimestampType()),
            StructField("_ingest_date", StringType()),
        ]))
    return (spark.createDataFrame(records)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingest_date", F.to_date(F.current_timestamp())))


@dlt.table(name="openelec_generation", comment="Silver: OpenElectricity generation typed deduped",
           table_properties={"quality": "silver"}, partition_cols=["interval_date"])
@dlt.expect_or_drop("valid_interval", "interval_datetime IS NOT NULL")
def silver_openelec_generation():
    return (
        dlt.read_stream("openelec_generation_raw")
        .select(
            F.to_timestamp("interval", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("interval_datetime"),
            F.col("network_region").cast(StringType()),
            F.col("fuel_tech").cast(StringType()),
            F.col("generated_mw").cast(DoubleType()),
            "_ingested_at",
        )
        .filter(F.col("interval_datetime").isNotNull())
        .dropDuplicates(["interval_datetime", "network_region", "fuel_tech"])
        .withColumn("interval_date", F.to_date("interval_datetime"))
    )


@dlt.table(name="openelec_fuel_mix", comment="Gold: 30min generation fuel-type mix by NEM region",
           table_properties={"quality": "gold"}, partition_cols=["interval_date"])
def gold_openelec_fuel_mix():
    return (
        dlt.read("openelec_generation")
        .withColumn("interval_30min", F.date_trunc("30 minute", F.col("interval_datetime")))
        .groupBy("interval_30min", "network_region", "fuel_tech")
        .agg(F.avg("generated_mw").alias("avg_generated_mw"), F.sum("generated_mw").alias("total_generated_mw"))
        .withColumnRenamed("interval_30min", "interval_datetime")
        .withColumnRenamed("network_region", "region_id")
        .withColumn("interval_date", F.to_date("interval_datetime"))
    )
