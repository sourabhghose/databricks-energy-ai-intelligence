# Databricks notebook source
"""
Pipeline 06: AEMO Gas Bulletin Board (GBB) Data Ingestion

Ingests public gas market data from AEMO's Gas Bulletin Board.
Uses the public CSV download API (no participant login required).

Source: https://www.aemo.com.au/energy-systems/gas/gas-bulletin-board-gbb
CSV endpoint: AEMO public data portal (free)

Target: energy_copilot_catalog.gold.gas_pipeline_flows
Runs: Daily (gas data published daily)
"""

import dlt
from pyspark.sql.functions import avg, col, current_timestamp, to_date
from pyspark.sql.functions import sum as _sum

try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "energy_copilot_catalog"
SCHEMA = "nemweb_analytics"

# AEMO GBB public CSV URLs (no authentication required)
GBB_BASE_URL = "https://nemweb.com.au/Reports/Current/GBB/"


@dlt.table(
    name="bronze_gas_pipeline_flows",
    comment="Bronze layer: Gas pipeline flow data from AEMO Gas Bulletin Board",
)
@dlt.expect_or_drop("gas_date_not_null", "gas_date IS NOT NULL")
def bronze_gas_pipeline_flows():
    """
    Ingest gas pipeline flow data from AEMO GBB public CSVs.

    The GBB publishes daily pipeline flow data including:
    - Actual flows (TJ/day)
    - Nominations
    - Capacity
    """
    volume_path = spark.conf.get("pipeline.volume_path", f"/Volumes/{CATALOG}/{SCHEMA}/nemweb_raw")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{volume_path}/_schemas/gas_flows")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("pathGlobFilter", "*GAS*PIPELINE*")
        .load(f"{volume_path}/")
        .withColumnRenamed("GAS_DATE", "gas_date")
        .withColumnRenamed("PIPELINE_NAME", "pipeline_name")
        .withColumnRenamed("ZONE", "zone")
        .withColumnRenamed("ACTUAL_QUANTITY_GJ", "actual_flow_gj")
        .withColumnRenamed("NOMINATED_QUANTITY_GJ", "nominated_flow_gj")
        .withColumnRenamed("CAPACITY_GJ", "capacity_gj")
        .withColumn("_ingested_at", current_timestamp())
    )


@dlt.table(
    name="gold_gas_pipeline_summary",
    comment="Gold layer: Daily gas pipeline flow summary",
)
def gold_gas_pipeline_summary():
    """Aggregate gas pipeline flows to daily summary by pipeline."""
    return (
        dlt.read("bronze_gas_pipeline_flows")
        .withColumn("flow_date", to_date(col("gas_date")))
        .groupBy("flow_date", "pipeline_name")
        .agg(
            _sum("actual_flow_gj").alias("total_actual_gj"),
            _sum("nominated_flow_gj").alias("total_nominated_gj"),
            _sum("capacity_gj").alias("total_capacity_gj"),
            avg("actual_flow_gj").alias("avg_actual_gj"),
        )
        .withColumn(
            "utilization_pct",
            col("total_actual_gj") / col("total_capacity_gj") * 100,
        )
        .orderBy("flow_date", "pipeline_name")
    )
