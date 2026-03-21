# Databricks notebook source
"""
Pipeline 04: AEMO Market Notices Ingestion

Ingests market notices from AEMO's public NEMWEB data feeds.
Market notices are published as plain-text files and include:
- System security alerts
- Market participant advisories
- Constraint equation changes
- Price/demand anomalies

Target: energy_copilot_catalog.nemweb_analytics.market_notices
Gold view: energy_copilot_catalog.gold.market_notices

Runs: Hourly (low frequency — notices published periodically)
"""

import dlt
from pyspark.sql.functions import (
    col,
    current_timestamp,
    regexp_extract,
    to_timestamp,
    trim,
    upper,
)
from pyspark.sql.types import StringType, StructField, StructType

# Schema for parsed market notices
NOTICE_SCHEMA = StructType([
    StructField("notice_id", StringType(), True),
    StructField("notice_type", StringType(), True),
    StructField("creation_date", StringType(), True),
    StructField("issue_date", StringType(), True),
    StructField("external_reference", StringType(), True),
    StructField("reason", StringType(), True),
])

try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "energy_copilot_catalog"
SCHEMA = "nemweb_analytics"


@dlt.table(
    name="bronze_market_notices",
    comment="Bronze layer: Raw AEMO market notices ingested from NEMWEB CSV files",
)
@dlt.expect_or_drop("notice_id_not_null", "notice_id IS NOT NULL")
def bronze_market_notices():
    """
    Ingest market notices from NEMWEB public data.

    AEMO publishes notices as CSV files in the MARKET_NOTICE table within
    the NEMPriceSetter and DispatchIS ZIP archives.
    """
    volume_path = spark.conf.get("pipeline.volume_path", f"/Volumes/{CATALOG}/{SCHEMA}/nemweb_raw")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{volume_path}/_schemas/market_notices")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("pathGlobFilter", "*MARKET_NOTICE*")
        .load(f"{volume_path}/")
        .withColumnRenamed("NOTICEID", "notice_id")
        .withColumnRenamed("NOTICETYPE", "notice_type")
        .withColumnRenamed("CREATIONDATE", "creation_date")
        .withColumnRenamed("ISSUEDATE", "issue_date")
        .withColumnRenamed("EXTERNALREFERENCE", "external_reference")
        .withColumnRenamed("REASON", "reason")
        .withColumn("_ingested_at", current_timestamp())
    )


@dlt.table(
    name="silver_market_notices",
    comment="Silver layer: Validated and parsed market notices with timestamps",
)
@dlt.expect_or_drop("notice_id_valid", "notice_id IS NOT NULL AND notice_id != ''")
def silver_market_notices():
    """Validate and parse market notices with proper timestamps."""
    return (
        dlt.read("bronze_market_notices")
        .withColumn("created_at", to_timestamp(col("creation_date"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("issued_at", to_timestamp(col("issue_date"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("notice_type_clean", upper(trim(col("notice_type"))))
        .withColumn("reason_clean", trim(col("reason")))
        .select(
            col("notice_id").cast("long"),
            "notice_type_clean",
            "created_at",
            "issued_at",
            "external_reference",
            "reason_clean",
            "_ingested_at",
        )
        .withColumnRenamed("notice_type_clean", "notice_type")
        .withColumnRenamed("reason_clean", "reason")
    )


@dlt.table(
    name="gold_market_notices",
    comment="Gold layer: Market notices with categorisation and region extraction",
)
def gold_market_notices():
    """
    Enrich market notices with category classification and region extraction.

    Categories based on notice_type and reason keywords:
    - PRICES: Price-related notices
    - SYSTEM_SECURITY: System security and reliability
    - MARKET_INTERVENTION: Directed dispatch, RERT, etc.
    - CONSTRAINT: Network constraint changes
    - OTHER: All other notices
    """
    return (
        dlt.read("silver_market_notices")
        .withColumn(
            "category",
            regexp_extract(
                col("reason"),
                r"(?i)(price|spike|negative|cap|floor|administere)",
                0,
            ),
        )
        .withColumn(
            "region",
            regexp_extract(col("reason"), r"(NSW1|QLD1|VIC1|SA1|TAS1)", 0),
        )
        .select(
            "notice_id",
            "notice_type",
            "created_at",
            "issued_at",
            "external_reference",
            "reason",
            "category",
            "region",
            "_ingested_at",
        )
    )
