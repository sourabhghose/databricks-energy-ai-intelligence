# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 01 — NEMWEB Ingest (Bronze -> Silver -> Gold)
# MAGIC DLT/SDP streaming pipeline. Autoloader reads CSVs from Volume.

from functools import reduce

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType

VOLUME_BASE = "/Volumes/energy_copilot/bronze/nemweb_raw"
NEM_REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]


def _autoloader(report_type: str):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"/Volumes/energy_copilot/bronze/_schema_ckpt/{report_type}")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(f"{VOLUME_BASE}/{report_type}/")
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_report_type", F.lit(report_type))
        .withColumn("_ingested_at", F.current_timestamp())
        .filter(F.col("_c0") == "D")
        .withColumn("_ingest_date", F.to_date("_ingested_at"))
    )


# === BRONZE ===

@dlt.table(
    name="nemweb_dispatch_gen",
    comment="Bronze: Dispatch_SCADA generator MW 5min. Raw landing zone for generator SCADA output; "
            "preserves all source columns for reprocessing. Freshness target: < 10 minutes behind NEMWEB publish.",
    table_properties={"quality": "bronze"},
    partition_cols=["_ingest_date"],
)
@dlt.expect("valid_source_row", "_c0 = 'D'")
@dlt.expect("source_file_present", "_source_file IS NOT NULL")
@dlt.expect("ingested_at_not_null", "_ingested_at IS NOT NULL")
def bronze_dispatch_gen():
    return _autoloader("Dispatch_SCADA")


@dlt.table(
    name="nemweb_dispatch_is",
    comment="Bronze: DispatchIS regional prices and FCAS prices 5min. Raw landing for DispatchIS_Reports; "
            "contains both spot and FCAS RRP columns. Freshness target: < 10 minutes behind NEMWEB publish.",
    table_properties={"quality": "bronze"},
    partition_cols=["_ingest_date"],
)
@dlt.expect("valid_source_row", "_c0 = 'D'")
@dlt.expect("source_file_present", "_source_file IS NOT NULL")
@dlt.expect("ingested_at_not_null", "_ingested_at IS NOT NULL")
def bronze_dispatch_is():
    return _autoloader("DispatchIS_Reports")


@dlt.table(
    name="nemweb_dispatch_price",
    comment="Bronze: DISPATCHPRICE 5min spot prices from Dispatch_Reports_Price. Primary raw source for "
            "NEM regional spot prices (RRP) and total demand. Freshness target: < 10 minutes behind NEMWEB publish.",
    table_properties={"quality": "bronze"},
    partition_cols=["_ingest_date"],
)
@dlt.expect("valid_source_row", "_c0 = 'D'")
@dlt.expect("source_file_present", "_source_file IS NOT NULL")
@dlt.expect("ingested_at_not_null", "_ingested_at IS NOT NULL")
@dlt.expect("valid_region_bronze", "REGIONID IN ('NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1')")
@dlt.expect("price_in_bounds", "RRP BETWEEN -1000 AND 17000")
@dlt.expect_or_drop("settlement_date_not_null", "SETTLEMENTDATE IS NOT NULL")
def bronze_dispatch_price():
    return _autoloader("Dispatch_Reports_Price")


@dlt.table(
    name="nemweb_dispatch_inter",
    comment="Bronze: Interconnector flows and limits from Dispatch_Reports_Interconnector. Contains metered MW "
            "flow, scheduled flow, losses, and export/import limits per interconnector. "
            "Freshness target: < 10 minutes behind NEMWEB publish.",
    table_properties={"quality": "bronze"},
    partition_cols=["_ingest_date"],
)
@dlt.expect("valid_source_row", "_c0 = 'D'")
@dlt.expect("source_file_present", "_source_file IS NOT NULL")
@dlt.expect("ingested_at_not_null", "_ingested_at IS NOT NULL")
@dlt.expect_or_drop("settlement_date_not_null", "SETTLEMENTDATE IS NOT NULL")
@dlt.expect("interconnector_id_present", "INTERCONNECTORID IS NOT NULL")
def bronze_dispatch_inter():
    return _autoloader("Dispatch_Reports_Interconnector")


@dlt.table(
    name="nemweb_trading_is",
    comment="Bronze: TradingIS 30min trading prices. Contains 30-minute trading interval prices used for "
            "settlement calculations. Freshness target: < 35 minutes behind NEMWEB publish.",
    table_properties={"quality": "bronze"},
    partition_cols=["_ingest_date"],
)
@dlt.expect("valid_source_row", "_c0 = 'D'")
@dlt.expect("source_file_present", "_source_file IS NOT NULL")
@dlt.expect("ingested_at_not_null", "_ingested_at IS NOT NULL")
@dlt.expect("valid_region_bronze", "REGIONID IN ('NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1')")
@dlt.expect_or_drop("settlement_date_not_null", "SETTLEMENTDATE IS NOT NULL")
def bronze_trading_is():
    return _autoloader("TradingIS_Reports")


@dlt.table(
    name="nemweb_predispatch",
    comment="Bronze: Pre-dispatch price forecasts 30min from Predispatch_Reports. AEMO's short-term price "
            "and demand outlook used as the baseline forecast. Freshness target: < 35 minutes behind NEMWEB publish.",
    table_properties={"quality": "bronze"},
    partition_cols=["_ingest_date"],
)
@dlt.expect("valid_source_row", "_c0 = 'D'")
@dlt.expect("source_file_present", "_source_file IS NOT NULL")
@dlt.expect("ingested_at_not_null", "_ingested_at IS NOT NULL")
@dlt.expect("valid_region_bronze", "REGIONID IN ('NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1')")
@dlt.expect_or_drop("datetime_not_null", "DATETIME IS NOT NULL")
def bronze_predispatch():
    return _autoloader("Predispatch_Reports")


@dlt.table(
    name="nemweb_bids",
    comment="Bronze: Generator bid stacks from Next_Day_Dispatch (daily). Contains DUID-level price/volume "
            "bid bands used to reconstruct the merit order. Freshness target: available by 08:00 AEST next day.",
    table_properties={"quality": "bronze"},
    partition_cols=["_ingest_date"],
)
@dlt.expect("valid_source_row", "_c0 = 'D'")
@dlt.expect("source_file_present", "_source_file IS NOT NULL")
@dlt.expect("ingested_at_not_null", "_ingested_at IS NOT NULL")
@dlt.expect("duid_present", "DUID IS NOT NULL")
@dlt.expect_or_drop("settlement_date_not_null", "SETTLEMENTDATE IS NOT NULL")
def bronze_bids():
    return _autoloader("Next_Day_Dispatch")


# === SILVER ===

@dlt.table(
    name="dispatch_gen",
    comment="Silver: Generator MW output per DUID, 5min, deduped. Typed, deduplicated SCADA readings ready "
            "for fuel-mix join. Duplicate (SETTLEMENTDATE, DUID) pairs are dropped. "
            "Freshness target: < 15 minutes behind real-time.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    partition_cols=["interval_date"],
)
@dlt.expect_or_drop("valid_settlement_date", "SETTLEMENTDATE IS NOT NULL")
@dlt.expect_or_drop("valid_duid", "DUID IS NOT NULL AND DUID != ''")
@dlt.expect("scada_mw_in_range", "SCADAVALUE BETWEEN -9999 AND 99999")
@dlt.expect("no_future_intervals", "SETTLEMENTDATE <= current_timestamp() + INTERVAL 10 MINUTES")
def silver_dispatch_gen():
    return (
        dlt.read_stream("nemweb_dispatch_gen")
        .select(
            F.col("SETTLEMENTDATE").cast(TimestampType()).alias("SETTLEMENTDATE"),
            F.col("DUID").cast(StringType()),
            F.col("SCADAVALUE").cast(DoubleType()),
            "_source_file", "_ingested_at",
        )
        .filter(F.col("SETTLEMENTDATE").isNotNull())
        .dropDuplicates(["SETTLEMENTDATE", "DUID"])
        .withColumn("interval_date", F.to_date("SETTLEMENTDATE"))
        .withColumn("interval_datetime", F.col("SETTLEMENTDATE"))
    )


@dlt.table(
    name="dispatch_price",
    comment="Silver: 5min regional spot and FCAS prices, deduped. Typed dispatch prices for all 5 NEM regions "
            "including all 8 FCAS service prices. Duplicate (SETTLEMENTDATE, REGIONID) pairs are dropped. "
            "Freshness target: < 15 minutes behind real-time.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    partition_cols=["interval_date"],
)
@dlt.expect_or_drop("valid_settlement_date", "SETTLEMENTDATE IS NOT NULL")
@dlt.expect_or_drop("valid_region", "REGIONID IN ('NSW1','QLD1','VIC1','SA1','TAS1')")
@dlt.expect_or_drop("non_null_price", "RRP IS NOT NULL")
@dlt.expect("price_within_market_limits", "RRP BETWEEN -1000 AND 17000")
@dlt.expect("positive_demand", "TOTALDEMAND > 0")
@dlt.expect("no_future_intervals", "SETTLEMENTDATE <= current_timestamp() + INTERVAL 10 MINUTES")
def silver_dispatch_price():
    return (
        dlt.read_stream("nemweb_dispatch_price")
        .select(
            F.col("SETTLEMENTDATE").cast(TimestampType()).alias("SETTLEMENTDATE"),
            F.col("REGIONID").cast(StringType()),
            F.col("RRP").cast(DoubleType()),
            F.col("RAISE6SECRRP").cast(DoubleType()),
            F.col("RAISE60SECRRP").cast(DoubleType()),
            F.col("RAISE5MINRRP").cast(DoubleType()),
            F.col("RAISEREGRRP").cast(DoubleType()),
            F.col("LOWER6SECRRP").cast(DoubleType()),
            F.col("LOWER60SECRRP").cast(DoubleType()),
            F.col("LOWER5MINRRP").cast(DoubleType()),
            F.col("LOWERREGRRP").cast(DoubleType()),
            F.col("TOTALDEMAND").cast(DoubleType()),
            F.col("DISPATCHABLELOAD").cast(DoubleType()),
            "_source_file", "_ingested_at",
        )
        .filter(F.col("SETTLEMENTDATE").isNotNull())
        .dropDuplicates(["SETTLEMENTDATE", "REGIONID"])
        .withColumn("interval_date", F.to_date("SETTLEMENTDATE"))
        .withColumn("interval_datetime", F.col("SETTLEMENTDATE"))
    )


@dlt.table(
    name="dispatch_interconnector",
    comment="Silver: Interconnector flows, limits, and utilisation ratio 5min. Deduplicated per "
            "(SETTLEMENTDATE, INTERCONNECTORID). Utilisation ratio computed as abs(MWFLOW) / limit. "
            "Freshness target: < 15 minutes behind real-time.",
    table_properties={"quality": "silver"},
    partition_cols=["interval_date"],
)
@dlt.expect_or_drop("valid_settlement_date", "SETTLEMENTDATE IS NOT NULL")
@dlt.expect_or_drop("valid_interconnector", "INTERCONNECTORID IS NOT NULL")
@dlt.expect("no_future_intervals", "SETTLEMENTDATE <= current_timestamp() + INTERVAL 10 MINUTES")
@dlt.expect("utilisation_ratio_bounded", "utilisation_ratio IS NULL OR utilisation_ratio BETWEEN 0 AND 5")
def silver_dispatch_interconnector():
    return (
        dlt.read_stream("nemweb_dispatch_inter")
        .select(
            F.col("SETTLEMENTDATE").cast(TimestampType()).alias("SETTLEMENTDATE"),
            F.col("INTERCONNECTORID").cast(StringType()),
            F.col("METEREDMWFLOW").cast(DoubleType()),
            F.col("MWFLOW").cast(DoubleType()),
            F.col("MWLOSSES").cast(DoubleType()),
            F.col("EXPORTLIMIT").cast(DoubleType()),
            F.col("IMPORTLIMIT").cast(DoubleType()),
            "_source_file", "_ingested_at",
        )
        .filter(F.col("SETTLEMENTDATE").isNotNull())
        .dropDuplicates(["SETTLEMENTDATE", "INTERCONNECTORID"])
        .withColumn("interval_date", F.to_date("SETTLEMENTDATE"))
        .withColumn("interval_datetime", F.col("SETTLEMENTDATE"))
        .withColumn(
            "utilisation_ratio",
            F.when(F.col("EXPORTLIMIT") > 0, F.abs(F.col("MWFLOW")) / F.col("EXPORTLIMIT"))
             .when(F.col("IMPORTLIMIT") > 0, F.abs(F.col("MWFLOW")) / F.col("IMPORTLIMIT"))
             .otherwise(F.lit(None).cast(DoubleType())),
        )
    )


# === GOLD ===

@dlt.table(
    name="nem_prices_5min",
    comment="Gold: 5min NEM spot prices ($/MWh), primary time-series. One row per (interval_datetime, region_id). "
            "Canonical source for downstream ML features, dashboards, and anomaly detection. "
            "Freshness target: < 15 minutes behind real-time.",
    table_properties={"quality": "gold"},
    partition_cols=["interval_date"],
)
@dlt.expect_all_or_drop({
    "valid_interval": "interval_datetime IS NOT NULL",
    "valid_region": "region_id IN ('NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1')",
    "valid_price": "spot_price_aud_mwh BETWEEN -1000 AND 17000",
    "valid_demand": "total_demand_mw BETWEEN 0 AND 50000",
})
def gold_nem_prices_5min():
    return dlt.read("dispatch_price").select(
        F.col("interval_datetime"), F.col("interval_date"),
        F.col("REGIONID").alias("region_id"),
        F.col("RRP").alias("spot_price_aud_mwh"),
        F.col("TOTALDEMAND").alias("total_demand_mw"),
        F.col("DISPATCHABLELOAD").alias("dispatchable_load_mw"),
    )


@dlt.table(
    name="nem_prices_30min",
    comment="Gold: 30min average NEM spot prices aggregated from 5min dispatch intervals. "
            "One row per (trading_interval, region_id). Used for settlement price reconciliation and "
            "coarser-grained trend analysis. Freshness target: < 35 minutes behind real-time.",
    table_properties={"quality": "gold"},
    partition_cols=["interval_date"],
)
@dlt.expect_all_or_drop({
    "valid_interval": "interval_datetime IS NOT NULL",
    "valid_region": "region_id IN ('NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1')",
    "valid_avg_price": "avg_spot_price_aud_mwh BETWEEN -1000 AND 17000",
    "valid_interval_count": "interval_count BETWEEN 1 AND 6",
})
def gold_nem_prices_30min():
    return (
        dlt.read("dispatch_price")
        .withColumn("trading_interval", F.date_trunc("30 minute", F.col("SETTLEMENTDATE")))
        .groupBy("trading_interval", "REGIONID")
        .agg(
            F.avg("RRP").alias("avg_spot_price_aud_mwh"),
            F.max("RRP").alias("max_spot_price_aud_mwh"),
            F.min("RRP").alias("min_spot_price_aud_mwh"),
            F.avg("TOTALDEMAND").alias("avg_demand_mw"),
            F.count("*").alias("interval_count"),
        )
        .withColumnRenamed("REGIONID", "region_id")
        .withColumnRenamed("trading_interval", "interval_datetime")
        .withColumn("interval_date", F.to_date("interval_datetime"))
    )


@dlt.table(
    name="nem_generation_by_fuel",
    comment="Gold: Generation MW by fuel type and region 5min. Aggregates SCADA DUID readings against the "
            "generator registry to produce fuel-type totals. Falls back to DUID-level output when registry "
            "is unavailable. Freshness target: < 15 minutes behind real-time.",
    table_properties={"quality": "gold"},
    partition_cols=["interval_date"],
)
@dlt.expect_or_drop("valid_interval", "interval_datetime IS NOT NULL")
@dlt.expect("valid_total_mw", "total_mw BETWEEN -9999 AND 99999")
@dlt.expect("unit_count_positive", "unit_count > 0")
def gold_generation_by_fuel():
    gen = dlt.read("dispatch_gen").select("interval_datetime", "interval_date", "DUID", "SCADAVALUE")
    try:
        registry = spark.table("energy_copilot.silver.generator_registry").select("DUID", "fuel_type", "region_id", "max_capacity_mw")
        return (
            gen.join(registry, on="DUID", how="left")
            .groupBy("interval_datetime", "interval_date", "region_id", "fuel_type")
            .agg(F.sum("SCADAVALUE").alias("total_mw"), F.count("DUID").alias("unit_count"))
        )
    except Exception:
        return (
            gen.groupBy("interval_datetime", "interval_date", "DUID")
            .agg(F.sum("SCADAVALUE").alias("total_mw"))
            .withColumn("region_id", F.lit(None).cast(StringType()))
            .withColumn("fuel_type", F.lit("UNKNOWN"))
            .withColumn("unit_count", F.lit(1))
        )


@dlt.table(
    name="nem_interconnectors",
    comment="Gold: Interconnector flows, limits, and utilisation 5min. One row per (interval_datetime, "
            "interconnector_id). Includes metered and scheduled flow, losses, export/import limits, "
            "and utilisation ratio. Freshness target: < 15 minutes behind real-time.",
    table_properties={"quality": "gold"},
    partition_cols=["interval_date"],
)
@dlt.expect_all_or_drop({
    "valid_interval": "interval_datetime IS NOT NULL",
    "valid_interconnector": "interconnector_id IS NOT NULL",
    "metered_flow_bounded": "metered_flow_mw BETWEEN -10000 AND 10000",
})
@dlt.expect("utilisation_ratio_bounded", "utilisation_ratio IS NULL OR utilisation_ratio BETWEEN 0 AND 5")
def gold_nem_interconnectors():
    return dlt.read("dispatch_interconnector").select(
        "interval_datetime", "interval_date",
        F.col("INTERCONNECTORID").alias("interconnector_id"),
        F.col("METEREDMWFLOW").alias("metered_flow_mw"),
        F.col("MWFLOW").alias("scheduled_flow_mw"),
        F.col("MWLOSSES").alias("losses_mw"),
        F.col("EXPORTLIMIT").alias("export_limit_mw"),
        F.col("IMPORTLIMIT").alias("import_limit_mw"),
        "utilisation_ratio",
    )


@dlt.table(
    name="nem_fcas_prices",
    comment="Gold: 8 FCAS service prices per region per 5min interval in long (unpivoted) format. "
            "One row per (interval_datetime, region_id, fcas_service). Used for FCAS cost analysis, "
            "contingency event detection, and feature engineering. Freshness target: < 15 minutes behind real-time.",
    table_properties={"quality": "gold"},
    partition_cols=["interval_date"],
)
@dlt.expect_all_or_drop({
    "valid_interval": "interval_datetime IS NOT NULL",
    "valid_region": "region_id IN ('NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1')",
    "valid_fcas_service": "fcas_service IN ('RAISE6SEC','RAISE60SEC','RAISE5MIN','RAISEREG',"
                          "'LOWER6SEC','LOWER60SEC','LOWER5MIN','LOWERREG')",
})
@dlt.expect("non_negative_fcas_price", "price_aud_mwh >= 0")
def gold_nem_fcas_prices():
    fcas_map = {
        "RAISE6SECRRP": "RAISE6SEC", "RAISE60SECRRP": "RAISE60SEC",
        "RAISE5MINRRP": "RAISE5MIN", "RAISEREGRRP": "RAISEREG",
        "LOWER6SECRRP": "LOWER6SEC", "LOWER60SECRRP": "LOWER60SEC",
        "LOWER5MINRRP": "LOWER5MIN", "LOWERREGRRP": "LOWERREG",
    }
    src = dlt.read("dispatch_price").select("SETTLEMENTDATE", "REGIONID", *fcas_map.keys())
    frames = [
        src.select(
            F.col("SETTLEMENTDATE").alias("interval_datetime"),
            F.col("REGIONID").alias("region_id"),
            F.lit(service).alias("fcas_service"),
            F.col(col).alias("price_aud_mwh"),
        )
        for col, service in fcas_map.items()
    ]
    return reduce(lambda a, b: a.union(b), frames).withColumn("interval_date", F.to_date("interval_datetime"))
