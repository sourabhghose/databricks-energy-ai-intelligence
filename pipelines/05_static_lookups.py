# Databricks notebook source
"""
Pipeline 05: Static Lookup Tables

Creates reference/dimension tables from public datasets:
1. NEM emissions factors by fuel type (CER/NGER data)
2. NEM region metadata (timezone, state, capacity)
3. Fuel type classification

These are static tables that rarely change — re-run quarterly.

Target schema: energy_copilot_catalog.gold
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "energy_copilot_catalog"
GOLD_SCHEMA = f"{CATALOG}.gold"


def create_emissions_factors(spark: SparkSession):
    """
    Create emissions factors lookup table.

    Data sourced from:
    - Clean Energy Regulator (CER) National Greenhouse and Energy Reporting (NGER)
    - National Greenhouse Accounts Factors (2024)
    """
    schema = StructType([
        StructField("fuel_type", StringType(), False),
        StructField("emissions_factor_tco2e_mwh", DoubleType(), False),
        StructField("scope", StringType(), False),
        StructField("source", StringType(), False),
    ])

    data = [
        ("Black Coal", 0.91, "Scope 1", "NGER 2024"),
        ("Brown Coal", 1.24, "Scope 1", "NGER 2024"),
        ("Natural Gas", 0.51, "Scope 1", "NGER 2024"),
        ("Gas (OCGT)", 0.65, "Scope 1", "NGER 2024"),
        ("Gas (CCGT)", 0.38, "Scope 1", "NGER 2024"),
        ("Distillate", 0.80, "Scope 1", "NGER 2024"),
        ("Solar", 0.0, "Scope 1", "NGER 2024"),
        ("Wind", 0.0, "Scope 1", "NGER 2024"),
        ("Hydro", 0.0, "Scope 1", "NGER 2024"),
        ("Battery", 0.0, "Scope 1", "NGER 2024"),
        ("Biomass", 0.02, "Scope 1", "NGER 2024"),
        ("Bagasse", 0.0, "Scope 1", "NGER 2024"),  # Biogenic CO2, not counted
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.emissions_factors")
    print(f"Created {GOLD_SCHEMA}.emissions_factors with {len(data)} rows")


def create_region_metadata(spark: SparkSession):
    """Create NEM region metadata lookup table."""
    schema = StructType([
        StructField("region_id", StringType(), False),
        StructField("state", StringType(), False),
        StructField("state_name", StringType(), False),
        StructField("timezone", StringType(), False),
        StructField("approx_latitude", DoubleType(), False),
        StructField("approx_longitude", DoubleType(), False),
        StructField("installed_capacity_mw", DoubleType(), False),
    ])

    data = [
        ("NSW1", "NSW", "New South Wales", "Australia/Sydney", -33.87, 151.21, 18500.0),
        ("QLD1", "QLD", "Queensland", "Australia/Brisbane", -27.47, 153.03, 16800.0),
        ("VIC1", "VIC", "Victoria", "Australia/Melbourne", -37.81, 144.96, 13200.0),
        ("SA1", "SA", "South Australia", "Australia/Adelaide", -34.93, 138.60, 7800.0),
        ("TAS1", "TAS", "Tasmania", "Australia/Hobart", -42.88, 147.33, 3200.0),
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.nem_region_metadata")
    print(f"Created {GOLD_SCHEMA}.nem_region_metadata with {len(data)} rows")


def create_fuel_type_classification(spark: SparkSession):
    """Create fuel type classification lookup."""
    schema = StructType([
        StructField("fuel_type", StringType(), False),
        StructField("fuel_category", StringType(), False),
        StructField("is_renewable", BooleanType(), False),
        StructField("is_dispatchable", BooleanType(), False),
        StructField("typical_capacity_factor", DoubleType(), False),
    ])

    data = [
        ("Black Coal", "Coal", False, True, 0.60),
        ("Brown Coal", "Coal", False, True, 0.75),
        ("Natural Gas", "Gas", False, True, 0.30),
        ("Gas (OCGT)", "Gas", False, True, 0.10),
        ("Gas (CCGT)", "Gas", False, True, 0.40),
        ("Distillate", "Liquid Fuel", False, True, 0.02),
        ("Solar", "Solar", True, False, 0.25),
        ("Wind", "Wind", True, False, 0.35),
        ("Hydro", "Hydro", True, True, 0.20),
        ("Battery", "Storage", True, True, 0.15),
        ("Pumps", "Storage", False, True, 0.10),
        ("Biomass", "Bioenergy", True, True, 0.50),
        ("Bagasse", "Bioenergy", True, True, 0.30),
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.fuel_type_classification")
    print(f"Created {GOLD_SCHEMA}.fuel_type_classification with {len(data)} rows")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    create_emissions_factors(spark)
    create_region_metadata(spark)
    create_fuel_type_classification(spark)
    print("All static lookup tables created successfully.")
