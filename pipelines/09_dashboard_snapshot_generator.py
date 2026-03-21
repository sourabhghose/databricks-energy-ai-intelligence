# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard Snapshot Generator
# MAGIC Pre-computes JSON payloads for all dashboard endpoints and writes them to
# MAGIC `energy_copilot_catalog.gold.dashboard_snapshots`. This enables sub-10ms reads
# MAGIC when served via Lakebase (Synced Tables) or fast Delta reads with caching.
# MAGIC
# MAGIC **Schedule:** Every 5 minutes via Databricks Job

# COMMAND ----------

import json
from datetime import datetime, timezone

try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "energy_copilot_catalog"
GOLD = f"{CATALOG}.gold"
SNAPSHOTS = f"{GOLD}.dashboard_snapshots"

now = datetime.now(timezone.utc).isoformat()

def _snap(endpoint_path: str, region: str, payload: dict):
    """Create a snapshot row."""
    return (endpoint_path, region, now, json.dumps(payload, default=str))

snapshots = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Home Dashboard Snapshot

# COMMAND ----------

# Price summary per region
try:
    prices_df = spark.sql(f"""
        SELECT region_id, AVG(rrp) AS avg_price, MAX(rrp) AS max_price,
               MIN(rrp) AS min_price, STDDEV(rrp) AS volatility,
               SUM(total_demand_mw) / COUNT(*) AS avg_demand
        FROM {GOLD}.nem_prices_5min
        WHERE interval_datetime >= current_timestamp() - INTERVAL 30 DAYS
        GROUP BY region_id
    """).collect()

    regions = []
    for r in prices_df:
        regions.append({
            "region_id": r["region_id"],
            "avg_price": round(float(r["avg_price"] or 0), 2),
            "max_price": round(float(r["max_price"] or 0), 2),
            "min_price": round(float(r["min_price"] or 0), 2),
            "volatility": round(float(r["volatility"] or 0), 2),
            "avg_demand": round(float(r["avg_demand"] or 0), 0),
        })

    snapshots.append(_snap("/api/home/price-summary", "ALL", {
        "regions": regions,
        "timestamp": now,
        "data_source": "nem_prices_5min",
    }))

    # Per-region snapshots
    for reg in regions:
        snapshots.append(_snap("/api/home/price-summary", reg["region_id"], {
            "region": reg,
            "timestamp": now,
            "data_source": "nem_prices_5min",
        }))
except Exception as e:
    print(f"Price summary failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generation Mix Snapshot

# COMMAND ----------

try:
    gen_df = spark.sql(f"""
        SELECT region_id, fuel_type, is_renewable,
               AVG(total_mw) AS avg_mw, SUM(total_mw) AS total_mw,
               AVG(capacity_factor) AS avg_cf,
               AVG(emissions_intensity) AS avg_ei
        FROM {GOLD}.nem_generation_by_fuel
        WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
        GROUP BY region_id, fuel_type, is_renewable
        ORDER BY region_id, avg_mw DESC
    """).collect()

    gen_by_region = {}
    for r in gen_df:
        rid = r["region_id"]
        if rid not in gen_by_region:
            gen_by_region[rid] = []
        gen_by_region[rid].append({
            "fuel_type": r["fuel_type"],
            "is_renewable": bool(r["is_renewable"]),
            "avg_mw": round(float(r["avg_mw"] or 0), 1),
            "capacity_factor": round(float(r["avg_cf"] or 0), 3),
            "emissions_intensity": round(float(r["avg_ei"] or 0), 4),
        })

    snapshots.append(_snap("/api/home/generation-mix", "ALL", {
        "regions": gen_by_region,
        "timestamp": now,
        "data_source": "nem_generation_by_fuel",
    }))
except Exception as e:
    print(f"Generation mix failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interconnector Snapshot

# COMMAND ----------

try:
    ic_df = spark.sql(f"""
        SELECT interconnector_id, from_region, to_region,
               AVG(mw_flow) AS avg_flow, MAX(mw_flow) AS max_flow,
               AVG(utilization_pct) AS avg_util,
               SUM(CASE WHEN is_congested THEN 1 ELSE 0 END) / COUNT(*) AS congestion_pct
        FROM {GOLD}.nem_interconnectors
        WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
        GROUP BY interconnector_id, from_region, to_region
    """).collect()

    interconnectors = []
    for r in ic_df:
        interconnectors.append({
            "id": r["interconnector_id"],
            "from_region": r["from_region"],
            "to_region": r["to_region"],
            "avg_flow_mw": round(float(r["avg_flow"] or 0), 1),
            "max_flow_mw": round(float(r["max_flow"] or 0), 1),
            "avg_utilization_pct": round(float(r["avg_util"] or 0), 1),
            "congestion_pct": round(float(r["congestion_pct"] or 0), 3),
        })

    snapshots.append(_snap("/api/home/interconnectors", "ALL", {
        "interconnectors": interconnectors,
        "timestamp": now,
        "data_source": "nem_interconnectors",
    }))
except Exception as e:
    print(f"Interconnectors failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Futures & Gas & Carbon Snapshots

# COMMAND ----------

# ASX Futures
try:
    futures_df = spark.sql(f"""
        SELECT contract_code, region, contract_type, quarter, year,
               settlement_price, change_1d, volume, open_interest, trade_date
        FROM {GOLD}.asx_futures_eod
        WHERE trade_date = (SELECT MAX(trade_date) FROM {GOLD}.asx_futures_eod)
        ORDER BY region, year, quarter
    """).collect()

    contracts = []
    for r in futures_df:
        contracts.append({
            "contract_code": r["contract_code"],
            "region": r["region"],
            "type": r["contract_type"],
            "quarter": r["quarter"],
            "year": int(r["year"]),
            "settlement_price": round(float(r["settlement_price"] or 0), 2),
            "change": round(float(r["change_1d"] or 0), 2),
            "volume": int(r["volume"] or 0),
            "open_interest": int(r["open_interest"] or 0),
        })

    for region in ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]:
        region_contracts = [c for c in contracts if c["region"] == region]
        if region_contracts:
            snapshots.append(_snap("/api/futures/dashboard", region, {
                "contracts": region_contracts,
                "timestamp": now,
                "data_source": "asx_futures_eod",
            }))
except Exception as e:
    print(f"Futures failed: {e}")

# Gas hub prices
try:
    gas_df = spark.sql(f"""
        WITH latest AS (
            SELECT hub, price_aud_gj, volume_tj, pipeline_flow_tj, trade_date,
                   ROW_NUMBER() OVER (PARTITION BY hub ORDER BY trade_date DESC) AS rn
            FROM {GOLD}.gas_hub_prices
        )
        SELECT * FROM latest WHERE rn = 1
    """).collect()

    hubs = []
    for r in gas_df:
        hubs.append({
            "hub": r["hub"],
            "price_aud_gj": round(float(r["price_aud_gj"] or 0), 2),
            "volume_tj": round(float(r["volume_tj"] or 0), 1),
            "pipeline_flow_tj": round(float(r["pipeline_flow_tj"] or 0), 1),
            "trade_date": str(r["trade_date"]),
        })

    snapshots.append(_snap("/api/dashboards/gas", "ALL", {
        "hub_prices": hubs,
        "timestamp": now,
        "data_source": "gas_hub_prices",
    }))
except Exception as e:
    print(f"Gas failed: {e}")

# Emissions factors
try:
    ef_df = spark.sql(f"""
        SELECT fuel_type, scope1_kg_co2_mwh, scope2_kg_co2_mwh,
               total_kg_co2_mwh, source
        FROM {GOLD}.emissions_factors
        ORDER BY total_kg_co2_mwh DESC
    """).collect()

    factors = []
    for r in ef_df:
        factors.append({
            "fuel_type": r["fuel_type"],
            "scope1": round(float(r["scope1_kg_co2_mwh"] or 0), 1),
            "scope2": round(float(r["scope2_kg_co2_mwh"] or 0), 1),
            "total": round(float(r["total_kg_co2_mwh"] or 0), 1),
            "source": r["source"],
        })

    snapshots.append(_snap("/api/dashboards/carbon", "ALL", {
        "emissions_factors": factors,
        "timestamp": now,
        "data_source": "emissions_factors",
    }))
except Exception as e:
    print(f"Emissions failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecasts Snapshot

# COMMAND ----------

try:
    price_fc = spark.sql(f"""
        SELECT region_id, AVG(predicted_rrp) AS avg_forecast,
               AVG(spike_probability) AS avg_spike_prob,
               MIN(prediction_lower_80) AS lower_80,
               MAX(prediction_upper_80) AS upper_80,
               COUNT(*) AS forecast_points
        FROM {GOLD}.price_forecasts
        WHERE interval_datetime >= current_timestamp()
          AND interval_datetime <= current_timestamp() + INTERVAL 24 HOURS
        GROUP BY region_id
    """).collect()

    forecasts = []
    for r in price_fc:
        forecasts.append({
            "region_id": r["region_id"],
            "avg_forecast": round(float(r["avg_forecast"] or 0), 2),
            "avg_spike_prob": round(float(r["avg_spike_prob"] or 0), 4),
            "lower_80": round(float(r["lower_80"] or 0), 2),
            "upper_80": round(float(r["upper_80"] or 0), 2),
            "forecast_points": int(r["forecast_points"] or 0),
        })

    snapshots.append(_snap("/api/home/price-forecasts", "ALL", {
        "forecasts": forecasts,
        "timestamp": now,
        "data_source": "price_forecasts",
    }))
except Exception as e:
    print(f"Forecasts failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write All Snapshots

# COMMAND ----------

if snapshots:
    cols = ["endpoint_path", "region", "snapshot_at", "payload_json"]
    snap_df = spark.createDataFrame(snapshots, cols)

    # MERGE into dashboard_snapshots (upsert on endpoint_path + region)
    snap_df.createOrReplaceTempView("new_snapshots")

    spark.sql(f"""
        MERGE INTO {SNAPSHOTS} t
        USING new_snapshots s
        ON t.endpoint_path = s.endpoint_path AND t.region = s.region
        WHEN MATCHED THEN UPDATE SET
            t.snapshot_at = s.snapshot_at,
            t.payload_json = s.payload_json
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"Wrote {len(snapshots)} snapshots to {SNAPSHOTS}")
else:
    print("No snapshots generated")

# COMMAND ----------

# Verify
display(spark.sql(f"SELECT endpoint_path, region, snapshot_at FROM {SNAPSHOTS} ORDER BY endpoint_path, region"))
