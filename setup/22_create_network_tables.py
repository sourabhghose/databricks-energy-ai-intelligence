# Databricks notebook source
# ============================================================
# Phase 4: Network Operations & Distribution Intelligence
# ============================================================
# Creates 19 gold tables + seeds realistic Australian DNSP data
# for distribution network monitoring, asset health, outages,
# DER management, and network planning.
# ============================================================

# COMMAND ----------

catalog = "energy_copilot_catalog"
sp_id = "67aaaa6b-778c-4c8b-b2f0-9f9b9728b3bb"

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

# ============================================================
# 1. network_assets — Zone substations, feeders, transformers
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.network_assets (
    asset_id STRING,
    asset_type STRING,
    asset_name STRING,
    nameplate_rating_mva DOUBLE,
    installation_year INT,
    condition_score DOUBLE,
    replacement_cost_aud DOUBLE,
    lat DOUBLE,
    lng DOUBLE,
    region STRING,
    parent_asset_id STRING,
    dnsp STRING,
    voltage_kv DOUBLE,
    status STRING
)
USING DELTA
COMMENT 'Distribution network asset register — substations, feeders, transformers'
""")
print("✓ network_assets")

# COMMAND ----------

# ============================================================
# 2. asset_loading_5min — Real-time loading telemetry
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.asset_loading_5min (
    asset_id STRING,
    interval_datetime TIMESTAMP,
    mw DOUBLE,
    mvar DOUBLE,
    utilization_pct DOUBLE
)
USING DELTA
COMMENT '5-minute asset loading telemetry for zone substations and feeders'
""")
print("✓ asset_loading_5min")

# COMMAND ----------

# ============================================================
# 3. asset_health_index — Condition-based health scores
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.asset_health_index (
    asset_id STRING,
    health_index DOUBLE,
    risk_factors STRING,
    updated_date DATE
)
USING DELTA
COMMENT 'Asset condition health index scores (0-100, lower = worse)'
""")
print("✓ asset_health_index")

# COMMAND ----------

# ============================================================
# 4. asset_failure_predictions — ML failure probability
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.asset_failure_predictions (
    asset_id STRING,
    failure_probability_12m DOUBLE,
    contributing_factors STRING,
    model_version STRING,
    scoring_date DATE
)
USING DELTA
COMMENT 'Predictive maintenance — 12-month failure probability per asset'
""")
print("✓ asset_failure_predictions")

# COMMAND ----------

# ============================================================
# 5. voltage_monitoring — Voltage excursion events
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.voltage_monitoring (
    monitoring_point_id STRING,
    interval_datetime TIMESTAMP,
    voltage_kv DOUBLE,
    nominal_kv DOUBLE,
    excursion_flag BOOLEAN,
    excursion_type STRING
)
USING DELTA
COMMENT 'Voltage monitoring — excursion detection at distribution level'
""")
print("✓ voltage_monitoring")

# COMMAND ----------

# ============================================================
# 6. power_quality — THD, flicker, voltage unbalance
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.power_quality (
    monitoring_point_id STRING,
    interval_datetime TIMESTAMP,
    thd_pct DOUBLE,
    flicker_pst DOUBLE,
    voltage_unbalance_pct DOUBLE
)
USING DELTA
COMMENT 'Power quality metrics — total harmonic distortion, flicker, unbalance'
""")
print("✓ power_quality")

# COMMAND ----------

# ============================================================
# 7. outage_events — Historical and active outages
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.outage_events (
    event_id STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    feeder_id STRING,
    zone_substation STRING,
    affected_customers INT,
    cause_code STRING,
    region STRING,
    status STRING,
    etr_minutes INT
)
USING DELTA
COMMENT 'Distribution outage events with cause codes and affected customers'
""")
print("✓ outage_events")

# COMMAND ----------

# ============================================================
# 8. reliability_kpis — SAIDI, SAIFI, CAIDI, MAIFI
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.reliability_kpis (
    region STRING,
    period_type STRING,
    period_start DATE,
    saidi_minutes DOUBLE,
    saifi_count DOUBLE,
    caidi_minutes DOUBLE,
    maifi_count DOUBLE,
    aer_target_saidi DOUBLE,
    aer_target_saifi DOUBLE
)
USING DELTA
COMMENT 'Distribution reliability KPIs — SAIDI/SAIFI/CAIDI/MAIFI vs AER targets'
""")
print("✓ reliability_kpis")

# COMMAND ----------

# ============================================================
# 9. gsl_tracking — Guaranteed Service Level payments
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.gsl_tracking (
    customer_nmi STRING,
    feeder_id STRING,
    interruption_count_ytd INT,
    cumulative_hours_ytd DOUBLE,
    gsl_eligible_flag BOOLEAN,
    projected_payment_aud DOUBLE
)
USING DELTA
COMMENT 'Guaranteed Service Level tracking — customer interruptions and projected payments'
""")
print("✓ gsl_tracking")

# COMMAND ----------

# ============================================================
# 10. der_fleet — Distributed energy resource register
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.der_fleet (
    der_id STRING,
    feeder_id STRING,
    zone_substation STRING,
    technology STRING,
    capacity_kw DOUBLE,
    connection_date DATE,
    lat DOUBLE,
    lng DOUBLE,
    region STRING
)
USING DELTA
COMMENT 'Distributed energy resource fleet — solar, battery, EV chargers by feeder'
""")
print("✓ der_fleet")

# COMMAND ----------

# ============================================================
# 11. der_output_estimated — Estimated DER generation
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.der_output_estimated (
    feeder_id STRING,
    interval_datetime TIMESTAMP,
    solar_mw DOUBLE,
    battery_dispatch_mw DOUBLE,
    net_export_mw DOUBLE,
    reverse_power_flow_flag BOOLEAN
)
USING DELTA
COMMENT 'Estimated DER output per feeder — solar, battery, net export'
""")
print("✓ der_output_estimated")

# COMMAND ----------

# ============================================================
# 12. hosting_capacity — Static and dynamic hosting capacity
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.hosting_capacity (
    feeder_id STRING,
    transformer_id STRING,
    static_capacity_kw DOUBLE,
    dynamic_capacity_kw DOUBLE,
    limiting_constraint STRING,
    scenario STRING
)
USING DELTA
COMMENT 'Feeder hosting capacity — static and dynamic DER export limits'
""")
print("✓ hosting_capacity")

# COMMAND ----------

# ============================================================
# 13. curtailment_events — DER curtailment
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.curtailment_events (
    feeder_id STRING,
    interval_datetime TIMESTAMP,
    curtailed_mwh DOUBLE,
    curtailment_reason STRING,
    affected_customers INT,
    lost_energy_value_aud DOUBLE
)
USING DELTA
COMMENT 'DER curtailment events — MWh lost, reason, financial impact'
""")
print("✓ curtailment_events")

# COMMAND ----------

# ============================================================
# 14. doe_compliance — Dynamic operating envelopes
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.doe_compliance (
    feeder_id STRING,
    interval_datetime TIMESTAMP,
    doe_limit_kw DOUBLE,
    customer_response_kw DOUBLE,
    compliance_rate_pct DOUBLE
)
USING DELTA
COMMENT 'Dynamic operating envelope compliance rates by feeder'
""")
print("✓ doe_compliance")

# COMMAND ----------

# ============================================================
# 15. vpp_dispatch_events — Virtual power plant dispatches
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.vpp_dispatch_events (
    program_id STRING,
    program_name STRING,
    dispatch_datetime TIMESTAMP,
    target_mw DOUBLE,
    response_mw DOUBLE,
    response_accuracy_pct DOUBLE,
    revenue_aud DOUBLE
)
USING DELTA
COMMENT 'VPP dispatch events — target vs actual response and revenue'
""")
print("✓ vpp_dispatch_events")

# COMMAND ----------

# ============================================================
# 16. demand_forecast_spatial — 10-year spatial demand
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.demand_forecast_spatial (
    zone_substation STRING,
    scenario STRING,
    forecast_year INT,
    peak_demand_mw DOUBLE,
    underlying_growth_mw DOUBLE,
    solar_impact_mw DOUBLE,
    battery_impact_mw DOUBLE,
    ev_impact_mw DOUBLE
)
USING DELTA
COMMENT 'Spatial demand forecasts — 10-year outlook per zone sub with DER/EV scenarios'
""")
print("✓ demand_forecast_spatial")

# COMMAND ----------

# ============================================================
# 17. network_constraints_register — Constraint register
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.network_constraints_register (
    constraint_id STRING,
    zone_substation STRING,
    constraint_type STRING,
    limiting_asset STRING,
    current_utilization_pct DOUBLE,
    breach_year_bau INT,
    breach_year_high INT,
    options_json STRING
)
USING DELTA
COMMENT 'Network constraint register — thermal/voltage/fault level with augmentation options'
""")
print("✓ network_constraints_register")

# COMMAND ----------

# ============================================================
# 18. ev_charging_profiles — EV load shapes by type
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.ev_charging_profiles (
    feeder_id STRING,
    charge_point_type STRING,
    hour_of_day INT,
    load_kw_per_ev DOUBLE
)
USING DELTA
COMMENT 'EV charging load profiles by charge point type and time of day'
""")
print("✓ ev_charging_profiles")

# COMMAND ----------

# ============================================================
# 19. ev_network_impact — EV penetration impact projections
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.ev_network_impact (
    feeder_id STRING,
    scenario STRING,
    target_year INT,
    peak_load_delta_mw DOUBLE,
    assets_at_risk INT,
    upgrade_required_flag BOOLEAN
)
USING DELTA
COMMENT 'EV network impact projections — peak load increase, assets at risk'
""")
print("✓ ev_network_impact")

# COMMAND ----------

# ============================================================
# SEED DATA — Realistic Australian DNSP data
# ============================================================

import random
import uuid
from datetime import datetime, timedelta

random.seed(42)
now = datetime.utcnow()

def _to_df(rows, schema=None):
    """Create DataFrame, auto-casting numerics to avoid int/float merge errors."""
    if not rows:
        return spark.createDataFrame([], schema) if schema else spark.createDataFrame([])
    if schema:
        return spark.createDataFrame(rows, schema)
    # Ensure all numeric values in a column share the same type
    # by converting everything to float for DOUBLE columns
    sample = rows[0]
    float_keys = set()
    for row in rows:
        for k, v in row.items():
            if isinstance(v, float):
                float_keys.add(k)
    if float_keys:
        for row in rows:
            for k in float_keys:
                if k in row and isinstance(row[k], int):
                    row[k] = float(row[k])
    return spark.createDataFrame(rows)

# --- Australian DNSP reference data ---
DNSPS = {
    "NSW1": ["Ausgrid", "Endeavour Energy"],
    "QLD1": ["Ergon Energy", "Energex"],
    "VIC1": ["CitiPower", "Powercor", "AusNet Services"],
    "SA1": ["SA Power Networks"],
    "TAS1": ["TasNetworks"],
}

ZONE_SUBS = {
    "NSW1": ["Sydney North", "Sydney South", "Parramatta", "Blacktown", "Penrith",
             "Liverpool", "Hornsby", "Chatswood", "Manly", "Newcastle",
             "Wollongong", "Campbelltown", "Bankstown", "Sutherland", "Ryde"],
    "QLD1": ["Brisbane CBD", "Gold Coast", "Sunshine Coast", "Ipswich", "Toowoomba",
             "Cairns", "Townsville", "Rockhampton", "Mackay", "Bundaberg"],
    "VIC1": ["Melbourne CBD", "Richmond", "Footscray", "Dandenong", "Ringwood",
             "Geelong", "Frankston", "Werribee", "Sunbury", "Cranbourne"],
    "SA1": ["Adelaide CBD", "Elizabeth", "Salisbury", "Marion", "Port Adelaide",
            "Noarlunga", "Modbury", "Glenelg"],
    "TAS1": ["Hobart", "Launceston", "Devonport", "Burnie"],
}

FEEDERS_PER_SUB = 4  # average feeders per zone substation
TX_PER_FEEDER = 2    # average distribution transformers per feeder

# COMMAND ----------

# --- 1. Seed network_assets ---
assets = []
feeder_ids = []
sub_ids = []

for region, subs in ZONE_SUBS.items():
    dnsps = DNSPS[region]
    for sub_name in subs:
        dnsp = random.choice(dnsps)
        sub_id = f"ZS-{sub_name.replace(' ', '_').upper()}"
        sub_ids.append(sub_id)
        lat_base = {"NSW1": -33.87, "QLD1": -27.47, "VIC1": -37.81, "SA1": -34.93, "TAS1": -42.88}[region]
        lng_base = {"NSW1": 151.21, "QLD1": 153.02, "VIC1": 144.96, "SA1": 138.60, "TAS1": 147.33}[region]

        # Zone substation
        assets.append({
            "asset_id": sub_id,
            "asset_type": "zone_substation",
            "asset_name": f"{sub_name} Zone Substation",
            "nameplate_rating_mva": float(random.choice([20, 30, 40, 60, 80, 100])),
            "installation_year": random.randint(1975, 2020),
            "condition_score": round(random.uniform(40, 95), 1),
            "replacement_cost_aud": float(random.randint(5, 25) * 1_000_000),
            "lat": round(lat_base + random.uniform(-0.3, 0.3), 4),
            "lng": round(lng_base + random.uniform(-0.3, 0.3), 4),
            "region": region,
            "parent_asset_id": "",
            "dnsp": dnsp,
            "voltage_kv": float(random.choice([33, 66, 132])),
            "status": "in_service",
        })

        # Feeders per substation
        n_feeders = random.randint(3, 6)
        for fi in range(n_feeders):
            fid = f"FDR-{sub_name.replace(' ', '_').upper()}-{fi+1:02d}"
            feeder_ids.append(fid)
            assets.append({
                "asset_id": fid,
                "asset_type": "feeder",
                "asset_name": f"{sub_name} Feeder {fi+1}",
                "nameplate_rating_mva": float(random.choice([5, 8, 10, 15, 20, 30])),
                "installation_year": random.randint(1980, 2022),
                "condition_score": round(random.uniform(30, 98), 1),
                "replacement_cost_aud": float(random.randint(500_000, 5_000_000)),
                "lat": round(lat_base + random.uniform(-0.3, 0.3), 4),
                "lng": round(lng_base + random.uniform(-0.3, 0.3), 4),
                "region": region,
                "parent_asset_id": sub_id,
                "dnsp": dnsp,
                "voltage_kv": 11.0,
                "status": "in_service",
            })

            # Distribution transformers per feeder
            n_tx = random.randint(1, 3)
            for ti in range(n_tx):
                assets.append({
                    "asset_id": f"TX-{sub_name.replace(' ', '_').upper()}-{fi+1:02d}-{ti+1:02d}",
                    "asset_type": "distribution_transformer",
                    "asset_name": f"{sub_name} TX {fi+1}-{ti+1}",
                    "nameplate_rating_mva": random.choice([0.1, 0.2, 0.315, 0.5, 0.75, 1.0]),
                    "installation_year": random.randint(1985, 2023),
                    "condition_score": round(random.uniform(25, 99), 1),
                    "replacement_cost_aud": float(random.randint(15_000, 150_000)),
                    "lat": round(lat_base + random.uniform(-0.35, 0.35), 4),
                    "lng": round(lng_base + random.uniform(-0.35, 0.35), 4),
                    "region": region,
                    "parent_asset_id": fid,
                    "dnsp": dnsp,
                    "voltage_kv": 0.415,
                    "status": random.choices(["in_service", "degraded"], weights=[90, 10])[0],
                })

df_assets = _to_df(assets)
df_assets.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.network_assets")
print(f"✓ network_assets seeded: {len(assets)} rows")

# COMMAND ----------

# --- 2. Seed asset_loading_5min (24h of 5-min data for zone subs) ---
loading_rows = []
for sub_id in sub_ids:
    rating = random.uniform(20, 100)
    for i in range(288):  # 24h × 12 intervals/hr
        dt = now - timedelta(minutes=5 * (287 - i))
        hour = dt.hour
        # Diurnal load curve: low overnight, peak 6-9pm, solar dip midday
        base = 0.35 + 0.25 * (1 if 7 <= hour <= 21 else 0)
        solar_dip = -0.12 if 10 <= hour <= 15 else 0
        evening_peak = 0.20 if 17 <= hour <= 20 else 0
        util = max(0.1, min(0.99, base + solar_dip + evening_peak + random.gauss(0, 0.05)))
        mw = round(rating * util, 2)
        loading_rows.append({
            "asset_id": sub_id,
            "interval_datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "mw": mw,
            "mvar": round(mw * random.uniform(0.2, 0.4), 2),
            "utilization_pct": round(util * 100, 1),
        })

df_loading = _to_df(loading_rows)
df_loading.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.asset_loading_5min")
print(f"✓ asset_loading_5min seeded: {len(loading_rows)} rows")

# COMMAND ----------

# --- 3. Seed asset_health_index ---
health_rows = []
risk_options = ["oil_degradation", "winding_insulation", "bushing_condition",
                "corrosion", "overloading_history", "age", "moisture_ingress",
                "partial_discharge", "tap_changer_wear"]

for a in assets:
    if a["asset_type"] in ("zone_substation", "feeder"):
        age = now.year - a["installation_year"]
        base_health = max(10, 100 - age * 1.2 + random.gauss(0, 10))
        factors = random.sample(risk_options, k=random.randint(1, 3))
        health_rows.append({
            "asset_id": a["asset_id"],
            "health_index": round(min(100, max(5, base_health)), 1),
            "risk_factors": str(factors),
            "updated_date": now.strftime("%Y-%m-%d"),
        })

df_health = _to_df(health_rows)
df_health.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.asset_health_index")
print(f"✓ asset_health_index seeded: {len(health_rows)} rows")

# COMMAND ----------

# --- 4. Seed asset_failure_predictions ---
failure_rows = []
for h in health_rows:
    hi = h["health_index"]
    # Inverse relationship: low health → high failure probability
    base_prob = max(0.01, min(0.95, (100 - hi) / 100 * 0.8 + random.gauss(0, 0.05)))
    factors = random.sample(["thermal_stress", "electrical_stress", "environmental",
                             "age_related", "manufacturing_defect", "overload_cycles"], k=random.randint(1, 3))
    failure_rows.append({
        "asset_id": h["asset_id"],
        "failure_probability_12m": round(base_prob, 3),
        "contributing_factors": str(factors),
        "model_version": "v2.1.0",
        "scoring_date": now.strftime("%Y-%m-%d"),
    })

df_failure = _to_df(failure_rows)
df_failure.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.asset_failure_predictions")
print(f"✓ asset_failure_predictions seeded: {len(failure_rows)} rows")

# COMMAND ----------

# --- 5. Seed voltage_monitoring ---
voltage_rows = []
monitor_ids = [f"VM-{fid}" for fid in feeder_ids[:60]]  # Monitor 60 feeders

for mid in monitor_ids:
    nominal = random.choice([11.0, 22.0])
    for i in range(48):  # 24h × 2 (30-min intervals)
        dt = now - timedelta(minutes=30 * (47 - i))
        hour = dt.hour
        # Voltage rises with solar at midday
        solar_rise = 0.04 if 10 <= hour <= 15 else 0
        v = nominal * (1.0 + solar_rise + random.gauss(0, 0.015))
        excursion = v > nominal * 1.06 or v < nominal * 0.94
        voltage_rows.append({
            "monitoring_point_id": mid,
            "interval_datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "voltage_kv": round(v, 3),
            "nominal_kv": nominal,
            "excursion_flag": excursion,
            "excursion_type": "over" if v > nominal * 1.06 else ("under" if v < nominal * 0.94 else ""),
        })

df_voltage = _to_df(voltage_rows)
df_voltage.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.voltage_monitoring")
print(f"✓ voltage_monitoring seeded: {len(voltage_rows)} rows")

# COMMAND ----------

# --- 6. Seed power_quality ---
pq_rows = []
for mid in monitor_ids[:40]:
    for i in range(48):
        dt = now - timedelta(minutes=30 * (47 - i))
        pq_rows.append({
            "monitoring_point_id": mid,
            "interval_datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "thd_pct": round(random.uniform(1.5, 8.0), 2),
            "flicker_pst": round(random.uniform(0.3, 1.5), 2),
            "voltage_unbalance_pct": round(random.uniform(0.5, 3.0), 2),
        })

df_pq = _to_df(pq_rows)
df_pq.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.power_quality")
print(f"✓ power_quality seeded: {len(pq_rows)} rows")

# COMMAND ----------

# --- 7. Seed outage_events ---
CAUSE_CODES = ["vegetation", "animal", "equipment_failure", "weather", "third_party", "unknown"]
CAUSE_WEIGHTS = [30, 10, 25, 20, 10, 5]

outage_rows = []
for i in range(150):
    region = random.choice(list(ZONE_SUBS.keys()))
    sub = random.choice(ZONE_SUBS[region])
    fid = random.choice([f for f in feeder_ids if sub.replace(" ", "_").upper() in f]) if any(sub.replace(" ", "_").upper() in f for f in feeder_ids) else random.choice(feeder_ids)
    start = now - timedelta(days=random.randint(0, 90), hours=random.randint(0, 23))
    duration_min = random.choices([15, 30, 60, 120, 240, 480], weights=[20, 25, 25, 15, 10, 5])[0]
    is_active = i < 5  # First 5 are still active
    outage_rows.append({
        "event_id": f"OUT-{uuid.uuid4().hex[:8].upper()}",
        "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": "" if is_active else (start + timedelta(minutes=duration_min)).strftime("%Y-%m-%d %H:%M:%S"),
        "feeder_id": fid,
        "zone_substation": f"ZS-{sub.replace(' ', '_').upper()}",
        "affected_customers": random.randint(50, 8000),
        "cause_code": random.choices(CAUSE_CODES, weights=CAUSE_WEIGHTS)[0],
        "region": region,
        "status": "active" if is_active else "restored",
        "etr_minutes": random.randint(30, 240) if is_active else 0,
    })

df_outages = _to_df(outage_rows)
df_outages.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.outage_events")
print(f"✓ outage_events seeded: {len(outage_rows)} rows")

# COMMAND ----------

# --- 8. Seed reliability_kpis ---
# AER benchmarks for Australian DNSPs
AER_TARGETS = {
    "NSW1": {"saidi": 85, "saifi": 1.1},
    "QLD1": {"saidi": 140, "saifi": 1.6},
    "VIC1": {"saidi": 75, "saifi": 1.0},
    "SA1": {"saidi": 120, "saifi": 1.3},
    "TAS1": {"saidi": 180, "saifi": 2.0},
}

rel_rows = []
for region, targets in AER_TARGETS.items():
    # Monthly data for 12 months
    for m in range(12):
        month_start = datetime(now.year - 1 if m < now.month else now.year, (m % 12) + 1, 1)
        saidi = round(targets["saidi"] / 12 * random.uniform(0.5, 1.8), 2)
        saifi = round(targets["saifi"] / 12 * random.uniform(0.5, 1.8), 3)
        caidi = round(saidi / max(saifi, 0.01), 1)
        rel_rows.append({
            "region": region,
            "period_type": "monthly",
            "period_start": month_start.strftime("%Y-%m-%d"),
            "saidi_minutes": saidi,
            "saifi_count": saifi,
            "caidi_minutes": caidi,
            "maifi_count": round(random.uniform(0.1, 0.8), 2),
            "aer_target_saidi": round(targets["saidi"] / 12, 2),
            "aer_target_saifi": round(targets["saifi"] / 12, 3),
        })
    # YTD
    ytd_saidi = sum(r["saidi_minutes"] for r in rel_rows if r["region"] == region)
    ytd_saifi = sum(r["saifi_count"] for r in rel_rows if r["region"] == region)
    rel_rows.append({
        "region": region,
        "period_type": "yearly",
        "period_start": datetime(now.year, 1, 1).strftime("%Y-%m-%d"),
        "saidi_minutes": round(ytd_saidi, 2),
        "saifi_count": round(ytd_saifi, 3),
        "caidi_minutes": round(ytd_saidi / max(ytd_saifi, 0.01), 1),
        "maifi_count": round(random.uniform(1.0, 5.0), 2),
        "aer_target_saidi": targets["saidi"],
        "aer_target_saifi": targets["saifi"],
    })

df_rel = _to_df(rel_rows)
df_rel.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.reliability_kpis")
print(f"✓ reliability_kpis seeded: {len(rel_rows)} rows")

# COMMAND ----------

# --- 9. Seed gsl_tracking ---
gsl_rows = []
for i in range(500):
    fid = random.choice(feeder_ids)
    interruptions = random.choices([0, 1, 2, 3, 4, 5, 6, 7], weights=[30, 25, 20, 10, 7, 4, 2, 2])[0]
    hours = round(interruptions * random.uniform(0.5, 3.0), 1)
    eligible = interruptions >= 4 or hours >= 12
    gsl_rows.append({
        "customer_nmi": f"NMI{random.randint(1000000000, 9999999999)}",
        "feeder_id": fid,
        "interruption_count_ytd": interruptions,
        "cumulative_hours_ytd": hours,
        "gsl_eligible_flag": eligible,
        "projected_payment_aud": round(random.uniform(80, 350), 2) if eligible else 0.0,
    })

df_gsl = _to_df(gsl_rows)
df_gsl.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.gsl_tracking")
print(f"✓ gsl_tracking seeded: {len(gsl_rows)} rows")

# COMMAND ----------

# --- 10. Seed der_fleet ---
DER_TECHNOLOGIES = ["solar", "battery", "ev_charger"]
DER_WEIGHTS = [60, 20, 20]

der_rows = []
for i in range(1000):
    region = random.choice(list(ZONE_SUBS.keys()))
    sub = random.choice(ZONE_SUBS[region])
    fid = random.choice([f for f in feeder_ids if sub.replace(" ", "_").upper() in f]) if any(sub.replace(" ", "_").upper() in f for f in feeder_ids) else random.choice(feeder_ids)
    tech = random.choices(DER_TECHNOLOGIES, weights=DER_WEIGHTS)[0]
    cap = {"solar": random.choice([3, 5, 6.6, 10, 13, 100]), "battery": random.choice([5, 10, 13.5, 50]), "ev_charger": random.choice([7, 11, 22, 50, 150])}[tech]
    lat_base = {"NSW1": -33.87, "QLD1": -27.47, "VIC1": -37.81, "SA1": -34.93, "TAS1": -42.88}[region]
    lng_base = {"NSW1": 151.21, "QLD1": 153.02, "VIC1": 144.96, "SA1": 138.60, "TAS1": 147.33}[region]
    der_rows.append({
        "der_id": f"DER-{uuid.uuid4().hex[:8].upper()}",
        "feeder_id": fid,
        "zone_substation": f"ZS-{sub.replace(' ', '_').upper()}",
        "technology": tech,
        "capacity_kw": cap,
        "connection_date": (now - timedelta(days=random.randint(30, 2000))).strftime("%Y-%m-%d"),
        "lat": round(lat_base + random.uniform(-0.4, 0.4), 4),
        "lng": round(lng_base + random.uniform(-0.4, 0.4), 4),
        "region": region,
    })

df_der = _to_df(der_rows)
df_der.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.der_fleet")
print(f"✓ der_fleet seeded: {len(der_rows)} rows")

# COMMAND ----------

# --- 11. Seed der_output_estimated ---
der_output_rows = []
sample_feeders = random.sample(feeder_ids, min(30, len(feeder_ids)))
for fid in sample_feeders:
    for i in range(96):  # 24h × 4 (15-min intervals)
        dt = now - timedelta(minutes=15 * (95 - i))
        hour = dt.hour
        solar = max(0, 2.5 * max(0, 1 - abs(hour - 12) / 6) + random.gauss(0, 0.3))
        battery = round(random.uniform(-0.5, 0.5), 2)  # charge/discharge
        net = round(solar + battery - random.uniform(1.0, 3.0), 2)
        der_output_rows.append({
            "feeder_id": fid,
            "interval_datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "solar_mw": round(solar, 3),
            "battery_dispatch_mw": battery,
            "net_export_mw": net,
            "reverse_power_flow_flag": net > 0,
        })

df_der_out = _to_df(der_output_rows)
df_der_out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.der_output_estimated")
print(f"✓ der_output_estimated seeded: {len(der_output_rows)} rows")

# COMMAND ----------

# --- 12. Seed hosting_capacity ---
hc_rows = []
for fid in feeder_ids:
    static_cap = random.randint(200, 2000)
    dynamic_cap = int(static_cap * random.uniform(1.1, 1.8))
    constraint = random.choice(["voltage_rise", "thermal_limit", "protection_settings", "fault_level"])
    for scenario in ["base", "high_solar"]:
        multiplier = 1.0 if scenario == "base" else 0.7
        hc_rows.append({
            "feeder_id": fid,
            "transformer_id": "",
            "static_capacity_kw": float(round(static_cap * multiplier)),
            "dynamic_capacity_kw": float(round(dynamic_cap * multiplier)),
            "limiting_constraint": constraint,
            "scenario": scenario,
        })

df_hc = _to_df(hc_rows)
df_hc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.hosting_capacity")
print(f"✓ hosting_capacity seeded: {len(hc_rows)} rows")

# COMMAND ----------

# --- 13. Seed curtailment_events ---
curt_rows = []
CURT_REASONS = ["voltage", "thermal", "dnsp_limit"]
for i in range(200):
    fid = random.choice(feeder_ids)
    dt = now - timedelta(days=random.randint(0, 30), hours=random.randint(9, 15))
    mwh = round(random.uniform(0.1, 5.0), 3)
    curt_rows.append({
        "feeder_id": fid,
        "interval_datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "curtailed_mwh": mwh,
        "curtailment_reason": random.choice(CURT_REASONS),
        "affected_customers": random.randint(10, 500),
        "lost_energy_value_aud": round(mwh * random.uniform(50, 150), 2),
    })

df_curt = _to_df(curt_rows)
df_curt.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.curtailment_events")
print(f"✓ curtailment_events seeded: {len(curt_rows)} rows")

# COMMAND ----------

# --- 14. Seed doe_compliance ---
doe_rows = []
for fid in random.sample(feeder_ids, min(50, len(feeder_ids))):
    for i in range(10):
        dt = now - timedelta(days=random.randint(0, 30))
        limit = random.uniform(3, 10)
        response = limit * random.uniform(0.6, 1.05)
        doe_rows.append({
            "feeder_id": fid,
            "interval_datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "doe_limit_kw": round(limit, 2),
            "customer_response_kw": round(response, 2),
            "compliance_rate_pct": round(min(100, response / limit * 100), 1),
        })

df_doe = _to_df(doe_rows)
df_doe.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.doe_compliance")
print(f"✓ doe_compliance seeded: {len(doe_rows)} rows")

# COMMAND ----------

# --- 15. Seed vpp_dispatch_events ---
VPP_PROGRAMS = [
    ("VPP-SAPN-001", "SA Power Networks VPP"),
    ("VPP-AGL-001", "AGL Virtual Power Plant"),
    ("VPP-ORIGIN-001", "Origin Energy VPP"),
    ("VPP-TESLA-001", "Tesla Energy Plan"),
    ("VPP-AMBER-001", "Amber SmartShift"),
]

vpp_rows = []
for prog_id, prog_name in VPP_PROGRAMS:
    for i in range(20):
        dt = now - timedelta(days=random.randint(0, 60), hours=random.randint(15, 20))
        target = round(random.uniform(5, 50), 1)
        response = round(target * random.uniform(0.7, 1.1), 1)
        vpp_rows.append({
            "program_id": prog_id,
            "program_name": prog_name,
            "dispatch_datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "target_mw": target,
            "response_mw": response,
            "response_accuracy_pct": round(min(100, response / target * 100), 1),
            "revenue_aud": round(response * random.uniform(100, 500) * random.uniform(0.5, 2), 2),
        })

df_vpp = _to_df(vpp_rows)
df_vpp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.vpp_dispatch_events")
print(f"✓ vpp_dispatch_events seeded: {len(vpp_rows)} rows")

# COMMAND ----------

# --- 16. Seed demand_forecast_spatial ---
SCENARIOS = ["bau", "high_solar", "high_ev", "combined"]
forecast_rows = []

for region, subs in ZONE_SUBS.items():
    for sub_name in subs:
        zs = f"ZS-{sub_name.replace(' ', '_').upper()}"
        base_peak = random.uniform(15, 80)
        for scenario in SCENARIOS:
            for year in range(2026, 2036):
                offset = year - 2026
                growth = base_peak * 0.015 * offset  # 1.5% underlying growth p.a.
                solar = -base_peak * 0.008 * offset if scenario in ("high_solar", "combined") else -base_peak * 0.003 * offset
                battery = -base_peak * 0.005 * offset if scenario in ("high_solar", "combined") else -base_peak * 0.002 * offset
                ev = base_peak * 0.012 * offset if scenario in ("high_ev", "combined") else base_peak * 0.004 * offset
                peak = round(base_peak + growth + solar + battery + ev, 2)
                forecast_rows.append({
                    "zone_substation": zs,
                    "scenario": scenario,
                    "forecast_year": year,
                    "peak_demand_mw": peak,
                    "underlying_growth_mw": round(growth, 2),
                    "solar_impact_mw": round(solar, 2),
                    "battery_impact_mw": round(battery, 2),
                    "ev_impact_mw": round(ev, 2),
                })

df_forecast = _to_df(forecast_rows)
df_forecast.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.demand_forecast_spatial")
print(f"✓ demand_forecast_spatial seeded: {len(forecast_rows)} rows")

# COMMAND ----------

# --- 17. Seed network_constraints_register ---
constraint_rows = []
CONSTRAINT_TYPES = ["thermal", "voltage", "fault_level"]
for i in range(50):
    region = random.choice(list(ZONE_SUBS.keys()))
    sub = random.choice(ZONE_SUBS[region])
    zs = f"ZS-{sub.replace(' ', '_').upper()}"
    ctype = random.choice(CONSTRAINT_TYPES)
    util = round(random.uniform(65, 105), 1)
    breach_bau = random.randint(2026, 2035)
    options = [
        {"option": "Network augmentation", "type": "network", "npv_aud": random.randint(2, 15) * 1_000_000},
        {"option": "Demand management", "type": "non_network", "npv_aud": random.randint(500_000, 5_000_000)},
        {"option": "Battery storage", "type": "non_network", "npv_aud": random.randint(1, 8) * 1_000_000},
    ]
    constraint_rows.append({
        "constraint_id": f"CON-{uuid.uuid4().hex[:8].upper()}",
        "zone_substation": zs,
        "constraint_type": ctype,
        "limiting_asset": f"{sub} {'transformer' if ctype == 'thermal' else 'feeder' if ctype == 'voltage' else 'switchgear'}",
        "current_utilization_pct": util,
        "breach_year_bau": breach_bau,
        "breach_year_high": max(2026, breach_bau - random.randint(1, 3)),
        "options_json": str(options).replace("'", '"'),
    })

df_constraints = _to_df(constraint_rows)
df_constraints.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.network_constraints_register")
print(f"✓ network_constraints_register seeded: {len(constraint_rows)} rows")

# COMMAND ----------

# --- 18. Seed ev_charging_profiles ---
ev_profile_rows = []
CHARGE_TYPES = ["residential", "commercial", "fast_charger"]
for fid in random.sample(feeder_ids, min(10, len(feeder_ids))):
    for ctype in CHARGE_TYPES:
        for hour in range(24):
            if ctype == "residential":
                load = 3.5 if 17 <= hour <= 23 else (1.0 if 0 <= hour <= 6 else 0.5)
            elif ctype == "commercial":
                load = 11 if 8 <= hour <= 17 else 2.0
            else:
                load = random.uniform(20, 50)
            ev_profile_rows.append({
                "feeder_id": fid,
                "charge_point_type": ctype,
                "hour_of_day": hour,
                "load_kw_per_ev": round(load + random.gauss(0, 0.5), 2),
            })

df_ev_prof = _to_df(ev_profile_rows)
df_ev_prof.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.ev_charging_profiles")
print(f"✓ ev_charging_profiles seeded: {len(ev_profile_rows)} rows")

# COMMAND ----------

# --- 19. Seed ev_network_impact ---
ev_impact_rows = []
EV_SCENARIOS = ["low", "medium", "high"]
for fid in feeder_ids:
    for scenario in EV_SCENARIOS:
        multiplier = {"low": 0.3, "medium": 0.6, "high": 1.0}[scenario]
        for year in [2028, 2030, 2033, 2035]:
            delta = round(random.uniform(0.2, 3.0) * multiplier * ((year - 2026) / 4), 2)
            at_risk = random.randint(0, 3) if delta > 1.0 else 0
            ev_impact_rows.append({
                "feeder_id": fid,
                "scenario": scenario,
                "target_year": year,
                "peak_load_delta_mw": delta,
                "assets_at_risk": at_risk,
                "upgrade_required_flag": at_risk > 0,
            })

# Limit to ~200 rows for reasonable size
ev_impact_rows = ev_impact_rows[:200]
df_ev = _to_df(ev_impact_rows)
df_ev.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.ev_network_impact")
print(f"✓ ev_network_impact seeded: {len(ev_impact_rows)} rows")

# COMMAND ----------

# ============================================================
# GRANT SP ACCESS — SELECT on all tables, MODIFY where needed
# ============================================================
TABLES_SELECT = [
    "network_assets", "asset_loading_5min", "asset_health_index",
    "asset_failure_predictions", "voltage_monitoring", "power_quality",
    "outage_events", "reliability_kpis", "gsl_tracking",
    "der_fleet", "der_output_estimated", "hosting_capacity",
    "curtailment_events", "doe_compliance", "vpp_dispatch_events",
    "demand_forecast_spatial", "network_constraints_register",
    "ev_charging_profiles", "ev_network_impact",
]

for t in TABLES_SELECT:
    spark.sql(f"GRANT SELECT ON TABLE {catalog}.gold.{t} TO `{sp_id}`")
    print(f"  ✓ SELECT on {t}")

# MODIFY for tables that may need writes from the app
for t in ["outage_events", "vpp_dispatch_events"]:
    spark.sql(f"GRANT MODIFY ON TABLE {catalog}.gold.{t} TO `{sp_id}`")
    print(f"  ✓ MODIFY on {t}")

print("\n✓ All SP grants applied")

# COMMAND ----------

# ============================================================
# VERIFY — Row counts
# ============================================================
for t in TABLES_SELECT:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.{t}").collect()[0]["cnt"]
    print(f"  {t}: {count} rows")

print("\n✓ Phase 4 network tables setup complete!")
