# Databricks notebook source
# MAGIC %md
# MAGIC # NEM Historical Backfill — 90 Days
# MAGIC
# MAGIC Generates 90 days of synthetic NEM market data at 5-minute resolution
# MAGIC and writes it to gold tables in `energy_copilot_catalog`.
# MAGIC
# MAGIC - 288 intervals/day x 90 days = 25,920 intervals
# MAGIC - Processes one day at a time to limit memory usage
# MAGIC - Uses the same simulation logic as `10_nem_simulator.py`

# COMMAND ----------

import math
import random
import uuid
from datetime import datetime, timedelta, timezone

spark.conf.set("spark.sql.session.timeZone", "Australia/Sydney")
try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "energy_copilot_catalog"
AEST = timezone(timedelta(hours=10))

DAYS_BACK = 90
INTERVALS_PER_DAY = 288  # 24h * 60min / 5min
CHUNK_SIZE = 500  # rows per INSERT statement

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration (same as live simulator)

# COMMAND ----------

REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
BASE_DEMAND = {"NSW1": 8500, "QLD1": 7200, "VIC1": 5900, "SA1": 1750, "TAS1": 1100}
BASE_PRICES = {"NSW1": 85, "QLD1": 90, "VIC1": 75, "SA1": 115, "TAS1": 60}

FUEL_CAPACITIES = {
    "NSW1": {"coal_black": 8500, "gas_ccgt": 1200, "gas_ocgt": 1600, "hydro": 900, "wind": 1400, "solar_utility": 2200, "battery": 400},
    "QLD1": {"coal_black": 9200, "gas_ccgt": 2800, "gas_ocgt": 1400, "hydro": 700, "wind": 800, "solar_utility": 3800, "battery": 200},
    "VIC1": {"coal_brown": 4500, "gas_ccgt": 1200, "gas_ocgt": 1000, "hydro": 600, "wind": 3200, "solar_utility": 1400, "battery": 320},
    "SA1":  {"gas_ccgt": 800, "gas_ocgt": 1800, "wind": 2800, "solar_utility": 1200, "battery": 300},
    "TAS1": {"hydro": 2700, "wind": 480, "gas_ocgt": 200},
}

INTERCONNECTORS = [
    ("N-Q-MNSP1", "NSW1", "QLD1", 700, 1078),
    ("VIC1-NSW1", "VIC1", "NSW1", 1350, 1600),
    ("V-SA", "VIC1", "SA1", 680, 680),
    ("T-V-MNSP1", "TAS1", "VIC1", 594, 478),
    ("V-S-MNSP1", "VIC1", "SA1", 220, 220),
]

EMISSIONS = {
    "coal_black": 0.9, "coal_brown": 1.2, "gas_ccgt": 0.4, "gas_ocgt": 0.6,
    "hydro": 0.0, "wind": 0.0, "solar_utility": 0.0, "battery": 0.0,
}

WEATHER_BASE = {
    "NSW1": {"temp": 28, "wind": 22, "solar": 800, "cloud": 30},
    "QLD1": {"temp": 32, "wind": 18, "solar": 900, "cloud": 25},
    "VIC1": {"temp": 26, "wind": 28, "solar": 750, "cloud": 35},
    "SA1":  {"temp": 34, "wind": 32, "solar": 850, "cloud": 20},
    "TAS1": {"temp": 20, "wind": 35, "solar": 600, "cloud": 45},
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotency Check

# COMMAND ----------

start_dt = datetime.now(AEST) - timedelta(days=DAYS_BACK)
start_str = start_dt.strftime("%Y-%m-%d")

existing = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {CATALOG}.gold.nem_prices_5min
    WHERE interval_datetime >= '{start_str}'
      AND interval_datetime < current_timestamp() - INTERVAL 2 DAYS
""").collect()[0]["cnt"]

if existing > 50000:
    msg = f"Found {existing} historical rows in nem_prices_5min. Backfill appears already done — skipping."
    print(msg)
    dbutils.notebook.exit(msg)

print(f"Starting backfill: {DAYS_BACK} days back from {datetime.now(AEST).strftime('%Y-%m-%d %H:%M')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def _diurnal(hour):
    return 0.6 + 0.3 * math.exp(-((hour - 8)**2) / 8) + 0.4 * math.exp(-((hour - 18)**2) / 6)

def _solar(hour):
    if hour < 6 or hour > 19:
        return 0.0
    return max(0, math.sin((hour - 6) / 13 * math.pi)) ** 1.3

def _wind(hour):
    return 0.5 + 0.3 * math.cos((hour - 3) / 24 * 2 * math.pi)

def _price(demand, avail, base, rng):
    ratio = demand / max(avail, 1)
    if ratio > 0.95:
        return base * (rng.uniform(2, 15) if rng.random() < 0.3 else rng.uniform(1.2, 2.5))
    elif ratio > 0.85:
        return base * rng.uniform(1.0, 1.5)
    elif ratio < 0.5:
        return base * rng.uniform(-0.2, 0.4)
    else:
        return base * rng.uniform(0.6, 1.1)

def _esc(s):
    """Escape single quotes for SQL."""
    return s.replace("'", "''")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL INSERT Writer (chunked)

# COMMAND ----------

def _sql_insert(table, columns, rows):
    """Build and execute multi-row INSERT statements in chunks."""
    if not rows:
        return
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        value_strs = []
        for row in chunk:
            vals = []
            for c in columns:
                v = row[c]
                if v is None:
                    vals.append("NULL")
                elif isinstance(v, bool):
                    vals.append("TRUE" if v else "FALSE")
                elif isinstance(v, (int, float)):
                    vals.append(str(round(v, 4)))
                elif isinstance(v, datetime):
                    vals.append(f"TIMESTAMP '{v.strftime('%Y-%m-%d %H:%M:%S')}'")
                elif isinstance(v, list):
                    arr_items = ", ".join(f"'{_esc(str(x))}'" for x in v)
                    vals.append(f"ARRAY({arr_items})")
                else:
                    vals.append(f"'{_esc(str(v))}'")
            value_strs.append(f"({', '.join(vals)})")

        col_list = ", ".join(columns)
        sql = f"INSERT INTO {CATALOG}.{table} ({col_list}) VALUES {', '.join(value_strs)}"
        spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate One Interval

# COMMAND ----------

def generate_interval(now_aest, rng):
    """Generate all table rows for a single 5-minute interval. Returns dicts of rows by table."""
    hour = now_aest.hour + now_aest.minute / 60.0
    diurnal = _diurnal(hour)
    solar = _solar(hour)
    wind_f = _wind(hour)
    # Use a deterministic drift based on timestamp instead of wall-clock
    drift = math.sin(now_aest.timestamp() / 600) * 0.05
    now_utc = (now_aest - timedelta(hours=10)).replace(tzinfo=None)
    now_local = now_aest.replace(tzinfo=None)

    # ── GENERATION BY FUEL ──
    gen_rows = []
    region_totals = {}
    for region in REGIONS:
        total_mw = 0
        for fuel, capacity in FUEL_CAPACITIES[region].items():
            if "solar" in fuel:
                cf = solar * rng.uniform(0.85, 1.0)
            elif "wind" in fuel:
                cf = wind_f * rng.uniform(0.6, 1.0)
            elif "hydro" in fuel:
                cf = rng.uniform(0.55, 0.80)
            elif "coal" in fuel:
                cf = rng.uniform(0.65, 0.85) * diurnal / 0.8
            elif "gas" in fuel:
                cf = max(0, rng.uniform(0.15, 0.50) * (diurnal - 0.3))
            elif "battery" in fuel:
                if 10 < hour < 15 and solar > 0.3:
                    cf = -rng.uniform(0.1, 0.5)
                elif 17 < hour < 21:
                    cf = rng.uniform(0.4, 0.9)
                else:
                    cf = rng.uniform(-0.1, 0.2)
            else:
                cf = rng.uniform(0.1, 0.3)

            cf = max(-1, min(1, cf + drift * rng.uniform(0.5, 1.5)))
            output_mw = round(capacity * cf, 1)
            gen_rows.append({
                "interval_datetime": now_local,
                "region_id": region,
                "fuel_type": fuel,
                "total_mw": output_mw,
                "unit_count": max(1, int(capacity / rng.uniform(200, 600))),
                "capacity_factor": cf,
                "emissions_tco2e": max(0, output_mw) * EMISSIONS.get(fuel, 0) / 12,
                "emissions_intensity": EMISSIONS.get(fuel, 0),
                "_updated_at": now_utc,
            })
            total_mw += max(0, output_mw)
        region_totals[region] = total_mw

    # ── PRICES ──
    price_rows = []
    region_prices = {}
    for region in REGIONS:
        demand = BASE_DEMAND[region] * diurnal * rng.uniform(0.95, 1.05)
        avail = region_totals[region] * rng.uniform(1.05, 1.25)
        rrp = round(_price(demand, avail, BASE_PRICES[region], rng), 2)
        region_prices[region] = rrp
        price_rows.append({
            "interval_datetime": now_local,
            "region_id": region,
            "rrp": rrp,
            "rop": round(rrp * rng.uniform(0.98, 1.02), 2),
            "total_demand_mw": round(demand, 1),
            "available_gen_mw": round(avail, 1),
            "net_interchange_mw": round(rng.uniform(-400, 400), 1),
            "intervention": False,
            "apc_flag": rrp > 15000,
            "market_suspended": False,
            "_updated_at": now_utc,
        })

    # ── INTERCONNECTORS ──
    ic_rows = []
    for ic_id, from_r, to_r, cap_fwd, cap_rev in INTERCONNECTORS:
        pdiff = region_prices.get(to_r, 80) - region_prices.get(from_r, 80)
        flow = min(cap_fwd, abs(pdiff) * rng.uniform(1, 5)) if pdiff > 0 else -min(cap_rev, abs(pdiff) * rng.uniform(1, 5))
        flow = round(flow * rng.uniform(0.7, 1.0), 1)
        limit = cap_fwd if flow >= 0 else cap_rev
        util = round(abs(flow) / max(limit, 1) * 100, 1)
        ic_rows.append({
            "interval_datetime": now_local,
            "interconnector_id": ic_id,
            "from_region": from_r,
            "to_region": to_r,
            "mw_flow": flow,
            "mw_losses": round(abs(flow) * rng.uniform(0.02, 0.05), 1),
            "export_limit_mw": float(cap_fwd),
            "import_limit_mw": float(cap_rev),
            "utilization_pct": util,
            "is_congested": util > 90,
            "marginal_value": round(rng.uniform(5, 50), 2) if util > 90 else 0.0,
            "_updated_at": now_utc,
        })

    # ── DEMAND ACTUALS ──
    demand_rows = []
    current_demand = {}
    for region in REGIONS:
        total_d = BASE_DEMAND[region] * diurnal * rng.uniform(0.95, 1.05)
        solar_rt = region_totals.get(region, 0) * solar * rng.uniform(0.03, 0.08)
        wb = WEATHER_BASE[region]
        temp = wb["temp"] + (hour - 14) * rng.uniform(-0.5, 0) + rng.uniform(-3, 3)
        current_demand[region] = round(total_d, 1)
        demand_rows.append({
            "interval_datetime": now_local,
            "region_id": region,
            "total_demand_mw": round(total_d, 1),
            "scheduled_demand_mw": round(total_d * rng.uniform(0.92, 0.98), 1),
            "net_demand_mw": round(total_d - solar_rt, 1),
            "solar_rooftop_mw": round(solar_rt, 1),
            "temperature_c": round(temp, 1),
            "is_peak": (7 <= now_aest.hour <= 9) or (17 <= now_aest.hour <= 20),
            "_updated_at": now_utc,
        })

    # ── WEATHER ──
    weather_rows = []
    for region in REGIONS:
        wb = WEATHER_BASE[region]
        temp = wb["temp"] + (hour - 14) * rng.uniform(-0.5, 0) + rng.uniform(-3, 3)
        weather_rows.append({
            "forecast_datetime": now_local,
            "api_call_datetime": now_utc,
            "nem_region": region,
            "is_historical": True,
            "temperature_c": round(temp, 1),
            "apparent_temp_c": round(temp + rng.uniform(-2, 4), 1),
            "max_temp_c": round(temp + rng.uniform(2, 6), 1),
            "wind_speed_100m_kmh": round(wb["wind"] * wind_f * rng.uniform(0.7, 1.3), 1),
            "solar_radiation_wm2": round(wb["solar"] * solar * rng.uniform(0.8, 1.1), 1),
            "cloud_cover_pct": round(min(100, wb["cloud"] * rng.uniform(0.5, 1.5)), 1),
            "heating_degree_days": round(max(0, 18 - temp), 2),
            "cooling_degree_days": round(max(0, temp - 18), 2),
            "_updated_at": now_utc,
        })

    # ── ANOMALY EVENTS ──
    anomaly_rows = []
    for region in REGIONS:
        rrp = region_prices.get(region, 80)
        if rrp > 300:
            anomaly_rows.append({
                "event_id": str(uuid.uuid4()),
                "detected_at": now_utc,
                "interval_datetime": now_local,
                "region_id": region,
                "event_type": "PRICE_SPIKE",
                "severity": "HIGH" if rrp > 1000 else "MEDIUM",
                "metric_value": rrp,
                "threshold_value": 300.0,
                "description": f"Price spike in {region}: ${rrp:.2f}/MWh exceeds $300 threshold",
                "is_resolved": False,
                "resolved_at": None,
                "alert_fired": True,
                "_created_at": now_utc,
            })
        elif rrp < 0:
            anomaly_rows.append({
                "event_id": str(uuid.uuid4()),
                "detected_at": now_utc,
                "interval_datetime": now_local,
                "region_id": region,
                "event_type": "PRICE_NEGATIVE",
                "severity": "LOW",
                "metric_value": rrp,
                "threshold_value": 0.0,
                "description": f"Negative price in {region}: ${rrp:.2f}/MWh - excess renewable supply",
                "is_resolved": False,
                "resolved_at": None,
                "alert_fired": False,
                "_created_at": now_utc,
            })

    # ── PRICE FORECASTS (every 6th interval = every 30 min) ──
    pf_rows = []
    interval_index = int((now_aest.hour * 60 + now_aest.minute) / 5)
    if interval_index % 6 == 0:
        for region in REGIONS:
            base = region_prices.get(region, 80)
            for ahead in [12, 24, 48]:
                future = now_aest + timedelta(minutes=ahead * 5)
                fh = future.hour + future.minute / 60
                shift = _diurnal(fh) / max(_diurnal(hour), 0.1)
                predicted = round(base * shift * rng.uniform(0.85, 1.15), 2)
                spread = abs(predicted) * rng.uniform(0.1, 0.3)
                pf_rows.append({
                    "forecast_run_at": now_utc,
                    "interval_datetime": future.replace(tzinfo=None),
                    "region_id": region,
                    "horizon_intervals": ahead,
                    "predicted_rrp": predicted,
                    "prediction_lower_80": round(predicted - spread, 2),
                    "prediction_upper_80": round(predicted + spread, 2),
                    "spike_probability": round(min(1, max(0, (predicted - 200) / 500)), 3),
                    "model_version": "1.2.0",
                    "model_name": f"price_forecast_{region.lower()}",
                    "_updated_at": now_utc,
                })

    # ── DEMAND FORECASTS (every 6th interval = every 30 min) ──
    df_rows = []
    if interval_index % 6 == 0:
        for region in REGIONS:
            base = current_demand.get(region, BASE_DEMAND[region])
            for ahead in [12, 24, 48]:
                future = now_aest + timedelta(minutes=ahead * 5)
                fh = future.hour + future.minute / 60
                shift = _diurnal(fh) / max(_diurnal(hour), 0.1)
                predicted = round(base * shift * rng.uniform(0.95, 1.05), 1)
                spread = predicted * rng.uniform(0.03, 0.08)
                df_rows.append({
                    "forecast_run_at": now_utc,
                    "interval_datetime": future.replace(tzinfo=None),
                    "region_id": region,
                    "horizon_intervals": ahead,
                    "predicted_demand_mw": predicted,
                    "prediction_lower_80": round(predicted - spread, 1),
                    "prediction_upper_80": round(predicted + spread, 1),
                    "model_version": "1.1.0",
                    "model_name": f"demand_forecast_{region.lower()}",
                    "_updated_at": now_utc,
                })

    return {
        "gen": gen_rows,
        "prices": price_rows,
        "ic": ic_rows,
        "demand": demand_rows,
        "weather": weather_rows,
        "anomaly": anomaly_rows,
        "price_forecast": pf_rows,
        "demand_forecast": df_rows,
        "region_prices": region_prices,
        "current_demand": current_demand,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Summary Generator

# COMMAND ----------

def build_daily_summary(day_date, all_prices, all_demand, all_gen, rng):
    """Build nem_daily_summary rows from accumulated daily data."""
    now_utc = datetime.utcnow()
    rows = []
    for region in REGIONS:
        rp = [p["rrp"] for p in all_prices if p["region_id"] == region]
        rd = [d["total_demand_mw"] for d in all_demand if d["region_id"] == region]
        rg = [g for g in all_gen if g["region_id"] == region]

        if not rp:
            continue

        # Renewable vs total generation
        renewable_fuels = {"wind", "solar_utility", "hydro", "battery"}
        renewable_mw = sum(g["total_mw"] for g in rg if g["fuel_type"] in renewable_fuels and g["total_mw"] > 0)
        total_gen_mw = sum(g["total_mw"] for g in rg if g["total_mw"] > 0)
        total_emissions = sum(g["emissions_tco2e"] for g in rg)

        rows.append({
            "trading_date": day_date.strftime("%Y-%m-%d"),
            "region_id": region,
            "avg_price_aud_mwh": round(sum(rp) / len(rp), 2),
            "min_price_aud_mwh": round(min(rp), 2),
            "max_price_aud_mwh": round(max(rp), 2),
            "price_spike_count": sum(1 for p in rp if p > 300),
            "price_negative_count": sum(1 for p in rp if p < 0),
            "avg_demand_mw": round(sum(rd) / len(rd), 1),
            "peak_demand_mw": round(max(rd), 1),
            "total_energy_gwh": round(total_gen_mw * 5 / 60 / 1000, 3),
            "renewables_pct": round(renewable_mw / max(total_gen_mw, 1) * 100, 1),
            "avg_emissions_intensity": round(total_emissions / max(total_gen_mw * 5 / 60, 1), 4),
            "_updated_at": now_utc,
        })
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Backfill Loop — One Day at a Time

# COMMAND ----------

rng = random.Random(42)
now_aest = datetime.now(AEST)
total_intervals = 0

# Column definitions for each table
GEN_COLS = ["interval_datetime", "region_id", "fuel_type", "total_mw", "unit_count",
            "capacity_factor", "emissions_tco2e", "emissions_intensity", "_updated_at"]
PRICE_COLS = ["interval_datetime", "region_id", "rrp", "rop", "total_demand_mw",
              "available_gen_mw", "net_interchange_mw", "intervention", "apc_flag",
              "market_suspended", "_updated_at"]
IC_COLS = ["interval_datetime", "interconnector_id", "from_region", "to_region",
           "mw_flow", "mw_losses", "export_limit_mw", "import_limit_mw",
           "utilization_pct", "is_congested", "marginal_value", "_updated_at"]
DEMAND_COLS = ["interval_datetime", "region_id", "total_demand_mw", "scheduled_demand_mw",
               "net_demand_mw", "solar_rooftop_mw", "temperature_c", "is_peak", "_updated_at"]
WEATHER_COLS = ["forecast_datetime", "api_call_datetime", "nem_region", "is_historical",
                "temperature_c", "apparent_temp_c", "max_temp_c", "wind_speed_100m_kmh",
                "solar_radiation_wm2", "cloud_cover_pct", "heating_degree_days",
                "cooling_degree_days", "_updated_at"]
ANOMALY_COLS = ["event_id", "detected_at", "interval_datetime", "region_id",
                "event_type", "severity", "metric_value", "threshold_value",
                "description", "is_resolved", "resolved_at", "alert_fired", "_created_at"]
PF_COLS = ["forecast_run_at", "interval_datetime", "region_id", "horizon_intervals",
           "predicted_rrp", "prediction_lower_80", "prediction_upper_80",
           "spike_probability", "model_version", "model_name", "_updated_at"]
DF_COLS = ["forecast_run_at", "interval_datetime", "region_id", "horizon_intervals",
           "predicted_demand_mw", "prediction_lower_80", "prediction_upper_80",
           "model_version", "model_name", "_updated_at"]
SUMMARY_COLS = ["trading_date", "region_id", "avg_price_aud_mwh", "min_price_aud_mwh",
                "max_price_aud_mwh", "price_spike_count", "price_negative_count",
                "avg_demand_mw", "peak_demand_mw", "total_energy_gwh",
                "renewables_pct", "avg_emissions_intensity", "_updated_at"]

print(f"Backfilling {DAYS_BACK} days × {INTERVALS_PER_DAY} intervals = {DAYS_BACK * INTERVALS_PER_DAY} total intervals")
print(f"Start date: {(now_aest - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%d')}")
print(f"End date:   {(now_aest - timedelta(days=1)).strftime('%Y-%m-%d')}")
print("=" * 60)

for day_offset in range(DAYS_BACK, 0, -1):
    day_start = (now_aest - timedelta(days=day_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
    day_str = day_start.strftime("%Y-%m-%d")

    # Accumulators for the day
    day_gen = []
    day_prices = []
    day_ic = []
    day_demand = []
    day_weather = []
    day_anomaly = []
    day_pf = []
    day_df = []

    # Generate all 288 intervals for this day
    for interval_idx in range(INTERVALS_PER_DAY):
        t = day_start + timedelta(minutes=interval_idx * 5)
        result = generate_interval(t, rng)
        day_gen.extend(result["gen"])
        day_prices.extend(result["prices"])
        day_ic.extend(result["ic"])
        day_demand.extend(result["demand"])
        day_weather.extend(result["weather"])
        day_anomaly.extend(result["anomaly"])
        day_pf.extend(result["price_forecast"])
        day_df.extend(result["demand_forecast"])

    # Write accumulated rows in chunks
    _sql_insert("gold.nem_generation_by_fuel", GEN_COLS, day_gen)
    _sql_insert("gold.nem_prices_5min", PRICE_COLS, day_prices)
    _sql_insert("gold.nem_interconnectors", IC_COLS, day_ic)
    _sql_insert("gold.demand_actuals", DEMAND_COLS, day_demand)
    _sql_insert("gold.weather_nem_regions", WEATHER_COLS, day_weather)
    if day_anomaly:
        _sql_insert("gold.anomaly_events", ANOMALY_COLS, day_anomaly)
    if day_pf:
        _sql_insert("gold.price_forecasts", PF_COLS, day_pf)
    if day_df:
        _sql_insert("gold.demand_forecasts", DF_COLS, day_df)

    # Daily summary
    summary_rows = build_daily_summary(day_start, day_prices, day_demand, day_gen, rng)
    _sql_insert("gold.nem_daily_summary", SUMMARY_COLS, summary_rows)

    total_intervals += INTERVALS_PER_DAY
    spikes = sum(1 for p in day_prices if p["rrp"] > 300)
    negatives = sum(1 for p in day_prices if p["rrp"] < 0)
    print(f"[{day_str}] Done — {len(day_prices)} price rows, {len(day_gen)} gen rows, {spikes} spikes, {negatives} negatives | {total_intervals}/{DAYS_BACK * INTERVALS_PER_DAY} intervals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print(f"BACKFILL COMPLETE: {total_intervals} intervals across {DAYS_BACK} days")
print("=" * 60)

# Verify row counts
tables = [
    "gold.nem_prices_5min",
    "gold.nem_generation_by_fuel",
    "gold.nem_interconnectors",
    "gold.demand_actuals",
    "gold.weather_nem_regions",
    "gold.anomaly_events",
    "gold.price_forecasts",
    "gold.demand_forecasts",
    "gold.nem_daily_summary",
]
for t in tables:
    cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{t}").collect()[0]["cnt"]
    print(f"  {t}: {cnt:,} rows")
