# Databricks notebook source
# ============================================================
# Phase 3 — Seed Data for All 21 Tables
# ============================================================

# COMMAND ----------

catalog = "energy_copilot_catalog"
import random
import uuid
from datetime import date, datetime, timedelta

random.seed(42)

def _uid():
    return str(uuid.uuid4())

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

# Truncate all tables to make this idempotent
_tables = [
    "bids_submitted", "bid_optimization_results", "dispatch_conformance", "revenue_attribution",
    "var_historical", "var_monte_carlo", "vol_surface", "stress_test_library",
    "battery_assets", "battery_dispatch_schedule", "battery_performance",
    "gas_sttm_prices", "gas_dwgm_prices", "gas_spark_spreads",
    "wem_balancing_prices", "wem_generation", "wem_demand",
    "compliance_obligations", "environmental_portfolio", "certificate_balances", "generated_reports",
]
for _t in _tables:
    try:
        spark.sql(f"TRUNCATE TABLE {catalog}.gold.{_t}")
    except Exception:
        pass
print("Truncated all Phase 3 tables")

# COMMAND ----------

# =========================================================================
# WS1: AI Bidding — 5 generators, 50 bids, 30 conformance, 35 revenue, 10 opt results
# =========================================================================

generators = [
    ("BAYSW1", "Bayswater", "NSW1", "coal_black"),
    ("ERGT01", "Eraring GT", "NSW1", "gas_ccgt"),
    ("HDWF1", "Hallet Wind Farm", "SA1", "wind"),
    ("LKBNL1", "Lake Bonney", "SA1", "wind"),
    ("CALL_A_1", "Callide A", "QLD1", "coal_black"),
]

# -- bids_submitted (50 bids) --
bid_rows = []
base_dt = datetime(2026, 3, 1)
for i in range(50):
    gen_id, gen_name, region, fuel = generators[i % 5]
    dt = base_dt + timedelta(hours=i * 4)
    total = random.uniform(200, 800)
    bands = []
    remaining = total
    for b in range(1, 11):
        if b < 10:
            mw = round(remaining * random.uniform(0.05, 0.2), 1)
        else:
            mw = round(remaining, 1)
        remaining -= mw
        remaining = max(0, remaining)
        price = round(-1000 + (b - 1) * random.uniform(50, 500), 2)
        bands.append((price, mw))
    band_sql = ", ".join(f"{p}, {m}" for p, m in bands)
    bid_rows.append(
        f"('{_uid()}', '{gen_id}', '{gen_name}', '{region}', 'ENERGY', "
        f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}', {band_sql}, {round(total, 1)}, "
        f"'ACCEPTED', 'Initial bid', '{dt.strftime('%Y-%m-%d %H:%M:%S')}')"
    )

for chunk_start in range(0, len(bid_rows), 25):
    chunk = bid_rows[chunk_start:chunk_start + 25]
    spark.sql(f"""
    INSERT INTO {catalog}.gold.bids_submitted VALUES {', '.join(chunk)}
    """)
print(f"Seeded {len(bid_rows)} bids_submitted")

# COMMAND ----------

# -- dispatch_conformance (30 events) --
conf_rows = []
statuses = ["CONFORMING", "CONFORMING", "CONFORMING", "NON_CONFORMING", "WARNING"]
for i in range(30):
    gen_id, gen_name, region, _ = generators[i % 5]
    dt = base_dt + timedelta(hours=i * 2)
    target = round(random.uniform(200, 600), 1)
    status = random.choice(statuses)
    if status == "CONFORMING":
        actual = round(target + random.uniform(-5, 5), 1)
    elif status == "WARNING":
        actual = round(target + random.uniform(-20, 20), 1)
    else:
        actual = round(target + random.uniform(-80, 80), 1)
    dev = round(actual - target, 1)
    dev_pct = round(dev / max(target, 1) * 100, 2)
    conf_rows.append(
        f"('{_uid()}', '{gen_id}', '{gen_name}', '{region}', "
        f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}', {target}, {actual}, {dev}, {dev_pct}, "
        f"'{status}', {str(status == 'NON_CONFORMING').lower()}, "
        f"'{status} dispatch')"
    )

spark.sql(f"""
INSERT INTO {catalog}.gold.dispatch_conformance VALUES {', '.join(conf_rows)}
""")
print(f"Seeded {len(conf_rows)} dispatch_conformance")

# COMMAND ----------

# -- revenue_attribution (35 records) --
rev_rows = []
for i in range(35):
    gen_id, gen_name, region, fuel = generators[i % 5]
    d = date(2026, 3, 1) + timedelta(days=i % 7)
    energy = round(random.uniform(50000, 500000), 2)
    fcas = round(random.uniform(5000, 50000), 2)
    spot = round(random.uniform(40, 200), 2)
    disp = round(random.uniform(200, 600), 1)
    cf = round(random.uniform(0.3, 0.95), 3)
    rev_rows.append(
        f"('{_uid()}', '{gen_id}', '{gen_name}', '{region}', '{d}', "
        f"{energy}, {fcas}, {round(energy + fcas, 2)}, {spot}, {disp}, {cf}, '{fuel}')"
    )

spark.sql(f"""
INSERT INTO {catalog}.gold.revenue_attribution VALUES {', '.join(rev_rows)}
""")
print(f"Seeded {len(rev_rows)} revenue_attribution")

# COMMAND ----------

# -- bid_optimization_results (10 results) --
opt_rows = []
strategies = ["PRICE_TAKER", "PRICE_MAKER", "PORTFOLIO_OPT", "MARGINAL_COST", "ML_OPTIMIZED"]
for i in range(10):
    gen_id = generators[i % 5][0]
    region = generators[i % 5][2]
    expected = round(random.uniform(100000, 500000), 2)
    actual = round(expected * random.uniform(0.85, 1.05), 2)
    optimal = round(expected * random.uniform(1.05, 1.25), 2)
    uplift = round((optimal - actual) / max(actual, 1) * 100, 2)
    opt_rows.append(
        f"('{_uid()}', '{_uid()}', '{gen_id}', '{region}', '{strategies[i % 5]}', "
        f"{expected}, {actual}, {optimal}, {uplift}, "
        f"'Optimized band allocation', '{datetime(2026, 3, 8).strftime('%Y-%m-%d %H:%M:%S')}')"
    )

spark.sql(f"""
INSERT INTO {catalog}.gold.bid_optimization_results VALUES {', '.join(opt_rows)}
""")
print(f"Seeded {len(opt_rows)} bid_optimization_results")

# COMMAND ----------

# =========================================================================
# WS2: Advanced Risk — stress scenarios, vol surface, VaR seeds
# =========================================================================

# -- stress_test_library (20 scenarios) --
scenarios = [
    ("sa_summer_heatwave", "SA Summer Heatwave", "WEATHER", "Extreme heat drives SA prices to $5000+", 300, 15, -10, -5, 20, "EXTREME"),
    ("wind_drought_national", "National Wind Drought", "WEATHER", "Wind drops below 5% nationally for 48h", 80, 5, -80, 0, 10, "HIGH"),
    ("coal_trip_2gw", "2GW Coal Trip NSW", "SUPPLY", "Sudden loss of 2GW coal in NSW", 150, 0, 0, 0, 0, "HIGH"),
    ("qni_failure", "QNI Interconnector Failure", "NETWORK", "QNI trips, isolating QLD", 60, 0, 0, 0, 0, "HIGH"),
    ("gas_price_spike", "Gas Price Spike", "COMMODITY", "East coast gas doubles to $20/GJ", 40, 0, 0, 0, 100, "MEDIUM"),
    ("solar_eclipse", "Solar Eclipse Event", "WEATHER", "Partial solar eclipse reduces output", 30, 5, 0, -60, 0, "MEDIUM"),
    ("bass_link_outage", "Basslink Cable Failure", "NETWORK", "Basslink trips, TAS isolated", 50, 0, 0, 0, 0, "HIGH"),
    ("demand_surge_vic", "VIC Demand Surge", "DEMAND", "VIC cold snap pushes demand to record", 80, 25, -5, 0, 15, "HIGH"),
    ("renewable_flood", "Renewable Oversupply", "SUPPLY", "Record wind+solar pushes prices negative", -50, -5, 50, 50, 0, "MEDIUM"),
    ("carbon_policy_shock", "Carbon Price Introduction", "POLICY", "New $50/tCO2 carbon price announced", 30, 0, 0, 0, 40, "MEDIUM"),
    ("lng_export_cut", "LNG Export Restriction", "POLICY", "Gas reservation policy cuts spot gas", -10, 0, 0, 0, -30, "LOW"),
    ("bushfire_transmission", "Bushfire Transmission Loss", "WEATHER", "Bushfire damages VIC-SA interconnector", 70, 5, 0, -10, 0, "HIGH"),
    ("battery_degradation", "Fleet Battery Degradation", "TECHNICAL", "Accelerated degradation reduces storage", 15, 0, 0, 0, 0, "LOW"),
    ("cyber_attack_scada", "SCADA Cyber Attack", "SECURITY", "Coordinated cyber attack on SCADA", 200, 10, -30, -20, 0, "EXTREME"),
    ("rate_hike_demand", "Interest Rate Shock", "ECONOMIC", "RBA rate hike slows industrial demand", -15, -10, 0, 0, -5, "LOW"),
    ("hydrogen_demand", "Hydrogen Electrolyser Load", "EMERGING", "500MW hydrogen load comes online", 20, 8, 0, 0, 5, "MEDIUM"),
    ("coal_closure_early", "Early Coal Closure", "SUPPLY", "1GW coal closes 2 years early", 40, 0, 0, 0, 10, "MEDIUM"),
    ("vic_min_demand", "VIC Minimum Demand", "DEMAND", "VIC spring min demand causes stability issue", -30, -15, 30, 40, 0, "MEDIUM"),
    ("transmission_upgrade", "VNI West Commissioning", "NETWORK", "VNI West adds 1.8GW capacity", -20, 0, 5, 0, -5, "LOW"),
    ("la_nina_wet", "La Nina Wet Season", "WEATHER", "Above-avg rainfall boosts hydro, reduces demand", -25, -8, 10, -15, -10, "MEDIUM"),
]

scen_rows = []
for s in scenarios:
    desc_escaped = s[3].replace("'", "''")
    scen_rows.append(
        f"('{_uid()}', '{s[1]}', '{s[2]}', '{desc_escaped}', "
        f"{s[4]}, {s[5]}, {s[6]}, {s[7]}, {s[8]}, '{s[9]}', true)"
    )

spark.sql(f"""
INSERT INTO {catalog}.gold.stress_test_library VALUES {', '.join(scen_rows)}
""")
print(f"Seeded {len(scen_rows)} stress_test_library")

# COMMAND ----------

# -- vol_surface (30 points) --
regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
tenors = [30, 60, 90, 180, 365, 730]
vol_rows = []
for region in regions:
    for tenor in tenors:
        strike = round(random.uniform(0.8, 1.2), 2)
        base_vol = {"NSW1": 0.45, "QLD1": 0.55, "VIC1": 0.40, "SA1": 0.65, "TAS1": 0.30}[region]
        impl_vol = round(base_vol * (1 + (tenor - 90) / 500) * (1 + abs(strike - 1.0) * 0.5), 4)
        hist_vol = round(impl_vol * random.uniform(0.85, 1.15), 4)
        skew = round(random.uniform(-0.3, 0.2), 4)
        kurt = round(random.uniform(2.5, 5.0), 4)
        vol_rows.append(
            f"('{_uid()}', '{region}', '2026-03-08', {tenor}, {strike}, "
            f"{impl_vol}, {hist_vol}, {skew}, {kurt})"
        )

spark.sql(f"""
INSERT INTO {catalog}.gold.vol_surface VALUES {', '.join(vol_rows)}
""")
print(f"Seeded {len(vol_rows)} vol_surface")

# COMMAND ----------

# =========================================================================
# WS3: Battery Optimisation — 3 assets, dispatch schedules, performance
# =========================================================================

batteries = [
    ("HPR1", "Hornsdale Power Reserve", "SA1", 150, 194, 87, 150, 150, 10, 90, 0.01, True, "2017-12-01"),
    ("VBBG1", "Victorian Big Battery", "VIC1", 300, 450, 89, 300, 300, 5, 95, 0.008, True, "2021-12-15"),
    ("BCMG1", "Bouldercombe Battery", "QLD1", 200, 400, 88, 200, 200, 10, 90, 0.012, False, "2024-06-01"),
]

bat_rows = []
for b in batteries:
    bat_rows.append(
        f"('{b[0]}', '{b[1]}', '{b[2]}', {b[3]}, {b[4]}, {b[5]}, "
        f"{b[6]}, {b[7]}, {b[8]}, {b[9]}, {b[10]}, {str(b[11]).lower()}, "
        f"'{b[12]}', 'ACTIVE')"
    )

spark.sql(f"""
INSERT INTO {catalog}.gold.battery_assets VALUES {', '.join(bat_rows)}
""")
print(f"Seeded {len(bat_rows)} battery_assets")

# COMMAND ----------

# -- battery_dispatch_schedule (7 days × 48 intervals × 3 assets = ~1008 rows) --
actions = ["CHARGE", "IDLE", "DISCHARGE"]
dispatch_rows = []
for bat_id, _, region, cap_mw, _, _, _, _, _, _, _, _, _ in batteries:
    for day_offset in range(7):
        for half_hour in range(48):
            dt = datetime(2026, 3, 2) + timedelta(days=day_offset, minutes=half_hour * 30)
            hour = dt.hour
            # Simple pattern: charge overnight/midday solar, discharge evening peak
            if 1 <= hour <= 5:
                action = "CHARGE"
                power = round(cap_mw * random.uniform(0.3, 0.8), 1)
                soc = round(min(90, 20 + half_hour * 2 + random.uniform(-3, 3)), 1)
            elif 10 <= hour <= 14:
                action = "CHARGE"
                power = round(cap_mw * random.uniform(0.4, 0.9), 1)
                soc = round(min(95, 40 + (half_hour - 20) * 3 + random.uniform(-3, 3)), 1)
            elif 16 <= hour <= 21:
                action = "DISCHARGE"
                power = round(cap_mw * random.uniform(0.5, 1.0), 1)
                soc = round(max(10, 85 - (half_hour - 32) * 5 + random.uniform(-3, 3)), 1)
            else:
                action = "IDLE"
                power = 0
                soc = round(random.uniform(20, 60), 1)

            price = round(random.uniform(30, 200) if action == "DISCHARGE" else random.uniform(-10, 60), 2)
            rev = round(power * price / 2, 2) if action == "DISCHARGE" else 0
            dispatch_rows.append(
                f"('{_uid()}', '{bat_id}', '{dt.strftime('%Y-%m-%d %H:%M:%S')}', "
                f"'{action}', {power}, {soc}, {price}, {rev}, 'OPTIMIZER')"
            )

# Insert in chunks
for chunk_start in range(0, len(dispatch_rows), 50):
    chunk = dispatch_rows[chunk_start:chunk_start + 50]
    spark.sql(f"""
    INSERT INTO {catalog}.gold.battery_dispatch_schedule VALUES {', '.join(chunk)}
    """)
print(f"Seeded {len(dispatch_rows)} battery_dispatch_schedule")

# COMMAND ----------

# -- battery_performance (7 days × 3 assets = 21 records) --
perf_rows = []
for bat_id, _, _, cap_mw, stor_mwh, eff, _, _, _, _, _, _, _ in batteries:
    for day_offset in range(7):
        d = date(2026, 3, 2) + timedelta(days=day_offset)
        cycles = round(random.uniform(1.0, 2.5), 2)
        throughput = round(cycles * stor_mwh, 1)
        arb_rev = round(random.uniform(20000, 120000) * (cap_mw / 150), 2)
        fcas_rev = round(random.uniform(5000, 30000) * (cap_mw / 150), 2)
        avg_charge = round(random.uniform(20, 50), 2)
        avg_discharge = round(random.uniform(80, 250), 2)
        perf_rows.append(
            f"('{_uid()}', '{bat_id}', '{d}', {cycles}, {throughput}, "
            f"{arb_rev}, {fcas_rev}, {round(arb_rev + fcas_rev, 2)}, "
            f"{avg_charge}, {avg_discharge}, {round(eff * random.uniform(0.95, 1.02), 1)}, "
            f"{round(random.uniform(0.005, 0.02), 4)})"
        )

spark.sql(f"""
INSERT INTO {catalog}.gold.battery_performance VALUES {', '.join(perf_rows)}
""")
print(f"Seeded {len(perf_rows)} battery_performance")

# COMMAND ----------

# =========================================================================
# WS4: Gas Markets — STTM (3 hubs × 90d), DWGM (90d), Spark Spreads (90d × 5 regions)
# =========================================================================

sttm_hubs = ["Sydney", "Adelaide", "Brisbane"]
sttm_rows = []
for hub in sttm_hubs:
    base = {"Sydney": 10.5, "Adelaide": 11.2, "Brisbane": 9.8}[hub]
    for day_offset in range(90):
        d = date(2025, 12, 10) + timedelta(days=day_offset)
        ex_ante = round(base + random.uniform(-2, 4), 2)
        ex_post = round(ex_ante + random.uniform(-1, 1), 2)
        excess = round(random.uniform(-500, 500), 1)
        total = round(random.uniform(8000, 15000), 1)
        sttm_rows.append(
            f"('{_uid()}', '{hub}', '{d}', {ex_ante}, {ex_post}, {excess}, {total})"
        )

for chunk_start in range(0, len(sttm_rows), 50):
    chunk = sttm_rows[chunk_start:chunk_start + 50]
    spark.sql(f"""INSERT INTO {catalog}.gold.gas_sttm_prices VALUES {', '.join(chunk)}""")
print(f"Seeded {len(sttm_rows)} gas_sttm_prices")

# COMMAND ----------

dwgm_rows = []
for day_offset in range(90):
    d = date(2025, 12, 10) + timedelta(days=day_offset)
    for interval in range(1, 7):  # 6 trading intervals per day
        price = round(random.uniform(6, 18), 2)
        demand = round(random.uniform(500, 1500), 1)
        supply = round(demand + random.uniform(-100, 200), 1)
        linepack = round(random.uniform(200, 800), 1)
        dwgm_rows.append(
            f"('{_uid()}', '{d}', {interval}, {price}, {demand}, {supply}, {linepack})"
        )

for chunk_start in range(0, len(dwgm_rows), 50):
    chunk = dwgm_rows[chunk_start:chunk_start + 50]
    spark.sql(f"""INSERT INTO {catalog}.gold.gas_dwgm_prices VALUES {', '.join(chunk)}""")
print(f"Seeded {len(dwgm_rows)} gas_dwgm_prices")

# COMMAND ----------

spark_rows = []
for region in regions:
    heat_rate = {"NSW1": 7.5, "QLD1": 7.8, "VIC1": 7.2, "SA1": 8.0, "TAS1": 7.0}[region]
    for day_offset in range(90):
        d = date(2025, 12, 10) + timedelta(days=day_offset)
        elec_price = round(random.uniform(40, 200), 2)
        gas_price = round(random.uniform(8, 16), 2)
        spark_spread = round(elec_price - gas_price * heat_rate, 2)
        carbon = round(gas_price * 0.05 * heat_rate, 2)
        clean_spark = round(spark_spread - carbon, 2)
        spark_rows.append(
            f"('{_uid()}', '{region}', '{d}', {elec_price}, {gas_price}, "
            f"{heat_rate}, {spark_spread}, {clean_spark}, {carbon})"
        )

for chunk_start in range(0, len(spark_rows), 50):
    chunk = spark_rows[chunk_start:chunk_start + 50]
    spark.sql(f"""INSERT INTO {catalog}.gold.gas_spark_spreads VALUES {', '.join(chunk)}""")
print(f"Seeded {len(spark_rows)} gas_spark_spreads")

# COMMAND ----------

# =========================================================================
# WS5: WEM — 90 days × 48 half-hours (balancing, generation, demand)
# =========================================================================

wem_fuel_types = ["gas_ccgt", "gas_ocgt", "coal_black", "wind", "solar_utility", "battery", "distillate"]
wem_price_rows = []
wem_gen_rows = []
wem_demand_rows = []

for day_offset in range(90):
    for half_hour in range(48):
        dt = datetime(2025, 12, 10) + timedelta(days=day_offset, minutes=half_hour * 30)
        hour = dt.hour

        # Balancing price — higher in evening peak
        base_price = 60 + (20 if 16 <= hour <= 21 else 0) + random.uniform(-30, 50)
        demand_mw = round(2000 + 500 * (1 if 8 <= hour <= 21 else 0) + random.uniform(-200, 200), 1)
        gen_mw = round(demand_mw + random.uniform(-50, 100), 1)
        reserve = round(gen_mw - demand_mw, 1)
        flag = "NORMAL" if base_price < 300 else "HIGH"

        wem_price_rows.append(
            f"('{_uid()}', '{dt.strftime('%Y-%m-%d %H:%M:%S')}', "
            f"{round(base_price, 2)}, {demand_mw}, {gen_mw}, {reserve}, '{flag}')"
        )

        solar_factor = min(1, (hour - 6) / 6) if hour <= 12 else max(0, (18 - hour) / 6)
        solar_mw = round(max(0, 800 * max(0, solar_factor) + random.uniform(-50, 50)), 1)
        forecast_mw = round(demand_mw + random.uniform(-100, 100), 1)
        temp_c = round(random.uniform(15, 42), 1)
        wind_mw = round(random.uniform(100, 600), 1)
        wem_demand_rows.append(
            f"('{_uid()}', '{dt.strftime('%Y-%m-%d %H:%M:%S')}', "
            f"{demand_mw}, {forecast_mw}, {temp_c}, {solar_mw}, {wind_mw})"
        )

# Insert WEM prices in chunks
for chunk_start in range(0, len(wem_price_rows), 50):
    chunk = wem_price_rows[chunk_start:chunk_start + 50]
    spark.sql(f"""INSERT INTO {catalog}.gold.wem_balancing_prices VALUES {', '.join(chunk)}""")
print(f"Seeded {len(wem_price_rows)} wem_balancing_prices")

# COMMAND ----------

# Insert WEM demand in chunks
for chunk_start in range(0, len(wem_demand_rows), 50):
    chunk = wem_demand_rows[chunk_start:chunk_start + 50]
    spark.sql(f"""INSERT INTO {catalog}.gold.wem_demand VALUES {', '.join(chunk)}""")
print(f"Seeded {len(wem_demand_rows)} wem_demand")

# COMMAND ----------

# WEM generation by fuel type (daily aggregates to keep manageable)
wem_gen_rows = []
for day_offset in range(90):
    for half_hour in range(48):
        dt = datetime(2025, 12, 10) + timedelta(days=day_offset, minutes=half_hour * 30)
        hour = dt.hour
        for fuel in wem_fuel_types:
            if fuel == "solar_utility" and (hour < 6 or hour > 19):
                mw = 0
            elif fuel == "solar_utility":
                mw = round(random.uniform(100, 600), 1)
            elif fuel == "wind":
                mw = round(random.uniform(50, 500), 1)
            elif fuel == "coal_black":
                mw = round(random.uniform(600, 900), 1)
            elif fuel == "gas_ccgt":
                mw = round(random.uniform(200, 600), 1)
            elif fuel == "gas_ocgt":
                mw = round(random.uniform(0, 200) * (1.5 if 16 <= hour <= 21 else 1), 1)
            elif fuel == "battery":
                mw = round(random.uniform(-50, 100), 1)
            else:
                mw = round(random.uniform(0, 50), 1)
            cap = round(random.uniform(0.2, 0.9), 3)
            wem_gen_rows.append(
                f"('{_uid()}', '{dt.strftime('%Y-%m-%d %H:%M:%S')}', '{fuel}', "
                f"{mw}, {random.randint(1, 10)}, {cap})"
            )

# Insert in big chunks (lots of rows)
for chunk_start in range(0, len(wem_gen_rows), 50):
    chunk = wem_gen_rows[chunk_start:chunk_start + 50]
    spark.sql(f"""INSERT INTO {catalog}.gold.wem_generation VALUES {', '.join(chunk)}""")
print(f"Seeded {len(wem_gen_rows)} wem_generation")

# COMMAND ----------

# =========================================================================
# WS6: Compliance, Environmentals, Reports
# =========================================================================

# -- compliance_obligations (15) --
obligation_types = ["AEMO_REGISTRATION", "NER_COMPLIANCE", "AER_REPORTING", "AEMC_RULE_CHANGE", "RET_OBLIGATION"]
statuses = ["COMPLIANT", "PENDING", "AT_RISK", "OVERDUE", "COMPLIANT"]
priorities = ["HIGH", "MEDIUM", "LOW", "CRITICAL"]

comp_rows = []
for i in range(15):
    otype = obligation_types[i % 5]
    status = statuses[i % 5]
    pri = priorities[i % 4]
    due = date(2026, 3, 15) + timedelta(days=i * 10)
    comp_rows.append(
        f"('{_uid()}', '{otype}', '{otype.replace('_', ' ').title()} Obligation {i+1}', "
        f"'Compliance obligation for {otype.lower().replace('_', ' ')}', "
        f"'{due}', '{status}', '{regions[i % 5]}', 'Compliance Team', '{pri}', "
        f"'2026-03-01 00:00:00', '2026-03-08 00:00:00')"
    )

spark.sql(f"""
INSERT INTO {catalog}.gold.compliance_obligations VALUES {', '.join(comp_rows)}
""")
print(f"Seeded {len(comp_rows)} compliance_obligations")

# COMMAND ----------

# -- environmental_portfolio (50 holdings) --
cert_types = ["LGC", "ACCU", "STC"]
env_rows = []
for i in range(50):
    cert = cert_types[i % 3]
    vintage = random.randint(2023, 2026)
    qty = random.randint(100, 10000)
    price = {"LGC": round(random.uniform(35, 55), 2), "ACCU": round(random.uniform(25, 40), 2), "STC": round(random.uniform(38, 42), 2)}[cert]
    status = random.choice(["HELD", "HELD", "COMMITTED", "SURRENDERED"])
    acq = date(2025, 6, 1) + timedelta(days=random.randint(0, 300))
    exp = acq + timedelta(days=365 * random.randint(1, 3))
    env_rows.append(
        f"('{_uid()}', '{cert}', {vintage}, {qty}, {price}, {round(qty * price, 2)}, "
        f"'{status}', 'Market Purchase', '{acq}', '{exp}')"
    )

spark.sql(f"""
INSERT INTO {catalog}.gold.environmental_portfolio VALUES {', '.join(env_rows)}
""")
print(f"Seeded {len(env_rows)} environmental_portfolio")

# COMMAND ----------

# -- certificate_balances (12 — 4 types × 3 years) --
bal_rows = []
for cert in cert_types:
    for year in [2024, 2025, 2026]:
        opening = random.randint(5000, 20000)
        acquired = random.randint(2000, 15000)
        surrendered = random.randint(1000, 10000)
        closing = opening + acquired - surrendered
        liability = random.randint(5000, 18000)
        bal_rows.append(
            f"('{_uid()}', '{cert}', {year}, {opening}, {acquired}, {surrendered}, "
            f"{closing}, {liability}, {closing - liability}, '2026-03-08')"
        )

spark.sql(f"""
INSERT INTO {catalog}.gold.certificate_balances VALUES {', '.join(bal_rows)}
""")
print(f"Seeded {len(bal_rows)} certificate_balances")

# COMMAND ----------

# -- generated_reports (5 sample reports) --
report_types = [
    ("DAILY_RISK", "Daily Risk Report", "Portfolio risk summary including VaR, credit exposure, and limit breaches."),
    ("WEEKLY_MARKET", "Weekly Market Summary", "NEM spot market review, generation trends, and price analytics."),
    ("MONTHLY_COMPLIANCE", "Monthly Compliance Report", "Regulatory compliance status across all obligations."),
    ("QUARTERLY_ENVIRONMENTAL", "Quarterly Environmental Report", "Certificate holdings, liabilities, and surrender schedule."),
    ("AD_HOC_ANALYSIS", "SA Price Spike Analysis", "Root cause analysis of SA1 price spikes on 2026-03-05."),
]

rpt_rows = []
for rtype, title, summary in report_types:
    content = f"# {title}\n\nGenerated: 2026-03-08\n\n## Summary\n{summary}\n\n## Details\nDetailed analysis follows..."
    rpt_rows.append(
        f"('{_uid()}', '{rtype}', '{title}', '2026-03-08', "
        f"'{content.replace(chr(39), chr(39)+chr(39))}', '{summary.replace(chr(39), chr(39)+chr(39))}', "
        f"'system', '2026-03-08 10:00:00', 'COMPLETED', '{{}}')"
    )

spark.sql(f"""
INSERT INTO {catalog}.gold.generated_reports VALUES {', '.join(rpt_rows)}
""")
print(f"Seeded {len(rpt_rows)} generated_reports")

# COMMAND ----------

print("Phase 3 seed data complete.")
