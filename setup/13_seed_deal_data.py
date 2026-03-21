# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Deal Capture Data — Real Market Data
# MAGIC Generates realistic trades from **real NEM market data**:
# MAGIC - ASX futures settlement prices → SWAP/FUTURE prices
# MAGIC - Spot price statistics → SPOT trade pricing
# MAGIC - Real renewable facility capacities → PPA volumes
# MAGIC - Real gas hub prices → gas-indexed swaps
# MAGIC
# MAGIC Creates: 5 counterparties, 3 portfolios, ~55 trades, portfolio mappings

# COMMAND ----------

import math
import random
import uuid
from datetime import datetime

random.seed(42)

try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "energy_copilot_catalog"

SCHEMA = f"{CATALOG}.gold"

def uid():
    return str(uuid.uuid4())

NOW = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Clear Existing Data (FK order)

# COMMAND ----------

for table in ["portfolio_trades", "trade_legs", "trade_amendments", "trades", "portfolios", "counterparties"]:
    try:
        spark.sql(f"DELETE FROM {SCHEMA}.{table} WHERE 1=1")
        print(f"  Cleared {SCHEMA}.{table}")
    except Exception as e:
        print(f"  Skip {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Counterparties (real NEM participants)

# COMMAND ----------

counterparties = [
    {"id": uid(), "name": "AGL Energy",          "rating": "BBB+", "limit": 500_000_000},
    {"id": uid(), "name": "Origin Energy",        "rating": "BBB",  "limit": 400_000_000},
    {"id": uid(), "name": "EnergyAustralia",      "rating": "BBB",  "limit": 350_000_000},
    {"id": uid(), "name": "Snowy Hydro",          "rating": "AAA",  "limit": 1_000_000_000},
    {"id": uid(), "name": "Iberdrola Australia",   "rating": "BBB+", "limit": 300_000_000},
]

# Concentration weights: AGL/Origin get ~60% of trades
CP_WEIGHTS = [0.30, 0.30, 0.15, 0.15, 0.10]

for c in counterparties:
    spark.sql(f"""
        INSERT INTO {SCHEMA}.counterparties VALUES (
            '{c["id"]}', '{c["name"]}', '{c["rating"]}', {c["limit"]}, 'ACTIVE', '{NOW}'
        )
    """)
    print(f"  Created counterparty: {c['name']}")

cp_map = {c["name"]: c["id"] for c in counterparties}
print(f"Created {len(counterparties)} counterparties")

def pick_counterparty():
    return random.choices(counterparties, weights=CP_WEIGHTS, k=1)[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Portfolios

# COMMAND ----------

portfolios = [
    {"id": uid(), "name": "Trading Book",   "owner": "Energy Trading Desk", "desc": "Active trading positions and spot exposure"},
    {"id": uid(), "name": "Hedge Book",      "owner": "Risk Management",     "desc": "Customer load hedging via swaps and options"},
    {"id": uid(), "name": "Renewable PPAs",  "owner": "Origination",         "desc": "Long-term PPA and REC contracts"},
]

for p in portfolios:
    spark.sql(f"""
        INSERT INTO {SCHEMA}.portfolios VALUES (
            '{p["id"]}', '{p["name"]}', '{p["owner"]}', '{p["desc"]}', '{NOW}'
        )
    """)
    print(f"  Created portfolio: {p['name']}")

pf_map = {p["name"]: p["id"] for p in portfolios}
print(f"Created {len(portfolios)} portfolios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query Real Market Data

# COMMAND ----------

# --- Query 1: Latest ASX futures settlement prices ---
asx_rows = spark.sql(f"""
    SELECT region, quarter, year, contract_type, settlement_price
    FROM {SCHEMA}.asx_futures_eod
    WHERE trade_date = (SELECT MAX(trade_date) FROM {SCHEMA}.asx_futures_eod)
    ORDER BY region, year, quarter
""").collect()

# Build lookup: (region, quarter_label) -> settlement_price
# quarter format in table is "Q1-2026" etc.
asx_prices = {}
for r in asx_rows:
    key = (r.region, r.quarter, r.year, r.contract_type)
    asx_prices[key] = float(r.settlement_price)
    # Also store base (region, quarter, year) -> price for fallback
    base_key = (r.region, r.quarter, r.year)
    if base_key not in asx_prices:
        asx_prices[base_key] = float(r.settlement_price)

print(f"ASX futures: {len(asx_rows)} rows loaded")
# Show sample prices
for k, v in list(asx_prices.items())[:5]:
    print(f"  {k} -> ${v:.2f}/MWh")

# COMMAND ----------

# --- Query 2: Recent spot price statistics by region ---
spot_rows = spark.sql(f"""
    SELECT region_id,
        AVG(rrp) as avg_price,
        PERCENTILE_APPROX(rrp, 0.1) as p10,
        PERCENTILE_APPROX(rrp, 0.9) as p90,
        STDDEV(rrp) as vol
    FROM {SCHEMA}.nem_prices_5min
    WHERE interval_datetime >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
    GROUP BY region_id
""").collect()

spot_stats = {}
for r in spot_rows:
    spot_stats[r.region_id] = {
        "avg": float(r.avg_price),
        "p10": float(r.p10),
        "p90": float(r.p90),
        "vol": float(r.vol) if r.vol else 30.0,
    }

print(f"Spot stats: {len(spot_stats)} regions")
for region, s in spot_stats.items():
    print(f"  {region}: avg=${s['avg']:.2f}, p10=${s['p10']:.2f}, p90=${s['p90']:.2f}, vol=${s['vol']:.2f}")

# COMMAND ----------

# --- Query 3: Real renewable facility capacities ---
facility_rows = spark.sql(f"""
    SELECT duid, station_name, region_id, fuel_type, capacity_mw
    FROM {SCHEMA}.nem_facilities
    WHERE is_renewable = TRUE AND capacity_mw >= 30
    ORDER BY capacity_mw DESC
    LIMIT 30
""").collect()

renewable_facilities = [
    {"duid": r.duid, "name": r.station_name, "region": r.region_id,
     "fuel": r.fuel_type, "capacity": float(r.capacity_mw)}
    for r in facility_rows
]

print(f"Renewable facilities: {len(renewable_facilities)} (>= 30 MW)")
for f in renewable_facilities[:5]:
    print(f"  {f['name']} ({f['region']}, {f['fuel']}): {f['capacity']:.0f} MW")

# COMMAND ----------

# --- Query 4: Real gas hub prices ---
gas_rows = spark.sql(f"""
    SELECT hub, AVG(price_aud_gj) as avg_gas_price
    FROM {SCHEMA}.gas_hub_prices
    WHERE trade_date >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
    GROUP BY hub
""").collect()

gas_prices = {r.hub: float(r.avg_gas_price) for r in gas_rows}
print(f"Gas hub prices: {len(gas_prices)} hubs")
for hub, price in gas_prices.items():
    print(f"  {hub}: ${price:.2f}/GJ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Trades from Real Data

# COMMAND ----------

REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
PROFILES = ["FLAT", "PEAK", "OFF_PEAK"]

def quarter_end(year, q):
    """Return quarter end date string."""
    ends = {1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}
    return ends[q]

def quarter_start(year, q):
    """Return quarter start date string."""
    starts = {1: f"{year}-01-01", 2: f"{year}-04-01", 3: f"{year}-07-01", 4: f"{year}-10-01"}
    return starts[q]

def get_asx_price(region, quarter_num, year):
    """Look up ASX settlement price for region/quarter/year. Returns None if not found."""
    q_label = f"Q{quarter_num}-{year}"
    # Try exact match with BASE contract type first
    for ct in ["BASE", "PEAK", None]:
        if ct:
            key = (region, q_label, year, ct)
        else:
            key = (region, q_label, year)
        if key in asx_prices:
            return asx_prices[key]
    # Try alternative quarter format Q1_2026
    q_label2 = f"Q{quarter_num}_{year}"
    for ct in ["BASE", "PEAK", None]:
        if ct:
            key = (region, q_label2, year, ct)
        else:
            key = (region, q_label2, year)
        if key in asx_prices:
            return asx_prices[key]
    return None

# Fallback price if ASX data missing for a region/quarter
def fallback_price(region):
    """Use spot avg as fallback if no ASX data."""
    if region in spot_stats:
        return spot_stats[region]["avg"]
    return 75.0  # conservative default

trades = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. SWAP trades (18) — ASX-derived prices ±2%

# COMMAND ----------

for i in range(18):
    region = REGIONS[i % 5]
    q = (i % 4) + 1
    year = 2026 if i < 12 else 2027

    asx_px = get_asx_price(region, q, year)
    if asx_px is None:
        asx_px = fallback_price(region)

    # Swap price = ASX settlement ± 2% noise
    price = round(asx_px * (1 + random.uniform(-0.02, 0.02)), 2)

    cp = pick_counterparty()
    pf = portfolios[1] if i >= 10 else portfolios[0]  # first 10 Trading, rest Hedge

    # Generators SELL, retailers BUY
    buy_sell = "SELL" if cp["name"] in ("AGL Energy", "Snowy Hydro") else "BUY"

    trades.append({
        "trade_id": uid(),
        "trade_type": "SWAP",
        "region": region,
        "buy_sell": buy_sell,
        "volume_mw": random.choice([25, 50, 75, 100, 150]),
        "price": price,
        "start_date": quarter_start(year, q),
        "end_date": quarter_end(year, q),
        "profile": random.choice(PROFILES),
        "status": random.choices(["CONFIRMED", "DRAFT", "SETTLED"], weights=[0.7, 0.2, 0.1])[0],
        "counterparty_id": cp["id"],
        "portfolio_id": pf["id"],
        "notes": f"Q{q} {year} {region} swap @ ASX ${asx_px:.2f}",
        "created_by": "seed_v2",
    })

print(f"SWAP trades: {len([t for t in trades if t['trade_type'] == 'SWAP'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. FUTURE trades (10) — Exact ASX settlement prices

# COMMAND ----------

for i in range(10):
    region = REGIONS[i % 5]
    q = random.choice([2, 3, 4])
    year = 2026

    asx_px = get_asx_price(region, q, year)
    if asx_px is None:
        asx_px = fallback_price(region)

    # Futures use exact ASX settlement price
    price = round(asx_px, 2)

    cp = pick_counterparty()

    trades.append({
        "trade_id": uid(),
        "trade_type": "FUTURE",
        "region": region,
        "buy_sell": random.choice(["BUY", "SELL"]),
        "volume_mw": random.choice([10, 25, 50]),  # exchange lot sizes
        "price": price,
        "start_date": quarter_start(year, q),
        "end_date": quarter_end(year, q),
        "profile": "FLAT",
        "status": "CONFIRMED",
        "counterparty_id": cp["id"],
        "portfolio_id": pf_map["Trading Book"],
        "notes": f"ASX Energy future Q{q} {year} {region}",
        "created_by": "seed_v2",
    })

print(f"FUTURE trades: {len([t for t in trades if t['trade_type'] == 'FUTURE'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. PPA trades (8) — Real facility capacities, renewable discount

# COMMAND ----------

# Pick 8 renewable facilities for PPAs
ppa_facilities = random.sample(renewable_facilities, min(8, len(renewable_facilities)))

for i, fac in enumerate(ppa_facilities):
    region = fac["region"]

    # PPA price = 60-75% of ASX base (renewable discount)
    asx_px = get_asx_price(region, 1, 2026) or get_asx_price(region, 2, 2026) or fallback_price(region)
    discount = random.uniform(0.60, 0.75)
    price = round(asx_px * discount, 2)

    # PPA tenor: 2-3 years
    end_year = random.choice([2028, 2029])

    cp = pick_counterparty()

    trades.append({
        "trade_id": uid(),
        "trade_type": "PPA",
        "region": region,
        "buy_sell": "BUY",  # buying renewable generation
        "volume_mw": round(fac["capacity"] * random.uniform(0.7, 1.0), 1),  # near full capacity
        "price": price,
        "start_date": "2026-01-01",
        "end_date": f"{end_year}-12-31",
        "profile": "FLAT",
        "status": "CONFIRMED",
        "counterparty_id": cp["id"],
        "portfolio_id": pf_map["Renewable PPAs"],
        "notes": f"PPA {fac['name']} ({fac['fuel']}, {fac['capacity']:.0f}MW) @ {discount*100:.0f}% ASX",
        "created_by": "seed_v2",
    })

print(f"PPA trades: {len([t for t in trades if t['trade_type'] == 'PPA'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4d. OPTION trades (6) — Black-76 approximation for premium

# COMMAND ----------

for i in range(6):
    region = REGIONS[i % 5]
    q = random.choice([3, 4])
    year = 2026

    asx_px = get_asx_price(region, q, year) or fallback_price(region)

    # Annualized vol from spot stats
    region_vol = spot_stats.get(region, {}).get("vol", 30.0)
    # Normalize vol to fraction of price
    vol_frac = min(region_vol / asx_px, 1.5) if asx_px > 0 else 0.5

    # Time to expiry in years (approx months/12)
    months_to_q = max(1, (q - 1) * 3 + 1 - 3)  # months from Mar 2026
    T = months_to_q / 12.0

    # Simplified Black-76: premium ≈ F * σ * √T * 0.4
    premium = max(2.0, round(asx_px * vol_frac * math.sqrt(T) * 0.4, 2))

    cp = pick_counterparty()

    trades.append({
        "trade_id": uid(),
        "trade_type": "OPTION",
        "region": region,
        "buy_sell": "BUY",  # buying cap protection
        "volume_mw": random.choice([25, 50, 75, 100]),
        "price": premium,
        "start_date": quarter_start(year, q),
        "end_date": quarter_end(year, q),
        "profile": "PEAK",
        "status": "CONFIRMED",
        "counterparty_id": cp["id"],
        "portfolio_id": pf_map["Hedge Book"],
        "notes": f"$300 cap {region} Q{q} {year}, fwd=${asx_px:.2f}, vol={region_vol:.1f}",
        "created_by": "seed_v2",
    })

print(f"OPTION trades: {len([t for t in trades if t['trade_type'] == 'OPTION'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4e. SPOT trades (8) — Real spot price statistics

# COMMAND ----------

# 5 regions + 3 extra for larger regions
spot_regions = REGIONS + random.sample(["NSW1", "QLD1", "VIC1"], 3)

for i, region in enumerate(spot_regions):
    stats = spot_stats.get(region, {"avg": 75, "p10": 40, "p90": 150})

    # Spot price from real distribution
    price = round(stats["avg"] + random.uniform(stats["p10"] - stats["avg"], stats["p90"] - stats["avg"]), 2)

    trades.append({
        "trade_id": uid(),
        "trade_type": "SPOT",
        "region": region,
        "buy_sell": "SELL",  # generator pool exposure
        "volume_mw": random.choice([100, 200, 300, 500]),
        "price": price,
        "start_date": "2026-03-01",
        "end_date": "2026-03-31",
        "profile": "FLAT",
        "status": random.choice(["CONFIRMED", "SETTLED"]),
        "counterparty_id": "",
        "portfolio_id": pf_map["Trading Book"],
        "notes": f"Spot pool exposure {region} (avg=${stats['avg']:.2f})",
        "created_by": "seed_v2",
    })

print(f"SPOT trades: {len([t for t in trades if t['trade_type'] == 'SPOT'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4f. REC trades (3) — Real LGC spot range

# COMMAND ----------

for i in range(3):
    region = random.choice(REGIONS[:4])  # no TAS for RECs

    # Real LGC spot range: $38-$48/MWh
    price = round(random.uniform(38, 48), 2)

    cp = pick_counterparty()

    trades.append({
        "trade_id": uid(),
        "trade_type": "REC",
        "region": region,
        "buy_sell": random.choice(["BUY", "SELL"]),
        "volume_mw": random.choice([10, 20, 30, 50]),
        "price": price,
        "start_date": "2026-01-01",
        "end_date": "2026-12-31",
        "profile": "FLAT",
        "status": random.choice(["CONFIRMED", "DRAFT"]),
        "counterparty_id": cp["id"],
        "portfolio_id": pf_map["Renewable PPAs"],
        "notes": f"LGC certificates {region} @ ${price:.2f}",
        "created_by": "seed_v2",
    })

print(f"REC trades: {len([t for t in trades if t['trade_type'] == 'REC'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4g. Gas-indexed SWAP trades (2) — Real gas hub prices × heat rate

# COMMAND ----------

HEAT_RATE = 10.0  # GJ/MWh (typical CCGT)

gas_hubs = list(gas_prices.keys())
if gas_hubs:
    for i in range(2):
        hub = random.choice(gas_hubs)
        gas_px = gas_prices[hub]

        # Gas-indexed price = gas $/GJ × heat rate GJ/MWh
        implied_elec = round(gas_px * HEAT_RATE * (1 + random.uniform(-0.05, 0.05)), 2)

        region = random.choice(["NSW1", "QLD1", "VIC1", "SA1"])
        cp = pick_counterparty()

        trades.append({
            "trade_id": uid(),
            "trade_type": "SWAP",
            "region": region,
            "buy_sell": random.choice(["BUY", "SELL"]),
            "volume_mw": random.choice([50, 75, 100]),
            "price": implied_elec,
            "start_date": "2026-04-01",
            "end_date": "2026-12-31",
            "profile": "FLAT",
            "status": "CONFIRMED",
            "counterparty_id": cp["id"],
            "portfolio_id": pf_map["Trading Book"],
            "notes": f"Gas-indexed swap ({hub} ${gas_px:.2f}/GJ × {HEAT_RATE} HR)",
            "created_by": "seed_v2",
        })
    print("Gas-indexed SWAP trades: 2")
else:
    print("No gas hub data available — skipping gas swaps")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

type_counts = {}
for t in trades:
    type_counts[t["trade_type"]] = type_counts.get(t["trade_type"], 0) + 1

print(f"\nTotal trades prepared: {len(trades)}")
for tt, cnt in sorted(type_counts.items()):
    print(f"  {tt}: {cnt}")

status_counts = {}
for t in trades:
    status_counts[t["status"]] = status_counts.get(t["status"], 0) + 1
print("\nStatus distribution:")
for st, cnt in sorted(status_counts.items()):
    print(f"  {st}: {cnt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Insert Trades + Portfolio Mappings

# COMMAND ----------

for t in trades:
    trade_id = t["trade_id"]
    # Escape single quotes in notes
    notes = t["notes"].replace("'", "''")
    spark.sql(f"""
        INSERT INTO {SCHEMA}.trades VALUES (
            '{trade_id}',
            '{t["trade_type"]}',
            '{t["region"]}',
            '{t["buy_sell"]}',
            {t["volume_mw"]},
            {t["price"]},
            '{t["start_date"]}',
            '{t["end_date"]}',
            '{t["profile"]}',
            '{t["status"]}',
            '{t["counterparty_id"]}',
            '{t["portfolio_id"]}',
            '{notes}',
            '{t["created_by"]}',
            '{NOW}',
            '{NOW}'
        )
    """)
    # Portfolio mapping
    if t["portfolio_id"]:
        spark.sql(f"""
            INSERT INTO {SCHEMA}.portfolio_trades VALUES (
                '{t["portfolio_id"]}', '{trade_id}'
            )
        """)

print(f"Inserted {len(trades)} trades + portfolio mappings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify

# COMMAND ----------

for table in ["counterparties", "portfolios", "trades", "portfolio_trades"]:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {SCHEMA}.{table}").collect()[0].cnt
    print(f"  {SCHEMA}.{table}: {cnt} rows")

# Show price ranges by trade type
print("\nPrice ranges by trade type:")
price_df = spark.sql(f"""
    SELECT trade_type,
           COUNT(*) as cnt,
           ROUND(MIN(price), 2) as min_price,
           ROUND(AVG(price), 2) as avg_price,
           ROUND(MAX(price), 2) as max_price,
           ROUND(SUM(volume_mw), 0) as total_mw
    FROM {SCHEMA}.trades
    GROUP BY trade_type
    ORDER BY trade_type
""")
price_df.show(truncate=False)

print("\nSeed data v2 complete! Trades derived from real NEM market data.")
