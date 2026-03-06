# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Deal Capture Data
# MAGIC Creates demo data for Phase 2 Deal Capture & Portfolio Management:
# MAGIC - 5 counterparties
# MAGIC - 3 portfolios
# MAGIC - ~50 sample trades across all types and regions
# MAGIC - Portfolio-trade assignments

# COMMAND ----------

import uuid
from datetime import datetime, timedelta

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
# MAGIC ## 1. Counterparties

# COMMAND ----------

counterparties = [
    {"id": uid(), "name": "AGL Energy", "rating": "BBB+", "limit": 500_000_000},
    {"id": uid(), "name": "Origin Energy", "rating": "BBB", "limit": 400_000_000},
    {"id": uid(), "name": "EnergyAustralia", "rating": "BBB", "limit": 350_000_000},
    {"id": uid(), "name": "Snowy Hydro", "rating": "AAA", "limit": 1_000_000_000},
    {"id": uid(), "name": "Iberdrola Australia", "rating": "BBB+", "limit": 300_000_000},
]

for c in counterparties:
    spark.sql(f"""
        INSERT INTO {SCHEMA}.counterparties VALUES (
            '{c["id"]}', '{c["name"]}', '{c["rating"]}', {c["limit"]}, 'ACTIVE', '{NOW}'
        )
    """)
    print(f"  Created counterparty: {c['name']}")

cp_map = {c["name"]: c["id"] for c in counterparties}
print(f"Created {len(counterparties)} counterparties")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Portfolios

# COMMAND ----------

portfolios = [
    {"id": uid(), "name": "Trading Book", "owner": "Energy Trading Desk", "desc": "Active trading positions"},
    {"id": uid(), "name": "Hedge Book", "owner": "Risk Management", "desc": "Customer load hedging"},
    {"id": uid(), "name": "Renewable PPAs", "owner": "Origination", "desc": "Long-term PPA contracts"},
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
# MAGIC ## 3. Sample Trades

# COMMAND ----------

import random
random.seed(42)

REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
PROFILES = ["FLAT", "PEAK", "OFF_PEAK", "SUPER_PEAK"]

trades = []

# Swaps (20 trades) — Trading Book + Hedge Book
for i in range(20):
    region = REGIONS[i % 5]
    q = (i % 4) + 1
    year = 2026 if q >= 2 else 2027
    start_month = (q - 1) * 3 + 1
    start_date = f"{year}-{start_month:02d}-01"
    end_month = start_month + 2
    end_date = f"{year}-{end_month:02d}-{'30' if end_month in (4,6,9,11) else '28' if end_month == 2 else '31'}"
    trade_id = uid()
    cp = random.choice(counterparties)
    pf = portfolios[0] if i < 12 else portfolios[1]
    trades.append({
        "trade_id": trade_id,
        "trade_type": "SWAP",
        "region": region,
        "buy_sell": random.choice(["BUY", "SELL"]),
        "volume_mw": random.choice([25, 50, 75, 100, 150]),
        "price": round(random.uniform(55, 120), 2),
        "start_date": start_date,
        "end_date": end_date,
        "profile": random.choice(PROFILES[:3]),
        "status": random.choice(["CONFIRMED", "CONFIRMED", "DRAFT"]),
        "counterparty_id": cp["id"],
        "portfolio_id": pf["id"],
        "notes": f"Q{q} {year} {region} swap",
        "created_by": "seed",
    })

# Futures (10 trades) — Trading Book
for i in range(10):
    region = REGIONS[i % 5]
    q = random.choice([3, 4])
    trade_id = uid()
    cp = random.choice(counterparties)
    trades.append({
        "trade_id": trade_id,
        "trade_type": "FUTURE",
        "region": region,
        "buy_sell": random.choice(["BUY", "SELL"]),
        "volume_mw": random.choice([10, 25, 50]),
        "price": round(random.uniform(60, 100), 2),
        "start_date": f"2026-{(q-1)*3+1:02d}-01",
        "end_date": f"2026-{q*3:02d}-30" if q*3 != 12 else "2026-12-31",
        "profile": "FLAT",
        "status": "CONFIRMED",
        "counterparty_id": cp["id"],
        "portfolio_id": pf_map["Trading Book"],
        "notes": f"ASX Energy future Q{q} 2026",
        "created_by": "seed",
    })

# PPAs (5 trades) — Renewable PPAs portfolio
for i in range(5):
    region = random.choice(["NSW1", "QLD1", "VIC1", "SA1"])
    trade_id = uid()
    cp = random.choice(counterparties)
    trades.append({
        "trade_id": trade_id,
        "trade_type": "PPA",
        "region": region,
        "buy_sell": "BUY",
        "volume_mw": random.choice([30, 50, 80, 100, 200]),
        "price": round(random.uniform(45, 70), 2),
        "start_date": "2026-01-01",
        "end_date": "2028-12-31",
        "profile": "FLAT",
        "status": "CONFIRMED",
        "counterparty_id": cp["id"],
        "portfolio_id": pf_map["Renewable PPAs"],
        "notes": f"Long-term solar/wind PPA in {region}",
        "created_by": "seed",
    })

# Caps/Options (5 trades) — Hedge Book
for i in range(5):
    region = REGIONS[i % 5]
    trade_id = uid()
    cp = random.choice(counterparties)
    trades.append({
        "trade_id": trade_id,
        "trade_type": "OPTION",
        "region": region,
        "buy_sell": "BUY",
        "volume_mw": random.choice([25, 50, 100]),
        "price": round(random.uniform(5, 15), 2),
        "start_date": "2026-07-01",
        "end_date": "2026-12-31",
        "profile": "PEAK",
        "status": "CONFIRMED",
        "counterparty_id": cp["id"],
        "portfolio_id": pf_map["Hedge Book"],
        "notes": f"$300 cap {region} H2 2026",
        "created_by": "seed",
    })

# Spot exposure (5 trades) — Trading Book
for i, region in enumerate(REGIONS):
    trade_id = uid()
    trades.append({
        "trade_id": trade_id,
        "trade_type": "SPOT",
        "region": region,
        "buy_sell": "SELL",
        "volume_mw": random.choice([200, 500, 800]),
        "price": round(random.uniform(60, 90), 2),
        "start_date": "2026-03-01",
        "end_date": "2026-03-31",
        "profile": "FLAT",
        "status": "CONFIRMED",
        "counterparty_id": "",
        "portfolio_id": pf_map["Trading Book"],
        "notes": f"Spot generation exposure {region}",
        "created_by": "seed",
    })

# RECs (5 trades) — Renewable PPAs
for i in range(5):
    region = random.choice(REGIONS[:4])
    trade_id = uid()
    trades.append({
        "trade_id": trade_id,
        "trade_type": "REC",
        "region": region,
        "buy_sell": random.choice(["BUY", "SELL"]),
        "volume_mw": random.choice([10, 20, 50]),
        "price": round(random.uniform(35, 55), 2),
        "start_date": "2026-01-01",
        "end_date": "2026-12-31",
        "profile": "FLAT",
        "status": random.choice(["CONFIRMED", "DRAFT"]),
        "counterparty_id": random.choice(counterparties)["id"],
        "portfolio_id": pf_map["Renewable PPAs"],
        "notes": f"LGC certificates {region}",
        "created_by": "seed",
    })

print(f"Prepared {len(trades)} trades")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert trades and portfolio mappings

# COMMAND ----------

for t in trades:
    trade_id = t["trade_id"]
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
            '{t["notes"]}',
            '{t["created_by"]}',
            '{NOW}',
            '{NOW}'
        )
    """)
    # Also add to portfolio_trades mapping
    if t["portfolio_id"]:
        spark.sql(f"""
            INSERT INTO {SCHEMA}.portfolio_trades VALUES (
                '{t["portfolio_id"]}', '{trade_id}'
            )
        """)

print(f"Inserted {len(trades)} trades + portfolio mappings")

# COMMAND ----------

# Verify
for table in ["counterparties", "portfolios", "trades", "portfolio_trades"]:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {SCHEMA}.{table}").collect()[0].cnt
    print(f"  {SCHEMA}.{table}: {cnt} rows")

print(f"\nSeed data complete!")
print(f"  Counterparties: {len(counterparties)}")
print(f"  Portfolios: {len(portfolios)}")
print(f"  Trades: {len(trades)}")
