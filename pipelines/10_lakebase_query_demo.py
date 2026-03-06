# Databricks notebook source
# MAGIC %md
# MAGIC # Query Lakebase Postgres Directly
# MAGIC This notebook connects to the Lakebase Provisioned instance via PostgreSQL protocol
# MAGIC and runs queries against the synced tables — proving data is served from Postgres, not Delta.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.0" "databricks-sdk>=0.81.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import psycopg
import uuid
import socket
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get instance details
instance = w.database.get_database_instance(name="energy-copilot-db")
print(f"Instance: {instance.name}")
print(f"State:    {instance.state}")
print(f"Host:     {instance.read_write_dns}")
print(f"Capacity: {instance.capacity}")

# Generate OAuth token (valid 1 hour)
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=["energy-copilot-db"],
)
username = w.current_user.me().user_name
print(f"User:     {username}")
print(f"Token:    {cred.token[:20]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Lakebase Postgres

# COMMAND ----------

host = instance.read_write_dns
ip = socket.gethostbyname(host)
print(f"Resolved {host} -> {ip}")

# Try multiple port/dbname combos
import traceback

for port, dbname in [(443, "energy_copilot_db"), (5432, "energy_copilot_db"), (443, "postgres"), (5432, "postgres")]:
    try:
        print(f"\nTrying port={port}, dbname={dbname}...")
        conn = psycopg.connect(
            host=host,
            hostaddr=ip,
            port=port,
            dbname=dbname,
            user=username,
            password=cred.token,
            sslmode="require",
            connect_timeout=10,
        )
        print(f"  SUCCESS! Connected on port {port} with dbname={dbname}")
        break
    except Exception as e:
        print(f"  Failed: {e}")
        conn = None

if conn is None:
    # Also try without hostaddr
    for port, dbname in [(443, "energy_copilot_db"), (5432, "energy_copilot_db")]:
        try:
            print(f"\nTrying without hostaddr: port={port}, dbname={dbname}...")
            conn = psycopg.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=username,
                password=cred.token,
                sslmode="require",
                connect_timeout=10,
            )
            print(f"  SUCCESS! Connected on port {port} with dbname={dbname}")
            break
        except Exception as e:
            print(f"  Failed: {e}")
            conn = None

if conn:
    print("\nConnected to Lakebase Postgres!")
else:
    raise RuntimeError("Could not connect to Lakebase Postgres on any port/dbname combination")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List all tables in Lakebase

# COMMAND ----------

cur = conn.cursor()
cur.execute("""
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY tablename
""")
tables = cur.fetchall()
print(f"\nTables in Lakebase Postgres ({len(tables)}):")
print(f"{'Schema':<10} {'Table':<45} {'Rows':>10}")
print("-" * 68)
for schema, table in tables:
    cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
    cnt = cur.fetchone()[0]
    print(f"{schema:<10} {table:<45} {cnt:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 1: Latest NEM Spot Prices (from Postgres)

# COMMAND ----------

cur.execute("""
    SELECT region_id, interval_datetime, rrp, total_demand_mw
    FROM gold.nem_prices_5min_dedup_synced
    ORDER BY interval_datetime DESC
    LIMIT 10
""")
cols = [d[0] for d in cur.description]
rows = cur.fetchall()

print(f"{'Region':>8}  {'Timestamp':>22}  {'Price':>10}  {'Demand':>10}")
print("-" * 56)
for r in rows:
    print(f"{r[0]:>8}  {str(r[1]):>22}  ${r[2]:>8.2f}  {r[3]:>8.0f} MW")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 2: ASX Futures Forward Curve (from Postgres)

# COMMAND ----------

cur.execute("""
    SELECT contract_code, region, contract_type, quarter, year,
           settlement_price, change_1d, volume
    FROM gold.asx_futures_eod_synced
    WHERE region = 'NSW1'
    ORDER BY year, quarter, contract_type
""")
rows = cur.fetchall()

print(f"{'Code':<12} {'Type':<8} {'Quarter':<8} {'Price':>10} {'Change':>8} {'Volume':>8}")
print("-" * 60)
for r in rows:
    print(f"{r[0]:<12} {r[2]:<8} {r[3]}-{r[4]:<4} ${r[5]:>8.2f} {r[6]:>+7.2f} {r[7]:>8,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 3: Gas Hub Prices (from Postgres)

# COMMAND ----------

cur.execute("""
    SELECT hub, trade_date, price_aud_gj, volume_tj
    FROM gold.gas_hub_prices_synced
    ORDER BY trade_date DESC, hub
    LIMIT 10
""")
rows = cur.fetchall()

print(f"{'Hub':<15} {'Date':<12} {'Price':>10} {'Volume':>10}")
print("-" * 50)
for r in rows:
    print(f"{r[0]:<15} {str(r[1]):<12} ${r[2]:>8.2f} {r[3]:>8.1f} TJ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 4: Emissions Factors (from Postgres)

# COMMAND ----------

cur.execute("""
    SELECT fuel_type, scope1_kg_co2_mwh, scope2_kg_co2_mwh,
           total_kg_co2_mwh, source
    FROM gold.emissions_factors_synced
    ORDER BY total_kg_co2_mwh DESC
""")
rows = cur.fetchall()

print(f"{'Fuel Type':<25} {'Scope1':>8} {'Scope2':>8} {'Total':>8} {'Source':<15}")
print("-" * 70)
for r in rows:
    print(f"{r[0]:<25} {r[1]:>8.1f} {r[2]:>8.1f} {r[3]:>8.1f} {r[4]:<15}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 5: Dashboard Snapshots (from Postgres)

# COMMAND ----------

cur.execute("""
    SELECT endpoint_path, region, snapshot_at,
           LENGTH(payload_json) AS payload_bytes
    FROM gold.dashboard_snapshots_synced
    ORDER BY endpoint_path, region
""")
rows = cur.fetchall()

print(f"{'Endpoint':<45} {'Region':<8} {'Snapshot At':<22} {'Size':>8}")
print("-" * 86)
for r in rows:
    print(f"{r[0]:<45} {r[1]:<8} {str(r[2]):<22} {r[3]:>6,} B")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 6: Interconnector Flows (from Postgres)

# COMMAND ----------

cur.execute("""
    SELECT interconnector_id, from_region, to_region,
           mw_flow, utilization_pct, is_congested
    FROM gold.nem_interconnectors_dedup_synced
    ORDER BY interval_datetime DESC
    LIMIT 10
""")
rows = cur.fetchall()

print(f"{'Interconnector':<20} {'From':>6} {'To':>6} {'Flow MW':>10} {'Util %':>8} {'Congested'}")
print("-" * 65)
for r in rows:
    print(f"{r[0]:<20} {r[1]:>6} {r[2]:>6} {r[3]:>10.1f} {r[4]:>7.1f}% {'YES' if r[5] else 'no'}")

# COMMAND ----------

conn.close()
print("Connection closed.")
