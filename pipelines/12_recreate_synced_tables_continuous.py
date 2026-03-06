# Databricks notebook source
# MAGIC %md
# MAGIC # Recreate Synced Tables with CONTINUOUS scheduling
# MAGIC Drop stale PG tables, then recreate synced tables with continuous sync.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.0" "databricks-sdk>=0.81.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import psycopg
import socket
import uuid
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import SyncedDatabaseTable, SyncedTableSpec, SyncedTableSchedulingPolicy

try:
    instance_name = dbutils.widgets.get("lakebase_instance_name")
except Exception:
    instance_name = "energy-copilot-db"
try:
    db_name = dbutils.widgets.get("lakebase_db")
except Exception:
    db_name = "energy_copilot_db"
try:
    sp_user = dbutils.widgets.get("app_sp_uuid")
except Exception:
    sp_user = "67aaaa6b-778c-4c8b-b2f0-9f9b9728b3bb"
try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = f"{CATALOG"

w = WorkspaceClient()
instance = w.database.get_database_instance(name=instance_name)
host = instance.read_write_dns
ip = socket.gethostbyname(host)
username = w.current_user.me().user_name
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[instance_name],
)

conn = None
for port in [443, 5432]:
    try:
        conn = psycopg.connect(
            host=host, hostaddr=ip, port=port, dbname="energy_copilot_db",
            user=username, password=cred.token, sslmode="require", connect_timeout=10,
        )
        print(f"Connected on port {port}")
        break
    except Exception as e:
        print(f"Port {port} failed: {e}")

assert conn, "Could not connect"
conn.autocommit = True
cur = conn.cursor()

# Tables to recreate with CONTINUOUS sync
tables = [
    {
        "synced_name": f"{CATALOG}.gold.nem_prices_5min_dedup_synced",
        "source": f"{CATALOG}.gold.nem_prices_5min_dedup",
        "pk": ["region_id", "interval_datetime"],
        "pg_table": "gold.nem_prices_5min_dedup_synced",
    },
    {
        "synced_name": f"{CATALOG}.gold.nem_interconnectors_dedup_synced",
        "source": f"{CATALOG}.gold.nem_interconnectors_dedup",
        "pk": ["interconnector_id", "interval_datetime"],
        "pg_table": "gold.nem_interconnectors_dedup_synced",
    },
    {
        "synced_name": f"{CATALOG}.gold.nem_generation_by_fuel_synced",
        "source": f"{CATALOG}.gold.nem_generation_by_fuel_dedup",
        "pk": ["region_id", "fuel_type", "interval_datetime"],
        "pg_table": "gold.nem_generation_by_fuel_synced",
    },
    {
        "synced_name": f"{CATALOG}.gold.dashboard_snapshots_synced",
        "source": f"{CATALOG}.gold.dashboard_snapshots",
        "pk": ["endpoint_path", "region"],
        "pg_table": "gold.dashboard_snapshots_synced",
    },
    # Phase 2: Deal Capture tables
    {
        "synced_name": f"{CATALOG}.gold.trades_synced",
        "source": f"{CATALOG}.gold.trades",
        "pk": ["trade_id"],
        "pg_table": "gold.trades_synced",
    },
    {
        "synced_name": f"{CATALOG}.gold.trade_legs_synced",
        "source": f"{CATALOG}.gold.trade_legs",
        "pk": ["leg_id"],
        "pg_table": "gold.trade_legs_synced",
    },
    {
        "synced_name": f"{CATALOG}.gold.counterparties_synced",
        "source": f"{CATALOG}.gold.counterparties",
        "pk": ["counterparty_id"],
        "pg_table": "gold.counterparties_synced",
    },
    {
        "synced_name": f"{CATALOG}.gold.portfolios_synced",
        "source": f"{CATALOG}.gold.portfolios",
        "pk": ["portfolio_id"],
        "pg_table": "gold.portfolios_synced",
    },
]

# COMMAND ----------

# Step 1: Delete existing synced table definitions (UC side)
for t in tables:
    try:
        w.api_client.do("DELETE", f"/api/2.0/database/synced_tables/{t['synced_name']}")
        print(f"Deleted UC synced table: {t['synced_name']}")
    except Exception as e:
        print(f"Delete UC {t['synced_name']}: {e}")

# COMMAND ----------

# Step 2: Drop PG tables that are left behind
for t in tables:
    try:
        cur.execute(f"DROP TABLE IF EXISTS {t['pg_table']} CASCADE")
        print(f"Dropped PG table: {t['pg_table']}")
    except Exception as e:
        print(f"Drop PG {t['pg_table']}: {e}")

# COMMAND ----------

# Step 3: Recreate synced tables with CONTINUOUS scheduling
for t in tables:
    try:
        result = w.database.create_synced_database_table(
            SyncedDatabaseTable(
                name=t["synced_name"],
                database_instance_name=instance_name,
                logical_database_name=db_name,
                spec=SyncedTableSpec(
                    source_table_full_name=t["source"],
                    primary_key_columns=t["pk"],
                    scheduling_policy=SyncedTableSchedulingPolicy.CONTINUOUS,
                ),
            )
        )
        print(f"Created CONTINUOUS synced table: {t['synced_name']}")
        print(f"  Pipeline: {result.data_synchronization_status.pipeline_id if result.data_synchronization_status else 'pending'}")
    except Exception as e:
        print(f"Create {t['synced_name']}: {e}")

# COMMAND ----------

# Step 4: Grant permissions on gold schema to the app SP
import time
time.sleep(10)  # Wait for tables to be created

# Reconnect (token may have changed)
cred2 = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[instance_name],
)
conn2 = psycopg.connect(
    host=host, hostaddr=ip, port=conn.info.port, dbname=db_name,
    user=username, password=cred2.token, sslmode="require", connect_timeout=10,
)
conn2.autocommit = True
cur2 = conn2.cursor()

cur2.execute(f'GRANT USAGE ON SCHEMA gold TO "{sp_user}"')
cur2.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA gold TO "{sp_user}"')
cur2.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO "{sp_user}"')
print("Granted SELECT permissions to app SP on gold schema")

# Verify sync status
for t in tables:
    try:
        info = w.api_client.do("GET", f"/api/2.0/database/synced_tables/{t['synced_name']}")
        state = info.get("data_synchronization_status", {}).get("detailed_state", "unknown")
        policy = info.get("spec", {}).get("scheduling_policy", "unknown")
        print(f"  {t['synced_name']}: state={state}, policy={policy}")
    except Exception as e:
        print(f"  Check {t['synced_name']}: {e}")

conn2.close()
conn.close()
print("Done!")
