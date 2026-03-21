# Databricks notebook source
# MAGIC %md
# MAGIC # Grant Lakebase Permissions to App Service Principal

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.0" "databricks-sdk>=0.81.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import socket
import uuid

import psycopg
from databricks.sdk import WorkspaceClient

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

w = WorkspaceClient()
instance = w.database.get_database_instance(name=instance_name)
host = instance.read_write_dns
ip = socket.gethostbyname(host)
username = w.current_user.me().user_name
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[instance_name],
)

print(f"Connecting as {username} to {host} ({ip}), dbname={db_name}")

conn = None
for port in [443, 5432]:
    try:
        conn = psycopg.connect(
            host=host, hostaddr=ip, port=port, dbname=db_name,
            user=username, password=cred.token, sslmode="require", connect_timeout=10,
        )
        print(f"Connected on port {port}")
        break
    except Exception as e:
        print(f"Port {port} failed: {e}")

assert conn, "Could not connect to Lakebase"
conn.autocommit = True
cur = conn.cursor()

# Grant USAGE on gold schema
cur.execute(f'GRANT USAGE ON SCHEMA gold TO "{sp_user}"')
print("Granted USAGE on schema gold")

# Grant SELECT on all existing tables
cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA gold TO "{sp_user}"')
print("Granted SELECT on all tables in gold schema")

# Set default privileges for future tables
cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO "{sp_user}"')
print("Set default privileges for future tables")

# Test query
cur.execute("SELECT COUNT(*) FROM gold.nem_prices_5min_dedup_synced")
print(f"Row count in nem_prices_5min_dedup_synced: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM gold.nem_interconnectors_dedup_synced")
print(f"Row count in nem_interconnectors_dedup_synced: {cur.fetchone()[0]}")

conn.close()
print("Done!")
