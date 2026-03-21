# Databricks notebook source
# MAGIC %md
# MAGIC # E13: Approval Workflow Tables
# MAGIC Creates `approval_rules` and `approval_requests` tables for trade approval workflows.

# COMMAND ----------

catalog = "energy_copilot_catalog"
schema = f"{catalog}.gold"
sp_id = "67aaaa6b-778c-4c8b-b2f0-9f9b9728b3bb"

# COMMAND ----------

# MAGIC %md
# MAGIC ## approval_rules

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {schema}.approval_rules (
    approval_rule_id STRING,
    rule_name STRING,
    event_type STRING,
    trade_type STRING,
    min_notional_aud DOUBLE,
    required_approvers INT,
    approver_role STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## approval_requests

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {schema}.approval_requests (
    request_id STRING,
    trade_id STRING,
    rule_id STRING,
    event_type STRING,
    status STRING,
    notional_aud DOUBLE,
    submitted_by STRING,
    submitted_at TIMESTAMP,
    decided_by STRING,
    decided_at TIMESTAMP,
    decision_reason STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grants

# COMMAND ----------

spark.sql(f"GRANT MODIFY ON TABLE {schema}.approval_rules TO `{sp_id}`")
spark.sql(f"GRANT MODIFY ON TABLE {schema}.approval_requests TO `{sp_id}`")
print("Grants applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed default approval rules

# COMMAND ----------

from datetime import datetime, timezone

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

seed_rules = [
    ("rule-001", "High-Value Trade", "trade_create", None, 500000.0, 1, "RISK_MANAGER", True),
    ("rule-002", "Very High-Value Trade", "trade_create", None, 2000000.0, 1, "TRADING_HEAD", True),
    ("rule-003", "Large Amendment", "trade_amend", None, 1000000.0, 1, "RISK_MANAGER", True),
]

rows = []
for r in seed_rules:
    rows.append({
        "approval_rule_id": r[0],
        "rule_name": r[1],
        "event_type": r[2],
        "trade_type": r[3],
        "min_notional_aud": r[4],
        "required_approvers": r[5],
        "approver_role": r[6],
        "is_active": r[7],
        "created_at": now,
        "updated_at": now,
    })

from pyspark.sql import Row

df = spark.createDataFrame([Row(**r) for r in rows])
df.write.mode("append").saveAsTable(f"{schema}.approval_rules")
print(f"Seeded {len(rows)} approval rules.")
