# Databricks notebook source
# MAGIC %md
# MAGIC # Evidence verification — run in Databricks, outputs exported to docs/evidence/runs/
# MAGIC
# MAGIC Every cell queries the live workspace and prints the result, so the exported run shows the actual numbers behind
# MAGIC the README and the evidence pack: pipeline data quality, Unity Catalog governance, lineage, forecast models, the
# MAGIC AI assistant's audit log, and a Genie question answered with SQL.

# COMMAND ----------

import json
import re

dbutils.widgets.text("catalog", "energy_copilot_catalog")
dbutils.widgets.text("genie_space_id", "")
CATALOG = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"Invalid catalog name: {CATALOG!r}")
G = f"{CATALOG}.gold"


def show(title: str, sql: str, n: int = 50) -> None:
    """Print a titled, fixed-width result table (readable in the exported run output)."""
    df = spark.sql(sql)
    print(f"\n=== {title} ===")
    df.show(n, truncate=False)

# COMMAND ----------
# MAGIC %md ## 1. Pipeline: real AEMO data in gold, and data-quality results per rule

# COMMAND ----------

show("gold.nem_prices_5min coverage", f"""
  SELECT region_id, count(*) AS intervals, min(interval_datetime) AS first_utc, max(interval_datetime) AS last_utc,
         round(avg(rrp), 2) AS avg_rrp, round(max(rrp), 2) AS max_rrp, round(min(rrp), 2) AS min_rrp
  FROM {G}.nem_prices_5min GROUP BY 1 ORDER BY 1""")
show("gold.dq_results — totals per rule", f"""
  SELECT rule_name, count(*) AS runs, sum(rows_checked) AS rows_checked, sum(rows_failed) AS rows_failed,
         count_if(status = 'FAIL') AS failed_runs FROM {G}.dq_results GROUP BY 1 ORDER BY 1""")
show("gold.dq_results — latest 5 runs", f"""
  SELECT job_run_id, min(run_ts) AS run_ts_utc, max(rows_checked) AS rows_checked, sum(rows_failed) AS rows_failed,
         max(CASE WHEN rule_name = 'freshness' THEN status END) AS freshness
  FROM {G}.dq_results GROUP BY 1 ORDER BY 2 DESC LIMIT 5""")
show("Rows landed in bronze by the live 5-minute ingest (one row per AEMO file, latest 12)", f"""
  SELECT _source_file, _ingested_at, count(*) AS rows_landed, concat_ws(',', sort_array(collect_set(regionid))) AS regions,
         max(settlement_datetime) AS settlement_datetime_aest
  FROM {CATALOG}.bronze.nemweb_dispatch_price GROUP BY 1, 2 ORDER BY 2 DESC, 1 DESC LIMIT 12""")
show("Latest real 5-minute prices in gold.nem_prices_5min", f"""
  SELECT interval_datetime, region_id, round(rrp, 2) AS rrp_aud_mwh, round(total_demand_mw) AS demand_mw,
         round(available_gen_mw) AS available_gen_mw, round(net_interchange_mw) AS net_interchange_mw
  FROM {G}.nem_prices_5min ORDER BY interval_datetime DESC, region_id LIMIT 10""")
show("gold.nem_prices_quarantine", f"""
  SELECT region_id, failed_rules, count(*) AS n, round(min(rrp), 2) AS min_rrp, round(max(rrp), 2) AS max_rrp
  FROM {G}.nem_prices_quarantine GROUP BY 1, 2""")

# COMMAND ----------
# MAGIC %md ## 2. Unity Catalog governance: policies, constraints, tags, lineage

# COMMAND ----------

show("Row filters and column masks registered in information_schema", f"""
  SELECT 'row_filter' AS kind, table_name, filter_name AS policy, target_columns AS columns
  FROM {CATALOG}.information_schema.row_filters WHERE table_schema = 'gold'
  UNION ALL
  SELECT 'column_mask', table_name, mask_name, column_name FROM {CATALOG}.information_schema.column_masks
  WHERE table_schema = 'gold'""")
show("Desk entitlements driving the gold.trades row filter", f"""
  SELECT e.principal, coalesce(p.name, 'ALL PORTFOLIOS') AS portfolio
  FROM {G}.desk_entitlements e LEFT JOIN {G}.portfolios p USING (portfolio_id) ORDER BY 1, 2""")
show("gold.trades as seen by this job's identity (row filter applied)", f"""
  SELECT current_user() AS identity, p.name AS portfolio, count(*) AS trades
  FROM {G}.trades t JOIN {G}.portfolios p USING (portfolio_id) GROUP BY 1, 2 ORDER BY 2""")
print("\n=== CHECK constraints on gold.nem_prices_5min ===")
spark.sql(f"SHOW TBLPROPERTIES {G}.nem_prices_5min").filter("key LIKE 'delta.constraints.%'").show(truncate=False)
show("Classification tags", f"""
  SELECT table_name, tag_name, tag_value FROM {CATALOG}.information_schema.table_tags
  WHERE schema_name = 'gold' AND tag_name LIKE 'ec_%' ORDER BY 1, 2""")
show("Lineage (system.access.table_lineage)", f"""
  SELECT source_table_full_name, target_table_full_name, entity_type, max(event_time) AS last_seen
  FROM system.access.table_lineage
  WHERE target_table_full_name IN ('{G}.nem_prices_5min', '{G}.nem_prices_quarantine', '{CATALOG}.bronze.nemweb_dispatch_price')
    AND source_table_full_name IS NOT NULL
  GROUP BY ALL ORDER BY last_seen DESC""")

# COMMAND ----------
# MAGIC %md ## 3. Forecast models: held-out back-test vs persistence, and live forecasts vs actuals

# COMMAND ----------

show("gold.forecast_model_metrics — held-out test (skill = 1 - MAE/persistence MAE)", f"""
  SELECT target, region, horizon_intervals * 5 AS horizon_min, round(mae, 2) AS mae, round(persistence_mae, 2) AS persistence_mae,
         round(skill_vs_persistence, 3) AS skill, round(interval_coverage_80, 3) AS cov80, model_version
  FROM {G}.forecast_model_metrics ORDER BY 1, 2, 3""")
show("Live 1-hour price forecasts (job 05) scored against actuals that have since arrived", f"""
  SELECT f.region_id, count(*) AS scored, round(avg(abs(f.predicted_rrp - a.rrp)), 2) AS live_mae,
         max(f.model_version) AS model_version, max(f.forecast_run_at) AS last_run_utc
  FROM {G}.price_forecasts f JOIN {G}.nem_prices_5min a
    ON a.region_id = f.region_id AND a.interval_datetime = f.interval_datetime
  WHERE f.model_name LIKE '%.ml.nem_price_forecast_%' AND f.horizon_intervals = 12 GROUP BY 1 ORDER BY 1""")

import mlflow

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")  # serverless: avoid reading spark.mlflow.modelRegistryUri
exp = mlflow.get_experiment_by_name("/Shared/energy_copilot_nem_forecast")
runs = mlflow.search_runs([exp.experiment_id], order_by=["start_time DESC"], max_results=12)
cols = [c for c in ["run_id", "tags.mlflow.runName", "start_time", "status", "params.target", "params.region",
                    "params.n_train", "metrics.h12_mae", "metrics.h12_persistence_mae", "metrics.h12_skill_vs_persistence"]
        if c in runs.columns]
print(f"\n=== MLflow training runs in /Shared/energy_copilot_nem_forecast (experiment {exp.experiment_id}, latest 12) ===")
print(runs[cols].to_string(index=False))

# COMMAND ----------
# MAGIC %md ## 4. AI assistant: audit log of real turns (question, tool calls, answer)

# COMMAND ----------

show("gold.copilot_interactions", f"""
  SELECT interaction_id, created_at, model, source, input_tokens, output_tokens, latency_ms, substr(question, 1, 90) AS question
  FROM {G}.copilot_interactions ORDER BY created_at""")
latest = spark.sql(f"SELECT * FROM {G}.copilot_interactions ORDER BY created_at DESC LIMIT 1").collect()
if latest:
    row = latest[0]
    print(f"\n=== Conversation transcript — interaction {row['interaction_id']} ({row['created_at']}, {row['model']}) ===")
    print(f"USER: {row['question']}")
    for call in json.loads(row["tool_calls"]):
        print(f"\nTOOL CALL (round {call['round']}): {call['name']}({json.dumps(call['arguments'])})")
        print(f"TOOL RESULT ({call['result_chars']} chars, first 600): {call['result_preview']}")
    print(f"\nASSISTANT ({row['input_tokens']} in / {row['output_tokens']} out tokens, {row['latency_ms']} ms):\n{row['answer']}")

# COMMAND ----------
# MAGIC %md ## 5. Genie: a natural-language question answered with SQL on governed tables

# COMMAND ----------

SPACE_ID = dbutils.widgets.get("genie_space_id")
if SPACE_ID:
    import time

    from databricks.sdk import WorkspaceClient

    api = WorkspaceClient().api_client  # authenticated REST client (Genie SDK surface varies across SDK versions)
    base = f"/api/2.0/genie/spaces/{SPACE_ID}"
    question = ("For October 2026, which NEM region had the most 5-minute intervals with a spot price above "
                "300 dollars per MWh, how many were there, and what was its maximum price?")
    started = api.do("POST", f"{base}/start-conversation", body={"content": question})
    conv_id, msg_id = started["conversation_id"], started["message_id"]
    for _ in range(60):
        msg = api.do("GET", f"{base}/conversations/{conv_id}/messages/{msg_id}")
        if msg.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(5)
    print(f"conversation {conv_id} | message {msg_id} | status {msg.get('status')}")
    print("QUESTION:", question)
    for att in msg.get("attachments", []):
        if att.get("query"):
            print("\nSQL GENERATED BY GENIE:\n" + att["query"]["query"])
            res = api.do("GET", f"{base}/conversations/{conv_id}/messages/{msg_id}/attachments/{att['attachment_id']}/query-result")
            stmt = res.get("statement_response", {})
            cols = [c["name"] for c in stmt.get("manifest", {}).get("schema", {}).get("columns", [])]
            print("\nRESULT:", cols, stmt.get("result", {}).get("data_array"))
        if att.get("text"):
            print("\nGENIE ANSWER:", att["text"]["content"])
    show("Independent check of the Genie answer", f"""
      SELECT region_id, count(*) AS intervals_over_300, max(rrp) AS max_rrp FROM {G}.nem_prices_5min
      WHERE interval_datetime >= '2026-10-01' AND interval_datetime < '2026-11-01' AND rrp > 300
      GROUP BY 1 ORDER BY 2 DESC""")
