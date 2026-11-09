# Run evidence — evidence_verification / task `evidence_verification`

| field | value |
|---|---|
| job run id | `678829619491776` |
| task run id | `197619732568832` |
| job | Evidence verification (FE BAR) |
| notebook | `/Workspace/Users/sourabh.ghose@databricks.com/.bundle/energy-copilot/dev/files/notebooks/evidence_verification` |
| started | 2026-11-09 11:12:29 UTC |
| result | **SUCCESS** |
| run page | https://fevm-serverless-stable-2b9lj3.cloud.databricks.com/?o=7474644340067319#job/739066963430187/run/197619732568832 |

Exported from the Databricks Jobs API with cell outputs as they were produced during the run.

## Cell 2

```python
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
```

## Cell 4

```python
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
show("gold.nem_prices_quarantine", f"""
  SELECT region_id, failed_rules, count(*) AS n, round(min(rrp), 2) AS min_rrp, round(max(rrp), 2) AS max_rrp
  FROM {G}.nem_prices_quarantine GROUP BY 1, 2""")
```

**Output:**

```text
=== gold.nem_prices_5min coverage ===
+---------+---------+-------------------+-------------------+-------+-------+-------+
|region_id|intervals|first_utc          |last_utc           |avg_rrp|max_rrp|min_rrp|
+---------+---------+-------------------+-------------------+-------+-------+-------+
|NSW1     |26461    |2026-08-09 14:05:00|2026-11-09 11:10:00|89.56  |23200.0|-23.5  |
|QLD1     |26461    |2026-08-09 14:05:00|2026-11-09 11:10:00|70.54  |860.05 |-245.64|
|SA1      |26461    |2026-08-09 14:05:00|2026-11-09 11:10:00|50.96  |3290.0 |-496.77|
|TAS1     |26461    |2026-08-09 14:05:00|2026-11-09 11:10:00|54.59  |490.01 |-1000.0|
|VIC1     |26461    |2026-08-09 14:05:00|2026-11-09 11:10:00|50.5   |406.11 |-365.23|
+---------+---------+-------------------+-------------------+-------+-------+-------+


=== gold.dq_results — totals per rule ===
+-----------------------------+----+------------+-----------+-----------+
|rule_name                    |runs|rows_checked|rows_failed|failed_runs|
+-----------------------------+----+------------+-----------+-----------+
|demand_non_negative          |3   |2880        |102        |1          |
|demand_within_physical_bounds|115 |264110      |0          |0          |
|freshness                    |118 |118         |0          |0          |
|interval_not_null            |118 |266990      |0          |0          |
|interval_on_5min_grid        |118 |266990      |0          |0          |
|rrp_is_numeric               |118 |266990      |0          |0          |
|rrp_within_market_limits     |118 |266990      |3          |1          |
|valid_nem_region             |118 |266990      |0          |0          |
+-----------------------------+----+------------+-----------+-----------+


=== gold.dq_results — latest 5 runs ===
+---------------+--------------------------+------------+-----------+---------+
|job_run_id     |run_ts_utc                |rows_checked|rows_failed|freshness|
+---------------+--------------------------+------------+-----------+---------+
|155565408982135|2026-11-09 11:10:59.445534|5           |0          |PASS     |
|68784862188005 |2026-11-09 11:05:58.072485|5           |0          |PASS     |
|16657246073146 |2026-11-09 11:01:04.249369|5           |0          |PASS     |
|171459585663333|2026-11-09 10:55:59.073301|5           |0          |PASS     |
|824493306133728|2026-11-09 10:51:02.803457|5           |0          |PASS     |
+---------------+--------------------------+------------+-----------+---------+


=== gold.nem_prices_quarantine ===
+---------+--------------------------+---+-------+-------+
|region_id|failed_rules              |n  |min_rrp|max_rrp|
+---------+--------------------------+---+-------+-------+
|SA1      |[demand_non_negative]     |102|-328.6 |12.01  |
|NSW1     |[rrp_within_market_limits]|3  |23107.0|23200.0|
+---------+--------------------------+---+-------+-------+
```

## Cell 6

```python
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
```

**Output:**

```text
=== Row filters and column masks registered in information_schema ===
+-----------+--------------+--------------------------------------------------------+----------------+
|kind       |table_name    |policy                                                  |columns         |
+-----------+--------------+--------------------------------------------------------+----------------+
|row_filter |trades        |serverless_stable_2b9lj3_catalog.tools.trades_row_filter|portfolio_id    |
|column_mask|counterparties|serverless_stable_2b9lj3_catalog.tools.mask_credit_limit|credit_limit_aud|
+-----------+--------------+--------------------------------------------------------+----------------+


=== Desk entitlements driving the gold.trades row filter ===
+------------------------------------+--------------+
|principal                           |portfolio     |
+------------------------------------+--------------+
|83619056-a863-47f9-afb0-3d7cd6d7307a|ALL PORTFOLIOS|
|energy_analysts                     |Renewable PPAs|
|energy_compliance                   |ALL PORTFOLIOS|
|energy_risk                         |ALL PORTFOLIOS|
|energy_traders                      |Hedge Book    |
|energy_traders                      |Trading Book  |
+------------------------------------+--------------+


=== gold.trades as seen by this job's identity (row filter applied) ===
+----------------------------+------------+------+
|identity                    |portfolio   |trades|
+----------------------------+------------+------+
|sourabh.ghose@databricks.com|Hedge Book  |14    |
|sourabh.ghose@databricks.com|Trading Book|28    |
+----------------------------+------------+------+


=== CHECK constraints on gold.nem_prices_5min ===
+------------------------------------------+------------------------------------------------+
|key                                       |value                                           |
+------------------------------------------+------------------------------------------------+
|delta.constraints.interval_not_null       |interval_datetime IS NOT NULL                   |
|delta.constraints.rrp_within_market_limits|rrp BETWEEN -1000.0 AND 23200.0                 |
|delta.constraints.valid_nem_region        |region_id IN ('NSW1','QLD1','VIC1','SA1','TAS1')|
+------------------------------------------+------------------------------------------------+


=== Classification tags ===
+---------------+-----------------+------------------+
|table_name     |tag_name         |tag_value         |
+---------------+-----------------+------------------+
|counterparties |ec_classification|confidential      |
|counterparties |ec_domain        |middle_office     |
|dq_results     |ec_classification|internal          |
|dq_results     |ec_domain        |governance        |
|nem_prices_5min|ec_classification|public_market_data|
|nem_prices_5min|ec_domain        |front_office      |
|nem_prices_5min|ec_source        |aemo_nemweb       |
|portfolios     |ec_classification|confidential      |
|portfolios     |ec_domain        |middle_office     |
|trades         |ec_classification|confidential      |
|trades         |ec_domain        |middle_office     |
+---------------+-----------------+------------------+


=== Lineage (system.access.table_lineage) ===
+-------------------------------------------------------------+-----------------------------------------------------------+-----------+-----------------------+
|source_table_full_name                                       |target_table_full_name                                     |entity_type|last_seen              |
+-------------------------------------------------------------+-----------------------------------------------------------+-----------+-----------------------+
|serverless_stable_2b9lj3_catalog.bronze.nemweb_dispatch_price|serverless_stable_2b9lj3_catalog.gold.nem_prices_5min      |JOB        |2026-11-09 10:56:14.819|
|serverless_stable_2b9lj3_catalog.bronze.nemweb_dispatch_price|serverless_stable_2b9lj3_catalog.gold.nem_prices_quarantine|JOB        |2026-11-09 02:50:42.386|
+-------------------------------------------------------------+-----------------------------------------------------------+-----------+-----------------------+
```

## Cell 8

```python
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
```

**Output:**

```text
=== gold.forecast_model_metrics — held-out test (skill = 1 - MAE/persistence MAE) ===
+------+------+-----------+------+---------------+------+-----+-------------+
|target|region|horizon_min|mae   |persistence_mae|skill |cov80|model_version|
+------+------+-----------+------+---------------+------+-----+-------------+
|demand|NSW1  |30         |132.56|205.17         |0.354 |0.756|6            |
|demand|NSW1  |60         |204.53|381.98         |0.465 |0.77 |6            |
|demand|NSW1  |240        |493.24|1236.13        |0.601 |0.714|6            |
|demand|QLD1  |30         |102.37|167.9          |0.39  |0.795|6            |
|demand|QLD1  |60         |168.3 |313.21         |0.463 |0.75 |6            |
|demand|QLD1  |240        |433.1 |1010.08        |0.571 |0.721|6            |
|demand|SA1   |30         |51.79 |76.8           |0.326 |0.771|6            |
|demand|SA1   |60         |77.25 |133.9          |0.423 |0.762|6            |
|demand|SA1   |240        |161.35|391.66         |0.588 |0.764|6            |
|demand|TAS1  |30         |20.02 |24.58          |0.185 |0.78 |6            |
|demand|TAS1  |60         |27.89 |42.56          |0.345 |0.791|6            |
|demand|TAS1  |240        |55.23 |124.76         |0.557 |0.771|6            |
|demand|VIC1  |30         |106.08|168.75         |0.371 |0.819|6            |
|demand|VIC1  |60         |166.79|301.46         |0.447 |0.812|6            |
|demand|VIC1  |240        |404.46|876.58         |0.539 |0.797|6            |
|price |NSW1  |30         |10.76 |11.21          |0.041 |0.845|6            |
|price |NSW1  |60         |12.83 |14.56          |0.119 |0.847|6            |
|price |NSW1  |240        |17.99 |28.37          |0.366 |0.83 |6            |
|price |QLD1  |30         |9.59  |9.87           |0.028 |0.881|6            |
|price |QLD1  |60         |12.01 |13.51          |0.111 |0.881|6            |
|price |QLD1  |240        |19.78 |32.16          |0.385 |0.841|6            |
|price |SA1   |30         |32.35 |29.64          |-0.091|0.681|6            |
|price |SA1   |60         |38.14 |39.94          |0.045 |0.699|6            |
|price |SA1   |240        |52.09 |80.05          |0.349 |0.708|6            |
|price |TAS1  |30         |17.06 |13.64          |-0.251|0.782|6            |
|price |TAS1  |60         |20.1  |18.11          |-0.11 |0.782|6            |
|price |TAS1  |240        |31.69 |36.69          |0.136 |0.768|6            |
|price |VIC1  |30         |12.87 |13.26          |0.029 |0.779|6            |
|price |VIC1  |60         |16.15 |18.38          |0.122 |0.782|6            |
|price |VIC1  |240        |26.17 |39.97          |0.345 |0.823|6            |
+------+------+-----------+------+---------------+------+-----+-------------+


=== Live 1-hour price forecasts (job 05) scored against actuals that have since arrived ===
+---------+------+--------+-------------+-------------------------+
|region_id|scored|live_mae|model_version|last_run_utc             |
+---------+------+--------+-------------+-------------------------+
|NSW1     |75    |11.45   |6            |2026-11-09 10:12:26.08721|
|QLD1     |75    |10.81   |6            |2026-11-09 10:12:26.08721|
|SA1      |75    |24.96   |6            |2026-11-09 10:12:26.08721|
|TAS1     |75    |20.59   |6            |2026-11-09 10:12:26.08721|
|VIC1     |75    |13.03   |6            |2026-11-09 10:12:26.08721|
+---------+------+--------+-------------+-------------------------+
```

## Cell 10

```python
show("gold.copilot_interactions", f"""
  SELECT interaction_id, created_at, model, source, input_tokens, output_tokens, latency_ms, substr(question, 1, 90) AS question
  FROM {G}.copilot_interactions ORDER BY created_at""")
latest = spark.sql(f"SELECT * FROM {G}.copilot_interactions ORDER BY created_at DESC LIMIT 1").collect()
if latest:
    row = latest[0]
    print(f"\n=== Latest turn {row['interaction_id']} — tool calls ===")
    for call in json.loads(row["tool_calls"]):
        print(f"round {call['round']}: {call['name']}({json.dumps(call['arguments'])}) -> {call['result_preview'][:300]}")
    print("\n=== Answer (first 1,500 chars) ===\n" + (row["answer"] or "")[:1500])
```

**Output:**

```text
=== gold.copilot_interactions ===
+------------------------------------+-------------------+----------------------------+------+------------+-------------+----------+------------------------------------------------------------------------------------------+
|interaction_id                      |created_at         |model                       |source|input_tokens|output_tokens|latency_ms|question                                                                                  |
+------------------------------------+-------------------+----------------------------+------+------------+-------------+----------+------------------------------------------------------------------------------------------+
|82d07d16-d2c8-4ab8-a4fb-173d2e109525|2026-11-09 08:56:05|databricks-claude-sonnet-4-6|local |24269       |1189         |49805     |Using your tools, give me the current SA1 and NSW1 spot prices and the models 1-hour-ahead|
|0b7fc9b7-56b1-4454-8b96-a37bb572e0bf|2026-11-09 09:00:15|databricks-claude-sonnet-4-6|local |23395       |1158         |41629     |Using your tools, give me the current SA1 and NSW1 spot prices and the models 1-hour-ahead|
+------------------------------------+-------------------+----------------------------+------+------------+-------------+----------+------------------------------------------------------------------------------------------+


=== Latest turn 0b7fc9b7-56b1-4454-8b96-a37bb572e0bf — tool calls ===
round 1: query_spot_prices({"region": "SA1"}) -> {"source": "gold.nem_prices_5min (AEMO NEMWEB DispatchIS)", "window_hours": 1, "regions": [{"region_id": "SA1", "latest_rrp": 115.89764, "as_of_utc": "2026-11-09T08:55:00.000Z", "avg_rrp": 119.83, "min_rrp": 112.45, "max_rrp": 137.44, "intervals_over_300": 0, "latest_demand_mw": 1384.0}]}
round 1: query_spot_prices({"region": "NSW1"}) -> {"source": "gold.nem_prices_5min (AEMO NEMWEB DispatchIS)", "window_hours": 1, "regions": [{"region_id": "NSW1", "latest_rrp": 121.01, "as_of_utc": "2026-11-09T08:55:00.000Z", "avg_rrp": 117.17, "min_rrp": 107.3, "max_rrp": 130.25, "intervals_over_300": 0, "latest_demand_mw": 8227.0}]}
round 1: query_price_forecasts({"region": "SA1", "hours": 1}) -> {"source": "gold.price_forecasts (UC model serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_sa1 v6)", "region": "SA1", "forecasts": [{"interval_datetime": "2026-11-09T09:25:00.000Z", "horizon_minutes": 30, "forecast": 101.96, "lower_80": 71.07, "upper_80": 128.67, "spike_probability": 0.0, "fo
round 1: query_price_forecasts({"region": "NSW1", "hours": 1}) -> {"source": "gold.price_forecasts (UC model serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_nsw1 v6)", "region": "NSW1", "forecasts": [{"interval_datetime": "2026-11-09T09:25:00.000Z", "horizon_minutes": 30, "forecast": 115.8, "lower_80": 101.75, "upper_80": 145.7, "spike_probability": 0.0, "f

=== Answer (first 1,500 chars) ===
Heres the full picture from live tool data as at **09:55 AEST, 9 Nov 2026**:

---

## 📊 Current Spot Prices (Last Dispatch Interval)

| Region | Latest RRP | 1-hr Avg | 1-hr Min | 1-hr Max | Intervals >$300 | Demand |
|--------|-----------|----------|----------|----------|-----------------|--------|
| **SA1** | **$115.90/MWh** | $119.83 | $112.45 | $137.44 | 0 | 1,384 MW |
| **NSW1** | **$121.01/MWh** | $117.17 | $107.30 | $130.25 | 0 | 8,227 MW |

---

## 🔮 1-Hour-Ahead Price Forecasts (Model v6)

| Region | 30-min Forecast | 60-min Forecast | 80% CI (60-min) | Spike Probability |
|--------|----------------|----------------|-----------------|-------------------|
| **SA1** | $101.96 | $97.29 | $56.06 – $132.44 | **0.0%** |
| **NSW1** | $115.80 | $114.88 | $98.70 – $151.98 | **0.0%** |

---

## ⚠️ Spike Risk Assessment

### SA1 — **Moderate Caution**
- The model forecasts a **softening trend**: from $115.90 now → $101.96 (30-min) → $97.29 (60-min), a ~$18 pullback.
- Spike probability is **0.0%**, but SA1s recent history demands respect — the 24-hour data shows **20 spike events**, a 24h average of **$108/MWh**, and a jaw-dropping intraday high of **$3,290/MWh** today. The 80% CI lower bound ($56) is also very wide, reflecting genuine uncertainty.
- **Model caveat**: At the 30-min horizon, SA1s model actually has *negative skill vs. persistence* (MAE 32.35 vs. persistence 29.64), meaning it is slightly *worse* than a naïve carry-forward at short horizons. Treat the 30-min fore
```

## Cell 12

```python
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
# … 7 more lines
```

**Output:**

```text
conversation 01f1dc2e8e5216498b3afa75f53bb0bb | message 01f1dc2e8e641840ad76967d2b82b139 | status COMPLETED
QUESTION: For October 2026, which NEM region had the most 5-minute intervals with a spot price above 300 dollars per MWh, how many were there, and what was its maximum price?

SQL GENERATED BY GENIE:
WITH `region_counts` AS (
  SELECT
    `region_id`,
    COUNT(*) AS `interval_count`,
    MAX(`rrp`) AS `max_price`
  FROM `serverless_stable_2b9lj3_catalog`.`gold`.`nem_prices_5min`
  WHERE `interval_datetime` >= TIMESTAMP '2026-10-01 00:00:00'
    AND `interval_datetime` < TIMESTAMP '2026-11-01 00:00:00'
    AND `rrp` IS NOT NULL
    AND `rrp` > 300
    AND `region_id` IS NOT NULL
  GROUP BY `region_id`
), `ranked` AS (
  SELECT
    `region_id`,
    `interval_count`,
    `max_price`,
    RANK() OVER (ORDER BY `interval_count` DESC) AS `rnk`
  FROM `region_counts`
)
SELECT
  `region_id`,
  `interval_count`,
  `max_price`
FROM `ranked`
WHERE `rnk` <= 1
ORDER BY `interval_count` DESC, `region_id` ASC

RESULT: ['region_id', 'interval_count', 'max_price'] [['NSW1', '87', '23200.0']]

GENIE ANSWER: For October 2026, **NSW1** had the most 5-minute intervals with spot prices above $300/MWh, with **87** intervals, and **NSW1** reached a maximum price of **$23,200.0/MWh**. Based on the returned result, **NSW1** was the top region on this measure for October 2026.

=== Independent check of the Genie answer ===
+---------+------------------+---------+
|region_id|intervals_over_300|max_rrp  |
+---------+------------------+---------+
|NSW1     |87                |23200.0  |
|QLD1     |20                |860.05142|
|VIC1     |8                 |406.11105|
|TAS1     |8                 |359.11544|
|SA1      |7                 |3210.98  |
+---------+------------------+---------+
```
