# Run evidence — evidence_verification / task `evidence_verification`

| field | value |
|---|---|
| job run id | `63172587989947` |
| task run id | `709237712614405` |
| job | Evidence verification (FE BAR) |
| notebook | `/Workspace/Users/sourabh.ghose@databricks.com/.bundle/energy-copilot/dev/files/notebooks/evidence_verification` |
| started | 2026-11-09 16:17:38 UTC |
| result | **SUCCESS** |
| run page | https://fevm-serverless-stable-2b9lj3.cloud.databricks.com/?o=7474644340067319#job/222771373518290/run/709237712614405 |

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
```

**Output:**

```text
=== gold.nem_prices_5min coverage ===
+---------+---------+-------------------+-------------------+-------+-------+-------+
|region_id|intervals|first_utc          |last_utc           |avg_rrp|max_rrp|min_rrp|
+---------+---------+-------------------+-------------------+-------+-------+-------+
|NSW1     |26522    |2026-08-09 14:05:00|2026-11-09 16:15:00|89.59  |23200.0|-23.5  |
|QLD1     |26522    |2026-08-09 14:05:00|2026-11-09 16:15:00|70.58  |860.05 |-245.64|
|SA1      |26522    |2026-08-09 14:05:00|2026-11-09 16:15:00|51.08  |3290.0 |-496.77|
|TAS1     |26522    |2026-08-09 14:05:00|2026-11-09 16:15:00|54.68  |490.01 |-1000.0|
|VIC1     |26522    |2026-08-09 14:05:00|2026-11-09 16:15:00|50.6   |406.11 |-365.23|
+---------+---------+-------------------+-------------------+-------+-------+-------+


=== gold.dq_results — totals per rule ===
+-----------------------------+----+------------+-----------+-----------+
|rule_name                    |runs|rows_checked|rows_failed|failed_runs|
+-----------------------------+----+------------+-----------+-----------+
|demand_non_negative          |3   |2880        |102        |1          |
|demand_within_physical_bounds|176 |264415      |0          |0          |
|freshness                    |179 |179         |0          |0          |
|interval_not_null            |179 |267295      |0          |0          |
|interval_on_5min_grid        |179 |267295      |0          |0          |
|rrp_is_numeric               |179 |267295      |0          |0          |
|rrp_within_market_limits     |179 |267295      |3          |1          |
|valid_nem_region             |179 |267295      |0          |0          |
+-----------------------------+----+------------+-----------+-----------+


=== gold.dq_results — latest 5 runs ===
+----------------+--------------------------+------------+-----------+---------+
|job_run_id      |run_ts_utc                |rows_checked|rows_failed|freshness|
+----------------+--------------------------+------------+-----------+---------+
|701712087461256 |2026-11-09 16:16:03.468696|5           |0          |PASS     |
|1054046715355596|2026-11-09 16:11:00.914962|5           |0          |PASS     |
|831685423674914 |2026-11-09 16:05:49.905415|1           |0          |PASS     |
|943787986431022 |2026-11-09 16:01:02.294662|10          |0          |PASS     |
|852912197667658 |2026-11-09 15:55:59.65193 |5           |0          |PASS     |
+----------------+--------------------------+------------+-----------+---------+


=== Rows landed in bronze by the live 5-minute ingest (one row per AEMO file, latest 12) ===
+---------------------------------------------------+--------------------------+-----------+-----------------------+------------------------+
|_source_file                                       |_ingested_at              |rows_landed|regions                |settlement_datetime_aest|
+---------------------------------------------------+--------------------------+-----------+-----------------------+------------------------+
|PUBLIC_DISPATCHIS_202611100215_0000000547707704.zip|2026-11-09 16:15:36.760323|5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 02:15:00     |
|PUBLIC_DISPATCHIS_202611100210_0000000547707161.zip|2026-11-09 16:10:36.005987|5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 02:10:00     |
|PUBLIC_DISPATCHIS_202611100205_0000000547706566.zip|2026-11-09 16:00:37.16688 |5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 02:05:00     |
|PUBLIC_DISPATCHIS_202611100200_0000000547705976.zip|2026-11-09 16:00:37.16688 |5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 02:00:00     |
|PUBLIC_DISPATCHIS_202611100155_0000000547705357.zip|2026-11-09 15:55:35.251352|5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 01:55:00     |
|PUBLIC_DISPATCHIS_202611100150_0000000547704630.zip|2026-11-09 15:50:38.552396|5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 01:50:00     |
|PUBLIC_DISPATCHIS_202611100145_0000000547704003.zip|2026-11-09 15:45:34.473165|5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 01:45:00     |
|PUBLIC_DISPATCHIS_202611100140_0000000547703482.zip|2026-11-09 15:40:36.031922|5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 01:40:00     |
|PUBLIC_DISPATCHIS_202611100135_0000000547702798.zip|2026-11-09 15:35:33.646027|5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 01:35:00     |
|PUBLIC_DISPATCHIS_202611100130_0000000547702205.zip|2026-11-09 15:30:39.504238|5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 01:30:00     |
|PUBLIC_DISPATCHIS_202611100125_0000000547701568.zip|2026-11-09 15:25:42.382026|5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 01:25:00     |
|PUBLIC_DISPATCHIS_202611100120_0000000547700887.zip|2026-11-09 15:20:38.108315|5          |NSW1,QLD1,SA1,TAS1,VIC1|2026/11/10 01:20:00     |
+---------------------------------------------------+--------------------------+-----------+-----------------------+------------------------+


=== Latest real 5-minute prices in gold.nem_prices_5min ===
+-------------------+---------+-----------+---------+----------------+------------------+
|interval_datetime  |region_id|rrp_aud_mwh|demand_mw|available_gen_mw|net_interchange_mw|
+-------------------+---------+-----------+---------+----------------+------------------+
|2026-11-09 16:15:00|NSW1     |90.21      |6431.0   |11618.0         |-1440.0           |
|2026-11-09 16:15:00|QLD1     |75.87      |5726.0   |10632.0         |1055.0            |
|2026-11-09 16:15:00|SA1      |84.35      |1313.0   |3612.0          |277.0             |
|2026-11-09 16:15:00|TAS1     |88.45      |941.0    |1725.0          |-282.0            |
|2026-11-09 16:15:00|VIC1     |85.53      |4536.0   |9749.0          |491.0             |
|2026-11-09 16:10:00|NSW1     |96.73      |6479.0   |11619.0         |-1394.0           |
|2026-11-09 16:10:00|QLD1     |82.48      |5812.0   |10653.0         |994.0             |
|2026-11-09 16:10:00|SA1      |90.44      |1320.0   |3619.0          |293.0             |
|2026-11-09 16:10:00|TAS1     |94.82      |941.0    |1733.0          |-273.0            |
|2026-11-09 16:10:00|VIC1     |91.69      |4560.0   |9734.0          |471.0             |
+-------------------+---------+-----------+---------+----------------+------------------+


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
|serverless_stable_2b9lj3_catalog.bronze.nemweb_dispatch_price|serverless_stable_2b9lj3_catalog.gold.nem_prices_5min      |JOB        |2026-11-09 16:01:16.097|
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
+---------+------+--------+-------------+--------------------------+
|region_id|scored|live_mae|model_version|last_run_utc              |
+---------+------+--------+-------------+--------------------------+
|NSW1     |136   |8.64    |6            |2026-11-09 15:17:27.345676|
|QLD1     |136   |8.06    |6            |2026-11-09 15:17:27.345676|
|SA1      |136   |20.3    |6            |2026-11-09 15:17:27.345676|
|TAS1     |136   |13.98   |6            |2026-11-09 15:17:27.345676|
|VIC1     |136   |9.84    |6            |2026-11-09 15:17:27.345676|
+---------+------+--------+-------------+--------------------------+

/databricks/python/lib/python3.11/site-packages/mlflow/protos/service_pb2.py:11: UserWarning: google.protobuf.service module is deprecated. RPC implementations should provide code generator plugins which generate code specific to the RPC implementation. service.py will be removed in Jan 2025
  from google.protobuf import service as _service

=== MLflow training runs in /Shared/energy_copilot_nem_forecast (experiment 1440555517568947, latest 12) ===
                          run_id        tags.mlflow.runName                       start_time   status params.target params.region params.n_train  metrics.h12_mae  metrics.h12_persistence_mae  metrics.h12_skill_vs_persistence
d70d76888d33444c8dc002308aead261                demand_TAS1 2026-11-09 04:46:13.574000+00:00 FINISHED        demand          TAS1          53565        27.892866                    42.561257                          0.344642
c41ecdd3ece84b73870c1963f5c8d475                 price_TAS1 2026-11-09 04:45:47.771000+00:00 FINISHED         price          TAS1          53565        20.102134                    18.114823                         -0.109706
2052a7ffeb33488daf5714a23d46db9b                 demand_SA1 2026-11-09 04:45:24.708000+00:00 FINISHED        demand           SA1          53565        77.254011                   133.899667                          0.423046
a2ebb855d15b4a34b6048385af7f761f                  price_SA1 2026-11-09 04:44:59.447000+00:00 FINISHED         price           SA1          53565        38.141157                    39.939028                          0.045015
0c9cbfde3a6444fea89cbc9a0125c293                demand_VIC1 2026-11-09 04:44:37.610000+00:00 FINISHED        demand          VIC1          53565       166.785766                   301.462120                          0.446744
f5d7dc2c381944ff9d1e1e76d7685ba8                 price_VIC1 2026-11-09 04:44:12.348000+00:00 FINISHED         price          VIC1          53565        16.149201                    18.382975                          0.121513
63a63a4f0b124d56ba3211c2ad759cd2                demand_QLD1 2026-11-09 04:43:47.658000+00:00 FINISHED        demand          QLD1          53565       168.297128                   313.206280                          0.462664
a4b2dddc471847bfb872e6c8faed39ab                 price_QLD1 2026-11-09 04:43:19.207000+00:00 FINISHED         price          QLD1          53565        12.005873                    13.511753                          0.111450
5b023657048d42518e481d2dabdfca09                demand_NSW1 2026-11-09 04:42:55.215000+00:00 FINISHED        demand          NSW1          53565       204.531862                   381.979129                          0.464547
13bcce5c5684408c9981b7bead02f994                 price_NSW1 2026-11-09 04:42:27.144000+00:00 FINISHED         price          NSW1          53565        12.827998                    14.564736                          0.119243
24865aa8c780473ebc17bdc4506c6891 nem_forecast_20261109_0442 2026-11-09 04:42:10.656000+00:00 FINISHED          None          None           None              NaN                          NaN                               NaN
37b3312cd2434a01abd8a2a2fb89a2b2                demand_TAS1 2026-11-09 04:40:29.558000+00:00 FINISHED        demand          TAS1          53565        28.662448                    42.572324                          0.326735
```

## Cell 10

```python
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


=== Conversation transcript — interaction 0b7fc9b7-56b1-4454-8b96-a37bb572e0bf (2026-11-09 09:00:15, databricks-claude-sonnet-4-6) ===
USER: Using your tools, give me the current SA1 and NSW1 spot prices and the models 1-hour-ahead price forecast for both regions. Then tell me whether the forecast suggests the desk should be worried about a spike.

TOOL CALL (round 1): query_spot_prices({"region": "SA1"})
TOOL RESULT (289 chars, first 600): {"source": "gold.nem_prices_5min (AEMO NEMWEB DispatchIS)", "window_hours": 1, "regions": [{"region_id": "SA1", "latest_rrp": 115.89764, "as_of_utc": "2026-11-09T08:55:00.000Z", "avg_rrp": 119.83, "min_rrp": 112.45, "max_rrp": 137.44, "intervals_over_300": 0, "latest_demand_mw": 1384.0}]}

TOOL CALL (round 1): query_spot_prices({"region": "NSW1"})
TOOL RESULT (286 chars, first 600): {"source": "gold.nem_prices_5min (AEMO NEMWEB DispatchIS)", "window_hours": 1, "regions": [{"region_id": "NSW1", "latest_rrp": 121.01, "as_of_utc": "2026-11-09T08:55:00.000Z", "avg_rrp": 117.17, "min_rrp": 107.3, "max_rrp": 130.25, "intervals_over_300": 0, "latest_demand_mw": 8227.0}]}

TOOL CALL (round 1): query_price_forecasts({"region": "SA1", "hours": 1})
TOOL RESULT (1193 chars, first 600): {"source": "gold.price_forecasts (UC model serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_sa1 v6)", "region": "SA1", "forecasts": [{"interval_datetime": "2026-11-09T09:25:00.000Z", "horizon_minutes": 30, "forecast": 101.96, "lower_80": 71.07, "upper_80": 128.67, "spike_probability": 0.0, "forecast_run_at": "2026-11-09T08:57:23.866Z", "model_name": "serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_sa1", "model_version": "6"}, {"interval_datetime": "2026-11-09T09:55:00.000Z", "horizon_minutes": 60, "forecast": 97.29, "lower_80": 56.06, "upper_80": 132.44, "spike_probability": 0.0, 

TOOL CALL (round 1): query_price_forecasts({"region": "NSW1", "hours": 1})
TOOL RESULT (1195 chars, first 600): {"source": "gold.price_forecasts (UC model serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_nsw1 v6)", "region": "NSW1", "forecasts": [{"interval_datetime": "2026-11-09T09:25:00.000Z", "horizon_minutes": 30, "forecast": 115.8, "lower_80": 101.75, "upper_80": 145.7, "spike_probability": 0.0, "forecast_run_at": "2026-11-09T08:57:23.866Z", "model_name": "serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_nsw1", "model_version": "6"}, {"interval_datetime": "2026-11-09T09:55:00.000Z", "horizon_minutes": 60, "forecast": 114.88, "lower_80": 98.7, "upper_80": 151.98, "spike_probability": 0.0

ASSISTANT (23395 in / 1158 out tokens, 41629 ms):
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
- **Model caveat**: At the 30-min horizon, SA1s model actually has *negative skill vs. persistence* (MAE 32.35 vs. persistence 29.64), meaning it is slightly *worse* than a naïve carry-forward at short horizons. Treat the 30-min forecast with caution.

### NSW1 — **Low Concern**
- Prices are forecast to hold relatively flat: $121 now → ~$115 over the next hour. The 80% CI upper bound of $151.98 is elevated but not alarming.
- Spike probability is **0.0%**, and the model has **positive skill vs. persistence** at both 30-min and 60-min horizons (skill scores +0.04 and +0.12 respectively) — its a more reliable signal here.
- No spike events in the last 24 hours; intraday range has been contained ($-10 to $146).

---

## 🧭 Bottom Line for the Desk

| | SA1 | NSW1 |
|---|---|---|
| **Spike risk (model)** | 🟡 Low (0%) but history warns | 🟢 Low (0%), model reliable |
| **Price direction** | ↘ Softening | ↔ Broadly flat |
| **Key watch** | SA1 has spiked to $3,290 today — stay alert for sudden reversals | NSW1 upper CI of $152 warrants watching if demand surges |

**Recommendation:** The model is not flashing a spike warning in either region right now, but **SA1 deserves active monitoring** given its extreme intraday volatility today (std dev $225/MWh, 20 spikes). Consider setting a live price alert on SA1 at, say, $300/MWh if one isnt already armed.
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
conversation 01f1dc59262a195d808c641ca73a45d8 | message 01f1dc5926411ff484c8a741a094efc3 | status COMPLETED
QUESTION: For October 2026, which NEM region had the most 5-minute intervals with a spot price above 300 dollars per MWh, how many were there, and what was its maximum price?

SQL GENERATED BY GENIE:
WITH region_counts AS (
  SELECT
    `region_id`,
    COUNT(*) AS `interval_count_above_300`,
    MAX(`rrp`) AS `max_price_aud_mwh`
  FROM `serverless_stable_2b9lj3_catalog`.`gold`.`nem_prices_5min`
  WHERE `interval_datetime` >= TIMESTAMP '2026-10-01 00:00:00'
    AND `interval_datetime` < TIMESTAMP '2026-11-01 00:00:00'
    AND `rrp` IS NOT NULL
    AND `region_id` IS NOT NULL
    AND `rrp` > 300
  GROUP BY `region_id`
), ranked AS (
  SELECT
    `region_id`,
    `interval_count_above_300`,
    `max_price_aud_mwh`,
    RANK() OVER (ORDER BY `interval_count_above_300` DESC) AS `rnk`
  FROM region_counts
)
SELECT
  `region_id`,
  `interval_count_above_300`,
  `max_price_aud_mwh`
FROM ranked
WHERE `rnk` <= 1
ORDER BY `interval_count_above_300` DESC, `region_id` ASC

RESULT: ['region_id', 'interval_count_above_300', 'max_price_aud_mwh'] [['NSW1', '87', '23200.0']]

GENIE ANSWER: For **October 2026**, **NSW1** had the most 5-minute intervals with a spot price above **$300/MWh**, with **87** intervals. The maximum spot price in **NSW1** during those intervals was **$23,200.0/MWh**.

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
