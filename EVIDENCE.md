# EVIDENCE — Energy Trading Copilot: governance and execution, with real outputs

Everything below was **printed by real Databricks runs or returned by the app's API**. It is copied verbatim from the exported
job-run notebooks in [`docs/evidence/runs/`](docs/evidence/runs/README.md) (HTML with outputs, plus Markdown transcripts).
Workspace `fevm-serverless-stable-2b9lj3.cloud.databricks.com`, catalog `serverless_stable_2b9lj3_catalog`, 2026-11-09.

Data journey: **AEMO NEMWEB → bronze → data-quality rules + Unity Catalog policies (governed step) → gold → models → app / AI**

---

## 1. Unity Catalog governance — the code

The policies are created by [`setup/30_governance.py`](setup/30_governance.py) (job "40 — UC Governance"). The same
statements are in plain SQL in [`GOVERNANCE.sql`](GOVERNANCE.sql), and the app reads them back live through
[`app/routers/governance.py`](app/routers/governance.py) (`GET /api/governance/status`).

```sql
-- Row-level security: each desk sees only its own book
CREATE OR REPLACE FUNCTION tools.trades_row_filter(p_portfolio_id STRING) RETURNS BOOLEAN
RETURN EXISTS (SELECT 1 FROM gold.desk_entitlements e
               WHERE (e.portfolio_id = '*' OR e.portfolio_id = p_portfolio_id)
                 AND (is_account_group_member(e.principal) OR e.principal = current_user()));
ALTER TABLE gold.trades SET ROW FILTER tools.trades_row_filter ON (portfolio_id);

-- Column-level security: credit limits for Risk and Compliance only
CREATE OR REPLACE FUNCTION tools.mask_credit_limit(v DOUBLE) RETURNS DOUBLE
RETURN CASE WHEN is_account_group_member('energy_risk') OR is_account_group_member('energy_compliance') THEN v ELSE NULL END;
ALTER TABLE gold.counterparties ALTER COLUMN credit_limit_aud SET MASK tools.mask_credit_limit;

-- Least-privilege grants
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `energy_traders`;
GRANT MODIFY ON TABLE gold.trades TO `energy_traders`;
GRANT MODIFY ON TABLE gold.copilot_interactions TO `<app service principal>`;

-- Data-quality constraints on the ingested price table
ALTER TABLE gold.nem_prices_5min ADD CONSTRAINT rrp_within_market_limits CHECK (rrp BETWEEN -1000.0 AND 23200.0);
ALTER TABLE gold.nem_prices_5min ADD CONSTRAINT valid_nem_region CHECK (region_id IN ('NSW1','QLD1','VIC1','SA1','TAS1'));
```

Data-quality expectations, applied to every ingested batch
([`pipelines/13_nemweb_bronze_to_gold.py`](pipelines/13_nemweb_bronze_to_gold.py)). Failing rows go to
`gold.nem_prices_quarantine`, per-rule counts go to `gold.dq_results`, and a freshness rule fails the job when data stops:

```python
DQ_RULES = (
    ("interval_not_null", "interval_datetime IS NOT NULL"),
    ("interval_on_5min_grid", "minute(interval_datetime) % 5 = 0 AND second(interval_datetime) = 0"),
    ("valid_nem_region", f"region_id IN {NEM_REGIONS}"),
    ("rrp_is_numeric", "rrp IS NOT NULL"),
    ("rrp_within_market_limits", f"rrp BETWEEN {PRICE_FLOOR} AND {PRICE_CAP}"),
    ("demand_within_physical_bounds", "total_demand_mw IS NULL OR total_demand_mw BETWEEN -2000 AND 20000"),
)
```

## 2. Unity Catalog governance — output of the run that applied it

Governance job run `962676474138249` (transcript [`uc_governance_…md`](docs/evidence/runs/uc_governance_governance_44255532387483.md)):

```text
OK  USE CATALOG -> app SP
OK  USE SCHEMA, SELECT gold -> app SP
OK  USE SCHEMA, SELECT ml -> app SP
OK  USE SCHEMA, EXECUTE tools -> app SP
OK  table gold.desk_entitlements
OK  entitlements upserted (6 rows)
OK  function tools.trades_row_filter
OK  row filter on gold.trades
OK  function tools.mask_credit_limit
OK  column mask on gold.counterparties.credit_limit_aud
OK  tags on serverless_stable_2b9lj3_catalog.gold.nem_prices_5min
OK  tags on serverless_stable_2b9lj3_catalog.gold.trades
OK  tags on serverless_stable_2b9lj3_catalog.gold.counterparties
OK  column tag credit_limit_aud
```

## 3. Unity Catalog governance — read back from the workspace (displayed query results)

Evidence notebook run `678829619491776` ([`notebooks/evidence_verification.py`](notebooks/evidence_verification.py),
transcript [`evidence_verification_…md`](docs/evidence/runs/evidence_verification_evidence_verification_197619732568832.md)):

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
(the Renewable PPAs book — 3 trades — is filtered out for a member of energy_traders)

=== CHECK constraints on gold.nem_prices_5min ===
|delta.constraints.interval_not_null       |interval_datetime IS NOT NULL                   |
|delta.constraints.rrp_within_market_limits|rrp BETWEEN -1000.0 AND 23200.0                 |
|delta.constraints.valid_nem_region        |region_id IN ('NSW1','QLD1','VIC1','SA1','TAS1')|

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

=== Lineage (system.access.table_lineage) ===
|serverless_stable_2b9lj3_catalog.bronze.nemweb_dispatch_price|serverless_stable_2b9lj3_catalog.gold.nem_prices_5min      |JOB|2026-11-09 10:56:14.819|
|serverless_stable_2b9lj3_catalog.bronze.nemweb_dispatch_price|serverless_stable_2b9lj3_catalog.gold.nem_prices_quarantine|JOB|2026-11-09 02:50:42.386|
```

When a trader is also added to `energy_risk`, the same query returns all **45** trades, and credit limits are no longer masked
([`docs/evidence/raw/rowfilter_as_risk.txt`](docs/evidence/raw/rowfilter_as_risk.txt)).

### Captured API response — `GET /api/governance/status` (excerpt; full: [`docs/evidence/raw/api_governance_status.json`](docs/evidence/raw/api_governance_status.json))

```json
{"row_filters_and_masks": [
   {"kind": "row_filter", "table_name": "trades", "policy": "serverless_stable_2b9lj3_catalog.tools.trades_row_filter", "columns": "portfolio_id"},
   {"kind": "column_mask", "table_name": "counterparties", "policy": "serverless_stable_2b9lj3_catalog.tools.mask_credit_limit", "columns": "credit_limit_aud"}],
 "trades_visible_to_caller": [
   {"identity": "sourabh.ghose@databricks.com", "portfolio": "Hedge Book", "trades": 14},
   {"identity": "sourabh.ghose@databricks.com", "portfolio": "Trading Book", "trades": 28}],
 "lineage": [{"source_table_full_name": "serverless_stable_2b9lj3_catalog.bronze.nemweb_dispatch_price",
              "target_table_full_name": "serverless_stable_2b9lj3_catalog.gold.nem_prices_5min", "entity_type": "JOB"}]}
```

## 4. Pipeline runs — notebook output cells

Data-quality run that **caught** bad data (job run `645430909659288`, [transcript](docs/evidence/runs/dq_quarantine_sa1_bronze_to_gold_dq_642097095889966.md)):

```text
Checked 2875 rows | quarantined 102 | rule failures: {'interval_not_null': 0, 'interval_on_5min_grid': 0, 'valid_nem_region': 0, 'rrp_is_numeric': 0, 'rrp_within_market_limits': 0, 'demand_non_negative': 102}
Merged 2773 valid rows into serverless_stable_2b9lj3_catalog.gold.nem_prices_5min
```

(The 102 rows were real negative SA1 demand under rooftop solar. The rule was replaced by a physical bound and the rows were reprocessed: `rows_checked 2885, rows_quarantined 0`.)

Live 5-minute ingest (job run `230199744739803`, [ingest](docs/evidence/runs/ingest_dq_latest_nemweb_ingest_205360817597398.md), [DQ](docs/evidence/runs/ingest_dq_latest_bronze_to_gold_dq_640394667412261.md)):

```text
Remote files: 575 | already landed: 26435 | to fetch: 1
Parsed 5 price rows from 1 files
{"rows_checked": 5, "rows_merged": 5, "rows_quarantined": 0, ..., "latest_real_interval_utc": "2026-11-09 09:05:00", "staleness_minutes": 1.1, "freshness": "PASS"}
```

90-day archive backfill (job run `161620000649581`, [transcript](docs/evidence/runs/archive_backfill_archive_backfill_963953110718303.md)):

```text
{"days_requested": 90, "days_failed": 0, "failed": [], "rows_written": 128860, "files_skipped_already_landed": 148,
 "bronze_coverage_aest": {"first_interval": "2026/08/10 00:05:00", "last_interval": "2026/11/09 12:50:00", "rows": 131805, "files": 26361}}
```

## 5. Model runs — notebook output cells

Training + held-out back-test + Unity Catalog registration (job run `940474602766977`, [transcript](docs/evidence/runs/model_training_train_243588330407321.md)).
h6 / h12 / h48 = 30 min / 1 h / 4 h ahead; persistence = latest observed value:

```text
NSW1 price: v6 | h6 MAE 10.8 (persistence 11.2), h12 MAE 12.8 (persistence 14.6), h48 MAE 18.0 (persistence 28.4)
QLD1 price: v6 | h6 MAE 9.6 (persistence 9.9), h12 MAE 12.0 (persistence 13.5), h48 MAE 19.8 (persistence 32.2)
SA1 price: v6 | h6 MAE 32.4 (persistence 29.6), h12 MAE 38.1 (persistence 39.9), h48 MAE 52.1 (persistence 80.0)
TAS1 price: v6 | h6 MAE 17.1 (persistence 13.6), h12 MAE 20.1 (persistence 18.1), h48 MAE 31.7 (persistence 36.7)
VIC1 price: v6 | h6 MAE 12.9 (persistence 13.3), h12 MAE 16.1 (persistence 18.4), h48 MAE 26.2 (persistence 40.0)
NSW1 demand: v6 | h6 MAE 132.6 (persistence 205.2), h12 MAE 204.5 (persistence 382.0), h48 MAE 493.2 (persistence 1236.1)
QLD1 demand: v6 | h6 MAE 102.4 (persistence 167.9), h12 MAE 168.3 (persistence 313.2), h48 MAE 433.1 (persistence 1010.1)
SA1 demand: v6 | h6 MAE 51.8 (persistence 76.8), h12 MAE 77.3 (persistence 133.9), h48 MAE 161.3 (persistence 391.7)
TAS1 demand: v6 | h6 MAE 20.0 (persistence 24.6), h12 MAE 27.9 (persistence 42.6), h48 MAE 55.2 (persistence 124.8)
VIC1 demand: v6 | h6 MAE 106.1 (persistence 168.8), h12 MAE 166.8 (persistence 301.5), h48 MAE 404.5 (persistence 876.6)
```

Live inference with the registered models (job run `481037869311618`, [transcript](docs/evidence/runs/live_inference_forecast_pipeline_517031954681478.md)):

```text
"price_forecasts": 15, "demand_forecasts": 15, "sample": [{"interval_datetime": "2026-11-09 09:30:00", "region_id": "NSW1", "horizon_intervals": 6, "model_version": "6", "predicted_rrp": 108.72689068444309, ...
```

The spike classifier is **not yet useful** (held-out AUC 0.50 in SA1). It is reported as such and no spike-recall claim is made.

## 6. AI assistant and Genie — real outputs

AI assistant audit log (`gold.copilot_interactions`), read back in the evidence notebook run:

```text
=== Latest turn 0b7fc9b7-56b1-4454-8b96-a37bb572e0bf — tool calls ===
round 1: query_spot_prices({"region": "SA1"}) -> {"source": "gold.nem_prices_5min (AEMO NEMWEB DispatchIS)", "window_hours": 1, "regions": [{"region_id": "SA1", "latest_rrp": 115.89764, ...
round 1: query_spot_prices({"region": "NSW1"}) -> {"source": "gold.nem_prices_5min (AEMO NEMWEB DispatchIS)", "window_hours": 1, "regions": [{"region_id": "NSW1", "latest_rrp": 121.01, ...
round 1: query_price_forecasts({"region": "SA1", "hours": 1}) -> {"source": "gold.price_forecasts (UC model serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_sa1 v6)", ...
round 1: query_price_forecasts({"region": "NSW1", "hours": 1}) -> {"source": "gold.price_forecasts (UC model serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_nsw1 v6)", ...
```

Genie question asked inside the same Databricks run:

```text
conversation 01f1dc2e8e5216498b3afa75f53bb0bb | message 01f1dc2e8e641840ad76967d2b82b139 | status COMPLETED
RESULT: ['region_id', 'interval_count', 'max_price'] [['NSW1', '87', '23200.0']]
GENIE ANSWER: For October 2026, **NSW1** had the most 5-minute intervals with spot prices above $300/MWh, with **87** intervals, and **NSW1** reached a maximum price of **$23,200.0/MWh**.
```

The independent check in the next cell returned `NSW1 | 87 | 23200.0`.

---

All exported runs: [`docs/evidence/runs/README.md`](docs/evidence/runs/README.md) · Full evidence pack: [`docs/evidence/README.md`](docs/evidence/README.md)
