# Exported job runs, with real notebook outputs

These are the actual Databricks job runs behind the numbers in the README, the evidence pack and the deck. Each was
exported from the Jobs API with [`scripts/export_run_evidence.py`](../../../scripts/export_run_evidence.py):

- `.md` — plain-text transcript: run metadata, then each cell's code and the output it printed
- `.html` — the notebook exactly as it ran, outputs included (open in a browser)

Workspace `fevm-serverless-stable-2b9lj3.cloud.databricks.com`, captured 2026-11-09.

| What it shows | Job run | Transcript | Key output in the transcript |
|---|---|---|---|
| **All evidence in one Databricks run:** DQ totals, UC policies, row filter applied to the job's identity, CHECK constraints, tags, lineage, model metrics, live forecast scores, AI assistant audit log, a fresh Genie question | `678829619491776` | [evidence_verification](evidence_verification_evidence_verification_197619732568832.md) | `row_filter \| trades \| …tools.trades_row_filter`; trader sees Hedge Book 14 + Trading Book 28; Genie `RESULT: [['NSW1', '87', '23200.0']]` matched by an independent query |
| Live 5-min ingest from AEMO NEMWEB | `230199744739803` | [nemweb_ingest](ingest_dq_latest_nemweb_ingest_205360817597398.md) | `Remote files: 575 \| … to fetch: 1`, `Parsed 5 price rows from 1 files` |
| Data-quality rules + MERGE to gold (same run) | `230199744739803` | [bronze_to_gold_dq](ingest_dq_latest_bronze_to_gold_dq_640394667412261.md) | per-rule failure counts and `"freshness": "PASS"` |
| DQ rule **caught** 102 negative-demand SA1 intervals | `645430909659288` | [bronze_to_gold_dq](dq_quarantine_sa1_bronze_to_gold_dq_642097095889966.md) | `Checked 2875 rows \| quarantined 102 \| … 'demand_non_negative': 102` |
| Reprocess after the rule fix | `252015160022365` | [bronze_to_gold_dq](dq_reprocess_bronze_to_gold_dq_47982450428573.md) | `rows_checked 2885, rows_quarantined 0` |
| 90-day archive backfill | `161620000649581` | [archive_backfill](archive_backfill_archive_backfill_963953110718303.md), [DQ](archive_backfill_bronze_to_gold_dq_304824563792095.md) | `rows_written: 128860, days_failed: 0` |
| Unity Catalog governance applied | `962676474138249` | [uc_governance](uc_governance_governance_44255532387483.md) | `OK  function tools.trades_row_filter`, `OK  row filter on gold.trades`, `OK  column mask …` |
| Model training + held-out back-test + UC registration | `940474602766977` | [model_training](model_training_train_243588330407321.md) | `NSW1 price: v6 \| h12 MAE 12.8 (persistence 14.6) …` for every region and target |
| Live inference with the registered models | `481037869311618` | [live_inference](live_inference_forecast_pipeline_517031954681478.md) | `price_forecasts: 15, demand_forecasts: 15`, per-region predictions from model v6 |
