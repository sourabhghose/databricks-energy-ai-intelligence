# Execution evidence — Energy Trading Copilot

Everything here was captured from a real run on `fevm-serverless-stable-2b9lj3.cloud.databricks.com` (catalog
`serverless_stable_2b9lj3_catalog`) on 2026-11-09. Each document lists the job run IDs, MLflow run IDs, conversation IDs and table
names, so every claim can be re-checked in the workspace. Raw query outputs are in [`raw/`](raw/).

| # | What it proves | Document |
|---|---|---|
| 01 | **Unity Catalog governance:** row filter (traders see 42 of 45 trades, risk sees all), credit-limit column mask, least-privilege grants, CHECK constraints, tags, automatic lineage | [01_governance.md](01_governance.md) |
| 02 | **Live pipeline + data quality:** 5-minute AEMO NEMWEB ingest, 90-day backfill, 8 DQ rules logged per run (93 runs), quarantine with two investigated rule failures, freshness rule | [02_pipeline_runs.md](02_pipeline_runs.md) |
| 03 | **Models:** LightGBM price/demand per region trained on real data, held-out back-test vs persistence, 10 UC-registered models, live inference every 5 min scored against actuals. Spike classifier reported as not yet useful | [03_models.md](03_models.md) |
| runs | **Notebook outputs from the actual job runs:** ingest, DQ (incl. the run that quarantined 102 rows), backfill, governance, training, inference, and an end-to-end verification run | [runs/README.md](runs/README.md) |
| 04 | **AI doing real work:** a Claude assistant turn with 4 tool calls on live data, logged to an inference table and checked against gold. A Genie question with its generated SQL and result rows, checked independently | [04_ai_assistant_and_genie.md](04_ai_assistant_and_genie.md) |

## Data honesty rules applied across the app

- No endpoint generates random values and presents them as data.
- Where no real source is ingested (AER opex benchmarking, battery dispatch, demand response, merit order/bids, FCAS, rooftop solar
  forecasts, long-range demand), the API returns **HTTP 503 with the reason** and the page shows "Not available in this build".
- Remaining illustrative dashboards (mostly DNSP/network and stub pages) are tagged with `@_sample_data`. They return
  `data_source: "sample"` and an `X-Data-Source: sample` header, and the UI shows an amber **"Sample data"** banner.
