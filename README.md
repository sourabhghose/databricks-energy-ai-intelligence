# AUS Energy Copilot

![Python 3.11](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![React 18](https://img.shields.io/badge/React-18-61dafb?logo=react)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?logo=fastapi)
![Databricks](https://img.shields.io/badge/Databricks-Runtime%2015.4-FF3621?logo=databricks)
![Claude Sonnet 4.6](https://img.shields.io/badge/Claude-Sonnet%204.6-8B5CF6?logo=anthropic)
![License MIT](https://img.shields.io/badge/License-MIT-green)

AI-powered market intelligence for the Australian National Electricity Market — built entirely on Databricks.

> **Executed notebooks with outputs:** [`notebooks/executed/`](notebooks/executed/README.md): ingestion landing rows, model training log + MLflow runs, and [conversation transcripts](notebooks/executed/conversation_transcripts.md) (AI assistant tool calls with returned rows; Genie SQL + result rows).
>
> **Reviewers — start with [`EVIDENCE.md`](EVIDENCE.md)** (real notebook output cells, displayed query results and a captured API response)
> **and [`GOVERNANCE.sql`](GOVERNANCE.sql)** (the Unity Catalog row filter, column mask, grants and CHECK constraints).
> The governance is implemented in [`setup/30_governance.py`](setup/30_governance.py) and served live by
> [`app/routers/governance.py`](app/routers/governance.py) (`GET /api/governance/status`); data-quality rules are in
> [`pipelines/13_nemweb_bronze_to_gold.py`](pipelines/13_nemweb_bronze_to_gold.py).
>
> Every step below links to the code that implements it and to the captured run output that proves it ran.
> All evidence was captured on 2026-11-09 on `fevm-serverless-stable-2b9lj3.cloud.databricks.com`
> (catalog `serverless_stable_2b9lj3_catalog`). Start here: **[docs/evidence/README.md](docs/evidence/README.md)**
>
> **Real notebook run outputs** (exported job runs with every cell's printed output): **[docs/evidence/runs/](docs/evidence/runs/README.md)**.
> Start with the one-run summary [`docs/evidence/runs/evidence_verification_…md`](docs/evidence/runs/evidence_verification_evidence_verification_709237712614405.md),
> which shows DQ totals, UC row filter and mask, lineage, model metrics, live forecasts, the AI assistant audit log and a Genie answer, all executed in Databricks.

## Data journey: raw → governed → AI, with an artifact for every step

| # | Step | Implemented in | Proof it ran |
|---|---|---|---|
| 1 | **Ingest** real AEMO NEMWEB DispatchIS every 5 min → `bronze.nemweb_dispatch_price` | [`pipelines/01_nemweb_price_ingest.py`](pipelines/01_nemweb_price_ingest.py), [`pipelines/nemweb_parse.py`](pipelines/nemweb_parse.py) | [job runs](docs/evidence/raw/job_runs.txt) |
| 2 | **Data-quality rules** + quarantine + freshness rule → `gold.nem_prices_5min`, `gold.dq_results`, `gold.nem_prices_quarantine` | [`pipelines/13_nemweb_bronze_to_gold.py`](pipelines/13_nemweb_bronze_to_gold.py) (`DQ_RULES`) | [DQ results](docs/evidence/raw/data_quality.txt), [02_pipeline_runs.md](docs/evidence/02_pipeline_runs.md) |
| 3 | **Unity Catalog governance:** grants, row filter, column mask, CHECK constraints, tags, lineage | [`GOVERNANCE.sql`](GOVERNANCE.sql), [`setup/30_governance.py`](setup/30_governance.py) | [row-filter results](docs/evidence/raw/rowfilter_as_trader.txt), [controls](docs/evidence/raw/governance_controls.txt), [01_governance.md](docs/evidence/01_governance.md) |
| 4 | **Models:** LightGBM price/demand per region, held-out back-test vs persistence, registered in UC, live inference every 5 min | [`models/nem_forecast/train.py`](models/nem_forecast/train.py), [`pipelines/05_forecast_pipeline.py`](pipelines/05_forecast_pipeline.py) | [model registry + metrics](docs/evidence/raw/models.txt), [03_models.md](docs/evidence/03_models.md) |
| 5 | **App + AI:** FastAPI/React Databricks App; Claude assistant with tools on gold, audit-logged; Genie spaces | [`app/routers/copilot.py`](app/routers/copilot.py), [`app/routers/copilot_tools.py`](app/routers/copilot_tools.py) | [assistant answer](docs/evidence/raw/copilot_answer_0b7fc9b7.txt), [audit log](docs/evidence/raw/copilot_interactions.txt), [Genie](docs/evidence/raw/genie_conversation.json), [04_ai_assistant_and_genie.md](docs/evidence/04_ai_assistant_and_genie.md) |

## Key evidence, inline

### Row-level security on the trading book (Unity Catalog row filter)

```sql
CREATE OR REPLACE FUNCTION tools.trades_row_filter(p_portfolio_id STRING) RETURNS BOOLEAN
RETURN EXISTS (SELECT 1 FROM gold.desk_entitlements e
               WHERE (e.portfolio_id = '*' OR e.portfolio_id = p_portfolio_id)
                 AND (is_account_group_member(e.principal) OR e.principal = current_user()));
ALTER TABLE gold.trades SET ROW FILTER tools.trades_row_filter ON (portfolio_id);
ALTER TABLE gold.counterparties ALTER COLUMN credit_limit_aud SET MASK tools.mask_credit_limit;
```

The same user ran the same query. The only change is the group membership:

| caller's groups | trades returned | credit limits |
|---|---|---|
| `energy_traders` | **42** (Hedge Book 14, Trading Book 28; Renewable PPAs hidden) | masked (NULL) |
| `energy_traders` + `energy_risk` | **45** (whole book) | visible (e.g. Snowy Hydro 1.0E9) |

Registered policies (`information_schema`): `row_filter | trades | tools.trades_row_filter | portfolio_id` and
`column_mask | counterparties | tools.mask_credit_limit | credit_limit_aud`.

### Data-quality rules: 93 pipeline runs, results per rule (`gold.dq_results`)

| rule | rows checked | rows failed |
|---|---|---|
| interval_not_null / interval_on_5min_grid / valid_nem_region / rrp_is_numeric | 266,865 each | 0 |
| rrp_within_market_limits | 266,865 | 3 → NSW1 $23,200 = FY2026-27 market price cap; the rule was updated and the rows reprocessed |
| demand_within_physical_bounds | 263,985 | 0 (replaced `demand_non_negative`, which wrongly quarantined 102 real negative SA1 demand intervals) |
| freshness (fails the job if data stops arriving) | 93 runs | 0 |

### Lineage (captured automatically in `system.access.table_lineage`)

| source | target | via |
|---|---|---|
| `bronze.nemweb_dispatch_price` | `gold.nem_prices_5min` | JOB |
| `bronze.nemweb_dispatch_price` | `gold.nem_prices_quarantine` | JOB |

### Pipeline and model runs

- Ingest → DQ → gold, every 5 min: job `741084016505266`, e.g. runs `230199744739803`, `388125770552595` (SUCCESS, ~85 s)
- 90-day archive backfill: run `161620000649581`, 128,860 rows, 0 failed days
- 10 models in Unity Catalog `ml.nem_{price|demand}_forecast_{region}` @ `production` v6; MLflow experiment `/Shared/energy_copilot_nem_forecast`
- Held-out test vs persistence: **demand 19–60% lower error in every region; price 14–39% lower at 4 h** (near persistence at 30–60 min).
  The spike classifier is not yet useful and is reported as such.

### AI doing real work

- **Assistant** (Claude Sonnet 4.6 via Foundation Model API), interaction `0b7fc9b7-56b1-4454-8b96-a37bb572e0bf`. It called
  `query_spot_prices` ×2 and `query_price_forecasts` ×2 on live gold data and answered with SA1 $115.90 now, 1-h forecast $97.29 from UC model v6,
  and "SA1 hit $3,290 today with 20 intervals > $300". All of these match an independent query. Every turn is logged to `gold.copilot_interactions`.
- **Genie**, space *NEM Spot Market Intelligence*, conversation `01f1dc1cb03e13b9911787894150f467`. Question: "October 2026: region with most
  intervals > $300?" Genie generated SQL on `gold.nem_prices_5min` and returned `NSW1, 87, 23200.0`, which matches an independent query.

## Data honesty

No endpoint generates random values and presents them as data. Where no real source is ingested (AER opex benchmarking, battery
dispatch, FCAS, merit order, demand response), the API returns **HTTP 503 with the reason**. The remaining illustrative dashboards are
tagged `data_source: "sample"` and show a "Sample data" banner in the app.

📖 **[Full Documentation →](https://sourabhghose.github.io/databricks-energy-ai-intelligence)**

## Authors

Built by **Beyza Yalavac** (beyza.yalavac@databricks.com) and **Sourabh Ghose** (sourabh.ghose@databricks.com), Databricks Field Engineering.

## License

MIT.
