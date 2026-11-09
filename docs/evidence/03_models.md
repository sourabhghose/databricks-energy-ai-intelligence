# Evidence 03 — Forecast models: training, held-out back-test, registry, live inference

**Code:** [`models/nem_forecast/features.py`](../../models/nem_forecast/features.py), [`models/nem_forecast/train.py`](../../models/nem_forecast/train.py),
[`pipelines/05_forecast_pipeline.py`](../../pipelines/05_forecast_pipeline.py) · **Raw:** [`raw/models.txt`](raw/models.txt), [`raw/job_runs.txt`](raw/job_runs.txt)
**MLflow experiment:** `/Shared/energy_copilot_nem_forecast` (id `1440555517568947`), parent run `24865aa8c780473ebc17bdc4506c6891`

## Method

- **Data:** 90 days of real `gold.nem_prices_5min` (Aug 11 – Nov 9 2026). Every feature at time *t* uses only data published at or before *t*.
- **Models:** one LightGBM model per region for **price** (L1 loss) and one for **demand** (L2 loss). Each predicts the *change* from the
  latest observed value. There is also one **spike classifier** (price > $300/MWh) pooled across all regions.
- **Horizons:** 30 min, 1 h and 4 h.
- **Split (chronological):** fit → 14-day validation (used for intervals and the spike threshold) → **14-day held-out test**, which is never
  used for training or tuning.
- **Baseline:** *persistence* (the forecast equals the latest observed value). `skill = 1 − MAE_model / MAE_persistence`.

## Registered in Unity Catalog (alias `production`)

| model | versions | production | MLflow run |
|---|---|---|---|
| `…ml.nem_price_forecast_nsw1` | 6 | v6 | `13bcce5c5684408c9981b7bead02f994` |
| `…ml.nem_price_forecast_sa1` | 6 | v6 | `a2ebb855d15b4a34b6048385af7f761f` |
| `…ml.nem_demand_forecast_vic1` | 6 | v6 | `0c9cbfde3a6444fea89cbc9a0125c293` |
| *(10 models in total, 5 regions × price/demand — full list in raw/models.txt)* | | | |

## Held-out results (test window 26 Oct – 9 Nov 2026, averaged over 5 regions)

| target | horizon | model MAE | persistence MAE | skill range across regions | regions beating persistence | 80% interval coverage |
|---|---|---|---|---|---|---|
| demand | 30 min | 82.6 MW | 128.6 MW | 0.19 – 0.39 | **5 / 5** | 0.78 |
| demand | 1 h | 129.0 MW | 234.6 MW | 0.35 – 0.47 | **5 / 5** | 0.78 |
| demand | 4 h | 309.5 MW | 727.8 MW | 0.54 – 0.60 | **5 / 5** | 0.75 |
| price | 30 min | $16.53 | $15.52 | −0.25 – 0.04 | 3 / 5 | 0.79 |
| price | 1 h | $19.85 | $20.90 | −0.11 – 0.12 | 4 / 5 | 0.80 |
| price | 4 h | $29.54 | $43.45 | 0.14 – 0.39 | **5 / 5** | 0.79 |

**Reading this honestly:** demand forecasting beats the baseline clearly at every horizon. Price forecasting adds real value at 4 hours. At
30–60 minutes it is close to persistence, and slightly worse in Tasmania and South Australia.

### Spike classifier — not yet useful (reported as-is)

| region | test spikes (1 h) | recall | precision | AUC |
|---|---|---|---|---|
| SA1 | 21 | 0.00 | 0.00 | 0.50 |
| TAS1 | 2 | 0.00 | 0.00 | 0.72 |
| NSW1 / QLD1 / VIC1 | 0 | – | – | – |

The test window has only 23 spike intervals. Most spikes in the validation window were *continuations* of a spike that was already
running, so the classifier learned "already spiking" rather than early warning. The chosen threshold collapses to p = 1.0. We did not tune
it further against the test results. The dashboards and the AI assistant label spike probability as a weak signal.

## Live inference (job 05, every 5 minutes)

Job 05 (`670211769373148`) loads `@production` for each region, builds features from the latest real interval, and appends forecasts to
`gold.price_forecasts` / `gold.demand_forecasts`. There is no synthetic fallback: a missing model or stale input fails the job. Recent
runs: `481037869311618`, `909147153735036`, `504284546022095` … all SUCCESS (~65 s).

Live 1-hour price forecasts, scored against the actual prices that have arrived since (50 forecasts per region):

| region | live MAE ($/MWh) | model |
|---|---|---|
| NSW1 | 13.59 | v6 |
| QLD1 | 13.12 | v6 |
| SA1 | 32.29 | v6 |
| TAS1 | 26.69 | v6 |
| VIC1 | 16.84 | v6 |

These are in line with the held-out results, which suggests the back-test is representative.

Latest run (issued 2026-11-09 09:02 UTC), e.g. SA1: 30 min **$99.08** (80% $68.20–$125.79), 1 h **$93.03**, 4 h **$71.07**.

## Where the results are used

`gold.forecast_model_metrics`, `gold.forecast_backtest` and `gold.forecast_feature_importance` feed the forecasting dashboards
([`app/routers/batch_forecasting.py`](../../app/routers/batch_forecasting.py), [`forecast_metrics.py`](../../app/routers/forecast_metrics.py),
[`spike_dashboards.py`](../../app/routers/spike_dashboards.py)). The live forecasts feed the AI assistant's `query_price_forecasts` tool and the
`SPIKE_RISK` trading-signal detector. No dashboard value is randomly generated. Where no real source exists, the endpoint returns HTTP 503
with the reason, or is labelled `data_source: "sample"` with a visible banner.
