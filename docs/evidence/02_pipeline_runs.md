# Evidence 02 — Live data pipeline and data-quality enforcement

**Workspace:** `fevm-serverless-stable-2b9lj3.cloud.databricks.com` · **Catalog:** `serverless_stable_2b9lj3_catalog`
**Captured:** 2026-11-09 · Raw outputs: [`raw/job_runs.txt`](raw/job_runs.txt), [`raw/data_quality.txt`](raw/data_quality.txt)

## What runs

| Job | Code | Schedule | What it does |
|---|---|---|---|
| **01 — NEMWEB ingest** (`741084016505266`) | [`pipelines/01_nemweb_price_ingest.py`](../../pipelines/01_nemweb_price_ingest.py) → [`pipelines/13_nemweb_bronze_to_gold.py`](../../pipelines/13_nemweb_bronze_to_gold.py) | every 5 min | Downloads new AEMO DispatchIS files and lands `DISPATCH,PRICE` + `REGIONSUM` rows in bronze. Then applies data-quality rules, quarantines failures and MERGEs valid rows into `gold.nem_prices_5min` |
| **36 — Archive backfill** (`522353803163446`) | [`pipelines/36_nemweb_archive_backfill.py`](../../pipelines/36_nemweb_archive_backfill.py) | manual | Loads 90 days of real history from the AEMO archive through the same parser ([`nemweb_parse.py`](../../pipelines/nemweb_parse.py)) and the same DQ task |

## Recent runs (job 01, every 5 minutes)

| run_id | start (UTC) | duration (s) | result |
|---|---|---|---|
| 230199744739803 | 2026-11-09 09:05:03 | 88 | SUCCESS |
| 388125770552595 | 2026-11-09 09:00:05 | 97 | SUCCESS |
| 1080049346804617 | 2026-11-09 08:55:02 | 84 | SUCCESS |
| 715831389980142 | 2026-11-09 08:50:04 | 84 | SUCCESS |

Each run exits with a JSON summary that can be read in the run output. Example from the backfill run `161620000649581`:

```json
{"days_requested": 90, "days_failed": 0, "rows_written": 128860, "files_skipped_already_landed": 148,
 "bronze_coverage_aest": {"first_interval": "2026/08/10 00:05:00", "last_interval": "2026/11/09 12:50:00",
                          "rows": 131805, "files": 26361}}
```

## Result: real data in gold

| region | intervals | first (UTC) | last (UTC) | avg $/MWh | max | min |
|---|---|---|---|---|---|---|
| NSW1 | 26,436 | 2026-08-09 14:05 | 2026-11-09 09:05 | 89.54 | 23,200.00 | −23.50 |
| QLD1 | 26,436 | 2026-08-09 14:05 | 2026-11-09 09:05 | 70.51 | 860.05 | −245.64 |
| SA1 | 26,436 | 2026-08-09 14:05 | 2026-11-09 09:05 | 50.91 | 3,290.00 | −496.77 |
| TAS1 | 26,436 | 2026-08-09 14:05 | 2026-11-09 09:05 | 54.53 | 490.01 | −1,000.00 |
| VIC1 | 26,436 | 2026-08-09 14:05 | 2026-11-09 09:05 | 50.44 | 406.11 | −365.23 |

The NSW1 maximum of $23,200/MWh is the AEMC market price cap for FY2026-27, reached on 10 Oct 2026.

## Data-quality rules (enforced on every run)

Rules live in `DQ_RULES` in `13_nemweb_bronze_to_gold.py`. Every run writes one row per rule to `gold.dq_results`. Rows that fail go to
`gold.nem_prices_quarantine` with the names of the rules they failed. A **freshness** rule makes the job fail if the newest real interval is
more than 30 minutes old.

Totals across all 93 runs so far:

| rule | runs | rows checked | rows failed |
|---|---|---|---|
| interval_not_null | 93 | 266,865 | 0 |
| interval_on_5min_grid | 93 | 266,865 | 0 |
| valid_nem_region | 93 | 266,865 | 0 |
| rrp_is_numeric | 93 | 266,865 | 0 |
| rrp_within_market_limits | 93 | 266,865 | **3** |
| demand_within_physical_bounds | 90 | 263,985 | 0 |
| demand_non_negative *(retired)* | 3 | 2,880 | **102** |
| freshness | 93 | 93 | 0 |

Gold also has `CHECK` constraints (`valid_nem_region`, `rrp_within_market_limits` −1000 … 23,200, `interval_not_null`), so a bad write
fails at the table even if it gets past the pipeline.

### The rules caught real issues, and both were investigated

| quarantined | rule | what it turned out to be | action |
|---|---|---|---|
| 102 SA1 intervals | `demand_non_negative` | SA1 operational demand really does go negative at midday under rooftop solar (min −238 MW) | The rule was wrong for the NEM. It was replaced with a physical bound (−2,000 … 20,000 MW) and the rows were reprocessed |
| 3 NSW1 intervals | `rrp_within_market_limits` | $23,107–$23,200/MWh on 10 and 12 Oct. The FY2026-27 market price cap is $23,200 (AEMC). The rule used last year's $20,300 | The cap parameter and the CHECK constraint were updated to $23,200 and the rows were reprocessed |

The quarantined rows are kept as an audit trail.

## Lineage (captured automatically by Unity Catalog)

`system.access.table_lineage` records `bronze.nemweb_dispatch_price → gold.nem_prices_5min` and `→ gold.nem_prices_quarantine`
(type JOB), plus downstream edges from `gold.nem_prices_5min` into the alert views. See [01_governance.md](01_governance.md) §7.

## What was broken before this fix (for transparency)

Before this change, job 01 ran a Lakeflow pipeline definition as a plain notebook task. It reported SUCCESS every 5 minutes but wrote
**0 rows**. The gold prices were a 14-day backfill/simulator set ending 2026-10-01. The freshness rule above exists so that this kind of
silent failure now fails the job instead.
