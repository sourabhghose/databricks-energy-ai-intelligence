# Evidence 01 — Unity Catalog governance

**Workspace:** `fevm-serverless-stable-2b9lj3.cloud.databricks.com` · **Catalog:** `serverless_stable_2b9lj3_catalog`
**Captured:** 2026-11-09 (AEDT) · **Code:** [`setup/30_governance.py`](../../setup/30_governance.py), job `40 — UC Governance` in [`resources/jobs.yml`](../../resources/jobs.yml)

The governance notebook ran as a one-time job (run `962676474138249`, result **SUCCESS**) and applied 20 controls.
Raw outputs are in [`raw/`](raw/).

## 1. Row-level security on the trading book — the same user, the same query, different results

`gold.trades` has the row filter `tools.trades_row_filter(portfolio_id)`, driven by the table `gold.desk_entitlements`:

| Principal | Portfolios visible |
|---|---|
| `energy_traders` | Hedge Book, Trading Book |
| `energy_analysts` | Renewable PPAs |
| `energy_risk`, `energy_compliance` | All portfolios |
| App service principal `83619056-…` | All portfolios |

Query ([`raw/q_trades.sql`](raw/q_trades.sql)): trades per portfolio, together with the caller's group membership.

**As a member of `energy_traders` only** ([`raw/rowfilter_as_trader.txt`](raw/rowfilter_as_trader.txt)):

| user | in_traders | in_risk | portfolio | trades |
|---|---|---|---|---|
| sourabh.ghose@databricks.com | true | false | Hedge Book | 14 |
| sourabh.ghose@databricks.com | true | false | Trading Book | 28 |

**After being added to `energy_risk`** ([`raw/rowfilter_as_risk.txt`](raw/rowfilter_as_risk.txt)):

| user | in_traders | in_risk | portfolio | trades |
|---|---|---|---|---|
| sourabh.ghose@databricks.com | true | true | Hedge Book | 14 |
| sourabh.ghose@databricks.com | true | true | **Renewable PPAs** | **3** |
| sourabh.ghose@databricks.com | true | true | Trading Book | 28 |

The trader sees 42 of the 45 trades; the Renewable PPAs book is hidden. The filter also applies through the view
`gold.trades_synced`, which the app reads: a trader gets 42 rows from both the table and the view
([`raw/rowfilter_view_check.txt`](raw/rowfilter_view_check.txt)). The `energy_risk` membership was removed after the test.

## 2. Column mask on counterparty credit limits

`gold.counterparties.credit_limit_aud` has the mask `tools.mask_credit_limit`: values are shown only to Risk, Compliance and the app.

| name | credit_rating | as trader | as risk |
|---|---|---|---|
| AGL Energy | BBB+ | *null* | 500,000,000 |
| EnergyAustralia | BBB | *null* | 350,000,000 |
| Iberdrola Australia | BBB+ | *null* | 300,000,000 |
| Origin Energy | BBB | *null* | 400,000,000 |
| Snowy Hydro | AAA | *null* | 1,000,000,000 |

## 3. Policies registered in Unity Catalog (`information_schema`)

| kind | table | policy | column |
|---|---|---|---|
| row_filter | trades | `tools.trades_row_filter` | portfolio_id |
| column_mask | counterparties | `tools.mask_credit_limit` | credit_limit_aud |

## 4. Least-privilege grants (`SHOW GRANTS`)

The governance script grants the app's service principal `USE SCHEMA` + `SELECT` on `gold` and `ml`, and `EXECUTE` on `tools`.
It grants **nothing on `bronze`**: `SHOW GRANTS ON SCHEMA bronze` lists only the catalog owner and the metastore-level `READ METADATA`
([`raw/governance_controls.txt`](raw/governance_controls.txt)).

From `information_schema.table_privileges`, the principal also still holds table-level grants made by earlier setup scripts:

| privilege | tables | why |
|---|---|---|
| `MODIFY` (12 gold tables) | alert_history, alert_rules, anomaly_explanations, approval_requests, approval_rules, constraint_forecasts, **copilot_interactions**, market_briefs, outage_events, trading_signal_config, trading_signals, vpp_dispatch_events | tables the app writes to (alerts, approvals, signals, the AI audit log) |
| `SELECT` (13 silver tables) | bids, dispatch_constraints, dispatch_generation, dispatch_interconnectors, dispatch_prices, generation_by_fuel, generator_registry, openelec_generation, openelec_prices, predispatch_prices, solar_rooftop, trading_prices, weather_nem_regions | legacy read access, **not needed by the current app**. Revoking it is a follow-up item |

## 5. CHECK constraints (writes are rejected if they break a rule)

```
delta.constraints.interval_not_null         interval_datetime IS NOT NULL
delta.constraints.rrp_within_market_limits  rrp BETWEEN -1000.0 AND 23200.0   # FY2026-27 market price cap (AEMC)
delta.constraints.valid_nem_region          region_id IN ('NSW1','QLD1','VIC1','SA1','TAS1')
```

These sit on `gold.nem_prices_5min`, on top of the pipeline's data-quality rules (see [02_pipeline_runs.md](02_pipeline_runs.md)).
`gold.trades` has no CHECK constraints because Unity Catalog does not allow them on a table with a row filter
(`ROW_LEVEL_SECURITY_CHECK_CONSTRAINT_UNSUPPORTED`). Trade fields are validated by the deal-capture API's Pydantic model instead
(`volume_mw: float = Field(..., gt=0)` in `app/routers/deals.py`).

## 6. Classification tags

| table | tags |
|---|---|
| nem_prices_5min | ec_classification=public_market_data, ec_domain=front_office, ec_source=aemo_nemweb |
| trades, counterparties, portfolios | ec_classification=confidential, ec_domain=middle_office |
| dq_results | ec_classification=internal, ec_domain=governance |
| counterparties.credit_limit_aud (column) | ec_sensitivity=restricted |

The tag keys are prefixed `ec_` because the account has a governed tag policy on `data_classification` that only
allows `u-nnpi` / `cui` / `pii`.

## 7. Lineage (captured automatically in `system.access.table_lineage`)

| source | target | via | last seen (UTC) |
|---|---|---|---|
| bronze.nemweb_dispatch_price | gold.nem_prices_5min | JOB | 2026-11-09 02:01 |
| bronze.nemweb_dispatch_price | gold.nem_prices_quarantine | JOB | 2026-11-09 01:42 |

Downstream of `gold.nem_prices_5min`, lineage also records the alert views (`_alert_price_high`, `_alert_data_stale`,
`_alert_model_drift`, …).

## Known limitation

The workspace sits on a shared FEVM account where the builder is not an account admin, so the four role groups are
**workspace-local** groups. Unity Catalog accepts only account principals in `GRANT`, so in this environment:

- the row filter and column mask check membership with `is_member()`; that part is fully enforced, as shown above;
- grants to the groups themselves are skipped (`group_scope=workspace`), and least privilege is shown on the app's service principal.

In a customer account, set the bundle variable `group_scope: account`. The same script then issues `GRANT`s to the
account groups and switches the checks to `is_account_group_member()`.
