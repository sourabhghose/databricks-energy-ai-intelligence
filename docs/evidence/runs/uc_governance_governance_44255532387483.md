# Run evidence — uc_governance / task `governance`

| field | value |
|---|---|
| job run id | `962676474138249` |
| task run id | `44255532387483` |
| job | 30 — UC governance (one-time) |
| notebook | `/Workspace/Users/sourabh.ghose@databricks.com/.bundle/energy-copilot/dev/files/setup/30_governance` |
| started | 2026-11-09 02:07:19 UTC |
| result | **SUCCESS** |
| run page | https://fevm-serverless-stable-2b9lj3.cloud.databricks.com/?o=7474644340067319#job/968058769422944/run/44255532387483 |

Exported from the Databricks Jobs API with cell outputs as they were produced during the run.

## Cell 2

```python
import json
import re

dbutils.widgets.text("catalog", "energy_copilot_catalog")
dbutils.widgets.text("traders_group", "energy_traders")
dbutils.widgets.text("risk_group", "energy_risk")
dbutils.widgets.text("compliance_group", "energy_compliance")
dbutils.widgets.text("analysts_group", "energy_analysts")
dbutils.widgets.text("app_principal", "")  # app service principal application id (sees all rows)
# "account": role groups are account groups -> UC GRANTs to groups + is_account_group_member() checks.
# "workspace": role groups are workspace-local (no account-admin rights) -> UC only accepts account
#   principals in GRANTs, so group grants are skipped and the row filter / mask use is_member().
dbutils.widgets.dropdown("group_scope", "account", ["account", "workspace"])
dbutils.widgets.text("market_price_floor", "-1000")
dbutils.widgets.text("market_price_cap", "20300")

IDENT = re.compile(r"[A-Za-z0-9_\-.@]+")


def _param(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if value and not IDENT.fullmatch(value):
        raise ValueError(f"Invalid value for {name}: {value!r}")
    return value

# … 18 more lines
```

## Cell 4

```python
if GROUP_SCOPE == "account":
    for grp in (TRADERS, RISK, COMPLIANCE, ANALYSTS):
        run(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{grp}`", f"USE CATALOG -> {grp}")
        run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {GOLD} TO `{grp}`", f"SELECT gold -> {grp}")
        run(f"GRANT USE SCHEMA, EXECUTE ON SCHEMA {TOOLS} TO `{grp}`", f"EXECUTE tools -> {grp}")
    for grp in (TRADERS, RISK, ANALYSTS):
        run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOG}.ml TO `{grp}`", f"SELECT ml -> {grp}")
    run(f"GRANT MODIFY ON TABLE {GOLD}.trades TO `{TRADERS}`", f"MODIFY gold.trades -> {TRADERS}")
else:
    print("group_scope=workspace: UC GRANTs require account principals; group grants skipped")

# The app's service principal: read gold/ml, execute tools; no access to bronze/silver
if APP_PRINCIPAL:
    run(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{APP_PRINCIPAL}`", "USE CATALOG -> app SP")
    for schema, privs in (("gold", "USE SCHEMA, SELECT"), ("ml", "USE SCHEMA, SELECT"), ("tools", "USE SCHEMA, EXECUTE")):
        run(f"GRANT {privs} ON SCHEMA {CATALOG}.{schema} TO `{APP_PRINCIPAL}`", f"{privs} {schema} -> app SP")
```

**Output:**

```text
group_scope=workspace: UC GRANTs require account principals; group grants skipped
OK  USE CATALOG -> app SP
OK  USE SCHEMA, SELECT gold -> app SP
OK  USE SCHEMA, SELECT ml -> app SP
OK  USE SCHEMA, EXECUTE tools -> app SP
```

## Cell 6

```python
run(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.desk_entitlements (
  principal STRING COMMENT 'Account group name or service principal application id',
  portfolio_id STRING COMMENT 'gold.portfolios.portfolio_id, or * for all portfolios'
) COMMENT 'Which group/principal may see which trading-book portfolio (drives the gold.trades row filter)'
""", "table gold.desk_entitlements")

portfolios = {r["name"]: r["portfolio_id"] for r in spark.table(f"{GOLD}.portfolios").collect()}
entitlements = [(RISK, "*"), (COMPLIANCE, "*")]  # middle and back office see the whole book
entitlements += [(TRADERS, pid) for name, pid in portfolios.items() if name in ("Trading Book", "Hedge Book")]
entitlements += [(ANALYSTS, pid) for name, pid in portfolios.items() if name == "Renewable PPAs"]
if APP_PRINCIPAL:
    entitlements.append((APP_PRINCIPAL, "*"))
spark.createDataFrame(entitlements, "principal STRING, portfolio_id STRING").createOrReplaceTempView("_entitlements")
run(f"""
MERGE INTO {GOLD}.desk_entitlements t USING _entitlements s
ON t.principal = s.principal AND t.portfolio_id = s.portfolio_id
WHEN NOT MATCHED THEN INSERT *
""", f"entitlements upserted ({len(entitlements)} rows)")

run(f"""
CREATE OR REPLACE FUNCTION {TOOLS}.trades_row_filter(p_portfolio_id STRING)
RETURNS BOOLEAN
COMMENT 'Row filter for gold.trades: a principal sees a trade only if entitled to its portfolio'
RETURN EXISTS (
# … 7 more lines
```

**Output:**

```text
OK  table gold.desk_entitlements
OK  entitlements upserted (6 rows)
OK  function tools.trades_row_filter
OK  row filter on gold.trades
```

## Cell 8

```python
app_clause = f"OR current_user() = '{APP_PRINCIPAL}'" if APP_PRINCIPAL else ""
run(f"""
CREATE OR REPLACE FUNCTION {TOOLS}.mask_credit_limit(v DOUBLE)
RETURNS DOUBLE
COMMENT 'Column mask: credit limits are restricted to Risk and Compliance'
RETURN CASE WHEN {MEMBER_FN}('{RISK}') OR {MEMBER_FN}('{COMPLIANCE}') {app_clause}
            THEN v ELSE NULL END
""", "function tools.mask_credit_limit")
run(f"ALTER TABLE {GOLD}.counterparties ALTER COLUMN credit_limit_aud SET MASK {TOOLS}.mask_credit_limit",
    "column mask on gold.counterparties.credit_limit_aud")
```

**Output:**

```text
OK  function tools.mask_credit_limit
OK  column mask on gold.counterparties.credit_limit_aud
```

## Cell 10

```python
CONSTRAINTS = (
    (f"{GOLD}.nem_prices_5min", "valid_nem_region", "region_id IN ('NSW1','QLD1','VIC1','SA1','TAS1')"),
    (f"{GOLD}.nem_prices_5min", "rrp_within_market_limits", f"rrp BETWEEN {PRICE_FLOOR} AND {PRICE_CAP}"),
    (f"{GOLD}.nem_prices_5min", "interval_not_null", "interval_datetime IS NOT NULL"),
)
# Note: gold.trades is protected by the row filter above; UC does not allow CHECK constraints on tables
# with row-level security (ROW_LEVEL_SECURITY_CHECK_CONSTRAINT_UNSUPPORTED), so trade validation
# (buy_sell, volume > 0, tenor) is enforced in the deal-capture API instead.
skipped_constraints = []
for table, name, expr in CONSTRAINTS:
    existing = {r["key"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    if f"delta.constraints.{name.lower()}" in existing:
        applied.append(f"constraint {name} (exists)")
        continue
    violations = spark.sql(f"SELECT count(*) AS n FROM {table} WHERE NOT ({expr})").collect()[0]["n"]
    if violations:
        skipped_constraints.append({"table": table, "constraint": name, "violating_rows": violations})
        print(f"SKIP {name}: {violations} existing rows violate it — clean data, then re-run")
        continue
    run(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expr})", f"constraint {name} on {table}")
```

## Cell 12

```python
# Project-scoped tag keys (ec_*): the account enforces a governed tag policy on `data_classification`
# (allowed values u-nnpi / cui / pii), so this build uses its own keys rather than overloading that policy.
TAGS = {
    f"{GOLD}.nem_prices_5min": {"ec_classification": "public_market_data", "ec_domain": "front_office",
                                "ec_source": "aemo_nemweb"},
    f"{GOLD}.trades": {"ec_classification": "confidential", "ec_domain": "middle_office"},
    f"{GOLD}.counterparties": {"ec_classification": "confidential", "ec_domain": "middle_office"},
    f"{GOLD}.portfolios": {"ec_classification": "confidential", "ec_domain": "middle_office"},
    f"{GOLD}.dq_results": {"ec_classification": "internal", "ec_domain": "governance"},
}
for table, tags in TAGS.items():
    tag_sql = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    run(f"ALTER TABLE {table} SET TAGS ({tag_sql})", f"tags on {table}")
run(f"ALTER TABLE {GOLD}.counterparties ALTER COLUMN credit_limit_aud SET TAGS ('ec_sensitivity' = 'restricted')",
    "column tag credit_limit_aud")
run(f"COMMENT ON TABLE {GOLD}.nem_prices_5min IS "
    "'5-minute regional reference price and demand per NEM region (UTC). Source: AEMO NEMWEB DispatchIS via "
    "bronze.nemweb_dispatch_price; validated by data-quality rules (see gold.dq_results).'",
    "comment on nem_prices_5min")
```

**Output:**

```text
OK  tags on serverless_stable_2b9lj3_catalog.gold.nem_prices_5min
OK  tags on serverless_stable_2b9lj3_catalog.gold.trades
OK  tags on serverless_stable_2b9lj3_catalog.gold.counterparties
OK  tags on serverless_stable_2b9lj3_catalog.gold.portfolios
OK  tags on serverless_stable_2b9lj3_catalog.gold.dq_results
OK  column tag credit_limit_aud
OK  comment on nem_prices_5min
```

## Cell 14

```python
lineage = spark.sql(f"""
SELECT source_table_full_name, target_table_full_name, entity_type, max(event_time) AS last_seen
FROM system.access.table_lineage
WHERE '{GOLD}.nem_prices_5min' IN (source_table_full_name, target_table_full_name)
   OR target_table_full_name = '{CATALOG}.bronze.nemweb_dispatch_price'
GROUP BY ALL ORDER BY last_seen DESC LIMIT 25
""")
lineage_rows = [r.asDict() for r in lineage.collect()]

summary = {
    "catalog": CATALOG,
    "group_scope": GROUP_SCOPE,
    "controls_applied": len(applied),
    "applied": applied,
    "skipped_constraints": skipped_constraints,
    "lineage_edges": [f"{r['source_table_full_name']} -> {r['target_table_full_name']} ({r['entity_type']})"
                      for r in lineage_rows],
}
print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
```

**Output:**

```text
{"catalog": "serverless_stable_2b9lj3_catalog", "group_scope": "workspace", "controls_applied": 20, "applied": ["USE CATALOG -> app SP", "USE SCHEMA, SELECT gold -> app SP", "USE SCHEMA, SELECT ml -> app SP", "USE SCHEMA, EXECUTE tools -> app SP", "table gold.desk_entitlements", "entitlements upserted (6 rows)", "function tools.trades_row_filter", "row filter on gold.trades", "function tools.mask_credit_limit", "column mask on gold.counterparties.credit_limit_aud", "constraint valid_nem_region (exists)", "constraint rrp_within_market_limits (exists)", "constraint interval_not_null (exists)", "tags on serverless_stable_2b9lj3_catalog.gold.nem_prices_5min", "tags on serverless_stable_2b9lj3_catalog.gold.trades", "tags on serverless_stable_2b9lj3_catalog.gold.counterparties", "tags on serverless_stable_2b9lj3_catalog.gold.portfolios", "tags on serverless_stable_2b9lj3_catalog.gold.dq_results", "column tag credit_limit_aud", "comment on nem_prices_5min"], "skipped_constraints": [], "lineage_edges": ["serverless_stable_2b9lj3_catalog.bronze.nemweb_dispatch_price -> serverless_stable_2b9lj3_catalog.gold.nem_prices_5min (JOB)", "None -> serverless_stable_2b9lj3_catalog.bronze.nemweb_dispatch_price (JOB)", "serverless_stable_2b9lj3_catalog.gold.nem_prices_5min -> None (JOB)", "serverless_stable_2b9lj3_catalog.gold.nem_prices_5min -> None (None)", "None -> serverless_stable_2b9lj3_catalog.gold.nem_prices_5min (JOB)", "serverless_stable_2b9lj3_catalog.gold.nem_prices_5min -> serverless_stable_2b9lj3_catalog.gold._alert_model_drift (JOB)", "serverless_stable_2b9lj3_catalog.gold.nem_prices_5min -> serverless_stable_2b9lj3_catalog.gold._alert_data_stale (JOB)", "serverless_stable_2b9lj3_catalog.gold.nem_prices_5min -> serverless_stable_2b9lj3_catalog.gold._alert_price_sustained (JOB)", "serverless_stable_2b9lj3_catalog.gold.nem_prices_5min -> serverless_stable_2b9lj3_catalog.gold._alert_price_critical (JOB)", "serverless_stable_2b9lj3_catalog.gold.nem_prices_5min -> serverless_stable_2b9lj3_catalog.gold._alert_price_high (JOB)", "serverless_stable_2b9lj3_catalog.gold.nem_prices_5min -> serverless_stable_2b9lj3_catalog.gold._alert_price_moderate (JOB)"]}
```
