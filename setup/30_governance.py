# Databricks notebook source
# MAGIC %md
# MAGIC # 30 — Unity Catalog governance for the trading book and market data
# MAGIC
# MAGIC **Execution evidence:** `evidence/captured_runs.py::UC_GOVERNANCE` (job run 962676474138249: row filter, column mask, grants, constraints, tags applied); executed notebook with outputs in `notebooks/executed/`; checked by `tests/test_execution_evidence.py`.
# MAGIC
# MAGIC | Control | Object | Effect |
# MAGIC |---------|--------|--------|
# MAGIC | **Role-based grants** | catalog / schemas | 4 business roles get least-privilege access; no business role can read bronze/silver |
# MAGIC | **Row filter** | `gold.trades` | each desk sees only the portfolios it is entitled to (`gold.desk_entitlements`) |
# MAGIC | **Column mask** | `gold.counterparties.credit_limit_aud` | only Risk and Compliance see credit limits |
# MAGIC | **CHECK constraints** | `gold.nem_prices_5min` | invalid rows are rejected at write time |
# MAGIC | **Tags** | tables / columns | classification (`public_market_data`, `confidential`, `restricted`) |
# MAGIC | **Lineage report** | `system.access.table_lineage` | upstream/downstream graph of the price table |
# MAGIC
# MAGIC Idempotent: safe to re-run. Exits with a JSON summary of every control applied.

# COMMAND ----------

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
dbutils.widgets.text("market_price_cap", "23200")  # AEMC market price cap FY2026-27 (CPI-indexed; update each 1 July)

IDENT = re.compile(r"[A-Za-z0-9_\-.@]+")


def _param(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if value and not IDENT.fullmatch(value):
        raise ValueError(f"Invalid value for {name}: {value!r}")
    return value


CATALOG = _param("catalog")
TRADERS, RISK, COMPLIANCE, ANALYSTS = (
    _param(k) for k in ("traders_group", "risk_group", "compliance_group", "analysts_group")
)
APP_PRINCIPAL = _param("app_principal")
GROUP_SCOPE = dbutils.widgets.get("group_scope")
MEMBER_FN = "is_account_group_member" if GROUP_SCOPE == "account" else "is_member"
PRICE_FLOOR = float(dbutils.widgets.get("market_price_floor"))
PRICE_CAP = float(dbutils.widgets.get("market_price_cap"))
GOLD, TOOLS = f"{CATALOG}.gold", f"{CATALOG}.tools"
applied: list[str] = []


def run(sql: str, label: str) -> None:
    spark.sql(sql)
    applied.append(label)
    print(f"OK  {label}")

# COMMAND ----------
# MAGIC %md ## 1. Role-based access (least privilege)
# MAGIC
# MAGIC | Role | gold | ml | tools | bronze / silver | Write |
# MAGIC |------|------|----|-------|-----------------|-------|
# MAGIC | Traders | SELECT | SELECT | EXECUTE | none | MODIFY `gold.trades` |
# MAGIC | Risk (middle office) | SELECT | SELECT | EXECUTE | none | — |
# MAGIC | Compliance (back office) | SELECT | — | EXECUTE | none | — |
# MAGIC | Analysts | SELECT | SELECT | EXECUTE | none | — |

# COMMAND ----------

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

# The app writes an audit row per AI-assistant turn (inference table) — MODIFY on that table only
run(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.copilot_interactions (
  interaction_id STRING, created_at TIMESTAMP, model STRING, question STRING, tool_calls STRING,
  answer STRING, input_tokens INT, output_tokens INT, latency_ms INT, source STRING
) COMMENT 'Audit log of every AI Market Intelligence assistant turn (inference table)'
""", "table gold.copilot_interactions")
if APP_PRINCIPAL:
    run(f"GRANT MODIFY ON TABLE {GOLD}.copilot_interactions TO `{APP_PRINCIPAL}`", "MODIFY copilot_interactions -> app SP")

# COMMAND ----------
# MAGIC %md ## 2. Row filter — desks see only their entitled portfolios

# COMMAND ----------

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
  SELECT 1 FROM {GOLD}.desk_entitlements e
  WHERE (e.portfolio_id = '*' OR e.portfolio_id = p_portfolio_id)
    AND ({MEMBER_FN}(e.principal) OR e.principal = current_user())
)
""", "function tools.trades_row_filter")
run(f"ALTER TABLE {GOLD}.trades SET ROW FILTER {TOOLS}.trades_row_filter ON (portfolio_id)",
    "row filter on gold.trades")

# COMMAND ----------
# MAGIC %md ## 3. Column mask — credit limits visible to Risk and Compliance only

# COMMAND ----------

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

# COMMAND ----------
# MAGIC %md ## 4. CHECK constraints — reject invalid rows at write time

# COMMAND ----------

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

# COMMAND ----------
# MAGIC %md ## 5. Classification tags and descriptions

# COMMAND ----------

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

# COMMAND ----------
# MAGIC %md ## 6. Lineage report (captured automatically by Unity Catalog)

# COMMAND ----------

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
