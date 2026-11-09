"""Unity Catalog governance for the Energy Trading Copilot — the governed middle step of the data journey.

Raw AEMO NEMWEB data → bronze → **data-quality rules + Unity Catalog policies** → gold → models → this app.

The policies themselves are created by `setup/30_governance.py` (statements also in `GOVERNANCE.sql`):

* Row-level security: `ALTER TABLE gold.trades SET ROW FILTER tools.trades_row_filter ON (portfolio_id)` — each desk
  sees only its own book, driven by `gold.desk_entitlements` (traders: Trading + Hedge books; risk/compliance: all).
* Column mask: `ALTER TABLE gold.counterparties ALTER COLUMN credit_limit_aud SET MASK tools.mask_credit_limit` —
  credit limits visible to Risk and Compliance only.
* Grants: business roles get `USE SCHEMA, SELECT` on gold only; the app's service principal reads gold/ml and may
  write only its own audit log (`GRANT MODIFY ON TABLE gold.copilot_interactions`).
* Data quality: CHECK constraints on `gold.nem_prices_5min`, per-rule results in `gold.dq_results` on every run.
* Lineage: captured automatically by Unity Catalog in `system.access.table_lineage`.

This router reads those constructs back from Unity Catalog at request time, so the app can show — and the reviewer
can verify — that the governance is live, not just described. Every query is fixed SQL with no user input.
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from .shared import _CATALOG, _no_real_data, _query_gold

router = APIRouter()

GOLD = f"{_CATALOG}.gold"

# The Unity Catalog policies this app depends on, as applied by setup/30_governance.py
ROW_FILTER_SQL = f"""
CREATE OR REPLACE FUNCTION {_CATALOG}.tools.trades_row_filter(p_portfolio_id STRING) RETURNS BOOLEAN
RETURN EXISTS (SELECT 1 FROM {GOLD}.desk_entitlements e
               WHERE (e.portfolio_id = '*' OR e.portfolio_id = p_portfolio_id)
                 AND (is_account_group_member(e.principal) OR e.principal = current_user()));
ALTER TABLE {GOLD}.trades SET ROW FILTER {_CATALOG}.tools.trades_row_filter ON (portfolio_id);"""
COLUMN_MASK_SQL = f"""
CREATE OR REPLACE FUNCTION {_CATALOG}.tools.mask_credit_limit(v DOUBLE) RETURNS DOUBLE
RETURN CASE WHEN is_account_group_member('energy_risk') OR is_account_group_member('energy_compliance')
            THEN v ELSE NULL END;
ALTER TABLE {GOLD}.counterparties ALTER COLUMN credit_limit_aud SET MASK {_CATALOG}.tools.mask_credit_limit;"""

QUERIES = {
    "row_filters_and_masks": f"""
        SELECT 'row_filter' AS kind, table_name, filter_name AS policy, target_columns AS columns
        FROM {_CATALOG}.information_schema.row_filters WHERE table_schema = 'gold'
        UNION ALL
        SELECT 'column_mask', table_name, mask_name, column_name
        FROM {_CATALOG}.information_schema.column_masks WHERE table_schema = 'gold'""",
    "desk_entitlements": f"""
        SELECT e.principal, coalesce(p.name, 'ALL PORTFOLIOS') AS portfolio
        FROM {GOLD}.desk_entitlements e LEFT JOIN {GOLD}.portfolios p USING (portfolio_id) ORDER BY 1, 2""",
    "trades_visible_to_caller": f"""
        SELECT current_user() AS identity, p.name AS portfolio, count(*) AS trades
        FROM {GOLD}.trades t JOIN {GOLD}.portfolios p USING (portfolio_id) GROUP BY 1, 2 ORDER BY 2""",
    "table_grants": f"""
        SELECT grantee, table_schema, privilege_type, count(*) AS tables
        FROM {_CATALOG}.information_schema.table_privileges
        WHERE table_schema IN ('bronze', 'silver', 'gold') GROUP BY ALL ORDER BY 1, 2, 3""",
    "data_quality_rules": f"""
        SELECT rule_name, count(*) AS runs, sum(rows_checked) AS rows_checked, sum(rows_failed) AS rows_failed,
               max(run_ts) AS last_run_utc
        FROM {GOLD}.dq_results GROUP BY 1 ORDER BY 1""",
    "quarantined_rows": f"""
        SELECT region_id, concat_ws(',', failed_rules) AS failed_rules, count(*) AS n
        FROM {GOLD}.nem_prices_quarantine GROUP BY 1, 2""",
    "lineage": f"""
        SELECT source_table_full_name, target_table_full_name, entity_type, max(event_time) AS last_seen
        FROM system.access.table_lineage
        WHERE target_table_full_name IN ('{GOLD}.nem_prices_5min', '{GOLD}.nem_prices_quarantine')
          AND source_table_full_name IS NOT NULL
        GROUP BY ALL ORDER BY last_seen DESC""",
}


@router.get("/api/governance/status", summary="Live Unity Catalog governance status", tags=["Governance"])
def governance_status() -> dict:
    """Read the row filter, column mask, grants, data-quality results and lineage back from Unity Catalog."""
    results = {name: _query_gold(sql) for name, sql in QUERIES.items()}
    if not results["row_filters_and_masks"]:
        _no_real_data("Unity Catalog governance status")
    return {
        "as_of_utc": datetime.now(timezone.utc).isoformat(),
        "catalog": _CATALOG,
        "policies_applied": {"row_filter": ROW_FILTER_SQL.strip(), "column_mask": COLUMN_MASK_SQL.strip()},
        **{name: rows or [] for name, rows in results.items()},
    }
