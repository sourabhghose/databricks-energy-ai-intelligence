-- =============================================================================================
-- Unity Catalog governance for the Energy Trading Copilot — the statements applied by
-- setup/30_governance.py (job "40 — UC Governance"), written out as plain SQL.
--
-- Applied on 2026-11-09 to catalog serverless_stable_2b9lj3_catalog (governance job run 962676474138249).
-- Evidence of the effect (same user, different results): docs/evidence/01_governance.md
-- Replace ${catalog} with your catalog name. Role groups: energy_traders, energy_risk,
-- energy_compliance, energy_analysts. App service principal: ${app_sp}.
--
-- This file shows the account-group form (group_scope=account). The evidence workspace is on a shared account
-- where the builder is not an account admin, so its role groups are workspace-local: there the script uses
-- is_member() in the row filter and mask, and grants to the groups are skipped. The filter and mask are enforced
-- the same way (see the trader vs risk results in docs/evidence/01_governance.md).
-- =============================================================================================

-- ---------------------------------------------------------------------------------------------
-- 1. Least-privilege grants (business roles read curated gold only; nothing on bronze/silver)
-- ---------------------------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG ${catalog} TO `energy_traders`;
GRANT USE CATALOG ON CATALOG ${catalog} TO `energy_risk`;
GRANT USE CATALOG ON CATALOG ${catalog} TO `energy_compliance`;
GRANT USE CATALOG ON CATALOG ${catalog} TO `energy_analysts`;

GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.gold TO `energy_traders`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.gold TO `energy_risk`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.gold TO `energy_compliance`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.gold TO `energy_analysts`;

GRANT USE SCHEMA, EXECUTE ON SCHEMA ${catalog}.tools TO `energy_traders`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA ${catalog}.tools TO `energy_risk`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA ${catalog}.tools TO `energy_compliance`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA ${catalog}.tools TO `energy_analysts`;

GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.ml TO `energy_traders`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.ml TO `energy_risk`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.ml TO `energy_analysts`;

GRANT MODIFY ON TABLE ${catalog}.gold.trades TO `energy_traders`;

-- App service principal: read gold/ml, execute tools, write only its own audit log
GRANT USE CATALOG ON CATALOG ${catalog} TO `${app_sp}`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.gold TO `${app_sp}`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.ml TO `${app_sp}`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA ${catalog}.tools TO `${app_sp}`;
GRANT MODIFY ON TABLE ${catalog}.gold.copilot_interactions TO `${app_sp}`;

-- ---------------------------------------------------------------------------------------------
-- 2. Row-level security: each desk sees only the trading-book portfolios it is entitled to
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.gold.desk_entitlements (
  principal    STRING COMMENT 'Account group name or service principal application id',
  portfolio_id STRING COMMENT 'gold.portfolios.portfolio_id, or * for all portfolios'
) COMMENT 'Which group/principal may see which trading-book portfolio (drives the gold.trades row filter)';

-- traders -> Trading Book + Hedge Book; analysts -> Renewable PPAs; risk, compliance, app -> all (*)
INSERT INTO ${catalog}.gold.desk_entitlements
SELECT 'energy_traders', portfolio_id FROM ${catalog}.gold.portfolios WHERE name IN ('Trading Book', 'Hedge Book')
UNION ALL SELECT 'energy_analysts', portfolio_id FROM ${catalog}.gold.portfolios WHERE name = 'Renewable PPAs'
UNION ALL SELECT 'energy_risk', '*'
UNION ALL SELECT 'energy_compliance', '*'
UNION ALL SELECT '${app_sp}', '*';

CREATE OR REPLACE FUNCTION ${catalog}.tools.trades_row_filter(p_portfolio_id STRING)
RETURNS BOOLEAN
COMMENT 'Row filter for gold.trades: a principal sees a trade only if entitled to its portfolio'
RETURN EXISTS (
  SELECT 1 FROM ${catalog}.gold.desk_entitlements e
  WHERE (e.portfolio_id = '*' OR e.portfolio_id = p_portfolio_id)
    AND (is_account_group_member(e.principal) OR e.principal = current_user())
);

ALTER TABLE ${catalog}.gold.trades SET ROW FILTER ${catalog}.tools.trades_row_filter ON (portfolio_id);

-- ---------------------------------------------------------------------------------------------
-- 3. Column mask: counterparty credit limits visible to Risk and Compliance only
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ${catalog}.tools.mask_credit_limit(v DOUBLE)
RETURNS DOUBLE
COMMENT 'Column mask: credit limits are restricted to Risk and Compliance'
RETURN CASE WHEN is_account_group_member('energy_risk') OR is_account_group_member('energy_compliance')
                 OR current_user() = '${app_sp}'
            THEN v ELSE NULL END;

ALTER TABLE ${catalog}.gold.counterparties ALTER COLUMN credit_limit_aud SET MASK ${catalog}.tools.mask_credit_limit;

-- ---------------------------------------------------------------------------------------------
-- 4. CHECK constraints on the curated price table (bad writes fail at the table)
--    (gold.trades is protected by the row filter; UC does not allow CHECK constraints on
--     tables with row-level security, so trade fields are validated in the deal-capture API)
-- ---------------------------------------------------------------------------------------------
ALTER TABLE ${catalog}.gold.nem_prices_5min ADD CONSTRAINT valid_nem_region
  CHECK (region_id IN ('NSW1','QLD1','VIC1','SA1','TAS1'));
ALTER TABLE ${catalog}.gold.nem_prices_5min ADD CONSTRAINT rrp_within_market_limits
  CHECK (rrp BETWEEN -1000.0 AND 23200.0);   -- AEMO market floor / AEMC FY2026-27 market price cap
ALTER TABLE ${catalog}.gold.nem_prices_5min ADD CONSTRAINT interval_not_null
  CHECK (interval_datetime IS NOT NULL);

-- ---------------------------------------------------------------------------------------------
-- 5. Classification tags (ec_* keys: the account governs `data_classification` with its own policy)
-- ---------------------------------------------------------------------------------------------
ALTER TABLE ${catalog}.gold.nem_prices_5min SET TAGS ('ec_classification' = 'public_market_data', 'ec_domain' = 'front_office', 'ec_source' = 'aemo_nemweb');
ALTER TABLE ${catalog}.gold.trades          SET TAGS ('ec_classification' = 'confidential', 'ec_domain' = 'middle_office');
ALTER TABLE ${catalog}.gold.counterparties  SET TAGS ('ec_classification' = 'confidential', 'ec_domain' = 'middle_office');
ALTER TABLE ${catalog}.gold.portfolios      SET TAGS ('ec_classification' = 'confidential', 'ec_domain' = 'middle_office');
ALTER TABLE ${catalog}.gold.dq_results      SET TAGS ('ec_classification' = 'internal', 'ec_domain' = 'governance');
ALTER TABLE ${catalog}.gold.counterparties ALTER COLUMN credit_limit_aud SET TAGS ('ec_sensitivity' = 'restricted');

-- ---------------------------------------------------------------------------------------------
-- 6. Data-quality results and lineage — verification queries
-- ---------------------------------------------------------------------------------------------
-- Per-rule results written by every pipeline run (pipelines/13_nemweb_bronze_to_gold.py)
SELECT rule_name, count(*) AS runs, sum(rows_checked) AS rows_checked, sum(rows_failed) AS rows_failed
FROM ${catalog}.gold.dq_results GROUP BY rule_name ORDER BY rule_name;

-- Lineage captured automatically by Unity Catalog
SELECT source_table_full_name, target_table_full_name, entity_type, max(event_time) AS last_seen
FROM system.access.table_lineage
WHERE target_table_full_name IN ('${catalog}.gold.nem_prices_5min', '${catalog}.gold.nem_prices_quarantine')
  AND source_table_full_name IS NOT NULL
GROUP BY ALL ORDER BY last_seen DESC;

-- Policies registered on the tables
SELECT 'row_filter' AS kind, table_name, filter_name AS policy FROM ${catalog}.information_schema.row_filters WHERE table_schema = 'gold'
UNION ALL
SELECT 'column_mask', table_name, mask_name FROM ${catalog}.information_schema.column_masks WHERE table_schema = 'gold';
