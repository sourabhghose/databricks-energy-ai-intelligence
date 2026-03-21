# Databricks notebook source
# ============================================================
# Phase 5B: DNSP Distribution Intelligence
# ============================================================
# Creates 28 gold Delta tables + seeds realistic Australian DNSP
# data for AER compliance, network tariffs, bushfire mitigation,
# rural network, connections, and capital program delivery.
# ============================================================

# COMMAND ----------

catalog = "energy_copilot_catalog"
sp_id = "67aaaa6b-778c-4c8b-b2f0-9f9b9728b3bb"

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

# ============================================================
# MODULE 1 — AER Regulatory Compliance & RIN Management
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.rin_submissions (
    rin_id STRING,
    dnsp STRING,
    submission_year INT,
    category STRING,
    data_value DOUBLE,
    unit STRING,
    submission_date DATE,
    aer_accepted BOOLEAN,
    notes STRING
)
USING DELTA
COMMENT 'AER Regulatory Information Notice submissions by DNSP'
""")
print("✓ rin_submissions")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.stpis_performance (
    dnsp STRING,
    period_year INT,
    saidi_actual DOUBLE,
    saidi_target DOUBLE,
    saifi_actual DOUBLE,
    saifi_target DOUBLE,
    s_factor_saidi DOUBLE,
    s_factor_saifi DOUBLE,
    revenue_at_risk_aud DOUBLE,
    performance_band STRING
)
USING DELTA
COMMENT 'STPIS service target performance incentive scheme — s-factors and revenue at risk'
""")
print("✓ stpis_performance")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.revenue_monitoring (
    dnsp STRING,
    period_type STRING,
    period_start DATE,
    allowed_revenue_m_aud DOUBLE,
    actual_revenue_m_aud DOUBLE,
    variance_m_aud DOUBLE,
    smoothing_account_balance_m_aud DOUBLE
)
USING DELTA
COMMENT 'Revenue cap monitoring — allowed vs actual revenue and smoothing account balance'
""")
print("✓ revenue_monitoring")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.pricing_proposals (
    dnsp STRING,
    regulatory_year INT,
    proposal_date DATE,
    aer_decision_date DATE,
    status STRING,
    proposed_revenue_m_aud DOUBLE,
    aer_approved_revenue_m_aud DOUBLE
)
USING DELTA
COMMENT 'DNSP pricing proposals and AER approval status'
""")
print("✓ pricing_proposals")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.regulatory_milestones (
    milestone_id STRING,
    dnsp STRING,
    milestone_type STRING,
    description STRING,
    due_date DATE,
    completed_date DATE,
    status STRING,
    responsible_team STRING
)
USING DELTA
COMMENT 'Regulatory calendar milestones — AER submissions, resets, audits'
""")
print("✓ regulatory_milestones")

# COMMAND ----------

# ============================================================
# MODULE 2 — Network Tariff Analytics
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.network_tariff_structures (
    tariff_id STRING,
    dnsp STRING,
    tariff_name STRING,
    tariff_type STRING,
    customer_class STRING,
    fixed_charge_aud_day DOUBLE,
    energy_charge_aud_kwh DOUBLE,
    demand_charge_aud_kw DOUBLE,
    peak_hours STRING,
    effective_date DATE
)
USING DELTA
COMMENT 'Network tariff structures — fixed, TOU, demand tariffs by DNSP and customer class'
""")
print("✓ network_tariff_structures")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.tariff_migration_progress (
    dnsp STRING,
    period_year INT,
    customer_class STRING,
    flat_rate_pct DOUBLE,
    tou_pct DOUBLE,
    demand_pct DOUBLE,
    inclining_block_pct DOUBLE,
    aer_target_cost_reflective_pct DOUBLE
)
USING DELTA
COMMENT 'Customer migration progress from flat to cost-reflective tariffs'
""")
print("✓ tariff_migration_progress")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.tariff_revenue_by_class (
    dnsp STRING,
    period_year INT,
    customer_class STRING,
    actual_revenue_m_aud DOUBLE,
    allocated_cost_m_aud DOUBLE,
    cost_recovery_ratio DOUBLE
)
USING DELTA
COMMENT 'Revenue by tariff class vs allocated cost — cross-subsidy analysis'
""")
print("✓ tariff_revenue_by_class")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.demand_tariff_performance (
    dnsp STRING,
    feeder_id STRING,
    customer_nmi STRING,
    contracted_peak_reduction_kw DOUBLE,
    actual_peak_reduction_kw DOUBLE,
    performance_ratio DOUBLE,
    period_year INT
)
USING DELTA
COMMENT 'Demand tariff customer performance — contracted vs actual peak reduction'
""")
print("✓ demand_tariff_performance")

# COMMAND ----------

# ============================================================
# MODULE 3 — Bushfire Mitigation Program (AusNet Critical)
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.bmp_asset_register (
    asset_id STRING,
    dnsp STRING,
    asset_type STRING,
    bmo_zone STRING,
    fire_risk_rating STRING,
    last_inspection_date DATE,
    next_inspection_date DATE,
    clearance_status STRING,
    action_required STRING,
    region STRING
)
USING DELTA
COMMENT 'Bushfire Mitigation Program asset register — BMO zone assets with fire risk ratings'
""")
print("✓ bmp_asset_register")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.elc_inspections (
    inspection_id STRING,
    dnsp STRING,
    feeder_id STRING,
    span_id STRING,
    inspection_date DATE,
    inspector STRING,
    clearance_status STRING,
    vegetation_species STRING,
    encroachment_m DOUBLE,
    action_taken STRING
)
USING DELTA
COMMENT 'Electrical Line Clearance inspections — span-level vegetation clearance records'
""")
print("✓ elc_inspections")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fire_risk_incidents (
    incident_id STRING,
    dnsp STRING,
    incident_date DATE,
    feeder_id STRING,
    region STRING,
    fire_origin_electrical BOOLEAN,
    esv_notified BOOLEAN,
    esv_notification_date DATE,
    customers_affected INT,
    fire_ha DOUBLE,
    corrective_action STRING
)
USING DELTA
COMMENT 'Fire risk incidents — electrical origin fires with ESV notification status'
""")
print("✓ fire_risk_incidents")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.bmp_spend (
    dnsp STRING,
    period_year INT,
    category STRING,
    approved_allowance_m_aud DOUBLE,
    actual_spend_m_aud DOUBLE,
    projects_complete INT,
    projects_total INT
)
USING DELTA
COMMENT 'BMP capex spend vs AER-approved allowance by category'
""")
print("✓ bmp_spend")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.seasonal_readiness (
    dnsp STRING,
    season_year INT,
    check_category STRING,
    check_item STRING,
    target_completion_date DATE,
    actual_completion_date DATE,
    status STRING,
    completion_pct DOUBLE
)
USING DELTA
COMMENT 'Pre-summer seasonal readiness checklist items and completion status'
""")
print("✓ seasonal_readiness")

# COMMAND ----------

# ============================================================
# MODULE 4 — Long Rural Network & CSO (Energy Queensland Critical)
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.cso_payments (
    dnsp STRING,
    period_quarter STRING,
    period_year INT,
    uneconomic_cost_m_aud DOUBLE,
    cso_payment_m_aud DOUBLE,
    payment_date DATE,
    qld_govt_approved BOOLEAN
)
USING DELTA
COMMENT 'Community Service Obligation payments from QLD government to Ergon for rural network costs'
""")
print("✓ cso_payments")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.rural_feeder_performance (
    dnsp STRING,
    feeder_id STRING,
    feeder_length_km DOUBLE,
    length_tier STRING,
    saidi_minutes DOUBLE,
    saifi_count DOUBLE,
    customers_affected INT,
    period_year INT
)
USING DELTA
COMMENT 'Rural feeder reliability — SAIDI/SAIFI by feeder length tier'
""")
print("✓ rural_feeder_performance")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.raps_fleet (
    site_id STRING,
    dnsp STRING,
    region STRING,
    customer_nmi STRING,
    solar_kw DOUBLE,
    battery_kwh DOUBLE,
    diesel_kva DOUBLE,
    site_status STRING,
    last_service_date DATE
)
USING DELTA
COMMENT 'Remote Area Power Supply fleet — solar/battery/diesel hybrid sites in remote QLD'
""")
print("✓ raps_fleet")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.non_network_alternatives (
    constraint_id STRING,
    dnsp STRING,
    zone_substation STRING,
    augmentation_cost_m_aud DOUBLE,
    non_network_cost_m_aud DOUBLE,
    recommended_solution STRING,
    assessment_date DATE,
    rit_d_required BOOLEAN
)
USING DELTA
COMMENT 'Non-network alternatives assessment — demand management vs augmentation for rural constraints'
""")
print("✓ non_network_alternatives")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.ergon_energex_split (
    business_unit STRING,
    period_year INT,
    saidi_minutes DOUBLE,
    saifi_count DOUBLE,
    network_km DOUBLE,
    customers INT,
    capex_m_aud DOUBLE,
    opex_m_aud DOUBLE,
    rab_m_aud DOUBLE
)
USING DELTA
COMMENT 'Ergon Energy vs Energex comparison — operational and financial KPIs'
""")
print("✓ ergon_energex_split")

# COMMAND ----------

# ============================================================
# MODULE 5 — Connections & NER Compliance
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.connection_applications (
    application_id STRING,
    dnsp STRING,
    application_date DATE,
    application_type STRING,
    customer_type STRING,
    zone_substation STRING,
    capacity_kw DOUBLE,
    status STRING,
    offer_date DATE,
    ner_deadline_date DATE,
    ner_compliant BOOLEAN,
    offer_price_aud DOUBLE
)
USING DELTA
COMMENT 'Connection applications — NER timeframe compliance tracking'
""")
print("✓ connection_applications")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.timely_connections_kpi (
    dnsp STRING,
    period_quarter STRING,
    period_year INT,
    application_type STRING,
    total_applications INT,
    compliant_count INT,
    compliance_rate_pct DOUBLE,
    ner_threshold_days INT
)
USING DELTA
COMMENT 'NER timely connections KPIs — compliance rate by application type and quarter'
""")
print("✓ timely_connections_kpi")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.connection_offers (
    offer_id STRING,
    application_id STRING,
    offer_date DATE,
    offer_price_aud DOUBLE,
    connection_capacity_kw DOUBLE,
    augmentation_required BOOLEAN,
    customer_accepted BOOLEAN,
    rejection_reason STRING
)
USING DELTA
COMMENT 'Connection offers — price, capacity, acceptance/rejection tracking'
""")
print("✓ connection_offers")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.large_customer_pipeline (
    application_id STRING,
    dnsp STRING,
    customer_name STRING,
    industry STRING,
    capacity_mw DOUBLE,
    zone_substation STRING,
    network_impact_mw DOUBLE,
    assessment_status STRING,
    application_date DATE,
    target_energisation_date DATE
)
USING DELTA
COMMENT 'Large customer connection pipeline — commercial and industrial applications'
""")
print("✓ large_customer_pipeline")

# COMMAND ----------

# ============================================================
# MODULE 6 — Field Force & Capital Program Delivery
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.capital_projects (
    project_id STRING,
    dnsp STRING,
    project_name STRING,
    category STRING,
    region STRING,
    budget_m_aud DOUBLE,
    actual_spend_m_aud DOUBLE,
    completion_pct DOUBLE,
    start_date DATE,
    target_completion_date DATE,
    status STRING,
    aer_approved BOOLEAN
)
USING DELTA
COMMENT 'Capital program project register — AER-approved projects with spend and progress tracking'
""")
print("✓ capital_projects")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.maintenance_orders (
    order_id STRING,
    dnsp STRING,
    asset_id STRING,
    order_type STRING,
    priority STRING,
    scheduled_date DATE,
    completed_date DATE,
    crew_id STRING,
    contractor STRING,
    status STRING,
    cost_aud DOUBLE
)
USING DELTA
COMMENT 'Maintenance work orders — planned and corrective maintenance with priority and cost'
""")
print("✓ maintenance_orders")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fault_response_kpis (
    dnsp STRING,
    region STRING,
    period_month STRING,
    fault_type STRING,
    avg_response_time_min DOUBLE,
    avg_restoration_time_min DOUBLE,
    sla_target_min DOUBLE,
    sla_compliance_pct DOUBLE,
    total_faults INT
)
USING DELTA
COMMENT 'Fault response KPIs — response and restoration times vs SLA targets'
""")
print("✓ fault_response_kpis")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.contractor_performance (
    contractor_id STRING,
    dnsp STRING,
    contractor_name STRING,
    period_year INT,
    total_jobs INT,
    completed_on_time INT,
    defect_rate_pct DOUBLE,
    avg_cost_variance_pct DOUBLE,
    quality_score DOUBLE
)
USING DELTA
COMMENT 'Contractor performance scorecard — on-time delivery, defect rates, quality scores'
""")
print("✓ contractor_performance")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.capex_opex_analysis (
    dnsp STRING,
    asset_id STRING,
    asset_type STRING,
    period_year INT,
    capex_aud DOUBLE,
    opex_aud DOUBLE,
    health_index DOUBLE,
    recommended_action STRING
)
USING DELTA
COMMENT 'Capex vs opex analysis by asset — health index and recommended intervention'
""")
print("✓ capex_opex_analysis")

# COMMAND ----------

# ============================================================
# SEED DATA — Realistic Australian DNSP data
# ============================================================

import random
from datetime import date, datetime, timedelta

random.seed(42)
now = datetime.utcnow()
today = date.today()

def _to_df(rows):
    if not rows:
        return spark.createDataFrame([])
    float_keys = set()
    for row in rows:
        for k, v in row.items():
            if isinstance(v, float):
                float_keys.add(k)
    if float_keys:
        for row in rows:
            for k in float_keys:
                if k in row and isinstance(row[k], int):
                    row[k] = float(row[k])
    return spark.createDataFrame(rows)

AUSNET_FEEDERS = [f"FDR-AUSNET-VIC1-{i:02d}" for i in range(1, 21)]
ERGON_FEEDERS = [f"FDR-ERGON-QLD1-{i:02d}" for i in range(1, 21)]
ENERGEX_FEEDERS = [f"FDR-ENERGEX-QLD1-{i:02d}" for i in range(1, 16)]
VIC_ZONE_SUBS = ["Healesville", "Yarra Glen", "Warrandyte", "Hurstbridge", "Lilydale",
                 "Dandenong", "Frankston", "Craigieburn", "Sunbury", "Mernda"]
QLD_ZONE_SUBS = ["Toowoomba", "Rockhampton", "Townsville", "Cairns", "Mackay",
                 "Bundaberg", "Hervey Bay", "Mount Isa", "Longreach", "Emerald"]
BMO_ZONES = ["BMO-1 Dandenong Ranges", "BMO-2 Kinglake", "BMO-3 Yarra Valley",
             "BMO-4 Mornington Peninsula", "BMO-5 Macedon Ranges"]

# COMMAND ----------

# --- 1. Seed rin_submissions ---
rin_categories = [
    "SAIDI", "SAIFI", "Opex", "Capex", "Customer Connections",
    "Asset Age Profile", "Network Length", "Zone Substation Utilisation",
    "DER Connection", "Fire Risk Management"
]
rin_rows = []
for dnsp, region in [("AusNet Services", "VIC1"), ("Ergon Energy", "QLD1"), ("Energex", "QLD1")]:
    for year in [2024, 2025, 2026]:
        for cat in rin_categories:
            rin_rows.append({
                "rin_id": f"RIN-{dnsp[:3].upper()}-{year}-{cat[:4].upper().replace(' ','')}-{random.randint(1,99):02d}",
                "dnsp": dnsp,
                "submission_year": year,
                "category": cat,
                "data_value": round(random.uniform(1.0, 500.0), 2),
                "unit": random.choice(["minutes", "count", "$M", "km", "%"]),
                "submission_date": (date(year, random.randint(9, 11), random.randint(1, 28))).isoformat(),
                "aer_accepted": random.random() > 0.05,
                "notes": "" if random.random() > 0.2 else random.choice(["Data query raised", "Revised submission", "Accepted with conditions"]),
            })

df = _to_df(rin_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.rin_submissions")
print(f"✓ rin_submissions seeded: {len(rin_rows)} rows")

# COMMAND ----------

# --- 2. Seed stpis_performance ---
stpis_rows = []
for dnsp, saidi_target, saifi_target, rev_base in [
    ("AusNet Services", 75.0, 1.0, 950.0),
    ("Ergon Energy", 140.0, 1.6, 1200.0),
    ("Energex", 80.0, 1.1, 1800.0),
]:
    for year in [2023, 2024, 2025, 2026]:
        saidi_actual = round(saidi_target * random.uniform(0.7, 1.3), 2)
        saifi_actual = round(saifi_target * random.uniform(0.7, 1.3), 3)
        s_saidi = round((saidi_target - saidi_actual) / saidi_target * 0.5, 4)
        s_saifi = round((saifi_target - saifi_actual) / saifi_target * 0.5, 4)
        combined_s = s_saidi + s_saifi
        band = "A" if combined_s > 0.05 else ("B" if combined_s > 0 else ("C" if combined_s > -0.05 else "D"))
        rev_risk = round(rev_base * abs(min(combined_s, 0)) * 10, 2)
        stpis_rows.append({
            "dnsp": dnsp,
            "period_year": year,
            "saidi_actual": saidi_actual,
            "saidi_target": saidi_target,
            "saifi_actual": saifi_actual,
            "saifi_target": saifi_target,
            "s_factor_saidi": s_saidi,
            "s_factor_saifi": s_saifi,
            "revenue_at_risk_aud": rev_risk,
            "performance_band": band,
        })

df = _to_df(stpis_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.stpis_performance")
print(f"✓ stpis_performance seeded: {len(stpis_rows)} rows")

# COMMAND ----------

# --- 3. Seed revenue_monitoring ---
rev_rows = []
for dnsp, base_rev in [("AusNet Services", 950.0), ("Ergon Energy", 1200.0), ("Energex", 1800.0)]:
    for year in [2023, 2024, 2025, 2026]:
        for month in range(1, 13):
            allowed = round(base_rev / 12 * random.uniform(0.97, 1.03), 2)
            actual = round(allowed * random.uniform(0.95, 1.05), 2)
            rev_rows.append({
                "dnsp": dnsp,
                "period_type": "monthly",
                "period_start": date(year, month, 1).isoformat(),
                "allowed_revenue_m_aud": allowed,
                "actual_revenue_m_aud": actual,
                "variance_m_aud": round(actual - allowed, 3),
                "smoothing_account_balance_m_aud": round(random.uniform(-50, 50), 2),
            })

df = _to_df(rev_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.revenue_monitoring")
print(f"✓ revenue_monitoring seeded: {len(rev_rows)} rows")

# COMMAND ----------

# --- 4. Seed pricing_proposals ---
pp_rows = []
for dnsp, base_rev in [("AusNet Services", 950.0), ("Ergon Energy", 1200.0), ("Energex", 1800.0)]:
    for year in range(2023, 2028):
        proposed = round(base_rev * random.uniform(1.02, 1.12), 2)
        approved = round(proposed * random.uniform(0.92, 0.99), 2)
        status = "Approved" if year <= 2025 else ("Under Review" if year == 2026 else "Draft")
        pp_rows.append({
            "dnsp": dnsp,
            "regulatory_year": year,
            "proposal_date": date(year - 1, 10, 15).isoformat(),
            "aer_decision_date": date(year - 1, 12, 31).isoformat() if year <= 2025 else "",
            "status": status,
            "proposed_revenue_m_aud": proposed,
            "aer_approved_revenue_m_aud": approved if year <= 2025 else 0.0,
        })

df = _to_df(pp_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.pricing_proposals")
print(f"✓ pricing_proposals seeded: {len(pp_rows)} rows")

# COMMAND ----------

# --- 5. Seed regulatory_milestones ---
milestone_types = ["RIN Submission", "Annual Pricing Proposal", "STPIS Assessment",
                   "AER Audit", "Annual Compliance Report", "Revenue Reset",
                   "Network Tariff Consultation", "Pricing Principles Review"]
teams = ["Regulatory Affairs", "Finance", "Network Planning", "Legal", "Operations"]
ms_rows = []
for dnsp in ["AusNet Services", "Ergon Energy", "Energex"]:
    for i, mtype in enumerate(milestone_types):
        due = today + timedelta(days=random.randint(-60, 365))
        completed = due - timedelta(days=random.randint(1, 10)) if due < today else None
        status = "COMPLETE" if completed else ("OVERDUE" if due < today else ("IN PROGRESS" if (due - today).days < 30 else "UPCOMING"))
        ms_rows.append({
            "milestone_id": f"MS-{dnsp[:3].upper()}-{i+1:03d}",
            "dnsp": dnsp,
            "milestone_type": mtype,
            "description": f"{mtype} for {dnsp} {2026 if due.year == 2026 else 2025} regulatory period",
            "due_date": due.isoformat(),
            "completed_date": completed.isoformat() if completed else "",
            "status": status,
            "responsible_team": random.choice(teams),
        })

df = _to_df(ms_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.regulatory_milestones")
print(f"✓ regulatory_milestones seeded: {len(ms_rows)} rows")

# COMMAND ----------

# --- 6. Seed network_tariff_structures ---
tariff_types = ["flat_rate", "tou", "demand", "inclining_block"]
customer_classes = ["residential", "small_business", "large_business", "industrial"]
tariff_rows = []
for dnsp in ["AusNet Services", "Ergon Energy", "Energex"]:
    for cc in customer_classes:
        for tt in tariff_types:
            fixed = round(random.uniform(0.20, 0.50), 4)
            energy = round(random.uniform(0.04, 0.15), 5)
            demand = round(random.uniform(8.0, 25.0), 2) if tt == "demand" else 0.0
            tariff_rows.append({
                "tariff_id": f"TAR-{dnsp[:3].upper()}-{cc[:3].upper()}-{tt[:3].upper()}",
                "dnsp": dnsp,
                "tariff_name": f"{dnsp} {cc.replace('_',' ').title()} {tt.replace('_',' ').upper()}",
                "tariff_type": tt,
                "customer_class": cc,
                "fixed_charge_aud_day": fixed,
                "energy_charge_aud_kwh": energy,
                "demand_charge_aud_kw": demand,
                "peak_hours": "7am-9am, 5pm-8pm" if tt in ("tou", "demand") else "all_day",
                "effective_date": "2025-07-01",
            })

df = _to_df(tariff_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.network_tariff_structures")
print(f"✓ network_tariff_structures seeded: {len(tariff_rows)} rows")

# COMMAND ----------

# --- 7. Seed tariff_migration_progress ---
tm_rows = []
for dnsp in ["AusNet Services", "Ergon Energy", "Energex"]:
    for year in [2023, 2024, 2025, 2026]:
        for cc in ["residential", "small_business", "large_business"]:
            # Progressive migration toward cost-reflective tariffs
            tou_base = {"residential": 0.15, "small_business": 0.30, "large_business": 0.55}[cc]
            demand_base = {"residential": 0.02, "small_business": 0.10, "large_business": 0.30}[cc]
            progress_factor = (year - 2022) * 0.08
            tou = min(0.80, tou_base + progress_factor + random.uniform(-0.02, 0.02))
            demand = min(0.50, demand_base + progress_factor * 0.5 + random.uniform(-0.01, 0.01))
            flat = max(0.05, 1.0 - tou - demand - 0.05)
            iblock = 1.0 - flat - tou - demand
            tm_rows.append({
                "dnsp": dnsp,
                "period_year": year,
                "customer_class": cc,
                "flat_rate_pct": round(flat * 100, 1),
                "tou_pct": round(tou * 100, 1),
                "demand_pct": round(demand * 100, 1),
                "inclining_block_pct": round(iblock * 100, 1),
                "aer_target_cost_reflective_pct": {"residential": 65.0, "small_business": 75.0, "large_business": 90.0}[cc],
            })

df = _to_df(tm_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.tariff_migration_progress")
print(f"✓ tariff_migration_progress seeded: {len(tm_rows)} rows")

# COMMAND ----------

# --- 8. Seed tariff_revenue_by_class ---
trbc_rows = []
for dnsp, total_rev in [("AusNet Services", 950.0), ("Ergon Energy", 1200.0), ("Energex", 1800.0)]:
    for year in [2024, 2025, 2026]:
        class_splits = {"residential": 0.45, "small_business": 0.25, "large_business": 0.20, "industrial": 0.10}
        for cc, split in class_splits.items():
            actual = round(total_rev * split * random.uniform(0.95, 1.05), 2)
            cost = round(total_rev * split * random.uniform(0.90, 1.10), 2)
            trbc_rows.append({
                "dnsp": dnsp,
                "period_year": year,
                "customer_class": cc,
                "actual_revenue_m_aud": actual,
                "allocated_cost_m_aud": cost,
                "cost_recovery_ratio": round(actual / cost, 4),
            })

df = _to_df(trbc_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.tariff_revenue_by_class")
print(f"✓ tariff_revenue_by_class seeded: {len(trbc_rows)} rows")

# COMMAND ----------

# --- 9. Seed demand_tariff_performance ---
dtp_rows = []
all_feeders = AUSNET_FEEDERS[:10] + ENERGEX_FEEDERS[:8]
for fid in all_feeders:
    dnsp = "AusNet Services" if "AUSNET" in fid else "Energex"
    for i in range(5):
        contracted = round(random.uniform(5.0, 50.0), 1)
        actual = round(contracted * random.uniform(0.7, 1.1), 1)
        dtp_rows.append({
            "dnsp": dnsp,
            "feeder_id": fid,
            "customer_nmi": f"NMI{random.randint(3000000000, 3999999999)}",
            "contracted_peak_reduction_kw": contracted,
            "actual_peak_reduction_kw": actual,
            "performance_ratio": round(actual / contracted, 4),
            "period_year": 2025,
        })

df = _to_df(dtp_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.demand_tariff_performance")
print(f"✓ demand_tariff_performance seeded: {len(dtp_rows)} rows")

# COMMAND ----------

# --- 10. Seed bmp_asset_register (AusNet VIC focus) ---
asset_types = ["aerial_conductor", "underground_cable", "distribution_transformer",
               "zone_substation_feeder", "service_line", "overhead_span"]
fire_ratings = ["EXTREME", "HIGH", "MEDIUM", "LOW"]
fire_weights = [15, 35, 35, 15]
clearance_statuses = ["COMPLIANT", "ACTION_REQUIRED", "OVERDUE", "UNDER_REVIEW"]

bmp_rows = []
for i in range(200):
    asset_type = random.choice(asset_types)
    bmo = random.choice(BMO_ZONES)
    fire_rating = random.choices(fire_ratings, weights=fire_weights)[0]
    last_insp = today - timedelta(days=random.randint(10, 400))
    next_insp = last_insp + timedelta(days=365 if fire_rating not in ("EXTREME", "HIGH") else 180)
    clearance = random.choices(clearance_statuses, weights=[50, 25, 15, 10])[0]
    bmp_rows.append({
        "asset_id": f"BMP-AUSNET-{i+1:04d}",
        "dnsp": "AusNet Services",
        "asset_type": asset_type,
        "bmo_zone": bmo,
        "fire_risk_rating": fire_rating,
        "last_inspection_date": last_insp.isoformat(),
        "next_inspection_date": next_insp.isoformat(),
        "clearance_status": clearance,
        "action_required": "" if clearance == "COMPLIANT" else random.choice(["Vegetation trim", "Conductor replacement", "Clearance audit", "Urgent re-inspection"]),
        "region": "VIC1",
    })

df = _to_df(bmp_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.bmp_asset_register")
print(f"✓ bmp_asset_register seeded: {len(bmp_rows)} rows")

# COMMAND ----------

# --- 11. Seed elc_inspections (AusNet VIC focus) ---
veg_species = ["Eucalyptus", "Acacia", "Melaleuca", "Callitris", "Pinus radiata",
               "Populus", "Corymbia", "Allocasuarina"]
actions = ["None required", "Notified landowner", "Trim scheduled", "Emergency trim completed",
           "Referral to contractor", "Re-inspection booked"]
elc_rows = []
for i in range(300):
    fid = random.choice(AUSNET_FEEDERS)
    insp_date = today - timedelta(days=random.randint(1, 180))
    encroachment = round(random.uniform(0.0, 4.5), 2)
    clearance = "NON_COMPLIANT" if encroachment > 3.0 else ("WARNING" if encroachment > 1.5 else "COMPLIANT")
    elc_rows.append({
        "inspection_id": f"ELC-{i+1:05d}",
        "dnsp": "AusNet Services",
        "feeder_id": fid,
        "span_id": f"SPAN-{fid}-{random.randint(1,50):03d}",
        "inspection_date": insp_date.isoformat(),
        "inspector": f"INSP-{random.randint(1,20):02d}",
        "clearance_status": clearance,
        "vegetation_species": random.choice(veg_species),
        "encroachment_m": encroachment,
        "action_taken": "None required" if clearance == "COMPLIANT" else random.choice(actions[1:]),
    })

df = _to_df(elc_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.elc_inspections")
print(f"✓ elc_inspections seeded: {len(elc_rows)} rows")

# COMMAND ----------

# --- 12. Seed fire_risk_incidents (AusNet VIC focus) ---
corrective_actions = [
    "Asset replacement", "Enhanced inspection schedule", "Vegetation management program",
    "Network reconfiguration", "ESV direction complied", "Staff re-training",
    "Technology upgrade — Rapidearth", "No further action required"
]
fi_rows = []
for i in range(50):
    fid = random.choice(AUSNET_FEEDERS)
    incident_date = today - timedelta(days=random.randint(1, 730))
    electrical_origin = random.random() > 0.4
    esv = electrical_origin and random.random() > 0.1
    fi_rows.append({
        "incident_id": f"FRI-AUSNET-{i+1:04d}",
        "dnsp": "AusNet Services",
        "incident_date": incident_date.isoformat(),
        "feeder_id": fid,
        "region": "VIC1",
        "fire_origin_electrical": electrical_origin,
        "esv_notified": esv,
        "esv_notification_date": (incident_date + timedelta(days=random.randint(0, 2))).isoformat() if esv else "",
        "customers_affected": random.randint(0, 2000) if electrical_origin else 0,
        "fire_ha": round(random.uniform(0.1, 500.0), 1) if electrical_origin else 0.0,
        "corrective_action": random.choice(corrective_actions),
    })

df = _to_df(fi_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.fire_risk_incidents")
print(f"✓ fire_risk_incidents seeded: {len(fi_rows)} rows")

# COMMAND ----------

# --- 13. Seed bmp_spend ---
bmp_cats = ["ELC Inspections", "Conductor Replacement", "Insulation Works",
            "Vegetation Management", "Asset Monitoring", "Technology Upgrades"]
bmp_sp_rows = []
for year in [2024, 2025, 2026]:
    for cat in bmp_cats:
        allowance = round(random.uniform(10.0, 85.0), 2)
        actual = round(allowance * random.uniform(0.75, 1.10), 2)
        total_proj = random.randint(8, 40)
        complete = int(total_proj * random.uniform(0.5, 1.0)) if year < 2026 else int(total_proj * random.uniform(0.2, 0.8))
        bmp_sp_rows.append({
            "dnsp": "AusNet Services",
            "period_year": year,
            "category": cat,
            "approved_allowance_m_aud": allowance,
            "actual_spend_m_aud": actual,
            "projects_complete": complete,
            "projects_total": total_proj,
        })

df = _to_df(bmp_sp_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.bmp_spend")
print(f"✓ bmp_spend seeded: {len(bmp_sp_rows)} rows")

# COMMAND ----------

# --- 14. Seed seasonal_readiness ---
readiness_items = [
    ("ELC Inspections", "Complete all BMO zone ELC inspections"),
    ("ELC Inspections", "Clear non-compliant vegetation from high-risk spans"),
    ("Asset Condition", "Complete thermal imaging of zone substations"),
    ("Asset Condition", "Inspect and replace deteriorated aerial conductors"),
    ("Operational", "Update emergency response plans"),
    ("Operational", "Complete field crew summer fire training"),
    ("Technology", "Test and calibrate rapidearth fault indicators"),
    ("Technology", "Validate network remote switching capability"),
    ("Customer Comms", "Issue pre-summer customer advisories"),
    ("Customer Comms", "Update online outage reporting portal"),
]
sr_rows = []
for year in [2025, 2026]:
    for cat, item in readiness_items:
        target = date(year - 1, 11, 30)
        completion_pct = round(random.uniform(60, 100), 1) if date(year - 1, 11, 1) < today else round(random.uniform(20, 80), 1)
        actual = target - timedelta(days=random.randint(0, 14)) if completion_pct >= 100 else None
        status = "COMPLETE" if completion_pct >= 100 else ("IN PROGRESS" if completion_pct > 0 else "NOT STARTED")
        sr_rows.append({
            "dnsp": "AusNet Services",
            "season_year": year,
            "check_category": cat,
            "check_item": item,
            "target_completion_date": target.isoformat(),
            "actual_completion_date": actual.isoformat() if actual else "",
            "status": status,
            "completion_pct": completion_pct,
        })

df = _to_df(sr_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.seasonal_readiness")
print(f"✓ seasonal_readiness seeded: {len(sr_rows)} rows")

# COMMAND ----------

# --- 15. Seed cso_payments (Ergon Energy QLD focus) ---
cso_rows = []
for year in [2023, 2024, 2025, 2026]:
    for q in ["Q1", "Q2", "Q3", "Q4"]:
        month_map = {"Q1": 3, "Q2": 6, "Q3": 9, "Q4": 12}
        uneconomic = round(random.uniform(120.0, 180.0), 2)
        cso_pay = round(uneconomic * random.uniform(0.85, 0.95), 2)
        payment_date = date(year, month_map[q], 28)
        cso_rows.append({
            "dnsp": "Ergon Energy",
            "period_quarter": q,
            "period_year": year,
            "uneconomic_cost_m_aud": uneconomic,
            "cso_payment_m_aud": cso_pay,
            "payment_date": payment_date.isoformat(),
            "qld_govt_approved": True if payment_date < today else False,
        })

df = _to_df(cso_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.cso_payments")
print(f"✓ cso_payments seeded: {len(cso_rows)} rows")

# COMMAND ----------

# --- 16. Seed rural_feeder_performance ---
length_tiers = [("<50km", 25), ("50-100km", 75), ("100-200km", 150), (">200km", 250)]
rfp_rows = []
for i in range(60):
    fid = random.choice(ERGON_FEEDERS)
    tier_name, tier_km = random.choice(length_tiers)
    length = round(tier_km * random.uniform(0.8, 1.2), 1)
    saidi = round(random.uniform(50, 600) * (tier_km / 100), 1)
    saifi = round(random.uniform(0.5, 5.0) * (tier_km / 100), 2)
    rfp_rows.append({
        "dnsp": "Ergon Energy",
        "feeder_id": fid,
        "feeder_length_km": length,
        "length_tier": tier_name,
        "saidi_minutes": saidi,
        "saifi_count": saifi,
        "customers_affected": random.randint(10, 500),
        "period_year": 2025,
    })

df = _to_df(rfp_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.rural_feeder_performance")
print(f"✓ rural_feeder_performance seeded: {len(rfp_rows)} rows")

# COMMAND ----------

# --- 17. Seed raps_fleet ---
qld_regions = ["Cape York", "Gulf Country", "Torres Strait", "Central West",
               "South West", "North West", "Outback QLD"]
raps_rows = []
for i in range(80):
    solar = random.choice([5.0, 10.0, 15.0, 20.0, 30.0])
    battery = solar * random.uniform(2.0, 4.0)
    diesel = round(random.choice([5.0, 10.0, 15.0, 20.0, 30.0]), 0)
    last_svc = today - timedelta(days=random.randint(30, 730))
    raps_rows.append({
        "site_id": f"RAPS-ERGON-{i+1:04d}",
        "dnsp": "Ergon Energy",
        "region": random.choice(qld_regions),
        "customer_nmi": f"NMI5{random.randint(100000000, 999999999)}",
        "solar_kw": solar,
        "battery_kwh": round(battery, 1),
        "diesel_kva": diesel,
        "site_status": random.choices(["OPERATIONAL", "MAINTENANCE", "FAULT"], weights=[85, 10, 5])[0],
        "last_service_date": last_svc.isoformat(),
    })

df = _to_df(raps_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.raps_fleet")
print(f"✓ raps_fleet seeded: {len(raps_rows)} rows")

# COMMAND ----------

# --- 18. Seed non_network_alternatives ---
nna_rows = []
for i in range(30):
    dnsp = random.choice(["Ergon Energy", "AusNet Services"])
    zs = random.choice(QLD_ZONE_SUBS if dnsp == "Ergon Energy" else VIC_ZONE_SUBS)
    aug_cost = round(random.uniform(5.0, 80.0), 2)
    nna_cost = round(aug_cost * random.uniform(0.3, 0.8), 2)
    solutions = ["Battery storage + demand management", "Solar + battery microgrid",
                 "RAPS hybrid system", "Demand response program",
                 "Non-firm connection offer", "Load control scheme"]
    rit_required = aug_cost > 20.0
    nna_rows.append({
        "constraint_id": f"NNA-{dnsp[:3].upper()}-{i+1:03d}",
        "dnsp": dnsp,
        "zone_substation": f"ZS-{zs.replace(' ', '_').upper()}",
        "augmentation_cost_m_aud": aug_cost,
        "non_network_cost_m_aud": nna_cost,
        "recommended_solution": random.choice(solutions),
        "assessment_date": (today - timedelta(days=random.randint(30, 365))).isoformat(),
        "rit_d_required": rit_required,
    })

df = _to_df(nna_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.non_network_alternatives")
print(f"✓ non_network_alternatives seeded: {len(nna_rows)} rows")

# COMMAND ----------

# --- 19. Seed ergon_energex_split ---
ee_rows = []
for year in [2023, 2024, 2025, 2026]:
    # Ergon: rural, large network, high SAIDI, low customer density
    ee_rows.append({
        "business_unit": "Ergon Energy",
        "period_year": year,
        "saidi_minutes": round(random.uniform(280, 380), 1),
        "saifi_count": round(random.uniform(3.2, 4.5), 2),
        "network_km": round(random.uniform(163000, 165000), 0),
        "customers": random.randint(730000, 750000),
        "capex_m_aud": round(random.uniform(480, 560), 2),
        "opex_m_aud": round(random.uniform(380, 420), 2),
        "rab_m_aud": round(random.uniform(8500, 9200), 2),
    })
    # Energex: urban SEQ, smaller network, lower SAIDI, higher customer density
    ee_rows.append({
        "business_unit": "Energex",
        "period_year": year,
        "saidi_minutes": round(random.uniform(60, 90), 1),
        "saifi_count": round(random.uniform(0.9, 1.3), 2),
        "network_km": round(random.uniform(52000, 54000), 0),
        "customers": random.randint(1430000, 1470000),
        "capex_m_aud": round(random.uniform(550, 680), 2),
        "opex_m_aud": round(random.uniform(420, 480), 2),
        "rab_m_aud": round(random.uniform(10800, 11500), 2),
    })

df = _to_df(ee_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.ergon_energex_split")
print(f"✓ ergon_energex_split seeded: {len(ee_rows)} rows")

# COMMAND ----------

# --- 20. Seed connection_applications ---
app_types = ["Standard Connection", "Large Customer", "EV Charger", "Solar Export", "Battery Storage"]
cust_types = ["residential", "small_business", "commercial", "industrial"]
statuses = ["Pending", "Offer Issued", "Accepted", "Rejected", "Under Assessment"]
ca_rows = []
for i in range(150):
    dnsp = random.choice(["AusNet Services", "Ergon Energy", "Energex"])
    app_date = today - timedelta(days=random.randint(1, 365))
    app_type = random.choice(app_types)
    ner_days = {"Standard Connection": 65, "Large Customer": 120, "EV Charger": 30, "Solar Export": 45, "Battery Storage": 65}[app_type]
    ner_deadline = app_date + timedelta(days=ner_days)
    offer_date = app_date + timedelta(days=random.randint(5, ner_days - 5)) if random.random() > 0.2 else None
    compliant = offer_date <= ner_deadline if offer_date else (ner_deadline >= today)
    capacity = round(random.choice([6.6, 10, 13.5, 30, 100, 500, 1000, 5000, 10000]), 1)
    ca_rows.append({
        "application_id": f"CA-{dnsp[:3].upper()}-{i+1:05d}",
        "dnsp": dnsp,
        "application_date": app_date.isoformat(),
        "application_type": app_type,
        "customer_type": random.choice(cust_types),
        "zone_substation": f"ZS-{random.choice(VIC_ZONE_SUBS + QLD_ZONE_SUBS).replace(' ','_').upper()}",
        "capacity_kw": capacity,
        "status": random.choice(statuses),
        "offer_date": offer_date.isoformat() if offer_date else "",
        "ner_deadline_date": ner_deadline.isoformat(),
        "ner_compliant": compliant,
        "offer_price_aud": round(random.uniform(500, 50000), 2),
    })

df = _to_df(ca_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.connection_applications")
print(f"✓ connection_applications seeded: {len(ca_rows)} rows")

# COMMAND ----------

# --- 21. Seed timely_connections_kpi ---
tc_rows = []
for dnsp in ["AusNet Services", "Ergon Energy", "Energex"]:
    for year in [2025, 2026]:
        for q in ["Q1", "Q2", "Q3", "Q4"]:
            for app_type in ["Standard Connection", "Large Customer", "Solar Export"]:
                ner_days = {"Standard Connection": 65, "Large Customer": 120, "Solar Export": 45}[app_type]
                total = random.randint(20, 200)
                compliance_rate = round(random.uniform(75, 99), 1)
                compliant = int(total * compliance_rate / 100)
                tc_rows.append({
                    "dnsp": dnsp,
                    "period_quarter": q,
                    "period_year": year,
                    "application_type": app_type,
                    "total_applications": total,
                    "compliant_count": compliant,
                    "compliance_rate_pct": compliance_rate,
                    "ner_threshold_days": ner_days,
                })

df = _to_df(tc_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.timely_connections_kpi")
print(f"✓ timely_connections_kpi seeded: {len(tc_rows)} rows")

# COMMAND ----------

# --- 22. Seed connection_offers ---
co_rows = []
for ca in ca_rows[:100]:
    if ca["offer_date"]:
        accepted = random.random() > 0.25
        co_rows.append({
            "offer_id": f"OFF-{ca['application_id']}",
            "application_id": ca["application_id"],
            "offer_date": ca["offer_date"],
            "offer_price_aud": ca["offer_price_aud"],
            "connection_capacity_kw": ca["capacity_kw"],
            "augmentation_required": ca["capacity_kw"] > 1000,
            "customer_accepted": accepted,
            "rejection_reason": "" if accepted else random.choice(["Price too high", "Changed plans", "Alternative connection sought", "Project delayed"]),
        })

df = _to_df(co_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.connection_offers")
print(f"✓ connection_offers seeded: {len(co_rows)} rows")

# COMMAND ----------

# --- 23. Seed large_customer_pipeline ---
industries = ["Mining", "Data Centre", "Manufacturing", "Green Hydrogen", "EV Charging Hub",
              "Cold Storage", "Agriculture Processing", "Water Treatment"]
assessment_statuses = ["Pre-Application", "Under Assessment", "Offer Issued", "Accepted", "Withdrawn"]
lcp_rows = []
for i in range(40):
    dnsp = random.choice(["AusNet Services", "Ergon Energy", "Energex"])
    cap_mw = random.choice([1, 2, 5, 10, 20, 50, 100, 200])
    app_date = today - timedelta(days=random.randint(30, 548))
    lcp_rows.append({
        "application_id": f"LCA-{i+1:04d}",
        "dnsp": dnsp,
        "customer_name": f"Customer {chr(65 + i % 26)}{i // 26 + 1} Pty Ltd",
        "industry": random.choice(industries),
        "capacity_mw": float(cap_mw),
        "zone_substation": f"ZS-{random.choice(VIC_ZONE_SUBS + QLD_ZONE_SUBS).replace(' ','_').upper()}",
        "network_impact_mw": round(cap_mw * random.uniform(0.7, 1.0), 1),
        "assessment_status": random.choice(assessment_statuses),
        "application_date": app_date.isoformat(),
        "target_energisation_date": (app_date + timedelta(days=random.randint(365, 1095))).isoformat(),
    })

df = _to_df(lcp_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.large_customer_pipeline")
print(f"✓ large_customer_pipeline seeded: {len(lcp_rows)} rows")

# COMMAND ----------

# --- 24. Seed capital_projects ---
proj_cats = ["Reliability Augmentation", "Bushfire Mitigation", "DER Integration",
             "Asset Replacement", "EV Network Readiness", "Zone Substation Upgrade",
             "Feeder Reconfiguration", "Smart Meter Rollout"]
proj_statuses = ["In Progress", "Complete", "On Hold", "Pending Approval"]
cp_rows = []
for i in range(80):
    dnsp = random.choice(["AusNet Services", "Ergon Energy", "Energex"])
    cat = random.choice(proj_cats)
    budget = round(random.uniform(0.5, 50.0), 2)
    completion = round(random.uniform(0, 100), 1)
    actual = round(budget * completion / 100 * random.uniform(0.9, 1.15), 2)
    start = today - timedelta(days=random.randint(30, 730))
    target_end = start + timedelta(days=random.randint(180, 1095))
    cp_rows.append({
        "project_id": f"PROJ-{dnsp[:3].upper()}-{i+1:04d}",
        "dnsp": dnsp,
        "project_name": f"{cat} — {random.choice(VIC_ZONE_SUBS + QLD_ZONE_SUBS)} {i+1}",
        "category": cat,
        "region": "VIC1" if dnsp == "AusNet Services" else "QLD1",
        "budget_m_aud": budget,
        "actual_spend_m_aud": actual,
        "completion_pct": completion,
        "start_date": start.isoformat(),
        "target_completion_date": target_end.isoformat(),
        "status": "Complete" if completion >= 100 else random.choice(proj_statuses[:3]),
        "aer_approved": random.random() > 0.1,
    })

df = _to_df(cp_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.capital_projects")
print(f"✓ capital_projects seeded: {len(cp_rows)} rows")

# COMMAND ----------

# --- 25. Seed maintenance_orders ---
order_types = ["Planned Inspection", "Corrective Maintenance", "Condition Based Maintenance",
               "Emergency Response", "Vegetation Management"]
priorities = ["P1 Emergency", "P2 Urgent", "P3 Routine", "P4 Planned"]
priority_weights = [5, 15, 40, 40]
mo_statuses = ["Complete", "In Progress", "Scheduled", "Cancelled"]
mo_rows = []
all_assets = ([f"ZS-{s.replace(' ','_').upper()}" for s in VIC_ZONE_SUBS] +
              [f"ZS-{s.replace(' ','_').upper()}" for s in QLD_ZONE_SUBS])
for i in range(200):
    dnsp = random.choice(["AusNet Services", "Ergon Energy", "Energex"])
    priority = random.choices(priorities, weights=priority_weights)[0]
    sched = today - timedelta(days=random.randint(-30, 180))
    complete = sched + timedelta(hours=random.randint(1, 48)) if sched < today and random.random() > 0.2 else None
    mo_rows.append({
        "order_id": f"MO-{dnsp[:3].upper()}-{i+1:05d}",
        "dnsp": dnsp,
        "asset_id": random.choice(all_assets),
        "order_type": random.choice(order_types),
        "priority": priority,
        "scheduled_date": sched.isoformat(),
        "completed_date": complete.isoformat() if complete else "",
        "crew_id": f"CREW-{random.randint(1, 30):02d}",
        "contractor": random.choice(["", "Silcar", "Downer EDI", "UGL", "CPB Contractors", "Nexus"]),
        "status": "Complete" if complete else ("In Progress" if sched <= today else "Scheduled"),
        "cost_aud": round(random.uniform(500, 150000), 2),
    })

df = _to_df(mo_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.maintenance_orders")
print(f"✓ maintenance_orders seeded: {len(mo_rows)} rows")

# COMMAND ----------

# --- 26. Seed fault_response_kpis ---
fault_types = ["HV Overhead", "LV Overhead", "Underground Cable", "Transformer", "Switchgear"]
frk_rows = []
for dnsp in ["AusNet Services", "Ergon Energy", "Energex"]:
    for month_offset in range(12):
        month_date = today.replace(day=1) - timedelta(days=month_offset * 30)
        period_month = month_date.strftime("%Y-%m")
        regions = ["VIC1"] if dnsp == "AusNet Services" else ["QLD1"]
        for fault_type in fault_types:
            sla = {"HV Overhead": 240, "LV Overhead": 300, "Underground Cable": 360, "Transformer": 480, "Switchgear": 300}[fault_type]
            avg_response = round(random.uniform(20, 90), 1)
            avg_restore = round(random.uniform(sla * 0.4, sla * 1.1), 0)
            frk_rows.append({
                "dnsp": dnsp,
                "region": regions[0],
                "period_month": period_month,
                "fault_type": fault_type,
                "avg_response_time_min": avg_response,
                "avg_restoration_time_min": avg_restore,
                "sla_target_min": float(sla),
                "sla_compliance_pct": round(min(100, (sla / max(avg_restore, 1)) * 100 * random.uniform(0.85, 1.0)), 1),
                "total_faults": random.randint(5, 80),
            })

df = _to_df(frk_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.fault_response_kpis")
print(f"✓ fault_response_kpis seeded: {len(frk_rows)} rows")

# COMMAND ----------

# --- 27. Seed contractor_performance ---
contractors = ["Silcar", "Downer EDI", "UGL", "CPB Contractors", "Nexus Infrastructure",
               "Service Stream", "Ventia", "Programmed Facility Management"]
contr_rows = []
for dnsp in ["AusNet Services", "Ergon Energy", "Energex"]:
    for c in contractors[:5]:
        for year in [2024, 2025, 2026]:
            total_jobs = random.randint(50, 800)
            on_time = int(total_jobs * random.uniform(0.75, 0.98))
            contr_rows.append({
                "contractor_id": f"CONTR-{c[:4].upper()}-{dnsp[:3].upper()}",
                "dnsp": dnsp,
                "contractor_name": c,
                "period_year": year,
                "total_jobs": total_jobs,
                "completed_on_time": on_time,
                "defect_rate_pct": round(random.uniform(0.5, 8.0), 2),
                "avg_cost_variance_pct": round(random.uniform(-5.0, 15.0), 2),
                "quality_score": round(random.uniform(65, 98), 1),
            })

df = _to_df(contr_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.contractor_performance")
print(f"✓ contractor_performance seeded: {len(contr_rows)} rows")

# COMMAND ----------

# --- 28. Seed capex_opex_analysis ---
coa_asset_types = ["zone_substation", "feeder", "distribution_transformer", "switchgear", "conductor"]
coa_actions = ["Monitor and review", "Opex maintenance", "Defer capex", "Schedule replacement",
               "Urgent replacement", "Refurbishment", "Technology upgrade"]
coa_rows = []
for dnsp in ["AusNet Services", "Ergon Energy", "Energex"]:
    for i in range(60):
        asset_type = random.choice(coa_asset_types)
        hi = round(random.uniform(20, 95), 1)
        capex = round(random.uniform(0, 5000000) if hi < 50 else random.uniform(0, 500000), 2)
        opex = round(random.uniform(5000, 500000), 2)
        action = coa_actions[0] if hi > 75 else (coa_actions[-2] if hi < 35 else random.choice(coa_actions[1:5]))
        coa_rows.append({
            "dnsp": dnsp,
            "asset_id": f"ASSET-{dnsp[:3].upper()}-{i+1:04d}",
            "asset_type": asset_type,
            "period_year": 2025,
            "capex_aud": capex,
            "opex_aud": opex,
            "health_index": hi,
            "recommended_action": action,
        })

df = _to_df(coa_rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.capex_opex_analysis")
print(f"✓ capex_opex_analysis seeded: {len(coa_rows)} rows")

# COMMAND ----------

# ============================================================
# GRANT SP ACCESS
# ============================================================
DNSP_TABLES = [
    "rin_submissions", "stpis_performance", "revenue_monitoring",
    "pricing_proposals", "regulatory_milestones",
    "network_tariff_structures", "tariff_migration_progress",
    "tariff_revenue_by_class", "demand_tariff_performance",
    "bmp_asset_register", "elc_inspections", "fire_risk_incidents",
    "bmp_spend", "seasonal_readiness",
    "cso_payments", "rural_feeder_performance", "raps_fleet",
    "non_network_alternatives", "ergon_energex_split",
    "connection_applications", "timely_connections_kpi",
    "connection_offers", "large_customer_pipeline",
    "capital_projects", "maintenance_orders", "fault_response_kpis",
    "contractor_performance", "capex_opex_analysis",
]

for t in DNSP_TABLES:
    spark.sql(f"GRANT SELECT ON TABLE {catalog}.gold.{t} TO `{sp_id}`")
    print(f"  ✓ SELECT on {t}")

print("\n✓ All Phase 5B SP grants applied")

# COMMAND ----------

# ============================================================
# VERIFY — Row counts
# ============================================================
for t in DNSP_TABLES:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.{t}").collect()[0]["cnt"]
    print(f"  {t}: {count} rows")

print("\n✓ Phase 5B DNSP tables setup complete!")
