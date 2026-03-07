# Product Requirements Document (PRD)
# AUS Energy Copilot — Phase 1: Market Intelligence & AI Assistant

**Version:** 1.0
**Date:** 19 February 2026
**Author:** Sourabh Ghose
**Status:** Draft

---

## 1. Executive Summary

AUS Energy Copilot is an AI-powered market intelligence and trading assistant for the Australian National Electricity Market (NEM). Phase 1 delivers a real-time market analytics platform (comparable to NemSight) combined with a conversational AI copilot — built entirely on Databricks-native technologies.

The product ingests open-source data from AEMO/NEMWEB, OpenElectricity, and Bureau of Meteorology weather feeds, processes it through a medallion lakehouse architecture, and serves it through three interfaces: interactive dashboards, natural language Genie analytics, and an AI agent copilot.

**Target users:** Energy traders, portfolio analysts, market analysts, and trading desk managers operating in the Australian NEM. Phase 4 extends to Distribution Network Service Providers (DNSPs) with network operations, DER management, and reliability analytics.

---

## 2. Problem Statement

### Current Pain Points

1. **Fragmented market data**: Traders juggle multiple tools (NEMWEB portal, spreadsheets, NemSight, custom scripts) to piece together a market view. There is no unified platform combining prices, generation, weather, and constraints.

2. **Reactive, not predictive**: Existing tools like NemSight show what *happened*. Traders need to know what *will* happen — price spikes, demand surges, renewable generation shortfalls — before they occur.

3. **Manual analysis bottleneck**: Understanding *why* a price spiked requires manually correlating outages, weather events, interconnector constraints, and demand patterns across multiple data sources. This takes experienced analysts 30-60 minutes per event.

4. **No natural language access**: Non-technical stakeholders (risk managers, executives) cannot self-serve market analytics without requesting analyst support.

5. **High tool costs**: Commercial solutions like NemSight + EOT can cost $50K-$200K+ per year in licensing. An open-data, Databricks-native solution reduces this significantly for teams already on the platform.

### Opportunity

Build an AI-first market intelligence platform that:
- Unifies all NEM market data in a governed lakehouse
- Provides predictive intelligence (price/demand/generation forecasts)
- Enables natural language interaction with market data
- Delivers proactive AI-driven insights and alerts
- Runs entirely on Databricks, leveraging existing platform investment

---

## 3. Target Users & Personas

### P1: Energy Trader (Primary)
- **Goal**: Make profitable trading decisions quickly
- **Needs**: Real-time prices, generation mix, interconnector flows, price forecasts, competitor bidstack analysis, alerts on price spikes
- **AI Copilot Use**: "What's the SA spot price forecast for 4-6pm today?", "Why did QLD price spike at 2pm?", "Show me the bidstack changes for Bayswater in the last hour"

### P2: Market Analyst (Primary)
- **Goal**: Produce market reports and identify trends
- **Needs**: Historical analysis, cross-regional comparisons, seasonal patterns, renewable penetration trends, demand correlations
- **AI Copilot Use**: "Compare average peak prices across all regions for Q4 2025 vs Q4 2024", "What's the correlation between wind generation and SA prices?"

### P3: Risk / Portfolio Manager (Secondary)
- **Goal**: Monitor portfolio exposure to market movements
- **Needs**: Price volatility metrics, regional risk profiles, interconnector congestion patterns, demand forecast accuracy
- **AI Copilot Use**: "What was the volatility in VIC prices this week?", "Summarize interconnector congestion events in the last month"

### P4: Trading Desk Manager / Executive (Secondary)
- **Goal**: High-level market overview without technical complexity
- **Needs**: Daily market summaries, key metrics dashboard, trend alerts
- **AI Copilot Use**: "Give me a morning market brief", "What were the key market events yesterday?"

---

## 4. Product Scope — Phase 1

### 4.1 In Scope

#### Data Platform (Foundation Layer)
- [x] Ingest AEMO NEMWEB public data (dispatch, trading, generation, interconnectors, constraints)
- [x] Ingest OpenElectricity API data (curated generation, demand, price, emissions)
- [x] Ingest Open-Meteo / BOM weather data (temperature, wind, solar radiation, precipitation)
- [x] Ingest APVI rooftop solar estimates
- [x] Medallion architecture: Bronze (raw) → Silver (cleaned/joined) → Gold (analytics-ready)
- [x] Unity Catalog governance with full lineage
- [x] Incremental/streaming data pipelines via Lakeflow SDP

#### Real-Time Market Dashboard (NemSight Alternative)
- [x] Live regional spot prices (NSW, QLD, VIC, SA, TAS) — 5-minute refresh
- [x] Generation mix by fuel type per region (coal, gas, hydro, wind, solar, battery)
- [x] Interconnector flows and limits (directional MW, congestion indicators)
- [x] Demand vs supply balance per region
- [x] Price history charts (intraday, daily, weekly, monthly)
- [x] Weather overlay (temperature, wind speed, solar irradiance by NEM region)
- [x] Configurable price/demand threshold alerts

#### AI/ML Forecasting Models
- [x] Regional spot price forecasting (1-hour, 4-hour, 24-hour ahead)
- [x] Regional demand forecasting (1-hour, 4-hour, 24-hour ahead)
- [x] Wind generation forecasting (per region)
- [x] Solar generation forecasting (utility + rooftop, per region)
- [x] Model performance tracking and versioning via MLflow

#### Genie Spaces (Natural Language Analytics)
- [x] NEM Prices & Demand Genie space
- [x] Generation & Fuel Mix Genie space
- [x] Interconnectors & Constraints Genie space
- [x] Sample questions and business glossary for each space

#### AI Copilot Agent
- [x] Conversational chat interface (Streamlit-based)
- [x] Market Q&A using RAG over AEMO documentation + market rules
- [x] Price spike / anomaly explanation (causal analysis)
- [x] AI-generated daily/hourly market summaries
- [x] Forecast retrieval ("What's the price forecast for SA?")
- [x] Tool calling: query live data, invoke forecast models, search historical patterns
- [x] Proactive alerts: predicted price spikes, demand surges, renewable shortfalls

#### Frontend Application
- [x] Databricks App (Streamlit) with tabbed layout
- [x] Tabs: Dashboard, Forecasts, Copilot Chat, Genie Analytics, Alerts
- [x] Responsive web design (desktop + tablet)
- [x] OAuth authentication via Databricks

### 4.2 Out of Scope (Deferred to Phase 2)
- Deal capture and portfolio management (ETRM)
- Forward curve construction and mark-to-market
- Basic risk analytics (portfolio exposure, P&L attribution)
- PPA valuation copilot
- FCAS market analytics and co-optimization
- Settlement reconciliation (AEMO vs internal)

### 4.3 Out of Scope (Deferred to Phase 3)
- Bid preparation and AEMO submission (EnergyOffer-like)
- Advanced risk analytics (VaR, Greeks, Monte Carlo sensitivity)
- Gas market data (STTM, DWGM)
- WEM (Western Australian) market data
- Battery dispatch optimization
- Native mobile application
- Multi-tenancy / white-labeling

---

## 5. Data Architecture

### 5.1 Data Sources

| Source | Data | Frequency | Access Method | License |
|--------|------|-----------|---------------|---------|
| **NEMWEB** | Dispatch prices, generation, interconnectors, constraints, bids | 5-min | HTTP file download (CSV ZIP) | Public / Free |
| **NEMWEB** | Trading interval summaries | 30-min | HTTP file download (CSV ZIP) | Public / Free |
| **NEMWEB** | Pre-dispatch forecasts (PREDISPATCH, P5MIN, STPASA) | 5-min / 30-min | HTTP file download (CSV ZIP) | Public / Free |
| **OpenElectricity** | Curated generation, demand, price, emissions, capacity | 5-min | REST API (JSON) | CC BY 4.0 / Free API key |
| **Open-Meteo** | Temperature, wind, solar radiation, humidity, precipitation | Hourly | REST API (JSON) | Free / No key |
| **APVI** | Rooftop solar generation by state/postcode | 30-min | REST API / CSV | Free |

### 5.2 Medallion Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                                │
│  Analytics-ready, aggregated, feature-engineered                 │
│                                                                  │
│  gold.nem_prices_5min          - Regional prices, 5-min         │
│  gold.nem_prices_30min         - Trading interval prices         │
│  gold.nem_demand_actuals       - Actual demand by region         │
│  gold.nem_generation_by_fuel   - Gen by fuel type & region       │
│  gold.nem_generation_by_unit   - Gen by individual DUID          │
│  gold.nem_interconnectors      - Flows, limits, congestion       │
│  gold.nem_constraints_active   - Active constraint sets          │
│  gold.nem_bidstacks            - Aggregated bidstack by region   │
│  gold.weather_nem_regions      - Weather mapped to NEM regions   │
│  gold.solar_rooftop            - Rooftop PV by region            │
│  gold.price_forecasts          - ML forecast outputs             │
│  gold.demand_forecasts         - ML forecast outputs             │
│  gold.generation_forecasts     - ML forecast outputs             │
│  gold.market_events            - Detected anomalies & events     │
│  gold.daily_market_summary     - AI-generated daily summaries    │
├──────────────────────────────────────────────────────────────────┤
│                       SILVER LAYER                               │
│  Cleaned, validated, joined, SCD Type 2 where applicable        │
│                                                                  │
│  silver.dispatch_prices        - Cleaned dispatch prices         │
│  silver.dispatch_generation    - Cleaned generation data         │
│  silver.dispatch_demand        - Cleaned demand data             │
│  silver.dispatch_interconnect  - Cleaned interconnector data     │
│  silver.trading_intervals      - 30-min trading data             │
│  silver.predispatch_prices     - Pre-dispatch price forecasts    │
│  silver.predispatch_demand     - Pre-dispatch demand forecasts   │
│  silver.generator_registry     - Generator metadata (DUID, fuel) │
│  silver.constraint_sets        - Constraint equations & RHS      │
│  silver.bidstacks_raw          - Individual generator bids       │
│  silver.weather_observations   - Cleaned weather time series     │
│  silver.weather_forecasts      - Forward weather forecasts       │
│  silver.rooftop_solar          - APVI rooftop PV data            │
├──────────────────────────────────────────────────────────────────┤
│                       BRONZE LAYER                               │
│  Raw ingestion, append-only, schema-on-read                     │
│                                                                  │
│  bronze.nemweb_dispatch_is     - Raw DISPATCHIS files            │
│  bronze.nemweb_dispatch_price  - Raw DISPATCHPRICE files         │
│  bronze.nemweb_dispatch_gen    - Raw DISPATCH_UNIT_SCADA         │
│  bronze.nemweb_dispatch_inter  - Raw DISPATCHINTERCONNECTORRES   │
│  bronze.nemweb_trading_is      - Raw TRADINGIS files             │
│  bronze.nemweb_predispatch     - Raw PREDISPATCH files           │
│  bronze.nemweb_bids            - Raw BIDPEROFFER files           │
│  bronze.nemweb_constraints     - Raw constraint files            │
│  bronze.openelec_raw           - Raw OpenElectricity API resp.   │
│  bronze.weather_raw            - Raw Open-Meteo API responses    │
│  bronze.apvi_raw               - Raw APVI solar data             │
└──────────────────────────────────────────────────────────────────┘
```

### 5.3 Data Freshness Targets

| Data Type | Target Latency | Pipeline Mode |
|-----------|---------------|---------------|
| Dispatch prices (5-min) | < 2 minutes from AEMO publish | Streaming (SDP) |
| Generation data (5-min) | < 2 minutes from AEMO publish | Streaming (SDP) |
| Trading intervals (30-min) | < 5 minutes | Triggered batch |
| Pre-dispatch forecasts | < 5 minutes | Triggered batch |
| Weather data (hourly) | < 15 minutes | Scheduled batch |
| Rooftop solar (30-min) | < 15 minutes | Scheduled batch |
| ML forecasts | < 10 minutes after new dispatch | Triggered batch |
| Daily market summary | By 06:00 AEST daily | Scheduled batch |

### 5.4 Next Step: Adopt NEMWEB Solution Accelerator for Bronze/Silver Ingestion

**Repository:** `databricks-industry-solutions/australian-energy-nemweb-analytics` (private, Databricks)
**Author:** David O'Keeffe (Databricks)
**Status:** Deployed to `energy_copilot_catalog.nemweb_analytics` (2026-03-05). Bid pipeline fully operational (2026-03-06) with streaming CSV parser fix for serverless 1GB memory limit.

#### Overview

A Databricks Solution Accelerator providing a production-grade NEMWEB ingestion pipeline using **Databricks Asset Bundles (DABs)** + **Lakeflow SDP** + **Unity Catalog**. It replaces our custom `nemweb_downloader.py` / `01_nemweb_ingest.py` scripts with a declarative, serverless pipeline covering **25+ NEMWEB report types** (vs our current ~5).

#### Architecture

- **Deployment:** Databricks Asset Bundles (`databricks.yml`), single `./scripts/deploy.sh dev` command
- **Pipeline:** Lakeflow SDP (serverless), runs every 5 minutes via Databricks Job
- **Data source:** Custom Spark DataSource V2 (`NemwebArrowDataSource`) — downloads NEMWEB ZIPs, parses CSV, returns Arrow batches
- **Schema evolution:** Auto-detects new/removed columns in NEMWEB CSV files
- **DQ checks:** DQX YAML expectations on silver/gold tables
- **ML:** XGBoost demand forecasting + price spike classification, registered in Unity Catalog

#### NEMWEB Coverage Comparison

| Report Type | Our Pipeline | Accelerator |
|---|---|---|
| DISPATCHPRICE (5-min prices) | Yes | Yes |
| DISPATCH_UNIT_SCADA | Yes | Yes |
| DISPATCHINTERCONNECTORRES | Yes | Yes |
| DISPATCHCONSTRAINT | Partial (bronze only) | Yes (bronze + silver) |
| DISPATCHLOAD | No | Yes |
| DISPATCH_REGION_SUM | No | Yes |
| DISPATCH_LOCAL_PRICE | No | Yes |
| TRADINGPRICE / TRADING_REGION | Yes | Yes |
| PREDISPATCH (price, load, constraint, interconnector, local price) | No | Yes |
| P5MIN (unit, constraint, case, local price, region, interconnector) | No | Yes |
| STPASA (case, region, constraint, interconnector, DUID availability) | No | Yes |
| MTPASA (interconnector, constraint) | No | Yes |
| BIDPEROFFER / BIDDAYOFFER | No | Yes |
| GENCONSET / GENCON | No | Yes |
| Rooftop PV (actual + forecast) | Via APVI | Via NEMWEB |
| FCAS (dispatch requirements) | No | Yes |
| Generator registration (stations, DUIDs, participants, loss factors) | Partial (one-time) | Yes (scheduled refresh) |
| Demand forecasts | No | Yes |
| Weather (BOM / Open-Meteo) | Yes | No |
| Solar (APVI API) | Yes | No |

#### Tables Produced

**Bronze (~40 tables):** `bronze_nem_dispatch_price`, `bronze_nem_dispatch_unit_scada`, `bronze_nem_dispatch_constraint`, `bronze_nem_dispatch_interconnector`, `bronze_nem_dispatch_case`, `bronze_nem_dispatch_region_sum`, `bronze_nem_dispatch_local_price`, `bronze_nem_dispatch_interconnector_res`, `bronze_nem_trading_price`, `bronze_nem_trading_region`, `bronze_nem_operational_demand`, `bronze_nem_predispatch_price`, `bronze_nem_predispatch_load`, `bronze_nem_predispatch_constraint`, `bronze_nem_predispatch_interconnector`, `bronze_nem_predispatch_local_price`, `bronze_nem_predispatch_region_sum`, `bronze_nem_predispatch_case`, `bronze_nem_predispatch_blocked_constraint`, `bronze_nem_p5min_unit`, `bronze_nem_p5min_constraint`, `bronze_nem_p5min_case`, `bronze_nem_p5min_local_price`, `bronze_nem_p5min_region`, `bronze_nem_p5min_interconnector`, `bronze_nem_p5min_blocked_constraint`, `bronze_nem_stpasa_case`, `bronze_nem_stpasa_interconnector`, `bronze_nem_stpasa_region`, `bronze_nem_stpasa_constraint`, `bronze_nem_stpasa_duid_availability`, `bronze_nem_mtpasa`, `bronze_nem_mtpasa_interconnector`, `bronze_nem_mtpasa_constraint`, `bronze_nem_bid_dayoffer`, `bronze_nem_bid_peroffer`, `bronze_nem_gencon`, `bronze_nem_genconset`, `bronze_nem_genconset_invoke`, `bronze_nem_dispatch_fcas_req_run`, `bronze_nem_rooftop_pv_actual`, `bronze_nem_rooftop_pv_forecast`, `bronze_nem_demand_forecast`, `bronze_nem_demand_operational_forecast`, `bronze_nem_genunits`, `bronze_nem_dudetail`, `bronze_nem_stations`, `bronze_nem_participants`, `bronze_nem_interconnector_master`, `bronze_nem_regions`, `bronze_nem_loss_factors`, `bronze_nem_interconnector_constraint`, `bronze_nem_transmission_loss_factor`, `bronze_nemweb_file_manifest`

**Silver (~8 tables):** `silver_nem_dispatch_price`, `silver_nem_trading_price`, `silver_nem_facility_dimension`, `silver_nem_dispatch_unit_scada`, `silver_nem_interconnector_flow`, `silver_nem_curtailment`, `silver_nem_interval_completeness`, `silver_nem_file_freshness`

**Gold (~5 tables):** `gold_nem_dispatch_price_30min`, `gold_nem_dispatch_price_daily`, `gold_nem_generation_by_fuel_type`, `gold_nem_interconnector_flow_30min`, `gold_nem_curtailment_30min`

#### Integration Plan — Status

1. **[DONE] Deploy the accelerator** to `energy_copilot_catalog` — 78 tables in `nemweb_analytics` schema, DLT pipeline running every 5 min
2. **[DONE] Create views** mapping accelerator gold tables to `energy_copilot_catalog.gold`:
   - `gold.nem_prices_5min` (167K rows), `gold.nem_generation_by_fuel` (968K rows), `gold.nem_interconnectors` (167K rows), `gold.nem_facilities` (650 rows), `gold.nem_region_summary` (3.5K rows)
   - **[DONE 2026-03-06]** `gold.nem_bid_stack` (11.5M rows) — bid price/volume by DUID/band/interval, joined with facilities for region/fuel metadata
   - **[DONE 2026-03-06]** `gold.nem_bid_statistics` (12 rows) — aggregated bid metrics per region
3. **Keep our weather + solar pipelines** — the accelerator doesn't cover Open-Meteo or APVI API
4. **[DONE] App layer reads from gold views** — ~61 endpoints wired to real data across 8 router files
5. **Retire** `nemweb_downloader.py`, `01_nemweb_ingest.py`, `00_historical_backfill.py` — superseded
6. **ML models deployed** but tables not yet populated (training jobs need to be triggered)

#### Key Benefits

- **5x more NEMWEB data** — fills Tier 1 gaps (bids, FCAS, constraints, predispatch, STPASA, MTPASA, rooftop PV, demand forecasts)
- **Production-grade SDP** — declarative, serverless, schema evolution tracking, DQ checks
- **DABs deployment** — reproducible, CI/CD-ready, multi-environment
- **Built-in ML** — demand forecasting + price spike classification out of the box

#### Caveats

- **Private repo** (Databricks License) — confirm internal reuse rights
- **Depends on forked OpenElectricity SDK** — custom wheel with PySpark integration (PR #14 pending merge to upstream)
- **Phase 3 streaming not fully deployed** — current folder scanning code written but wheel not rebuilt
- **Gold table names differ** from ours — need views or rename to maintain API compatibility
- **Azure workspace** (original) — needs testing on AWS (our workspace is AWS `fevm-energy-copilot`)

### 5.5 Data Gap Action Plan — Progress Tracker

**Current state (2026-03-06):** ~75 of ~172 API endpoints serve real NEMWEB data. The remaining serve mock data with fallback.

#### Dashboard Coverage After Each Step

| Step | Action | Real Endpoints | Cumulative | Status |
|---|---|---|---|---|
| Step 1a | Deploy NEMWEB accelerator + gold views | +56 | ~56 | **DONE** (2026-03-05) |
| Step 1b | Wire bidding endpoints to bid pipeline | +5 | ~61 | **DONE** (2026-03-06) |
| Step 1c | Wire forecast + remaining stubs to existing gold tables | +14 | ~75 | **DONE** (2026-03-06) |
| Step 2 | Add settlement + SRA ingestion | +8 | ~83 | Planned |
| Step 3 | Add AEMO Market Notices API | +3 | ~86 | Planned |
| Step 4 | Scrape ASX Energy free end-of-day futures prices | +15 | ~101 | Planned |
| Step 5 | Load static lookups (emissions, DER register, LGC) | +8 | ~109 | Planned |
| Step 6 | Add Gas Bulletin Board API | +1 | ~110 | Planned |
| Step 7 | Keep synthetic (credit risk, cyber, retail, hydrogen, regulatory) | +0 | ~110 | By design |

#### Step 1: Deploy NEMWEB Solution Accelerator — COMPLETED (2026-03-05 / 2026-03-06)

**Unlocked:** ~75 endpoints serving real NEMWEB data

**Completed:**
- [x] Deployed `australian-energy-nemweb-analytics` DAB to `energy_copilot_catalog.nemweb_analytics` (78 tables)
- [x] Created gold views in `energy_copilot_catalog.gold` (nem_prices_5min, nem_generation_by_fuel, nem_interconnectors, nem_facilities, nem_region_summary)
- [x] Fixed BIDPEROFFER_D OOM: streaming CSV parser (`_parse_csv_streaming()`) + 10K-row chunked RecordBatch yielding for serverless 1GB limit
- [x] Created bid gold views: `gold.nem_bid_stack` (11.5M rows), `gold.nem_bid_statistics` (12 rows)
- [x] Wired ~61 endpoints across 8 router files (Step 1a+1b)
- [x] Wired 4 batch_forecasting.py endpoints to ML forecast gold tables (price_forecasts, demand_forecasts, demand_actuals)
- [x] Wired 6 stubs.py endpoints (spot-forecast, stpasa-adequacy, aemo-market-ops, settlement, vpp, planned-outage)
- [x] Wired 3 batch_bidding.py endpoints (market-bidding-strategy, wholesale-bidding-strategy, system-operator via anomaly_events)

**Key technical fix:** NEMWEB `Bidmove_Complete` files contain BIDPEROFFER_D with ~600K+ rows per day. The default CSV parser loaded everything into memory, hitting the 1GB serverless UDF limit. Fixed by adding a streaming generator that yields row dicts one at a time and batches into 10K-row Arrow RecordBatches.

#### Step 2: Add Settlement & SRA Ingestion (2-3 days)

**Unlocks:** ~11 endpoints (Settlement Analytics, 5-Min Settlement, SRA Analytics, Congestion Revenue)

| Data | Source | Method |
|---|---|---|
| Settlement statements (trading amounts, FCAS payments) | NEMWEB `/Reports/Current/Settlements/` | CSV ZIP download — same pattern as dispatch. Add to accelerator's `NemwebArrowDataSource` or standalone notebook |
| SRA auction results | NEMWEB `/Reports/Current/SRA_Results/` | CSV ZIP download — bronze table, aggregate to gold |

#### Step 3: Add AEMO Market Notices API (1 day)

**Unlocks:** ~3 endpoints (Market Events, Market Notices, Alerts)

| Data | Source | Method |
|---|---|---|
| Market notices (LOR, directions, interventions) | AEMO Market Notices API (`api.aemo.com.au`) | REST API → JSON → Delta. Paginated GET, ~100 notices/day. Free, public |

#### Step 4: ASX Energy Futures — Free End-of-Day Prices (2 days)

**Unlocks:** ~15 of 27 futures/hedging endpoints (futures dashboard, forward curves, price discovery, hedging portfolio)

| Data | Source | Method |
|---|---|---|
| Base/peak/cap futures settlement prices | ASX Energy (`asxenergy.com.au/futures_au`) | Free end-of-day CSV with 20-min delay. Daily scrape → bronze → gold. Covers base load, peak, $300 cap contracts for NSW, QLD, VIC, SA |
| Options data (Greeks, OI, vol surface) | ASX Energy (paid) or Bloomberg/Refinitiv | **Not available free** — keep synthetic for 12 options-specific endpoints, label as "illustrative" |

**Note:** Forward curves can also be derived from AEMO settlement file contract data (Step 2), providing a second independent source.

#### Step 5: Static Lookup Tables (2 days)

**Unlocks:** ~8 endpoints (Carbon/Emissions, DER, Sustainability)

| Data | Source | Method | Refresh |
|---|---|---|---|
| Emissions factors (NGA scope 2, kg CO2/MWh per fuel) | DCEEW National Greenhouse Accounts | One-time CSV load, ~30 rows | Annual |
| LGC/STC certificate prices | CER LGC Registry + ASX Environmental | End-of-day scrape or manual CSV | Daily/weekly |
| DER Register (rooftop PV, battery by postcode) | CER DER Register API (`api.rec-registry.gov.au`) | REST API bulk download, ~1M records | Monthly |
| AER enforcement actions | AER Enforcement Register (web) | Simple scrape, ~50 entries/year | Quarterly |
| Retail DMO/VDO prices | AER Annual Retail Report | Manual XLSX extract | Annual |

#### Step 6: Gas Bulletin Board API (2-3 days)

**Unlocks:** ~1 dashboard (Gas Market)

| Data | Source | Method |
|---|---|---|
| Gas hub prices (Wallumbilla, Sydney, Adelaide, Brisbane) | AEMO GBB API (`api.aemo.com.au/gbb`) | REST API → JSON → Delta. Free, public, well-documented |
| Pipeline flows and capacity | AEMO GBB API | Same endpoint, different data tables |

#### Step 7: Keep Synthetic — Customer-Specific Data (0 days)

**~7 endpoints remain synthetic by design** — these require customer-specific or restricted data:

| Dashboard | Why Synthetic | Path to Real Data |
|---|---|---|
| Credit Risk / Counterparty Exposure | Requires customer's own ETRM/portfolio system | Phase 2 deal capture integration (section 15.1) |
| Cyber Security (OT/ICS) | Requires internal SOC/ASD data | Customer-provided or keep as capability demo |
| Options Greeks / Vol Surface | Requires paid ASX Energy or Bloomberg feed | Customer brings their own market data subscription |
| Retail Market Shares | AER publishes annually as PDF, no API | Manual annual update or keep as illustrative |

#### Estimated Timeline

| Week | Steps | Outcome |
|---|---|---|
| Week 1 | Step 1 (accelerator deployment + views) | ~110 real endpoints |
| Week 2 | Steps 2 + 3 (settlement, SRA, market notices) | ~124 real endpoints |
| Week 3 | Steps 4 + 5 + 6 (ASX futures, static lookups, gas) | ~148 real endpoints |

**Result:** 148 of 155 endpoints serving real data within 3 weeks. The remaining 7 are synthetic by design (customer-specific data).

#### Data Source Summary After Completion

| Source | Endpoints Powered | Access | Cost |
|---|---|---|---|
| NEMWEB (via accelerator) | ~110 | Public, free | $0 |
| NEMWEB (settlement + SRA, new pipelines) | ~11 | Public, free | $0 |
| AEMO Market Notices API | ~3 | Public, free | $0 |
| ASX Energy (free EOD) | ~15 | Public, 20-min delayed | $0 |
| Open-Meteo (existing pipeline) | ~5 | Public, free | $0 |
| APVI (existing pipeline) | ~2 | Public, free | $0 |
| Static lookups (DCEEW, CER, AER) | ~8 | Public, free | $0 |
| AEMO GBB (gas) | ~1 | Public, free | $0 |
| Synthetic (customer-specific) | ~7 | N/A | $0 |
| **Total** | **~155** | | **$0** |

### 5.6 Lakebase Serving Layer — Sub-10ms Dashboard Reads

#### Problem

The 11 real-data endpoints in `home.py` query gold Delta tables via a SQL Warehouse connection. This has two issues:
1. **Latency:** SQL Warehouse queries take 200-800ms (cold start can be seconds), too slow for real-time dashboard UX
2. **Cost:** Every page load spins a SQL Warehouse, even for data that changes only every 5 minutes
3. **Mock endpoints:** The remaining ~145 endpoints serve hardcoded inline mock data with no path to real data without a low-latency read layer

#### Solution: Gold → Synced Tables → Lakebase → FastAPI

```
Gold Delta Tables (analytics)
    ↓ Synced Tables (Continuous mode, ~15s latency)
Lakebase Postgres (OLTP reads)
    ↓ psycopg2 connection pool
FastAPI endpoints (<10ms reads, mock fallback)
```

#### Architecture

**Single snapshot table** — `energy_copilot_catalog.gold.dashboard_snapshots` stores pre-computed JSON payloads keyed by `(endpoint_path, region)`. This avoids creating 155 individual tables.

```sql
CREATE TABLE energy_copilot_catalog.gold.dashboard_snapshots (
    endpoint_path   STRING      NOT NULL,
    region          STRING      NOT NULL DEFAULT 'ALL',
    snapshot_at     TIMESTAMP   NOT NULL,
    payload_json    STRING      NOT NULL,
    CONSTRAINT pk PRIMARY KEY (endpoint_path, region)
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

**Synced Tables** (GA, replaces deprecated Online Tables) auto-replicate Delta → Lakebase with ~15s latency in Continuous mode. Requires CDF enabled + primary key on the Delta table.

**Lakebase Postgres** serves as the low-latency OLTP read layer. The FastAPI backend connects via a `psycopg2` threaded connection pool (2-10 connections).

#### FastAPI Integration

Three new helpers in `app/routers/shared.py`:

| Helper | Purpose |
|---|---|
| `_get_lakebase_pool()` | Lazy-init psycopg2 `ThreadedConnectionPool` using `LAKEBASE_HOST/PORT/DATABASE/USER/PASSWORD` env vars |
| `_query_lakebase(sql, params)` | Run arbitrary SQL against Lakebase Postgres, return list of dicts (same interface as `_query_gold`) |
| `_query_snapshot(endpoint_path, region)` | Read pre-computed JSON payload from `dashboard_snapshots` table, parse and return, or `None` on miss |

#### Endpoint Rewiring Pattern

Every mock endpoint gets a 3-line addition at the top of the function body. Zero risk — if Lakebase is unavailable, `_query_snapshot()` returns `None` and the endpoint falls through to existing mock logic unchanged.

```python
@router.get("/api/some/endpoint")
async def some_endpoint(region: str = "NSW1"):
    # --- Lakebase snapshot lookup ---
    snap = _query_snapshot("/api/some/endpoint", region)
    if snap is not None:
        return snap
    # --- existing mock logic below (unchanged) ---
    ...
```

**Modules to rewire:**

| Module | Endpoints | Pattern |
|---|---|---|
| `dashboards.py` | 12 | Snapshot lookup before cache/mock |
| `sidebar.py` | 26 | Snapshot lookup, some with `region` param |
| `stubs.py` | 31 | Snapshot lookup before inline mock |
| `market_events.py` | 10 | Snapshot lookup before mock |
| `spike_analysis.py` | 18 | Snapshot lookup before mock |
| `batch_forecasting.py` | 9 | Snapshot lookup before mock |
| `batch_futures_hedging.py` | 27 | Snapshot lookup before mock |
| `batch_bidding.py` | 9 | Snapshot lookup before mock |
| **Total** | **~142** | |

#### Home.py Migration

The 11 real SQL Warehouse endpoints in `home.py` switch from `_query_gold(sql)` to `_query_lakebase(sql)`. The SQL stays the same (both speak PostgreSQL-compatible SQL). The existing mock fallback remains.

#### Mock Data Generator

A Databricks Job (`pipelines/09_dashboard_snapshot_generator.py`) runs every 5 minutes:
1. Generates JSON payloads for all ~155 endpoints (same logic as inline mock, extracted to helper functions)
2. Writes/merges into `gold.dashboard_snapshots` using `MERGE INTO` on `(endpoint_path, region)`
3. Synced Tables replicates to Lakebase within ~15 seconds

As real data pipelines come online (section 5.5), the generator is replaced endpoint-by-endpoint with real aggregations from gold tables.

#### Health Check

`/api/system/health` includes a Lakebase connectivity check:

| Field | Description |
|---|---|
| `lakebase_ok` | `true` if `SELECT 1` succeeds against Lakebase pool |
| `snapshot_count` | Number of endpoints with active snapshots |
| `snapshot_freshness_seconds` | Age of oldest snapshot |

#### Environment Variables

| Variable | Value | Description |
|---|---|---|
| `LAKEBASE_HOST` | (from Lakebase provisioning) | Postgres host endpoint |
| `LAKEBASE_PORT` | `5432` | Default Postgres port |
| `LAKEBASE_DATABASE` | `energy_copilot` | Lakebase database name |
| `LAKEBASE_USER` | `energy_copilot_app` | Service principal or app user |
| `LAKEBASE_PASSWORD` | (from secret scope) | Connection password |

#### Rollback

Every endpoint retains its full mock logic. If Lakebase is unavailable:
- `_query_snapshot()` returns `None` → endpoint falls through to mock data
- `_query_lakebase()` returns `None` → `home.py` endpoints fall through to existing `_query_gold()` → mock
- Zero-risk deployment with graceful degradation

#### Performance Targets

| Metric | Target | Current (mock) | Current (SQL Warehouse) |
|---|---|---|---|
| Dashboard endpoint latency (p50) | < 10ms | ~2ms (inline mock) | 200-800ms |
| Dashboard endpoint latency (p99) | < 50ms | ~5ms | 2-5s (cold start) |
| Data freshness | < 20 seconds | N/A (static mock) | ~25s (cache TTL) |
| SQL Warehouse dependency for reads | None | None | Required |
| Fallback on Lakebase failure | Mock data | N/A | N/A |

---

## 6. AI/ML Models Specification

### 6.1 Regional Spot Price Forecasting

| Attribute | Detail |
|-----------|--------|
| **Objective** | Predict regional reference price (RRP) for each NEM region |
| **Horizons** | 1-hour, 4-hour, 24-hour ahead (rolling) |
| **Granularity** | 5-minute dispatch intervals, aggregated to 30-min for longer horizons |
| **Features** | Historical prices, demand, generation by fuel, interconnector flows, weather (temp, wind, solar rad), calendar (hour, day-of-week, holiday, season), AEMO pre-dispatch forecasts |
| **Algorithms** | Ensemble: LightGBM (primary) + temporal fusion transformer (experimental) |
| **Target Metrics** | MAE < $15/MWh (1-hr), MAE < $25/MWh (4-hr), MAE < $40/MWh (24-hr) |
| **Training** | Rolling window, retrained weekly on latest 12 months |
| **Serving** | Batch inference on each new dispatch interval; results to `gold.price_forecasts` |

### 6.2 Regional Demand Forecasting

| Attribute | Detail |
|-----------|--------|
| **Objective** | Predict total demand (MW) per NEM region |
| **Horizons** | 1-hour, 4-hour, 24-hour ahead |
| **Features** | Historical demand, temperature forecast, solar radiation, calendar features, rooftop solar estimate, AEMO pre-dispatch demand |
| **Algorithms** | LightGBM + linear regression baseline |
| **Target Metrics** | MAPE < 3% (1-hr), MAPE < 5% (24-hr) |

### 6.3 Wind Generation Forecasting

| Attribute | Detail |
|-----------|--------|
| **Objective** | Predict aggregate wind generation (MW) per region |
| **Horizons** | 1-hour, 4-hour, 24-hour ahead |
| **Features** | Wind speed/direction at hub height, historical wind generation, installed capacity, seasonal patterns |
| **Algorithms** | Gradient boosted trees + quantile regression for confidence intervals |
| **Target Metrics** | MAPE < 15% (1-hr), MAPE < 25% (24-hr) |

### 6.4 Solar Generation Forecasting

| Attribute | Detail |
|-----------|--------|
| **Objective** | Predict aggregate solar generation (utility + rooftop) per region |
| **Horizons** | 1-hour, 4-hour, next-day |
| **Features** | Solar irradiance forecast, cloud cover, historical solar generation, installed capacity, rooftop PV estimates |
| **Algorithms** | Gradient boosted trees + clear-sky model baseline |
| **Target Metrics** | MAPE < 10% (1-hr), MAPE < 20% (next-day) |

### 6.5 Anomaly / Event Detection

| Attribute | Detail |
|-----------|--------|
| **Objective** | Detect and classify unusual market events (price spikes, demand surges, generation trips, constraint binding) |
| **Method** | Statistical thresholds + isolation forest + rule-based classification |
| **Output** | Event records in `gold.market_events` with type, severity, affected region, probable cause |

---

## 7. AI Copilot Agent Specification

### 7.1 Architecture

```
User (Streamlit Chat UI)
        │
        ▼
┌─────────────────────────────────┐
│     Mosaic AI Agent             │
│     (Compound AI System)        │
│                                 │
│  ┌───────────┐ ┌─────────────┐  │
│  │ LLM       │ │ Tool Router │  │
│  │ (System   │ │             │  │
│  │  Prompt)  │ │ ┌─────────┐ │  │
│  │           │ │ │ UC Func │ │  │
│  │ Context:  │ │ │ Tools   │ │  │
│  │ - NEM     │ │ │         │ │  │
│  │   domain  │ │ └─────────┘ │  │
│  │ - Trading │ │             │  │
│  │   terms   │ │ ┌─────────┐ │  │
│  │ - AEMO    │ │ │ Vector  │ │  │
│  │   rules   │ │ │ Search  │ │  │
│  └───────────┘ │ │ (RAG)   │ │  │
│                │ └─────────┘ │  │
│                └─────────────┘  │
└─────────────────────────────────┘
        │
        ▼ Tool Calls
┌──────────────────────────────────────────────────────┐
│                Unity Catalog Functions                │
│                                                      │
│  get_latest_prices(region)                           │
│  get_price_forecast(region, horizon)                 │
│  get_demand_forecast(region, horizon)                │
│  get_generation_mix(region, start, end)              │
│  get_interconnector_flows(interconnector_id)          │
│  get_price_history(region, start, end, interval)     │
│  get_weather_forecast(region, hours_ahead)            │
│  explain_price_event(region, timestamp)               │
│  get_market_summary(date)                            │
│  search_market_rules(query)           [Vector Search] │
│  get_active_constraints(region)                       │
│  get_generator_info(duid)                            │
│  compare_regions(metric, start, end)                 │
└──────────────────────────────────────────────────────┘
```

### 7.2 Copilot Capabilities

| Capability | Description | Tools Used |
|------------|-------------|------------|
| **Market Q&A** | Answer factual questions about current and historical market state | `get_latest_prices`, `get_price_history`, `get_generation_mix`, `get_interconnector_flows` |
| **Forecasting** | Retrieve and explain price/demand/generation forecasts | `get_price_forecast`, `get_demand_forecast`, `get_weather_forecast` |
| **Event Explanation** | Explain why a price spike or unusual event occurred | `explain_price_event`, `get_active_constraints`, `get_generation_mix`, `get_weather_forecast` |
| **Market Summaries** | Generate natural language summaries of market conditions | `get_market_summary`, `get_latest_prices`, `get_generation_mix` |
| **Comparative Analysis** | Compare metrics across regions or time periods | `compare_regions`, `get_price_history` |
| **Rule/Regulation Lookup** | Answer questions about NEM rules, AEMO procedures | `search_market_rules` (RAG) |
| **Proactive Alerts** | Surface predicted events before they happen | `get_price_forecast` + anomaly thresholds |

### 7.3 Example Conversations

**Trader at 2pm:**
> **User**: "What's the SA spot price forecast for the next 4 hours?"
>
> **Copilot**: "The current SA spot price is $87/MWh. My forecast for the next 4 hours:
> - 2:30pm: $92/MWh (↑ rising demand, wind dropping)
> - 3:00pm: $105/MWh (↑ afternoon peak approaching)
> - 3:30pm: $118/MWh (↑ peak demand, low wind)
> - 4:00pm: $135/MWh (↑ evening ramp, solar declining)
> - 4:30pm: $142/MWh (↑ peak — solar offline, wind still low)
> - 5:00pm: $128/MWh (↓ demand easing)
>
> Key drivers: BOM forecasts wind speed dropping to 8 km/h across SA wind farms by 3pm. Temperature forecast of 38°C in Adelaide driving aircon load. Heywood interconnector currently at 85% capacity."

**Analyst at 9am:**
> **User**: "Give me a morning market brief"
>
> **Copilot**: "NEM Morning Brief — 19 Feb 2026:
>
> **Overnight**: Prices were subdued across all regions. NSW averaged $52/MWh, VIC $48/MWh, QLD $55/MWh, SA $61/MWh, TAS $44/MWh.
>
> **Key events**: Loy Yang A Unit 2 returned from outage at 04:00 AEST (500MW). SA wind generation averaged 1,200MW overnight (65% capacity factor).
>
> **Today's outlook**: Hot day forecast — Adelaide 39°C, Sydney 34°C. Expect afternoon price spikes in SA (forecast peak $180/MWh at 5pm) and NSW ($120/MWh). QNI interconnector maintenance reducing capacity by 200MW until 6pm.
>
> **Watch items**: SA evening peak risk — low wind forecast + high temp = potential $300+ price if Heywood congests."

---

## 8. Frontend Application Specification

### 8.1 Technology

- **Framework**: Streamlit (deployed via Databricks Apps)
- **Charting**: Plotly (interactive), Altair (declarative)
- **Maps**: Folium / Plotly mapbox for geographic overlays
- **Authentication**: Databricks OAuth (SSO)
- **State Management**: Streamlit session state + Lakebase for persistent preferences

### 8.2 Page Layout

```
┌─────────────────────────────────────────────────────────────┐
│  🔋 AUS Energy Copilot           [Region: All ▼]  [👤 User] │
├──────┬──────┬──────────┬──────────┬────────┬────────────────┤
│ Home │ Live │ Forecasts│ Copilot  │ Genie  │ Alerts         │
│      │Market│          │  Chat    │Analytics│               │
├──────┴──────┴──────────┴──────────┴────────┴────────────────┤
│                                                             │
│                    [Active Tab Content]                      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 8.3 Tab Specifications

#### Tab 1: Home (Executive Overview)
- 5-region price ticker bar (live, color-coded: green < $100, amber $100-$300, red > $300)
- NEM-wide demand vs available capacity gauge
- Today's AI market brief (auto-generated)
- Key alerts / predicted events
- 24-hour price sparklines per region

#### Tab 2: Live Market
- **Prices panel**: 5-minute regional price chart (multi-line, interactive zoom)
- **Generation panel**: Stacked area chart by fuel type per selected region
- **Interconnectors panel**: Directional flow diagram with congestion indicators
- **Demand panel**: Actual vs forecast demand overlay
- Filters: region selector, time range, fuel type

#### Tab 3: Forecasts
- Price forecast chart: actual vs predicted with confidence bands
- Demand forecast chart: actual vs predicted
- Wind/solar generation forecast vs actual
- Model accuracy metrics (MAE, MAPE) over rolling windows
- Forecast horizon selector (1hr, 4hr, 24hr)

#### Tab 4: Copilot Chat
- Full-screen chat interface
- Message history (session-persistent)
- Inline charts/tables rendered from agent tool responses
- Suggested questions sidebar
- "Morning Brief" quick-action button

#### Tab 5: Genie Analytics
- Embedded Genie space selector (Prices, Generation, Interconnectors)
- Natural language query input
- Results rendered as tables and charts
- Query history

#### Tab 6: Alerts
- Active alerts table (triggered, acknowledged, resolved)
- Alert configuration: metric, region, threshold, notification channel
- Alert history with linked market events
- Predicted alert section (AI-driven "likely to trigger in next 4 hours")

---

## 9. Genie Spaces Design

### Space 1: NEM Prices & Demand

**Tables/Views**: `gold.nem_prices_5min`, `gold.nem_prices_30min`, `gold.nem_demand_actuals`, `gold.price_forecasts`, `gold.demand_forecasts`

**Sample Questions**:
- "What was the average price in SA last week?"
- "Show me the top 10 highest price intervals in VIC this month"
- "Compare peak vs off-peak prices across all regions for January 2026"
- "What is the 95th percentile price in QLD for the past 90 days?"
- "Show daily average demand in NSW for the past 30 days"

**Glossary**: RRP (Regional Reference Price), Dispatch Interval, Trading Interval, Peak (7am-10pm weekdays), Off-Peak, Super-Peak (4pm-8pm), Baseload

### Space 2: Generation & Fuel Mix

**Tables/Views**: `gold.nem_generation_by_fuel`, `gold.nem_generation_by_unit`, `gold.solar_rooftop`, `gold.generation_forecasts`

**Sample Questions**:
- "What percentage of QLD generation came from renewables this week?"
- "Show me the top 10 generators by output in NSW today"
- "How much rooftop solar was generated in SA yesterday?"
- "Compare coal vs gas generation in VIC over the past year"
- "What is the average wind capacity factor in SA by month?"

**Glossary**: DUID (Dispatchable Unit Identifier), Capacity Factor, Nameplate Capacity, Rooftop PV, Semi-Scheduled, Scheduled

### Space 3: Interconnectors & Constraints

**Tables/Views**: `gold.nem_interconnectors`, `gold.nem_constraints_active`, `gold.market_events`

**Sample Questions**:
- "How often was Heywood interconnector congested in the past month?"
- "Show average flows on QNI by hour of day"
- "What constraints were binding in SA during the price spike on Feb 15?"
- "Compare interconnector utilization across all NEM interconnectors this quarter"

**Glossary**: Interconnector (VIC1-NSW1, N-Q-MNSP1, V-SA, T-V-MNSP1, V-S-MNSP1), Binding Constraint, RHS (Right Hand Side), Marginal Value

---

## 10. Non-Functional Requirements

### 10.1 Performance
- Dashboard page load: < 3 seconds
- Chart data refresh: < 5 seconds for 5-min data
- Copilot response time: < 15 seconds for simple queries, < 30 seconds for complex multi-tool queries
- Genie query response: < 20 seconds
- Forecast model inference: < 60 seconds per region per horizon

### 10.2 Reliability
- Data pipeline SLA: 99.5% uptime
- Dashboard availability: 99.5% (Databricks Apps SLA)
- Data freshness: alert if dispatch data > 10 minutes stale
- Model monitoring: alert if forecast MAE degrades > 20% from baseline

### 10.3 Security & Governance
- All data governed via Unity Catalog
- Row/column-level security where needed
- OAuth SSO via Databricks identity
- Audit logging for all copilot queries
- No PII in market data (public AEMO data)

### 10.4 Scalability
- Support 5+ years of historical data (~50M+ dispatch intervals)
- Support 20+ concurrent dashboard users
- Forecast models scale to all 5 regions x 3 horizons = 15 model variants

---

## 11. Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Data freshness | < 2 min from AEMO publish to dashboard | Pipeline monitoring |
| Price forecast accuracy (1-hr MAE) | < $15/MWh | MLflow model metrics |
| Demand forecast accuracy (1-hr MAPE) | < 3% | MLflow model metrics |
| Copilot query resolution rate | > 80% answered without human escalation | Agent evaluation |
| Daily active users | 10+ within first month of launch | App analytics |
| Genie query accuracy | > 85% correct SQL generation | Genie benchmarks |
| Dashboard load time | < 3 seconds | App performance monitoring |
| Time to insight (price spike explanation) | < 30 seconds (vs 30-60 min manual) | User feedback |

---

## 12. Dependencies & Risks

### Dependencies
| Dependency | Risk Level | Mitigation |
|------------|-----------|------------|
| AEMO NEMWEB data availability | Low | Public, stable for 10+ years; fallback to OpenElectricity API |
| OpenElectricity API uptime | Medium | Cache data in Delta Lake; not sole source for any critical data |
| Open-Meteo / BOM weather API | Low | Free, reliable; cache forecasts; fallback to persistence model |
| Databricks Apps GA availability | Low | GA since 2025; Streamlit is stable framework |
| Lakebase availability in ap-southeast-2 | Medium | Available; fallback to external Postgres if needed |
| Genie API (public preview) | Medium | Can launch without embedding; use Genie spaces directly |
| Mosaic AI Agent Framework | Low | GA; well-documented; MLflow-based |

### Risks
| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| AEMO changes NEMWEB file format | High | Low | Schema validation in Bronze layer; alerts on schema drift |
| Price forecast accuracy insufficient | Medium | Medium | Ensemble models; benchmark against AEMO pre-dispatch; iterate |
| Copilot hallucination on market data | High | Medium | Tool-calling architecture (grounded); evaluation suite; guardrails |
| Data volume exceeds cost expectations | Medium | Low | Incremental processing; partitioning; lifecycle policies |
| User adoption below target | Medium | Medium | Co-design with traders; iterative UX feedback; training |

---

---

# Phase 2: Lightweight ETRM — Trading & Risk

**Estimated Duration:** 12-16 weeks (following Phase 1 completion)
**Prerequisites:** Phase 1 fully operational (data pipelines, dashboard, copilot, Genie)

---

## 14. Phase 2 — Executive Summary

Phase 2 transforms AUS Energy Copilot from a market intelligence tool into an active **trading support platform** by adding deal capture, portfolio tracking, forward curve management, risk analytics, PPA valuation, and FCAS market support. This directly addresses the gentailer workflow: traders can now ask the copilot "what's my exposure?" — not just "what's the market doing?"

The phase targets the core gap identified in Phase 1 for gentailer customers like Alinta Energy and Energy Australia: connecting market intelligence to **portfolio positions**.

---

## 15. Phase 2 — Product Scope

### 15.1 Deal Capture & Portfolio Management

#### Deal Capture UI
- **Trade entry forms** in Streamlit for common NEM contract types:
  - Spot exposure (generation/load positions by region)
  - OTC forwards and swaps (flat, peak, off-peak, super-peak profiles)
  - Futures (ASX Energy futures — base, peak, cap contracts)
  - Options (caps, floors, collars on NEM regional prices)
  - Power Purchase Agreements (fixed price, CPI-escalated, floor/cap, pay-as-produced, baseload-shaped)
  - Renewable Energy Certificates (LGCs, STCs)
- **AI-assisted deal entry**: Copilot can create trades from natural language ("Enter a 50MW peak swap for VIC Q3 2026 at $85/MWh")
- **Bulk import** via CSV upload for existing portfolio migration
- **Trade amendment and cancellation** with full audit trail

#### Portfolio Views
- **Position summary**: Net long/short by region, time bucket (month, quarter, year), and contract type
- **Portfolio P&L**: Realized + unrealized, broken down by trade, region, and contract type
- **Exposure heatmap**: Visual grid of MW exposure by region x time period, color-coded by direction and size
- **Trade blotter**: Filterable/sortable list of all trades with status, counterparty, volume, price, and current MtM value

#### Data Model (Lakebase)

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `trades` | trade_id, trade_type, region, buy_sell, volume_mw, price, start_date, end_date, profile, status, counterparty, created_by, created_at | Master trade record |
| `trade_legs` | leg_id, trade_id, settlement_date, interval_start, interval_end, volume_mw, price, profile_factor | Time-bucketed trade cashflows |
| `trade_amendments` | amendment_id, trade_id, field_changed, old_value, new_value, amended_by, amended_at | Audit trail |
| `counterparties` | counterparty_id, name, credit_rating, credit_limit | Counterparty master |
| `portfolios` | portfolio_id, name, owner, description | Portfolio groupings |
| `portfolio_trades` | portfolio_id, trade_id | Many-to-many mapping |

### 15.2 Forward Curve Construction & Mark-to-Market

#### Forward Curve Engine
- **Base curve sources**: ASX Energy futures settlement prices (daily), broker quotes (OTC forwards)
- **Curve building methodology**:
  - Bootstrap from liquid futures maturities (monthly, quarterly)
  - Shape to hourly granularity using historical load-shape profiles (peak/off-peak/super-peak ratios)
  - Apply seasonal shaping factors derived from 3-year historical price patterns
  - Solar and wind cannibalization adjustments for renewable capture price curves
- **Curve storage**: Versioned in Delta Lake (`gold.forward_curves`) with daily snapshots for historical MtM
- **Curve visualization**: Interactive term structure chart in dashboard

#### Mark-to-Market (MtM) Valuation
- **Daily MtM batch job**: Values entire portfolio against latest forward curves
- **MtM methodology**:
  - Forwards/swaps: Sum of (forward price − contract price) × volume × discount factor per settlement period
  - Options: Black-76 model for European-style caps/floors; intrinsic + time value decomposition
  - PPAs: Hourly cashflow projection using shaped forward curves × forecast generation profile, discounted to NPV
  - RECs: Current spot/forward LGC price × certificate volume
- **P&L attribution**: Decompose daily P&L change into price move, volume change, curve roll, new trades, and time decay
- **Results stored** in `gold.portfolio_mtm` and `gold.pnl_attribution`

### 15.3 Risk Analytics

#### Portfolio Risk Metrics
- **Net position by region and time bucket**: Real-time calculation from trade legs vs generation/load forecasts
- **Portfolio Greeks**:
  - Delta: MW sensitivity to $1/MWh price move per region
  - Gamma: Second-order price sensitivity (relevant for option positions)
  - Vega: Sensitivity to implied volatility changes
  - Theta: Time decay on option positions
- **Value at Risk (VaR)**: Parametric VaR (95%, 99% confidence) using historical volatility by region
- **Stress testing**: Pre-defined scenarios:
  - "SA heatwave" (SA price +300%, demand +20%)
  - "Wind drought" (wind gen -80% across NEM for 48 hours)
  - "Coal trip" (2GW unplanned outage in NSW)
  - "Interconnector failure" (Heywood or QNI capacity = 0)
  - Custom user-defined scenarios

#### Credit Risk
- Counterparty exposure tracking against credit limits
- Alerts when exposure exceeds 80% of credit limit
- Counterparty exposure aging (current, 30-day, 90-day)

### 15.4 PPA Valuation Copilot

#### AI-Powered PPA Analysis
- **Copilot PPA tool**: `value_ppa(strike_price, term_years, technology, region, profile, escalation, volume_mw)`
- **Valuation approach**:
  - Project hourly generation profile using technology-specific shape (solar, wind) and regional capacity factors
  - Apply shaped forward curves to calculate merchant revenue baseline
  - Calculate PPA cashflows: (strike price − merchant price) × forecast generation per hour
  - Discount to NPV using configurable WACC
  - Run Monte Carlo simulation (1,000 paths) varying price, generation, and correlation to produce NPV distribution
- **Key outputs**: Expected NPV, P10/P50/P90 NPV range, breakeven strike price, capture price discount, annual cashflow profile
- **Copilot conversation example**:
  > "Value a 10-year solar PPA in SA at $55/MWh with 2.5% CPI escalation, 100MW nameplate"
  >
  > Copilot returns NPV range, capture price analysis, sensitivity table, and recommendation

### 15.5 FCAS Market Analytics

#### FCAS Data & Dashboards
- Ingest all 8 FCAS markets: Raise 6s, Raise 60s, Raise 5min, Raise Reg, Lower 6s, Lower 60s, Lower 5min, Lower Reg
- **FCAS price dashboard**: Live and historical FCAS prices by service and region
- **FCAS bidstack analysis**: View FCAS bids by generator/battery
- **Co-optimization view**: Show energy + FCAS co-optimization outcomes per dispatch interval
- **Battery FCAS revenue tracker**: Track FCAS revenue by battery asset (relevant for Alinta, EA battery portfolios)

#### FCAS Forecasting
- FCAS price forecast models (LightGBM) for each service × region
- Features: system frequency, inertia levels, renewable penetration, demand, time-of-day
- Copilot tool: `get_fcas_forecast(service, region, horizon)`

### 15.6 Settlement Reconciliation

#### AEMO Settlement Matching
- Ingest AEMO preliminary and final settlement files
- Match AEMO settlements against internal trade positions
- **Reconciliation dashboard**: Matched, unmatched, and disputed items
- **Variance analysis**: Highlight settlement differences > $1,000
- **Copilot tool**: `get_settlement_variance(region, settlement_date)` — explains material variances

### 15.7 Phase 2 — Copilot Agent Extensions

New tools added to the Mosaic AI agent:

| Tool | Input | Output | Description |
|------|-------|--------|-------------|
| `get_portfolio_position(portfolio, region, period)` | Portfolio name, region, time bucket | Net MW position, direction | Query current portfolio exposure |
| `get_portfolio_pnl(portfolio, date_range)` | Portfolio, date range | Realized + unrealized P&L | Portfolio P&L summary |
| `get_portfolio_risk(portfolio, metric)` | Portfolio, VaR/Greeks/stress | Risk metric values | Risk analytics on demand |
| `value_ppa(params)` | Strike, term, tech, region, volume | NPV range, capture price | AI-powered PPA valuation |
| `create_trade(description)` | Natural language trade description | Trade confirmation | AI-assisted deal entry |
| `get_forward_curve(region, tenor)` | Region, time range | Forward price curve | Retrieve latest curves |
| `get_fcas_forecast(service, region, horizon)` | FCAS service, region, horizon | Price forecast | FCAS market predictions |
| `get_settlement_variance(region, date)` | Region, settlement date | Variance items | Settlement reconciliation |
| `explain_pnl_move(portfolio, date)` | Portfolio, date | P&L attribution breakdown | Explain what drove P&L changes |
| `run_stress_test(portfolio, scenario)` | Portfolio, scenario name | Stressed P&L impact | Portfolio stress testing |

### 15.8 Phase 2 — Frontend Extensions

#### New Tabs
- **Portfolio tab**: Position summary, exposure heatmap, trade blotter, P&L chart
- **Deal Entry tab**: Trade capture forms, AI-assisted entry, bulk import
- **Risk tab**: VaR dashboard, Greeks summary, stress test results, credit exposure
- **Curves tab**: Forward curve term structure, historical curve comparison, curve building inputs
- **FCAS tab**: FCAS prices, bidstacks, co-optimization, battery revenue tracker

#### Enhanced Existing Tabs
- **Home tab**: Add portfolio P&L ticker, top exposure alerts, settlement status
- **Copilot Chat**: All new tools available; suggested questions updated with portfolio/risk queries
- **Alerts tab**: Add portfolio-level alerts (position limit breach, credit limit, MtM threshold, settlement variance)

### 15.9 Phase 2 — Data Architecture Extensions

#### New Gold Layer Tables

| Table | Description |
|-------|-------------|
| `gold.forward_curves` | Daily forward curve snapshots by region (hourly granularity, versioned) |
| `gold.forward_curves_shaped` | Hourly shaped forward prices (peak/off-peak/solar/wind profiles) |
| `gold.portfolio_mtm` | Daily mark-to-market by trade, portfolio, region |
| `gold.pnl_attribution` | Daily P&L decomposition (price, volume, curve roll, new trades, time decay) |
| `gold.portfolio_positions` | Aggregated net positions by portfolio, region, time bucket |
| `gold.risk_metrics` | Daily VaR, Greeks per portfolio |
| `gold.stress_test_results` | Scenario stress test outputs |
| `gold.fcas_prices` | 5-minute FCAS prices by service and region |
| `gold.fcas_bidstacks` | FCAS bidstack data by generator |
| `gold.settlement_reconciliation` | AEMO settlement vs internal position matching |
| `gold.credit_exposure` | Counterparty credit exposure tracking |

#### New Lakebase Tables

| Table | Purpose |
|-------|---------|
| `trades` | Master trade record store |
| `trade_legs` | Time-bucketed cashflows per trade |
| `trade_amendments` | Trade change audit trail |
| `counterparties` | Counterparty master data |
| `portfolios` | Portfolio definitions |
| `curve_configs` | Forward curve building parameters |
| `risk_limits` | Position and risk limit definitions |
| `settlement_disputes` | Flagged settlement variances |

### 15.10 Phase 2 — New Genie Spaces

#### Space 4: Portfolio & P&L
**Tables/Views**: `gold.portfolio_mtm`, `gold.pnl_attribution`, `gold.portfolio_positions`

**Sample Questions**:
- "What is the total unrealized P&L for the base portfolio this month?"
- "Show me the top 10 trades by MtM value"
- "What was the daily P&L attribution for VIC positions last week?"
- "Which portfolio has the largest net short position in SA for Q2?"

#### Space 5: FCAS Markets
**Tables/Views**: `gold.fcas_prices`, `gold.fcas_bidstacks`

**Sample Questions**:
- "What was the average Raise 6s price in SA this week?"
- "Show me FCAS revenue by battery asset for the past month"
- "Compare Reg Raise prices across all regions for January"
- "Which generators bid the most FCAS volume in QLD today?"

### 15.11 Phase 2 — Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Trade entry time (AI-assisted) | < 30 seconds per trade | App analytics |
| Daily MtM batch completion | < 15 minutes for full portfolio | Workflow monitoring |
| PPA valuation response (copilot) | < 45 seconds including Monte Carlo | Agent evaluation |
| Forward curve build time | < 5 minutes (daily) | Pipeline monitoring |
| Settlement reconciliation match rate | > 98% auto-matched | Reconciliation dashboard |
| FCAS forecast accuracy (1-hr MAPE) | < 20% | MLflow model metrics |
| Portfolio-related copilot queries resolved | > 75% without escalation | Agent evaluation |
| Risk metric calculation freshness | < 30 minutes from market close | Workflow monitoring |

### 15.12 Phase 2 — Estimated Timeline

| Sprint | Duration | Focus |
|--------|----------|-------|
| Sprint 5 | 2 weeks | Lakebase trade data model, deal capture UI, basic portfolio views |
| Sprint 6 | 2 weeks | Forward curve engine, MtM valuation batch, curve dashboard |
| Sprint 7 | 2 weeks | Risk analytics (VaR, Greeks, stress tests), risk dashboard |
| Sprint 8 | 2 weeks | PPA valuation engine, PPA copilot tool, Monte Carlo simulation |
| Sprint 9 | 2 weeks | FCAS data ingestion, FCAS dashboards, FCAS forecast models |
| Sprint 10 | 2 weeks | Settlement reconciliation, new copilot tools, new Genie spaces |
| Sprint 11 | 2 weeks | Integration testing, copilot evaluation, UX polish, launch |

### 15.13 Phase 2 — Implementation Status & Enhancement Backlog

**Completed (PRD 15.1 — Deal Capture & Portfolio Management):**

| Feature | Status | Notes |
|---------|--------|-------|
| Trade entry form (7 NEM contract types) | Done | DealCapture.tsx — SPOT, FORWARD, SWAP, FUTURE, OPTION, PPA, REC |
| AI-assisted deal entry | Done | Copilot `create_trade` FMAPI tool |
| Bulk CSV import | Done | POST `/api/deals/trades/bulk-import` |
| Trade amendment with audit trail | Done | PUT endpoint + `trade_amendments` table |
| Trade cancellation (soft delete) | Done | DELETE endpoint sets status=CANCELLED |
| Position summary (region × quarter) | Done | Portfolio.tsx — net/gross MW, trade count |
| Exposure heatmap (region × month) | Done | Color-coded green/red intensity grid |
| P&L chart (realized + unrealized) | Done | Recharts bar chart by region |
| Trade blotter (filterable, expandable) | Done | TradeBlotter.tsx — legs + amendment history |
| Counterparty management | Done | CRUD endpoints + dropdown in deal form |
| Portfolio management | Done | 3 portfolios, trade assignment |
| 6 Delta tables with CDF | Done | trades, trade_legs, trade_amendments, counterparties, portfolios, portfolio_trades |
| Seed data (50 trades, 5 counterparties) | Done | setup/13_seed_deal_data.py |
| Batch leg generation | Done | `_insert_gold_batch()` — chunks of 50 rows per INSERT |
| Forward curve engine (E1) | Done | `curves.py` — ASX bootstrap + seasonal shaping + peak/off-peak |
| Forward curve API (5 endpoints) | Done | term-structure, compare, history, snapshot, snapshots |
| Forward curve dashboard | Done | `ForwardCurves.tsx` — 3 charts + data table |
| Forward curve Copilot tool | Done | `get_forward_curve` — 10th FMAPI tool |
| `gold.forward_curves` Delta table | Done | 360 rows persisted (5 regions × 3 profiles × 24 months) |

**Enhancement Backlog (Phase 2 remaining scope — PRD 15.2–15.6):**

| ID | Feature | PRD Section | Priority | Complexity | Description |
|----|---------|-------------|----------|------------|-------------|
| E1 | **Forward Curve Construction** | 15.2 | High | Large | **DONE** (2026-03-07). Curve engine bootstraps from ASX futures (`asx_futures_eod`), quarterly→monthly decomposition with NEM seasonal shape factors, peak/off-peak shaping per region. 5 API endpoints (`/api/curves/*`), ForwardCurves.tsx page, `get_forward_curve` Copilot tool, `gold.forward_curves` Delta table. Tested: NSW1 FLAT $67-$99/MWh across 24 months from real ASX data. |
| E2 | **Mark-to-Market Valuation** | 15.2 | High | Large | Daily MtM batch job: value portfolio against forward curves. Forwards/swaps (DCF), Options (Black-76), PPAs (hourly cashflow × shaped curves), RECs (spot × volume). P&L attribution (price, volume, curve roll, time decay). |
| E3 | **VaR & Portfolio Greeks** | 15.3 | High | Medium | Parametric VaR (95/99%), Delta/Gamma/Vega/Theta per region. Historical volatility from `nem_prices_5min`. |
| E4 | **Stress Testing** | 15.3 | Medium | Medium | Pre-defined scenarios: SA heatwave (+300% price), wind drought (-80% wind), coal trip (2GW NSW), interconnector failure. Custom user-defined scenarios. |
| E5 | **Credit Risk** | 15.3 | Medium | Small | Counterparty exposure vs credit limits, alerts at 80%, exposure aging (current/30d/90d). |
| E6 | **PPA Valuation Copilot** | 15.4 | Medium | Large | `value_ppa()` tool: hourly generation projection, shaped forward curves, NPV with Monte Carlo (1000 paths), P10/P50/P90, breakeven strike, capture price discount. |
| E7 | **FCAS Market Analytics** | 15.5 | Low | Large | Ingest 8 FCAS markets, price dashboard, bidstack analysis, co-optimization view, battery FCAS revenue tracker, FCAS price forecast models. |
| E8 | **Settlement Reconciliation** | 15.6 | Low | Medium | Match AEMO preliminary/final settlement files against internal positions. Variance analysis (>$1K threshold). Copilot tool for explaining variances. |
| E9 | **Synced Tables for Deal Data** | 15.1 | Medium | Small | Run `pipelines/12_recreate_synced_tables_continuous.py` to create 4 new synced tables (trades, trade_legs, counterparties, portfolios) for Lakebase reads. |
| E10 | **Additional Copilot Tools** | 15.7 | Medium | Medium | ~~`get_forward_curve`~~ (done in E1), `get_portfolio_pnl`, `get_portfolio_risk`, `explain_pnl_move`, `run_stress_test`, `value_ppa`, `get_fcas_forecast`, `get_settlement_variance` |
| E11 | **New Genie Spaces** | 15.10 | Low | Small | Trading & Portfolio space, Risk Analytics space (from PRD 15.10) |

---

# Phase 3: Bidding, Advanced Risk & Market Expansion

**Estimated Duration:** 16-24 weeks (following Phase 2 completion)
**Prerequisites:** Phase 2 fully operational (portfolio, curves, risk, FCAS)

---

## 16. Phase 3 — Executive Summary

Phase 3 completes the platform as a full-featured **energy trading and operations system** — adding AI-optimized bid/offer management (EnergyOffer-like), advanced risk analytics, battery dispatch optimization, gas market coverage, WEM expansion, and multi-tenant deployment. This phase positions AUS Energy Copilot as a comprehensive alternative to Energy One's product suite for the Databricks ecosystem.

---

## 17. Phase 3 — Product Scope

### 17.1 AI-Optimized Bidding & Dispatch (EnergyOffer Alternative)

#### Bid Preparation
- **Bid/offer editor** for NEM energy market:
  - 10 price-quantity band pairs per generator (NEM format)
  - Support for daily, rebid, and default bids
  - FCAS bid/offer preparation (8 services)
  - Bid validation against AEMO rules (price band ordering, ramp rate limits, minimum generation)
- **AI bid optimization copilot**:
  - Agent analyzes: price forecasts, demand forecasts, weather, competitor bidstack patterns, constraint forecasts, portfolio position, fuel costs, and emission obligations
  - Recommends optimal bid curves per generator to maximize portfolio revenue
  - Tool: `optimize_bid(duid, date, objective)` — returns recommended 10-band bid curve with reasoning
  - Supports multiple objectives: maximize revenue, minimize risk, target dispatch volume
- **Rebid intelligence**:
  - Monitor market conditions in real-time
  - Alert when rebid opportunity detected (material change in forecast, constraint, or competitor behavior)
  - Auto-generate rebid reason text compliant with AEMO rebid guidelines
  - Tool: `suggest_rebid(duid, reason_category)` — returns new bid bands + compliant reason string

#### AEMO Market Submission
- **Direct API integration** with AEMO NEM Dispatch Bidding API (`dev.aemo.com.au`)
- Bid submission workflow: prepare → review → approve → submit → confirm
- Mandatory dual-approval for bids exceeding configurable thresholds
- Submission audit trail with timestamps, approver, and AEMO acknowledgment
- Bid compliance tracking: flag late submissions, missing rebid reasons, price band violations

#### Dispatch Monitoring
- **Unit conformance dashboard**: Actual output vs dispatch target per generator
- **Conformance alerts**: Flag non-conformance events (deviation > 5% from target for > 2 intervals)
- **Dispatch outcome analysis**: Post-interval review of dispatch results vs bid expectations
- **Revenue attribution**: Actual revenue vs optimal strategy comparison per unit per day

### 17.2 Advanced Risk Analytics

#### Enhanced VaR Models
- **Historical simulation VaR**: Full revaluation using 2+ years of historical scenarios (1,000+ paths)
- **Monte Carlo VaR**: Correlated simulation across regions, fuel types, and weather factors
- **Conditional VaR (CVaR / Expected Shortfall)**: Tail risk quantification at 95% and 99%
- **Incremental VaR**: Marginal risk contribution of each trade to total portfolio VaR
- **Component VaR**: Risk decomposition by region, contract type, and time bucket

#### Option Analytics
- **Full Greeks suite**: Delta, Gamma, Vega, Theta, Rho per option position and portfolio aggregate
- **Implied volatility surface**: Construct and display vol surface by strike × tenor for each NEM region
- **Scenario Greeks**: Greeks under stressed market conditions
- **Exotic option support**: Asian options (average price), barrier options, swing contracts

#### Advanced Stress Testing
- **Scenario library** with 20+ pre-built scenarios covering:
  - Extreme weather (heatwave, cold snap, storm, wind drought, cloud cover)
  - Infrastructure (interconnector outage, pipeline failure, coal plant trip, transmission constraint)
  - Regulatory (price cap change, carbon price shock, renewable target acceleration)
  - Market structure (demand response event, large generator exit, battery saturation)
- **Reverse stress testing**: "What scenario would cause a $X loss?" — AI agent identifies market conditions
- **Historical event replay**: Replay portfolio through actual historical events (SA blackout 2016, QLD flood 2022, etc.)
- **Copilot tool**: `reverse_stress_test(portfolio, loss_threshold)` — identifies breaking scenarios

#### Real-Time Risk Monitoring
- **Intraday risk dashboard**: VaR and exposure updated every 5 minutes with dispatch data
- **Limit monitoring**: Real-time position vs limit tracking with automatic breach alerts
- **Risk escalation workflow**: Breach → alert → acknowledge → action → resolve, stored in Lakebase

### 17.3 Battery Dispatch Optimization

#### Battery Operations Module
- **Battery asset registry**: Capacity (MW), energy (MWh), round-trip efficiency, degradation model, location, grid connection
- **Optimal dispatch scheduler**:
  - Mixed-integer linear program (MILP) or reinforcement learning agent
  - Objective: maximize revenue from energy arbitrage + FCAS co-optimization
  - Constraints: state of charge, max cycles/day, degradation budget, grid constraints
  - Horizon: rolling 24-48 hour optimization, re-solved every 5 minutes
- **Battery dispatch copilot**:
  - Tool: `optimize_battery(asset_id, horizon, objective)` — returns charge/discharge schedule
  - "When should I charge/discharge Hornsdale today for maximum revenue?"
  - "What's the expected FCAS vs energy revenue split for the battery this week?"
- **Battery performance tracking**: Actual vs optimal revenue, degradation monitoring, cycle count, state of health

### 17.4 Gas Market Expansion

#### STTM (Short Term Trading Market) — Adelaide, Sydney, Brisbane
- Ingest AEMO STTM data: ex-ante and ex-post prices, pipeline flows, withdrawals
- Gas price dashboard by hub
- Gas price forecasting models
- Gas-electric spread analysis (spark spread tracking for gas generators)

#### DWGM (Declared Wholesale Gas Market) — Victoria
- Ingest AEMO DWGM data: market prices, schedules, pipeline nominations
- Victorian gas market dashboard
- Gas demand forecasting

#### Gas Trading Integration
- Deal capture for gas contracts (spot, forwards, transport)
- Gas portfolio P&L and risk alongside electricity
- Combined gas + power portfolio view
- Copilot tools: `get_gas_price(hub)`, `get_gas_forecast(hub, horizon)`, `get_spark_spread(region)`

### 17.5 WEM (Western Australian) Market Expansion

#### WEM Data Ingestion
- Ingest AEMO WEM data: balancing prices, LFAS prices, facility generation, demand
- WEM-specific pipeline (separate from NEM, different market rules)
- Reference price: Balancing Price (not RRP)
- Facilities data: WEM generator registry

#### WEM Dashboard & Analytics
- WEM balancing price chart (30-minute intervals)
- WEM generation mix and demand
- WEM price forecasting models
- Genie space for WEM data

#### WEM Trading Support
- WEM-specific deal capture (different contract structures)
- WEM portfolio tracking and P&L
- Combined NEM + WEM portfolio view for national gentailers

### 17.6 Advanced Copilot Capabilities

#### New Phase 3 Agent Tools

| Tool | Input | Output | Description |
|------|-------|--------|-------------|
| `optimize_bid(duid, date, objective)` | Generator DUID, target date, objective | 10-band bid curve + reasoning | AI-recommended bid strategy |
| `suggest_rebid(duid, reason)` | Generator DUID, reason category | New bid bands + compliant reason text | Rebid opportunity detection |
| `optimize_battery(asset, horizon, objective)` | Battery asset ID, hours ahead, max revenue/min risk | Charge/discharge schedule | Battery dispatch optimization |
| `reverse_stress_test(portfolio, threshold)` | Portfolio, loss amount | Scenario description | Find scenario that causes target loss |
| `replay_event(portfolio, event_name)` | Portfolio, historical event | P&L impact, position analysis | Replay portfolio through historical crisis |
| `get_gas_price(hub)` | Gas hub name | Current/historical gas price | Gas market data |
| `get_spark_spread(region)` | NEM region | Gas price vs electricity price spread | Gas-electric economics |
| `get_bid_compliance(duid, date_range)` | Generator, period | Compliance report | Bid compliance check |
| `compare_bid_vs_optimal(duid, date)` | Generator, date | Revenue comparison | Post-dispatch bid quality analysis |
| `get_wem_price(date_range)` | Date range | WEM balancing prices | WA market data |

#### Enhanced Agent Capabilities
- **Multi-step reasoning**: Agent chains multiple tools for complex analysis (e.g., "Optimize tomorrow's bids for all SA generators considering the weather forecast and my current portfolio exposure")
- **Proactive operations alerts**: Agent monitors battery state-of-charge, bid submission deadlines, conformance events, and settlement deadlines
- **Report generation**: Agent produces formatted PDF/HTML reports for management (weekly risk report, monthly portfolio review, quarterly performance attribution)
- **What-if analysis**: "What if I add a 200MW wind PPA in VIC at $60/MWh — how does that change my portfolio risk?"

### 17.7 Multi-Tenancy & Enterprise Features

#### Multi-Tenant Architecture
- **Workspace isolation**: Separate Unity Catalog schemas per tenant (portfolio data, trades, risk limits)
- **Shared data layer**: Market data (Bronze/Silver/Gold) shared across tenants (read-only)
- **Tenant admin console**: User management, role assignment, feature flags
- **Data segregation**: Trades, positions, and risk data strictly isolated per tenant

#### Enterprise Security
- **Role-based access control**:
  - Trader: deal entry, bid preparation, market views
  - Risk Manager: risk dashboards, limit management, portfolio override
  - Analyst: full read access, model results, historical analysis
  - Admin: user management, system configuration
  - Executive: summary dashboards, report access
- **Four-eyes principle** for bid submission and large trade entry
- **Audit trail**: Every action (trade, bid, limit change, approval) logged with user, timestamp, and context
- **Data retention policies**: Configurable per regulation (NER compliance)

#### White-Labeling
- Configurable app branding (logo, colors, company name)
- Custom Genie space naming and glossary per client
- Deployable as a managed service or self-hosted on client's Databricks workspace

### 17.8 Phase 3 — Frontend Extensions

#### New Tabs
- **Bidding tab**: Bid editor, AI optimization panel, submission workflow, compliance tracker
- **Battery tab**: Dispatch schedule, charge/discharge plan, revenue tracker, state of health
- **Gas tab**: Gas prices, gas-electric spreads, gas portfolio (if gas market enabled)
- **WEM tab**: WA market dashboard (if WEM enabled)

#### Enhanced Existing Tabs
- **Risk tab**: Full VaR dashboard (historical, Monte Carlo, CVaR), vol surface, scenario library, reverse stress test
- **Portfolio tab**: Combined NEM + WEM + Gas view, incremental VaR per trade
- **Home tab**: Bidding deadlines, conformance alerts, battery optimization status
- **Copilot Chat**: All Phase 3 tools available, report generation capability

### 17.9 Phase 3 — Data Architecture Extensions

#### New Gold Layer Tables

| Table | Description |
|-------|-------------|
| `gold.bids_submitted` | All bids/offers submitted to AEMO with status |
| `gold.bid_optimization_results` | AI-recommended bid curves and reasoning |
| `gold.dispatch_conformance` | Unit conformance vs dispatch targets |
| `gold.revenue_attribution` | Actual vs optimal revenue per generator |
| `gold.var_historical` | Daily VaR time series by portfolio |
| `gold.var_monte_carlo` | Monte Carlo VaR results with scenario paths |
| `gold.vol_surface` | Implied volatility surface by region × strike × tenor |
| `gold.stress_test_library` | Pre-defined and custom stress scenarios |
| `gold.battery_dispatch_schedule` | Optimized charge/discharge plans |
| `gold.battery_performance` | Actual vs planned revenue, degradation, cycle tracking |
| `gold.gas_sttm_prices` | STTM hub prices (Adelaide, Sydney, Brisbane) |
| `gold.gas_dwgm_prices` | DWGM Victorian gas prices |
| `gold.gas_spark_spreads` | Spark spread calculations by region |
| `gold.wem_balancing_prices` | WEM balancing market prices |
| `gold.wem_generation` | WEM facility generation data |
| `gold.wem_demand` | WEM demand data |

#### New Lakebase Tables

| Table | Purpose |
|-------|---------|
| `bids` | Bid/offer records per generator per interval |
| `bid_approvals` | Bid approval workflow state |
| `bid_submissions` | AEMO submission tracking and acknowledgments |
| `battery_assets` | Battery asset registry and parameters |
| `battery_schedules` | Active and historical dispatch schedules |
| `gas_trades` | Gas contract records |
| `tenants` | Tenant configuration (if multi-tenant) |
| `tenant_users` | User-tenant mappings and roles |
| `audit_log` | Comprehensive action audit trail |

### 17.10 Phase 3 — New Genie Spaces

#### Space 6: Bidding & Dispatch
**Tables/Views**: `gold.bids_submitted`, `gold.bid_optimization_results`, `gold.dispatch_conformance`, `gold.revenue_attribution`

**Sample Questions**:
- "Show me all rebids for Torrens Island in the past week with reasons"
- "What was the conformance deviation for Bayswater today?"
- "Compare actual vs AI-optimal revenue for all NSW generators this month"
- "How many late bid submissions did we have this quarter?"

#### Space 7: Gas Markets
**Tables/Views**: `gold.gas_sttm_prices`, `gold.gas_dwgm_prices`, `gold.gas_spark_spreads`

**Sample Questions**:
- "What was the average STTM Adelaide gas price this week?"
- "Show me spark spreads for Pelican Point over the past 3 months"
- "Compare gas prices across all STTM hubs for January"

#### Space 8: WEM (Western Australia)
**Tables/Views**: `gold.wem_balancing_prices`, `gold.wem_generation`, `gold.wem_demand`

**Sample Questions**:
- "What was the average WEM balancing price this month?"
- "Show me WEM generation by fuel type for the past week"
- "Compare WEM demand patterns weekday vs weekend"

### 17.11 Phase 3 — Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| AI bid optimization revenue uplift | > 3% vs manual bidding (backtested) | Revenue attribution analysis |
| Bid submission compliance rate | 100% on-time submissions | AEMO acknowledgment tracking |
| Battery optimization revenue uplift | > 5% vs simple rule-based dispatch | Actual vs baseline revenue comparison |
| Monte Carlo VaR computation time | < 10 minutes for full portfolio (1,000 paths) | Workflow monitoring |
| Conformance event detection latency | < 10 minutes from dispatch interval | Streaming pipeline monitoring |
| Gas price forecast accuracy (1-day MAPE) | < 8% | MLflow model metrics |
| WEM price forecast accuracy (1-hr MAE) | < $10/MWh | MLflow model metrics |
| Multi-step copilot query resolution | > 70% | Agent evaluation |
| Report generation time (weekly risk) | < 2 minutes | Agent evaluation |

### 17.12 Phase 3 — Estimated Timeline

| Sprint | Duration | Focus |
|--------|----------|-------|
| Sprint 12 | 2 weeks | Bid editor UI, bid validation, AEMO API integration scaffold |
| Sprint 13 | 2 weeks | AI bid optimization engine, rebid intelligence, bid submission workflow |
| Sprint 14 | 2 weeks | Advanced VaR (historical sim, Monte Carlo), CVaR, component VaR |
| Sprint 15 | 2 weeks | Option analytics (Greeks, vol surface), advanced stress testing, reverse stress tests |
| Sprint 16 | 2 weeks | Battery optimization engine (MILP/RL), battery dashboard, performance tracking |
| Sprint 17 | 2 weeks | Gas market data ingestion (STTM, DWGM), gas dashboards, spark spreads |
| Sprint 18 | 2 weeks | WEM data ingestion, WEM dashboard, WEM forecasting models |
| Sprint 19 | 2 weeks | Multi-tenancy architecture, RBAC, audit trail, enterprise security |
| Sprint 20 | 2 weeks | New copilot tools, agent evaluation, report generation capability |
| Sprint 21 | 2 weeks | Genie spaces (3 new), white-labeling, integration testing |
| Sprint 22 | 2 weeks | End-to-end testing, performance tuning, documentation, production launch |

---

# Phase 4: Network Operations & Distribution Intelligence

**Estimated Duration:** 14-20 weeks (following Phase 3 completion)
**Prerequisites:** Phase 1 operational (market data, forecasting, copilot)

---

## 18. Phase 4 — Executive Summary

Phase 4 extends AUS Energy Copilot beyond the wholesale market to serve **Distribution Network Service Providers (DNSPs)** — the poles-and-wires businesses that deliver electricity to homes and businesses. This phase adds network asset monitoring, DER (Distributed Energy Resource) visibility, outage analytics, regulatory compliance dashboards, and network planning tools. It positions the platform for a new customer segment: DNSPs like Ausgrid, Endeavour Energy, SA Power Networks, Ergon Energy, CitiPower/Powercor, and AusNet Services.

DNSPs face mounting pressure from rooftop solar proliferation, battery adoption, EV charging load growth, and increasing AEMO/AER regulatory requirements. Their existing SCADA/ADMS systems generate vast volumes of data but lack the AI-driven analytics layer to convert network telemetry into actionable intelligence. AUS Energy Copilot fills this gap by bringing lakehouse analytics, AI forecasting, and a conversational copilot to the distribution network.

---

## 19. Phase 4 — Target Users & Personas

### P5: Network Operations Engineer (Primary)
- **Goal**: Maintain network reliability and power quality within regulatory limits
- **Needs**: Real-time asset loading, voltage monitoring, fault detection, outage management, DER curtailment visibility
- **AI Copilot Use**: "Which zone substations are above 80% utilization right now?", "Show me all voltage excursions in the Blacktown area today", "Why did feeder FDR-2145 trip at 3pm?"

### P6: Network Planning Engineer (Primary)
- **Goal**: Plan network augmentation and demand management to defer capex
- **Needs**: Load growth forecasts, hosting capacity analysis, DER penetration scenarios, constraint identification, augmentation cost-benefit
- **AI Copilot Use**: "What's the 10-year peak demand forecast for the Canterbury zone substation?", "Which feeders will exceed hosting capacity by 2028 under the high-solar scenario?", "What's the cost of augmenting vs a demand management solution for the Penrith constraint?"

### P7: Regulatory & Performance Manager (Secondary)
- **Goal**: Meet AER determination targets and report on network performance
- **Needs**: SAIDI/SAIFI tracking, guaranteed service level (GSL) monitoring, capex/opex efficiency, customer minutes off supply, regulatory benchmarking
- **AI Copilot Use**: "Are we on track to meet the AER SAIDI target for this year?", "Show me GSL payment liability by region for Q3", "Compare our reliability performance against the AER peer group"

### P8: DER Integration Manager (Secondary)
- **Goal**: Manage the growing fleet of rooftop solar, batteries, and EVs on the distribution network
- **Needs**: Solar hosting capacity maps, export curtailment analytics, virtual power plant (VPP) performance, EV charging load forecasts, dynamic operating envelope compliance
- **AI Copilot Use**: "What percentage of solar systems were curtailed in the Randwick area today?", "Show me the VPP dispatch performance for the SA Home Battery Scheme", "Forecast the EV charging load impact on zone substation XYZ for 2028"

---

## 20. Phase 4 — Product Scope

### 20.1 Network Asset Monitoring & Visibility

#### Asset Registry & Hierarchy
- **Network asset model**: Zone substations → Distribution substations → Feeders → Distribution transformers → Customer connection points
- **Asset metadata**: Nameplate rating, installation year, condition score, replacement cost, maintenance history, geographic coordinates
- **Data sources**: DNSP GIS/AM systems (exported as CSV/Parquet), SCADA telemetry (real-time and historical)

#### Real-Time Network Dashboard
- **Zone substation loading**: Current MW load vs nameplate capacity, utilization % with color-coded thresholds (green < 60%, amber 60-80%, red > 80%, critical > 100%)
- **Feeder loading**: Active/reactive power, current, power factor per feeder
- **Voltage monitoring**: Bus voltage at zone substations and key monitoring points, flagging excursions outside ±6% of nominal (AS 61000.3.100)
- **Power quality metrics**: Harmonic distortion (THD), flicker, voltage unbalance
- **Network topology view**: Interactive map showing asset hierarchy, DER locations, and real-time status
- **Temperature monitoring**: Transformer oil temperature, ambient temperature, thermal loading estimates

#### Asset Health & Condition
- **Condition-based monitoring**: Transformer dissolved gas analysis (DGA), oil quality, partial discharge, tap changer health
- **Asset health index**: Composite score per asset based on age, condition, loading history, fault history, and criticality
- **Predictive failure model**: ML model (XGBoost) predicting asset failure probability within 12 months based on telemetry patterns
- **Replacement priority ranking**: Risk-weighted prioritization considering: failure probability × consequence (customers affected × outage duration × cost)
- **Copilot tool**: `get_asset_health(asset_id)` — returns health index, risk factors, recommended actions

### 20.2 Outage Management & Reliability Analytics

#### Outage Tracking
- **Outage event capture**: Start time, end time, affected feeders, affected customers, cause code (vegetation, animal, equipment failure, weather, third party, unknown), restoration steps
- **Data source**: DNSP OMS (Outage Management System) export — or manual entry via copilot
- **Live outage map**: Geographic display of current outages with affected area polygons and estimated restoration times

#### Reliability KPIs (AER Metrics)
- **SAIDI** (System Average Interruption Duration Index): Minutes off supply per customer per year, tracked daily/monthly/yearly against AER target
- **SAIFI** (System Average Interruption Frequency Index): Number of interruptions per customer per year
- **CAIDI** (Customer Average Interruption Duration): Average duration per interruption event
- **MAIFI** (Momentary Average Interruption Frequency): Momentary interruptions (< 1 minute)
- **Worst-performing feeder tracking**: Identify the bottom 5% feeders by SAIDI contribution
- **Exclusion event management**: Track major event days (MEDs) excluded from AER reliability reporting per the 2.5-beta methodology
- **Trend analysis**: Year-on-year comparison, rolling 5-year average, seasonal patterns

#### Guaranteed Service Level (GSL) Monitoring
- **GSL payment tracking**: Customers eligible for GSL payments (e.g., > N interruptions per year, or > M hours cumulative)
- **GSL liability forecast**: Projected GSL payments based on YTD reliability performance
- **Copilot tool**: `get_reliability_metrics(region, period)` — returns SAIDI, SAIFI, CAIDI with trends and AER target comparison

### 20.3 Distributed Energy Resources (DER) Management

#### DER Visibility
- **DER fleet dashboard**: Total installed rooftop solar (MW), batteries (MW/MWh), EVs by zone substation and feeder
- **Real-time DER output**: Estimated aggregate solar generation and battery dispatch per feeder (via smart meter data or SCADA)
- **DER growth tracker**: Monthly new connections by technology type (solar, battery, EV charger), with growth rate trends
- **Reverse power flow detection**: Flag feeders experiencing reverse power flow (net export) and duration/magnitude

#### Hosting Capacity Analysis
- **Static hosting capacity**: Maximum DER export (kW) per distribution transformer and feeder before voltage rise exceeds limits
- **Dynamic hosting capacity**: Time-varying capacity based on current load, voltage, and network configuration
- **Hosting capacity map**: Interactive geographic view showing available capacity by area (green = ample, amber = constrained, red = full)
- **Scenario modeling**: Project hosting capacity under DER growth scenarios (BAU, high solar, high EV, combined)
- **Copilot tool**: `get_hosting_capacity(feeder_id, scenario)` — returns current and projected capacity with limiting constraint

#### Export Curtailment Analytics
- **Curtailment dashboard**: Volume of solar export curtailed (MWh) by feeder and reason (voltage, thermal, DNSP limit, AEMO backstop)
- **Dynamic Operating Envelope (DOE) compliance**: Track DOE limit issuance and customer compliance rates
- **Curtailment equity analysis**: Identify customers disproportionately curtailed due to network position (end-of-feeder)
- **Lost energy value**: Calculate financial impact of curtailment on customers (curtailed MWh × feed-in tariff)

#### Virtual Power Plant (VPP) & Demand Response
- **VPP performance tracking**: Dispatch events, response volume, response accuracy, and revenue per VPP program
- **Demand response program analytics**: DR event performance (MW shed vs target), participation rates, reliability impact
- **Network support contracts**: Track non-network solutions (batteries, VPPs, DR) contracted to defer network augmentation, with performance vs contracted capacity

### 20.4 Network Demand Forecasting & Planning

#### Demand Forecasting
- **Spatial demand forecast**: 10-year peak demand forecast by zone substation and feeder, incorporating:
  - Underlying load growth (economic/demographic drivers)
  - Rooftop solar impact (reduced daytime peak, increased evening ramp)
  - Battery impact (peak shaving)
  - EV charging load (residential overnight + commercial daytime fast charging)
  - Energy efficiency trends
- **Scenario-based forecasting**: High/medium/low growth scenarios aligned with AEMO ISP inputs
- **Model**: Gradient-boosted ensemble (XGBoost) trained on 5+ years of smart meter data, weather, and economic indicators
- **Forecast accuracy tracking**: Actual vs forecast comparison by zone substation, with MAE/MAPE metrics

#### Network Constraint Identification
- **Constraint register**: All identified network constraints (thermal, voltage, fault level), with constraint type, limiting asset, current utilization, and year of breach under each scenario
- **N-1 contingency analysis**: Flag assets where loss of a single element causes remaining elements to exceed emergency ratings
- **Constraint cost estimation**: Estimated cost of unserved energy or alternative supply if constraint is not addressed

#### Augmentation Planning
- **Augmentation options register**: For each constraint, list options: network build (new assets), non-network (demand management, DER, VPP), or hybrid
- **Cost-benefit analysis**: NPV comparison of augmentation vs non-network alternatives, considering deferral value
- **Regulatory Investment Test (RIT-D) support**: Generate data for RIT-D analysis (credible options, market benefit assessment)
- **Copilot tool**: `get_constraint_analysis(zone_substation, horizon_years)` — returns constraints, breach year, options, and recommended action

### 20.5 EV Integration & Load Management

#### EV Charging Load Analytics
- **EV registration tracking**: Estimated EVs per feeder based on vehicle registration data and charging point locations
- **Charging load profiles**: Typical residential overnight, commercial daytime, and fast-charger profiles by time-of-day
- **Network impact assessment**: Identify feeders and transformers at risk of overload under EV growth scenarios
- **Managed charging analytics**: Track uptake and effectiveness of managed/smart charging programs (shifting load from peak to off-peak)
- **Copilot tool**: `forecast_ev_impact(feeder_id, ev_growth_scenario, year)` — returns peak load impact, upgrade requirement, and managed charging benefit

### 20.6 Phase 4 — Copilot Agent Extensions

New tools added to the Mosaic AI agent:

| Tool | Input | Output | Description |
|------|-------|--------|-------------|
| `get_asset_health(asset_id)` | Asset identifier | Health index, risk factors, recommendations | Asset condition assessment |
| `get_network_loading(zone_substation)` | Zone substation name | Current MW, utilization %, DER output | Real-time loading status |
| `get_reliability_metrics(region, period)` | DNSP region, time period | SAIDI, SAIFI, CAIDI, AER targets | Reliability KPI dashboard |
| `get_outage_summary(region, period)` | Region, date range | Outage events, causes, affected customers | Outage history analysis |
| `get_hosting_capacity(feeder_id, scenario)` | Feeder, growth scenario | Available capacity kW, limiting constraint | DER hosting capacity |
| `get_curtailment_analysis(feeder_id, period)` | Feeder, date range | Curtailed MWh, affected customers, reasons | Solar curtailment reporting |
| `get_constraint_analysis(zone_sub, horizon)` | Zone substation, years | Constraints, breach year, options | Network planning assessment |
| `forecast_ev_impact(feeder, scenario, year)` | Feeder, EV scenario, target year | Peak load delta MW, upgrade needs | EV load forecasting |
| `get_vpp_performance(program, period)` | VPP program, date range | Dispatch events, response accuracy | VPP/DR performance tracking |
| `get_der_fleet(zone_substation)` | Zone substation | Solar MW, battery MW/MWh, EVs | DER fleet visibility |

### 20.7 Phase 4 — Frontend Extensions

#### New Tabs
- **Network tab**: Zone substation loading map, real-time utilization dashboard, voltage monitoring, power quality
- **Assets tab**: Asset health index table, predictive failure alerts, condition monitoring trends, replacement priority
- **Outages tab**: Live outage map, reliability KPI tracker (SAIDI/SAIFI vs AER target), worst-performing feeders, GSL liability
- **DER tab**: DER fleet dashboard, hosting capacity map, curtailment analytics, VPP performance, DOE compliance
- **Planning tab**: Spatial demand forecast, constraint register, augmentation options, RIT-D analysis, EV impact scenarios

#### Enhanced Existing Tabs
- **Home tab**: Add network KPI summary (SAIDI YTD vs target, current overloaded assets, active outages, DER curtailment today)
- **Copilot Chat**: All Phase 4 tools available; suggested questions updated with network operations queries
- **Alerts tab**: Add network-level alerts (asset overload, voltage excursion, hosting capacity breach, reliability target at risk, GSL payment threshold)

### 20.8 Phase 4 — Data Architecture Extensions

#### New Data Sources

| Source | Data | Frequency | Ingestion |
|--------|------|-----------|-----------|
| SCADA/ADMS | Substation and feeder telemetry (MW, MVAr, kV, current, temperature) | 1-5 minute intervals | Streaming (Kafka/Autoloader) |
| Smart Meters | Customer interval consumption/export data (30-minute NMI reads) | Daily | Batch (SFTP → Autoloader) |
| GIS/Asset Management | Network asset registry, topology, condition scores | Weekly | Batch (API or CSV export) |
| OMS | Outage events, cause codes, restoration times | Event-driven | Streaming or batch (API) |
| DER Register | CER (Clean Energy Regulator) Small Generation Unit data, battery registrations | Monthly | Batch (CSV download) |
| BoM Weather | Temperature, rainfall, wind, solar radiation by weather station | 30-minute | Existing Phase 1 pipeline |
| ABS/Economic | Population growth, building approvals, economic indicators by region | Quarterly | Batch (API) |
| EV Registration | State motor vehicle registry data, charge point locations | Monthly | Batch (CSV/API) |

#### New Gold Layer Tables

| Table | Description |
|-------|-------------|
| `gold.network_assets` | Master asset registry with hierarchy, ratings, condition, coordinates |
| `gold.asset_loading_5min` | 5-minute zone substation and feeder loading (MW, MVAr, utilization %) |
| `gold.asset_health_index` | Composite health score per asset, updated weekly |
| `gold.asset_failure_predictions` | ML-predicted failure probability per asset (12-month horizon) |
| `gold.voltage_monitoring` | Bus voltage readings with excursion flags |
| `gold.power_quality` | THD, flicker, unbalance metrics by monitoring point |
| `gold.outage_events` | Outage records with cause, duration, affected customers |
| `gold.reliability_kpis` | Daily/monthly/yearly SAIDI, SAIFI, CAIDI, MAIFI by region |
| `gold.gsl_tracking` | GSL-eligible customers and projected payments |
| `gold.der_fleet` | Installed DER by technology, capacity, location, connection date |
| `gold.der_output_estimated` | Estimated aggregate DER output per feeder (30-minute) |
| `gold.hosting_capacity` | Static and dynamic hosting capacity per transformer and feeder |
| `gold.curtailment_events` | Solar export curtailment events with volume, reason, affected customers |
| `gold.doe_compliance` | Dynamic Operating Envelope limit issuance and compliance rates |
| `gold.vpp_dispatch_events` | VPP and DR dispatch records with performance vs target |
| `gold.demand_forecast_spatial` | 10-year spatial demand forecast by zone substation and scenario |
| `gold.network_constraints` | Identified constraints with type, breach year, and options |
| `gold.ev_charging_profiles` | EV charging load profiles and growth projections |
| `gold.ev_network_impact` | Projected EV impact on transformers and feeders by scenario |

#### New Lakebase Tables

| Table | Purpose |
|-------|---------|
| `network_assets` | Editable asset master with condition updates |
| `outage_log` | Manual outage entries and annotations |
| `constraint_register` | Active network constraints and assigned options |
| `augmentation_projects` | Planned augmentation projects with status and cost |
| `non_network_contracts` | Demand management and VPP contracts |
| `ev_programs` | Managed charging program configurations |

### 20.9 Phase 4 — New Genie Spaces

#### Space 9: Network Operations
**Tables/Views**: `gold.asset_loading_5min`, `gold.voltage_monitoring`, `gold.outage_events`, `gold.reliability_kpis`

**Sample Questions**:
- "Which zone substations exceeded 80% utilization today?"
- "Show me SAIDI by region for the past 12 months compared to AER targets"
- "What were the top 5 causes of outages in the Central region this quarter?"
- "List all voltage excursion events in the past week"

#### Space 10: DER & Hosting Capacity
**Tables/Views**: `gold.der_fleet`, `gold.hosting_capacity`, `gold.curtailment_events`, `gold.vpp_dispatch_events`

**Sample Questions**:
- "What is the total installed rooftop solar capacity by zone substation?"
- "Which feeders have less than 20% hosting capacity remaining?"
- "How much solar energy was curtailed last month and what was the financial impact?"
- "Show me VPP dispatch performance for the summer peak program"

#### Space 11: Network Planning
**Tables/Views**: `gold.demand_forecast_spatial`, `gold.network_constraints`, `gold.ev_network_impact`

**Sample Questions**:
- "What is the 10-year peak demand forecast for Canterbury zone substation under the high-growth scenario?"
- "Which zone substations will breach capacity before 2030?"
- "What's the projected EV charging load impact on the top 10 constrained feeders by 2028?"
- "Compare augmentation cost vs non-network solution cost for the Penrith constraint"

### 20.10 Phase 4 — Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Asset overload detection latency | < 5 minutes from SCADA reading | Streaming pipeline monitoring |
| Demand forecast accuracy (zone sub, 1-year MAE) | < 5% | Actual vs forecast comparison |
| Hosting capacity calculation refresh | Daily (static), 5-minute (dynamic) | Pipeline monitoring |
| Asset failure prediction AUC | > 0.85 | MLflow model metrics |
| Reliability KPI dashboard freshness | < 1 hour from OMS update | Dashboard monitoring |
| EV impact forecast error (5-year, peak MW) | < 15% | Backtest validation |
| Network copilot query resolution | > 75% without escalation | Agent evaluation |
| DER curtailment reporting latency | < 24 hours | Pipeline monitoring |
| DNSP user adoption (monthly active) | > 80% of registered users | App analytics |

### 20.11 Phase 4 — Estimated Timeline

| Sprint | Duration | Focus |
|--------|----------|-------|
| Sprint 23 | 2 weeks | Network asset model, SCADA/GIS data ingestion, asset registry gold table |
| Sprint 24 | 2 weeks | Real-time network loading dashboard, voltage monitoring, power quality |
| Sprint 25 | 2 weeks | Outage management data model, reliability KPI engine (SAIDI/SAIFI), outage dashboard |
| Sprint 26 | 2 weeks | Asset health index, predictive failure ML model, asset condition dashboard |
| Sprint 27 | 2 weeks | DER fleet dashboard, hosting capacity calculation engine, hosting capacity map |
| Sprint 28 | 2 weeks | Curtailment analytics, DOE compliance, VPP/DR performance tracking |
| Sprint 29 | 2 weeks | Spatial demand forecasting model, network constraint identification, planning dashboard |
| Sprint 30 | 2 weeks | EV integration analytics, managed charging impact, EV scenario modeling |
| Sprint 31 | 2 weeks | New copilot tools (10), Genie spaces (3), network alerts |
| Sprint 32 | 2 weeks | Integration testing, DNSP user testing, performance tuning, launch |

---

## 21. Full Product Roadmap Summary

```
Phase 1 (Wk 1-10)       Phase 2 (Wk 11-24)       Phase 3 (Wk 25-46)       Phase 4 (Wk 47-66)
Market Intelligence      Lightweight ETRM          Full Trading Platform    Network Operations
& AI Copilot             & Risk                    & Market Expansion       & Distribution Intel
─────────────────        ──────────────────        ─────────────────────    ─────────────────────
✦ Data pipelines         ✦ Deal capture            ✦ AI bid optimization   ✦ SCADA/GIS ingestion
✦ Medallion lakehouse    ✦ Portfolio tracking       ✦ AEMO bid submission   ✦ Asset health & failure
✦ NemSight-like dash     ✦ Forward curves          ✦ Advanced VaR/CVaR     ✦ Outage management
✦ Price/demand fcst      ✦ Mark-to-market          ✦ Option Greeks & vol   ✦ DER & hosting capacity
✦ AI copilot (13 tools)  ✦ Basic risk (VaR)        ✦ Battery optimization  ✦ Reliability (SAIDI)
✦ Genie spaces (3)       ✦ PPA valuation           ✦ Gas markets           ✦ Demand forecasting
✦ Alerting               ✦ FCAS analytics          ✦ WEM expansion         ✦ EV integration
                         ✦ Settlement recon        ✦ Multi-tenancy         ✦ Network planning
                         ✦ Copilot +10 tools       ✦ Copilot +10 tools     ✦ Copilot +10 tools
                         ✦ Genie spaces +2         ✦ Genie spaces +3       ✦ Genie spaces +3

         MVP                Trading-Ready             Enterprise-Grade         DNSP-Ready
     (NemSight++)        (SimEnergy-level)           (EOT Alternative)     (Full Value Chain)
```

| Phase | Cumulative Copilot Tools | Cumulative Genie Spaces | Cumulative Gold Tables |
|-------|-------------------------|------------------------|----------------------|
| Phase 1 | 13 | 3 | 15 |
| Phase 2 | 23 | 5 | 26 |
| Phase 3 | 33 | 8 | 42 |
| Phase 4 | 43 | 11 | 62 |

---

## 22. Glossary

| Term | Definition | Phase |
|------|-----------|-------|
| **NEM** | National Electricity Market — covers QLD, NSW, VIC, SA, TAS | 1 |
| **WEM** | Wholesale Electricity Market — Western Australia (separate from NEM) | 3 |
| **AEMO** | Australian Energy Market Operator | 1 |
| **NEMWEB** | AEMO's public data portal for NEM market data | 1 |
| **RRP** | Regional Reference Price — the spot electricity price in $/MWh | 1 |
| **Dispatch Interval** | 5-minute interval for physical dispatch and pricing | 1 |
| **Trading Interval** | 30-minute interval for financial settlement | 1 |
| **DUID** | Dispatchable Unit Identifier — unique ID for each generator/load | 1 |
| **FCAS** | Frequency Control Ancillary Services — 8 markets for frequency regulation | 2 |
| **Interconnector** | Transmission link between NEM regions | 1 |
| **Bidstack** | Ordered set of price-quantity bids from generators | 1 |
| **SDP** | Structured Data Processing (Lakeflow Spark Declarative Pipelines) | 1 |
| **PPA** | Power Purchase Agreement — long-term contract between generator and buyer | 2 |
| **MAE** | Mean Absolute Error | 1 |
| **MAPE** | Mean Absolute Percentage Error | 1 |
| **MtM** | Mark-to-Market — revaluing open positions against current market prices | 2 |
| **Forward Curve** | Term structure of expected future electricity prices by delivery period | 2 |
| **VaR** | Value at Risk — maximum expected loss at a given confidence level over a period | 2 |
| **CVaR** | Conditional Value at Risk (Expected Shortfall) — average loss beyond the VaR threshold | 3 |
| **Greeks** | Option risk sensitivities: Delta, Gamma, Vega, Theta, Rho | 2 |
| **Black-76** | Standard option pricing model for commodity/energy options | 2 |
| **Capture Price** | Generation-weighted average price a renewable asset actually earns | 2 |
| **Spark Spread** | Difference between electricity price and gas fuel cost — measures gas plant profitability | 3 |
| **STTM** | Short Term Trading Market — gas hubs in Adelaide, Sydney, Brisbane | 3 |
| **DWGM** | Declared Wholesale Gas Market — Victorian gas market | 3 |
| **LGC** | Large-scale Generation Certificate — renewable energy certificate under RET | 2 |
| **STC** | Small-scale Technology Certificate — rooftop solar certificate under SRES | 2 |
| **ETRM** | Energy Trading and Risk Management — integrated trading, risk, and settlement system | 2 |
| **Rebid** | Updating a generator's bid/offer bands intra-day with a valid reason | 3 |
| **Conformance** | Generator adherence to AEMO dispatch targets within tolerance bands | 3 |
| **MILP** | Mixed-Integer Linear Programming — optimization technique for battery dispatch | 3 |
| **Gentailer** | Vertically integrated generator-retailer (e.g., Alinta, Energy Australia, AGL, Origin) | All |
| **NER** | National Electricity Rules — regulatory framework governing the NEM | 3 |
| **DNSP** | Distribution Network Service Provider — operates poles and wires delivering electricity to customers (e.g., Ausgrid, Endeavour, SA Power Networks) | 4 |
| **SAIDI** | System Average Interruption Duration Index — total minutes off supply per customer per year | 4 |
| **SAIFI** | System Average Interruption Frequency Index — number of interruptions per customer per year | 4 |
| **CAIDI** | Customer Average Interruption Duration Index — average outage duration per event | 4 |
| **MAIFI** | Momentary Average Interruption Frequency Index — brief interruptions (< 1 minute) per customer | 4 |
| **DER** | Distributed Energy Resources — customer-sited generation and storage (rooftop solar, batteries, EVs) | 4 |
| **Hosting Capacity** | Maximum DER export a feeder or transformer can accommodate before voltage/thermal limits are breached | 4 |
| **DOE** | Dynamic Operating Envelope — time-varying export/import limits issued to DER based on real-time network conditions | 4 |
| **VPP** | Virtual Power Plant — aggregated fleet of DER dispatched as a single resource for grid services | 4 |
| **GSL** | Guaranteed Service Level — regulated payment DNSPs must make to customers experiencing excessive outages | 4 |
| **RIT-D** | Regulatory Investment Test for Distribution — cost-benefit analysis required before major DNSP capital expenditure | 4 |
| **SCADA** | Supervisory Control and Data Acquisition — real-time telemetry system for monitoring network assets | 4 |
| **ADMS** | Advanced Distribution Management System — software platform for managing distribution network operations | 4 |
| **THD** | Total Harmonic Distortion — measure of power quality (waveform distortion from harmonics) | 4 |
| **N-1 Contingency** | Network planning criterion: system must remain secure after loss of any single element | 4 |
