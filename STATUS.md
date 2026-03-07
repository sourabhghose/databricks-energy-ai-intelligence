# AUS Energy Copilot — Project Status

## Frontend Workstream — completed
**Completed:** package.json, tsconfig.json, vite.config.ts, index.html, src/main.tsx, src/App.tsx, **475 pages** (6 core + 469 domain-specific analytics dashboards), 4 components, api/client.ts, hooks/useMarketData.ts
**Notes:** React 18 + Vite 5 + TypeScript. Recharts for charts, Tailwind for styling, React Router v6 for navigation. SSE streaming in ChatInterface for real-time copilot responses. InterconnectorMap uses inline SVG. Databricks branding applied: DM Sans font, Databricks logo (inline SVG in sidebar + data URI favicon), color palette (#FF3621 red, #1B3139 dark sidebar, #0D1117 dark mode). Build command: `npx vite build` (skips tsc type-checking). **115 API endpoints** serving dashboards across Spot Prices, Renewables, Hydrogen, Battery/Storage, Carbon, Trading, Gas, Retail, Networks, FCAS, and more.

## Backend + Agent Workstream — 2026-02-19T00:00:00+11:00 (AEDT)

**Completed:**
- `app/backend/requirements.txt` — pinned dependencies: fastapi, uvicorn[standard], databricks-sdk, databricks-sql-connector, anthropic, python-dotenv, pydantic, pydantic-settings, sse-starlette, httpx
- `app/backend/main.py` — FastAPI application with CORS middleware, 7 REST market-data endpoints (/api/prices/latest, /api/prices/history, /api/forecasts, /api/generation, /api/interconnectors, /api/fcas, /api/alerts), and POST /api/chat SSE streaming endpoint with a full agentic loop (Claude Sonnet 4.5 with 6 inline tools)
- `app/backend/.env.example` — environment variable template for all secrets (DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID, DATABRICKS_CATALOG, ANTHROPIC_API_KEY, LAKEBASE_HOST, LAKEBASE_PASSWORD)
- `app/app.yaml` — Databricks Apps deployment config with secret-scope injection
- `agent/copilot_agent.py` — Mosaic AI Agent: LangChain AgentExecutor wrapping Claude Sonnet 4.5 with all 13 tools, MLflow PyFunc wrapper for Model Serving registration, interactive CLI REPL
- `agent/__init__.py`, `agent/tools/__init__.py`, `agent/rag/__init__.py`, `agent/evaluation/__init__.py` — package init files; ALL_TOOLS list in tools/__init__.py
- `agent/tools/market_data_tools.py` — 6 market data tools: get_latest_prices, get_price_history, get_generation_mix, get_interconnector_flows, get_active_constraints, get_fcas_prices
- `agent/tools/forecast_tools.py` — 3 forecast tools: get_price_forecast, get_demand_forecast, get_weather_forecast
- `agent/tools/analysis_tools.py` — 3 analysis tools: explain_price_event, compare_regions, get_market_summary
- `agent/tools/rag_tools.py` — 1 RAG tool: search_market_rules (Databricks Vector Search)
- `agent/rag/index_documents.py` — Document chunking (512-token, 64-token overlap), Delta table upsert, VS index creation and sync trigger
- `agent/evaluation/eval_dataset.py` — 50 Q&A evaluation pairs: 15 factual, 10 forecast, 8 event explanation, 7 comparative, 5 market rules RAG, 5 out-of-scope
- `agent/evaluation/run_evaluation.py` — Evaluation harness with 4-dimension scoring, LLM judge, MLflow tracking, JSON report output

**Notes:**
- LLM model used throughout: claude-sonnet-4-5 (as specified in requirements).
- The /api/chat SSE endpoint implements a full agentic loop (up to 5 tool-call rounds), streaming typed events (text, tool_call, tool_result, done, error) so the React frontend can show live tool execution progress.
- All "latest per partition" SQL queries use QUALIFY + ROW_NUMBER() OVER() for warehouse-scale performance rather than correlated subqueries.
- Agent tools use langchain-anthropic as LLM provider. Swap ChatAnthropic for ChatDatabricks to route through a Databricks External Model endpoint (LiteLLM proxy) if preferred.
- RAG index uses databricks-gte-large-en embeddings with TRIGGERED pipeline sync; switch to CONTINUOUS for near-real-time updates.
- Evaluation scores on: key_fact_coverage (30%), tool_usage (20%), decline_correctness (20%), LLM_judge_quality (30%). Pass threshold: 0.70. Target: 80%+ of pairs pass.
- All Gold table references use the DATABRICKS_CATALOG env var (default: energy_copilot) resolved at runtime.

## ML Models Workstream -- 2026-02-19T00:00:00

**Completed:**
- `models/price_forecast/feature_engineering.py` -- PySpark feature store pipeline writing to gold.feature_store_price; temporal, lag, rolling stats, generation-by-fuel, interconnector, weather (current + NWP +1h/+4h/+24h), cross-regional, and horizon explosion features
- `models/price_forecast/train.py` -- LightGBM multi-horizon price forecast training (5 regions, Optuna 50-trial HPO, MLflow logging, MAE+MAPE+spike metrics, "production" alias registration)
- `models/price_forecast/evaluate.py` -- Backtesting evaluation vs AEMO pre-dispatch baseline; spike recall; per-horizon metrics; gold.price_forecast_evaluation Delta table
- `models/demand_forecast/train.py` -- Demand forecast training (same LightGBM / Optuna structure, target: totaldemand MW, MAPE target < 3%)
- `models/demand_forecast/evaluate.py` -- Demand evaluation with MAPE compliance check vs 3% target
- `models/wind_forecast/train.py` -- Wind generation forecast (target: gen_wind_mw, windspeed_100m NWP as key feature)
- `models/wind_forecast/evaluate.py` -- Wind evaluation with bias and MAPE per region
- `models/solar_forecast/train.py` -- Solar generation forecast (night hours excluded from training, zero-clamp on predictions, target: gen_solar_mw)
- `models/solar_forecast/evaluate.py` -- Solar evaluation with daytime-only MAPE, clear-sky MAE, zero-compliance metric
- `models/anomaly_detection/train.py` -- IsolationForest (sklearn Pipeline with StandardScaler) + rule-based classifier (spike >$5000, negative <-$100, separation >$500 regional spread); registered as "anomaly_detector"
- `models/anomaly_detection/evaluate.py` -- Precision/recall/F1 per event type; ROC-AUC binary anomaly; FP rate on normal periods; falls back to rule-based GT if gold.known_market_events is absent
- `tests/test_models.py` -- Unit tests: mandatory feature columns, null checks, temporal range validation, horizon explosion count, prediction shape (5 regions x 6 horizons = 30), anomaly rule-classifier correctness, IsolationForest extreme-outlier detection, integration test stubs (gated by ENERGY_COPILOT_INTEGRATION_TEST=1)

**Notes:**
- All 5 NEM regions (NSW1, QLD1, SA1, TAS1, VIC1) get individual models for price, demand, wind, solar
- `forecast_horizon` is included as an integer feature (1, 4, 8, 12, 24, 48 x 5-min intervals) so each region trains a single multi-horizon model rather than 6 separate models -- reduces registry footprint and improves generalisation across horizons
- Feature store (gold.feature_store_price) is shared across price, demand, wind, and solar model training; demand/wind/solar add their own target column via a PySpark self-join rather than a separate feature table
- Chronological train/val/test split: 18 months train | 3 months val | 3 months test; split boundaries stored as MLflow run tags so evaluate.py can reproduce the exact split without parameter passing
- Solar training excludes night-time hours (20:00-05:59 AEST approximate) to prevent the model learning trivial zero-output -- predictions are zero-clamped at inference time by the forecast pipeline
- Anomaly model uses the most recent 21 months (train+val equivalent) to maximise coverage of rare events; test window inferred from model tags or defaults to last 90 days
- Known events table (gold.known_market_events) is optional for anomaly evaluation; the module falls back to rule-derived ground truth if the table is absent
- All models are registered in MLflow Model Registry with alias "production"; downstream inference pipeline (pipelines/05_forecast_pipeline.py) loads via models:/<name>@production URI

## Setup/DDL Workstream — 2026-02-19T00:00+11:00
**Completed:**
- setup/00_create_catalog.sql — Creates energy_copilot Unity Catalog with 5 schemas (bronze, silver, gold, ml, tools), RBAC grants for 4 principal groups (admins, data-engineers, ml-engineers, app-users, pipeline service principals)
- setup/01_create_schemas.sql — Full schema definitions with extended COMMENT metadata for all 5 schemas
- setup/02_create_tables.sql — Complete DDL for 40 tables (11 Bronze + 13 Silver + 16 Gold). All tables use Delta format with TBLPROPERTIES for auto-optimise. All high-volume time-series tables partitioned by CAST(interval_datetime AS DATE). Every column has a COMMENT annotation for the Genie semantic layer.
- setup/03_setup_lakebase.sql — Postgres DDL for 3 Lakebase tables: user_preferences (dashboard settings + notification prefs), alert_configs (thresholds, channels, predictive alerting flag), copilot_sessions (JSONB message history, ratings, token tracking). Includes indexes, constraints, and auto-update triggers.
- setup/04_create_genie_spaces.py — Databricks SDK script creating 3 Genie Spaces (NEM Prices & Demand, Generation & Fuel Mix, Interconnectors & Constraints). Each space configured with: table semantic metadata (descriptions + column-level comments), 15 benchmark/sample questions, domain glossary (10-12 terms each). Includes REST API fallback if SDK method not yet GA.
- setup/05_create_alerts.sql — 7 Databricks SQL alert views covering: A1) price threshold >300/MWh (moderate spike), A2) price >1000/MWh (high spike), A3) price >5000/MWh (critical spike), A4) sustained high price for 30+ consecutive minutes, B) demand surge >95th-percentile, C) data staleness >10 minutes, D) ML model drift (price MAE >15/MWh excluding extremes, or demand MAPE >3%), E) predictive price spike (ML spike_probability >=70%), F) interconnector congestion >95% utilisation. Each view includes inline comments specifying alert schedule and notification channels.
- setup/06_register_uc_functions.py — Registers all 14 Unity Catalog SQL functions in energy_copilot.tools for the Mosaic AI agent: get_latest_prices, get_price_history, get_generation_mix, get_interconnector_flows, get_active_constraints, get_fcas_prices (market data); get_price_forecast, get_demand_forecast, get_weather_forecast (forecasting); explain_price_event, compare_regions, get_market_summary (analysis); search_market_rules (RAG/Vector Search); get_generator_info (utility). Full SQL bodies, parameter types, return schemas, and docstrings included.

**Notes:**
- All SQL uses fully qualified names energy_copilot.<schema>.<table> throughout.
- 02_create_tables.sql is the largest file (1,278 lines, ~86KB) and covers the complete medallion architecture. Gold tables include delta.enableChangeDataFeed=true on tables used by the agent (prices, forecasts, anomalies).
- 06_register_uc_functions.py: the compare_regions tool uses a UNION-based pattern because Databricks SQL functions cannot use dynamic pivot. The agent should be told to call this tool separately per metric.
- 04_create_genie_spaces.py: Genie Spaces API is in preview in Databricks SDK. The script includes a REST API fallback (POST /api/2.0/genie/spaces) for workspaces where the SDK method is not yet GA.
- 05_create_alerts.sql creates views only; actual alert objects (schedule + notification destination) must be configured in the Databricks SQL Alerts UI or via REST API after the views are created.
- Run order: 00 -> 01 -> 02 -> 03 (separate Postgres connection) -> 04 -> 05 -> 06.

## Pipelines Workstream — 2026-02-19
**Completed:**
- pipelines/nemweb_downloader.py — polling utility, exponential backoff, Delta dedup tracking
- pipelines/01_nemweb_ingest.py — DLT Autoloader Bronze→Silver→Gold (prices, gen, interconnectors, FCAS pivot)
- pipelines/02_openelec_ingest.py — OpenElectricity API incremental Bronze→Silver→Gold
- pipelines/03_weather_ingest.py — Open-Meteo BOM ACCESS-G Bronze→Silver→Gold with energy-demand derived features
- pipelines/04_solar_ingest.py — APVI rooftop solar Bronze→Silver→Gold (ACT→NSW1 mapping)
- pipelines/05_forecast_pipeline.py — MLflow UC registry model loading, inference, spike alerts
- pipelines/06_market_summary.py — Claude Sonnet 4.5 daily summary pipeline (05:30 AEST)

**Notes:**
- FCAS prices pivoted from 8 columns to long format in Gold for query flexibility
- Gold tables partitioned by interval_date throughout
- All pipelines degrade gracefully when Spark unavailable (unit test mode)
- Market summary uses claude-sonnet-4-5 with no-trading-advice guardrails in system prompt

## Backend Refinements Workstream — Sprint 1
**Completed:** app/backend/main.py (rewritten), app/backend/db.py, app/backend/mock_data.py
**Notes:**
- main.py rewritten with: FastAPI lifespan startup health check (Databricks + Lakebase); per-request structured JSON logging middleware that emits request_id, method, path, status_code, duration_ms; global exception handler returning {"error": "...", "detail": "...", "request_id": "..."} JSON; get_db_cursor() generator dependency raising 503 on warehouse failure; ALLOW_ORIGINS env var for CORS (default "*"); ANTHROPIC_API_KEY is now optional at startup (chat endpoint returns an error SSE event if unset rather than crashing at import time).
- In-memory TTL cache dict with time.monotonic() expiry: /api/prices/latest 10s, /api/generation 30s, /api/interconnectors 30s, /api/forecasts 60s. Cache keys include region/horizon parameters to avoid cross-region collisions.
- All endpoints now raise 404 when DB returns empty rows for a specific region/filter, 503 on DB connectivity failure, 422 via Pydantic for bad request bodies.
- New alerts endpoints: GET /api/alerts (list, filterable by is_active), POST /api/alerts (create, returns 201 with full AlertConfig), GET /api/alerts/:id (single), DELETE /api/alerts/:id (204 No Content). All four delegate to LakebaseClient.upsert / execute / execute_one; fall through to mock data when LAKEBASE_HOST is unset.
- AlertConfig model extended with notification_channel and updated_at fields. AlertCreateRequest model added (threshold_value ge=0 validation).
- db.py: DatabricksSQLClient wraps databricks-sql-connector; exposes execute(), execute_one(), health_check(), and a context-manager cursor(). LakebaseClient wraps psycopg2; exposes execute(), execute_one(), upsert() (INSERT ... ON CONFLICT DO UPDATE), health_check(). Both classes detect missing env vars at __init__ and switch to mock_mode=True, logging a warning.
- mock_data.py: get_mock_latest_prices() — 5-region prices at NSW1=$85, QLD1=$78, VIC1=$92, SA1=$105, TAS1=$71 with +/-5% noise. get_mock_price_history() — sine-wave day shape (morning/evening peaks) + +/-10% noise, 5-min intervals. get_mock_forecasts() — widening confidence bands with horizon distance. get_mock_generation() — per-region realistic fuel mix (e.g. TAS1 82% hydro, SA1 38% wind). get_mock_interconnectors() — 5 NEM interconnectors with plausible base flows. get_mock_alerts() — 3 example alert configs. get_mock_fcas() — 8 FCAS service types with realistic price ranges.
- requirements.txt: added psycopg2-binary==2.9.10. .env.example: added LAKEBASE_PORT, LAKEBASE_DB, LAKEBASE_USER, ALLOW_ORIGINS, LOG_LEVEL entries.

## Agent Tests + Infrastructure Workstream — Sprint 1
**Completed:** tests/test_agent.py, .gitignore, README.md, requirements-dev.txt
**Notes:**
- tests/test_agent.py uses unittest.mock throughout; no real Databricks, Anthropic, or MLflow calls are made. All Databricks SQL connections are patched at the _connection() and _query() helper level rather than at the tool function level, keeping tests stable against future SQL changes. FastAPI endpoint tests use httpx.AsyncClient with ASGITransport (no live server required). The _stream_chat generator is patched in test_chat_endpoint_streams so the full SSE content-type assertion works without touching Anthropic. pytest-asyncio is required for the async test class.
- .gitignore covers Python, Node/React/Vite, Databricks bundle files, MLflow run artefacts, IDE configs (.vscode, .idea), OS noise (.DS_Store), and a comprehensive secrets block (*.key, *.pem, credentials.json, secrets.yml). .env.example is explicitly excluded from the secrets block so the template file remains tracked.
- README.md reproduces the condensed ASCII architecture diagram from IMPLEMENTATION_PLAN.md (4-layer stack), a 3-step Quick Start, a full directory tree with one-line descriptions, the 4 external data sources as a table, and a Sprint Status checklist mirroring the Phase 1 Definition of Done.
- requirements-dev.txt pins pytest, pytest-asyncio, pytest-mock, httpx, unittest-mock, black, ruff, and mypy. unittest-mock (PyPI shim) is included alongside stdlib unittest.mock for toolchain compatibility; the stdlib version is what the test file actually imports.

## Frontend Wiring Workstream — Sprint 1
**Completed:** Home.tsx, LiveMarket.tsx, Forecasts.tsx, PriceTicker.tsx, GenerationChart.tsx, InterconnectorMap.tsx, useMarketData.ts
**Notes:**
- useMarketData.ts: Added useGeneration(region, pollMs=60000) and useInterconnectors(pollMs=30000). Both hooks attempt the real API first and fall back to plausible mock data on error so the UI is never blank. Mock generation defaults: coal 2000 MW, gas 500 MW, wind 800 MW, solar 400 MW, hydro 300 MW, battery ~65 MW. Mock interconnectors match the 5 standard NEM interconnectors with realistic limit values.
- PriceTicker.tsx: Added REGION_FULL_NAMES map (NSW1→"New South Wales", etc.) displayed below the region badge. Price formatted via toLocaleString('en-AU', {minimumFractionDigits:2}) which produces the $X,XXX.XX pattern. Trend icon wrapper uses CSS transition on opacity/transform; card background uses transition on background-color/border-color for smooth price-level colour changes.
- GenerationChart.tsx: Replaced built-in Recharts Legend with a custom colour-swatch legend div below the chart. Added CustomTooltip component showing MW value and % of total for each fuel type at the hovered timestamp. buildMockData() fallback fires when data prop is empty (coal 2000 MW, gas 500 MW, wind 800 MW, solar 400 MW, hydro 300 MW, battery ~65 MW).
- InterconnectorMap.tsx: Redrawn with specified node positions (QLD1 250,60 · NSW1 330,180 · VIC1 280,310 · SA1 130,300 · TAS1 270,420) on 500×450 viewBox. All 5 interconnectors rendered as FlowLine components with SVG marker arrowheads. Murraylink is visually separated from V-SA via a ±12px perpendicular offset and a dashed stroke. Line colour: green (flow>0), red (flow<0), gray (flow=0). Stroke width = 1 + (abs(flowMw)/limitMw)*5, clamped to [1,6]px. STATIC_FLOWS ensure the diagram is always drawn even with no API data.
- Home.tsx: Replaced stub Sparkline with RegionSparkline using usePriceHistory (last 24 hrs, down-sampled to ≤96 pts). Recharts LineChart 120×40 with no axes. Shows animate-pulse skeleton while loading. DemandCard row uses avg_demand_mw and peak_demand_mw from merged regionData (real API prices + static demand placeholders). Last-updated timestamp shown top-right via Clock icon. Error banner downgrades to amber "API unavailable – showing indicative mock data" rather than hard red, since mock data is always shown.
- LiveMarket.tsx: Wired to usePriceHistory, useGeneration, useInterconnectors hooks. Auto-refresh implemented via a 30s setInterval that updates the timeWindow state, causing usePriceHistory to refetch. ReferenceLine at $300 (amber) and $5000 (red, dashed). Spinning RefreshCw icon shows during fetch. Generation and interconnector sections each show animate-pulse skeleton during initial load.
- Forecasts.tsx: Added useAccuracy hook that fetches /api/forecasts/accuracy?region=… with fallback to STATIC_ACCURACY (MAE $10.2/$13.7/$18.5 for 1hr/4hr/24hr; MAPE 8.4%/11.9%/16.3%). Accuracy table colour-codes MAE green/amber/red relative to $12/$15 thresholds. ChartSkeleton component used as loading placeholder for the chart section. isAnimationActive=false on all lines to avoid re-animation on every 30s refresh.

## Data Quality + Registry Workstream — Sprint 1
**Completed:** setup/load_generator_registry.py, tests/test_pipelines.py, tests/test_data_quality.py
**Notes:**
- setup/load_generator_registry.py: Plain Python script (not a notebook). Downloads AEMO NEM Registration and Exemption List XLSX via direct API URL (https://aemo.com.au/aemo/apps/api/report/NEM_REGISTRATION_AND_EXEMPTION_LIST); falls back to --local-csv CLI arg or GENERATOR_REGISTRY_CSV env var. Parses multi-sheet XLSX by finding the header row containing "DUID" (robust to AEMO's varying row offsets). Normalises column names using a column map keyed on lowercased AEMO header strings. Fuel-type mapping uses two-level lookup: fuel_source_primary first (exact then partial-match), then tech_type_primary as tiebreaker; all 10 canonical labels supported (coal, gas, hydro, wind, solar_utility, battery, pumps, liquid, biomass, other). CO2 intensity coefficients (tCO2e/MWh) are injected per fuel_type using Australian NGA Factors 2023 values. region_weather_mapping table built from the 5 hardcoded NEM-region coordinates from Implementation Plan §3.3. Both tables written as Delta with mode=overwrite via PySpark when a session is available; falls back to local delta-rs (or Parquet) for local/CI runs. --dry-run flag skips all writes. Structured JSON logging throughout with graceful error handling; sys.exit(1) on fatal data-source failure.
- tests/test_pipelines.py: 10 unit tests across TestNemwebDownloader (6 tests) and TestProcessedFilesTracker (4 extra + 3 core = all required tests present). All HTTP calls are patched via unittest.mock; no network I/O. In-memory ZIPs built with Python's zipfile module for extract tests. _process_directory is tested directly for the skip-processed case (avoids needing to stub run_once's full loop). Backoff test patches time.sleep to avoid CI slowdowns; side_effect list with 4 ConnectionErrors + 1 success validates MAX_RETRIES=5 call count. Tracker's Spark-write path tested by verifying mock_spark.createDataFrame called and saveAsTable called with PROCESSED_FILES_TABLE constant.
- tests/test_data_quality.py: 4 test classes covering Gold prices (5 tests), Gold FCAS (2 tests), Silver dispatch_gen (2 tests), and generator_registry (3 tests). Each test has a paired "_detects_*" variant that seeds the mock data with the defect and asserts it is caught — this ensures the assertion logic itself is correct and not trivially passing. MockDataFrame backed by pandas provides a Spark-like API (filter, count, dropDuplicates, col(), isin(), isNull()) without requiring PySpark. Integration test classes for all 4 groups are gated by ENERGY_COPILOT_INTEGRATION_TEST=1 and use the real Spark table API (pyspark.sql.functions). NEM price limits set to [-1000, 17000] matching the APC floor and MPC headroom. FCAS prices asserted non-negative (FCAS markets clear at >= 0 by design). Future-interval test uses timezone-naive datetime comparison consistent with NEMWEB SETTLEMENTDATE format.

## ML Model Depth Workstream — Sprint 2
**Completed:** models/price_forecast/feature_engineering.py (enhanced), models/price_forecast/train.py (enhanced), models/anomaly_detection/train.py (rewritten), models/register_all_models.py (new)
**Notes:**
- `feature_engineering.py`: added five new feature groups to the existing pipeline without breaking backward compatibility. (1) Volatility regime: 5-day (1440-interval) range-based rolling std of spot price, classified into low/medium/high regimes using $/MWh thresholds (20/80). (2) FCAS correlation: `build_fcas_features()` loads `silver.fcas_prices` and produces raise6sec current + 1-interval + 6-interval lags as leading indicators for spike events. (3) Interconnector stress: `build_interconnector_features()` now also aggregates NEM-wide absolute net import (`nem_ic_stress_mw`) alongside existing per-region metrics. (4) Time flags: `is_peak_period` (07:00–09:00 and 17:00–20:00 AEST via UTC+10 shift), `is_business_day` (Mon–Fri, overridden to 0 on public holidays), `days_to_quarter_end` (approximate days to next Q-end for strategic bidding signals). (5) Spike history: range-based rolling count of rrp > $300/MWh over preceding 24h and 7d windows. `build_feature_store()` now explicitly filters `silver.dispatch_prices` to all five NEM regions and logs region count in the completion message.
- `train.py`: expanded Optuna search space to full sprint spec (num_leaves [20,300], learning_rate log [1e-4,0.3], n_estimators [200,2000], min_child_samples [5,100], subsample [0.5,1.0], colsample_bytree [0.5,1.0], lambda_l1/l2 [0,10]). Added `SPIKE_WEIGHT=3.0` sample weights applied to `lgb.Dataset(weight=...)` for all intervals where rrp_target > $300/MWh — improves spike recall without a separate model. Integrated `optuna.integration.lightgbm.LightGBMPruningCallback` alongside `lgb.early_stopping(50)` so bad trials are pruned early. Best hyperparameters are logged both as MLflow params (`best_<k>`) and as run tags (`best_param_<k>`) for programmatic retrieval. Changed registration alias from "production" to "champion" with an explicit code comment explaining that "production" should only be assigned after evaluate.py confirms DoD thresholds (MAE < $15/MWh, spike MAE < $200/MWh). Added `volatility_regime` to `EXCLUDED_COLS` for one-hot encoding via `encode_categoricals()`.
- `anomaly_detection/train.py`: complete rewrite loading from `gold.nem_prices_5min` (with fallback to `gold.feature_store_price`) for 21 months. Features are now engineered in pure pandas: hour_of_day, day_of_week, LabelEncoder region_id, 12-interval rolling mean/std for price and demand. IsolationForest contamination lowered to 0.01 to match the NEM anomaly base rate. Separation rule is computed at the interval level using `groupby("interval_datetime").transform()` so all regions in an interval are flagged consistently. `is_anomaly` is the union of IsolationForest and rule-based flags. Feature importance uses `sklearn.inspection.permutation_importance` with a custom scorer measuring score-distribution variance (native feature importance unavailable for IsolationForest); values logged as individual MLflow metrics. Handles `RESOURCE_DOES_NOT_EXIST` on model registration gracefully. Unit-test mode triggers when `spark=None` or PySpark is not installed, generating synthetic NEM-like data via `_make_synthetic_data()`. Registers to `energy_copilot.ml.anomaly_detection_nem@production`.
- `register_all_models.py`: standalone CLI script using `argparse`. Iterates all 20 regional forecast experiments + anomaly detection experiment. For each model: looks up the latest `phase=final_training` run (region-filtered for forecast models), calls `_ensure_registered_model()` which catches `RESOURCE_DOES_NOT_EXIST` and creates the model if absent, then creates a model version and sets the alias. Anomaly detection falls back to the latest run if no `final_training` tag is present. Prints a formatted ASCII summary table with columns: model_name | run_id (12-char) | metric_value | alias_set | status. `--dry-run` flag skips all write operations. Requires `DATABRICKS_HOST` and `DATABRICKS_TOKEN` env vars; validates at startup. Exit code 1 if any model failed to register.

## Agent + RAG Workstream — Sprint 3
**Completed:** agent/copilot_agent.py (enhanced), agent/rag/index_documents.py (enhanced), agent/tools/rag_tools.py (enhanced), agent/prompt_templates.py (new)
**Notes:**
- Primary vs. fallback agent paths: The agent now has two distinct execution paths. The primary path uses `run_agentic_loop()` (raw Anthropic SDK with `client.messages.create()`) which gives direct control over the tool-call loop, token counting, and stop-reason handling. The secondary path is the existing LangChain `AgentExecutor` kept for local dev convenience via `--use-langchain` CLI flag. Both paths share `MAX_TOOL_ROUNDS = 10` and the same system prompt.
- Security enforcement at the agent layer (not just system prompt): Two independent guards are applied before any tool calls are made. `_is_trading_advice_request()` uses a pre-compiled whole-word regex against `REFUSAL_PATTERNS = ["buy", "sell", "invest", "trade", "position", "short", "long"]` and returns the `REFUSAL_RESPONSE` canned message without touching the LLM. `_contains_injection_token()` checks for known LLM jailbreak tokens (`</s>`, `<|im_end|>`, `<|system|>`, etc.) as case-insensitive substrings and returns `INJECTION_RESPONSE`. Both checks are in the hot path of `predict()` and the interactive REPL.
- Token tracking: `run_agentic_loop()` accumulates `input_tokens` and `output_tokens` across all API calls in the loop (including tool-call rounds) via `response.usage`. These are logged as MLflow metrics (`mlflow.log_metric`) inside the `predict()` nested run, enabling per-session cost visibility in the MLflow experiment.
- Streaming via `stream_predict()`: Uses `client.messages.stream()` context manager and `stream.get_final_message()` to get the final usage stats. Yields typed SSE event dicts (`text`, `tool_call`, `tool_result`, `done`, `error`), matching the event taxonomy the FastAPI `/api/chat` SSE endpoint already expects.
- System prompt is dynamic: `_build_system_prompt()` injects the current AEST date/time (formatted `%Y-%m-%d %H:%M AEST`) and the 5 NEM region codes at every call — not at module import time — so the timestamp is always fresh.
- Tool schema bridge: `_langchain_tool_to_anthropic_tool()` converts LangChain `@tool`-decorated functions (Pydantic `args_schema`) to Anthropic `input_schema` format, allowing the same tool definitions to serve both the LangChain and raw Anthropic paths without duplication.
- RAG index restructured: `index_documents.py` now targets `energy_copilot.gold.aemo_document_chunks` (was `energy_copilot.rag.aemo_document_chunks`) and `energy_copilot.gold.aemo_docs_vs_index` (was `energy_copilot.rag.aemo_documents_vs_index`) to consolidate all Gold assets in one schema. Table schema changed to `chunk_id / content / metadata_json / embedding_model / indexed_at` to store all doc-type-specific metadata in a single JSON blob rather than adding nullable columns per doc type.
- Chunking strategies by doc type: PDFs use `\n\n` paragraph-boundary splitting with a 1500-char target and 200-char overlap sliding-window fallback for oversized paragraphs. XML constraint files use `xml.etree.ElementTree` to emit one chunk per `<ConstraintEquation>` element with the equation ID as `section_title`. HTML notices split on `<h1>`–`<h3>` heading tags after tag-stripping with a pure-regex approach (no BeautifulSoup dependency).
- Deterministic chunk IDs: `_make_chunk()` generates UUIDs from `SHA-256(source_url + "::" + chunk_index)` so re-indexing the same document yields the same `chunk_id` values, making upserts fully idempotent.
- Graceful VS degradation: Both `index_documents.py` and `rag_tools.py` wrap all `VectorSearchClient` usage in `try/except ImportError` + `try/except Exception` blocks. If the SDK is not installed or the endpoint is down, the indexer logs a warning and continues (Delta write still completes). The `search_market_rules` tool falls back to a `spark.sql()` LIKE query against the Delta table, with `_sanitize_sql_query()` stripping single quotes, semicolons, backslashes, and SQL comment sequences before embedding the query in the SQL string.
- `prompt_templates.py`: Centralises all prompt text to make system prompt changes testable in isolation. Contains `SYSTEM_PROMPT_TEMPLATE` (f-string with `{current_datetime_aest}` and `{nem_regions}`), `REFUSAL_RESPONSE`, `ERROR_RESPONSE`, `INJECTION_RESPONSE`, `CLARIFICATION_PROMPTS` dict (8 NEM terminology entries), and `FEW_SHOT_EXAMPLES` list (3 Q&A pairs covering live data, event explanation, and market rules lookup).

## CI/CD + Config Workstream — Sprint 4
**Completed:** .github/workflows/ci.yml, pyproject.toml, .pre-commit-config.yaml, tests/test_models.py (5 new anomaly tests), docs/DEPLOYMENT.md
**Notes:**
- `.github/workflows/ci.yml` has three jobs: `python-tests` (Python 3.11, pytest with `-k "not integration"`, dummy env vars for Databricks/Anthropic so imports succeed), `frontend-build` (Node 20, npm ci + npm run build in app/frontend), and `lint` (ruff with GitHub output format + mypy). All jobs use `ubuntu-latest` only (no cross-platform matrix). pip and npm caches are keyed on the respective lock files using `actions/cache@v4`.
- `pyproject.toml` centralises all tool config: pytest `asyncio_mode=auto` for pytest-asyncio, `filterwarnings` suppressing DeprecationWarning from pydantic/lightgbm/numpy, ruff `F821` ignore so Databricks `spark`/`dbutils` globals do not cause false positives in notebooks, mypy `check_untyped_defs=true` for stricter type checking without requiring full annotations.
- `.pre-commit-config.yaml` uses pinned revisions for ruff (v0.4.9), black (24.4.2), and pre-commit-hooks (v4.6.0). The ruff hook uses `--fix --exit-non-zero-on-fix` so auto-fixable issues are corrected but the commit is still blocked, requiring a re-stage.
- The 5 new tests in `TestAnomalyDetection` use `classify_events_rule()` from `models.anomaly_detection.train` directly (imported at test time to avoid top-level PySpark dependency). `test_isolation_forest_unit_test_mode` patches all MLflow calls (`set_experiment`, `start_run`, `log_params`, `log_metrics`, `sklearn.log_model`, `log_artifact`, `MlflowClient`) so the full train path runs in-process with synthetic data and no network I/O.
- `docs/DEPLOYMENT.md` covers all 9 deployment steps, a complete environment variables table, and three common troubleshooting scenarios (missing secret scope, wrong catalog, VS index not ready).

## Deployment + Monitoring Workstream — Sprint 4
**Completed:** databricks.yml, setup/07_setup_monitoring.py, Makefile, app/frontend/.env.example, app/backend/main.py (market-summary endpoint added)
**Notes:**
- `databricks.yml` uses DAB v2 schema (`targets:`, not `environments:`). Two targets defined: `dev` (mode: development, catalog: energy_copilot_dev) and `prod` (mode: production, catalog: energy_copilot). Variable substitution `${var.catalog}` is threaded through all job `base_parameters` and DLT pipeline `configuration` blocks so a single bundle deploy switches the catalog without touching notebook code.
- Job schedules: job_01_nemweb_ingest and job_05_forecast_pipeline run on `0 */5 * * * ?` (every 5 min); jobs 02/03/04 run every 30 min on staggered cron offsets (`:00`, `:10`, `:20`) to avoid thundering-herd on the SQL warehouse. job_06_market_summary uses `0 30 19 * * ?` UTC which is 05:30 AEST (UTC+10).
- Cluster policy: all jobs use `SPOT_WITH_FALLBACK` with `first_on_demand: 1` to keep one on-demand driver for reliability. Autoscale 2–8 workers, DBR 15.4 LTS on ingest/summary jobs, and DBR 15.4 LTS ML (`15.4.x-ml-scala2.12`) on the forecast job which requires scikit-learn and lightgbm pre-installed. Photon enabled on all clusters for Delta write acceleration.
- DLT pipelines mirror jobs 01–04 as `resources.pipelines` entries. `development: ${bundle.target == "dev"}` enables DLT development mode (no auto-retry, cheaper) on the dev target automatically.
- MLflow experiments are declared under `resources.experiments` for all five model types (price, demand, wind, solar, anomaly); paths follow the `/Shared/energy_copilot/experiments/<type>_forecast` convention used by the training scripts.
- `setup/07_setup_monitoring.py` follows the Databricks notebook source convention (`# Databricks notebook source` header, `# COMMAND ----------` separators). Uses `databricks.sdk.WorkspaceClient` and `w.quality_monitors.create()`. Regression monitors (`MonitorInferenceLog` with `PROBLEM_TYPE_REGRESSION`) are created for price/demand/wind/solar gold tables; a snapshot-based custom monitor is created for `daily_market_summary`. All five monitors share a daily 07:00 UTC schedule (`MonitorCronSchedule`). `ResourceAlreadyExists` is caught and logged as a warning so the notebook is safely re-entrant.
- Drift thresholds are implemented as `MonitorCustomMetric` objects of type `AGGREGATE`. JS divergence uses a symmetric log-ratio approximation expressible in Spark SQL (Lakehouse Monitoring evaluates metrics as SQL against the profile data). MAE relative increase is computed as `(current_mae - baseline_mae) / baseline_mae`; a value > 0.25 represents a 25%+ regression. Slicing by `region_id` and `horizon_hours` means the drift metrics are computed per-region per-horizon, enabling fine-grained alerting.
- `GET /api/market-summary/latest` queries `energy_copilot.gold.daily_market_summary ORDER BY summary_date DESC LIMIT 1`. Response fields: `summary_date`, `narrative`, `model_id`, `generated_at`, `word_count`, `generation_succeeded`. Cached for 3600 s (`_TTL_MARKET_SUMMARY`) because the pipeline only writes once per day. Any DB exception falls back to `_MOCK_MARKET_SUMMARY` rather than raising 503, keeping the dashboard functional when the warehouse is cold.
- `app/frontend/.env.example` exposes four Vite-prefixed variables: `VITE_API_BASE_URL`, `VITE_APP_TITLE`, `VITE_ENABLE_MOCK_DATA`, `VITE_GENIE_SPACE_ID`. The file is committed; developers copy it to `.env.local` (gitignored) for local overrides.
- Makefile uses GNU Make tab-indented recipes throughout. The `help` target auto-generates usage from `## comment` annotations using `grep` + `awk` so the list stays in sync with target definitions without manual maintenance.

## Frontend Completion Workstream — Sprint 4
**Completed:** app/frontend/src/pages/Copilot.tsx (rewritten), app/frontend/src/components/ChatInterface.tsx (enhanced), app/frontend/src/pages/Alerts.tsx (enhanced), app/frontend/src/pages/Genie.tsx (enhanced), app/frontend/src/api/client.ts (enhanced)
**Notes:**
- `Copilot.tsx` rewritten as a full-featured page. `ChatInterface` is now a `forwardRef` component exposing an imperative `ChatInterfaceHandle` with `clearChat()`, `sendQuestion()`, and `focusInput()` methods. The Copilot page holds all telemetry state (token counter, session stats, API error indicator) and receives it from `ChatInterface` via `onDoneEvent` and `onApiError` callback props rather than prop-drilling into the SSE stream. The collapsible 200px sidebar uses CSS `w-[200px]` and a `ChevronLeft`/`ChevronRight` toggle button overlaid on the chat area edge. Cmd/Ctrl+K global keyboard shortcut uses a `window.addEventListener` effect that calls `chatRef.current?.focusInput()`.
- Token counting is done by parsing the `event: done` SSE frame in `ChatInterface` (which carries `{"input_tokens": X, "output_tokens": Y}`). The parsed values are passed to the parent via `onDoneEvent(inputTokens, outputTokens, elapsedMs)`; the Copilot page accumulates them with `setTotalTokens(prev => prev + input + output)`. The `performance.now()` API provides sub-millisecond timing for the avg response time stat.
- `Alerts.tsx` now owns its own data-fetching directly via `api.getAlerts()` rather than the `useAlerts` hook (which had no fallback); falls back to `MOCK_ALERTS` on error with an amber banner. The `Alert` interface was extended with optional `isActive` and `notificationChannel` fields. The create modal posts to `api.createAlert()` and degrades gracefully to a local optimistic mock on network failure. Delete uses optimistic removal then `api.deleteAlert()` with re-fetch on error. Toggle is fully optimistic with a `TODO` comment marking the `PATCH /api/alerts/:id` stub. Severity is derived from thresholds: RED ≥$5000, ORANGE ≥$300, YELLOW ≥$100, GREY otherwise; per-row left-border color coding uses Tailwind's `border-l-4` with severity-matched colors.
- `Genie.tsx` now checks both `VITE_GENIE_SPACE_ID` and `VITE_GENIE_URL`. When `VITE_GENIE_SPACE_ID` is set (but no explicit URL), the iframe URL is constructed as `https://<workspace>.azuredatabricks.net/genie/spaces/<space-id>` — developers are expected to also set the full `VITE_GENIE_URL` for production. Preset query buttons send `{ type: "genie_query", query }` via `window.postMessage` to the iframe's `contentWindow`; the Genie app must handle this message type (Databricks Genie spaces do not have a published postMessage API as of 2026-02, so this is forward-compatible plumbing). The Genie info box is always shown above the iframe/setup panel.
- `api/client.ts` adds: `createAlert(AlertCreateRequest) → Alert` (POST /api/alerts), `deleteAlert(id) → void` (DELETE /api/alerts/{id}), `getMarketSummary() → MarketSummaryRecord` (GET /api/market-summary/latest). Internal `post<TBody, TResponse>()` and `del()` helpers added. The `Alert` interface extended with optional `isActive` and `notificationChannel` fields to match the Lakebase `alert_configs` schema documented in Sprint 1. New interfaces `AlertCreateRequest` and `MarketSummaryRecord` exported.

## Frontend Polish + Dev Infrastructure Workstream — Sprint 5
**Completed:** app/frontend/src/pages/Home.tsx (market summary widget), tests/conftest.py (shared pytest fixtures), docker-compose.yml, app/backend/Dockerfile, app/frontend/Dockerfile.dev
**Notes:**
- Home.tsx market summary widget: calls api.getMarketSummary() on mount, shows loading skeleton (animate-pulse), renders narrative in overflow-y-auto max-h-48 card with date/word-count/model badges. Placed between PriceTicker row and lower dashboard content. "Powered by Claude Sonnet 4.5" attribution badge with Zap icon.
- conftest.py: mock_spark (MagicMock with table/sql/createDataFrame methods), mock_anthropic_client (messages.create returning mock text), databricks_env autouse fixture (sets dummy DATABRICKS_HOST/TOKEN/ANTHROPIC_API_KEY). IS_INTEGRATION constant. All fixtures use try/except ImportError for pyspark.
- docker-compose.yml: backend service (build from repo root, port 8000, .env.local, volumes for agent/ and models/), frontend service (Dockerfile.dev, port 5173, VITE_API_BASE_URL, depends_on backend healthcheck). Backend healthcheck uses GET /api/health.
- Dockerfiles: backend Dockerfile copies app/backend/, agent/, models/ into /app (built from repo root context). Frontend Dockerfile.dev is a Node 20 slim dev-only image with npm run dev --host 0.0.0.0.

## Backend Quality + Deployment Workstream — Sprint 5
**Completed:** tests/test_backend.py (FastAPI integration test suite), app/app.yaml (Databricks Apps config)
**Notes:**
- test_backend.py: 5 test classes covering health, prices (latest + history), forecasts, generation, alerts (list/create/delete), and chat (streaming). All tests use FastAPI TestClient (no live services). Module-level env var stubs enable mock_mode in DatabricksSQLClient/LakebaseClient. TestChatEndpoint tests that the /api/chat endpoint returns a 200 with either SSE or JSON content-type. Alert DELETE 404 test validates Lakebase 404 propagation. create_alert validates request body schema enforcement.
- app/app.yaml: Databricks Apps v2 config using `command:` (uvicorn with 2 workers). Env vars use `valueFrom.fieldRef` for auto-injected DATABRICKS_HOST/TOKEN, `valueFrom.secretRef` for Anthropic key and Lakebase creds (scope: energy_copilot). Static values for DATABRICKS_CATALOG, LOG_LEVEL, PYTHONPATH=/app. The PYTHONPATH=/app ensures that `import agent.*` and `import models.*` resolve correctly when the app is deployed with the repo structure mounted at /app.

## CI + Config Quality Workstream — Sprint 6
**Completed:** pyproject.toml (pythonpath fix), .github/workflows/ci.yml (PYTHONPATH env var), .dockerignore, setup.py stub, .gitignore updates
**Notes:**
- pyproject.toml: Added `pythonpath = ["."]` to `[tool.pytest.ini_options]`. This is required for `from app.backend.main import app` in test_backend.py and `from models.anomaly_detection.train import classify_events_rule` in test_models.py to resolve from the repo root. pytest 7+ supports the `pythonpath` ini option natively.
- ci.yml: Added `PYTHONPATH: .` to python-tests job env (belt-and-suspenders alongside pyproject.toml pythonpath config). This ensures the env var is set even if an older pytest version is used or if pyproject.toml pythonpath support is not available.
- .dockerignore: Excludes .git, Python bytecode, node_modules, .databricks, docs, and test artifacts from Docker build context. Keeps image build fast and prevents credential files from being accidentally included.
- setup.py: Stub enabling `pip install -e .` for editable installs, useful in CI if any tool resolves the package via setuptools.

## Wind + Solar Model Enhancement — Sprint 6
**Completed:** models/wind_forecast/train.py (rewritten, ~380 lines), models/solar_forecast/train.py (rewritten, ~380 lines)
**Notes:**
- wind_forecast/train.py: 6 wind-specific features added (lags 1-12, 4h rolling stats, ramp rate, capacity factor, high/low wind flags). Optuna 50 trials, MedianPruner, MAE objective. MAPE target <8%. Feature importances logged as JSON. Unit test mode with synthetic sine-wave wind data. Alias = "production".
- solar_forecast/train.py: 7 solar-specific features (lags, capacity factor, daylight flag, sun angle proxy, season sin/cos, cloud proxy). Night interval exclusion from training to prevent trivial-zero fitting. Optuna 50 trials, MAE objective. MAPE target <10%. Alias = "production".


## Sprint 7b — Agent Eval CI + Production Hardening — 2026-02-19
**Completed:** .github/workflows/ci.yml (agent-eval job, syntax-check lint), agent/evaluation/run_evaluation.py (--dry-run/--max-pairs/--output-file/mock mode), app/backend/main.py (rate limiting middleware, X-Request-ID header, /api/market-summary/latest extended fields), tests/test_backend.py (rate limit + market summary tests)
**Notes:**
- ci.yml: `agent-evaluation` job added with `needs: python-tests`; runs `--dry-run --max-pairs 5` for fast CI; uploads eval-report.json as artifact. `lint` job extended with `find agent/models -name "*.py" -exec python -m py_compile {} \;` syntax checks.
- run_evaluation.py: `--dry-run` wraps all `mlflow.*` calls; `--max-pairs N` slices `ALL_EVAL_PAIRS[:N]`; `--output-file PATH` writes CI-format JSON with `pass_rate`, `total_pairs`, `passed`, `failed`, `scores_by_dimension`. Mock LLM judge (score=0.8) activated when `ANTHROPIC_API_KEY` starts with `sk-ant-dummy` or `sk-ant-test`.
- main.py: In-process per-IP rate limiter middleware added before logging middleware using `defaultdict(list)` timestamp store; configurable via `RATE_LIMIT_REQUESTS` and `RATE_LIMIT_WINDOW_SECONDS` env vars. X-Request-ID response header was already present. `MarketSummaryRecord` extended with `summary_id`, `summary_text`, `highest_price_region`, `lowest_price_region`, `avg_nem_price` optional fields; mock data updated with NSW1 highest, TAS1 lowest, avg_nem_price=$86.20.
- test_backend.py: `TestRateLimiting` class added with `test_rate_limit_not_triggered_by_single_request` and `test_market_summary_endpoint` covering the new endpoint fields.

## Sprint 7a -- Wind/Solar Evaluate Depth -- 2026-02-19
**Completed:** models/wind_forecast/evaluate.py (rewritten ~400 lines), models/solar_forecast/evaluate.py (rewritten ~450 lines), tests/test_models.py (4 new evaluate tests added in TestRenewableEvaluate class)
**Notes:**
- `wind_forecast/evaluate.py`: Full rewrite from 151 to ~400 lines. Reconstructs the exact Sprint 6 feature set (wind_lag_1-12, wind_roll_mean/std_4h, wind_ramp_rate, wind_capacity_factor, is_high/low_wind, hour/dow/month sin+cos). Loads 'production' alias from `models:/energy_copilot.ml.wind_forecast_{region}@production`. Computes: MAE, RMSE, MAPE, capacity-weighted bias (bias/installed_capacity), ramp accuracy (% matching up/down/flat within 50 MW deadband), high-wind MAPE (speed > 10 m/s), low-wind MAPE (speed < 3 m/s). Writes to `gold.wind_forecast_evaluation` Delta table with passed_dod BOOLEAN. MLflow metrics logged as `wind_{region}_{metric}`. Promotion logic: if ALL regions MAPE < 8% sets 'champion' alias. Rich console table with NEM-wide capacity-weighted summary row. Unit-test mode (spark=None) uses synthetic sine+noise wind data; persistence baseline replaces model. CLI: `--region / --all-regions / --promote / --unit-test`.
- `solar_forecast/evaluate.py`: Full rewrite from 189 to ~450 lines. Matches train.py night exclusion (NIGHT_HOURS = hours 0-5 + 20-23). Reconstructs: solar_lag_1-12, solar_roll_mean/std_4h, solar_capacity_factor, is_daylight_hour, sun_angle_proxy, season_sin/cos, cloud_proxy, hour/dow sin+cos. Loads 'production' alias from `models:/energy_copilot.ml.solar_forecast_{region}@production`. Computes: daytime MAPE (primary, target < 10%), clear-sky MAE (cloud_proxy < 0.2), zero-compliance (% night preds correctly clamped to 0), overall MAE/RMSE, seasonal MAPE (DJF/MAM/JJA/SON). Writes to `gold.solar_forecast_evaluation` with full schema including all seasonal columns. MLflow metrics as `solar_{region}_{metric}`. Promotion on daytime MAPE < 10%. Two-section console output: core metrics + seasonal breakdown. Unit-test mode uses Gaussian daylight bell-curve data (zero at night). CLI: `--region / --all-regions / --promote / --unit-test`.
- `tests/test_models.py`: Added `TestRenewableEvaluate` class with 4 tests: `test_wind_evaluate_unit_mode` (asserts run_evaluation(spark=None) returns valid region dicts for NSW1+VIC1), `test_solar_evaluate_unit_mode` (asserts daytime_mape_pct/zero_compliance_pct/passed_dod for QLD1+SA1), `test_ramp_accuracy_computation` (verifies compute_ramp_accuracy gives 100% for perfect forecast and [0,100] for zeros baseline), `test_zero_compliance` (verifies 70% for 7/10 near-zero preds, 100% for all-zero preds, NaN for daytime-only rows). All tests verified passing in-process without Spark, MLflow, or LightGBM.


## Sprint 7c - Frontend Polish + CHANGELOG + Demand Evaluate - 2026-02-19
**Completed:** models/demand_forecast/evaluate.py (rewritten ~380 lines), app/frontend/src/pages/LiveMarket.tsx (region selector + localStorage, spike indicator, FUEL_COLORS, empty state), app/frontend/src/pages/Forecasts.tsx (localStorage region, confidence band, accuracy badge), CHANGELOG.md (full sprint history)
**Notes:** evaluate.py adds reconstruct_features() with all 22 demand features (12 lags, 2 rolling stats, 6 cyclical encodings, 3 calendar flags, 5 temperature cols), peak/off-peak MAPE, per-season MAPE (DJF/MAM/JJA/SON), major miss rate (>500 MW), champion promotion, unit-test mode with synthetic 5000 MW base + daily sine + weekday boost. LiveMarket.tsx adds SpikeBadge (amber >$300, red >$5000), EmptyStateCard with AlertCircle, FUEL_COLORS constant at top, localStorage persistence for region. Forecasts.tsx adds localStorage region state, AccuracyBadge (green/amber/red by MAPE threshold), MAE confidence band as translucent grey Area behind forecast line.

## Sprint 8b — Forecast + Market Summary Pipeline — 2026-02-19
**Completed:** pipelines/05_forecast_pipeline.py (rewritten ~350 lines, production inference with version tracking, spike probability, confidence scores, stale-mode fallback), pipelines/06_market_summary.py (rewritten ~280 lines, Claude Sonnet 4.5 narrative generation with quality checks, retry logic, template fallback)
**Notes:** Forecast pipeline loads 20 regional models from MLflow @production, clamps predictions to physical bounds, writes MERGE upsert to gold.nem_forecasts_realtime. Market summary pipeline assembles structured context from 5 Gold tables, generates 400-600 word narrative via Claude Sonnet 4.5, validates quality before writing to gold.daily_market_summary.

## Sprint 8a -- Price + Anomaly Evaluate Depth -- 2026-02-19
**Completed:** models/price_forecast/evaluate.py (rewritten ~1057 lines), models/anomaly_detection/evaluate.py (rewritten ~971 lines)
**Notes:** Price evaluation adds spike recall (% of actual spikes >$300/MWh correctly predicted), per-horizon MAE (one row per forecast_horizon in 1,4,8,12,24,48), volatility-regime breakdown (MAE per low/medium/high regime), AEMO pre-dispatch baseline comparison from silver.dispatch_prices P5MIN column, DoD thresholds (MAE <$15/MWh AND spike MAE <$200/MWh AND MAPE <12%), champion promotion when all 5 NEM regions pass. Feature reconstruction mirrors feature_engineering.py exactly: hour/dow/month sin+cos, is_peak_period (07-09 and 17-20 AEST), is_business_day, days_to_quarter_end, price lags 1-48, rolling mean/std (30min/1h/4h/24h), price_std_5day volatility regime one-hot, spike_count_24h/7d, raise6sec_price_lag1/lag6, nem_ic_stress_mw. MLflow metrics named price_{region}_{metric}. Anomaly evaluation adds per-event-type precision/recall/F1 (spike/negative/separation), ROC-AUC for binary anomaly score, false positive rate on confirmed-normal intervals (DoD: <5%), alert storm rate (% of intervals in consecutive anomaly runs >3). Feature reconstruction matches train.py engineer_features() exactly: hour_of_day, day_of_week (Mon=0), LabelEncoder region_id_encoded, price_roll_mean_12/std_12, demand_roll_mean_12/std_12. Ground truth: gold.known_market_events primary, rule-derived fallback. MLflow metrics named anomaly_{region}_{metric}. Both scripts support unit-test mode (spark=None) with synthetic data.

## Sprint 8c — System Monitoring Frontend — 2026-02-19
**Completed:** app/backend/main.py (/api/system/health endpoint, ModelHealthRecord + SystemHealthResponse Pydantic models), app/frontend/src/api/client.ts (SystemHealthResponse + ModelHealthRecord interfaces + getSystemHealth method), app/frontend/src/pages/Monitoring.tsx (new page ~300 lines: infra health cards, model registry grid, auto-refresh, loading skeletons), app/frontend/src/App.tsx (Monitoring route + nav link with Activity icon)
**Notes:** Monitoring page shows 4 infra health cards (Databricks, Lakebase, data freshness, pipeline last-run) and a region×model-type grid for all 21 ML models. Auto-refreshes every 30s. Stopped a stuck subagent that was using base64-encoding to work around JSX template-literal escaping — rewrote Monitoring.tsx directly.

## Sprint 9a — README + Test Coverage — 2026-02-19
**Completed:** README.md (full rewrite ~300 lines: architecture ASCII diagram, quick start, project structure, ML model table, API table, sprint checklist), tests/test_backend.py (TestSprintEightEndpoints: 4 new tests for /api/system/health and extended /api/market-summary/latest validation)
**Notes:** README now reflects the full production architecture. System health endpoint verified to return correct model count (21) and per-model structure.

## Sprint 9b — Agent Tools Expansion + Eval Dataset — 2026-02-19
**Completed:** agent/tools/analysis_tools.py (3 new tools: get_anomaly_events, get_model_health, get_forecast_accuracy — all with mock fallback), agent/tools/__init__.py (ALL_TOOLS updated), agent/evaluation/eval_dataset.py (10 new Q&A pairs: anomaly events, model health, forecast accuracy, multi-tool chains, out-of-scope declines)
**Notes:** get_anomaly_events queries gold.anomaly_detection_results with region/time/type filters. get_model_health queries MLflow UC registry for all 21 models. get_forecast_accuracy reads evaluation Delta tables. All tools degrade to mock data when Databricks unavailable.

## Sprint 9c — Model Serving + Batch Scoring — 2026-02-19
**Completed:** databricks.yml (model_serving_endpoints for price_forecast + anomaly_detection, auto-capture logging to ml schema, RBAC permissions), notebooks/batch_scoring.py (Databricks notebook ~250 lines: load 21 models from MLflow, batch inference for all regions+horizons, physical clamping, spike probability, write to gold.nem_forecasts_batch), databricks.yml job_05 updated with batch_scoring_notebook task dependency
**Notes:** Model serving endpoints use scale_to_zero to minimize cost when idle. Auto-capture logs all serving requests/responses to Delta for drift monitoring. Batch notebook separates the heavy model-loading step from the real-time inference pipeline (05_forecast_pipeline.py handles near-real-time 5-min intervals; batch notebook handles daily full-history scoring).

## Sprint 10a — API Auth + OpenAPI Docs — 2026-02-19
**Completed:** app/backend/main.py (FastAPI OpenAPI metadata, API key auth middleware via X-API-Key header, /api/version endpoint, Depends(verify_api_key) on all /api/* routes), app/backend/.env.example (ENERGY_COPILOT_API_KEY + rate limit vars), tests/test_backend.py (TestApiKeyAuth: 3 tests), CHANGELOG.md (Sprint 10 section)
**Notes:** API auth is opt-in — set ENERGY_COPILOT_API_KEY env var to enable; unset (default) disables auth for dev/mock mode. All existing tests continue to pass since ENERGY_COPILOT_API_KEY is not set in test env.

## Sprint 10b — DLT Expectations + Data Quality Pipeline — 2026-02-19
**Completed:** pipelines/01_nemweb_ingest.py (@dlt.expect/@dlt.expect_or_drop/@dlt.expect_all_or_drop on all Bronze/Silver/Gold tables, comment= annotations), pipelines/08_data_quality_report.py (new ~280 lines: DLT event log metrics, table freshness, row count trends, null rate scan, writes to gold.data_quality_daily_report, alert stub), databricks.yml (job_08_data_quality_report scheduled 06:00 AEST)
**Notes:** DLT expectations use expect_or_drop for Silver/Gold (hard failures drop bad rows) and expect for Bronze (warnings preserve raw data for debugging). Data quality pipeline queries DLT event log via event_log() TVF; falls back gracefully when event log unavailable. Overall status (green/amber/red) drives alert routing.

## Sprint 10c — Dark Mode + Mobile Responsive — 2026-02-19
**Completed:** app/frontend/src/hooks/useDarkMode.ts (new hook: localStorage + prefers-color-scheme, applies .dark class to <html>), app/frontend/tailwind.config.js (darkMode: 'class'), app/frontend/src/App.tsx (TopBar dark mode toggle with Moon/Sun icon, Sidebar + container dark classes), app/frontend/src/pages/Home.tsx (dark mode class additions), app/frontend/src/pages/Monitoring.tsx (dark mode class additions), app/frontend/src/index.css (dark mode base styles)
**Notes:** Dark mode uses Tailwind's class strategy (.dark on documentElement) for maximum control. Preference persisted to localStorage. System preference used as initial fallback. All 7 pages have dark mode support added to page-level containers; chart components (Recharts) are left with their existing colors as they render on white/transparent backgrounds.

## Sprint 11b — Conversation Session Persistence — 2026-02-19
**Completed:** app/backend/main.py (5 new session endpoints: list/get/create/rate/delete, ChatRequest.session_id, mock session storage dict), app/frontend/src/api/client.ts (CopilotSession + SessionMessage types, 5 session API methods), app/frontend/src/pages/Copilot.tsx (Sessions tab in sidebar: session history list, New Session button, session selection loads history), tests/test_backend.py (TestSessionEndpoints: 4 session tests)
**Notes:** Sessions stored in Lakebase copilot_sessions table in production; in mock mode uses in-process _MOCK_SESSIONS dict. session_id optional on ChatRequest — when provided, messages are appended to the session record. Star ratings (1-5) persist to Lakebase for agent quality tracking.

## Sprint 11a — Historical Backfill Pipeline — 2026-02-19
**Completed:** pipelines/00_historical_backfill.py (new ~300 lines: month-by-month NEMWEB archive download, ThreadPoolExecutor parallelism, BackfillTracker for resume support, Bronze Delta write with dedup, dry-run mode, unit test mode, CLI), pipelines/nemweb_downloader.py (NEMWEB_FILE_TYPES registry, checksum verification, DownloadMetrics dataclass, URL health check)
**Notes:** Backfill pipeline iterates month-by-month over NEMWEB MMSDM archive URLs. ThreadPoolExecutor downloads 4 months in parallel (configurable). BackfillTracker skips already-completed files, making re-runs safe. Unit test mode generates synthetic CSV to validate pipeline without internet access.

## Sprint 11c — Multi-region Comparison + CSV Export — 2026-02-19
**Completed:** app/backend/main.py (/api/prices/compare endpoint: pivoted multi-region price series, configurable interval aggregation, mock data with per-region sine offsets), app/frontend/src/api/client.ts (RegionComparisonPoint type, getPricesCompare method, exportToCSV utility), app/frontend/src/pages/Forecasts.tsx (Compare Regions tab: 5-region LineChart, time range + interval pickers, region toggles, CSV download), app/frontend/src/pages/LiveMarket.tsx (Export CSV button on price chart)
**Notes:** /api/prices/compare returns a pivoted response (one row per timestamp, one column per region) to minimize frontend aggregation. exportToCSV handles null values and comma-escaping for clean CSV output. Region colours are consistent across all charts: NSW1=blue, QLD1=amber, VIC1=purple, SA1=red, TAS1=green.

## Sprint 12a — Constraint & FCAS Analysis — 2026-02-19
**Completed:** app/backend/main.py (GET /api/constraints: binding constraint records with marginalvalue/violationdegree, GET /api/fcas: 8-service FCAS prices with clearings), app/frontend/src/api/client.ts (ConstraintRecord + FcasRecord types, getConstraints/getFcas methods), app/frontend/src/pages/MarketDepth.tsx (new page: Binding Constraints tab with horizontal bar chart + table, FCAS Markets tab with price + cleared MW charts), app/frontend/src/App.tsx (MarketDepth route + nav item)
**Notes:** Constraint analysis shows top binding constraints by marginal value (cost of constraint in $/MW). FCAS tab provides breakdown of all 8 ancillary services (Raise/Lower × 6sec/60sec/5min/Reg). Both tabs auto-refresh on region/time range change.

## Sprint 12b — Alert Notification Dispatch — 2026-02-19
**Completed:** app/backend/main.py (GET /api/alerts/history: trigger event log, GET /api/alerts/stats: alert summary stats, POST /api/alerts/test-notification: webhook dispatch test), app/frontend/src/api/client.ts (AlertTriggerEvent + AlertStats types, 3 new alert API methods), app/frontend/src/pages/Alerts.tsx (stats bar, Alert History tab with region filter, Test Notification inline form), tests/test_backend.py (TestAlertEndpoints: 3 tests)
**Notes:** Notification dispatch supports Slack webhook (POST JSON), email (simulated), and generic webhook. In mock mode all notifications return success without network calls. Alert history queryable by region and time range. Stats endpoint aggregates from both active alerts and trigger event log.

## Sprint 12c — Forecast Confidence Intervals — 2026-02-19
**Completed:** app/backend/main.py (ForecastRecord updated with price_p10/p90/demand_p10/p90/forecast_confidence optional fields, mock CI generation with horizon-scaled width, GET /api/forecasts/summary endpoint), app/frontend/src/api/client.ts (ForecastPoint CI fields, ForecastSummary interface, getForecastSummary method), app/frontend/src/pages/Forecasts.tsx (ComposedChart with CI band, Model Performance MAPE table, confidence score bar, show/hide CI toggle), tests/test_backend.py (TestForecastEndpoints: 3 tests), CHANGELOG.md (Sprint 12c section)
**Notes:** CI band width scales with sqrt(horizon_hours/24) — wider intervals at longer horizons. Confidence score decays linearly from 1.0 at 0h to minimum 0.4. MAPE values from gold.forecast_evaluation table in production; static mock values in mock mode.

## Sprint 13b — Price Spike & Volatility Analysis — 2026-02-19
**Completed:** app/backend/main.py (PriceSpikeEvent/VolatilityStats/SpikeAnalysisSummary Pydantic models, GET /api/prices/spikes with region/hours_back/spike_type filters, GET /api/prices/volatility returning per-region volatility stats with CPT utilisation), app/frontend/src/api/client.ts (PriceSpikeEvent/VolatilityStats/SpikeAnalysisSummary interfaces, getPriceSpikes/getVolatilityStats methods), app/frontend/src/pages/PriceAnalysis.tsx (new ~450-line page: volatility cards, CPT progress bar, spike events table with type filters, regional volatility BarChart), app/frontend/src/App.tsx (price-analysis route + nav item), tests/test_backend.py (TestPriceSpikeEndpoints: 3 tests)
**Notes:** CPT (Cumulative Price Threshold) = $1,359,100 over 7-day rolling window. When reached, AEMO can suspend the spot market and invoke administered pricing. SA1 modelled as most volatile (high wind, gas peakers). TAS1 least volatile (predominantly hydro). Spike types: high (>$300), voll (>$15000), negative (<$0).

## Sprint 13c — Interconnector Flows & NEM Settlement — 2026-02-19
**Completed:** app/backend/main.py (InterconnectorRecord/InterconnectorSummary/SettlementRecord Pydantic models, GET /api/interconnectors with 5 NEM interconnector mock flows + congestion detection, GET /api/settlement/summary with per-region FCAS+spot prices), app/frontend/src/api/client.ts (InterconnectorRecord/InterconnectorSummary/SettlementRecord interfaces, getInterconnectorsSummary/getSettlementSummary methods), app/frontend/src/pages/Interconnectors.tsx (new ~400-line page: SVG NEM topology diagram with flow direction + congestion labels, interconnector detail table with utilisation %, settlement summary table with FCAS prices), app/frontend/src/App.tsx (interconnectors route + nav item), tests/test_backend.py (TestInterconnectorEndpoints: 3 tests)
**Notes:** NEM interconnectors: NSW1-QLD1 (1078MW limit), VIC1-NSW1 (1600MW), VIC1-SA1 (500MW), V-SA Heywood (650MW), T-V-MNSP1 Basslink (478MW HVDC). Congestion flag when flow ≥ 95% of limit. Settlement records include FCAS ancillary service prices (raise/lower reg + 6sec) alongside spot price.

## Sprint 13a — Generator Fleet Dashboard — 2026-02-19
**Completed:** app/backend/main.py (GeneratorRecord/GenerationMixRecord/GenerationSummary Pydantic models, GET /api/generation/units with region/fuel_type/min_output filters, GET /api/generation/mix with weighted carbon intensity calculation), app/frontend/src/api/client.ts (GeneratorRecord/GenerationMixRecord/GenerationSummary interfaces, getGenerationUnits/getGenerationMix methods), app/frontend/src/pages/GeneratorFleet.tsx (new ~400-line page: summary cards, donut PieChart fuel mix, renewable vs fossil progress bar, sortable generator units table with status badges), app/frontend/src/App.tsx (generator-fleet route + nav item), tests/test_backend.py (TestGenerationEndpoints: 3 tests)
**Notes:** Carbon intensity computed as weighted average of per-fuel-type CO2 factors. Capacity factor = current_output / registered_capacity. Generator status badges: High (>80% CF), Running (>20% CF), Low otherwise. Fuel type colors consistent across charts.

## Sprint 14a — Market Notices & Dispatch Interval Analysis — 2026-02-19
**Completed:** app/backend/main.py (MarketNotice/DispatchInterval/DispatchSummary Pydantic models, GET /api/market/notices with severity/type filters, GET /api/dispatch/intervals with 5-min interval series + pre-dispatch comparison), app/frontend/src/api/client.ts (MarketNotice/DispatchInterval/DispatchSummary interfaces, getMarketNotices/getDispatchIntervals methods), app/frontend/src/pages/MarketNotices.tsx (new ~420-line page: LOR alert strip, notices table with type/severity filters, ComposedChart dispatch vs pre-dispatch with deviation tracking), app/frontend/src/App.tsx (market-notices route + nav item), tests/test_backend.py (TestMarketNoticesEndpoints: 3 tests)
**Notes:** Market notices include LOR1/2/3 (Lack of Reserve levels 1-3), constraint binding events, reclassification events, and price limit notifications. rrp_deviation = dispatch_rrp - predispatch_rrp measures forecast surprise. Surprise threshold set at ±$50/MWh. LOR3 is most critical (< 750 MW reserve in a region).

## Sprint 14c — Weather Correlation & Demand Response Analytics — 2026-02-19
**Completed:** app/backend/main.py (WeatherDemandPoint/DemandResponseEvent/DemandResponseSummary Pydantic models, GET /api/weather/demand with diurnal temperature patterns + demand correlation, GET /api/demand/response with RERT/interruptible load/EV fleet DR programs), app/frontend/src/api/client.ts (WeatherDemandPoint/DemandResponseEvent/DemandResponseSummary interfaces, getWeatherDemand/getDemandResponse methods), app/frontend/src/pages/WeatherDemand.tsx (new ~440-line page: weather summary cards, ComposedChart temperature+demand dual-axis, DR summary strip, DR events table with status badges), app/frontend/src/App.tsx (weather-demand route + nav item), tests/test_backend.py (TestWeatherDemandEndpoints: 3 tests)
**Notes:** Demand correlates with temperature deviation from 18°C comfort zone (both hot and cold increase demand). Diurnal temperature pattern peaks ~3pm, troughs ~5am. DR programs: RERT (emergency reserve), Interruptible Load (industrial), EV Fleet Response (aggregated EVs), Demand Aggregator. total_enrolled_mw ~500-1500MW across NEM.

## Sprint 14b — Battery & Storage Analytics — 2026-02-19
**Completed:** app/backend/main.py (BessUnit/BessDispatchInterval/BessFleetSummary Pydantic models, GET /api/bess/fleet with 8 real-world-inspired NEM BESS units, GET /api/bess/dispatch with charge/discharge cycle simulation), app/frontend/src/api/client.ts (BessUnit/BessDispatchInterval/BessFleetSummary interfaces, getBessFleet/getBessDispatch methods), app/frontend/src/pages/BessAnalytics.tsx (new ~450-line page: fleet summary cards, BESS units table with SOC bar + mode badges, dispatch history ComposedChart with MW bars + price line + SOC line), app/frontend/src/App.tsx (bess route + nav item), tests/test_backend.py (TestBessEndpoints: 3 tests)
**Notes:** BESS units modelled on real NEM assets: Hornsdale (SA1, 150MW/193.5MWh), Victorian Big Battery (VIC1, 300MW/450MWh), Waratah Super Battery (NSW1, 850MW/1680MWh), plus 5 others. Revenue = abs(mw) * rrp * (5/60) for discharging intervals. Charge cycles shown as negative MW (cost of charge). Round-trip efficiency 85-92%.

## Sprint 15a — Portfolio Trading Desk — 2026-02-19
**Completed:** app/backend/main.py (PortfolioAsset/HedgePosition/PortfolioSummary Pydantic models, GET /api/portfolio/summary with 7 generation assets + 3-4 hedge positions + region P&L, GET /api/portfolio/pnl_history with 7-day daily P&L series), app/frontend/src/api/client.ts (PortfolioAsset/HedgePosition/PortfolioSummary interfaces, getPortfolioSummary/getPortfolioPnlHistory methods), app/frontend/src/pages/TradingDesk.tsx (new ~450-line page: MtM P&L cards, P&L history BarChart with green/red bars, portfolio assets table, hedge positions table with ITM badges), app/frontend/src/App.tsx (trading-desk route + nav item), tests/test_backend.py (TestPortfolioEndpoints: 3 tests)
**Notes:** MtM P&L = (current_spot - contract_price) * contracted_volume / 1000. Assets include major NEM generators: Eraring (2880MW coal, NSW1), Liddell (1000MW coal), Torrens Island (400MW gas, SA1), Bango Wind (244MW, NSW1), Darlington Point Solar (275MW, NSW1), Hallet (99MW wind, SA1). Hedge types: cap (ceiling on price paid), swap (fixed-for-floating), floor (minimum price received), collar (cap + floor combined).

## Sprint 15c — Carbon & Sustainability Dashboard — 2026-02-19
**Completed:** app/backend/main.py (CarbonIntensityRecord/LgcMarketRecord/SustainabilityDashboard Pydantic models, GET /api/sustainability/dashboard with NEM decarbonisation stats + LGC market + regional intensities, GET /api/sustainability/intensity_history with hourly series), app/frontend/src/api/client.ts (CarbonIntensityRecord/LgcMarketRecord/SustainabilityDashboard interfaces, getSustainabilityDashboard/getCarbonIntensityHistory methods), app/frontend/src/pages/Sustainability.tsx (new ~450-line page: decarbonisation progress banner, summary cards, regional intensity BarChart, hourly trend AreaChart, LGC market cards), app/frontend/src/App.tsx (sustainability route + nav item), tests/test_backend.py (TestSustainabilityEndpoints: 3 tests)
**Notes:** NEM 2026 carbon intensity ~0.5 kg CO2/MWh (down 36% from 2005 baseline of 0.82). Regional variation: TAS1~0.05 (hydro), SA1~0.15 (high wind), VIC1~0.45, NSW1/QLD1~0.7 (coal dominated). LGCs (Large-scale Generation Certificates) are the tradeable instrument for Australia's Renewable Energy Target. 1 LGC = 1 MWh of eligible renewable generation.

## Sprint 15b — Merit Order & Dispatch Stack — 2026-02-19
**Completed:** app/backend/main.py (MeritOrderUnit/MeritOrderCurve/DispatchStackSummary Pydantic models, GET /api/merit/order with per-region merit order stack sorted by SRMC + marginal unit identification, GET /api/merit/stack for all 5 NEM regions), app/frontend/src/api/client.ts (MeritOrderUnit/MeritOrderCurve interfaces, getMeritOrder method), app/frontend/src/pages/MeritOrder.tsx (new ~450-line page: region tabs, SMC/marginal generator/demand cards, ComposedChart step-curve merit order with demand reference line, dispatch units table with on/off merit status), app/frontend/src/App.tsx (merit-order route + nav item), tests/test_backend.py (TestMeritOrderEndpoints: 3 tests)
**Notes:** SRMC (Short-Run Marginal Cost) by fuel: Hydro/Wind/Solar=$0-20, Brown Coal=$25-45, Black Coal=$35-55, Gas CCGT=$65-95, Gas OCGT=$150-350, Battery=$50-500 (strategic), Diesel=$400-800. Marginal generator is the unit whose cumulative capacity first exceeds current demand. System Marginal Cost = marginal generator SRMC = spot price in competitive equilibrium.

## Sprint 16b — Scenario / What-If Analysis — 2026-02-19
**Completed:** app/backend/main.py (ScenarioInput/ScenarioResult/ScenarioComparison Pydantic models, POST /api/scenario/run with linear sensitivity model, GET /api/scenario/presets with 6 pre-built scenarios), app/frontend/src/api/client.ts (ScenarioInput/ScenarioResult/ScenarioComparison interfaces, runScenario POST + getScenarioPresets methods), app/frontend/src/pages/ScenarioAnalysis.tsx (new ~480-line page: preset scenario cards, parameter sliders for 6 inputs, results panel with price/demand impact arrows, sensitivity table), app/frontend/src/App.tsx (scenario route + nav item), tests/test_backend.py (TestScenarioEndpoints: 3 tests)
**Notes:** Simplified price sensitivity model: temperature +$3.50/MWh per degree C above 30 degrees C (AC load), gas pass-through ~35%, wind shortfall +15% price impact, coal outage +$0.05/MWh per MW. Presets: Hot Summer Day, Cold Snap, Wind Drought, Gas Price Spike, Major Coal Outage, Perfect Green Day.

## Sprint 16c — Data Pipeline & Catalog Health Dashboard — 2026-02-19
**Completed:** app/backend/main.py (PipelineRunRecord/TableHealthRecord/DataQualityExpectation/DataCatalogDashboard Pydantic models, GET /api/catalog/dashboard with DLT pipeline runs + table health + DQ expectations, GET /api/catalog/pipeline_runs with filters), app/frontend/src/api/client.ts (PipelineRunRecord/TableHealthRecord/DataQualityExpectation/DataCatalogDashboard interfaces, getCatalogDashboard/getPipelineRuns methods), app/frontend/src/pages/DataCatalog.tsx (new ~450-line page: health summary cards, pipeline runs table, table health grid grouped by Bronze/Silver/Gold, DQ expectations table), app/frontend/src/App.tsx (data-catalog route + nav item), tests/test_backend.py (TestCatalogEndpoints: 3 tests)
**Notes:** 4 DLT pipelines tracked: nemweb_bronze (5-min), silver_transform (5-min), gold_aggregation (30-min), ml_feature (1-hr). 8 Unity Catalog tables across bronze/silver/gold schemas. 10 DQ expectations covering not_null, unique, in_range, matches_regex. Freshness thresholds: fresh < 10min, stale 10-30min, critical > 30min.

## Sprint 17a — Load Duration Curve & Statistical Analysis — 2026-02-19
**Completed:** app/backend/main.py (DurationCurvePoint/StatisticalSummary/SeasonalPattern Pydantic models, GET /api/stats/duration_curve with 101-point percentile series, GET /api/stats/summary with box-plot statistics + demand-price correlation, GET /api/stats/seasonal with 12-month patterns), app/frontend/src/api/client.ts (DurationCurvePoint/StatisticalSummary/SeasonalPattern interfaces, getDurationCurve/getStatsSummary/getSeasonalPattern methods), app/frontend/src/pages/LoadDuration.tsx (new ~480-line page: stats cards, load + price duration AreaCharts with percentile reference lines, seasonal BarChart, statistical summary table), app/frontend/src/App.tsx (load-duration route + nav item), tests/test_backend.py (TestLoadDurationEndpoints: 3 tests)
**Notes:** Load duration curve shows what fraction of time demand exceeds a given level (monotonically decreasing). NSW1 demand range P0=2000MW to P100=14800MW; price range P0=-$50 to P100=$15500 (VOLL). Demand-price correlation ~0.40 (positive: high demand = high prices). Seasonal patterns: Jan/Feb peak (summer AC), Jun/Jul moderate (heating), Apr/Oct trough (shoulder season).

## Sprint 16a — MLflow Experiment & Model Management Dashboard — 2026-02-19
**Completed:** app/backend/main.py (MlflowRun/FeatureImportance/ModelDriftRecord/MlDashboard Pydantic models, GET /api/ml/dashboard with 8-10 mock runs + feature importance + drift summary, GET /api/ml/runs with model_type/region/limit filters), app/frontend/src/api/client.ts (MlflowRun/FeatureImportance/ModelDriftRecord/MlDashboardData interfaces, getMlDashboard/getMlRuns methods), app/frontend/src/pages/MlDashboard.tsx (new ~450-line page: summary cards, model drift table, feature importance horizontal BarChart, training runs table with status badges), app/frontend/src/App.tsx (ml-dashboard route + nav item), tests/test_backend.py (TestMlDashboardEndpoints: 3 tests)
**Notes:** MLflow experiments map to Unity Catalog model registry (energy_copilot.models.<name>). Feature importance from LightGBM gain metric. Drift detected when production MAE / training MAE > 1.3 (warning) or > 1.5 (critical). 8 features tracked for price forecast: hour_of_day, day_of_week, temp_c, nem_demand, gas_price, prev_rrp_1h, wind_mw, solar_mw.

## Sprint 17c — Historical Trend & Long-Run Analysis — 2026-02-19
**Completed:** app/backend/main.py (AnnualSummary/YearOverYearChange/LongRunTrendSummary Pydantic models, GET /api/trends/annual with 2015-2025 data + CAGR calculations, GET /api/trends/yoy with year-over-year metric comparisons), app/frontend/src/api/client.ts (AnnualSummary/YearOverYearChange/LongRunTrendSummary interfaces, getAnnualTrends/getYoyChanges methods), app/frontend/src/pages/HistoricalTrends.tsx (new ~480-line page: CAGR + renewable growth + carbon reduction summary cards, annual price AreaChart with CPI-adjusted line, renewable+carbon ComposedChart, YoY comparison table, full annual data table with 2022 gas crisis highlight), app/frontend/src/App.tsx (trends route + nav item), tests/test_backend.py (TestTrendsEndpoints: 3 tests)
**Notes:** Key NEM price milestones: 2017 high (~$100/MWh gas supply constraints), 2020 low (~$45 COVID + renewables), 2022 high (~$180 global gas crisis), 2024 moderate (~$75 renewables normalizing). Renewable penetration grew from ~15% in 2015 to ~40% in 2025. Carbon intensity declined ~39% over the decade. CPI adjustment uses 2.5%/yr inflation assumption.

## Sprint 17b — Frequency & System Strength Analytics — 2026-02-19
**Completed:** app/backend/main.py (FrequencyRecord/InertiaRecord/FrequencyEventRecord/FrequencyDashboard Pydantic models, GET /api/frequency/dashboard with 5-sec frequency series + inertia by region + event log, GET /api/frequency/history with per-minute series), app/frontend/src/api/client.ts (FrequencyRecord/InertiaRecord/FrequencyEventRecord/FrequencyDashboard interfaces, getFrequencyDashboard/getFrequencyHistory methods), app/frontend/src/pages/FrequencyAnalytics.tsx (new ~460-line page: live frequency Hz display with band coloring, 5-min trend LineChart with normal band reference lines, inertia table, frequency events log), app/frontend/src/App.tsx (frequency route + nav item), tests/test_backend.py (TestFrequencyEndpoints: 3 tests)
**Notes:** NEM normal frequency band: 49.85-50.15 Hz. Warning band: 49.5-50.5 Hz. Emergency: <49.5 or >50.5 Hz. ROCOF (Rate of Change of Frequency) critical for protecting equipment — AEMO requires < ±1 Hz/s under credible contingency. SA1 lowest inertia (inverter-based renewables dominant). UFLS (Under Frequency Load Shedding) activates below 49.0 Hz.

## Sprint 18a — ASX Energy Futures & Hedge Market — 2026-02-19
**Completed:** app/backend/main.py (FuturesContract/ForwardCurvePoint/HedgeEffectivenessRecord/FuturesDashboard Pydantic models, GET /api/futures/dashboard with CAL+quarterly contracts + forward curve + hedge analytics, GET /api/futures/contracts with region/type filters), app/frontend/src/api/client.ts (FuturesContract/ForwardCurvePoint/HedgeEffectivenessRecord/FuturesDashboard interfaces, getFuturesDashboard/getFuturesContracts methods), app/frontend/src/pages/EnergyFutures.tsx (new ~470-line page: market summary strip, forward curve LineChart, futures contracts table grouped by CAL/quarterly, hedge effectiveness cards), app/frontend/src/App.tsx (futures route + nav item), tests/test_backend.py (TestFuturesEndpoints: 3 tests)
**Notes:** ASX Energy futures settle against average NEM spot price for the contract period. CAL contracts cover full calendar year. Quarterly contracts (Q1-Q4) allow more granular hedging. Peak contracts hedge peak periods (7am-10pm weekdays). Forward curve shows declining prices 2025→2028 as renewables expand (contango → backwardation transition expected by 2027).

## Sprint 18c — Market Participant Registry & Credit Analytics — 2026-02-19
**Completed:** app/backend/main.py (MarketParticipant/ParticipantAsset/ParticipantRegistry Pydantic models, GET /api/registry/participants with 12 NEM participants + HHI concentration metrics, GET /api/registry/assets with participant/region/fuel filters), app/frontend/src/api/client.ts (MarketParticipant/ParticipantAsset/ParticipantRegistry interfaces, getParticipantRegistry/getParticipantAssets methods), app/frontend/src/pages/ParticipantRegistry.tsx (new ~450-line page: market concentration cards, donut PieChart market share, participants table with compliance + credit badges, asset detail expansion panel), app/frontend/src/App.tsx (registry route + nav item), tests/test_backend.py (TestRegistryEndpoints: 3 tests)
**Notes:** NEM major participants: AGL (~22% share), Origin (~18%), Snowy Hydro (4100MW hydro), CS Energy (QLD govt), Energy Australia, ERM Power, Tilt Renewables, Neoen (large BESS), Alinta, Glencore (coal NSW). HHI index ~1200 (moderately concentrated but below 2500 concentrated threshold). Credit limits set by AEMO based on 7-day maximum exposure.

## Sprint 18b — Outage Schedule & PASA Adequacy Assessment — 2026-02-19
**Completed:** app/backend/main.py (OutageRecord/PasaRecord/PasaDashboard Pydantic models, GET /api/outages/dashboard with active/upcoming/returned outages + 7-day PASA outlook, GET /api/outages/list with region/type/status filters), app/frontend/src/api/client.ts (OutageRecord/PasaRecord/PasaDashboard interfaces, getOutageDashboard/getOutageList methods), app/frontend/src/pages/OutageSchedule.tsx (new ~460-line page: capacity impact cards, 7-day PASA outlook timeline, active outages table, upcoming outages section), app/frontend/src/App.tsx (outages route + nav item), tests/test_backend.py (TestOutageEndpoints: 3 tests)
**Notes:** PASA (Projected Assessment of System Adequacy) is AEMO's reliability outlook. Reserve status thresholds: SURPLUS (>25%), ADEQUATE (>LOR1 requirement ~750MW), LOR1 (<750MW reserve), LOR2 (<450MW), LOR3 (<0MW). Mock forced outages: Yallourn W unit 3 (VIC1, 380MW), Callide C unit 4 (QLD1, 460MW). Planned: Eraring unit 2 (NSW1, 720MW annual maintenance).

## Sprint 19a — VPP & Distributed Energy Resources Dashboard — 2026-02-19
**Completed:** app/backend/main.py (VppUnit/DerSummary/DerDashboard Pydantic models, GET /api/der/dashboard with 6 VPPs + 5-region DER summary + 24-hour solar forecast, GET /api/der/vpp with region/mode filters), app/frontend/src/api/client.ts (VppUnit/DerSummary/DerDashboard interfaces, getDerDashboard/getVppFleet methods), app/frontend/src/pages/DerDashboard.tsx (new ~460-line page: NEM DER summary cards, 24-hour solar AreaChart showing duck curve, VPP fleet table, regional DER stacked BarChart), app/frontend/src/App.tsx (der route + nav item), tests/test_backend.py (TestDerEndpoints: 3 tests)
**Notes:** NEM 2026 DER landscape: ~22 GW rooftop solar (3.5M households), ~3.5 GWh BTM batteries, ~250,000 smart-charging EVs. VPPs: AGL (SA1, 50MW), Origin (NSW1, 80MW), Tesla Powerwall (SA1, 40MW), EnergyAustralia (VIC1, 60MW), Amber Electric (multi-region, 30MW). Duck curve shows midday solar depression followed by evening ramp. Net demand reduction during peak solar: 8,000-12,000 MW across NEM.

## Sprint 19c — Admin Settings & API Configuration Panel — 2026-02-19
**Completed:** app/backend/main.py (UserPreferences/ApiKeyInfo/DataSourceConfig/SystemConfig Pydantic models, GET/PUT /api/admin/preferences, GET /api/admin/api_keys, GET /api/admin/data_sources, GET /api/admin/system_config), app/frontend/src/api/client.ts (UserPreferences/ApiKeyInfo/DataSourceConfig/SystemConfig interfaces, getAdminPreferences/updateAdminPreferences/getApiKeys/getDataSources/getSystemConfig methods), app/frontend/src/pages/Settings.tsx (new ~500-line page: 4-tab layout — User Preferences (form with save), API Keys (masked table + revoke), Data Sources (status cards + sync), System Info (read-only config)), app/frontend/src/App.tsx (settings route + nav item), tests/test_backend.py (TestAdminEndpoints: 3 tests)
**Notes:** Settings page provides operational control panel for platform administrators. API keys use prefix masking (show first 8 chars, mask rest). Data sources include NEMWEB, MLflow, gas bulletin board, Lakebase, ASX futures, BOM weather. PUT /api/admin/preferences is stateless in mock mode (echoes back input). SystemConfig exposes environment metadata, uptime, cache performance.

## Sprint 19b — Gas Market & Pipeline Analytics — 2026-02-19
**Completed:** app/backend/main.py (GasPipelineFlow/GasHubPrice/LngExportRecord/GasMarketDashboard Pydantic models, GET /api/gas/dashboard with 3 hub prices + 5 pipeline flows + 5 LNG terminals, GET /api/gas/pipeline_flows with utilisation filter), app/frontend/src/api/client.ts (GasPipelineFlow/GasHubPrice/LngExportRecord/GasMarketDashboard interfaces, getGasDashboard/getGasPipelineFlows methods), app/frontend/src/pages/GasMarket.tsx (new ~450-line page: hub price cards, pipeline flows table with utilisation bars, hub price BarChart, LNG terminals table), app/frontend/src/App.tsx (gas route + nav item), tests/test_backend.py (TestGasEndpoints: 3 tests)
**Notes:** Australian east coast gas hubs: Wallumbilla (QLD, largest trading hub), Moomba (SA, major production centre), Longford (VIC, Bass Strait supply point). Gas-to-power is key NEM fuel input: ~450-800 TJ/day. LNG export terminals: 3 in QLD (QCLNG, APLNG, GLNG) + 2 in WA (Darwin LNG, NW Shelf). Domestic Reservation Obligation (WA) requires 15% of LNG production reserved for domestic market.

## Sprint 20a — Retail Market Analytics — 2026-02-19
**Completed:** app/backend/main.py (RetailerMarketShare/DefaultOfferPrice/CustomerSwitchingRecord/RetailMarketDashboard Pydantic models, GET /api/retail/dashboard with 7 retailers + DMO/VDO prices + switching data, GET /api/retail/offers with state filter), app/frontend/src/api/client.ts (RetailerMarketShare/DefaultOfferPrice/CustomerSwitchingRecord/RetailMarketDashboard interfaces, getRetailDashboard/getRetailOffers methods), app/frontend/src/pages/RetailMarket.tsx (new ~460-line page: market overview cards, retailer PieChart, DMO/VDO prices table, customer switching BarChart), app/frontend/src/App.tsx (retail route + nav item), tests/test_backend.py (TestRetailEndpoints: 3 tests)
**Notes:** DMO (Default Market Offer) = Australian federal government's reference price cap, set annually by AER. VDO (Victorian Default Offer) = Victorian-specific equivalent set by ESC. SA has highest electricity prices in NEM (~$2,172/yr reference). ~18-22% of customers switch retailers annually, saving average $300-400/yr. ~30% of residential customers still on expensive standing offers.

## Sprint 20b — Transmission Loss Factor & Network Analytics — 2026-02-19
**Completed:** app/backend/main.py (LossFactorRecord/NetworkConstraintLimit/NetworkDashboard Pydantic models, GET /api/network/dashboard with 20-25 MLF records + 8-10 network elements, GET /api/network/loss_factors with region/category filters), app/frontend/src/api/client.ts (LossFactorRecord/NetworkConstraintLimit/NetworkDashboard interfaces, getNetworkDashboard/getLossFactors methods), app/frontend/src/pages/NetworkAnalytics.tsx (new ~460-line page: MLF summary cards, MLF distribution BarChart, loss factors table sorted by MLF, network element loading table), app/frontend/src/App.tsx (network route + nav item), tests/test_backend.py (TestNetworkEndpoints: 3 tests)
**Notes:** MLF (Marginal Loss Factor) measures the marginal transmission loss at a generator's connection point. MLF < 1.0 means revenue penalty (generator receives less than the spot price). Remote renewable zones in SA and QLD often have MLFs 0.85-0.95, significantly reducing effective revenue. MLFs are set annually by AEMO for each financial year. Combined LF = MLF x DLF.

## Sprint 20c — REZ & Infrastructure Investment Analytics (2026-02-19)
- Added RezProject, IspProject, CisContract, RezDashboard Pydantic models to main.py
- Created /api/rez/dashboard, /api/rez/projects, /api/rez/cis_contracts endpoints
- Created RezInfrastructure.tsx with REZ project table, ISP project table, CIS contracts table
- Added TypeScript interfaces and API methods to client.ts
- Added /rez route to App.tsx
- Added TestRezInfrastructureEndpoints tests to test_backend.py

## Sprint 21a — Renewable Curtailment & Integration Analytics (2026-02-19)
- Added CurtailmentEvent, MinimumOperationalDemandRecord, RenewableIntegrationLimit, CurtailmentDashboard Pydantic models
- Created /api/curtailment/dashboard and /api/curtailment/events endpoints
- Created CurtailmentAnalytics.tsx with curtailment events table, MOD records table, integration limits table
- Added TypeScript interfaces and API methods to client.ts
- Added /curtailment route to App.tsx
- Added TestCurtailmentEndpoints tests to test_backend.py

## Sprint 21b — Demand Side Participation & Load Curtailment Analytics (2026-02-19)
- Added DspParticipant, DspActivationEvent, LoadCurtailmentRecord, DspDashboard Pydantic models
- Created /api/dsp/dashboard and /api/dsp/participants endpoints
- Created DemandResponse.tsx with participants table, sector capacity chart, activation events table, curtailment table
- Added TypeScript interfaces and API methods to client.ts
- Added /dsp route to App.tsx
- Added TestDspEndpoints tests to test_backend.py

## Sprint 21c — Power System Security & Inertia Analytics (2026-02-19)
- Added PssInertiaRecord, SynchronousCondenserRecord, FcasDispatchRecord, PowerSystemSecurityDashboard Pydantic models
- Created /api/pss/dashboard and /api/pss/fcas endpoints
- Created SystemSecurity.tsx with inertia table, synchronous condenser table, FCAS dispatch table
- Added TypeScript interfaces and API methods to client.ts
- Added /security route to App.tsx
- Added TestPowerSystemSecurityEndpoints tests to test_backend.py

## Sprint 22a — Generator Bidding & Offer Stack Analytics (2026-02-19)
- Added OfferBand, GeneratorOfferRecord, RebidRecord, BidStackSummary Pydantic models
- Created /api/bids/stack endpoint (offer records, rebid log, fuel type breakdown)
- Created BiddingAnalytics.tsx with fuel type BarChart, offer records table, rebid log
- Added TypeScript interfaces and API methods to client.ts
- Added /bidding route to App.tsx
- Added TestBiddingEndpoints tests to test_backend.py

## Sprint 22b — NEM Market Events & Intervention Timeline (2026-02-19)
**Status**: COMPLETE

### What was built:
- `MarketEvent`, `MarketIntervention`, `PriceCapEvent`, `MarketEventsDashboard` Pydantic models
- Endpoints: GET /api/market-events/dashboard, /api/market-events/events, /api/market-events/interventions
- `NemEvents.tsx` (~500 lines) — event timeline table, intervention log, price cap events, KPI cards, region filter
- TypeScript interfaces in client.ts + API methods
- 4 tests in TestMarketEventsEndpoints
- Covers price cap events (APC/MPC), AEMO directions, LOR declarations, reclassified events

## Sprint 22c — FCAS Market & Ancillary Services Deep-Dive (2026-02-19)
**Status**: COMPLETE

### What was built:
- `FcasServicePrice`, `FcasProvider`, `FcasTrapRecord`, `FcasMarketDashboard` Pydantic models
- Endpoints: GET /api/fcas/market, /api/fcas/services, /api/fcas/providers
- `FcasMarket.tsx` (~520 lines) — 8-service price table, cost PieChart, provider table, constraint/trap records, regional requirements
- TypeScript interfaces in client.ts + API methods
- 4 tests in TestFcasMarketEndpoints
- Covers all 8 FCAS services (R6S/R60S/R5M/R5RE/L6S/L60S/L5M/L5RE), causer pays, enablement limits

## Sprint 23c — NEM Settlement & Prudential Management Analytics (2026-02-19)
**Status**: COMPLETE
- SettlementResidueRecord, PrudentialRecord, SettlementRun, TecAdjustment, SettlementDashboard models
- GET /api/settlement/dashboard, /residues, /prudential
- NemSettlement.tsx (~520 lines): settlement runs table, prudential status, residues, TEC adjustments
- 5 tests in TestSettlementEndpoints

## Sprint 23a — Battery Storage Arbitrage & Economics Analytics (2026-02-19)
**Status**: COMPLETE
- BatteryArbitrageSlot, BatteryUnit, ArbitrageOpportunity, BatteryEconomicsDashboard models
- GET /api/battery-economics/dashboard, /batteries, /schedule
- BatteryEconomics.tsx (~500 lines): dispatch schedule chart, fleet table, arbitrage opportunities, revenue PieChart
- 4 tests in TestBatteryEconomicsEndpoints

## Sprint 23b — Carbon Emissions Intensity & Net Zero Tracking (2026-02-19)
**Status**: COMPLETE
- RegionEmissionsRecord, FuelEmissionsFactor, EmissionsTrajectory, Scope2Calculator, CarbonDashboard models
- GET /api/carbon/dashboard, /regions, /trajectory
- CarbonAnalytics.tsx (~520 lines): trajectory ComposedChart, region intensity table, fuel factors, Scope 2 calculator
- 4 tests in TestCarbonAnalyticsEndpoints

## Sprint 24a — OTC Hedging & Contract Portfolio Analytics (2026-02-19)
**Status**: COMPLETE
- HedgeContract, HedgePortfolioSummary, HedgingDashboard models
- GET /api/hedging/dashboard, /contracts, /portfolio
- HedgingAnalytics.tsx (~530 lines): quarterly position BarChart, portfolio table, contract details with filters
- 4 tests in TestHedgingEndpoints

## Sprint 24b — Hydro Storage & Water Value Analytics (2026-02-19)
**Status**: COMPLETE
- ReservoirRecord, HydroInflowForecast, WaterValuePoint, HydroSchemeSummary, HydroDashboard models
- GET /api/hydro/dashboard, /reservoirs, /water-value
- HydroStorage.tsx (~510 lines): water value curve, scheme summary cards, reservoir table, inflow forecasts
- 4 tests in TestHydroStorageEndpoints

## Sprint 24c — Market Power & Concentration Analytics (2026-02-19)
**Status**: COMPLETE
- HhiRecord, PivotalSupplierRecord, MarketShareTrend, MarketPowerDashboard models
- GET /api/market-power/dashboard, /hhi, /pivotal
- MarketPower.tsx (~500 lines): HHI BarChart, share trend LineChart, pivotal supplier table, HHI details
- 4 tests in TestMarketPowerEndpoints

## Sprint 25a — PASA Availability & Generator Forced Outage Statistics (2026-02-19)
**Status**: COMPLETE
- PasaPeriod, ForcedOutageRecord, GeneratorReliabilityStats, PasaDashboard models
- GET /api/pasa/dashboard, /periods, /forced-outages
- PasaAnalytics.tsx (~510 lines): reserve margin BarChart, PASA periods table, forced outage log, reliability stats
- 4 tests in TestPasaEndpoints

## Sprint 25b — SRA Auction & Interconnector Firm Transfer Rights (2026-02-19)
**Status**: COMPLETE
- SraUnit, SraAuctionResult, InterconnectorRevenueSummary, SraDashboard models
- GET /api/sra/dashboard, /units, /auction-results
- SraAuction.tsx (~500 lines): auction results table, revenue BarChart, active SRA units, interconnector cards
- 4 tests in TestSraEndpoints

## Sprint 25c — Corporate PPA Market & Green Energy Procurement (2026-02-19)
**Status**: COMPLETE
- CorporatePpa, LgcMarket, BehindMeterAsset, PpaDashboard models
- GET /api/ppa/dashboard, /contracts, /lgc-market
- PpaMarket.tsx (~520 lines): tech mix PieChart, LGC trend AreaChart, PPA table, behind-meter assets
- 4 tests in TestPpaEndpoints

## Sprint 27b — LRMC & Investment Signal Analytics (2026-02-19)
**Status**: COMPLETE
- LcoeTechnology, InvestmentSignal, CapacityMechanismScenario, LrmcDashboard models
- GET /api/lrmc/dashboard, /technologies, /signals
- LrmcAnalytics.tsx (~520 lines): LCOE range chart, investment signal table, capacity scenarios, LCOE comparison
- 4 tests in TestLrmcEndpoints

## Sprint 26c — AEMO ISP Transmission Investment Tracker (2026-02-19)
**Status**: COMPLETE
- IspProjectMilestone, IspMajorProject, TnspCapexProgram, IspDashboard models
- GET /api/isp/dashboard, /projects, /tnsp-programs
- IspTracker.tsx (~540 lines): capex BarChart, projects table with milestone timeline, TNSP programs
- 4 tests in TestIspTrackerEndpoints

## Sprint 26b — Pre-dispatch Accuracy & 5-Minute Settlement Analytics (2026-02-19)
**Status**: COMPLETE
- PredispatchInterval, FiveMinuteSettlementSummary, DispatchAccuracyStats, DispatchDashboard models
- GET /api/dispatch/dashboard, /predispatch, /accuracy
- DispatchAccuracy.tsx (~500 lines): pre-dispatch vs actual ComposedChart, 5-min settlement table, accuracy stats
- 4 tests in TestDispatchAccuracyEndpoints

## Sprint 26a — NEM Rule Change & Regulatory Reform Tracker (2026-02-19)
**Status**: COMPLETE
- RuleChangeRequest, AerDetermination, RegulatoryCalendarEvent, RegulatoryDashboard models
- GET /api/regulatory/dashboard, /rule-changes, /calendar
- RegulatoryTracker.tsx (~510 lines): calendar timeline, rule changes table, AER determinations, pipeline BarChart
- 4 tests in TestRegulatoryEndpoints

## Sprint 27a — Small-Scale Solar & EV Fleet Analytics (2026-02-19)
**Status**: COMPLETE
- SolarGenerationRecord, EvFleetRecord, SolarEvDashboard models
- GET /api/solar-ev/dashboard, /solar, /ev-fleet
- SolarEvAnalytics.tsx (~520 lines): duck curve chart, growth projection, solar table, EV fleet table
- 4 tests in TestSolarEvEndpoints

## Sprint 27c — Network Constraint Equation & Binding Constraint Analytics (2026-02-19)
**Status**: COMPLETE
- ConstraintEquation, ConstraintSummaryByRegion, ConstraintViolationRecord, ConstraintDashboard models
- GET /api/constraints/dashboard, /equations, /violations
- NetworkConstraints.tsx (~510 lines): constraint cost chart, equations table with slack coloring, violations log
- 4 tests in TestNetworkConstraintEndpoints

## Sprint 28a — Price Setter & Marginal Generator Analytics (2026-02-19)
**Status**: COMPLETE
- PriceSetterRecord, PriceSetterFrequency, FuelTypePriceSetting, PriceSetterDashboard models
- GET /api/price-setter/dashboard, /records, /frequency
- PriceSetterAnalytics.tsx (~510 lines): timeline bar chart, fuel PieChart, frequency table, interval records
- 4 tests in TestPriceSetterEndpoints

## Sprint 28c — Electricity Retail Tariff Structure & Bill Analytics (2026-02-19)
**Status**: COMPLETE
- TariffComponent, TouTariffStructure, BillComposition, TariffDashboard models
- GET /api/tariff/dashboard, /components, /structures
- TariffAnalytics.tsx (~530 lines): stacked bill BarChart, composition PieChart, TOU table, component breakdown
- 4 tests in TestTariffEndpoints

## Sprint 28b — Smart Meter & Grid Modernisation Analytics (2026-02-19)
**Status**: COMPLETE
- SmartMeterRecord, GridModernisationProject, NetworkReliabilityStats, GridModernisationDashboard models
- GET /api/grid-modernisation/dashboard, /smart-meters, /projects
- GridModernisation.tsx (~520 lines): penetration BarChart, SAIDI reliability, smart meter table, projects table
- 4 tests in TestGridModernisationEndpoints

## Sprint 29a — Spot Price Cap & CPT Analytics [COMPLETE]
- SpotCapAnalytics.tsx: CPT tracker, cap/floor events, regional summary table
- /api/spot-cap/dashboard, /cpt-tracker, /cap-events
- 4 TestSpotCapEndpoints tests

## Sprint 29b — Causer Pays & FCAS Performance Analytics [COMPLETE]
- CauserPays.tsx: performance factor chart, contributors table, FCAS market summary
- /api/causer-pays/dashboard, /contributors, /performance
- 4 TestCauserPaysEndpoints tests

## Sprint 29c — WEM Western Australia Energy Market [COMPLETE]
- WemOverview.tsx: balancing price chart, facility table with tech filter, reserve capacity (SRMC) chart
- /api/wem/dashboard, /prices, /facilities endpoints
- 4 TestWemEndpoints tests

## Sprint 30a — Power System Inertia & System Strength Analytics [COMPLETE]
- InertiaAnalytics.tsx: inertia bar chart vs thresholds, system strength table, detail table
- /api/inertia/dashboard, /records, /strength endpoints
- 4 TestInertiaEndpoints tests

## Sprint 30c — TNSP Revenue & AER Determinations Analytics [COMPLETE]
- TnspAnalytics.tsx: revenue vs approved chart, AER determinations table, asset reliability table
- /api/tnsp/dashboard, /revenue, /determinations endpoints
- 4 TestTnspEndpoints tests

## Sprint 30b — AEMO Market Surveillance & Compliance Dashboard [COMPLETE]
- MarketSurveillance.tsx: surveillance notices table, compliance records, anomaly detection
- /api/surveillance/dashboard, /notices, /anomalies endpoints
- 4 TestSurveillanceEndpoints tests

## Sprint 31a — Green Hydrogen & Electrolysis Economics [COMPLETE]
- HydrogenAnalytics.tsx: LCOH comparison chart, project pipeline table, capacity stacked bar chart
- /api/hydrogen/dashboard, /projects, /benchmarks endpoints
- 4 TestHydrogenEndpoints tests

## Sprint 31b — Offshore Wind Project Tracker [COMPLETE]
- OffshoreWind.tsx: declared zones table, project tracker with approvals status, milestone timeline
- /api/offshore-wind/dashboard, /projects, /zones endpoints (8 projects, 7 zones)
- 4 TestOffshoreWindEndpoints tests

## Sprint 31c — Clean Energy Regulator (CER) & RET Dashboard [COMPLETE]
- CerDashboard.tsx: LRET/LGC price history, SRES solar installation chart, accredited stations table
- /api/cer/dashboard, /lret, /stations endpoints (11-year LRET history, 10 accredited stations)
- 4 TestCerEndpoints tests

## Sprint 32a — Pumped Hydro Energy Storage (PHES) Analytics [COMPLETE]
- PhesAnalytics.tsx: capacity outlook chart, project pipeline table, generation/pumping operations chart
- /api/phes/dashboard, /projects, /outlook endpoints (8 projects, 2025-2035 outlook)
- 4 TestPhesEndpoints tests

## Sprint 32c — Safeguard Mechanism & ERF Analytics [COMPLETE]
- SafeguardAnalytics.tsx: ACCU price/volume chart, safeguard facilities compliance table, ERF projects table
- /api/safeguard/dashboard, /facilities, /accu-market (10 facilities, 8 ERF projects, 12-month ACCU market)
- 4 TestSafeguardEndpoints tests

## Sprint 32b — Major Transmission Projects Dashboard [COMPLETE]
- TransmissionProjects.tsx: project table with RIT-T/AER status, milestone tracker, capex vs benefit chart
- /api/transmission/dashboard, /projects, /milestones (8 projects: HumeLink, VNI West, EnergyConnect, Marinus Link)
- 4 TestTransmissionEndpoints tests

## Sprint 33b — Virtual Power Plant (VPP) Performance Dashboard [COMPLETE]
- VppDashboard.tsx: capacity by scheme chart, schemes table with filters, 12-month performance LineChart
- /api/vpp/dashboard, /schemes, /dispatches (10 VPP schemes: AGL, Origin, sonnen, EA, Simply)
- 4 TestVppEndpoints tests

## Sprint 33a — DNSP Distribution Network Analytics [COMPLETE]
- DnspAnalytics.tsx: SAIDI bar chart vs regulatory target, DNSP table with reliability metrics, investment table
- /api/dnsp/dashboard, /records, /investments (14 DNSPs across all states)
- 4 TestDnspEndpoints tests

## Sprint 33c — NEM Market Reform Tracker [COMPLETE]
- MarketReformTracker.tsx: reforms table with status/category/agency filters, milestone timeline, financial impact table
- /api/reform/dashboard, /list, /milestones (10 reforms: 5MS, GSL, DER Integration, Two-Sided Markets, Capacity Mechanism)
- 4 TestReformEndpoints tests

## Sprint 34c — EV Charging Infrastructure & Grid Impact Analytics [COMPLETE]
- EvCharging.tsx: state/type filter charger table, peak vs offpeak grid impact bar chart, grid impact state table
- /api/ev/dashboard, /chargers, /grid-impact (12 chargers: Chargefox/BP Pulse/NRMA/Evie, 8-state grid impact)
- 4 TestEvEndpoints tests

## Sprint 34a — TNSP Network Pricing (TUoS) Analytics [COMPLETE]
- TuosAnalytics.tsx: TUoS rate bar chart by zone/TNSP, zones pricing table, MLF table with HIGH/LOW/AVG filters
- /api/tuos/dashboard, /zones, /mlf (12 TUoS zones: TransGrid/AusNet/Powerlink/ElectraNet/TasNetworks, 15 MLF records)
- 4 TestTuosEndpoints tests

## Sprint 34b — Carbon Credit & ACCU Registry Analytics [COMPLETE]
- CarbonRegistry.tsx: 24-month ACCU spot/futures price chart, projects table with method filters, method breakdown bar chart
- /api/carbon/dashboard, /projects, /market (12 ACCU projects: Savanna Fire, HIR, Soil Carbon; 24-month market data)
- 4 TestCarbonEndpoints tests

## Sprint 35a — Grid-Scale Energy Storage Arbitrage Analytics [COMPLETE]
- StorageArbitrage.tsx: revenue stack chart (arbitrage+FCAS+capacity), projects table, 48-period dispatch/SoC/price chart
- /api/storage/dashboard, /projects, /dispatch (12 BESS projects: HPR/VBB/Waratah/Snowy 2.0, 48-interval dispatch)
- 4 TestStorageEndpoints tests

## Sprint 35b — NEM Demand Forecasting Accuracy & PASA Analytics [COMPLETE]
- DemandForecastAnalytics.tsx: MAE by horizon bar chart, forecast records table, PASA reliability table with reserve margin colour-coding
- /api/demand-forecast/dashboard, /records, /pasa (100 forecast records, 60 PASA reliability records)
- 4 TestDemandForecastEndpoints tests

## Sprint 35c — Renewable Energy Zone (REZ) Development Analytics [COMPLETE]
- RezDevelopment.tsx: REZ capacity progress chart, zones table with state/status filters, generation pipeline table
- /api/rez/dashboard, /zones, /projects (11 REZs: New England, CWO, Southern QLD, Gippsland; 14 generation projects)
- 4 TestRezEndpoints tests

## Sprint 36a — NEM Trading Desk Real-Time Analytics [COMPLETE]
- TradingDesk.tsx: positions P&L table, regional spread bar chart, arbitrage opportunity badges
- /api/trading/dashboard, /positions, /spreads (15 positions, 5 interconnector spreads)
- 4 TestTradingEndpoints tests

## Sprint 36b — Network Congestion & Constraint Binding Analytics [COMPLETE]
- CongestionAnalytics.tsx: cost vs rent bar chart by interconnector, events table with cause/region filters, constraint binding frequency table
- /api/congestion/dashboard, /events, /constraints (20 congestion events, 10 constraint records)
- 4 TestCongestionEndpoints tests

## Sprint 36c — Energy Poverty & Social Equity Analytics [COMPLETE]
- EnergyEquity.tsx: hardship rate bar chart with disconnection rate line, state hardship table, demographic affordability table
- /api/equity/dashboard, /hardship, /affordability (8-state hardship records, 14 demographic affordability indicators)
- 4 TestEquityEndpoints tests

## Sprint 37a — Demand Response & RERT Analytics [COMPLETE]
- DemandResponseAnalytics.tsx: contracted vs available MW bar chart, activations table with trigger/type badges, providers table
- /api/demand-response/dashboard, /contracts, /activations, /providers (12 RERT/SRAS/NSCAS contracts, 15 activations, 10 providers)
- 4 TestDemandResponseEndpoints tests

## Sprint 37b — Behind-the-Meter (BTM) Analytics [COMPLETE]
- BehindTheMeter.tsx: rooftop PV capacity trend by state (12 months), home battery table, EV managed charging table
- /api/btm/dashboard, /rooftop-pv, /home-batteries, /ev (8-state PV records, 5-state battery+EV records, 12-month trend)
- 4 TestBtmEndpoints tests

## Sprint 37c — Regulatory Asset Base (RAB) & Network Revenue Analytics [COMPLETE]
- RabAnalytics.tsx: allowed revenue bar chart (top 10 networks), determinations table with AER decision badges, yearly CAPEX/OPEX variance table
- /api/rab/dashboard, /determinations, /yearly (15 TNSP+DNSP determinations, 25 yearly records)
- 3 TestRabEndpoints tests

## Sprint 38a — NEM Real-Time Overview Dashboard [COMPLETE]
- NemRealTimeDashboard.tsx: 5-region KPI price cards (colour-coded by band), generation mix stacked bar chart by fuel type, interconnector loading table
- /api/realtime/dashboard, /dispatch, /generation-mix, /interconnectors (5 regions, 28+ gen mix records, 6 interconnectors)
- 4 TestRealtimeEndpoints tests

## Sprint 38b — Network Investment Test (RIT-T/RIT-D) Analytics [COMPLETE]
- RitAnalytics.tsx: cost vs benefit scatter chart, projects table with BCR highlighting, options comparison table
- /api/rit/dashboard, /projects, /cost-benefits, /options (12 projects: 8 RIT-T + 4 RIT-D, 30+ cost-benefit records, 30+ option records)
- 4 TestRitEndpoints tests

## Sprint 38c — Electricity Derivatives & Forward Curve Analytics [COMPLETE]
- ForwardCurveAnalytics.tsx: forward curve line chart (CAL25/26/27 by region), cap/floor options table, seasonal premium heatmap
- /api/forward-curve/dashboard, /prices, /options, /seasonal (4-region curve, 27 cap/floor options, 80 seasonal premium records)
- 4 TestForwardCurveEndpoints tests

## Sprint 39a — Coal Fleet Retirement & Energy Transition Analytics [COMPLETE]
- CoalRetirement.tsx: capacity gap grouped bar chart (retirements vs renewables vs storage by state/year), retirement units table, transition investment pipeline table
- /api/coal-retirement/dashboard, /units, /capacity-gaps, /investments (16 coal units incl. retired, 20+ gap records, 15 investment records)
- 4 TestCoalRetirementEndpoints tests

## Sprint 39b — Gas-Fired Generation Economics Analytics [COMPLETE]
- GasGenEconomics.tsx: 12-month spark spread trend chart by region, gas generators SRMC table, gas contract type badges
- /api/gas-gen/dashboard, /generators, /spark-spreads (14 CCGT/OCGT generators, 48 spark spread records across 4 regions)
- 3 TestGasGenEndpoints tests

## Sprint 39c — Consumer Protection & Retail Market Analytics [COMPLETE]
- ConsumerProtection.tsx: complaints stacked bar chart by quarter, retail offers table (DMO vs market), switching rates table
- /api/consumer-protection/dashboard, /offers, /complaints, /switching (48 retail offers, 96 complaint records, 32 switching records)
- 4 TestConsumerProtectionEndpoints tests

## Sprint 40a — Generator Availability & EFOR Analytics [COMPLETE]
- GeneratorAvailability.tsx: EFOR trend line chart by technology (2018-2024), availability table sorted by EFOR with colour-coded thresholds
- /api/efor/dashboard, /availability, /trends (60 unit-year records, 49 technology trend records)
- 3 TestEforEndpoints tests

## Sprint 40b — Climate Risk & Infrastructure Resilience Analytics [COMPLETE]
- ClimateRiskAnalytics.tsx: composite risk horizontal bar chart (top 10 assets), asset risk table with adaptation status, climate event timeline table
- /api/climate-risk/dashboard, /assets, /events (18 network assets with risk scores, 12 historical climate events)
- 3 TestClimateRiskEndpoints tests

## Sprint 40c — Smart Grid Innovation & Grid Modernisation Analytics [COMPLETE]
- SmartGridAnalytics.tsx: DOE program bar chart with state filters, DERMS systems table, AMI adoption tracking
- /api/smart-grid/dashboard, /doe-programs, /derms, /ami (12 DOE programs, 8 DERMS systems, 24 AMI adoption records)
- 4 TestSmartGridEndpoints tests

## Sprint 41a — Minimum Demand & Duck Curve Analytics [COMPLETE]
- MinimumDemandAnalytics.tsx: duck curve line chart (demand/PV/net 48 half-hour periods), min demand records table, negative pricing stacked bar chart
- /api/minimum-demand/dashboard, /records, /duck-curve, /negative-pricing (8 min demand events, 8 duck curve profiles with 48 data points each, 48 negative pricing records)
- 4 TestMinDemandEndpoints tests

## Sprint 41b — NEM Market Events & AEMO Interventions Analysis [COMPLETE]
- MarketEventsAnalysis.tsx: event duration bar chart, events table with expandable descriptions, interventions table, chronological milestone timeline
- /api/nem-suspension/dashboard, /events, /interventions, /timeline (9 major events incl. 2022 suspension + 2016 SA blackout, 10 interventions, 12 timeline milestones)
- 4 TestNEMSuspensionEndpoints tests

## Sprint 41c — Battery Technology Economics & Learning Rate Analytics [COMPLETE]
- BatteryTechAnalytics.tsx: 10-year pack cost trend line chart by technology, LCOS comparison bar chart (2024), technology spec comparison table, critical mineral supply chain risk table
- /api/battery-tech/dashboard, /costs, /lcos, /supply-chain (34 cost records 2015-2024, 12 LCOS records, 6 supply chain records)
- 4 TestBatteryTechEndpoints tests

## Sprint 42a — Community Energy & Microgrids Analytics [COMPLETE]
- CommunityEnergy.tsx: community batteries table with program/status badges, solar gardens table (waitlist + low-income allocation), standalone power systems (SPS) table with network deferral value
- /api/community-energy/dashboard, /batteries, /solar-gardens, /sps (10 community batteries, 8 solar gardens, 11 SPS systems incl. NT Aboriginal communities)
- 4 TestCommunityEnergyEndpoints tests

## Sprint 42b — Transmission Asset Management & Inspection Analytics [COMPLETE]
- AssetManagement.tsx: condition score horizontal bar chart, assets table with priority/condition badges, inspections table with severity badges
- /api/asset-management/dashboard, /assets, /inspections, /maintenance (16 assets across 5 TNSPs, 10 inspection events, 7 maintenance program records)
- 4 TestAssetManagementEndpoints tests

## Sprint 42c — Energy Transition & Decarbonization Pathway Analytics [COMPLETE]
- DecarbonizationPathway.tsx: sectoral emissions vs target bar chart, technology deployment trend line chart, net zero milestones table with progress bars and funding gap
- /api/decarbonization/dashboard, /sectors, /milestones, /technology (10 sectoral records, 12 milestones, 25 technology deployment records)
- 4 TestDecarbonizationEndpoints tests

## Sprint 43a — Nuclear SMR & Long-Duration Energy Storage Investment Analytics [COMPLETE]
- NuclearLongDuration.tsx: 4 KPI cards (Total SMR Pipeline GW, Total LDES Pipeline GWh, Avg SMR LCOE, Avg LDES LCOS), stacked area chart for Clean Firm Capacity Outlook 2025-2040 (nuclear/LDES/pumped hydro/gas CCS/H2 peaker), SMR projects table with technology/status badges, long-duration storage table with technology/status badges
- Backend Pydantic models: SmrProjectRecord, LongDurationStorageRecord, CleanFirmCapacityRecord, NuclearLongDurationDashboard
- /api/nuclear-ldes/dashboard, /smr-projects, /long-duration, /capacity-outlook (8 SMR projects across NSW/VIC/SA/QLD covering AP1000/BWRX-300/ARC-100/NuScale; 10 LDES projects covering IRON_AIR/FLOW_BATTERY/COMPRESSED_AIR/GRAVITY/PUMPED_HYDRO/LIQUID_AIR; 16-year capacity outlook 2025-2040)
- Mock data: 8 SMR projects (CAPEX 4-12B AUD, LCOE 110-180 $/MWh, first power 2035-2045, CF 85-92%, CO2 <10 kg/MWh); 10 LDES projects (duration 4-100h, LCOS 55-200 $/MWh, status PROPOSED/FEASIBILITY/APPROVED)
- TypeScript interfaces: SmrProjectRecord, LongDurationStorageRecord, CleanFirmCapacityRecord, NuclearLongDurationDashboard; api.getNuclearLdesDashboard() method added to client.ts
- App.tsx: Atom icon imported, NuclearLongDuration page imported, /nuclear-ldes route added, "Nuclear & LDES" nav item added
- 4 TestNuclearLdesEndpoints tests appended to tests/test_backend.py

## Sprint 43b — Wholesale Market Bidding Behaviour & Strategic Withholding Analytics [COMPLETE]
- BiddingBehaviour.tsx: 4 KPI cards (Total Withheld MW, Avg Withholding Ratio %, High Withholding Events, Market Power Index), Market Concentration table (5 NEM regions x 2 years with HHI color coding: <1500 green / <2500 amber / >=2500 red), Capacity Withholding Records table with region filter (participant, technology badges, withheld MW, withholding ratio color coded, spot price, rebid count badge, rebid reason), Rebid Patterns ComposedChart with dual Y-axis (total rebids + late rebids as bars, market impact score as line overlay, participant selector), Bid Price Distribution table (participants vs price bands heatmap showing volume MW and portfolio %)
- Backend Pydantic models: BidWithholdingRecord, BidPriceDistRecord, RebidPatternRecord, MarketConcentrationRecord, BiddingBehaviourDashboard
- /api/bidding-behaviour/dashboard, /withholding, /price-distribution, /rebid-patterns, /market-concentration (all require X-API-Key)
  - 12 withholding records: AGL, Origin, EnergyAustralia, Snowy Hydro, Alinta across NSW1/VIC1/QLD1/SA1; technologies COAL/GAS/HYDRO/SOLAR; withholding ratios 10.98-82.86%; rebid counts 1-8; spot prices $50-$15,000/MWh
  - 15 price distribution records: 5 participants x multiple price bands (-$1000, $0, $50, $100, $300, $1000, $5000, $15000)
  - 36 rebid pattern records: 6 participants (AGL, Origin, EnergyAustralia, Snowy, Alinta, Meridian) x 6 months (Jan-Jun 2024); late_rebids 2-20; market_impact_score 2.0-8.1
  - 10 market concentration records: NSW1, VIC1, QLD1, SA1, TAS1 x years 2023 and 2024; HHI 1520-3380; CR3 45.8-74.2%
  - KPI aggregates: total_withheld_mw, avg_withholding_ratio_pct, high_withholding_events (ratio >40%), market_power_index (HHI/500 normalised)
- TypeScript interfaces: BidWithholdingRecord, BidPriceDistRecord, RebidPatternRecord, MarketConcentrationRecord, BiddingBehaviourDashboard added to client.ts
- api methods: getBiddingBehaviourDashboard, getBiddingBehaviourWithholding, getBiddingBehaviourPriceDistribution, getBiddingBehaviourRebidPatterns, getBiddingBehaviourMarketConcentration added to api object in client.ts
- App.tsx: BiddingBehaviour page imported, /bidding-behaviour route added, "Bidding Behaviour" nav item added (BarChart2 icon already present)
- 4 TestBiddingBehaviourEndpoints tests appended to tests/test_backend.py

## Sprint 44a — Electricity Spot Price Forecasting Dashboard [COMPLETE]
- SpotForecastDashboard.tsx: 5 KPI mini-cards (one per region — current price with trend arrow + spike probability progress bar), Forecast Price Bands AreaChart (P10/P50/P90 confidence intervals + actual prices over 24h, region toggle for NSW1/QLD1/VIC1/SA1/TAS1), Regional Price Summary table (region, current price, 24h avg, 7d avg, spike prob color-coded <15% green / <35% amber / else red, volatility index, trend badge), Model Performance table with period selector (NEURAL/GBDT/ENSEMBLE, MAE, RMSE, MAPE%, R², spike detection rate)
- Backend Pydantic models: SpotForecastInterval, RegionalPriceSummary, ModelPerformanceRecord, SpotForecastDashboard
- /api/spot-forecast/dashboard, /intervals, /regional-summary, /model-performance (all with `dependencies=[Depends(verify_api_key)]`)
  - 48 forecast intervals (30-min for next 24h) for NSW1; actual prices for past 24 intervals, None for future; P10/P50/P90 bands; NEURAL/ENSEMBLE/GBDT model rotation
  - 5 regional summaries (NSW1 $87.5, QLD1 $74.2, VIC1 $112.8, SA1 $198.4, TAS1 $44.6; spike probs 5.1–40.2%; volatility 21.7–78.4)
  - 15 model performance records (NEURAL/GBDT/ENSEMBLE × 5 regions; MAE $4.8–24.5; MAPE 7.8–20.7%; R² 0.67–0.86; spike detection 65.4–85.9%)
  - next_spike_alert: "NSW1 spike risk 73% at 18:30 AEST"; overall_forecast_accuracy_pct: 88.6%
- TypeScript interfaces: SpotForecastInterval, RegionalPriceSummary, ModelPerformanceRecord, SpotForecastDashboard added to client.ts before `// Internal helpers`
- api methods: getSpotForecastDashboard, getSpotForecastIntervals, getSpotForecastRegionalSummary, getSpotForecastModelPerformance added to api object in client.ts
- App.tsx: SpotForecastDashboard page imported, /spot-forecast route added, "Spot Forecast" nav item added (Zap icon already imported)
- 4 TestSpotForecastEndpoints tests appended to tests/test_backend.py

## Sprint 44c — Carbon Credit & Offset Market Analytics [COMPLETE]
- CarbonCreditMarket.tsx: TreePine icon header, 4 KPI cards (Current ACCU Price $/unit, Total Issued Mt CO2-e, Safeguard Demand kt CO2, Market Size $B AUD), ACCU Price History line chart (Jan-Jun 2024 daily spot by type: GENERIC/HIR/LANDFILL/SAVANNA_BURNING), Carbon Price Forecast line chart (2024-2035 BASE/HIGH/LOW + EU ETS AUD), Carbon Projects table with type filter badges (REFORESTATION green, SOIL_CARBON amber, SAVANNA_BURNING orange, LANDFILL_GAS gray, AVOIDED_DEFORESTATION teal), Corporate Buyers table with purpose badges (SAFEGUARD_COMPLIANCE red, VOLUNTARY_NET_ZERO green, EXPORT blue)
- Backend Pydantic models (collision-safe names): AccuSpotRecord, CarbonOffsetProjectRecord, CarbonOffsetBuyerRecord, AccuPriceForecastRecord, CarbonCreditMarketDashboard
- /api/carbon-credit/dashboard, /spot, /projects, /buyers, /price-forecast (all with `dependencies=[Depends(verify_api_key)]`)
  - 24 ACCU spot records: daily Jan-Jun 2024; GENERIC $28-42, HIR $44-55+, LANDFILL ~$30, SAVANNA ~$34-42; volumes 8K-95K; buyer_category rotation SAFEGUARD/VOLUNTARY/GOVERNMENT/EXPORT
  - 15 carbon offset projects: QLD/NSW/WA/NT/VIC/SA states; all 5 project types; vintage 2019-2023; prices $29-48/ACCU; HIR/soil carbon at premium; co_benefits including BIODIVERSITY/WATER/INDIGENOUS_EMPLOYMENT
  - 10 corporate buyers: BHP/Rio Tinto/AGL/Qantas/Woolworths/ANZ/Santos/Woodside/CBA/Coles; purchase volumes 18K-480K ACCUs; net zero targets 2030-2050
  - 36 price forecast records: BASE/HIGH/LOW x 12 years (2024-2035); BASE $36-88; HIGH $38-148; LOW $33-65; EU ETS AUD $78-147; California cap-and-trade AUD $48-99
- TypeScript interfaces: AccuSpotRecord, CarbonOffsetProjectRecord, CarbonOffsetBuyerRecord, AccuPriceForecastRecord, CarbonCreditMarketDashboard added to client.ts
- api methods: getCarbonCreditDashboard, getCarbonCreditSpot, getCarbonCreditProjects, getCarbonCreditBuyers, getCarbonCreditPriceForecast added to api object in client.ts
- App.tsx: CarbonCreditMarket imported, TreePine imported from lucide-react, /carbon-credit route added, "Carbon Credits" nav item (TreePine icon)
- 5 TestCarbonCreditEndpoints tests appended to tests/test_backend.py
- Note: test execution blocked by pre-existing NameError at line 11930 of main.py (`@router.get` used without `router` defined — not introduced by Sprint 44c)

## Sprint 45b — Power System Resilience & Extreme Weather Analytics [COMPLETE]
- GridResilience.tsx: ShieldAlert icon header, 4 KPI cards (Total Unserved Energy MWh, Customers Affected, Total Resilience Investment $M, Avg Recovery days), SAIDI Trend grouped bar chart 2022–2024 by state (NSW/VIC/QLD/SA/WA in distinct colors), Weather Outage Events table (event type badges: BUSHFIRE red, FLOOD blue, HEATWAVE amber, STORM gray, CYCLONE purple, DROUGHT yellow-brown; severity badges: LOW green / MODERATE amber / HIGH orange / EXTREME red), Grid Asset Vulnerability table (type badges, vulnerability score progress bar 0–10 color-coded, risk badges per hazard, replacement priority badges), Resilience Investment Programs table (investment type badges, status badges)
- Backend Pydantic models: WeatherOutageEvent, ResilienceInvestmentRecord, GridVulnerabilityRecord, ResilienceKpiRecord, GridResilienceDashboard
- /api/grid-resilience/dashboard, /outage-events, /investments, /vulnerability, /kpis (all with `dependencies=[Depends(verify_api_key)]`)
  - 12 weather outage events: Black Summer NSW/VIC fires 2019-20, 2022 QLD/NSW floods, Cyclone Ilsa WA 2023, 2024 SE QLD heatwave, 2019 SA storm, 2023 VIC alpine fires, 2023 NSW central west floods, 2022 WA Cyclone Ellie, 2024 TAS drought, 2023 NT storms; severity mix LOW–EXTREME; recovery 0.5–30 days
  - 10 resilience investments: Western Sydney undergrounding ($245M), Lismore substation flood barrier ($38M), Gippsland microgrids ($82M), Powerlink fire hardening ($145M), SA remote backup power ($28M), Pilbara comms upgrade ($55M), North QLD undergrounding ($180M), VIC transmission flood protection ($62M), Adelaide Hills fire hardening ($95M), TAS diesel backup ($18M); statuses PLANNING/APPROVED/CONSTRUCTION/COMPLETE
  - 15 grid vulnerability records: mix of SUBSTATION/TRANSMISSION_LINE/DISTRIBUTION_LINE/TRANSFORMER/CONTROL_SYSTEM; age 5–45 years; vulnerability scores 4.8–8.9
  - 15 KPI records: 5 states × 3 years (2022–2024); SAIDI 140–396 min; weather_related_pct 38–71.5%; resilience investment $78M–$248M
- TypeScript interfaces: WeatherOutageEvent, ResilienceInvestmentRecord, GridVulnerabilityRecord, ResilienceKpiRecord, GridResilienceDashboard added to client.ts
- api methods: getGridResilienceDashboard, getGridResilienceOutageEvents, getGridResilienceInvestments, getGridResilienceVulnerability, getGridResilienceKpis added to api object in client.ts
- App.tsx: GridResilience imported, ShieldAlert imported from lucide-react, /grid-resilience route added, "Grid Resilience" nav item (ShieldAlert icon)
- 5 TestGridResilienceEndpoints tests appended to tests/test_backend.py

## Sprint 45a — EV Fleet & Grid-Scale Charging Integration Analytics [COMPLETE]
- EvFleetCharging.tsx: Car icon header, 4 KPI cards (Total EV Fleet Vehicles, Total Charging Power MW, Avg Fleet EV Penetration %, V2G Capable Sites), EV Demand Forecast composed chart 2024–2035 (stacked areas for total_ev_demand/managed_charging/v2g_discharge TWh; line for peak_demand_increase_gw on right axis), Fleet Registry table (fleet type badges: BUS cyan/TRUCK amber/DELIVERY_VAN orange/GOVERNMENT blue/TAXI purple; penetration % colored <30% red/<60% amber/else green; charging strategy badges: OVERNIGHT gray/OPPORTUNITY amber/SMART_V2G green), Charging Infrastructure table (location type badges: DEPOT blue/HIGHWAY green/RETAIL purple/WORKPLACE amber/RESIDENTIAL_HUB cyan; charger type badges: AC_SLOW gray/AC_FAST blue/DC_FAST amber/DC_ULTRA red; V2G green badge; status badges), V2G Dispatch composed chart (bar for v2g_export_mw; line overlay for spot price on right axis)
- Backend Pydantic models: EvFleet45Record, ChargingInfra45Record, V2GDispatch45Record, EvDemandForecast45Record, EvFleet45Dashboard (note: Sprint 27a had EvFleetRecord so 45-suffix used to avoid collision)
- /api/ev-fleet/dashboard, /fleets, /charging-infra, /v2g-dispatch, /demand-forecast (all with `dependencies=[Depends(verify_api_key)]`)
  - 10 fleet records: Sydney Buses, Melbourne Metro Trains, Australia Post, Woolworths Distribution, NSW Government Fleet, QLD Taxi Fleet, Brisbane City Council, Toll Group, StarTrack, TransLink; EV penetration 10–85%; fleet_type BUS/TRUCK/DELIVERY_VAN/GOVERNMENT/TAXI; charging_strategy mix with 5 V2G-enabled fleets
  - 12 charging infrastructure sites: depots/highways/retail/workplace/residential hub across NSW/VIC/QLD/SA/WA; AC_SLOW to DC_ULTRA; utilisation 30–80%; 5 V2G-capable sites; statuses OPERATING/CONSTRUCTION/PLANNED
  - 24 V2G dispatch intervals: 30-min for 2026-02-20 (06:00–17:30); 3 fleet IDs rotating; export 0.2–5 MW following price curve; spot prices $55–$500/MWh; SoC tracking 20–95%
  - 12 demand forecast records (2024–2035): EV stock 0.25M–11M; fleet EV % 8–91%; total demand 1.2–95 TWh; managed charging 0.3–42 TWh; V2G discharge 0.05–15.5 TWh
- TypeScript interfaces: EvFleet45Record, ChargingInfra45Record, V2GDispatch45Record, EvDemandForecast45Record, EvFleet45Dashboard added to client.ts
- api method: getEvFleetDashboard added to api object in client.ts
- App.tsx: EvFleetCharging imported, Car imported from lucide-react, /ev-fleet route added, "EV Fleet & V2G" nav item (Car icon)
- 5 TestEvFleetEndpoints tests appended to tests/test_backend.py
- Note: test execution blocked by pre-existing NameError at line 11930 of main.py (`@router.get` used without `router` defined — not introduced by Sprint 45a)

## Sprint 43c — Energy Poverty & Just Transition Analytics [COMPLETE]
- EnergyPoverty.tsx: 4 KPI cards (national hardship rate, workers in transition, total transition fund, low-income solar gap), hardship rate trend line chart by state over 4 quarters, coal worker transition table (wage ratio colored red/amber/green), affordability table (bill % income colored by stress level), just transition programs table (program type and status badges, outcomes score bar)
- /api/energy-poverty/dashboard, /hardship, /worker-transition, /affordability, /programs
  - 20 hardship records (5 states × 4 quarters 2024; hardship rates 8.6–15.8%; avg bills $1550–$2450 AUD)
  - 8 coal worker transition records (Liddell NSW, Yallourn VIC, Eraring NSW, Bayswater NSW, Muja WA, Callide QLD, Tarong QLD, Hazelwood VIC; closure years 2017–2032; statuses PLANNING/ACTIVE/COMPLETED)
  - 5 affordability records (NSW, VIC, QLD, SA, WA for 2024; low-income solar penetration 8–29%; concession coverage 68–82%)
  - 12 just transition programs (WORKER_RETRAINING, COMMUNITY_FUND, CLEAN_ENERGY_ACCESS, ECONOMIC_DIVERSIFICATION; budgets $20M–$500M; up to 58,000 beneficiaries)
- 4 TestEnergyPovertyEndpoints tests
- App.tsx: Heart icon nav entry at /energy-poverty; Heart already in lucide-react imports

## Sprint 45c — Renewable Energy Certificate Market Analytics [COMPLETE]
- RecMarket.tsx: Award icon header, title "Renewable Energy Certificate Market (LGC & STC)", subtitle about LRET compliance and certificate markets
  - 4 KPI cards: Current LGC Price ($/cert), Current STC Price ($/cert), LRET Progress %, LGC Surplus/Deficit (M)
  - LGC Spot Price History line chart (Jan–Mar 2024 daily; color-coded by technology source WIND/SOLAR/HYDRO/BIOMASS)
  - LRET Target Progress bar: visual progress indicator with achieved TWh, gap to target, and 2030 deadline
  - LGC Creation Registry table: filterable by technology (WIND/SOLAR/HYDRO/BIOMASS/WASTE_COAL_MINE); 20 stations including Macarthur Wind Farm, Snowtown, Bungala Solar, Snowy 2.0, Hornsdale, etc.; technology badges (WIND blue, SOLAR amber, HYDRO cyan, BIOMASS green, WASTE_COAL_MINE gray)
  - LRET Compliance table: 8 liable entities (AGL, Origin, EnergyAustralia, Alinta, ERM, Snowy, Ergon, Powercor); compliance % color-coded; CheckCircle/AlertTriangle icons
  - STC Market section: quarterly bar+line chart (2022–2024) + detail table with quarter, price, volume, rooftop solar MW, solar HW units, heat pumps, total value
- Backend Pydantic models: LgcSpotRecord, SurplusDeficitRecord, LgcCreationRecord, StcRecord, RecMarketDashboard
- Endpoints: /api/rec-market/dashboard, /lgc-spot, /lgc-creation, /surplus-deficit, /stc (all with verify_api_key)
  - 24 LGC spot records: Jan–Mar 2024; prices $42.80–$58.10/cert; volumes 7.6K–41.2K; technologies WIND/SOLAR/HYDRO/BIOMASS; vintages 2023–2024
  - 8 surplus/deficit records: AGL/Origin/EnergyAustralia/Alinta/ERM/Snowy/Ergon/Powercor; 3 entities with shortfalls (EnergyAustralia 25K LGCs/$1.32M, ERM 12K/$0.63M, Powercor 9K/$0.47M)
  - 20 LGC creation records: major wind/solar/hydro/biomass/waste-coal-mine stations; LGCs created 112K–4.23M; capacities 38–2000 MW; avg prices $44.80–$54.80/cert
  - 8 STC quarterly records: 2022-Q3 through 2024-Q2; prices $37.50–$40.20/cert; volumes 18.45M–25.6M; rooftop solar 389–568 MW/quarter; total values $691.9M–$1021.4M
- Dashboard KPIs: current_lgc_price=$42.80, current_stc_price=$40.20, lgc_surplus_deficit_m=-2.3 (deficit), lret_target_2030_twh=33.0, lret_progress_pct=67.4%
- Cache TTL: 1800s (30 minutes)
- TypeScript interfaces: LgcSpotRecord, SurplusDeficitRecord, LgcCreationRecord, StcRecord, RecMarketDashboard added to client.ts
- api methods: getRecMarketDashboard, getRecMarketLgcSpot, getRecMarketLgcCreation, getRecMarketSurplusDeficit, getRecMarketStc added to api object in client.ts
- App.tsx: RecMarket imported, Award imported from lucide-react, /rec-market route added, "REC Market (LGC/STC)" nav item (Award icon)
- 5 TestRecMarketEndpoints tests appended to tests/test_backend.py

## Sprint 46a — Transmission Congestion & Nodal Pricing Analytics [COMPLETE]
- TransmissionCongestion.tsx: Network icon header, title "Transmission Congestion & Nodal Pricing Analytics", subtitle about NEM constraint binding, LMP decomposition, congestion rent
  - 4 KPI cards: Total Congestion Rent ($M AUD, all interconnectors 2024), Most Constrained Interconnector, Avg Binding %, Peak Shadow Price ($/MWh)
  - Congestion Heatmap chart: Recharts ComposedChart — grouped bars by interconnector (VIC1-NSW1 blue, QLD1-NSW1 green, SA1-VIC1 amber, TAS1-VIC1 cyan) for utilisation % + pink line overlay for avg price separation (right axis); 6 months Jan–Jun 2024
  - Constraint Binding table: sorted descending by binding %; interconnector badge (colored), direction badge (IMPORT blue, EXPORT purple), binding hours, binding % (red/amber/green threshold coloring), avg/max shadow prices, congestion rent ($M), primary cause badge (THERMAL red, STABILITY orange, VOLTAGE amber, NETWORK_OUTAGE gray)
  - Nodal LMP Decomposition stacked bar chart: Energy (blue), Loss (green), Congestion (amber) components per node; 12 nodes across NSW/VIC/QLD/SA/TAS
  - Congestion Rent Distribution table: per interconnector-quarter with total rent, SREC allocated, TNSP retained, hedging value; total row in footer
  - Interconnector color legend panel at bottom
- Backend Pydantic models: ConstraintBindingRecord, NodalPriceRecord, CongestionRentRecord, CongestionHeatmapRecord, TransmissionCongestionDashboard
- Endpoints: /api/transmission-congestion/dashboard, /constraints, /nodal-prices, /congestion-rent, /heatmap (all with verify_api_key)
  - 10 constraint binding records: VIC1-NSW1/QLD1-NSW1/SA1-VIC1/TAS1-VIC1 (IMPORT+EXPORT directions); binding 5–39%; avg shadow prices $42–$487/MWh; max shadow prices $2.1K–$15.1K; congestion rent $9.7M–$198.4M; causes THERMAL/STABILITY/VOLTAGE/NETWORK_OUTAGE
  - 12 nodal price records: Eraring/Sydney CBD (NSW), Loy Yang/Melbourne (VIC), Tarong/Brisbane (QLD), Torrens/Adelaide (SA), Basslink (TAS), TransGrid North (NSW), Moorabool Wind (VIC), Western Downs Solar (QLD); LMP $44.7–$134.2/MWh; congestion -$46.8 to +$38.4/MWh
  - 16 congestion rent records: 4 interconnectors × 4 quarters 2024; total rent $19.8M–$67.4M per IC-quarter; SREC/TNSP/hedging components
  - 24 heatmap records: 4 interconnectors × 6 months (Jan–Jun 2024); utilisation 45–95%; binding events 98–412; price separation $8.2–$78.6/MWh
- Dashboard KPIs: total_congestion_rent_m_aud (sum across all 16 rent records), most_constrained_interconnector=SA1-VIC1 (38.9% binding), avg_binding_pct across 10 constraints, peak_shadow_price=$15,100/MWh
- Cache TTL: 600s (10 minutes)
- TypeScript interfaces: ConstraintBindingRecord, NodalPriceRecord, CongestionRentRecord, CongestionHeatmapRecord, TransmissionCongestionDashboard added to client.ts
- api methods: getTransmissionCongestionDashboard, getTransmissionCongestionConstraints, getTransmissionCongestionNodalPrices, getTransmissionCongestionRent, getTransmissionCongestionHeatmap added to api object in client.ts
- App.tsx: TransmissionCongestion imported; Network icon already in lucide-react imports; /transmission-congestion route added; "Transmission Congestion" nav item (Network icon) added before Settings
- 5 TestTransmissionCongestionEndpoints tests appended to tests/test_backend.py

## Sprint 46c — Electricity Market Design & Reform Tracker [COMPLETE]
- MarketDesignReform.tsx: BookOpen icon header, title "Electricity Market Design & Reform Tracker", subtitle about NEM reform pipeline, capacity mechanisms, and global market benchmarking
  - 4 KPI cards: Active Reform Proposals (3), Implemented Reforms (6), Total Reform Benefit ($1.9B AUD), Capacity Mechanism Pipeline (5.4 GW)
  - Proposals by Reform Area bar chart: Recharts BarChart showing count per area (indigo bars); 7 areas represented
  - Capacity Mechanisms Target vs Contracted bar chart: Recharts grouped BarChart; amber=target, green=contracted; per region
  - Reform Proposals table: filterable by status dropdown; title+summary excerpt, proposing body, reform area badge (CAPACITY_MECHANISM red, PRICING amber, SETTLEMENT blue, STORAGE cyan, DER green, RETAIL purple, PLANNING gray), status badge (CONSULTATION blue, DRAFT_DETERMINATION amber, FINAL_DETERMINATION orange, IMPLEMENTED green, REJECTED red), impact badge (LOW/MEDIUM/HIGH/TRANSFORMATIVE), decision date, annual benefit ($M)
  - Capacity Mechanisms table: mechanism name, region, type badge (RELIABILITY_OBLIGATION red, CAPACITY_AUCTION blue, STRATEGIC_RESERVE amber, CAPACITY_PAYMENT purple), status badge, target/contracted MW with progress bar + fill %, cost per MW ($AUD), storage eligible checkmark/x
  - Settlement Reforms section: grid of cards (1-3 col responsive); each card shows pre/post reform avg price with delta, volatility change %, storage revenue impact, demand response change, winner badge (GENERATORS red, STORAGE blue, CONSUMERS green, MIXED amber), assessment excerpt
  - Global Market Comparison table: NEM row highlighted (indigo background + HOME badge); 8 markets (NEM/WEM/ERCOT/PJM/CAISO/NORDPOOL/GB_NETA/SINGAPORE); market type badge, settlement interval, capacity mechanism type badge, price cap, renewables % (green if >=50%), avg price, market size TWh
- Backend Pydantic models: MarketDesignProposalRecord, CapacityMechanismRecord, SettlementReformRecord, MarketDesignComparisonRecord, MarketDesignDashboard
- Endpoints: /api/market-design/dashboard, /proposals, /capacity-mechanisms, /settlement-reforms, /market-comparison (all with verify_api_key)
  - 12 reform proposals: 5-minute settlement (IMPLEMENTED), Capacity Investment Scheme (IMPLEMENTED), Storage registration (IMPLEMENTED), Integrating ESS (FINAL_DETERMINATION), DER integration (IMPLEMENTED), Default Market Offer (IMPLEMENTED), ISP actionable projects (DRAFT_DETERMINATION), Intraday auction proposal (CONSULTATION), Minimum demand management (FINAL_DETERMINATION), Demand response mechanism (IMPLEMENTED), Whole of System Plan (CONSULTATION), NEM price cap review (REJECTED)
  - 6 capacity mechanism records: NSW Retailer Reliability Obligation (OPERATIONAL), QLD Strategic Reserve (OPERATIONAL), SA Capacity Auction Pilot (PILOT), VIC Capacity Payment Review (PROPOSED), National Capacity Mechanism Proposal (PROPOSED), WEM Capacity Credit Mechanism (OPERATIONAL)
  - 5 settlement reform records: 5-minute settlement NEM 2021 (STORAGE winner), WEM Real-Time 2023 (MIXED), Intraday Auction proposal 2026 (MIXED), SA Real-Time Pricing Pilot 2022 (CONSUMERS), NEM Demand Response 2021 (CONSUMERS)
  - 8 market comparison records: NEM (GROSS_POOL, 5-min, OBLIGATION, 38.2% RE), WEM (GROSS_POOL, 30-min), ERCOT (GROSS_POOL, 5-min, NONE), PJM (GROSS_POOL, 5-min, AUCTION), CAISO (GROSS_POOL, 5-min, OBLIGATION), NORDPOOL (HYBRID, 60-min, NONE, 91.4% RE), GB_NETA (NET_POOL, 30-min, AUCTION), SINGAPORE (GROSS_POOL, 30-min, PAYMENT)
- Dashboard KPIs: active_proposals=3 (CONSULTATION+DRAFT+FINAL), implemented_reforms=6, total_reform_benefit_b_aud (sum of annual benefits / 1000), capacity_mechanism_pipeline_gw (PROPOSED+PILOT target MW / 1000)
- Cache TTL: 600s (10 minutes)
- TypeScript interfaces: MarketDesignProposalRecord, CapacityMechanismRecord, SettlementReformRecord, MarketDesignComparisonRecord, MarketDesignDashboard added to client.ts
- api methods: getMarketDesignDashboard, getMarketDesignProposals, getMarketDesignCapacityMechanisms, getMarketDesignSettlementReforms, getMarketDesignMarketComparison added to api object in client.ts
- App.tsx: MarketDesignReform imported; BookOpen added to lucide-react imports; /market-design route added; "Market Design" nav item (BookOpen icon) added before Settings
- 5 TestMarketDesignEndpoints tests appended to tests/test_backend.py

## Sprint 46b — DERMS & DER Orchestration Analytics [COMPLETE]
- DermsOrchestration.tsx: Layers icon header, title "DERMS & DER Orchestration Analytics", subtitle about VPP dispatch, grid flexibility services and behind-the-meter resource orchestration
  - 4 KPI cards: Total Controllable Capacity (MW), Total Enrolled Devices, Avg Dispatch Accuracy %, Peak Flexibility (MW)
  - DER Portfolio stacked bar chart: potential_flexibility_mw per DER type (ROOFTOP_SOLAR amber, HOME_BATTERY blue, EV_CHARGER green, HVAC purple, HOT_WATER cyan) grouped by state (NSW/VIC/QLD/SA/WA)
  - Monthly KPI line chart: peak_coincidence_reduction_mw by state (NSW/VIC/QLD/SA/WA) over Jan–Apr 2024
  - Aggregator Registry table: name, state, DER types, enrolled/controllable devices, peak dispatch MW, market registration badge (VPP green, FCAS amber, DEMAND_RESPONSE blue, ALL purple), response time, dispatch success % (color-coded)
  - VPP Dispatch Event Log table: date, aggregator, trigger badge (PRICE_SPIKE red, GRID_FREQUENCY orange, OPERATOR_INSTRUCTION blue, SCHEDULED gray), requested/delivered MW, accuracy % (color-coded), duration, revenue ($AUD), grid service badge (ENERGY blue, FCAS_R6/R60 amber, DEMAND_RESPONSE green)
- Backend Pydantic models: DerAggregatorRecord, DerDispatchEventRecord, DerPortfolioRecord, DerOrchestrationKpiRecord, DermsOrchestrationDashboard
- Endpoints: /api/derms-orchestration/dashboard, /aggregators, /dispatch-events, /der-portfolio, /kpis (all with verify_api_key)
  - 8 aggregator records: AGL VPP (NSW), Origin VPP (VIC), Tesla Energy Plan (SA), Amber Electric (QLD), Reposit Power (NSW), EnergyAustralia Smart Home (VIC), Simply Energy DER (SA), Ausgrid DER Program (NSW); enrolled 5100–48500 devices; dispatch success 87.6–98.4%
  - 15 dispatch events: Jan–Apr 2024; all 4 trigger types; accuracy 79.5–99.7%; all grid service types; revenue $19.8K–$305K
  - 20 DER portfolio records: 4 DER types × 5 states (NSW/VIC/QLD/SA/WA); smart penetration 15–90%; VPP enrolment 4.1–32.1%
  - 20 KPI records: 5 states × 4 months Jan–Apr 2024; dispatches 10–51/month; peak coincidence reduction 46–210 MW
- Dashboard KPIs: total_controllable_mw (sum peak_dispatch_mw), total_enrolled_devices (sum enrolled_devices), avg_dispatch_accuracy_pct (mean across 15 events), peak_flexibility_mw (sum potential_flexibility_mw across portfolio)
- Cache TTL: 300s (5 minutes)
- TypeScript interfaces: DerAggregatorRecord, DerDispatchEventRecord, DerPortfolioRecord, DerOrchestrationKpiRecord, DermsOrchestrationDashboard added to client.ts
- api methods: getDermsOrchestrationDashboard, getDermsOrchestrationAggregators, getDermsOrchestrationDispatchEvents, getDermsOrchestrationDerPortfolio, getDermsOrchestrationKpis added to api object in client.ts
- App.tsx: DermsOrchestration imported; Layers icon added to lucide-react imports; /derms-orchestration route added; "DERMS & VPP" nav item (Layers icon) added before Settings
- 5 TestDermsOrchestrationEndpoints tests appended to tests/test_backend.py

## Sprint 47b — Retail Offer Comparison & Tariff Analytics [COMPLETE]
- RetailOfferComparison.tsx: Tag icon header, title "Electricity Retail Offer Comparison", subtitle about DMO vs market offers, solar FIT, tariff structures
  - 4 KPI cards: Avg Market Discount vs DMO %, Cheapest Offer State, Avg Solar FIT Rate (c/kWh), TOU Adoption %
  - DMO vs Market ComposedChart: grouped bars for dmo_annual_bill (red), avg_market_offer_bill (blue), cheapest_offer_bill (green) by state/year; line for consumers_on_dmo_pct (amber, right axis)
  - Market Offers table: retailer, state, offer name, offer type badge (FLAT_RATE gray, TOU blue, DEMAND purple, FLEXIBLE amber, EV_OPTIMISED green), supply charge, peak rate, solar FIT, est. annual bill 5000 kWh — filterable by state and type dropdowns
  - Solar FIT Comparison table: state, retailer, FIT type badge (GROSS yellow, NET teal, BATTERY_FEED_IN indigo), rate, minimum rate flag, time-varying, peak/off-peak rates
  - Tariff Structure grid of cards: peak hours, peak/off-peak/shoulder rates, demand charge, battery optimisation badge, EV discount badge
- Backend Pydantic models: MarketOfferRecord, DmoVsMarketRecord, SolarFitRecord, TariffStructureRecord, RetailOfferComparisonDashboard
- Endpoints: /api/retail-offer-comparison/dashboard, /offers, /dmo-comparison, /solar-fit, /tariff-structures (all with verify_api_key)
  - 20 market offer records: AGL/Origin/EnergyAustralia/Alinta/Simply Energy/Amber/Powershop across NSW/VIC/QLD/SA; FLAT_RATE/TOU/DEMAND/FLEXIBLE/EV_OPTIMISED types; bills $800–$2400; solar FIT 6–18c
  - 8 DMO vs market records: NSW/VIC/QLD/SA × 2023/2024; market discount 16–25.5%; consumers on DMO 18–40%
  - 12 solar FIT records: major retailers across 4 states; NET/GROSS/BATTERY_FEED_IN; rates 6.7–18c; time-varying options with peak/off-peak rates
  - 8 tariff structure records: FLAT_RATE/TOU/DEMAND/FLEXIBLE/EV_OPTIMISED across states; peak hours descriptions; battery optimisation and EV discount flags
- Dashboard KPIs: avg_market_discount_pct (mean across DMO records), cheapest_offer_state (state with lowest cheapest_offer_bill), avg_solar_fit_rate (mean FIT rates), tou_adoption_pct (TOU offers / total offers × 100)
- Cache TTL: 300s (5 minutes)
- TypeScript interfaces: MarketOfferRecord, DmoVsMarketRecord, SolarFitRecord, TariffStructureRecord, RetailOfferComparisonDashboard appended to client.ts
- api methods: getRetailOfferComparisonDashboard, getRetailOfferComparisonOffers, getRetailOfferComparisonDmo, getRetailOfferComparisonSolarFit, getRetailOfferComparisonTariffStructures added to api object in client.ts
- App.tsx: RetailOfferComparison imported; Tag icon added to lucide-react imports; /retail-offer-comparison route added; "Retail Offer Compare" nav item (Tag icon) added before Settings
- 5 TestRetailOfferComparisonEndpoints tests appended to tests/test_backend.py

## Sprint 47c — AEMO System Operator Actions & Instructions Dashboard [COMPLETE]
- SystemOperatorActions.tsx: AlertOctagon icon header, title "AEMO System Operator Actions & Instructions", subtitle about emergency interventions, directions, RERT activations, load shedding
  - 4 KPI cards: Total Directions (2024), RERT Activations (2024), Total Load Shed (MWh), Direction Cost ($M AUD)
  - Directions table: direction ID, datetime, region badge, participant, type badge (GENERATE green/REDUCE_OUTPUT red/INCREASE_LOAD blue/REDUCE_LOAD orange/MAINTAIN gray), MW directed, reason badge (LOW_RESERVE amber/FREQUENCY purple/VOLTAGE indigo/NETWORK cyan/SECURITY rose), duration, compliance %, cost ($AUD), outcome badge
  - RERT Activations cards: activation ID, trigger badge (LOR1 green/LOR2 amber/LOR3 red), region, date, contracted/activated MW, duration, cost, reserve margin before/after arrow, provider chips
  - Load Shedding Events table: date, region badge, state, cause badge (GENERATION_SHORTFALL red/NETWORK_FAILURE orange/EXTREME_DEMAND amber/CASCADING purple), peak MW, duration, customers affected, unserved energy MWh, financial cost, VoLL cost
  - Constraint Relaxations table: constraint name + ID, region badge, date, original/relaxed/delta MW, approval authority, duration, risk badge (LOW green/MEDIUM amber/HIGH red)
- Backend Pydantic models: SysOpDirectionRecord, SysOpRertActivation, LoadSheddingEvent, ConstraintRelaxation, SystemOperatorDashboard
- Endpoints: /api/system-operator/dashboard, /directions, /rert-activations, /load-shedding, /constraint-relaxations (all with verify_api_key)
  - 12 direction records: 2024 events; SA1/NSW1/VIC1/QLD1; AGL/Origin/EnergyAustralia/Snowy/CS Energy/Stanwell; GENERATE/REDUCE_OUTPUT/INCREASE_LOAD/MAINTAIN types; LOW_RESERVE/FREQUENCY/VOLTAGE/NETWORK/SECURITY reasons; costs $95K-$4.9M; mostly SUCCESSFUL (2 PARTIAL, 1 FAILED)
  - 5 RERT activations: SA1 (Jan 2023 LOR3, Jul 2024 LOR3), NSW1 (Jun 2023 LOR2), VIC1 (Aug 2023 LOR1), QLD1 (Feb 2024 LOR2); contracted 200-1000 MW; costs $1.8M-$30.1M
  - 4 load shedding events: 2019 QLD GENERATION_SHORTFALL (200 MWh), 2022 NEM CASCADING (5000 MWh), 2023 SA EXTREME_DEMAND (50 MWh), 2024 VIC NETWORK_FAILURE (380 MWh)
  - 8 constraint relaxations: SA1/NSW1/VIC1/QLD1 constraints; relaxation 50-500 MW; LOW/MEDIUM/HIGH risk; approved by AEMO/TNSPs
- Dashboard KPIs: total_directions_2024 (len directions), total_rert_activations_2024 (2024 RERT count), total_load_shed_mwh (sum unserved_energy_mwh), total_direction_cost_m_aud (sum cost_aud / 1M)
- Cache TTL: 300s (5 minutes)
- TypeScript interfaces: SysOpDirectionRecord, SysOpRertActivation, LoadSheddingEvent, ConstraintRelaxation, SystemOperatorDashboard appended to client.ts
- api methods: getSystemOperatorDashboard, getSystemOperatorDirections, getSystemOperatorRertActivations, getSystemOperatorLoadShedding, getSystemOperatorConstraintRelaxations added to api object in client.ts
- App.tsx: SystemOperatorActions imported; AlertOctagon already present in lucide-react imports; /system-operator route added; "System Operator" nav item (AlertOctagon icon) added before Settings
- 5 TestSystemOperatorEndpoints tests appended to tests/test_backend.py

## Sprint 47a — REZ Capacity & Development Tracking [COMPLETE]
- RezCapacityTracking.tsx: MapPin icon header, title "Renewable Energy Zone (REZ) Capacity Tracker", subtitle about ISP REZ pipeline, network augmentation, generation build-out
  - 4 KPI cards: Total REZ Target (GW), Connected Capacity (GW), Total Pipeline (GW), Network Augmentation Cost ($B)
  - REZ Capacity Progress chart: horizontal stacked bar per zone (Connected green, Construction blue, Approved amber, Proposed gray) vs target; sorted by target MW descending
  - Build-out Forecast chart: stacked area 2024–2029 cumulative MW for 5 REZs (New England, Central-West Orana, Western Victoria, South Australia, Queensland Central); 5 distinct colours
  - REZ Zones table: REZ name, state, zone type badge (WIND blue / SOLAR amber / HYBRID green), ISP priority badge (STEP_CHANGE green / CENTRAL amber / SLOW_CHANGE gray), target/connected/pipeline MW, network limit, augmentation required, cost ($M); sorted by target MW desc
  - Projects table: project name, REZ, technology badge (WIND/SOLAR/HYBRID/STORAGE), developer, state, capacity MW, status badge (OPERATING/CONSTRUCTION/APPROVED/PROPOSED), connection year, PPA signed, offtake badge (MERCHANT gray/PPA_CORPORATE blue/PPA_RETAILER green/GOVERNMENT purple) — filterable by zone and status dropdowns
  - Network Augmentations table: project name, REZ ID, TNSP, augmentation type badge (NEW_LINE blue/UPGRADE green/SUBSTATION amber/TRANSFORMER purple/REACTIVE_SUPPORT gray), voltage kV, capacity increase MW, CAPEX $M, status badge, completion year
- Backend Pydantic models (with RezGen prefix to avoid collisions with Sprint 20c): RezZoneRecord, RezProjectRecord, RezNetworkAugRecord, RezBuildOutRecord, RezCapacityDashboard
- Naming collision check: existing RezProject, RezDashboard (Sprint 20c), RezRecord confirmed; new models use distinct names; existing /api/rez/ routes unaffected
- Mock data: 10 REZ zone records (real ISP REZs: New England NSW, Central-West Orana NSW, South West NSW, Western Victoria, North East Victoria, SA REZ, Queensland Central, NW Tasmania, Hunter Valley NSW, Darling Downs QLD), 15 project records (Neoen/Origin/AGL/EDF/Tilt/CWP; WIND/SOLAR/HYBRID; OPERATING through PROPOSED), 8 network augmentation projects (TransGrid/AusNet/ElectraNet/Powerlink/TasNetworks; 500kV and 275kV), 30 build-out records (2024–2029 × 5 REZs)
- Dashboard KPIs computed: total_target_capacity_gw (sum target / 1000), total_connected_gw (sum connected / 1000), total_pipeline_gw (sum construction+approved+proposed / 1000), network_augmentation_cost_b_aud (sum capex / 1000)
- Endpoints: /api/rez-capacity/dashboard, /api/rez-capacity/zones, /api/rez-capacity/projects, /api/rez-capacity/network-augmentations, /api/rez-capacity/build-out (all with verify_api_key, TTL 300s cache)
- TypeScript interfaces: RezZoneRecord, RezProjectRecord, RezNetworkAugRecord, RezBuildOutRecord, RezCapacityDashboard appended to client.ts
- api methods: getRezCapacityDashboard, getRezCapacityZones, getRezCapacityProjects, getRezCapacityNetworkAugmentations, getRezCapacityBuildOut added to api object in client.ts
- App.tsx: RezCapacityTracking imported; MapPin already present in lucide-react imports; /rez-capacity route added; "REZ Capacity" nav item (MapPin icon) added before Settings
- 5 TestRezCapacityEndpoints tests appended to tests/test_backend.py

## Sprint 48b — NEM Price Spike Post-Event Analysis [COMPLETE]
- PriceSpikeAnalysis.tsx: TrendingUp icon header (Flame already used by Gas/Hydrogen/Coal Retirement), title "NEM Price Spike Post-Event Analysis", subtitle about root cause analysis, market participant behaviour, consumer impact, and regulatory response
  - 4 KPI cards: Spike Events 2024 (AlertTriangle red), Total Consumer Cost $M (DollarSign amber), Avg Spike Duration min (Clock blue), Most Affected Region (MapPin teal)
  - Spike Severity Timeline: ScatterChart with custom bubble shapes; X=event date (epoch ms), Y=peak price $/MWh; bubble size proportional to sqrt(duration_minutes)*4 capped at 60; color by severity (MODERATE amber, HIGH orange, EXTREME red, MARKET_SUSPENSION purple); custom tooltip showing event name, region, peak price, date
  - Spike Events table: event name, RegionBadge, date, duration (min), peak price ($/MWh, orange), price multiple (amber), consumer cost ($M, red), RootCauseBadge, SeverityBadge (MARKET_SUSPENSION pulses); clickable rows to filter detail panels
  - Top Contributors detail panel (filters to selected spike): participant, technology, ContributionTypeBadge, MW impact (+/- formatted), price contribution ($/MWh), revenue gained ($M), RegulatoryActionBadge (FINED red / CAUTIONED orange / INVESTIGATED yellow / CLEARED green)
  - Consumer Impact detail panel: stacked BarChart (hedged blue, unhedged red per segment); supplement table with hedged%, unhedged cost, demand response MW, A/C curtailment MW, price signal response%
  - Notes section: explains root cause taxonomy, hedging exposure definitions, regulatory action statuses
- Naming collision check: Sprint 13b uses PriceSpikeEvent and SpikeAnalysisSummary; Sprint 48b uses PSA-prefix (PSAEventRecord, PSAContributorRecord, PSAConsumerImpact, PSARegionalTimeline, PSADashboard) to avoid any conflict; endpoints use /api/spike-analysis/ (not /api/price-spike/)
- Backend Pydantic models: PSAEventRecord, PSAContributorRecord, PSAConsumerImpact, PSARegionalTimeline, PSADashboard
- Mock data:
  - 8 spike events: SPK-2022-001 (NEM Market Suspension Jun 2022 MARKET_SUSPENSION), SPK-2022-002 (NSW Summer Heatwave Jan 2022 EXTREME), SPK-2023-001 (SA Heatwave Jan 2023 EXTREME), SPK-2023-002 (VIC Gas Shortage Jul 2023 HIGH), SPK-2023-003 (QLD Network Constraint Nov 2023 HIGH), SPK-2024-001 (NSW Summer Peak Jan 2024 EXTREME), SPK-2024-002 (VIC Winter Peak Jul 2024 HIGH), SPK-2024-003 (QLD Cycling Sep 2024 MODERATE); peak prices $5200-$15300; durations 30-10080 min; root causes: GENERATION_SHORTFALL/DEMAND_SPIKE/WEATHER/STRATEGIC_BIDDING/NETWORK_CONSTRAINT
  - 12 contributor records: 3 for NEM Suspension (Origin/AGL/EnergyAustralia coal), 2 for NSW heatwave (Snowy Hydro/Meridian), 2 for SA heatwave (AGL OCGT/ElectraNet), 2 for VIC gas (Origin/AGL OCGT — one FINED), 1 for NSW 2024 (Origin), 1 for VIC 2024 (EnergyAustralia), 1 for QLD 2024 (CS Energy)
  - 16 consumer impact records: 2 segments × 8 spikes (RESIDENTIAL/INDUSTRIAL/C_AND_I/SME across NEM-WIDE/NSW1/SA1/VIC1/QLD1)
  - 40 regional timeline records: 5 intervals × 8 spikes; showing pre-spike, ramp-up, peak, decline, recovery
- Dashboard KPIs: total_spike_events_2024=3, total_consumer_cost_m_aud=sum of all 8 events, avg_spike_duration_min=avg across 8 events, most_affected_region="NSW1"
- Endpoints: GET /api/spike-analysis/dashboard, /events, /contributors, /consumer-impacts, /regional-timeline (all with Depends(verify_api_key), tag="Spike Analysis")
- TypeScript interfaces: PSAEventRecord, PSAContributorRecord, PSAConsumerImpact, PSARegionalTimeline, PSADashboard appended to client.ts
- api methods: getSpikeAnalysisDashboard, getSpikeAnalysisEvents, getSpikeAnalysisContributors, getSpikeAnalysisConsumerImpacts, getSpikeAnalysisRegionalTimeline added to api object in client.ts
- App.tsx: PriceSpikeAnalysis imported; Flame already present (used by Gas Market, Green Hydrogen, Coal Retirement); /spike-analysis route added; "Price Spike Analysis" nav item (Flame icon) added after Network Tariff Reform, before Settings
- 5 TestSpikeAnalysisEndpoints tests appended to tests/test_backend.py

## Sprint 49a — Energy Storage Revenue Stacking & Optimisation Analytics [COMPLETE]
- StorageRevenueStack.tsx: BarChart3 icon header, title "Energy Storage Revenue Stacking & Optimisation", subtitle about multi-service co-optimisation, FCAS + energy + capacity stacking
  - 4 KPI cards: Avg Total Revenue ($M/yr) blue, Best Revenue Project green, Energy vs FCAS Split % amber, Co-optimisation Benefit % purple
  - Revenue Waterfall stacked bar chart: 8 BESS projects sorted by total revenue descending; 6 revenue streams (energy arbitrage blue, FCAS raise green, FCAS lower cyan, capacity amber, network purple, ancillary gray); detail table with LCOE, IRR%, payback years
  - Scenario Comparison grouped bar chart: ENERGY_ONLY/FCAS_ONLY/FULL_STACK/NETWORK_CONTRACT showing Revenue/Cost/Profit per scenario; ROI%, payback, NPV table demonstrating clear revenue uplift from full stacking
  - Dispatch Optimisation: action heatmap row (24 hourly cells coloured CHARGE blue/DISCHARGE red/IDLE gray/FCAS_STANDBY amber with hover tooltip); dual-axis LineChart showing SOC trajectory + hourly revenue ($K); scrollable hourly table with SOC transition, action badge, energy MWh, P/L
  - Multi-Service Bids table: 10 records across 8 BESS projects; columns for energy bid MW, FCAS contingency raise/lower, FCAS regulation raise/lower, FCAS revenue, energy revenue, co-optimisation uplift % (amber)
- Naming collision check: Sprint 14b used /api/storage/ (BessUnit/BessDispatchInterval/BessFleetSummary); Sprint 23a used /api/battery-economics/ (BatteryArbitrageSlot); Sprint 35a used /api/storage/ again (StorageArbitrage); Sprint 44b added StorageArbitrage at /api/storage/; Sprint 49a uses /api/storage-revenue-stack/ prefix and SRV- prefixed data variables; no collisions
- Backend Pydantic models: StorageRevenueWaterfall, StorageDispatchOptRecord, MultiServiceBidRecord, StorageScenarioRecord, StorageRevenueStackDashboard
- Mock data:
  - 8 revenue waterfall records: Hornsdale PR (150MW/194MWh SA, $21.6M total, IRR 16.5%), Victorian Big Battery (300MW VIC, $28.2M, IRR 18%), Waratah Super Battery (850MW NSW, $31.7M, IRR 17.8%), Torrens Island BESS (250MW SA, $19.2M), Moorabool (200MW VIC), Kwinana (100MW WA), Gannawarra (25MW VIC), Lake Bonney (100MW SA); FCAS $4.9-12.3M, energy $2-8.1M, capacity $0.5-3M, total $7.9-31.7M
  - 24 dispatch optimisation records: representative Jan peak summer day; actions: CHARGE (8 intervals off-peak), DISCHARGE (6 intervals peak/morning peak), FCAS_STANDBY (7 intervals, including evening peak), IDLE (1 interval); SOC 10%→95%→10%; peak DISCHARGE revenue $42.8K-73.2K at hours 12,19; FCAS regulation priority at hours 10,14,17; revenue-negative CHARGE intervals -$0.8K to -4.9K
  - 10 multi-service bid records: HPR/VBB/WSB/TIB/MBB/KBB/GBB/LBB across Jan-Aug 2024 trading dates; co-optimisation uplift 15.2%-31.7%; FCAS contingency raise 14-350 MW; energy bids 18-420 MW
  - 4 scenario comparison records: ENERGY_ONLY ($8.2M rev, $2.8M profit, ROI 9.3%, 12.5yr payback, NPV $18.4M) → FCAS_ONLY ($14.6M rev, $9.2M profit, ROI 15.8%) → FULL_STACK ($26.8M rev, $21M profit, ROI 24.5%, 4.9yr payback, NPV $142.7M) → NETWORK_CONTRACT ($31.4M rev, $25.3M profit, ROI 28.2%, 4.2yr payback, NPV $175.3M)
- Dashboard KPIs: avg_total_revenue_m_aud=18.7, best_revenue_project="Waratah Super Battery", energy_vs_fcas_split_pct=38.5 (energy share), co_optimisation_benefit_pct=23.2
- Endpoints: GET /api/storage-revenue-stack/dashboard, /waterfall, /dispatch-optimisation, /multi-service-bids, /scenarios (all with Depends(verify_api_key), tag="Storage Revenue Stack")
- TypeScript interfaces: StorageRevenueWaterfall, StorageDispatchOptRecord, MultiServiceBidRecord, StorageScenarioRecord, StorageRevenueStackDashboard appended to client.ts
- api methods: getStorageRevenueStackDashboard, getStorageRevenueStackWaterfall, getStorageRevenueStackDispatchOptimisation, getStorageRevenueStackMultiServiceBids, getStorageRevenueStackScenarios added to api object in client.ts
- App.tsx: StorageRevenueStack imported; BarChart3 added to lucide-react imports (new icon, not previously imported); /storage-revenue-stack route added; "Storage Revenue Stack" nav item (BarChart3 icon) added after Price Spike Analysis, before Settings
- 5 TestStorageRevenueStackEndpoints tests appended to tests/test_backend.py

## Sprint 49c — Solar Irradiance & Resource Assessment Analytics — 2026-02-20T00:00:00+11:00 (AEDT)

**Completed:**
- Naming collision check: No collisions found for IrradianceSiteRecord, SolarFarmYieldRecord, MonthlyIrradianceRecord, SolarDegradationRecord, SolarResourceDashboard, /api/solar-resource/* endpoints
- Backend Pydantic models: IrradianceSiteRecord, SolarFarmYieldRecord, MonthlyIrradianceRecord, SolarDegradationRecord, SolarResourceDashboard appended to app/backend/main.py
- Mock data:
  - 10 irradiance sites: Broken Hill NSW (GHI 2310), Longreach QLD (2380), Port Augusta SA (2240), Carnarvon WA (2420), Alice Springs NT (2400), Mildura VIC (2050), Dubbo NSW (1980), Toowoomba QLD (1860), Geraldton WA (2200), Roxby Downs SA (2280); resource classes EXCELLENT/VERY_GOOD/GOOD; peak sun hours 5.1-6.6; dust soiling 2.1-3.5%
  - 12 solar farms: Bungala SA (220MW SAT, CF 21.0%), Darlington Point NSW (275MW fixed, CF 19.8%), Limondale NSW (249MW SAT, CF 20.7%), Finley NSW (133MW fixed, CF 19.3%), Rugby Run NSW (110MW SAT, CF 20.8%), Ouyen VIC (180MW fixed), Sunlands SA (100MW SAT, CF 21.9%), DeGrussa WA (10.6MW fixed, CF 23.6%), Molong NSW (93MW bifacial), Merredin WA (120MW dual-axis, CF 24.6%), Hornsdale Solar Reserve SA (315MW SAT), Kalgoorlie WA (85MW fixed); specific yield 1680-2157 kWh/kWp
  - 120 monthly irradiance records (10 sites × 12 months): sinusoidal seasonal GHI pattern, southern hemisphere summer peak Jan/Dec; GHI 3.4-9.8 kWh/m²/day; DNI/DHI computed from GHI ratios
  - 25 degradation records (5 panel types × 5 year milestones 0/5/10/15/20yr): MONO_PERC (0.50%+0.55%/yr), POLY (0.70%+0.65%/yr — fastest degrader), BIFACIAL_MONO (0.45%+0.50%/yr), HJT (0.25%+0.40%/yr — lowest), TOPCon (0.30%+0.42%/yr); includes efficiency, PR, cumulative degradation, failure rate
- Endpoints: GET /api/solar-resource/dashboard, /sites, /farm-yields, /monthly-irradiance, /degradation (all with Depends(verify_api_key), tags=["Solar Resource"])
- TypeScript interfaces: IrradianceSiteRecord, SolarFarmYieldRecord, MonthlyIrradianceRecord, SolarDegradationRecord, SolarResourceDashboard appended to app/frontend/src/api/client.ts
- api methods: getSolarResourceDashboard, getSolarResourceSites, getSolarResourceFarmYields, getSolarResourceMonthlyIrradiance, getSolarResourceDegradation added to api object in client.ts
- New page: app/frontend/src/pages/SolarResourceAnalytics.tsx — 468 lines; dark theme; 4 KPI cards (Best Solar Resource Site, Avg CF%, Total MW, Avg Specific Yield); Monthly GHI multi-line chart (5 high-resource sites, 12 months, seasonal curves); Solar Farm Yields table with TechnologyBadge (FIXED_TILT gray/SAT amber/DUAL_AXIS green/BIFACIAL blue); Irradiance Sites table with ResourceClassBadge (EXCELLENT gold/VERY_GOOD green/GOOD blue/MODERATE gray); Panel Degradation line chart (5 panel types, years 0-20, showing HJT/TOPCon vs POLY degradation delta)
- App.tsx: SolarResourceAnalytics imported; SunMedium added to lucide-react imports (new icon); /solar-resource route added; "Solar Resource" nav item (SunMedium icon) added after Storage Revenue Stack, before Settings
- 5 TestSolarResourceEndpoints tests appended to tests/test_backend.py

## Sprint 49b — Electricity Futures Market Risk Analytics — 2026-02-20T00:00:00+11:00 (AEDT)

**Files modified:**
- `app/backend/main.py` — appended Sprint 49b models, mock data, and endpoints
- `app/frontend/src/api/client.ts` — appended TS interfaces + api methods
- `app/frontend/src/App.tsx` — added import, nav item, route
- `tests/test_backend.py` — appended TestFuturesMarketRiskEndpoints

**New file:**
- `app/frontend/src/pages/FuturesMarketRisk.tsx` — full page component

**Backend models (appended to main.py):**
- `VaRRecord` — 1-day 95%/99% VaR, CVaR, Greeks (delta, gamma, vega, theta) per region/portfolio type
- `FMR_HedgeEffectivenessRecord` — named with FMR_ prefix to avoid collision with Sprint 18a's `HedgeEffectivenessRecord`; hedge ratio %, instrument, gain/loss, effectiveness %, basis risk
- `BasisRiskRecord` — futures settlement vs spot, basis $/MWh, volatility, min/max basis, exposure $M
- `FuturesPositionRecord` — long/short/net MW, avg entry price, MTM $M, margin $M
- `FuturesMarketRiskDashboard` — aggregated KPIs + all sub-lists

**Mock data volumes:**
- 10 VaR records: 5 regions x 2 portfolio types (NSW1/QLD1/VIC1 → RETAILER+GENERATOR, SA1 → GENERATOR+TRADER, TAS1 → GENERATOR+RETAILER); VaR 95% range $0.9M–$11.2M; delta -1200 to +1850 MWh; full Greeks
- 15 hedge effectiveness records: AGL (3×NSW1), Origin (3×QLD1), EnergyAustralia (3×VIC1), Snowy Hydro (2×NSW1+1×QLD1), ERM Power (3×SA1); Q1–Q3 2024; hedge ratios 62–92%; effectiveness 71–94%; instruments FUTURES/SWAP/CAP/FLOOR/COLLAR/PPA
- 20 basis risk records: 5 regions × 4 quarters 2024; basis range -$17.5 to +$27.0/MWh; volatility 5.8–30.1; risk exposure $0.5M–$8.8M
- 12 futures position records: AGL (×2), Origin (×2), EnergyAustralia, Snowy Hydro, ERM Power, Macquarie Energy (×2), CS Energy, Trafigura, Alinta Energy; mix GENERATOR/RETAILER/GENTAILER/FINANCIAL; mark-to-market -$4.1M to +$7.4M

**Endpoints (all with Depends(verify_api_key), tag="Futures Market Risk"):**
- GET /api/futures-market-risk/dashboard → FuturesMarketRiskDashboard
- GET /api/futures-market-risk/var → list[VaRRecord]
- GET /api/futures-market-risk/hedge-effectiveness → list[FMR_HedgeEffectivenessRecord]
- GET /api/futures-market-risk/basis-risk → list[BasisRiskRecord]
- GET /api/futures-market-risk/positions → list[FuturesPositionRecord]

**Frontend page (FuturesMarketRisk.tsx):**
- Header: Activity icon (TrendingDown already used by Curtailment/LRMC/Min Demand; Activity re-used as nav icon), title + subtitle
- 4 KPI cards: Portfolio VaR 95% ($M, red), Avg Hedge Ratio % (blue), Total Open Interest (MW, green), Avg Basis Risk ($/MWh, amber)
- VaR chart: grouped bars (VaR 95% blue, VaR 99% red) per region + CVaR 95% line on right axis (orange); aggregated by region
- Hedge Effectiveness scatter: X=hedge ratio %, Y=effectiveness %; bubble size proportional to |gain/loss|; one scatter series per instrument; custom tooltip showing participant + quarter
- Basis Risk heatmap: table — rows=regions, cols=Q1–Q4 2024; cells color-coded red (negative) → gray (near-zero) → green (positive) with legend
- Futures Positions table: 10 columns including type badge, net MW (green/red sign), MTM (green/red), margin
- Hedge Effectiveness detail table: 10 columns including instrument badge, effectiveness % color-coded by threshold (>=85% green, >=75% amber, <75% red), basis risk color-coded by severity

**Instrument badges:** FUTURES blue, SWAP cyan, CAP amber, FLOOR teal, COLLAR purple, PPA green
**Participant type badges:** GENERATOR red, RETAILER blue, GENTAILER purple, FINANCIAL gray

**TypeScript interfaces appended to client.ts:** VaRRecord, FMRHedgeEffectivenessRecord, BasisRiskRecord, FuturesPositionRecord, FuturesMarketRiskDashboard
**api methods added to api object:** getFuturesMarketRiskDashboard, getFuturesMarketRiskVar, getFuturesMarketRiskHedgeEffectiveness, getFuturesMarketRiskBasisRisk, getFuturesMarketRiskPositions
**App.tsx:** FuturesMarketRisk imported; Activity already imported (used by Monitoring, Scenario, PASA, Demand Response, etc.); /futures-market-risk route added; "Futures Market Risk" nav item (Activity icon) added after Solar Resource, before Settings
**Tests:** 5 TestFuturesMarketRiskEndpoints tests appended to tests/test_backend.py

---

## Sprint 50b — Corporate PPA Market Analytics — 2026-02-20T00:00:00

**Files modified:**
- `app/backend/main.py` — appended Sprint 50b models + mock data + 5 endpoints (lines ~35521–35842)
- `app/frontend/src/pages/CorporatePpaMarket.tsx` — new page (NEW FILE)
- `app/frontend/src/api/client.ts` — appended 5 TypeScript interfaces; added 5 api methods to api object
- `app/frontend/src/App.tsx` — imported CorporatePpaMarket; added /corporate-ppa-market route; added "Corporate PPA Market" nav entry (FileText icon already present)
- `tests/test_backend.py` — appended TestCorporatePpaMarketEndpoints class (5 tests)

**Backend models (app/backend/main.py):**
- `CorporatePpaDeal` — 19 fields: deal_id, project_name, technology (WIND/SOLAR/HYBRID/STORAGE), state, offtaker_name, offtaker_sector (TECH/RETAIL/MINING/MANUFACTURING/FINANCE/GOVERNMENT), deal_type (PHYSICAL/FINANCIAL_FIRMING/SLEEVED/VIRTUAL), contract_length_years, capacity_mw, annual_energy_gwh, strike_price_mwh, market_price_at_signing, signing_date, first_delivery_date (nullable), additionality, bundled_lgcs, green_power_accredited
- `PpaOfftakerRecord` — 10 fields: offtaker_name, sector, total_contracted_mw, total_contracted_gwh, num_deals, avg_strike_price, earliest_deal_year, net_zero_target (nullable), re100_member, sustainability_rating (AAA/AA/A/BBB/BB)
- `PpaPriceTrendRecord` — 10 fields: year, quarter, technology, region, avg_strike_price_mwh, min_strike_price_mwh, max_strike_price_mwh, num_deals, total_capacity_mw, spot_price_comparison
- `PpaMarketSummaryRecord` — 9 fields: year, total_deals, total_capacity_mw, total_value_m_aud, avg_contract_years, physical_pct, financial_pct, additionality_pct, top_sector
- `CorporatePpaMarketDashboard` — aggregates all 4 lists + 4 KPI scalars

**Mock data volumes:**
- 15 PPA deals: Amazon Web Services (×2 VIC WIND), Google Australia (QLD SOLAR), Microsoft Australia (TAS WIND), BHP (×2 QLD/SA SOLAR+HYBRID), Rio Tinto (×2 NSW/QLD SOLAR+HYBRID), Fortescue (SA WIND), Coles (NSW SOLAR), Woolworths (QLD SOLAR), CBA (VIC WIND), ANZ (VIC HYBRID), Sydney Water (NSW SOLAR), NSW Gov (SA SOLAR), Clarke Creek (QLD HYBRID); strikes $42–$65/MWh; 2018–2024 signings; 10–20 year terms
- 10 offtaker records: AWS, Google, Microsoft, BHP, Rio Tinto, Fortescue, Coles, Woolworths, CBA, ANZ; aggregated totals; RE100 flags; net zero targets 2030–2050; ratings AAA–BBB
- 30 price trend records: WIND×3 regions (NSW/VIC/QLD) × 5 years (2020–2024) + SOLAR×3 regions × 5 years; price decline $74→$55 WIND, $65→$42 SOLAR
- 6 market summary records: 2019–2024; deals 5→50/yr; capacity 950→9100 MW/yr; additionality 90%→68%

**Endpoints (all with Depends(verify_api_key), tag="Corporate PPA Market"):**
- GET /api/corporate-ppa-market/dashboard → CorporatePpaMarketDashboard
- GET /api/corporate-ppa-market/deals → list[CorporatePpaDeal]
- GET /api/corporate-ppa-market/offtakers → list[PpaOfftakerRecord]
- GET /api/corporate-ppa-market/price-trends → list[PpaPriceTrendRecord]
- GET /api/corporate-ppa-market/market-summary → list[PpaMarketSummaryRecord]

**Frontend page (CorporatePpaMarket.tsx):**
- Header: FileText icon (already in lucide-react imports), title "Corporate Power Purchase Agreement (PPA) Market", subtitle about corporate renewable PPAs, offtaker analysis, contract structures, pricing benchmarks, additionality tracking
- 4 KPI cards: Total Contracted Capacity (MW), Avg PPA Price ($/MWh), Additionality %, YoY Growth %
- PPA Price Trend chart: LineChart — WIND avg (cyan), SOLAR avg (amber), Spot avg (gray dashed); data aggregated by year+technology across all 3 regions; Y-axis $30–$90/MWh
- Market Summary chart: ComposedChart (dual-axis) — Contracted Capacity MW bars (blue, left axis GW), Total Deals line (amber, right axis), Additionality % line (green dashed, right axis)
- PPA Deals table: 13 columns — project name, tech badge, state, offtaker, sector badge, deal type badge, MW, strike $/MWh (green), market $/MWh (gray), signed year, term (yr), additionality icon, LGC icon
- Top Offtakers table: 9 columns — offtaker, sector badge, contracted MW, contracted GWh, deals, avg strike, RE100 badge, net zero target, sustainability rating badge

**Badge color scheme:**
- Sector: TECH blue, RETAIL purple, MINING amber, MANUFACTURING orange, FINANCE green, GOVERNMENT teal
- Deal type: PHYSICAL green, FINANCIAL_FIRMING blue, SLEEVED gray, VIRTUAL purple
- Tech: WIND cyan, SOLAR yellow, HYBRID indigo, STORAGE pink
- Sustainability: AAA/AA dark/light emerald, A green, BBB amber, BB red

**TypeScript interfaces appended to client.ts:** CorporatePpaDeal, PpaOfftakerRecord, PpaPriceTrendRecord, PpaMarketSummaryRecord, CorporatePpaMarketDashboard
**api methods added to api object:** getCorporatePpaMarketDashboard, getCorporatePpaMarketDeals, getCorporatePpaMarketOfftakers, getCorporatePpaMarketPriceTrends, getCorporatePpaMarketSummary
**App.tsx:** CorporatePpaMarket imported; FileText already in lucide-react imports (used by Settlement, Regulatory, Reform); /corporate-ppa-market route added; "Corporate PPA Market" nav item (FileText icon) added before Settings
**Tests:** 5 TestCorporatePpaMarketEndpoints tests appended to tests/test_backend.py

## Sprint 50c — Microgrids & Remote Area Power Systems (RAPS) Analytics — 2026-02-20

**Completed:**
- `app/backend/main.py` — Appended 5 Pydantic models (MicrogridRecord, DieselDisplacementRecord, MicrogridEnergyRecord, OffGridTechnologyRecord, MicrogridDashboard), mock data (12 microgrids, 20 diesel displacement records, 48 energy records, 6 technology records), and 5 endpoints under `/api/microgrid-raps/*` (dashboard, microgrids, diesel-displacement, energy-records, technology-summary); all protected by `verify_api_key`
- `app/frontend/src/pages/MicrogridRaps.tsx` — New 464-line page: Wifi icon header, 4 KPI cards (Total Microgrids, Avg RF%, Total Diesel Displaced ML/yr, Total CO2 Avoided t/yr), horizontal RF bar chart (sorted desc, color-coded green/lime/amber/red), monthly energy mix stacked area chart with microgrid toggle, diesel displacement quarterly bar+line chart with state toggle, full Microgrid Registry table with community/grid type badges, technology summary grid cards
- `app/frontend/src/api/client.ts` — Appended 5 TypeScript interfaces (MicrogridRecord, DieselDisplacementRecord, MicrogridEnergyRecord, OffGridTechnologyRecord, MicrogridDashboard) and 5 API methods to `api` object (getMicrogridRapsDashboard, getMicrogridRapsMicrogrids, getMicrogridRapsDieselDisplacement, getMicrogridRapsEnergyRecords, getMicrogridRapsTechnologySummary)
- `app/frontend/src/App.tsx` — Imported MicrogridRaps; Wifi icon already in lucide-react imports; added `/microgrid-raps` route and "Microgrids & RAPS" nav item (Wifi icon) before Settings
- `tests/test_backend.py` — Appended `TestMicrogridRapsEndpoints` class with 5 tests (test_dashboard, test_microgrids, test_diesel_displacement, test_energy_records, test_technology_summary); all 5 pass

**Bug fixes (pre-existing issues resolved to enable tests):**
- `app/backend/main.py` — Replaced 6 instances of `@router.get(` with `@app.get(` (battery-economics and settlement endpoints used undefined `router` variable, blocking all test imports)
- `tests/conftest.py` — Added `client` pytest fixture (session-scoped FastAPI TestClient) so the many newer test classes using `def test_*(self, client)` pattern can resolve the fixture

**Mock data highlights:**
- 12 real-ish Australian remote microgrids: Doomadgee QLD, Coober Pedy SA, Mawson Station Antarctica, Lord Howe Island NSW, Esperance WA, Carnarvon WA, King Island TAS, Rottnest Island WA, Marble Bar WA, Windorah QLD, Nguiu NT, Wiluna WA
- Mix of community types (ABORIGINAL, PASTORAL, MINING, ISLAND, TOURISM, DEFENCE) and grid types (ISOLATED_DIESEL, HYBRID_SOLAR_DIESEL, FULL_RENEWABLE, PARTIAL_GRID)
- Renewable fractions range 30% (Mawson Station) to 100% (King Island, Rottnest Island)
- 20 diesel displacement records across QLD, WA, NT, SA, TAS showing quarterly improvement in renewable fraction through 2024
- 48 monthly energy records for 4 selected microgrids with seasonal solar patterns (high Dec/Jan/Feb, lower Jun/Jul)
- 6 technology summary records covering SOLAR_PV, WIND, BATTERY, DIESEL, FLYWHEEL, FUEL_CELL

## Sprint 51c — Thermal Power Plant Heat Rate & Efficiency Analytics — 2026-02-20

**Completed:**
- `app/backend/main.py` — Appended 5 Pydantic models (ThermalUnitRecord, HeatRateTrendRecord, FuelCostRecord, ThermalBenchmarkRecord, ThermalEfficiencyDashboard), comprehensive mock data (15 thermal unit records, 30 heat rate trend records, 20 fuel cost records, 15 benchmark records), and 5 endpoints under `/api/thermal-efficiency/*` (dashboard, units, heat-rate-trends, fuel-costs, benchmarks); all protected by `verify_api_key`
- `app/frontend/src/pages/ThermalEfficiency.tsx` — New page: Thermometer icon header, 4 KPI cards (Fleet Avg Heat Rate GJ/MWh, Fleet Avg Efficiency %, Worst Performing Unit, Total Fuel Cost $B AUD/yr), heat rate scatter chart (design vs actual, diagonal benchmark line, color by technology), heat rate trend multi-line chart (6 units × 2020-2024, overhaul markers via ReferenceLine), thermal units table with technology filter and degradation color coding (red >10%, amber >5%), fuel cost SRMC table sortable by SRMC/all-in/fuel cost/year
- `app/frontend/src/api/client.ts` — Appended 5 TypeScript interfaces (ThermalUnitRecord, HeatRateTrendRecord, FuelCostRecord, ThermalBenchmarkRecord, ThermalEfficiencyDashboard) and 5 API methods (getThermalEfficiencyDashboard, getThermalEfficiencyUnits, getThermalEfficiencyHeatRateTrends, getThermalEfficiencyFuelCosts, getThermalEfficiencyBenchmarks) added to `api` object
- `app/frontend/src/App.tsx` — Imported ThermalEfficiency; Thermometer already in lucide-react imports (used by /weather-demand); added `/thermal-efficiency` route and "Thermal Efficiency" nav item (Thermometer icon) before Settings
- `tests/test_backend.py` — Appended `TestThermalEfficiencyEndpoints` class with 5 tests (test_dashboard, test_units, test_heat_rate_trends, test_fuel_costs, test_benchmarks)

**Mock data highlights:**
- 15 real-ish Australian thermal units: Eraring 1-4 NSW (720 MW each, black coal), Vales Point 5-6 NSW (660 MW), Bayswater 3 NSW (665 MW), Loy Yang A1-2 VIC (brown coal), Yallourn W3 VIC (360 MW), Torrens Island B3 SA (gas steam), Pelican Point 1 SA (CCGT 478 MW), Mortlake 1 VIC (OCGT 282 MW), Darling Downs 1 QLD (CCGT 630 MW), Callide C3 QLD (black coal 450 MW)
- Heat rates: black coal 9.6-12.1 GJ/MWh, brown coal 13.9-14.8 GJ/MWh, CCGT 6.9-7.2 GJ/MWh, OCGT 10.2 GJ/MWh, gas steam 11.9 GJ/MWh
- 30 heat rate trend records: 6 units × 5 years (2020-2024) showing gradual degradation with step improvements after major overhauls (2023 for most units); Callide C3 shows anomalous 2022 degradation reflecting real-world unplanned outage
- 20 fuel cost records: 5 units × 4 years; coal $3-6.2/GJ, gas $8.5-20/GJ; SRMC ranges from $12.8/MWh (Loy Yang brown coal) to $141/MWh (Darling Downs 2022 gas crisis)
- 15 benchmark records: 5 technologies × 3 benchmark types (BEST_PRACTICE, AVERAGE, FLEET_BOTTOM); best practice CCGT 6.5 GJ/MWh, best practice black coal 9.0 GJ/MWh

**Technology color scheme:** BLACK_COAL dark gray #4b5563, BROWN_COAL brown #92400e, GAS_CCGT blue #3b82f6, GAS_OCGT gray #6b7280, GAS_STEAM cyan #06b6d4

## Sprint 51a — Electricity Market Liquidity & Trading Volume Analytics — 2026-02-20

**Completed:**
- `app/backend/main.py` — Appended 5 Pydantic models (TradingVolumeRecord, BidAskSpreadRecord, MarketDepthRecord, LiquidityMetricRecord, MarketLiquidityDashboard), comprehensive mock data (20 trading volume records across 4 regions × 5 dates with ASX/OTC/BILATERAL/EXCHANGE venues; 15 bid-ask spread records across 3 regions × 5 products with spreads $0.5-$8/MWh and market depth 50-500 MW; 20 market depth records across 4 regions × 5 price levels showing bid/ask order book; 20 liquidity metric records across 4 regions × 5 quarters with turnover ratio 0.5-3.0 and exchange share 30-60%), and 5 endpoints under `/api/market-liquidity/*` (dashboard, trading-volumes, bid-ask-spreads, market-depth, metrics); all protected by `verify_api_key`
- `app/frontend/src/pages/MarketLiquidity.tsx` — New page (536 lines): BarChart icon header with title "Electricity Market Liquidity & Trading Volume" and NEM/ASX subtitle; 4 KPI cards (Total Daily Volume GWh, Avg Bid-Ask Spread $/MWh, Exchange Share %, Turnover Ratio); stacked bar + VWAP line ComposedChart for trading volumes by venue; AreaChart for market share evolution by quarter (Exchange/OTC/Bilateral % stack); horizontal RechartsBarChart order book depth visualization (bid blue left / ask red right) with region selector; BidAskSpreadRecord table with product and venue color badges; quarterly LiquidityMetricRecord table with turnover ratio and HHI
- `app/frontend/src/api/client.ts` — Appended 5 TypeScript interfaces (TradingVolumeRecord, BidAskSpreadRecord, MarketDepthRecord, LiquidityMetricRecord, MarketLiquidityDashboard) and 5 API methods (getMarketLiquidityDashboard, getMarketLiquidityTradingVolumes, getMarketLiquidityBidAskSpreads, getMarketLiquidityMarketDepth, getMarketLiquidityMetrics) added to `api` object
- `app/frontend/src/App.tsx` — Imported MarketLiquidity; added `BarChart` (without suffix) to lucide-react imports (BarChart2/BarChart3 already used); added `/market-liquidity` route and "Market Liquidity" nav item (BarChart icon) before Settings
- `tests/test_backend.py` — Appended `TestMarketLiquidityEndpoints` class with 5 tests (test_dashboard, test_trading_volumes, test_bid_ask_spreads, test_market_depth, test_metrics)

**Mock data highlights:**
- 20 trading volume records: 4 NEM regions (NSW1, QLD1, VIC1, SA1) × 5 dates (Jan 15-19 2025); venue rotates ASX/OTC_BROKER/BILATERAL/EXCHANGE by region; product rotates BASE_LOAD/PEAK/CAP/FLOOR/SWAP by date; VWAP ranges $50-$120/MWh; volumes 200-520 MW per record
- 15 bid-ask spreads: 3 regions × 5 products; spreads range from $0.5 (NSW BASE_LOAD) to $8+/MWh (SA SWAP); market depth 50-500 MW; 3-9 market makers per product
- 20 market depth records: 4 regions × 5 price levels ($60-$100); cumulative bid/ask volumes build up the order book ladder for the horizontal chart
- 20 liquidity metrics: 4 regions × 5 quarters (2024-Q1 to 2025-Q1); exchange share 30-60%, OTC 20-40%, bilateral 10-30%; turnover ratio 0.5-3.0x; HHI 0.15-0.34 (moderate concentration)

**Product badge colors:** BASE_LOAD blue, PEAK amber, CAP red, FLOOR green, SWAP gray
**Venue badge colors:** ASX blue, OTC_BROKER green, BILATERAL gray, EXCHANGE teal

## Sprint 51b — Industrial Demand Flexibility & Load Management Analytics — 2026-02-20

**Completed:**
- `app/backend/main.py` — Appended 5 Pydantic models (LargeConsumerRecord, FlexibilityEventRecord, IndustrialLoadShapeRecord, DemandFlexAggregateRecord, IndustrialDemandFlexDashboard), comprehensive mock data, and 5 endpoints under `/api/industrial-demand-flex/*` (dashboard, consumers, events, load-shapes, aggregate); all protected by `verify_api_key`
- `app/frontend/src/pages/IndustrialDemandFlex.tsx` — New page (635 lines): Factory icon header with title "Industrial Demand Flexibility & Load Management" and large industrial consumers subtitle; 4 KPI cards (Total Flex Capacity MW, Activated Events 2024, Total DR Revenue $M, Avg Response Accuracy %); horizontal bar chart for flexibility capacity by consumer with per-industry-type colors (Cell-based coloring); events table with RERT_ACTIVATION/PRICE_SIGNAL/NETWORK_SUPPORT/VOLUNTARY badges, accuracy color-coding, and settlement/grid-benefit columns; large consumers table with industry type and contract type badges, sustainability score 0-10 bar; seasonal load shape chart (multi-line SUMMER/WINTER/SHOULDER baseline + flex band) with consumer toggle buttons
- `app/frontend/src/api/client.ts` — Appended 5 TypeScript interfaces (LargeConsumerRecord, FlexibilityEventRecord, IndustrialLoadShapeRecord, DemandFlexAggregateRecord, IndustrialDemandFlexDashboard) and 5 API methods (getIndustrialDemandFlexDashboard, getIndustrialDemandFlexConsumers, getIndustrialDemandFlexEvents, getIndustrialDemandFlexLoadShapes, getIndustrialDemandFlexAggregate) added to `api` object
- `app/frontend/src/App.tsx` — Imported IndustrialDemandFlex; added Factory to lucide-react imports; added `/industrial-demand-flex` route and "Industrial Demand Flex" nav item (Factory icon) before Settings
- `tests/test_backend.py` — Appended `TestIndustrialDemandFlexEndpoints` class with 5 tests (test_dashboard, test_consumers, test_events, test_load_shapes, test_aggregate)

**Mock data highlights:**
- 12 large consumer records: Tomago Aluminium NSW (380 MW flex, RERT), BlueScope Steel Port Kembla NSW (210 MW, WHOLESALE_DR), BHP Olympic Dam SA (60 MW, NETWORK_DR), Santos Moomba SA (45 MW, SPOT_RESPONSE), Snowy Hydro Guthega NSW (160 MW pumping, RERT), Loy Yang Power Aux VIC (35 MW, NETWORK_DR), Sydney Desalination Plant NSW (80 MW, WHOLESALE_DR), Kwinana Industrial WA (90 MW, NETWORK_DR), Alcoa Portland VIC (400 MW, RERT), Rio Tinto Yarwun QLD (130 MW, WHOLESALE_DR), BHP Nickel West WA (55 MW, SPOT_RESPONSE), AWS Sydney DC NSW (25 MW, WHOLESALE_DR); sustainability scores 3.2-9.4
- 15 flexibility event records (2024 Jan-Dec): 4 RERT activations (prices $14,800-$15,200/MWh, settlements $680K-$1.35M), 5 price signals ($3,200-$8,200/MWh), 4 network support events (zero trigger price, paid separately), 2 voluntary reductions; response accuracy 80-99.3%
- 18 load shape records: 3 consumers (Tomago Al., Alcoa Portland, BlueScope Steel) × 3 seasons × 2 representative hours (00:00 and 12:00); baseline MW, min curtailable MW, flexibility band MW, spot response threshold ($/MWh) per record
- 20 aggregate records: 5 regions (NSW1, VIC1, SA1, QLD1, WEM) × 4 quarters (Q1-Q4 2024); enrolled consumers 1-4, flex capacity 105-1155 MW, activated events 0-5, revenue $0-4.2M per region/quarter

**Industry type badge colors:** EAF_STEEL red, ALUMINIUM silver/gray, DATA_CENTRE blue, DESALINATION cyan, CHEMICALS orange, MINING amber, CEMENT gray
**Contract type badge colors:** RERT red, WHOLESALE_DR blue, NETWORK_DR green, SPOT_RESPONSE amber
**Event type badge colors:** RERT_ACTIVATION red, PRICE_SIGNAL amber, NETWORK_SUPPORT blue, VOLUNTARY gray

## Sprint 52a — Energy Storage LCA & Sustainability Analytics — 2026-02-20

**Completed:**
- `app/backend/main.py` — Appended 5 Pydantic models (StorageLcaRecord, CriticalMineralRecord, RecyclingRecord, LcaScenarioRecord, StorageLcaDashboard), comprehensive mock data, and 5 endpoints under `/api/storage-lca/*` (dashboard, lca-records, critical-minerals, recycling, scenarios); all protected by `verify_api_key`
- `app/frontend/src/pages/StorageLca.tsx` — New page (512 lines): RefreshCw icon header with title "Energy Storage Life Cycle Assessment & Sustainability" and embodied carbon / circular economy subtitle; 4 KPI cards (Best Lifecycle Technology, Avg Recyclability %, Critical Minerals at Risk, Technologies Assessed); stacked bar chart for lifecycle carbon by phase (embodied/operational/EOL) sorted by total; multi-line scenario chart showing lifecycle carbon reduction trajectory CURRENT vs 2030_GRID_CLEAN per technology; full LCA detail table sorted by total lifecycle carbon with energy payback / water / land use columns; Critical Mineral Supply Chain table with supply risk, price trend, and circular economy potential badges; End-of-Life Recycling table with process, maturity, and key players
- `app/frontend/src/api/client.ts` — Added 5 TypeScript interfaces (StorageLcaRecord, CriticalMineralRecord, RecyclingRecord, LcaScenarioRecord, StorageLcaDashboard) appended after existing interfaces; added 5 API methods (getStorageLcaDashboard, getStorageLcaRecords, getStorageLcaCriticalMinerals, getStorageLcaRecycling, getStorageLcaScenarios) inside the `api` object
- `app/frontend/src/App.tsx` — Imported StorageLca; added `RefreshCw` to lucide-react imports; added `/storage-lca` route and "Storage LCA" nav item (RefreshCw icon) before Settings
- `tests/test_backend.py` — Appended `TestStorageLcaEndpoints` class with 5 tests (test_dashboard, test_lca_records, test_critical_minerals, test_recycling, test_scenarios)

**Mock data highlights:**
- 8 LCA records: Li-Ion NMC (183 kgCO2/kWh total, 68% recyclability), LFP (136, 75%), NMC 811 (159.5, 72%), Na-Ion (103, 80%), Flow Battery Vanadium (192, 90%), Compressed Air (103, 65%), Pumped Hydro (67 — best, 85%), Gravity Storage (69.5, 95%)
- 12 critical mineral records: Lithium (x2 techs, AU 57% reserves, MEDIUM risk, FALLING price), Cobalt (x2, AU 1.2%, CRITICAL/HIGH, RISING), Nickel (x2, AU 22%, MEDIUM/HIGH), Manganese (x2, AU 8%, LOW), Graphite (x2, AU 1.5%, HIGH, RISING), Vanadium (x2, AU 3.5%, HIGH/LOW)
- 8 recycling records: NMC Hydromet (95% recovery, COMMERCIAL), NMC Pyrmet (70%, MATURE), NMC Direct (90%, PILOT), LFP Hydromet (88%, COMMERCIAL), LFP Direct (92%, EMERGING), NMC 811 Hydromet (93%, PILOT), Flow Vanadium Remanufacturing (95%, COMMERCIAL), Na-Ion Hydromet (80%, EMERGING)
- 24 LCA scenario records: 8 technologies x 3 scenarios (CURRENT 2024, 2030_GRID_CLEAN, 2035_GREEN_MANUFACTURING); showing progressive carbon reduction; vs_gas_peaker_ratio and vs_diesel_ratio computed vs 620 and 820 kgCO2/kWh baselines

**Technology palette:** Li-Ion NMC blue (#3b82f6), LFP green (#22c55e), NMC 811 teal (#0d9488), Na-Ion purple (#8b5cf6), Flow Battery Vanadium amber (#f59e0b), Compressed Air gray (#6b7280), Pumped Hydro cyan (#06b6d4), Gravity dark (#1e293b)
**Supply risk badge colors:** LOW green, MEDIUM amber, HIGH orange, CRITICAL red
**Process badge colors:** all gray-700 (process name shown as human-readable label)
**Maturity badge colors:** EMERGING blue, PILOT amber, COMMERCIAL green, MATURE gray

## Sprint 53c — Firming Technology Economics — 2026-02-20

**Completed:**
- `app/backend/main.py` — Appended 5 Pydantic models (FirmingTechnologyRecord, FirmingDispatchRecord, FirmingCostCurveRecord, FirmingScenarioRecord, FirmingTechDashboard), realistic NEM mock data (8 technology records, 16 dispatch records across 2 scenarios, 15 cost curve records across 3 VRE scenarios, 3 scenario records), and 1 endpoint `GET /api/firming-technology/dashboard` protected by `verify_api_key`
- `app/frontend/src/pages/FirmingTechnologyEconomics.tsx` — New page: Flame icon header with title "Firming Technology Economics" and NEM firming description; 4 KPI cards (cheapest LCOS, fastest response, lowest CO2, total firming capacity GW); technology comparison table with CAPEX/OPEX/LCOS/response/CO2/maturity badge columns; LCOS vs Duration scatter chart with bubble size = capacity factor and category-coloured bubbles; stacked revenue bar chart (dispatch + capacity payments) with per-scenario filter toggle; firming cost curve line chart for 3 VRE scenarios vs firming requirement %; scenario comparison cards (HIGH/MEDIUM/LOW VRE)
- `app/frontend/src/api/client.ts` — Appended 5 TypeScript interfaces (FirmingTechnologyRecord, FirmingDispatchRecord, FirmingCostCurveRecord, FirmingScenarioRecord, FirmingTechDashboard); added `getFirmingTechDashboard()` method to `api` object
- `app/frontend/src/App.tsx` — Imported FirmingTechnologyEconomics; added `/firming-technology-economics` route and "Firming Tech Economics" nav item (Flame icon)
- `tests/test_backend.py` — Appended `TestFirmingTechnologyEconomics` class with `test_firming_technology_dashboard()` validating status 200, 8 technologies, 16 dispatch records, 15 cost curves, 3 scenarios, and field shapes

**Mock data highlights:**
- 8 technologies: OCGT ($185/MWh LCOS, 580 kg/MWh CO2, COMMERCIAL), CCGT ($145, 390, COMMERCIAL), H2 Turbine ($320, 5, DEMONSTRATION), Battery 4HR ($135, 28, COMMERCIAL), Battery 8HR ($175, 28, COMMERCIAL), Pumped Hydro ($115, 12 — cheapest, COMMERCIAL), Biomass ($235, 25, COMMERCIAL), Demand Response ($65 — lowest LCOS, 0 CO2, COMMERCIAL)
- 3 VRE scenarios: HIGH (90% VRE, 18.5 GW firming, $95/MWh LCOE), MEDIUM (70%, 13.2 GW, $82/MWh), LOW (50%, 8.8 GW, $72/MWh)
- Cost curves: firming cost rises from $28–45/MWh at 10% requirement to $84–132/MWh at 30%, steeper for higher VRE scenarios

## Sprint 53b — Electricity Demand Forecasting Models (2026-02-20)

Added comparative analytics page for ML-based electricity demand forecasting across NEM regions.

**Backend (`app/backend/main.py`):**
- Pydantic models: `DFMModelRecord`, `DFMForecastRecord`, `DFMSeasonalPatternRecord`, `DFMFeatureImportanceRecord`, `DemandForecastModelsDashboard`
- Endpoint: `GET /api/demand-forecast-models/dashboard` — returns 21 model records (6 model types × 5 NEM regions), 48 hourly forecast records (24 h × 2 models for NSW1), 20 seasonal pattern records, and 33 feature importance records with realistic NEM values
- All names prefixed `DFM` to avoid collisions with existing `DemandForecast*` models

**Frontend (`app/frontend/src/pages/DemandForecastingModels.tsx`):**
- 4 KPI cards: best MAPE %, best RMSE, best R², total model count
- Sortable model performance comparison table with per-region filter and green highlight for best MAPE
- 24-hour ComposedChart (NSW1) with actual vs LSTM vs Ensemble lines and 90% CI bands
- Horizontal grouped BarChart for feature importance (LSTM / XGBoost / Ensemble, NSW1)
- Seasonal patterns table with color-coded peak demand and temperature sensitivity

**API client (`app/frontend/src/api/client.ts`):** Added TypeScript interfaces and `getDemandForecastModelsDashboard()` method.
**Routing (`app/frontend/src/App.tsx`):** Import, nav entry (`Brain` icon), and route at `/demand-forecasting-models`.
**Tests (`tests/test_backend.py`):** `TestDemandForecastingModels.test_demand_forecast_models_dashboard` validates all response fields, record counts, and data constraints.

## Sprint 53a — Energy Market Stress Testing — 2026-02-20

**Completed:**
- `app/backend/main.py` — Added Pydantic models (`MarketStressScenario`, `StressTestResult`, `SystemVulnerabilityRecord`, `StressTestKpiRecord`, `MarketStressDashboard`) and `GET /api/market-stress/dashboard` endpoint with realistic NEM mock data: 8 stress scenarios (heatwave, gas disruption, Basslink trip, SCADA cyber attack, wind drought, solar eclipse, major generator failure, combined compound extreme), 20 regional stress results across 5 NEM regions, 6 component vulnerability records, 8 KPI records.
- `app/frontend/src/pages/MarketStressTesting.tsx` — New React/TypeScript analytics page with: 4 KPI summary cards (worst-case price spike, max unserved energy, most vulnerable component, highest economic cost); 8-row scenario severity matrix table with colour-coded severity badges; RadarChart showing system vulnerability profile across 6 NEM components; grouped BarChart comparing economic cost and unserved energy across all 8 scenarios; colour-coded vulnerability heatmap table with inline score bars; detailed stress results table with baseline vs stressed comparison. Tailwind dark theme (bg-gray-900/bg-gray-800), Recharts, lucide-react icons.
- `app/frontend/src/api/client.ts` — Appended TypeScript interfaces (`MarketStressScenario`, `StressTestResult`, `SystemVulnerabilityRecord`, `StressTestKpiRecord`, `MarketStressDashboard`) and `getMarketStressDashboard()` exported function.
- `app/frontend/src/App.tsx` — Added `MarketStressTesting` import, `/market-stress-testing` route, and nav item with `ShieldAlert` icon.
- `tests/test_backend.py` — Appended `TestMarketStressTesting` class with `test_market_stress_dashboard()` covering top-level structure, scenario enum validation, vulnerability score bounds, and KPI record counts.

## Sprint 54a — NEM Frequency Control Analytics — 2026-02-20

**Completed:**
- `app/backend/main.py` — Added Pydantic models (`NFCFrequencyRecord`, `NFCEventRecord`, `NFCContributorRecord`, `NFCPerformanceRecord`, `FrequencyControlDashboard`) and `GET /api/frequency-control/dashboard` endpoint with realistic NEM mock data: 12 monthly frequency performance records for NSW1 (2024), 8 major frequency events across NEM regions (covering GENERATOR_TRIP, LOAD_REJECTION, INTERCONNECTOR_SEPARATION, DEMAND_FORECAST_ERROR triggers), 10 technology contributor records (BESS, Hydro, OCGT, CCGT, Coal, Solar, Wind, PHES, Demand Response, VPP), and 12 monthly performance records tracking compliance, FCAS shortfalls, PFR adequacy, average nadir, and ROCOF.
- `app/frontend/src/pages/FrequencyControlAnalytics.tsx` — New React/TypeScript analytics page with: 4 KPI summary cards (current avg frequency, time-in-band %, worst nadir Hz, PFR compliance rate %); AreaChart of monthly avg frequency with reference lines at 49.85 and 50.15 Hz band boundaries; event severity table sorted by nadir Hz ascending (worst first) with colour-coded trigger badges, nadir, ROCOF, recovery time, and unserved energy columns; horizontal BarChart of PFR MW response by technology coloured by response speed (green ≤ 500 ms, amber ≤ 1500 ms, red > 1500 ms); dual-axis LineChart showing compliance rate %, PFR adequacy %, and FCAS shortfall event count over 12 months. Tailwind dark theme (bg-gray-900/bg-gray-800), Recharts, lucide-react Activity icon.
- `app/frontend/src/api/client.ts` — Appended TypeScript interfaces (`NFCFrequencyRecord`, `NFCEventRecord`, `NFCContributorRecord`, `NFCPerformanceRecord`, `FrequencyControlDashboard`) and `getFrequencyControlDashboard()` exported function.
- `app/frontend/src/App.tsx` — Added `FrequencyControlAnalytics` import, `/frequency-control-analytics` route, and nav item with `Activity` icon.
- `tests/test_backend.py` — Appended `TestFrequencyControlAnalytics` class with `test_frequency_control_dashboard()` covering top-level structure validation, 12 frequency records with plausibility bounds, 8 events with trigger enum validation, 10 contributor records with contribution-percentage sum check (~100%), and 12 performance records with all numeric field bounds.

## Sprint 54b — NEM Capacity Investment Signals — 2026-02-20

**Completed:**
- `app/backend/main.py` — Added Pydantic models (`CISNewEntrantRecord`, `CISInvestmentActivityRecord`, `CISPriceSignalRecord`, `CISExitRiskRecord`, `CapacityInvestmentDashboard`) and `GET /api/capacity-investment/dashboard` endpoint with comprehensive NEM mock data: 8 new entrant cost records covering Utility Solar, Onshore Wind, BESS 2h, OCGT, CCGT, Pumped Hydro, Offshore Wind, and Green Hydrogen with CAPEX, WACC, LCOE, breakeven price, payback years, NPV at $85/MWh average, and IRR; 20 investment activity records (5 years 2020-2024 × 4 technologies) with committed/cancelled MW and financing secured %; 20 price signal records (5 NEM regions × 4 years 2021-2024) with revenue adequacy signal enum (STRONG/ADEQUATE/WEAK/INSUFFICIENT); 10 exit risk records for real NEM units with age, remaining life, exit probability, trigger type, and capacity.
- `app/frontend/src/pages/CapacityInvestmentSignals.tsx` — New React/TypeScript analytics page with: 4 KPI summary cards (cheapest new entrant LCOE, total 2024 committed capacity, regions with STRONG signal, total exit risk capacity); new entrant cost stacked BarChart with CAPEX + WACC Uplift breakdown and $85/MWh average breakeven ReferenceLine; investment activity stacked BarChart (committed vs cancelled by technology by year); average spot price LineChart by region over 2021-2024 with LRMC reference line; revenue adequacy signal heatmap table colour-coded by strength (green/yellow/orange/red) across 5 regions × 4 years with $/MWh inlay; exit risk register table sorted by exit probability with inline progress bars (red/orange/yellow) and trigger badges. Tailwind dark theme (bg-gray-900/bg-gray-800), Recharts, lucide-react icons.
- `app/frontend/src/api/client.ts` — Appended TypeScript interfaces (`CISNewEntrantRecord`, `CISInvestmentActivityRecord`, `CISPriceSignalRecord`, `CISExitRiskRecord`, `CapacityInvestmentDashboard`) and `getCapacityInvestmentDashboard()` exported function.
- `app/frontend/src/App.tsx` — Added `CapacityInvestmentSignals` import, `/capacity-investment-signals` route, and nav item with `TrendingUp` icon.
- `tests/test_backend.py` — Appended `TestCapacityInvestmentSignals` class with `test_capacity_investment_dashboard()` covering all response keys, record counts (8 new entrant, 20 activity, 20 price signal, 10 exit risk), enum validation, value bounds, and cross-record consistency checks.

## Sprint 54c — REC & PPAs Certificate Tracking — 2026-02-20

Added Australian RET scheme tracking page for LGCs, STCs, GreenPower, and corporate renewable matching.

**Backend (`app/backend/main.py`):**
- Pydantic models: `RCTLgcPriceRecord`, `RCTSurplusDeficitRecord`, `RCTCreationRecord`, `RCTComplianceRecord`, `RCTGreenPowerRecord`, `RecCertificateDashboard`
- Endpoint: `GET /api/rec-tracking/dashboard` — returns 24 monthly LGC price records (2023-2024) with spot and forward curves (2026/2027), 8 LRET surplus/deficit records (2017-2024), 20 LGC creation records (5 technologies × 4 NEM regions: Wind, Large Solar, Hydro, Biomass/Waste, Rooftop Solar), 10 retailer compliance records with status enum (COMPLIANT/SHORTFALL/DEFERRED), 6 state GreenPower records (NSW/VIC/QLD/SA/WA/TAS)
- All model names prefixed `RCT` to avoid collisions with existing REC-related models

**Frontend (`app/frontend/src/pages/RecCertificateTracking.tsx`):**
- 4 KPI cards: current LGC spot price (Dec 2024 $78.40), 2024 LRET surplus (+3,200 GWh / +9.70%), total LGCs created (all techs & regions), GreenPower customers (k, nationwide)
- LGC price trend AreaChart — spot price with gradient fill + 2026/2027 forward curves as dashed lines over 24 months
- LRET surplus/deficit BarChart — annual surplus (positive) / deficit (negative) bars with zero ReferenceLine
- LGC creation by technology stacked BarChart — per NEM region, colour-coded by technology type
- Retailer compliance table — market share %, liable energy GWh, certificates surrendered, compliance status badge (green/red/amber with icon), shortfall charge AUD$M
- GreenPower state table — state badge, customers k, GWh, avg premium $/MWh, YoY growth % with colour coding

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces (`RCTLgcPriceRecord`, `RCTSurplusDeficitRecord`, `RCTCreationRecord`, `RCTComplianceRecord`, `RCTGreenPowerRecord`, `RecCertificateDashboard`) and `getRecCertificateDashboard()` exported function.
**Routing (`app/frontend/src/App.tsx`):** Import, nav entry (`Award` icon), and route at `/rec-certificate-tracking`.
**Tests (`tests/test_backend.py`):** `TestRecCertificateTracking.test_rec_certificate_dashboard` validates top-level structure, 24 LGC price records with forward/spot proximity, 8 surplus/deficit records with correct 33,000 GWh target, 20 creation records with tech/region enum checks, 10 compliance records with status enum validation and zero-shortfall-charge constraint for COMPLIANT retailers, 6 GreenPower records covering all states.

## Sprint 55a — NEM Spot Market Depth & Order Flow Analytics — 2026-02-20

Added NEM Spot Market Depth & Order Flow Analytics page covering real-time bid stacks, dispatch interval market depth, and participant order flow patterns.

**Backend (`app/backend/main.py`):**
- Pydantic models: `SMDBidStackRecord`, `SMDOrderFlowRecord`, `SMDMarketDepthSnapshot`, `SMDParticipantFlowRecord`, `SpotMarketDepthDashboard` (all prefixed `SMD` to avoid collisions; `DispatchInterval` already existed)
- Endpoint: `GET /api/spot-depth/dashboard` — returns 20 bid stack records (10 price bands × 2 regions: NSW1, VIC1; technologies: Hydro, Wind, Large Solar, Black/Brown Coal, Gas CCGT/OCGT, Demand Response, Diesel), 15 order flow records across all 5 NEM regions with buy/sell volumes and net flow, 5 market depth snapshots (one per NEM region: NSW1/VIC1/QLD1/SA1/TAS1) with bid/offer depth, best bid/ask, spread, and imbalance ratio, 8 participant strategic flow records with market share, rebid frequency, and strategic withholding score (0-10)

**Frontend (`app/frontend/src/pages/SpotMarketDepthAnalytics.tsx`):**
- 4 KPI cards: total bid depth (all regions combined, MW/GW), average bid-ask spread (AUD/MWh), widest spread region with spread value, highest withholding score participant with score and region
- Bid stack stepped AreaChart — cumulative MW vs price band using `stepAfter` type, coloured dots per technology with a technology legend; region filter buttons for NSW1/VIC1
- Order flow ComposedChart — dual-axis buy/sell volume BarChart with net flow Line overlay (purple), ReferenceLine at zero, interval labels showing time and region
- Market depth snapshots table — 5 NEM regions with bid/ask depth, best bid (green), best ask (red), spread (amber), and imbalance ratio (colour-coded: red >1.1 oversupply pressure, blue <0.9 undersupply, green balanced)
- Participant strategic analysis table — sorted descending by withholding score, with colour-coded score badges (green 0-4, amber 4-7, red 7-10)

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces (`SMDBidStackRecord`, `SMDOrderFlowRecord`, `SMDMarketDepthSnapshot`, `SMDParticipantFlowRecord`, `SpotMarketDepthDashboard`) and `getSpotMarketDepthDashboard()` exported function.
**Routing (`app/frontend/src/App.tsx`):** Import, nav entry (`Layers` icon at `/spot-market-depth`), and `Route` element `<SpotMarketDepthAnalytics />`.
**Tests (`tests/test_backend.py`):** `TestSpotMarketDepthAnalytics.test_spot_depth_dashboard` validates top-level structure, 20 bid stack records with region/technology/price-band checks, 15 order flow records with positive volumes and typed net flow, 5 depth snapshots covering all NEM regions with spread/imbalance/best-bid-ask constraints, 8 participant flow records with score bounds (0-10) and market share (0-100%).

---

## Sprint 55c — Energy Storage Technology Roadmap (2026-02-20)

**New page:** `app/frontend/src/pages/StorageTechRoadmap.tsx` — route `/storage-tech-roadmap`, nav entry "Storage Tech Roadmap" with `GitBranch` icon.

**Backend (`app/backend/main.py`):** Appended Pydantic models (`STRTechnologyRecord`, `STRCostTrajectoryRecord`, `STRDeploymentMilestoneRecord`, `STRMarketForecastRecord`, `StorageTechRoadmapDashboard`) and `GET /api/storage-roadmap/dashboard` endpoint with:
- 10 technology records covering Li-Ion NMC/LFP, Solid-State, Na-Ion, Flow Vanadium/Zinc, CAES, Gravity, LAES, Green Hydrogen Storage
- 50 cost trajectory records (10 techs × 5 years 2024–2028)
- 15 deployment milestone records across all technologies
- 30 market forecast records (6 technologies × 5 years 2024–2028)

**Frontend visualisations:**
- Header + 4 KPI cards: commercial-stage count, cheapest 2030 LCOS target, total AU installed GWh, milestones on-track/achieved count
- Technology matrix table — 10 technologies with maturity badge, duration, current/target LCOS, cycle life, calendar life, AU installed
- LCOS cost trajectory LineChart — top 5 technologies 2024–2028 converging toward competitiveness
- Deployment forecast stacked AreaChart — cumulative GWh by 6 technologies 2024–2028
- Cost reduction horizontal BarChart — % LCOS reduction 2024→2028 by technology
- Milestone tracker table — Gantt-style with status badges (ACHIEVED/ON_TRACK/AT_RISK/NOT_STARTED)
- Technology scatter plot (ScatterChart) — energy density vs cycle life, bubble size = AU installed MWh

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces (`STRTechnologyRecord`, `STRCostTrajectoryRecord`, `STRDeploymentMilestoneRecord`, `STRMarketForecastRecord`, `StorageTechRoadmapDashboard`) and `getStorageTechRoadmapDashboard()`.

**Tests (`tests/test_backend.py`):** `TestStorageTechRoadmap.test_storage_tech_roadmap_dashboard` validates top-level structure, exactly 10 technology records with maturity enum and 2030 cost improvement constraint, 50 cost trajectory records spanning all 5 years and 10 technologies, 15 milestones with status enum validation, 30 market forecasts with correct baseline (0% cost reduction in 2024) and LFP 2028 deployment threshold.

## Sprint 55b — Renewable Integration Cost Analytics — 2026-02-20

Added Renewable Integration Cost Analytics page covering the system costs of integrating high VRE penetration — firming, network augmentation, FCAS markets, and curtailment.

**Backend (`app/backend/main.py`):**
- Pydantic enums: `RICCostComponent` (NETWORK_AUGMENTATION/FIRMING_CAPACITY/FCAS_MARKETS/CURTAILMENT_COST/SYSTEM_RESTART/INERTIA_SERVICES), `RICCurtailmentCause` (NETWORK_CONSTRAINT/DEMAND_LOW/OVERSUPPLY/DISPATCH_ORDER), `RICCostTrend` (RISING/STABLE/FALLING), `RICSystemService` (INERTIA/SYSTEM_RESTART/VOLTAGE_CONTROL/REACTIVE_POWER/FAST_FREQUENCY_RESPONSE)
- Pydantic models: `RICCostComponentRecord`, `RICNetworkAugRecord`, `RICCurtailmentRecord`, `RICSystemServiceRecord`, `RenewableIntegrationCostDashboard`
- Endpoint: `GET /api/integration-cost/dashboard` — returns 30 cost component records (5 years × 6 components: 2020-2024 with VRE penetration 24.5%→47.3%), 8 network augmentation projects (EnergyConnect, HumeLink, VNI-West, QNI-M, and 4 REZ enabling projects with BCR 1.9x-4.2x), 20 curtailment records (4 technologies × 5 years across SA/VIC/NSW/QLD with growing trend from 730 GWh in 2020 to 2,830 GWh in 2024), 5 system service records with INERTIA trending RISING, SYSTEM_RESTART STABLE, VOLTAGE_CONTROL and REACTIVE_POWER FALLING

**Frontend (`app/frontend/src/pages/RenewableIntegrationCost.tsx`):**
- 4 KPI cards: Total integration cost 2024 ($4,435 M), highest cost component (NETWORK AUGMENTATION at $1,380 M), total 2024 curtailment (2,830 GWh), network augmentation pipeline ($14,370 M across 8 projects)
- Stacked AreaChart — 6 cost components (indigo/amber/green/red/purple/cyan) stacked by year 2020-2024 showing VRE cost growth trajectory
- Network augmentation table — project name, region badge, investment (M AUD), VRE-enabled MW, cost/MW, commissioning year, BCR badge (green ≥3x, blue ≥2x, amber otherwise)
- Curtailment stacked BarChart — Large Solar/Wind/Rooftop PV GWh by year showing growing curtailment trend
- System services horizontal bar chart — 5 services with RISING/STABLE/FALLING trend badges, provider count, and VRE correlation description
- Cost intensity grid — AUD/MWh VRE for each of 6 components in 2024 with component-coloured values

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces (`RICCostComponentRecord`, `RICNetworkAugRecord`, `RICCurtailmentRecord`, `RICSystemServiceRecord`, `RenewableIntegrationCostDashboard`) and `getRenewableIntegrationCostDashboard()` exported function.
**Routing (`app/frontend/src/App.tsx`):** Added `GitMerge` to lucide-react import block; import `RenewableIntegrationCost`; nav entry (`GitMerge` icon at `/renewable-integration-cost`); `Route` element `<RenewableIntegrationCost />`.
**Tests (`tests/test_backend.py`):** `TestRenewableIntegrationCost.test_integration_cost_dashboard` validates top-level structure, 30 cost component records with all 6 components present in each of 5 years, 8 network augmentation projects with viable BCR (>1.0), 20 curtailment records with valid cause enums, 5 system service records covering all services, business logic assertions: 2024 VRE penetration highest, curtailment growing 2020→2024, INERTIA service has RISING trend.

## Sprint 56c — NEM Participant Market Share & Concentration Tracker — 2026-02-20

Added a NEM Participant Market Share & Concentration Tracker page covering generator portfolio concentration, market power analysis, and ownership changes across the NEM.

**Backend (`app/backend/main.py`):**
- Pydantic enum: `PMSCompetitionLevel` (COMPETITIVE/MODERATE/CONCENTRATED/HIGHLY_CONCENTRATED)
- Pydantic models: `PMSParticipantRecord`, `PMSConcentrationRecord`, `PMSOwnershipChangeRecord`, `PMSRegionalShareRecord`, `MarketShareDashboard`
- Endpoint: `GET /api/participant-market-share/dashboard` — returns 36 participant records (12 generators × 3 years: AGL/Origin/EnergyAustralia/Snowy/Alinta/CS Energy/Stanwell/Macquarie/Shell/BP/Tilt/Neoen with portfolio MW, renewable/thermal/storage breakdown, market share %, HHI contribution), 15 concentration records (5 regions × 3 years: NSW/QLD/VIC/SA/WA with HHI, CR3, CR5, competition level), 5 ownership change events (2020–2024: Macquarie/AGL/Origin/Neoen/Shell transactions with capacity, transaction value, regulatory status, HHI impact), 30 regional share records (6 participants × 5 regions with generation share %, capacity share %, peak share %, rebid events)

**Frontend (`app/frontend/src/pages/MarketShareTracker.tsx`):**
- Header with `Users` icon and descriptive subtitle
- Year selector (2022/2023/2024) controlling KPI cards and concentration table
- 4 KPI cards: top participant market share %, national average HHI, number of highly concentrated regions (HHI > 2,500), total M&A transaction value (M AUD)
- Portfolio stacked BarChart — 12 participants sorted by total MW, stacked Renewable (green) / Thermal (red) / Storage (indigo)
- Regional HHI grouped BarChart — 5 regions × 3 years showing concentration trend (3 shades of purple)
- Portfolio MW PieChart — top 8 participants + Others with participant-specific colours and inline percentage labels
- Concentration metrics table — region, HHI score (colour-coded by band), CR3%, CR5%, dominant participant, competition level badge
- Ownership change timeline table — year, acquirer, target/assets, capacity MW, transaction value, HHI impact (red positive / green negative), regulatory approval badge
- Regional generation share heatmap table — 6 participants × 5 regions with colour-coded cells (red ≥25%, amber ≥15%, blue ≥8%, green ≥3%, grey <3%)
- Market share trend LineChart — AGL/Origin/EnergyAustralia/Snowy 2022–2024 with distinct colour lines

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces (`PMSParticipantRecord`, `PMSConcentrationRecord`, `PMSOwnershipChangeRecord`, `PMSRegionalShareRecord`, `MarketShareDashboard`) and `getMarketShareDashboard()` exported function.

**Routing (`app/frontend/src/App.tsx`):** Import `MarketShareTracker`; nav entry (`Users` icon, label "Market Share", path `/market-share-tracker`); `Route` element `<MarketShareTracker />`.

**Tests (`tests/test_backend.py`):** `TestMarketShareTracker.test_market_share_dashboard` validates top-level structure; 36 participant records with valid market share %, HHI contribution, year enum; participant ID coverage (AGL/ORG/EA/SNOWY/TILT/NEOEN); 15 concentration records across NSW/QLD/VIC/SA/WA with CR5 ≥ CR3 constraint and valid competition level enums; 5 ownership change events with positive capacity and transaction value; 30 regional share records; business logic: WA HHI > 2,000 (monopoly-like market); total market share > 50%; total M&A value > $1B AUD.

## Sprint 56a — Generator Planned Outage & Maintenance Scheduling Analytics — 2026-02-20

Added Generator Planned Outage & Maintenance Scheduling Analytics page covering AEMO PASA planned outage submissions, maintenance windows, reserve margin impacts, conflict detection, and technology reliability KPIs.

**Backend (`app/backend/main.py`):**
- Pydantic enums: `GPOOutageType` (FULL/PARTIAL/DERATING), `GPOOutageReason` (MAJOR_OVERHAUL/MINOR_MAINTENANCE/REGULATORY_INSPECTION/FUEL_SYSTEM/ENVIRONMENTAL_COMPLIANCE), `GPOReserveStatus` (ADEQUATE/TIGHT/CRITICAL), `GPORiskLevel` (LOW/MEDIUM/HIGH/CRITICAL)
- Pydantic models: `GPOPlannedOutageRecord`, `GPOReserveMarginRecord`, `GPOOutageConflictRecord`, `GPOMaintenanceKpiRecord`, `PlannedOutageDashboard`
- Endpoint: `GET /api/planned-outage/dashboard` (requires `X-API-Key`) — returns 15 planned outage records (mix of Coal/Gas CCGT/Gas OCGT/Hydro across NSW/VIC/QLD/SA — Q1 2025), 20 weekly reserve margin records (5 regions × 4 weeks; VIC W3 at 3.4% CRITICAL), 5 outage conflict records (including CRITICAL QLD Callide+Kogan Creek overlap requiring AEMO intervention), 7 technology KPI records (Black Coal/Brown Coal/Gas CCGT/Gas OCGT/Hydro/Wind/Utility Solar with EFOR, POR, reliability index)

**Frontend (`app/frontend/src/pages/PlannedOutageAnalytics.tsx`):**
- 4 KPI cards: total capacity offline (MW), tightest reserve margin (% coloured red/yellow/green by threshold), outage conflict count (with AEMO intervention count), highest EFOR technology
- Gantt-style outage calendar table — unit name, technology badge (colour-coded by fuel type), region, start/end dates (monospace), duration (days), capacity (MW), outage type badge (FULL/PARTIAL/DERATING), reason label; hover highlight
- Reserve margin line chart — dual (all 5) region comparison with region toggle buttons, 15% CRITICAL threshold ReferenceLine (red dashed), Y-axis 0-80%, Recharts LineChart with dots
- Conflict risk matrix — colour-coded card layout by risk level (CRITICAL=red/HIGH=orange/MEDIUM=yellow/LOW=green), AEMO intervention badge, unit pair, overlap dates, combined MW
- Maintenance KPI horizontal BarChart — planned outage rate % sorted descending with technology-colour-coded Cell fills; companion summary table with EFOR (coloured by threshold), POR, avg days/yr, reliability index

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces (`GPOPlannedOutageRecord`, `GPOReserveMarginRecord`, `GPOOutageConflictRecord`, `GPOMaintenanceKpiRecord`, `PlannedOutageDashboard`) and `getPlannedOutageDashboard()` exported function calling `GET /api/planned-outage/dashboard`.
**Routing (`app/frontend/src/App.tsx`):** Import `PlannedOutageAnalytics`; nav entry (`Calendar` icon at `/planned-outage-analytics`); `Route` element `<PlannedOutageAnalytics />`. No new lucide-react imports needed — `Calendar` was already present.
**Tests (`tests/test_backend.py`):** `TestPlannedOutageAnalytics.test_planned_outage_dashboard` validates top-level structure (timestamp/outages/reserve_margins/conflicts/kpis), all 15 outage records with valid enums, all 20 reserve margin records across 5 regions with valid statuses, business logic: VIC W3 is CRITICAL with reserve_margin_pct < 10, 5 conflict records with valid risk levels, QLD conflict is CRITICAL with aemo_intervention=True, 7 KPI records, Utility Solar has lowest EFOR, Brown Coal has highest EFOR.

## Sprint 56b — Wholesale Price Volatility Regime Analytics — 2026-02-20

**Backend (`app/backend/main.py`):** Appended Pydantic models (`VRARegime`, `VRAClusterTrigger` enums; `VRARegimeRecord`, `VRAVolatilityClusterRecord`, `VRAHedgingImplicationRecord`, `VRARegimeTransitionRecord`, `VolatilityRegimeDashboard`) and `GET /api/volatility-regime/dashboard` endpoint with `Depends(verify_api_key)`. Mock dataset: 60 regime records (12 months × 5 NEM regions, 2024) with region-tuned volatility parameters (SA1 highest), 8 volatility cluster events (spanning Jan–Dec 2024 with 6 trigger types), 4 hedging implication records (one per regime with escalating hedge ratios/cap strikes), and 12 regime transition records covering all 4×3 from→to pairs.

**Frontend (`app/frontend/src/pages/VolatilityRegimeAnalytics.tsx`):** New page created with:
- Header + 4 KPI cards: EXTREME regime region-months (10), highest volatility index (peak SA1 Feb), total cluster cost impact (M AUD), optimal cap strike price (EXTREME regime)
- Regime timeline AreaChart — 12-month NSW1/SA1 volatility index with HIGH/EXTREME reference lines at VI 0.5 and 1.0
- Cluster event BarChart — cost impact per event, and detailed table with trigger badges (colour-coded by type), duration, max/avg price, total cost
- Hedging implication cards — 4 regime cards (green/indigo/amber/red borders) showing hedge ratio, cap strike, swap volume, VaR 95%, and cost of hedging
- Regime transition probability matrix — 4×4 heatmap table with cell colour intensity (red ≥60%, amber 35–59%, indigo 15–34%, slate <15%)
- Regional summary table — per-region count of LOW/NORMAL/HIGH/EXTREME months, avg VI, avg price

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces (`VRARegimeRecord`, `VRAVolatilityClusterRecord`, `VRAHedgingImplicationRecord`, `VRARegimeTransitionRecord`, `VolatilityRegimeDashboard`) and `getVolatilityRegimeDashboard()` exported function.

**Routing (`app/frontend/src/App.tsx`):** Added `import VolatilityRegimeAnalytics`; nav entry with `TrendingUp` icon at `/volatility-regime-analytics`; `Route` element `<VolatilityRegimeAnalytics />`.

**Tests (`tests/test_backend.py`):** `TestVolatilityRegimeAnalytics.test_volatility_regime_dashboard` validates: 60 regime records across 5 regions and 12 months, SA1 has highest avg volatility index vs all other regions, 8 cluster events with valid trigger enums and price ordering (max > avg), 4 hedging records with EXTREME > HIGH > NORMAL > LOW ordering for both hedge ratio and cap strike, 12 transition records covering all 4 from-regime states with valid probability and no self-transitions.

---

## Sprint 57b — Power System Black Start & System Restart Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended Pydantic models `BSARestartZoneRecord`, `BSABlackStartUnitRecord`, `BSASystemStrengthRecord`, `BSARestoreProgressRecord`, and `BlackStartDashboard`. Added mock data factories producing 10 restart zone records (2 per NEM region: NSW1, VIC1, QLD1, SA1, TAS1), 12 black start unit records spanning OCGT/CCGT/Hydro/Coal Steam/Synchronous Condenser technologies with FULL/PARTIAL capability and MARKET/CONTRACTED/MANDATORY contract types, 5 system strength records (one per NEM region — SA1 flagged INADEQUATE with fault level below minimum due to high IBR penetration at 74.6%), and 20 restore progress records (10 hourly steps × 2 scenarios: BEST_CASE and WORST_CASE). Endpoint: `GET /api/black-start/dashboard` using `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/BlackStartCapability.tsx`):** New page built with Recharts (LineChart, BarChart, Cell) and lucide-react (ShieldAlert). Dark Tailwind theme throughout. Sections:
- Header with ShieldAlert icon and data timestamp
- 4 KPI cards: total black start capacity (MW), zones assessed ADEQUATE count, fastest restore estimate (hours), system strength INADEQUATE region count
- Restart zone grid cards — anchor units, cranking path, adequacy badge (green/amber/red), restore hours, zone load MW, last tested date
- Black start unit table — unit name/ID, technology badge (colour-coded by type), capability badge, cranking MW, self-excitation indicator, contract type badge, contract value, compliance status
- System strength table + side-by-side bar chart — region fault level vs minimum required (MVA), IBR %, strength providers, status badge; bar chart cells coloured by adequacy status
- System restore progress line chart — BEST_CASE (green) and WORST_CASE (red) percentage restored over 10 hours with 50% and 80% reference lines

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `BSARestartZoneRecord`, `BSABlackStartUnitRecord`, `BSASystemStrengthRecord`, `BSARestoreProgressRecord`, `BlackStartDashboard` and `getBlackStartDashboard()` exported function calling `GET /api/black-start/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import BlackStartCapability`; nav entry with `ShieldAlert` icon at `/black-start-capability`; `Route` element `<BlackStartCapability />`.

**Tests (`tests/test_backend.py`):** `TestBlackStartCapability.test_black_start_dashboard` validates: 200 response, 10 restart zone records covering all 5 NEM regions with exactly 2 per region, all adequacy/capability/contract enum values valid, 12 black start unit records with FULL capability units having cranking_power_mw > 0 and MANDATORY contracts having zero contract value, 5 system strength records (one per region) with SA1 INADEQUATE and highest IBR percentage, 20 restore progress records (10 per scenario across BEST_CASE and WORST_CASE), BEST_CASE ahead of WORST_CASE at hour 5, both scenarios starting at 0% load at hour 0.

## Sprint 57a — FCAS & Ancillary Services Cost Allocation Analytics — 2026-02-20

**Backend (`app/backend/main.py`):** Appended Pydantic models (`ASCServiceType`, `ASCAllocationMechanism`, `ASCCauseType` enums; `ASCServiceRecord`, `ASCProviderRecord`, `ASCCostAllocationRecord`, `ASCCauserPaysRecord`, `AncillaryCostDashboard`) and `GET /api/ancillary-cost/dashboard` endpoint with `Depends(verify_api_key)`. Mock dataset: 24 service records (8 FCAS services across 3 months Jan-Mar 2024) with RAISE_REG having highest clearing prices, 20 provider records across diverse technologies, 15 cost allocation records (5 NEM regions x 3 months) with SA1 highest cost/MWh, and 12 causer-pays records covering all 4 cause types.

**Frontend (`app/frontend/src/pages/AncillaryServicesCost.tsx`):** New page created with: Header + 4 KPI cards (Total FCAS Cost Q1 2024, Most Expensive Service, Highest Cost Region $/MWh, RAISE_REG Market HHI); service cost monthly trend LineChart (8 services as colour-coded lines); provider revenue PieChart (top 8 by revenue); regional cost allocation table (region/cost/share/mechanism badge); causer-pays leaderboard (ranked with badges, factor progress bars, cause type badges); provider performance table (20 providers sorted by revenue with market share progress bars).

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript types `ASCServiceType`, `ASCAllocationMechanism`, `ASCCauseType` and interfaces `ASCServiceRecord`, `ASCProviderRecord`, `ASCCostAllocationRecord`, `ASCCauserPaysRecord`, `AncillaryCostDashboard` plus `getAncillaryCostDashboard()` exported function calling `GET /api/ancillary-cost/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import AncillaryServicesCost`; nav entry with `Gauge` icon at `/ancillary-services-cost`; `Route` element `<AncillaryServicesCost />`.

**Tests (`tests/test_backend.py`):** `TestAncillaryServicesCost.test_ancillary_cost_dashboard` validates: 200 response, 24 service records with all 8 service types across 3 months, RAISE_REG avg clearing price > RAISE_6SEC (and LOWER_REG > LOWER_6SEC), all HHI values between 0 and 1, 20 provider records, 15 cost allocation records with SA1 highest $/MWh and NSW1 highest energy market share, all mechanism enums valid, 12 causer-pays records with AGL Energy holding top factor (0.182), INTERCONNECTOR and GENERATION_VARIATION cause types present.

## Sprint 57c — CBAM & Australian Export Trade Analytics — 2026-02-20

**Backend (`app/backend/main.py`):** Appended Pydantic enums (`_CBAExportSector`, `_CBAPolicyStatus`, `_CBACleanProduct`) and models (`CBAExportSectorRecord`, `CBATradeFlowRecord`, `CBACleanExportRecord`, `CBAPolicyRecord`, `CbamTradeDashboard`) and `GET /api/cbam-trade/dashboard` endpoint with `Depends(verify_api_key)`. Mock dataset: 8 export sector records (ALUMINIUM/STEEL/CEMENT/CHEMICALS/LNG/CLEAN_HYDROGEN/GREEN_AMMONIA/BATTERY_MATERIALS), 20 trade flow records (10 trading partners × 2 sectors each: EU/UK/USA/Canada/Japan/South Korea/Germany/Netherlands/India/China), 6 clean export product records (GREEN_HYDROGEN/GREEN_AMMONIA/GREEN_STEEL/CLEAN_ALUMINUM/SILICON_METAL/LITHIUM_HYDROXIDE), 5 CBAM policy records (EU/UK/USA/Canada/Japan with ENACTED/PROPOSED/UNDER_REVIEW statuses).

**Frontend (`app/frontend/src/pages/CbamTradeAnalytics.tsx`):** New page created with:
- Header + 4 KPI cards: total CBAM exposure (sum across all sectors in M AUD), largest exposed sector (ALUMINIUM at A$1,420M), clean export market opportunity (sum of addressable market sizes in bn AUD), number of enacted CBAM policies
- Export sector exposure table — sorted by CBAM exposure descending, showing export value, carbon intensity, CBAM exposure (red/amber/green colour-coded), clean alternative badge, transition timeline, and competitive advantage text
- Trade flow bubble chart — ScatterChart with x=CBAM tariff rate, y=embedded carbon kt CO₂, bubble size=export volume kt, each trading partner in a distinct colour with custom tooltip showing all fields
- Clean export opportunity bar chart — BarChart showing Production Cost vs Target Price per product (AUD/tonne), with market size in tooltip
- Clean export product detail cards — 6 cards sorted by competitiveness rank showing cost gap (red/green), market size, 2030 target, and key markets
- Policy tracker table — country flag emoji, policy name, carbon price (amber), implementation year, sectors covered, AU exposure (red >1000, amber >300), status badge (green=ENACTED, amber=PROPOSED, blue=UNDER_REVIEW)

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces (`CBAExportSectorRecord`, `CBATradeFlowRecord`, `CBACleanExportRecord`, `CBAPolicyRecord`, `CbamTradeDashboard`) and `getCbamTradeDashboard()` exported function.

**Routing (`app/frontend/src/App.tsx`):** Added `Globe` to lucide-react imports; added `import CbamTradeAnalytics`; nav entry with `Globe` icon at `/cbam-trade-analytics`; `Route` element `<CbamTradeAnalytics />`.

**Tests (`tests/test_backend.py`):** `TestCbamTradeAnalytics.test_cbam_trade_dashboard` validates: top-level structure (timestamp/export_sectors/trade_flows/clean_exports/policies), all 8 sector records with valid enums and non-negative exposures, CLEAN_HYDROGEN and GREEN_AMMONIA have zero CBAM exposure, ALUMINIUM has the highest CBAM exposure, 20 trade flow records across exactly 10 trading partners all for year=2025, EU has the highest CBAM tariff rate, 6 clean export product records with all required fields, LITHIUM_HYDROXIDE has the largest market size, 5 policy records with valid status enums, EU and UK are ENACTED, EU has the highest AU exposure and highest carbon price.

## Sprint 58a — Network Congestion Revenue & SRA Analytics — 2026-02-20

**Backend (`app/backend/main.py`):** Appended Pydantic enum `_NCRDirection` (FORWARD/REVERSE) and models `NCRSraContractRecord`, `NCRCongestionRentRecord`, `NCRNodalPriceRecord`, `NCRInterconnectorEconomicsRecord`, `CongestionRevenueDashboard`, plus `GET /api/congestion-revenue/dashboard` endpoint with `Depends(verify_api_key)`. Mock dataset: 12 SRA contract records (4 interconnectors × 3 quarters: VIC1-NSW1, NSW1-QLD1, SA1-VIC1, TAS1-VIC1), 8 congestion rent records (4 interconnectors × 2 quarters with allocated vs retained split), 10 nodal price records across NSW1/QLD1/VIC1/SA1/TAS1 regions including LMP decomposition (energy + congestion + loss), 6 interconnector economics records (2 years × 3 interconnectors) with BCR, net benefit, and capacity factor data.

**Frontend (`app/frontend/src/pages/CongestionRevenueAnalytics.tsx`):** New page created with:
- Header + 4 KPI cards: total SRA contract value (A$M), highest congestion rent interconnector (NSW1-QLD1), total retained congestion rent (A$M), best BCR interconnector (NSW1-QLD1 2.54x)
- SRA contract table with interconnector colour badges, direction badges (FORWARD=blue/REVERSE=amber), MW, clearing price, total value, holder, and utilisation % (colour-coded green/amber/red by threshold)
- Congestion rent stacked bar chart (Recharts BarChart) — SRA-allocated vs TNSP-retained per interconnector, aggregated across quarters
- Nodal price scatter chart (Recharts ScatterChart) — x=congestion component, y=avg LMP, distinct colours per NEM region, zero-reference line, custom tooltip
- Interconnector economics BCR horizontal bar chart — latest year per interconnector, reference line at BCR=1.0, colour-coded by interconnector

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `NCRSraContractRecord`, `NCRCongestionRentRecord`, `NCRNodalPriceRecord`, `NCRInterconnectorEconomicsRecord`, `CongestionRevenueDashboard` and exported `getCongestionRevenueDashboard()` function.

**Routing (`app/frontend/src/App.tsx`):** Added `import CongestionRevenueAnalytics`; nav entry `{ to: '/congestion-revenue-analytics', label: 'Congestion Revenue & SRA', Icon: Network }`; `<Route path="/congestion-revenue-analytics" element={<CongestionRevenueAnalytics />} />`.

**Tests (`tests/test_backend.py`):** `TestCongestionRevenueAnalytics.test_congestion_revenue_dashboard` validates: top-level structure (5 keys), 12 SRA contracts across exactly 4 interconnectors with valid directions/values, 8 congestion rent records with SRA-allocated + retained = total (±0.1), NSW1-QLD1 highest total rent, 10 nodal price records across ≥4 regions with LMP decomposition correctness (energy+congestion+loss ≈ avg_lmp), SA1 highest congestion components, 6 economics records with BCR>1.0 and net_benefit = revenue - cost (±0.2), NSW1-QLD1 best BCR in latest year.

## Sprint 58c — Energy Affordability & Household Bill Analytics — 2026-02-20

**Backend (`app/backend/main.py`):** Appended Pydantic enums (`_EAHIncomeCohort`, `_EAHHouseholdType`, `_EAHProgramType`) and models (`EAHBillTrendRecord`, `EAHIncomeAffordabilityRecord`, `EAHSolarImpactRecord`, `EAHAssistanceProgramRecord`, `EnergyAffordabilityDashboard`) and `GET /api/energy-affordability/dashboard` endpoint with `Depends(verify_api_key)`. Mock dataset: 30 bill trend records (6 states × 5 years 2020–2024 — NSW/VIC/QLD/SA/WA/TAS — tracking avg_annual_bill, median_income_pct, usage_kwh, and four bill component charges), 30 income affordability records (6 states × 5 cohorts BOTTOM_20PCT/LOWER_MIDDLE/MIDDLE/UPPER_MIDDLE/TOP_20PCT — including energy_burden_pct, solar_ownership_pct, hardship_rate_pct), 24 solar impact records (6 states × 4 household types NO_SOLAR/SOLAR_ONLY/SOLAR_BATTERY/VPP_PARTICIPANT — including net_energy_cost, payback_years, adoption_pct), 12 assistance program records spanning all 4 program types (REBATE/CONCESSION/PAYMENT_PLAN/FREE_APPLIANCE).

**Frontend (`app/frontend/src/pages/EnergyAffordabilityAnalytics.tsx`):** New page created with:
- Header with DollarSign icon + 4 KPI cards: avg national bill 2024 (AUD), bill as % of median income, bottom 20% avg energy burden %, total assistance program spending (M AUD)
- Bill trend multi-line chart (LineChart) — 6 states over 5 years with toggle buttons to show/hide individual states; custom BillTrendTooltip
- Bill component stacked bar chart (BarChart) — network/wholesale/environmental/retail margin breakdown for 2024 by state; colour-coded segments
- Income cohort grouped bar chart (BarChart) — energy burden % by state for all 5 cohorts with red dashed 6% hardship threshold ReferenceLine; custom CohortTooltip
- Solar impact comparison (ComposedChart with Bars) — net_energy_cost_aud by household type per state with zero ReferenceLine; adoption % mini-summary cards
- Assistance program table — sorted by effectiveness_score desc; columns: program name, state badge, eligible cohort, rebate AUD, recipients (k), total cost (M AUD), program type badge, star effectiveness rating; summary totals row

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces (`EAHBillTrendRecord`, `EAHIncomeAffordabilityRecord`, `EAHSolarImpactRecord`, `EAHAssistanceProgramRecord`, `EnergyAffordabilityDashboard`) and `getEnergyAffordabilityDashboard()` exported function.

**Routing (`app/frontend/src/App.tsx`):** Added `import EnergyAffordabilityAnalytics`; nav entry `{ to: '/energy-affordability', label: 'Energy Affordability', Icon: DollarSign }`; `Route` element `<EnergyAffordabilityAnalytics />` at path `/energy-affordability`.

**Tests (`tests/test_backend.py`):** `TestEnergyAffordabilityAnalytics.test_energy_affordability_dashboard` validates: top-level keys present, 30 bill trend records with all 6 states and all 5 years, SA has the highest 2024 avg bill and WA the lowest, 30 income affordability records with all 5 cohorts, bottom-20% energy burden exceeds top-20% within every state, SA has the highest bottom-20% energy burden, 24 solar impact records with all 4 household types, NO_SOLAR net cost exceeds SOLAR_ONLY in every state, VPP_PARTICIPANT has the lowest net cost in QLD and SA, 12 assistance programs covering all 4 program types, NSW Solar for Low Income has the highest effectiveness score, total assistance spending exceeds $1,000M AUD.

## Sprint 58b — Climate Physical Risk to Grid Assets — 2026-02-20

**Backend (`app/backend/main.py`):** Appended models `CPRAssetRecord`, `CPRHazardProjectionRecord`, `CPRClimateEventRecord`, `CPRAdaptationMeasureRecord`, and `ClimatePhysicalRiskDashboard` (all prefixed `CPR` to avoid collision with existing `ClimateRiskDashboard`). Mock data: 12 grid asset records (TRANSMISSION_LINE, SUBSTATION, GENERATION, DISTRIBUTION, STORAGE) across NSW/VIC/QLD/SA/WA with exposure/vulnerability/risk scores (0-100) and primary hazard (EXTREME_HEAT/FLOODING/BUSHFIRE/CYCLONE/SEA_LEVEL_RISE/DROUGHT); 20 hazard projection records (5 hazards × 2 scenarios RCP4.5/RCP8.5 × 2 key regions) with 2030/2050/2070 change % and frequency multipliers; 8 historical climate event records; 10 adaptation measures with benefit-cost ratios. Endpoint: `GET /api/climate-risk/physical-dashboard` → `ClimatePhysicalRiskDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/ClimatePhysicalRisk.tsx`):** New file. Dark Tailwind theme. Header with `Thermometer` icon. 4 KPI cards (total exposed asset value in $B AUD, highest risk asset name/score, total event damage M AUD, high-priority adaptation count). Risk matrix ScatterChart (x=exposure_score, y=vulnerability_score, ZAxis=value_m_aud, coloured by asset_type with legend). Hazard projection LineChart showing 2030/2050/2070 change % for RCP8.5 per hazard. Climate event damage BarChart sorted by damage_m_aud with dual Y-axis for outage_hours. Adaptation measures table sorted by priority with BCR, cost, risk reduction %, and colour-coded priority badge.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `CPRAssetRecord`, `CPRHazardProjectionRecord`, `CPRClimateEventRecord`, `CPRAdaptationMeasureRecord`, `ClimatePhysicalRiskDashboard`, and exported `getClimatePhysicalRiskDashboard()` function calling `/api/climate-risk/physical-dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import ClimatePhysicalRisk from './pages/ClimatePhysicalRisk'`; nav entry `{ to: '/climate-physical-risk', label: 'Climate Physical Risk', Icon: Thermometer }` (Thermometer already imported); `Route path="/climate-physical-risk"` with `element={<ClimatePhysicalRisk />}`.

**Tests (`tests/test_backend.py`):** Appended `TestClimatePhysicalRisk.test_climate_physical_risk_dashboard` validating: HTTP 200, all top-level keys present, 12 assets with valid enums and score ranges 0-100, highest risk >= 80, all 5 asset types represented, 20 hazard projections with RCP8.5 always higher than RCP4.5 at 2070, 8 climate events with highest-damage event being BUSHFIRE, 10 adaptation measures with at least 3 HIGH-priority and best BCR > 5.

## Sprint 59c — Australian Electricity Export Infrastructure (2026-02-20)

**Backend (`app/backend/main.py`):** Appended Pydantic models `EEICableProjectRecord`, `EEIRenewableZoneRecord`, `EEIExportMarketRecord`, `EEIEconomicProjectionRecord`, and `ElectricityExportDashboard`. Added mock data: 7 cable project records (Sun Cable AAPowerLink, Marinus Link Leg 1 & 2, Bass Strait Cable, Japan-Australia Energy Link, NZ-Australia Clean Energy Link, Australia-Asia PowerLink domestic), 8 renewable zone records spanning NT/WA/QLD/NSW/TAS/SA, 8 export market records (Japan, Singapore, South Korea, Indonesia, Philippines, Malaysia, India, China), and 15 economic projection records across 3 scenarios (Conservative/Moderate/Accelerated) × 5 years (2027-2035). Endpoint `GET /api/electricity-export/dashboard` protected with `verify_api_key`.

**Frontend (`app/frontend/src/pages/ElectricityExportInfra.tsx`):** Created new page with Tailwind dark theme. Includes: header with `Globe2` icon, 4 KPI cards (total cable capacity GW, total pipeline capex A$bn, total export potential TWh/yr, signed trade agreements count), cable project status table with technology and status badges, export-oriented renewable zones table with resource-type badges, export market comparison bar chart coloured by preferred form (ELECTRICITY/GREEN_H2/GREEN_AMMONIA/LNG_CCS), and economic projections multi-line chart showing export revenue A$bn/yr for 3 scenarios.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `EEICableProjectRecord`, `EEIRenewableZoneRecord`, `EEIExportMarketRecord`, `EEIEconomicProjectionRecord`, `ElectricityExportDashboard`, and exported `getElectricityExportDashboard()` function calling `/api/electricity-export/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `Globe2` to lucide-react imports; added `import ElectricityExportInfra from './pages/ElectricityExportInfra'`; nav entry `{ to: '/electricity-export-infra', label: 'Electricity Export Infra', Icon: Globe2 }`; `Route path="/electricity-export-infra"` with `element={<ElectricityExportInfra />}`.

**Tests (`tests/test_backend.py`):** Appended `TestElectricityExportInfra.test_electricity_export_dashboard` validating: HTTP 200, all top-level keys present, 7 cable projects with valid technology/status enums (Sun Cable PROPOSED at 3.2 GW, both Marinus Link legs APPROVED), 8 renewable zones with all 3 resource types represented and at least 3 export-oriented, 8 export markets covering all required countries (Japan & Singapore SIGNED, China highest import potential), 15 economic projections across 3 scenarios × 5 years with Accelerated always higher than Conservative and 2035 Accelerated revenue > A$50 bn.

---

## Sprint 59a — Building Electrification & Heat Pump Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended enums `_BEAApplianceType` (HEAT_PUMP_HVAC/HEAT_PUMP_WATER/INDUCTION_COOKTOP/EV_CHARGER/ALL_ELECTRIC_HOME), `_BEARegulatoryStatus` (ALLOWED/UNDER_REVIEW/RESTRICTED/BANNED), `_BEAProgramType` (REBATE/LOAN/VPP_INCENTIVE/BULK_PURCHASE). Models: `BEAAdoptionRecord`, `BEALoadImpactRecord`, `BEAGasNetworkRecord`, `BEAProgramRecord`, `ElectrificationDashboard`. Mock data: 25 adoption records (5 appliance types × 5 states: NSW/VIC/QLD/SA/WA) with total units, annual additions, market penetration, install cost, payback; 20 load impact records (5 states × 4 years 2024-2027) with peak MW, annual GWh, gas displaced PJ, CO2 reduction kt, grid augmentation cost, flexibility potential; 8 gas network records (Jemena, Multinet, AusNet, Evoenergy, Allgas, Atco, AGN SA, Tas Gas) with connections, consumption, electrification risk %, asset value, stranded asset risk, and all 4 regulatory statuses represented; 10 program records across VIC/NSW/SA/QLD/WA covering all 4 program types. Endpoint: `GET /api/electrification/dashboard` → `ElectrificationDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/ElectrificationAnalytics.tsx`):** New file. Dark Tailwind theme. Header with `Flame` icon. 4 KPI cards: total heat pump installations (k units), additional peak demand by 2027 (MW), total gas displaced PJ, total rebate program spending M AUD. Adoption by state BarChart (all 5 appliance types grouped). Annual additions LineChart (by appliance type across states). Load impact AreaChart stacked by state 2024-2027 (additional GWh). Gas network stranded asset risk table sorted by stranded risk descending with electrification risk % colour-coded, state badge, and regulatory status badge. Program comparison table sorted by abatement cost ascending with uptake % and type badges. CO2 reduction stacked BarChart by state and year.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `BEAAdoptionRecord`, `BEALoadImpactRecord`, `BEAGasNetworkRecord`, `BEAProgramRecord`, `ElectrificationDashboard`, and exported `getElectrificationDashboard()` calling `GET /api/electrification/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import ElectrificationAnalytics from './pages/ElectrificationAnalytics'`; nav entry `{ to: '/electrification-analytics', label: 'Building Electrification', Icon: Flame }` (Flame already imported); `Route path="/electrification-analytics"` with `element={<ElectrificationAnalytics />}`.

**Tests (`tests/test_backend.py`):** Appended `TestElectrificationAnalytics.test_electrification_dashboard` validating: HTTP 200, all top-level keys present, 25 adoption records with all 5 states and 5 appliance types represented, induction cooktop lowest avg cost, all-electric home highest avg cost, 20 load impact records with year-over-year peak MW growth per state, VIC 2027 > SA 2027, 8 gas network records with all 4 regulatory statuses represented and BANNED network having highest electrification risk, 10 program records with all 4 program types represented, bulk purchase lower avg abatement cost than rebate, and all REBATE programs have rebate_amount_aud > 0.

## Sprint 59b — Long Duration Energy Storage (LDES) Economics — 2026-02-20

**Backend (`app/backend/main.py`):** Appended models `LDETechnologyRecord`, `LDEEconomicCaseRecord`, `LDEProjectRecord`, `LDESeasonalRecord`, and `LdesEconomicsDashboard`. All models use `LDE` prefix to avoid collision with existing `LongDurationStorageRecord`. Supporting enums `_LDETechId`, `_LDEScenario`, `_LDEProjectStatus`. Mock data: 10 technology records (PUMPED_HYDRO/COMPRESSED_AIR/FLOW_VANADIUM/FLOW_ZINC/LIQUID_AIR/GREEN_HYDROGEN_STORAGE/THERMAL_MOLTEN_SALT/GRAVITY_RAIL/IRON_AIR/ADIABATIC_CAES) with duration ranges 4–2160 hr, current LCOS $120–$450/MWh, TRL 4–9, CAPEX, round-trip efficiency, self-discharge rate, project lifetime, and Australian project count; 3 economic case scenario records (HIGH_VRE_90/HIGH_VRE_75/MEDIUM_VRE_60) with optimal duration, storage GWh, avoided curtailment, system savings, optimal technology, and breakeven LCOS; 12 Australian project records (Snowy 2.0, Pioneer-Burdekin, Borumba Dam, Battery of the Nation, Kidston PHES, AGL Vanadium Flow Pilot, Onslow Vanadium, Port Augusta CSP, Whyalla Green H2, HyP SA, Compressed Air Quarry Pilot, Iron-Air Demonstration) spanning OPERATING/CONSTRUCTION/APPROVED/PROPOSED; 12 monthly seasonal pattern records with VRE surplus/deficit, optimal charge/discharge, storage utilisation %, and price arbitrage AUD/MWh. Endpoint: `GET /api/ldes-economics/dashboard` → `LdesEconomicsDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/LdesEconomicsAnalytics.tsx`):** New file. Dark Tailwind theme. Header with `Database` icon. 4 KPI cards (technologies at TRL≥7, cheapest 2035 LCOS target with tech name, total pipeline GWh excl. operating, peak seasonal VRE surplus GWh). Technology matrix table sorted by TRL desc then LCOS asc — columns: name, duration hr, LCOS current (amber) → 2035 target (emerald), LCOS reduction %, TRL badge (colour-coded Commercial/Demo/Pilot/R&D), round-trip efficiency %, AU projects count. LCOS current vs 2035 target grouped BarChart sorted by current LCOS with $200/MWh competitive threshold ReferenceLine. Three economic case scenario cards with colour-coded top border showing optimal duration, storage required, avoided curtailment TWh/yr, system savings M AUD, optimal technology, and breakeven LCOS. Seasonal VRE surplus/deficit ComposedChart with dual Y-axis: Area layers for surplus (amber) and deficit (red), dashed Lines for optimal charge (emerald) and discharge (blue), utilisation % line (purple) on right axis. Monthly price arbitrage LineChart with PHES breakeven reference line. LCOS vs round-trip efficiency ScatterChart with ZAxis bubble size = TRL×12, coloured by tech_id, custom tooltip showing name/duration/current LCOS/2035 target/TRL/efficiency, plus manual colour legend below. Project tracker table sorted by status priority then capacity desc — columns: project name, status badge, technology, region badge, capacity GWh, power MW, COD year, CAPEX (B for ≥$1B, M otherwise); summary row showing total projects, total capacity GWh, total CAPEX $B, and operating count.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `LDETechnologyRecord`, `LDEEconomicCaseRecord`, `LDEProjectRecord`, `LDESeasonalRecord`, `LdesEconomicsDashboard` and exported `getLdesEconomicsDashboard()` function calling `/api/ldes-economics/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import LdesEconomicsAnalytics from './pages/LdesEconomicsAnalytics'`; nav entry `{ to: '/ldes-economics', label: 'LDES Economics', Icon: Database }` (Database already imported); `Route path="/ldes-economics"` with `element={<LdesEconomicsAnalytics />}`.

**Tests (`tests/test_backend.py`):** Appended `TestLdesEconomicsAnalytics.test_ldes_economics_dashboard` validating: HTTP 200, all 5 top-level keys present, exactly 10 technology records with all 10 valid tech_ids and invariants (2035 target < current LCOS, TRL 1–9 range, efficiency 0–100, non-negative AU projects), pumped hydro has highest TRL, ≥4 technologies at TRL≥7, exactly 3 economic case scenarios with all 3 valid scenario enums, storage required increases monotonically with VRE penetration, system savings higher at 90% VRE vs 60% VRE, exactly 12 project records with all 4 status types present, Snowy 2.0 is largest by capacity, ≥1 PUMPED_HYDRO project in CONSTRUCTION, exactly 12 monthly seasonal records with optimal charge ≤ surplus and discharge ≤ deficit, winter price arbitrage (Jun/Jul average) exceeds summer (Dec/Jan average), summer VRE surplus exceeds winter.

## Sprint 60c — Prosumer & Behind-the-Meter (BTM) Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended models `BTMInstallationRecord`, `BTMNetLoadRecord`, `BTMExportRecord`, `BTMVppRecord`, and `ProsumerDashboard`. All models use `BTM` prefix to avoid collision with existing `BehindMeterAsset`. Mock data: 25 installation records (5 states × 5 years 2020-2024) with rooftop solar MW, BTM battery MWh, average system/battery sizes, export-capable %, smart meter %; 30 net load records (5 states × 6 months Jan/Mar/May/Jul/Sep/Nov) with gross demand, BTM solar generation, BTM battery discharge, net demand, minimum net demand MW, duck curve depth MW, evening ramp MW/hr; 20 export records (5 states × 4 years 2021-2024) with total exports GWh, FiT payments M AUD, average FiT rate AUD/kWh, export curtailment GWh/%, grid constraint flag; 8 VPP records (AGL VPP SA, Tesla VPP SA, Origin Spike Response NSW, Amber SmartShift VIC, EnergyAustralia FlexPower QLD, Synergy Home Battery WA, Simply Energy EV Grid VIC, Reposit Power GridCredits NSW). Endpoint: `GET /api/prosumer/dashboard` → `ProsumerDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/ProsumerAnalytics.tsx`):** New file. Dark Tailwind theme. Header with `Sun` icon. 4 KPI cards (total rooftop solar GW, BTM battery GWh, total annual exports GWh, VPP enrolled customers k). Installation growth stacked AreaChart — rooftop solar MW by state 2020-2024 with state colour coding. Duck curve ComposedChart (SA) — gross demand vs net demand Area layers, duck curve depth MW dashed Line on right axis, demand floor ReferenceLine. Export trends LineChart — total exports GWh by state 2021-2024. Export curtailment BarChart — curtailment % by state 2024 with colour-coded bars (red=grid constraint triggered, green=normal), 5% threshold ReferenceLine. VPP comparison table sorted by battery MWh desc with state badges, summary totals/averages footer row.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `BTMInstallationRecord`, `BTMNetLoadRecord`, `BTMExportRecord`, `BTMVppRecord`, `ProsumerDashboard` and exported `getProsumerDashboard()` function calling `/api/prosumer/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import ProsumerAnalytics from './pages/ProsumerAnalytics'`; nav entry `{ to: '/prosumer-analytics', label: 'Prosumer & BTM Analytics', Icon: Sun }` (Sun already imported); `Route path="/prosumer-analytics"` with `element={<ProsumerAnalytics />}`.

**Tests (`tests/test_backend.py`):** Appended `TestProsumerAnalytics.test_prosumer_dashboard` validating: HTTP 200, all 5 top-level keys present, exactly 25 installation records with all 5 states and years 2020-2024, all invariants (solar MW > 0, export % 0-100, smart meter % 0-100), 2024 solar exceeds 2020 for every state; exactly 30 net load records with all 5 states and 6 months, net demand < gross demand for all records, January BTM solar exceeds July for every state; exactly 20 export records with all 5 states and years 2021-2024, 2024 exports exceed 2021 for every state, SA has highest curtailment in 2024; exactly 8 VPP records with all invariants, total battery > 500 MWh, largest VPP by battery in WA.

## Sprint 60a — Gas-Fired Generation Transition Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended models `GFTGeneratorRecord`, `GFTGasSupplyRecord`, `GFTHydrogenBlendRecord`, `GFTCapacityOutlookRecord`, and `GasTransitionDashboard`. All models use `GFT` prefix to avoid potential naming collisions. Supporting enums `_GFTTechnology` (OCGT/CCGT/RECIP/STEAM), `_GFTExitTrigger` (ECONOMICS/FUEL/POLICY/AGE), `_GFTPriceTrend` (RISING/STABLE/FALLING). Mock data: 12 generator records (mix of QLD, VIC, SA, NSW, WA units covering Darling Downs CCGTs, Tallawarra A/B, Hallett, Torrens Island, Loy Yang B, Ladbroke Grove, Kwinana, Collie, Spring Creek, Mortlake) with full h2_capable/h2_ready_year/exit_year/exit_trigger fields; 8 gas basin supply records (Cooper, Surat/Bowen CSG, Gippsland, Otway, Carnarvon, Perth, Beetaloo, Browse) with reserves_pj, production, reserve life, domestic reservation %, price and pipeline connectivity; 8 H2 blending records covering h2-ready and conventional units with non-decreasing blend percentages 2025→2030→2035; 8 capacity outlook records (2024–2031) showing OCGT/CCGT/H2 turbine stacking with declining gas generation TWh. Endpoint: `GET /api/gas-transition/dashboard` → `GasTransitionDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/GasTransitionAnalytics.tsx`):** New file. Dark Tailwind theme. Header with `Fuel` icon. 4 KPI cards (total gas capacity MW, H2-capable units count, average gas SRMC $/MWh, average gas reserve life years). Generator fleet table with technology badge (CCGT/OCGT/RECIP/STEAM colour-coded), region, capacity, H2-capable badge with ready year, SRMC, capacity factor %, exit year, exit trigger badge. Gas supply basin table with reserves PJ, production, reserve life (showing "Undevel." for unconnected basins), domestic reservation %, price, price trend badge (RISING/STABLE/FALLING colour-coded), pipeline connectivity. H2 blending pathway grouped BarChart (2025/2030/2035 blend % per unit, 20% reference line). Capacity outlook ComposedChart stacked Area chart (OCGT/CCGT/H2 Turbine, 2024–2031). H2 blending details table with conversion cost, operational risk badge (LOW/MEDIUM/HIGH), derating %. Capacity outlook detail table with annual breakdown and NEM role column.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `GFTGeneratorRecord`, `GFTGasSupplyRecord`, `GFTHydrogenBlendRecord`, `GFTCapacityOutlookRecord`, `GasTransitionDashboard` and exported `getGasTransitionDashboard()` function calling `/api/gas-transition/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import GasTransitionAnalytics from './pages/GasTransitionAnalytics'`; nav entry `{ to: '/gas-transition-analytics', label: 'Gas Transition Analytics', Icon: Fuel }` (Fuel already imported at line 49); `Route path="/gas-transition-analytics"` with `element={<GasTransitionAnalytics />}`.

**Tests (`tests/test_backend.py`):** Appended `TestGasTransitionAnalytics.test_gas_transition_dashboard` validating: HTTP 200, all 5 top-level keys present, exactly 12 generator records with valid technologies/exit triggers/h2-capable invariants (h2-capable units must have h2_ready_year, exit_year units must have exit_trigger), at least 3 CCGT units, at least 1 H2-capable unit; exactly 8 gas supply basin records with valid price trends, at least 1 unconnected basin, Gippsland 100% domestically reserved, largest basin is Carnarvon or Browse; exactly 8 hydrogen blending records with non-decreasing blend percentages 2025→2030→2035, TALWB1 has high 2035 blend %; exactly 8 capacity outlook records for years 2024–2031, H2 turbine MW zero in 2024 and growing by 2031, gas generation TWh declining from 2024 to 2031.

---

## Sprint 60b — TNSP Revenue & Investment Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended models `TNATnspRecord`, `TNAReliabilityRecord`, `TNAProjectRecord`, `TNARegulatoryRecord`, and `TnspAnalyticsDashboard`. All models use `TNA` prefix to avoid collision with existing `TnspRevenueRecord`, `TnspAssetRecord`, and `TnspDashboard` models. Mock data: 5 TNSP records (TransGrid/NSW-ACT, ElectraNet/SA, TasNetworks/TAS, Powerlink/QLD, AusNet Transmission/VIC) with RAB, revenue determination, WACC, network length, substations, capex, opex, and determination period; 15 reliability records (5 TNSPs x 3 years: 2022–2024) with SAIDI, system minutes lost, circuit outages, unplanned outage rate, constraint hours, and asset age; 12 project records (HumeLink 500kV, Sydney Ring Upgrade, EnergyConnect SA-NSW, Heywood Interconnector Upgrade, Marinus Link Cable, Longford-Latrobe Valley Upgrade, CopperString 2032, North Queensland REZ Strengthening, Darlington Point Substation, VNI West, Moorabool-Sydenham 500kV, Transmission Vegetation Management) covering all 4 statuses and 5 primary drivers; 10 regulatory records (2 periods per TNSP) with allowed vs actual revenue, efficiency carryover, CPI escalator, and AER decisions (ACCEPTED/REVISED only). Endpoint: `GET /api/tnsp-analytics/dashboard` → `TnspAnalyticsDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/TnspAnalytics.tsx`):** Created new file (replacing stub). Dark Tailwind theme. Header with `Network` icon. 4 KPI cards: Total RAB (bn AUD), Total Annual Revenue (bn AUD), Average WACC % (real), Most Constrained TNSP by SAIDI. TNSP comparison table with RAB, revenue, WACC, network length, substations, capex, opex, and determination period — TNSP names colour-coded per entity. SAIDI reliability trend LineChart (5 lines, one per TNSP, over 2022–2024). Project investment pipeline stacked BarChart (investment by TNSP split by project type: AUGMENTATION/REPLACEMENT/UPGRADE/NEW_BUILD with colour legend). Regulatory performance table with allowed vs actual revenue, variance (green/red), efficiency carryover, CPI escalator, and AER decision badge (ACCEPTED=emerald, REVISED=amber, REJECTED=red). Transmission project pipeline table sorted by investment descending, with status badges, project type chips, B/M formatting for investment, and VRE enabled MW.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `TNATnspRecord`, `TNAReliabilityRecord`, `TNAProjectRecord`, `TNARegulatoryRecord`, `TnspAnalyticsDashboard` and exported `getTnspAnalyticsDashboard()` function calling `/api/tnsp-analytics/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Existing `TnspAnalytics` import at line 135 reused (no duplicate). Nav entry `{ to: '/tnsp-analytics', label: 'TNSP Revenue & Investment', Icon: Building2 }` added (Building2 already imported). `Route path="/tnsp-analytics"` with `element={<TnspAnalytics />}` added alongside existing `/tnsp` route.

**Tests (`tests/test_backend.py`):** Appended `TestTnspAnalytics.test_tnsp_analytics_dashboard` validating: HTTP 200, all 5 top-level keys present, exactly 5 TNSP records with correct IDs and positive invariants (RAB, revenue, WACC 0–10%, positive network/substations/capex/opex), TransGrid has largest RAB; exactly 15 reliability records across 3 years (2022–2024) and 5 TNSPs with all positive invariants, ElectraNet most constrained in latest year, TransGrid SAIDI improving over period; exactly 12 project records with all valid status/type/driver enums, all 4 statuses present, CopperString is largest by investment, at least 3 ISP-driven projects; exactly 10 regulatory records covering all 5 TNSPs with all valid AER decisions, actual revenue <= allowed revenue for all records.

---

## Sprint 61a — NEM Reliability Standard & Unserved Energy (USE) Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended models `RSAUseRecord`, `RSAReserveMarginRecord`, `RSAReliabilityEventRecord`, `RSADemandSideRecord`, and `ReliabilityStandardDashboard`. All models use `RSA` prefix to avoid collision with any future reliability models. Mock data: 15 USE records (5 NEM regions × 3 years: 2022–2024) with USE MWh, USE %, standard % (0.002%), compliance status (COMPLIANT/BREACH/AT_RISK), events count, max event duration, and economic cost; 15 reserve margin records (5 regions × 3 years) with peak demand, available capacity, reserve margin %, required reserve 15%, surplus/deficit MW, and probability of exceeding standard; 8 reliability event records covering NSW1/VIC1/SA1 events with cause, duration, customers affected, USE MWh, NEM intervention flag, and estimated cost; 12 demand-side mechanism records (4 mechanisms × 3 regions: RERT/DSP/VPP/INTERRUPTIBLE_LOAD) with registered MW, activated MW, activation events per year, cost per MWh, and reliability contribution %. Endpoint: `GET /api/reliability-standard/dashboard` → `ReliabilityStandardDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/ReliabilityStandardAnalytics.tsx`):** Created new file. Dark Tailwind theme. Header with `ShieldCheck` icon. 4 KPI cards: National USE (MWh) in latest year, Regions in BREACH count (red if >0), Tightest Reserve Margin % (red if <15%), Total Demand-Side Registered MW. USE compliance table: region × year pivot with USE %, standard % (0.002%), and colour-coded compliance badge (emerald=COMPLIANT, amber=AT_RISK, red=BREACH). Reserve margin trend LineChart: 5 region lines over 2022–2024 with red dashed 15% required threshold ReferenceLine. Reliability events table sorted by date descending with region colour coding, cause badge (colour-coded by cause type), duration, customers, USE MWh, cost, and NEM intervention badge. Demand-side bar chart: registered vs activated MW aggregated by mechanism with mechanism colour coding; followed by detailed table with per-record breakdown of region, registered/activated MW, events per year, cost $/MWh, reliability contribution %.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `RSAUseRecord`, `RSAReserveMarginRecord`, `RSAReliabilityEventRecord`, `RSADemandSideRecord`, `ReliabilityStandardDashboard` and exported `getReliabilityStandardDashboard()` function calling `/api/reliability-standard/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import ReliabilityStandardAnalytics from './pages/ReliabilityStandardAnalytics'`; added `ShieldCheck` to lucide-react imports; nav entry `{ to: '/reliability-standard-analytics', label: 'NEM Reliability Standard', Icon: ShieldCheck }`; `Route path="/reliability-standard-analytics"` with `element={<ReliabilityStandardAnalytics />}`.

**Tests (`tests/test_backend.py`):** Appended `TestReliabilityStandardAnalytics.test_reliability_standard_dashboard` validating: HTTP 200, all 5 top-level keys present; exactly 15 USE records across 5 regions and 3 years with valid compliance values, standard_pct=0.002, BREACH records have use_pct>standard_pct, COMPLIANT records have use_pct<=standard_pct, SA1 has at least 1 BREACH, NSW1/QLD1 fully COMPLIANT; exactly 15 reserve margin records with required_reserve_pct=15.0, surplus_deficit_mw negative iff below standard, QLD1 reserve margin > SA1, TAS1 reserve margin >50%; exactly 8 events with valid cause enums, all positive invariants, at least 4 NEM interventions, SA1 and VIC1 have at least 2 events each; exactly 12 demand-side records covering all 4 mechanisms with activated_mw<=registered_mw, RERT more expensive than INTERRUPTIBLE_LOAD per MWh, SA1 VPP contribution >5%, total registered capacity >3000 MW.

---

## Sprint 61b — DNSP Performance & Investment Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended models `DPADnspRecord`, `DPAReliabilityRecord`, `DPADerHostingRecord`, `DPAInvestmentRecord`, and `DnspAnalyticsDashboard`. All models use `DPA` prefix to avoid collision with existing `DnspRecord`, `DnspFaultRecord`, `DnspInvestmentRecord`, and `DnspDashboard` models. Mock data: 10 DNSP records (Ausgrid/NSW, Endeavour Energy/NSW, Essential Energy/NSW, Evoenergy/ACT, SA Power Networks/SA, CitiPower/VIC, Powercor/VIC, United Energy/VIC, Jemena Electricity Networks/VIC, TasNetworks Distribution/TAS) with RAB, customers, network length, substations, capex, opex, WACC and determination period; 30 reliability records (10 DNSPs × 3 years: 2022–2024) with SAIDI, SAIFI, CAIDI, momentary interruptions, planned/unplanned SAIDI decomposition, and worst-served customer %; 20 DER hosting records covering URBAN/SUBURBAN/RURAL/REMOTE feeder types with VOLTAGE/THERMAL/PROTECTION/UNCONSTRAINED constraint types, connected DER MW, hosting capacity MW, utilisation %, and upgrade cost; 30 investment records (FY2024) across RELIABILITY/GROWTH/SAFETY/DER_INTEGRATION/BUSHFIRE_MITIGATION/CYBER_SECURITY categories with RAB addition and customers benefited. Endpoint: `GET /api/dnsp-analytics/dashboard` → `DnspAnalyticsDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/DnspPerformanceAnalytics.tsx`):** Created new file. Dark Tailwind theme. Header with `Building` icon. 4 KPI cards: Total National DNSP RAB (bn AUD), Average SAIDI (minutes, 2024), Total DER Connected (MW), Total Annual Investment (M AUD). DNSP overview table with RAB, customers, network length, WACC, capex, opex and determination period — DNSP names colour-coded per entity. SAIDI/SAIFI grouped bar chart (dual Y-axis) for latest year, grouped by DNSP. DER hosting scatter chart with x=connected DER MW, y=hosting capacity MW, bubble size proportional to RAB, coloured by constraint type (VOLTAGE=red, THERMAL=orange, PROTECTION=purple, UNCONSTRAINED=emerald) with custom tooltip showing DNSP, feeder type and constraint. Investment stacked bar chart by DNSP split by project category with category colour legend. SAIDI trend line chart (10 lines, one per DNSP) over 2022–2024.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `DPADnspRecord`, `DPAReliabilityRecord`, `DPADerHostingRecord`, `DPAInvestmentRecord`, `DnspAnalyticsDashboard61b` and exported `getDnspAnalyticsDashboard()` function calling `/api/dnsp-analytics/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import DnspPerformanceAnalytics from './pages/DnspPerformanceAnalytics'`. Nav entry `{ to: '/dnsp-analytics', label: 'DNSP Performance & Investment', Icon: Network }` added after TNSP entry (Network already imported). `Route path="/dnsp-analytics"` with `element={<DnspPerformanceAnalytics />}` added.

**Tests (`tests/test_backend.py`):** Appended `TestDnspAnalytics.test_dnsp_analytics_dashboard` validating: HTTP 200, all 5 top-level keys present, exactly 10 DNSP records with correct IDs and positive invariants (RAB, customers, network length, substations, capex, opex, WACC 0–10%), Ausgrid has largest RAB; exactly 30 reliability records across 3 years (2022–2024) and 10 DNSPs with positive SAIDI/SAIFI/CAIDI, planned+unplanned SAIDI summing to total SAIDI, Ausgrid SAIDI improving over period, CitiPower has best SAIDI and Essential Energy has worst SAIDI in 2024; exactly 20 DER hosting records with valid feeder/constraint enums, all 4 feeder types present, connected DER never exceeding hosting capacity, Ausgrid has highest total connected DER; exactly 30 investment records for year 2024 with valid categories, all 6 categories present, RAB addition never exceeding investment, Ausgrid has highest total investment, all 10 DNSPs represented.

---

## Sprint 61c — Battery Storage Revenue Stacking Optimisation (2026-02-20)

**Backend (`app/backend/main.py`):** Appended models `BSOServiceAllocationRecord`, `BSOPriceCorrelationRecord`, `BSOOptimisationResultRecord`, `BSODegradationRecord`, and `StorageOptimisationDashboard` (all with `BSO` prefix to avoid collision). Mock data: 8 service allocation records representing real Australian utility-scale BESS projects (Hornsdale Power Reserve/SA, Victorian Big Battery/Geelong/VIC, Capital Power Station BESS/NSW, Lake Bonney BESS/SA, Gannawarra Energy Storage/VIC, Bomen Solar Farm BESS/NSW, Wandoan South BESS/QLD, Waratah Super Battery/NSW) with per-service % allocations summing to 100% and revenue/MW metrics; 12 monthly price correlation records for NSW1 (Jan–Dec 2024) including energy price, raise/lower reg and 6-sec FCAS prices, optimal service selection, and arbitrage spread (winter months show highest spreads); 5 optimisation scenario records (ENERGY_ONLY, FCAS_ONLY, ENERGY_FCAS, FULL_STACK, AI_OPTIMISED) for a 200 MW / 2 hr reference BESS with annual revenue, IRR, payback, CAPEX, and LCOE (AI_OPTIMISED: $72.8M, 21.4% IRR, 5.2 yr payback; ENERGY_ONLY: $28.4M, 7.2% IRR, 14.2 yr); 8 degradation records for HORNSDALE1 (years 1–4, replacement 2034) and VIC_BIG (years 1–4, replacement 2036) with capacity retention declining monotonically from ~99% to ~94–95%, calendar/cycle degradation components, annual cycles, and cumulative revenue impact. Endpoint: `GET /api/storage-optimisation/dashboard` → `StorageOptimisationDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/StorageOptimisationAnalytics.tsx`):** Created new file. Dark Tailwind theme with `Battery`, `Zap`, and `BarChart2` icons from lucide-react. 4 KPI cards: Best Revenue per MW (k AUD, sourced from Bomen at $330k/MW), Highest IRR Scenario % (AI_OPTIMISED at 21.4%), Most Profitable BESS name and revenue (Waratah $268.4M), AI Optimisation Uplift vs Energy Only (+156.3%). Service allocation stacked horizontal BarChart showing energy/raise FCAS/lower FCAS/capacity market/demand response/idle % per BESS (8 bars). Optimisation scenario comparison horizontal BarChart with per-bar colour cells and IRR annotation via LabelList on right. Price correlation LineChart showing NSW1 arbitrage spread, energy price, and raise reg price over 12 months with a $50 reference threshold line. Degradation impact LineChart showing capacity retention % over 4 years for HORNSDALE1 (amber) and VIC_BIG (emerald) with a 96% threshold reference line. Two summary tables: BESS Portfolio table and Optimisation Scenario Financials table with scenario badges colour-coded by tier.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `BSOServiceAllocationRecord`, `BSOPriceCorrelationRecord`, `BSOOptimisationResultRecord`, `BSODegradationRecord`, `StorageOptimisationDashboard` and exported `getStorageOptimisationDashboard()` function calling `/api/storage-optimisation/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import StorageOptimisationAnalytics from './pages/StorageOptimisationAnalytics'`; nav entry `{ to: '/storage-optimisation-analytics', label: 'Storage Revenue Optimisation', Icon: Battery }` (Battery already imported); `Route path="/storage-optimisation-analytics"` with `element={<StorageOptimisationAnalytics />}`.

**Tests (`tests/test_backend.py`):** Appended `TestStorageOptimisationAnalytics.test_storage_optimisation_dashboard` validating: HTTP 200, all 5 top-level keys; 8 service allocation records with per-record pct sum ~100%, valid regions, positive metrics, WARATAH_BESS largest by revenue, HORNSDALE1/VIC_BIG/WARATAH_BESS all present; 12 NSW1 monthly price records with valid optimal_service values, August spread exceeds summer average; 5 optimisation scenario records covering all 5 scenarios, AI_OPTIMISED beats ENERGY_ONLY on revenue/IRR/payback, FULL_STACK beats FCAS_ONLY; 8 degradation records for exactly 2 BESS across 4 years each, monotonically declining capacity retention per BESS, year 1 revenue impact = 0.

## Sprint 62a — NEM 5-Minute Settlement & Prudential Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended models `WSASettlementRecord`, `WSAPrudentialRecord`, `WSAShortfallRecord`, `WSAParticipantExposureRecord`, and `SettlementAnalyticsDashboard` (all with `WSA` prefix to avoid collision with existing `SettlementRecord` at line 353 and `PrudentialRecord` at line 12013). Mock data: 10 settlement records covering NSW1/VIC1/QLD1/SA1/TAS1 across 5 weeks (2024-W01 to 2024-W05) with total energy value, avg/peak settlement prices, 5-min vs 30-min variance, and positive/negative residue components; 12 prudential compliance records for major NEM participants (AGL Energy, Origin Energy, EnergyAustralia, Snowy Hydro, Alinta Energy, Neoen, Infigen, Tilt Renewables, Pacific Hydro, Stanwell, CS Energy, Walcha Energy) with credit support, maximum credit limits, utilisation %, collateral type (BANK_GUARANTEE/CASH/LETTER_OF_CREDIT), credit rating, and compliance status (1 BREACH for Walcha Energy at 96% utilisation, 4 WARNINGs, 7 COMPLIANT); 5 shortfall event records for 2024 (Walcha Energy x2 with DRAW_FINANCIAL_SECURITY, Neoen, Infigen, CS Energy) with event IDs, dates, shortfall amounts, types (SETTLEMENT/PRUDENTIAL/MARKET_FEES), resolution days, financial security drawn flag, and AEMO action; 20 participant exposure records for top 5 participants (AGL Energy/NSW1, Origin Energy/QLD1, EnergyAustralia/VIC1, Snowy Hydro/NSW1, Alinta Energy/SA1) across 4 weeks with gross purchases, gross sales, net positions, and exposure utilisation %. Endpoint: `GET /api/settlement-analytics/dashboard` → `SettlementAnalyticsDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/SettlementAnalytics.tsx`):** Created new file. Dark Tailwind theme with `CreditCard` icon from lucide-react. 4 KPI cards: Total Weekly Settlement Value (M AUD across all regions), 5-Min vs 30-Min Variance (M AUD cumulative residue), Highest Credit Limit Participant (AGL Energy $250M), Shortfall Events Count (5 YTD 2024). Settlement Value stacked BarChart by region/week with region colour palette (NSW1 blue, VIC1 purple, QLD1 amber, SA1 red, TAS1 emerald). Settlement variance ComposedChart showing positive residue (emerald bars), negative residue (red bars), and net variance line (amber). Participant Credit Exposure Utilisation LineChart for top 5 participants over 4 weeks with 85% warning reference line. Prudential Compliance table sorted by utilisation % descending with collateral type badges (blue=BANK_GUARANTEE, green=CASH, purple=LETTER_OF_CREDIT), credit rating, and compliance status badges (emerald=COMPLIANT, amber=WARNING, red=BREACH). Shortfall Event Log table with event ID, date, participant, shortfall M AUD (red), type badge (orange=SETTLEMENT, red=PRUDENTIAL, gray=MARKET_FEES), resolution days, financial security drawn (red=YES), and AEMO action text.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `WSASettlementRecord`, `WSAPrudentialRecord`, `WSAShortfallRecord`, `WSAParticipantExposureRecord`, `SettlementAnalyticsDashboard62a` and exported `getSettlementAnalyticsDashboard()` function calling `/api/settlement-analytics/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import SettlementAnalytics from './pages/SettlementAnalytics'`; added `CreditCard` to lucide-react imports; nav entry `{ to: '/settlement-analytics', label: '5-Min Settlement & Prudential', Icon: CreditCard }`; `Route path="/settlement-analytics"` with `element={<SettlementAnalytics />}`.

**Tests (`tests/test_backend.py`):** Appended `TestSettlementAnalytics.test_settlement_analytics_dashboard` validating: HTTP 200, all 5 top-level keys; 10 settlement records with valid regions, positive values, peak_interval > avg_price, positive_residue > negative_residue, SA1 higher variance than NSW1, at least 4 weeks covered; 12 prudential records with valid collateral types, compliance statuses, AGL Energy having highest max credit limit, Walcha Energy in BREACH, BREACH participant having highest utilisation, at least 1 WARNING; 5 shortfall events with positive amounts, valid types, boolean financial_security_drawn, DRAW_FINANCIAL_SECURITY action for drawn events, Walcha Energy present, largest shortfall having security drawn; 20 exposure records with positive gross values, net_position = sale - purchase (within 0.1M), valid regions, all 5 top participants present, at least 4 weeks of data.

---

## Sprint 62c — NEM Real-Time Operational Overview Dashboard (2026-02-20)

**Backend (`app/backend/main.py`):** Appended models `RTORegionSnapshot`, `RTOInterconnectorFlow`, `RTOFcasSnapshot`, `RTOSystemAlert`, and `RealtimeOpsDashboard` (all with `RTO` prefix to avoid collisions). Mock data: 5 NEM region snapshots (NSW1, QLD1, VIC1, SA1, TAS1) each with demand, generation, rooftop solar, net interchange, spot price, frequency, reserve, and a technology generation_mix dict; 6 interconnector flow records (QNI, NSW1-QLD1, VIC1-NSW1, V-SA, V-S-MNSP1, BASSLINK) with flow MW, capacity, utilisation %, binding flag and marginal loss; 8 FCAS service snapshots (RAISE/LOWER × 6SEC/60SEC/5MIN/REG) with cleared MW, clearing price, requirement, and surplus %; 6 system alerts with mixed severities (2× CRITICAL, 2× WARNING, 2× INFO) and categories (PRICE, FREQUENCY, CONSTRAINT, RESERVE, MARKET) representing a realistic SA1 high-price event scenario with an interconnector constraint. Endpoint: `GET /api/realtime-ops/dashboard` → `RealtimeOpsDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/RealtimeOperationsDashboard.tsx`):** Created new file. Premium dark Tailwind dashboard with pulsing green animated "LIVE" badge and AEST timestamp in header. 5 regional price tiles colour-coded by spot price (green <$100, amber $100–500, red >$500) showing region code, spot price, demand, generation, rooftop solar, reserve, and frequency Hz in a card grid. 5 generation-mix PieChart donuts (one per region, innerRadius donut style) using Recharts showing technology composition with FUEL_COLORS palette and percentage tooltips. Interconnector flow table with direction arrows, MW flow (green/amber colouring), capacity, utilisation progress bar (green/amber/red thresholds), BINDING badge, and MLF loss %. FCAS market section: 8 compact service cards (raise = emerald, lower = blue) showing cleared MW, requirement, clearing price, and surplus %; plus a side-by-side BarChart comparing cleared vs requirement for all 8 services. System alerts panel with colour-coded severity rows (CRITICAL/red, WARNING/amber, INFO/blue), category badge, region badge, message text, and acknowledged/unacknowledged state with CheckCircle indicator. Auto-refreshes every 30 s via `setInterval`.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `RTORegionSnapshot`, `RTOInterconnectorFlow`, `RTOFcasSnapshot`, `RTOSystemAlert`, `RealtimeOpsDashboard` and exported `getRealtimeOpsDashboard()` function calling `/api/realtime-ops/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import RealtimeOperationsDashboard from './pages/RealtimeOperationsDashboard'`; nav entry `{ to: '/realtime-operations', label: 'Live Ops Dashboard', Icon: Radio }` inserted prominently as 3rd item (after Home and Live Market) using the already-imported `Radio` icon; `Route path="/realtime-operations"` with `element={<RealtimeOperationsDashboard />}` added before Settings.

**Tests (`tests/test_backend.py`):** Appended `TestRealtimeOperationsDashboard.test_realtime_ops_dashboard` validating: HTTP 200; all 5 top-level keys present; exactly 5 region snapshots covering all NEM regions with positive demand/generation/price, plausible frequency (49–51 Hz), non-negative reserve, positive mix totals; SA1 spot price >$500 (high-price event); TAS1 hydro >400 MW; exactly 6 interconnector records including BASSLINK, V-SA, QNI, all with valid capacity/utilisation/marginal_loss ranges and bool binding; V-SA binding=True; exactly 8 FCAS services with all 8 required service names, cleared_mw >= requirement_mw, non-negative prices, RAISE_REG higher price than RAISE_6SEC; exactly 6 alerts with CRITICAL/WARNING/INFO severities all present, PRICE and FREQUENCY categories present, all required fields non-empty, CRITICAL alert for SA1, mix of acknowledged and unacknowledged.

---

## Sprint 62b — Renewable Energy Auction Results & CfD Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended enums `RAATechnologyEnum` (WIND_ONSHORE/WIND_OFFSHORE/UTILITY_SOLAR/HYBRID/STORAGE) and `RAAStatusEnum` (CONTRACTED/UNDER_CONSTRUCTION/COMMISSIONED/TERMINATED). Appended models `RAAAuctionResultRecord`, `RAATechnologyTrendRecord`, `RAAPerformanceRecord`, `RAAStateComparisonRecord`, and `RenewableAuctionDashboard` (all with `RAA` prefix to avoid collision). Mock data: 15 auction result records covering NSW LTESA Round 1 & 2 (AGL, Tilt Renewables, Origin, Akaysha), VIC VRET Rounds 2 & 3 (Vestas/CIP, Lightsource BP, Neoen, RES Australia), QLD QRET Rounds 1 & 2 (Acciona, Windlab, SunCable), SA REZ tenders (Pacific Hydro, Beon Energy), and one terminated NSW LTESA offshore wind contract; 20 technology trend records (UTILITY_SOLAR, WIND_ONSHORE, HYBRID, STORAGE × years 2018–2022) showing cost reduction curves, oversubscription ratios, and total contracted MW; 10 performance records for real Australian projects (Rye Park Wind, Bungaban Solar, Cattle Hill, Neoen Bulgana, Lal Lal Wind, Murra Warra II, Darlington Point, Crystal Brook, Coppabella, Snapper Point) with actual vs bid capacity factors, CfD payments, and market revenue; 5 state comparison records (NSW, VIC, QLD, SA, WA) with contracted MW, avg strike, cheapest technology, pipeline, and policy target completion %. Endpoint: `GET /api/renewable-auction/dashboard` → `RenewableAuctionDashboard`, uses `@app.get()` with `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/RenewableAuctionAnalytics.tsx`):** Created new file. Dark Tailwind theme with `Wind` and `Award` icons from lucide-react. 4 KPI cards: Total Contracted Capacity (GW across all contracts), Avg CfD Strike Price (AUD/MWh), Lowest-Ever Strike Price with auction name, Total CfD Payments (M AUD across operating projects). Auction Results table showing all 15 contracts with technology colour-coded badge, status badge, strike price highlighted in amber, and reference price. CfD Strike Price Trend LineChart (2018–2022) showing cost decline curves for all 4 technologies with a $60/MWh reference line. Technology Oversubscription BarChart comparing bid demand volume (demand_mw) vs contracted supply (total_contracted_mw) per technology in latest auction round, with per-technology colour cells. State Comparison table with progress bar showing completion % of policy target per state. Performance ScatterChart plotting actual vs bid capacity factor (bubble size = contracted MW), with a diagonal "bid = actual" reference line and per-point tooltip showing over/underperformance in percentage points.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `RAAAuctionResultRecord`, `RAATechnologyTrendRecord`, `RAAPerformanceRecord`, `RAAStateComparisonRecord`, `RenewableAuctionDashboard` and exported `getRenewableAuctionDashboard()` function calling `/api/renewable-auction/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import RenewableAuctionAnalytics from './pages/RenewableAuctionAnalytics'`. Nav entry `{ to: '/renewable-auction-analytics', label: 'Renewable Auction & CfD', Icon: Award }` added before Settings (Award already imported). `Route path="/renewable-auction-analytics"` with `element={<RenewableAuctionAnalytics />}` added.

**Tests (`tests/test_backend.py`):** Appended `TestRenewableAuctionAnalytics.test_renewable_auction_dashboard` validating: HTTP 200, all 5 top-level keys present; exactly 15 auction result records with valid technology/status/state enums, positive capacity and strike prices, all CfD terms 5–25 years, at least one TERMINATED record, cheapest record being UTILITY_SOLAR; exactly 20 technology trend records (4 techs × 5 years 2018–2022) with min_strike ≤ avg_strike, oversubscription ≥ 1, 2018 baseline having 0% cost reduction, UTILITY_SOLAR cheaper than STORAGE in 2022; exactly 10 performance records with valid ranges for capacity factors (0–100%), non-negative CfD payments, positive market revenue, at least one CfD-paying project, wind having higher average CF than solar; exactly 5 state comparison records with pipeline not exceeding policy target, VIC having higher contracted MW than NSW, UTILITY_SOLAR cheapest in at least 3 states.

---

## Sprint 63a — Power Outage Economic Cost / VoLL Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended enums `VCAMethodologyEnum` (SURVEY/REVEALED_PREFERENCE/HYBRID) and `VCASensitivityEnum` (HIGH/MEDIUM/LOW). Added Pydantic models `VCAVollRecord`, `VCAOutageCostRecord`, `VCAIndustrySectorRecord`, `VCAReliabilityValueRecord`, and `VollAnalyticsDashboard`. Mock data: 6 VoLL estimate records (3 years × 2 methodologies), 15 outage cost records (5 NEM regions × 3 years: 2022–2024), 10 industry sector records (Data Centres through Residential, ordered by outage cost severity), 5 reliability value records (NSW/VIC/QLD/SA/TAS). Endpoint `GET /api/voll-analytics/dashboard` returns `VollAnalyticsDashboard` with `Depends(verify_api_key)`.

**Frontend (`app/frontend/src/pages/VollAnalytics.tsx`):** New page. Four KPI cards: NEM Regulatory VoLL (AUD/MWh), 2024 total outage costs (M AUD), highest-cost sector (AUD/hr), average benefit-cost ratio for reliability investment. VoLL estimate comparison grouped BarChart (residential/commercial/industrial per methodology per year) with regulatory VoLL ReferenceLine. Regional outage cost trend LineChart (5 lines, 3 years). Industry sector sensitivity table with colour-coded HIGH/MEDIUM/LOW badges and backup adoption progress bars. Reliability investment ScatterChart (SAIDI reduction vs VoLL saved, bubble size proportional to investment cost, custom tooltip showing BCR). Recharts + lucide-react (DollarSign, AlertTriangle, Zap) + Tailwind dark theme.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `VCAVollRecord`, `VCAOutageCostRecord`, `VCAIndustrySectorRecord`, `VCAReliabilityValueRecord`, `VollAnalyticsDashboard` and exported `getVollAnalyticsDashboard()` calling `GET /api/voll-analytics/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import VollAnalytics from './pages/VollAnalytics'`. Nav entry `{ to: '/voll-analytics', label: 'VoLL Outage Cost Analytics', Icon: AlertTriangle }` added before Settings. `Route path="/voll-analytics"` with `element={<VollAnalytics />}` added.

**Tests (`tests/test_backend.py`):** Appended `TestVollAnalytics.test_voll_analytics_dashboard` validating: HTTP 200; all 5 dashboard keys present; exactly 6 VoLL estimate records with valid methodologies and positive values; commercial VoLL exceeding residential for each record; 3 distinct years with latest regulatory VoLL exceeding earliest; exactly 15 outage cost records covering all 5 NEM regions; component costs summing to total within tolerance; direct+indirect cost percentages summing to 100; QLD average cost exceeding TAS; exactly 10 industry sector records with valid sensitivities and backup adoption in 0–100%; all HIGH-sensitivity sector costs exceeding all LOW-sensitivity costs; exactly 5 reliability value records with current SAIDI exceeding target; VoLL saved exceeding investment cost for each region; NSW having highest customers_benefited_k.

---

## Sprint 63c – Demand Flexibility & Industrial Load Management Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended Pydantic enums `DFLIndustryType`, `DFLContractType`, `DFLTriggerType`, `DFLTechnologyType` and models `DFLConsumerRecord`, `DFLEventRecord`, `DFLBenefitRecord`, `DFLTechnologyRecord`, `DemandFlexibilityDashboard`. Added `GET /api/demand-flexibility/dashboard` endpoint (protected by `verify_api_key`) returning 10 consumer records (across ALUMINIUM_SMELTER, STEEL_EAF, DATA_CENTRE, WATER_UTILITY, MINING, CEMENT, PAPER_PULP, COLD_STORAGE industries), 8 demand response event records, 10 benefit records (with stacked revenue/savings breakdown), and 6 technology records. `DFL` prefix used on all new model classes to avoid collision with existing `IndustrialLoadShapeRecord` and `DemandFlexAggregateRecord`.

**Frontend (`app/frontend/src/pages/DemandFlexibilityAnalytics.tsx`):** New page created with Recharts and Tailwind dark theme. Includes: header with `Gauge` icon; 4 KPI cards (total flexible capacity MW, total DR events 2024, total consumer benefits M AUD, average response time min); industrial consumer flexibility table with colour-coded industry badges, contract type badges, flexible MW, flexibility %, response time, and annual revenue; demand response event log with trigger badges, curtailed MW, duration, price, cost avoided, and colour-coded success rate; stacked horizontal BarChart of economic benefits per consumer (flexibility revenue + energy savings + network savings); ScatterChart of technology response time vs reliability score sized by adoption %; technology detail table with reliability colour-coding.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `DFLConsumerRecord`, `DFLEventRecord`, `DFLBenefitRecord`, `DFLTechnologyRecord`, `DemandFlexibilityDashboard` and exported `getDemandFlexibilityDashboard()` calling `GET /api/demand-flexibility/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import DemandFlexibilityAnalytics from './pages/DemandFlexibilityAnalytics'`. Nav entry `{ to: '/demand-flexibility-analytics', label: 'Demand Flexibility & ILM', Icon: Gauge }` added before Settings (Gauge already imported at line 34). `Route path="/demand-flexibility-analytics"` with `element={<DemandFlexibilityAnalytics />}` added.

**Tests (`tests/test_backend.py`):** Appended `TestDemandFlexibilityAnalytics.test_demand_flexibility_dashboard` validating: HTTP 200, all 5 top-level keys present; exactly 10 consumer records with valid industry/contract enums, positive MW values, flexibility_pct in range, at least 2 aluminium smelters; exactly 8 event records with valid trigger enums, positive curtailed_mw/duration/participants/price/cost_avoided, success_rate in 0–100, at least 1 HIGH_PRICE event; exactly 10 benefit records with non-negative values and total matching component sum within 0.05 tolerance; exactly 6 technology records with valid technology enum, adoption 0–100, non-negative response time, AUTOMATED_DR faster than MANUAL_DR, SMART_INVERTER reliability >= 9.0.

## Sprint 63b — ASX Energy Futures Price Discovery & Term Structure Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended Pydantic models `FPDTermStructureRecord` (region, contract_month, product BASE/PEAK/CAP, settlement_price_aud_mwh, open_interest_lots, daily_volume_lots, implied_vol_pct, days_to_expiry), `FPDBasisRecord` (region, month, futures_price, spot_price, basis_aud, basis_pct, convergence_trend CONVERGING/DIVERGING/STABLE, seasonal_factor), `FPDCarryRecord` (region, near_contract, far_contract, carry_cost_aud, storage_premium_aud, risk_premium_aud, convenience_yield_pct), `FPDCurveShapeRecord` (region, snapshot_date, curve_shape CONTANGO/BACKWARDATION/FLAT/KINKED, q1-q4 prices, annual_slope_pct, inflection_quarter), and `FuturesPriceDiscoveryDashboard`. Mock data: 20 term structure records (5 regions × 4 quarterly contracts: 2025-Q1 to 2025-Q4), 20 basis records (5 regions × 4 months), 10 carry records (5 regions × 2 near/far spreads), 10 curve shape records (5 regions × 2 snapshots). Endpoint `GET /api/futures-price-discovery/dashboard` secured with `Depends(verify_api_key)`.

**Frontend (`app/frontend/src/pages/FuturesPriceDiscovery.tsx`):** New page created. Fetches `/api/futures-price-discovery/dashboard` via `getFuturesPriceDiscoveryDashboard()`. Four KPI cards: most liquid contract (by open interest), largest basis (AUD/MWh), steepest curve slope (%), highest implied vol (%). Recharts `LineChart` for forward curve by region over 4 quarters. `LineChart` with `ReferenceLine` for basis analysis (futures vs spot with spot reference lines annotated per region). `BarChart` for carry cost breakdown (storage premium, risk premium, convenience yield per near→far spread). Summary table with curve shape badges (CONTANGO green, BACKWARDATION red, FLAT gray, KINKED amber), Q1-Q4 prices, annual slope %, and inflection quarter. Tailwind dark theme throughout.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `FPDTermStructureRecord`, `FPDBasisRecord`, `FPDCarryRecord`, `FPDCurveShapeRecord`, `FuturesPriceDiscoveryDashboard` and exported `getFuturesPriceDiscoveryDashboard()` calling `GET /api/futures-price-discovery/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import FuturesPriceDiscovery from './pages/FuturesPriceDiscovery'`. Nav entry `{ to: '/futures-price-discovery', label: 'Futures Price Discovery', Icon: TrendingUp }` added before Settings (TrendingUp already imported). `Route path="/futures-price-discovery"` with `element={<FuturesPriceDiscovery />}` added.

**Tests (`tests/test_backend.py`):** Appended `TestFuturesPriceDiscovery.test_futures_price_discovery_dashboard` validating: HTTP 200, all 5 top-level keys present; exactly 20 term structure records with valid region/product/quarter enums, positive settlement prices, OI and volume, implied vol in 0-100% range, SA prices exceeding TAS prices across all quarters, max OI >= 10000 lots; exactly 20 basis records with valid convergence trend enums, positive futures/spot prices, basis_aud consistency (futures-spot), non-empty seasonal_factor; exactly 10 carry records covering all 5 regions with distinct near/far contracts, non-negative storage and risk premiums; exactly 10 curve shape records covering all 5 regions with valid shape enums, positive Q1-Q4 prices, inflection quarter starting with "Q", SA all CONTANGO, TAS all FLAT, NSW all KINKED.

## Sprint 64c — Electricity Consumer Price Index & Retail Market Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended Pydantic models `EPICpiRecord` (quarter, electricity_cpi_index, electricity_cpi_yoy_pct, all_cpi_index, all_cpi_yoy_pct, electricity_vs_all_cpi_diff_pct, state, key_driver), `EPIDmoRecord` (year, state, distribution_zone, annual_usage_kwh, dmo_price_aud, dmo_change_pct, market_offer_avg_aud, market_offer_avg_change_pct, best_market_offer_aud, potential_saving_aud), `EPITariffComponentRecord` (state, year, network/wholesale/environmental/retail_margin/metering charges in AUD/kWh, total_tariff_aud_kwh), `EPIRetailerRecord` (retailer, state, market_share_pct, avg_offer_aud, cheapest_offer_aud, customer_satisfaction_score, complaints_per_1000, churn_rate_pct), and `ElectricityPriceIndexDashboard`. Mock data: 24 CPI records (6 quarters × 4 states: NSW/VIC/QLD/SA), 20 DMO records (5 states × 4 years 2021–2024), 20 tariff component records (5 states × 4 years), 15 retailer records covering NSW/VIC/QLD/SA/WA. Endpoint `GET /api/electricity-price-index/dashboard` secured with `Depends(verify_api_key)`. No prefix collisions found before adding.

**Frontend (`app/frontend/src/pages/ElectricityPriceIndex.tsx`):** New page created with Recharts and Tailwind dark theme. Includes: header with `ShoppingCart` icon; 4 KPI cards (latest NSW electricity CPI YoY %, electricity vs all-CPI premium %, max DMO potential saving in AUD with state label, highest market share retailer with market share % and state); CPI comparison ComposedChart with dual Y-axis showing electricity CPI YoY % (left) and all-CPI YoY % (right) for NSW across 6 quarters with a red RBA reference line at 7%; DMO vs market offer BarChart showing DMO price, average market offer, and best market offer per state for 2024; tariff component stacked area ComposedChart for NSW 2021–2024 with network, wholesale, environmental, and retail margin components; retailer comparison table with market share progress bars, avg offer, cheapest offer, colour-coded satisfaction score (green >= 8, yellow >= 7, red otherwise), complaints per 1000, and churn rate — sorted by market share descending.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `EPICpiRecord`, `EPIDmoRecord`, `EPITariffComponentRecord`, `EPIRetailerRecord`, `ElectricityPriceIndexDashboard` and exported `getElectricityPriceIndexDashboard()` calling `GET /api/electricity-price-index/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `ShoppingCart` to lucide-react imports. Added `import ElectricityPriceIndex from './pages/ElectricityPriceIndex'`. Nav entry `{ to: '/electricity-price-index', label: 'Electricity CPI & Retail', Icon: ShoppingCart }` added before Settings. `Route path="/electricity-price-index"` with `element={<ElectricityPriceIndex />}` added.

**Tests (`tests/test_backend.py`):** Appended `TestElectricityPriceIndex.test_electricity_price_index_dashboard` validating: HTTP 200, all 5 top-level keys present; exactly 24 CPI records with positive YoY values, valid states, non-empty key_driver, SA avg CPI YoY exceeding NSW; exactly 20 DMO records covering all 5 states and 4 years, DMO price always exceeding best market offer, positive potential savings, SA 2024 DMO exceeding VIC 2024 DMO; exactly 20 tariff records with component sum matching total within 0.005 tolerance, network charges exceeding retail margin, NSW 2023 total exceeding 2021 total; exactly 15 retailer records with market_share_pct in 0–100, satisfaction_score in 0–10, avg_offer >= cheapest_offer, non-negative complaints and churn, Synergy having the highest market share overall, and Red Energy having the highest satisfaction score among VIC retailers.

## Sprint 64a — Marginal Loss Factor (MLF) Deep Dive Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended Pydantic models `MLFConnectionPointRecord` (connection_point, generator_name, technology, region, mlf_2024, mlf_2023, mlf_2022, mlf_trend IMPROVING/DECLINING/STABLE, revenue_impact_pct, reason), `MLFRezRecord` (rez_id, rez_name, region, projected_mlf_2028, current_mlf, mlf_deterioration_pct, connected_capacity_mw, pipeline_capacity_mw, risk_level HIGH/MEDIUM/LOW, aemo_mitigation), `MLFRevenueImpactRecord` (generator_name, technology, capacity_mw, annual_generation_gwh, mlf_value, spot_price_aud_mwh, effective_price_aud_mwh, revenue_loss_m_aud, revenue_loss_pct), `MLFHistoricalRecord` (year, region, avg_mlf, min_mlf, max_mlf, generators_below_095, total_generators, avg_revenue_impact_pct), and `MlfAnalyticsDashboard`. Used `MLF` prefix to avoid collision with existing `MlfRecord` class. Mock data: 15 connection point records across all 5 NEM regions covering solar and wind assets, 8 REZ records mapping current to projected 2028 MLFs for N1/N2/V1/V2/Q1/Q2/S1/T1 zones, 12 revenue impact records with effective_price = spot × MLF, 20 historical records (5 regions × 4 years 2021–2024). Endpoint `GET /api/mlf-analytics/dashboard` secured with `Depends(verify_api_key)`.

**Frontend (`app/frontend/src/pages/MlfAnalytics.tsx`):** New page created. Fetches `/api/mlf-analytics/dashboard` via `getMlfAnalyticsDashboard()`. Header with `TrendingDown` icon. Four KPI cards: lowest MLF 2024 (generator name), highest revenue loss M AUD (generator name), REZs at HIGH risk (count of N total), generators below 0.95 MLF. Connection point MLF table with technology colour badges, region colour-coded, MLF 2022/2023/2024 with red highlight for values < 0.95, trend badge (DECLINING red, IMPROVING green, STABLE gray), revenue impact % with sign-aware colouring. REZ risk table with current vs projected 2028 MLF, deterioration %, connected and pipeline capacity MW, risk badge (HIGH red, MEDIUM amber, LOW green), and AEMO mitigation text. Revenue impact scatter chart (x=MLF value, y=revenue loss %, bubble size=capacity MW, colour=technology type) with vertical reference line at MLF=0.95. Historical average MLF line chart (5 regions, 4 years) with horizontal ReferenceLine at 0.95 threshold. Revenue loss bar chart sorted by magnitude, coloured by technology. Tailwind dark theme throughout.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `MLFConnectionPointRecord`, `MLFRezRecord`, `MLFRevenueImpactRecord`, `MLFHistoricalRecord`, `MlfAnalyticsDashboard` and exported `getMlfAnalyticsDashboard()` calling `GET /api/mlf-analytics/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import MlfAnalytics from './pages/MlfAnalytics'`. Nav entry `{ to: '/mlf-analytics', label: 'MLF Deep Dive Analytics', Icon: TrendingDown }` added before Settings (TrendingDown already imported at line 6). `Route path="/mlf-analytics"` with `element={<MlfAnalytics />}` added before settings route.

**Tests (`tests/test_backend.py`):** Appended `TestMlfAnalytics.test_mlf_analytics_dashboard` validating: HTTP 200, all 5 top-level keys present; exactly 15 connection point records with valid mlf_trend/region enums, MLF values in range 0–1.1, non-empty connection_point and generator_name, at least 1 DECLINING and 1 IMPROVING trend, at least 1 generator with MLF 2024 < 0.95; exactly 8 REZ records with valid risk_level/region enums, non-negative deterioration, positive capacities, non-empty mitigation, at least 4 HIGH risk REZs; exactly 12 revenue impact records with positive capacity/generation/prices, effective_price approximately equal to spot × MLF within $2 tolerance, at least 1 generator with revenue loss > $5M AUD; exactly 20 historical records (5 regions × 4 years) with min <= avg <= max MLF constraint, non-negative generators_below_095 not exceeding total_generators, SA avg_mlf 2024 lower than TAS avg_mlf 2024, TAS generators_below_095 = 0 in all years.

## Sprint 64b — Interconnector Upgrade Business Case Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended Pydantic models `ICBProjectRecord` (project_id, project_name, from_region, to_region, capacity_increase_mw, capex_bn_aud, opex_m_aud_yr, commissioning_year, status OPERATING/CONSTRUCTION/APPROVED/PROPOSED, aer_approved bool, regulatory_test_outcome PASS/FAIL/PENDING, bcr, npv_bn_aud), `ICBBenefitRecord` (project_id, benefit_type CONGESTION_RENT/FUEL_COST_SAVING/RELIABILITY/RENEWABLE_FIRMING/AVOIDED_INVESTMENT/CONSUMER_SURPLUS, benefit_m_aud_yr, confidence HIGH/MEDIUM/LOW, beneficiary_region, quantification_method), `ICBScenarioRecord` (project_id, scenario STEP_CHANGE/CENTRAL/SLOW_CHANGE, npv_bn_aud, bcr, consumer_benefit_bn_aud, renewable_firming_mw, breakeven_price_aud_mwh), `ICBFlowAnalysisRecord` (project_id, year, annual_flow_twh, peak_flow_mw, avg_utilisation_pct, congestion_hours_pct, marginal_value_aud_mwh), and `InterconnectorUpgradeDashboard`. All enums use string literals with `ICB` prefix to avoid collisions. Mock data: 6 project records (HumeLink, VNI-West, QNI-Middleware, EnergyConnect, Marinus Link Stage 1, CopperString 2.0); 20 benefit records spanning CONGESTION_RENT, FUEL_COST_SAVING, RELIABILITY, RENEWABLE_FIRMING, AVOIDED_INVESTMENT, CONSUMER_SURPLUS with HIGH/MEDIUM/LOW confidence and PLEXOS/ISP/LOLP/CGE quantification methods; 18 scenario records (6 projects × 3 AEMO ISP scenarios) with STEP_CHANGE NPV >= CENTRAL NPV >= SLOW_CHANGE NPV for every project; 18 flow analysis records (6 projects × 3 projection years) with utilisation increasing over time. Endpoint `GET /api/interconnector-upgrade/dashboard` secured with `Depends(verify_api_key)`.

**Frontend (`app/frontend/src/pages/InterconnectorUpgradeAnalytics.tsx`):** New page created. Fetches `/api/interconnector-upgrade/dashboard` via `getInterconnectorUpgradeDashboard()`. Header with `ArrowLeftRight` icon. Four KPI cards: total capex pipeline (bn AUD across all projects), total portfolio NPV (central scenario), projects with BCR > 1 out of total, highest congestion relief (project + year + pre-upgrade congestion hours %). Project overview table with project name (colour dot), from/to route, capex, capacity increase MW, status badge (OPERATING green, CONSTRUCTION blue, APPROVED amber, PROPOSED gray), AER approved (Yes/No), BCR (green >= 1, red < 1), NPV (green positive, red negative), commissioning year. Benefit breakdown stacked BarChart (benefit type on Y-axis as M AUD/yr, project on X-axis, stacked by CONGESTION_RENT/FUEL_COST_SAVING/RELIABILITY/RENEWABLE_FIRMING/AVOIDED_INVESTMENT/CONSUMER_SURPLUS with distinct colours, human-readable legend). Scenario NPV comparison grouped BarChart (3 scenarios per project, bn AUD, with ReferenceLine at 0 to highlight negative NPV). Flow analysis LineChart (annual flow TWh by project over projected years with connectNulls for sparse data coverage). Tailwind dark theme throughout.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `ICBProjectRecord`, `ICBBenefitRecord`, `ICBScenarioRecord`, `ICBFlowAnalysisRecord`, `InterconnectorUpgradeDashboard` and exported `getInterconnectorUpgradeDashboard()` calling `GET /api/interconnector-upgrade/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import InterconnectorUpgradeAnalytics from './pages/InterconnectorUpgradeAnalytics'`. Nav entry `{ to: '/interconnector-upgrade-analytics', label: 'Interconnector Upgrade Business Case', Icon: ArrowLeftRight }` added before Settings (ArrowLeftRight already imported). `Route path="/interconnector-upgrade-analytics"` with `element={<InterconnectorUpgradeAnalytics />}` added before settings route.

**Tests (`tests/test_backend.py`):** Appended `TestInterconnectorUpgradeAnalytics.test_interconnector_upgrade_dashboard` validating: HTTP 200; all 5 top-level keys present; exactly 6 project records matching expected project IDs, valid status/regulatory_test_outcome enums, positive capex/capacity/BCR, bool aer_approved, commissioning_year in 2020–2040 range; EnergyConnect is OPERATING with AER approved and PASS outcome and BCR > 1.5; QNI-Middleware is PROPOSED, not AER approved, PENDING outcome; exactly 20 benefit records with valid benefit_type/confidence enums, positive benefit_m_aud_yr, every project having at least one benefit; exactly 18 scenario records with valid scenario enums, positive BCR and breakeven_price, non-negative firming_mw, STEP_CHANGE NPV >= CENTRAL NPV >= SLOW_CHANGE NPV constraint verified for all 6 projects; exactly 18 flow analysis records with year in 2020–2040, positive flow/peak values, utilisation in 0–100%, congestion in 0–100%, positive marginal value, and utilisation monotonically non-decreasing over time per project.

**Sprint 65a — CSP & Solar Thermal Technology Analytics (2026-02-20)**

**Backend (`app/backend/main.py`):** Appended Pydantic models `CSPTechnologyRecord` (tech_id, name PARABOLIC_TROUGH/SOLAR_TOWER/LINEAR_FRESNEL/DISH_STIRLING, peak_efficiency_pct, annual_capacity_factor_pct, storage_hours, capex_m_aud_mw, lcoe_aud_mwh, water_use_l_mwh, land_use_ha_mw, trl 1-9, dispatchability_score 0-10), `CSPProjectRecord` (project_id, project_name, developer, region, technology, capacity_mw, storage_hours, status OPERATING/CONSTRUCTION/APPROVED/PROPOSED/CANCELLED, capex_m_aud, cod_year, dni_kwh_m2_yr, expected_lcoe_aud_mwh), `CSPResourceRecord` (location, state, dni_kwh_m2_yr, dni_class WORLD_CLASS/EXCELLENT/GOOD/MARGINAL, area_km2, grid_distance_km, proximity_to_existing_project), `CSPDispatchRecord` (project_name, month, solar_mw, storage_mw, total_output_mw, storage_utilisation_pct, curtailment_mw, firming_hours_provided), and `CspAnalyticsDashboard`. Mock data: 4 technology records (Parabolic Trough, Solar Tower, Linear Fresnel, Dish Stirling) with full techno-economic specifications; 8 project records (Port Augusta Renewable Energy Park, Vast Solar Jemalong Pilot, Aurora Solar, Sundrop Farms CSP, Vast Solar Mt Isa, Glenrowan Parabolic Trough, Pilbara Solar Thermal Hub, Mildura Solar Concentrator); 10 DNI resource records (Longreach, Port Augusta, Newman, Alice Springs, Broken Hill, Mt Isa, Mildura, Carnarvon, Coober Pedy, Geraldton) with WORLD_CLASS/EXCELLENT classifications; 12 monthly dispatch records for Vast Solar Jemalong Pilot with seasonal solar/storage variation. Endpoint `GET /api/csp-analytics/dashboard` secured with `Depends(verify_api_key)`.

**Frontend (`app/frontend/src/pages/CspAnalytics.tsx`):** New page created. Fetches `/api/csp-analytics/dashboard` via `getCspAnalyticsDashboard()`. Header with `Sun` icon. Four KPI cards: total CSP pipeline capacity (MW, operating + construction + approved), average portfolio LCOE (AUD/MWh), world-class DNI site count, average dispatchability score. Technology Comparison RadarChart with 5 axes (Efficiency, Capacity Factor, Storage Hours, LCOE inverse, Dispatchability) comparing all 4 technologies in distinct colours (amber/orange/cyan/pink). LCOE BarChart by technology with colour-coded cells. Monthly dispatch AreaChart with gradient fills comparing solar vs thermal storage contributions over 12 months with seasonal variation. Project pipeline table with status badges (OPERATING green, CONSTRUCTION blue, APPROVED amber, PROPOSED purple, CANCELLED red) and technology badges. DNI resource quality table with DNI class badges and proximity-to-project indicators. Technology specification cards showing all 9 parameters per technology. Tailwind dark theme throughout.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `CSPTechnologyRecord`, `CSPProjectRecord`, `CSPResourceRecord`, `CSPDispatchRecord`, `CspAnalyticsDashboard` and exported `getCspAnalyticsDashboard()` calling `GET /api/csp-analytics/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import CspAnalytics from './pages/CspAnalytics'`. Nav entry `{ to: '/csp-analytics', label: 'CSP & Solar Thermal Analytics', Icon: Sun }` added after MLF Analytics (Sun already imported). `Route path="/csp-analytics"` with `element={<CspAnalytics />}` added before settings route.

**Tests (`tests/test_backend.py`):** Appended `TestCspAnalytics.test_csp_analytics_dashboard` validating: HTTP 200; all 5 top-level keys present; exactly 4 technology records with valid tech names, efficiency/capacity factor in 0-100%, non-negative storage hours, positive capex/LCOE, TRL in 1-9, dispatchability 0-10; SOLAR_TOWER dispatchability exceeds DISH_STIRLING; exactly 8 project records with valid status/technology enums, positive capacity/capex/LCOE, cod_year in 2000-2040; Vast Solar Jemalong is OPERATING SOLAR_TOWER; Sundrop Farms uses LINEAR_FRESNEL; Pilbara is APPROVED with >= 10 storage hours; exactly 10 resource records with valid DNI class enums, positive area/DNI, at least 4 WORLD_CLASS sites each with DNI >= 2700; Newman WA is WORLD_CLASS; exactly 12 dispatch records covering all 12 months; January solar output exceeds June solar output (seasonal check).

## Sprint 65c — Carbon Intensity Real-Time & Historical Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended Pydantic models `CIRGridIntensityRecord` (region, year, month, avg/min/max_intensity_kgco2_mwh, zero_carbon_hours_pct, total_emissions_kt_co2, vre_penetration_pct), `CIRMarginalEmissionRecord` (region, hour 0-23, marginal_emission_factor_kgco2_mwh, marginal_technology, typical_price_aud_mwh, flexibility_benefit_kg_co2_kwh), `CIRTechnologyEmissionRecord` (technology, lifecycle/operational/construction/fuel_kgco2_mwh, category FOSSIL/LOW_CARBON/RENEWABLE/STORAGE), `CIRDecarbonisationRecord` (region, year, emissions_mt_co2, intensity_kgco2_mwh, vre_pct, coal_pct, gas_pct, target_intensity_kgco2_mwh, target_year, on_track bool), and `CarbonIntensityDashboard`. All models use `CIR` prefix to avoid collision with pre-existing `CarbonIntensityRecord` (Sprint 15c). Mock data: 30 grid intensity records (5 regions × 6 months Jul-Dec 2024) with SA intensity ~230 kgCO2/MWh and TAS ~50, SA/TAS with highest VRE penetration and zero-carbon hours; 24 marginal emission records (4 regions × 6 sampled hours) with overnight coal MEF ~900 and midday solar MEF ~15-50 for NSW, SA scaled to 0.6× NSW due to high renewables; 12 technology records spanning run-of-river hydro (4 kgCO2/MWh) to black coal (870 kgCO2/MWh) across RENEWABLE/STORAGE/LOW_CARBON/FOSSIL categories; 20 decarbonisation records (5 regions × 4 years 2021-2024) with declining intensity trend per region, on_track based on linear interpolation toward target. Endpoint `GET /api/carbon-intensity/dashboard` secured with `Depends(verify_api_key)`.

**Frontend (`app/frontend/src/pages/CarbonIntensityAnalytics.tsx`):** New page created with Recharts and Tailwind dark theme. Header with `Leaf` icon. Four KPI cards: lowest regional intensity (kgCO2/MWh with region/month label), average zero-carbon hours % across regions, national total emissions (Mt CO2), and regions on track (count / total). Grid intensity trend LineChart: 5 coloured lines (NSW blue, VIC purple, QLD orange, SA green, TAS amber) over 6 months, Y-axis in kg units, SA and TAS visibly lowest. Marginal emission factor BarChart for NSW by 6 sampled hours: color-coded green (<300), amber (300-600), red (>600) kgCO2/MWh with ReferenceLine at 300 and tooltip showing marginal technology. Technology lifecycle emissions horizontal BarChart: 12 technologies sorted cleanest to dirtiest, color-coded by category (RENEWABLE green, STORAGE blue, LOW_CARBON amber, FOSSIL red), with legend. Decarbonisation progress table for 2024: columns for region (with color dot), actual intensity, target intensity, target year, VRE %, coal %, emissions (Mt), on-track badge (green/red pill).

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `CIRGridIntensityRecord`, `CIRMarginalEmissionRecord`, `CIRTechnologyEmissionRecord`, `CIRDecarbonisationRecord`, `CarbonIntensityDashboard` and exported `getCarbonIntensityDashboard()` calling `GET /api/carbon-intensity/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import CarbonIntensityAnalytics from './pages/CarbonIntensityAnalytics'`. Nav entry `{ to: '/carbon-intensity-analytics', label: 'Carbon Intensity Analytics', Icon: Leaf }` added before Settings (Leaf already imported at line 19). `Route path="/carbon-intensity-analytics"` with `element={<CarbonIntensityAnalytics />}` added before settings route.

**Tests (`tests/test_backend.py`):** Appended `TestCarbonIntensityAnalytics.test_carbon_intensity_dashboard` validating: HTTP 200; all 5 top-level keys present; exactly 30 grid intensity records with valid regions, min <= avg <= max intensity constraint, zero_carbon_hours_pct in 0-100, positive emissions, SA avg lower than NSW avg, TAS avg lower than SA avg, exactly 6 distinct months; exactly 24 marginal emission records with valid MEF regions, hours 0-23, non-negative MEF/flexibility, valid marginal technologies (Black Coal/Gas CCGT/Gas OCGT/Solar PV), NSW daytime MEF lower than overnight MEF, SA avg MEF lower than NSW avg MEF; exactly 12 technology records with valid categories, non-negative emission components, all FOSSIL lifecycle > all RENEWABLE lifecycle, Black Coal having the maximum lifecycle emissions; exactly 20 decarbonisation records with valid regions/years (2021-2024), positive intensity/emissions, on_track boolean, per-region declining intensity 2021-2024, TAS 2024 < NSW 2024 intensity, at least 1 region on track in 2024.

## Sprint 65b — Network Tariff Reform & DER Incentive Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended Pydantic models `NTRTariffStructureRecord` (dnsp, tariff_class RESIDENTIAL/SME/LARGE_COMMERCIAL/INDUSTRIAL, tariff_type FLAT/TOU/DEMAND/DYNAMIC_NETWORK, daily_supply_aud, flat/peak/offpeak rates as Optional[float], demand_rate_aud_kw_month Optional[float], solar_export_rate_aud_kwh Optional[float], customers_k), `NTRTariffImpactRecord` (tariff_type, customer_type AVERAGE/HIGH_SOLAR/EV_OWNER/BATTERY_OWNER/HIGH_DEMAND, annual_bill before/after, bill_change_pct, der_incentive_score 0-10, peak_shift_mw_potential), `NTRDerIncentiveRecord` (incentive_type SOLAR_FIT/BATTERY_REBATE/EV_SMART_CHARGING/VPP_PARTICIPATION/DEMAND_RESPONSE/SOLAR_SPONGE, dnsp, incentive_value_aud, eligible_customers_k, uptake_rate_pct, peak_reduction_mw, annual_network_benefit_m_aud), `NTRReformOutcomeRecord` (dnsp, reform_name, implementation_year, customers_affected_k, peak_demand_reduction_mw, revenue_neutral bool, consumer_avg_saving_aud, aer_approved bool), and `NTRDashboard` (timestamp, tariff_structures, tariff_impacts, der_incentives, reform_outcomes). All models use `NTR` prefix to avoid collision with existing `NetworkTariffReformDashboard` and `TariffReformRecord`. Mock data: 20 tariff structure records (5 DNSPs — Ausgrid, Energex, SA Power Networks, CitiPower, Western Power — each with 4 tariff classes); 20 tariff impact records (4 tariff types × 5 customer types with realistic bill savings from FLAT to DYNAMIC_NETWORK); 15 DER incentive records spread across all 5 DNSPs covering SOLAR_FIT, BATTERY_REBATE, EV_SMART_CHARGING, VPP_PARTICIPATION, DEMAND_RESPONSE, SOLAR_SPONGE; 8 reform outcome records for major DNSP tariff reforms 2021–2025. Endpoint `GET /api/tariff-reform/dashboard` (distinct from existing `/api/network-tariff-reform/dashboard`) secured with `Depends(verify_api_key)`.

**Frontend (`app/frontend/src/pages/NetworkTariffReformAnalytics.tsx`):** New page created. Fetches `/api/tariff-reform/dashboard` via `getNTRDashboard()`. Header with `Sliders` icon. Four KPI cards: tariff types in market (count of distinct tariff_type values), highest peak demand reduction MW (across all reform outcomes), total DER incentive spending M AUD (uptake-weighted sum across all incentives), highest solar export rate AUD/kWh. Tariff structure comparison table with DNSP, tariff class badge (blue/amber/purple/red by class), tariff type badge (gray/blue/amber/emerald by type), daily supply charge, flat rate (c/kWh), peak rate (red), off-peak rate (blue), demand charge (amber, $/kW/month), solar export rate (emerald), customers (k). Bill impact grouped bar chart (before vs after reform) with tariff-type selector buttons (FLAT/TOU/DEMAND/Dynamic), bars coloured per tariff type. DER incentive effectiveness ScatterChart (x=uptake rate %, y=annual network benefit M AUD, z=eligible customers k for bubble size), custom tooltip, colour legend by incentive type. Reform outcomes table with DNSP, reform name, year, customers k, peak reduction MW (green-highlighted if ≥ 100 MW), revenue neutral badge (green/red), consumer average saving (green if ≥ $200), AER approved badge (Approved/Pending). Tailwind dark theme throughout.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `NTRTariffStructureRecord`, `NTRTariffImpactRecord`, `NTRDerIncentiveRecord`, `NTRReformOutcomeRecord`, `NTRDashboard` and exported `getNTRDashboard()` (renamed from `getNetworkTariffReformDashboard` to avoid collision with existing method at line 5084 in object literal) calling `GET /api/tariff-reform/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `Sliders` to lucide-react import block. Added `import NetworkTariffReformAnalytics from './pages/NetworkTariffReformAnalytics'`. Nav entry `{ to: '/network-tariff-reform-analytics', label: 'Tariff Reform & DER Incentives', Icon: Sliders }` added before Settings. `Route path="/network-tariff-reform-analytics"` with `element={<NetworkTariffReformAnalytics />}` added before settings route.

**Tests (`tests/test_backend.py`):** Appended `TestNetworkTariffReformAnalytics.test_network_tariff_reform_dashboard` using `(self, client, auth_headers)` fixture pattern. Validates: HTTP 200; all 5 top-level keys present; exactly 20 tariff structure records with all 5 expected DNSPs (Ausgrid, Energex, SA Power Networks, CitiPower, Western Power) fully covered; all 4 tariff types present; RESIDENTIAL/SME/LARGE_COMMERCIAL/INDUSTRIAL classes validated; at least 3 records with solar export rates; positive daily_supply_aud and customers_k; exactly 20 tariff impact records (all 4 tariff types × all 5 customer types) with der_incentive_score 0–10, positive peak_shift_mw_potential, FLAT avg DER score < DYNAMIC_NETWORK avg DER score, and DYNAMIC_NETWORK peak shift exceeds FLAT for every customer type; exactly 15 DER incentive records with valid incentive_type enum values, uptake_rate_pct 0–100, positive values, Ausgrid covers at least 3 distinct incentive types; exactly 8 reform outcome records with implementation_year ≥ 2020, bool revenue_neutral and aer_approved, non-negative consumer_avg_saving_aud, SA Power Networks mandatory demand reform is AER approved with peak reduction > 100 MW, at least 5 of 8 reforms AER approved, Ausgrid EV smart charging consumer saving > $300.

---

## Sprint 66c — AI & Digital Twin in the Electricity Sector Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended enums `ADTUseCaseEnum`, `ADTDeploymentStatus`, `ADTTechnology`, `ADTAssetType`, `ADTAutomationDomain`, `ADTInvestmentCategory`, `ADTMaturityLevel`; Pydantic models `ADTUseCaseRecord`, `ADTDigitalTwinRecord`, `ADTAutomationRecord`, `ADTInvestmentRecord`, `AiDigitalTwinDashboard`; mock data lists `_ADT_USE_CASES` (8 records), `_ADT_DIGITAL_TWINS` (8 records), `_ADT_AUTOMATION` (6 records), `_ADT_INVESTMENTS` (15 records across 5 years × 3 categories); endpoint `GET /api/ai-digital-twin/dashboard` with `@app.get()` and `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/AiDigitalTwinAnalytics.tsx`):** Created new page with dark Tailwind theme. Contains: header with `Cpu` icon; 4 KPI cards (AI use cases in PRODUCTION, total cost savings M AUD/yr, highest automation domain %, total AI investment M AUD); use case deployment table with StatusBadge and TechBadge components; digital twin performance table with 8 columns; automation frontier horizontal bar chart (current vs 2030 target) with ReferenceLine at 80%; investment trend multi-line chart by category over 5 years; automation domain detail table.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript types `ADTDeploymentStatus`, `ADTTechnology`, `ADTAssetType`, `ADTAutomationDomain`, `ADTInvestmentCategory`, `ADTMaturityLevel`; interfaces `ADTUseCaseRecord`, `ADTDigitalTwinRecord`, `ADTAutomationRecord`, `ADTInvestmentRecord`, `AiDigitalTwinDashboard`; exported `getAiDigitalTwinDashboard()` calling `GET /api/ai-digital-twin/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import AiDigitalTwinAnalytics from './pages/AiDigitalTwinAnalytics'`. Nav entry `{ to: '/ai-digital-twin-analytics', label: 'AI & Digital Twin Analytics', Icon: Cpu }` added before Settings. `Route path="/ai-digital-twin-analytics"` with `element={<AiDigitalTwinAnalytics />}` added before settings route. `Cpu` icon was already imported in App.tsx.

**Tests (`tests/test_backend.py`):** Appended `TestAiDigitalTwinAnalytics.test_ai_digital_twin_dashboard` using `(self, client, auth_headers)` fixture pattern. Validates: HTTP 200; all 5 top-level keys present; exactly 8 use case records covering all 8 expected use case types; valid deployment_status and technology enum values; positive accuracy/cost/adoption/organisations values; at least 4 PRODUCTION use cases; TRADING_OPTIMISATION cost saving > $100M; exactly 8 digital twin records with valid asset_type enum, ranges for coverage/accuracy/outage, BATTERY coverage > 90%; exactly 6 automation domain records covering all 6 expected domains; target_2030_pct >= current for all domains; FCAS_OPTIMISATION current automation > 75%; exactly 15 investment records with 5 distinct years; valid category and maturity enums; positive investment/organisation/roi values; total investment in latest year exceeds earliest year.

## Sprint 66b — AEMO ESOO Generation Adequacy Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Appended Pydantic models `EGAAdequacyRecord` (year, region, peak_demand_mw, available_capacity_mw, capacity_gap_mw, use_probability_pct, capacity_shortage_risk LOW/MEDIUM/HIGH/CRITICAL, new_investment_needed_mw), `EGAInvestmentPipelineRecord` (project_name, technology, region, capacity_mw, expected_cod, confidence FIRM/LIKELY/SPECULATIVE, investment_m_aud, capacity_market_eligible bool), `EGARetirementRecord` (unit_name, technology, region, capacity_mw, expected_retirement_year, retirement_trigger ECONOMICS/AGE/POLICY/OWNER_DECISION, replacement_committed bool, reliability_impact), `EGAScenarioRecord` (scenario STEP_CHANGE/CENTRAL/SLOW_CHANGE/HIGH_DER, year, region, total_capacity_gw, vre_capacity_gw, dispatchable_capacity_gw, storage_gw, adequacy_status ADEQUATE/MARGINAL/INADEQUATE), and `EsooAdequacyDashboard`. All models use `EGA` prefix to avoid collision with pre-existing PASA adequacy models. Mock data: 20 adequacy records (5 regions NSW/VIC/QLD/SA/TAS × 4 years 2025-2028) with VIC and SA reaching CRITICAL risk by 2028, TAS remaining LOW throughout; 15 investment pipeline records covering Waratah Super Battery (FIRM/NSW), Eraring Life Extension (FIRM/NSW), Snowy 2.0 (LIKELY/NSW), Callide C Replacement (LIKELY/QLD), Pioneer-Burdekin Pumped Hydro (SPECULATIVE/QLD), Barker Inlet OCGT (FIRM/SA) and others; 10 retirement records covering Loy Yang A (VIC/ECONOMICS), Northern Power Station (SA/POLICY), Vales Point B (NSW/AGE) and others, majority coal; 20 scenario records (5 regions × 4 scenarios STEP_CHANGE/CENTRAL/SLOW_CHANGE/HIGH_DER for 2030) with STEP_CHANGE yielding ADEQUATE for most regions and SA SLOW_CHANGE INADEQUATE. Endpoint `GET /api/esoo-adequacy/dashboard` secured with `Depends(verify_api_key)`.

**Frontend (`app/frontend/src/pages/EsooAdequacyAnalytics.tsx`):** New page created with Recharts and Tailwind dark theme. Header with `Activity` icon. Four KPI cards: regions at HIGH/CRITICAL risk by 2028 (count), total new investment needed MW (sum across all region/year records), retirement pipeline MW (sum of all retirement unit capacities), STEP_CHANGE scenario adequacy rate (% of regions ADEQUATE by 2030). Per-region adequacy gap ComposedChart showing Available Capacity, Peak Demand and Capacity Gap bars across 4 years for each of the 5 NEM regions, Y-axis in GW, gap bars in red. Investment pipeline table: project name, technology badge (blue), region (color-coded), capacity MW, COD year, confidence badge (FIRM green/LIKELY blue/SPECULATIVE gray), investment M AUD, capacity market eligibility pill. Retirement risk table sorted by retirement year: unit name, technology, region (color-coded), capacity MW, year, trigger badge (ECONOMICS orange/AGE yellow/POLICY blue/OWNER_DECISION purple), replacement committed pill (green/red), reliability impact badge (CRITICAL red/HIGH orange/MODERATE yellow/LOW gray). Scenario comparison section: two grouped BarCharts for dispatchable capacity GW and storage GW across 4 scenarios for NSW and SA; adequacy status grid table showing ADEQUATE/MARGINAL/INADEQUATE badges for all 5 regions × 4 scenarios.

**API client (`app/frontend/src/api/client.ts`):** Appended TypeScript interfaces `EGAAdequacyRecord`, `EGAInvestmentPipelineRecord`, `EGARetirementRecord`, `EGAScenarioRecord`, `EsooAdequacyDashboard` and exported `getEsooAdequacyDashboard()` calling `GET /api/esoo-adequacy/dashboard`.

**Routing (`app/frontend/src/App.tsx`):** Added `import EsooAdequacyAnalytics from './pages/EsooAdequacyAnalytics'`. Nav entry `{ to: '/esoo-adequacy-analytics', label: 'ESOO Generation Adequacy', Icon: Activity }` added before Settings (Activity already imported). `Route path="/esoo-adequacy-analytics"` with `element={<EsooAdequacyAnalytics />}` added before settings route.

**Tests (`tests/test_backend.py`):** Appended `TestEsooAdequacyAnalytics.test_esoo_adequacy_dashboard` using `(self, client, auth_headers)` fixture pattern. Validates: HTTP 200; all 5 top-level keys present; exactly 20 adequacy records with valid regions/years/risk levels, each region appears 4 times, peak_demand_mw/available_capacity_mw positive, use_probability_pct and new_investment_needed_mw non-negative, VIC and SA 2028 HIGH/CRITICAL, TAS all years LOW, at least 2 HIGH/CRITICAL regions by 2028; exactly 15 investment pipeline records with valid regions/confidence enums, positive capacity/investment, COD >= 2025, capacity_market_eligible bool, at least one FIRM NSW project, Snowy 2.0 is NSW >= 2000 MW, Waratah is Battery Storage; exactly 10 retirement records with valid regions/triggers, capacity > 0, retirement_year >= 2025, replacement_committed bool, at least one VIC HIGH/CRITICAL impact, at least 6 coal retirements, Loy Yang A Unit 1 is VIC ECONOMICS trigger; exactly 20 scenario records (each region 4 times), valid scenario/adequacy_status enums, vre_capacity_gw <= total_capacity_gw, STEP_CHANGE adequate count >= SLOW_CHANGE adequate count, NSW STEP_CHANGE ADEQUATE, SA SLOW_CHANGE INADEQUATE, NSW dispatchable within total capacity bounds.

---

## Sprint 66a — Social Licence & Energy Transition Equity Analytics (2026-02-20)

**Backend (`app/backend/main.py`):** Checked for collisions — `JustTransitionProgramRecord` already existed so the SLE prefix was applied throughout. Appended enums `SLEOppositionReason`, `SLEEngagementQuality`, `SLEConsultationAdequacy`, `SLEEquityCohort`, `SLEEquityTrend`; Pydantic models `SLEProjectRecord`, `SLEFirstNationsRecord`, `SLEJustTransitionRecord`, `SLEEquityRecord`, `SocialLicenceDashboard`; mock data lists `_SLE_PROJECTS` (10 records across SA, NSW, NT, TAS, QLD, VIC, WA), `_SLE_FIRST_NATIONS` (5 regional records: NT, WA, QLD, SA, TAS), `_SLE_JUST_TRANSITION` (5 coal region records: Hunter Valley, Latrobe Valley, Callide, Collie, Leigh Creek), `_SLE_EQUITY` (6 cohort records: LOW_INCOME, RENTERS, REMOTE_COMMUNITIES, FIRST_NATIONS, ELDERLY, DISABILITY); endpoint `GET /api/social-licence/dashboard` with `@app.get()` and `dependencies=[Depends(verify_api_key)]`.

**Frontend (`app/frontend/src/pages/SocialLicenceAnalytics.tsx`):** Created new page with dark Tailwind theme. Contains: header with `Users` icon; 4 KPI cards (avg community support %, projects with POOR engagement, total just transition funding M AUD, cohorts with WORSENING equity); project social licence table with colour-coded support %, OppositionBadge and EngagementBadge components; First Nations engagement regional summary table with ConsultBadge; just transition progress stacked BarChart (retrained vs not retrained workers) with dual y-axis for funding M AUD; equity cohort RadarChart with 6 cohorts across equity score, program coverage, solar access, and low hardship; equity cohort detail table with TrendBadge.

**API client (`app/frontend/src/api/client.ts`):** Added `getSocialLicenceDashboard` method to the `api` object calling `GET /api/social-licence/dashboard`. Appended TypeScript interfaces `SLEProjectRecord`, `SLEFirstNationsRecord`, `SLEJustTransitionRecord`, `SLEEquityRecord`, `SocialLicenceDashboard` at end of file.

**Routing (`app/frontend/src/App.tsx`):** Added `import SocialLicenceAnalytics from './pages/SocialLicenceAnalytics'`. Nav entry `{ to: '/social-licence-analytics', label: 'Social Licence & Equity Analytics', Icon: Users }` added before Settings. `Route path="/social-licence-analytics"` with `element={<SocialLicenceAnalytics />}` added before settings route. `Users` icon was already imported in App.tsx.

**Tests (`tests/test_backend.py`):** Appended `TestSocialLicenceAnalytics.test_social_licence_dashboard` using `(self, client, auth_headers)` fixture pattern. Validates: HTTP 200; all 5 top-level keys present; exactly 10 project records with valid opposition_reason and engagement_quality enums, support+opposition+neutral summing to ~100%, at least 1 aboriginal land project, avg support >= 50%; exactly 5 First Nations records with valid consultation_adequacy enum, boolean land_rights_respected, positive project counts and benefit sharing; exactly 5 just transition records covering Hunter Valley/Latrobe Valley/Callide/Collie/Leigh Creek with valid ranges, Hunter Valley as largest affected workforce; exactly 6 equity cohort records covering all 6 expected cohorts with equity_score 0–10, valid trend enum, First Nations having lowest equity score, at least 1 WORSENING cohort.

---

## Sprint 163 — Battery Chemistry Risk (BCR) and AEMO 5-Min Settlement Analytics (2026-02-25)

Backend, frontend page, API client types, routing, and tests for BatteryChemistryRiskAnalytics and Aemo5MinSettlementAnalytics dashboards.

---

## Sprint 164 — Electricity Retail Competition (ERCA), Grid Frequency Response (GFRA), Demand Response Aggregation (DRAA) Analytics (2026-02-25)

**ERCA:** Retail competition analytics — 8 retailers x 5 regions, switching rates, price dispersion (standing/market/VPP/green), hardship programs. Endpoint: `/api/retail-competition/dashboard`. 18 backend tests.

**GFRA:** Grid frequency response — 12 frequency events, 10 FCAS providers (Hornsdale fastest response), 18 inertia records (SA below threshold), 12 RoCoF trend records. Endpoint: `/api/grid-frequency/dashboard`. 15 backend tests.

**DRAA:** Demand response aggregation — 8 aggregators (Enel X largest at ~200MW), 15 DR events, 60 seasonal records, 24 revenue records. Endpoint: `/api/demand-response-aggregation/dashboard`. 11 backend tests.

---

## Sprint 165 — Hydrogen Economy Outlook (HEOA), Network Augmentation Deferral (NADA), EV Fleet Grid Integration (EFGA) Analytics (2026-02-25)

**HEOA:** 12 major Australian H2 projects, LCOH declining from $6.50 to $3.20/kg, 60 demand records, 5 export MOUs. Endpoint: `/api/hydrogen-economy-outlook/dashboard`. 14 tests.

**NADA:** 15 deferral projects across 10 DNSPs, 6 annual investment trends (deferral rate 4.6%→42%), 10 RIT-D assessments, 20 DER hosting zones. Endpoint: `/api/network-augmentation/dashboard`. 13 tests.

**EFGA:** EV adoption 2024-2035, 24-hour charging profiles, 8 state impacts, 8 fleet operators, V2G only during peak hours. Endpoint: `/api/ev-fleet-grid/dashboard`. 10 tests.

---

## Sprint 166 — Grid Emissions Intensity (GEIA), Capacity Investment Scheme (CISA), Microgrid Resilience (MGRA) Analytics (2026-02-25)

**GEIA:** 90 regional emissions records (VIC highest, TAS lowest), 15 top generators, 15 time-of-day records, 90 correlation records. Declining trend. Endpoint: `/api/grid-emissions/dashboard`. 14 tests.

**CISA:** 20 CIS contracts, 4 tender rounds, 7 projection years, 18 monthly revenue records. NSW/VIC most capacity. Endpoint: `/api/cisa/dashboard`. 16 tests.

**MGRA:** 12 Australian microgrids (Coober Pedy, King Island, etc.), 20 islanding events, 12 resilience scores, 18 monthly records. Endpoint: `/api/microgrid-resilience/dashboard`. 13 tests.

---

## Sprint 167 — Power Quality Monitoring (PQMA), REZ Development (REZA), Energy Poverty & Affordability (EPAA) Analytics (2026-02-25)

**PQMA:** 15 substations, 120 voltage profiles, 18 monthly incidents, 25 recent events. Endpoint: `/api/power-quality/dashboard`. 13 tests.

**REZA:** 10 REZs (Central-West Orana, New England, etc.), 25 projects, 90 timeline records, 10 tech mix records. Endpoint: `/api/rez-development/dashboard`. 18 tests.

**EPAA:** 8 states, 5 income quintiles (Q1 ~8% burden, Q5 ~2%), 48 disconnection records, 8 retailers, 20 LGA records. SA/TAS highest burden. Endpoint: `/api/energy-poverty-affordability/dashboard`. 15 tests.

---

## Sprint 168 — Coal Fleet Retirement (CFRA), VPP Operations (VPOA), Pumped Hydro Storage (PHSA) Analytics (2026-02-25)

**CFRA:** 20 coal units (Eraring, Bayswater, Loy Yang A/B, Yallourn, Mt Piper, Vales Point), 15 replacement projects, 17 capacity trajectory years (2024-2040), 50 reliability records. Endpoint: `/api/coal-retirement/dashboard`. 16 tests.

**VPOA:** 8 VPP operators (Tesla 250MW largest), 18 monthly dispatch records, 6 asset types, 20 recent events. Endpoint: `/api/vpp-operations/dashboard`. 15 tests.

**PHSA:** 10 pumped hydro projects (Snowy 2.0, Pioneer-Burdekin, Borumba, etc.), 10 operation records, 24 hourly profiles, 10 revenue records. Endpoint: `/api/pumped-hydro/dashboard`. 19 tests.

---

## Sprint 169 — Integrated System Plan (ISPA), Smart Meter Data (SMDA), East Coast Gas Market (ECGA) Analytics (2026-02-25)

**ISPA:** 10 ISP transmission projects (HumeLink, VNI West, Marinus Link, etc.), 42 capacity records, 6 savings records, 7 transfer corridors, 4 scenarios. Endpoint: `/api/isp-analytics/dashboard`. 18 tests.

**SMDA:** 48 half-hour load profiles, 10 DNSP rollout records, 18 monthly consumption records, 6 customer segments, 6 tariff migration years. Endpoint: `/api/smart-meter/dashboard`. 18 tests.

**ECGA:** 72 hub price records (4 hubs x 18 months), 24 supply records, 8 demand records, 18 LNG records, 6 basin balance records. Endpoint: `/api/east-coast-gas/dashboard`. 14 tests.

---

## Sprint 170 — Energy Cyber Security (ESCA), Community Battery (CBA), Wholesale Market Reform (WMRA) Analytics (2026-02-25)

**ESCA:** 20 incidents, 18 detection trend months, 6 maturity domains (OT/SCADA lowest), 4 vulnerability types, 8 asset classes. Endpoint: `/api/energy-cyber-security/dashboard`. 21 tests.

**CBA:** 15 community batteries (Ausgrid Beacon Hill, SA Power Networks Payneham, etc.), 24 hourly operations, 12 monthly benefits, 15 revenue records. Endpoint: `/api/community-battery/dashboard`. 19 tests.

**WMRA:** 15 reforms, 36 efficiency months, 15 consumer impact records, 7 price cap years, 10 metrics. Endpoint: `/api/wholesale-market-reform/dashboard`.

---

## Sprint 171 — Route Ordering Fix, Databricks Branding & Copilot Context Fix (2026-02-26)

### Route Ordering Fix
- **Problem:** 32 stub dashboard endpoints (added in Sprint 163-170) were placed AFTER the API catch-all route `@app.api_route("/api/{path:path}")` in `app/main.py`, causing all of them to return 404.
- **Fix:** Moved the 315-line block of 32 endpoint definitions to BEFORE the catch-all route using a Python line-manipulation script.
- **Verification:** All 31 new endpoints confirmed returning HTTP 200 via Chrome DevTools fetch() calls.

### Graceful 501 Fallback
- **Problem:** Frontend has ~586 unique API endpoints but only ~86 are implemented. Pages with unimplemented endpoints crashed to blank screens (generic 200 mock data had wrong TypeScript shape, causing `TypeError: t.filter is not a function`).
- **Fix:** Changed API catch-all from 404 to HTTP 501 with structured JSON error: `{"error": "Endpoint /api/{path} is not yet implemented", "message": "..."}`. Frontend `get<T>()` throws on non-2xx, so each page's catch block shows a red error message instead of crashing.

### Databricks Branding
- **`app/frontend/index.html`:** Updated favicon to Databricks diamond logo as data URI SVG; added DM Sans font via Google Fonts; title changed to "AUS Energy Copilot | Databricks".
- **`app/frontend/src/App.tsx`:** Sidebar brand replaced `<Zap>` icon with inline Databricks SVG; sidebar background `#1B3139`; accent colors changed from amber to Databricks red `#FF3621`; nav link active states `bg-white/10`; footer "Powered by Databricks"; top bar dark mode `#161B22`; main container `#F5F7FA` / `#0D1117`.
- **`app/frontend/src/index.css`:** Font-family changed to `"DM Sans"`; added Databricks CSS custom properties (`--db-red`, `--db-sidebar`, `--db-dark`, etc.); dark mode variables.
- **`app/frontend/dist/databricks-icon.svg`:** Official Databricks diamond logo SVG added.
- **Note:** SVG files in `public/` are NOT served correctly by the SPA catch-all route (served as `text/html`); inline SVG and data URI approach used instead.

### Copilot Data Context Fix
- **Problem:** AI copilot always responded "the live market data feed is temporarily unavailable" despite data endpoints working.
- **Root Cause 1:** `NameError: name 'lg' is not defined` in `_build_market_context()`. Logger was defined as `logger` (line 70) but exception handlers used `lg`. This caused the entire function to throw, caught by the chat handler which set `market_context = "(Market data temporarily unavailable)"`.
- **Root Cause 2:** `TypeError: string indices must be integers` in volatility section. `prices_volatility()` returns `{"timestamp": ..., "regions": [...]}` but context builder iterated over the dict directly (`for v in vol`) yielding string keys. Also used wrong field names (`avgPrice24h` vs `mean_price`).
- **Fix:** Replaced all `lg.` with `logger.`; extracted `.regions` from volatility response; used correct field names (`mean_price`, `std_dev`, `min_price`, `max_price`, `spike_count`).
- **Verification:** `/api/debug/context` endpoint returns 4,674 chars of rich market context; copilot now gives data-driven answers with specific prices ($77.28/MWh for NSW).

### Debug Endpoint
- Added `GET /api/debug/context` — returns `_build_market_context()` output or full traceback on error, for troubleshooting copilot system prompt.

### Deployment
- Built frontend: `npx vite build` (skips tsc type-checking)
- Uploaded to workspace and deployed as Databricks App
- App URL: https://energy-copilot-7474645691011751.aws.databricksapps.com
- Git commits: `6437150` (route fix + 501 fallback), `2a12897` (branding + copilot fix)

## Sprint 172 — Spot Price Dashboard Endpoints (Batch 1) [COMPLETE] (2026-02-27)

### 10 Spot Prices Endpoints Added
- **Problem:** All Spot Prices category dashboards were broken — frontend pages hit the 501 catch-all because no backend endpoints existed for them.
- **Fix:** Created `app/_spot_endpoints.py` with 10 mock data endpoints matching exact TypeScript interfaces from `client.ts`:
  1. `/api/spot-price-volatility-regime/dashboard` — volatility regimes, regime transitions, regional stats
  2. `/api/spot-forecast/dashboard` — multi-horizon forecasts, model accuracy, drift monitoring
  3. `/api/spot-price-spike-prediction/dashboard` — spike predictions, confusion matrix, feature importance
  4. `/api/price-spike-analysis/dashboard` — spike events, regional stats, generator correlations
  5. `/api/spot-price-prediction/dashboard` — ensemble predictions with confidence intervals
  6. `/api/spot-depth/dashboard` — order books, price discovery, trading activity
  7. `/api/negative-price-events/dashboard` — negative price events, curtailment correlation
  8. `/api/electricity-spot-price-seasonality/dashboard` — hourly/day-type patterns, decomposition, forecasts
  9. `/api/spot-price-event-analysis/dashboard` — event timeline, root causes, market response
  10. `/api/price-model-comparison/dashboard` — model accuracy, backtests, feature importance

### Response Size Optimization
- **Problem:** Initial endpoint responses were 400-520KB each, causing container timeouts.
- **Fix:** Systematically reduced loop iterations across all dimensions:
  - Fewer regions (5→3), fewer years (2→1), fewer hours (24→every 4th hour), fewer day types (8→3)
  - All 10 endpoints verified under 50KB each (total ~314KB, down from 920KB+)
- Inserted all 10 endpoints (1,481 lines) into `main.py` before the catch-all route.

### Deployment
- Built frontend, uploaded to workspace, deployed successfully.
- Git commit: `b3599a6`

## Sprint 173 — PRD Phase 4: Network Operations & Distribution Intelligence [COMPLETE] (2026-02-27)

### PRD Update (`docs/PRD.md`)
- Added Phase 4: Network Operations & Distribution Intelligence (320 insertions, 20 deletions)
- New target personas: P5 (Distribution Network Operator), P6 (Asset Manager), P7 (DER Operations Manager), P8 (Network Planning Engineer)
- 6 product scope sections: Asset Monitoring, Outage Management, DER Management, Network Demand Forecasting, EV Integration, Copilot Extensions
- 5 new frontend tabs, 10 new copilot tools, 20+ gold tables, 6 Lakebase tables, 3 new Genie spaces
- 8 new data sources (SCADA/ADMS, GIS, OMS, AMI, DER gateway, weather stations, AER, traffic/planning)
- 10-sprint timeline (Sprints 22-31)
- Updated roadmap summary: Phase 4 brings cumulative totals to 43 copilot tools, 11 Genie spaces, 62 gold tables
- Added 17 glossary terms (DNSP, SAIDI, SAIFI, CAIDI, MAIFI, DER, Hosting Capacity, DOE, VPP, GSL, RIT-D, SCADA, ADMS, THD, N-1 Contingency)

### Git
- Git commit: `f070178`

## Sprint 174 — Spot Price Dashboard Endpoints (Batch 2) + Deployment Fix [COMPLETE] (2026-02-28)

### 6 Remaining Spot Prices Endpoints
- **Problem:** 6 Spot Prices dashboards still hitting 501 catch-all after Sprint 172.
- **Fix:** Created `app/_spot_endpoints_2.py` with 6 endpoints matching TypeScript interfaces:
  1. `/api/electricity-price-index/dashboard` (20KB) — CPI records, DMO records, tariff components, retailer comparison
  2. `/api/market-price-formation-review/dashboard` (16KB) — price caps, scarcity events, VCR values, reform pipeline
  3. `/api/electricity-market-price-formation/dashboard` (15KB) — price drivers, marginal units, price events, bidding behaviour
  4. `/api/electricity-price-cap-intervention/dashboard` (12KB) — cap events, market impact, generator response, remedy actions
  5. `/api/nem-price-review/dashboard` (24KB) — spot prices, retail prices, affordability metrics
  6. `/api/demand-curve-price-anchor/dashboard` (25KB) — demand curves, price anchors, elasticity, forecasts
- All endpoints under 25KB each (112KB total). Inserted 514 lines into `main.py` (now 6,673 lines total).

### Deployment Fix — node_modules Cleanup
- **Problem:** Deployments stuck on "Preparing source code for new app deployment" for 30+ minutes. Two deployments (`01f113f24b79`, `01f113f7b68b`) stuck/cancelled.
- **Root Cause:** `databricks workspace import-dir` had previously uploaded `frontend/node_modules/` (157MB, 35,000+ files) to the workspace. Unlike `git`, `import-dir --overwrite` does not delete files absent from the source — they persist. The deployment source snapshot was trying to package 157MB.
- **Fix:**
  1. Created clean rsync directory at `/tmp/energy-copilot-deploy/` excluding node_modules (24MB vs 181MB)
  2. Uploaded clean directory to workspace
  3. Deleted `node_modules` from workspace: `databricks workspace delete .../frontend/node_modules --recursive`
  4. Stopped app to cancel stuck deployment, restarted, redeployed
  5. Deployment succeeded in ~2 minutes (vs 30+ min stuck)
- **Prevention:** Created `app/.databricksignore` with `frontend/node_modules/` entry (helps with `databricks bundle` but not `import-dir`)

### Current Stats
- **Frontend pages:** 475
- **API endpoints:** 115 (GET + POST)
- **main.py:** 6,673 lines
- **App URL:** https://energy-copilot-7474645691011751.aws.databricksapps.com
- **All 19 Spot Prices dashboards now have working backend endpoints**

### Git
- Git commit: `3d43350`

## Sprint 175 — Fix 14 Broken Dashboards + Missing Imports [COMPLETE] (2026-03-01)

### Root Cause: Missing Module-Level Imports
- All endpoints from `_spot_endpoints.py` used `_r` (alias for `random`) and `_dt`/`_td` (aliases for `datetime`/`timedelta`), but these were only imported inside individual functions, not at module level.
- Fix: Added `import random as _r` and `from datetime import datetime as _dt, timedelta as _td` at the top of main.py.

### Missing `getHeaders()` in client.ts
- Newer API client functions (Sprint 163+) used `fetch(url, { headers: getHeaders() })` but `getHeaders()` was never defined.
- Fix: Added `function getHeaders(): Record<string, string> { return { Accept: 'application/json' } }` to client.ts.

### Missing `/api/market-stress/dashboard` Endpoint
- `MarketStressTesting.tsx` called `/api/market-stress/dashboard` but only `/api/spot-market-stress/dashboard` existed (different component).
- Fix: Created new endpoint with scenarios, stress test results, vulnerabilities, and KPIs.

### 9 Routes Missing Leading `/`
- Route paths in App.tsx route map and `<Route>` elements were missing the leading `/`, causing pages to show "Select a dashboard" placeholder.
- Fixed: electricity-spot-price-events, hydrogen-refuelling-transport, large-scale-renewable-auction, nem-ancillary-services, australian-carbon-credit, ev-grid-integration, generator-capacity-adequacy, smart-grid-cybersecurity, pumped-hydro-reservoir.

### Missing `/api/demand-forecast/dashboard` Endpoint
- `DemandForecastAnalytics.tsx` called this endpoint but it didn't exist (501 catch-all).
- Fix: Created endpoint with 75 forecast records (5 regions × 3 horizons × 5 dates) and 30 PASA reliability records.

### Audited All 76 Endpoints Across 6 Subcategories
- Futures & Forward (6): all passing
- Trading (12): all passing (including trading desk)
- Hedging & Risk (10): all passing
- Forecasting (13): all passing after demand-forecast fix
- Settlement (10): all passing
- Market Structure (9): all passing

### Git
- Commits: `e46afbd` (14 dashboard fixes + route fixes), `073cb3a` (demand-forecast endpoint)

---

## Sprint 176 — Genie AI/BI Integration & UI Overhaul (2026-03-01)

### Genie Spaces Created
- Created 4 Databricks AI/BI Genie spaces via REST API (`POST /api/2.0/genie/spaces`)
- Each space configured with Unity Catalog gold tables, text instructions, example SQL queries, and sample questions
- API quirks discovered: `serialized_space` must be JSON string, tables sorted by identifier, IDs must be valid sorted 32-hex UUIDs, max 1 text_instruction

### Genie Space IDs (v2 — recreated to update sample questions)
| Space | ID |
|-------|----|
| NEM Spot Market Intelligence | `01f1151e4cea13f3abdca55d4894e777` |
| NEM Generation & Renewables | `01f1151e5c7316948adc16d6623ad6c1` |
| NEM Network & Interconnectors | `01f1151e5e4012fa91e403fc47fcb0a4` |
| NEM Forecasting & Weather | `01f1151e60481c12abe205ed51ccc8f3` |

### Genie Backend Proxy (5 endpoints in main.py)
- `GET /api/genie/spaces` — lists configured spaces with metadata
- `POST /api/genie/spaces/{id}/start-conversation` — starts new conversation
- `POST /api/genie/spaces/{id}/conversations/{cid}/messages` — sends follow-up
- `GET /api/genie/spaces/{id}/conversations/{cid}/messages/{mid}` — polls status
- `GET /api/genie/spaces/{id}/conversations/{cid}/messages/{mid}/query-result` — fetches SQL results
- Auth via app service principal (`WorkspaceClient().config.authenticate()`)
- Host fix: `w.config.host` in Databricks Apps returns the app URL (`.databricksapps.com`), not the workspace URL — code detects this and uses correct workspace host

### Genie Permissions Granted to App Service Principal
- `CAN_MANAGE` on all 4 Genie spaces (via `/api/2.0/permissions/genie/{id}`)
- `CAN_USE` on SQL warehouse `4d44ca1635a22803`
- `USE CATALOG`, `USE SCHEMA`, `SELECT` on `energy_copilot_catalog.gold`
- SP identified by applicationId `67aaaa6b-778c-4c8b-b2f0-9f9b9728b3bb`

### Genie.tsx — Copilot-Style UI Overhaul
- Redesigned from purple theme to blue theme matching Copilot page
- AI/You avatar bubbles (gradient blue "AI" circle, gray "You" circle)
- Collapsible right sidebar with categorized example questions and session stats
- Textarea input with auto-resize and Cmd+Enter to send (was single-line input)
- Header with query count badge and Clear Chat button
- Cmd+K shortcut to focus input
- Each space has 4 question categories with ~6 questions each (~24 per space)

### Copilot vs Genie — Architecture Comparison
| | Copilot (`/copilot`) | Genie (`/genie`) |
|---|---|---|
| Engine | Claude Sonnet 4.6 via Databricks FMAPI | Databricks AI/BI Genie |
| Data access | Pre-fetched from 10 internal API endpoints, injected into system prompt | Direct SQL against Unity Catalog gold tables |
| Output | Streaming markdown text | SQL query + tabular results |
| Strength | Reasoning, explanations, market knowledge | Precise data queries, real numbers |
| Scope | Anything NEM-related | Scoped to configured gold tables |

Copilot's `_build_market_context()` fetches live data from: spot prices, market summary, price spikes, interconnector flows, generation mix (all 5 regions), price volatility, BESS fleet, alerts, demand response, and price forecasts — all injected into Claude's system prompt before each response.

### 36 Missing API Endpoints Added
- Added 36 endpoints across 6 categories that were returning 501 (catch-all):
  - Forecasting (9), Futures (5), Hedging (10), Settlement (9), More (3)
  - Each uses `_r.seed(<unique_seed>)` for reproducible mock data
  - All 36 tested locally (36/36 passing)

### Git
- Commits: `8d3b5cb` (36 missing endpoints), `d6e6f12` (Genie page), `63fa3f1` (Genie API fix)

---

## Sprint 177 — Backend Router Refactor (2026-03-02)

### Refactor: main.py → 12 APIRouter modules
Split the 8,060-line monolith `app/main.py` into a thin 183-line entry point + 14 files under `app/routers/`. Purely mechanical — no logic changes, all 173 API paths preserved.

**New structure:**
```
app/
  main.py                        (183 lines — init, middleware, include_router, catch-alls)
  routers/
    __init__.py                  (empty)
    shared.py                    (169 lines — logging, cache, SQL helpers, NEM constants)
    health.py                    (107 lines — 4 routes)
    dashboards.py                (1,408 lines — 12 routes, Pydantic models + _build_* helpers)
    home.py                      (712 lines — 11 routes, real SQL + mock fallback)
    sidebar.py                   (1,120 lines — 26 routes, sidebar nav + realtime-ops)
    copilot.py                   (423 lines — 5 routes, LLM chat/debug/sessions)
    stubs.py                     (375 lines — 31 routes, missing dashboard placeholders)
    market_events.py             (280 lines — 11 routes, NEM suspension/price setter/spot cap)
    spike_analysis.py            (2,162 lines — 18 routes, spike/price/volatility analysis)
    genie.py                     (308 lines — 5 routes, Genie AI/BI proxy)
    batch_forecasting.py         (122 lines — 9 routes, forecasting + volatility-regime)
    batch_futures_hedging.py     (329 lines — 27 routes, futures/hedging/settlement/misc)
    batch_bidding.py             (488 lines — 9 routes, bidding analytics)
```

**Cross-module imports (no circular deps):**
- DAG: `shared ← home/sidebar ← copilot`
- `copilot.py` imports 8 functions from `home.py` + 4 from `sidebar.py` for `_build_market_context()`

**Deployed:** `8e236fa` — app running, all 173 routes verified

## Step 1b: Wire Mock Endpoints to Real NEMWEB Data — 2026-03-05

### Pass 1: Initial wiring (~25 endpoints in home.py)
- Commit `fc359f8` — wired ~25 endpoints in `home.py` to gold views (`nem_prices_5min`, `nem_generation_by_fuel`, `nem_interconnectors`, `nem_region_summary`, `nem_facilities`)
- Pattern: `_query_gold(SQL)` → transform → return; fall back to existing mock if SQL fails

### Pass 2: NEM Real-Time + 13 more endpoints
- Commit `069dec8` — NEM Real-Time Operational Overview dashboard
- Commit `c406b7e` — 13 additional endpoints across dashboards.py, sidebar.py, spike_analysis.py, market_events.py

### Pass 3: 18 Tier 1+2 endpoints + column name fixes
- Commit `d0258bc` — wired 18 more endpoints across 7 router files:
  - `dashboards.py` — curtailment, carbon dashboard
  - `sidebar.py` — BESS fleet, BESS dispatch, trading spreads
  - `stubs.py` — carbon intensity, load curve, rooftop solar, participant market share
  - `batch_futures_hedging.py` — VoLL analytics, NEM 5-min settlement, congestion revenue
  - `spike_analysis.py` — spot market stress, demand forecast
  - `batch_forecasting.py` — NEM demand forecast
  - `batch_bidding.py` — market concentration
- **Gold view column name fixes** (discovered during testing):
  - `network_region` → `region_id` in `nem_generation_by_fuel`
  - `total_generation_MW` → `total_mw` in `nem_generation_by_fuel`
  - `export_limit`/`import_limit` → `export_limit_mw`/`import_limit_mw` in `nem_interconnectors`
  - `available_generation` → `available_gen_mw` (from `nem_prices_5min`, not `nem_region_summary`)

### Verification results (post column-fix deploy)
**10 endpoints returning REAL data:**
- Load Curve (120 hourly profiles), VoLL Analytics (3 regions), Spot Market Stress (30 tail risk metrics)
- Carbon Intensity (NSW 673.5 kgCO2/MWh), Carbon Dashboard (22.7% renewable)
- Rooftop Solar (96 generation records), NEM 5-Min Settlement (100 intervals)
- NEM Demand Forecast (20 regional forecasts), Trading Spreads (5 pairs), BESS Dispatch (48 records)

**6 endpoints on mock fallback:**
- BESS Fleet, Participant Market Share, Market Concentration (empty `silver_nem_facility_dimension`)
- Curtailment, Congestion Revenue, Demand Forecast (partial data / derived mock enrichment)

### Gold view actual column schemas (reference)
```
nem_prices_5min:        region_id, rrp, total_demand_mw, available_gen_mw, net_interchange_mw, interval_datetime
nem_generation_by_fuel: region_id, fuel_type, is_renewable, total_mw, unit_count, capacity_factor, emissions_tco2e, emissions_intensity, interval_datetime
nem_interconnectors:    interconnector_id, from_region, to_region, mw_flow, export_limit_mw, import_limit_mw, utilization_pct, is_congested, interval_datetime
nem_region_summary:     region_id, interval_datetime, total_demand, rrp
nem_facilities:         duid, station_name, region_id, fuel_type, is_renewable, capacity_mw (503 rows, fuel_type/region_id mostly NULL)
```

### Skip list (no suitable NEMWEB data)
- `batch_futures_hedging.py` remaining endpoints — require futures/contract data
- `batch_bidding.py` remaining endpoints — require bid data (not yet ingested)
- `batch_forecasting.py` remaining endpoints — require ML forecast tables
- `stubs.py` remaining endpoints — Phase 2-4 features

### Pass 4: Fix facility dimension view + query migration
- Commit `cf20618` — root cause: `gold.nem_facilities` was built from `silver_nem_dispatch_unit_scada` (null fuel_type, hardcoded NULL region_id)
- Fix: recreated view to source from `bronze_facilities_fueltech_dim` (650 rows, 5 regions, 16 fuel types)
- Migrated 4 queries from `silver_nem_facility_dimension` → `gold.nem_facilities` with correct columns
- Result: **all 16 Step 1b endpoints now returning real data** (0 mock fallbacks)

**Deployed:** `cf20618` — ~56 endpoints wired to real NEMWEB data total, facility dimension fixed

## E1 — Forward Curve Construction — 2026-03-07

### Overview
Phase 2 Enhancement E1: Forward curve construction engine that bootstraps NEM forward electricity curves from ASX futures data (`gold.asx_futures_eod` — 1800 rows), applies peak/off-peak shaping and seasonal adjustments. Exposes curves via 5 new API endpoints + frontend page + Copilot tool.

### Backend — `app/routers/curves.py` (NEW, ~310 lines)
- **Engine**: `_build_forward_curve(region, profile, curve_date, num_quarters)` — pure Python, no external deps
  - Step 1: Fetch ASX futures for region, get latest settlement prices by quarter/year for Base contracts
  - Step 2: Quarterly → monthly decomposition with NEM seasonal shape factors (summer premium Dec-Feb, winter shoulder Jun-Aug)
  - Step 3: Peak/Off-Peak shaping — PEAK applies region-specific ratio (1.10-1.25x), OFF_PEAK derived as inverse
  - Step 4: Extrapolation for quarters beyond available futures data (trend decay)
- **Seasonal factors**: `{1:1.12, 2:1.08, 3:0.95, 4:0.88, 5:0.92, 6:1.02, 7:1.05, 8:1.00, 9:0.93, 10:0.90, 11:0.95, 12:1.10}`
- **Peak ratios**: `{NSW1:1.22, QLD1:1.18, VIC1:1.20, SA1:1.25, TAS1:1.10}`
- **Key fix**: ASX data has `quarter="Q1-2026"` format — initial code generated `Q1_2026_2026` (duplicated year). Fixed to parse and normalize correctly.

### 5 New Endpoints
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/curves/term-structure` | Monthly prices for 8 quarters (region, profile, as_of params) |
| GET | `/api/curves/compare` | Multi-region overlay (comma-sep regions param) |
| GET | `/api/curves/history` | Historical curve snapshots from different trade dates |
| POST | `/api/curves/snapshot` | Persist curves for all 5 regions × 3 profiles to `gold.forward_curves` |
| GET | `/api/curves/snapshots` | Read saved curve snapshots |

### Delta Table — `gold.forward_curves`
- 10 columns: curve_id, curve_date, region, profile, quarter, month, price_mwh, source, model_version, created_at
- CDF enabled, MODIFY granted to app SP (`67aaaa6b-...`)
- Created via SQL Statements API (column defaults not supported — removed DEFAULT clauses)

### Frontend — `ForwardCurves.tsx` (NEW, ~280 lines)
- Route: `/forward-curves` (classified under `prices` category)
- Controls: Region selector (5 NEM regions), Profile toggle (FLAT/PEAK/OFF_PEAK), Refresh, Save Snapshot
- KPI cards: Avg/Max/Min forward price
- 3 Recharts LineCharts: Term Structure (single line), Regional Comparison (5 lines), Curve History (dashed overlays)
- Data table with month, price, quarter, source badges (SHAPED/EXTRAPOLATED/ILLUSTRATIVE)

### Copilot Tool — `get_forward_curve`
- 10th FMAPI tool added to `copilot.py`
- Parameters: region (optional, default NSW1), profile (optional, default FLAT)
- Dispatch: imports `_build_forward_curve` from curves.py, formats as text table
- Tested: "What is the forward curve for VIC?" → returns 24-month table with seasonal analysis

### Wiring
- `batch_futures_hedging.py` futures dashboard `forward_curve` field now calls `_build_forward_curve()` with try/except fallback
- `main.py` — curves_router included before auto_stubs_router
- `client.ts` — `curvesApi` object with 5 methods + 6 TypeScript interfaces
- `App.tsx` — route, nav item, route map entry, nav classification added

### Test Results (all passing)
- **Term Structure**: NSW1 FLAT 24 pts, $67-$99/MWh, avg $79.25, source=SHAPED
- **Profiles**: PEAK avg $96.69, OFF_PEAK avg $64.98 (correct peak/off-peak spread)
- **Regional**: NSW1 $79.25, VIC1 $76.06, QLD1 $56.95, SA1 $123.11, TAS1 $10.08
- **History**: 3 snapshots from different ASX trade dates
- **Snapshot POST**: 360 rows persisted (5 regions × 3 profiles × 24 months)
- **Snapshots GET**: Read-back confirmed 24 points, avg $79.25
- **Copilot**: Tool invoked correctly, returned formatted table with quarter averages and seasonal analysis
- **Dashboard**: All 3 charts render, KPIs correct, data table shows SHAPED badges

### Files Changed
| File | Action |
|------|--------|
| `app/routers/curves.py` | **NEW** — Forward curve engine + 5 endpoints |
| `app/frontend/src/pages/ForwardCurves.tsx` | **NEW** — Term structure, comparison, history charts |
| `app/main.py` | Added curves_router import + include |
| `app/routers/copilot.py` | Added get_forward_curve tool definition + dispatch |
| `app/routers/batch_futures_hedging.py` | Wired forward_curve to real engine |
| `app/frontend/src/api/client.ts` | Added curvesApi + 6 interfaces |
| `app/frontend/src/App.tsx` | Added /forward-curves route, nav, classification |
| `setup/02_create_tables.py` | Added forward_curves table DDL |

**Deployed:** 2 deploys (initial + quarter key fix) — app running, all endpoints verified
