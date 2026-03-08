from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException

from .shared import _NEM_REGIONS, _AEST, _CATALOG, _query_gold, _sql_escape, logger
from .home import (
    prices_latest,
    market_summary_latest,
    interconnectors,
    prices_spikes,
    prices_volatility,
    generation_mix_pct,
    forecasts,
)
from .sidebar import bess_fleet, alerts_list, demand_response, merit_order

router = APIRouter()

# =========================================================================
# Copilot Chat — Databricks Foundation Model API (pay-per-token)
# =========================================================================

_CHAT_MODEL = "databricks-claude-sonnet-4-6"
_MAX_TOOL_ROUNDS = 5

# ---------------------------------------------------------------------------
# FMAPI Tool Definitions
# ---------------------------------------------------------------------------
_FMAPI_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_spot_prices",
            "description": "Get current and recent NEM spot prices (RRP) per region. Returns price, demand, and available generation.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region e.g. NSW1, QLD1, VIC1, SA1, TAS1. Omit for all regions.", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                    "hours": {"type": "integer", "description": "Lookback hours (default 1)", "default": 1},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_generation_mix",
            "description": "Get generation mix by fuel type for a NEM region. Returns MW output, capacity factor, emissions per fuel type.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                },
                "required": ["region"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_interconnectors",
            "description": "Get interstate interconnector flows, limits, and congestion status across the NEM.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_price_forecasts",
            "description": "Get price forecasts for a region including predicted RRP, confidence bounds, and spike probability.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                    "hours": {"type": "integer", "description": "Forecast horizon in hours (default 4)", "default": 4},
                },
                "required": ["region"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_weather",
            "description": "Get current weather conditions (temperature, wind, solar, cloud cover) for NEM regions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region. Omit for all.", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_demand_forecasts",
            "description": "Get demand forecasts for a region including predicted demand MW and confidence bounds.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                    "hours": {"type": "integer", "description": "Forecast horizon in hours (default 4)", "default": 4},
                },
                "required": ["region"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_market_rules",
            "description": "Search AEMO market rules, NER (National Electricity Rules), market procedures, and regulatory documents. Use for questions about NEM rules, dispatch procedures, FCAS requirements, bidding rules, etc.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Natural language search query about NEM rules or procedures"},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_trade",
            "description": "Create a new trade in the deal capture system. Parse natural language trade descriptions into structured trade parameters. Example: '50MW peak swap VIC Q3 2026 at $85' → trade_type=SWAP, region=VIC1, volume_mw=50, price=85, profile=PEAK, start_date=2026-07-01, end_date=2026-09-30.",
            "parameters": {
                "type": "object",
                "properties": {
                    "trade_type": {"type": "string", "enum": ["SPOT", "FORWARD", "SWAP", "FUTURE", "OPTION", "PPA", "REC"], "description": "Contract type"},
                    "region": {"type": "string", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"], "description": "NEM region"},
                    "buy_sell": {"type": "string", "enum": ["BUY", "SELL"], "description": "Trade direction"},
                    "volume_mw": {"type": "number", "description": "Volume in MW"},
                    "price": {"type": "number", "description": "Price in $/MWh"},
                    "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
                    "end_date": {"type": "string", "description": "End date YYYY-MM-DD"},
                    "profile": {"type": "string", "enum": ["FLAT", "PEAK", "OFF_PEAK", "SUPER_PEAK"], "description": "Load profile"},
                },
                "required": ["trade_type", "region", "buy_sell", "volume_mw", "price", "start_date", "end_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_portfolio_position",
            "description": "Get portfolio position summary showing net MW by region and quarter. Shows long/short positions and trade counts.",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio_name": {"type": "string", "description": "Portfolio name to look up. Omit to list all portfolios."},
                    "region": {"type": "string", "description": "Filter by NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_forward_curve",
            "description": "Get the forward electricity price curve for a NEM region. Returns monthly forward prices bootstrapped from ASX futures data with seasonal shaping. Useful for answering questions about future electricity prices, forward curves, and price outlooks.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                    "profile": {"type": "string", "description": "Load profile: FLAT (default), PEAK, or OFF_PEAK", "enum": ["FLAT", "PEAK", "OFF_PEAK"]},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_mtm_valuation",
            "description": "Run mark-to-market valuation for all trades or a specific portfolio. Values trades against forward curves to compute MtM, unrealised P&L, and daily P&L attribution. Returns total MtM and breakdown by portfolio.",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio_name": {"type": "string", "description": "Optional portfolio name to filter. Omit for all portfolios."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_risk_metrics",
            "description": "Get Value-at-Risk (VaR) and portfolio Greeks (delta, gamma, vega, theta) for a portfolio. Shows risk exposure at 95% and 99% confidence levels for 1-day and 10-day horizons.",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio_name": {"type": "string", "description": "Portfolio name to analyse. Required."},
                },
                "required": ["portfolio_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_credit_exposure",
            "description": "Get counterparty credit exposure summary showing current exposure, potential future exposure (PFE), credit utilisation, and alerts (WARNING/CRITICAL).",
            "parameters": {
                "type": "object",
                "properties": {
                    "counterparty_name": {"type": "string", "description": "Optional counterparty name to filter."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_alert_rule",
            "description": "Create a new alert rule to monitor NEM market conditions. Supports price thresholds, demand surges, FCAS prices, and forecast spikes. Rules are evaluated every 5 minutes.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                    "alert_type": {"type": "string", "description": "Type of alert", "enum": ["PRICE_THRESHOLD", "DEMAND_SURGE", "FCAS_PRICE", "FORECAST_SPIKE"]},
                    "threshold_value": {"type": "number", "description": "Threshold value (e.g. 300 for $300/MWh price alert)"},
                    "channel": {"type": "string", "description": "Notification channel", "enum": ["EMAIL", "SLACK", "IN_APP"]},
                },
                "required": ["region", "alert_type", "threshold_value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_anomaly",
            "description": "Get an AI-powered root cause explanation for a market anomaly (price spike, negative price, congestion event). Analyses generation changes, interconnector congestion, weather conditions, and constraints.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                    "timestamp": {"type": "string", "description": "ISO timestamp of the event (e.g. 2026-03-08T14:30:00)"},
                    "event_id": {"type": "string", "description": "Optional event ID for caching"},
                },
                "required": ["region"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_market_brief",
            "description": "Generate a market intelligence brief summarising overnight prices, key events, renewable share, congestion, and weather watch items across all NEM regions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "brief_type": {"type": "string", "description": "Type of brief", "enum": ["daily", "weekly", "flash"]},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_constraint_forecast",
            "description": "Get constraint binding heatmap showing when interconnector constraints are most likely to bind by hour-of-day and day-of-week for a specific NEM region.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                    "days": {"type": "integer", "description": "Lookback days (default 7)", "default": 7},
                },
                "required": ["region"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_portfolio_pnl",
            "description": "Get P&L attribution and MtM summary for a portfolio. Shows price effect, volume effect, new trades effect, time decay, and total P&L. Also returns latest MtM valuation.",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio_name": {"type": "string", "description": "Portfolio name to look up"},
                    "days_back": {"type": "integer", "description": "Number of days of history (default 30)", "default": 30},
                },
                "required": ["portfolio_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_pnl_move",
            "description": "Explain what drove the P&L change for a portfolio on a specific date. Breaks down into price, volume, new trades, and time decay effects by region.",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio_name": {"type": "string", "description": "Portfolio name"},
                    "valuation_date": {"type": "string", "description": "Date to explain (YYYY-MM-DD). Defaults to latest."},
                },
                "required": ["portfolio_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "value_ppa",
            "description": "Value a Power Purchase Agreement (PPA) using Monte Carlo simulation. Returns expected NPV, P10/P50/P90 NPVs, breakeven strike, and annual cashflows. Supports solar and wind technologies.",
            "parameters": {
                "type": "object",
                "properties": {
                    "strike_price": {"type": "number", "description": "PPA strike price in $/MWh"},
                    "term_years": {"type": "integer", "description": "Contract term in years"},
                    "technology": {"type": "string", "description": "Generation technology", "enum": ["solar_utility", "wind"]},
                    "region": {"type": "string", "description": "NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                    "volume_mw": {"type": "number", "description": "Capacity in MW"},
                    "escalation": {"type": "number", "description": "Annual price escalation (default 0.025 = 2.5%)", "default": 0.025},
                },
                "required": ["strike_price", "term_years", "technology", "region", "volume_mw"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_stress_test",
            "description": "Run a stress test on the portfolio. Predefined scenarios: sa_heatwave (SA prices +300%), wind_drought (wind gen -80%), coal_trip_2gw (NSW coal offline), interconnector_failure (QNI failure). Returns base vs stressed MtM, regional impacts, and top trade impacts.",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID or 'all'", "default": "all"},
                    "scenario_id": {"type": "string", "description": "Scenario ID", "enum": ["sa_heatwave", "wind_drought", "coal_trip_2gw", "interconnector_failure"]},
                },
                "required": ["scenario_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "portfolio_what_if",
            "description": "Run a what-if scenario analysis on the portfolio. Accepts natural language scenarios like 'What if SA prices spike 50%?' or 'What if there is a heatwave?'. Returns portfolio P&L impact, regional breakdown, and top affected trades.",
            "parameters": {
                "type": "object",
                "properties": {
                    "scenario_description": {"type": "string", "description": "Natural language scenario description"},
                    "portfolio_id": {"type": "string", "description": "Portfolio ID or 'all'", "default": "all"},
                    "price_change_pct": {"type": "number", "description": "Explicit price change % (optional, overrides NL parsing)"},
                    "region": {"type": "string", "description": "Target region (optional)", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                },
                "required": ["scenario_description"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "predict_constraints",
            "description": "Predict which transmission constraints will bind in the next 4-48 hours using ML. Returns probability of binding, expected marginal value, and price impact for each constraint.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                    "horizon_hours": {"type": "integer", "description": "Forecast horizon (4-48 hours)", "default": 24},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_fcas_summary",
            "description": "Get FCAS (Frequency Control Ancillary Services) market summary — 8 service prices, total cost, top providers, and regional requirements.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "NEM region", "enum": ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_settlement_summary",
            "description": "Get settlement reconciliation summary — AEMO settlement totals vs internal trade positions, variance analysis, FCAS settlement, and inter-regional residues.",
            "parameters": {
                "type": "object",
                "properties": {
                    "days_back": {"type": "integer", "description": "Lookback days (default 7)", "default": 7},
                },
                "required": [],
            },
        },
    },
]

# ---------------------------------------------------------------------------
# Vector Search client (lazy-loaded)
# ---------------------------------------------------------------------------
_vs_index = None
_VS_ENDPOINT = os.environ.get("VS_ENDPOINT_NAME", "energy-copilot-vs-endpoint")
_VS_INDEX_NAME = os.environ.get("VS_INDEX_NAME", f"{_CATALOG}.gold.aemo_docs_vs_index")


def _dispatch_tool(name: str, arguments: dict) -> str:
    """Execute a tool call and return JSON result string."""
    try:
        if name == "query_spot_prices":
            region = arguments.get("region")
            hours = arguments.get("hours", 1)
            where = f"AND region_id = '{region}'" if region else ""
            rows = _query_gold(
                f"SELECT region_id, rrp, total_demand_mw, available_gen_mw, interval_datetime "
                f"FROM {_CATALOG}.gold.nem_prices_5min "
                f"WHERE interval_datetime >= current_timestamp() - INTERVAL {hours} HOURS {where} "
                f"ORDER BY interval_datetime DESC LIMIT 50"
            )
            if rows:
                for r in rows:
                    r["interval_datetime"] = str(r["interval_datetime"])
                return json.dumps(rows[:20], default=str)
            return json.dumps({"error": "No price data available"})

        elif name == "query_generation_mix":
            region = arguments.get("region", "NSW1")
            rows = _query_gold(
                f"SELECT fuel_type, is_renewable, total_mw, unit_count, capacity_factor, "
                f"emissions_tco2e, emissions_intensity "
                f"FROM {_CATALOG}.gold.nem_generation_by_fuel "
                f"WHERE region_id = '{region}' "
                f"AND interval_datetime = ("
                f"  SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_generation_by_fuel"
                f") ORDER BY total_mw DESC"
            )
            if rows:
                return json.dumps(rows, default=str)
            return json.dumps({"error": f"No generation data for {region}"})

        elif name == "query_interconnectors":
            rows = _query_gold(
                f"SELECT interconnector_id, from_region, to_region, mw_flow, "
                f"export_limit_mw, import_limit_mw, utilization_pct, is_congested "
                f"FROM {_CATALOG}.gold.nem_interconnectors "
                f"WHERE interval_datetime = ("
                f"  SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_interconnectors"
                f")"
            )
            if rows:
                return json.dumps(rows, default=str)
            return json.dumps({"error": "No interconnector data"})

        elif name == "query_price_forecasts":
            region = arguments.get("region", "NSW1")
            hours = arguments.get("hours", 4)
            rows = _query_gold(
                f"SELECT interval_datetime, predicted_rrp, prediction_lower_80, "
                f"prediction_upper_80, spike_probability, model_name "
                f"FROM {_CATALOG}.gold.price_forecasts "
                f"WHERE region_id = '{region}' "
                f"AND forecast_run_at = ("
                f"  SELECT MAX(forecast_run_at) FROM {_CATALOG}.gold.price_forecasts WHERE region_id = '{region}'"
                f") "
                f"AND interval_datetime <= current_timestamp() + INTERVAL {hours} HOURS "
                f"ORDER BY interval_datetime LIMIT 50"
            )
            if rows:
                for r in rows:
                    r["interval_datetime"] = str(r["interval_datetime"])
                return json.dumps(rows, default=str)
            return json.dumps({"error": f"No price forecasts for {region}"})

        elif name == "query_weather":
            region = arguments.get("region")
            where = f"AND nem_region = '{region}'" if region else ""
            rows = _query_gold(
                f"SELECT nem_region, forecast_datetime, temperature_c, "
                f"wind_speed_100m_kmh, solar_radiation_wm2, cloud_cover_pct "
                f"FROM {_CATALOG}.gold.weather_nem_regions "
                f"WHERE forecast_datetime >= current_timestamp() - INTERVAL 2 HOURS {where} "
                f"ORDER BY forecast_datetime DESC LIMIT 25"
            )
            if rows:
                for r in rows:
                    r["forecast_datetime"] = str(r["forecast_datetime"])
                return json.dumps(rows, default=str)
            return json.dumps({"error": "No weather data available"})

        elif name == "query_demand_forecasts":
            region = arguments.get("region", "NSW1")
            hours = arguments.get("hours", 4)
            rows = _query_gold(
                f"SELECT interval_datetime, predicted_demand_mw, prediction_lower_80, "
                f"prediction_upper_80, model_name "
                f"FROM {_CATALOG}.gold.demand_forecasts "
                f"WHERE region_id = '{region}' "
                f"AND forecast_run_at = ("
                f"  SELECT MAX(forecast_run_at) FROM {_CATALOG}.gold.demand_forecasts WHERE region_id = '{region}'"
                f") "
                f"AND interval_datetime <= current_timestamp() + INTERVAL {hours} HOURS "
                f"ORDER BY interval_datetime LIMIT 50"
            )
            if rows:
                for r in rows:
                    r["interval_datetime"] = str(r["interval_datetime"])
                return json.dumps(rows, default=str)
            return json.dumps({"error": f"No demand forecasts for {region}"})

        elif name == "search_market_rules":
            query = arguments.get("query", "")
            if not query:
                return json.dumps({"error": "Query parameter required"})
            # Try Vector Search index
            try:
                global _vs_index
                if _vs_index is None:
                    from databricks.sdk import WorkspaceClient
                    w = WorkspaceClient()
                    _vs_index = w.vector_search_indexes.get_index(
                        index_name=_VS_INDEX_NAME,
                        endpoint_name=_VS_ENDPOINT,
                    )
                results = _vs_index.similarity_search(
                    query_text=query,
                    columns=["content", "metadata_json"],
                    num_results=5,
                )
                if results and results.get("result", {}).get("data_array"):
                    chunks = []
                    for row in results["result"]["data_array"]:
                        chunks.append({"content": row[0], "metadata": row[1] if len(row) > 1 else ""})
                    return json.dumps(chunks, default=str)
            except Exception as vs_exc:
                logger.warning("Vector search failed: %s — falling back to static", vs_exc)
            # Fallback: return a helpful message with common NEM rules
            return json.dumps({
                "note": "Vector search index not yet populated. Here are key NEM rules references:",
                "references": [
                    {"topic": "NER Chapter 3 - Market Rules", "summary": "Covers bidding, dispatch, pricing, and settlement"},
                    {"topic": "NER Chapter 4 - Power System Security", "summary": "System security, frequency control, load shedding"},
                    {"topic": "MASS - Market Ancillary Services", "summary": "FCAS requirements, regulation, contingency services"},
                    {"topic": "SO_OP 3705 - Dispatch", "summary": "5-minute dispatch process, NEMDE, constraint formulation"},
                    {"topic": "Administered Price Cap", "summary": f"Cumulative Price Threshold triggers $300/MWh cap"},
                ],
            })

        elif name == "create_trade":
            from .shared import _insert_gold, _invalidate_cache
            trade_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            trade_data = {
                "trade_id": trade_id,
                "trade_type": arguments.get("trade_type", "SWAP"),
                "region": arguments.get("region", "NSW1"),
                "buy_sell": arguments.get("buy_sell", "BUY"),
                "volume_mw": arguments.get("volume_mw", 50),
                "price": arguments.get("price", 75),
                "start_date": arguments.get("start_date", "2026-07-01"),
                "end_date": arguments.get("end_date", "2026-09-30"),
                "profile": arguments.get("profile", "FLAT"),
                "status": "DRAFT",
                "counterparty_id": "",
                "portfolio_id": "",
                "notes": "Created via Copilot AI",
                "created_by": "copilot",
                "created_at": now,
                "updated_at": now,
            }
            ok = _insert_gold(f"{_CATALOG}.gold.trades", trade_data)
            if ok:
                _invalidate_cache("sql:")
                return json.dumps({
                    "status": "created",
                    "trade_id": trade_id,
                    "summary": (
                        f"{trade_data['buy_sell']} {trade_data['volume_mw']}MW "
                        f"{trade_data['profile']} {trade_data['trade_type']} "
                        f"{trade_data['region']} at ${trade_data['price']}/MWh "
                        f"({trade_data['start_date']} to {trade_data['end_date']})"
                    ),
                })
            return json.dumps({"error": "Failed to insert trade into database"})

        elif name == "get_portfolio_position":
            portfolio_name = arguments.get("portfolio_name")
            region = arguments.get("region")
            if portfolio_name:
                portfolios = _query_gold(
                    f"SELECT portfolio_id, name FROM {_CATALOG}.gold.portfolios "
                    f"WHERE LOWER(name) LIKE '%{portfolio_name.lower()}%' LIMIT 5"
                )
            else:
                portfolios = _query_gold(
                    f"SELECT portfolio_id, name FROM {_CATALOG}.gold.portfolios ORDER BY name LIMIT 10"
                )
            if not portfolios:
                return json.dumps({"error": "No portfolios found", "suggestion": "Create portfolios via Deal Capture first"})
            results = []
            for p in portfolios:
                pid = p["portfolio_id"]
                where = f"AND t.region = '{region}'" if region else ""
                rows = _query_gold(
                    f"SELECT t.region, "
                    f"CONCAT('Q', QUARTER(t.start_date), ' ', YEAR(t.start_date)) as quarter, "
                    f"SUM(CASE WHEN t.buy_sell = 'BUY' THEN t.volume_mw ELSE -t.volume_mw END) as net_mw, "
                    f"COUNT(*) as trade_count "
                    f"FROM {_CATALOG}.gold.trades t "
                    f"INNER JOIN {_CATALOG}.gold.portfolio_trades pt ON t.trade_id = pt.trade_id "
                    f"WHERE pt.portfolio_id = '{pid}' AND t.status != 'CANCELLED' {where} "
                    f"GROUP BY t.region, QUARTER(t.start_date), YEAR(t.start_date) "
                    f"ORDER BY t.region, YEAR(t.start_date), QUARTER(t.start_date)"
                )
                results.append({
                    "portfolio": p["name"],
                    "positions": rows or [],
                })
            return json.dumps(results, default=str)

        elif name == "get_forward_curve":
            from .curves import _build_forward_curve
            region = arguments.get("region", "NSW1")
            profile = arguments.get("profile", "FLAT")
            points = _build_forward_curve(region=region, profile=profile)
            if points:
                lines = [f"Forward curve for {region} ({profile} profile):"]
                lines.append(f"{'Month':<10} {'$/MWh':>8} {'Source':<15}")
                lines.append("-" * 35)
                for pt in points:
                    lines.append(f"{pt['month']:<10} ${pt['price_mwh']:>7.2f} {pt['source']:<15}")
                return json.dumps({"text": "\n".join(lines), "points": points}, default=str)
            return json.dumps({"error": f"No forward curve data for {region}"})

        elif name == "run_mtm_valuation":
            from .risk import _run_mtm_core
            portfolio_name = arguments.get("portfolio_name")
            portfolio_id = None
            if portfolio_name:
                rows = _query_gold(
                    f"SELECT portfolio_id FROM {_CATALOG}.gold.portfolios "
                    f"WHERE LOWER(name) LIKE LOWER('%{_sql_escape(portfolio_name)}%') LIMIT 1"
                )
                if rows:
                    portfolio_id = rows[0]["portfolio_id"]
            result = _run_mtm_core(portfolio_id=portfolio_id)
            return json.dumps(result, default=str)

        elif name == "get_risk_metrics":
            from .risk import _calculate_var_greeks
            portfolio_name = arguments.get("portfolio_name", "")
            rows = _query_gold(
                f"SELECT portfolio_id FROM {_CATALOG}.gold.portfolios "
                f"WHERE LOWER(name) LIKE LOWER('%{_sql_escape(portfolio_name)}%') LIMIT 1"
            )
            if not rows:
                return json.dumps({"error": f"Portfolio '{portfolio_name}' not found"})
            result = _calculate_var_greeks(portfolio_id=rows[0]["portfolio_id"])
            return json.dumps(result, default=str)

        elif name == "get_credit_exposure":
            from .risk import _calculate_credit_exposure
            result = _calculate_credit_exposure()
            cp_name = arguments.get("counterparty_name")
            if cp_name and result.get("exposures"):
                filtered = [
                    e for e in result["exposures"]
                    if cp_name.lower() in str(e.get("counterparty_name", "")).lower()
                ]
                if filtered:
                    result["exposures"] = filtered
                    result["counterparties"] = len(filtered)
            return json.dumps(result, default=str)

        elif name == "create_alert_rule":
            from .sidebar import _create_alert_rule_core
            result = _create_alert_rule_core({
                "region": arguments.get("region", "NSW1"),
                "alert_type": arguments.get("alert_type", "PRICE_THRESHOLD"),
                "threshold_value": arguments.get("threshold_value", 300),
                "notification_channel": arguments.get("channel", "IN_APP"),
                "created_by": "copilot",
            })
            return json.dumps(result, default=str)

        elif name == "explain_anomaly":
            from .alerts import _explain_anomaly_core
            region = arguments.get("region", "NSW1")
            timestamp = arguments.get("timestamp", "")
            event_id = arguments.get("event_id", "")
            if not timestamp:
                from datetime import datetime as _dt, timezone as _tz
                timestamp = _dt.now(_tz.utc).isoformat()
            result = _explain_anomaly_core(event_id, region, timestamp)
            return json.dumps(result, default=str)

        elif name == "generate_market_brief":
            from .market_briefs import _generate_brief_core
            brief_type = arguments.get("brief_type", "daily")
            result = _generate_brief_core(brief_type)
            return json.dumps(result, default=str)

        elif name == "get_constraint_forecast":
            region = arguments.get("region", "NSW1")
            days = arguments.get("days", 7)
            rows = _query_gold(
                f"SELECT HOUR(interval_datetime) AS hour_of_day, "
                f"DAYOFWEEK(interval_datetime) AS day_of_week, "
                f"COUNT(*) AS total_intervals, "
                f"SUM(CASE WHEN is_congested = true THEN 1 ELSE 0 END) AS binding_count "
                f"FROM {_CATALOG}.gold.nem_interconnectors "
                f"WHERE (from_region = '{region}' OR to_region = '{region}') "
                f"AND interval_datetime >= current_timestamp() - INTERVAL {days} DAYS "
                f"GROUP BY HOUR(interval_datetime), DAYOFWEEK(interval_datetime) "
                f"ORDER BY binding_count DESC LIMIT 20"
            )
            if rows:
                lines = [f"Constraint binding heatmap for {region} (last {days} days):"]
                lines.append(f"{'Hour':>4} {'Day':>4} {'Binding%':>9} {'Count':>6}")
                for r in rows:
                    total = int(r.get("total_intervals", 1))
                    binding = int(r.get("binding_count", 0))
                    pct = binding / max(total, 1) * 100
                    lines.append(f"{r['hour_of_day']:>4} {r['day_of_week']:>4} {pct:>8.1f}% {binding:>6}")
                return json.dumps({"text": "\n".join(lines), "data": rows}, default=str)
            return json.dumps({"message": f"No constraint data for {region} in last {days} days"})

        elif name == "get_portfolio_pnl":
            from .risk import _run_mtm_core
            portfolio_name = arguments.get("portfolio_name", "")
            rows = _query_gold(
                f"SELECT portfolio_id, name FROM {_CATALOG}.gold.portfolios "
                f"WHERE LOWER(name) LIKE LOWER('%{_sql_escape(portfolio_name)}%') LIMIT 1"
            )
            if not rows:
                return json.dumps({"error": f"Portfolio '{portfolio_name}' not found"})
            pid = rows[0]["portfolio_id"]
            # Get latest MtM
            mtm = _query_gold(
                f"SELECT SUM(mtm_value) as total_mtm, COUNT(*) as trades_valued "
                f"FROM {_CATALOG}.gold.portfolio_mtm "
                f"WHERE portfolio_id = '{pid}' "
                f"AND valuation_date = (SELECT MAX(valuation_date) FROM {_CATALOG}.gold.portfolio_mtm WHERE portfolio_id = '{pid}')"
            )
            # Get latest PnL attribution
            attr = _query_gold(
                f"SELECT * FROM {_CATALOG}.gold.pnl_attribution "
                f"WHERE portfolio_id = '{pid}' "
                f"AND valuation_date = (SELECT MAX(valuation_date) FROM {_CATALOG}.gold.pnl_attribution WHERE portfolio_id = '{pid}')"
            )
            result = {
                "portfolio": rows[0]["name"],
                "mtm_summary": mtm[0] if mtm else {"total_mtm": 0, "trades_valued": 0},
                "pnl_attribution": attr or [],
            }
            return json.dumps(result, default=str)

        elif name == "explain_pnl_move":
            portfolio_name = arguments.get("portfolio_name", "")
            valuation_date = arguments.get("valuation_date", "")
            rows = _query_gold(
                f"SELECT portfolio_id, name FROM {_CATALOG}.gold.portfolios "
                f"WHERE LOWER(name) LIKE LOWER('%{_sql_escape(portfolio_name)}%') LIMIT 1"
            )
            if not rows:
                return json.dumps({"error": f"Portfolio '{portfolio_name}' not found"})
            pid = rows[0]["portfolio_id"]
            date_filter = f"AND valuation_date = '{_sql_escape(valuation_date)}'" if valuation_date else (
                f"AND valuation_date = (SELECT MAX(valuation_date) FROM {_CATALOG}.gold.pnl_attribution WHERE portfolio_id = '{pid}')"
            )
            attr = _query_gold(
                f"SELECT * FROM {_CATALOG}.gold.pnl_attribution "
                f"WHERE portfolio_id = '{pid}' {date_filter} "
                f"ORDER BY ABS(total_pnl) DESC"
            )
            if not attr:
                return json.dumps({"error": "No P&L attribution data. Run MtM valuation first."})
            lines = [f"P&L Attribution for {rows[0]['name']}:"]
            lines.append(f"{'Region':<8} {'Price':>12} {'Volume':>12} {'NewTrades':>12} {'Decay':>12} {'Total':>12}")
            for a in attr:
                lines.append(
                    f"{a.get('region', '?'):<8} "
                    f"${a.get('price_effect', 0):>10,.0f} "
                    f"${a.get('volume_effect', 0):>10,.0f} "
                    f"${a.get('new_trades_effect', 0):>10,.0f} "
                    f"${a.get('time_decay', 0):>10,.0f} "
                    f"${a.get('total_pnl', 0):>10,.0f}"
                )
            total = sum(float(a.get("total_pnl", 0) or 0) for a in attr)
            lines.append(f"\nTotal P&L: ${total:,.0f}")
            return json.dumps({"text": "\n".join(lines), "attribution": attr}, default=str)

        elif name == "value_ppa":
            from .risk import _value_ppa_core
            result = _value_ppa_core(
                strike_price=arguments.get("strike_price", 55),
                term_years=arguments.get("term_years", 10),
                technology=arguments.get("technology", "solar_utility"),
                region=arguments.get("region", "NSW1"),
                volume_mw=arguments.get("volume_mw", 100),
                escalation=arguments.get("escalation", 0.025),
            )
            lines = [f"PPA Valuation: {result['volume_mw']}MW {result['technology']} in {result['region']}"]
            lines.append(f"Strike: ${result['strike_price']}/MWh, Term: {result['term_years']}yr, Escalation: {result['escalation']:.1%}")
            lines.append(f"\nNPV Distribution:")
            lines.append(f"  Expected NPV: ${result['expected_npv']:,.0f}")
            lines.append(f"  P10 (downside): ${result['p10_npv']:,.0f}")
            lines.append(f"  P50 (median):   ${result['p50_npv']:,.0f}")
            lines.append(f"  P90 (upside):   ${result['p90_npv']:,.0f}")
            lines.append(f"\nKey Metrics:")
            lines.append(f"  Capacity Factor: {result['capacity_factor']:.0%}")
            lines.append(f"  Capture Discount: {result['capture_price_discount']:.0%}")
            lines.append(f"  Breakeven Strike: ${result['breakeven_strike']:.2f}/MWh")
            return json.dumps({"text": "\n".join(lines), "valuation": result}, default=str)

        elif name == "run_stress_test":
            from .risk import _run_stress_test_core
            result = _run_stress_test_core(
                portfolio_id=arguments.get("portfolio_id", "all"),
                scenario_id=arguments.get("scenario_id", "sa_heatwave"),
            )
            if "error" in result:
                return json.dumps(result)
            lines = [f"Stress Test: {result['scenario_name']}"]
            lines.append(f"  {result['scenario_description']}")
            lines.append(f"\nPortfolio Impact ({result['trades_analysed']} trades):")
            lines.append(f"  Base MtM:     ${result['base_mtm']:>14,.0f}")
            lines.append(f"  Stressed MtM: ${result['stressed_mtm']:>14,.0f}")
            lines.append(f"  Impact:       ${result['total_impact']:>14,.0f} ({result['impact_pct']:+.1f}%)")
            if result.get("region_impacts"):
                lines.append("\nRegional Breakdown:")
                for r, v in sorted(result["region_impacts"].items(), key=lambda x: abs(x[1]), reverse=True):
                    lines.append(f"  {r}: ${v:>12,.0f}")
            if result.get("top_trade_impacts"):
                lines.append("\nTop Affected Trades:")
                for ti in result["top_trade_impacts"][:5]:
                    lines.append(f"  {ti['trade_id']} {ti['direction']} {ti['volume_mw']}MW {ti['region']} → ${ti['impact']:,.0f}")
            return json.dumps({"text": "\n".join(lines), "stress_test": result}, default=str)

        elif name == "portfolio_what_if":
            from .risk import _what_if_core
            result = _what_if_core(
                scenario_description=arguments.get("scenario_description", ""),
                portfolio_id=arguments.get("portfolio_id", "all"),
                price_change_pct=arguments.get("price_change_pct"),
                region=arguments.get("region"),
            )
            if "error" in result:
                return json.dumps(result)
            lines = [f"What-If Analysis: {result['scenario_name']}"]
            lines.append(f"\nPortfolio Impact ({result['trades_analysed']} trades):")
            lines.append(f"  Base MtM:     ${result['base_mtm']:>14,.0f}")
            lines.append(f"  Scenario MtM: ${result['stressed_mtm']:>14,.0f}")
            lines.append(f"  Delta P&L:    ${result['total_impact']:>14,.0f} ({result['impact_pct']:+.1f}%)")
            if result.get("region_impacts"):
                lines.append("\nRegional P&L Impact:")
                for r, v in sorted(result["region_impacts"].items(), key=lambda x: abs(x[1]), reverse=True):
                    lines.append(f"  {r}: ${v:>12,.0f}")
            if result.get("top_trade_impacts"):
                lines.append("\nMost Affected Trades:")
                for ti in result["top_trade_impacts"][:5]:
                    lines.append(f"  {ti['trade_id']} {ti['direction']} {ti['volume_mw']}MW {ti['region']} → ${ti['impact']:,.0f}")
            return json.dumps({"text": "\n".join(lines), "what_if": result}, default=str)

        elif name == "predict_constraints":
            from .constraints import _predict_constraint_binding
            region = arguments.get("region")
            horizon = arguments.get("horizon_hours", 24)
            result = _predict_constraint_binding(region, horizon, min_probability=0.3)
            forecasts = result.get("forecasts", [])[:10]
            lines = [f"Constraint Binding Forecast ({result.get('horizon_hours', 24)}h horizon):"]
            lines.append(f"  Model: {result.get('model_info', {}).get('version', 'unknown')}")
            lines.append(f"  Constraints analysed: {result.get('constraints_analysed', 0)}")
            if forecasts:
                lines.append(f"\n{'Constraint':<25} {'Region':<6} {'Hour':<18} {'Prob':>6} {'MV':>8} {'Impact':>8} {'Conf':<7}")
                lines.append("-" * 80)
                for f in forecasts:
                    lines.append(
                        f"{f['constraint_id'][:24]:<25} {f['region']:<6} {f['target_hour']:<18} "
                        f"{f['binding_probability']:>5.1%} ${f['expected_marginal_value']:>7.1f} "
                        f"${f['price_impact_estimate']:>7.2f} {f['confidence']:<7}"
                    )
            else:
                lines.append("\nNo constraints predicted to bind above threshold.")
            return json.dumps({"text": "\n".join(lines), "forecasts": forecasts}, default=str)

        elif name == "get_fcas_summary":
            from .sidebar import _build_fcas_dashboard
            region = arguments.get("region", "NSW1")
            result = _build_fcas_dashboard(region)
            lines = [f"FCAS Market Summary for {region}:"]
            lines.append(f"  Total FCAS Cost: ${result['total_fcas_cost_today_aud']:,.0f}")
            lines.append(f"  Regulation: ${result['regulation_cost_aud']:,.0f} | Contingency: ${result['contingency_cost_aud']:,.0f}")
            lines.append(f"  Enabled MW: {result['total_enabled_mw']:,.0f} | Shortfall Risk: {result['shortfall_risk']}")
            lines.append(f"\n{'Service':<12} {'Price $/MW':>10} {'Req MW':>8} {'Enabled':>8} {'Util%':>6} {'Provider':<20}")
            lines.append("-" * 70)
            for s in result["services"]:
                lines.append(
                    f"{s['service']:<12} ${s['clearing_price_aud_mw']:>9.2f} "
                    f"{s['requirement_mw']:>7.0f} {s['enabled_mw']:>7.0f} "
                    f"{s['utilisation_pct']:>5.1f} {s['main_provider'][:19]:<20}"
                )
            if result.get("providers"):
                lines.append(f"\nTop Providers ({len(result['providers'])}):")
                for p in result["providers"][:5]:
                    lines.append(f"  {p['station_name']} ({p['fuel_type']}) — {p['total_enabled_mw']}MW, ${p['total_revenue_est_aud']:,.0f}/yr")
            return json.dumps({"text": "\n".join(lines), "dashboard": result}, default=str)

        elif name == "get_settlement_summary":
            from .sidebar import _build_settlement_reconciliation
            days = arguments.get("days_back", 7)
            result = _build_settlement_reconciliation(days)
            lines = [f"Settlement Reconciliation (last {days} days):"]
            lines.append(f"  AEMO Settlement:    ${result['total_aemo_settlement']:>14,.2f}")
            lines.append(f"  Internal Settlement: ${result['total_internal_settlement']:>14,.2f}")
            lines.append(f"  Variance:           ${result['variance']:>14,.2f} ({result['variance_pct']:+.2f}%)")
            lines.append(f"  Threshold Breaches: {result['threshold_breaches']}")
            if result.get("variances_by_region"):
                lines.append(f"\n{'Region':<8} {'AEMO':>14} {'Internal':>14} {'Variance':>14} {'Status':<8}")
                lines.append("-" * 62)
                for v in result["variances_by_region"]:
                    lines.append(
                        f"{v['region']:<8} ${v['aemo_aud']:>13,.2f} ${v['internal_aud']:>13,.2f} "
                        f"${v['variance_aud']:>13,.2f} {v['status']:<8}"
                    )
            if result.get("residues"):
                lines.append(f"\nInter-Regional Residues:")
                for r in result["residues"]:
                    lines.append(f"  {r['interconnector_id']}: ${r['settlement_residue_aud']:,.2f} ({r['direction']}, {r['flow_mw']}MW avg)")
            return json.dumps({"text": "\n".join(lines), "reconciliation": result}, default=str)

        else:
            return json.dumps({"error": f"Unknown tool: {name}"})
    except Exception as exc:
        logger.warning("Tool dispatch error %s: %s", name, exc)
        return json.dumps({"error": f"Tool execution failed: {str(exc)[:200]}"})

_SYSTEM_PROMPT_BASE = (
    "You are the AUS Energy Copilot, an expert AI assistant specialising in "
    "Australia's National Electricity Market (NEM). You have LIVE access to "
    "NEM market data which is provided below. Use this data to give specific, "
    "data-driven answers. Never say you don't have access to data — you DO. "
    "When discussing prices, use AUD $/MWh. Refer to NEM regions as NSW1, "
    "QLD1, VIC1, SA1, TAS1. Be concise but thorough."
)


async def _build_market_context() -> str:
    """Gather live market data from our own endpoints to inject as LLM context."""
    parts = []
    now = datetime.now(_AEST)
    parts.append(f"Current time (AEST): {now.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Current spot prices
    try:
        prices = await prices_latest()
        lines = []
        for p in prices:
            lines.append(f"  {p['region']}: ${p['price']:.2f}/MWh ({p['trend']})")
        parts.append("CURRENT SPOT PRICES:\n" + "\n".join(lines))
    except Exception:
        pass

    # 2. Market summary narrative
    try:
        summary = await market_summary_latest()
        parts.append(f"MARKET SUMMARY:\n  {summary['narrative']}")
    except Exception:
        pass

    # 3. Price spikes (recent events across all regions)
    try:
        all_spikes = []
        for r in _NEM_REGIONS:
            spikes = await prices_spikes(region=r)
            all_spikes.extend(spikes)
        all_spikes.sort(key=lambda s: s["timestamp"], reverse=True)
        if all_spikes:
            lines = []
            for s in all_spikes[:10]:
                ts = s["timestamp"][:16].replace("T", " ")
                lines.append(
                    f"  {s['region']} {ts}: ${s['peakPrice']:.0f}/MWh "
                    f"({s['durationMinutes']}min, trigger={s['trigger']})"
                )
            parts.append("RECENT PRICE SPIKES (last 72h):\n" + "\n".join(lines))
    except Exception:
        pass

    # 4. Interconnector flows
    try:
        ic_data = await interconnectors()
        lines = []
        for ic in ic_data.get("interconnectors", []):
            cong = " CONGESTED" if ic.get("congested") else ""
            lines.append(
                f"  {ic['interconnectorid']} ({ic['from_region']}→{ic['to_region']}): "
                f"{ic['mw_flow']:.0f} MW / {ic['mw_flow_limit']} MW limit{cong}"
            )
        parts.append(
            f"INTERCONNECTOR FLOWS (total interstate: {ic_data.get('total_interstate_mw', 0):.0f} MW):\n"
            + "\n".join(lines)
        )
    except Exception:
        pass

    # 5. Generation mix for each region
    nem_total_gen = 0.0
    nem_total_renew = 0.0
    try:
        for r in _NEM_REGIONS:
            mix = await generation_mix_pct(region=r)
            total_mw = mix.get("total_generation_mw", 0)
            renew_pct = mix.get("renewable_percentage", 0)
            renew_mw = mix.get("renewable_mw", 0)
            nem_total_gen += total_mw
            nem_total_renew += renew_mw
            fuel_lines = []
            for fm in mix.get("fuel_mix", []):
                fuel_lines.append(
                    f"    {fm['fuel_type']}: {fm['percentage']:.1f}% "
                    f"({fm.get('total_mw', 0):.0f} MW, "
                    f"{'renewable' if fm.get('is_renewable') else 'fossil'})"
                )
            parts.append(
                f"GENERATION MIX — {r} (total {total_mw:.0f} MW, "
                f"renewable {renew_pct:.1f}% = {renew_mw:.0f} MW, "
                f"carbon intensity {mix.get('carbon_intensity_kg_co2_mwh', 0)} kg CO₂/MWh):\n"
                + "\n".join(fuel_lines)
            )
    except Exception as exc:
        logger.warning("Context: generation mix failed: %s", exc)

    if nem_total_gen > 0:
        parts.append(
            f"NEM TOTAL GENERATION: {nem_total_gen:.0f} MW, "
            f"renewable {nem_total_renew:.0f} MW ({nem_total_renew / nem_total_gen * 100:.1f}%)"
        )

    # 6. Price volatility
    try:
        vol = await prices_volatility()
        vol_regions = vol.get("regions", []) if isinstance(vol, dict) else vol
        lines = []
        for v in vol_regions:
            lines.append(
                f"  {v['region']}: avg=${v.get('mean_price', 0):.1f}, "
                f"std_dev=${v.get('std_dev', 0):.1f}, "
                f"range=[${v.get('min_price', 0):.0f}, ${v.get('max_price', 0):.0f}], "
                f"spikes={v.get('spike_count', 0)}"
            )
        parts.append("PRICE VOLATILITY (24h):\n" + "\n".join(lines))
    except Exception as exc:
        logger.warning("Context: volatility failed: %s", exc)

    # 7. BESS fleet summary
    try:
        fleet_data = await bess_fleet()
        if isinstance(fleet_data, dict):
            units = fleet_data.get("units", [])
            parts.append(
                f"BATTERY STORAGE FLEET: {len(units)} units, "
                f"{fleet_data.get('total_power_mw', 0):.0f} MW power capacity, "
                f"{fleet_data.get('total_capacity_mwh', 0):.0f} MWh energy capacity, "
                f"avg SoC {fleet_data.get('fleet_avg_soc_pct', 0):.1f}%, "
                f"{fleet_data.get('units_discharging', 0)} discharging, "
                f"{fleet_data.get('units_charging', 0)} charging, "
                f"{fleet_data.get('units_idle', 0)} idle, "
                f"fleet revenue today ${fleet_data.get('fleet_revenue_today_aud', 0):,.0f}"
            )
            bess_lines = []
            for u in units:
                bess_lines.append(
                    f"  {u['station_name']} ({u['region']}): {u['mode']}, "
                    f"{u['current_mw']:.0f} MW, SoC {u['soc_pct']:.0f}%, "
                    f"capacity {u['power_mw']} MW / {u['capacity_mwh']} MWh"
                )
            parts.append("BESS UNIT DETAIL:\n" + "\n".join(bess_lines))
    except Exception as exc:
        logger.warning("Context: BESS fleet failed: %s", exc)

    # 8. Alerts
    try:
        alert_data = await alerts_list()
        alerts = alert_data if isinstance(alert_data, list) else alert_data.get("alerts", [])
        if alerts:
            triggered = [a for a in alerts if a.get("status") == "triggered"]
            armed = [a for a in alerts if a.get("status") == "armed"]
            lines = []
            for a in triggered[:8]:
                lines.append(
                    f"  [{a.get('status', '').upper()}] {a['region']} {a['metric']} "
                    f"threshold={a.get('threshold')} triggered={a.get('triggeredAt', 'N/A')[:16]}"
                )
            for a in armed[:4]:
                lines.append(f"  [ARMED] {a['region']} {a['metric']} threshold={a.get('threshold')}")
            parts.append(
                f"ALERTS ({len(triggered)} triggered, {len(armed)} armed, {len(alerts)} total):\n"
                + "\n".join(lines)
            )
    except Exception as exc:
        logger.warning("Context: alerts failed: %s", exc)

    # 9. Demand response
    try:
        dr = await demand_response()
        if isinstance(dr, dict):
            parts.append(
                f"DEMAND RESPONSE: {dr.get('active_programs', 0)} active programs, "
                f"{dr.get('total_enrolled_mw', 0):.0f} MW enrolled, "
                f"{dr.get('total_activated_mw_today', 0):.0f} MW activated today, "
                f"{dr.get('events_today', 0)} events today"
            )
            events = dr.get("events", [])
            if events:
                ev_lines = []
                for ev in events[:5]:
                    ev_lines.append(
                        f"  {ev.get('program_name', 'N/A')}: {ev.get('mw_reduction', 0)} MW reduction, "
                        f"{ev.get('participants', 0)} participants, trigger={ev.get('trigger_reason', 'N/A')}"
                    )
                parts.append("DR EVENTS TODAY:\n" + "\n".join(ev_lines))
    except Exception as exc:
        logger.warning("Context: demand response failed: %s", exc)

    # 10. Forecasts summary (next 4h price outlook)
    try:
        for r in _NEM_REGIONS:
            fc = await forecasts(region=r, horizon="4h")
            if fc:
                prices_fc = [p["predicted"] for p in fc]
                avg_fc = sum(prices_fc) / len(prices_fc)
                min_fc = min(prices_fc)
                max_fc = max(prices_fc)
                parts.append(
                    f"PRICE FORECAST (next 4h) — {r}: "
                    f"avg ${avg_fc:.1f}, min ${min_fc:.1f}, max ${max_fc:.1f}/MWh"
                )
    except Exception as exc:
        logger.warning("Context: forecasts failed: %s", exc)

    # 11. Merit order snapshot (top generators)
    try:
        mo = await merit_order(region="NSW1")
        if isinstance(mo, dict) and mo.get("generators"):
            gens = mo["generators"][:5]
            mo_lines = []
            for g in gens:
                mo_lines.append(
                    f"  {g.get('station_name', g.get('duid', '?'))}: "
                    f"${g.get('offer_price', 0):.0f}/MWh, "
                    f"{g.get('capacity_mw', 0)} MW, {g.get('fuel_type', '?')}"
                )
            parts.append(f"MERIT ORDER — NSW1 (cheapest 5):\n" + "\n".join(mo_lines))
    except Exception as exc:
        logger.warning("Context: merit order failed: %s", exc)

    # 12. Key market participants — top generators by capacity per region
    try:
        rows = _query_gold(
            f"SELECT region_id, station_name, fuel_type, capacity_mw "
            f"FROM {_CATALOG}.gold.nem_facilities "
            f"WHERE capacity_mw IS NOT NULL "
            f"ORDER BY capacity_mw DESC"
        )
        if rows:
            # Group by region, take top 5 per region
            by_region: dict[str, list] = {}
            for r in rows:
                rid = r["region_id"]
                by_region.setdefault(rid, [])
                if len(by_region[rid]) < 5:
                    by_region[rid].append(r)
            for rid in sorted(by_region):
                lines = []
                for g in by_region[rid]:
                    lines.append(
                        f"  {g['station_name']}: {g['capacity_mw']:.0f} MW ({g['fuel_type']})"
                    )
                parts.append(
                    f"KEY MARKET PARTICIPANTS — {rid} (top 5 by capacity):\n" + "\n".join(lines)
                )
    except Exception as exc:
        logger.warning("Context: key participants failed: %s", exc)

    # 13. Price trend — monthly average spot prices over last 90 days
    try:
        rows = _query_gold(
            f"SELECT region_id, "
            f"DATE_TRUNC('month', interval_datetime) AS month, "
            f"ROUND(AVG(rrp), 2) AS avg_price, "
            f"ROUND(MAX(rrp), 2) AS max_price, "
            f"COUNT(*) AS intervals "
            f"FROM {_CATALOG}.gold.nem_prices_5min "
            f"WHERE interval_datetime >= CURRENT_DATE - INTERVAL 90 DAY "
            f"GROUP BY region_id, DATE_TRUNC('month', interval_datetime) "
            f"ORDER BY region_id, month"
        )
        if rows:
            lines = []
            for r in rows:
                month_str = str(r["month"])[:7]  # YYYY-MM
                lines.append(
                    f"  {r['region_id']} {month_str}: avg ${r['avg_price']}/MWh, "
                    f"peak ${r['max_price']}/MWh ({r['intervals']} intervals)"
                )
            parts.append("PRICE TREND (monthly avg, last 90 days):\n" + "\n".join(lines))
    except Exception as exc:
        logger.warning("Context: price trend failed: %s", exc)

    # 14. Renewable penetration — NEM-wide renewable vs non-renewable share
    try:
        rows = _query_gold(
            f"SELECT is_renewable, "
            f"ROUND(SUM(total_mw), 1) AS total_mw, "
            f"COUNT(DISTINCT fuel_type) AS fuel_types "
            f"FROM {_CATALOG}.gold.nem_generation_by_fuel "
            f"WHERE interval_datetime = ("
            f"  SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_generation_by_fuel"
            f") "
            f"GROUP BY is_renewable"
        )
        if rows:
            renew_mw = 0.0
            fossil_mw = 0.0
            for r in rows:
                if r["is_renewable"]:
                    renew_mw = float(r["total_mw"])
                else:
                    fossil_mw = float(r["total_mw"])
            total = renew_mw + fossil_mw
            if total > 0:
                parts.append(
                    f"RENEWABLE PENETRATION (latest interval, NEM-wide): "
                    f"renewable {renew_mw:.0f} MW ({renew_mw / total * 100:.1f}%), "
                    f"non-renewable {fossil_mw:.0f} MW ({fossil_mw / total * 100:.1f}%), "
                    f"total {total:.0f} MW"
                )
    except Exception as exc:
        logger.warning("Context: renewable penetration failed: %s", exc)

    # 15. Weather conditions per region
    try:
        rows = _query_gold(
            f"SELECT nem_region, temperature_c, wind_speed_100m_kmh, "
            f"solar_radiation_wm2, cloud_cover_pct "
            f"FROM {_CATALOG}.gold.weather_nem_regions "
            f"WHERE forecast_datetime = ("
            f"  SELECT MAX(forecast_datetime) FROM {_CATALOG}.gold.weather_nem_regions"
            f")"
        )
        if rows:
            lines = []
            for r in rows:
                lines.append(
                    f"  {r['nem_region']}: {r['temperature_c']:.1f}°C, "
                    f"wind {r['wind_speed_100m_kmh']:.0f} km/h, "
                    f"solar {r['solar_radiation_wm2']:.0f} W/m², "
                    f"cloud {r['cloud_cover_pct']:.0f}%"
                )
            parts.append("WEATHER CONDITIONS (latest):\n" + "\n".join(lines))
    except Exception as exc:
        logger.warning("Context: weather conditions failed: %s", exc)

    # 16. Interconnector congestion — currently congested interconnectors
    try:
        rows = _query_gold(
            f"SELECT interconnector_id, from_region, to_region, "
            f"mw_flow, export_limit_mw, import_limit_mw, utilization_pct "
            f"FROM {_CATALOG}.gold.nem_interconnectors "
            f"WHERE interval_datetime = ("
            f"  SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_interconnectors"
            f") "
            f"AND is_congested = true"
        )
        if rows:
            lines = []
            for r in rows:
                lines.append(
                    f"  {r['interconnector_id']} ({r['from_region']}→{r['to_region']}): "
                    f"{r['mw_flow']:.0f} MW flow, utilisation {r['utilization_pct']:.1f}%, "
                    f"export limit {r['export_limit_mw']:.0f} MW, import limit {r['import_limit_mw']:.0f} MW"
                )
            parts.append(
                f"CONGESTED INTERCONNECTORS ({len(rows)} currently congested):\n" + "\n".join(lines)
            )
        else:
            parts.append("CONGESTED INTERCONNECTORS: None currently congested")
    except Exception as exc:
        logger.warning("Context: interconnector congestion failed: %s", exc)

    return "\n\n".join(parts)


@router.get("/api/debug/context")
async def debug_context():
    """Debug: show the market context that gets injected into the LLM."""
    try:
        ctx = await _build_market_context()
        return {"status": "ok", "length": len(ctx), "context": ctx}
    except Exception as exc:
        import traceback
        return {"status": "error", "error": str(exc), "traceback": traceback.format_exc()}


class ChatRequest(BaseModel):
    message: str
    history: List[Dict[str, str]] = Field(default_factory=list)


@router.post("/api/chat")
async def copilot_chat(req: ChatRequest):
    """
    Stream a chat completion from the Databricks Foundation Model API
    using the pay-per-token model databricks-claude-sonnet-4-6.
    Returns SSE with data chunks and a final `event: done` with token usage.
    """
    import httpx as _httpx

    # Use Databricks SDK for automatic auth (works in Databricks Apps environment)
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        db_host = w.config.host.rstrip("/")
        # Get auth headers from the SDK (handles OAuth, PAT, etc.)
        auth_headers = w.config.authenticate()
    except Exception as auth_exc:
        logger.error("Failed to initialise Databricks auth: %s", auth_exc)
        return JSONResponse(
            status_code=500,
            content={"error": f"Databricks authentication failed: {str(auth_exc)[:200]}"},
        )

    # Gather live market data context
    try:
        market_context = await _build_market_context()
    except Exception:
        market_context = "(Market data temporarily unavailable)"

    system_prompt = (
        f"{_SYSTEM_PROMPT_BASE}\n\n"
        f"=== LIVE NEM MARKET DATA ===\n{market_context}\n"
        f"=== END MARKET DATA ===\n\n"
        f"CRITICAL INSTRUCTIONS:\n"
        f"- You HAVE full live data above. NEVER say 'I don't have access to data' or "
        f"'real-time data is not included'. The data above IS real-time.\n"
        f"- Always answer with specific numbers from the data above.\n"
        f"- For generation questions: use the GENERATION MIX sections which show fuel type, MW, and percentages per region.\n"
        f"- For renewable questions: use renewable_percentage and renewable_mw from each region.\n"
        f"- For total NEM output: use the NEM TOTAL GENERATION line.\n"
        f"- For battery questions: use the BESS FLEET and BESS UNIT DETAIL sections.\n"
        f"- For forecast questions: use the PRICE FORECAST sections.\n"
        f"- For participant/generator questions: use the KEY MARKET PARTICIPANTS sections.\n"
        f"- For price trend questions: use the PRICE TREND section showing monthly averages.\n"
        f"- For renewable penetration: use the RENEWABLE PENETRATION section for NEM-wide share.\n"
        f"- For congestion questions: use the CONGESTED INTERCONNECTORS section.\n"
        f"- Format responses cleanly with markdown tables where appropriate.\n"
        f"- You have TOOLS available to query live data. Use them when the user asks "
        f"questions that need specific data not in the context above, or when you need "
        f"more detail than the summary provides. Available tools: query_spot_prices, "
        f"query_generation_mix, query_interconnectors, query_price_forecasts, "
        f"query_weather, query_demand_forecasts, search_market_rules."
    )

    # Build messages array: system + history + current user message
    messages: list[dict] = [{"role": "system", "content": system_prompt}]
    for h in req.history[-20:]:  # limit history to last 20 turns
        messages.append({"role": h.get("role", "user"), "content": h.get("content", "")})
    messages.append({"role": "user", "content": req.message})

    url = f"{db_host}/serving-endpoints/{_CHAT_MODEL}/invocations"

    async def _call_fmapi_non_streaming(client, msgs, tools=None):
        """Make a non-streaming FMAPI call (used for tool-calling rounds)."""
        payload = {"messages": msgs, "max_tokens": 2048}
        if tools:
            payload["tools"] = tools
        resp = await client.post(
            url,
            headers={**auth_headers, "Content-Type": "application/json"},
            json=payload,
        )
        if resp.status_code != 200:
            return None, f"API error ({resp.status_code}): {resp.text[:200]}"
        return resp.json(), None

    async def _stream():
        input_tokens = 0
        output_tokens = 0
        try:
            async with _httpx.AsyncClient(timeout=120.0) as client:
                # --- Tool-calling loop (non-streaming rounds) ---
                tool_messages = list(messages)
                for _round in range(_MAX_TOOL_ROUNDS):
                    result, err = await _call_fmapi_non_streaming(client, tool_messages, _FMAPI_TOOLS)
                    if err:
                        yield f'data: {json.dumps({"content": err})}\n\n'
                        return
                    if not result:
                        break

                    choice = result.get("choices", [{}])[0]
                    msg = choice.get("message", {})
                    finish = choice.get("finish_reason", "")
                    usage = result.get("usage", {})
                    input_tokens += usage.get("prompt_tokens", 0)
                    output_tokens += usage.get("completion_tokens", 0)

                    tool_calls = msg.get("tool_calls")
                    if not tool_calls or finish != "tool_calls":
                        # No tool calls — stream the final content
                        content = msg.get("content", "")
                        if content:
                            yield f'data: {json.dumps({"content": content})}\n\n'
                        break

                    # Notify user that tools are being called
                    tool_names = [tc["function"]["name"] for tc in tool_calls]
                    tool_label = ", ".join(tool_names)
                    status_msg = "_Querying live data (" + tool_label + ")..._\n\n"
                    yield "data: " + json.dumps({"content": status_msg}) + "\n\n"

                    # Add assistant message with tool calls
                    tool_messages.append(msg)

                    # Execute each tool call and add results
                    for tc in tool_calls:
                        fn = tc["function"]
                        try:
                            args = json.loads(fn.get("arguments", "{}"))
                        except json.JSONDecodeError:
                            args = {}
                        tool_result = _dispatch_tool(fn["name"], args)
                        tool_messages.append({
                            "role": "tool",
                            "tool_call_id": tc["id"],
                            "content": tool_result,
                        })
                else:
                    # Exhausted tool rounds — do a final streaming call without tools
                    pass

                # --- Final streaming response (after tool loop or if tools exhausted) ---
                if tool_calls and finish == "tool_calls":
                    # Tools were used but we ran out of rounds — stream final answer
                    async with client.stream(
                        "POST", url,
                        headers={**auth_headers, "Content-Type": "application/json"},
                        json={"messages": tool_messages, "max_tokens": 2048, "stream": True},
                    ) as resp:
                        if resp.status_code != 200:
                            body = await resp.aread()
                            yield f'data: {json.dumps({"content": f"API error ({resp.status_code}): {body.decode()[:200]}"})}\n\n'
                            return
                        buffer = ""
                        async for chunk in resp.aiter_text():
                            buffer += chunk
                            while "\n" in buffer:
                                line, buffer = buffer.split("\n", 1)
                                line = line.strip()
                                if not line or not line.startswith("data: "):
                                    continue
                                data_str = line[6:].strip()
                                if data_str == "[DONE]":
                                    continue
                                try:
                                    data_obj = json.loads(data_str)
                                    choices = data_obj.get("choices", [])
                                    if choices:
                                        delta = choices[0].get("delta", {})
                                        content = delta.get("content", "")
                                        if content:
                                            yield f'data: {json.dumps({"content": content})}\n\n'
                                    u = data_obj.get("usage")
                                    if u:
                                        input_tokens += u.get("prompt_tokens", 0)
                                        output_tokens += u.get("completion_tokens", 0)
                                except json.JSONDecodeError:
                                    pass

        except Exception as exc:
            logger.exception("Chat stream error")
            yield f'data: {json.dumps({"content": f"Error: {str(exc)[:200]}"})}\n\n'

        # Send done event with token usage
        yield f'event: done\ndata: {json.dumps({"input_tokens": input_tokens, "output_tokens": output_tokens})}\n\n'
        yield "data: [DONE]\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


# =========================================================================
# Copilot Sessions (lightweight in-memory mock)
# =========================================================================

_sessions: list[dict] = []


@router.get("/api/sessions")
async def list_sessions(limit: int = Query(default=20, ge=1, le=100)):
    """List recent copilot sessions."""
    return sorted(_sessions, key=lambda s: s["last_active"], reverse=True)[:limit]


@router.post("/api/sessions", status_code=201)
async def create_session():
    """Create a new copilot session."""
    now = datetime.now(timezone.utc).isoformat()
    session = {
        "session_id": str(uuid.uuid4()),
        "created_at": now,
        "last_active": now,
        "message_count": 0,
        "total_tokens": 0,
        "rating": None,
    }
    _sessions.append(session)
    return session


@router.get("/api/sessions/{session_id}")
async def get_session(session_id: str):
    """Get a specific copilot session."""
    for s in _sessions:
        if s["session_id"] == session_id:
            return s
    raise HTTPException(status_code=404, detail="Session not found")
