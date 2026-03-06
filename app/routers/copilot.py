from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException

from .shared import _NEM_REGIONS, _AEST, _CATALOG, _query_gold, logger
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
]

# ---------------------------------------------------------------------------
# Vector Search client (lazy-loaded)
# ---------------------------------------------------------------------------
_vs_index = None
_VS_ENDPOINT = "energy-copilot-vs-endpoint"
_VS_INDEX_NAME = "energy_copilot_catalog.gold.aemo_docs_vs_index"


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
