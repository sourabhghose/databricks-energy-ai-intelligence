from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException

from .shared import _NEM_REGIONS, _AEST, logger
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
        f"- Format responses cleanly with markdown tables where appropriate."
    )

    # Build messages array: system + history + current user message
    messages: list[dict] = [{"role": "system", "content": system_prompt}]
    for h in req.history[-20:]:  # limit history to last 20 turns
        messages.append({"role": h.get("role", "user"), "content": h.get("content", "")})
    messages.append({"role": "user", "content": req.message})

    url = f"{db_host}/serving-endpoints/{_CHAT_MODEL}/invocations"

    async def _stream():
        input_tokens = 0
        output_tokens = 0
        try:
            async with _httpx.AsyncClient(timeout=120.0) as client:
                async with client.stream(
                    "POST",
                    url,
                    headers={
                        **auth_headers,
                        "Content-Type": "application/json",
                    },
                    json={
                        "messages": messages,
                        "max_tokens": 2048,
                        "stream": True,
                    },
                ) as resp:
                    if resp.status_code != 200:
                        body = await resp.aread()
                        err_text = body.decode("utf-8", errors="replace")
                        yield f'data: {json.dumps({"content": f"API error ({resp.status_code}): {err_text[:200]}"})}\n\n'
                        return

                    buffer = ""
                    async for chunk in resp.aiter_text():
                        buffer += chunk
                        while "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            line = line.strip()
                            if not line:
                                continue
                            if line.startswith("data: "):
                                data_str = line[6:].strip()
                                if data_str == "[DONE]":
                                    continue
                                try:
                                    data_obj = json.loads(data_str)
                                    # Extract token from streaming chunk
                                    choices = data_obj.get("choices", [])
                                    if choices:
                                        delta = choices[0].get("delta", {})
                                        content = delta.get("content", "")
                                        if content:
                                            yield f'data: {json.dumps({"content": content})}\n\n'
                                    # Capture usage if present (often in last chunk)
                                    usage = data_obj.get("usage")
                                    if usage:
                                        input_tokens = usage.get("prompt_tokens", 0)
                                        output_tokens = usage.get("completion_tokens", 0)
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
