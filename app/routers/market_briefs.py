"""Market Briefs Router — scheduled and on-demand market intelligence briefs."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query

from .shared import _CATALOG, _NEM_REGIONS, _insert_gold, _query_gold, _sql_escape, logger

router = APIRouter()

# ---------------------------------------------------------------------------
# Core brief generation engine
# ---------------------------------------------------------------------------


_RENEWABLE_FUELS = {"wind", "solar_utility", "solar_rooftop", "hydro", "battery"}


def _generate_brief_core(brief_type: str = "daily", regions: Optional[List[str]] = None) -> Dict[str, Any]:
    """Generate a market intelligence brief from live data.

    Queries:
    1. Price stats (24h or 168h for weekly)
    2. Recent anomaly events
    3. Renewable share
    4. Interconnector congestion
    5. Weather
    Builds a markdown narrative and persists to gold.market_briefs.
    """
    now = datetime.now(timezone.utc)
    sections: List[str] = []
    key_metrics: Dict[str, Any] = {}

    lookback_hours = 168 if brief_type == "weekly" else 24
    region_filter = ""
    if regions:
        quoted = ", ".join(f"'{_sql_escape(r)}'" for r in regions)
        region_filter = f"AND region_id IN ({quoted})"

    # Anchor to latest available data so the brief works even when the
    # simulator is temporarily stale (avoids "Price data unavailable").
    anchor_rows = _query_gold(
        f"SELECT MAX(interval_datetime) AS latest FROM {_CATALOG}.gold.nem_prices_5min"
    )
    anchor_ts = anchor_rows[0]["latest"] if anchor_rows and anchor_rows[0].get("latest") else None
    if anchor_ts:
        time_filter = (
            f"interval_datetime >= TIMESTAMP('{anchor_ts}') - INTERVAL {lookback_hours} HOURS"
            f" AND interval_datetime <= TIMESTAMP('{anchor_ts}')"
        )
        ic_time_filter = (
            f"interval_datetime >= TIMESTAMP('{anchor_ts}') - INTERVAL {lookback_hours} HOURS"
            f" AND interval_datetime <= TIMESTAMP('{anchor_ts}')"
        )
    else:
        time_filter = f"interval_datetime >= current_timestamp() - INTERVAL {lookback_hours} HOURS"
        ic_time_filter = f"interval_datetime >= current_timestamp() - INTERVAL {lookback_hours} HOURS"

    # 1. Price stats
    price_stats = _query_gold(
        f"SELECT region_id, "
        f"ROUND(AVG(rrp), 2) AS avg_price, "
        f"ROUND(MAX(rrp), 2) AS max_price, "
        f"ROUND(MIN(rrp), 2) AS min_price, "
        f"COUNT(*) AS intervals, "
        f"SUM(CASE WHEN rrp > 300 THEN 1 ELSE 0 END) AS spike_intervals, "
        f"SUM(CASE WHEN rrp < 0 THEN 1 ELSE 0 END) AS negative_intervals "
        f"FROM {_CATALOG}.gold.nem_prices_5min "
        f"WHERE {time_filter} "
        f"{region_filter} "
        f"GROUP BY region_id ORDER BY region_id"
    )

    if price_stats:
        sections.append("## Overnight Summary")
        lines = []
        for r in price_stats:
            region = r["region_id"]
            avg_p = float(r.get("avg_price", 0))
            max_p = float(r.get("max_price", 0))
            min_p = float(r.get("min_price", 0))
            spikes = int(r.get("spike_intervals", 0))
            negatives = int(r.get("negative_intervals", 0))

            status = "stable"
            if spikes > 5:
                status = "volatile"
            elif negatives > 10:
                status = "oversupplied"

            lines.append(
                f"- **{region}**: avg ${avg_p:.2f}/MWh, "
                f"range [${min_p:.2f}, ${max_p:.2f}], "
                f"{spikes} spike intervals, {negatives} negative — *{status}*"
            )

        sections.append("\n".join(lines))
        key_metrics["price_stats"] = [
            {
                "region": r["region_id"],
                "avg": float(r.get("avg_price", 0)),
                "max": float(r.get("max_price", 0)),
                "min": float(r.get("min_price", 0)),
            }
            for r in price_stats
        ]
    else:
        sections.append("## Overnight Summary\n*Price data unavailable*")

    # 2. Anomaly events
    anomaly_rows = _query_gold(
        f"SELECT region_id, rrp, interval_datetime "
        f"FROM {_CATALOG}.gold.nem_prices_5min "
        f"WHERE {time_filter} "
        f"{region_filter} "
        f"AND (rrp > 500 OR rrp < -50) "
        f"ORDER BY ABS(rrp) DESC LIMIT 10"
    )
    if anomaly_rows:
        sections.append("\n## Key Events")
        for r in anomaly_rows[:5]:
            price = float(r.get("rrp", 0))
            ts = str(r.get("interval_datetime", ""))[:16]
            region = r.get("region_id", "?")
            if price > 500:
                sections.append(f"- **Price spike** ${price:,.0f}/MWh in {region} at {ts}")
            else:
                sections.append(f"- **Negative price** ${price:,.2f}/MWh in {region} at {ts}")
        key_metrics["key_events"] = len(anomaly_rows)
    else:
        sections.append("\n## Key Events\n*No significant price events in last 24h*")
        key_metrics["key_events"] = 0

    # 3. Renewable share (derive from fuel_type — no is_renewable column)
    gen_rows = _query_gold(
        f"SELECT fuel_type, ROUND(SUM(total_mw), 1) AS total_mw "
        f"FROM {_CATALOG}.gold.nem_generation_by_fuel "
        f"WHERE interval_datetime = ("
        f"  SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_generation_by_fuel"
        f") GROUP BY fuel_type"
    )
    if gen_rows:
        renew_mw = sum(float(r["total_mw"]) for r in gen_rows if str(r.get("fuel_type", "")).lower() in _RENEWABLE_FUELS)
        total_mw = sum(float(r["total_mw"]) for r in gen_rows)
        pct = (renew_mw / total_mw * 100) if total_mw > 0 else 0
        sections.append(
            f"\n## Today's Outlook\n"
            f"- NEM renewable share: **{pct:.1f}%** ({renew_mw:.0f} MW of {total_mw:.0f} MW)"
        )
        key_metrics["renewable_pct"] = round(pct, 1)
    else:
        sections.append("\n## Today's Outlook\n*Generation data unavailable*")

    # 4. Congestion — aggregate over the full lookback window (not just latest snapshot)
    congested = _query_gold(
        f"SELECT interconnector_id, from_region, to_region, "
        f"ROUND(AVG(utilization_pct), 1) AS avg_util, "
        f"SUM(CASE WHEN is_congested = true THEN 1 ELSE 0 END) AS congested_intervals "
        f"FROM {_CATALOG}.gold.nem_interconnectors "
        f"WHERE {ic_time_filter} "
        f"GROUP BY interconnector_id, from_region, to_region "
        f"HAVING congested_intervals > 0 "
        f"ORDER BY congested_intervals DESC"
    )
    if congested:
        ic_list = ", ".join(
            f"{r['interconnector_id']} ({int(r['congested_intervals'])} intervals, avg {float(r['avg_util']):.0f}%)"
            for r in congested
        )
        sections.append(f"- Congested interconnectors: {ic_list}")
        key_metrics["congested_ics"] = len(congested)
    else:
        sections.append("- No interconnector congestion in period")
        key_metrics["congested_ics"] = 0

    # 5. Weather
    weather_rows = _query_gold(
        f"SELECT nem_region, temperature_c, wind_speed_100m_kmh "
        f"FROM {_CATALOG}.gold.weather_nem_regions "
        f"WHERE forecast_datetime = ("
        f"  SELECT MAX(forecast_datetime) FROM {_CATALOG}.gold.weather_nem_regions"
        f") ORDER BY nem_region"
    )
    if weather_rows:
        sections.append("\n## Watch Items")
        hot_regions = [r for r in weather_rows if float(r.get("temperature_c", 0)) > 35]
        low_wind = [r for r in weather_rows if float(r.get("wind_speed_100m_kmh", 0)) < 10]
        if hot_regions:
            regions_str = ", ".join(f"{r['nem_region']} ({r['temperature_c']:.1f}°C)" for r in hot_regions)
            sections.append(f"- **Heat risk**: {regions_str}")
        if low_wind:
            regions_str = ", ".join(f"{r['nem_region']} ({r['wind_speed_100m_kmh']:.0f}km/h)" for r in low_wind)
            sections.append(f"- **Low wind**: {regions_str}")
        if not hot_regions and not low_wind:
            sections.append("- No significant weather risks")
    else:
        sections.append("\n## Watch Items\n*Weather data unavailable*")

    # Build final narrative
    if brief_type == "weekly":
        week_start = (now - timedelta(days=7)).strftime("%d %b")
        title = f"NEM Weekly Brief — week ending {now.strftime('%d %b %Y')}"
    else:
        title = f"NEM {brief_type.title()} Brief — {now.strftime('%d %b %Y')}"
    narrative = "\n".join(sections)
    word_count = len(narrative.split())

    # Persist (don't pre-escape — _insert_gold handles escaping)
    brief_id = str(uuid.uuid4())
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    _insert_gold(f"{_CATALOG}.gold.market_briefs", {
        "brief_id": brief_id,
        "brief_date": now.strftime("%Y-%m-%d"),
        "brief_type": brief_type,
        "title": title,
        "narrative": narrative,
        "key_metrics": json.dumps(key_metrics),
        "regions": json.dumps(regions or _NEM_REGIONS),
        "model_id": "heuristic-v1",
        "generated_at": now_str,
        "word_count": word_count,
    })

    return {
        "brief_id": brief_id,
        "brief_date": now.strftime("%Y-%m-%d"),
        "brief_type": brief_type,
        "title": title,
        "narrative": narrative,
        "key_metrics": key_metrics,
        "regions": regions or _NEM_REGIONS,
        "generated_at": now_str,
        "word_count": word_count,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/api/market-briefs", summary="List market briefs archive", tags=["Market Briefs"])
async def list_briefs(
    brief_type: str = Query("daily"),
    limit: int = Query(20),
):
    """Return recent market briefs from archive."""
    try:
        rows = _query_gold(
            f"SELECT brief_id, brief_date, brief_type, title, narrative, "
            f"key_metrics, generated_at, word_count "
            f"FROM {_CATALOG}.gold.market_briefs "
            f"WHERE brief_type = '{_sql_escape(brief_type)}' "
            f"ORDER BY generated_at DESC LIMIT {min(limit, 50)}"
        )
        if rows:
            briefs = []
            for r in rows:
                briefs.append({
                    "brief_id": r.get("brief_id", ""),
                    "brief_date": str(r.get("brief_date", "")),
                    "brief_type": r.get("brief_type", ""),
                    "title": r.get("title", ""),
                    "narrative": r.get("narrative", ""),
                    "key_metrics": json.loads(r["key_metrics"]) if r.get("key_metrics") else {},
                    "generated_at": str(r.get("generated_at", "")),
                    "word_count": int(r.get("word_count", 0)),
                })
            return {"briefs": briefs, "count": len(briefs)}
    except Exception as exc:
        logger.warning("List briefs failed: %s", exc)

    # Mock fallback
    now = datetime.now(timezone.utc)
    return {
        "briefs": [
            {
                "brief_id": "mock-1",
                "brief_date": now.strftime("%Y-%m-%d"),
                "brief_type": "daily",
                "title": f"NEM Daily Brief — {now.strftime('%d %b %Y')}",
                "narrative": (
                    "## Overnight Summary\n"
                    "- **NSW1**: avg $72.50/MWh, stable\n"
                    "- **QLD1**: avg $65.30/MWh, stable\n\n"
                    "## Key Events\n*No significant events*\n\n"
                    "## Today's Outlook\nRenewable share ~35%. No congestion.\n\n"
                    "## Watch Items\n- No significant weather risks"
                ),
                "key_metrics": {"key_events": 0, "renewable_pct": 35.0},
                "generated_at": now.isoformat(),
                "word_count": 42,
            }
        ],
        "count": 1,
    }


@router.get("/api/market-briefs/latest", summary="Latest market brief", tags=["Market Briefs"])
async def latest_brief(brief_type: str = Query("daily")):
    """Return the most recent brief.  Auto-generates a fresh brief if the latest is >6 hours old."""
    try:
        rows = _query_gold(
            f"SELECT brief_id, brief_date, brief_type, title, narrative, "
            f"key_metrics, generated_at, word_count "
            f"FROM {_CATALOG}.gold.market_briefs "
            f"WHERE brief_type = '{_sql_escape(brief_type)}' "
            f"ORDER BY generated_at DESC LIMIT 1"
        )
        if rows:
            r = rows[0]
            # Check staleness — auto-regenerate if >6 hours old
            gen_at = r.get("generated_at")
            stale = True
            if gen_at:
                try:
                    ts = str(gen_at).replace("Z", "+00:00").replace("T", " ")
                    # Handle "2026-03-08 06:40:25" and "2026-03-08T06:40:25.000Z"
                    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S"):
                        try:
                            gen_dt = datetime.strptime(ts, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        gen_dt = None
                    if gen_dt:
                        if gen_dt.tzinfo is None:
                            gen_dt = gen_dt.replace(tzinfo=timezone.utc)
                        stale = (datetime.now(timezone.utc) - gen_dt) > timedelta(hours=6)
                except Exception:
                    stale = True
            if stale:
                try:
                    return _generate_brief_core(brief_type)
                except Exception as gen_exc:
                    logger.warning("Auto-regeneration failed, returning stale brief: %s", gen_exc)
            return {
                "brief_id": r.get("brief_id", ""),
                "brief_date": str(r.get("brief_date", "")),
                "brief_type": r.get("brief_type", ""),
                "title": r.get("title", ""),
                "narrative": r.get("narrative", ""),
                "key_metrics": json.loads(r["key_metrics"]) if r.get("key_metrics") else {},
                "generated_at": str(r.get("generated_at", "")),
                "word_count": int(r.get("word_count", 0)),
            }
    except Exception as exc:
        logger.warning("Latest brief failed: %s", exc)

    # No briefs at all — generate the first one
    try:
        return _generate_brief_core(brief_type)
    except Exception as gen_exc:
        logger.warning("Initial brief generation failed: %s", gen_exc)

    result = await list_briefs(brief_type=brief_type, limit=1)
    if result.get("briefs"):
        return result["briefs"][0]
    return {"brief_id": "", "title": "No briefs generated yet", "narrative": "", "word_count": 0}


@router.post("/api/market-briefs/generate", summary="Generate a market brief", tags=["Market Briefs"])
async def generate_brief(
    brief_type: str = Query("daily"),
    regions: Optional[str] = Query(None, description="Comma-separated regions filter"),
):
    """Trigger on-demand brief generation."""
    try:
        region_list = [r.strip() for r in regions.split(",") if r.strip()] if regions else None
        return _generate_brief_core(brief_type, regions=region_list)
    except Exception as exc:
        logger.warning("Brief generation failed: %s", exc)
        return {
            "brief_id": "",
            "title": "Generation failed",
            "narrative": f"Error: {str(exc)[:200]}",
            "word_count": 0,
            "error": True,
        }
