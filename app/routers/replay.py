"""Market Replay Router — time-travel through historical NEM states."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from fastapi import APIRouter, Query

from .shared import _CATALOG, _no_real_data, _query_gold, _sql_escape, logger

router = APIRouter()


def _build_snapshot(timestamp_str: str, region: str) -> Dict[str, Any]:
    """Build a synchronized market snapshot at a specific timestamp.

    Queries prices (all 5 regions), generation by fuel, interconnectors,
    demand, and weather at the nearest 5-minute interval.
    """
    ts = timestamp_str.replace("T", " ")[:19]
    snapshot: Dict[str, Any] = {"timestamp": timestamp_str, "region": region}

    # 1. Prices (all 5 regions)
    price_rows = _query_gold(
        f"SELECT region_id, rrp, total_demand_mw, available_gen_mw, interval_datetime "
        f"FROM {_CATALOG}.gold.nem_prices_5min "
        f"WHERE interval_datetime BETWEEN TIMESTAMP('{ts}') - INTERVAL 3 MINUTES "
        f"AND TIMESTAMP('{ts}') + INTERVAL 3 MINUTES "
        f"ORDER BY interval_datetime DESC"
    )
    if price_rows:
        prices = {}
        for r in price_rows:
            rid = r["region_id"]
            if rid not in prices:
                prices[rid] = {
                    "region": rid,
                    "rrp": float(r.get("rrp", 0)),
                    "demand_mw": float(r.get("total_demand_mw", 0)),
                    "available_gen_mw": float(r.get("available_gen_mw", 0)),
                }
        snapshot["prices"] = prices
    else:
        snapshot["prices"] = {}

    # 2. Generation by fuel (for the selected region)
    _RENEWABLE_FUELS = {"wind", "solar_utility", "solar_rooftop", "hydro", "battery"}
    gen_rows = _query_gold(
        f"SELECT fuel_type, total_mw "
        f"FROM {_CATALOG}.gold.nem_generation_by_fuel "
        f"WHERE region_id = '{_sql_escape(region)}' "
        f"AND interval_datetime BETWEEN TIMESTAMP('{ts}') - INTERVAL 3 MINUTES "
        f"AND TIMESTAMP('{ts}') + INTERVAL 3 MINUTES "
        f"ORDER BY total_mw DESC"
    )
    if gen_rows:
        # Deduplicate by fuel_type (take first = highest mw due to ORDER BY)
        seen_fuels = set()
        deduped = []
        for r in gen_rows:
            ft = r.get("fuel_type", "")
            if ft not in seen_fuels:
                seen_fuels.add(ft)
                deduped.append(r)
        snapshot["generation"] = [
            {
                "fuel_type": r.get("fuel_type", ""),
                "mw": float(r.get("total_mw", 0)),
                "is_renewable": r.get("fuel_type", "").lower() in _RENEWABLE_FUELS,
            }
            for r in deduped
        ]
    else:
        snapshot["generation"] = []

    # 3. Interconnectors
    ic_rows = _query_gold(
        f"SELECT interconnector_id, from_region, to_region, mw_flow, "
        f"export_limit_mw, import_limit_mw, is_congested "
        f"FROM {_CATALOG}.gold.nem_interconnectors "
        f"WHERE interval_datetime BETWEEN TIMESTAMP('{ts}') - INTERVAL 3 MINUTES "
        f"AND TIMESTAMP('{ts}') + INTERVAL 3 MINUTES "
        f"ORDER BY interconnector_id"
    )
    if ic_rows:
        seen = set()
        ics = []
        for r in ic_rows:
            icid = r["interconnector_id"]
            if icid not in seen:
                seen.add(icid)
                ics.append({
                    "id": icid,
                    "from_region": r.get("from_region", ""),
                    "to_region": r.get("to_region", ""),
                    "flow_mw": float(r.get("mw_flow", 0)),
                    "limit_mw": float(r.get("export_limit_mw", 0)),
                    "congested": bool(r.get("is_congested")),
                })
        snapshot["interconnectors"] = ics
    else:
        snapshot["interconnectors"] = []

    # 4. Weather
    weather_rows = _query_gold(
        f"SELECT temperature_c, wind_speed_100m_kmh, solar_radiation_wm2 "
        f"FROM {_CATALOG}.gold.weather_nem_regions "
        f"WHERE nem_region = '{_sql_escape(region)}' "
        f"AND forecast_datetime BETWEEN TIMESTAMP('{ts}') - INTERVAL 30 MINUTES "
        f"AND TIMESTAMP('{ts}') + INTERVAL 30 MINUTES "
        f"ORDER BY ABS(TIMESTAMPDIFF(SECOND, forecast_datetime, TIMESTAMP('{ts}'))) LIMIT 1"
    )
    if weather_rows:
        w = weather_rows[0]
        snapshot["weather"] = {
            "temperature_c": float(w.get("temperature_c", 0)),
            "wind_kmh": float(w.get("wind_speed_100m_kmh", 0)),
            "solar_wm2": float(w.get("solar_radiation_wm2", 0)),
        }
    else:
        snapshot["weather"] = {}

    return snapshot


@router.get("/api/replay/snapshot", summary="Market snapshot at timestamp", tags=["Market Replay"])
async def replay_snapshot(
    timestamp: str = Query(""),
    region: str = Query("NSW1"),
):
    """Return synchronized market state at a specific timestamp."""
    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat()

    try:
        result = _build_snapshot(timestamp, region)
        if result.get("prices"):
            return result
    except Exception as exc:
        logger.warning("Replay snapshot failed: %s", exc)

    _no_real_data("replay snapshot")


@router.get("/api/replay/range", summary="Market replay range", tags=["Market Replay"])
async def replay_range(
    start: str = Query(""),
    end: str = Query(""),
    region: str = Query("NSW1"),
    step_minutes: int = Query(5),
):
    """Return array of snapshots for playback (max 288 = 24h at 5min)."""
    now = datetime.now(timezone.utc)
    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00")) if start else now - timedelta(hours=24)
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else now
    except Exception:
        start_dt = now - timedelta(hours=24)
        end_dt = now

    step = max(5, min(step_minutes, 60))
    max_snapshots = 288
    total_minutes = (end_dt - start_dt).total_seconds() / 60
    if total_minutes / step > max_snapshots:
        step = int(total_minutes / max_snapshots) + 1

    # Try real data first — query batch of prices + generation + interconnectors
    _RENEWABLE_FUELS = {"wind", "solar_utility", "solar_rooftop", "hydro", "battery"}
    try:
        ts_start = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        ts_end = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        price_rows = _query_gold(
            f"SELECT region_id, rrp, total_demand_mw, available_gen_mw, interval_datetime "
            f"FROM {_CATALOG}.gold.nem_prices_5min "
            f"WHERE interval_datetime BETWEEN '{ts_start}' AND '{ts_end}' "
            f"ORDER BY interval_datetime"
        )
        if price_rows and len(price_rows) > 10:
            # Group prices by rounded timestamp
            by_ts: Dict[str, Dict] = {}
            for r in price_rows:
                ts_key = str(r["interval_datetime"])[:16]
                if ts_key not in by_ts:
                    by_ts[ts_key] = {"timestamp": str(r["interval_datetime"]), "region": region, "prices": {}, "generation": [], "interconnectors": []}
                by_ts[ts_key]["prices"][r["region_id"]] = {
                    "region": r["region_id"],
                    "rrp": float(r.get("rrp", 0)),
                    "demand_mw": float(r.get("total_demand_mw", 0)),
                    "available_gen_mw": float(r.get("available_gen_mw", 0)),
                }

            # Batch-query generation for the focus region
            try:
                gen_rows = _query_gold(
                    f"SELECT fuel_type, total_mw, interval_datetime "
                    f"FROM {_CATALOG}.gold.nem_generation_by_fuel "
                    f"WHERE region_id = '{_sql_escape(region)}' "
                    f"AND interval_datetime BETWEEN '{ts_start}' AND '{ts_end}' "
                    f"ORDER BY interval_datetime, total_mw DESC"
                )
                if gen_rows:
                    for r in gen_rows:
                        ts_key = str(r["interval_datetime"])[:16]
                        if ts_key in by_ts:
                            by_ts[ts_key]["generation"].append({
                                "fuel_type": r.get("fuel_type", ""),
                                "mw": float(r.get("total_mw", 0)),
                                "is_renewable": r.get("fuel_type", "").lower() in _RENEWABLE_FUELS,
                            })
            except Exception:
                pass

            # Batch-query interconnectors
            try:
                ic_rows = _query_gold(
                    f"SELECT interconnector_id, from_region, to_region, mw_flow, export_limit_mw, is_congested, interval_datetime "
                    f"FROM {_CATALOG}.gold.nem_interconnectors "
                    f"WHERE interval_datetime BETWEEN '{ts_start}' AND '{ts_end}' "
                    f"ORDER BY interval_datetime, interconnector_id"
                )
                if ic_rows:
                    for r in ic_rows:
                        ts_key = str(r["interval_datetime"])[:16]
                        if ts_key in by_ts:
                            by_ts[ts_key]["interconnectors"].append({
                                "id": r.get("interconnector_id", ""),
                                "from_region": r.get("from_region", ""),
                                "to_region": r.get("to_region", ""),
                                "flow_mw": float(r.get("mw_flow", 0)),
                                "limit_mw": float(r.get("export_limit_mw", 0)),
                                "congested": bool(r.get("is_congested")),
                            })
            except Exception:
                pass

            # Deduplicate interconnectors per snapshot (keep first per ID)
            for snap in by_ts.values():
                seen_ids = set()
                deduped_ics = []
                for ic in snap["interconnectors"]:
                    if ic["id"] not in seen_ids:
                        seen_ids.add(ic["id"])
                        deduped_ics.append(ic)
                snap["interconnectors"] = deduped_ics

            # Batch-query weather for the focus region (hourly → map to 5-min snapshots)
            try:
                weather_rows = _query_gold(
                    f"SELECT forecast_datetime, temperature_c, wind_speed_100m_kmh, solar_radiation_wm2 "
                    f"FROM {_CATALOG}.gold.weather_nem_regions "
                    f"WHERE nem_region = '{_sql_escape(region)}' "
                    f"AND forecast_datetime BETWEEN '{ts_start}' AND '{ts_end}' "
                    f"ORDER BY forecast_datetime"
                )
                if weather_rows:
                    # Build hour→weather lookup
                    wx_by_hour: Dict[str, Dict] = {}
                    for w in weather_rows:
                        hr_key = str(w["forecast_datetime"])[:13]  # "YYYY-MM-DD HH"
                        wx_by_hour[hr_key] = {
                            "temperature_c": float(w.get("temperature_c") or 0),
                            "wind_kmh": float(w.get("wind_speed_100m_kmh") or 0),
                            "solar_wm2": float(w.get("solar_radiation_wm2") or 0),
                        }
                    # Map weather to snapshots by hour prefix
                    for snap in by_ts.values():
                        ts_str = snap.get("timestamp", "")
                        hr = ts_str[:13]
                        if hr in wx_by_hour:
                            snap["weather"] = wx_by_hour[hr]
            except Exception:
                pass

            snapshots = list(by_ts.values())
            # Thin to step interval
            if step > 5 and len(snapshots) > max_snapshots:
                stride = max(1, len(snapshots) * 5 // step)
                snapshots = snapshots[::stride]
            return {"snapshots": snapshots[:max_snapshots], "count": len(snapshots[:max_snapshots]), "step_minutes": step}
    except Exception as exc:
        logger.warning("Replay range real data failed: %s", exc)

    _no_real_data("replay range")


