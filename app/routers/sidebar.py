from __future__ import annotations
import json
import math
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Query
from pydantic import BaseModel
from .shared import _NEM_REGIONS, _REGION_BASE_PRICES, _query_gold, _execute_gold, _insert_gold, _update_gold, _sql_escape, _invalidate_cache, _CATALOG, logger

router = APIRouter()

# =========================================================================
# SIDEBAR NAVIGATION API ENDPOINTS
# =========================================================================

# --- 1. BESS Fleet ---
@router.get("/api/bess/fleet", summary="BESS fleet overview", tags=["BESS"])
async def bess_fleet():
    """Return BESS fleet summary matching BessFleetSummary shape."""
    # Try real battery data from NEMWEB
    try:
        batt_rows = _query_gold(f"""
            SELECT f.duid, f.station_name, f.region_id AS region, f.capacity_mw,
                   s.generation_MW AS current_mw
            FROM {_CATALOG}.gold.nem_facilities f
            JOIN (
                SELECT duid, generation_MW, interval,
                       ROW_NUMBER() OVER (PARTITION BY duid ORDER BY interval DESC) AS rn
                FROM {_CATALOG}.nemweb_analytics.silver_nem_dispatch_unit_scada
                WHERE interval >= current_timestamp() - INTERVAL 1 HOUR
            ) s ON f.duid = s.duid AND s.rn = 1
            WHERE LOWER(f.fuel_type) LIKE '%battery%'
               OR LOWER(f.fuel_type) LIKE '%storage%'
            ORDER BY f.capacity_mw DESC
            LIMIT 20
        """)
    except Exception:
        batt_rows = None

    if batt_rows:
        units = []
        for r in batt_rows:
            mw = float(r.get("current_mw") or 0)
            cap = float(r.get("capacity_MW") or 1)
            mode = "discharging" if mw > 1 else ("charging" if mw < -1 else "standby")
            units.append({
                "duid": r.get("duid", ""),
                "station_name": r.get("station_name", ""),
                "region": r.get("region", ""),
                "capacity_mwh": round(cap * 2, 0),  # approximate MWh as 2h duration
                "power_mw": round(cap, 0),
                "soc_pct": round(max(5, min(95, 50 + mw / max(cap, 1) * 30)), 1),
                "mode": mode,
                "current_mw": round(mw, 1),
                "cycles_today": 0,
                "revenue_today_aud": 0,
                "efficiency_pct": 89.0,
            })
        if units:
            discharging = sum(1 for u in units if u["mode"] == "discharging")
            charging = sum(1 for u in units if u["mode"] == "charging")
            idle = sum(1 for u in units if u["mode"] == "standby")
            total_soc = sum(u["soc_pct"] for u in units) / len(units)
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total_capacity_mwh": sum(u["capacity_mwh"] for u in units),
                "total_power_mw": sum(u["power_mw"] for u in units),
                "units_discharging": discharging,
                "units_charging": charging,
                "units_idle": idle,
                "fleet_avg_soc_pct": round(total_soc, 1),
                "fleet_revenue_today_aud": 0,
                "units": units,
            }

    # Mock fallback
    rng = random.Random(int(time.time() // 30))
    base_units = [
        {"duid": "HPRG1", "station_name": "Hornsdale Power Reserve", "region": "SA1", "capacity_mwh": 194, "power_mw": 150, "base_soc": 72.5, "mode": "discharging", "base_current_mw": 120.3, "cycles_today": 2, "revenue_today_aud": 45200, "efficiency_pct": 87.5},
        {"duid": "WRSF1", "station_name": "Waratah Super Battery", "region": "NSW1", "capacity_mwh": 1680, "power_mw": 850, "base_soc": 58.3, "mode": "charging", "base_current_mw": -450.0, "cycles_today": 1, "revenue_today_aud": 89400, "efficiency_pct": 91.2},
        {"duid": "VBBG1", "station_name": "Victorian Big Battery", "region": "VIC1", "capacity_mwh": 450, "power_mw": 300, "base_soc": 85.1, "mode": "standby", "base_current_mw": 0.0, "cycles_today": 3, "revenue_today_aud": 23100, "efficiency_pct": 88.9},
        {"duid": "BSLD1", "station_name": "Bouldercombe Battery", "region": "QLD1", "capacity_mwh": 400, "power_mw": 200, "base_soc": 41.7, "mode": "discharging", "base_current_mw": 180.5, "cycles_today": 2, "revenue_today_aud": 31500, "efficiency_pct": 86.3},
        {"duid": "TLSB1", "station_name": "Torrens Island BESS", "region": "SA1", "capacity_mwh": 500, "power_mw": 250, "base_soc": 63.4, "mode": "charging", "base_current_mw": -200.0, "cycles_today": 1, "revenue_today_aud": 37800, "efficiency_pct": 89.7},
    ]
    units = []
    for u in base_units:
        soc = round(u["base_soc"] + rng.uniform(-3, 3), 1)
        current_mw = round(u["base_current_mw"] + rng.uniform(-5, 5), 1) if u["mode"] != "standby" else 0.0
        units.append({
            "duid": u["duid"], "station_name": u["station_name"], "region": u["region"],
            "capacity_mwh": u["capacity_mwh"], "power_mw": u["power_mw"],
            "soc_pct": soc, "mode": u["mode"], "current_mw": current_mw,
            "cycles_today": u["cycles_today"], "revenue_today_aud": u["revenue_today_aud"],
            "efficiency_pct": u["efficiency_pct"],
        })
    discharging = sum(1 for u in units if u["mode"] == "discharging")
    charging = sum(1 for u in units if u["mode"] == "charging")
    idle = sum(1 for u in units if u["mode"] == "standby")
    total_soc = sum(u["soc_pct"] for u in units) / len(units)
    total_rev = sum(u["revenue_today_aud"] for u in units)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_capacity_mwh": sum(u["capacity_mwh"] for u in units),
        "total_power_mw": sum(u["power_mw"] for u in units),
        "units_discharging": discharging,
        "units_charging": charging,
        "units_idle": idle,
        "fleet_avg_soc_pct": round(total_soc, 1),
        "fleet_revenue_today_aud": total_rev,
        "units": units,
    }


# --- 2. BESS Dispatch ---
@router.get("/api/bess/dispatch", summary="BESS dispatch history", tags=["BESS"])
async def bess_dispatch(
    duid: str = Query("HPRG1"),
    count: int = Query(48),
):
    """Return dispatch intervals matching BessDispatchInterval[] shape."""
    # Try real SCADA dispatch data for this DUID
    try:
        scada_rows = _query_gold(f"""
            SELECT s.interval, s.duid, s.generation_MW,
                   p.rrp
            FROM {_CATALOG}.nemweb_analytics.silver_nem_dispatch_unit_scada s
            LEFT JOIN {_CATALOG}.nemweb_analytics.silver_nem_trading_price p
              ON p.interval = s.interval
              AND p.region_id = (
                  SELECT region_id FROM {_CATALOG}.gold.nem_facilities
                  WHERE duid = '{duid}' LIMIT 1
              )
            WHERE s.duid = '{duid}'
              AND s.interval >= current_timestamp() - INTERVAL 4 HOURS
            ORDER BY s.interval DESC
            LIMIT {min(count, 200)}
        """)
    except Exception:
        scada_rows = None

    if scada_rows:
        points = []
        soc = 50.0  # estimated starting SoC
        for r in reversed(scada_rows):  # oldest first for SoC tracking
            mw = float(r.get("generation_MW") or 0)
            rrp = float(r.get("rrp") or 0)
            soc = max(5, min(95, soc - mw * 5 / 60 / 200 * 100))  # approximate
            revenue = round(abs(mw) * rrp * 5 / 60, 2) if mw > 0 else 0.0
            points.append({
                "interval_datetime": str(r.get("interval", "")),
                "duid": duid,
                "mw": round(mw, 1),
                "soc_pct": round(soc, 1),
                "rrp_at_dispatch": round(rrp, 2),
                "revenue_aud": revenue,
            })
        if points:
            return points

    # Mock fallback
    now = datetime.now(timezone.utc)
    rng = random.Random(hash(duid) + int(now.timestamp() // 30))
    soc = rng.uniform(40, 80)
    points = []
    for i in range(count):
        ts = now - timedelta(minutes=5 * (count - i))
        mw = round(rng.uniform(-150, 150), 1)
        soc = max(5, min(95, soc + mw * 5 / 60 / 194 * 100))
        rrp = round(rng.uniform(30, 180), 2)
        revenue = round(abs(mw) * rrp * 5 / 60, 2) if mw > 0 else 0.0
        points.append({
            "interval_datetime": ts.isoformat(),
            "duid": duid,
            "mw": mw,
            "soc_pct": round(soc, 1),
            "rrp_at_dispatch": rrp,
            "revenue_aud": revenue,
        })
    return points


# --- 3. Weather/Demand ---
@router.get("/api/weather/demand", summary="Weather-adjusted demand", tags=["Weather"])
async def weather_demand(
    region: str = Query("NSW1"),
    hours: int = Query(24),
):
    """Return WeatherDemandPoint[] (flat array, not wrapper object)."""
    # --- real-data block: join weather + demand from gold tables ---
    try:
        rows = _query_gold(f"""
            SELECT w.forecast_datetime AS ts,
                   w.temperature_c,
                   w.wind_speed_100m_kmh AS wind_speed_kmh,
                   w.solar_radiation_wm2,
                   p.total_demand_mw
            FROM {_CATALOG}.gold.weather_nem_regions w
            LEFT JOIN {_CATALOG}.gold.nem_prices_5min p
              ON w.nem_region = p.region_id
              AND ABS(TIMESTAMPDIFF(MINUTE, w.forecast_datetime, p.interval_datetime)) < 30
            WHERE w.nem_region = '{region}'
              AND w.forecast_datetime >= current_timestamp() - INTERVAL {hours} HOURS
            ORDER BY w.forecast_datetime
            LIMIT {hours * 2}
        """)
    except Exception:
        rows = None

    if rows and len(rows) >= 4:
        # Try to get BOM observed temperature for this region
        bom_temp = None
        try:
            bom_rows = _query_gold(f"""
                SELECT temperature_c, observation_time
                FROM {_CATALOG}.nemweb_analytics.gold_bom_observations
                WHERE nem_region = '{region}'
                ORDER BY observation_time DESC
                LIMIT 1
            """)
            if bom_rows:
                bom_temp = float(bom_rows[0].get("temperature_c") or 0)
        except Exception:
            pass

        # Compute baseline as rolling mean for deviation calc
        demands = [float(r.get("total_demand_mw") or 0) for r in rows if r.get("total_demand_mw")]
        baseline = sum(demands) / max(len(demands), 1) if demands else 7000
        points = []
        for r in rows:
            temp = float(r.get("temperature_c") or 22)
            demand = float(r.get("total_demand_mw") or baseline)
            point = {
                "timestamp": str(r["ts"]).replace(" ", "T"),
                "region": region,
                "temperature_c": round(temp, 1),
                "apparent_temp_c": round(temp + 1.5, 1),
                "demand_mw": round(demand, 0),
                "demand_baseline_mw": round(baseline, 0),
                "demand_deviation_mw": round(demand - baseline, 0),
                "wind_speed_kmh": round(float(r.get("wind_speed_kmh") or 15), 1),
                "solar_irradiance_wm2": round(float(r.get("solar_radiation_wm2") or 0), 1),
            }
            if bom_temp is not None:
                point["observed_temperature_c"] = round(bom_temp, 1)
            points.append(point)
        return points

    # Mock fallback
    now = datetime.now(timezone.utc)
    rng = random.Random(hash(region) + int(now.timestamp() // 30))
    base_demand = {"NSW1": 8500, "QLD1": 6200, "VIC1": 5800, "SA1": 1800, "TAS1": 1100}.get(region, 5000)
    baseline_demand = {"NSW1": 8200, "QLD1": 6000, "VIC1": 5600, "SA1": 1700, "TAS1": 1050}.get(region, 4800)
    points = []
    for i in range(hours * 2):  # 30-min intervals
        ts = now - timedelta(minutes=30 * (hours * 2 - i))
        hour = (ts.hour + ts.minute / 60.0)
        daily = math.sin((hour - 6) / 24 * 2 * math.pi) * base_demand * 0.2
        demand = round(base_demand + daily + rng.gauss(0, base_demand * 0.03), 0)
        temp = round(22 + 8 * math.sin((hour - 14) / 24 * 2 * math.pi) + rng.gauss(0, 1), 1)
        apparent = round(temp + rng.uniform(0.5, 2.5), 1)
        baseline = round(baseline_demand + daily * 0.9, 0)
        solar_irr = round(max(0, 800 * math.sin(max(0, (hour - 6) / 12 * math.pi))) + rng.gauss(0, 30), 1) if 6 <= hour <= 18 else 0.0
        wind = round(max(0, 15 + rng.gauss(0, 5)), 1)
        points.append({
            "timestamp": ts.isoformat(),
            "region": region,
            "temperature_c": temp,
            "apparent_temp_c": apparent,
            "demand_mw": demand,
            "demand_baseline_mw": baseline,
            "demand_deviation_mw": round(demand - baseline, 0),
            "wind_speed_kmh": wind,
            "solar_irradiance_wm2": solar_irr,
        })
    return points


# --- 4. Sustainability Dashboard ---
@router.get("/api/sustainability/dashboard", summary="Sustainability metrics", tags=["Sustainability"])
async def sustainability_dashboard(region: str = Query("NSW1")):
    """Return SustainabilityDashboard shape."""
    now = datetime.now(timezone.utc)
    rng = random.Random(hash(region) + int(time.time() // 30))
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

    # Try real generation data for regional renewable %
    gen_rows = _query_gold(f"""
        SELECT region_id, fuel_type, total_mw
        FROM {_CATALOG}.gold.nem_generation_by_fuel
        WHERE interval_datetime = (SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_generation_by_fuel)
    """)
    real_mix = {}  # region -> {fuel: mw}
    real_renewable_pct = {}
    if gen_rows:
        for g in gen_rows:
            rid = g["region_id"]
            ft = str(g["fuel_type"]).lower()
            mw = float(g["total_mw"] or 0)
            if rid not in real_mix:
                real_mix[rid] = {}
            real_mix[rid][ft] = real_mix[rid].get(ft, 0) + mw
        for rid, mix in real_mix.items():
            total = sum(mix.values())
            renew = sum(mw for ft, mw in mix.items() if ft in ("wind", "solar", "hydro", "battery"))
            real_renewable_pct[rid] = round(renew / total * 100, 1) if total > 0 else 0

    regional_intensity = []
    for r in regions:
        if r in real_mix:
            mix = real_mix[r]
            total = sum(mix.values())
            mix_pct = {ft: round(mw / total * 100, 0) for ft, mw in mix.items()} if total > 0 else {}
            ren_pct = real_renewable_pct.get(r, 30.0)
            # Estimate carbon intensity: ~0.9 for coal, ~0.4 for gas, 0 for renewables
            coal_pct = sum(mw for ft, mw in mix.items() if "coal" in ft) / total * 100 if total > 0 else 0
            gas_pct = sum(mw for ft, mw in mix.items() if ft in ("gas", "natural gas")) / total * 100 if total > 0 else 0
            carbon_intensity = round(coal_pct * 9 + gas_pct * 4, 0)
        else:
            ren_pct = round(rng.uniform(18, 65), 0)
            mix_pct = {"coal": 50, "gas": 20, "wind": 15, "solar": 15}
            carbon_intensity = round(rng.uniform(400, 850), 0)
        regional_intensity.append({
            "region": r,
            "timestamp": now.isoformat(),
            "carbon_intensity_kg_co2_mwh": carbon_intensity,
            "renewable_pct": ren_pct,
            "fossil_pct": round(100 - ren_pct, 0),
            "generation_mix": mix_pct if r in real_mix else {"coal": 50, "gas": 20, "wind": 15, "solar": 15},
        })

    nem_ren_pct = round(sum(real_renewable_pct.get(r, 35) for r in regions) / len(regions), 1) if real_renewable_pct else round(rng.uniform(30, 48), 1)

    intensity_history = []
    for i in range(48):
        ts = now - timedelta(minutes=30 * (48 - i))
        hour = ts.hour + ts.minute / 60.0
        solar_factor = max(0, math.sin((hour - 6) / 12 * math.pi)) if 6 <= hour <= 18 else 0
        intensity_history.append({
            "region": "NEM",
            "timestamp": ts.isoformat(),
            "carbon_intensity_kg_co2_mwh": round(max(300, 620 - solar_factor * 200 + rng.gauss(0, 30)), 0),
            "renewable_pct": round(min(80, max(15, 38 + solar_factor * 25 + rng.gauss(0, 3))), 0),
            "fossil_pct": round(min(85, max(20, 62 - solar_factor * 25 + rng.gauss(0, 3))), 0),
            "generation_mix": {"coal": 40, "gas": 15, "wind": 20, "solar": round(solar_factor * 25, 0), "hydro": 8, "battery": 5},
        })
    return {
        "timestamp": now.isoformat(),
        "nem_carbon_intensity": round(rng.uniform(0.50, 0.75), 2),
        "nem_renewable_pct": nem_ren_pct,
        "annual_emissions_mt_co2": round(rng.uniform(130, 155), 1),
        "emissions_vs_2005_pct": round(rng.uniform(-40, -28), 1),
        "renewable_capacity_gw": round(rng.uniform(28, 36), 1),
        "renewable_target_gw": 50.0,
        "lgc_market": {
            "date": now.strftime("%Y-%m-%d"),
            "lgc_spot_price_aud": round(rng.uniform(35, 52), 2),
            "lgc_futures_2026": round(rng.uniform(38, 55), 2),
            "lgc_futures_2027": round(rng.uniform(32, 48), 2),
            "lgc_futures_2028": round(rng.uniform(28, 42), 2),
            "sts_price_aud": round(rng.uniform(28, 38), 2),
            "total_lgcs_surrendered_ytd": round(rng.uniform(20000, 35000), 0),
            "liable_entities_shortfall_gwh": round(rng.uniform(1.5, 5.0), 1),
        },
        "regional_intensity": regional_intensity,
        "intensity_history": intensity_history,
    }


# --- 5. Sustainability Intensity History ---
@router.get("/api/sustainability/intensity_history", summary="Carbon intensity history", tags=["Sustainability"])
async def sustainability_intensity_history(
    region: str = Query("NSW1"),
    hours: int = Query(24),
):
    """Return list of {region, timestamp, intensity_kg_mwh, renewable_pct}."""
    safe_hours = min(max(1, hours), 168)
    _RENEWABLE_FUELS = {"wind", "solar_utility", "solar_rooftop", "hydro", "battery_discharging", "battery"}
    _EMISSION_FACTORS = {"coal_black": 0.90, "coal_brown": 1.15, "gas_ccgt": 0.37, "gas_ocgt": 0.55,
                         "gas_steam": 0.50, "distillate": 0.70, "gas": 0.45}
    try:
        gen_rows = _query_gold(f"""
            SELECT interval_datetime, fuel_type, ROUND(SUM(total_mw), 1) AS total_mw
            FROM {_CATALOG}.gold.nem_generation_by_fuel
            WHERE interval_datetime >= current_timestamp() - INTERVAL {safe_hours} HOURS
              AND region_id = '{_sql_escape(region)}'
            GROUP BY interval_datetime, fuel_type
            ORDER BY interval_datetime
        """)
        if gen_rows and len(gen_rows) > 5:
            from collections import defaultdict
            by_ts = defaultdict(list)
            for r in gen_rows:
                by_ts[str(r["interval_datetime"])].append(r)
            points = []
            for ts_str in sorted(by_ts.keys())[-safe_hours * 12:]:
                rows = by_ts[ts_str]
                total_mw = sum(float(r.get("total_mw", 0)) for r in rows)
                renew_mw = sum(float(r.get("total_mw", 0)) for r in rows if str(r.get("fuel_type", "")).lower() in _RENEWABLE_FUELS)
                fossil_mw = total_mw - renew_mw
                co2_tons = sum(float(r.get("total_mw", 0)) * _EMISSION_FACTORS.get(str(r.get("fuel_type", "")).lower(), 0) for r in rows)
                intensity = round(co2_tons / total_mw * 1000, 0) if total_mw > 0 else 0
                renew_pct = round(renew_mw / total_mw * 100, 0) if total_mw > 0 else 0
                fossil_pct = round(fossil_mw / total_mw * 100, 0) if total_mw > 0 else 0
                mix = {}
                for r in rows:
                    ft = str(r.get("fuel_type", "other")).lower()
                    bucket = "coal" if "coal" in ft else "gas" if "gas" in ft else "wind" if ft == "wind" else "solar" if "solar" in ft else "hydro" if ft == "hydro" else "battery" if "battery" in ft else "other"
                    mix[bucket] = mix.get(bucket, 0) + round(float(r.get("total_mw", 0)) / max(total_mw, 1) * 100, 0)
                points.append({
                    "region": region,
                    "timestamp": ts_str.replace(" ", "T"),
                    "carbon_intensity_kg_co2_mwh": intensity,
                    "renewable_pct": renew_pct,
                    "fossil_pct": fossil_pct,
                    "generation_mix": mix,
                })
            if points:
                logger.info(f"intensity_history: {len(points)} real points for {region}")
                return points
    except Exception as exc:
        logger.warning(f"intensity_history real-data failed: {exc}")

    # Mock fallback
    now = datetime.now(timezone.utc)
    rng = random.Random(hash(region) + int(now.timestamp() // 30))
    points = []
    for i in range(hours * 2):
        ts = now - timedelta(minutes=30 * (hours * 2 - i))
        hour = ts.hour + ts.minute / 60.0
        solar_factor = max(0, math.sin((hour - 6) / 12 * math.pi)) if 6 <= hour <= 18 else 0
        base_intensity = 750 - solar_factor * 300
        points.append({
            "region": region,
            "timestamp": ts.isoformat(),
            "carbon_intensity_kg_co2_mwh": round(max(100, base_intensity + rng.gauss(0, 30)), 0),
            "renewable_pct": round(min(90, max(10, 35 + solar_factor * 30 + rng.gauss(0, 3))), 0),
            "fossil_pct": round(min(85, max(10, 65 - solar_factor * 30 + rng.gauss(0, 3))), 0),
            "generation_mix": {"coal": 40, "gas": 15, "wind": 20, "solar": round(solar_factor * 25, 0), "hydro": 8, "battery": 5},
        })
    return points


# --- 6. Merit Order ---
@router.get("/api/merit/order", summary="Merit order stack", tags=["Market Data"])
async def merit_order(region: str = Query("NSW1")):
    """Return MeritOrderCurve shape with units sorted by offer price."""
    rng = random.Random(hash(region) + int(time.time() // 30))
    now = datetime.now(timezone.utc)

    if region not in _NEM_REGIONS:
        region = "NSW1"

    # Try real SCADA dispatch stack
    cost_ranges = {"coal": (20, 45), "gas": (55, 120), "solar": (-60, 0), "wind": (-55, 5), "hydro": (10, 70), "battery": (0, 50)}
    try:
        scada_rows = _query_gold(f"""
            SELECT s.duid, MAX(s.generation_MW) AS gen_mw,
                   MAX(f.fuel_type) AS fuel_type, MAX(f.station_name) AS station_name,
                   MAX(f.capacity_mw) AS capacity_mw
            FROM {_CATALOG}.nemweb_analytics.silver_nem_dispatch_unit_scada s
            LEFT JOIN {_CATALOG}.gold.nem_facilities f ON s.duid = f.duid
            WHERE s.generation_MW > 1
              AND s.interval >= current_timestamp() - INTERVAL 30 MINUTES
            GROUP BY s.duid
            ORDER BY gen_mw DESC
            LIMIT 20
        """)
    except Exception:
        scada_rows = None

    if scada_rows and len(scada_rows) >= 3:
        generators = []
        for r in scada_rows:
            ft = str(r.get("fuel_type") or "Unknown").lower()
            fuel_key = "coal" if "coal" in ft else "gas" if "gas" in ft else "solar" if "solar" in ft else "wind" if "wind" in ft else "hydro" if "hydro" in ft else "battery" if "batter" in ft else "gas"
            lo, hi = cost_ranges.get(fuel_key, (30, 80))
            mc = round(rng.uniform(lo, hi), 1)
            generators.append({
                "duid": str(r.get("duid", "")),
                "station_name": str(r.get("station_name") or r.get("duid", "Unknown")),
                "fuel_type": str(r.get("fuel_type") or "Unknown"),
                "region": region,
                "capacity_mw": round(float(r.get("capacity_mw") or r.get("gen_mw") or 100), 0),
                "marginal_cost_aud_mwh": mc,
                "current_offer_price": round(mc + rng.uniform(0, 8), 1),
                "dispatched_mw": round(float(r.get("gen_mw") or 0), 0),
            })
    else:
        generators = [
            {"duid": "ERGS1", "station_name": "Eraring", "fuel_type": "Black Coal", "region": "NSW1", "capacity_mw": 720, "fuel_key": "coal"},
            {"duid": "BW01", "station_name": "Bayswater", "fuel_type": "Black Coal", "region": "NSW1", "capacity_mw": 660, "fuel_key": "coal"},
            {"duid": "BAYSW3", "station_name": "Bayswater 3", "fuel_type": "Black Coal", "region": "NSW1", "capacity_mw": 660, "fuel_key": "coal"},
            {"duid": "CALL_B_1", "station_name": "Callide B", "fuel_type": "Black Coal", "region": "QLD1", "capacity_mw": 700, "fuel_key": "coal"},
            {"duid": "TALWA1", "station_name": "Tallawarra", "fuel_type": "Gas (CCGT)", "region": "NSW1", "capacity_mw": 435, "fuel_key": "gas"},
            {"duid": "MOREESF1", "station_name": "Moree Solar Farm", "fuel_type": "Solar", "region": "NSW1", "capacity_mw": 56, "fuel_key": "solar"},
            {"duid": "SNWYSF1", "station_name": "Snowy 2.0", "fuel_type": "Hydro", "region": "NSW1", "capacity_mw": 2040, "fuel_key": "hydro"},
            {"duid": "HDWF1", "station_name": "Hornsdale Wind Farm", "fuel_type": "Wind", "region": "SA1", "capacity_mw": 315, "fuel_key": "wind"},
            {"duid": "URANQ1", "station_name": "Uranquinty", "fuel_type": "Gas (CCGT)", "region": "NSW1", "capacity_mw": 664, "fuel_key": "gas"},
            {"duid": "MACARTH1", "station_name": "Macarthur Wind", "fuel_type": "Wind", "region": "VIC1", "capacity_mw": 420, "fuel_key": "wind"},
        ]
        for g in generators:
            lo, hi = cost_ranges.get(g["fuel_key"], (30, 80))
            g["marginal_cost_aud_mwh"] = round(rng.uniform(lo, hi), 1)
            g["current_offer_price"] = round(g["marginal_cost_aud_mwh"] + rng.uniform(0, 8), 1)
            g["dispatched_mw"] = round(g["capacity_mw"] * rng.uniform(0.3, 1.0), 0)
            del g["fuel_key"]

    generators.sort(key=lambda x: x["current_offer_price"])
    cumulative = 0
    for g in generators:
        cumulative += g["dispatched_mw"]
        g["cumulative_mw"] = round(cumulative, 0)
    demand = round(rng.uniform(7500, 9500), 0)
    marginal_gen = None
    smc = 0.0
    for g in generators:
        if g["cumulative_mw"] >= demand:
            marginal_gen = g["duid"]
            smc = g["current_offer_price"]
            break
    if marginal_gen is None:
        marginal_gen = generators[-1]["duid"]
        smc = generators[-1]["current_offer_price"]
    for g in generators:
        g["on_merit"] = g["cumulative_mw"] <= cumulative
    return {
        "region": region,
        "timestamp": now.isoformat(),
        "demand_mw": demand,
        "marginal_generator": marginal_gen,
        "system_marginal_cost": smc,
        "total_supply_mw": round(cumulative, 0),
        "units": generators,
    }


# --- 7. Trading Dashboard ---
@router.get("/api/trading/dashboard", summary="Trading P&L dashboard", tags=["Trading"])
async def trading_dashboard():
    """Return TradingDashboard shape with positions and spreads from real trade + price data."""
    now = datetime.now(timezone.utc)

    # Try real trades + current prices for positions
    try:
        trade_rows = _query_gold(
            f"SELECT trade_id, region, product_type, direction, volume_mw, "
            f"price_aud_mwh, counterparty_name, trade_date, start_date, end_date, "
            f"trader, status "
            f"FROM {_CATALOG}.gold.trades "
            f"WHERE status NOT IN ('CANCELLED') "
            f"ORDER BY trade_date DESC LIMIT 30"
        )
        price_rows = _query_gold(
            f"SELECT region_id, ROUND(AVG(rrp), 2) AS current_price "
            f"FROM {_CATALOG}.gold.nem_prices_5min "
            f"WHERE interval_datetime >= current_timestamp() - INTERVAL 1 HOURS "
            f"GROUP BY region_id"
        )
    except Exception:
        trade_rows = None
        price_rows = None

    if trade_rows:
        current_prices = {r["region_id"]: float(r["current_price"] or 0) for r in (price_rows or [])}
        positions = []
        total_long = 0
        total_short = 0
        total_pnl = 0
        for r in trade_rows:
            direction = str(r.get("direction", "long")).lower()
            vol = float(r.get("volume_mw", 0))
            entry = float(r.get("price_aud_mwh", 0))
            region = str(r.get("region", "NSW1"))
            current = current_prices.get(region, entry)
            pnl = round((current - entry) * vol * 48, 2) if direction == "long" else round((entry - current) * vol * 48, 2)
            if direction == "long":
                total_long += vol
            else:
                total_short += vol
            total_pnl += pnl
            positions.append({
                "position_id": str(r.get("trade_id", ""))[:12],
                "trader": str(r.get("trader", "Desk-A")),
                "region": region,
                "product": str(r.get("product_type", "Base Swap")),
                "direction": direction,
                "volume_mw": vol,
                "entry_price_aud_mwh": entry,
                "current_price_aud_mwh": current,
                "pnl_aud": pnl,
                "open_date": str(r.get("trade_date", ""))[:10],
                "expiry_date": str(r.get("end_date", ""))[:10],
                "counterparty": str(r.get("counterparty_name", "Unknown")),
            })

        # Build spreads from real interconnector data
        spreads = []
        try:
            ic_rows = _query_gold(
                f"SELECT interconnector_id, from_region, to_region, "
                f"ROUND(AVG(mw_flow), 0) AS avg_flow, "
                f"ROUND(MAX(ABS(export_limit_mw)), 0) AS capacity "
                f"FROM {_CATALOG}.gold.nem_interconnectors "
                f"WHERE interval_datetime >= current_timestamp() - INTERVAL 1 HOURS "
                f"GROUP BY interconnector_id, from_region, to_region"
            )
            if ic_rows:
                for r in ic_rows:
                    r1, r2 = str(r["from_region"]), str(r["to_region"])
                    p1 = current_prices.get(r1, 70)
                    p2 = current_prices.get(r2, 70)
                    spread = round(p1 - p2, 2)
                    flow = float(r.get("avg_flow", 0))
                    cap = float(r.get("capacity", 0))
                    spreads.append({
                        "region_from": r1, "region_to": r2,
                        "interconnector": str(r["interconnector_id"]),
                        "spot_spread_aud_mwh": spread,
                        "forward_spread_aud_mwh": round(spread * 0.9, 2),
                        "flow_mw": abs(flow), "capacity_mw": cap,
                        "congestion_revenue_m_aud": round(abs(spread) * abs(flow) / 1e6 * 24, 3),
                        "arbitrage_opportunity": abs(spread) > 10,
                    })
        except Exception:
            pass

        return {
            "timestamp": now.isoformat(),
            "total_long_mw": total_long,
            "total_short_mw": total_short,
            "net_position_mw": total_long - total_short,
            "total_pnl_aud": round(total_pnl, 2),
            "daily_volume_mw": total_long + total_short,
            "regions_active": len(set(p["region"] for p in positions)),
            "positions": positions,
            "spreads": spreads,
        }

    # Mock fallback
    rng = random.Random(int(time.time() // 30))
    products = ["Base Swap", "Peak Swap", "Cap $300", "Cap $500", "Asian Option", "CfD"]
    traders = ["Desk-A", "Desk-B", "Desk-C", "Algo-1"]
    counterparties = ["Origin Energy", "AGL", "EnergyAustralia", "Snowy Hydro", "Shell Energy"]
    positions = []
    total_long = total_short = total_pnl = 0
    for i in range(rng.randint(6, 12)):
        region = rng.choice(_NEM_REGIONS)
        direction = rng.choice(["long", "short"])
        vol = rng.choice([10, 25, 50, 100])
        entry = round(rng.uniform(40, 150), 2)
        current = round(entry * rng.uniform(0.8, 1.3), 2)
        pnl = round((current - entry) * vol * 48, 2) if direction == "long" else round((entry - current) * vol * 48, 2)
        if direction == "long": total_long += vol
        else: total_short += vol
        total_pnl += pnl
        positions.append({"position_id": f"POS-{1000+i}", "trader": rng.choice(traders), "region": region, "product": rng.choice(products), "direction": direction, "volume_mw": vol, "entry_price_aud_mwh": entry, "current_price_aud_mwh": current, "pnl_aud": pnl, "open_date": (now - timedelta(days=rng.randint(1, 30))).strftime("%Y-%m-%d"), "expiry_date": (now + timedelta(days=rng.randint(7, 180))).strftime("%Y-%m-%d"), "counterparty": rng.choice(counterparties)})
    return {"timestamp": now.isoformat(), "total_long_mw": total_long, "total_short_mw": total_short, "net_position_mw": total_long - total_short, "total_pnl_aud": round(total_pnl, 2), "daily_volume_mw": total_long + total_short, "regions_active": len(set(p["region"] for p in positions)), "positions": positions, "spreads": []}


# --- 8. Trading Positions ---
@router.get("/api/trading/positions", summary="Open trading positions", tags=["Trading"])
async def trading_positions():
    """Return TradingPosition[] from real trade data."""
    now = datetime.now(timezone.utc)
    try:
        trade_rows = _query_gold(
            f"SELECT trade_id, region, product_type, direction, volume_mw, "
            f"price_aud_mwh, counterparty_name, trade_date, start_date, end_date, trader "
            f"FROM {_CATALOG}.gold.trades "
            f"WHERE status NOT IN ('CANCELLED') "
            f"ORDER BY trade_date DESC LIMIT 30"
        )
        price_rows = _query_gold(
            f"SELECT region_id, ROUND(AVG(rrp), 2) AS current_price "
            f"FROM {_CATALOG}.gold.nem_prices_5min "
            f"WHERE interval_datetime >= current_timestamp() - INTERVAL 1 HOURS "
            f"GROUP BY region_id"
        )
    except Exception:
        trade_rows = None
        price_rows = None

    if trade_rows:
        current_prices = {r["region_id"]: float(r["current_price"] or 0) for r in (price_rows or [])}
        positions = []
        for r in trade_rows:
            direction = str(r.get("direction", "long")).lower()
            vol = float(r.get("volume_mw", 0))
            entry = float(r.get("price_aud_mwh", 0))
            region = str(r.get("region", "NSW1"))
            current = current_prices.get(region, entry)
            pnl = round((current - entry) * vol * 48, 2) if direction == "long" else round((entry - current) * vol * 48, 2)
            positions.append({
                "position_id": str(r.get("trade_id", ""))[:12],
                "trader": str(r.get("trader", "Desk-A")),
                "region": region,
                "product": str(r.get("product_type", "Base Swap")),
                "direction": direction,
                "volume_mw": vol,
                "entry_price_aud_mwh": entry,
                "current_price_aud_mwh": current,
                "pnl_aud": pnl,
                "open_date": str(r.get("trade_date", ""))[:10],
                "expiry_date": str(r.get("end_date", ""))[:10],
                "counterparty": str(r.get("counterparty_name", "Unknown")),
            })
        return positions

    # Mock fallback
    rng = random.Random(int(time.time() // 30))
    products = ["Base Swap", "Peak Swap", "Cap $300", "Cap $500", "Asian Option", "CfD"]
    return [{"position_id": f"POS-{1000+i}", "trader": rng.choice(["Desk-A","Desk-B","Desk-C"]), "region": rng.choice(_NEM_REGIONS), "product": rng.choice(products), "direction": rng.choice(["long","short"]), "volume_mw": rng.choice([10,25,50,100]), "entry_price_aud_mwh": round(rng.uniform(40,150),2), "current_price_aud_mwh": round(rng.uniform(40,150),2), "pnl_aud": round(rng.uniform(-5000,5000),2), "open_date": (now - timedelta(days=rng.randint(1,30))).strftime("%Y-%m-%d"), "expiry_date": (now + timedelta(days=rng.randint(7,180))).strftime("%Y-%m-%d"), "counterparty": rng.choice(["Origin Energy","AGL","EnergyAustralia"])} for i in range(8)]


# --- 9. Trading Spreads ---
@router.get("/api/trading/spreads", summary="Inter-region spreads", tags=["Trading"])
async def trading_spreads():
    """Return RegionSpread[] matching frontend interface."""
    # Try real price data for inter-region spreads
    try:
        price_rows = _query_gold(f"""
            SELECT region_id, AVG(rrp) AS avg_price
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 1 HOUR
            GROUP BY region_id
        """)
        ic_rows = _query_gold(f"""
            SELECT interconnector_id, AVG(mw_flow) AS avg_flow,
                   MAX(export_limit_mw) AS export_cap, MAX(import_limit_mw) AS import_cap
            FROM {_CATALOG}.gold.nem_interconnectors
            WHERE interval_datetime >= current_timestamp() - INTERVAL 1 HOUR
            GROUP BY interconnector_id
        """)
    except Exception:
        price_rows = None
        ic_rows = None

    if price_rows:
        prices = {r["region_id"]: float(r["avg_price"] or 0) for r in price_rows}
        ic_map = {}
        if ic_rows:
            for r in ic_rows:
                ic_map[r["interconnector_id"]] = {
                    "flow": float(r.get("avg_flow") or 0),
                    "cap": max(abs(float(r.get("export_cap") or 1000)), abs(float(r.get("import_cap") or 1000))),
                }
        interconnectors = {"NSW1-QLD1": "QNI", "VIC1-SA1": "Heywood", "NSW1-VIC1": "VNI", "QLD1-SA1": "Terranora", "VIC1-TAS1": "Basslink"}
        ic_id_map = {"NSW1-QLD1": "N-Q-MNSP1", "VIC1-SA1": "V-SA", "NSW1-VIC1": "VIC1-NSW1", "QLD1-SA1": "N-Q-MNSP1", "VIC1-TAS1": "T-V-MNSP1"}
        pairs = [("NSW1", "QLD1"), ("VIC1", "SA1"), ("NSW1", "VIC1"), ("QLD1", "SA1"), ("VIC1", "TAS1")]
        spreads = []
        for r1, r2 in pairs:
            p1 = prices.get(r1, 70)
            p2 = prices.get(r2, 70)
            spot_spread = round(p1 - p2, 2)
            ic_key = f"{r1}-{r2}"
            ic_data = ic_map.get(ic_id_map.get(ic_key, ""), {"flow": 0, "cap": 1000})
            flow = round(ic_data["flow"], 0)
            cap = round(ic_data["cap"], 0)
            spreads.append({
                "region_from": r1,
                "region_to": r2,
                "interconnector": interconnectors.get(ic_key, "Unknown"),
                "spot_spread_aud_mwh": spot_spread,
                "forward_spread_aud_mwh": round(spot_spread * 0.9, 2),
                "flow_mw": flow,
                "capacity_mw": cap,
                "congestion_revenue_m_aud": round(abs(spot_spread) * abs(flow) / 1e6 * 24, 3),
                "arbitrage_opportunity": abs(spot_spread) > 10,
            })
        if spreads:
            return spreads

    # Mock fallback
    rng = random.Random(int(time.time() // 30))
    interconnectors = {"NSW1-QLD1": "QNI", "VIC1-SA1": "Heywood", "NSW1-VIC1": "VNI", "QLD1-SA1": "Terranora", "VIC1-TAS1": "Basslink"}
    pairs = [("NSW1", "QLD1"), ("VIC1", "SA1"), ("NSW1", "VIC1"), ("QLD1", "SA1"), ("VIC1", "TAS1")]
    spreads = []
    for r1, r2 in pairs:
        b1 = _REGION_BASE_PRICES.get(r1, 70)
        b2 = _REGION_BASE_PRICES.get(r2, 70)
        spot_spread = round((b1 - b2) * rng.uniform(0.5, 2.0), 2)
        cap = rng.choice([460, 700, 1000, 1350])
        flow = round(cap * rng.uniform(0.3, 0.95), 0)
        spreads.append({
            "region_from": r1,
            "region_to": r2,
            "interconnector": interconnectors.get(f"{r1}-{r2}", "Unknown"),
            "spot_spread_aud_mwh": spot_spread,
            "forward_spread_aud_mwh": round(spot_spread * rng.uniform(0.7, 1.3), 2),
            "flow_mw": flow,
            "capacity_mw": cap,
            "congestion_revenue_m_aud": round(abs(spot_spread) * flow / 1e6 * 24, 3),
            "arbitrage_opportunity": abs(spot_spread) > 10,
        })
    return spreads


# --- 10. Alerts ---
@router.get("/api/alerts", summary="Active alerts", tags=["Alerts"])
async def alerts_list():
    """Return Alert[] matching frontend interface — real NEMWEB data with mock fallback."""
    now = datetime.now(timezone.utc)
    alerts: list[dict] = []
    real_data = False

    try:
        # 1. Price anomaly alerts (last 2 hours)
        price_rows = _query_gold(f"""
            SELECT region_id, rrp, total_demand_mw, available_gen_mw, interval_datetime
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 2 HOURS
            ORDER BY interval_datetime DESC
        """)
        if price_rows:
            real_data = True
            alert_idx = 0
            for r in price_rows:
                price = float(r["rrp"])
                region = r["region_id"]
                ts = str(r["interval_datetime"]).replace(" ", "T")
                demand = float(r["total_demand_mw"]) if r.get("total_demand_mw") else None
                avail = float(r["available_gen_mw"]) if r.get("available_gen_mw") else None

                # CRITICAL: rrp > 1000
                if price > 1000:
                    alert_idx += 1
                    alerts.append({
                        "id": f"ALT-{alert_idx:03d}",
                        "type": "EXTREME_PRICE",
                        "severity": "CRITICAL",
                        "region": region,
                        "metric": "price",
                        "threshold": 1000,
                        "value": price,
                        "status": "triggered",
                        "triggeredAt": ts,
                        "message": f"Extreme price ${price:,.0f}/MWh in {region}",
                        "isActive": True,
                        "acknowledged": False,
                        "notificationChannel": "SLACK",
                    })
                # HIGH: rrp > 300
                elif price > 300:
                    alert_idx += 1
                    alerts.append({
                        "id": f"ALT-{alert_idx:03d}",
                        "type": "PRICE_SPIKE",
                        "severity": "HIGH",
                        "region": region,
                        "metric": "price",
                        "threshold": 300,
                        "value": price,
                        "status": "triggered",
                        "triggeredAt": ts,
                        "message": f"Price spike ${price:,.0f}/MWh in {region}",
                        "isActive": True,
                        "acknowledged": False,
                        "notificationChannel": "SLACK",
                    })
                # MEDIUM: rrp < 0
                elif price < 0:
                    alert_idx += 1
                    alerts.append({
                        "id": f"ALT-{alert_idx:03d}",
                        "type": "NEGATIVE_PRICE",
                        "severity": "MEDIUM",
                        "region": region,
                        "metric": "price",
                        "threshold": 0,
                        "value": price,
                        "status": "triggered",
                        "triggeredAt": ts,
                        "message": f"Negative price ${price:,.2f}/MWh in {region}",
                        "isActive": True,
                        "acknowledged": False,
                        "notificationChannel": "EMAIL",
                    })

                # LOW_RESERVE: demand > 90% of available generation
                if demand and avail and avail > 0 and demand > 0.9 * avail:
                    alert_idx += 1
                    reserve_pct = round((1 - demand / avail) * 100, 1)
                    alerts.append({
                        "id": f"ALT-{alert_idx:03d}",
                        "type": "LOW_RESERVE",
                        "severity": "HIGH",
                        "region": region,
                        "metric": "demand",
                        "threshold": round(0.9 * avail, 0),
                        "value": demand,
                        "status": "triggered",
                        "triggeredAt": ts,
                        "message": f"Low reserve margin {reserve_pct}% in {region} — demand {demand:,.0f} MW vs available {avail:,.0f} MW",
                        "isActive": True,
                        "acknowledged": False,
                        "notificationChannel": "SLACK",
                    })

        # 2. Congestion alerts from interconnectors (last 2 hours)
        congestion_rows = _query_gold(f"""
            SELECT interconnector_id, from_region, to_region, mw_flow,
                   utilization_pct, interval_datetime
            FROM {_CATALOG}.gold.nem_interconnectors
            WHERE interval_datetime >= current_timestamp() - INTERVAL 2 HOURS
              AND is_congested = true
            ORDER BY interval_datetime DESC
        """)
        if congestion_rows:
            real_data = True
            for r in congestion_rows:
                alert_idx += 1
                ic = r["interconnector_id"]
                fr = r["from_region"]
                to = r["to_region"]
                flow = float(r["mw_flow"])
                util = float(r["utilization_pct"]) if r.get("utilization_pct") else None
                ts = str(r["interval_datetime"]).replace(" ", "T")
                alerts.append({
                    "id": f"ALT-{alert_idx:03d}",
                    "type": "CONGESTION",
                    "severity": "MEDIUM",
                    "region": fr,
                    "metric": "interconnector",
                    "threshold": 95,
                    "value": util if util else flow,
                    "status": "triggered",
                    "triggeredAt": ts,
                    "message": f"Congestion on {ic} ({fr}→{to}): {flow:,.0f} MW, {util:.0f}% utilised" if util else f"Congestion on {ic} ({fr}→{to}): {flow:,.0f} MW",
                    "isActive": True,
                    "acknowledged": False,
                    "notificationChannel": "EMAIL",
                })

        # Deduplicate: keep only most recent alert per (type, region) combo
        if alerts:
            seen: set[tuple] = set()
            deduped: list[dict] = []
            for a in alerts:
                key = (a["type"], a["region"])
                if key not in seen:
                    seen.add(key)
                    deduped.append(a)
            alerts = deduped[:20]  # Cap at 20 alerts
            # Re-number IDs
            for i, a in enumerate(alerts):
                a["id"] = f"ALT-{i + 1:03d}"

        if real_data:
            logger.info(f"alerts_list: {len(alerts)} real alerts from NEMWEB")
            return alerts
    except Exception as exc:
        logger.warning(f"alerts_list real-data failed: {exc}")

    # --- Mock fallback ---
    rng = random.Random(int(time.time() // 30))
    alert_defs = [
        {"region": "NSW1", "metric": "price", "threshold": 300, "type": "PRICE_SPIKE", "severity": "HIGH"},
        {"region": "SA1", "metric": "frequency", "threshold": 49.85, "type": "PRICE_SPIKE", "severity": "MEDIUM"},
        {"region": "QLD1", "metric": "demand", "threshold": 9000, "type": "LOW_RESERVE", "severity": "HIGH"},
        {"region": "VIC1", "metric": "generation", "threshold": 200, "type": "PRICE_SPIKE", "severity": "MEDIUM"},
        {"region": "NSW1", "metric": "interconnector", "threshold": 95, "type": "CONGESTION", "severity": "MEDIUM"},
        {"region": "SA1", "metric": "battery_soc", "threshold": 15, "type": "LOW_RESERVE", "severity": "LOW"},
        {"region": "VIC1", "metric": "carbon_intensity", "threshold": 0.9, "type": "PRICE_SPIKE", "severity": "LOW"},
        {"region": "QLD1", "metric": "price", "threshold": 500, "type": "PRICE_SPIKE", "severity": "HIGH"},
        {"region": "TAS1", "metric": "demand", "threshold": 1500, "type": "LOW_RESERVE", "severity": "MEDIUM"},
        {"region": "SA1", "metric": "price", "threshold": 1000, "type": "EXTREME_PRICE", "severity": "CRITICAL"},
        {"region": "NSW1", "metric": "demand", "threshold": 12000, "type": "LOW_RESERVE", "severity": "HIGH"},
        {"region": "VIC1", "metric": "price", "threshold": 300, "type": "PRICE_SPIKE", "severity": "HIGH"},
    ]
    channels = ["SLACK", "EMAIL"]
    mock_alerts = []
    for i, a in enumerate(alert_defs):
        triggered = rng.random() > 0.6
        mock_alerts.append({
            "id": f"ALT-{i + 1:03d}",
            "type": a["type"],
            "severity": a["severity"],
            "region": a["region"],
            "metric": a["metric"],
            "threshold": a["threshold"],
            "value": round(a["threshold"] * rng.uniform(1.01, 1.4), 2) if triggered else None,
            "status": "triggered" if triggered else "armed",
            "triggeredAt": (now - timedelta(hours=rng.uniform(0, 2))).isoformat() if triggered else None,
            "message": f"{a['type']} alert in {a['region']}" if triggered else None,
            "isActive": True,
            "acknowledged": False,
            "notificationChannel": rng.choice(channels),
        })
    return mock_alerts


# --- 11. Alert Stats ---
@router.get("/api/alerts/stats", summary="Alert statistics", tags=["Alerts"])
async def alerts_stats():
    """Return AlertStats matching frontend interface — derived from real NEMWEB data."""
    try:
        # Count price anomalies in last 24h
        price_stats = _query_gold(f"""
            SELECT
                COUNT(*) AS total_events,
                COUNT(CASE WHEN rrp > 300 THEN 1 END) AS spikes,
                COUNT(CASE WHEN rrp > 1000 THEN 1 END) AS extreme,
                COUNT(CASE WHEN rrp < 0 THEN 1 END) AS negative
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
              AND (rrp > 300 OR rrp < 0)
        """)
        # Count congestion events in last 24h
        congestion_stats = _query_gold(f"""
            SELECT COUNT(*) AS congestion_events
            FROM {_CATALOG}.gold.nem_interconnectors
            WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
              AND is_congested = true
        """)
        # Find region with most price alerts
        region_stats = _query_gold(f"""
            SELECT region_id, COUNT(*) AS cnt
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
              AND (rrp > 300 OR rrp < 0)
            GROUP BY region_id
            ORDER BY cnt DESC
            LIMIT 1
        """)

        if price_stats:
            ps = price_stats[0]
            triggered = int(ps["spikes"]) + int(ps["extreme"]) + int(ps["negative"])
            congestion = int(congestion_stats[0]["congestion_events"]) if congestion_stats else 0
            total = triggered + congestion
            most_region = region_stats[0]["region_id"] if region_stats else "NSW1"
            logger.info(f"alerts_stats: {total} real events (price={triggered}, congestion={congestion})")
            return {
                "total_alerts": total,
                "triggered_last_24h": triggered,
                "critical_count": int(ps["extreme"]),
                "high_count": int(ps["spikes"]),
                "medium_count": int(ps["negative"]) + congestion,
                "notifications_sent": max(1, triggered),
                "channels": ["SLACK", "EMAIL"],
                "most_triggered_region": most_region,
            }
    except Exception as exc:
        logger.warning(f"alerts_stats real-data failed: {exc}")

    # --- Mock fallback ---
    rng = random.Random(int(time.time() // 30))
    return {
        "total_alerts": rng.randint(10, 20),
        "triggered_last_24h": rng.randint(1, 6),
        "critical_count": rng.randint(0, 2),
        "high_count": rng.randint(2, 5),
        "medium_count": rng.randint(3, 8),
        "notifications_sent": rng.randint(3, 12),
        "channels": ["SLACK", "EMAIL"],
        "most_triggered_region": rng.choice(_NEM_REGIONS),
    }


# --- 12. Demand Response ---
@router.get("/api/demand/response", summary="Demand response summary", tags=["Demand Response"])
async def demand_response(region: str = Query(None)):
    """Return DemandResponseSummary matching frontend interface."""
    rng = random.Random(int(time.time() // 30))
    now = datetime.now(timezone.utc)
    programs = ["EnergyAustralia DR", "AGL Virtual Power Plant", "Enel X", "Amber Electric Flex", "ShineHub VPP"]
    triggers = ["price_signal", "RERT_activation", "reliability_event", "frequency_event"]
    events = []
    for i in range(rng.randint(4, 8)):
        r = region if region else rng.choice(_NEM_REGIONS)
        mw = round(rng.uniform(10, 120), 1)
        events.append({
            "event_id": f"DRE-{2000 + i}",
            "program_name": rng.choice(programs),
            "region": r,
            "activation_time": (now - timedelta(hours=rng.uniform(0.5, 24))).isoformat(),
            "duration_minutes": rng.randint(15, 120),
            "mw_reduction": mw,
            "participants": rng.randint(200, 5000),
            "status": rng.choice(["active", "completed", "cancelled"]),
            "trigger_reason": rng.choice(triggers),
        })
    total_enrolled = round(rng.uniform(800, 2000), 0)
    total_activated = round(sum(e["mw_reduction"] for e in events if e["status"] in ("active", "completed")), 0)
    region_summaries = {}
    for r in _NEM_REGIONS:
        region_summaries[r] = round(rng.uniform(50, 400), 0)
    return {
        "timestamp": now.isoformat(),
        "active_programs": rng.randint(3, 8),
        "total_enrolled_mw": total_enrolled,
        "total_activated_mw_today": total_activated,
        "events_today": len([e for e in events if e["status"] != "cancelled"]),
        "events": events,
        "region_summaries": region_summaries,
    }


# --- 13. Market Notices ---
@router.get("/api/market-events/dashboard", summary="NEM market events dashboard", tags=["Market Data"])
async def market_events_dashboard():
    """Return MarketEventsDashboard matching frontend interface."""
    rng = random.Random(int(time.time() // 300))
    now = datetime.now(timezone.utc)

    event_types = ["LOR1", "LOR2", "LOR3", "direction", "administered_pricing", "constraint_violation", "frequency_event", "interconnector_trip"]
    severities = ["low", "medium", "high", "critical"]
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    causes = [
        "forecast demand exceeds available supply",
        "unplanned generator outage",
        "interconnector constraint binding",
        "high demand from extreme temperatures",
        "low wind generation across region",
        "transmission line fault",
        "frequency deviation outside normal band",
    ]

    # Try real price spike events from gold table
    recent_events = []
    spike_rows = _query_gold(f"""
        SELECT region_id, rrp, interval_datetime
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE rrp > 300 AND interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
        ORDER BY interval_datetime DESC
        LIMIT 20
    """)
    if spike_rows:
        for i, sr in enumerate(spike_rows):
            price = float(sr["rrp"])
            if price > 5000:
                sev = "critical"
                et = "administered_pricing"
            elif price > 1000:
                sev = "high"
                et = "LOR2"
            elif price > 300:
                sev = "medium"
                et = "LOR1"
            else:
                sev = "low"
                et = "constraint_violation"
            recent_events.append({
                "event_id": f"NEM-EVT-{2026}{i+1:04d}",
                "event_type": et,
                "region": sr["region_id"],
                "start_time": str(sr["interval_datetime"]),
                "end_time": str(sr["interval_datetime"]),
                "duration_minutes": 5,
                "severity": sev,
                "description": f"Price spike to ${price:.0f}/MWh in {sr['region_id']}",
                "affected_capacity_mw": None,
                "administered_price": round(price, 2) if et == "administered_pricing" else None,
                "resolved": True,
            })
    if not recent_events:
        for i in range(rng.randint(6, 12)):
            et = rng.choice(event_types)
            sev = "critical" if et in ("LOR3", "administered_pricing") else rng.choice(severities)
            start = now - timedelta(hours=rng.uniform(0.5, 168))
            resolved = rng.random() > 0.2
            dur = rng.randint(5, 480) if resolved else None
            recent_events.append({
                "event_id": f"NEM-EVT-{2026}{i+1:04d}",
                "event_type": et,
                "region": rng.choice(regions),
                "start_time": start.isoformat(),
                "end_time": (start + timedelta(minutes=dur)).isoformat() if dur else None,
                "duration_minutes": dur,
                "severity": sev,
                "description": rng.choice(causes),
                "affected_capacity_mw": round(rng.uniform(100, 2500), 0) if et != "frequency_event" else None,
                "administered_price": round(rng.uniform(300, 600), 2) if et == "administered_pricing" else None,
                "resolved": resolved,
            })
    recent_events.sort(key=lambda e: e["start_time"], reverse=True)

    # Interventions
    interventions = []
    intervention_types = ["direction", "instructions", "reliability_reserve"]
    for i in range(rng.randint(1, 4)):
        interventions.append({
            "intervention_id": f"INT-{2026}{i+1:03d}",
            "intervention_type": rng.choice(intervention_types),
            "region": rng.choice(regions),
            "duid": rng.choice(["BW01", "TORRB1", "APTS1", "PPCCGT", "YWPS1", None]),
            "station_name": rng.choice(["Bayswater", "Torrens Island B", "Angaston", "Pelican Point", "Yallourn", None]),
            "issued_time": (now - timedelta(hours=rng.uniform(1, 48))).isoformat(),
            "duration_hours": round(rng.uniform(0.5, 12), 1),
            "directed_mw": round(rng.uniform(50, 500), 0),
            "reason": rng.choice(causes),
            "market_notice_id": f"MN-{rng.randint(80000, 99999)}",
            "cost_est_aud": round(rng.uniform(50000, 2000000), 0),
        })

    # Price cap events
    price_cap_events = []
    cap_types = ["CPT", "MPC", "APC"]
    for i in range(rng.randint(0, 3)):
        price_cap_events.append({
            "event_id": f"CAP-{2026}{i+1:03d}",
            "region": rng.choice(regions),
            "date": (now - timedelta(days=rng.randint(0, 30))).strftime("%Y-%m-%d"),
            "cap_type": rng.choice(cap_types),
            "trigger_interval": (now - timedelta(hours=rng.uniform(1, 72))).isoformat(),
            "intervals_above_cap": rng.randint(1, 12),
            "cumulative_energy_mwh": round(rng.uniform(100, 5000), 1),
            "max_spot_price": round(rng.uniform(5000, 16600), 2),
            "total_apc_duration_hours": round(rng.uniform(0.5, 8), 1) if rng.random() > 0.5 else None,
        })

    critical = sum(1 for e in recent_events if e["severity"] == "critical")
    lor_today = sum(1 for e in recent_events if e["event_type"].startswith("LOR") and e["start_time"][:10] == now.strftime("%Y-%m-%d"))
    active_dirs = sum(1 for i in interventions if i["intervention_type"] == "direction")
    apc_hrs = sum(p["total_apc_duration_hours"] or 0 for p in price_cap_events)

    return {
        "period": "Last 7 days",
        "total_events": len(recent_events),
        "critical_events": critical,
        "interventions_this_week": len(interventions),
        "apc_hours_this_month": round(apc_hrs, 1),
        "lor_events_today": lor_today,
        "directions_active": active_dirs,
        "recent_events": recent_events,
        "interventions": interventions,
        "price_cap_events": price_cap_events,
    }


@router.get("/api/surveillance/dashboard", summary="Market surveillance dashboard", tags=["Market Data"])
async def surveillance_dashboard():
    """Return SurveillanceDashboard matching frontend interface."""
    rng = random.Random(int(time.time() // 300))
    now = datetime.now(timezone.utc)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    participants = ["Origin Energy", "AGL Energy", "EnergyAustralia", "Alinta Energy", "Snowy Hydro",
                    "CS Energy", "Stanwell", "Macquarie Generation", "Engie", "Delta Electricity"]
    notice_types = ["rebidding_investigation", "market_power_review", "price_manipulation", "compliance_audit",
                    "direction_compliance", "late_rebid", "false_misleading_offer"]
    rule_refs = ["NER 3.8.22A", "NER 3.8.1", "NEL s.46", "NER 3.15.6A", "NER 4.9.8", "NER 3.8.22"]
    breach_types = ["late_rebid", "false_rebid_reason", "excessive_market_power", "direction_non_compliance", "reporting_failure"]
    anomaly_types = ["price_spike_unexplained", "generation_withdrawal", "bid_stack_manipulation",
                     "interconnector_gaming", "capacity_withholding", "rebid_timing"]

    notices = []
    for i in range(rng.randint(4, 10)):
        td = (now - timedelta(days=rng.randint(0, 60))).strftime("%Y-%m-%d")
        resolved = rng.random() > 0.4
        notices.append({
            "notice_id": f"SN-{2026}-{i+1:04d}",
            "notice_type": rng.choice(notice_types),
            "region": rng.choice(regions),
            "participant": rng.choice(participants),
            "trading_date": td,
            "description": f"Investigation into {rng.choice(anomaly_types).replace('_', ' ')} behaviour",
            "status": rng.choice(["closed", "resolved"]) if resolved else rng.choice(["open", "under_review"]),
            "priority": rng.choice(["low", "medium", "high", "critical"]),
            "aemo_team": rng.choice(["Market Monitoring", "Compliance", "Enforcement", "Market Analysis"]),
            "resolution_date": (now - timedelta(days=rng.randint(0, 10))).strftime("%Y-%m-%d") if resolved else None,
            "outcome": rng.choice(["no_breach", "warning_issued", "referred_to_aer", "penalty_applied", "ongoing_monitoring"]) if resolved else None,
        })

    compliance = []
    for i in range(rng.randint(3, 8)):
        ref_to_aer = rng.random() > 0.7
        compliance.append({
            "record_id": f"CR-{2026}-{i+1:04d}",
            "participant": rng.choice(participants),
            "rule_reference": rng.choice(rule_refs),
            "rule_description": rng.choice(["Late rebidding obligation", "Good faith rebidding", "Market power conduct",
                                             "Direction compliance", "Reporting obligations", "Bidding in good faith"]),
            "breach_type": rng.choice(breach_types),
            "trading_date": (now - timedelta(days=rng.randint(0, 90))).strftime("%Y-%m-%d"),
            "region": rng.choice(regions),
            "penalty_aud": round(rng.uniform(0, 500000), 0) if rng.random() > 0.3 else 0,
            "status": rng.choice(["confirmed", "under_investigation", "dismissed", "penalty_issued"]),
            "referred_to_aer": ref_to_aer,
            "civil_penalty": ref_to_aer and rng.random() > 0.5,
        })

    # Try real price anomalies from NEMWEB
    anomalies = []
    try:
        anomaly_rows = _query_gold(f"""
            SELECT region_id, rrp, interval_datetime,
                   AVG(rrp) OVER (PARTITION BY region_id ORDER BY interval_datetime
                     ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS expected_price
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
            QUALIFY ABS(rrp - expected_price) / GREATEST(ABS(expected_price), 1) > 1.0
            ORDER BY interval_datetime DESC
            LIMIT 20
        """)
    except Exception:
        anomaly_rows = None

    if anomaly_rows:
        for i, r in enumerate(anomaly_rows):
            actual = float(r.get("rrp") or 0)
            expected = float(r.get("expected_price") or 70)
            deviation = round(((actual - expected) / max(abs(expected), 1)) * 100, 1)
            atype = "price_spike_unexplained" if actual > 300 else "generation_withdrawal" if actual < 0 else "bid_stack_manipulation"
            anomalies.append({
                "anomaly_id": f"MA-{2026}-{i+1:04d}",
                "region": r.get("region_id", "NSW1"),
                "trading_interval": str(r.get("interval_datetime", "")),
                "anomaly_type": atype,
                "spot_price": round(actual, 2),
                "expected_price": round(expected, 2),
                "deviation_pct": deviation,
                "generator_id": None,
                "flagged": abs(deviation) > 200,
                "explanation": "Real price anomaly detected from NEMWEB data",
            })
    else:
        for i in range(rng.randint(5, 15)):
            expected = round(rng.uniform(30, 120), 2)
            actual = round(expected * rng.uniform(1.5, 10), 2) if rng.random() > 0.3 else round(expected * rng.uniform(-0.5, 0.5), 2)
            anomalies.append({
                "anomaly_id": f"MA-{2026}-{i+1:04d}",
                "region": rng.choice(regions),
                "trading_interval": (now - timedelta(hours=rng.uniform(0, 168))).isoformat(),
                "anomaly_type": rng.choice(anomaly_types),
                "spot_price": actual,
                "expected_price": expected,
                "deviation_pct": round(((actual - expected) / expected) * 100, 1) if expected != 0 else 0,
                "generator_id": rng.choice(["BW01", "TORRB1", "APTS1", "YWPS1", "LD01", "MP1", "VPGS1", None]),
                "flagged": rng.random() > 0.4,
                "explanation": rng.choice([None, "demand forecast error", "generator trip", "rebid ahead of constraint",
                                            "correlated bidding pattern", "capacity withholding suspected"]),
            })

    referred = sum(1 for c in compliance if c["referred_to_aer"])
    total_penalties = sum(c["penalty_aud"] for c in compliance)
    open_inv = sum(1 for n in notices if n["status"] in ("open", "under_review"))

    return {
        "timestamp": now.isoformat(),
        "open_investigations": open_inv,
        "referred_to_aer_ytd": referred,
        "total_penalties_ytd_aud": round(total_penalties, 0),
        "participants_under_review": len(set(n["participant"] for n in notices if n["status"] in ("open", "under_review"))),
        "notices": notices,
        "compliance_records": compliance,
        "anomalies": anomalies,
    }


@router.get("/api/market-notices", summary="AEMO market notices", tags=["Live Ops"])
async def market_notices():
    """Return list of recent AEMO market notices."""
    # Try real market notice data
    rows = _query_gold(f"""
        SELECT notice_id, notice_type, category, region, reason,
               effective_date, external_reference
        FROM {_CATALOG}.gold.nem_market_notices
        ORDER BY effective_date DESC
        LIMIT 20
    """)
    if rows and len(rows) >= 3:
        notices = []
        for r in rows:
            cat = (r.get("category") or "OTHER").upper()
            sev = "critical" if cat in ("SYSTEM_SECURITY", "MARKET_INTERVENTION") else "warning" if cat == "CONSTRAINT" else "info"
            notices.append({
                "id": f"MN-{r['notice_id']}",
                "type": r.get("notice_type") or cat,
                "severity": sev,
                "message": (r.get("reason") or "")[:200],
                "issued_at": str(r.get("effective_date") or "").replace(" ", "T"),
                "region": r.get("region") or "NEM",
            })
        return notices

    # Mock fallback
    rng = random.Random(int(time.time() // 30))
    now = datetime.now(timezone.utc)
    notice_types = [
        ("PRICES UNCHANGED", "Inter-regional prices unchanged for {region}"),
        ("MARKET INTERVENTION", "AEMO intervention event in {region}"),
        ("RECLASSIFICATION", "Non-credible contingency reclassified as credible: {region} interconnector"),
        ("GENERAL NOTICE", "Updated demand forecast for {region} trading interval"),
        ("CONSTRAINTS", "Constraint set invoked: {region}_XFER_LIMIT"),
        ("RESERVE NOTICE", "Lack of reserve level 1 (LOR1) in {region}"),
        ("MARKET SYSTEMS", "Market system update — scheduled maintenance window"),
    ]
    notices = []
    for i in range(rng.randint(5, 12)):
        ntype, template = rng.choice(notice_types)
        region = rng.choice(_NEM_REGIONS)
        notices.append({
            "id": f"MN-{50000 + rng.randint(0, 9999)}",
            "type": ntype,
            "severity": rng.choice(["info", "warning", "critical"]),
            "message": template.format(region=region),
            "issued_at": (now - timedelta(minutes=rng.uniform(5, 1440))).isoformat(),
            "region": region,
        })
    notices.sort(key=lambda x: x["issued_at"], reverse=True)
    return notices


# --- 14. Live Ops Status ---
@router.get("/api/live-ops/status", summary="System status overview", tags=["Live Ops"])
async def live_ops_status():
    """Return system status overview."""
    rng = random.Random(int(time.time() // 30))
    now = datetime.now(timezone.utc)
    services = ["AEMO MMS", "AEMO NEMWeb", "AEMO NEMDE", "Price Feed", "Generation Feed", "Interconnector Feed", "Forecast Engine"]
    statuses = []
    for svc in services:
        up = rng.random() > 0.08
        statuses.append({
            "service": svc,
            "status": "operational" if up else rng.choice(["degraded", "outage"]),
            "latency_ms": round(rng.uniform(15, 250), 0) if up else None,
            "last_check": (now - timedelta(seconds=rng.randint(5, 120))).isoformat(),
        })
    # Get real notice count from last 24h
    notice_count_rows = _query_gold(f"""
        SELECT COUNT(*) AS cnt
        FROM {_CATALOG}.gold.nem_market_notices
        WHERE effective_date >= current_timestamp() - INTERVAL 24 HOURS
    """)
    active_notices = int((notice_count_rows[0] if notice_count_rows else {}).get("cnt") or rng.randint(2, 8))

    return {
        "overall": "operational" if all(s["status"] == "operational" for s in statuses) else "degraded",
        "services": statuses,
        "active_market_notices": active_notices,
        "nem_trading_interval": now.strftime("%Y-%m-%dT%H:%M:00Z"),
    }


# =========================================================================
# ADDITIONAL SIDEBAR ENDPOINTS (missing from initial deployment)
# =========================================================================

# --- Market Notices (frontend calls /api/market/notices) ---
@router.get("/api/market/notices", summary="Market notices (frontend path)", tags=["Live Ops"])
async def market_notices_frontend(severity: str = Query(None), notice_type: str = Query(None), limit: int = Query(20)):
    """Return MarketNotice[] matching frontend interface."""
    safe_limit = min(max(1, limit), 50)

    # Try real market notice data
    rows = _query_gold(f"""
        SELECT notice_id, notice_type, category, region, reason,
               effective_date, external_reference
        FROM {_CATALOG}.gold.nem_market_notices
        ORDER BY effective_date DESC
        LIMIT {safe_limit * 2}
    """)
    if rows and len(rows) >= 3:
        notices = []
        for r in rows:
            cat = (r.get("category") or "OTHER").upper()
            ntype = r.get("notice_type") or cat
            sev = "critical" if cat in ("SYSTEM_SECURITY", "MARKET_INTERVENTION") else "warning" if cat == "CONSTRAINT" else "info"
            reg = r.get("region")
            regions_affected = [reg] if reg else []

            if severity and sev != severity:
                continue
            if notice_type and ntype != notice_type:
                continue

            notices.append({
                "notice_id": f"MN-{r['notice_id']}",
                "notice_type": ntype,
                "creation_date": str(r.get("effective_date") or "").replace(" ", "T"),
                "external_reference": r.get("external_reference") or "",
                "reason": (r.get("reason") or "")[:300],
                "regions_affected": regions_affected,
                "severity": sev,
                "resolved": True,
            })
            if len(notices) >= safe_limit:
                break
        return notices

    # Mock fallback
    rng = random.Random(int(time.time() // 30))
    now = datetime.now(timezone.utc)
    notice_types = [
        ("PRICES UNCHANGED", "Inter-regional prices unchanged"),
        ("MARKET INTERVENTION", "AEMO intervention event"),
        ("RECLASSIFICATION", "Non-credible contingency reclassified as credible"),
        ("GENERAL NOTICE", "Updated demand forecast for trading interval"),
        ("CONSTRAINTS", "Constraint set invoked"),
        ("RESERVE NOTICE", "Lack of reserve level (LOR) declared"),
        ("MARKET SYSTEMS", "Market system update — scheduled maintenance window"),
    ]
    severities = ["info", "warning", "critical"]
    notices = []
    for i in range(min(limit, rng.randint(8, 18))):
        ntype_label, reason_base = rng.choice(notice_types)
        affected = rng.sample(_NEM_REGIONS, rng.randint(1, 3))
        sev = rng.choice(severities)
        if severity and sev != severity:
            continue
        if notice_type and ntype_label != notice_type:
            continue
        notices.append({
            "notice_id": f"MN-{50000 + rng.randint(0, 9999)}",
            "notice_type": ntype_label,
            "creation_date": (now - timedelta(minutes=rng.uniform(5, 2880))).isoformat(),
            "external_reference": f"AEMO-{rng.randint(10000, 99999)}",
            "reason": f"{reason_base} for {', '.join(affected)}",
            "regions_affected": affected,
            "severity": sev,
            "resolved": rng.random() > 0.4,
        })
    notices.sort(key=lambda x: x["creation_date"], reverse=True)
    return notices


# --- Dispatch Intervals ---
@router.get("/api/dispatch/intervals", summary="Dispatch interval analysis", tags=["Market Data"])
async def dispatch_intervals(region: str = Query("NSW1"), count: int = Query(12)):
    """Return DispatchSummary matching frontend interface."""
    if region not in _NEM_REGIONS:
        region = "NSW1"
    safe_count = min(max(1, count), 288)

    # Try real data
    rows = _query_gold(f"""
        SELECT interval_datetime, rrp, total_demand_mw, available_gen_mw, net_interchange_mw
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE region_id = '{region}'
        ORDER BY interval_datetime DESC
        LIMIT {safe_count}
    """)
    if rows and len(rows) >= 2:
        rows.reverse()  # chronological order
        intervals = []
        deviations = []
        for i, r in enumerate(rows):
            rrp = round(float(r["rrp"]), 2)
            # Use previous interval as pseudo pre-dispatch estimate
            pd_rrp = round(float(rows[i - 1]["rrp"]), 2) if i > 0 else rrp
            dev = round(rrp - pd_rrp, 2)
            deviations.append(abs(dev))
            intervals.append({
                "interval_datetime": str(r["interval_datetime"]).replace(" ", "T"),
                "region": region,
                "rrp": rrp,
                "predispatch_rrp": pd_rrp,
                "rrp_deviation": dev,
                "totaldemand": round(float(r.get("total_demand_mw") or 0), 0),
                "dispatchablegeneration": round(float(r.get("available_gen_mw") or 0), 0),
                "net_interchange": round(float(r.get("net_interchange_mw") or 0), 0),
                "lower_reg_mw": 0,
            })
        max_dev = max(deviations) if deviations else 0
        base_price = float(rows[-1]["rrp"]) if rows else 70
        surprise_count = sum(1 for d in deviations if d > max(base_price * 0.1, 10))
        return {
            "region": region,
            "intervals": intervals,
            "mean_deviation": round(sum(deviations) / len(deviations), 2) if deviations else 0,
            "max_surprise": round(max_dev, 2),
            "surprise_intervals": surprise_count,
        }

    # Fallback to mock
    rng = random.Random(hash(region) + int(time.time() // 30))
    now = datetime.now(timezone.utc)
    base_price = _REGION_BASE_PRICES.get(region, 70.0)
    intervals = []
    deviations = []
    for i in range(safe_count):
        ts = now - timedelta(minutes=5 * (safe_count - i))
        rrp = round(base_price + rng.gauss(0, base_price * 0.15), 2)
        pd_rrp = round(rrp + rng.gauss(0, 10), 2)
        dev = round(rrp - pd_rrp, 2)
        deviations.append(abs(dev))
        intervals.append({
            "interval_datetime": ts.isoformat(),
            "region": region,
            "rrp": rrp,
            "predispatch_rrp": pd_rrp,
            "rrp_deviation": dev,
            "totaldemand": round(rng.uniform(5000, 10000), 0),
            "dispatchablegeneration": round(rng.uniform(6000, 12000), 0),
            "net_interchange": round(rng.uniform(-500, 500), 0),
            "lower_reg_mw": round(rng.uniform(100, 400), 0),
        })
    max_dev = max(deviations) if deviations else 0
    surprise_count = sum(1 for d in deviations if d > base_price * 0.1)
    return {
        "region": region,
        "intervals": intervals,
        "mean_deviation": round(sum(deviations) / len(deviations), 2) if deviations else 0,
        "max_surprise": round(max_dev, 2),
        "surprise_intervals": surprise_count,
    }


# --- Settlement Summary ---
@router.get("/api/settlement/summary", summary="Settlement records", tags=["Market Data"])
async def settlement_summary():
    """Return SettlementRecord[] matching frontend interface."""
    # Try real data — last 6 intervals per region
    rows = _query_gold(f"""
        WITH ranked AS (
            SELECT region_id, interval_datetime, rrp, total_demand_mw, net_interchange_mw,
                   ROW_NUMBER() OVER (PARTITION BY region_id ORDER BY interval_datetime DESC) AS rn
            FROM {_CATALOG}.gold.nem_prices_5min
        )
        SELECT region_id, interval_datetime, rrp, total_demand_mw, net_interchange_mw
        FROM ranked WHERE rn <= 6
        ORDER BY region_id, interval_datetime
    """)
    if rows and len(rows) > 5:
        rng = random.Random(42)
        records = []
        for r in rows:
            records.append({
                "trading_interval": str(r["interval_datetime"]).replace(" ", "T"),
                "region": r["region_id"],
                "totaldemand_mw": round(float(r.get("total_demand_mw") or 0), 0),
                "net_interchange_mw": round(float(r.get("net_interchange_mw") or 0), 0),
                "rrp_aud_mwh": round(float(r["rrp"]), 2),
                "raise_reg_rrp": round(rng.uniform(5, 25), 2),
                "lower_reg_rrp": round(rng.uniform(3, 15), 2),
                "raise6sec_rrp": round(rng.uniform(2, 20), 2),
                "lower6sec_rrp": round(rng.uniform(1, 12), 2),
            })
        return records

    # Fallback to mock
    rng = random.Random(int(time.time() // 30))
    now = datetime.now(timezone.utc)
    records = []
    for region in _NEM_REGIONS:
        base = _REGION_BASE_PRICES.get(region, 70.0)
        for i in range(6):
            ts = now - timedelta(minutes=30 * (6 - i))
            records.append({
                "trading_interval": ts.isoformat(),
                "region": region,
                "totaldemand_mw": round(rng.uniform(4000, 12000), 0),
                "net_interchange_mw": round(rng.uniform(-600, 600), 0),
                "rrp_aud_mwh": round(base + rng.gauss(0, base * 0.2), 2),
                "raise_reg_rrp": round(rng.uniform(5, 25), 2),
                "lower_reg_rrp": round(rng.uniform(3, 15), 2),
                "raise6sec_rrp": round(rng.uniform(2, 20), 2),
                "lower6sec_rrp": round(rng.uniform(1, 12), 2),
            })
    return records


# --- Alert History ---
@router.get("/api/alerts/history", summary="Alert trigger history", tags=["Alerts"])
async def alert_history(region: str = Query(None), hours_back: int = Query(24)):
    """Return AlertTriggerEvent[] matching frontend interface — real NEMWEB data with mock fallback."""
    try:
        region_filter = f"AND region_id = '{region}'" if region else ""
        # Price anomaly history
        price_events = _query_gold(f"""
            SELECT region_id, rrp, interval_datetime
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL {hours_back} HOURS
              AND (rrp > 300 OR rrp < 0)
              {region_filter}
            ORDER BY interval_datetime DESC
            LIMIT 30
        """)
        # Congestion history
        ic_filter = f"AND from_region = '{region}'" if region else ""
        congestion_events = _query_gold(f"""
            SELECT interconnector_id, from_region, to_region, mw_flow,
                   utilization_pct, interval_datetime
            FROM {_CATALOG}.gold.nem_interconnectors
            WHERE interval_datetime >= current_timestamp() - INTERVAL {hours_back} HOURS
              AND is_congested = true
              {ic_filter}
            ORDER BY interval_datetime DESC
            LIMIT 15
        """)

        events = []
        idx = 0
        if price_events:
            for r in price_events:
                idx += 1
                price = float(r["rrp"])
                if price > 1000:
                    atype = "EXTREME_PRICE"
                    threshold = 1000.0
                elif price > 300:
                    atype = "PRICE_SPIKE"
                    threshold = 300.0
                else:
                    atype = "NEGATIVE_PRICE"
                    threshold = 0.0
                events.append({
                    "event_id": f"ATE-{idx:04d}",
                    "alert_id": f"ALT-{idx:03d}",
                    "triggered_at": str(r["interval_datetime"]).replace(" ", "T"),
                    "resolved_at": str(r["interval_datetime"]).replace(" ", "T"),
                    "region": r["region_id"],
                    "alert_type": atype,
                    "threshold": threshold,
                    "actual_value": price,
                    "notification_sent": True,
                    "channel": "SLACK",
                })
        if congestion_events:
            for r in congestion_events:
                idx += 1
                flow = float(r["mw_flow"])
                util = float(r["utilization_pct"]) if r.get("utilization_pct") else None
                events.append({
                    "event_id": f"ATE-{idx:04d}",
                    "alert_id": f"ALT-{idx:03d}",
                    "triggered_at": str(r["interval_datetime"]).replace(" ", "T"),
                    "resolved_at": str(r["interval_datetime"]).replace(" ", "T"),
                    "region": r["from_region"],
                    "alert_type": "CONGESTION",
                    "threshold": 95.0,
                    "actual_value": util if util else flow,
                    "notification_sent": True,
                    "channel": "EMAIL",
                })

        if events:
            events.sort(key=lambda x: x["triggered_at"], reverse=True)
            logger.info(f"alert_history: {len(events)} real events (hours_back={hours_back}, region={region})")
            return events
    except Exception as exc:
        logger.warning(f"alert_history real-data failed: {exc}")

    # --- Mock fallback ---
    rng = random.Random(int(time.time() // 30))
    now = datetime.now(timezone.utc)
    alert_types = ["PRICE_SPIKE", "EXTREME_PRICE", "NEGATIVE_PRICE", "LOW_RESERVE", "CONGESTION"]
    events = []
    for i in range(rng.randint(5, 15)):
        r = region if region else rng.choice(_NEM_REGIONS)
        at = rng.choice(alert_types)
        threshold = round(rng.uniform(100, 1000), 0)
        events.append({
            "event_id": f"ATE-{i + 1:04d}",
            "alert_id": f"ALT-{rng.randint(1, 12):03d}",
            "triggered_at": (now - timedelta(hours=rng.uniform(0, hours_back))).isoformat(),
            "resolved_at": (now - timedelta(hours=rng.uniform(0, hours_back * 0.8))).isoformat(),
            "region": r,
            "alert_type": at,
            "threshold": threshold,
            "actual_value": round(threshold * rng.uniform(1.01, 1.5), 2),
            "notification_sent": rng.random() > 0.1,
            "channel": rng.choice(["SLACK", "EMAIL", "IN_APP"]),
        })
    events.sort(key=lambda x: x["triggered_at"], reverse=True)
    return events


# --- Forecast Summary ---
@router.get("/api/forecasts/summary", summary="Forecast model summary", tags=["Forecasts"])
async def forecast_summary():
    """Return ForecastSummary matching frontend interface."""
    try:
        # Compare price_forecasts vs actual nem_prices_5min for MAPE calculation
        mape_rows = _query_gold(f"""
            WITH actuals AS (
                SELECT region_id, interval_datetime, rrp
                FROM {_CATALOG}.gold.nem_prices_5min
                WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
            ),
            forecasts AS (
                SELECT region_id, target_datetime, predicted_price
                FROM {_CATALOG}.gold.price_forecasts
                WHERE target_datetime >= current_timestamp() - INTERVAL 24 HOURS
                  AND horizon_hours IN (1, 4, 24)
            )
            SELECT f.region_id,
                   f.horizon_hours,
                   ROUND(AVG(ABS(f.predicted_price - a.rrp) / NULLIF(ABS(a.rrp), 0) * 100), 1) AS mape
            FROM (
                SELECT region_id, target_datetime, predicted_price, horizon_hours
                FROM {_CATALOG}.gold.price_forecasts
                WHERE target_datetime >= current_timestamp() - INTERVAL 24 HOURS
                  AND horizon_hours IN (1, 4, 24)
            ) f
            JOIN actuals a ON f.region_id = a.region_id
              AND ABS(UNIX_TIMESTAMP(f.target_datetime) - UNIX_TIMESTAMP(a.interval_datetime)) < 300
            GROUP BY f.region_id, f.horizon_hours
        """)
        if mape_rows and len(mape_rows) >= 2:
            price_mape = {}
            for r in mape_rows:
                h = int(r.get("horizon_hours", 0))
                m = float(r.get("mape", 0))
                if h not in price_mape or m > 0:
                    price_mape[h] = price_mape.get(h, [])
                    price_mape[h].append(m)
            avg_1 = round(sum(price_mape.get(1, [8])) / max(len(price_mape.get(1, [1])), 1), 1)
            avg_4 = round(sum(price_mape.get(4, [12])) / max(len(price_mape.get(4, [1])), 1), 1)
            avg_24 = round(sum(price_mape.get(24, [20])) / max(len(price_mape.get(24, [1])), 1), 1)
            all_mape = [m for ms in price_mape.values() for m in ms]
            avg_conf = round(max(0.5, 1.0 - sum(all_mape) / len(all_mape) / 100), 2) if all_mape else 0.82
            logger.info(f"forecast_summary: real MAPE 1h={avg_1}, 4h={avg_4}, 24h={avg_24}")
            return {
                "regions": list(_NEM_REGIONS),
                "horizons_available": [1, 4, 12, 24, 48],
                "models_loaded": 12,
                "avg_confidence": avg_conf,
                "price_mape_1hr": avg_1,
                "price_mape_4hr": avg_4,
                "price_mape_24hr": avg_24,
                "demand_mape_1hr": round(avg_1 * 0.5, 1),
                "demand_mape_4hr": round(avg_4 * 0.5, 1),
                "demand_mape_24hr": round(avg_24 * 0.5, 1),
                "last_evaluation": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as exc:
        logger.warning(f"forecast_summary real-data failed: {exc}")

    # Mock fallback
    rng = random.Random(int(time.time() // 300))
    return {
        "regions": list(_NEM_REGIONS),
        "horizons_available": [1, 4, 12, 24, 48],
        "models_loaded": rng.randint(8, 15),
        "avg_confidence": round(rng.uniform(0.72, 0.92), 2),
        "price_mape_1hr": round(rng.uniform(3, 8), 1),
        "price_mape_4hr": round(rng.uniform(6, 14), 1),
        "price_mape_24hr": round(rng.uniform(12, 25), 1),
        "demand_mape_1hr": round(rng.uniform(1.5, 4), 1),
        "demand_mape_4hr": round(rng.uniform(3, 7), 1),
        "demand_mape_24hr": round(rng.uniform(5, 12), 1),
        "last_evaluation": datetime.now(timezone.utc).isoformat(),
    }


# --- Forecast Accuracy ---
@router.get("/api/forecasts/accuracy", summary="Forecast accuracy by region", tags=["Forecasts"])
async def forecast_accuracy(region: str = Query("NSW1")):
    """Return AccuracyRow[] matching frontend interface: [{horizon, mae, mape}]."""
    try:
        rows = _query_gold(f"""
            WITH joined AS (
                SELECT f.horizon_hours,
                       f.predicted_price,
                       a.rrp
                FROM {_CATALOG}.gold.price_forecasts f
                JOIN {_CATALOG}.gold.nem_prices_5min a
                  ON f.region_id = a.region_id
                  AND ABS(UNIX_TIMESTAMP(f.target_datetime) - UNIX_TIMESTAMP(a.interval_datetime)) < 300
                WHERE f.region_id = '{_sql_escape(region)}'
                  AND f.target_datetime >= current_timestamp() - INTERVAL 48 HOURS
                  AND f.horizon_hours IN (1, 4, 24)
            )
            SELECT horizon_hours,
                   ROUND(AVG(ABS(predicted_price - rrp)), 1) AS mae,
                   ROUND(AVG(ABS(predicted_price - rrp) / NULLIF(ABS(rrp), 0) * 100), 1) AS mape
            FROM joined
            GROUP BY horizon_hours
            ORDER BY horizon_hours
        """)
        if rows and len(rows) >= 2:
            horizon_map = {1: "1hr", 4: "4hr", 24: "24hr"}
            result = []
            for r in rows:
                h = int(r.get("horizon_hours", 0))
                label = horizon_map.get(h)
                if label:
                    result.append({
                        "horizon": label,
                        "mae": float(r.get("mae", 0)),
                        "mape": float(r.get("mape", 0)),
                    })
            if result:
                logger.info(f"forecast_accuracy: {len(result)} real horizons for {region}")
                return result
    except Exception as exc:
        logger.warning(f"forecast_accuracy real-data failed: {exc}")

    # Mock fallback
    rng = random.Random(hash(region) + int(time.time() // 300))
    return [
        {"horizon": "1hr",  "mae": round(8 + rng.uniform(-2, 4), 1),  "mape": round(6 + rng.uniform(-1, 4), 1)},
        {"horizon": "4hr",  "mae": round(12 + rng.uniform(-2, 4), 1), "mape": round(10 + rng.uniform(-1, 4), 1)},
        {"horizon": "24hr", "mae": round(17 + rng.uniform(-3, 5), 1), "mape": round(15 + rng.uniform(-2, 4), 1)},
    ]


# --- Constraints ---
@router.get("/api/constraints", summary="Binding constraints", tags=["Market Data"])
async def constraints(region: str = Query("NSW1"), hours_back: int = Query(24), binding_only: str = Query("true")):
    """Return ConstraintRecord[] matching frontend interface."""
    safe_hours = min(max(1, hours_back), 168)
    binding_filter = "AND is_binding = true" if binding_only == "true" else ""
    region_filter = f"AND region = '{region}'" if region != "NEM" else ""

    try:
        rows = _query_gold(f"""
            SELECT interval_datetime, constraint_id, constraint_type,
                   rhs, marginal_value, violation_degree, is_binding, region
            FROM {_CATALOG}.nemweb_analytics.gold_nem_constraints
            WHERE interval_datetime >= current_timestamp() - INTERVAL {safe_hours} HOURS
              {binding_filter} {region_filter}
            ORDER BY interval_datetime DESC
            LIMIT 50
        """)
    except Exception:
        rows = None

    if rows and len(rows) >= 2:
        return [
            {
                "interval_datetime": str(r.get("interval_datetime") or "").replace(" ", "T"),
                "constraintid": r.get("constraint_id") or "",
                "constraint_type": r.get("constraint_type") or "SYSTEM",
                "rhs": round(float(r.get("rhs") or 0), 0),
                "marginalvalue": round(float(r.get("marginal_value") or 0), 2),
                "violationdegree": round(float(r.get("violation_degree") or 0), 2),
            }
            for r in rows
        ]

    # Mock fallback
    rng = random.Random(hash(region) + int(time.time() // 30))
    now = datetime.now(timezone.utc)
    constraint_names = [
        f"{region}_XFER_LIMIT", f"N_Q_MNSP1_LIMIT", f"V_SA_FLOW_LIMIT",
        f"SYSTEM_NORMAL_{region}", f"N_NIL_CLWP_LIMIT", f"V_T_FLOW_LIMIT",
    ]
    records = []
    for i in range(rng.randint(4, 10)):
        ts = now - timedelta(hours=rng.uniform(0, hours_back))
        mv = round(rng.uniform(0, 150), 2)
        records.append({
            "interval_datetime": ts.isoformat(),
            "constraintid": rng.choice(constraint_names),
            "rhs": round(rng.uniform(200, 1500), 0),
            "marginalvalue": mv,
            "violationdegree": round(rng.uniform(0, 20), 2) if mv > 50 else 0.0,
        })
    return records


# --- FCAS Market (E7 — wired to real data) ---
_FCAS_SERVICES = [
    {"service": "RAISE6SEC", "service_name": "Raise 6-Second", "direction": "RAISE", "type": "CONTINGENCY"},
    {"service": "RAISE60SEC", "service_name": "Raise 60-Second", "direction": "RAISE", "type": "CONTINGENCY"},
    {"service": "RAISE5MIN", "service_name": "Raise 5-Minute", "direction": "RAISE", "type": "CONTINGENCY"},
    {"service": "RAISEREG", "service_name": "Raise Regulation", "direction": "RAISE", "type": "REGULATION"},
    {"service": "LOWER6SEC", "service_name": "Lower 6-Second", "direction": "LOWER", "type": "CONTINGENCY"},
    {"service": "LOWER60SEC", "service_name": "Lower 60-Second", "direction": "LOWER", "type": "CONTINGENCY"},
    {"service": "LOWER5MIN", "service_name": "Lower 5-Minute", "direction": "LOWER", "type": "CONTINGENCY"},
    {"service": "LOWERREG", "service_name": "Lower Regulation", "direction": "LOWER", "type": "REGULATION"},
]
_FCAS_CAPABLE_FUELS = ("battery", "hydro", "gas_ocgt", "gas_ccgt", "gas_steam", "gas_recip")
_FCAS_PRICE_FACTORS = {
    "RAISE6SEC": (0.15, 0.25), "RAISE60SEC": (0.10, 0.20), "RAISE5MIN": (0.08, 0.15),
    "RAISEREG": (0.20, 0.35), "LOWER6SEC": (0.09, 0.15), "LOWER60SEC": (0.06, 0.12),
    "LOWER5MIN": (0.05, 0.10), "LOWERREG": (0.12, 0.22),
}


def _build_fcas_dashboard(region: str = "NSW1") -> Dict:
    """Build FCAS market dashboard from real gold table data."""
    now = datetime.now(timezone.utc)

    # Get spot price and volatility to derive FCAS service prices
    price_data = _query_gold(f"""
        SELECT AVG(rrp) AS avg_price, STDDEV(rrp) AS price_vol,
               MAX(rrp) AS max_price, MIN(rrp) AS min_price,
               AVG(total_demand_mw) AS avg_demand
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE region_id = '{region}'
          AND interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
    """)

    # Get FCAS-capable facilities
    facility_rows = _query_gold(f"""
        SELECT duid, station_name, fuel_type, region_id AS region, capacity_mw
        FROM {_CATALOG}.gold.nem_facilities
        WHERE LOWER(fuel_type) IN ({','.join(repr(f) for f in _FCAS_CAPABLE_FUELS)})
          AND region_id = '{region}'
        ORDER BY capacity_mw DESC
        LIMIT 30
    """)

    # Get latest generation from FCAS-capable units
    gen_rows = _query_gold(f"""
        SELECT fuel_type, SUM(total_mw) AS total_mw
        FROM {_CATALOG}.gold.nem_generation_by_fuel
        WHERE region_id = '{region}'
          AND LOWER(fuel_type) IN ({','.join(repr(f) for f in _FCAS_CAPABLE_FUELS)})
          AND interval_datetime = (SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_generation_by_fuel WHERE region_id = '{region}')
        GROUP BY fuel_type
    """)

    rng = random.Random(hash(region) + int(time.time() // 60))

    if price_data and price_data[0].get("avg_price"):
        avg_price = float(price_data[0]["avg_price"])
        price_vol = float(price_data[0].get("price_vol") or 20)
        max_price = float(price_data[0].get("max_price") or avg_price * 2)
        min_price = float(price_data[0].get("min_price") or 0)
        avg_demand = float(price_data[0].get("avg_demand") or 7000)
    else:
        avg_price = _REGION_BASE_PRICES.get(region, 70)
        price_vol = 20
        max_price = avg_price * 2
        min_price = max(0, avg_price * 0.3)
        avg_demand = 7000

    # Volatility factor: higher vol → higher FCAS prices
    vol_factor = 1 + min(price_vol / 100, 1.5)

    # Try real FCAS prices from gold_nem_fcas_prices (nemweb-accelerator pipeline)
    real_fcas_prices: Dict[str, Dict[str, float]] = {}  # service -> {avg, max, min}
    try:
        _fcas_rows = _query_gold(f"""
            SELECT service,
                   AVG(clearing_price_aud_mw) AS avg_price,
                   MAX(clearing_price_aud_mw) AS max_price,
                   MIN(clearing_price_aud_mw) AS min_price
            FROM {_CATALOG}.nemweb_analytics.gold_nem_fcas_prices
            WHERE region_id = '{region}'
              AND interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
            GROUP BY service
        """)
        if _fcas_rows:
            for r in _fcas_rows:
                svc_name = r.get("service", "")
                if svc_name:
                    real_fcas_prices[svc_name] = {
                        "avg": float(r.get("avg_price") or 0),
                        "max": float(r.get("max_price") or 0),
                        "min": float(r.get("min_price") or 0),
                    }
    except Exception:
        pass  # Fall back to heuristic derivation

    # Derive 8 FCAS service prices
    services = []
    total_fcas_cost = 0
    regulation_cost = 0
    contingency_cost = 0
    total_enabled = 0

    for svc in _FCAS_SERVICES:
        # Use real FCAS price if available, else derive heuristically
        real = real_fcas_prices.get(svc["service"])
        if real and real["avg"] > 0:
            clearing = round(real["avg"], 2)
        else:
            lo, hi = _FCAS_PRICE_FACTORS[svc["service"]]
            base_clearing = avg_price * rng.uniform(lo, hi) * vol_factor
            clearing = round(max(1.0, base_clearing), 2)

        # Requirements based on demand
        if svc["direction"] == "RAISE":
            req_mw = round(avg_demand * rng.uniform(0.04, 0.06), 0)
        else:
            req_mw = round(avg_demand * rng.uniform(0.025, 0.04), 0)

        enabled_mw = round(req_mw * rng.uniform(0.85, 1.15), 0)
        cost = clearing * enabled_mw
        total_fcas_cost += cost
        if svc["type"] == "REGULATION":
            regulation_cost += cost
        else:
            contingency_cost += cost
        total_enabled += enabled_mw

        # Top provider from facilities
        main_prov = "Unknown"
        if facility_rows:
            preferred = [f for f in facility_rows
                         if svc["type"] == "REGULATION" and f.get("fuel_type", "").lower() in ("battery", "hydro")]
            if not preferred:
                preferred = facility_rows[:5]
            main_prov = rng.choice(preferred)["station_name"] if preferred else "Unknown"

        real = real_fcas_prices.get(svc["service"])
        max_clearing = round(real["max"], 2) if real and real["max"] > 0 else round(clearing * rng.uniform(1.5, 4.0), 2)
        min_clearing = round(real["min"], 2) if real and real["min"] > 0 else round(clearing * rng.uniform(0.2, 0.6), 2)

        services.append({
            **svc,
            "clearing_price_aud_mw": clearing,
            "volume_mw": enabled_mw,
            "requirement_mw": req_mw,
            "enabled_mw": enabled_mw,
            "utilisation_pct": round(enabled_mw / max(req_mw, 1) * 100, 1),
            "max_clearing_today": max_clearing,
            "min_clearing_today": min_clearing,
            "main_provider": main_prov,
        })

    # Build provider list from facilities (match FcasProvider interface)
    providers = []
    if facility_rows:
        gen_by_fuel = {}
        if gen_rows:
            for g in gen_rows:
                gen_by_fuel[g["fuel_type"].lower()] = float(g["total_mw"] or 0)

        avg_svc_price = sum(s["clearing_price_aud_mw"] for s in services) / max(len(services), 1)
        for f in facility_rows[:20]:
            cap = float(f.get("capacity_mw") or 0)
            ft = str(f.get("fuel_type", "")).lower()
            if ft in ("battery", "hydro"):
                svc_enabled = ["RAISE6SEC", "RAISE60SEC", "RAISE5MIN", "RAISEREG",
                               "LOWER6SEC", "LOWER60SEC", "LOWER5MIN", "LOWERREG"]
            else:
                svc_enabled = ["RAISE6SEC", "RAISE60SEC", "RAISE5MIN", "LOWER6SEC", "LOWER60SEC", "LOWER5MIN"]

            raise_mw = round(cap * rng.uniform(0.3, 0.6), 0)
            lower_mw = round(cap * rng.uniform(0.2, 0.5), 0)
            reg_mw = round(cap * rng.uniform(0.1, 0.3), 0) if ft in ("battery", "hydro") else 0
            cont_mw = round(raise_mw + lower_mw - reg_mw, 0)
            rev_today = round((raise_mw + lower_mw) * avg_svc_price * rng.uniform(0.3, 0.7), 0)

            providers.append({
                "duid": f["duid"],
                "station_name": f["station_name"],
                "fuel_type": f.get("fuel_type", "unknown"),
                "region": f.get("region", region),
                "services_enabled": svc_enabled,
                "raise_mw": raise_mw,
                "lower_mw": lower_mw,
                "regulation_mw": reg_mw,
                "contingency_mw": cont_mw,
                "revenue_today_aud": rev_today,
                "cost_per_mw": round(avg_svc_price * rng.uniform(0.8, 1.3), 2),
            })

    # Trap records (match FcasTrapRecord interface)
    trap_records = []
    _trap_types = ["CAUSER_PAYS", "NON_CONFORMANCE", "SHORTFALL"]
    for i in range(rng.randint(1, 4)):
        prov = rng.choice(providers) if providers else None
        trap_records.append({
            "duid": prov["duid"] if prov else f"GEN-{i}",
            "station_name": prov["station_name"] if prov else "Unknown",
            "region": prov["region"] if prov else region,
            "service": rng.choice(["RAISE6SEC", "RAISEREG", "LOWER6SEC"]),
            "trap_type": rng.choice(_trap_types),
            "constraint_id": f"F_T_{region}_{rng.randint(1000, 9999)}",
            "mw_limited": round(rng.uniform(5, 40), 0),
            "revenue_foregone_est": round(rng.uniform(500, 10000), 0),
            "period": (now - timedelta(hours=rng.uniform(0, 48))).strftime("%Y-%m-%d %H:%M"),
        })

    # Regional requirements
    regional_req = []
    for r in _NEM_REGIONS:
        r_demand = avg_demand if r == region else avg_demand * rng.uniform(0.6, 1.2)
        regional_req.append({
            "region": r,
            "raise_req_mw": round(r_demand * 0.05, 0),
            "lower_req_mw": round(r_demand * 0.03, 0),
        })

    return {
        "timestamp": now.isoformat(),
        "total_fcas_cost_today_aud": round(total_fcas_cost, 0),
        "regulation_cost_aud": round(regulation_cost, 0),
        "contingency_cost_aud": round(contingency_cost, 0),
        "total_enabled_mw": round(total_enabled, 0),
        "shortfall_risk": "LOW" if total_enabled > avg_demand * 0.04 else "MEDIUM",
        "services": services,
        "providers": providers,
        "trap_records": trap_records,
        "regional_requirement": regional_req,
    }


@router.get("/api/fcas/market", summary="FCAS market dashboard", tags=["Market Data"])
async def fcas_market(region: str = Query("NSW1"), hours_back: int = Query(24)):
    """Return FcasMarketDashboard with 8 FCAS services, providers, traps, requirements."""
    if region not in _NEM_REGIONS:
        region = "NSW1"
    return _build_fcas_dashboard(region)


@router.get("/api/fcas/services", summary="FCAS service prices", tags=["Market Data"])
async def fcas_services_endpoint(region: str = Query("NSW1")):
    """Return FcasServicePrice[] — 8 FCAS services with clearing prices."""
    dash = _build_fcas_dashboard(region)
    return dash["services"]


@router.get("/api/fcas/providers", summary="FCAS providers", tags=["Market Data"])
async def fcas_providers_endpoint(
    region: str = Query(None),
    fuel_type: str = Query(None),
):
    """Return FcasProvider[] — FCAS-capable generators with enabled services."""
    r = region or "NSW1"
    dash = _build_fcas_dashboard(r)
    providers = dash["providers"]
    if fuel_type:
        providers = [p for p in providers if fuel_type.lower() in str(p.get("fuel_type", "")).lower()]
    return providers


# --- Settlement Reconciliation (E8) ---

def _build_settlement_reconciliation(days_back: int = 7) -> Dict:
    """Build settlement reconciliation — AEMO settlement vs internal trade positions."""
    now = datetime.now(timezone.utc)

    # AEMO settlement from gold tables
    energy_rows = _query_gold(f"""
        SELECT region_id,
               SUM(rrp * total_demand_mw * 5 / 60) AS energy_settlement_aud,
               SUM(total_demand_mw * 5 / 60) AS total_energy_mwh,
               COUNT(*) AS intervals
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE interval_datetime >= current_timestamp() - INTERVAL {days_back} DAYS
        GROUP BY region_id
    """)

    # FCAS settlement
    fcas_rows = _query_gold(f"""
        SELECT region_id,
               SUM(total_recovery_aud) AS fcas_aud
        FROM {_CATALOG}.gold.nem_settlement_summary
        GROUP BY region_id
    """)

    # SRA residues
    sra_rows = _query_gold(f"""
        SELECT interconnector_id,
               SUM(total_irsr_aud) AS irsr_aud,
               AVG(avg_mw_flow) AS avg_flow
        FROM {_CATALOG}.gold.nem_sra_residues
        GROUP BY interconnector_id
    """)

    # Internal trade positions
    trade_rows = _query_gold(f"""
        SELECT t.region, t.buy_sell,
               SUM(l.volume_mw * l.hours * l.price_aud_mwh) AS notional_aud
        FROM {_CATALOG}.gold.trades t
        JOIN {_CATALOG}.gold.trade_legs l ON t.trade_id = l.trade_id
        WHERE t.status IN ('CONFIRMED', 'SETTLED')
          AND l.start_date >= current_date() - INTERVAL {days_back} DAYS
        GROUP BY t.region, t.buy_sell
    """)

    # Build per-region reconciliation
    total_aemo = 0
    total_internal = 0
    variances_by_region = []

    if energy_rows:
        fcas_by_region = {}
        if fcas_rows:
            for f in fcas_rows:
                fcas_by_region[f["region_id"]] = abs(float(f.get("fcas_aud") or 0))

        trade_by_region = {}
        if trade_rows:
            for t in trade_rows:
                r = t["region"]
                val = float(t.get("notional_aud") or 0)
                if t["buy_sell"] == "BUY":
                    trade_by_region[r] = trade_by_region.get(r, 0) - val
                else:
                    trade_by_region[r] = trade_by_region.get(r, 0) + val

        for row in energy_rows:
            r = row["region_id"]
            aemo_energy = float(row.get("energy_settlement_aud") or 0)
            aemo_fcas = fcas_by_region.get(r, 0)
            aemo_total = aemo_energy + aemo_fcas
            internal = abs(trade_by_region.get(r, 0))

            total_aemo += aemo_total
            total_internal += internal
            variance = aemo_total - internal
            variance_pct = (variance / max(abs(aemo_total), 1)) * 100

            variances_by_region.append({
                "region": r,
                "aemo_aud": round(aemo_total, 2),
                "internal_aud": round(internal, 2),
                "variance_aud": round(variance, 2),
                "variance_pct": round(variance_pct, 2),
                "status": "BREACH" if abs(variance) > 1000 else "OK",
            })

    variance = total_aemo - total_internal
    threshold_breaches = sum(1 for v in variances_by_region if v["status"] == "BREACH")

    # Residues
    residues = []
    if sra_rows:
        for s in sra_rows:
            flow = float(s.get("avg_flow") or 0)
            irsr = float(s.get("irsr_aud") or 0)
            residues.append({
                "interconnector_id": s["interconnector_id"],
                "flow_mw": round(flow, 1),
                "price_differential": round(irsr / max(abs(flow) * 24 * days_back, 1), 2),
                "settlement_residue_aud": round(irsr, 2),
                "direction": "EXPORT" if flow >= 0 else "IMPORT",
            })

    return {
        "days_back": days_back,
        "total_aemo_settlement": round(total_aemo, 2),
        "total_internal_settlement": round(total_internal, 2),
        "variance": round(variance, 2),
        "variance_pct": round((variance / max(abs(total_aemo), 1)) * 100, 2),
        "variances_by_region": variances_by_region,
        "threshold_breaches": threshold_breaches,
        "residues": residues,
        "timestamp": now.isoformat(),
    }


@router.get("/api/settlement/reconciliation", summary="Settlement reconciliation", tags=["Settlement"])
async def settlement_reconciliation(days_back: int = Query(7, ge=1, le=90)):
    """AEMO settlement vs internal trade positions with variance analysis."""
    return _build_settlement_reconciliation(days_back)


@router.get("/api/settlement/residues", summary="Settlement residues", tags=["Settlement"])
async def settlement_residues(interconnector: str = Query(None)):
    """Inter-regional settlement residues from SRA data."""
    recon = _build_settlement_reconciliation(30)
    residues = recon.get("residues", [])
    if interconnector:
        residues = [r for r in residues if interconnector.lower() in r["interconnector_id"].lower()]
    return residues


# --- Settlement Disputes CRUD ---

@router.get("/api/settlement/disputes", summary="List settlement disputes", tags=["Settlement"])
async def list_disputes(
    region: str = Query(None),
    status: str = Query(None),
    limit: int = Query(50),
):
    """List settlement disputes, optionally filtered by region/status."""
    where_parts = []
    if region:
        where_parts.append(f"region = '{_sql_escape(region)}'")
    if status:
        where_parts.append(f"status = '{_sql_escape(status)}'")
    where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    rows = _query_gold(
        f"SELECT * FROM {_CATALOG}.gold.settlement_disputes "
        f"{where_clause} ORDER BY raised_at DESC LIMIT {min(limit, 200)}"
    )
    if rows:
        for r in rows:
            for k in ("settlement_date", "raised_at", "resolved_at"):
                if k in r and r[k] is not None:
                    r[k] = str(r[k])
        return {"disputes": rows, "count": len(rows)}
    return {"disputes": [], "count": 0}


@router.post("/api/settlement/disputes", summary="Create dispute", tags=["Settlement"])
async def create_dispute(
    region: str = Query(...),
    settlement_date: str = Query(...),
    dispute_type: str = Query(...),
    aemo_amount_aud: float = Query(...),
    internal_amount_aud: float = Query(...),
    description: str = Query(""),
    raised_by: str = Query("app_user"),
):
    """Create a new settlement dispute. Variance auto-computed."""
    dispute_id = str(uuid.uuid4())
    variance = round(aemo_amount_aud - internal_amount_aud, 2)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _insert_gold(f"{_CATALOG}.gold.settlement_disputes", {
        "dispute_id": dispute_id,
        "region": _sql_escape(region),
        "settlement_date": settlement_date,
        "dispute_type": _sql_escape(dispute_type),
        "aemo_amount_aud": aemo_amount_aud,
        "internal_amount_aud": internal_amount_aud,
        "variance_aud": variance,
        "status": "OPEN",
        "description": _sql_escape(description),
        "resolution_notes": "",
        "raised_by": _sql_escape(raised_by),
        "raised_at": now_str,
    })
    return {
        "status": "created",
        "dispute_id": dispute_id,
        "variance_aud": variance,
    }


@router.put("/api/settlement/disputes/{dispute_id}", summary="Update dispute", tags=["Settlement"])
async def update_dispute(
    dispute_id: str,
    status: str = Query(None),
    resolution_notes: str = Query(None),
):
    """Update a dispute status and/or resolution notes."""
    set_parts = []
    if status:
        set_parts.append(f"status = '{_sql_escape(status)}'")
        if status == "RESOLVED":
            set_parts.append(f"resolved_at = current_timestamp()")
    if resolution_notes is not None:
        set_parts.append(f"resolution_notes = '{_sql_escape(resolution_notes)}'")
    if not set_parts:
        return {"status": "no_changes"}
    _execute_gold(
        f"UPDATE {_CATALOG}.gold.settlement_disputes "
        f"SET {', '.join(set_parts)} "
        f"WHERE dispute_id = '{_sql_escape(dispute_id)}'"
    )
    return {"status": "updated", "dispute_id": dispute_id}


@router.get("/api/settlement/disputes/summary", summary="Dispute summary", tags=["Settlement"])
async def dispute_summary():
    """Counts by status + total variance."""
    rows = _query_gold(
        f"SELECT status, COUNT(*) AS cnt, SUM(ABS(variance_aud)) AS total_variance "
        f"FROM {_CATALOG}.gold.settlement_disputes "
        f"GROUP BY status"
    )
    summary = {}
    total_variance = 0.0
    total_count = 0
    if rows:
        for r in rows:
            s = r["status"]
            c = int(r["cnt"])
            v = float(r.get("total_variance") or 0)
            summary[s] = {"count": c, "total_variance_aud": round(v, 2)}
            total_variance += v
            total_count += c
    return {
        "by_status": summary,
        "total_disputes": total_count,
        "total_variance_aud": round(total_variance, 2),
    }


# --- DSP Dashboard ---
@router.get("/api/dsp/dashboard", summary="DSP dashboard", tags=["Demand Response"])
async def dsp_dashboard():
    """Return DspDashboard matching frontend interface."""
    rng = random.Random(int(time.time() // 30))
    now = datetime.now(timezone.utc)
    sectors = ["Aluminium Smelting", "Water Pumping", "Cold Storage", "Commercial HVAC", "Data Centres", "Steel Manufacturing"]
    programs = ["RERT", "WDR", "Reliability Panel", "ST-PASA"]
    participants = []
    for i in range(rng.randint(6, 12)):
        cap = round(rng.uniform(5, 200), 0)
        participants.append({
            "duid": f"DSP-{i + 1:03d}",
            "participant_name": f"{rng.choice(['AGL', 'Origin', 'EnergyAustralia', 'Enel X', 'Flow Power'])} - {rng.choice(sectors)}",
            "industry_sector": rng.choice(sectors),
            "region": rng.choice(_NEM_REGIONS),
            "registered_capacity_mw": cap,
            "response_time_minutes": rng.choice([5, 10, 15, 30, 60]),
            "dsp_program": rng.choice(programs),
            "min_activation_duration_hrs": rng.choice([1, 2, 4, 6]),
            "payment_type": rng.choice(["availability", "activation", "hybrid"]),
            "avg_activations_per_year": round(rng.uniform(2, 20), 1),
            "reliability_score_pct": round(rng.uniform(75, 99), 1),
        })
    activations = []
    for i in range(rng.randint(4, 10)):
        act_mw = round(rng.uniform(20, 300), 0)
        del_mw = round(act_mw * rng.uniform(0.7, 1.0), 0)
        activations.append({
            "event_id": f"DSPA-{i + 1:04d}",
            "date": (now - timedelta(days=rng.randint(0, 90))).strftime("%Y-%m-%d"),
            "region": rng.choice(_NEM_REGIONS),
            "trigger": rng.choice(["LOR2", "LOR3", "price_signal", "reliability_event"]),
            "activated_mw": act_mw,
            "delivered_mw": del_mw,
            "delivery_pct": round(del_mw / act_mw * 100, 1) if act_mw > 0 else 0,
            "duration_minutes": rng.randint(30, 240),
            "average_price_mwh": round(rng.uniform(300, 5000), 0),
            "participants_called": rng.randint(3, 30),
            "season": rng.choice(["summer", "winter", "shoulder"]),
        })
    curtailment = []
    for i in range(rng.randint(2, 5)):
        curtailment.append({
            "date": (now - timedelta(days=rng.randint(0, 180))).strftime("%Y-%m-%d"),
            "region": rng.choice(_NEM_REGIONS),
            "curtailment_type": rng.choice(["manual", "automatic", "rolling"]),
            "total_load_shed_mwh": round(rng.uniform(50, 500), 0),
            "customers_affected": rng.randint(1000, 50000),
            "duration_minutes": rng.randint(30, 360),
            "trigger_event": rng.choice(["extreme_heat", "generation_failure", "transmission_fault", "LOR3"]),
        })
    total_cap = sum(p["registered_capacity_mw"] for p in participants)
    return {
        "timestamp": now.isoformat(),
        "total_registered_capacity_mw": round(total_cap, 0),
        "total_participants": len(participants),
        "activations_ytd": len(activations),
        "total_delivered_mwh_ytd": round(sum(a["delivered_mw"] * a["duration_minutes"] / 60 for a in activations), 0),
        "avg_delivery_reliability_pct": round(sum(a["delivery_pct"] for a in activations) / len(activations), 1) if activations else 0,
        "top_sector_by_capacity": max(sectors, key=lambda s: sum(p["registered_capacity_mw"] for p in participants if p["industry_sector"] == s)),
        "participants": participants,
        "activations": activations,
        "curtailment_records": curtailment,
    }


# =========================================================================
# --- Realtime Operations Dashboard ---
@router.get("/api/realtime-ops/dashboard", summary="Realtime ops dashboard", tags=["Live Ops"])
async def realtime_ops_dashboard():
    """Return RealtimeOpsDashboard matching frontend interface."""
    rng = random.Random(int(time.time() // 30))
    now = datetime.now(timezone.utc)

    # --- Try real data for regions ---
    price_rows = _query_gold(f"""
        SELECT region_id, rrp, total_demand_mw, available_gen_mw, net_interchange_mw
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE interval_datetime = (SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_prices_5min)
    """)
    gen_rows = _query_gold(f"""
        SELECT region_id, fuel_type, total_mw
        FROM {_CATALOG}.gold.nem_generation_by_fuel
        WHERE interval_datetime = (SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_generation_by_fuel)
    """)
    ic_rows = _query_gold(f"""
        SELECT interconnector_id, from_region, to_region, mw_flow, export_limit_mw, mw_losses, utilization_pct
        FROM {_CATALOG}.gold.nem_interconnectors
        WHERE interval_datetime = (SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_interconnectors)
    """)

    # Build generation mix per region
    real_mix = {}
    if gen_rows:
        for g in gen_rows:
            rid = g["region_id"]
            ft = str(g["fuel_type"]).lower()
            mw = float(g["total_mw"] or 0)
            if rid not in real_mix:
                real_mix[rid] = {}
            real_mix[rid][ft] = real_mix[rid].get(ft, 0) + round(mw, 0)

    regions = []
    if price_rows:
        for r in price_rows:
            reg = r["region_id"]
            demand = float(r.get("total_demand_mw") or 0)
            gen = float(r.get("available_gen_mw") or demand)
            nic = float(r.get("net_interchange_mw") or 0)
            price = float(r["rrp"])
            mix = real_mix.get(reg, {})
            solar_mw = mix.get("solar", 0)
            regions.append({
                "region": reg,
                "timestamp": now.isoformat(),
                "total_demand_mw": round(demand, 0),
                "generation_mw": round(gen, 0),
                "rooftop_solar_mw": round(solar_mw * 0.3, 0),  # Estimate rooftop as ~30% of total solar
                "net_interchange_mw": round(nic, 0),
                "spot_price_aud_mwh": round(price, 2),
                "frequency_hz": round(50 + rng.gauss(0, 0.02), 3),
                "reserve_mw": round(max(0, gen - demand), 0),
                "generation_mix": mix,
            })

    if not regions:
        # Fallback to mock
        region_configs = {
            "NSW1": {"demand": 8500, "gen": 9200, "pv": 1800, "base_price": 85},
            "QLD1": {"demand": 6200, "gen": 6800, "pv": 2200, "base_price": 72},
            "VIC1": {"demand": 5800, "gen": 6100, "pv": 1200, "base_price": 58},
            "SA1":  {"demand": 1800, "gen": 2100, "pv": 900,  "base_price": 81},
            "TAS1": {"demand": 1100, "gen": 1300, "pv": 100,  "base_price": 42},
        }
        fuel_bases = {
            "NSW1": {"coal": 4800, "gas": 800, "wind": 1200, "solar": 1400, "hydro": 700, "battery": 300},
            "QLD1": {"coal": 3600, "gas": 600, "wind": 500,  "solar": 1500, "hydro": 200, "battery": 400},
            "VIC1": {"coal": 3200, "gas": 500, "wind": 1600, "solar": 400,  "hydro": 200, "battery": 200},
            "SA1":  {"gas": 500,   "wind": 900, "solar": 400, "battery": 300},
            "TAS1": {"hydro": 900, "wind": 250, "gas": 50,    "solar": 100},
        }
        for reg, cfg in region_configs.items():
            demand = round(cfg["demand"] + rng.gauss(0, cfg["demand"] * 0.04), 0)
            gen = round(cfg["gen"] + rng.gauss(0, cfg["gen"] * 0.03), 0)
            pv = round(cfg["pv"] * rng.uniform(0.3, 1.0), 0)
            price = round(cfg["base_price"] + rng.gauss(0, cfg["base_price"] * 0.2), 2)
            mix = {}
            for fuel, base_mw in fuel_bases.get(reg, {}).items():
                mix[fuel] = round(base_mw * rng.uniform(0.6, 1.2), 0)
            regions.append({
                "region": reg,
                "timestamp": now.isoformat(),
                "total_demand_mw": demand,
                "generation_mw": gen,
                "rooftop_solar_mw": pv,
                "net_interchange_mw": round(gen - demand, 0),
                "spot_price_aud_mwh": price,
                "frequency_hz": round(50 + rng.gauss(0, 0.02), 3),
                "reserve_mw": round(max(0, gen - demand + rng.uniform(200, 800)), 0),
                "generation_mix": mix,
            })

    # Interconnectors — real or mock
    interconnectors = []
    if ic_rows:
        for ic in ic_rows:
            flow = float(ic["mw_flow"])
            cap = float(ic.get("export_limit_mw") or 700)
            util = float(ic.get("utilization_pct") or (abs(flow) / cap * 100 if cap > 0 else 0))
            losses = float(ic.get("mw_losses") or abs(flow) * 0.03)
            interconnectors.append({
                "interconnector": ic["interconnector_id"],
                "from_region": ic.get("from_region") or "",
                "to_region": ic.get("to_region") or "",
                "flow_mw": round(flow, 1),
                "capacity_mw": round(cap, 0),
                "utilisation_pct": round(util, 1),
                "binding": util > 85,
                "marginal_loss": round(losses / max(abs(flow), 1), 4),
            })
    else:
        ic_defs = [
            {"ic": "N-Q-MNSP1",  "from": "NSW1", "to": "QLD1", "cap": 700,  "base": 280},
            {"ic": "VIC1-NSW1",   "from": "VIC1", "to": "NSW1", "cap": 1350, "base": 420},
            {"ic": "V-SA",        "from": "VIC1", "to": "SA1",  "cap": 680,  "base": -142},
            {"ic": "T-V-MNSP1",   "from": "TAS1", "to": "VIC1", "cap": 594,  "base": 87},
            {"ic": "Murraylink",   "from": "SA1",  "to": "VIC1", "cap": 220,  "base": 95},
        ]
        for ic in ic_defs:
            flow = round(ic["base"] * rng.uniform(0.5, 1.5), 1)
            util = round(abs(flow) / ic["cap"] * 100, 1)
            interconnectors.append({
                "interconnector": ic["ic"],
                "from_region": ic["from"],
                "to_region": ic["to"],
                "flow_mw": flow,
                "capacity_mw": ic["cap"],
                "utilisation_pct": util,
                "binding": util > 85,
                "marginal_loss": round(rng.uniform(0.005, 0.04), 4),
            })

    # FCAS — still mock (no NEMWEB source)
    fcas_services = ["RAISE_6SEC", "RAISE_60SEC", "RAISE_5MIN", "RAISE_REG",
                     "LOWER_6SEC", "LOWER_60SEC", "LOWER_5MIN", "LOWER_REG"]
    fcas = []
    for svc in fcas_services:
        req = round(rng.uniform(150, 500), 0)
        cleared = round(req * rng.uniform(0.9, 1.15), 0)
        surplus = round((cleared - req) / req * 100, 1)
        fcas.append({
            "service": svc,
            "cleared_mw": cleared,
            "clearing_price_aud_mw": round(rng.uniform(2, 60), 2),
            "requirement_mw": req,
            "surplus_pct": max(0, surplus),
        })

    # Alerts — derive from real price data
    alerts = []
    if price_rows:
        for r in price_rows:
            price = float(r["rrp"])
            reg = r["region_id"]
            if price > 300:
                alerts.append({
                    "alert_id": f"RTO-{hash(reg) % 90000 + 10000}",
                    "severity": "CRITICAL",
                    "category": "PRICE",
                    "message": f"Spot price ${price:.0f}/MWh in {reg}",
                    "region": reg,
                    "timestamp": now.isoformat(),
                    "acknowledged": False,
                })
            elif price < 0:
                alerts.append({
                    "alert_id": f"RTO-{hash(reg) % 90000 + 10000}",
                    "severity": "WARNING",
                    "category": "PRICE",
                    "message": f"Negative pricing ${price:.2f}/MWh in {reg}",
                    "region": reg,
                    "timestamp": now.isoformat(),
                    "acknowledged": False,
                })
    if not alerts:
        alerts.append({
            "alert_id": f"RTO-{rng.randint(10000, 99999)}",
            "severity": "INFO",
            "category": "MARKET",
            "message": "All regions within normal operating parameters",
            "region": "NEM",
            "timestamp": now.isoformat(),
            "acknowledged": True,
        })
    alerts.sort(key=lambda a: ({"CRITICAL": 0, "WARNING": 1, "INFO": 2}.get(a["severity"], 3), a["timestamp"]))

    return {
        "timestamp": now.isoformat(),
        "regions": regions,
        "interconnectors": interconnectors,
        "fcas": fcas,
        "alerts": alerts,
    }


# =========================================================================
# ALERT RULE CRUD — Feature 1: Multi-channel Alert Engine
# =========================================================================


class AlertRuleCreate(BaseModel):
    region: str = "NSW1"
    alert_type: str = "PRICE_THRESHOLD"
    threshold_value: float = 300.0
    notification_channel: str = "IN_APP"


def _create_alert_rule_core(data_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Core alert rule creation — used by both REST endpoint and Copilot tool."""
    rule_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "rule_id": rule_id,
        "region": data_dict.get("region", "NSW1"),
        "alert_type": data_dict.get("alert_type", "PRICE_THRESHOLD"),
        "threshold_value": float(data_dict.get("threshold_value", 300)),
        "notification_channel": data_dict.get("notification_channel", "IN_APP"),
        "is_active": 1,
        "created_at": now,
        "updated_at": now,
        "created_by": data_dict.get("created_by", "user"),
    }
    ok = _insert_gold(f"{_CATALOG}.gold.alert_rules", row)
    if ok:
        _invalidate_cache("sql:")
        return {"status": "created", "rule_id": rule_id, **row}
    return {"status": "error", "message": "Failed to persist alert rule"}


@router.post("/api/alerts", summary="Create alert rule", tags=["Alerts"])
async def create_alert_rule(body: AlertRuleCreate):
    """Persist a new alert rule to gold.alert_rules."""
    try:
        return _create_alert_rule_core(body.model_dump())
    except Exception as exc:
        logger.warning("Create alert rule failed: %s", exc)
        return {"status": "error", "message": str(exc)[:200]}


@router.delete("/api/alerts/{rule_id}", summary="Deactivate alert rule", tags=["Alerts"])
async def deactivate_alert_rule(rule_id: str):
    """Soft-delete: set is_active=false."""
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        ok = _update_gold(
            f"{_CATALOG}.gold.alert_rules",
            {"is_active": 0, "updated_at": f"'{now}'"},
            f"rule_id = '{_sql_escape(rule_id)}'",
        )
        _invalidate_cache("sql:")
        return {"status": "deactivated" if ok else "error", "rule_id": rule_id}
    except Exception as exc:
        logger.warning("Deactivate alert rule failed: %s", exc)
        return {"status": "error", "message": str(exc)[:200]}


@router.patch("/api/alerts/{rule_id}", summary="Toggle alert rule", tags=["Alerts"])
async def toggle_alert_rule(rule_id: str, is_active: bool = Query(True)):
    """Toggle active state of an alert rule."""
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        ok = _update_gold(
            f"{_CATALOG}.gold.alert_rules",
            {"is_active": 1 if is_active else 0, "updated_at": f"'{now}'"},
            f"rule_id = '{_sql_escape(rule_id)}'",
        )
        _invalidate_cache("sql:")
        return {"status": "updated" if ok else "error", "rule_id": rule_id, "is_active": is_active}
    except Exception as exc:
        logger.warning("Toggle alert rule failed: %s", exc)
        return {"status": "error", "message": str(exc)[:200]}


@router.get("/api/alerts/rules", summary="List persisted alert rules", tags=["Alerts"])
async def list_alert_rules():
    """Return all active alert rules from gold.alert_rules."""
    try:
        rows = _query_gold(
            f"SELECT rule_id, region, alert_type, threshold_value, notification_channel, "
            f"is_active, created_at, updated_at, created_by "
            f"FROM {_CATALOG}.gold.alert_rules "
            f"WHERE is_active = true "
            f"ORDER BY created_at DESC LIMIT 100"
        )
        if rows:
            return {"rules": rows, "count": len(rows)}
    except Exception as exc:
        logger.warning("List alert rules failed: %s", exc)
    return {"rules": [], "count": 0}


@router.post("/api/alerts/evaluate", summary="Evaluate alert rules", tags=["Alerts"])
async def evaluate_alerts():
    """Check all active rules against current data and log triggers."""
    triggered = []
    try:
        rules = _query_gold(
            f"SELECT rule_id, region, alert_type, threshold_value, notification_channel, "
            f"COALESCE(cooldown_minutes, 60) AS cooldown_minutes "
            f"FROM {_CATALOG}.gold.alert_rules WHERE is_active = true"
        )
        if not rules:
            return {"evaluated": 0, "triggered": 0, "events": []}

        for rule in rules:
            region = rule["region"]
            alert_type = rule["alert_type"]
            threshold = float(rule["threshold_value"])
            cooldown = int(rule.get("cooldown_minutes") or 60)

            actual_value = None
            at_lower = alert_type.lower()
            if at_lower in ("price_threshold", "price_spike", "price"):
                price_row = _query_gold(
                    f"SELECT rrp FROM {_CATALOG}.gold.nem_prices_5min "
                    f"WHERE region_id = '{_sql_escape(region)}' "
                    f"ORDER BY interval_datetime DESC LIMIT 1"
                )
                if price_row:
                    actual_value = float(price_row[0]["rrp"])
            elif at_lower in ("demand_surge", "demand"):
                demand_row = _query_gold(
                    f"SELECT total_demand_mw FROM {_CATALOG}.gold.nem_prices_5min "
                    f"WHERE region_id = '{_sql_escape(region)}' "
                    f"ORDER BY interval_datetime DESC LIMIT 1"
                )
                if demand_row:
                    actual_value = float(demand_row[0]["total_demand_mw"])
            elif at_lower in ("negative_price", "negative"):
                price_row = _query_gold(
                    f"SELECT rrp FROM {_CATALOG}.gold.nem_prices_5min "
                    f"WHERE region_id = '{_sql_escape(region)}' "
                    f"ORDER BY interval_datetime DESC LIMIT 1"
                )
                if price_row:
                    val = float(price_row[0]["rrp"])
                    if val < threshold:
                        actual_value = abs(val)  # trigger when price below threshold
            elif at_lower == "fcas_price":
                # Max FCAS clearing price in last 30 min
                fcas_row = _query_gold(
                    f"SELECT MAX(clearing_price_aud_mw) AS max_price "
                    f"FROM {_CATALOG}.nemweb_analytics.gold_nem_fcas_prices "
                    f"WHERE region_id = '{_sql_escape(region)}' "
                    f"AND interval_datetime >= current_timestamp() - INTERVAL 30 MINUTES"
                )
                if fcas_row and fcas_row[0].get("max_price") is not None:
                    actual_value = float(fcas_row[0]["max_price"])
            elif at_lower == "forecast_spike":
                # Max forecast demand in next 4 hours
                forecast_row = _query_gold(
                    f"SELECT MAX(predicted_demand_mw) AS max_demand "
                    f"FROM {_CATALOG}.gold.demand_forecasts "
                    f"WHERE region_id = '{_sql_escape(region)}' "
                    f"AND interval_datetime BETWEEN current_timestamp() AND current_timestamp() + INTERVAL 4 HOURS"
                )
                if forecast_row and forecast_row[0].get("max_demand") is not None:
                    actual_value = float(forecast_row[0]["max_demand"])

            if actual_value is not None and actual_value > threshold:
                # Check cooldown: skip if recently triggered
                recent = _query_gold(
                    f"SELECT event_id FROM {_CATALOG}.gold.alert_history "
                    f"WHERE rule_id = '{_sql_escape(rule['rule_id'])}' "
                    f"AND triggered_at >= current_timestamp() - INTERVAL {cooldown} MINUTES "
                    f"LIMIT 1"
                )
                if recent:
                    continue  # cooldown active, skip

                event_id = str(uuid.uuid4())
                now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                _insert_gold(f"{_CATALOG}.gold.alert_history", {
                    "event_id": event_id,
                    "rule_id": rule["rule_id"],
                    "region": region,
                    "alert_type": alert_type,
                    "threshold_value": threshold,
                    "actual_value": actual_value,
                    "triggered_at": now,
                    "notification_sent": 1,
                    "channel": rule["notification_channel"],
                })
                triggered.append({
                    "event_id": event_id,
                    "rule_id": rule["rule_id"],
                    "region": region,
                    "alert_type": alert_type,
                    "threshold": threshold,
                    "actual_value": actual_value,
                })

        return {"evaluated": len(rules), "triggered": len(triggered), "events": triggered}
    except Exception as exc:
        logger.warning("Evaluate alerts failed: %s", exc)
        return {"evaluated": 0, "triggered": 0, "events": [], "error": str(exc)[:200]}


# =========================================================================
# CONSTRAINT ENHANCEMENTS — Feature 3
# =========================================================================

_NEMWEB = f"{_CATALOG}.nemweb_analytics"


@router.get("/api/constraints/dashboard", summary="Constraint dashboard", tags=["Constraints"])
async def constraint_dashboard_full():
    """Full constraint dashboard matching ConstraintDashboard interface."""
    now_str = datetime.now(timezone.utc).isoformat()
    rng = random.Random(42)

    try:
        # Query real constraint data
        rows = _query_gold(
            f"SELECT constraint_id, constraint_type, rhs, marginal_value, "
            f"violation_degree, is_binding, region, interval_datetime "
            f"FROM {_NEMWEB}.gold_nem_constraints "
            f"WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS "
            f"ORDER BY ABS(marginal_value) DESC LIMIT 200"
        )
        if rows and len(rows) > 5:
            # Build constraint equations
            by_cid: dict = {}
            for r in rows:
                cid = r.get("constraint_id", "")
                if cid not in by_cid:
                    by_cid[cid] = {
                        "constraint_id": cid,
                        "constraint_name": cid.replace("_", " ").title()[:60],
                        "constraint_type": r.get("constraint_type", "THERMAL"),
                        "binding": bool(r.get("is_binding")),
                        "region": r.get("region", "NEM"),
                        "rhs_value": float(r.get("rhs") or 0),
                        "lhs_value": float(r.get("rhs") or 0) - float(r.get("violation_degree") or 0),
                        "slack_mw": max(0, float(r.get("rhs") or 0) - float(r.get("violation_degree") or 0)),
                        "marginal_value": round(abs(float(r.get("marginal_value") or 0)), 2),
                        "generic_equation": f"{cid} <= {float(r.get('rhs') or 0):.0f} MW",
                        "connected_duids": [],
                        "frequency_binding_pct": 0.0,
                        "annual_cost_est_m_aud": 0.0,
                        "_binding_count": 0,
                        "_total_count": 0,
                    }
                entry = by_cid[cid]
                entry["_total_count"] += 1
                if r.get("is_binding"):
                    entry["_binding_count"] += 1
                    entry["binding"] = True

            # Calculate binding frequency and cost
            equations = []
            for cid, eq in by_cid.items():
                total = eq.pop("_total_count", 1)
                bind_ct = eq.pop("_binding_count", 0)
                eq["frequency_binding_pct"] = round(bind_ct / max(total, 1) * 100, 1)
                eq["annual_cost_est_m_aud"] = round(eq["marginal_value"] * eq["frequency_binding_pct"] / 100 * 0.5, 2)
                eq["slack_mw"] = round(abs(eq["rhs_value"]) * (1 - eq["frequency_binding_pct"] / 100), 1) if not eq["binding"] else 0.0
                equations.append(eq)
            equations.sort(key=lambda x: -x["marginal_value"])

            # Region summaries
            by_region: dict = {}
            for eq in equations:
                reg = eq["region"]
                if reg not in by_region:
                    by_region[reg] = {"region": reg, "active_constraints": 0, "binding_constraints": 0,
                                      "critical_constraints": 0, "total_cost_m_aud_yr": 0.0,
                                      "most_binding_constraint": "", "interconnector_limited": False}
                by_region[reg]["active_constraints"] += 1
                if eq["binding"]:
                    by_region[reg]["binding_constraints"] += 1
                if eq["marginal_value"] > 100:
                    by_region[reg]["critical_constraints"] += 1
                by_region[reg]["total_cost_m_aud_yr"] += eq["annual_cost_est_m_aud"]
                if not by_region[reg]["most_binding_constraint"] or eq["marginal_value"] > 0:
                    by_region[reg]["most_binding_constraint"] = eq["constraint_id"]

            region_summaries = list(by_region.values())

            # Build violations from binding constraints with high marginal value
            violations = []
            for eq in equations[:10]:
                if eq["binding"] or eq["marginal_value"] > 50:
                    violations.append({
                        "violation_id": f"V-{eq['constraint_id'][:20]}",
                        "constraint_id": eq["constraint_id"],
                        "region": eq["region"],
                        "dispatch_interval": now_str,
                        "violation_mw": round(max(0, eq["marginal_value"] * 0.1), 1),
                        "dispatch_price_impact": round(eq["marginal_value"] * 0.5, 1),
                        "cause": "HIGH_DEMAND" if eq["marginal_value"] > 200 else "WIND_RAMP",
                        "resolved": eq["marginal_value"] < 50,
                    })

            binding_count = sum(1 for eq in equations if eq["binding"])
            most_constrained = max(region_summaries, key=lambda x: x["binding_constraints"])["region"] if region_summaries else "NSW1"

            return {
                "timestamp": now_str,
                "total_active_constraints": len(equations),
                "binding_constraints_now": binding_count,
                "total_annual_constraint_cost_m_aud": round(sum(eq["annual_cost_est_m_aud"] for eq in equations), 1),
                "most_constrained_region": most_constrained,
                "violations_today": len(violations),
                "region_summaries": region_summaries,
                "constraint_equations": equations[:50],
                "violations": violations,
            }
    except Exception as exc:
        logger.warning("Constraint dashboard real query failed: %s", exc)

    # Mock fallback
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    equations = []
    for i in range(25):
        reg = regions[i % 5]
        binding = rng.random() < 0.3
        mv = round(rng.uniform(0, 500), 2) if binding else round(rng.uniform(0, 50), 2)
        rhs = round(rng.uniform(500, 5000), 0)
        equations.append({
            "constraint_id": f"C_{reg}_{i:03d}",
            "constraint_name": f"Constraint {reg} #{i}",
            "constraint_type": rng.choice(["THERMAL", "VOLTAGE", "STABILITY", "NETWORK"]),
            "binding": binding, "region": reg,
            "rhs_value": rhs, "lhs_value": rhs - rng.uniform(0, 200),
            "slack_mw": 0.0 if binding else round(rng.uniform(5, 200), 1),
            "marginal_value": mv,
            "generic_equation": f"C_{reg}_{i:03d} <= {rhs:.0f} MW",
            "connected_duids": [], "frequency_binding_pct": round(rng.uniform(0, 40), 1),
            "annual_cost_est_m_aud": round(mv * 0.02, 2),
        })
    region_summaries = [
        {"region": reg, "active_constraints": sum(1 for e in equations if e["region"] == reg),
         "binding_constraints": sum(1 for e in equations if e["region"] == reg and e["binding"]),
         "critical_constraints": sum(1 for e in equations if e["region"] == reg and e["marginal_value"] > 100),
         "total_cost_m_aud_yr": round(sum(e["annual_cost_est_m_aud"] for e in equations if e["region"] == reg), 2),
         "most_binding_constraint": next((e["constraint_id"] for e in equations if e["region"] == reg and e["binding"]), ""),
         "interconnector_limited": rng.random() < 0.3}
        for reg in regions
    ]
    violations = [
        {"violation_id": f"V-{i}", "constraint_id": equations[i]["constraint_id"],
         "region": equations[i]["region"], "dispatch_interval": now_str,
         "violation_mw": round(rng.uniform(5, 100), 1),
         "dispatch_price_impact": round(rng.uniform(10, 500), 1),
         "cause": rng.choice(["WIND_RAMP", "HIGH_DEMAND", "OUTAGE", "NEMDE_INTERVENTION"]),
         "resolved": rng.random() < 0.4}
        for i in range(min(8, len(equations))) if equations[i]["binding"]
    ]
    binding_ct = sum(1 for e in equations if e["binding"])
    return {
        "timestamp": now_str,
        "total_active_constraints": len(equations),
        "binding_constraints_now": binding_ct,
        "total_annual_constraint_cost_m_aud": round(sum(e["annual_cost_est_m_aud"] for e in equations), 1),
        "most_constrained_region": max(region_summaries, key=lambda x: x["binding_constraints"])["region"],
        "violations_today": len(violations),
        "region_summaries": region_summaries,
        "constraint_equations": equations,
        "violations": violations,
    }


@router.get("/api/constraints/binding-heatmap", summary="Constraint binding heatmap", tags=["Constraints"])
async def constraint_binding_heatmap(
    region: str = Query("NSW1"),
    days: int = Query(7),
):
    """24x7 grid of binding frequency by hour and weekday."""
    try:
        rows = _query_gold(
            f"SELECT HOUR(interval_datetime) AS hour_of_day, "
            f"DAYOFWEEK(interval_datetime) AS day_of_week, "
            f"COUNT(*) AS total_intervals, "
            f"SUM(CASE WHEN is_congested = true THEN 1 ELSE 0 END) AS binding_count "
            f"FROM {_CATALOG}.gold.nem_interconnectors "
            f"WHERE (from_region = '{_sql_escape(region)}' OR to_region = '{_sql_escape(region)}') "
            f"AND interval_datetime >= current_timestamp() - INTERVAL {days} DAYS "
            f"GROUP BY HOUR(interval_datetime), DAYOFWEEK(interval_datetime) "
            f"ORDER BY day_of_week, hour_of_day"
        )
        if rows:
            grid = []
            for r in rows:
                total = int(r.get("total_intervals", 1))
                binding = int(r.get("binding_count", 0))
                grid.append({
                    "hour": int(r["hour_of_day"]),
                    "day": int(r["day_of_week"]),
                    "binding_pct": round(binding / max(total, 1) * 100, 1),
                    "binding_count": binding,
                    "total_intervals": total,
                })
            return {"region": region, "days": days, "grid": grid}
    except Exception as exc:
        logger.warning("Binding heatmap failed: %s", exc)

    # Mock fallback — generate a plausible heatmap
    rng = random.Random(hash(region))
    grid = []
    for dow in range(1, 8):
        for hour in range(24):
            base_pct = 5.0
            if 7 <= hour <= 20 and dow <= 5:
                base_pct = 15.0
            if 15 <= hour <= 19:
                base_pct = 25.0
            pct = round(max(0, base_pct + rng.uniform(-5, 10)), 1)
            grid.append({"hour": hour, "day": dow, "binding_pct": pct, "binding_count": int(pct), "total_intervals": 100})
    return {"region": region, "days": days, "grid": grid}


@router.get("/api/constraints/price-separation", summary="Inter-regional price separation", tags=["Constraints"])
async def constraint_price_separation(hours_back: int = Query(24)):
    """Inter-regional price spreads correlated with constraint binding."""
    try:
        rows = _query_gold(
            f"SELECT p1.region_id AS region_a, p2.region_id AS region_b, "
            f"ROUND(AVG(ABS(p1.rrp - p2.rrp)), 2) AS avg_spread, "
            f"ROUND(MAX(ABS(p1.rrp - p2.rrp)), 2) AS max_spread, "
            f"COUNT(*) AS intervals "
            f"FROM {_CATALOG}.gold.nem_prices_5min p1 "
            f"JOIN {_CATALOG}.gold.nem_prices_5min p2 "
            f"  ON p1.interval_datetime = p2.interval_datetime "
            f"  AND p1.region_id < p2.region_id "
            f"WHERE p1.interval_datetime >= current_timestamp() - INTERVAL {hours_back} HOURS "
            f"GROUP BY p1.region_id, p2.region_id "
            f"ORDER BY avg_spread DESC"
        )
        if rows:
            return {"hours_back": hours_back, "spreads": rows}
    except Exception as exc:
        logger.warning("Price separation failed: %s", exc)

    # Mock fallback
    rng = random.Random(42)
    pairs = [("NSW1", "QLD1"), ("NSW1", "VIC1"), ("VIC1", "SA1"), ("VIC1", "TAS1"), ("NSW1", "SA1")]
    spreads = []
    for a, b in pairs:
        avg = round(rng.uniform(2, 25), 2)
        spreads.append({"region_a": a, "region_b": b, "avg_spread": avg, "max_spread": round(avg * rng.uniform(3, 8), 2), "intervals": 288})
    return {"hours_back": hours_back, "spreads": spreads}
