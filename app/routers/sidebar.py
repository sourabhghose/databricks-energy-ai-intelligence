from __future__ import annotations
import math
import random
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Query
from .shared import _NEM_REGIONS, _REGION_BASE_PRICES, _query_gold, _CATALOG, logger

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
    """Return TradingDashboard shape with positions and spreads."""
    now = datetime.now(timezone.utc)
    rng = random.Random(int(time.time() // 30))
    # Build positions inline
    products = ["Base Swap", "Peak Swap", "Cap $300", "Cap $500", "Asian Option", "CfD"]
    traders = ["Desk-A", "Desk-B", "Desk-C", "Algo-1"]
    counterparties = ["Origin Energy", "AGL", "EnergyAustralia", "Snowy Hydro", "Shell Energy"]
    positions = []
    total_long = 0
    total_short = 0
    total_pnl = 0
    for i in range(rng.randint(6, 12)):
        region = rng.choice(_NEM_REGIONS)
        direction = rng.choice(["long", "short"])
        vol = rng.choice([10, 25, 50, 100])
        entry = round(rng.uniform(40, 150), 2)
        current = round(entry * rng.uniform(0.8, 1.3), 2)
        pnl = round((current - entry) * vol * 48, 2) if direction == "long" else round((entry - current) * vol * 48, 2)
        if direction == "long":
            total_long += vol
        else:
            total_short += vol
        total_pnl += pnl
        positions.append({
            "position_id": f"POS-{1000 + i}",
            "trader": rng.choice(traders),
            "region": region,
            "product": rng.choice(products),
            "direction": direction,
            "volume_mw": vol,
            "entry_price_aud_mwh": entry,
            "current_price_aud_mwh": current,
            "pnl_aud": pnl,
            "open_date": (now - timedelta(days=rng.randint(1, 30))).strftime("%Y-%m-%d"),
            "expiry_date": (now + timedelta(days=rng.randint(7, 180))).strftime("%Y-%m-%d"),
            "counterparty": rng.choice(counterparties),
        })
    # Build spreads inline
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


# --- 8. Trading Positions ---
@router.get("/api/trading/positions", summary="Open trading positions", tags=["Trading"])
async def trading_positions():
    """Return TradingPosition[] matching frontend interface."""
    now = datetime.now(timezone.utc)
    rng = random.Random(int(time.time() // 30))
    products = ["Base Swap", "Peak Swap", "Cap $300", "Cap $500", "Asian Option", "CfD"]
    traders = ["Desk-A", "Desk-B", "Desk-C", "Algo-1"]
    counterparties = ["Origin Energy", "AGL", "EnergyAustralia", "Snowy Hydro", "Shell Energy"]
    positions = []
    for i in range(rng.randint(6, 15)):
        region = rng.choice(_NEM_REGIONS)
        direction = rng.choice(["long", "short"])
        vol = rng.choice([10, 25, 50, 100])
        entry = round(rng.uniform(40, 150), 2)
        current = round(entry * rng.uniform(0.8, 1.3), 2)
        pnl = round((current - entry) * vol * 48, 2) if direction == "long" else round((entry - current) * vol * 48, 2)
        positions.append({
            "position_id": f"POS-{1000 + i}",
            "trader": rng.choice(traders),
            "region": region,
            "product": rng.choice(products),
            "direction": direction,
            "volume_mw": vol,
            "entry_price_aud_mwh": entry,
            "current_price_aud_mwh": current,
            "pnl_aud": pnl,
            "open_date": (now - timedelta(days=rng.randint(1, 30))).strftime("%Y-%m-%d"),
            "expiry_date": (now + timedelta(days=rng.randint(7, 180))).strftime("%Y-%m-%d"),
            "counterparty": rng.choice(counterparties),
        })
    return positions


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
    """Return Alert[] matching frontend interface."""
    now = datetime.now(timezone.utc)
    alerts = []

    # Try to derive price alerts from real data
    price_rows = _query_gold(f"""
        SELECT region_id, rrp, interval_datetime
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
          AND (rrp > 300 OR rrp < 0)
        ORDER BY interval_datetime DESC
        LIMIT 10
    """)
    if price_rows:
        for i, r in enumerate(price_rows):
            price = float(r["rrp"])
            region = r["region_id"]
            if price < 0:
                metric = "price"
                threshold = 0
                status = "triggered"
            elif price > 1000:
                metric = "price"
                threshold = 1000
                status = "triggered"
            else:
                metric = "price"
                threshold = 300
                status = "triggered"
            alerts.append({
                "id": f"ALT-{str(i + 1).zfill(3)}",
                "region": region,
                "metric": metric,
                "threshold": threshold,
                "status": status,
                "triggeredAt": str(r["interval_datetime"]).replace(" ", "T"),
                "isActive": True,
                "notificationChannel": "SLACK",
            })

    # Always include the full set of alert definitions (some armed, some from real triggers)
    rng = random.Random(int(time.time() // 30))
    alert_defs = [
        {"region": "NSW1", "metric": "price", "threshold": 300},
        {"region": "SA1", "metric": "frequency", "threshold": 49.85},
        {"region": "QLD1", "metric": "demand", "threshold": 9000},
        {"region": "VIC1", "metric": "generation", "threshold": 200},
        {"region": "NSW1", "metric": "interconnector", "threshold": 95},
        {"region": "SA1", "metric": "battery_soc", "threshold": 15},
        {"region": "VIC1", "metric": "carbon_intensity", "threshold": 0.9},
        {"region": "QLD1", "metric": "price", "threshold": 500},
        {"region": "TAS1", "metric": "demand", "threshold": 1500},
        {"region": "SA1", "metric": "price", "threshold": 1000},
        {"region": "NSW1", "metric": "demand", "threshold": 12000},
        {"region": "VIC1", "metric": "price", "threshold": 300},
    ]
    channels = ["SLACK", "EMAIL"]
    existing_count = len(alerts)
    for i, a in enumerate(alert_defs):
        alerts.append({
            "id": f"ALT-{str(existing_count + i + 1).zfill(3)}",
            "region": a["region"],
            "metric": a["metric"],
            "threshold": a["threshold"],
            "status": "armed",
            "triggeredAt": None,
            "isActive": True,
            "notificationChannel": rng.choice(channels),
        })
    return alerts


# --- 11. Alert Stats ---
@router.get("/api/alerts/stats", summary="Alert statistics", tags=["Alerts"])
async def alerts_stats():
    """Return AlertStats matching frontend interface."""
    rng = random.Random(int(time.time() // 30))
    return {
        "total_alerts": rng.randint(10, 20),
        "triggered_last_24h": rng.randint(1, 6),
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
    return {
        "overall": "operational" if all(s["status"] == "operational" for s in statuses) else "degraded",
        "services": statuses,
        "active_market_notices": rng.randint(2, 8),
        "nem_trading_interval": now.strftime("%Y-%m-%dT%H:%M:00Z"),
    }


# =========================================================================
# ADDITIONAL SIDEBAR ENDPOINTS (missing from initial deployment)
# =========================================================================

# --- Market Notices (frontend calls /api/market/notices) ---
@router.get("/api/market/notices", summary="Market notices (frontend path)", tags=["Live Ops"])
async def market_notices_frontend(severity: str = Query(None), notice_type: str = Query(None), limit: int = Query(20)):
    """Return MarketNotice[] matching frontend interface."""
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
    """Return AlertTriggerEvent[] matching frontend interface."""
    rng = random.Random(int(time.time() // 30))
    now = datetime.now(timezone.utc)
    alert_types = ["PRICE_THRESHOLD", "DEMAND_SURGE", "FCAS_PRICE", "FORECAST_SPIKE"]
    events = []
    for i in range(rng.randint(5, 15)):
        r = region if region else rng.choice(_NEM_REGIONS)
        at = rng.choice(alert_types)
        threshold = round(rng.uniform(100, 1000), 0)
        events.append({
            "event_id": f"ATE-{i + 1:04d}",
            "alert_id": f"ALT-{rng.randint(1, 12):03d}",
            "triggered_at": (now - timedelta(hours=rng.uniform(0, hours_back))).isoformat(),
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


# --- FCAS Market ---
@router.get("/api/fcas/market", summary="FCAS market data", tags=["Market Data"])
async def fcas_market(region: str = Query("NSW1"), hours_back: int = Query(24)):
    """Return FcasRecord[] matching frontend interface."""
    rng = random.Random(hash(region) + int(time.time() // 30))
    now = datetime.now(timezone.utc)
    services = ["RAISE6SEC", "RAISE60SEC", "RAISE5MIN", "RAISEREG", "LOWER6SEC", "LOWER60SEC", "LOWER5MIN", "LOWERREG"]
    records = []
    for i in range(rng.randint(8, 20)):
        ts = now - timedelta(hours=rng.uniform(0, hours_back))
        records.append({
            "interval_datetime": ts.isoformat(),
            "regionid": region,
            "service": rng.choice(services),
            "totaldemand": round(rng.uniform(100, 600), 0),
            "clearedmw": round(rng.uniform(50, 400), 0),
            "rrp": round(rng.uniform(2, 80), 2),
        })
    return records


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
