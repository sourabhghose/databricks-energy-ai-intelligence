from __future__ import annotations
import math
import random
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from .shared import _cache_get, _cache_set, _query_gold, _CATALOG, _NEM_REGIONS, logger

router = APIRouter()


# =========================================================================
# 2. NEM REAL-TIME DASHBOARD (main overview)
# =========================================================================

class RegionalDispatch(BaseModel):
    region: str
    dispatch_price_aud_mwh: float
    predispatch_price_aud_mwh: float
    demand_mw: float
    generation_mw: float
    net_interchange_mw: float
    rrp_band: str
    renewable_pct: float
    scheduled_gen_mw: float
    semi_sched_gen_mw: float


class NemGenMixRecord(BaseModel):
    region: str
    fuel_type: str
    registered_capacity_mw: float
    available_mw: float
    dispatch_mw: float
    capacity_factor_pct: float
    marginal_cost_aud_mwh: float


class NemIcFlowRecord(BaseModel):
    interconnector_id: str
    from_region: str
    to_region: str
    mw_flow: float
    mw_limit: float
    loading_pct: float
    losses_mw: float
    direction: str


class NemRealTimeDashboard(BaseModel):
    dispatch_interval: str
    timestamp: str
    nem_total_demand_mw: float
    nem_total_generation_mw: float
    nem_avg_price_aud_mwh: float
    nem_renewable_pct: float
    max_price_region: str
    min_price_region: str
    regional_dispatch: list[RegionalDispatch]
    generation_mix: list[NemGenMixRecord]
    interconnector_flows: list[NemIcFlowRecord]


def _price_band(p):
    if p < 0:       return "NEGATIVE"
    if p < 100:     return "LOW"
    if p < 300:     return "NORMAL"
    if p < 1000:    return "HIGH"
    if p < 5000:    return "VHIGH"
    return "SPIKE"


def _build_realtime_dashboard_from_db() -> Optional[NemRealTimeDashboard]:
    """Try to build the real-time dashboard from gold tables."""
    # --- Prices + demand per region (latest interval) ---
    price_rows = _query_gold(f"""
        SELECT region_id, rrp, interval_datetime, total_demand_mw, available_gen_mw, net_interchange_mw
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE interval_datetime = (SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_prices_5min)
    """)
    if not price_rows:
        return None

    now_ts = str(price_rows[0]["interval_datetime"])
    interval = now_ts[:16].replace(" ", "T") + ":00"

    # --- Generation mix per region (latest interval) ---
    gen_rows = _query_gold(f"""
        SELECT region_id, fuel_type, total_mw, unit_count, capacity_factor
        FROM {_CATALOG}.gold.nem_generation_by_fuel
        WHERE interval_datetime = (SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_generation_by_fuel)
    """)
    # Build renewable % lookup from generation data
    region_renewable_pct = {}
    if gen_rows:
        region_total = {}
        region_renew = {}
        for g in gen_rows:
            rid = g["region_id"]
            mw = float(g["total_mw"] or 0)
            region_total[rid] = region_total.get(rid, 0) + mw
            ft = str(g["fuel_type"]).lower()
            if ft in ("wind", "solar", "hydro", "battery"):
                region_renew[rid] = region_renew.get(rid, 0) + mw
        for rid in region_total:
            if region_total[rid] > 0:
                region_renewable_pct[rid] = round(region_renew.get(rid, 0) / region_total[rid] * 100, 1)

    # --- Interconnectors (latest interval) ---
    ic_rows = _query_gold(f"""
        SELECT interconnector_id, from_region, to_region, mw_flow, mw_losses,
               export_limit_mw, import_limit_mw, utilization_pct
        FROM {_CATALOG}.gold.nem_interconnectors
        WHERE interval_datetime = (SELECT MAX(interval_datetime) FROM {_CATALOG}.gold.nem_interconnectors)
    """)

    # Build regional dispatch from prices
    regional_dispatch = []
    for r in price_rows:
        region = r["region_id"]
        dp = float(r["rrp"])
        dem = float(r.get("total_demand_mw") or 0)
        gen_mw = float(r.get("available_gen_mw") or dem)
        nic = float(r.get("net_interchange_mw") or 0)
        ren = region_renewable_pct.get(region, 30.0)
        regional_dispatch.append(RegionalDispatch(
            region=region,
            dispatch_price_aud_mwh=round(dp, 2),
            predispatch_price_aud_mwh=round(dp * 0.98, 2),
            demand_mw=round(dem, 1),
            generation_mw=round(gen_mw, 1),
            net_interchange_mw=round(nic, 1),
            rrp_band=_price_band(dp),
            renewable_pct=round(ren, 1),
            scheduled_gen_mw=round(gen_mw * 0.65, 1),
            semi_sched_gen_mw=round(gen_mw * 0.22, 1),
        ))

    # Build generation_mix records
    generation_mix = []
    if gen_rows:
        for g in gen_rows:
            mw = float(g["total_mw"] or 0)
            cf = float(g.get("capacity_factor") or 0.5)
            cap = mw / cf if cf > 0 else mw * 2
            fuel = str(g["fuel_type"]).upper().replace(" ", "_")
            generation_mix.append(NemGenMixRecord(
                region=g["region_id"], fuel_type=fuel,
                registered_capacity_mw=round(cap, 0),
                available_mw=round(cap * 0.85, 0),
                dispatch_mw=round(mw, 0),
                capacity_factor_pct=round(cf * 100, 1),
                marginal_cost_aud_mwh=0.0,
            ))

    # Build interconnector flow records
    interconnector_flows = []
    if ic_rows:
        for ic in ic_rows:
            flow = float(ic["mw_flow"])
            limit = float(ic.get("export_limit_mw") or 700)
            losses = float(ic.get("mw_losses") or abs(flow) * 0.03)
            util = float(ic.get("utilization_pct") or (abs(flow) / limit * 100 if limit > 0 else 0))
            interconnector_flows.append(NemIcFlowRecord(
                interconnector_id=ic["interconnector_id"],
                from_region=ic.get("from_region") or "",
                to_region=ic.get("to_region") or "",
                mw_flow=round(flow, 1),
                mw_limit=round(limit, 0),
                loading_pct=round(util, 1),
                losses_mw=round(losses, 1),
                direction="FORWARD" if flow >= 0 else "REVERSE",
            ))

    # Aggregate NEM-wide stats
    prices = [rd.dispatch_price_aud_mwh for rd in regional_dispatch]
    regions = [rd.region for rd in regional_dispatch]
    total_demand = sum(rd.demand_mw for rd in regional_dispatch)
    total_gen = sum(rd.generation_mw for rd in regional_dispatch)
    avg_price = sum(prices) / len(prices) if prices else 0
    weighted_ren = (sum(rd.renewable_pct * rd.demand_mw for rd in regional_dispatch) / total_demand) if total_demand > 0 else 0

    max_idx = prices.index(max(prices)) if prices else 0
    min_idx = prices.index(min(prices)) if prices else 0

    return NemRealTimeDashboard(
        dispatch_interval=interval,
        timestamp=now_ts,
        nem_total_demand_mw=round(total_demand, 1),
        nem_total_generation_mw=round(total_gen, 1),
        nem_avg_price_aud_mwh=round(avg_price, 2),
        nem_renewable_pct=round(weighted_ren, 1),
        max_price_region=regions[max_idx] if regions else "NSW1",
        min_price_region=regions[min_idx] if regions else "TAS1",
        regional_dispatch=regional_dispatch,
        generation_mix=generation_mix,
        interconnector_flows=interconnector_flows,
    )


def _build_realtime_dashboard() -> NemRealTimeDashboard:
    # Try real data first
    try:
        result = _build_realtime_dashboard_from_db()
        if result is not None:
            return result
    except Exception as exc:
        logger.warning("Real-time dashboard from DB failed: %s", exc)

    # Fallback to mock
    rng = random.Random(7731)
    now = datetime.now(timezone.utc)
    now_ts = now.isoformat()
    interval = now.strftime("%Y-%m-%dT%H:%M:00")

    base_prices = {"NSW1": 87.5, "QLD1": 92.3, "VIC1": 74.8, "SA1": 118.4, "TAS1": 61.2}
    base_demand = {"NSW1": 8420.0, "QLD1": 7180.0, "VIC1": 5840.0, "SA1": 1720.0, "TAS1": 1080.0}
    base_renewable_pct = {"NSW1": 28.4, "QLD1": 22.1, "VIC1": 32.7, "SA1": 61.5, "TAS1": 87.3}
    net_interchange = {"NSW1": -320.0, "QLD1": 280.0, "VIC1": 95.0, "SA1": -142.0, "TAS1": 87.0}

    regional_dispatch = []
    for region in ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]:
        dp = base_prices[region] * rng.uniform(0.95, 1.05)
        pd_ = dp * rng.uniform(0.97, 1.03)
        dem = base_demand[region] * rng.uniform(0.98, 1.02)
        gen = dem - net_interchange[region] * rng.uniform(0.95, 1.05)
        ren = base_renewable_pct[region] * rng.uniform(0.92, 1.08)
        sched = gen * rng.uniform(0.58, 0.72)
        semi = gen * rng.uniform(0.18, 0.28)
        regional_dispatch.append(RegionalDispatch(
            region=region,
            dispatch_price_aud_mwh=round(dp, 2),
            predispatch_price_aud_mwh=round(pd_, 2),
            demand_mw=round(dem, 1),
            generation_mw=round(gen, 1),
            net_interchange_mw=round(net_interchange[region], 1),
            rrp_band=_price_band(dp),
            renewable_pct=round(ren, 1),
            scheduled_gen_mw=round(sched, 1),
            semi_sched_gen_mw=round(semi, 1),
        ))

    generation_mix = []
    fuel_mix = {
        "NSW1": [("BLACK_COAL", 8500, 5800, 0.68, 42.0), ("GAS", 2800, 820, 0.29, 85.0),
                 ("HYDRO", 900, 680, 0.76, 18.0), ("WIND", 1400, 780, 0.56, 0.0),
                 ("SOLAR", 2200, 720, 0.33, 0.0), ("BATTERY", 400, 120, 0.30, 95.0)],
        "QLD1": [("BLACK_COAL", 9200, 6200, 0.67, 38.0), ("GAS", 4200, 1100, 0.26, 82.0),
                 ("HYDRO", 700, 420, 0.60, 15.0), ("WIND", 800, 480, 0.60, 0.0),
                 ("SOLAR", 3800, 980, 0.26, 0.0), ("BATTERY", 200, 60, 0.30, 95.0)],
        "VIC1": [("BROWN_COAL", 4500, 3200, 0.71, 28.0), ("GAS", 2200, 720, 0.33, 88.0),
                 ("HYDRO", 600, 380, 0.63, 16.0), ("WIND", 3200, 1980, 0.62, 0.0),
                 ("SOLAR", 1400, 420, 0.30, 0.0), ("BATTERY", 320, 98, 0.31, 92.0)],
        "SA1":  [("GAS", 2600, 620, 0.24, 92.0), ("WIND", 2800, 1820, 0.65, 0.0),
                 ("SOLAR", 1200, 360, 0.30, 0.0), ("BATTERY", 300, 210, 0.70, 90.0)],
        "TAS1": [("HYDRO", 2700, 1950, 0.72, 12.0), ("WIND", 480, 310, 0.65, 0.0),
                 ("GAS", 200, 40, 0.20, 95.0)],
    }
    for region, fuels in fuel_mix.items():
        for fuel, cap, disp, cf, mc in fuels:
            generation_mix.append(NemGenMixRecord(
                region=region, fuel_type=fuel,
                registered_capacity_mw=float(cap),
                available_mw=float(cap) * 0.85,
                dispatch_mw=float(disp),
                capacity_factor_pct=cf * 100,
                marginal_cost_aud_mwh=mc,
            ))

    interconnector_flows = [
        NemIcFlowRecord(interconnector_id="N-Q-MNSP1", from_region="NSW1", to_region="QLD1",
                        mw_flow=280.0, mw_limit=700.0, loading_pct=40.0, losses_mw=5.6, direction="FORWARD"),
        NemIcFlowRecord(interconnector_id="VIC1-NSW1", from_region="VIC1", to_region="NSW1",
                        mw_flow=420.0, mw_limit=1350.0, loading_pct=31.1, losses_mw=12.6, direction="FORWARD"),
        NemIcFlowRecord(interconnector_id="V-SA", from_region="VIC1", to_region="SA1",
                        mw_flow=-142.0, mw_limit=680.0, loading_pct=20.9, losses_mw=4.3, direction="REVERSE"),
        NemIcFlowRecord(interconnector_id="T-V-MNSP1", from_region="TAS1", to_region="VIC1",
                        mw_flow=87.0, mw_limit=594.0, loading_pct=14.6, losses_mw=2.6, direction="FORWARD"),
        NemIcFlowRecord(interconnector_id="V-S-MNSP1", from_region="VIC1", to_region="SA1",
                        mw_flow=95.0, mw_limit=220.0, loading_pct=43.2, losses_mw=3.8, direction="FORWARD"),
    ]

    prices = [rd.dispatch_price_aud_mwh for rd in regional_dispatch]
    regions = [rd.region for rd in regional_dispatch]
    max_idx = prices.index(max(prices))
    min_idx = prices.index(min(prices))
    total_demand = sum(rd.demand_mw for rd in regional_dispatch)
    total_gen = sum(rd.generation_mw for rd in regional_dispatch)
    avg_price = sum(prices) / len(prices)
    weighted_ren = sum(rd.renewable_pct * rd.demand_mw for rd in regional_dispatch) / total_demand

    return NemRealTimeDashboard(
        dispatch_interval=interval,
        timestamp=now_ts,
        nem_total_demand_mw=round(total_demand, 1),
        nem_total_generation_mw=round(total_gen, 1),
        nem_avg_price_aud_mwh=round(avg_price, 2),
        nem_renewable_pct=round(weighted_ren, 1),
        max_price_region=regions[max_idx],
        min_price_region=regions[min_idx],
        regional_dispatch=regional_dispatch,
        generation_mix=generation_mix,
        interconnector_flows=interconnector_flows,
    )


@router.get("/api/nem-realtime/dashboard", response_model=NemRealTimeDashboard,
         summary="NEM Real-Time Dashboard", tags=["Market Data"])
@router.get("/api/realtime/dashboard", response_model=NemRealTimeDashboard,
         summary="NEM Real-Time Dashboard (alias)", tags=["Market Data"], include_in_schema=False)
def get_nem_realtime_dashboard() -> NemRealTimeDashboard:
    """NEM-wide real-time dispatch overview with regional prices, generation mix, and interconnector flows."""
    cached = _cache_get("nem_realtime")
    if cached is not None:
        return cached
    result = _build_realtime_dashboard()
    _cache_set("nem_realtime", result, 10)
    return result


# =========================================================================
# 3. POWER SYSTEM SECURITY DASHBOARD
# =========================================================================

class PssInertiaRecord(BaseModel):
    region: str
    timestamp: str
    total_inertia_mws: float
    synchronous_generation_mw: float
    non_synchronous_pct: float
    rocof_limit_hz_s: float
    min_inertia_requirement_mws: float
    inertia_headroom_mws: float
    status: str


class SynchronousCondenserRecord(BaseModel):
    unit_id: str
    site_name: str
    region: str
    operator: str
    rated_mvar: float
    inertia_contribution_mws: float
    status: str
    commissioning_year: int
    purpose: str


class FcasDispatchRecord(BaseModel):
    service: str
    region: str
    requirement_mw: float
    dispatched_mw: float
    price_mwh: float
    enablement_pct: float
    primary_provider: str
    timestamp: str


class PowerSystemSecurityDashboard(BaseModel):
    timestamp: str
    nem_inertia_total_mws: float
    lowest_inertia_region: str
    synchronous_condensers_online: int
    total_syncon_capacity_mvar: float
    fcas_raise_total_mw: float
    fcas_lower_total_mw: float
    system_strength_status: str
    inertia_records: list[PssInertiaRecord]
    synchronous_condensers: list[SynchronousCondenserRecord]
    fcas_dispatch: list[FcasDispatchRecord]


def _build_pss_dashboard() -> PowerSystemSecurityDashboard:
    ts = datetime.now(timezone.utc).isoformat()
    inertia_records = [
        PssInertiaRecord(region="SA1", timestamp=ts, total_inertia_mws=4200.0, synchronous_generation_mw=480.0,
                         non_synchronous_pct=74.2, rocof_limit_hz_s=1.0, min_inertia_requirement_mws=3500.0,
                         inertia_headroom_mws=700.0, status="Secure"),
        PssInertiaRecord(region="VIC1", timestamp=ts, total_inertia_mws=18500.0, synchronous_generation_mw=3200.0,
                         non_synchronous_pct=52.8, rocof_limit_hz_s=0.5, min_inertia_requirement_mws=12000.0,
                         inertia_headroom_mws=6500.0, status="Secure"),
        PssInertiaRecord(region="NSW1", timestamp=ts, total_inertia_mws=22000.0, synchronous_generation_mw=5800.0,
                         non_synchronous_pct=44.1, rocof_limit_hz_s=0.5, min_inertia_requirement_mws=15000.0,
                         inertia_headroom_mws=7000.0, status="Secure"),
        PssInertiaRecord(region="QLD1", timestamp=ts, total_inertia_mws=19800.0, synchronous_generation_mw=4600.0,
                         non_synchronous_pct=49.3, rocof_limit_hz_s=0.5, min_inertia_requirement_mws=14000.0,
                         inertia_headroom_mws=5800.0, status="Secure"),
        PssInertiaRecord(region="TAS1", timestamp=ts, total_inertia_mws=2800.0, synchronous_generation_mw=620.0,
                         non_synchronous_pct=38.4, rocof_limit_hz_s=1.0, min_inertia_requirement_mws=2200.0,
                         inertia_headroom_mws=600.0, status="Secure"),
    ]
    condensers = [
        SynchronousCondenserRecord(unit_id="SC-SA-001", site_name="Davenport Synchronous Condenser 1",
                                   region="SA1", operator="ElectraNet", rated_mvar=100.0,
                                   inertia_contribution_mws=500.0, status="Online", commissioning_year=2023, purpose="Both"),
        SynchronousCondenserRecord(unit_id="SC-SA-002", site_name="Davenport Synchronous Condenser 2",
                                   region="SA1", operator="ElectraNet", rated_mvar=100.0,
                                   inertia_contribution_mws=500.0, status="Online", commissioning_year=2023, purpose="Both"),
        SynchronousCondenserRecord(unit_id="SC-SA-003", site_name="Robertstown Synchronous Condenser",
                                   region="SA1", operator="ElectraNet", rated_mvar=100.0,
                                   inertia_contribution_mws=500.0, status="Online", commissioning_year=2022, purpose="System Strength"),
        SynchronousCondenserRecord(unit_id="SC-VIC-001", site_name="Moorabool Synchronous Condenser 1",
                                   region="VIC1", operator="AusNet Services", rated_mvar=200.0,
                                   inertia_contribution_mws=1000.0, status="Commissioning", commissioning_year=2026, purpose="Both"),
        SynchronousCondenserRecord(unit_id="SC-NSW-001", site_name="Transgrid Tomago Synchronous Condenser",
                                   region="NSW1", operator="Transgrid", rated_mvar=250.0,
                                   inertia_contribution_mws=1200.0, status="Online", commissioning_year=2024, purpose="System Strength"),
        SynchronousCondenserRecord(unit_id="SC-QLD-001", site_name="Greenbank Synchronous Condenser",
                                   region="QLD1", operator="Powerlink", rated_mvar=160.0,
                                   inertia_contribution_mws=800.0, status="Online", commissioning_year=2025, purpose="Voltage Support"),
    ]
    fcas = [
        FcasDispatchRecord(service="R6S", region="NEM", requirement_mw=240.0, dispatched_mw=240.0,
                           price_mwh=12.50, enablement_pct=100.0, primary_provider="Batteries", timestamp=ts),
        FcasDispatchRecord(service="R60S", region="NEM", requirement_mw=280.0, dispatched_mw=278.0,
                           price_mwh=8.20, enablement_pct=99.3, primary_provider="Batteries + Hydro", timestamp=ts),
        FcasDispatchRecord(service="R5M", region="NEM", requirement_mw=320.0, dispatched_mw=320.0,
                           price_mwh=4.80, enablement_pct=100.0, primary_provider="Gas + Batteries", timestamp=ts),
        FcasDispatchRecord(service="R5RE", region="NEM", requirement_mw=420.0, dispatched_mw=415.0,
                           price_mwh=2.40, enablement_pct=98.8, primary_provider="Gas + DSP", timestamp=ts),
        FcasDispatchRecord(service="L6S", region="NEM", requirement_mw=210.0, dispatched_mw=210.0,
                           price_mwh=15.80, enablement_pct=100.0, primary_provider="Batteries", timestamp=ts),
        FcasDispatchRecord(service="L60S", region="NEM", requirement_mw=250.0, dispatched_mw=248.0,
                           price_mwh=9.40, enablement_pct=99.2, primary_provider="Batteries + DSP", timestamp=ts),
        FcasDispatchRecord(service="L5M", region="NEM", requirement_mw=300.0, dispatched_mw=298.0,
                           price_mwh=5.20, enablement_pct=99.3, primary_provider="Gas + DSP", timestamp=ts),
        FcasDispatchRecord(service="L5RE", region="NEM", requirement_mw=380.0, dispatched_mw=378.0,
                           price_mwh=2.80, enablement_pct=99.5, primary_provider="Gas + Hydro", timestamp=ts),
    ]
    total_inertia = sum(r.total_inertia_mws for r in inertia_records)
    lowest_region = min(inertia_records, key=lambda r: r.inertia_headroom_mws).region
    online_condensers = [c for c in condensers if c.status == "Online"]
    fcas_raise = sum(f.dispatched_mw for f in fcas if f.service.startswith("R"))
    fcas_lower = sum(f.dispatched_mw for f in fcas if f.service.startswith("L"))

    return PowerSystemSecurityDashboard(
        timestamp=ts,
        nem_inertia_total_mws=total_inertia,
        lowest_inertia_region=lowest_region,
        synchronous_condensers_online=len(online_condensers),
        total_syncon_capacity_mvar=sum(c.rated_mvar for c in online_condensers),
        fcas_raise_total_mw=fcas_raise,
        fcas_lower_total_mw=fcas_lower,
        system_strength_status="Secure",
        inertia_records=inertia_records,
        synchronous_condensers=condensers,
        fcas_dispatch=fcas,
    )


@router.get("/api/pss/dashboard", response_model=PowerSystemSecurityDashboard,
         summary="Power System Security & Inertia Dashboard", tags=["Grid Operations"])
def get_pss_dashboard() -> PowerSystemSecurityDashboard:
    """Inertia, synchronous condensers, and FCAS dispatch dashboard."""
    cached = _cache_get("pss_dashboard")
    if cached is not None:
        return cached
    result = _build_pss_dashboard()
    _cache_set("pss_dashboard", result, 30)
    return result


# =========================================================================
# 4. CURTAILMENT DASHBOARD (Renewable Integration)
# =========================================================================

class CurtailmentEvent(BaseModel):
    event_id: str
    date: str
    region: str
    technology: str
    curtailed_mwh: float
    curtailed_pct: float
    duration_minutes: int
    cause: str
    peak_available_mw: float


class MinimumOperationalDemandRecord(BaseModel):
    date: str
    region: str
    min_demand_mw: float
    min_demand_time: str
    renewable_share_pct: float
    instantaneous_renewable_mw: float
    storage_charging_mw: float
    exports_mw: float
    record_broken: bool


class RenewableIntegrationLimit(BaseModel):
    region: str
    limit_type: str
    current_limit_mw: float
    headroom_mw: float
    mitigation_project: str
    mitigation_year: int
    description: str


class CurtailmentDashboard(BaseModel):
    timestamp: str
    total_curtailment_gwh_ytd: float
    curtailment_events_ytd: int
    worst_region: str
    lowest_mod_record_mw: float
    lowest_mod_date: str
    renewable_penetration_record_pct: float
    renewable_penetration_record_date: str
    curtailment_events: list[CurtailmentEvent]
    mod_records: list[MinimumOperationalDemandRecord]
    integration_limits: list[RenewableIntegrationLimit]


def _build_curtailment_dashboard() -> CurtailmentDashboard:
    # Try real curtailment data from NEMWEB accelerator
    try:
        curt_rows = _query_gold(f"""
            SELECT network_region, interval_window,
                   avg_solar_curtailment_MW, max_solar_curtailment_MW,
                   avg_wind_curtailment_MW, max_wind_curtailment_MW,
                   avg_total_curtailment_MW,
                   avg_solar_curtailment_pct, avg_wind_curtailment_pct
            FROM {_CATALOG}.nemweb_analytics.gold_nem_curtailment_30min
            WHERE avg_total_curtailment_MW > 0
            ORDER BY interval_window.start DESC
            LIMIT 50
        """)
    except Exception:
        curt_rows = None

    if curt_rows:
        events = []
        total_curt_mwh = 0.0
        causes = ["System Strength", "Thermal Limit", "Voltage", "Frequency Control"]
        for i, r in enumerate(curt_rows):
            solar_mw = float(r.get("avg_solar_curtailment_MW") or 0)
            wind_mw = float(r.get("avg_wind_curtailment_MW") or 0)
            total_mw = float(r.get("avg_total_curtailment_MW") or 0)
            solar_pct = float(r.get("avg_solar_curtailment_pct") or 0)
            wind_pct = float(r.get("avg_wind_curtailment_pct") or 0)
            region = r.get("network_region", "NSW1")
            iw = r.get("interval_window", {})
            dt_str = str(iw.get("start", ""))[:10] if isinstance(iw, dict) else str(iw)[:10]
            tech = "Solar" if solar_mw >= wind_mw else "Wind"
            curt_mwh = round(total_mw * 0.5, 1)  # 30-min interval -> MWh
            total_curt_mwh += curt_mwh
            pct = solar_pct if tech == "Solar" else wind_pct
            events.append(CurtailmentEvent(
                event_id=f"CE-2026-{i+1:04d}", date=dt_str, region=region,
                technology=tech, curtailed_mwh=curt_mwh,
                curtailed_pct=round(pct, 1), duration_minutes=30,
                cause=causes[i % len(causes)],
                peak_available_mw=round(total_mw / max(pct / 100, 0.01), 0) if pct > 0 else 1000.0,
            ))
        worst_region = max(set(e.region for e in events), key=lambda rg: sum(e.curtailed_mwh for e in events if e.region == rg)) if events else "SA1"
    else:
        events = [
            CurtailmentEvent(event_id="CE-2025-0842", date="2025-10-14", region="SA1", technology="Wind",
                             curtailed_mwh=842.0, curtailed_pct=38.2, duration_minutes=95,
                             cause="System Strength", peak_available_mw=1580.0),
            CurtailmentEvent(event_id="CE-2025-0831", date="2025-10-13", region="VIC1", technology="Solar",
                             curtailed_mwh=312.0, curtailed_pct=18.5, duration_minutes=45,
                             cause="Thermal Limit", peak_available_mw=1890.0),
            CurtailmentEvent(event_id="CE-2025-0819", date="2025-10-11", region="QLD1", technology="Solar",
                             curtailed_mwh=1240.0, curtailed_pct=42.1, duration_minutes=135,
                             cause="Voltage", peak_available_mw=4200.0),
            CurtailmentEvent(event_id="CE-2025-0808", date="2025-10-10", region="SA1", technology="Wind",
                             curtailed_mwh=620.0, curtailed_pct=29.8, duration_minutes=72,
                             cause="Frequency Control", peak_available_mw=1460.0),
            CurtailmentEvent(event_id="CE-2025-0795", date="2025-10-08", region="NSW1", technology="Solar",
                             curtailed_mwh=480.0, curtailed_pct=22.4, duration_minutes=60,
                             cause="Thermal Limit", peak_available_mw=2800.0),
            CurtailmentEvent(event_id="CE-2025-0781", date="2025-10-07", region="VIC1", technology="Wind",
                             curtailed_mwh=950.0, curtailed_pct=35.6, duration_minutes=110,
                             cause="System Strength", peak_available_mw=2650.0),
        ]
        total_curt_mwh = 3820.0
        worst_region = "SA1"

    mod_records = [
        MinimumOperationalDemandRecord(date="2025-10-13", region="SA1", min_demand_mw=312.0,
                                       min_demand_time="13:00", renewable_share_pct=92.4,
                                       instantaneous_renewable_mw=1820.0, storage_charging_mw=680.0,
                                       exports_mw=420.0, record_broken=True),
        MinimumOperationalDemandRecord(date="2025-10-12", region="VIC1", min_demand_mw=2840.0,
                                       min_demand_time="12:30", renewable_share_pct=78.2,
                                       instantaneous_renewable_mw=5180.0, storage_charging_mw=420.0,
                                       exports_mw=1200.0, record_broken=False),
        MinimumOperationalDemandRecord(date="2025-09-28", region="QLD1", min_demand_mw=2120.0,
                                       min_demand_time="13:30", renewable_share_pct=85.6,
                                       instantaneous_renewable_mw=6840.0, storage_charging_mw=950.0,
                                       exports_mw=380.0, record_broken=True),
    ]
    integration_limits = [
        RenewableIntegrationLimit(region="SA1", limit_type="System Strength", current_limit_mw=2200.0,
                                  headroom_mw=380.0, mitigation_project="Synchronous Condensers (Davenport)",
                                  mitigation_year=2026, description="SA system strength limits non-synchronous renewable infeed."),
        RenewableIntegrationLimit(region="VIC1", limit_type="System Strength", current_limit_mw=6000.0,
                                  headroom_mw=1200.0, mitigation_project="VNI West Transmission Upgrade",
                                  mitigation_year=2030, description="VIC system strength constrained near Latrobe Valley as coal retires."),
        RenewableIntegrationLimit(region="QLD1", limit_type="Thermal Limit", current_limit_mw=8500.0,
                                  headroom_mw=2400.0, mitigation_project="QNI Upgrade + Central QLD REZ",
                                  mitigation_year=2029, description="North QLD solar/wind export limited by 275kV thermal constraints."),
    ]
    return CurtailmentDashboard(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_curtailment_gwh_ytd=round(total_curt_mwh / 1000, 2),
        curtailment_events_ytd=len(events),
        worst_region=worst_region,
        lowest_mod_record_mw=312.0,
        lowest_mod_date="2025-10-13",
        renewable_penetration_record_pct=92.4,
        renewable_penetration_record_date="2025-10-13",
        curtailment_events=events,
        mod_records=mod_records,
        integration_limits=integration_limits,
    )


@router.get("/api/curtailment/dashboard", response_model=CurtailmentDashboard,
         summary="Renewable Curtailment Dashboard", tags=["Renewable Energy"])
def get_curtailment_dashboard() -> CurtailmentDashboard:
    """Curtailment events, minimum operational demand records, and renewable integration limits."""
    cached = _cache_get("curtailment_dashboard")
    if cached is not None:
        return cached
    result = _build_curtailment_dashboard()
    _cache_set("curtailment_dashboard", result, 300)
    return result


# =========================================================================
# 5. BATTERY ECONOMICS DASHBOARD
# =========================================================================

class BatteryUnit(BaseModel):
    bess_id: str
    station_name: str
    region: str
    technology: str
    capacity_mwh: float
    power_mw: float
    roundtrip_efficiency_pct: float
    cycles_today: float
    soc_current_pct: float
    energy_revenue_today: float
    fcas_revenue_today: float
    sras_revenue_today: float
    total_revenue_today: float
    annual_revenue_est_aud: float
    lcoe_aud_mwh: float


class ArbitrageOpportunity(BaseModel):
    region: str
    date: str
    peak_price: float
    off_peak_price: float
    spread: float
    optimal_cycles: float
    theoretical_max_revenue_mw: float
    actual_captured_pct: float


class BatteryEconomicsDashboard(BaseModel):
    timestamp: str
    total_fleet_capacity_mwh: float
    total_fleet_power_mw: float
    avg_roundtrip_efficiency_pct: float
    fleet_revenue_today_aud: float
    energy_pct: float
    fcas_pct: float
    sras_pct: float
    best_arbitrage_region: str
    best_spread_today: float
    batteries: List[BatteryUnit]
    opportunities: List[ArbitrageOpportunity]


def _build_battery_dashboard() -> BatteryEconomicsDashboard:
    # Try real battery data from facilities + price spreads
    try:
        batt_rows = _query_gold(f"""
            SELECT duid, station_name, region_id, fuel_type, capacity_mw
            FROM {_CATALOG}.gold.nem_facilities
            WHERE LOWER(fuel_type) LIKE '%battery%'
            AND capacity_mw > 0
            ORDER BY capacity_mw DESC
        """)
        price_rows = _query_gold(f"""
            SELECT region_id,
                   MAX(rrp) AS peak_price,
                   MIN(rrp) AS off_peak_price,
                   MAX(rrp) - MIN(rrp) AS spread,
                   AVG(rrp) AS avg_price
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
            GROUP BY region_id
        """)
    except Exception:
        batt_rows = None
        price_rows = None

    if batt_rows and price_rows:
        rng = random.Random(42)
        price_map = {r["region_id"]: r for r in price_rows}
        batteries = []
        for b in batt_rows:
            region = b["region_id"]
            cap_mw = float(b["capacity_mw"] or 0)
            cap_mwh = cap_mw * 2  # assume 2hr duration
            rte = rng.uniform(88, 93)
            cycles = rng.uniform(0.5, 1.5)
            soc = rng.uniform(30, 80)
            pr = price_map.get(region, {})
            avg_p = float(pr.get("avg_price") or 80)
            spread = float(pr.get("spread") or 100)
            daily_rev = cap_mw * spread * cycles * 0.001  # rough revenue estimate
            e_rev = daily_rev * 0.45
            f_rev = daily_rev * 0.40
            s_rev = daily_rev * 0.15
            annual = daily_rev * 365
            lcoe = rng.uniform(85, 110)
            tech = "Li-Ion" if "charging" in str(b["fuel_type"]).lower() or "discharging" in str(b["fuel_type"]).lower() else "Li-Ion"
            batteries.append(BatteryUnit(
                bess_id=b["duid"], station_name=b["station_name"] or b["duid"],
                region=region, technology=tech,
                capacity_mwh=round(cap_mwh, 1), power_mw=round(cap_mw, 1),
                roundtrip_efficiency_pct=round(rte, 1), cycles_today=round(cycles, 2),
                soc_current_pct=round(soc, 1),
                energy_revenue_today=round(e_rev, 2), fcas_revenue_today=round(f_rev, 2),
                sras_revenue_today=round(s_rev, 2), total_revenue_today=round(e_rev + f_rev + s_rev, 2),
                annual_revenue_est_aud=round(annual, 0), lcoe_aud_mwh=round(lcoe, 1),
            ))

        today_str = date.today().isoformat()
        opportunities = []
        for region in ["SA1", "QLD1", "VIC1", "NSW1", "TAS1"]:
            pr = price_map.get(region, {})
            peak = float(pr.get("peak_price") or 500)
            off_peak = float(pr.get("off_peak_price") or 20)
            sp = peak - off_peak
            opt_cyc = min(sp / 200, 2.5)
            opportunities.append(ArbitrageOpportunity(
                region=region, date=today_str, peak_price=round(peak, 2),
                off_peak_price=round(off_peak, 2), spread=round(sp, 2),
                optimal_cycles=round(opt_cyc, 1),
                theoretical_max_revenue_mw=round(sp * opt_cyc * 0.5, 2),
                actual_captured_pct=round(rng.uniform(60, 85), 1),
            ))

        if batteries:
            total_cap = sum(b.capacity_mwh for b in batteries)
            total_pwr = sum(b.power_mw for b in batteries)
            avg_rte = sum(b.roundtrip_efficiency_pct * b.capacity_mwh for b in batteries) / max(total_cap, 1)
            total_rev = sum(b.total_revenue_today for b in batteries)
            total_e = sum(b.energy_revenue_today for b in batteries)
            total_f = sum(b.fcas_revenue_today for b in batteries)
            total_s = sum(b.sras_revenue_today for b in batteries)
            best_region = max(opportunities, key=lambda o: o.spread).region if opportunities else "SA1"
            best_spread = max(o.spread for o in opportunities) if opportunities else 0

            return BatteryEconomicsDashboard(
                timestamp=datetime.now(timezone.utc).isoformat(),
                total_fleet_capacity_mwh=total_cap,
                total_fleet_power_mw=total_pwr,
                avg_roundtrip_efficiency_pct=round(avg_rte, 1),
                fleet_revenue_today_aud=round(total_rev, 2),
                energy_pct=round(total_e / total_rev * 100, 1) if total_rev else 0,
                fcas_pct=round(total_f / total_rev * 100, 1) if total_rev else 0,
                sras_pct=round(total_s / total_rev * 100, 1) if total_rev else 0,
                best_arbitrage_region=best_region,
                best_spread_today=round(best_spread, 2),
                batteries=batteries,
                opportunities=opportunities,
            )

    # Mock fallback
    rng = random.Random(42)
    batteries_raw = [
        ("HPSA1", "Hornsdale Power Reserve", "SA1", "Li-Ion", 194.0, 150.0, 92.5, 0.95, 68.0, 8_200_000, 95.0),
        ("LBSA1", "Lake Bonney BESS", "SA1", "Li-Ion", 52.0, 25.0, 91.0, 0.60, 55.0, 1_450_000, 98.0),
        ("WSBNSW1", "Waratah Super Battery", "NSW1", "Li-Ion", 1680.0, 850.0, 93.0, 1.10, 72.0, 45_000_000, 88.0),
        ("OBQLD1", "Onslow Battery", "QLD1", "Li-Ion", 200.0, 100.0, 90.5, 0.85, 60.0, 5_500_000, 97.0),
        ("TIBSA1", "Torrens Island BESS", "SA1", "Li-Ion", 500.0, 250.0, 92.0, 1.00, 65.0, 13_800_000, 92.0),
        ("VBBVIC1", "Victorian Big Battery", "VIC1", "Li-Ion", 450.0, 300.0, 91.5, 0.90, 58.0, 12_100_000, 94.0),
        ("HWBVIC1", "Hazelwood Battery", "VIC1", "Flow Battery", 100.0, 50.0, 78.0, 0.40, 45.0, 2_200_000, 110.0),
    ]
    batteries = []
    for bess_id, name, region, tech, cap, pwr, rte, cycles, soc, annual, lcoe in batteries_raw:
        daily = annual / 365.0
        e_rev = daily * 0.45 * (1 + rng.uniform(-0.05, 0.05))
        f_rev = daily * 0.40 * (1 + rng.uniform(-0.05, 0.05))
        s_rev = daily * 0.15 * (1 + rng.uniform(-0.05, 0.05))
        batteries.append(BatteryUnit(
            bess_id=bess_id, station_name=name, region=region, technology=tech,
            capacity_mwh=cap, power_mw=pwr, roundtrip_efficiency_pct=rte,
            cycles_today=cycles, soc_current_pct=soc,
            energy_revenue_today=round(e_rev, 2), fcas_revenue_today=round(f_rev, 2),
            sras_revenue_today=round(s_rev, 2), total_revenue_today=round(e_rev + f_rev + s_rev, 2),
            annual_revenue_est_aud=annual, lcoe_aud_mwh=lcoe,
        ))

    today_str = date.today().isoformat()
    opportunities = [
        ArbitrageOpportunity(region="SA1", date=today_str, peak_price=820.0, off_peak_price=15.0,
                             spread=420.0, optimal_cycles=2.0, theoretical_max_revenue_mw=310.0, actual_captured_pct=78.5),
        ArbitrageOpportunity(region="QLD1", date=today_str, peak_price=750.0, off_peak_price=18.0,
                             spread=380.0, optimal_cycles=1.8, theoretical_max_revenue_mw=270.0, actual_captured_pct=74.2),
        ArbitrageOpportunity(region="VIC1", date=today_str, peak_price=680.0, off_peak_price=22.0,
                             spread=310.0, optimal_cycles=1.5, theoretical_max_revenue_mw=225.0, actual_captured_pct=71.8),
        ArbitrageOpportunity(region="NSW1", date=today_str, peak_price=620.0, off_peak_price=20.0,
                             spread=280.0, optimal_cycles=1.4, theoretical_max_revenue_mw=200.0, actual_captured_pct=68.9),
        ArbitrageOpportunity(region="TAS1", date=today_str, peak_price=450.0, off_peak_price=25.0,
                             spread=190.0, optimal_cycles=1.2, theoretical_max_revenue_mw=140.0, actual_captured_pct=65.0),
    ]

    total_cap = sum(b.capacity_mwh for b in batteries)
    total_pwr = sum(b.power_mw for b in batteries)
    avg_rte = sum(b.roundtrip_efficiency_pct * b.capacity_mwh for b in batteries) / total_cap
    total_rev = sum(b.total_revenue_today for b in batteries)
    total_e = sum(b.energy_revenue_today for b in batteries)
    total_f = sum(b.fcas_revenue_today for b in batteries)
    total_s = sum(b.sras_revenue_today for b in batteries)

    return BatteryEconomicsDashboard(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_fleet_capacity_mwh=total_cap,
        total_fleet_power_mw=total_pwr,
        avg_roundtrip_efficiency_pct=round(avg_rte, 1),
        fleet_revenue_today_aud=round(total_rev, 2),
        energy_pct=round(total_e / total_rev * 100, 1) if total_rev else 0,
        fcas_pct=round(total_f / total_rev * 100, 1) if total_rev else 0,
        sras_pct=round(total_s / total_rev * 100, 1) if total_rev else 0,
        best_arbitrage_region="SA1",
        best_spread_today=420.0,
        batteries=batteries,
        opportunities=opportunities,
    )


@router.get("/api/battery-economics/dashboard", response_model=BatteryEconomicsDashboard,
         summary="Battery Economics & Arbitrage Dashboard", tags=["Battery Storage"])
def get_battery_economics_dashboard() -> BatteryEconomicsDashboard:
    """Fleet-wide battery economics including revenue splits, arbitrage opportunities."""
    cached = _cache_get("battery_economics")
    if cached is not None:
        return cached
    result = _build_battery_dashboard()
    _cache_set("battery_economics", result, 60)
    return result


# =========================================================================
# 6. GAS MARKET DASHBOARD
# =========================================================================

class GasHubPrice(BaseModel):
    hub: str
    price_aud_gj: float
    change_1d_pct: float
    volume_tj: float
    timestamp: str


class GasPipelineFlow(BaseModel):
    pipeline_id: str
    pipeline_name: str
    from_location: str
    to_location: str
    capacity_tj_day: float
    flow_tj_day: float
    utilisation_pct: float
    pressure_kpa: float
    timestamp: str


class LngExportRecord(BaseModel):
    terminal: str
    state: str
    capacity_mtpa: float
    utilisation_pct: float
    exports_today_tj: float
    destination: str
    timestamp: str


class GasMarketDashboard(BaseModel):
    timestamp: str
    wallumbilla_price: float
    moomba_price: float
    longford_price: float
    total_pipeline_flow_tj: float
    lng_exports_today_tj: float
    domestic_demand_tj: float
    gas_power_generation_tj: float
    hub_prices: List[GasHubPrice]
    pipeline_flows: List[GasPipelineFlow]
    lng_terminals: List[LngExportRecord]


def _build_gas_dashboard() -> GasMarketDashboard:
    ts = datetime.now(timezone.utc).isoformat()
    # Try real gas hub price data
    try:
        gas_rows = _query_gold(f"""
            WITH latest AS (
                SELECT hub, price_aud_gj, volume_tj, pipeline_flow_tj, trade_date,
                       ROW_NUMBER() OVER (PARTITION BY hub ORDER BY trade_date DESC) AS rn
                FROM {_CATALOG}.gold.gas_hub_prices
            ), prev AS (
                SELECT hub, price_aud_gj AS prev_price,
                       ROW_NUMBER() OVER (PARTITION BY hub ORDER BY trade_date DESC) AS rn
                FROM {_CATALOG}.gold.gas_hub_prices
            )
            SELECT l.hub, l.price_aud_gj, l.volume_tj, l.pipeline_flow_tj, l.trade_date,
                   ROUND((l.price_aud_gj - p.prev_price) / NULLIF(p.prev_price, 0) * 100, 1) AS change_pct
            FROM latest l LEFT JOIN prev p ON l.hub = p.hub AND p.rn = 2
            WHERE l.rn = 1
            ORDER BY l.hub
        """)
        if gas_rows and len(gas_rows) >= 3:
            hub_prices = [GasHubPrice(
                hub=r["hub"], price_aud_gj=float(r["price_aud_gj"] or 0),
                change_1d_pct=float(r["change_pct"] or 0),
                volume_tj=float(r["volume_tj"] or 0), timestamp=ts
            ) for r in gas_rows]
            hub_map = {r["hub"]: float(r["price_aud_gj"] or 0) for r in gas_rows}
            pipeline_flows = [
                GasPipelineFlow(pipeline_id="MSP", pipeline_name="Moomba Sydney Pipeline",
                                from_location="Moomba", to_location="Sydney", capacity_tj_day=310.0,
                                flow_tj_day=float(next((r["pipeline_flow_tj"] for r in gas_rows if r["hub"] == "Sydney"), 245)),
                                utilisation_pct=round(float(next((r["pipeline_flow_tj"] for r in gas_rows if r["hub"] == "Sydney"), 245)) / 310 * 100, 1),
                                pressure_kpa=10000.0, timestamp=ts),
                GasPipelineFlow(pipeline_id="SWQP", pipeline_name="South West Queensland Pipeline",
                                from_location="Wallumbilla", to_location="Moomba", capacity_tj_day=210.0,
                                flow_tj_day=float(next((r["pipeline_flow_tj"] for r in gas_rows if r["hub"] == "Wallumbilla"), 163)),
                                utilisation_pct=round(float(next((r["pipeline_flow_tj"] for r in gas_rows if r["hub"] == "Wallumbilla"), 163)) / 210 * 100, 1),
                                pressure_kpa=11500.0, timestamp=ts),
                GasPipelineFlow(pipeline_id="SEA", pipeline_name="SEA Gas Pipeline",
                                from_location="Victoria", to_location="Adelaide", capacity_tj_day=90.0,
                                flow_tj_day=float(next((r["pipeline_flow_tj"] for r in gas_rows if r["hub"] == "Adelaide"), 58)),
                                utilisation_pct=round(float(next((r["pipeline_flow_tj"] for r in gas_rows if r["hub"] == "Adelaide"), 58)) / 90 * 100, 1),
                                pressure_kpa=7400.0, timestamp=ts),
            ]
            lng_terminals = [
                LngExportRecord(terminal="GLNG", state="QLD", capacity_mtpa=7.8, utilisation_pct=82.0,
                                exports_today_tj=420.0, destination="Asia-Pacific", timestamp=ts),
                LngExportRecord(terminal="APLNG", state="QLD", capacity_mtpa=9.0, utilisation_pct=88.5,
                                exports_today_tj=510.0, destination="Asia-Pacific", timestamp=ts),
                LngExportRecord(terminal="QCLNG", state="QLD", capacity_mtpa=8.5, utilisation_pct=85.0,
                                exports_today_tj=480.0, destination="Asia-Pacific", timestamp=ts),
            ]
            total_pipeline = sum(p.flow_tj_day for p in pipeline_flows)
            total_lng = sum(t.exports_today_tj for t in lng_terminals)
            return GasMarketDashboard(
                timestamp=ts,
                wallumbilla_price=hub_map.get("Wallumbilla", 8.45),
                moomba_price=hub_map.get("Moomba", 9.12),
                longford_price=hub_map.get("Sydney", 10.80),
                total_pipeline_flow_tj=total_pipeline,
                lng_exports_today_tj=total_lng,
                domestic_demand_tj=round(total_pipeline * 0.65, 0),
                gas_power_generation_tj=round(total_pipeline * 0.2, 0),
                hub_prices=hub_prices,
                pipeline_flows=pipeline_flows,
                lng_terminals=lng_terminals,
            )
    except Exception as e:
        logger.warning(f"Gas dashboard real data failed: {e}")
    hub_prices = [
        GasHubPrice(hub="Wallumbilla", price_aud_gj=8.45, change_1d_pct=2.1, volume_tj=285.0, timestamp=ts),
        GasHubPrice(hub="Moomba", price_aud_gj=9.12, change_1d_pct=-0.8, volume_tj=310.0, timestamp=ts),
        GasHubPrice(hub="Longford", price_aud_gj=10.80, change_1d_pct=1.5, volume_tj=195.0, timestamp=ts),
        GasHubPrice(hub="Adelaide", price_aud_gj=11.20, change_1d_pct=3.2, volume_tj=58.0, timestamp=ts),
        GasHubPrice(hub="Sydney", price_aud_gj=10.50, change_1d_pct=0.6, volume_tj=245.0, timestamp=ts),
    ]
    pipeline_flows = [
        GasPipelineFlow(pipeline_id="MSP", pipeline_name="Moomba Sydney Pipeline",
                        from_location="Moomba", to_location="Sydney", capacity_tj_day=310.0,
                        flow_tj_day=245.0, utilisation_pct=79.0, pressure_kpa=10000.0, timestamp=ts),
        GasPipelineFlow(pipeline_id="EGP", pipeline_name="Eastern Gas Pipeline",
                        from_location="Longford", to_location="Sydney", capacity_tj_day=195.0,
                        flow_tj_day=142.0, utilisation_pct=72.8, pressure_kpa=8600.0, timestamp=ts),
        GasPipelineFlow(pipeline_id="SEA", pipeline_name="SEA Gas Pipeline",
                        from_location="Victoria", to_location="Adelaide", capacity_tj_day=90.0,
                        flow_tj_day=58.0, utilisation_pct=64.4, pressure_kpa=7400.0, timestamp=ts),
        GasPipelineFlow(pipeline_id="CGP", pipeline_name="Carpentaria Gas Pipeline",
                        from_location="Ballera", to_location="Mount Isa", capacity_tj_day=110.0,
                        flow_tj_day=74.0, utilisation_pct=67.3, pressure_kpa=9200.0, timestamp=ts),
        GasPipelineFlow(pipeline_id="SWQP", pipeline_name="South West Queensland Pipeline",
                        from_location="Wallumbilla", to_location="Moomba", capacity_tj_day=210.0,
                        flow_tj_day=163.0, utilisation_pct=77.6, pressure_kpa=11500.0, timestamp=ts),
    ]
    lng_terminals = [
        LngExportRecord(terminal="GLNG", state="QLD", capacity_mtpa=7.8, utilisation_pct=82.0,
                        exports_today_tj=420.0, destination="Asia-Pacific", timestamp=ts),
        LngExportRecord(terminal="APLNG", state="QLD", capacity_mtpa=9.0, utilisation_pct=88.5,
                        exports_today_tj=510.0, destination="Asia-Pacific", timestamp=ts),
        LngExportRecord(terminal="QCLNG", state="QLD", capacity_mtpa=8.5, utilisation_pct=85.0,
                        exports_today_tj=480.0, destination="Asia-Pacific", timestamp=ts),
    ]
    total_pipeline = sum(p.flow_tj_day for p in pipeline_flows)
    total_lng = sum(t.exports_today_tj for t in lng_terminals)

    return GasMarketDashboard(
        timestamp=ts,
        wallumbilla_price=8.45,
        moomba_price=9.12,
        longford_price=10.80,
        total_pipeline_flow_tj=total_pipeline,
        lng_exports_today_tj=total_lng,
        domestic_demand_tj=682.0,
        gas_power_generation_tj=185.0,
        hub_prices=hub_prices,
        pipeline_flows=pipeline_flows,
        lng_terminals=lng_terminals,
    )


@router.get("/api/gas-market/dashboard", response_model=GasMarketDashboard,
         summary="Eastern Australian Gas Market Dashboard", tags=["Gas Market"])
def get_gas_market_dashboard() -> GasMarketDashboard:
    """Gas hub prices, pipeline flows, and LNG export data."""
    cached = _cache_get("gas_market")
    if cached is not None:
        return cached
    result = _build_gas_dashboard()
    _cache_set("gas_market", result, 60)
    return result


# =========================================================================
# 7. CARBON & EMISSIONS DASHBOARD
# =========================================================================

class RegionEmissionsRecord(BaseModel):
    region: str
    timestamp: str
    emissions_intensity_kg_co2_mwh: float
    renewable_pct: float
    coal_pct: float
    gas_pct: float
    hydro_pct: float
    wind_pct: float
    solar_pct: float
    battery_pct: float
    total_generation_mw: float
    net_emissions_t_co2_hr: float


class FuelEmissionsFactor(BaseModel):
    fuel_type: str
    scope: str
    kg_co2_mwh: float
    kg_co2_mwh_with_losses: float
    notes: str


class EmissionsTrajectory(BaseModel):
    year: int
    emissions_mt_co2: float
    renewable_share_pct: float
    coal_capacity_gw: float
    target_met: bool


class CarbonDashboard(BaseModel):
    timestamp: str
    nem_emissions_intensity_now: float
    lowest_region: str
    lowest_intensity: float
    highest_region: str
    highest_intensity: float
    renewable_share_now_pct: float
    vs_same_time_last_year_pct: float
    annual_trajectory: List[EmissionsTrajectory]
    region_records: List[RegionEmissionsRecord]
    fuel_factors: List[FuelEmissionsFactor]


def _build_carbon_dashboard() -> CarbonDashboard:
    ts = datetime.now(timezone.utc).isoformat()

    # Emission factors (kg CO2/MWh) by fuel type keyword
    _EMISSION_FACTORS = {
        "coal": 900, "black_coal": 820, "brown_coal": 1100,
        "gas": 450, "ocgt": 550, "ccgt": 370,
        "solar": 0, "wind": 0, "hydro": 0, "battery": 0, "biomass": 50,
    }

    def _fuel_emission_factor(fuel: str) -> float:
        fl = fuel.lower().replace(" ", "_")
        for key, val in _EMISSION_FACTORS.items():
            if key in fl:
                return val
        return 0.0 if "renew" in fl else 450.0

    def _fuel_category(fuel: str) -> str:
        fl = fuel.lower()
        if "coal" in fl:
            return "coal"
        if "gas" in fl or "ocgt" in fl or "ccgt" in fl:
            return "gas"
        if "solar" in fl:
            return "solar"
        if "wind" in fl:
            return "wind"
        if "hydro" in fl:
            return "hydro"
        if "battery" in fl or "storage" in fl:
            return "battery"
        return "other"

    # Try real generation by fuel data
    try:
        gen_rows = _query_gold(f"""
            SELECT region_id AS network_region, fuel_type,
                   SUM(total_mw) AS total_mw
            FROM {_CATALOG}.gold.nem_generation_by_fuel
            WHERE interval_datetime >= current_timestamp() - INTERVAL 24 HOURS
            GROUP BY region_id, fuel_type
        """)
    except Exception:
        gen_rows = None

    if gen_rows:
        # Build per-region generation breakdown
        region_data: Dict[str, Dict[str, float]] = {}
        for r in gen_rows:
            reg = r["network_region"]
            if reg not in region_data:
                region_data[reg] = {"total": 0, "coal": 0, "gas": 0, "solar": 0, "wind": 0, "hydro": 0, "battery": 0, "other": 0, "emissions": 0}
            mw = float(r["total_mw"] or 0)
            cat = _fuel_category(str(r["fuel_type"]))
            region_data[reg]["total"] += mw
            region_data[reg][cat] = region_data[reg].get(cat, 0) + mw
            region_data[reg]["emissions"] += mw * _fuel_emission_factor(str(r["fuel_type"]))

        regions = []
        for reg in ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]:
            rd = region_data.get(reg)
            if not rd or rd["total"] <= 0:
                continue
            total = rd["total"]
            em_intensity = round(rd["emissions"] / total, 1) if total > 0 else 0
            renewable_mw = rd["solar"] + rd["wind"] + rd["hydro"] + rd["battery"]
            regions.append(RegionEmissionsRecord(
                region=reg, timestamp=ts,
                emissions_intensity_kg_co2_mwh=em_intensity,
                renewable_pct=round(renewable_mw / total * 100, 1),
                coal_pct=round(rd["coal"] / total * 100, 1),
                gas_pct=round(rd["gas"] / total * 100, 1),
                hydro_pct=round(rd["hydro"] / total * 100, 1),
                wind_pct=round(rd["wind"] / total * 100, 1),
                solar_pct=round(rd["solar"] / total * 100, 1),
                battery_pct=round(rd["battery"] / total * 100, 1),
                total_generation_mw=round(total, 0),
                net_emissions_t_co2_hr=round(rd["emissions"] / 1000, 1),
            ))
    else:
        regions = [
            RegionEmissionsRecord(region="NSW1", timestamp=ts, emissions_intensity_kg_co2_mwh=450.0,
                                  renewable_pct=22.0, coal_pct=58.0, gas_pct=12.0, hydro_pct=6.0,
                                  wind_pct=3.5, solar_pct=4.5, battery_pct=0.5,
                                  total_generation_mw=9800.0, net_emissions_t_co2_hr=4410.0),
            RegionEmissionsRecord(region="QLD1", timestamp=ts, emissions_intensity_kg_co2_mwh=520.0,
                                  renewable_pct=18.0, coal_pct=62.0, gas_pct=12.0, hydro_pct=2.0,
                                  wind_pct=3.0, solar_pct=5.0, battery_pct=0.0,
                                  total_generation_mw=8200.0, net_emissions_t_co2_hr=4264.0),
            RegionEmissionsRecord(region="VIC1", timestamp=ts, emissions_intensity_kg_co2_mwh=500.0,
                                  renewable_pct=24.0, coal_pct=55.0, gas_pct=12.0, hydro_pct=4.0,
                                  wind_pct=8.0, solar_pct=4.0, battery_pct=1.0,
                                  total_generation_mw=6500.0, net_emissions_t_co2_hr=3250.0),
            RegionEmissionsRecord(region="SA1", timestamp=ts, emissions_intensity_kg_co2_mwh=120.0,
                                  renewable_pct=70.0, coal_pct=0.0, gas_pct=22.0, hydro_pct=0.0,
                                  wind_pct=38.0, solar_pct=28.0, battery_pct=4.0,
                                  total_generation_mw=2100.0, net_emissions_t_co2_hr=252.0),
            RegionEmissionsRecord(region="TAS1", timestamp=ts, emissions_intensity_kg_co2_mwh=50.0,
                                  renewable_pct=95.0, coal_pct=0.0, gas_pct=5.0, hydro_pct=88.0,
                                  wind_pct=7.0, solar_pct=0.0, battery_pct=0.0,
                                  total_generation_mw=1600.0, net_emissions_t_co2_hr=80.0),
        ]

    # Try real emissions factors from gold table
    fuel_factors = []
    try:
        ef_rows = _query_gold(f"""
            SELECT fuel_type, scope2_kg_co2_mwh, scope1_kg_co2_mwh, total_kg_co2_mwh, source
            FROM {_CATALOG}.gold.emissions_factors
            WHERE fuel_type NOT LIKE '%Grid%'
            ORDER BY total_kg_co2_mwh DESC
        """)
        if ef_rows and len(ef_rows) >= 5:
            for r in ef_rows:
                scope2 = float(r["scope2_kg_co2_mwh"] or 0)
                fuel_factors.append(FuelEmissionsFactor(
                    fuel_type=r["fuel_type"], scope="Scope 1+2",
                    kg_co2_mwh=scope2,
                    kg_co2_mwh_with_losses=round(scope2 * 1.1, 1),
                    notes=f"Source: {r['source']}"
                ))
            # Also update the hardcoded emission factors used in region calcs
            for r in ef_rows:
                ft = str(r["fuel_type"]).lower().replace(" ", "_").replace("(", "").replace(")", "")
                _EMISSION_FACTORS[ft] = float(r["scope2_kg_co2_mwh"] or 0)
    except Exception:
        pass
    if not fuel_factors:
        fuel_factors = [
            FuelEmissionsFactor(fuel_type="Black Coal", scope="Scope 1", kg_co2_mwh=820.0,
                                kg_co2_mwh_with_losses=902.0, notes="NSW/QLD bituminous coal"),
            FuelEmissionsFactor(fuel_type="Brown Coal", scope="Scope 1", kg_co2_mwh=1100.0,
                                kg_co2_mwh_with_losses=1210.0, notes="VIC Latrobe Valley lignite"),
            FuelEmissionsFactor(fuel_type="Natural Gas (CCGT)", scope="Scope 1", kg_co2_mwh=370.0,
                                kg_co2_mwh_with_losses=407.0, notes="Combined cycle gas turbine"),
            FuelEmissionsFactor(fuel_type="Natural Gas (OCGT)", scope="Scope 1", kg_co2_mwh=550.0,
                                kg_co2_mwh_with_losses=605.0, notes="Open cycle peaker — lower efficiency"),
            FuelEmissionsFactor(fuel_type="Solar PV", scope="Scope 1", kg_co2_mwh=0.0,
                                kg_co2_mwh_with_losses=0.0, notes="Zero operational emissions"),
            FuelEmissionsFactor(fuel_type="Wind", scope="Scope 1", kg_co2_mwh=0.0,
                                kg_co2_mwh_with_losses=0.0, notes="Zero operational emissions"),
        ]
    trajectory = [
        EmissionsTrajectory(year=2020, emissions_mt_co2=165.0, renewable_share_pct=24.0, coal_capacity_gw=23.0, target_met=True),
        EmissionsTrajectory(year=2022, emissions_mt_co2=155.0, renewable_share_pct=32.0, coal_capacity_gw=21.0, target_met=True),
        EmissionsTrajectory(year=2024, emissions_mt_co2=140.0, renewable_share_pct=40.0, coal_capacity_gw=18.5, target_met=True),
        EmissionsTrajectory(year=2026, emissions_mt_co2=120.0, renewable_share_pct=52.0, coal_capacity_gw=15.0, target_met=False),
        EmissionsTrajectory(year=2028, emissions_mt_co2=95.0, renewable_share_pct=65.0, coal_capacity_gw=10.0, target_met=False),
        EmissionsTrajectory(year=2030, emissions_mt_co2=70.0, renewable_share_pct=82.0, coal_capacity_gw=5.0, target_met=False),
    ]

    total_gen = sum(r.total_generation_mw for r in regions)
    weighted_intensity = sum(r.emissions_intensity_kg_co2_mwh * r.total_generation_mw for r in regions) / total_gen
    weighted_renewable = sum(r.renewable_pct * r.total_generation_mw for r in regions) / total_gen

    sorted_by_intensity = sorted(regions, key=lambda r: r.emissions_intensity_kg_co2_mwh)
    lowest = sorted_by_intensity[0] if sorted_by_intensity else None
    highest = sorted_by_intensity[-1] if sorted_by_intensity else None

    return CarbonDashboard(
        timestamp=ts,
        nem_emissions_intensity_now=round(weighted_intensity, 1),
        lowest_region=lowest.region if lowest else "TAS1",
        lowest_intensity=lowest.emissions_intensity_kg_co2_mwh if lowest else 50.0,
        highest_region=highest.region if highest else "QLD1",
        highest_intensity=highest.emissions_intensity_kg_co2_mwh if highest else 520.0,
        renewable_share_now_pct=round(weighted_renewable, 1),
        vs_same_time_last_year_pct=-8.2,
        annual_trajectory=trajectory,
        region_records=regions,
        fuel_factors=fuel_factors,
    )


@router.get("/api/carbon/dashboard", response_model=CarbonDashboard,
         summary="Carbon & Emissions Dashboard", tags=["Carbon & Emissions"])
def get_carbon_dashboard() -> CarbonDashboard:
    """NEM emissions intensity, regional breakdown, fuel factors, and decarbonisation trajectory."""
    cached = _cache_get("carbon_dashboard")
    if cached is not None:
        return cached
    result = _build_carbon_dashboard()
    _cache_set("carbon_dashboard", result, 300)
    return result


# =========================================================================
# 8. RETAIL MARKET DASHBOARD
# =========================================================================

class RetailerMarketShare(BaseModel):
    retailer: str
    state: str
    residential_customers: int
    sme_customers: int
    large_commercial_customers: int
    market_share_pct: float
    electricity_volume_gwh: float
    avg_retail_margin_pct: float


class DefaultOfferPrice(BaseModel):
    state: str
    offer_type: str
    annual_cost_aud: float
    annual_usage_kwh: int
    cents_per_kwh: float
    daily_supply_charge_aud: float
    effective_date: str


class CustomerSwitchingRecord(BaseModel):
    quarter: str
    state: str
    switches_count: int
    churn_rate_pct: float
    avg_saving_aud: float


class RetailMarketDashboard(BaseModel):
    timestamp: str
    total_residential_customers: int
    total_market_offers_count: int
    best_market_offer_discount_pct: float
    standing_offer_customers_pct: float
    market_shares: List[RetailerMarketShare]
    default_offers: List[DefaultOfferPrice]
    switching_data: List[CustomerSwitchingRecord]


def _build_retail_dashboard() -> RetailMarketDashboard:
    market_shares = [
        RetailerMarketShare(retailer="AGL Energy", state="NEM", residential_customers=2_100_000,
                            sme_customers=210_000, large_commercial_customers=4_200,
                            market_share_pct=23.0, electricity_volume_gwh=38_400.0, avg_retail_margin_pct=6.2),
        RetailerMarketShare(retailer="Origin Energy", state="NEM", residential_customers=2_010_000,
                            sme_customers=195_000, large_commercial_customers=3_800,
                            market_share_pct=22.0, electricity_volume_gwh=36_100.0, avg_retail_margin_pct=5.8),
        RetailerMarketShare(retailer="EnergyAustralia", state="NEM", residential_customers=1_820_000,
                            sme_customers=175_000, large_commercial_customers=3_500,
                            market_share_pct=20.0, electricity_volume_gwh=33_200.0, avg_retail_margin_pct=5.5),
        RetailerMarketShare(retailer="Simply Energy", state="NEM", residential_customers=600_000,
                            sme_customers=62_000, large_commercial_customers=1_100,
                            market_share_pct=7.0, electricity_volume_gwh=10_800.0, avg_retail_margin_pct=4.9),
        RetailerMarketShare(retailer="Alinta Energy", state="NEM", residential_customers=400_000,
                            sme_customers=38_000, large_commercial_customers=700,
                            market_share_pct=4.0, electricity_volume_gwh=7_100.0, avg_retail_margin_pct=4.7),
        RetailerMarketShare(retailer="Others", state="NEM", residential_customers=1_650_000,
                            sme_customers=160_000, large_commercial_customers=3_000,
                            market_share_pct=19.0, electricity_volume_gwh=31_500.0, avg_retail_margin_pct=4.3),
    ]
    default_offers = [
        DefaultOfferPrice(state="NSW", offer_type="DMO", annual_cost_aud=1_829.0, annual_usage_kwh=3_900,
                          cents_per_kwh=38.5, daily_supply_charge_aud=1.12, effective_date="2025-07-01"),
        DefaultOfferPrice(state="QLD", offer_type="DMO", annual_cost_aud=1_762.0, annual_usage_kwh=4_600,
                          cents_per_kwh=31.2, daily_supply_charge_aud=1.08, effective_date="2025-07-01"),
        DefaultOfferPrice(state="SA", offer_type="DMO", annual_cost_aud=2_185.0, annual_usage_kwh=4_000,
                          cents_per_kwh=44.8, daily_supply_charge_aud=1.28, effective_date="2025-07-01"),
        DefaultOfferPrice(state="VIC", offer_type="VDO", annual_cost_aud=1_684.0, annual_usage_kwh=4_000,
                          cents_per_kwh=33.2, daily_supply_charge_aud=1.02, effective_date="2025-01-01"),
    ]
    switching_data = [
        CustomerSwitchingRecord(quarter="2025-Q1", state="NSW", switches_count=142_000,
                                churn_rate_pct=8.2, avg_saving_aud=185.0),
        CustomerSwitchingRecord(quarter="2025-Q1", state="VIC", switches_count=128_000,
                                churn_rate_pct=9.1, avg_saving_aud=210.0),
        CustomerSwitchingRecord(quarter="2025-Q1", state="QLD", switches_count=98_000,
                                churn_rate_pct=7.5, avg_saving_aud=165.0),
        CustomerSwitchingRecord(quarter="2025-Q1", state="SA", switches_count=52_000,
                                churn_rate_pct=10.8, avg_saving_aud=240.0),
    ]
    return RetailMarketDashboard(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_residential_customers=8_580_000,
        total_market_offers_count=412,
        best_market_offer_discount_pct=28.5,
        standing_offer_customers_pct=12.8,
        market_shares=market_shares,
        default_offers=default_offers,
        switching_data=switching_data,
    )


@router.get("/api/retail-market/dashboard", response_model=RetailMarketDashboard,
         summary="Retail Market Competition Dashboard", tags=["Retail Market"])
def get_retail_market_dashboard() -> RetailMarketDashboard:
    """Retail market shares, DMO/VDO prices, and customer switching data."""
    cached = _cache_get("retail_market")
    if cached is not None:
        return cached
    result = _build_retail_dashboard()
    _cache_set("retail_market", result, 3600)
    return result


# =========================================================================
# 9. DEMAND RESPONSE DASHBOARD
# =========================================================================

class RertContract(BaseModel):
    contract_id: str
    provider: str
    region: str
    contract_type: str
    contracted_mw: float
    available_mw: float
    strike_price_aud_mwh: float
    contract_start: str
    contract_end: str
    activations_ytd: int
    total_activation_mw: float
    contract_cost_m_aud: float


class DemandResponseActivation(BaseModel):
    activation_id: str
    trading_interval: str
    region: str
    provider: str
    activation_type: str
    activated_mw: float
    duration_min: int
    trigger: str
    spot_price_aud_mwh: float
    avoided_voll_m_aud: float
    cost_aud: float


class DemandResponseProvider(BaseModel):
    provider_id: str
    provider_name: str
    provider_type: str
    registered_mw: float
    regions: List[str]
    technologies: List[str]
    reliability_pct: float
    avg_response_time_min: float


class DemandResponseDashboard(BaseModel):
    timestamp: str
    total_contracted_mw: float
    total_available_mw: float
    activations_ytd: int
    total_activation_cost_m_aud: float
    avoided_voll_m_aud: float
    avg_activation_duration_min: float
    contracts: list[RertContract]
    activations: list[DemandResponseActivation]
    providers: list[DemandResponseProvider]


def _build_dr_dashboard() -> DemandResponseDashboard:
    rng = random.Random(9901)
    contracts = [
        RertContract(contract_id="RERT-NSW-01", provider="Energy Australia DR", region="NSW1",
                     contract_type="RERT", contracted_mw=150.0, available_mw=140.0,
                     strike_price_aud_mwh=285.0, contract_start="2025-04-01", contract_end="2025-10-31",
                     activations_ytd=3, total_activation_mw=420.0, contract_cost_m_aud=4.2),
        RertContract(contract_id="RERT-VIC-01", provider="AGL Energy DR", region="VIC1",
                     contract_type="RERT", contracted_mw=120.0, available_mw=115.0,
                     strike_price_aud_mwh=295.0, contract_start="2025-04-01", contract_end="2025-10-31",
                     activations_ytd=5, total_activation_mw=580.0, contract_cost_m_aud=3.5),
        RertContract(contract_id="RERT-QLD-01", provider="Origin Energy DR", region="QLD1",
                     contract_type="RERT", contracted_mw=100.0, available_mw=95.0,
                     strike_price_aud_mwh=275.0, contract_start="2025-04-01", contract_end="2025-10-31",
                     activations_ytd=2, total_activation_mw=190.0, contract_cost_m_aud=2.8),
        RertContract(contract_id="RERT-SA-01", provider="Tesla VPP DR", region="SA1",
                     contract_type="RERT", contracted_mw=80.0, available_mw=75.0,
                     strike_price_aud_mwh=310.0, contract_start="2025-04-01", contract_end="2025-10-31",
                     activations_ytd=7, total_activation_mw=540.0, contract_cost_m_aud=2.5),
        RertContract(contract_id="SRAS-NSW-01", provider="Schneider Electric", region="NSW1",
                     contract_type="SRAS", contracted_mw=60.0, available_mw=58.0,
                     strike_price_aud_mwh=450.0, contract_start="2025-01-01", contract_end="2025-12-31",
                     activations_ytd=1, total_activation_mw=55.0, contract_cost_m_aud=2.7),
        RertContract(contract_id="SRAS-SA-01", provider="Neoen DR (HPR)", region="SA1",
                     contract_type="SRAS", contracted_mw=70.0, available_mw=70.0,
                     strike_price_aud_mwh=420.0, contract_start="2025-01-01", contract_end="2025-12-31",
                     activations_ytd=8, total_activation_mw=540.0, contract_cost_m_aud=2.9),
    ]

    activations = [
        DemandResponseActivation(activation_id="ACT-001", trading_interval="2025-07-14T15:30:00",
                                 region="NSW1", provider="Energy Australia DR", activation_type="RERT",
                                 activated_mw=120.5, duration_min=90, trigger="PRICE_SPIKE",
                                 spot_price_aud_mwh=12800.0, avoided_voll_m_aud=2.61, cost_aud=48200.0),
        DemandResponseActivation(activation_id="ACT-002", trading_interval="2025-07-12T16:15:00",
                                 region="SA1", provider="Tesla VPP DR", activation_type="RERT",
                                 activated_mw=75.2, duration_min=60, trigger="RESERVE_LOW",
                                 spot_price_aud_mwh=9500.0, avoided_voll_m_aud=1.09, cost_aud=28600.0),
        DemandResponseActivation(activation_id="ACT-003", trading_interval="2025-07-10T18:00:00",
                                 region="VIC1", provider="AGL Energy DR", activation_type="SRAS",
                                 activated_mw=95.0, duration_min=120, trigger="SYSTEM_EMERGENCY",
                                 spot_price_aud_mwh=14200.0, avoided_voll_m_aud=2.76, cost_aud=52800.0),
        DemandResponseActivation(activation_id="ACT-004", trading_interval="2025-07-08T13:05:00",
                                 region="QLD1", provider="Origin Energy DR", activation_type="RERT",
                                 activated_mw=88.3, duration_min=30, trigger="PRICE_SPIKE",
                                 spot_price_aud_mwh=7800.0, avoided_voll_m_aud=0.64, cost_aud=15400.0),
    ]

    providers = [
        DemandResponseProvider(provider_id="PROV-001", provider_name="Enel X Australia",
                               provider_type="AGGREGATOR", registered_mw=210.0,
                               regions=["NSW1", "VIC1", "SA1"], technologies=["HVAC", "Lighting", "Industrial Process"],
                               reliability_pct=96.5, avg_response_time_min=8.2),
        DemandResponseProvider(provider_id="PROV-002", provider_name="Tesla VPP",
                               provider_type="AGGREGATOR", registered_mw=185.0,
                               regions=["SA1", "VIC1", "NSW1"], technologies=["Residential Battery", "EV", "Solar"],
                               reliability_pct=94.8, avg_response_time_min=3.1),
        DemandResponseProvider(provider_id="PROV-003", provider_name="Energy Australia DR",
                               provider_type="RETAILER", registered_mw=320.0,
                               regions=["NSW1", "QLD1", "VIC1"], technologies=["C&I Load", "Pumps", "Refrigeration"],
                               reliability_pct=97.2, avg_response_time_min=12.5),
        DemandResponseProvider(provider_id="PROV-004", provider_name="AGL Energy DR",
                               provider_type="RETAILER", registered_mw=275.0,
                               regions=["VIC1", "NSW1", "SA1"], technologies=["C&I Load", "HVAC", "Pumping"],
                               reliability_pct=95.9, avg_response_time_min=11.8),
    ]

    total_contracted = sum(c.contracted_mw for c in contracts)
    total_available = sum(c.available_mw for c in contracts)
    total_cost = sum(c.contract_cost_m_aud for c in contracts)
    total_avoided = sum(a.avoided_voll_m_aud for a in activations)
    avg_dur = sum(a.duration_min for a in activations) / len(activations) if activations else 0

    return DemandResponseDashboard(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_contracted_mw=total_contracted,
        total_available_mw=total_available,
        activations_ytd=len(activations),
        total_activation_cost_m_aud=total_cost,
        avoided_voll_m_aud=round(total_avoided, 2),
        avg_activation_duration_min=avg_dur,
        contracts=contracts,
        activations=activations,
        providers=providers,
    )


@router.get("/api/demand-response/dashboard", response_model=DemandResponseDashboard,
         summary="Demand Response & RERT Dashboard", tags=["Demand Response"])
def get_demand_response_dashboard() -> DemandResponseDashboard:
    """RERT contracts, demand response activations, and provider analytics."""
    cached = _cache_get("demand_response")
    if cached is not None:
        return cached
    result = _build_dr_dashboard()
    _cache_set("demand_response", result, 3600)
    return result


# =========================================================================
# 10. FREQUENCY DASHBOARD
# =========================================================================

class FrequencyRecord(BaseModel):
    timestamp: str
    frequency_hz: float
    rocof_hz_per_s: float
    region: str
    deviation_hz: float
    band: str


class InertiaRecord(BaseModel):
    timestamp: str
    region: str
    synchronous_mws: float
    synthetic_mws: float
    total_mws: float
    min_requirement_mws: float
    adequate: bool
    rocof_risk: str


class FrequencyEventRecord(BaseModel):
    event_id: str
    timestamp: str
    event_type: str
    nadir_hz: float
    zenith_hz: float
    duration_seconds: float
    trigger: str
    region: str
    resolved: bool


class FrequencyDashboard(BaseModel):
    timestamp: str
    current_frequency_hz: float
    current_rocof: float
    current_band: str
    total_synchronous_inertia_mws: float
    recent_frequency: List[FrequencyRecord]
    inertia_by_region: List[InertiaRecord]
    recent_events: List[FrequencyEventRecord]


def _frequency_band(hz: float) -> str:
    if 49.85 <= hz <= 50.15:
        return "normal"
    elif 49.5 <= hz <= 50.5:
        return "warning"
    return "emergency"


def _build_frequency_dashboard() -> FrequencyDashboard:
    now = datetime.now(timezone.utc)
    ts = now.isoformat()
    seed = int(now.timestamp() / 5)

    # Build recent frequency series (60 points, 5-sec intervals)
    recent = []
    for i in range(60):
        pt = now - timedelta(seconds=5 * (59 - i))
        t = (seed - (59 - i)) * 0.3
        osc = 0.03 * math.sin(t) + 0.015 * math.sin(t * 2.7 + 1.1)
        excursion = 0.10 * math.sin(t * 4.0) if (seed + i) % 13 == 0 else 0.0
        hz = round(max(49.2, min(50.8, 50.0 + osc + excursion)), 4)
        if i == 0:
            rocof = 0.0
        else:
            rocof = round((hz - recent[-1].frequency_hz) / 5, 6)
        recent.append(FrequencyRecord(
            timestamp=pt.isoformat(), frequency_hz=hz, rocof_hz_per_s=rocof,
            region="NEM", deviation_hz=round(hz - 50.0, 6), band=_frequency_band(hz),
        ))

    inertia = [
        InertiaRecord(timestamp=ts, region="NSW1", synchronous_mws=14500.0, synthetic_mws=1200.0,
                      total_mws=15700.0, min_requirement_mws=6000.0, adequate=True, rocof_risk="low"),
        InertiaRecord(timestamp=ts, region="QLD1", synchronous_mws=12800.0, synthetic_mws=900.0,
                      total_mws=13700.0, min_requirement_mws=5500.0, adequate=True, rocof_risk="low"),
        InertiaRecord(timestamp=ts, region="VIC1", synchronous_mws=8200.0, synthetic_mws=1500.0,
                      total_mws=9700.0, min_requirement_mws=4000.0, adequate=True, rocof_risk="medium"),
        InertiaRecord(timestamp=ts, region="SA1", synchronous_mws=1800.0, synthetic_mws=2200.0,
                      total_mws=4000.0, min_requirement_mws=1500.0, adequate=True, rocof_risk="high"),
        InertiaRecord(timestamp=ts, region="TAS1", synchronous_mws=3500.0, synthetic_mws=400.0,
                      total_mws=3900.0, min_requirement_mws=1200.0, adequate=True, rocof_risk="medium"),
    ]

    events = [
        FrequencyEventRecord(event_id="FE-2025-0142", timestamp=(now - timedelta(hours=3)).isoformat(),
                             event_type="Under-frequency", nadir_hz=49.72, zenith_hz=50.02,
                             duration_seconds=8.2, trigger="Generator trip — Callide C4 460MW",
                             region="QLD1", resolved=True),
        FrequencyEventRecord(event_id="FE-2025-0141", timestamp=(now - timedelta(hours=18)).isoformat(),
                             event_type="Over-frequency", nadir_hz=49.98, zenith_hz=50.28,
                             duration_seconds=4.1, trigger="Interconnector trip — Basslink",
                             region="TAS1", resolved=True),
        FrequencyEventRecord(event_id="FE-2025-0140", timestamp=(now - timedelta(days=1, hours=6)).isoformat(),
                             event_type="Under-frequency", nadir_hz=49.65, zenith_hz=50.01,
                             duration_seconds=12.5, trigger="Load surge — smelter reconnection",
                             region="VIC1", resolved=True),
    ]

    current = recent[-1] if recent else None
    total_sync = sum(i.synchronous_mws for i in inertia)

    return FrequencyDashboard(
        timestamp=ts,
        current_frequency_hz=current.frequency_hz if current else 50.0,
        current_rocof=current.rocof_hz_per_s if current else 0.0,
        current_band=current.band if current else "normal",
        total_synchronous_inertia_mws=total_sync,
        recent_frequency=recent,
        inertia_by_region=inertia,
        recent_events=events,
    )


@router.get("/api/frequency/dashboard", response_model=FrequencyDashboard,
         summary="Frequency & Inertia Dashboard", tags=["Grid Operations"])
def get_frequency_dashboard() -> FrequencyDashboard:
    """Real-time frequency, ROCOF, inertia by region, and recent frequency events."""
    cached = _cache_get("frequency_dashboard")
    if cached is not None:
        return cached
    result = _build_frequency_dashboard()
    _cache_set("frequency_dashboard", result, 5)
    return result


# =========================================================================
# 11. ENERGY CYBER SECURITY DASHBOARD
# =========================================================================

class ESCAIncidentRecord(BaseModel):
    incident_id: str
    date: str
    severity: str
    category: str
    affected_systems: List[str]
    region: str
    status: str
    response_time_min: float
    description: str


class ESCAMaturityRecord(BaseModel):
    domain: str
    current_score: float
    target_score: float
    gap: float
    priority: str


class ESCAVulnerabilityRecord(BaseModel):
    cve_id: str
    severity: str
    affected_component: str
    exploit_available: bool
    patch_available: bool
    remediation_status: str


class ESCAAssetRecord(BaseModel):
    asset_class: str
    count: int
    patched_pct: float
    monitored_pct: float
    avg_age_years: float
    eol_pct: float


class ESCADashboard(BaseModel):
    timestamp: str
    incidents: List[ESCAIncidentRecord]
    maturity: List[ESCAMaturityRecord]
    vulnerabilities: List[ESCAVulnerabilityRecord]
    assets: List[ESCAAssetRecord]


def _build_esca_dashboard() -> ESCADashboard:
    incidents = [
        ESCAIncidentRecord(incident_id="ESCA-2025-0042", date="2025-10-14", severity="HIGH",
                           category="Phishing", affected_systems=["SCADA HMI", "Corporate Email"],
                           region="NSW1", status="Contained", response_time_min=12.5,
                           description="Targeted spear-phishing campaign aimed at SCADA operators"),
        ESCAIncidentRecord(incident_id="ESCA-2025-0041", date="2025-10-12", severity="MEDIUM",
                           category="Malware", affected_systems=["Engineering Workstation"],
                           region="VIC1", status="Resolved", response_time_min=28.0,
                           description="Malware detected on engineering workstation via USB media"),
        ESCAIncidentRecord(incident_id="ESCA-2025-0040", date="2025-10-10", severity="CRITICAL",
                           category="Network Intrusion", affected_systems=["OT Network", "Firewall"],
                           region="QLD1", status="Investigating", response_time_min=5.2,
                           description="Anomalous traffic pattern detected crossing IT/OT boundary"),
    ]
    maturity = [
        ESCAMaturityRecord(domain="Asset Management", current_score=2.8, target_score=4.0, gap=1.2, priority="High"),
        ESCAMaturityRecord(domain="Access Control", current_score=3.5, target_score=4.5, gap=1.0, priority="High"),
        ESCAMaturityRecord(domain="Detection & Response", current_score=3.2, target_score=4.0, gap=0.8, priority="Medium"),
        ESCAMaturityRecord(domain="Recovery Planning", current_score=2.5, target_score=4.0, gap=1.5, priority="Critical"),
        ESCAMaturityRecord(domain="Supply Chain Security", current_score=2.0, target_score=3.5, gap=1.5, priority="Critical"),
    ]
    vulnerabilities = [
        ESCAVulnerabilityRecord(cve_id="CVE-2025-21987", severity="CRITICAL", affected_component="Siemens S7-1500 PLC",
                                exploit_available=True, patch_available=True, remediation_status="Patching in progress"),
        ESCAVulnerabilityRecord(cve_id="CVE-2025-18432", severity="HIGH", affected_component="ABB RTU560",
                                exploit_available=False, patch_available=True, remediation_status="Scheduled"),
        ESCAVulnerabilityRecord(cve_id="CVE-2025-15891", severity="MEDIUM", affected_component="GE MarkVIe Turbine Controller",
                                exploit_available=False, patch_available=False, remediation_status="Mitigating controls applied"),
    ]
    assets = [
        ESCAAssetRecord(asset_class="SCADA Servers", count=245, patched_pct=62.0, monitored_pct=88.0, avg_age_years=8.5, eol_pct=22.0),
        ESCAAssetRecord(asset_class="RTUs", count=1820, patched_pct=45.0, monitored_pct=72.0, avg_age_years=12.0, eol_pct=35.0),
        ESCAAssetRecord(asset_class="PLCs", count=3400, patched_pct=38.0, monitored_pct=65.0, avg_age_years=10.5, eol_pct=28.0),
        ESCAAssetRecord(asset_class="Network Switches", count=890, patched_pct=85.0, monitored_pct=95.0, avg_age_years=5.0, eol_pct=8.0),
        ESCAAssetRecord(asset_class="IoT Sensors", count=12500, patched_pct=32.0, monitored_pct=55.0, avg_age_years=3.5, eol_pct=12.0),
    ]
    return ESCADashboard(
        timestamp=datetime.now(timezone.utc).isoformat(),
        incidents=incidents,
        maturity=maturity,
        vulnerabilities=vulnerabilities,
        assets=assets,
    )


@router.get("/api/energy-cyber-security/dashboard", response_model=ESCADashboard,
         summary="Energy Sector Cyber Security Dashboard", tags=["Cyber Security"])
def get_esca_dashboard() -> ESCADashboard:
    """OT/ICS security incidents, maturity scores, vulnerabilities, and asset inventory."""
    cached = _cache_get("esca_dashboard")
    if cached is not None:
        return cached
    result = _build_esca_dashboard()
    _cache_set("esca_dashboard", result, 300)
    return result


# =========================================================================
# 12. DER (Distributed Energy Resources) DASHBOARD
# =========================================================================

class VppUnit(BaseModel):
    vpp_id: str
    name: str
    region: str
    operator: str
    enrolled_sites: int
    total_capacity_mw: float
    available_mw: float
    dispatched_mw: float
    technology: str
    response_time_s: float


class DerSummary(BaseModel):
    region: str
    rooftop_solar_gw: float
    btm_battery_gwh: float
    ev_chargers: int
    smart_inverters: int
    active_vpp_mw: float
    net_demand_reduction_mw: float


class DerDashboard(BaseModel):
    timestamp: str
    nem_rooftop_solar_gw: float
    nem_btm_battery_gwh: float
    nem_net_demand_reduction_mw: float
    vpp_fleet: List[VppUnit]
    regional_der: List[DerSummary]


def _build_der_dashboard() -> DerDashboard:
    vpp_fleet = [
        VppUnit(vpp_id="VPP-SA-001", name="SA Power Networks VPP", region="SA1", operator="Tesla / SAPN",
                enrolled_sites=12_400, total_capacity_mw=62.0, available_mw=55.0, dispatched_mw=18.0,
                technology="Residential Battery", response_time_s=2.0),
        VppUnit(vpp_id="VPP-VIC-001", name="AGL Virtual Power Plant", region="VIC1", operator="AGL Energy",
                enrolled_sites=8_200, total_capacity_mw=41.0, available_mw=38.0, dispatched_mw=12.0,
                technology="Residential Battery + Solar", response_time_s=3.5),
        VppUnit(vpp_id="VPP-NSW-001", name="Endeavour Energy VPP", region="NSW1", operator="Endeavour / Origin",
                enrolled_sites=6_800, total_capacity_mw=34.0, available_mw=30.0, dispatched_mw=8.0,
                technology="Residential Battery", response_time_s=4.0),
        VppUnit(vpp_id="VPP-QLD-001", name="Energex Virtual Solar", region="QLD1", operator="Energex / Simply",
                enrolled_sites=15_200, total_capacity_mw=76.0, available_mw=68.0, dispatched_mw=22.0,
                technology="Solar + Battery", response_time_s=2.5),
    ]
    regional_der = [
        DerSummary(region="NSW1", rooftop_solar_gw=5.8, btm_battery_gwh=1.2, ev_chargers=42_000,
                   smart_inverters=380_000, active_vpp_mw=34.0, net_demand_reduction_mw=1850.0),
        DerSummary(region="QLD1", rooftop_solar_gw=7.2, btm_battery_gwh=0.9, ev_chargers=28_000,
                   smart_inverters=420_000, active_vpp_mw=76.0, net_demand_reduction_mw=2400.0),
        DerSummary(region="VIC1", rooftop_solar_gw=4.5, btm_battery_gwh=1.0, ev_chargers=38_000,
                   smart_inverters=310_000, active_vpp_mw=41.0, net_demand_reduction_mw=1420.0),
        DerSummary(region="SA1", rooftop_solar_gw=2.8, btm_battery_gwh=0.8, ev_chargers=18_000,
                   smart_inverters=195_000, active_vpp_mw=62.0, net_demand_reduction_mw=980.0),
        DerSummary(region="TAS1", rooftop_solar_gw=0.4, btm_battery_gwh=0.1, ev_chargers=4_000,
                   smart_inverters=28_000, active_vpp_mw=0.0, net_demand_reduction_mw=120.0),
    ]
    total_solar = sum(r.rooftop_solar_gw for r in regional_der)
    total_battery = sum(r.btm_battery_gwh for r in regional_der)
    total_reduction = sum(r.net_demand_reduction_mw for r in regional_der)

    return DerDashboard(
        timestamp=datetime.now(timezone.utc).isoformat(),
        nem_rooftop_solar_gw=round(total_solar, 1),
        nem_btm_battery_gwh=round(total_battery, 1),
        nem_net_demand_reduction_mw=round(total_reduction, 1),
        vpp_fleet=vpp_fleet,
        regional_der=regional_der,
    )


@router.get("/api/der/dashboard", response_model=DerDashboard,
         summary="Distributed Energy Resources Dashboard", tags=["Renewable Energy"])
def get_der_dashboard() -> DerDashboard:
    """Rooftop solar, behind-the-meter batteries, VPP fleet, and DER by region."""
    cached = _cache_get("der_dashboard")
    if cached is not None:
        return cached
    result = _build_der_dashboard()
    _cache_set("der_dashboard", result, 60)
    return result
