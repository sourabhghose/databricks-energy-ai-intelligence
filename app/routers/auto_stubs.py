"""Auto-generated stub endpoints with real NEMWEB data where available."""
from __future__ import annotations

import random as _r
from datetime import datetime as _dt

from fastapi import APIRouter

from .shared import _CATALOG, _query_gold

router = APIRouter()


# --- Shared data caches (lazy-loaded, refreshed per request cycle) ---
_cache = {}


def _get_cached(key, query_fn):
    """Simple per-process cache for gold table queries."""
    import time
    now = time.time()
    if key in _cache and now - _cache[key][1] < 300:  # 5 min TTL
        return _cache[key][0]
    try:
        data = query_fn()
        if data:
            _cache[key] = (data, now)
            return data
    except Exception:
        pass
    return _cache.get(key, (None, 0))[0]


def _battery_data():
    """Fetch battery/storage facility data."""
    return _get_cached('battery', lambda: _query_gold(f"""
        SELECT duid, station_name, region_id, fuel_type, capacity_mw
        FROM {_CATALOG}.gold.nem_facilities
        WHERE LOWER(fuel_type) LIKE '%battery%'
        ORDER BY capacity_mw DESC
    """))


def _renewable_data():
    """Fetch renewable generation data."""
    return _get_cached('renewable', lambda: _query_gold(f"""
        SELECT region_id, fuel_type, SUM(total_mw) AS total_mw, SUM(unit_count) AS units,
               AVG(capacity_factor) AS avg_cf
        FROM {_CATALOG}.gold.nem_generation_by_fuel
        WHERE is_renewable = true
        AND interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
        GROUP BY region_id, fuel_type
        ORDER BY total_mw DESC
    """))


def _carbon_data():
    """Fetch emissions data."""
    return _get_cached('carbon', lambda: _query_gold(f"""
        SELECT region_id, fuel_type, is_renewable,
               AVG(emissions_intensity) AS avg_intensity,
               SUM(emissions_tco2e) AS total_emissions,
               SUM(total_mw) AS total_mw
        FROM {_CATALOG}.gold.nem_generation_by_fuel
        WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
        GROUP BY region_id, fuel_type, is_renewable
    """))


def _generation_data():
    """Fetch generation by fuel type."""
    return _get_cached('generation', lambda: _query_gold(f"""
        SELECT fuel_type, region_id, SUM(total_mw) AS total_mw,
               SUM(unit_count) AS units, AVG(capacity_factor) AS avg_cf
        FROM {_CATALOG}.gold.nem_generation_by_fuel
        WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
        GROUP BY fuel_type, region_id
        ORDER BY total_mw DESC
    """))


def _price_data():
    """Fetch regional price statistics."""
    return _get_cached('prices', lambda: _query_gold(f"""
        SELECT region_id, AVG(rrp) AS avg_price, STDDEV(rrp) AS price_vol,
               MAX(rrp) AS max_price, MIN(rrp) AS min_price,
               AVG(total_demand_mw) AS avg_demand,
               COUNT(*) AS intervals
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE interval_datetime >= current_timestamp() - INTERVAL 14 DAYS
        GROUP BY region_id
    """))


def _grid_data():
    """Fetch interconnector and grid data."""
    return _get_cached('grid', lambda: _query_gold(f"""
        SELECT interconnector_id, from_region, to_region,
               AVG(mw_flow) AS avg_flow, AVG(utilization_pct) AS avg_util,
               SUM(CASE WHEN is_congested THEN 1 ELSE 0 END) AS congested_intervals,
               COUNT(*) AS total_intervals
        FROM {_CATALOG}.gold.nem_interconnectors
        WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
        GROUP BY interconnector_id, from_region, to_region
    """))


def _facilities_data():
    """Fetch all facilities."""
    return _get_cached('facilities', lambda: _query_gold(f"""
        SELECT duid, station_name, region_id, fuel_type, is_renewable, capacity_mw
        FROM {_CATALOG}.gold.nem_facilities
        ORDER BY capacity_mw DESC
    """))


def _build_battery_response(label, seed):
    """Build response for battery/storage endpoints."""
    rows = _battery_data()
    if rows:
        total_cap = sum(float(r.get("capacity_mw") or 0) for r in rows)
        regions = {}
        for r in rows:
            reg = r.get("region_id", "NSW1")
            regions.setdefault(reg, {"count": 0, "capacity_mw": 0})
            regions[reg]["count"] += 1
            regions[reg]["capacity_mw"] += float(r.get("capacity_mw") or 0)
        return {
            "summary": {"title": label, "total_capacity_mw": round(total_cap, 1),
                        "total_units": len(rows), "regions": len(regions),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"duid": r.get("duid"), "station_name": r.get("station_name"),
                         "region": r.get("region_id"), "capacity_mw": float(r.get("capacity_mw") or 0),
                         "fuel_type": r.get("fuel_type")} for r in rows[:20]],
            "by_region": [{"region": k, **v} for k, v in regions.items()]
        }
    return None


def _build_renewable_response(label, seed):
    """Build response for renewable endpoints."""
    rows = _renewable_data()
    if rows:
        total_mw = sum(float(r.get("total_mw") or 0) for r in rows)
        by_type = {}
        for r in rows:
            ft = r.get("fuel_type", "unknown")
            by_type.setdefault(ft, 0)
            by_type[ft] += float(r.get("total_mw") or 0)
        return {
            "summary": {"title": label, "total_renewable_mw": round(total_mw, 1),
                        "fuel_types": len(by_type), "last_updated": _dt.utcnow().isoformat()},
            "records": [{"region": r.get("region_id"), "fuel_type": r.get("fuel_type"),
                         "total_mw": round(float(r.get("total_mw") or 0), 1),
                         "capacity_factor": round(float(r.get("avg_cf") or 0), 2)} for r in rows[:20]],
            "by_fuel_type": [{"fuel_type": k, "total_mw": round(v, 1)} for k, v in sorted(by_type.items(), key=lambda x: -x[1])]
        }
    return None


def _build_carbon_response(label, seed):
    """Build response for carbon/emissions endpoints."""
    rows = _carbon_data()
    if rows:
        total_em = sum(float(r.get("total_emissions") or 0) for r in rows)
        total_mw = sum(float(r.get("total_mw") or 0) for r in rows)
        ren_mw = sum(float(r.get("total_mw") or 0) for r in rows if r.get("is_renewable"))
        return {
            "summary": {"title": label, "total_emissions_tco2e": round(total_em, 0),
                        "avg_intensity": round(total_em / max(total_mw, 1), 3),
                        "renewable_share_pct": round(ren_mw / max(total_mw, 1) * 100, 1),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"region": r.get("region_id"), "fuel_type": r.get("fuel_type"),
                         "emissions_tco2e": round(float(r.get("total_emissions") or 0), 0),
                         "intensity": round(float(r.get("avg_intensity") or 0), 3),
                         "generation_mw": round(float(r.get("total_mw") or 0), 1)} for r in rows[:20]]
        }
    return None


def _build_generation_response(label, seed):
    """Build response for generation/fuel endpoints."""
    rows = _generation_data()
    if rows:
        total_mw = sum(float(r.get("total_mw") or 0) for r in rows)
        by_fuel = {}
        for r in rows:
            ft = r.get("fuel_type", "unknown")
            by_fuel.setdefault(ft, 0)
            by_fuel[ft] += float(r.get("total_mw") or 0)
        return {
            "summary": {"title": label, "total_generation_mw": round(total_mw, 1),
                        "fuel_types": len(by_fuel), "last_updated": _dt.utcnow().isoformat()},
            "records": [{"region": r.get("region_id"), "fuel_type": r.get("fuel_type"),
                         "total_mw": round(float(r.get("total_mw") or 0), 1),
                         "units": int(r.get("units") or 0),
                         "capacity_factor": round(float(r.get("avg_cf") or 0), 2)} for r in rows[:20]],
            "by_fuel_type": [{"fuel_type": k, "total_mw": round(v, 1), "share_pct": round(v / max(total_mw, 1) * 100, 1)} for k, v in sorted(by_fuel.items(), key=lambda x: -x[1])]
        }
    return None


def _build_pricing_response(label, seed):
    """Build response for pricing/market endpoints."""
    rows = _price_data()
    if rows:
        avg_price = sum(float(r.get("avg_price") or 0) for r in rows) / max(len(rows), 1)
        return {
            "summary": {"title": label, "nem_avg_price_aud_mwh": round(avg_price, 2),
                        "regions": len(rows), "last_updated": _dt.utcnow().isoformat()},
            "records": [{"region": r.get("region_id"),
                         "avg_price": round(float(r.get("avg_price") or 0), 2),
                         "volatility": round(float(r.get("price_vol") or 0), 2),
                         "max_price": round(float(r.get("max_price") or 0), 2),
                         "min_price": round(float(r.get("min_price") or 0), 2),
                         "avg_demand_mw": round(float(r.get("avg_demand") or 0), 0)} for r in rows]
        }
    return None


def _build_grid_response(label, seed):
    """Build response for grid/network endpoints."""
    rows = _grid_data()
    if rows:
        total_flow = sum(abs(float(r.get("avg_flow") or 0)) for r in rows)
        congested = sum(1 for r in rows if int(r.get("congested_intervals") or 0) > 0)
        return {
            "summary": {"title": label, "interconnectors": len(rows),
                        "congested_count": congested,
                        "total_flow_mw": round(total_flow, 0),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"interconnector": r.get("interconnector_id"),
                         "from_region": r.get("from_region"), "to_region": r.get("to_region"),
                         "avg_flow_mw": round(float(r.get("avg_flow") or 0), 1),
                         "utilization_pct": round(float(r.get("avg_util") or 0), 1),
                         "congested_intervals": int(r.get("congested_intervals") or 0)} for r in rows]
        }
    return None


def _constraints_data():
    """Fetch network constraints data."""
    return _get_cached('constraints', lambda: _query_gold(f"""
        SELECT constraint_id, constraint_type, region, is_binding,
               AVG(ABS(marginal_value)) AS avg_marginal_value,
               COUNT(*) AS intervals
        FROM {_CATALOG}.nemweb_analytics.gold_nem_constraints
        WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
        GROUP BY constraint_id, constraint_type, region, is_binding
        ORDER BY avg_marginal_value DESC
        LIMIT 50
    """))


def _isp_projects_data():
    """Fetch ISP project data."""
    return _get_cached('isp_projects', lambda: _query_gold(f"""
        SELECT project_name, type, status, expected_completion, capex_m_aud,
               tnsp, route_km, capacity_mw
        FROM {_CATALOG}.gold.isp_projects
        ORDER BY capex_m_aud DESC
    """))


def _isp_capacity_data():
    """Fetch ISP capacity outlook data."""
    return _get_cached('isp_capacity', lambda: _query_gold(f"""
        SELECT scenario, year, region, fuel_type, capacity_mw, generation_twh
        FROM {_CATALOG}.gold.isp_capacity_outlook
        ORDER BY year, region, fuel_type
    """))


def _rez_data():
    """Fetch REZ assessment data."""
    return _get_cached('rez', lambda: _query_gold(f"""
        SELECT rez_name, region, solar_capacity_mw, wind_capacity_mw,
               network_capacity_mw, development_status, score
        FROM {_CATALOG}.gold.rez_assessments
        ORDER BY score DESC
    """))


def _tariff_data():
    """Fetch retail tariff data."""
    return _get_cached('tariffs', lambda: _query_gold(f"""
        SELECT t.plan_id, t.retailer, t.state, t.fuel_type, t.plan_type,
               c.component_type, c.rate_aud_kwh, c.daily_supply_charge_aud, c.tou_period
        FROM {_CATALOG}.gold.retail_tariffs t
        LEFT JOIN {_CATALOG}.gold.tariff_components c ON t.plan_id = c.plan_id
        ORDER BY t.retailer, t.state
    """))


def _lgc_data():
    """Fetch LGC registry and spot price data."""
    return _get_cached('lgc', lambda: _query_gold(f"""
        SELECT power_station, state, fuel_type, capacity_mw, lgc_created_mwh, year, quarter
        FROM {_CATALOG}.gold.lgc_registry
        ORDER BY lgc_created_mwh DESC
    """))


def _lgc_spot_data():
    """Fetch LGC spot price data."""
    return _get_cached('lgc_spot', lambda: _query_gold(f"""
        SELECT trade_date, price_aud_mwh, source
        FROM {_CATALOG}.gold.lgc_spot_prices
        ORDER BY trade_date DESC
        LIMIT 100
    """))


def _build_constraints_response(label, seed):
    """Build response for network constraints endpoints."""
    rows = _constraints_data()
    if rows:
        binding = [r for r in rows if r.get("is_binding")]
        by_type = {}
        for r in rows:
            ct = r.get("constraint_type", "UNKNOWN")
            by_type.setdefault(ct, 0)
            by_type[ct] += 1
        return {
            "summary": {"title": label, "total_constraints": len(rows),
                        "binding_count": len(binding),
                        "constraint_types": len(by_type),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"constraint_id": r.get("constraint_id"),
                         "constraint_type": r.get("constraint_type"),
                         "region": r.get("region"),
                         "is_binding": r.get("is_binding"),
                         "avg_marginal_value": round(float(r.get("avg_marginal_value") or 0), 2),
                         "intervals": int(r.get("intervals") or 0)} for r in rows[:20]],
            "by_type": [{"type": k, "count": v} for k, v in sorted(by_type.items(), key=lambda x: -x[1])]
        }
    return None


def _build_isp_response(label, seed):
    """Build response for ISP project endpoints."""
    rows = _isp_projects_data()
    if rows:
        total_capex = sum(float(r.get("capex_m_aud") or 0) for r in rows)
        total_cap = sum(float(r.get("capacity_mw") or 0) for r in rows)
        by_status = {}
        for r in rows:
            s = r.get("status", "Unknown")
            by_status.setdefault(s, 0)
            by_status[s] += 1
        return {
            "summary": {"title": label, "total_projects": len(rows),
                        "total_capex_m_aud": round(total_capex, 0),
                        "total_capacity_mw": round(total_cap, 0),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"project_name": r.get("project_name"), "type": r.get("type"),
                         "status": r.get("status"),
                         "expected_completion": r.get("expected_completion"),
                         "capex_m_aud": float(r.get("capex_m_aud") or 0),
                         "tnsp": r.get("tnsp"),
                         "route_km": float(r.get("route_km") or 0),
                         "capacity_mw": float(r.get("capacity_mw") or 0)} for r in rows],
            "by_status": [{"status": k, "count": v} for k, v in sorted(by_status.items(), key=lambda x: -x[1])]
        }
    return None


def _build_isp_capacity_response(label, seed):
    """Build response for ISP capacity outlook endpoints."""
    rows = _isp_capacity_data()
    if rows:
        by_year = {}
        for r in rows:
            y = str(r.get("year", ""))
            by_year.setdefault(y, {"capacity_mw": 0, "generation_twh": 0})
            by_year[y]["capacity_mw"] += float(r.get("capacity_mw") or 0)
            by_year[y]["generation_twh"] += float(r.get("generation_twh") or 0)
        return {
            "summary": {"title": label, "scenarios": 1, "years": len(by_year),
                        "data_points": len(rows),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"scenario": r.get("scenario"), "year": r.get("year"),
                         "region": r.get("region"), "fuel_type": r.get("fuel_type"),
                         "capacity_mw": float(r.get("capacity_mw") or 0),
                         "generation_twh": float(r.get("generation_twh") or 0)} for r in rows],
            "by_year": [{"year": k, "total_capacity_mw": round(v["capacity_mw"], 0),
                         "total_generation_twh": round(v["generation_twh"], 1)} for k, v in sorted(by_year.items())]
        }
    return None


def _build_rez_response(label, seed):
    """Build response for REZ endpoints."""
    rows = _rez_data()
    if rows:
        total_solar = sum(float(r.get("solar_capacity_mw") or 0) for r in rows)
        total_wind = sum(float(r.get("wind_capacity_mw") or 0) for r in rows)
        by_status = {}
        for r in rows:
            s = r.get("development_status", "Unknown")
            by_status.setdefault(s, 0)
            by_status[s] += 1
        return {
            "summary": {"title": label, "total_zones": len(rows),
                        "total_solar_mw": round(total_solar, 0),
                        "total_wind_mw": round(total_wind, 0),
                        "avg_score": round(sum(float(r.get("score") or 0) for r in rows) / max(len(rows), 1), 1),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"rez_name": r.get("rez_name"), "region": r.get("region"),
                         "solar_capacity_mw": float(r.get("solar_capacity_mw") or 0),
                         "wind_capacity_mw": float(r.get("wind_capacity_mw") or 0),
                         "network_capacity_mw": float(r.get("network_capacity_mw") or 0),
                         "development_status": r.get("development_status"),
                         "score": float(r.get("score") or 0)} for r in rows],
            "by_status": [{"status": k, "count": v} for k, v in sorted(by_status.items(), key=lambda x: -x[1])]
        }
    return None


def _build_tariff_response(label, seed):
    """Build response for tariff/retail endpoints."""
    rows = _tariff_data()
    if rows:
        retailers = set(r.get("retailer") for r in rows if r.get("retailer"))
        states = set(r.get("state") for r in rows if r.get("state"))
        avg_rate = sum(float(r.get("rate_aud_kwh") or 0) for r in rows if r.get("rate_aud_kwh")) / max(sum(1 for r in rows if r.get("rate_aud_kwh")), 1)
        return {
            "summary": {"title": label, "retailers": len(retailers),
                        "states": len(states), "plans": len(set(r.get("plan_id") for r in rows)),
                        "avg_rate_aud_kwh": round(avg_rate, 4),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"plan_id": r.get("plan_id"), "retailer": r.get("retailer"),
                         "state": r.get("state"), "fuel_type": r.get("fuel_type"),
                         "plan_type": r.get("plan_type"),
                         "component_type": r.get("component_type"),
                         "rate_aud_kwh": float(r.get("rate_aud_kwh") or 0),
                         "daily_supply_charge_aud": float(r.get("daily_supply_charge_aud") or 0),
                         "tou_period": r.get("tou_period")} for r in rows[:30]]
        }
    return None


def _build_lgc_response(label, seed):
    """Build response for LGC/REC market endpoints."""
    rows = _lgc_data()
    spot = _lgc_spot_data()
    if rows:
        total_lgc = sum(float(r.get("lgc_created_mwh") or 0) for r in rows)
        by_fuel = {}
        for r in rows:
            ft = r.get("fuel_type", "unknown")
            by_fuel.setdefault(ft, 0)
            by_fuel[ft] += float(r.get("lgc_created_mwh") or 0)
        result = {
            "summary": {"title": label, "total_generators": len(rows),
                        "total_lgc_created_mwh": round(total_lgc, 0),
                        "fuel_types": len(by_fuel),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"power_station": r.get("power_station"), "state": r.get("state"),
                         "fuel_type": r.get("fuel_type"),
                         "capacity_mw": float(r.get("capacity_mw") or 0),
                         "lgc_created_mwh": float(r.get("lgc_created_mwh") or 0),
                         "year": r.get("year"), "quarter": r.get("quarter")} for r in rows[:20]],
            "by_fuel_type": [{"fuel_type": k, "lgc_mwh": round(v, 0)} for k, v in sorted(by_fuel.items(), key=lambda x: -x[1])]
        }
        if spot:
            result["spot_prices"] = [{"trade_date": s.get("trade_date"),
                                       "price_aud_mwh": float(s.get("price_aud_mwh") or 0),
                                       "source": s.get("source")} for s in spot[:20]]
        return result
    return None


# --- Tier 3: New data fetchers for pipelines 22-29 ---

def _safeguard_data():
    return _get_cached('safeguard', lambda: _query_gold(f"""
        SELECT facility_name, company, sector, state, baseline_tco2e,
               reported_emissions_tco2e, excess_emissions_tco2e, accus_surrendered,
               compliance_year, status
        FROM {_CATALOG}.gold.safeguard_facilities ORDER BY baseline_tco2e DESC
    """))

def _accu_data():
    return _get_cached('accu', lambda: _query_gold(f"""
        SELECT trade_date, accu_spot_price_aud, volume_traded,
               total_accus_issued, total_accus_surrendered
        FROM {_CATALOG}.gold.accu_market ORDER BY trade_date DESC
    """))

def _aemc_data():
    return _get_cached('aemc_rules', lambda: _query_gold(f"""
        SELECT title, proponent, status, rule_type, date_initiated, date_completed,
               category, description
        FROM {_CATALOG}.gold.aemc_rule_changes ORDER BY date_initiated DESC
    """))

def _btm_data():
    return _get_cached('btm', lambda: _query_gold(f"""
        SELECT category, state, year, quarter, cumulative_installations,
               new_installations, total_capacity_kw, avg_system_size_kw
        FROM {_CATALOG}.gold.btm_installations ORDER BY year DESC, state
    """))

def _rab_data():
    return _get_cached('rab', lambda: _query_gold(f"""
        SELECT network_business, network_type, state, regulatory_period,
               rab_opening_m_aud, rab_closing_m_aud, allowed_revenue_m_aud,
               capex_allowance_m_aud, opex_allowance_m_aud, wacc_nominal_pct, status
        FROM {_CATALOG}.gold.rab_determinations ORDER BY rab_opening_m_aud DESC
    """))

def _consumer_data():
    return _get_cached('consumer_prot', lambda: _query_gold(f"""
        SELECT metric_type, retailer, state, year, quarter, metric_value, metric_unit
        FROM {_CATALOG}.gold.consumer_protection ORDER BY year DESC, metric_type
    """))

def _tnsp_perf_data():
    return _get_cached('tnsp_perf', lambda: _query_gold(f"""
        SELECT tnsp, state, year, circuit_km, energy_delivered_gwh,
               peak_demand_mw, loss_factor_pct, opex_per_km_aud, capex_m_aud
        FROM {_CATALOG}.gold.tnsp_performance ORDER BY year DESC, tnsp
    """))

def _tuos_data():
    return _get_cached('tuos', lambda: _query_gold(f"""
        SELECT tnsp, state, financial_year, locational_aud_per_kw,
               non_locational_aud_per_mwh, common_service_aud_per_mwh, total_tuos_revenue_m_aud
        FROM {_CATALOG}.gold.tuos_charges ORDER BY tnsp
    """))

def _rit_data():
    return _get_cached('rit', lambda: _query_gold(f"""
        SELECT project_name, rit_type, proponent, state, status,
               estimated_cost_m_aud, net_benefit_m_aud, date_published,
               date_completed, preferred_option, description
        FROM {_CATALOG}.gold.rit_register ORDER BY estimated_cost_m_aud DESC
    """))

def _offshore_wind_data():
    return _get_cached('offshore_wind', lambda: _query_gold(f"""
        SELECT area_name, state, declared_date, area_sq_km, estimated_capacity_gw,
               water_depth_m, distance_from_shore_km, licence_applicants, status,
               key_projects, description
        FROM {_CATALOG}.gold.offshore_wind_areas ORDER BY estimated_capacity_gw DESC
    """))


@router.get("/api/aemc-rule-change/dashboard")
def aemc_rule_change_dashboard():
    rows = _aemc_data()
    if rows:
        in_progress = [r for r in rows if r.get("status") == "IN_PROGRESS"]
        by_cat = {}
        for r in rows:
            c = r.get("category", "OTHER")
            by_cat.setdefault(c, 0)
            by_cat[c] += 1
        return {
            "summary": {"title": "AEMC Rule Change Register", "total_rules": len(rows),
                        "in_progress": len(in_progress), "categories": len(by_cat),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"title": r.get("title"), "proponent": r.get("proponent"),
                         "status": r.get("status"), "rule_type": r.get("rule_type"),
                         "category": r.get("category"), "date_initiated": r.get("date_initiated"),
                         "description": (r.get("description") or "")[:200]} for r in rows],
            "by_category": [{"category": k, "count": v} for k, v in sorted(by_cat.items(), key=lambda x: -x[1])]}
    _r.seed(6000)
    return {
        "summary": {"title": "Aemc Rule Change > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ai-digital-twin/dashboard")
def ai_digital_twin_dashboard():
    _r.seed(6001)
    return {
        "summary": {"title": "Ai Digital Twin > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/alerts/test-notification")
def alerts_test_notification():
    _r.seed(6002)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Alerts > Test Notification", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/ancillary-cost-allocation/dashboard")
def ancillary_cost_allocation_dashboard():
    result = _build_pricing_response("Ancillary Cost Allocation > Dashboard", 6003)
    if result:
        return result
    _r.seed(6003)
    return {
        "summary": {"title": "Ancillary Cost Allocation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ancillary-cost/dashboard")
def ancillary_cost_dashboard():
    result = _build_pricing_response("Ancillary Cost > Dashboard", 6004)
    if result:
        return result
    _r.seed(6004)
    return {
        "summary": {"title": "Ancillary Cost > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ancillary-market-depth/dashboard")
def ancillary_market_depth_dashboard():
    result = _build_grid_response("Ancillary Market Depth > Dashboard", 6005)
    if result:
        return result
    _r.seed(6005)
    return {
        "summary": {"title": "Ancillary Market Depth > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ancillary-services-procurement/dashboard")
def ancillary_services_procurement_dashboard():
    result = _build_grid_response("Ancillary Services Procurement > Dashboard", 6006)
    if result:
        return result
    _r.seed(6006)
    return {
        "summary": {"title": "Ancillary Services Procurement > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/asset-management/assets")
def asset_management_assets():
    _r.seed(6007)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Asset Management > Assets", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/asset-management/dashboard")
def asset_management_dashboard():
    _r.seed(6008)
    return {
        "summary": {"title": "Asset Management > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/asset-management/inspections")
def asset_management_inspections():
    _r.seed(6009)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Asset Management > Inspections", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/asset-management/maintenance")
def asset_management_maintenance():
    _r.seed(6010)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Asset Management > Maintenance", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/australia-electricity-export/dashboard")
def australia_electricity_export_dashboard():
    _r.seed(6011)
    return {
        "summary": {"title": "Australia Electricity Export > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/australian-carbon-credit/dashboard")
def australian_carbon_credit_dashboard():
    result = _build_carbon_response("Australian Carbon Credit > Dashboard", 6012)
    if result:
        return result
    _r.seed(6012)
    return {
        "summary": {"title": "Australian Carbon Credit > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/australian-carbon-policy/dashboard")
def australian_carbon_policy_dashboard():
    result = _build_carbon_response("Australian Carbon Policy > Dashboard", 6013)
    if result:
        return result
    _r.seed(6013)
    return {
        "summary": {"title": "Australian Carbon Policy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/battery-dispatch-strategy/dashboard")
def battery_dispatch_strategy_dashboard():
    result = _build_battery_response("Battery Dispatch Strategy > Dashboard", 6014)
    if result:
        return result
    _r.seed(6014)
    return {
        "summary": {"title": "Battery Dispatch Strategy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/battery-revenue-stack/dashboard")
def battery_revenue_stack_dashboard():
    result = _build_battery_response("Battery Revenue Stack > Dashboard", 6015)
    if result:
        return result
    _r.seed(6015)
    return {
        "summary": {"title": "Battery Revenue Stack > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/battery-second-life/dashboard")
def battery_second_life_dashboard():
    result = _build_battery_response("Battery Second Life > Dashboard", 6016)
    if result:
        return result
    _r.seed(6016)
    return {
        "summary": {"title": "Battery Second Life > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/battery-storage-degradation-lifetime/dashboard")
def battery_storage_degradation_lifetime_dashboard():
    result = _build_battery_response("Battery Storage Degradation Lifetime > Dashboard", 6017)
    if result:
        return result
    _r.seed(6017)
    return {
        "summary": {"title": "Battery Storage Degradation Lifetime > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/battery-tech/costs")
def battery_tech_costs():
    result = _build_battery_response("Battery Tech > Costs", 6018)
    if result and "records" in result:
        return result["records"]
    _r.seed(6018)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Battery Tech > Costs", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/battery-tech/dashboard")
def battery_tech_dashboard():
    result = _build_battery_response("Battery Tech > Dashboard", 6019)
    if result:
        return result
    _r.seed(6019)
    return {
        "summary": {"title": "Battery Tech > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/battery-tech/lcos")
def battery_tech_lcos():
    result = _build_battery_response("Battery Tech > Lcos", 6020)
    if result and "records" in result:
        return result["records"]
    _r.seed(6020)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Battery Tech > Lcos", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/battery-tech/supply-chain")
def battery_tech_supply_chain():
    result = _build_battery_response("Battery Tech > Supply Chain", 6021)
    if result and "records" in result:
        return result["records"]
    _r.seed(6021)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Battery Tech > Supply Chain", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/behind-meter-commercial/dashboard")
def behind_meter_commercial_dashboard():
    _r.seed(6022)
    return {
        "summary": {"title": "Behind Meter Commercial > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/bess-degradation/dashboard")
def bess_degradation_dashboard():
    result = _build_battery_response("Bess Degradation > Dashboard", 6023)
    if result:
        return result
    _r.seed(6023)
    return {
        "summary": {"title": "Bess Degradation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/bess-performance/dashboard")
def bess_performance_dashboard():
    result = _build_battery_response("Bess Performance > Dashboard", 6024)
    if result:
        return result
    _r.seed(6024)
    return {
        "summary": {"title": "Bess Performance > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/biogas-landfill/dashboard")
def biogas_landfill_dashboard():
    result = _build_generation_response("Biogas Landfill > Dashboard", 6025)
    if result:
        return result
    _r.seed(6025)
    return {
        "summary": {"title": "Biogas Landfill > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/biomass-bioenergy/dashboard")
def biomass_bioenergy_dashboard():
    result = _build_generation_response("Biomass Bioenergy > Dashboard", 6026)
    if result:
        return result
    _r.seed(6026)
    return {
        "summary": {"title": "Biomass Bioenergy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/biomethane-gas-grid-injection/dashboard")
def biomethane_gas_grid_injection_dashboard():
    result = _build_grid_response("Biomethane Gas Grid Injection > Dashboard", 6027)
    if result:
        return result
    _r.seed(6027)
    return {
        "summary": {"title": "Biomethane Gas Grid Injection > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/black-start/dashboard")
def black_start_dashboard():
    _r.seed(6028)
    return {
        "summary": {"title": "Black Start > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/btm/dashboard")
def btm_dashboard():
    rows = _btm_data()
    if rows:
        by_cat = {}
        for r in rows:
            c = r.get("category", "OTHER")
            by_cat.setdefault(c, {"installations": 0, "capacity_kw": 0})
            by_cat[c]["installations"] += int(r.get("cumulative_installations") or 0)
            by_cat[c]["capacity_kw"] += float(r.get("total_capacity_kw") or 0)
        return {
            "summary": {"title": "Behind-the-Meter > Dashboard", "categories": len(by_cat),
                        "total_records": len(rows), "last_updated": _dt.utcnow().isoformat()},
            "records": [{"category": r.get("category"), "state": r.get("state"),
                         "cumulative": int(r.get("cumulative_installations") or 0),
                         "new": int(r.get("new_installations") or 0),
                         "capacity_kw": float(r.get("total_capacity_kw") or 0)} for r in rows],
            "by_category": [{"category": k, **v} for k, v in by_cat.items()]}
    _r.seed(6029)
    return {
        "summary": {"title": "Btm > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/btm/ev")
def btm_ev():
    rows = _btm_data()
    if rows:
        evs = [r for r in rows if r.get("category") == "EV"]
        if evs:
            return [{"id": i+1, "state": r.get("state"), "cumulative_evs": int(r.get("cumulative_installations") or 0),
                     "new_evs": int(r.get("new_installations") or 0)} for i, r in enumerate(evs)]
    _r.seed(6030)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Btm > Ev", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/btm/home-batteries")
def btm_home_batteries():
    rows = _btm_data()
    if rows:
        batt = [r for r in rows if r.get("category") == "HOME_BATTERY"]
        if batt:
            return [{"id": i+1, "state": r.get("state"), "cumulative": int(r.get("cumulative_installations") or 0),
                     "new": int(r.get("new_installations") or 0),
                     "capacity_kw": float(r.get("total_capacity_kw") or 0)} for i, r in enumerate(batt)]
    _r.seed(6031)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Btm > Home Batteries", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/btm/rooftop-pv")
def btm_rooftop_pv():
    result = _build_renewable_response("Btm > Rooftop Pv", 6032)
    if result and "records" in result:
        return result["records"]
    _r.seed(6032)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Btm > Rooftop Pv", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/capacity-investment-scheme/dashboard")
def capacity_investment_scheme_dashboard():
    _r.seed(6033)
    return {
        "summary": {"title": "Capacity Investment Scheme > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/capacity-mechanism/dashboard")
def capacity_mechanism_dashboard():
    _r.seed(6034)
    return {
        "summary": {"title": "Capacity Mechanism > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon-accounting/dashboard")
def carbon_accounting_dashboard():
    result = _build_carbon_response("Carbon Accounting > Dashboard", 6035)
    if result:
        return result
    _r.seed(6035)
    return {
        "summary": {"title": "Carbon Accounting > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon-border-adjustment-x/dashboard")
def carbon_border_adjustment_x_dashboard():
    result = _build_carbon_response("Carbon Border Adjustment X > Dashboard", 6036)
    if result:
        return result
    _r.seed(6036)
    return {
        "summary": {"title": "Carbon Border Adjustment X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon-border-adjustment/dashboard")
def carbon_border_adjustment_dashboard():
    result = _build_carbon_response("Carbon Border Adjustment > Dashboard", 6037)
    if result:
        return result
    _r.seed(6037)
    return {
        "summary": {"title": "Carbon Border Adjustment > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon-capture-storage-project/dashboard")
def carbon_capture_storage_project_dashboard():
    result = _build_battery_response("Carbon Capture Storage Project > Dashboard", 6038)
    if result:
        return result
    _r.seed(6038)
    return {
        "summary": {"title": "Carbon Capture Storage Project > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon-capture-utilisation/dashboard")
def carbon_capture_utilisation_dashboard():
    result = _build_carbon_response("Carbon Capture Utilisation > Dashboard", 6039)
    if result:
        return result
    _r.seed(6039)
    return {
        "summary": {"title": "Carbon Capture Utilisation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon-credit/dashboard")
def carbon_credit_dashboard():
    result = _build_carbon_response("Carbon Credit > Dashboard", 6040)
    if result:
        return result
    _r.seed(6040)
    return {
        "summary": {"title": "Carbon Credit > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon-offset-market/dashboard")
def carbon_offset_market_dashboard():
    result = _build_carbon_response("Carbon Offset Market > Dashboard", 6041)
    if result:
        return result
    _r.seed(6041)
    return {
        "summary": {"title": "Carbon Offset Market > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon-offset-project-x/dashboard")
def carbon_offset_project_x_dashboard():
    result = _build_carbon_response("Carbon Offset Project X > Dashboard", 6042)
    if result:
        return result
    _r.seed(6042)
    return {
        "summary": {"title": "Carbon Offset Project X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon-offset-project/dashboard")
def carbon_offset_project_dashboard():
    result = _build_carbon_response("Carbon Offset Project > Dashboard", 6043)
    if result:
        return result
    _r.seed(6043)
    return {
        "summary": {"title": "Carbon Offset Project > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon/regions")
def carbon_regions():
    result = _build_carbon_response("Carbon > Regions", 6044)
    if result and "records" in result:
        return result["records"]
    _r.seed(6044)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Carbon > Regions", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/carbon/registry/dashboard")
def carbon_registry_dashboard():
    result = _build_carbon_response("Carbon > Registry > Dashboard", 6045)
    if result:
        return result
    _r.seed(6045)
    return {
        "summary": {"title": "Carbon > Registry > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/carbon/registry/market")
def carbon_registry_market():
    result = _build_carbon_response("Carbon > Registry > Market", 6046)
    if result and "records" in result:
        return result["records"]
    _r.seed(6046)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Carbon > Registry > Market", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/carbon/trajectory")
def carbon_trajectory():
    result = _build_carbon_response("Carbon > Trajectory", 6047)
    if result and "records" in result:
        return result["records"]
    _r.seed(6047)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Carbon > Trajectory", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/causer-pays/dashboard")
def causer_pays_dashboard():
    rows = _get_cached('causer_pays', lambda: _query_gold(f"""
        SELECT region_id,
               AVG(raise_6sec_price) AS avg_raise6, AVG(raise_60sec_price) AS avg_raise60,
               AVG(raise_5min_price) AS avg_raise5, AVG(lower_6sec_price) AS avg_lower6,
               AVG(lower_60sec_price) AS avg_lower60, AVG(lower_5min_price) AS avg_lower5,
               COUNT(*) AS intervals
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
        GROUP BY region_id
    """))
    if rows:
        return {
            "summary": {"title": "Causer Pays > Dashboard", "regions": len(rows),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"region": r.get("region_id"),
                         "avg_raise_6sec": round(float(r.get("avg_raise6") or 0), 2),
                         "avg_raise_60sec": round(float(r.get("avg_raise60") or 0), 2),
                         "avg_raise_5min": round(float(r.get("avg_raise5") or 0), 2),
                         "avg_lower_6sec": round(float(r.get("avg_lower6") or 0), 2),
                         "avg_lower_60sec": round(float(r.get("avg_lower60") or 0), 2),
                         "avg_lower_5min": round(float(r.get("avg_lower5") or 0), 2),
                         "intervals": int(r.get("intervals") or 0)} for r in rows]}
    _r.seed(6048)
    return {
        "summary": {"title": "Causer Pays > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/cbam-trade-exposure/dashboard")
def cbam_trade_exposure_dashboard():
    _r.seed(6049)
    return {
        "summary": {"title": "Cbam Trade Exposure > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/cbam-trade/dashboard")
def cbam_trade_dashboard():
    _r.seed(6050)
    return {
        "summary": {"title": "Cbam Trade > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/cer-orchestration/dashboard")
def cer_orchestration_dashboard():
    _r.seed(6051)
    return {
        "summary": {"title": "Cer Orchestration > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/cer/dashboard")
def cer_dashboard():
    result = _build_lgc_response("CER > Dashboard", 6052)
    if result:
        # Enrich with certificate balances
        bal = _get_cached('cer_bal', lambda: _query_gold(f"""
            SELECT scheme, certificate_type, SUM(balance) AS total_balance
            FROM {_CATALOG}.gold.certificate_balances
            GROUP BY scheme, certificate_type
        """))
        if bal:
            result["certificate_balances"] = [{"scheme": r.get("scheme"),
                "certificate_type": r.get("certificate_type"),
                "total_balance": float(r.get("total_balance") or 0)} for r in bal]
        return result
    _r.seed(6052)
    return {
        "summary": {"title": "Cer > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/cer/lret")
def cer_lret():
    rows = _get_cached('cer_lret', lambda: _query_gold(f"""
        SELECT fuel_type, state, SUM(lgc_created_mwh) AS total_lgc, COUNT(*) AS generators
        FROM {_CATALOG}.gold.lgc_registry
        GROUP BY fuel_type, state ORDER BY total_lgc DESC
    """))
    if rows:
        total = sum(float(r.get("total_lgc") or 0) for r in rows)
        return [{"id": i+1, "fuel_type": r.get("fuel_type"), "state": r.get("state"),
                 "total_lgc_mwh": round(float(r.get("total_lgc") or 0), 0),
                 "generators": int(r.get("generators") or 0),
                 "share_pct": round(float(r.get("total_lgc") or 0) / max(total, 1) * 100, 1)
                } for i, r in enumerate(rows)]
    _r.seed(6053)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Cer > Lret", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/clean-energy-finance-x/dashboard")
def clean_energy_finance_x_dashboard():
    _r.seed(6054)
    return {
        "summary": {"title": "Clean Energy Finance X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/clean-energy-finance/dashboard")
def clean_energy_finance_dashboard():
    _r.seed(6055)
    return {
        "summary": {"title": "Clean Energy Finance > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/clean-hydrogen-production-cost/dashboard")
def clean_hydrogen_production_cost_dashboard():
    result = _build_generation_response("Clean Hydrogen Production Cost > Dashboard", 6056)
    if result:
        return result
    _r.seed(6056)
    return {
        "summary": {"title": "Clean Hydrogen Production Cost > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/climate-risk/assets")
def climate_risk_assets():
    result = _build_carbon_response("Climate Risk > Assets", 6057)
    if result and "records" in result:
        return result["records"]
    _r.seed(6057)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Climate Risk > Assets", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/climate-risk/dashboard")
def climate_risk_dashboard():
    result = _build_carbon_response("Climate Risk > Dashboard", 6058)
    if result:
        return result
    _r.seed(6058)
    return {
        "summary": {"title": "Climate Risk > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/climate-risk/events")
def climate_risk_events():
    result = _build_carbon_response("Climate Risk > Events", 6059)
    if result and "records" in result:
        return result["records"]
    _r.seed(6059)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Climate Risk > Events", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/climate-risk/physical-dashboard")
def climate_risk_physical_dashboard():
    result = _build_carbon_response("Climate Risk > Physical Dashboard", 6060)
    if result and "records" in result:
        return result["records"]
    _r.seed(6060)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Climate Risk > Physical Dashboard", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/coal-mine-energy/dashboard")
def coal_mine_energy_dashboard():
    result = _build_generation_response("Coal Mine Energy > Dashboard", 6061)
    if result:
        return result
    _r.seed(6061)
    return {
        "summary": {"title": "Coal Mine Energy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/coal-retirement/capacity-gaps")
def coal_retirement_capacity_gaps():
    result = _build_generation_response("Coal Retirement > Capacity Gaps", 6062)
    if result and "records" in result:
        return result["records"]
    _r.seed(6062)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Coal Retirement > Capacity Gaps", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/coal-retirement/investments")
def coal_retirement_investments():
    result = _build_generation_response("Coal Retirement > Investments", 6063)
    if result and "records" in result:
        return result["records"]
    _r.seed(6063)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Coal Retirement > Investments", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/coal-retirement/units")
def coal_retirement_units():
    result = _build_generation_response("Coal Retirement > Units", 6064)
    if result and "records" in result:
        return result["records"]
    _r.seed(6064)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Coal Retirement > Units", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/coal-seam-gas/dashboard")
def coal_seam_gas_dashboard():
    result = _build_generation_response("Coal Seam Gas > Dashboard", 6065)
    if result:
        return result
    _r.seed(6065)
    return {
        "summary": {"title": "Coal Seam Gas > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/community-energy-microgrid/dashboard")
def community_energy_microgrid_dashboard():
    result = _build_grid_response("Community Energy Microgrid > Dashboard", 6066)
    if result:
        return result
    _r.seed(6066)
    return {
        "summary": {"title": "Community Energy Microgrid > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/community-energy-storage/dashboard")
def community_energy_storage_dashboard():
    result = _build_battery_response("Community Energy Storage > Dashboard", 6067)
    if result:
        return result
    _r.seed(6067)
    return {
        "summary": {"title": "Community Energy Storage > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/community-energy/batteries")
def community_energy_batteries():
    _r.seed(6068)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Community Energy > Batteries", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/community-energy/solar-gardens")
def community_energy_solar_gardens():
    result = _build_renewable_response("Community Energy > Solar Gardens", 6069)
    if result and "records" in result:
        return result["records"]
    _r.seed(6069)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Community Energy > Solar Gardens", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/community-energy/sps")
def community_energy_sps():
    _r.seed(6070)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Community Energy > Sps", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/congestion/dashboard")
def congestion_dashboard():
    result = _build_grid_response("Congestion > Dashboard", 6071)
    if result:
        return result
    _r.seed(6071)
    return {
        "summary": {"title": "Congestion > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/constraints/dashboard")
def constraints_dashboard():
    result = _build_constraints_response("Constraints > Dashboard", 6072)
    if result:
        return result
    _r.seed(6072)
    return {
        "summary": {"title": "Constraints > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/consumer-energy-affordability/dashboard")
def consumer_energy_affordability_dashboard():
    _r.seed(6073)
    return {
        "summary": {"title": "Consumer Energy Affordability > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/consumer-hardship/dashboard")
def consumer_hardship_dashboard():
    _r.seed(6074)
    return {
        "summary": {"title": "Consumer Hardship > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/consumer-protection/complaints")
def consumer_protection_complaints():
    rows = _consumer_data()
    if rows:
        complaints = [r for r in rows if r.get("metric_type") == "COMPLAINTS"]
        if complaints:
            return [{"id": i+1, "retailer": r.get("retailer"), "complaints": int(r.get("metric_value") or 0),
                     "year": r.get("year"), "quarter": r.get("quarter")} for i, r in enumerate(complaints)]
    _r.seed(6075)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Consumer Protection > Complaints", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/consumer-protection/dashboard")
def consumer_protection_dashboard():
    rows = _consumer_data()
    if rows:
        by_type = {}
        for r in rows:
            t = r.get("metric_type", "OTHER")
            by_type.setdefault(t, [])
            by_type[t].append(r)
        return {
            "summary": {"title": "Consumer Protection > Dashboard", "metric_types": len(by_type),
                        "total_records": len(rows), "last_updated": _dt.utcnow().isoformat()},
            "records": [{"metric_type": r.get("metric_type"), "retailer": r.get("retailer"),
                         "state": r.get("state"), "value": float(r.get("metric_value") or 0),
                         "unit": r.get("metric_unit")} for r in rows],
            "by_type": [{"type": k, "count": len(v)} for k, v in by_type.items()]}
    _r.seed(6076)
    return {
        "summary": {"title": "Consumer Protection > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/consumer-protection/offers")
def consumer_protection_offers():
    rows = _consumer_data()
    if rows:
        offers = [r for r in rows if r.get("metric_type") == "AVG_OFFER_RATE"]
        if offers:
            return [{"id": i+1, "state": r.get("state"),
                     "avg_offer_aud_kwh": float(r.get("metric_value") or 0)} for i, r in enumerate(offers)]
    _r.seed(6077)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Consumer Protection > Offers", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/consumer-protection/switching")
def consumer_protection_switching():
    rows = _consumer_data()
    if rows:
        switching = [r for r in rows if r.get("metric_type") == "SWITCHING_RATE"]
        if switching:
            return [{"id": i+1, "state": r.get("state"),
                     "switching_rate_pct": float(r.get("metric_value") or 0)} for i, r in enumerate(switching)]
    _r.seed(6078)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Consumer Protection > Switching", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/consumer-segmentation/dashboard")
def consumer_segmentation_dashboard():
    _r.seed(6079)
    return {
        "summary": {"title": "Consumer Segmentation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/consumer-switching-retail-churn/dashboard")
def consumer_switching_retail_churn_dashboard():
    result = _build_pricing_response("Consumer Switching Retail Churn > Dashboard", 6080)
    if result:
        return result
    _r.seed(6080)
    return {
        "summary": {"title": "Consumer Switching Retail Churn > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/corporate-ppa-market/dashboard")
def corporate_ppa_market_dashboard():
    result = _build_pricing_response("Corporate Ppa Market > Dashboard", 6081)
    if result:
        return result
    _r.seed(6081)
    return {
        "summary": {"title": "Corporate Ppa Market > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/corporate-ppa-market/deals")
def corporate_ppa_market_deals():
    result = _build_pricing_response("Corporate Ppa Market > Deals", 6082)
    if result and "records" in result:
        return result["records"]
    _r.seed(6082)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Corporate Ppa Market > Deals", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/corporate-ppa-market/market-summary")
def corporate_ppa_market_market_summary():
    result = _build_pricing_response("Corporate Ppa Market > Market Summary", 6083)
    if result and "records" in result:
        return result["records"]
    _r.seed(6083)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Corporate Ppa Market > Market Summary", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/corporate-ppa-market/offtakers")
def corporate_ppa_market_offtakers():
    result = _build_pricing_response("Corporate Ppa Market > Offtakers", 6084)
    if result and "records" in result:
        return result["records"]
    _r.seed(6084)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Corporate Ppa Market > Offtakers", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/corporate-ppa-market/price-trends")
def corporate_ppa_market_price_trends():
    result = _build_pricing_response("Corporate Ppa Market > Price Trends", 6085)
    if result and "records" in result:
        return result["records"]
    _r.seed(6085)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Corporate Ppa Market > Price Trends", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/corporate-ppa-x/dashboard")
def corporate_ppa_x_dashboard():
    result = _build_pricing_response("Corporate Ppa X > Dashboard", 6086)
    if result:
        return result
    _r.seed(6086)
    return {
        "summary": {"title": "Corporate Ppa X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/cost-reflective-tariff-reform/dashboard")
def cost_reflective_tariff_reform_dashboard():
    result = _build_pricing_response("Cost Reflective Tariff Reform > Dashboard", 6087)
    if result:
        return result
    _r.seed(6087)
    return {
        "summary": {"title": "Cost Reflective Tariff Reform > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/csp-analytics/dashboard")
def csp_analytics_dashboard():
    _r.seed(6088)
    return {
        "summary": {"title": "Csp Analytics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/decarbonization/dashboard")
def decarbonization_dashboard():
    result = _build_carbon_response("Decarbonization > Dashboard", 6089)
    if result:
        return result
    _r.seed(6089)
    return {
        "summary": {"title": "Decarbonization > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/decarbonization/milestones")
def decarbonization_milestones():
    result = _build_carbon_response("Decarbonization > Milestones", 6090)
    if result and "records" in result:
        return result["records"]
    _r.seed(6090)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Decarbonization > Milestones", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/decarbonization/sectors")
def decarbonization_sectors():
    result = _build_carbon_response("Decarbonization > Sectors", 6091)
    if result and "records" in result:
        return result["records"]
    _r.seed(6091)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Decarbonization > Sectors", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/decarbonization/technology")
def decarbonization_technology():
    result = _build_carbon_response("Decarbonization > Technology", 6092)
    if result and "records" in result:
        return result["records"]
    _r.seed(6092)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Decarbonization > Technology", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/demand-flexibility-market/dashboard")
def demand_flexibility_market_dashboard():
    result = _build_grid_response("Demand Flexibility Market > Dashboard", 6093)
    if result:
        return result
    _r.seed(6093)
    return {
        "summary": {"title": "Demand Flexibility Market > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/demand-flexibility/dashboard")
def demand_flexibility_dashboard():
    result = _build_grid_response("Demand Flexibility > Dashboard", 6094)
    if result:
        return result
    _r.seed(6094)
    return {
        "summary": {"title": "Demand Flexibility > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/demand-forecast-models/dashboard")
def demand_forecast_models_dashboard():
    result = _build_grid_response("Demand Forecast Models > Dashboard", 6095)
    if result:
        return result
    _r.seed(6095)
    return {
        "summary": {"title": "Demand Forecast Models > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/demand-response-aggregator/dashboard")
def demand_response_aggregator_dashboard():
    result = _build_grid_response("Demand Response Aggregator > Dashboard", 6096)
    if result:
        return result
    _r.seed(6096)
    return {
        "summary": {"title": "Demand Response Aggregator > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/demand-response-programs/dashboard")
def demand_response_programs_dashboard():
    result = _build_grid_response("Demand Response Programs > Dashboard", 6097)
    if result:
        return result
    _r.seed(6097)
    return {
        "summary": {"title": "Demand Response Programs > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/demand-response/activations")
def demand_response_activations():
    result = _build_grid_response("Demand Response > Activations", 6098)
    if result and "records" in result:
        return result["records"]
    _r.seed(6098)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Demand Response > Activations", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/demand-response/contracts")
def demand_response_contracts():
    result = _build_grid_response("Demand Response > Contracts", 6099)
    if result and "records" in result:
        return result["records"]
    _r.seed(6099)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Demand Response > Contracts", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/demand-response/providers")
def demand_response_providers():
    result = _build_grid_response("Demand Response > Providers", 6100)
    if result and "records" in result:
        return result["records"]
    _r.seed(6100)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Demand Response > Providers", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/demand-side-management-program/dashboard")
def demand_side_management_program_dashboard():
    result = _build_grid_response("Demand Side Management Program > Dashboard", 6101)
    if result:
        return result
    _r.seed(6101)
    return {
        "summary": {"title": "Demand Side Management Program > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/derms-orchestration/aggregators")
def derms_orchestration_aggregators():
    _r.seed(6102)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Derms Orchestration > Aggregators", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/derms-orchestration/dashboard")
def derms_orchestration_dashboard():
    _r.seed(6103)
    return {
        "summary": {"title": "Derms Orchestration > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/derms-orchestration/der-portfolio")
def derms_orchestration_der_portfolio():
    _r.seed(6104)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Derms Orchestration > Der Portfolio", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/derms-orchestration/dispatch-events")
def derms_orchestration_dispatch_events():
    _r.seed(6105)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Derms Orchestration > Dispatch Events", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/derms-orchestration/kpis")
def derms_orchestration_kpis():
    _r.seed(6106)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Derms Orchestration > Kpis", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/digital-energy-twin/dashboard")
def digital_energy_twin_dashboard():
    _r.seed(6107)
    return {
        "summary": {"title": "Digital Energy Twin > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/digital-transformation/dashboard")
def digital_transformation_dashboard():
    _r.seed(6108)
    return {
        "summary": {"title": "Digital Transformation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/distributed-asset-optimisation/dashboard")
def distributed_asset_optimisation_dashboard():
    _r.seed(6109)
    return {
        "summary": {"title": "Distributed Asset Optimisation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/distributed-energy-resource-management-x/dashboard")
def distributed_energy_resource_management_x_dashboard():
    _r.seed(6110)
    return {
        "summary": {"title": "Distributed Energy Resource Management X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/distributed-energy-resource-management/dashboard")
def distributed_energy_resource_management_dashboard():
    _r.seed(6111)
    return {
        "summary": {"title": "Distributed Energy Resource Management > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/distribution-network-planning/dashboard")
def distribution_network_planning_dashboard():
    result = _build_grid_response("Distribution Network Planning > Dashboard", 6112)
    if result:
        return result
    _r.seed(6112)
    return {
        "summary": {"title": "Distribution Network Planning > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/dnsp-analytics/dashboard")
def dnsp_analytics_dashboard():
    _r.seed(6113)
    return {
        "summary": {"title": "Dnsp Analytics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/dnsp/dashboard")
def dnsp_dashboard():
    _r.seed(6114)
    return {
        "summary": {"title": "Dnsp > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/dsr-aggregator/dashboard")
def dsr_aggregator_dashboard():
    _r.seed(6115)
    return {
        "summary": {"title": "Dsr Aggregator > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/efor/availability")
def efor_availability():
    rows = _get_cached('efor_avail', lambda: _query_gold(f"""
        SELECT region_id, fuel_type, SUM(total_mw) AS total_mw, SUM(unit_count) AS units,
               AVG(capacity_factor) AS avg_cf
        FROM {_CATALOG}.gold.nem_generation_by_fuel
        WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
        GROUP BY region_id, fuel_type ORDER BY total_mw DESC
    """))
    if rows:
        return [{"id": i+1, "region": r.get("region_id"), "fuel_type": r.get("fuel_type"),
                 "availability_mw": round(float(r.get("total_mw") or 0), 1),
                 "units": int(r.get("units") or 0),
                 "capacity_factor": round(float(r.get("avg_cf") or 0), 3)} for i, r in enumerate(rows[:20])]
    _r.seed(6116)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Efor > Availability", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/efor/dashboard")
def efor_dashboard():
    rows = _get_cached('efor_kpis', lambda: _query_gold(f"""
        SELECT business_name, region, metric_name, metric_value, year, period
        FROM {_CATALOG}.gold.reliability_kpis
        ORDER BY year DESC, business_name
    """))
    if rows:
        businesses = set(r.get("business_name") for r in rows if r.get("business_name"))
        return {
            "summary": {"title": "EFOR > Dashboard", "businesses": len(businesses),
                        "total_records": len(rows), "last_updated": _dt.utcnow().isoformat()},
            "records": [{"business": r.get("business_name"), "region": r.get("region"),
                         "metric": r.get("metric_name"), "value": r.get("metric_value"),
                         "year": r.get("year"), "period": r.get("period")} for r in rows[:30]]}
    _r.seed(6117)
    return {
        "summary": {"title": "Efor > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/efor/trends")
def efor_trends():
    rows = _get_cached('efor_trends', lambda: _query_gold(f"""
        SELECT business_name, metric_name, year, metric_value
        FROM {_CATALOG}.gold.reliability_kpis
        WHERE metric_name IN ('SAIDI', 'SAIFI', 'CAIDI')
        ORDER BY year, business_name
    """))
    if rows:
        return [{"id": i+1, "business": r.get("business_name"), "metric": r.get("metric_name"),
                 "year": r.get("year"), "value": r.get("metric_value")} for i, r in enumerate(rows)]
    _r.seed(6118)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Efor > Trends", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/electric-vehicle-grid-integration/dashboard")
def electric_vehicle_grid_integration_dashboard():
    result = _build_grid_response("Electric Vehicle Grid Integration > Dashboard", 6119)
    if result:
        return result
    _r.seed(6119)
    return {
        "summary": {"title": "Electric Vehicle Grid Integration > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-consumer-behaviour/dashboard")
def electricity_consumer_behaviour_dashboard():
    _r.seed(6120)
    return {
        "summary": {"title": "Electricity Consumer Behaviour > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-consumer-switching-churn/dashboard")
def electricity_consumer_switching_churn_dashboard():
    _r.seed(6121)
    return {
        "summary": {"title": "Electricity Consumer Switching Churn > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-demand-elasticity/dashboard")
def electricity_demand_elasticity_dashboard():
    result = _build_grid_response("Electricity Demand Elasticity > Dashboard", 6122)
    if result:
        return result
    _r.seed(6122)
    return {
        "summary": {"title": "Electricity Demand Elasticity > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-export-cable/dashboard")
def electricity_export_cable_dashboard():
    _r.seed(6123)
    return {
        "summary": {"title": "Electricity Export Cable > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-export-economics/dashboard")
def electricity_export_economics_dashboard():
    _r.seed(6124)
    return {
        "summary": {"title": "Electricity Export Economics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-export/dashboard")
def electricity_export_dashboard():
    _r.seed(6125)
    return {
        "summary": {"title": "Electricity Export > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-frequency-performance/dashboard")
def electricity_frequency_performance_dashboard():
    result = _build_grid_response("Electricity Frequency Performance > Dashboard", 6126)
    if result:
        return result
    _r.seed(6126)
    return {
        "summary": {"title": "Electricity Frequency Performance > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-market-competition-concentration/dashboard")
def electricity_market_competition_concentration_dashboard():
    result = _build_pricing_response("Electricity Market Competition Concentration > Dashboard", 6127)
    if result:
        return result
    _r.seed(6127)
    return {
        "summary": {"title": "Electricity Market Competition Concentration > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-market-design-reform/dashboard")
def electricity_market_design_reform_dashboard():
    result = _build_pricing_response("Electricity Market Design Reform > Dashboard", 6128)
    if result:
        return result
    _r.seed(6128)
    return {
        "summary": {"title": "Electricity Market Design Reform > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-market-liquidity/dashboard")
def electricity_market_liquidity_dashboard():
    result = _build_pricing_response("Electricity Market Liquidity > Dashboard", 6129)
    if result:
        return result
    _r.seed(6129)
    return {
        "summary": {"title": "Electricity Market Liquidity > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-market-microstructure/dashboard")
def electricity_market_microstructure_dashboard():
    _r.seed(6130)
    return {
        "summary": {"title": "Electricity Market Microstructure > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-market-regulatory-appeals/dashboard")
def electricity_market_regulatory_appeals_dashboard():
    _r.seed(6131)
    return {
        "summary": {"title": "Electricity Market Regulatory Appeals > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-network-capital-investment/dashboard")
def electricity_network_capital_investment_dashboard():
    result = _build_grid_response("Electricity Network Capital Investment > Dashboard", 6132)
    if result:
        return result
    _r.seed(6132)
    return {
        "summary": {"title": "Electricity Network Capital Investment > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-network-investment-deferral/dashboard")
def electricity_network_investment_deferral_dashboard():
    result = _build_grid_response("Electricity Network Investment Deferral > Dashboard", 6133)
    if result:
        return result
    _r.seed(6133)
    return {
        "summary": {"title": "Electricity Network Investment Deferral > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-network-tariff-reform/dashboard")
def electricity_network_tariff_reform_dashboard():
    result = _build_pricing_response("Electricity Network Tariff Reform > Dashboard", 6134)
    if result:
        return result
    _r.seed(6134)
    return {
        "summary": {"title": "Electricity Network Tariff Reform > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-retailer-churn/dashboard")
def electricity_retailer_churn_dashboard():
    result = _build_pricing_response("Electricity Retailer Churn > Dashboard", 6135)
    if result:
        return result
    _r.seed(6135)
    return {
        "summary": {"title": "Electricity Retailer Churn > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electricity-workforce/dashboard")
def electricity_workforce_dashboard():
    _r.seed(6136)
    return {
        "summary": {"title": "Electricity Workforce > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/electrification/dashboard")
def electrification_dashboard():
    _r.seed(6137)
    return {
        "summary": {"title": "Electrification > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/emerging-markets/dashboard")
def emerging_markets_dashboard():
    _r.seed(6138)
    return {
        "summary": {"title": "Emerging Markets > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-affordability/dashboard")
def energy_affordability_dashboard():
    _r.seed(6139)
    return {
        "summary": {"title": "Energy Affordability > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-asset-life-extension/dashboard")
def energy_asset_life_extension_dashboard():
    _r.seed(6140)
    return {
        "summary": {"title": "Energy Asset Life Extension > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-asset-maintenance/dashboard")
def energy_asset_maintenance_dashboard():
    _r.seed(6141)
    return {
        "summary": {"title": "Energy Asset Maintenance > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-commodity-trading/dashboard")
def energy_commodity_trading_dashboard():
    _r.seed(6142)
    return {
        "summary": {"title": "Energy Commodity Trading > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-community-microgrid/dashboard")
def energy_community_microgrid_dashboard():
    result = _build_grid_response("Energy Community Microgrid > Dashboard", 6143)
    if result:
        return result
    _r.seed(6143)
    return {
        "summary": {"title": "Energy Community Microgrid > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-hub-microstructure/dashboard")
def energy_hub_microstructure_dashboard():
    _r.seed(6144)
    return {
        "summary": {"title": "Energy Hub Microstructure > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-infrastructure-cyber-threat/dashboard")
def energy_infrastructure_cyber_threat_dashboard():
    _r.seed(6145)
    return {
        "summary": {"title": "Energy Infrastructure Cyber Threat > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-optimisation/dashboard")
def energy_optimisation_dashboard():
    _r.seed(6146)
    return {
        "summary": {"title": "Energy Optimisation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-poverty-hardship-x/dashboard")
def energy_poverty_hardship_x_dashboard():
    _r.seed(6147)
    return {
        "summary": {"title": "Energy Poverty Hardship X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-poverty-hardship/dashboard")
def energy_poverty_hardship_dashboard():
    _r.seed(6148)
    return {
        "summary": {"title": "Energy Poverty Hardship > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-poverty-vulnerable-consumer/dashboard")
def energy_poverty_vulnerable_consumer_dashboard():
    _r.seed(6149)
    return {
        "summary": {"title": "Energy Poverty Vulnerable Consumer > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-poverty/dashboard")
def energy_poverty_dashboard():
    _r.seed(6150)
    return {
        "summary": {"title": "Energy Poverty > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-retail-competition/dashboard")
def energy_retail_competition_dashboard():
    result = _build_pricing_response("Energy Retail Competition > Dashboard", 6151)
    if result:
        return result
    _r.seed(6151)
    return {
        "summary": {"title": "Energy Retail Competition > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-retailer-margin/dashboard")
def energy_retailer_margin_dashboard():
    result = _build_pricing_response("Energy Retailer Margin > Dashboard", 6152)
    if result:
        return result
    _r.seed(6152)
    return {
        "summary": {"title": "Energy Retailer Margin > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-storage-arbitrage/dashboard")
def energy_storage_arbitrage_dashboard():
    result = _build_battery_response("Energy Storage Arbitrage > Dashboard", 6153)
    if result:
        return result
    _r.seed(6153)
    return {
        "summary": {"title": "Energy Storage Arbitrage > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-storage-dispatch-optimisation/dashboard")
def energy_storage_dispatch_optimisation_dashboard():
    result = _build_battery_response("Energy Storage Dispatch Optimisation > Dashboard", 6154)
    if result:
        return result
    _r.seed(6154)
    return {
        "summary": {"title": "Energy Storage Dispatch Optimisation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-storage-duration-x/dashboard")
def energy_storage_duration_x_dashboard():
    result = _build_battery_response("Energy Storage Duration X > Dashboard", 6155)
    if result:
        return result
    _r.seed(6155)
    return {
        "summary": {"title": "Energy Storage Duration X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-storage-duration/dashboard")
def energy_storage_duration_dashboard():
    result = _build_battery_response("Energy Storage Duration > Dashboard", 6156)
    if result:
        return result
    _r.seed(6156)
    return {
        "summary": {"title": "Energy Storage Duration > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-storage-merchant-revenue/dashboard")
def energy_storage_merchant_revenue_dashboard():
    result = _build_battery_response("Energy Storage Merchant Revenue > Dashboard", 6157)
    if result:
        return result
    _r.seed(6157)
    return {
        "summary": {"title": "Energy Storage Merchant Revenue > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-storage-technology-comparison/dashboard")
def energy_storage_technology_comparison_dashboard():
    result = _build_battery_response("Energy Storage Technology Comparison > Dashboard", 6158)
    if result:
        return result
    _r.seed(6158)
    return {
        "summary": {"title": "Energy Storage Technology Comparison > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-trading-algorithmic-strategy/dashboard")
def energy_trading_algorithmic_strategy_dashboard():
    _r.seed(6159)
    return {
        "summary": {"title": "Energy Trading Algorithmic Strategy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-transition-finance-x/dashboard")
def energy_transition_finance_x_dashboard():
    _r.seed(6160)
    return {
        "summary": {"title": "Energy Transition Finance X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-transition-finance/dashboard")
def energy_transition_finance_dashboard():
    _r.seed(6161)
    return {
        "summary": {"title": "Energy Transition Finance > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/energy-transition-jobs/dashboard")
def energy_transition_jobs_dashboard():
    _r.seed(6162)
    return {
        "summary": {"title": "Energy Transition Jobs > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/epv/dashboard")
def epv_dashboard():
    _r.seed(6163)
    return {
        "summary": {"title": "Epv > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/equity/dashboard")
def equity_dashboard():
    _r.seed(6164)
    return {
        "summary": {"title": "Equity > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/esoo-adequacy/dashboard")
def esoo_adequacy_dashboard():
    rows = _get_cached('esoo', lambda: _query_gold(f"""
        SELECT d.region_id,
               MAX(d.predicted_demand_mw) AS peak_demand_mw,
               g.total_capacity_mw,
               (g.total_capacity_mw - MAX(d.predicted_demand_mw)) AS reserve_margin_mw
        FROM {_CATALOG}.gold.demand_forecasts d
        LEFT JOIN (
            SELECT region_id, SUM(capacity_mw) AS total_capacity_mw
            FROM {_CATALOG}.gold.nem_facilities
            GROUP BY region_id
        ) g ON d.region_id = g.region_id
        WHERE d.interval_datetime >= current_timestamp()
          AND d.interval_datetime < current_timestamp() + INTERVAL 7 DAYS
        GROUP BY d.region_id, g.total_capacity_mw
    """))
    if rows:
        return {
            "summary": {"title": "ESOO Adequacy > Dashboard", "regions": len(rows),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"region": r.get("region_id"),
                         "peak_demand_mw": round(float(r.get("peak_demand_mw") or 0), 0),
                         "total_capacity_mw": round(float(r.get("total_capacity_mw") or 0), 0),
                         "reserve_margin_mw": round(float(r.get("reserve_margin_mw") or 0), 0),
                         "reserve_margin_pct": round(float(r.get("reserve_margin_mw") or 0) / max(float(r.get("peak_demand_mw") or 1), 1) * 100, 1)
                        } for r in rows]}
    _r.seed(6165)
    return {
        "summary": {"title": "Esoo Adequacy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ev-battery-technology/dashboard")
def ev_battery_technology_dashboard():
    result = _build_battery_response("Ev Battery Technology > Dashboard", 6166)
    if result:
        return result
    _r.seed(6166)
    return {
        "summary": {"title": "Ev Battery Technology > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ev-fleet-charging/dashboard")
def ev_fleet_charging_dashboard():
    _r.seed(6167)
    return {
        "summary": {"title": "Ev Fleet Charging > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ev-fleet-depot/dashboard")
def ev_fleet_depot_dashboard():
    _r.seed(6168)
    return {
        "summary": {"title": "Ev Fleet Depot > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ev-fleet-grid-impact/dashboard")
def ev_fleet_grid_impact_dashboard():
    result = _build_grid_response("Ev Fleet Grid Impact > Dashboard", 6169)
    if result:
        return result
    _r.seed(6169)
    return {
        "summary": {"title": "Ev Fleet Grid Impact > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ev-fleet/dashboard")
def ev_fleet_dashboard():
    _r.seed(6170)
    return {
        "summary": {"title": "Ev Fleet > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ev-grid-integration-v2g/dashboard")
def ev_grid_integration_v2g_dashboard():
    result = _build_grid_response("Ev Grid Integration V2G > Dashboard", 6171)
    if result:
        return result
    _r.seed(6171)
    return {
        "summary": {"title": "Ev Grid Integration V2G > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/extreme-weather-resilience/dashboard")
def extreme_weather_resilience_dashboard():
    _r.seed(6172)
    return {
        "summary": {"title": "Extreme Weather Resilience > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/fcas-procurement/dashboard")
def fcas_procurement_dashboard():
    result = _build_grid_response("Fcas Procurement > Dashboard", 6173)
    if result:
        return result
    _r.seed(6173)
    return {
        "summary": {"title": "Fcas Procurement > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/fcas/services")
def fcas_services():
    result = _build_grid_response("Fcas > Services", 6174)
    if result and "records" in result:
        return result["records"]
    _r.seed(6174)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Fcas > Services", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/firming-technology/dashboard")
def firming_technology_dashboard():
    _r.seed(6175)
    return {
        "summary": {"title": "Firming Technology > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/forward-curve/options")
def forward_curve_options():
    _r.seed(6176)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Forward Curve > Options", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/forward-curve/prices")
def forward_curve_prices():
    result = _build_pricing_response("Forward Curve > Prices", 6177)
    if result and "records" in result:
        return result["records"]
    _r.seed(6177)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Forward Curve > Prices", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/forward-curve/seasonal")
def forward_curve_seasonal():
    _r.seed(6178)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Forward Curve > Seasonal", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/frequency-control-performance/dashboard")
def frequency_control_performance_dashboard():
    result = _build_grid_response("Frequency Control Performance > Dashboard", 6179)
    if result:
        return result
    _r.seed(6179)
    return {
        "summary": {"title": "Frequency Control Performance > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/frequency-control/dashboard")
def frequency_control_dashboard():
    result = _build_grid_response("Frequency Control > Dashboard", 6180)
    if result:
        return result
    _r.seed(6180)
    return {
        "summary": {"title": "Frequency Control > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/frequency-reserve-planning/dashboard")
def frequency_reserve_planning_dashboard():
    result = _build_grid_response("Frequency Reserve Planning > Dashboard", 6181)
    if result:
        return result
    _r.seed(6181)
    return {
        "summary": {"title": "Frequency Reserve Planning > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/futures-market-risk/basis-risk")
def futures_market_risk_basis_risk():
    _r.seed(6182)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Futures Market Risk > Basis Risk", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/futures-market-risk/hedge-effectiveness")
def futures_market_risk_hedge_effectiveness():
    _r.seed(6183)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Futures Market Risk > Hedge Effectiveness", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/futures-market-risk/positions")
def futures_market_risk_positions():
    _r.seed(6184)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Futures Market Risk > Positions", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/futures-market-risk/var")
def futures_market_risk_var():
    _r.seed(6185)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Futures Market Risk > Var", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/gas-electricity-nexus/dashboard")
def gas_electricity_nexus_dashboard():
    result = _build_generation_response("Gas Electricity Nexus > Dashboard", 6186)
    if result:
        return result
    _r.seed(6186)
    return {
        "summary": {"title": "Gas Electricity Nexus > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/gas-gen/dashboard")
def gas_gen_dashboard():
    result = _build_generation_response("Gas Gen > Dashboard", 6187)
    if result:
        return result
    _r.seed(6187)
    return {
        "summary": {"title": "Gas Gen > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/gas-gen/generators")
def gas_gen_generators():
    result = _build_generation_response("Gas Gen > Generators", 6188)
    if result and "records" in result:
        return result["records"]
    _r.seed(6188)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Gas Gen > Generators", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/gas-gen/spark-spreads")
def gas_gen_spark_spreads():
    result = _build_generation_response("Gas Gen > Spark Spreads", 6189)
    if result and "records" in result:
        return result["records"]
    _r.seed(6189)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Gas Gen > Spark Spreads", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/gas-network-pipeline/dashboard")
def gas_network_pipeline_dashboard():
    result = _build_generation_response("Gas Network Pipeline > Dashboard", 6190)
    if result:
        return result
    _r.seed(6190)
    return {
        "summary": {"title": "Gas Network Pipeline > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/gas-power-plant-flexibility/dashboard")
def gas_power_plant_flexibility_dashboard():
    result = _build_generation_response("Gas Power Plant Flexibility > Dashboard", 6191)
    if result:
        return result
    _r.seed(6191)
    return {
        "summary": {"title": "Gas Power Plant Flexibility > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/gas-to-power-transition/dashboard")
def gas_to_power_transition_dashboard():
    _r.seed(6192)
    return {
        "summary": {"title": "Gas To Power Transition > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/gas-transition/dashboard")
def gas_transition_dashboard():
    _r.seed(6193)
    return {
        "summary": {"title": "Gas Transition > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/generation-expansion/dashboard")
def generation_expansion_dashboard():
    _r.seed(6194)
    return {
        "summary": {"title": "Generation Expansion > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/generation-mix-transition/dashboard")
def generation_mix_transition_dashboard():
    _r.seed(6195)
    return {
        "summary": {"title": "Generation Mix Transition > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/generator-capacity-adequacy/dashboard")
def generator_capacity_adequacy_dashboard():
    _r.seed(6196)
    return {
        "summary": {"title": "Generator Capacity Adequacy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/generator-performance-standards/dashboard")
def generator_performance_standards_dashboard():
    _r.seed(6197)
    return {
        "summary": {"title": "Generator Performance Standards > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/generator-retirement/dashboard")
def generator_retirement_dashboard():
    _r.seed(6198)
    return {
        "summary": {"title": "Generator Retirement > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/geothermal-energy-development/dashboard")
def geothermal_energy_development_dashboard():
    result = _build_generation_response("Geothermal Energy Development > Dashboard", 6199)
    if result:
        return result
    _r.seed(6199)
    return {
        "summary": {"title": "Geothermal Energy Development > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/geothermal-energy-potential/dashboard")
def geothermal_energy_potential_dashboard():
    result = _build_generation_response("Geothermal Energy Potential > Dashboard", 6200)
    if result:
        return result
    _r.seed(6200)
    return {
        "summary": {"title": "Geothermal Energy Potential > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/green-ammonia-export/dashboard")
def green_ammonia_export_dashboard():
    result = _build_carbon_response("Green Ammonia Export > Dashboard", 6201)
    if result:
        return result
    _r.seed(6201)
    return {
        "summary": {"title": "Green Ammonia Export > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/green-tariff-hydrogen/dashboard")
def green_tariff_hydrogen_dashboard():
    result = _build_carbon_response("Green Tariff Hydrogen > Dashboard", 6202)
    if result:
        return result
    _r.seed(6202)
    return {
        "summary": {"title": "Green Tariff Hydrogen > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-congestion-constraint/dashboard")
def grid_congestion_constraint_dashboard():
    result = _build_grid_response("Grid Congestion Constraint > Dashboard", 6203)
    if result:
        return result
    _r.seed(6203)
    return {
        "summary": {"title": "Grid Congestion Constraint > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-cybersecurity/dashboard")
def grid_cybersecurity_dashboard():
    result = _build_grid_response("Grid Cybersecurity > Dashboard", 6204)
    if result:
        return result
    _r.seed(6204)
    return {
        "summary": {"title": "Grid Cybersecurity > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-decarbonisation-pathway/dashboard")
def grid_decarbonisation_pathway_dashboard():
    result = _build_carbon_response("Grid Decarbonisation Pathway > Dashboard", 6205)
    if result:
        return result
    _r.seed(6205)
    return {
        "summary": {"title": "Grid Decarbonisation Pathway > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-edge-technology-x/dashboard")
def grid_edge_technology_x_dashboard():
    result = _build_grid_response("Grid Edge Technology X > Dashboard", 6206)
    if result:
        return result
    _r.seed(6206)
    return {
        "summary": {"title": "Grid Edge Technology X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-edge-technology/dashboard")
def grid_edge_technology_dashboard():
    result = _build_grid_response("Grid Edge Technology > Dashboard", 6207)
    if result:
        return result
    _r.seed(6207)
    return {
        "summary": {"title": "Grid Edge Technology > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-flexibility-services/dashboard")
def grid_flexibility_services_dashboard():
    result = _build_grid_response("Grid Flexibility Services > Dashboard", 6208)
    if result:
        return result
    _r.seed(6208)
    return {
        "summary": {"title": "Grid Flexibility Services > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-forming-inverter-x/dashboard")
def grid_forming_inverter_x_dashboard():
    result = _build_grid_response("Grid Forming Inverter X > Dashboard", 6209)
    if result:
        return result
    _r.seed(6209)
    return {
        "summary": {"title": "Grid Forming Inverter X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-forming-inverter/dashboard")
def grid_forming_inverter_dashboard():
    result = _build_grid_response("Grid Forming Inverter > Dashboard", 6210)
    if result:
        return result
    _r.seed(6210)
    return {
        "summary": {"title": "Grid Forming Inverter > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-modernisation-digital-twin/dashboard")
def grid_modernisation_digital_twin_dashboard():
    result = _build_grid_response("Grid Modernisation Digital Twin > Dashboard", 6211)
    if result:
        return result
    _r.seed(6211)
    return {
        "summary": {"title": "Grid Modernisation Digital Twin > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-modernisation/dashboard")
def grid_modernisation_dashboard():
    result = _build_grid_response("Grid Modernisation > Dashboard", 6212)
    if result:
        return result
    _r.seed(6212)
    return {
        "summary": {"title": "Grid Modernisation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-reliability/dashboard")
def grid_reliability_dashboard():
    result = _build_grid_response("Grid Reliability > Dashboard", 6213)
    if result:
        return result
    _r.seed(6213)
    return {
        "summary": {"title": "Grid Reliability > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-resilience/dashboard")
def grid_resilience_dashboard():
    result = _build_grid_response("Grid Resilience > Dashboard", 6214)
    if result:
        return result
    _r.seed(6214)
    return {
        "summary": {"title": "Grid Resilience > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/grid-scale-battery-degradation/dashboard")
def grid_scale_battery_degradation_dashboard():
    result = _build_battery_response("Grid Scale Battery Degradation > Dashboard", 6215)
    if result:
        return result
    _r.seed(6215)
    return {
        "summary": {"title": "Grid Scale Battery Degradation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydro/dashboard")
def hydro_dashboard():
    _r.seed(6216)
    return {
        "summary": {"title": "Hydro > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydrogen-economy-analytics/dashboard")
def hydrogen_economy_analytics_dashboard():
    result = _build_generation_response("Hydrogen Economy Analytics > Dashboard", 6217)
    if result:
        return result
    _r.seed(6217)
    return {
        "summary": {"title": "Hydrogen Economy Analytics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydrogen-economy/cost-benchmarks")
def hydrogen_economy_cost_benchmarks():
    result = _build_generation_response("Hydrogen Economy > Cost Benchmarks", 6218)
    if result and "records" in result:
        return result["records"]
    _r.seed(6218)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Hydrogen Economy > Cost Benchmarks", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/hydrogen-economy/dashboard")
def hydrogen_economy_dashboard():
    result = _build_generation_response("Hydrogen Economy > Dashboard", 6219)
    if result:
        return result
    _r.seed(6219)
    return {
        "summary": {"title": "Hydrogen Economy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydrogen-economy/export-terminals")
def hydrogen_economy_export_terminals():
    result = _build_generation_response("Hydrogen Economy > Export Terminals", 6220)
    if result and "records" in result:
        return result["records"]
    _r.seed(6220)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Hydrogen Economy > Export Terminals", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/hydrogen-economy/production")
def hydrogen_economy_production():
    result = _build_generation_response("Hydrogen Economy > Production", 6221)
    if result and "records" in result:
        return result["records"]
    _r.seed(6221)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Hydrogen Economy > Production", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/hydrogen-economy/refuelling")
def hydrogen_economy_refuelling():
    result = _build_generation_response("Hydrogen Economy > Refuelling", 6222)
    if result and "records" in result:
        return result["records"]
    _r.seed(6222)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Hydrogen Economy > Refuelling", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/hydrogen-electrolysis-cost/dashboard")
def hydrogen_electrolysis_cost_dashboard():
    result = _build_generation_response("Hydrogen Electrolysis Cost > Dashboard", 6223)
    if result:
        return result
    _r.seed(6223)
    return {
        "summary": {"title": "Hydrogen Electrolysis Cost > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydrogen-export-terminal/dashboard")
def hydrogen_export_terminal_dashboard():
    result = _build_generation_response("Hydrogen Export Terminal > Dashboard", 6224)
    if result:
        return result
    _r.seed(6224)
    return {
        "summary": {"title": "Hydrogen Export Terminal > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydrogen-fuel-cell-vehicles/dashboard")
def hydrogen_fuel_cell_vehicles_dashboard():
    result = _build_generation_response("Hydrogen Fuel Cell Vehicles > Dashboard", 6225)
    if result:
        return result
    _r.seed(6225)
    return {
        "summary": {"title": "Hydrogen Fuel Cell Vehicles > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydrogen-pipeline-infrastructure/dashboard")
def hydrogen_pipeline_infrastructure_dashboard():
    result = _build_generation_response("Hydrogen Pipeline Infrastructure > Dashboard", 6226)
    if result:
        return result
    _r.seed(6226)
    return {
        "summary": {"title": "Hydrogen Pipeline Infrastructure > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydrogen-refuelling-station/dashboard")
def hydrogen_refuelling_station_dashboard():
    result = _build_generation_response("Hydrogen Refuelling Station > Dashboard", 6227)
    if result:
        return result
    _r.seed(6227)
    return {
        "summary": {"title": "Hydrogen Refuelling Station > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydrogen-refuelling-transport/dashboard")
def hydrogen_refuelling_transport_dashboard():
    result = _build_generation_response("Hydrogen Refuelling Transport > Dashboard", 6228)
    if result:
        return result
    _r.seed(6228)
    return {
        "summary": {"title": "Hydrogen Refuelling Transport > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydrogen-valley-cluster/dashboard")
def hydrogen_valley_cluster_dashboard():
    result = _build_generation_response("Hydrogen Valley Cluster > Dashboard", 6229)
    if result:
        return result
    _r.seed(6229)
    return {
        "summary": {"title": "Hydrogen Valley Cluster > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/hydrogen/benchmarks")
def hydrogen_benchmarks():
    result = _build_generation_response("Hydrogen > Benchmarks", 6230)
    if result and "records" in result:
        return result["records"]
    _r.seed(6230)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Hydrogen > Benchmarks", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/industrial-decarbonisation/dashboard")
def industrial_decarbonisation_dashboard():
    result = _build_carbon_response("Industrial Decarbonisation > Dashboard", 6231)
    if result:
        return result
    _r.seed(6231)
    return {
        "summary": {"title": "Industrial Decarbonisation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/industrial-demand-flex/aggregate")
def industrial_demand_flex_aggregate():
    result = _build_grid_response("Industrial Demand Flex > Aggregate", 6232)
    if result and "records" in result:
        return result["records"]
    _r.seed(6232)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Industrial Demand Flex > Aggregate", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/industrial-demand-flex/consumers")
def industrial_demand_flex_consumers():
    result = _build_grid_response("Industrial Demand Flex > Consumers", 6233)
    if result and "records" in result:
        return result["records"]
    _r.seed(6233)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Industrial Demand Flex > Consumers", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/industrial-demand-flex/dashboard")
def industrial_demand_flex_dashboard():
    result = _build_grid_response("Industrial Demand Flex > Dashboard", 6234)
    if result:
        return result
    _r.seed(6234)
    return {
        "summary": {"title": "Industrial Demand Flex > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/industrial-demand-flex/events")
def industrial_demand_flex_events():
    result = _build_grid_response("Industrial Demand Flex > Events", 6235)
    if result and "records" in result:
        return result["records"]
    _r.seed(6235)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Industrial Demand Flex > Events", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/industrial-demand-flex/load-shapes")
def industrial_demand_flex_load_shapes():
    result = _build_grid_response("Industrial Demand Flex > Load Shapes", 6236)
    if result and "records" in result:
        return result["records"]
    _r.seed(6236)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Industrial Demand Flex > Load Shapes", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/industrial-electrification-x/dashboard")
def industrial_electrification_x_dashboard():
    _r.seed(6237)
    return {
        "summary": {"title": "Industrial Electrification X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/industrial-electrification/dashboard")
def industrial_electrification_dashboard():
    _r.seed(6238)
    return {
        "summary": {"title": "Industrial Electrification > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/industrial-energy-efficiency/dashboard")
def industrial_energy_efficiency_dashboard():
    _r.seed(6239)
    return {
        "summary": {"title": "Industrial Energy Efficiency > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/inertia/dashboard")
def inertia_dashboard():
    _r.seed(6240)
    return {
        "summary": {"title": "Inertia > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/integration-cost/dashboard")
def integration_cost_dashboard():
    result = _build_pricing_response("Integration Cost > Dashboard", 6241)
    if result:
        return result
    _r.seed(6241)
    return {
        "summary": {"title": "Integration Cost > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/interconnector-congestion/dashboard")
def interconnector_congestion_dashboard():
    result = _build_grid_response("Interconnector Congestion > Dashboard", 6242)
    if result:
        return result
    _r.seed(6242)
    return {
        "summary": {"title": "Interconnector Congestion > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/interconnector-flow-analytics/dashboard")
def interconnector_flow_analytics_dashboard():
    result = _build_grid_response("Interconnector Flow Analytics > Dashboard", 6243)
    if result:
        return result
    _r.seed(6243)
    return {
        "summary": {"title": "Interconnector Flow Analytics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/interconnector-flow-analytics/flows")
def interconnector_flow_analytics_flows():
    result = _build_grid_response("Interconnector Flow Analytics > Flows", 6244)
    if result and "records" in result:
        return result["records"]
    _r.seed(6244)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Interconnector Flow Analytics > Flows", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/interconnector-flow-analytics/interconnectors")
def interconnector_flow_analytics_interconnectors():
    result = _build_grid_response("Interconnector Flow Analytics > Interconnectors", 6245)
    if result and "records" in result:
        return result["records"]
    _r.seed(6245)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Interconnector Flow Analytics > Interconnectors", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/interconnector-flow-analytics/patterns")
def interconnector_flow_analytics_patterns():
    result = _build_grid_response("Interconnector Flow Analytics > Patterns", 6246)
    if result and "records" in result:
        return result["records"]
    _r.seed(6246)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Interconnector Flow Analytics > Patterns", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/interconnector-flow-analytics/upgrades")
def interconnector_flow_analytics_upgrades():
    result = _build_grid_response("Interconnector Flow Analytics > Upgrades", 6247)
    if result and "records" in result:
        return result["records"]
    _r.seed(6247)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Interconnector Flow Analytics > Upgrades", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/interconnector-flow-rights/dashboard")
def interconnector_flow_rights_dashboard():
    result = _build_grid_response("Interconnector Flow Rights > Dashboard", 6248)
    if result:
        return result
    _r.seed(6248)
    return {
        "summary": {"title": "Interconnector Flow Rights > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/interconnector-upgrade/dashboard")
def interconnector_upgrade_dashboard():
    result = _build_grid_response("Interconnector Upgrade > Dashboard", 6249)
    if result:
        return result
    _r.seed(6249)
    return {
        "summary": {"title": "Interconnector Upgrade > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/isp-progress/actionable-projects")
def isp_progress_actionable_projects():
    result = _build_isp_response("ISP Progress > Actionable Projects", 6250)
    if result and "records" in result:
        return result["records"]
    _r.seed(6250)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Isp Progress > Actionable Projects", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/isp-progress/capacity-milestones")
def isp_progress_capacity_milestones():
    result = _build_isp_capacity_response("ISP Progress > Capacity Milestones", 6251)
    if result and "records" in result:
        return result["records"]
    _r.seed(6251)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Isp Progress > Capacity Milestones", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/isp-progress/delivery-risks")
def isp_progress_delivery_risks():
    result = _build_isp_response("ISP Progress > Delivery Risks", 6252)
    if result and "records" in result:
        return [{"project_name": r["project_name"], "status": r["status"],
                 "expected_completion": r["expected_completion"],
                 "risk_level": "High" if r["status"] in ("Planning", "Feasibility", "Assessment") else "Medium" if r["status"] == "Approved" else "Low",
                 "capex_m_aud": r["capex_m_aud"]} for r in result["records"]]
    _r.seed(6252)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Isp Progress > Delivery Risks", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/isp-progress/scenarios")
def isp_progress_scenarios():
    result = _build_isp_capacity_response("ISP Progress > Scenarios", 6253)
    if result and "by_year" in result:
        return result["by_year"]
    _r.seed(6253)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Isp Progress > Scenarios", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/isp/tnsp-programs")
def isp_tnsp_programs():
    result = _build_isp_response("ISP > TNSP Programs", 6254)
    if result and "records" in result:
        return [{"project_name": r["project_name"], "tnsp": r["tnsp"],
                 "type": r["type"], "status": r["status"],
                 "route_km": r["route_km"], "capex_m_aud": r["capex_m_aud"]} for r in result["records"]]
    _r.seed(6254)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Isp > Tnsp Programs", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/large-industrial-demand/dashboard")
def large_industrial_demand_dashboard():
    result = _build_grid_response("Large Industrial Demand > Dashboard", 6255)
    if result:
        return result
    _r.seed(6255)
    return {
        "summary": {"title": "Large Industrial Demand > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/large-scale-renewable-auction/dashboard")
def large_scale_renewable_auction_dashboard():
    result = _build_renewable_response("Large Scale Renewable Auction > Dashboard", 6256)
    if result:
        return result
    _r.seed(6256)
    return {
        "summary": {"title": "Large Scale Renewable Auction > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ldes-analytics/dashboard")
def ldes_analytics_dashboard():
    _r.seed(6257)
    return {
        "summary": {"title": "Ldes Analytics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ldes-economics/dashboard")
def ldes_economics_dashboard():
    _r.seed(6258)
    return {
        "summary": {"title": "Ldes Economics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/lgc-market/dashboard")
def lgc_market_dashboard():
    result = _build_lgc_response("LGC Market > Dashboard", 6259)
    if result:
        return result
    _r.seed(6259)
    return {
        "summary": {"title": "Lgc Market > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/lng-export/dashboard")
def lng_export_dashboard():
    _r.seed(6260)
    return {
        "summary": {"title": "Lng Export > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/long-duration-energy-storage-x/dashboard")
def long_duration_energy_storage_x_dashboard():
    result = _build_battery_response("Long Duration Energy Storage X > Dashboard", 6261)
    if result:
        return result
    _r.seed(6261)
    return {
        "summary": {"title": "Long Duration Energy Storage X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/lrmc/dashboard")
def lrmc_dashboard():
    _r.seed(6262)
    return {
        "summary": {"title": "Lrmc > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/market-anomaly-detection/dashboard")
def market_anomaly_detection_dashboard():
    _r.seed(6263)
    return {
        "summary": {"title": "Market Anomaly Detection > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/market-design-simulation/dashboard")
def market_design_simulation_dashboard():
    result = _build_pricing_response("Market Design Simulation > Dashboard", 6264)
    if result:
        return result
    _r.seed(6264)
    return {
        "summary": {"title": "Market Design Simulation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/market-design/capacity-mechanisms")
def market_design_capacity_mechanisms():
    result = _build_pricing_response("Market Design > Capacity Mechanisms", 6265)
    if result and "records" in result:
        return result["records"]
    _r.seed(6265)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Market Design > Capacity Mechanisms", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/market-design/dashboard")
def market_design_dashboard():
    result = _build_pricing_response("Market Design > Dashboard", 6266)
    if result:
        return result
    _r.seed(6266)
    return {
        "summary": {"title": "Market Design > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/market-design/market-comparison")
def market_design_market_comparison():
    result = _build_pricing_response("Market Design > Market Comparison", 6267)
    if result and "records" in result:
        return result["records"]
    _r.seed(6267)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Market Design > Market Comparison", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/market-design/proposals")
def market_design_proposals():
    result = _build_pricing_response("Market Design > Proposals", 6268)
    if result and "records" in result:
        return result["records"]
    _r.seed(6268)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Market Design > Proposals", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/market-design/settlement-reforms")
def market_design_settlement_reforms():
    result = _build_pricing_response("Market Design > Settlement Reforms", 6269)
    if result and "records" in result:
        return result["records"]
    _r.seed(6269)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Market Design > Settlement Reforms", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/market-liquidity/bid-ask-spreads")
def market_liquidity_bid_ask_spreads():
    result = _build_pricing_response("Market Liquidity > Bid Ask Spreads", 6270)
    if result and "records" in result:
        return result["records"]
    _r.seed(6270)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Market Liquidity > Bid Ask Spreads", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/market-liquidity/dashboard")
def market_liquidity_dashboard():
    result = _build_pricing_response("Market Liquidity > Dashboard", 6271)
    if result:
        return result
    _r.seed(6271)
    return {
        "summary": {"title": "Market Liquidity > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/market-liquidity/market-depth")
def market_liquidity_market_depth():
    result = _build_pricing_response("Market Liquidity > Market Depth", 6272)
    if result and "records" in result:
        return result["records"]
    _r.seed(6272)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Market Liquidity > Market Depth", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/market-liquidity/metrics")
def market_liquidity_metrics():
    result = _build_pricing_response("Market Liquidity > Metrics", 6273)
    if result and "records" in result:
        return result["records"]
    _r.seed(6273)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Market Liquidity > Metrics", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/market-liquidity/trading-volumes")
def market_liquidity_trading_volumes():
    result = _build_pricing_response("Market Liquidity > Trading Volumes", 6274)
    if result and "records" in result:
        return result["records"]
    _r.seed(6274)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Market Liquidity > Trading Volumes", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/market-trading-strategy/dashboard")
def market_trading_strategy_dashboard():
    _r.seed(6275)
    return {
        "summary": {"title": "Market Trading Strategy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/merchant-renewable/dashboard")
def merchant_renewable_dashboard():
    result = _build_renewable_response("Merchant Renewable > Dashboard", 6276)
    if result:
        return result
    _r.seed(6276)
    return {
        "summary": {"title": "Merchant Renewable > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/microgrid-raps/dashboard")
def microgrid_raps_dashboard():
    result = _build_grid_response("Microgrid Raps > Dashboard", 6277)
    if result:
        return result
    _r.seed(6277)
    return {
        "summary": {"title": "Microgrid Raps > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/microgrid-raps/diesel-displacement")
def microgrid_raps_diesel_displacement():
    result = _build_grid_response("Microgrid Raps > Diesel Displacement", 6278)
    if result and "records" in result:
        return result["records"]
    _r.seed(6278)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Microgrid Raps > Diesel Displacement", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/microgrid-raps/energy-records")
def microgrid_raps_energy_records():
    result = _build_grid_response("Microgrid Raps > Energy Records", 6279)
    if result and "records" in result:
        return result["records"]
    _r.seed(6279)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Microgrid Raps > Energy Records", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/microgrid-raps/microgrids")
def microgrid_raps_microgrids():
    result = _build_grid_response("Microgrid Raps > Microgrids", 6280)
    if result and "records" in result:
        return result["records"]
    _r.seed(6280)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Microgrid Raps > Microgrids", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/microgrid-raps/technology-summary")
def microgrid_raps_technology_summary():
    result = _build_grid_response("Microgrid Raps > Technology Summary", 6281)
    if result and "records" in result:
        return result["records"]
    _r.seed(6281)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Microgrid Raps > Technology Summary", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/minimum-demand/dashboard")
def minimum_demand_dashboard():
    result = _build_grid_response("Minimum Demand > Dashboard", 6282)
    if result:
        return result
    _r.seed(6282)
    return {
        "summary": {"title": "Minimum Demand > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/minimum-demand/duck-curve")
def minimum_demand_duck_curve():
    result = _build_grid_response("Minimum Demand > Duck Curve", 6283)
    if result and "records" in result:
        return result["records"]
    _r.seed(6283)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Minimum Demand > Duck Curve", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/minimum-demand/negative-pricing")
def minimum_demand_negative_pricing():
    result = _build_grid_response("Minimum Demand > Negative Pricing", 6284)
    if result and "records" in result:
        return result["records"]
    _r.seed(6284)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Minimum Demand > Negative Pricing", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/minimum-demand/records")
def minimum_demand_records():
    result = _build_grid_response("Minimum Demand > Records", 6285)
    if result and "records" in result:
        return result["records"]
    _r.seed(6285)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Minimum Demand > Records", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/national-energy-market-reform/dashboard")
def national_energy_market_reform_dashboard():
    _r.seed(6286)
    return {
        "summary": {"title": "National Energy Market Reform > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/national-energy-transition-investment/dashboard")
def national_energy_transition_investment_dashboard():
    _r.seed(6287)
    return {
        "summary": {"title": "National Energy Transition Investment > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/natural-gas-trading/dashboard")
def natural_gas_trading_dashboard():
    _r.seed(6288)
    return {
        "summary": {"title": "Natural Gas Trading > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/nem-ancillary-services-regulation/dashboard")
def nem_ancillary_services_regulation_dashboard():
    result = _build_grid_response("Nem Ancillary Services Regulation > Dashboard", 6289)
    if result:
        return result
    _r.seed(6289)
    return {
        "summary": {"title": "Nem Ancillary Services Regulation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/nem-frequency-control-x/dashboard")
def nem_frequency_control_x_dashboard():
    result = _build_grid_response("Nem Frequency Control X > Dashboard", 6290)
    if result:
        return result
    _r.seed(6290)
    return {
        "summary": {"title": "Nem Frequency Control X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/nem-generation-mix/dashboard")
def nem_generation_mix_dashboard():
    _r.seed(6291)
    return {
        "summary": {"title": "Nem Generation Mix > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/nem-inertia-synchronous-condenser/dashboard")
def nem_inertia_synchronous_condenser_dashboard():
    _r.seed(6292)
    return {
        "summary": {"title": "Nem Inertia Synchronous Condenser > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/nem-market-microstructure/dashboard")
def nem_market_microstructure_dashboard():
    _r.seed(6293)
    return {
        "summary": {"title": "Nem Market Microstructure > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/nem-post-reform-market-design/dashboard")
def nem_post_reform_market_design_dashboard():
    result = _build_pricing_response("Nem Post Reform Market Design > Dashboard", 6294)
    if result:
        return result
    _r.seed(6294)
    return {
        "summary": {"title": "Nem Post Reform Market Design > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/net-zero-emissions/dashboard")
def net_zero_emissions_dashboard():
    result = _build_carbon_response("Net Zero Emissions > Dashboard", 6295)
    if result:
        return result
    _r.seed(6295)
    return {
        "summary": {"title": "Net Zero Emissions > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/network-asset-life-cycle/dashboard")
def network_asset_life_cycle_dashboard():
    result = _build_grid_response("Network Asset Life Cycle > Dashboard", 6296)
    if result:
        return result
    _r.seed(6296)
    return {
        "summary": {"title": "Network Asset Life Cycle > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/network-congestion-relief/dashboard")
def network_congestion_relief_dashboard():
    result = _build_grid_response("Network Congestion Relief > Dashboard", 6297)
    if result:
        return result
    _r.seed(6297)
    return {
        "summary": {"title": "Network Congestion Relief > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/network-investment-pipeline/dashboard")
def network_investment_pipeline_dashboard():
    result = _build_grid_response("Network Investment Pipeline > Dashboard", 6298)
    if result:
        return result
    _r.seed(6298)
    return {
        "summary": {"title": "Network Investment Pipeline > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/network-protection-system/dashboard")
def network_protection_system_dashboard():
    result = _build_grid_response("Network Protection System > Dashboard", 6299)
    if result:
        return result
    _r.seed(6299)
    return {
        "summary": {"title": "Network Protection System > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/network-regulatory-framework/dashboard")
def network_regulatory_framework_dashboard():
    result = _build_grid_response("Network Regulatory Framework > Dashboard", 6300)
    if result:
        return result
    _r.seed(6300)
    return {
        "summary": {"title": "Network Regulatory Framework > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/network-tariff-design-reform/dashboard")
def network_tariff_design_reform_dashboard():
    result = _build_tariff_response("Network Tariff Design Reform > Dashboard", 6301)
    if result:
        return result
    _r.seed(6301)
    return {
        "summary": {"title": "Network Tariff Design Reform > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/network-tariff-reform/der-impacts")
def network_tariff_reform_der_impacts():
    result = _build_tariff_response("Network Tariff Reform > DER Impacts", 6302)
    if result and "records" in result:
        return result["records"]
    _r.seed(6302)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Network Tariff Reform > Der Impacts", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/network-tariff-reform/reforms")
def network_tariff_reform_reforms():
    result = _build_tariff_response("Network Tariff Reform > Reforms", 6303)
    if result and "records" in result:
        return result["records"]
    _r.seed(6303)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Network Tariff Reform > Reforms", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/network-tariff-reform/revenue")
def network_tariff_reform_revenue():
    result = _build_tariff_response("Network Tariff Reform > Revenue", 6304)
    if result and "records" in result:
        return result["records"]
    _r.seed(6304)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Network Tariff Reform > Revenue", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/network-tariff-reform/tariffs")
def network_tariff_reform_tariffs():
    result = _build_tariff_response("Network Tariff Reform > Tariffs", 6305)
    if result and "records" in result:
        return result["records"]
    _r.seed(6305)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Network Tariff Reform > Tariffs", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/nuclear-energy-economics/dashboard")
def nuclear_energy_economics_dashboard():
    _r.seed(6306)
    return {
        "summary": {"title": "Nuclear Energy Economics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/nuclear-energy-feasibility/dashboard")
def nuclear_energy_feasibility_dashboard():
    _r.seed(6307)
    return {
        "summary": {"title": "Nuclear Energy Feasibility > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/nuclear-energy/dashboard")
def nuclear_energy_dashboard():
    _r.seed(6308)
    return {
        "summary": {"title": "Nuclear Energy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/nuclear-ldes/dashboard")
def nuclear_ldes_dashboard():
    _r.seed(6309)
    return {
        "summary": {"title": "Nuclear Ldes > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/nuclear-small-modular-reactor/dashboard")
def nuclear_small_modular_reactor_dashboard():
    _r.seed(6310)
    return {
        "summary": {"title": "Nuclear Small Modular Reactor > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/offshore-wind-dev-analytics/dashboard")
def offshore_wind_dev_analytics_dashboard():
    result = _build_renewable_response("Offshore Wind Dev Analytics > Dashboard", 6311)
    if result:
        return result
    _r.seed(6311)
    return {
        "summary": {"title": "Offshore Wind Dev Analytics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/offshore-wind-development/dashboard")
def offshore_wind_development_dashboard():
    result = _build_renewable_response("Offshore Wind Development > Dashboard", 6312)
    if result:
        return result
    _r.seed(6312)
    return {
        "summary": {"title": "Offshore Wind Development > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/offshore-wind-finance/dashboard")
def offshore_wind_finance_dashboard():
    result = _build_renewable_response("Offshore Wind Finance > Dashboard", 6313)
    if result:
        return result
    _r.seed(6313)
    return {
        "summary": {"title": "Offshore Wind Finance > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/offshore-wind-leasing-site/dashboard")
def offshore_wind_leasing_site_dashboard():
    result = _build_renewable_response("Offshore Wind Leasing Site > Dashboard", 6314)
    if result:
        return result
    _r.seed(6314)
    return {
        "summary": {"title": "Offshore Wind Leasing Site > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/offshore-wind-pipeline/capacity-outlook")
def offshore_wind_pipeline_capacity_outlook():
    result = _build_renewable_response("Offshore Wind Pipeline > Capacity Outlook", 6315)
    if result and "records" in result:
        return result["records"]
    _r.seed(6315)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Offshore Wind Pipeline > Capacity Outlook", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/offshore-wind-pipeline/dashboard")
def offshore_wind_pipeline_dashboard():
    rows = _offshore_wind_data()
    if rows:
        total_gw = sum(float(r.get("estimated_capacity_gw") or 0) for r in rows)
        return {
            "summary": {"title": "Offshore Wind Pipeline", "declared_areas": len(rows),
                        "total_capacity_gw": round(total_gw, 1),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"area": r.get("area_name"), "state": r.get("state"),
                         "capacity_gw": float(r.get("estimated_capacity_gw") or 0),
                         "area_sq_km": float(r.get("area_sq_km") or 0),
                         "status": r.get("status"),
                         "applicants": int(r.get("licence_applicants") or 0)} for r in rows]}
    result = _build_renewable_response("Offshore Wind Pipeline > Dashboard", 6316)
    if result:
        return result
    _r.seed(6316)
    return {
        "summary": {"title": "Offshore Wind Pipeline > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/offshore-wind-pipeline/declared-areas")
def offshore_wind_pipeline_declared_areas():
    rows = _offshore_wind_data()
    if rows:
        return [{"id": i+1, "area": r.get("area_name"), "state": r.get("state"),
                 "declared_date": r.get("declared_date"), "area_sq_km": float(r.get("area_sq_km") or 0),
                 "capacity_gw": float(r.get("estimated_capacity_gw") or 0),
                 "water_depth": r.get("water_depth_m"),
                 "distance_km": float(r.get("distance_from_shore_km") or 0),
                 "description": (r.get("description") or "")[:200]} for i, r in enumerate(rows)]
    _r.seed(6317)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Offshore Wind Pipeline > Declared Areas", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/offshore-wind-pipeline/licences")
def offshore_wind_pipeline_licences():
    rows = _offshore_wind_data()
    if rows:
        with_licences = [r for r in rows if int(r.get("licence_applicants") or 0) > 0]
        return [{"id": i+1, "area": r.get("area_name"), "state": r.get("state"),
                 "licence_applicants": int(r.get("licence_applicants") or 0),
                 "status": r.get("status"),
                 "key_projects": r.get("key_projects")} for i, r in enumerate(with_licences)]
    _r.seed(6318)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Offshore Wind Pipeline > Licences", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/offshore-wind-pipeline/supply-chain")
def offshore_wind_pipeline_supply_chain():
    result = _build_renewable_response("Offshore Wind Pipeline > Supply Chain", 6319)
    if result and "records" in result:
        return result["records"]
    _r.seed(6319)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Offshore Wind Pipeline > Supply Chain", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/offshore-wind-project-finance-x/dashboard")
def offshore_wind_project_finance_x_dashboard():
    result = _build_renewable_response("Offshore Wind Project Finance X > Dashboard", 6320)
    if result:
        return result
    _r.seed(6320)
    return {
        "summary": {"title": "Offshore Wind Project Finance X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/offshore-wind/dashboard")
def offshore_wind_dashboard():
    result = _build_renewable_response("Offshore Wind > Dashboard", 6321)
    if result:
        return result
    _r.seed(6321)
    return {
        "summary": {"title": "Offshore Wind > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/offshore-wind/zones")
def offshore_wind_zones():
    result = _build_renewable_response("Offshore Wind > Zones", 6322)
    if result and "records" in result:
        return result["records"]
    _r.seed(6322)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Offshore Wind > Zones", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/ot-ics-cyber-security/dashboard")
def ot_ics_cyber_security_dashboard():
    _r.seed(6323)
    return {
        "summary": {"title": "Ot Ics Cyber Security > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/phes/dashboard")
def phes_dashboard():
    _r.seed(6324)
    return {
        "summary": {"title": "Phes > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/phes/outlook")
def phes_outlook():
    _r.seed(6325)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Phes > Outlook", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/portfolio-risk-optimisation/dashboard")
def portfolio_risk_optimisation_dashboard():
    _r.seed(6326)
    return {
        "summary": {"title": "Portfolio Risk Optimisation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/power-grid-climate-resilience/dashboard")
def power_grid_climate_resilience_dashboard():
    result = _build_carbon_response("Power Grid Climate Resilience > Dashboard", 6327)
    if result:
        return result
    _r.seed(6327)
    return {
        "summary": {"title": "Power Grid Climate Resilience > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/power-grid-topology/dashboard")
def power_grid_topology_dashboard():
    result = _build_grid_response("Power Grid Topology > Dashboard", 6328)
    if result:
        return result
    _r.seed(6328)
    return {
        "summary": {"title": "Power Grid Topology > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/power-purchase-agreement-market/dashboard")
def power_purchase_agreement_market_dashboard():
    _r.seed(6329)
    return {
        "summary": {"title": "Power Purchase Agreement Market > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/power-system-events/dashboard")
def power_system_events_dashboard():
    _r.seed(6330)
    return {
        "summary": {"title": "Power System Events > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/power-system-inertia/dashboard")
def power_system_inertia_dashboard():
    _r.seed(6331)
    return {
        "summary": {"title": "Power System Inertia > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/power-system-stability-x/dashboard")
def power_system_stability_x_dashboard():
    _r.seed(6332)
    return {
        "summary": {"title": "Power System Stability X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/power-system-stability/dashboard")
def power_system_stability_dashboard():
    _r.seed(6333)
    return {
        "summary": {"title": "Power System Stability > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/power-to-x-economics/dashboard")
def power_to_x_economics_dashboard():
    _r.seed(6334)
    return {
        "summary": {"title": "Power To X Economics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ppa-market/dashboard")
def ppa_market_dashboard():
    result = _build_pricing_response("Ppa Market > Dashboard", 6335)
    if result:
        return result
    _r.seed(6335)
    return {
        "summary": {"title": "Ppa Market > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ppa-structuring/dashboard")
def ppa_structuring_dashboard():
    result = _build_pricing_response("Ppa Structuring > Dashboard", 6336)
    if result:
        return result
    _r.seed(6336)
    return {
        "summary": {"title": "Ppa Structuring > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/ppa/lgc-market")
def ppa_lgc_market():
    result = _build_pricing_response("Ppa > Lgc Market", 6337)
    if result and "records" in result:
        return result["records"]
    _r.seed(6337)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Ppa > Lgc Market", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/prosumer/dashboard")
def prosumer_dashboard():
    _r.seed(6338)
    return {
        "summary": {"title": "Prosumer > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/pumped-hydro-dispatch/dashboard")
def pumped_hydro_dispatch_dashboard():
    result = _build_battery_response("Pumped Hydro Dispatch > Dashboard", 6339)
    if result:
        return result
    _r.seed(6339)
    return {
        "summary": {"title": "Pumped Hydro Dispatch > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/pumped-hydro-reservoir-operations/dashboard")
def pumped_hydro_reservoir_operations_dashboard():
    result = _build_battery_response("Pumped Hydro Reservoir Operations > Dashboard", 6340)
    if result:
        return result
    _r.seed(6340)
    return {
        "summary": {"title": "Pumped Hydro Reservoir Operations > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/pumped-hydro-resource-assessment/dashboard")
def pumped_hydro_resource_assessment_dashboard():
    result = _build_battery_response("Pumped Hydro Resource Assessment > Dashboard", 6341)
    if result:
        return result
    _r.seed(6341)
    return {
        "summary": {"title": "Pumped Hydro Resource Assessment > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rab/dashboard")
def rab_dashboard():
    rows = _rab_data()
    if rows:
        total_rab = sum(float(r.get("rab_opening_m_aud") or 0) for r in rows)
        by_type = {}
        for r in rows:
            t = r.get("network_type", "DNSP")
            by_type.setdefault(t, {"count": 0, "rab_m": 0})
            by_type[t]["count"] += 1
            by_type[t]["rab_m"] += float(r.get("rab_opening_m_aud") or 0)
        return {
            "summary": {"title": "RAB & Network Determinations", "businesses": len(rows),
                        "total_rab_b_aud": round(total_rab / 1000, 1),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"business": r.get("network_business"), "type": r.get("network_type"),
                         "state": r.get("state"), "period": r.get("regulatory_period"),
                         "rab_m": float(r.get("rab_opening_m_aud") or 0),
                         "capex_m": float(r.get("capex_allowance_m_aud") or 0),
                         "wacc_pct": float(r.get("wacc_nominal_pct") or 0)} for r in rows],
            "by_type": [{"type": k, **v} for k, v in by_type.items()]}
    _r.seed(6342)
    return {
        "summary": {"title": "Rab > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rab/determinations")
def rab_determinations():
    rows = _rab_data()
    if rows:
        return [{"id": i+1, "business": r.get("network_business"), "state": r.get("state"),
                 "period": r.get("regulatory_period"),
                 "rab_opening_m": float(r.get("rab_opening_m_aud") or 0),
                 "rab_closing_m": float(r.get("rab_closing_m_aud") or 0),
                 "revenue_m": float(r.get("allowed_revenue_m_aud") or 0),
                 "status": r.get("status")} for i, r in enumerate(rows)]
    _r.seed(6343)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rab > Determinations", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rab/yearly")
def rab_yearly():
    rows = _rab_data()
    if rows:
        by_state = {}
        for r in rows:
            s = r.get("state", "")
            by_state.setdefault(s, {"rab_m": 0, "capex_m": 0, "opex_m": 0})
            by_state[s]["rab_m"] += float(r.get("rab_opening_m_aud") or 0)
            by_state[s]["capex_m"] += float(r.get("capex_allowance_m_aud") or 0)
            by_state[s]["opex_m"] += float(r.get("opex_allowance_m_aud") or 0)
        return [{"id": i+1, "state": k, "total_rab_m": round(v["rab_m"], 0),
                 "total_capex_m": round(v["capex_m"], 0),
                 "total_opex_m": round(v["opex_m"], 0)} for i, (k, v) in enumerate(sorted(by_state.items()))]
    _r.seed(6344)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rab > Yearly", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/reactive-power-voltage/dashboard")
def reactive_power_voltage_dashboard():
    result = _build_grid_response("Reactive Power Voltage > Dashboard", 6345)
    if result:
        return result
    _r.seed(6345)
    return {
        "summary": {"title": "Reactive Power Voltage > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/realtime/dispatch")
def realtime_dispatch():
    result = _build_pricing_response("Realtime > Dispatch", 6346)
    if result and "records" in result:
        return result["records"]
    _r.seed(6346)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Realtime > Dispatch", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/realtime/generation-mix")
def realtime_generation_mix():
    result = _build_generation_response("Realtime > Generation Mix", 6347)
    if result and "records" in result:
        return result["records"]
    _r.seed(6347)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Realtime > Generation Mix", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/realtime/interconnectors")
def realtime_interconnectors():
    result = _build_grid_response("Realtime > Interconnectors", 6348)
    if result and "records" in result:
        return result["records"]
    _r.seed(6348)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Realtime > Interconnectors", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rec-market-analytics/dashboard")
def rec_market_analytics_dashboard():
    result = _build_lgc_response("REC Market Analytics > Dashboard", 6349)
    if result:
        return result
    _r.seed(6349)
    return {
        "summary": {"title": "Rec Market Analytics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rec-market/lgc-creation")
def rec_market_lgc_creation():
    result = _build_lgc_response("REC Market > LGC Creation", 6350)
    if result and "records" in result:
        return result["records"]
    _r.seed(6350)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rec Market > Lgc Creation", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rec-market/lgc-spot")
def rec_market_lgc_spot():
    spot = _lgc_spot_data()
    if spot:
        return [{"trade_date": s.get("trade_date"),
                 "price_aud_mwh": float(s.get("price_aud_mwh") or 0),
                 "source": s.get("source")} for s in spot[:20]]
    _r.seed(6351)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rec Market > Lgc Spot", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rec-market/stc")
def rec_market_stc():
    result = _build_lgc_response("REC Market > STC", 6352)
    if result and "records" in result:
        return result["records"]
    _r.seed(6352)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rec Market > Stc", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rec-market/surplus-deficit")
def rec_market_surplus_deficit():
    result = _build_lgc_response("REC Market > Surplus Deficit", 6353)
    if result and "by_fuel_type" in result:
        return result["by_fuel_type"]
    _r.seed(6353)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rec Market > Surplus Deficit", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rec-tracking/dashboard")
def rec_tracking_dashboard():
    result = _build_lgc_response("REC Tracking > Dashboard", 6354)
    if result:
        return result
    _r.seed(6354)
    return {
        "summary": {"title": "Rec Tracking > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/reform/dashboard")
def reform_dashboard():
    _r.seed(6355)
    return {
        "summary": {"title": "Reform > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/reliability-standard/dashboard")
def reliability_standard_dashboard():
    _r.seed(6356)
    return {
        "summary": {"title": "Reliability Standard > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/renewable-auction/dashboard")
def renewable_auction_dashboard():
    result = _build_renewable_response("Renewable Auction > Dashboard", 6357)
    if result:
        return result
    _r.seed(6357)
    return {
        "summary": {"title": "Renewable Auction > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/renewable-certificate-nem/dashboard")
def renewable_certificate_nem_dashboard():
    result = _build_renewable_response("Renewable Certificate Nem > Dashboard", 6358)
    if result:
        return result
    _r.seed(6358)
    return {
        "summary": {"title": "Renewable Certificate Nem > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/renewable-energy-certificate/dashboard")
def renewable_energy_certificate_dashboard():
    result = _build_renewable_response("Renewable Energy Certificate > Dashboard", 6359)
    if result:
        return result
    _r.seed(6359)
    return {
        "summary": {"title": "Renewable Energy Certificate > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/renewable-energy-certificatex/dashboard")
def renewable_energy_certificatex_dashboard():
    result = _build_renewable_response("Renewable Energy Certificatex > Dashboard", 6360)
    if result:
        return result
    _r.seed(6360)
    return {
        "summary": {"title": "Renewable Energy Certificatex > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/renewable-energy-zone-development/dashboard")
def renewable_energy_zone_development_dashboard():
    result = _build_renewable_response("Renewable Energy Zone Development > Dashboard", 6361)
    if result:
        return result
    _r.seed(6361)
    return {
        "summary": {"title": "Renewable Energy Zone Development > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/renewable-export/dashboard")
def renewable_export_dashboard():
    result = _build_renewable_response("Renewable Export > Dashboard", 6362)
    if result:
        return result
    _r.seed(6362)
    return {
        "summary": {"title": "Renewable Export > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/residential-solar-self-consumption/dashboard")
def residential_solar_self_consumption_dashboard():
    result = _build_renewable_response("Residential Solar Self Consumption > Dashboard", 6363)
    if result:
        return result
    _r.seed(6363)
    return {
        "summary": {"title": "Residential Solar Self Consumption > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/retail-market-design/dashboard")
def retail_market_design_dashboard():
    result = _build_pricing_response("Retail Market Design > Dashboard", 6364)
    if result:
        return result
    _r.seed(6364)
    return {
        "summary": {"title": "Retail Market Design > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/retail-offer-comparison/dashboard")
def retail_offer_comparison_dashboard():
    result = _build_tariff_response("Retail Offer Comparison > Dashboard", 6365)
    if result:
        return result
    _r.seed(6365)
    return {
        "summary": {"title": "Retail Offer Comparison > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/retail-offer-comparison/dmo-comparison")
def retail_offer_comparison_dmo_comparison():
    result = _build_tariff_response("Retail Offer Comparison > DMO Comparison", 6366)
    if result and "records" in result:
        return result["records"]
    _r.seed(6366)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Retail Offer Comparison > Dmo Comparison", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/retail-offer-comparison/offers")
def retail_offer_comparison_offers():
    result = _build_tariff_response("Retail Offer Comparison > Offers", 6367)
    if result and "records" in result:
        return result["records"]
    _r.seed(6367)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Retail Offer Comparison > Offers", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/retail-offer-comparison/solar-fit")
def retail_offer_comparison_solar_fit():
    result = _build_renewable_response("Retail Offer Comparison > Solar Fit", 6368)
    if result and "records" in result:
        return result["records"]
    _r.seed(6368)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Retail Offer Comparison > Solar Fit", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/retail-offer-comparison/tariff-structures")
def retail_offer_comparison_tariff_structures():
    result = _build_tariff_response("Retail Offer Comparison > Tariff Structures", 6369)
    if result and "records" in result:
        return result["records"]
    _r.seed(6369)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Retail Offer Comparison > Tariff Structures", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/retailer-competition/dashboard")
def retailer_competition_dashboard():
    result = _build_pricing_response("Retailer Competition > Dashboard", 6370)
    if result:
        return result
    _r.seed(6370)
    return {
        "summary": {"title": "Retailer Competition > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/retailer-financial-health/dashboard")
def retailer_financial_health_dashboard():
    result = _build_pricing_response("Retailer Financial Health > Dashboard", 6371)
    if result:
        return result
    _r.seed(6371)
    return {
        "summary": {"title": "Retailer Financial Health > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rez-auction-cis/dashboard")
def rez_auction_cis_dashboard():
    result = _build_rez_response("REZ Auction CIS > Dashboard", 6372)
    if result:
        return result
    _r.seed(6372)
    return {
        "summary": {"title": "Rez Auction Cis > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rez-capacity-factor/dashboard")
def rez_capacity_factor_dashboard():
    result = _build_rez_response("REZ Capacity Factor > Dashboard", 6373)
    if result:
        return result
    _r.seed(6373)
    return {
        "summary": {"title": "Rez Capacity Factor > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rez-capacity/build-out")
def rez_capacity_build_out():
    result = _build_rez_response("REZ Capacity > Build Out", 6374)
    if result and "records" in result:
        return result["records"]
    _r.seed(6374)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rez Capacity > Build Out", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rez-capacity/dashboard")
def rez_capacity_dashboard():
    result = _build_rez_response("REZ Capacity > Dashboard", 6375)
    if result:
        return result
    _r.seed(6375)
    return {
        "summary": {"title": "Rez Capacity > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rez-capacity/network-augmentations")
def rez_capacity_network_augmentations():
    result = _build_rez_response("REZ Capacity > Network Augmentations", 6376)
    if result and "records" in result:
        return result["records"]
    _r.seed(6376)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rez Capacity > Network Augmentations", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rez-capacity/projects")
def rez_capacity_projects():
    result = _build_rez_response("REZ Capacity > Projects", 6377)
    if result and "records" in result:
        return result["records"]
    _r.seed(6377)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rez Capacity > Projects", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rez-capacity/zones")
def rez_capacity_zones():
    result = _build_rez_response("REZ Capacity > Zones", 6378)
    if result and "records" in result:
        return result["records"]
    _r.seed(6378)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rez Capacity > Zones", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rez-connection-queue/dashboard")
def rez_connection_queue_dashboard():
    result = _build_rez_response("REZ Connection Queue > Dashboard", 6379)
    if result:
        return result
    _r.seed(6379)
    return {
        "summary": {"title": "Rez Connection Queue > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rez-progress/dashboard")
def rez_progress_dashboard():
    result = _build_rez_response("REZ Progress > Dashboard", 6380)
    if result:
        return result
    _r.seed(6380)
    return {
        "summary": {"title": "Rez Progress > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rez-transmission/dashboard")
def rez_transmission_dashboard():
    result = _build_rez_response("REZ Transmission > Dashboard", 6381)
    if result:
        return result
    _r.seed(6381)
    return {
        "summary": {"title": "Rez Transmission > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rit/cost-benefits")
def rit_cost_benefits():
    result = _build_pricing_response("Rit > Cost Benefits", 6382)
    if result and "records" in result:
        return result["records"]
    _r.seed(6382)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rit > Cost Benefits", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rit/dashboard")
def rit_dashboard():
    rows = _rit_data()
    if rows:
        total_cost = sum(float(r.get("estimated_cost_m_aud") or 0) for r in rows)
        total_benefit = sum(float(r.get("net_benefit_m_aud") or 0) for r in rows)
        by_status = {}
        for r in rows:
            s = r.get("status", "UNKNOWN")
            by_status.setdefault(s, 0)
            by_status[s] += 1
        return {
            "summary": {"title": "RIT Register", "total_projects": len(rows),
                        "total_cost_b_aud": round(total_cost / 1000, 1),
                        "total_benefit_b_aud": round(total_benefit / 1000, 1),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"project": r.get("project_name"), "type": r.get("rit_type"),
                         "proponent": r.get("proponent"), "state": r.get("state"),
                         "status": r.get("status"), "cost_m": float(r.get("estimated_cost_m_aud") or 0),
                         "benefit_m": float(r.get("net_benefit_m_aud") or 0)} for r in rows],
            "by_status": [{"status": k, "count": v} for k, v in by_status.items()]}
    _r.seed(6383)
    return {
        "summary": {"title": "Rit > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rit/options")
def rit_options():
    rows = _rit_data()
    if rows:
        completed = [r for r in rows if r.get("preferred_option")]
        return [{"id": i+1, "project": r.get("project_name"),
                 "preferred_option": r.get("preferred_option"),
                 "cost_m": float(r.get("estimated_cost_m_aud") or 0)} for i, r in enumerate(completed)]
    _r.seed(6384)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rit > Options", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rit/projects")
def rit_projects():
    rows = _rit_data()
    if rows:
        return [{"id": i+1, "project": r.get("project_name"), "type": r.get("rit_type"),
                 "proponent": r.get("proponent"), "state": r.get("state"),
                 "status": r.get("status"), "description": (r.get("description") or "")[:200]
                } for i, r in enumerate(rows)]
    _r.seed(6385)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Rit > Projects", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/rooftop-solar-feed-in-tariff/dashboard")
def rooftop_solar_feed_in_tariff_dashboard():
    result = _build_renewable_response("Rooftop Solar Feed In Tariff > Dashboard", 6386)
    if result:
        return result
    _r.seed(6386)
    return {
        "summary": {"title": "Rooftop Solar Feed In Tariff > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/rooftop-solar-network-impact/dashboard")
def rooftop_solar_network_impact_dashboard():
    result = _build_renewable_response("Rooftop Solar Network Impact > Dashboard", 6387)
    if result:
        return result
    _r.seed(6387)
    return {
        "summary": {"title": "Rooftop Solar Network Impact > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/safeguard/accu-market")
def safeguard_accu_market():
    rows = _accu_data()
    if rows:
        return [{"id": i+1, "trade_date": r.get("trade_date"),
                 "accu_spot_price_aud": float(r.get("accu_spot_price_aud") or 0),
                 "volume_traded": int(r.get("volume_traded") or 0),
                 "total_issued": int(r.get("total_accus_issued") or 0),
                 "total_surrendered": int(r.get("total_accus_surrendered") or 0)} for i, r in enumerate(rows)]
    _r.seed(6388)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Safeguard > Accu Market", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/safeguard/dashboard")
def safeguard_dashboard():
    rows = _safeguard_data()
    if rows:
        total_baseline = sum(float(r.get("baseline_tco2e") or 0) for r in rows)
        total_reported = sum(float(r.get("reported_emissions_tco2e") or 0) for r in rows)
        by_sector = {}
        for r in rows:
            s = r.get("sector", "Other")
            by_sector.setdefault(s, 0)
            by_sector[s] += 1
        return {
            "summary": {"title": "Safeguard Mechanism > Dashboard", "total_facilities": len(rows),
                        "total_baseline_mt": round(total_baseline / 1e6, 1),
                        "total_reported_mt": round(total_reported / 1e6, 1),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"facility": r.get("facility_name"), "company": r.get("company"),
                         "sector": r.get("sector"), "state": r.get("state"),
                         "baseline_mt": round(float(r.get("baseline_tco2e") or 0) / 1e6, 2),
                         "reported_mt": round(float(r.get("reported_emissions_tco2e") or 0) / 1e6, 2),
                         "status": r.get("status")} for r in rows],
            "by_sector": [{"sector": k, "count": v} for k, v in sorted(by_sector.items(), key=lambda x: -x[1])]}
    _r.seed(6389)
    return {
        "summary": {"title": "Safeguard > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/smart-grid-cybersecurity/dashboard")
def smart_grid_cybersecurity_dashboard():
    result = _build_grid_response("Smart Grid Cybersecurity > Dashboard", 6390)
    if result:
        return result
    _r.seed(6390)
    return {
        "summary": {"title": "Smart Grid Cybersecurity > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/smart-grid/ami")
def smart_grid_ami():
    result = _build_grid_response("Smart Grid > Ami", 6391)
    if result and "records" in result:
        return result["records"]
    _r.seed(6391)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Smart Grid > Ami", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/smart-grid/dashboard")
def smart_grid_dashboard():
    result = _build_grid_response("Smart Grid > Dashboard", 6392)
    if result:
        return result
    _r.seed(6392)
    return {
        "summary": {"title": "Smart Grid > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/smart-grid/derms")
def smart_grid_derms():
    result = _build_grid_response("Smart Grid > Derms", 6393)
    if result and "records" in result:
        return result["records"]
    _r.seed(6393)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Smart Grid > Derms", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/smart-grid/doe-programs")
def smart_grid_doe_programs():
    result = _build_grid_response("Smart Grid > Doe Programs", 6394)
    if result and "records" in result:
        return result["records"]
    _r.seed(6394)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Smart Grid > Doe Programs", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/social-licence/dashboard")
def social_licence_dashboard():
    _r.seed(6395)
    return {
        "summary": {"title": "Social Licence > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/solar-ev/dashboard")
def solar_ev_dashboard():
    result = _build_renewable_response("Solar Ev > Dashboard", 6396)
    if result:
        return result
    _r.seed(6396)
    return {
        "summary": {"title": "Solar Ev > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/solar-farm-operations/dashboard")
def solar_farm_operations_dashboard():
    result = _build_renewable_response("Solar Farm Operations > Dashboard", 6397)
    if result:
        return result
    _r.seed(6397)
    return {
        "summary": {"title": "Solar Farm Operations > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/solar-farm-performance/dashboard")
def solar_farm_performance_dashboard():
    result = _build_renewable_response("Solar Farm Performance > Dashboard", 6398)
    if result:
        return result
    _r.seed(6398)
    return {
        "summary": {"title": "Solar Farm Performance > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/solar-irradiance-resource/dashboard")
def solar_irradiance_resource_dashboard():
    result = _build_renewable_response("Solar Irradiance Resource > Dashboard", 6399)
    if result:
        return result
    _r.seed(6399)
    return {
        "summary": {"title": "Solar Irradiance Resource > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/solar-park-registry/dashboard")
def solar_park_registry_dashboard():
    result = _build_renewable_response("Solar Park Registry > Dashboard", 6400)
    if result:
        return result
    _r.seed(6400)
    return {
        "summary": {"title": "Solar Park Registry > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/solar-pv-soiling/dashboard")
def solar_pv_soiling_dashboard():
    result = _build_renewable_response("Solar Pv Soiling > Dashboard", 6401)
    if result:
        return result
    _r.seed(6401)
    return {
        "summary": {"title": "Solar Pv Soiling > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/solar-resource/dashboard")
def solar_resource_dashboard():
    result = _build_renewable_response("Solar Resource > Dashboard", 6402)
    if result:
        return result
    _r.seed(6402)
    return {
        "summary": {"title": "Solar Resource > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/solar-resource/degradation")
def solar_resource_degradation():
    result = _build_renewable_response("Solar Resource > Degradation", 6403)
    if result and "records" in result:
        return result["records"]
    _r.seed(6403)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Solar Resource > Degradation", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/solar-resource/farm-yields")
def solar_resource_farm_yields():
    result = _build_renewable_response("Solar Resource > Farm Yields", 6404)
    if result and "records" in result:
        return result["records"]
    _r.seed(6404)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Solar Resource > Farm Yields", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/solar-resource/monthly-irradiance")
def solar_resource_monthly_irradiance():
    result = _build_renewable_response("Solar Resource > Monthly Irradiance", 6405)
    if result and "records" in result:
        return result["records"]
    _r.seed(6405)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Solar Resource > Monthly Irradiance", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/solar-resource/sites")
def solar_resource_sites():
    result = _build_renewable_response("Solar Resource > Sites", 6406)
    if result and "records" in result:
        return result["records"]
    _r.seed(6406)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Solar Resource > Sites", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/solar-thermal-csp/dashboard")
def solar_thermal_csp_dashboard():
    result = _build_renewable_response("Solar Thermal Csp > Dashboard", 6407)
    if result:
        return result
    _r.seed(6407)
    return {
        "summary": {"title": "Solar Thermal Csp > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/solar-thermal-power-plant-x/dashboard")
def solar_thermal_power_plant_x_dashboard():
    result = _build_renewable_response("Solar Thermal Power Plant X > Dashboard", 6408)
    if result:
        return result
    _r.seed(6408)
    return {
        "summary": {"title": "Solar Thermal Power Plant X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/spike-analysis/consumer-impacts")
def spike_analysis_consumer_impacts():
    _r.seed(6409)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Spike Analysis > Consumer Impacts", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/spike-analysis/contributors")
def spike_analysis_contributors():
    _r.seed(6410)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Spike Analysis > Contributors", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/spike-analysis/events")
def spike_analysis_events():
    _r.seed(6411)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Spike Analysis > Events", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/spike-analysis/regional-timeline")
def spike_analysis_regional_timeline():
    _r.seed(6412)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Spike Analysis > Regional Timeline", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/spot-forecast/intervals")
def spot_forecast_intervals():
    result = _build_pricing_response("Spot Forecast > Intervals", 6413)
    if result and "records" in result:
        return result["records"]
    _r.seed(6413)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Spot Forecast > Intervals", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/spot-forecast/model-performance")
def spot_forecast_model_performance():
    result = _build_pricing_response("Spot Forecast > Model Performance", 6414)
    if result and "records" in result:
        return result["records"]
    _r.seed(6414)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Spot Forecast > Model Performance", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/spot-forecast/regional-summary")
def spot_forecast_regional_summary():
    result = _build_pricing_response("Spot Forecast > Regional Summary", 6415)
    if result and "records" in result:
        return result["records"]
    _r.seed(6415)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Spot Forecast > Regional Summary", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/storage-cost-curves/dashboard")
def storage_cost_curves_dashboard():
    result = _build_battery_response("Storage Cost Curves > Dashboard", 6416)
    if result:
        return result
    _r.seed(6416)
    return {
        "summary": {"title": "Storage Cost Curves > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/storage-duration-economics/dashboard")
def storage_duration_economics_dashboard():
    result = _build_battery_response("Storage Duration Economics > Dashboard", 6417)
    if result:
        return result
    _r.seed(6417)
    return {
        "summary": {"title": "Storage Duration Economics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/storage-lca/critical-minerals")
def storage_lca_critical_minerals():
    result = _build_battery_response("Storage Lca > Critical Minerals", 6418)
    if result and "records" in result:
        return result["records"]
    _r.seed(6418)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Storage Lca > Critical Minerals", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/storage-lca/dashboard")
def storage_lca_dashboard():
    result = _build_battery_response("Storage Lca > Dashboard", 6419)
    if result:
        return result
    _r.seed(6419)
    return {
        "summary": {"title": "Storage Lca > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/storage-lca/lca-records")
def storage_lca_lca_records():
    result = _build_battery_response("Storage Lca > Lca Records", 6420)
    if result and "records" in result:
        return result["records"]
    _r.seed(6420)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Storage Lca > Lca Records", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/storage-lca/recycling")
def storage_lca_recycling():
    result = _build_battery_response("Storage Lca > Recycling", 6421)
    if result and "records" in result:
        return result["records"]
    _r.seed(6421)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Storage Lca > Recycling", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/storage-lca/scenarios")
def storage_lca_scenarios():
    result = _build_battery_response("Storage Lca > Scenarios", 6422)
    if result and "records" in result:
        return result["records"]
    _r.seed(6422)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Storage Lca > Scenarios", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/storage-optimisation/dashboard")
def storage_optimisation_dashboard():
    result = _build_battery_response("Storage Optimisation > Dashboard", 6423)
    if result:
        return result
    _r.seed(6423)
    return {
        "summary": {"title": "Storage Optimisation > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/storage-revenue-stack/dashboard")
def storage_revenue_stack_dashboard():
    result = _build_battery_response("Storage Revenue Stack > Dashboard", 6424)
    if result:
        return result
    _r.seed(6424)
    return {
        "summary": {"title": "Storage Revenue Stack > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/storage-revenue-stack/dispatch-optimisation")
def storage_revenue_stack_dispatch_optimisation():
    result = _build_battery_response("Storage Revenue Stack > Dispatch Optimisation", 6425)
    if result and "records" in result:
        return result["records"]
    _r.seed(6425)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Storage Revenue Stack > Dispatch Optimisation", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/storage-revenue-stack/multi-service-bids")
def storage_revenue_stack_multi_service_bids():
    result = _build_battery_response("Storage Revenue Stack > Multi Service Bids", 6426)
    if result and "records" in result:
        return result["records"]
    _r.seed(6426)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Storage Revenue Stack > Multi Service Bids", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/storage-revenue-stack/scenarios")
def storage_revenue_stack_scenarios():
    result = _build_battery_response("Storage Revenue Stack > Scenarios", 6427)
    if result and "records" in result:
        return result["records"]
    _r.seed(6427)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Storage Revenue Stack > Scenarios", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/storage-revenue-stack/waterfall")
def storage_revenue_stack_waterfall():
    result = _build_battery_response("Storage Revenue Stack > Waterfall", 6428)
    if result and "records" in result:
        return result["records"]
    _r.seed(6428)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Storage Revenue Stack > Waterfall", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/storage-revenue/dashboard")
def storage_revenue_dashboard():
    result = _build_battery_response("Storage Revenue > Dashboard", 6429)
    if result:
        return result
    _r.seed(6429)
    return {
        "summary": {"title": "Storage Revenue > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/storage-roadmap/dashboard")
def storage_roadmap_dashboard():
    result = _build_battery_response("Storage Roadmap > Dashboard", 6430)
    if result:
        return result
    _r.seed(6430)
    return {
        "summary": {"title": "Storage Roadmap > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/system-load-balancing/dashboard")
def system_load_balancing_dashboard():
    _r.seed(6431)
    return {
        "summary": {"title": "System Load Balancing > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/system-operator/constraint-relaxations")
def system_operator_constraint_relaxations():
    rows = _get_cached('so_constraints', lambda: _query_gold(f"""
        SELECT notice_id, notice_type, region, issue_date, description
        FROM {_CATALOG}.gold.nem_market_notices
        WHERE LOWER(description) LIKE '%constraint%' OR LOWER(notice_type) LIKE '%constraint%'
        ORDER BY issue_date DESC LIMIT 20
    """))
    if rows:
        return [{"id": i+1, "notice_id": r.get("notice_id"), "region": r.get("region","NEM"),
                 "description": (r.get("description") or "")[:200], "issue_date": r.get("issue_date"),
                 "type": "constraint_relaxation"} for i, r in enumerate(rows)]
    _r.seed(6432)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "System Operator > Constraint Relaxations", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/system-operator/directions")
def system_operator_directions():
    rows = _get_cached('so_directions', lambda: _query_gold(f"""
        SELECT notice_id, notice_type, region, issue_date, description
        FROM {_CATALOG}.gold.nem_market_notices
        WHERE LOWER(description) LIKE '%direction%' OR LOWER(notice_type) LIKE '%direction%'
        ORDER BY issue_date DESC LIMIT 20
    """))
    if rows:
        return [{"id": i+1, "notice_id": r.get("notice_id"), "region": r.get("region","NEM"),
                 "description": (r.get("description") or "")[:200], "issue_date": r.get("issue_date"),
                 "type": "direction"} for i, r in enumerate(rows)]
    _r.seed(6433)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "System Operator > Directions", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/system-operator/load-shedding")
def system_operator_load_shedding():
    rows = _get_cached('so_loadshed', lambda: _query_gold(f"""
        SELECT notice_id, notice_type, region, issue_date, description
        FROM {_CATALOG}.gold.nem_market_notices
        WHERE LOWER(description) LIKE '%load shed%' OR LOWER(description) LIKE '%lack of reserve%'
           OR LOWER(notice_type) LIKE '%LOR%'
        ORDER BY issue_date DESC LIMIT 20
    """))
    if rows:
        return [{"id": i+1, "notice_id": r.get("notice_id"), "region": r.get("region","NEM"),
                 "description": (r.get("description") or "")[:200], "issue_date": r.get("issue_date"),
                 "type": "load_shedding"} for i, r in enumerate(rows)]
    _r.seed(6434)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "System Operator > Load Shedding", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/system-operator/rert-activations")
def system_operator_rert_activations():
    rows = _get_cached('so_rert', lambda: _query_gold(f"""
        SELECT notice_id, notice_type, region, issue_date, description
        FROM {_CATALOG}.gold.nem_market_notices
        WHERE LOWER(description) LIKE '%rert%' OR LOWER(description) LIKE '%reliability%reserve%'
        ORDER BY issue_date DESC LIMIT 20
    """))
    if rows:
        return [{"id": i+1, "notice_id": r.get("notice_id"), "region": r.get("region","NEM"),
                 "description": (r.get("description") or "")[:200], "issue_date": r.get("issue_date"),
                 "type": "rert_activation"} for i, r in enumerate(rows)]
    _r.seed(6435)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "System Operator > Rert Activations", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/tariff-cross-subsidy/dashboard")
def tariff_cross_subsidy_dashboard():
    result = _build_tariff_response("Tariff Cross Subsidy > Dashboard", 6436)
    if result:
        return result
    _r.seed(6436)
    return {
        "summary": {"title": "Tariff Cross Subsidy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/tariff-reform/dashboard")
def tariff_reform_dashboard():
    result = _build_tariff_response("Tariff Reform > Dashboard", 6437)
    if result:
        return result
    _r.seed(6437)
    return {
        "summary": {"title": "Tariff Reform > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/thermal-coal-power-transition/dashboard")
def thermal_coal_power_transition_dashboard():
    result = _build_generation_response("Thermal Coal Power Transition > Dashboard", 6438)
    if result:
        return result
    _r.seed(6438)
    return {
        "summary": {"title": "Thermal Coal Power Transition > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/thermal-efficiency/benchmarks")
def thermal_efficiency_benchmarks():
    result = _build_generation_response("Thermal Efficiency > Benchmarks", 6439)
    if result and "records" in result:
        return result["records"]
    _r.seed(6439)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Thermal Efficiency > Benchmarks", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/thermal-efficiency/dashboard")
def thermal_efficiency_dashboard():
    result = _build_generation_response("Thermal Efficiency > Dashboard", 6440)
    if result:
        return result
    _r.seed(6440)
    return {
        "summary": {"title": "Thermal Efficiency > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/thermal-efficiency/fuel-costs")
def thermal_efficiency_fuel_costs():
    result = _build_generation_response("Thermal Efficiency > Fuel Costs", 6441)
    if result and "records" in result:
        return result["records"]
    _r.seed(6441)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Thermal Efficiency > Fuel Costs", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/thermal-efficiency/heat-rate-trends")
def thermal_efficiency_heat_rate_trends():
    result = _build_generation_response("Thermal Efficiency > Heat Rate Trends", 6442)
    if result and "records" in result:
        return result["records"]
    _r.seed(6442)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Thermal Efficiency > Heat Rate Trends", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/thermal-efficiency/units")
def thermal_efficiency_units():
    result = _build_generation_response("Thermal Efficiency > Units", 6443)
    if result and "records" in result:
        return result["records"]
    _r.seed(6443)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Thermal Efficiency > Units", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/tidal-wave-marine-energy/dashboard")
def tidal_wave_marine_energy_dashboard():
    _r.seed(6444)
    return {
        "summary": {"title": "Tidal Wave Marine Energy > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/tnsp-analytics/dashboard")
def tnsp_analytics_dashboard():
    _r.seed(6445)
    return {
        "summary": {"title": "Tnsp Analytics > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/tnsp/dashboard")
def tnsp_dashboard():
    rows = _tnsp_perf_data()
    if rows:
        total_km = sum(float(r.get("circuit_km") or 0) for r in rows if r.get("year") == 2024)
        total_gwh = sum(float(r.get("energy_delivered_gwh") or 0) for r in rows if r.get("year") == 2024)
        return {
            "summary": {"title": "TNSP Performance", "tnsps": len(set(r.get("tnsp") for r in rows)),
                        "total_circuit_km": round(total_km, 0), "total_energy_gwh": round(total_gwh, 0),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"tnsp": r.get("tnsp"), "state": r.get("state"), "year": r.get("year"),
                         "circuit_km": float(r.get("circuit_km") or 0),
                         "energy_gwh": float(r.get("energy_delivered_gwh") or 0),
                         "peak_mw": float(r.get("peak_demand_mw") or 0),
                         "loss_pct": float(r.get("loss_factor_pct") or 0)} for r in rows]}
    _r.seed(6446)
    return {
        "summary": {"title": "Tnsp > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/transmission-access-reform/dashboard")
def transmission_access_reform_dashboard():
    result = _build_grid_response("Transmission Access Reform > Dashboard", 6447)
    if result:
        return result
    _r.seed(6447)
    return {
        "summary": {"title": "Transmission Access Reform > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/transmission-congestion/congestion-rent")
def transmission_congestion_congestion_rent():
    result = _build_grid_response("Transmission Congestion > Congestion Rent", 6448)
    if result and "records" in result:
        return result["records"]
    _r.seed(6448)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Transmission Congestion > Congestion Rent", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/transmission-congestion/constraints")
def transmission_congestion_constraints():
    result = _build_grid_response("Transmission Congestion > Constraints", 6449)
    if result and "records" in result:
        return result["records"]
    _r.seed(6449)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Transmission Congestion > Constraints", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/transmission-congestion/dashboard")
def transmission_congestion_dashboard():
    result = _build_grid_response("Transmission Congestion > Dashboard", 6450)
    if result:
        return result
    _r.seed(6450)
    return {
        "summary": {"title": "Transmission Congestion > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/transmission-congestion/heatmap")
def transmission_congestion_heatmap():
    result = _build_grid_response("Transmission Congestion > Heatmap", 6451)
    if result and "records" in result:
        return result["records"]
    _r.seed(6451)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Transmission Congestion > Heatmap", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/transmission-congestion/nodal-prices")
def transmission_congestion_nodal_prices():
    result = _build_pricing_response("Transmission Congestion > Nodal Prices", 6452)
    if result and "records" in result:
        return result["records"]
    _r.seed(6452)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Transmission Congestion > Nodal Prices", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/transmission-investment/dashboard")
def transmission_investment_dashboard():
    result = _build_grid_response("Transmission Investment > Dashboard", 6453)
    if result:
        return result
    _r.seed(6453)
    return {
        "summary": {"title": "Transmission Investment > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/transmission/dashboard")
def transmission_dashboard():
    result = _build_grid_response("Transmission > Dashboard", 6454)
    if result:
        return result
    _r.seed(6454)
    return {
        "summary": {"title": "Transmission > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/tuos/dashboard")
def tuos_dashboard():
    rows = _tuos_data()
    if rows:
        total_rev = sum(float(r.get("total_tuos_revenue_m_aud") or 0) for r in rows)
        return {
            "summary": {"title": "TUoS Charges", "tnsps": len(rows),
                        "total_revenue_b_aud": round(total_rev / 1000, 2),
                        "last_updated": _dt.utcnow().isoformat()},
            "records": [{"tnsp": r.get("tnsp"), "state": r.get("state"),
                         "financial_year": r.get("financial_year"),
                         "locational_aud_kw": float(r.get("locational_aud_per_kw") or 0),
                         "non_locational_aud_mwh": float(r.get("non_locational_aud_per_mwh") or 0),
                         "common_service_aud_mwh": float(r.get("common_service_aud_per_mwh") or 0),
                         "total_revenue_m": float(r.get("total_tuos_revenue_m_aud") or 0)} for r in rows]}
    _r.seed(6455)
    return {
        "summary": {"title": "Tuos > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/utility-solar-farm-operations/dashboard")
def utility_solar_farm_operations_dashboard():
    result = _build_renewable_response("Utility Solar Farm Operations > Dashboard", 6456)
    if result:
        return result
    _r.seed(6456)
    return {
        "summary": {"title": "Utility Solar Farm Operations > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/wave-tidal-ocean/dashboard")
def wave_tidal_ocean_dashboard():
    _r.seed(6457)
    return {
        "summary": {"title": "Wave Tidal Ocean > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

# /api/wem/dashboard removed — served by wem.py router with real WEM data

@router.get("/api/wem/prices")
def wem_prices():
    result = _build_pricing_response("Wem > Prices", 6459)
    if result and "records" in result:
        return result["records"]
    _r.seed(6459)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Wem > Prices", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/wholesale-gas-market/dashboard")
def wholesale_gas_market_dashboard():
    result = _build_pricing_response("Wholesale Gas Market > Dashboard", 6460)
    if result:
        return result
    _r.seed(6460)
    return {
        "summary": {"title": "Wholesale Gas Market > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/wholesale-liquidity/dashboard")
def wholesale_liquidity_dashboard():
    result = _build_pricing_response("Wholesale Liquidity > Dashboard", 6461)
    if result:
        return result
    _r.seed(6461)
    return {
        "summary": {"title": "Wholesale Liquidity > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/wholesale-market-liquidity/dashboard")
def wholesale_market_liquidity_dashboard():
    result = _build_pricing_response("Wholesale Market Liquidity > Dashboard", 6462)
    if result:
        return result
    _r.seed(6462)
    return {
        "summary": {"title": "Wholesale Market Liquidity > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/wind-capacity-market/dashboard")
def wind_capacity_market_dashboard():
    result = _build_renewable_response("Wind Capacity Market > Dashboard", 6463)
    if result:
        return result
    _r.seed(6463)
    return {
        "summary": {"title": "Wind Capacity Market > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/wind-farm-wake-effect-x/dashboard")
def wind_farm_wake_effect_x_dashboard():
    result = _build_renewable_response("Wind Farm Wake Effect X > Dashboard", 6464)
    if result:
        return result
    _r.seed(6464)
    return {
        "summary": {"title": "Wind Farm Wake Effect X > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/wind-farm-wake-turbine/dashboard")
def wind_farm_wake_turbine_dashboard():
    result = _build_renewable_response("Wind Farm Wake Turbine > Dashboard", 6465)
    if result:
        return result
    _r.seed(6465)
    return {
        "summary": {"title": "Wind Farm Wake Turbine > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/wind-farm-wake/dashboard")
def wind_farm_wake_dashboard():
    result = _build_renewable_response("Wind Farm Wake > Dashboard", 6466)
    if result:
        return result
    _r.seed(6466)
    return {
        "summary": {"title": "Wind Farm Wake > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/wind-resource-variability/dashboard")
def wind_resource_variability_dashboard():
    result = _build_renewable_response("Wind Resource Variability > Dashboard", 6467)
    if result:
        return result
    _r.seed(6467)
    return {
        "summary": {"title": "Wind Resource Variability > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/wind-resource/dashboard")
def wind_resource_dashboard():
    result = _build_renewable_response("Wind Resource > Dashboard", 6468)
    if result:
        return result
    _r.seed(6468)
    return {
        "summary": {"title": "Wind Resource > Dashboard", "status": "operational",
                    "last_updated": _dt.utcnow().isoformat(),
                    "data_points": _r.randint(50, 500), "trend": _r.choice(["up", "down", "stable"])},
        "records": [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
                      "value": round(_r.uniform(10, 500), 2), "timestamp": _dt.utcnow().isoformat()}
                     for j in range(10)]}

@router.get("/api/wind-resource/farm-performance")
def wind_resource_farm_performance():
    result = _build_renewable_response("Wind Resource > Farm Performance", 6469)
    if result and "records" in result:
        return result["records"]
    _r.seed(6469)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Wind Resource > Farm Performance", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/wind-resource/monthly-resource")
def wind_resource_monthly_resource():
    result = _build_renewable_response("Wind Resource > Monthly Resource", 6470)
    if result and "records" in result:
        return result["records"]
    _r.seed(6470)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Wind Resource > Monthly Resource", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/wind-resource/site-assessments")
def wind_resource_site_assessments():
    result = _build_renewable_response("Wind Resource > Site Assessments", 6471)
    if result and "records" in result:
        return result["records"]
    _r.seed(6471)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Wind Resource > Site Assessments", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]

@router.get("/api/wind-resource/wake-losses")
def wind_resource_wake_losses():
    result = _build_renewable_response("Wind Resource > Wake Losses", 6472)
    if result and "records" in result:
        return result["records"]
    _r.seed(6472)
    return [{"id": j + 1, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]),
              "label": "Wind Resource > Wake Losses", "value": round(_r.uniform(10, 500), 2),
              "timestamp": _dt.utcnow().isoformat()} for j in range(10)]
