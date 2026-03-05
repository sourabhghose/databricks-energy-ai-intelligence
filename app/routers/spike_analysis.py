from __future__ import annotations
import random as _r
from datetime import datetime as _dt, timedelta as _td, timezone as _tz
from typing import Optional
from fastapi import APIRouter, Query
from .shared import _query_gold, _CATALOG, _NEM_REGIONS, logger

router = APIRouter()


# ---------------------------------------------------------------------------
# 1) /api/spike-analysis/dashboard  ->  PSADashboard
# ---------------------------------------------------------------------------

@router.get("/api/spike-analysis/dashboard")
def spike_analysis_dashboard():
    root_causes = [
        "GENERATION_SHORTFALL", "NETWORK_CONSTRAINT", "DEMAND_SPIKE",
        "STRATEGIC_BIDDING", "WEATHER",
    ]
    severities = ["MODERATE", "HIGH", "EXTREME", "MARKET_SUSPENSION"]
    contribution_types = [
        "WITHDREW_CAPACITY", "REBID_HIGH", "FCAS_RESPONSE",
        "DEMAND_REDUCTION", "CONSTRAINT_BINDING",
    ]
    regulatory_actions = ["INVESTIGATED", "CAUTIONED", "FINED", "CLEARED", None]
    technologies = ["Gas Peaker", "Coal", "Battery", "Hydro", "Wind"]
    consumer_segments = ["Residential", "Commercial", "Industrial", "Large User"]
    participants = [
        "AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro",
        "CS Energy", "Stanwell", "Alinta Energy", "Engie",
    ]

    # Try real spike data from prices
    spike_rows = _query_gold(f"""
        SELECT region_id, rrp, interval_datetime, total_demand_mw
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE rrp > 300
        ORDER BY rrp DESC
        LIMIT 30
    """)

    spike_events = []
    rng = _r.Random(101)

    if spike_rows and len(spike_rows) > 0:
        # Group consecutive spikes by region+date into events
        seen_events = {}
        for sr in spike_rows:
            region = sr["region_id"]
            ts = str(sr["interval_datetime"])
            date_str = ts[:10]
            key = f"{region}-{date_str}"
            price = float(sr["rrp"])
            if key not in seen_events:
                seen_events[key] = {
                    "region": region, "date": date_str, "peak": price,
                    "start": ts, "end": ts, "count": 1,
                }
            else:
                if price > seen_events[key]["peak"]:
                    seen_events[key]["peak"] = price
                seen_events[key]["count"] += 1
                seen_events[key]["end"] = ts

        for i, (key, ev) in enumerate(sorted(seen_events.items(), key=lambda x: -x[1]["peak"])):
            peak = ev["peak"]
            dur = ev["count"] * 5
            if peak > 5000:
                sev = "EXTREME"
            elif peak > 1000:
                sev = "HIGH"
            else:
                sev = "MODERATE"
            pre_avg = rng.uniform(40, 120)
            spike_events.append({
                "spike_id": f"SPK-{ev['date'][:4]}-{i + 1:03d}",
                "event_name": f"{ev['region']} Price Spike {ev['date']}",
                "region": ev["region"],
                "event_date": ev["date"],
                "start_time": ev["start"][11:16] if len(ev["start"]) > 11 else "12:00",
                "end_time": ev["end"][11:16] if len(ev["end"]) > 11 else "13:00",
                "duration_minutes": dur,
                "peak_price_aud_mwh": round(peak, 0),
                "avg_price_during_spike": round(peak * rng.uniform(0.35, 0.65), 2),
                "pre_spike_avg_price": round(pre_avg, 2),
                "price_multiple": round(peak / pre_avg, 1),
                "total_revenue_m_aud": round(peak * dur / 60 / 1000 * rng.uniform(0.5, 2), 2),
                "consumer_cost_m_aud": round(peak * dur / 60 / 1000 * rng.uniform(1, 3), 1),
                "hedged_consumer_cost_m_aud": round(peak * dur / 60 / 1000 * rng.uniform(0.1, 0.5), 2),
                "root_cause": rng.choice(root_causes),
                "severity": sev,
            })

    if not spike_events:
        # Fallback to mock
        _r.seed(101)
        regions = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]
        event_names = [
            "SA Heatwave Jan 2024", "QLD Cyclone Grid Stress", "VIC Evening Ramp Failure",
            "NSW Coal Trip Aug 2023", "TAS Low Hydro Event", "SA Interconnector Trip",
            "QLD Summer Demand Peak", "NSW Strategic Rebid Event",
            "VIC Wind Drought Feb 2024", "SA Solar Cliff Event",
            "QLD Generator Outage Mar 2024", "NSW Bushfire Network Fault",
        ]
        for i, name in enumerate(event_names):
            region = regions[i % len(regions)]
            base_date = _dt(2022, 6, 1) + _td(days=_r.randint(0, 900))
            peak = _r.choice([5000, 8500, 12000, 15100, 16600])
            dur = _r.choice([30, 60, 90, 120, 180, 240, 360])
            pre_avg = _r.uniform(40, 120)
            spike_events.append({
                "spike_id": f"SPK-{2022 + i // 4}-{i + 1:03d}",
                "event_name": name,
                "region": region,
                "event_date": base_date.strftime("%Y-%m-%d"),
                "start_time": f"{_r.randint(6, 20):02d}:{_r.choice(['00', '30'])}",
                "end_time": f"{_r.randint(7, 23):02d}:{_r.choice(['00', '30'])}",
                "duration_minutes": dur,
                "peak_price_aud_mwh": peak,
                "avg_price_during_spike": round(peak * _r.uniform(0.35, 0.65), 2),
                "pre_spike_avg_price": round(pre_avg, 2),
                "price_multiple": round(peak / pre_avg, 1),
                "total_revenue_m_aud": round(_r.uniform(1.5, 45.0), 2),
                "consumer_cost_m_aud": round(_r.uniform(2.0, 55.0), 1),
                "hedged_consumer_cost_m_aud": round(_r.uniform(0.5, 15.0), 2),
                "root_cause": _r.choice(root_causes),
                "severity": _r.choice(severities),
            })

    contributors = []
    for ev in spike_events:
        for _ in range(rng.randint(2, 5)):
            contributors.append({
                "spike_id": ev["spike_id"],
                "participant_name": rng.choice(participants),
                "technology": rng.choice(technologies),
                "contribution_type": rng.choice(contribution_types),
                "mw_impact": round(rng.uniform(-500, 800), 0),
                "price_contribution_aud_mwh": round(rng.uniform(200, 8000), 0),
                "revenue_gained_m_aud": round(rng.uniform(0.2, 12.0), 1),
                "regulatory_action": rng.choice(regulatory_actions),
            })

    consumer_impacts = []
    for ev in spike_events:
        for seg in consumer_segments:
            consumer_impacts.append({
                "spike_id": ev["spike_id"],
                "consumer_segment": seg,
                "region": ev["region"],
                "hedged_exposure_pct": round(rng.uniform(30, 95), 1),
                "unhedged_cost_m_aud": round(rng.uniform(0.1, 18.0), 1),
                "demand_response_mw": round(rng.uniform(5, 350), 0),
                "air_con_curtailment_mw": round(rng.uniform(0, 180), 0),
                "price_signal_response_pct": round(rng.uniform(2, 25), 1),
            })

    regional_timelines = []
    for ev in spike_events[:4]:
        for minute_offset in range(0, 360, 30):
            regional_timelines.append({
                "spike_id": ev["spike_id"],
                "region": ev["region"],
                "interval": (
                    _dt.strptime(ev["event_date"], "%Y-%m-%d")
                    + _td(minutes=minute_offset)
                ).isoformat(),
                "spot_price": round(rng.uniform(-50, ev["peak_price_aud_mwh"]), 2),
                "generation_mw": round(rng.uniform(6000, 12000), 0),
                "demand_mw": round(rng.uniform(7000, 13000), 0),
                "interconnector_flow_mw": round(rng.uniform(-1000, 1000), 0),
                "reserve_margin_pct": round(rng.uniform(-5, 25), 1),
            })

    # Find most affected region
    region_counts = {}
    for ev in spike_events:
        region_counts[ev["region"]] = region_counts.get(ev["region"], 0) + 1
    most_affected = max(region_counts, key=region_counts.get) if region_counts else "SA1"

    return {
        "timestamp": _dt.now(_tz.utc).isoformat(),
        "spike_events": spike_events,
        "contributors": contributors,
        "consumer_impacts": consumer_impacts,
        "regional_timelines": regional_timelines,
        "total_spike_events_2024": sum(1 for e in spike_events if "2024" in e.get("event_date", "")),
        "total_consumer_cost_m_aud": round(sum(e["consumer_cost_m_aud"] for e in spike_events), 1),
        "avg_spike_duration_min": round(
            sum(e["duration_minutes"] for e in spike_events) / len(spike_events), 0
        ) if spike_events else 0,
        "most_affected_region": most_affected,
    }


# ---------------------------------------------------------------------------
# 2) /api/spot-price-forecast/dashboard  ->  SPFDashboard
# ---------------------------------------------------------------------------

@router.get("/api/spot-price-forecast/dashboard")
def spot_price_forecast_dashboard():
    _r.seed(202)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    model_types = ["XGBOOST", "LSTM", "PROPHET", "ENSEMBLE", "LINEAR"]
    statuses = ["PRODUCTION", "SHADOW", "DEPRECATED"]
    regimes = ["NORMAL", "SPIKE", "NEGATIVE", "EXTREME"]
    categories = ["DEMAND", "SUPPLY", "WEATHER", "MARKET", "TIME"]
    horizons = [5, 30, 60, 288]

    models = []
    model_defs = [
        ("M001", "XGBoost-NSW-5min", "XGBOOST", "NSW1", 5, "PRODUCTION"),
        ("M002", "LSTM-VIC-30min", "LSTM", "VIC1", 30, "DEPRECATED"),
        ("M003", "Ensemble-NEM-1hr", "ENSEMBLE", "QLD1", 60, "PRODUCTION"),
        ("M004", "Prophet-SA-DayAhead", "PROPHET", "SA1", 288, "SHADOW"),
        ("M005", "XGBoost-QLD-30min", "XGBOOST", "QLD1", 30, "PRODUCTION"),
        ("M006", "LSTM-SA-Spike", "LSTM", "SA1", 5, "SHADOW"),
    ]
    for mid, mname, mtype, region, horizon, status in model_defs:
        models.append({
            "model_id": mid,
            "model_name": mname,
            "model_type": mtype,
            "region": region,
            "horizon_min": horizon,
            "mae_per_mwh": round(_r.uniform(3.5, 18.0), 1),
            "rmse_per_mwh": round(_r.uniform(8.0, 35.0), 1),
            "mape_pct": round(_r.uniform(4.0, 22.0), 1),
            "r2_score": round(_r.uniform(0.72, 0.96), 2),
            "spike_detection_recall_pct": _r.randint(45, 92),
            "negative_price_recall_pct": _r.randint(38, 85),
            "training_period": "2020-01 to 2024-06",
            "deployment_status": status,
        })

    dates = [
        (_dt(2024, 7, 1) + _td(days=d)).strftime("%Y-%m-%d")
        for d in range(0, 14)
    ]
    intervals = [f"{h:02d}:{m:02d}" for h in range(0, 24) for m in [0, 30]]

    forecasts = []
    for date in dates[:3]:
        for region in regions[:3]:
            for interval in intervals[:6]:
                actual = round(_r.uniform(-20, 350), 2)
                fc = actual + _r.uniform(-15, 15)
                forecasts.append({
                    "date": date,
                    "region": region,
                    "trading_interval": interval,
                    "actual_price": round(actual, 2),
                    "forecast_price": round(fc, 2),
                    "forecast_low": round(fc - _r.uniform(5, 25), 2),
                    "forecast_high": round(fc + _r.uniform(5, 25), 2),
                    "error_per_mwh": round(abs(actual - fc), 2),
                    "model_used": _r.choice(["M001", "M003", "M005"]),
                    "price_regime": _r.choice(regimes),
                })

    features = []
    feature_names = [
        "demand_forecast_mw", "temperature_forecast_c", "solar_generation_mw",
        "wind_generation_mw", "gas_price_gj", "interconnector_flow_mw",
        "time_of_day", "day_of_week", "coal_gen_mw", "battery_soc_pct",
        "lagged_price_5min", "demand_ramp_rate",
    ]
    for mtype in model_types[:4]:
        for rank_i, fname in enumerate(feature_names[:10], 1):
            cat = _r.choice(categories)
            features.append({
                "feature_name": fname,
                "model_type": mtype,
                "region": _r.choice(regions),
                "importance_score": round(_r.uniform(0.01, 0.22), 4),
                "rank": rank_i,
                "category": cat,
            })

    drift = []
    for mid in ["M001", "M003", "M004"]:
        baseline_mae = round(_r.uniform(5.0, 10.0), 2)
        for d_off in range(14):
            date_str = (_dt(2024, 6, 1) + _td(days=d_off)).strftime("%Y-%m-%d")
            rolling = round(baseline_mae + _r.uniform(-3, 6), 2)
            drift_score = round(max(0, min(1, (rolling - baseline_mae) / baseline_mae)), 3)
            drift.append({
                "model_id": mid,
                "date": date_str,
                "mae_rolling_7d": rolling,
                "mae_baseline": baseline_mae,
                "drift_score": drift_score,
                "drift_alert": drift_score > 0.7,
                "regime_shift": _r.choice(["NONE", "SPIKE_ENTRY", "NEGATIVE_ENTRY", "NONE"]),
            })

    prod_models = [m for m in models if m["deployment_status"] == "PRODUCTION"]
    best_mae = min(models, key=lambda m: m["mae_per_mwh"])
    best_spike = max(models, key=lambda m: m["spike_detection_recall_pct"])

    return {
        "models": models,
        "forecasts": forecasts,
        "features": features,
        "drift": drift,
        "summary": {
            "production_models": len(prod_models),
            "best_mae_model": best_mae["model_name"],
            "best_spike_recall_pct": best_spike["spike_detection_recall_pct"],
            "avg_mape_pct": round(
                sum(m["mape_pct"] for m in prod_models) / max(len(prod_models), 1), 1
            ),
            "total_forecasts": len(forecasts),
        },
    }


# ---------------------------------------------------------------------------
# 3) /api/spot-market-stress/dashboard  ->  SSTDashboard
# ---------------------------------------------------------------------------

@router.get("/api/spot-market-stress/dashboard")
def spot_market_stress_dashboard():
    _r.seed(303)
    regions = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]
    sev_list = ["EXTREME", "SEVERE", "MODERATE", "MILD"]
    cat_list = ["WEATHER", "SUPPLY", "DEMAND", "FUEL", "NETWORK", "POLICY"]
    status_list = ["ADEQUATE", "MARGINAL", "STRESSED"]
    trend_list = ["IMPROVING", "STABLE", "DETERIORATING"]
    metrics_list = [
        "PRICE_SPIKE_RECOVERY_HRS", "SUPPLY_ADEQUACY_MARGIN_PCT",
        "INTERCONNECTOR_REDUNDANCY_PCT", "FCAS_HEADROOM_MW",
        "RAMP_CAPABILITY_MW_MIN",
    ]
    tail_metrics = ["VAR_95", "VAR_99", "CVAR_95", "CVAR_99", "MAX_DRAWDOWN", "STRESS_VaR"]
    sensitivity_factors = [
        "GAS_PRICE_SHOCK", "COAL_PLANT_CLOSURE", "RENEWABLE_INTERMITTENCY",
        "DEMAND_HEATWAVE", "INTERCONNECTOR_FAILURE", "POLICY_CHANGE",
    ]

    scenario_names = [
        "EXTREME_HEATWAVE", "COAL_FLEET_FAILURE", "GAS_SUPPLY_DISRUPTION",
        "RENEWABLE_DROUGHT", "INTERCONNECTOR_LOSS", "DEMAND_SURGE_CYCLONE",
        "BUSHFIRE_GRID_DAMAGE", "COORDINATED_CYBER_ATTACK",
    ]
    scenarios = []
    for i, sname in enumerate(scenario_names):
        scenarios.append({
            "scenario_id": f"SST-{i + 1:03d}",
            "scenario_name": sname,
            "category": _r.choice(cat_list),
            "description": f"Stress scenario modelling {sname.replace('_', ' ').lower()} conditions across NEM regions with cascading impacts.",
            "probability_annual_pct": round(_r.uniform(0.5, 15.0), 1),
            "severity": _r.choice(sev_list),
            "affected_regions": _r.sample(regions, k=_r.randint(2, 5)),
            "duration_days": _r.randint(1, 21),
            "peak_price_impact": _r.choice([5000, 10000, 15100, 16600]),
            "avg_price_impact_pct": round(_r.uniform(50, 800), 0),
            "energy_cost_impact_m": round(_r.uniform(50, 2500), 0),
        })

    tail_risks = []
    for region in regions:
        for metric in tail_metrics:
            val = round(_r.uniform(150, 2500), 1)
            tail_risks.append({
                "region": region,
                "metric": metric,
                "lookback_years": 10,
                "value": val,
                "percentile_pct": round(_r.uniform(90, 99.9), 1),
                "return_period_years": round(_r.uniform(2, 50), 0),
                "historical_worst": round(val * _r.uniform(1.1, 1.8), 1),
                "stress_test_worst": round(val * _r.uniform(1.5, 3.0), 1),
            })

    resilience = []
    for region in regions:
        for metric in metrics_list:
            curr = round(_r.uniform(1, 100), 1)
            resilience.append({
                "region": region,
                "metric": metric,
                "current_value": curr,
                "adequate_threshold": round(curr * 1.2, 1),
                "stress_threshold": round(curr * 0.5, 1),
                "status": _r.choice(status_list),
                "trend": _r.choice(trend_list),
            })

    historical_events = [
        {
            "event_name": "SA System Black 2016",
            "date": "2016-09-28",
            "region": "SA1",
            "category": "WEATHER",
            "peak_price": 14000,
            "avg_price_during": 5200,
            "duration_hrs": 18,
            "total_cost_m": 367,
            "load_shed_mwh": 52400,
            "market_intervention": True,
            "lesson_learned": "Exposed SA reliance on single interconnector (Heywood). Led to Hornsdale battery investment and system strength requirements.",
        },
        {
            "event_name": "QLD-NSW Heatwave Jan 2019",
            "date": "2019-01-24",
            "region": "QLD1",
            "category": "DEMAND",
            "peak_price": 14500,
            "avg_price_during": 3800,
            "duration_hrs": 8,
            "total_cost_m": 180,
            "load_shed_mwh": 12000,
            "market_intervention": False,
            "lesson_learned": "Record demand combined with low wind output. Battery response improved after Hornsdale upgrade.",
        },
        {
            "event_name": "Energy Crisis Jun 2022",
            "date": "2022-06-15",
            "region": "NSW1",
            "category": "FUEL",
            "peak_price": 15100,
            "avg_price_during": 6800,
            "duration_hrs": 168,
            "total_cost_m": 2100,
            "load_shed_mwh": 0,
            "market_intervention": True,
            "lesson_learned": "Coal plant outages combined with gas shortages led to AEMO market suspension. Resulted in Capacity Investment Scheme.",
        },
        {
            "event_name": "VIC Storm Damage Dec 2023",
            "date": "2023-12-08",
            "region": "VIC1",
            "category": "WEATHER",
            "peak_price": 8500,
            "avg_price_during": 2200,
            "duration_hrs": 12,
            "total_cost_m": 95,
            "load_shed_mwh": 8200,
            "market_intervention": False,
            "lesson_learned": "Transmission line damage from severe storms caused localized load shedding. Highlighted need for undergrounding in fire-prone areas.",
        },
        {
            "event_name": "TAS Low Hydro 2024",
            "date": "2024-03-15",
            "region": "TAS1",
            "category": "SUPPLY",
            "peak_price": 4200,
            "avg_price_during": 1100,
            "duration_hrs": 48,
            "total_cost_m": 45,
            "load_shed_mwh": 0,
            "market_intervention": False,
            "lesson_learned": "Basslink import constraints during low hydro storage period. Demonstrates ongoing TAS dependency on interconnector availability.",
        },
    ]

    sensitivity = []
    for factor in sensitivity_factors:
        for region in regions:
            sensitivity.append({
                "factor": factor,
                "region": region,
                "magnitude": _r.choice(["LOW", "MEDIUM", "HIGH"]),
                "price_response": round(_r.uniform(10, 800), 1),
                "probability_annual_pct": round(_r.uniform(1, 30), 1),
                "risk_contribution_pct": round(_r.uniform(0.5, 25), 2),
            })

    nsw1_var99 = [t for t in tail_risks if t["region"] == "NSW1" and t["metric"] == "VAR_99"]
    avg_var_99_nsw1 = round(nsw1_var99[0]["value"], 1) if nsw1_var99 else 450.0
    adequate_count = sum(1 for r in resilience if r["status"] == "ADEQUATE")
    stressed_count = sum(1 for r in resilience if r["status"] == "STRESSED")

    return {
        "scenarios": scenarios,
        "tail_risks": tail_risks,
        "resilience": resilience,
        "historical_events": historical_events,
        "sensitivity": sensitivity,
        "summary": {
            "scenarios_count": len(scenarios),
            "highest_risk_scenario": "EXTREME_HEATWAVE",
            "avg_var_99_nsw1": avg_var_99_nsw1,
            "stressed_regions_count": stressed_count,
            "resilience_adequate_pct": round(adequate_count / max(len(resilience), 1) * 100, 1),
            "historical_events_analyzed": len(historical_events),
            "total_historical_cost_m": sum(e["total_cost_m"] for e in historical_events),
        },
    }


# ---------------------------------------------------------------------------
# 4) /api/spot-price-volatility-regime/dashboard  ->  SVRDashboard
# ---------------------------------------------------------------------------

@router.get("/api/spot-price-volatility-regime/dashboard")
def spot_price_volatility_regime_dashboard():
    _r.seed(404)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    regime_types = ["LOW_VOL", "NORMAL", "HIGH_VOL", "EXTREME"]
    drivers_list = [
        "WIND_OUTPUT", "SOLAR_OUTPUT", "DEMAND_LEVEL", "GAS_PRICE",
        "COAL_AVAILABILITY", "INTERCONNECTOR_FLOW", "BATTERY_DISPATCH",
        "TEMPERATURE", "ROOFTOP_SOLAR",
    ]
    causes = ["LOW_WIND", "HIGH_DEMAND", "CONSTRAINT", "OUTAGE", "STRATEGIC_BIDDING"]
    sig_levels = ["HIGH", "MEDIUM", "LOW"]

    # Try real volatility data from prices
    try:
        vol_rows = _query_gold(f"""
            SELECT region_id,
                   DATE_TRUNC('day', interval_datetime) AS trade_date,
                   AVG(rrp) AS mean_price,
                   STDDEV(rrp) AS std_price,
                   MAX(rrp) AS max_price,
                   MIN(rrp) AS min_price,
                   SUM(CASE WHEN rrp > 300 THEN 1 ELSE 0 END) AS spike_count,
                   SUM(CASE WHEN rrp < 0 THEN 1 ELSE 0 END) AS negative_count
            FROM {_CATALOG}.gold.nem_prices_5min
            GROUP BY region_id, DATE_TRUNC('day', interval_datetime)
            ORDER BY trade_date DESC
            LIMIT 100
        """)
    except Exception:
        vol_rows = None

    if vol_rows:
        # Build regimes from real daily stats
        regimes = []
        for r in vol_rows:
            std_p = float(r.get("std_price") or 0)
            mean_p = float(r.get("mean_price") or 0)
            if std_p > 500:
                regime = "EXTREME"
            elif std_p > 100:
                regime = "HIGH_VOL"
            elif std_p > 30:
                regime = "NORMAL"
            else:
                regime = "LOW_VOL"
            dt_str = str(r.get("trade_date", ""))[:10]
            regimes.append({
                "region": r.get("region_id", "NSW1"),
                "regime": regime,
                "start_date": dt_str,
                "end_date": dt_str,
                "duration_days": 1,
                "mean_price": round(mean_p, 1),
                "std_price": round(std_p, 1),
                "max_price": round(float(r.get("max_price") or 0), 0),
                "min_price": round(float(r.get("min_price") or 0), 0),
                "spike_count": int(r.get("spike_count") or 0),
                "negative_count": int(r.get("negative_count") or 0),
            })

        # Build spike clusters from high-price days
        spike_clusters = []
        for cid, r in enumerate([rw for rw in vol_rows if float(rw.get("max_price") or 0) > 300][:12], 1):
            spike_clusters.append({
                "cluster_id": cid,
                "region": r.get("region_id", "NSW1"),
                "start_datetime": str(r.get("trade_date", "")),
                "end_datetime": str(r.get("trade_date", "")),
                "duration_intervals": int(r.get("spike_count") or 1),
                "peak_price": round(float(r.get("max_price") or 0), 0),
                "total_cost_m": round(float(r.get("mean_price") or 0) * int(r.get("spike_count") or 1) * 5 / 60 / 1000, 1),
                "primary_cause": _r.choice(causes),
            })
    else:
        regimes = []
        for region in regions:
            start = _dt(2023, 1, 1)
            for _ in range(_r.randint(4, 7)):
                regime = _r.choice(regime_types)
                dur = _r.randint(15, 120)
                end = start + _td(days=dur)
                mean_p = {"LOW_VOL": 45, "NORMAL": 85, "HIGH_VOL": 220, "EXTREME": 650}[regime]
                regimes.append({
                    "region": region, "regime": regime,
                    "start_date": start.strftime("%Y-%m-%d"), "end_date": end.strftime("%Y-%m-%d"),
                    "duration_days": dur, "mean_price": round(mean_p + _r.uniform(-20, 20), 1),
                    "std_price": round(mean_p * _r.uniform(0.2, 0.8), 1),
                    "max_price": round(mean_p * _r.uniform(2, 25), 0),
                    "min_price": round(_r.uniform(-100, mean_p * 0.3), 0),
                    "spike_count": _r.randint(0, 80), "negative_count": _r.randint(0, 60),
                })
                start = end
        spike_clusters = []
        for cid in range(1, 13):
            spike_clusters.append({
                "cluster_id": cid, "region": _r.choice(regions),
                "start_datetime": (_dt(2023, 6, 1) + _td(days=_r.randint(0, 500))).isoformat(),
                "end_datetime": (_dt(2023, 6, 1) + _td(days=_r.randint(501, 600))).isoformat(),
                "duration_intervals": _r.randint(3, 48),
                "peak_price": _r.choice([2500, 5000, 8000, 12000, 15100]),
                "total_cost_m": round(_r.uniform(0.5, 45.0), 1),
                "primary_cause": _r.choice(causes),
            })

    # Volatility metrics (keep mock — needs quarterly aggregation beyond available data)
    quarters = [f"{y}-Q{q}" for y in [2023, 2024] for q in [1, 2, 3, 4]]
    volatility_metrics = []
    for region in regions:
        for quarter in quarters:
            volatility_metrics.append({
                "region": region, "quarter": quarter,
                "realized_volatility_annualized": round(_r.uniform(0.3, 2.5), 3),
                "garch_volatility": round(_r.uniform(0.25, 2.0), 3),
                "conditional_var_95": round(_r.uniform(100, 600), 1),
                "conditional_var_99": round(_r.uniform(200, 1200), 1),
                "price_range_pct": round(_r.uniform(50, 500), 1),
                "iqr_price": round(_r.uniform(20, 120), 1),
            })

    transition_matrix = []
    for from_r in regime_types:
        probs = [_r.random() for _ in regime_types]
        total = sum(probs)
        probs = [p / total for p in probs]
        for to_r, prob in zip(regime_types, probs):
            transition_matrix.append({
                "from_regime": from_r, "to_regime": to_r,
                "transition_probability": round(prob, 3),
                "avg_duration_days": round(_r.uniform(10, 90), 1),
            })

    regime_drivers = []
    for regime in regime_types:
        for driver in _r.sample(drivers_list, k=_r.randint(3, 6)):
            regime_drivers.append({
                "regime": regime, "driver": driver,
                "correlation": round(_r.uniform(-0.85, 0.92), 2),
                "significance": _r.choice(sig_levels),
            })

    extreme_time = sum(r["duration_days"] for r in regimes if r["regime"] == "EXTREME")
    total_time = sum(r["duration_days"] for r in regimes)
    most_vol_region = max(regions, key=lambda rg: sum(
        r["std_price"] for r in regimes if r["region"] == rg
    )) if regimes else "SA1"

    return {
        "regimes": regimes,
        "transition_matrix": transition_matrix,
        "volatility_metrics": volatility_metrics,
        "spike_clusters": spike_clusters,
        "regime_drivers": regime_drivers,
        "summary": {
            "total_regimes_identified": len(regimes),
            "extreme_regime_pct": round(extreme_time / max(total_time, 1) * 100, 1),
            "avg_spike_duration_hrs": round(
                sum(c["duration_intervals"] * 0.0833 for c in spike_clusters) / max(len(spike_clusters), 1), 1
            ),
            "most_volatile_region": most_vol_region,
            "regime_persistence_avg_days": round(
                total_time / max(len(regimes), 1), 0
            ),
        },
    }


# ---------------------------------------------------------------------------
# 5) /api/spot-price-spike-prediction/dashboard  ->  SPPDashboard
# ---------------------------------------------------------------------------

@router.get("/api/spot-price-spike-prediction/dashboard")
def spot_price_spike_prediction_dashboard():
    _r.seed(505)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    statuses = ["ACTIVE", "RESOLVED_SPIKE", "RESOLVED_NO_SPIKE", "EXPIRED"]
    feature_categories = ["MARKET", "WEATHER", "GRID", "TEMPORAL", "FUEL"]
    cause_list = [
        "Generator Trip", "Demand Surge", "Low Wind", "Heatwave",
        "Network Constraint", "Gas Shortage", "Strategic Bidding",
    ]
    trigger_factors_pool = [
        "High demand forecast", "Low wind forecast", "Generator outage notice",
        "Heatwave warning", "Gas price spike", "Interconnector limit",
        "Evening ramp", "Low reserve margin",
    ]
    model_names = ["Ensemble-Spike", "XGBoost-Spike", "LSTM-Spike"]

    predictions = []
    for _ in range(30):
        prob = round(_r.random(), 3)
        actual = round(_r.uniform(-50, 500), 2) if _r.random() > 0.15 else None
        threshold = 300
        predictions.append({
            "prediction_id": f"PRED-{_r.randint(10000, 99999)}",
            "region": _r.choice(regions),
            "dispatch_interval": (
                _dt(2024, 7, 1) + _td(minutes=_r.randint(0, 43200))
            ).isoformat(),
            "predicted_spike_probability": prob,
            "predicted_price_aud_mwh": round(_r.uniform(50, 800), 2),
            "actual_price_aud_mwh": actual,
            "threshold_aud_mwh": threshold,
            "correct_prediction": (
                (prob >= 0.5 and actual >= threshold) or (prob < 0.5 and actual < threshold)
            ) if actual is not None else None,
            "confidence_interval_low": round(_r.uniform(10, 150), 2),
            "confidence_interval_high": round(_r.uniform(300, 1200), 2),
        })

    model_performance = []
    for model_name in model_names:
        for region in regions:
            for period in ["2024-H1", "2024-H2"]:
                prec = round(_r.uniform(0.65, 0.92), 3)
                rec = round(_r.uniform(0.60, 0.90), 3)
                model_performance.append({
                    "model_name": model_name,
                    "region": region,
                    "period": period,
                    "precision": prec,
                    "recall": rec,
                    "f1_score": round(2 * prec * rec / max(prec + rec, 0.001), 3),
                    "auc_roc": round(_r.uniform(0.85, 0.97), 3),
                    "false_positive_rate": round(_r.uniform(0.03, 0.15), 3),
                    "spike_threshold_aud_mwh": 300,
                    "total_spikes": _r.randint(20, 120),
                    "predicted_spikes": _r.randint(15, 110),
                })

    feature_names = [
        "demand_forecast_mw", "temperature_max_c", "wind_generation_forecast_mw",
        "solar_generation_mw", "gas_price_aud_gj", "interconnector_available_mw",
        "hour_of_day", "day_of_week", "lagged_price_30min", "reserve_margin_mw",
        "coal_gen_available_mw", "battery_soc_pct", "rooftop_solar_mw",
    ]
    features = []
    for fname in feature_names:
        features.append({
            "feature": fname,
            "importance": round(_r.uniform(0.01, 0.15), 4),
            "category": _r.choice(feature_categories),
            "spike_correlation": round(_r.uniform(-0.6, 0.85), 3),
            "lag_minutes": _r.choice([0, 5, 30, 60, 120]),
        })

    alerts = []
    for i in range(8):
        alerts.append({
            "alert_id": f"ALERT-{_r.randint(1000, 9999)}",
            "region": _r.choice(regions),
            "issued_at": (
                _dt(2024, 7, 15) + _td(hours=_r.randint(0, 168))
            ).isoformat(),
            "forecast_window_minutes": _r.choice([30, 60, 120, 240]),
            "spike_probability": round(_r.uniform(0.45, 0.98), 2),
            "expected_price_aud_mwh": _r.choice([450, 800, 1500, 5000, 12000]),
            "trigger_factors": _r.sample(trigger_factors_pool, k=_r.randint(2, 4)),
            "status": _r.choice(statuses),
        })

    spike_history = []
    for _ in range(20):
        predicted = _r.random() > 0.3
        spike_history.append({
            "region": _r.choice(regions),
            "date": (
                _dt(2024, 1, 1) + _td(days=_r.randint(0, 200))
            ).strftime("%Y-%m-%d"),
            "hour": _r.randint(6, 22),
            "max_price_aud_mwh": _r.choice([500, 1200, 5000, 8000, 15100]),
            "duration_intervals": _r.randint(2, 24),
            "cause": _r.choice(cause_list),
            "predicted": predicted,
            "warning_lead_time_minutes": _r.randint(10, 120) if predicted else None,
        })

    active_alerts = sum(1 for a in alerts if a["status"] == "ACTIVE")
    best_auc = max(model_performance, key=lambda m: m["auc_roc"])
    ensemble_perfs = [m for m in model_performance if m["model_name"] == "Ensemble-Spike"]
    avg_recall = round(sum(m["recall"] for m in ensemble_perfs) / max(len(ensemble_perfs), 1), 3)

    return {
        "predictions": predictions,
        "model_performance": model_performance,
        "features": features,
        "alerts": alerts,
        "spike_history": spike_history,
        "summary": {
            "active_alerts": active_alerts,
            "best_auc_roc": best_auc["auc_roc"],
            "best_model": best_auc["model_name"],
            "spike_detection_rate_pct": round(avg_recall * 100, 1),
            "false_positive_rate_pct": round(
                sum(m["false_positive_rate"] for m in ensemble_perfs) / max(len(ensemble_perfs), 1) * 100, 1
            ),
            "avg_warning_lead_time_minutes": round(
                sum(s["warning_lead_time_minutes"] for s in spike_history if s["warning_lead_time_minutes"])
                / max(sum(1 for s in spike_history if s["warning_lead_time_minutes"]), 1), 0
            ),
            "total_spikes_ytd": len(spike_history),
        },
    }


# ---------------------------------------------------------------------------
# 6) /api/spot-market-depth-x/dashboard  ->  ESMDXDashboard
# ---------------------------------------------------------------------------

@router.get("/api/spot-market-depth-x/dashboard")
def spot_market_depth_x_dashboard():
    _r.seed(606)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    anomaly_types = [
        "Price Spike", "Price Collapse", "Volume Surge",
        "Bid Withdrawal", "Coordinated Bidding", "Abnormal Spread",
    ]
    suspected_causes = [
        "Generator rebid", "Demand forecast error", "Interconnector outage",
        "Solar cliff", "Wind drought", "Strategic withdrawal",
    ]
    participant_names = [
        "AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro",
        "CS Energy", "Stanwell", "Alinta Energy", "Engie", "Shell Energy",
    ]

    dates = [
        (_dt(2024, 1, 15) + _td(days=d * 7)).strftime("%Y-%m-%d")
        for d in range(4)
    ]

    order_books = []
    for region in regions[:3]:
        for date in dates:
            for hour in range(0, 24, 12):
                mid = round(_r.uniform(40, 250), 2)
                order_books.append({
                    "book_id": f"OB-{region}-{date}-{hour:02d}",
                    "region": region,
                    "snapshot_date": date,
                    "hour": hour,
                    "bid_volume_mw_10pct": round(_r.uniform(200, 2000), 0),
                    "bid_volume_mw_total": round(_r.uniform(3000, 12000), 0),
                    "offer_volume_mw_10pct": round(_r.uniform(200, 2500), 0),
                    "offer_volume_mw_total": round(_r.uniform(3000, 14000), 0),
                    "bid_ask_spread_dolpermwh": round(_r.uniform(1.5, 45.0), 2),
                    "market_depth_mw_within_5pct": round(_r.uniform(500, 5000), 0),
                    "mid_price_dolpermwh": mid,
                    "bid_price_95pct": round(mid - _r.uniform(5, 50), 2),
                    "offer_price_5pct": round(mid + _r.uniform(5, 50), 2),
                    "volume_weighted_mid_price": round(mid + _r.uniform(-5, 5), 2),
                    "liquidity_score": round(_r.uniform(30, 95), 1),
                })

    price_discovery = []
    for region in regions[:3]:
        for date in dates:
            for hour in [8, 16]:
                pre_dispatch = round(_r.uniform(40, 200), 2)
                dispatch = round(pre_dispatch + _r.uniform(-30, 40), 2)
                price_discovery.append({
                    "discovery_id": f"PD-{region}-{date}-{hour:02d}",
                    "region": region,
                    "date": date,
                    "hour": hour,
                    "pre_dispatch_price": pre_dispatch,
                    "dispatch_price": dispatch,
                    "price_revision_pct": round((dispatch - pre_dispatch) / max(pre_dispatch, 1) * 100, 2),
                    "discovery_efficiency_pct": round(_r.uniform(60, 98), 1),
                    "informed_trading_pct": round(_r.uniform(20, 70), 1),
                    "noise_trading_pct": round(_r.uniform(10, 40), 1),
                    "price_impact_per_mw": round(_r.uniform(0.01, 0.5), 3),
                    "market_depth_mw": round(_r.uniform(1000, 6000), 0),
                    "contribution_of_rebids_pct": round(_r.uniform(5, 45), 1),
                    "information_asymmetry_index": round(_r.uniform(0.1, 0.8), 2),
                })

    trading_activity = []
    for region in regions[:3]:
        for date in dates:
            for hour in range(0, 24, 12):
                avg_p = round(_r.uniform(30, 200), 2)
                trading_activity.append({
                    "activity_id": f"TA-{region}-{date}-{hour:02d}",
                    "region": region,
                    "date": date,
                    "hour": hour,
                    "total_traded_volume_mwh": round(_r.uniform(500, 8000), 0),
                    "num_dispatch_intervals": _r.randint(6, 12),
                    "avg_interval_price": avg_p,
                    "max_interval_price": round(avg_p * _r.uniform(1.1, 5.0), 2),
                    "min_interval_price": round(avg_p * _r.uniform(0.1, 0.8), 2),
                    "price_std_dev": round(_r.uniform(5, 120), 2),
                    "high_price_intervals": _r.randint(0, 4),
                    "zero_price_intervals": _r.randint(0, 2),
                    "negative_price_intervals": _r.randint(0, 3),
                    "largest_single_bid_mw": round(_r.uniform(100, 1500), 0),
                    "market_concentration_index": round(_r.uniform(800, 3500), 0),
                })

    market_impacts = []
    for i, pname in enumerate(participant_names):
        region = regions[i % len(regions)]
        market_impacts.append({
            "impact_id": f"MI-{i + 1:03d}",
            "participant_name": pname,
            "region": region,
            "period": "2024-H1",
            "avg_bid_volume_mw": round(_r.uniform(200, 3000), 0),
            "market_share_dispatched_pct": round(_r.uniform(5, 28), 1),
            "price_impact_dolpermwh_per_100mw": round(_r.uniform(0.5, 8.0), 2),
            "strategic_trading_indicator": round(_r.uniform(1.0, 9.0), 1),
            "rebid_frequency_per_interval": round(_r.uniform(0.05, 2.5), 2),
            "price_setter_hours_pct": round(_r.uniform(5, 45), 1),
            "market_impact_cost_m_pa": round(_r.uniform(2, 120), 1),
            "regulatoryaction_flag": _r.random() > 0.7,
        })

    seasonal_patterns = []
    for region in regions[:3]:
        for month in range(1, 13, 3):
            for hour in range(0, 24, 8):
                seasonal_patterns.append({
                    "pattern_id": f"SP-{region}-{month:02d}-{hour:02d}",
                    "region": region,
                    "month": month,
                    "hour_of_day": hour,
                    "avg_price_dolpermwh": round(_r.uniform(20, 300), 2),
                    "avg_volume_mwh": round(_r.uniform(500, 6000), 0),
                    "price_volatility_pct": round(_r.uniform(5, 80), 1),
                    "liquidity_score": round(_r.uniform(30, 95), 1),
                    "high_price_probability_pct": round(_r.uniform(0, 25), 1),
                    "negative_price_probability_pct": round(_r.uniform(0, 15), 1),
                    "renewable_share_pct": round(_r.uniform(10, 75), 1),
                    "peak_demand_flag": hour in [16, 17, 18, 19],
                })

    anomalies = []
    for i in range(10):
        detected = round(_r.uniform(50, 15000), 0)
        expected = round(_r.uniform(40, 200), 0)
        anomalies.append({
            "anomaly_id": f"ANOM-{i + 1:03d}",
            "region": _r.choice(regions),
            "detected_date": (_dt(2024, 1, 1) + _td(days=_r.randint(0, 200))).strftime("%Y-%m-%d"),
            "hour": _r.randint(0, 23),
            "anomaly_type": _r.choice(anomaly_types),
            "detected_price_dolpermwh": detected,
            "expected_price_dolpermwh": expected,
            "deviation_pct": round((detected - expected) / max(expected, 1) * 100, 1),
            "volume_deviation_pct": round(_r.uniform(-50, 200), 1),
            "suspected_cause": _r.choice(suspected_causes),
            "market_impact_m": round(_r.uniform(0.1, 25.0), 1),
            "referred_to_aer": _r.random() > 0.6,
        })

    all_spreads = [ob["bid_ask_spread_dolpermwh"] for ob in order_books]
    all_liq = [ob["liquidity_score"] for ob in order_books]
    all_eff = [pd["discovery_efficiency_pct"] for pd in price_discovery]

    return {
        "order_books": order_books,
        "price_discovery": price_discovery,
        "trading_activity": trading_activity,
        "market_impacts": market_impacts,
        "seasonal_patterns": seasonal_patterns,
        "anomalies": anomalies,
        "summary": {
            "avg_bid_ask_spread_dolpermwh": round(sum(all_spreads) / max(len(all_spreads), 1), 2),
            "avg_liquidity_score": round(sum(all_liq) / max(len(all_liq), 1), 1),
            "anomalies_detected_ytd": len(anomalies),
            "avg_price_discovery_efficiency_pct": round(sum(all_eff) / max(len(all_eff), 1), 1),
        },
    }


# ---------------------------------------------------------------------------
# 7) /api/negative-price-events/dashboard  ->  NPEDashboard
# ---------------------------------------------------------------------------

@router.get("/api/negative-price-events/dashboard")
def negative_price_events_dashboard():
    _r.seed(707)
    regions = ["SA1", "VIC1", "NSW1", "QLD1", "TAS1"]
    years = list(range(2018, 2025))

    # Try real negative price frequency from NEMWEB
    try:
        neg_rows = _query_gold(f"""
            SELECT region_id,
                   COUNT(*) AS neg_intervals,
                   AVG(rrp) AS avg_neg_price,
                   MIN(rrp) AS deepest_price
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE rrp < 0
            GROUP BY region_id
        """)
    except Exception:
        neg_rows = None

    real_neg = {}
    if neg_rows:
        for nr in neg_rows:
            real_neg[nr["region_id"]] = {
                "intervals": int(nr["neg_intervals"] or 0),
                "avg_price": float(nr["avg_neg_price"] or -30),
                "deepest": float(nr["deepest_price"] or -100),
            }

    frequency = []
    for year in years:
        for region in regions:
            rn = real_neg.get(region)
            if rn and year == 2024:
                neg_intervals = rn["intervals"]
                avg_neg = round(rn["avg_price"], 1)
                deepest = round(rn["deepest"], 0)
            else:
                base_intervals = {"SA1": 3500, "VIC1": 2200, "NSW1": 1500, "QLD1": 1800, "TAS1": 800}[region]
                growth = (year - 2018) * _r.randint(80, 250)
                neg_intervals = base_intervals + growth + _r.randint(-300, 300)
                avg_neg = round(_r.uniform(-80, -5), 1)
                deepest = round(_r.uniform(-1000, -200), 0)
            neg_hours = round(neg_intervals * 5 / 60, 1)
            frequency.append({
                "year": year,
                "region": region,
                "negative_price_intervals": neg_intervals,
                "negative_price_hours": neg_hours,
                "pct_of_year": round(neg_hours / 8760 * 100, 2),
                "avg_negative_price": avg_neg,
                "deepest_price": deepest,
                "consecutive_negative_hrs_max": round(_r.uniform(1, 18), 1),
                "total_negative_energy_mwh": round(neg_intervals * _r.uniform(50, 300), 0),
            })

    quarters = [f"{y}-Q{q}" for y in [2022, 2023, 2024] for q in [1, 2, 3, 4]]
    drivers = []
    for region in regions:
        for quarter in quarters:
            solar = round(_r.uniform(15, 55), 1)
            wind = round(_r.uniform(10, 35), 1)
            must_run = round(_r.uniform(10, 30), 1)
            hydro = round(_r.uniform(2, 15), 1)
            low_dem = round(_r.uniform(5, 20), 1)
            export = round(100 - solar - wind - must_run - hydro - low_dem, 1)
            drivers.append({
                "region": region,
                "quarter": quarter,
                "rooftop_solar_contribution_pct": solar,
                "wind_contribution_pct": wind,
                "must_run_baseload_pct": must_run,
                "pumped_hydro_pct": hydro,
                "low_demand_pct": low_dem,
                "combined_export_constraint_pct": max(0, export),
            })

    battery_opportunity = []
    for region in regions:
        for year in [2022, 2023, 2024]:
            neg_mwh = _r.randint(50000, 500000)
            battery_opportunity.append({
                "region": region,
                "year": year,
                "negative_price_mwh_available": neg_mwh,
                "optimal_charge_value_m": round(neg_mwh * _r.uniform(0.00005, 0.0002), 1),
                "battery_capacity_mw_needed": round(_r.uniform(50, 600), 0),
                "avg_charge_price": round(_r.uniform(-80, -5), 1),
                "arbitrage_spread_to_peak": round(_r.uniform(80, 350), 0),
            })

    plant_names = [
        "Liddell", "Eraring", "Bayswater", "Vales Point",
        "Yallourn", "Loy Yang A", "Tarong", "Gladstone",
    ]
    technologies = ["COAL", "COAL", "COAL", "COAL", "COAL", "COAL", "GAS", "COGENERATION"]
    must_run = []
    for i, pname in enumerate(plant_names):
        region = _r.choice(regions[:4])
        min_stable = round(_r.uniform(200, 800), 0)
        must_run.append({
            "plant_name": pname,
            "technology": technologies[i % len(technologies)],
            "region": region,
            "min_stable_load_mw": min_stable,
            "technical_min_mw": round(min_stable * _r.uniform(0.4, 0.7), 0),
            "startup_cost_k": round(_r.uniform(200, 2000), 0),
            "ramp_rate_mw_min": round(_r.uniform(2, 15), 1),
            "negative_price_hours_yr": round(_r.uniform(100, 1500), 0),
            "estimated_loss_m_yr": round(_r.uniform(1, 25), 1),
        })

    market_design = [
        {
            "mechanism": "FIVE_MINUTE_SETTLEMENT",
            "description": "Transition from 30-minute to 5-minute settlement to reduce manipulation of negative price windows.",
            "estimated_negative_price_reduction_pct": 12,
            "implementation_cost_m": 150,
            "aemo_recommendation": True,
            "status": "IMPLEMENTED",
        },
        {
            "mechanism": "BATTERY_STORAGE_INCENTIVES",
            "description": "Tax incentives and CIS funding for grid-scale batteries to absorb excess renewable generation.",
            "estimated_negative_price_reduction_pct": 25,
            "implementation_cost_m": 800,
            "aemo_recommendation": True,
            "status": "IMPLEMENTED",
        },
        {
            "mechanism": "INTERCONNECTOR_UPGRADE",
            "description": "Expand SA-NSW (EnergyConnect) and QLD-NSW interconnector capacity to export surplus generation.",
            "estimated_negative_price_reduction_pct": 18,
            "implementation_cost_m": 2400,
            "aemo_recommendation": True,
            "status": "UNDER_REVIEW",
        },
        {
            "mechanism": "DEMAND_FLEXIBILITY_PROGRAM",
            "description": "Industrial and residential demand response programs to shift load into negative price periods.",
            "estimated_negative_price_reduction_pct": 15,
            "implementation_cost_m": 120,
            "aemo_recommendation": True,
            "status": "PROPOSED",
        },
        {
            "mechanism": "NEGATIVE_PRICE_FLOOR_REFORM",
            "description": "Review of market price floor from -$1000/MWh to reduce perverse bidding incentives.",
            "estimated_negative_price_reduction_pct": 30,
            "implementation_cost_m": 10,
            "aemo_recommendation": False,
            "status": "UNDER_REVIEW",
        },
        {
            "mechanism": "PUMPED_HYDRO_OPTIMISATION",
            "description": "Optimise Snowy 2.0 and Borumba pumped hydro to charge during negative prices.",
            "estimated_negative_price_reduction_pct": 20,
            "implementation_cost_m": 600,
            "aemo_recommendation": True,
            "status": "IMPLEMENTED",
        },
    ]

    sa_2024 = [f for f in frequency if f["region"] == "SA1" and f["year"] == 2024]
    total_neg_2024 = sum(f["negative_price_intervals"] for f in frequency if f["year"] == 2024)
    batt_total = sum(b["optimal_charge_value_m"] for b in battery_opportunity if b["year"] == 2024)
    deepest = min(f["deepest_price"] for f in frequency)
    prev_year_total = sum(f["negative_price_intervals"] for f in frequency if f["year"] == 2023)

    return {
        "frequency": frequency,
        "drivers": drivers,
        "battery_opportunity": battery_opportunity,
        "must_run": must_run,
        "market_design": market_design,
        "summary": {
            "total_negative_intervals_2024": total_neg_2024,
            "pct_of_year_sa1": sa_2024[0]["pct_of_year"] if sa_2024 else 8.5,
            "avg_negative_price_2024": round(
                sum(f["avg_negative_price"] for f in frequency if f["year"] == 2024)
                / max(sum(1 for f in frequency if f["year"] == 2024), 1), 1
            ),
            "battery_arbitrage_value_m": round(batt_total, 0),
            "deepest_price": deepest,
            "yoy_increase_pct": round(
                (total_neg_2024 - prev_year_total) / max(prev_year_total, 1) * 100, 1
            ),
        },
    }


# ---------------------------------------------------------------------------
# 8) /api/electricity-spot-price-seasonality/dashboard  ->  ESPSDashboard
# ---------------------------------------------------------------------------

@router.get("/api/electricity-spot-price-seasonality/dashboard")
def electricity_spot_price_seasonality_dashboard():
    _r.seed(808)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    seasons = ["Summer", "Autumn", "Winter", "Spring"]
    day_types = [
        "Monday", "Tuesday", "Wednesday", "Thursday",
        "Friday", "Saturday", "Sunday", "Public Holiday",
    ]
    scenarios = ["Base", "High Renewable", "Low Renewable"]
    regime_types = [
        "High-Price", "Low-Price", "Volatile",
        "Renewable-Driven", "Gas-Driven", "Managed",
    ]

    # Try real hourly price patterns from NEMWEB
    try:
        hourly_rows = _query_gold(f"""
            SELECT region_id,
                   HOUR(interval_datetime) AS hour_of_day,
                   AVG(rrp) AS avg_price,
                   PERCENTILE_APPROX(rrp, 0.5) AS median_price,
                   PERCENTILE_APPROX(rrp, 0.1) AS p10_price,
                   PERCENTILE_APPROX(rrp, 0.9) AS p90_price,
                   SUM(CASE WHEN rrp > 300 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS spike_freq_pct
            FROM {_CATALOG}.gold.nem_prices_5min
            GROUP BY region_id, HOUR(interval_datetime)
            ORDER BY region_id, hour_of_day
        """)
    except Exception:
        hourly_rows = None

    def _month_to_season(m):
        if m in (12, 1, 2):
            return "Summer"
        if m in (3, 4, 5):
            return "Autumn"
        if m in (6, 7, 8):
            return "Winter"
        return "Spring"

    hourly_patterns = []
    if hourly_rows:
        for r in hourly_rows:
            region = r.get("region_id", "NSW1")
            if region not in regions[:3]:
                continue
            hour = int(r.get("hour_of_day", 0))
            avg_p = round(float(r.get("avg_price") or 0), 1)
            hourly_patterns.append({
                "region": region, "hour_of_day": hour,
                "season": "Year-Round", "year": 2026,
                "avg_price_mwh": avg_p,
                "median_price_mwh": round(float(r.get("median_price") or avg_p), 1),
                "p10_price_mwh": round(float(r.get("p10_price") or avg_p * 0.3), 1),
                "p90_price_mwh": round(float(r.get("p90_price") or avg_p * 2), 1),
                "spike_frequency_pct": round(float(r.get("spike_freq_pct") or 0), 1),
                "avg_demand_mw": round(_r.uniform(4000, 12000), 0),
                "avg_solar_mw": round(max(0, _r.uniform(0, 3000) if 6 <= hour <= 18 else 0), 0),
                "avg_wind_mw": round(_r.uniform(200, 3000), 0),
            })
    else:
        for region in regions[:3]:
            for season in seasons:
                for year in [2024]:
                    for hour in range(0, 24, 4):
                        base = {"Summer": 95, "Autumn": 65, "Winter": 80, "Spring": 55}[season]
                        hour_adj = 40 * (1 if 16 <= hour <= 20 else (-0.3 if 10 <= hour <= 14 else 0))
                        avg_p = round(base + hour_adj + _r.uniform(-20, 20), 1)
                        hourly_patterns.append({
                            "region": region, "hour_of_day": hour, "season": season, "year": year,
                            "avg_price_mwh": avg_p,
                            "median_price_mwh": round(avg_p * _r.uniform(0.85, 1.0), 1),
                            "p10_price_mwh": round(avg_p * _r.uniform(0.2, 0.5), 1),
                            "p90_price_mwh": round(avg_p * _r.uniform(1.5, 3.5), 1),
                            "spike_frequency_pct": round(_r.uniform(0, 12), 1),
                            "avg_demand_mw": round(_r.uniform(4000, 12000), 0),
                            "avg_solar_mw": round(max(0, _r.uniform(-500, 3000) if 6 <= hour <= 18 else 0), 0),
                            "avg_wind_mw": round(_r.uniform(200, 3000), 0),
                        })

    day_type_patterns = []
    for region in regions[:3]:
        for season in seasons:
            for year in [2024]:
                for dt in ["Weekday", "Weekend", "Public Holiday"]:
                    avg_p = round(_r.uniform(40, 150), 1)
                    day_type_patterns.append({
                        "region": region, "day_type": dt, "season": season, "year": year,
                        "avg_price_mwh": avg_p, "peak_hour": _r.randint(16, 20),
                        "trough_hour": _r.randint(2, 6),
                        "price_volatility_pct": round(_r.uniform(15, 80), 1),
                        "demand_factor": round(_r.uniform(0.7, 1.3), 2),
                        "evening_ramp_magnitude_mwh": round(_r.uniform(20, 150), 1),
                        "morning_ramp_magnitude_mwh": round(_r.uniform(10, 80), 1),
                    })

    # Try real monthly trends
    try:
        monthly_rows = _query_gold(f"""
            SELECT region_id,
                   DATE_FORMAT(interval_datetime, 'yyyy-MM') AS year_month,
                   AVG(rrp) AS avg_price, MIN(rrp) AS min_price, MAX(rrp) AS max_price,
                   SUM(CASE WHEN rrp < 0 THEN 1 ELSE 0 END) * 5.0 / 60 AS neg_price_hrs,
                   SUM(CASE WHEN rrp > 15000 THEN 1 ELSE 0 END) * 5.0 / 60 AS voll_hrs
            FROM {_CATALOG}.gold.nem_prices_5min
            GROUP BY region_id, DATE_FORMAT(interval_datetime, 'yyyy-MM')
            ORDER BY year_month DESC
            LIMIT 60
        """)
    except Exception:
        monthly_rows = None

    monthly_trends = []
    if monthly_rows:
        for r in monthly_rows:
            region = r.get("region_id", "NSW1")
            if region not in regions[:3]:
                continue
            avg_p = round(float(r.get("avg_price") or 0), 1)
            monthly_trends.append({
                "year_month": r.get("year_month", "2026-01"), "region": region,
                "avg_price_mwh": avg_p,
                "min_price_mwh": round(float(r.get("min_price") or 0), 1),
                "max_price_mwh": round(float(r.get("max_price") or 0), 1),
                "negative_price_hrs": round(float(r.get("neg_price_hrs") or 0), 0),
                "voll_price_hrs": round(float(r.get("voll_hrs") or 0), 0),
                "cumulative_avg_ytd_mwh": avg_p,
                "renewable_pct": round(_r.uniform(25, 65), 1),
                "coal_pct": round(_r.uniform(15, 55), 1),
                "gas_pct": round(_r.uniform(5, 25), 1),
                "temperature_anomaly_c": round(_r.uniform(-3, 5), 1),
            })
    else:
        for region in regions[:3]:
            for year in [2024]:
                for month in range(1, 10):
                    ym = f"{year}-{month:02d}"
                    avg_p = round(_r.uniform(30, 160), 1)
                    monthly_trends.append({
                        "year_month": ym, "region": region,
                        "avg_price_mwh": avg_p,
                        "min_price_mwh": round(_r.uniform(-100, 10), 1),
                        "max_price_mwh": round(avg_p * _r.uniform(3, 20), 1),
                        "negative_price_hrs": round(_r.uniform(0, 120), 0),
                        "voll_price_hrs": round(_r.uniform(0, 5), 0),
                        "cumulative_avg_ytd_mwh": round(avg_p * _r.uniform(0.9, 1.1), 1),
                        "renewable_pct": round(_r.uniform(25, 65), 1),
                        "coal_pct": round(_r.uniform(15, 55), 1),
                        "gas_pct": round(_r.uniform(5, 25), 1),
                        "temperature_anomaly_c": round(_r.uniform(-3, 5), 1),
                    })

    price_regimes = []
    rid = 1
    for region in regions:
        start = _dt(2023, 1, 1)
        for _ in range(_r.randint(2, 4)):
            rtype = _r.choice(regime_types)
            dur = _r.randint(30, 180)
            end = start + _td(days=dur)
            avg_p = round(_r.uniform(30, 250), 1)
            price_regimes.append({
                "regime_id": f"R{rid:03d}",
                "region": region,
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
                "regime_type": rtype,
                "duration_days": dur,
                "avg_price_mwh": avg_p,
                "p95_price_mwh": round(avg_p * _r.uniform(3, 15), 1),
                "total_energy_cost_m": round(_r.uniform(50, 2000), 0),
                "primary_driver": _r.choice(["Solar surplus", "Gas price", "Demand heatwave", "Coal retirement", "Wind drought"]),
                "renewable_share_pct": round(_r.uniform(20, 70), 1),
            })
            rid += 1
            start = end

    decomposition = []
    for region in regions[:3]:
        for year in range(2022, 2025):
            trend = round(_r.uniform(40, 100), 2)
            seasonal = round(_r.uniform(-15, 25), 2)
            renewable_effect = round(_r.uniform(-30, -5), 2)
            residual = round(_r.uniform(-10, 10), 2)
            decomposition.append({
                "year": year,
                "region": region,
                "trend_component_mwh": trend,
                "seasonal_component_mwh": seasonal,
                "residual_component_mwh": residual,
                "renewable_penetration_effect_mwh": renewable_effect,
                "demand_effect_mwh": round(_r.uniform(-5, 15), 2),
                "fuel_cost_effect_mwh": round(_r.uniform(-10, 20), 2),
                "policy_effect_mwh": round(_r.uniform(-8, 5), 2),
                "long_run_avg_mwh": round(trend + seasonal + renewable_effect + residual, 2),
            })

    forecasts = []
    for region in regions[:3]:
        for year in range(2024, 2027):
            for scenario in scenarios:
                base_price = {"Base": 75, "High Renewable": 55, "Low Renewable": 95}[scenario]
                year_adj = (year - 2024) * _r.uniform(-5, 3)
                forecasts.append({
                    "year": year,
                    "region": region,
                    "scenario": scenario,
                    "forecast_avg_price_mwh": round(base_price + year_adj + _r.uniform(-10, 10), 1),
                    "forecast_peak_price_mwh": round((base_price + year_adj) * _r.uniform(3, 8), 1),
                    "forecast_negative_price_hrs": round(_r.uniform(100, 1500), 0),
                    "price_volatility_index": round(_r.uniform(0.3, 2.0), 2),
                    "renewable_pct_forecast": round(min(95, 35 + (year - 2024) * 5 + _r.uniform(-5, 5)), 1),
                    "structural_break_expected": year >= 2026 and scenario == "High Renewable",
                })

    summer_prices = [h["avg_price_mwh"] for h in hourly_patterns if h["season"] == "Summer" and h["year"] == 2024]
    winter_prices = [h["avg_price_mwh"] for h in hourly_patterns if h["season"] == "Winter" and h["year"] == 2024]

    return {
        "hourly_patterns": hourly_patterns,
        "day_type_patterns": day_type_patterns,
        "monthly_trends": monthly_trends,
        "price_regimes": price_regimes,
        "decomposition": decomposition,
        "forecasts": forecasts,
        "summary": {
            "avg_price_summer_mwh": round(sum(summer_prices) / max(len(summer_prices), 1), 1),
            "avg_price_winter_mwh": round(sum(winter_prices) / max(len(winter_prices), 1), 1),
            "peak_hour_of_day": 17,
            "trough_hour_of_day": 3,
            "negative_price_hrs_ytd": sum(m["negative_price_hrs"] for m in monthly_trends if m["year_month"].startswith("2024")),
            "most_volatile_region": "SA1",
        },
    }


# ---------------------------------------------------------------------------
# 9) /api/electricity-spot-price-events/dashboard  ->  ESPEDashboard
# ---------------------------------------------------------------------------

@router.get("/api/electricity-spot-price-events/dashboard")
def electricity_spot_price_events_dashboard():
    _r.seed(909)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    event_types = ["Spike", "Negative", "Cap Hit", "VoLL"]
    trigger_causes = [
        "Generator Trip", "Demand Spike", "Renewable Drop",
        "Gas Shortage", "Network Constraint", "Heatwave", "Cold Snap",
    ]
    driver_types = ["Supply", "Demand", "Weather", "Network", "Fuel", "Market Structure"]
    seasonal_patterns = ["Summer", "Winter", "Year-Round", "Random"]
    price_bands = ["Negative", "$0-$50", "$50-$300", "$300+"]
    measure_types = ["FCAS", "RERT", "DSP", "Battery", "Interconnector", "Manual"]

    # Try real extreme price events from NEMWEB
    try:
        event_rows = _query_gold(f"""
            SELECT region_id, rrp, interval_datetime,
                   MONTH(interval_datetime) AS month_num
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE rrp > 300 OR rrp < -50
            ORDER BY ABS(rrp) DESC
            LIMIT 50
        """)
    except Exception:
        event_rows = None

    events = []
    if event_rows:
        for i, r in enumerate(event_rows[:25]):
            price = float(r.get("rrp") or 0)
            if price > 15000:
                etype = "VoLL"
            elif price > 300:
                etype = "Spike"
            elif price < -50:
                etype = "Negative"
            else:
                etype = "Cap Hit"
            events.append({
                "event_id": f"ESPE-{i + 1:04d}",
                "event_type": etype,
                "region": r.get("region_id", "NSW1"),
                "year": 2026,
                "month": int(r.get("month_num") or 1),
                "duration_intervals": _r.randint(1, 12),
                "max_price_mwh": round(price, 2),
                "avg_price_mwh": round(price * 0.7, 2),
                "total_energy_affected_mwh": round(abs(price) * _r.uniform(1, 10), 0),
                "financial_impact_m_aud": round(abs(price) * _r.uniform(0.001, 0.01), 3),
                "trigger_cause": _r.choice(trigger_causes),
            })
    else:
        for i in range(25):
            events.append({
                "event_id": f"ESPE-{i + 1:04d}",
                "event_type": _r.choice(event_types),
                "region": _r.choice(regions),
                "year": _r.choice([2022, 2023, 2024]),
                "month": _r.randint(1, 12),
                "duration_intervals": _r.randint(1, 48),
                "max_price_mwh": round(_r.uniform(-1000, 16600), 2),
                "avg_price_mwh": round(_r.uniform(-200, 5000), 2),
                "total_energy_affected_mwh": round(_r.uniform(100, 50000), 0),
                "financial_impact_m_aud": round(_r.uniform(0.01, 15.0), 3),
                "trigger_cause": _r.choice(trigger_causes),
            })

    # Try real regional stats
    try:
        stat_rows = _query_gold(f"""
            SELECT region_id,
                   SUM(CASE WHEN rrp > 300 THEN 1 ELSE 0 END) AS spike_count,
                   SUM(CASE WHEN rrp < 0 THEN 1 ELSE 0 END) AS negative_count,
                   SUM(CASE WHEN rrp > 15000 THEN 1 ELSE 0 END) AS cap_hit_count,
                   PERCENTILE_APPROX(rrp, 0.05) AS p5_price,
                   PERCENTILE_APPROX(rrp, 0.95) AS p95_price,
                   STDDEV(rrp) AS price_std,
                   SUM(CASE WHEN rrp > 300 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS time_above_300_pct
            FROM {_CATALOG}.gold.nem_prices_5min
            GROUP BY region_id
        """)
    except Exception:
        stat_rows = None

    regional_stats = []
    if stat_rows:
        for r in stat_rows:
            region = r.get("region_id", "NSW1")
            spk = int(r.get("spike_count") or 0)
            regional_stats.append({
                "region": region, "year": 2026, "half": "H1",
                "spike_count": spk,
                "negative_count": int(r.get("negative_count") or 0),
                "cap_hit_count": int(r.get("cap_hit_count") or 0),
                "total_spike_revenue_m_aud": round(spk * _r.uniform(0.1, 0.5), 1),
                "p5_price_mwh": round(float(r.get("p5_price") or -10), 1),
                "p95_price_mwh": round(float(r.get("p95_price") or 200), 1),
                "volatility_index": round(float(r.get("price_std") or 50) / 100, 2),
                "time_above_300_pct": round(float(r.get("time_above_300_pct") or 0), 2),
            })
    else:
        for region in regions:
            for year in [2022, 2023, 2024]:
                for half in ["H1", "H2"]:
                    regional_stats.append({
                        "region": region, "year": year, "half": half,
                        "spike_count": _r.randint(5, 80),
                        "negative_count": _r.randint(10, 200),
                        "cap_hit_count": _r.randint(0, 5),
                        "total_spike_revenue_m_aud": round(_r.uniform(1, 50), 1),
                        "p5_price_mwh": round(_r.uniform(-100, -10), 1),
                        "p95_price_mwh": round(_r.uniform(200, 2000), 1),
                        "volatility_index": round(_r.uniform(0.3, 3.5), 2),
                        "time_above_300_pct": round(_r.uniform(0.1, 5.0), 2),
                    })

    driver_names = [
        "Coal Plant Outage", "High Demand Heatwave", "Wind Drought",
        "Solar Cliff Evening", "Gas Price Surge", "Interconnector Limit",
        "Strategic Rebidding", "Cold Snap Heating Load", "Bushfire Network Damage",
        "Renewable Curtailment", "Low Reserve Margin", "Battery Exhaustion",
        "Rooftop Solar Excess", "Transmission Congestion", "Cyclone Impact",
    ]
    espe_drivers = []
    for i, dname in enumerate(driver_names):
        espe_drivers.append({
            "driver_id": f"DRV-{i + 1:03d}",
            "driver_name": dname,
            "driver_type": _r.choice(driver_types),
            "contribution_pct": round(_r.uniform(2, 18), 1),
            "year": _r.choice([2022, 2023, 2024]),
            "region": _r.choice(regions),
            "events_caused": _r.randint(3, 50),
            "avg_price_impact_mwh": round(_r.uniform(50, 3000), 1),
            "seasonal_pattern": _r.choice(seasonal_patterns),
        })

    price_distribution = []
    for region in regions:
        for year in [2023, 2024]:
            for band in price_bands:
                base_hrs = {"Negative": 150, "$0-$50": 4000, "$50-$300": 3500, "$300+": 200}[band]
                hrs = base_hrs + _r.randint(-100, 200)
                price_distribution.append({
                    "region": region,
                    "year": year,
                    "price_band": band,
                    "hours_count": hrs,
                    "percentage_of_year": round(hrs / 8760 * 100, 2),
                    "avg_price_mwh": round(
                        {"Negative": -45, "$0-$50": 28, "$50-$300": 120, "$300+": 2500}[band]
                        + _r.uniform(-10, 10), 1
                    ),
                    "volume_mwh": round(hrs * _r.uniform(3000, 8000), 0),
                })

    measure_names = [
        "FCAS Raise Dispatch", "RERT Activation", "Demand Side Programme",
        "Battery Emergency Discharge", "Interconnector Emergency",
        "AEMO Manual Direction", "FCAS Lower Service", "DR Industrial",
    ]
    mitigation = []
    for i, mname in enumerate(measure_names):
        mitigation.append({
            "measure_id": f"MIT-{i + 1:03d}",
            "measure_name": mname,
            "measure_type": _r.choice(measure_types),
            "region": _r.choice(regions),
            "year": _r.choice([2023, 2024]),
            "activations": _r.randint(5, 200),
            "mw_dispatched": round(_r.uniform(50, 2000), 0),
            "cost_m_aud": round(_r.uniform(0.5, 30), 1),
            "effectiveness_pct": round(_r.uniform(25, 95), 1),
        })

    total_spike_rev = round(sum(rs["total_spike_revenue_m_aud"] for rs in regional_stats if rs["year"] == 2024), 1)
    biggest_event = max(events, key=lambda e: e["financial_impact_m_aud"])
    most_volatile = max(
        regions,
        key=lambda rg: sum(
            rs["volatility_index"] for rs in regional_stats
            if rs["region"] == rg and rs["year"] == 2024
        ),
    )

    summary = {
        "total_events_2024": sum(1 for e in events if e["year"] == 2024),
        "total_spike_revenue_m_aud": total_spike_rev,
        "most_volatile_region": most_volatile,
        "avg_negative_price_hrs_pa": round(
            sum(pd["hours_count"] for pd in price_distribution if pd["price_band"] == "Negative")
            / max(len(set(pd["year"] for pd in price_distribution)), 1)
            / max(len(regions), 1), 1
        ),
        "total_cap_hit_events": sum(rs["cap_hit_count"] for rs in regional_stats if rs["year"] == 2024),
        "biggest_single_event_m_aud": biggest_event["financial_impact_m_aud"],
        "avg_event_duration_intervals": round(
            sum(e["duration_intervals"] for e in events) / max(len(events), 1), 1
        ),
    }

    return {
        "events": events,
        "regional_stats": regional_stats,
        "drivers": espe_drivers,
        "price_distribution": price_distribution,
        "mitigation": mitigation,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# 10) /api/price-model-comparison/dashboard  ->  PMCDashboard
# ---------------------------------------------------------------------------

@router.get("/api/price-model-comparison/dashboard")
def price_model_comparison_dashboard():
    _r.seed(1010)
    regions_pmc = ["NSW", "VIC", "QLD", "SA", "WA"]
    model_families = ["STATISTICAL", "ML", "DEEP_LEARNING", "HYBRID", "FUNDAMENTAL", "EXPERT_SYSTEM"]
    complexities = ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]
    accuracy_req = ["HIGH", "MEDIUM", "LOW"]
    feature_categories = ["DEMAND", "GENERATION", "WEATHER", "FUEL", "CALENDAR", "MARKET"]
    scenario_list = ["NORMAL", "HIGH_VRE", "PRICE_SPIKE", "MARKET_STRESS", "NEGATIVE_PRICE"]

    model_defs = [
        ("M001", "ARIMA-GARCH", "STATISTICAL", "ARIMA(2,1,2)-GARCH(1,1)", "MEDIUM", 24, 2.5, "WEEKLY", None),
        ("M002", "XGBoost-Price", "ML", "XGBoost Regressor", "MEDIUM", 48, 8.0, "DAILY", None),
        ("M003", "LSTM-Seq2Seq", "DEEP_LEARNING", "LSTM Encoder-Decoder", "HIGH", 24, 45.0, "WEEKLY", None),
        ("M004", "Prophet-Additive", "STATISTICAL", "Facebook Prophet", "LOW", 168, 1.5, "MONTHLY", None),
        ("M005", "Dispatch-Sim", "FUNDAMENTAL", "Merit-order dispatch simulation", "VERY_HIGH", 48, 120.0, "MONTHLY", "AEMO"),
        ("M006", "Ensemble-Stack", "HYBRID", "Stacked ensemble (XGB+LSTM+ARIMA)", "HIGH", 24, 55.0, "DAILY", None),
        ("M007", "SARIMA-GARCH", "STATISTICAL", "SARIMA(1,1,1)(1,1,1)[48]-GARCH", "MEDIUM", 48, 5.0, "WEEKLY", None),
        ("M008", "Transformer-NEM", "DEEP_LEARNING", "Temporal Fusion Transformer", "VERY_HIGH", 24, 90.0, "WEEKLY", "Google DeepMind"),
    ]

    models = []
    for mid, mname, mfamily, algo, complexity, horizon, compute, freq, vendor in model_defs:
        features = _r.sample(
            ["demand", "temperature", "wind_gen", "solar_gen", "gas_price",
             "coal_gen", "interconnector", "time_of_day", "lagged_price", "calendar"],
            k=_r.randint(4, 8),
        )
        models.append({
            "model_id": mid,
            "model_name": mname,
            "model_family": mfamily,
            "algorithm": algo,
            "input_features": features,
            "training_frequency": freq,
            "forecast_horizon_hrs": horizon,
            "compute_time_mins": compute,
            "model_complexity": complexity,
            "commercial_vendor": vendor,
        })

    accuracy = []
    for m in models:
        for region in regions_pmc[:3]:
            for year in [2024]:
                for horizon in [1, 24]:
                    base_mae = {"STATISTICAL": 12, "ML": 8, "DEEP_LEARNING": 7, "HYBRID": 6, "FUNDAMENTAL": 15, "EXPERT_SYSTEM": 18}[m["model_family"]]
                    mae = round(base_mae + horizon * 0.3 + _r.uniform(-3, 5), 1)
                    accuracy.append({
                        "model_id": m["model_id"],
                        "region": region,
                        "year": year,
                        "horizon_hrs": horizon,
                        "mae": mae,
                        "rmse": round(mae * _r.uniform(1.2, 1.8), 1),
                        "mape": round(_r.uniform(5, 35), 1),
                        "r_squared": round(_r.uniform(0.55, 0.95), 2),
                        "spike_detection_rate_pct": round(_r.uniform(20, 90), 1),
                        "directional_accuracy_pct": round(_r.uniform(55, 85), 1),
                        "pit_coverage_pct": round(_r.uniform(80, 98), 1),
                    })

    use_cases = [
        ("INTRADAY_TRADING", "ML", "HIGH", 4, 120, 72.5),
        ("DAY_AHEAD_BIDDING", "HYBRID", "HIGH", 24, 85, 68.0),
        ("HEDGING_STRATEGY", "STATISTICAL", "MEDIUM", 8760, 45, 55.0),
        ("RENEWABLE_PPA_PRICING", "FUNDAMENTAL", "MEDIUM", 17520, 60, 40.0),
        ("BATTERY_DISPATCH", "ML", "HIGH", 4, 95, 80.0),
        ("RETAIL_PRICING", "STATISTICAL", "LOW", 168, 30, 65.0),
    ]
    commercial_uses = []
    for uc, fam, acc, horiz, val, adopt in use_cases:
        commercial_uses.append({
            "use_case": uc,
            "preferred_model_family": fam,
            "accuracy_requirement": acc,
            "horizon_needed_hrs": horiz,
            "annual_value_m": val,
            "adoption_pct": adopt,
        })

    feature_names = [
        "demand_forecast", "temperature", "wind_generation",
        "solar_generation", "gas_price", "coal_generation",
        "interconnector_flow", "time_of_day", "lagged_price",
        "calendar_effects", "battery_soc",
    ]
    feature_importance = []
    for m in models[:4]:
        remaining = 100.0
        for fi, fname in enumerate(feature_names[:6]):
            imp = round(min(remaining, _r.uniform(2, 20)), 1)
            remaining -= imp
            feature_importance.append({
                "model_id": m["model_id"],
                "feature": fname,
                "importance_pct": imp,
                "feature_category": _r.choice(feature_categories),
            })

    backtests = []
    for m in models:
        for scenario in scenario_list:
            for region in regions_pmc[:2]:
                rank = _r.randint(1, len(models))
                backtests.append({
                    "model_id": m["model_id"],
                    "backtest_period": "2022-01 to 2024-06",
                    "region": region,
                    "scenario": scenario,
                    "mae_normal": round(_r.uniform(5, 20), 1),
                    "mae_spike": round(_r.uniform(50, 500), 1),
                    "mae_negative": round(_r.uniform(20, 150), 1),
                    "overall_rank": rank,
                })

    # Make ranks per scenario unique
    for scenario in scenario_list:
        for region in regions_pmc[:2]:
            scenario_backtests = [b for b in backtests if b["scenario"] == scenario and b["region"] == region]
            scenario_backtests.sort(key=lambda b: b["mae_normal"] + b["mae_spike"])
            for rank, bt in enumerate(scenario_backtests, 1):
                bt["overall_rank"] = rank

    best_mae_model = min(
        accuracy, key=lambda a: a["mae"] if a["year"] == 2024 and a["horizon_hrs"] == 24 else 999
    )
    best_spike_model = max(
        accuracy, key=lambda a: a["spike_detection_rate_pct"] if a["year"] == 2024 else 0
    )
    avg_mae = round(
        sum(a["mae"] for a in accuracy if a["year"] == 2024 and a["horizon_hrs"] == 24)
        / max(sum(1 for a in accuracy if a["year"] == 2024 and a["horizon_hrs"] == 24), 1), 1
    )
    total_annual_value = sum(cu["annual_value_m"] for cu in commercial_uses)
    avg_adoption = round(sum(cu["adoption_pct"] for cu in commercial_uses) / max(len(commercial_uses), 1), 1)

    return {
        "models": models,
        "accuracy": accuracy,
        "commercial_uses": commercial_uses,
        "feature_importance": feature_importance,
        "backtests": backtests,
        "summary": {
            "best_model_mae": best_mae_model["model_id"],
            "best_spike_model": best_spike_model["model_id"],
            "avg_mae_all_models": avg_mae,
            "spike_detection_leader_pct": best_spike_model["spike_detection_rate_pct"],
            "commercial_adoption_pct": avg_adoption,
            "annual_forecast_value_m": total_annual_value,
        },
    }


# ---------------------------------------------------------------------------
# 1) /api/electricity-price-index/dashboard  ->  ElectricityPriceIndexDashboard
# ---------------------------------------------------------------------------

@router.get("/api/electricity-price-index/dashboard")
def electricity_price_index_dashboard():
    _r.seed(201)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    states = ["NSW", "VIC", "QLD", "SA", "WA"]
    quarters = ["2023-Q1", "2023-Q2", "2023-Q3", "2023-Q4", "2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4"]
    drivers = [
        "Wholesale cost increase", "Network investment", "Renewable transition",
        "Gas price spike", "Demand growth", "Carbon policy",
    ]

    cpi_records = []
    for q in quarters:
        for s in states:
            elec_idx = round(_r.uniform(140, 210), 1)
            all_idx = round(_r.uniform(120, 145), 1)
            elec_yoy = round(_r.uniform(-5, 25), 1)
            all_yoy = round(_r.uniform(2, 7), 1)
            cpi_records.append({
                "quarter": q,
                "electricity_cpi_index": elec_idx,
                "electricity_cpi_yoy_pct": elec_yoy,
                "all_cpi_index": all_idx,
                "all_cpi_yoy_pct": all_yoy,
                "electricity_vs_all_cpi_diff_pct": round(elec_yoy - all_yoy, 1),
                "state": s,
                "key_driver": _r.choice(drivers),
            })

    dist_zones = ["Ausgrid", "Endeavour", "Essential", "Energex", "Ergon", "CitiPower", "Powercor", "SA Power", "Western Power"]
    dmo_records = []
    for yr in [2022, 2023, 2024]:
        for s in states[:4]:
            zone = _r.choice(dist_zones)
            usage = _r.choice([3900, 4600, 5500, 6200])
            dmo = round(_r.uniform(1200, 2200), 2)
            market_avg = round(dmo * _r.uniform(0.82, 0.98), 2)
            best = round(market_avg * _r.uniform(0.85, 0.95), 2)
            dmo_records.append({
                "year": yr,
                "state": s,
                "distribution_zone": zone,
                "annual_usage_kwh": usage,
                "dmo_price_aud": dmo,
                "dmo_change_pct": round(_r.uniform(-8, 25), 1),
                "market_offer_avg_aud": market_avg,
                "market_offer_avg_change_pct": round(_r.uniform(-10, 20), 1),
                "best_market_offer_aud": best,
                "potential_saving_aud": round(dmo - best, 2),
            })

    tariff_components = []
    for yr in [2022, 2023, 2024]:
        for s in states[:4]:
            nw = round(_r.uniform(0.08, 0.14), 4)
            wh = round(_r.uniform(0.06, 0.12), 4)
            env = round(_r.uniform(0.01, 0.03), 4)
            ret = round(_r.uniform(0.01, 0.04), 4)
            met = round(_r.uniform(0.005, 0.015), 4)
            tariff_components.append({
                "state": s,
                "year": yr,
                "network_charges_aud_kwh": nw,
                "wholesale_charges_aud_kwh": wh,
                "environmental_charges_aud_kwh": env,
                "retail_margin_aud_kwh": ret,
                "metering_aud_kwh": met,
                "total_tariff_aud_kwh": round(nw + wh + env + ret + met, 4),
            })

    retailer_names = ["AGL", "Origin Energy", "EnergyAustralia", "Alinta", "Red Energy", "Lumo"]
    retailers = []
    for ret_name in retailer_names:
        for s in states[:4]:
            avg_offer = round(_r.uniform(1100, 2000), 2)
            retailers.append({
                "retailer": ret_name,
                "state": s,
                "market_share_pct": round(_r.uniform(3, 32), 1),
                "avg_offer_aud": avg_offer,
                "cheapest_offer_aud": round(avg_offer * _r.uniform(0.82, 0.95), 2),
                "customer_satisfaction_score": round(_r.uniform(55, 85), 1),
                "complaints_per_1000": round(_r.uniform(2, 18), 1),
                "churn_rate_pct": round(_r.uniform(8, 28), 1),
            })

    return {
        "timestamp": _dt.utcnow().isoformat() + "Z",
        "cpi_records": cpi_records,
        "dmo_records": dmo_records,
        "tariff_components": tariff_components,
        "retailers": retailers,
    }


# ---------------------------------------------------------------------------
# 2) /api/market-price-formation-review/dashboard  ->  MPFRDashboard
# ---------------------------------------------------------------------------

@router.get("/api/market-price-formation-review/dashboard")
def market_price_formation_review_dashboard():
    _r.seed(202)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    states = ["NSW", "VIC", "QLD", "SA", "WA"]
    scarcity_types = [
        "Lack of Reserve 1", "LOR2", "LOR3", "Pre-LOR",
        "Emergency", "VoLL", "RERT", "Voluntary", "Market Suspended",
    ]
    customer_classes = ["Residential", "Small Business", "Commercial", "Industrial", "Agricultural"]
    reform_names = [
        "Capacity Investment Scheme", "Post-2025 Market Design",
        "Transmission Access Reform", "Congestion Management Model",
        "Operating Reserve Demand Curve", "Strategic Reserve Enhancement",
    ]
    consultation_stages = ["Draft Rule", "Final Determination", "Implementation", "Review", "Consultation"]
    outcomes = ["Approved", "Under Review", "Deferred", "Partially Approved"]

    price_caps = []
    for yr in [2022, 2023, 2024]:
        for r in regions:
            price_caps.append({
                "region": r,
                "year": yr,
                "market_price_cap_mwh": 16600.0,
                "administered_price_cap_mwh": 300.0,
                "voll_mwh": round(_r.uniform(33000, 40000), 0),
            })

    scarcity_events = []
    for _ in range(20):
        scarcity_events.append({
            "region": _r.choice(regions),
            "scarcity_type": _r.choice(scarcity_types),
        })

    vcr_values = []
    for cc in customer_classes:
        for s in states:
            vcr_values.append({
                "customer_class": cc,
                "state": s,
                "vcr_aud_per_mwh": round(_r.uniform(12000, 45000), 0),
            })

    reforms = []
    for name in reform_names:
        reforms.append({
            "reform_name": name,
            "consultation_stage": _r.choice(consultation_stages),
            "outcome": _r.choice(outcomes),
            "financial_impact_consumer_m": round(_r.uniform(-200, 500), 1),
        })

    quarters = ["2023-Q1", "2023-Q2", "2023-Q3", "2023-Q4", "2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4"]
    marginal_costs = []
    for r in regions:
        for q in quarters:
            fuel = round(_r.uniform(20, 60), 2)
            carbon = round(_r.uniform(2, 15), 2)
            cap = round(_r.uniform(1, 10), 2)
            scarcity = round(_r.uniform(0, 8), 2)
            actual = round(fuel + carbon + cap + scarcity + _r.uniform(-10, 20), 2)
            marginal_costs.append({
                "region": r,
                "quarter": q,
                "fuel_cost_contribution_mwh": fuel,
                "carbon_cost_contribution_mwh": carbon,
                "capacity_adequacy_premium_mwh": cap,
                "scarcity_premium_mwh": scarcity,
                "actual_spot_price_mwh": actual,
            })

    demand_side = []
    for r in regions:
        for yr in [2022, 2023, 2024]:
            demand_side.append({
                "region": r,
                "year": yr,
                "demand_response_capacity_mw": round(_r.uniform(50, 600), 0),
            })

    return {
        "price_caps": price_caps,
        "scarcity_events": scarcity_events,
        "vcr_values": vcr_values,
        "reforms": reforms,
        "marginal_costs": marginal_costs,
        "demand_side": demand_side,
        "summary": {
            "current_mpc_mwh": 16600.0,
            "total_apc_activations_ytd": _r.randint(2, 12),
            "total_scarcity_cost_m": round(_r.uniform(50, 350), 1),
            "avg_vcr_residential_aud_per_mwh": round(_r.uniform(25000, 38000), 0),
            "active_reforms_count": len(reform_names),
            "demand_response_capacity_mw": round(_r.uniform(800, 2200), 0),
        },
    }


# ---------------------------------------------------------------------------
# 3) /api/electricity-market-price-formation/dashboard  ->  EMPFDashboard
# ---------------------------------------------------------------------------

@router.get("/api/electricity-market-price-formation/dashboard")
def electricity_market_price_formation_dashboard():
    _r.seed(203)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    months = [f"2024-{m:02d}" for m in range(1, 13)]
    fuel_types = ["Black Coal", "Brown Coal", "Gas CCGT", "Gas OCGT", "Hydro", "Wind", "Solar"]
    event_types = ["Price Spike >$300", "Negative Price", "Administered Price", "VOLL"]
    participants = ["AGL", "Origin", "Snowy Hydro", "EnergyAustralia"]
    technologies = ["New Entrant CCGT", "New Entrant Wind", "New Entrant Solar", "New Entrant BESS"]
    investment_signals = ["Invest", "Marginal", "Do Not Invest", "Under Review"]

    price_drivers = []
    for r in regions:
        for m in months[:6]:
            price_drivers.append({
                "region": r,
                "month": m,
                "fuel_cost_component": round(_r.uniform(15, 55), 2),
                "carbon_cost_component": round(_r.uniform(2, 12), 2),
                "capacity_scarcity_component": round(_r.uniform(0, 15), 2),
                "network_constraint_component": round(_r.uniform(1, 10), 2),
                "renewable_suppression_component": round(_r.uniform(-8, 0), 2),
                "fcas_cost_component": round(_r.uniform(0.5, 5), 2),
            })

    marginal_units = []
    for ft in fuel_types:
        marginal_units.append({
            "fuel_type": ft,
            "pct_intervals_marginal": round(_r.uniform(2, 35), 1),
        })

    price_events = []
    for _ in range(15):
        price_events.append({
            "peak_price": round(_r.uniform(300, 16600), 2),
            "duration_intervals": _r.randint(1, 24),
            "total_cost_m": round(_r.uniform(0.5, 45), 2),
            "event_type": _r.choice(event_types),
            "region": _r.choice(regions),
        })

    quarters = ["2023-Q1", "2023-Q2", "2023-Q3", "2023-Q4", "2024-Q1", "2024-Q2"]
    bidding_behaviour = []
    for q in quarters:
        for p in participants:
            bidding_behaviour.append({
                "quarter": q,
                "participant": p,
                "capacity_bid_at_voll_pct": round(_r.uniform(0, 15), 1),
            })

    long_run_costs = []
    for tech in technologies:
        for r in regions:
            lrmc = round(_r.uniform(45, 120), 2)
            req_cf = round(_r.uniform(15, 60), 1)
            cur_cf = round(req_cf * _r.uniform(0.6, 1.2), 1)
            long_run_costs.append({
                "technology": tech,
                "region": r,
                "lrmc_per_mwh": lrmc,
                "required_capacity_factor_pct": req_cf,
                "current_cf_pct": cur_cf,
                "investment_signal": _r.choice(investment_signals),
            })

    return {
        "price_drivers": price_drivers,
        "marginal_units": marginal_units,
        "price_events": price_events,
        "bidding_behaviour": bidding_behaviour,
        "long_run_costs": long_run_costs,
        "summary": {
            "avg_spot_price_mwh": round(_r.uniform(55, 120), 2),
            "most_frequent_price_setter": _r.choice(fuel_types),
            "price_spike_events_ytd": _r.randint(15, 85),
            "avg_lrmc_new_wind": round(_r.uniform(50, 75), 2),
            "most_concentrated_region": _r.choice(regions),
        },
    }


# ---------------------------------------------------------------------------
# 4) /api/electricity-price-cap-intervention/dashboard  ->  EPCIDashboard
# ---------------------------------------------------------------------------

@router.get("/api/electricity-price-cap-intervention/dashboard")
def electricity_price_cap_intervention_dashboard():
    _r.seed(204)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    trigger_types = ["MPC Activated", "CPT Breach", "RERT Trigger", "APC Price"]
    action_types = ["RERT Activation", "DR Activation", "Interconnector Dispatch", "Reserve Trader"]
    action_outcomes = ["Prevented Cap", "Reduced Duration", "Ineffective"]
    generators = ["AGL Macquarie", "Origin Eraring", "Stanwell Corp", "Snowy Hydro", "CS Energy", "EnergyAustralia Yallourn"]

    # Try real extreme price events for cap/intervention data
    try:
        cap_rows = _query_gold(f"""
            SELECT region_id, rrp, interval_datetime
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE rrp > 300 OR rrp < -100
            ORDER BY interval_datetime DESC
            LIMIT 50
        """)
    except Exception:
        cap_rows = None

    price_cap_events = []
    if cap_rows:
        for r in cap_rows[:18]:
            price = float(r.get("rrp") or 0)
            if price > 15000:
                trig = "MPC Activated"
            elif price > 5000:
                trig = "CPT Breach"
            elif price > 300:
                trig = "RERT Trigger"
            else:
                trig = "APC Price"
            price_cap_events.append({
                "trigger_type": trig,
                "spot_price_avg_aud_mwh": round(price, 2),
            })
        total_cap = len(cap_rows)
        most_affected = max(set(r.get("region_id", "SA1") for r in cap_rows),
                           key=lambda rg: sum(1 for r in cap_rows if r.get("region_id") == rg))
    else:
        for _ in range(18):
            price_cap_events.append({
                "trigger_type": _r.choice(trigger_types),
                "spot_price_avg_aud_mwh": round(_r.uniform(300, 16600), 2),
            })
        total_cap = _r.randint(10, 45)
        most_affected = _r.choice(regions)

    # Try real cumulative price tracker
    try:
        cum_rows = _query_gold(f"""
            SELECT region_id,
                   SUM(rrp) AS cumulative_price,
                   SUM(CASE WHEN rrp > 300 THEN 1 ELSE 0 END) AS cap_events
            FROM {_CATALOG}.gold.nem_prices_5min
            GROUP BY region_id
        """)
    except Exception:
        cum_rows = None

    threshold_tracker = []
    if cum_rows:
        for cr in cum_rows:
            reg = cr.get("region_id", "NSW1")
            if reg not in regions[:3]:
                continue
            cum_price = float(cr.get("cumulative_price") or 0)
            threshold_tracker.append({
                "year": 2026, "month": 3, "region": reg,
                "cumulative_price_atd": round(cum_price, 0),
            })
    else:
        for yr in [2023, 2024]:
            for m in range(1, 13):
                for r in regions[:3]:
                    threshold_tracker.append({
                        "year": yr, "month": m, "region": r,
                        "cumulative_price_atd": round(_r.uniform(50000, 1200000), 0),
                    })

    market_impact = []
    for yr in [2022, 2023, 2024]:
        for q in ["Q1", "Q2", "Q3", "Q4"]:
            for r in regions[:3]:
                market_impact.append({
                    "year": yr, "quarter": q, "region": r,
                    "cap_events_count": _r.randint(0, 8),
                })

    generator_response = []
    for yr in [2022, 2023, 2024]:
        for g in generators:
            generator_response.append({
                "year": yr, "generator": g,
                "revenue_impact_m": round(_r.uniform(-50, 30), 1),
            })

    remedy_actions = []
    for _ in range(15):
        remedy_actions.append({
            "action_type": _r.choice(action_types),
            "outcome": _r.choice(action_outcomes),
            "capacity_mw": round(_r.uniform(50, 800), 0),
        })

    return {
        "price_cap_events": price_cap_events,
        "market_impact": market_impact,
        "generator_response": generator_response,
        "threshold_tracker": threshold_tracker,
        "remedy_actions": remedy_actions,
        "summary": {
            "total_cap_events_fy": total_cap if cap_rows else _r.randint(10, 45),
            "total_hours_at_cap_fy": round(total_cap * 5 / 60, 1) if cap_rows else round(_r.uniform(5, 80), 1),
            "max_administered_price_aud_mwh": 300.0,
            "total_consumer_savings_m": round(_r.uniform(100, 800), 1),
            "most_affected_region": most_affected,
        },
    }


# ---------------------------------------------------------------------------
# 5) /api/nem-price-review/dashboard  ->  NEPRdashboard
# ---------------------------------------------------------------------------

@router.get("/api/nem-price-review/dashboard")
def nem_price_review_dashboard():
    _r.seed(205)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    states = ["NSW", "VIC", "QLD", "SA", "WA", "TAS"]

    spot_prices = []
    for r in regions:
        for yr in [2022, 2023, 2024]:
            for m in range(1, 13):
                spot_prices.append({
                    "region": r,
                    "year": yr,
                    "month": m,
                    "avg_spot_price_mwh": round(_r.uniform(30, 180), 2),
                })

    retail_prices = []
    for s in states:
        for yr in [2022, 2023, 2024]:
            retail_prices.append({
                "state": s,
                "year": yr,
                "avg_residential_c_kwh": round(_r.uniform(22, 42), 2),
                "avg_sme_c_kwh": round(_r.uniform(18, 36), 2),
            })

    quarters = ["Q1", "Q2", "Q3", "Q4"]
    price_drivers = []
    for r in regions:
        for yr in [2023, 2024]:
            for q in quarters:
                wh = round(_r.uniform(25, 45), 1)
                nw = round(_r.uniform(30, 45), 1)
                env = round(_r.uniform(5, 15), 1)
                ret = round(_r.uniform(5, 12), 1)
                oth = round(100 - wh - nw - env - ret, 1)
                price_drivers.append({
                    "region": r,
                    "year": yr,
                    "quarter": q,
                    "wholesale_component_pct": wh,
                    "network_component_pct": nw,
                    "environmental_component_pct": env,
                    "retail_margin_pct": ret,
                    "other_pct": oth,
                })

    affordability = []
    for s in states:
        for yr in [2022, 2023, 2024]:
            affordability.append({
                "state": s,
                "year": yr,
                "avg_household_bill_aud": round(_r.uniform(1200, 2600), 0),
                "energy_poverty_pct": round(_r.uniform(5, 22), 1),
            })

    return {
        "spot_prices": spot_prices,
        "retail_prices": retail_prices,
        "price_drivers": price_drivers,
        "affordability": affordability,
        "summary": {
            "avg_spot_price_2024_mwh": round(_r.uniform(60, 110), 2),
            "avg_retail_price_2024_c_kwh": round(_r.uniform(28, 38), 2),
            "highest_price_region": _r.choice(regions),
            "total_cap_price_events": _r.randint(8, 40),
        },
    }


# ---------------------------------------------------------------------------
# 6) /api/demand-curve-price-anchor/dashboard  ->  DCPAdashboard
# ---------------------------------------------------------------------------

@router.get("/api/demand-curve-price-anchor/dashboard")
def demand_curve_price_anchor_dashboard():
    _r.seed(206)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    sectors = ["Residential", "Commercial", "Industrial", "Agricultural"]
    quarters = ["Q1", "Q2", "Q3", "Q4"]

    demand = []
    for yr in [2022, 2023, 2024]:
        for m in range(1, 13):
            for r in regions:
                demand.append({
                    "year": yr,
                    "month": m,
                    "region": r,
                    "peak_demand_mw": round(_r.uniform(3000, 14000), 0),
                })

    price_anchors = []
    for yr in [2022, 2023, 2024]:
        for q in quarters:
            for r in regions:
                vwap = round(_r.uniform(50, 150), 2)
                twa = round(_r.uniform(40, 130), 2)
                price_anchors.append({
                    "year": yr,
                    "quarter": q,
                    "region": r,
                    "vwap_mwh": vwap,
                    "twa_price_mwh": twa,
                    "price_anchor_mwh": round((vwap + twa) / 2, 2),
                })

    elasticity = []
    for sec in sectors:
        elasticity.append({
            "sector": sec,
            "price_elasticity": round(_r.uniform(-0.8, -0.05), 3),
        })

    forecasts = []
    for yr in [2024, 2025]:
        for q in quarters:
            for r in regions:
                forecast = round(_r.uniform(4000, 13500), 0)
                actual = round(forecast * _r.uniform(0.9, 1.1), 0)
                forecasts.append({
                    "year": yr,
                    "quarter": q,
                    "region": r,
                    "forecast_demand_mw": forecast,
                    "actual_demand_mw": actual,
                })

    return {
        "demand": demand,
        "price_anchors": price_anchors,
        "elasticity": elasticity,
        "forecasts": forecasts,
        "summary": {
            "avg_peak_demand_mw": round(_r.uniform(7000, 10000), 0),
            "avg_price_anchor_mwh": round(_r.uniform(65, 95), 2),
            "most_elastic_region": _r.choice(regions),
            "total_demand_response_mw": round(_r.uniform(800, 2500), 0),
        },
    }


# ---------------------------------------------------------------------------
# /api/market-stress/dashboard  ->  MarketStressDashboard
# ---------------------------------------------------------------------------

@router.get("/api/market-stress/dashboard")
def market_stress_dashboard():
    _r.seed(777)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    severities = ["MILD", "MODERATE", "SEVERE", "EXTREME"]
    triggers = [
        "Heatwave exceeding 45\u00b0C across eastern seaboard",
        "Simultaneous coal unit trips (3+ units)",
        "Gas supply disruption \u2014 Longford plant outage",
        "Basslink interconnector failure",
        "Wind drought across SA and VIC (48hr+)",
        "Cyber attack on SCADA systems",
        "Major bushfire cutting transmission lines",
        "LNG export surge reducing domestic gas supply",
    ]
    scenario_names = [
        "Eastern Seaboard Heatwave", "Multi-Unit Coal Trip", "Gas Supply Shock",
        "Basslink Outage", "Prolonged Wind Drought", "Cyber Disruption",
        "Bushfire Transmission Loss", "LNG Export Squeeze",
    ]

    scenarios = []
    for i, name in enumerate(scenario_names):
        scenarios.append({
            "scenario_id": f"STRESS-{i+1:03d}",
            "name": name,
            "description": f"Stress test scenario: {name.lower()}",
            "trigger_event": triggers[i],
            "severity": severities[min(i // 2, 3)],
            "probability_pct": round(_r.uniform(2, 35), 1),
            "duration_days": _r.randint(1, 14),
        })

    metrics = ["PRICE", "AVAILABILITY", "RELIABILITY", "REVENUE"]
    results = []
    for sc in scenarios[:5]:
        for reg in regions[:3]:
            for met in metrics:
                baseline = round(_r.uniform(50, 500), 2) if met == "PRICE" else round(_r.uniform(85, 99), 1)
                impact = round(_r.uniform(5, 120), 1)
                results.append({
                    "scenario_id": sc["scenario_id"],
                    "region": reg,
                    "metric": met,
                    "baseline_value": baseline,
                    "stressed_value": round(baseline * (1 + impact / 100), 2),
                    "impact_pct": impact,
                    "recovery_days": _r.randint(1, 21),
                })

    components = [
        "Coal Fleet (aging)", "Gas Pipeline Network", "Interconnector Capacity",
        "Rooftop Solar Inverters", "BESS Fleet", "SCADA/EMS Systems",
        "Market IT Systems", "Transmission Towers (bushfire zones)",
    ]
    vulnerabilities = []
    for comp in components:
        vulnerabilities.append({
            "component": comp,
            "vulnerability_score": round(_r.uniform(0.2, 0.95), 2),
            "single_point_of_failure": _r.random() > 0.6,
            "mitigation_status": _r.choice(["Mitigated", "Partial", "Unmitigated", "Under Review"]),
        })

    kpis = []
    for sc in scenarios[:6]:
        kpis.append({
            "scenario": sc["name"],
            "avg_price_spike_pct": round(_r.uniform(50, 800), 1),
            "max_price_aud_mwh": round(_r.uniform(1000, 16600), 0),
            "unserved_energy_mwh": round(_r.uniform(0, 5000), 0),
            "affected_consumers_k": round(_r.uniform(10, 2500), 0),
            "economic_cost_m_aud": round(_r.uniform(5, 500), 1),
        })

    return {
        "timestamp": _dt.utcnow().isoformat() + "Z",
        "scenarios": scenarios,
        "results": results,
        "vulnerabilities": vulnerabilities,
        "kpis": kpis,
    }


# ---------------------------------------------------------------------------
# /api/demand-forecast/dashboard  ->  DemandForecastDashboard
# ---------------------------------------------------------------------------

@router.get("/api/demand-forecast/dashboard")
def demand_forecast_dashboard():
    _r.seed(888)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    models = ["AEMO_ST_PASA", "AEMO_MT_PASA", "ML_ENHANCED"]
    conditions = ["HOT", "MODERATE", "COLD", "STORM"]
    horizons = [1, 24, 168]
    base_demands = {"NSW1": 8500, "QLD1": 6200, "VIC1": 5800, "SA1": 1800, "TAS1": 1100}

    forecast_records = []
    for reg in regions:
        base = base_demands[reg]
        for h in horizons:
            for d in range(5):
                actual = base + _r.uniform(-800, 800)
                error_scale = 1 + h * 0.02
                forecast = actual + _r.uniform(-200, 200) * error_scale
                error = forecast - actual
                mae_pct = round(abs(error) / actual * 100, 2)
                forecast_records.append({
                    "region": reg,
                    "forecast_date": (_dt(2024, 7, 1) + _td(days=d)).strftime("%Y-%m-%d"),
                    "forecast_horizon_h": h,
                    "forecast_mw": round(forecast, 1),
                    "actual_mw": round(actual, 1),
                    "error_mw": round(error, 1),
                    "mae_pct": mae_pct,
                    "forecast_model": _r.choice(models),
                    "temperature_c": round(_r.uniform(8, 42), 1),
                    "conditions": _r.choice(conditions),
                })

    pasa_records = []
    months = ["2024-07", "2024-08", "2024-09", "2024-10", "2024-11", "2024-12"]
    for reg in regions:
        base = base_demands[reg]
        for m in months:
            cap = base * _r.uniform(1.15, 1.45)
            d10 = base * _r.uniform(1.05, 1.25)
            d50 = base * _r.uniform(0.85, 1.05)
            margin = round((cap - d10) / d10 * 100, 1)
            pasa_records.append({
                "region": reg,
                "month": m,
                "reserve_margin_pct": margin,
                "ues_mwh": round(_r.uniform(0, 50), 1) if margin < 10 else 0.0,
                "lrc_mw": round(cap - d10, 0),
                "capacity_available_mw": round(cap, 0),
                "demand_10poe_mw": round(d10, 0),
                "demand_50poe_mw": round(d50, 0),
                "reliability_standard_met": margin >= 10,
            })

    mae_1h = round(sum(r["mae_pct"] for r in forecast_records if r["forecast_horizon_h"] == 1) / max(1, sum(1 for r in forecast_records if r["forecast_horizon_h"] == 1)), 2)
    mae_24h = round(sum(r["mae_pct"] for r in forecast_records if r["forecast_horizon_h"] == 24) / max(1, sum(1 for r in forecast_records if r["forecast_horizon_h"] == 24)), 2)
    mae_168h = round(sum(r["mae_pct"] for r in forecast_records if r["forecast_horizon_h"] == 168) / max(1, sum(1 for r in forecast_records if r["forecast_horizon_h"] == 168)), 2)

    return {
        "timestamp": _dt.utcnow().isoformat() + "Z",
        "regions": regions,
        "avg_mae_1h_pct": mae_1h,
        "avg_mae_24h_pct": mae_24h,
        "avg_mae_168h_pct": mae_168h,
        "forecast_records": forecast_records,
        "pasa_records": pasa_records,
    }
