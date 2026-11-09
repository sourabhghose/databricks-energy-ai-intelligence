"""Forecasting dashboards — every number is computed from real data.

Sources (see forecast_metrics.py):
* gold.forecast_model_metrics / forecast_backtest / forecast_feature_importance — held-out back-test of the
  LightGBM price, spike and demand models trained on real AEMO NEMWEB data (models/nem_forecast/train.py)
* gold.nem_prices_5min — real 5-minute prices and demand

Fields with no real source (e.g. training wall-clock time, weather error) are returned as null or as
empty lists, and endpoints return HTTP 503 rather than illustrative numbers when the tables are empty.
"""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, datetime, timezone
from statistics import mean

from fastapi import APIRouter, HTTPException

from . import forecast_metrics as fm

router = APIRouter()
AEST = fm.AEST
DATE_SQL = f"to_date(from_utc_timestamp(interval_datetime, {AEST}))"
YEAR_SQL = f"year(from_utc_timestamp(interval_datetime, {AEST}))"
QUARTER_SQL = f"quarter(from_utc_timestamp(interval_datetime, {AEST}))"
MONTH_SQL = f"month(from_utc_timestamp(interval_datetime, {AEST}))"
TOD_SQL = (f"CASE WHEN hour(from_utc_timestamp(interval_datetime, {AEST})) < 6 THEN 'overnight' "
           f"WHEN hour(from_utc_timestamp(interval_datetime, {AEST})) < 11 THEN 'morning' "
           f"WHEN hour(from_utc_timestamp(interval_datetime, {AEST})) < 15 THEN 'midday' "
           f"WHEN hour(from_utc_timestamp(interval_datetime, {AEST})) < 18 THEN 'afternoon' ELSE 'evening' END")


def _r(v, n: int = 2):
    return None if v is None else round(float(v), n)


def _meta(**extra) -> dict:
    return {"data_source": "model_backtest", "source_note": fm.SOURCE_NOTE,
            "timestamp": datetime.now(timezone.utc).isoformat(), **extra}


def _by_region_h(metrics: list[dict]) -> dict[tuple[str, int], dict]:
    return {(m["region"], int(m["horizon_intervals"])): m for m in metrics}


def _season(d: date) -> str:
    return {12: "Summer", 1: "Summer", 2: "Summer", 3: "Autumn", 4: "Autumn", 5: "Autumn",
            6: "Winter", 7: "Winter", 8: "Winter"}.get(d.month, "Spring")


def _lag_hours(feature: str) -> float | None:
    m = re.search(r"_(lag|mean|std|max)_(\d+)$", feature)
    return round(int(m.group(2)) * 5 / 60, 2) if m else None


# =========================================================================
# Demand forecast accuracy
# =========================================================================

@router.get("/api/nem-demand-forecasting-accuracy/dashboard")
def ndfa_dashboard():
    metrics = _by_region_h(fm.model_metrics("demand"))
    acc = fm.backtest_grouped("demand", [("region_id", "region"), ("horizon_intervals", "h"),
                                         (YEAR_SQL, "year"), (QUARTER_SQL, "quarter")])
    forecast_accuracy = [{
        "region": a["region"], "forecast_horizon": fm.HORIZON_LABEL.get(a["h"], f"{a['h']}x5min"),
        "year": a["year"], "quarter": a["quarter"], "mae_mw": _r(a["mae"], 1), "rmse_mw": _r(a["rmse"], 1),
        "mape_pct": _r(a["mape_pct"]), "bias_mw": _r(a["bias"], 1),
        "r_squared": _r(metrics.get((a["region"], a["h"]), {}).get("r_squared"), 3)} for a in acc]
    hourly = fm.backtest_grouped("demand", [("region_id", "region"),
                                            (f"hour(from_utc_timestamp(interval_datetime, {AEST}))", "hr"),
                                            (fm.SEASON_SQL, "season")], horizon=12)
    hourly_profiles = [{
        "region": h["region"], "hour_of_day": h["hr"], "season": h["season"],
        "actual_demand_mw": _r(h["avg_actual"], 0), "forecast_demand_mw": _r(h["avg_forecast"], 0),
        "error_mw": _r(h["bias"], 1), "error_pct": _r(h["bias"] / h["avg_actual"] * 100 if h["avg_actual"] else None)}
        for h in hourly]
    model_comparison = []
    for (region, h), m in sorted(metrics.items()):
        if h != 12:
            continue
        year = int(str(m["test_start"])[:4])
        model_comparison += [
            {"model_name": "LightGBM", "region": region, "year": year, "mae_mw": _r(m["mae"], 1),
             "rmse_mw": _r(m["rmse"], 1), "mape_pct": _r(m["mape_pct"]), "training_time_s": None, "inference_time_ms": None},
            {"model_name": "Persistence baseline", "region": region, "year": year, "mae_mw": _r(m["persistence_mae"], 1),
             "rmse_mw": _r(m.get("persistence_rmse"), 1), "mape_pct": _r(m.get("persistence_mape_pct")),
             "training_time_s": None, "inference_time_ms": None}]
    extreme_events = [{
        "event_type": "Peak demand day", "region": p["region_id"], "event_date": str(p["d"]),
        "actual_peak_mw": _r(p["actual_peak"], 0), "forecast_peak_mw": _r(p["forecast_peak"], 0),
        "error_pct": _r((p["forecast_peak"] - p["actual_peak"]) / p["actual_peak"] * 100), "forecast_horizon_hours": 1}
        for p in fm.demand_peak_days()]
    feature_importance = [{"feature_name": f["feature"], "region": f["region"],
                           "importance_score": _r(f["importance_share"], 4), "rank": f["rank"]}
                          for f in fm.feature_importance("demand")]
    h12 = [m for (r, h), m in metrics.items() if h == 12]
    return {**_meta(), "forecast_accuracy": forecast_accuracy, "hourly_profiles": hourly_profiles,
            "model_comparison": model_comparison, "extreme_events": extreme_events,
            "feature_importance": feature_importance,
            "summary": {
                "best_model": "LightGBM (per region)",
                "avg_day_ahead_mape_pct": _r(mean(m["mape_pct"] for (r, h), m in metrics.items() if h == 48)),
                "avg_30min_mae_mw": _r(mean(m["mae"] for (r, h), m in metrics.items() if h == 6), 1),
                "worst_performing_region": max(h12, key=lambda m: m["mape_pct"])["region"],
                "extreme_event_avg_error_pct": _r(mean(abs(e["error_pct"]) for e in extreme_events))
                if extreme_events else None}}


@router.get("/api/electricity-demand-forecasting-ml-x/dashboard")
def edfmx_dashboard():
    metrics = fm.model_metrics("demand")
    fi = fm.feature_importance("demand", top_n=50)
    years = fm.training_window_years()
    features_count = defaultdict(int)
    for f in fi:
        features_count[f["region"]] += 1
    models = [{
        "model_id": fm.short_model_id(m["model_name"], m["model_version"]), "model_name": f"LightGBM demand {m['region']}",
        "model_type": "LightGBM", "forecast_horizon": fm.HORIZON_LABEL.get(m["horizon_intervals"]), "region": m["region"],
        "mape_pct": _r(m["mape_pct"]), "mae_mw": _r(m["mae"], 1), "rmse_mw": _r(m["rmse"], 1),
        "r_squared": _r(m["r_squared"], 3), "training_data_years": years, "features_count": features_count[m["region"]],
        "last_trained": str(m["trained_at"])[:10], "inference_time_ms": None, "in_production": True} for m in metrics]
    feature_importance = [{
        "model_id": fm.short_model_id(f["model_name"], f["model_version"]), "feature_name": f["feature"],
        "feature_importance_pct": _r(f["importance_share"] * 100), "feature_type": fm.feature_category(f["feature"]),
        "data_source": "AEMO NEMWEB (gold.nem_prices_5min)", "lag_hours": _lag_hours(f["feature"]),
        "missing_rate_pct": None} for f in fi if f["rank"] <= 10]
    acc = fm.backtest_grouped("demand", [(DATE_SQL, "d"), ("region_id", "region"), ("horizon_intervals", "h")])
    ids = {(m["region"]): fm.short_model_id(m["model_name"], m["model_version"]) for m in metrics}
    accuracy_records = [{
        "evaluation_date": str(a["d"]), "model_id": ids.get(a["region"]), "region": a["region"],
        "horizon_type": fm.HORIZON_LABEL.get(a["h"]), "mape_pct": _r(a["mape_pct"]), "mae_mw": _r(a["mae"], 1),
        "bias_mw": _r(a["bias"], 1), "peak_demand_error_pct": None, "trough_demand_error_pct": None,
        "extreme_event_accuracy_pct": None, "data_points_evaluated": a["n"]} for a in acc]
    drift = _drift_records("demand")
    daily = fm.backtest_grouped("demand", [(DATE_SQL, "d"), ("region_id", "region")], horizon=12)
    peak_by_region = {r: max(a["avg_actual"] for a in daily if a["region"] == r) for r in {a["region"] for a in daily}}
    forecasts = [{
        "forecast_date": str(a["d"]), "region": a["region"], "horizon_h": 1, "model_used": ids.get(a["region"]),
        "forecast_demand_mw": _r(a["avg_forecast"], 0), "actual_demand_mw": _r(a["avg_actual"], 0),
        "absolute_error_mw": _r(a["mae"], 1), "pct_error": _r(a["mape_pct"]), "temperature_c": None,
        "renewable_pct_actual": None, "ev_load_estimated_mw": None,
        "was_peak_day": a["avg_actual"] == peak_by_region[a["region"]]} for a in daily]
    h12 = [m for m in metrics if m["horizon_intervals"] == 12]
    best = min(h12, key=lambda m: m["mape_pct"])
    return {**_meta(), "models": models, "feature_importance": feature_importance, "accuracy_records": accuracy_records,
            "structural_breaks": [], "drift_monitoring": drift, "forecasts": forecasts,
            "summary": {"best_model_name": f"LightGBM demand {best['region']}", "best_model_mape_pct": _r(best["mape_pct"]),
                        "production_models_count": len({m["model_name"] for m in metrics}),
                        "models_with_drift": sum(1 for d in drift if d["concept_drift_detected"]),
                        "structural_breaks_ytd": "n/a",
                        "avg_forecast_error_pct": _r(mean(m["mape_pct"] for m in h12))}}


def _drift_records(target: str) -> list[dict]:
    out = []
    for d in fm.performance_drift(target):
        degradation = ((d["recent_mae"] - d["earlier_mae"]) / d["earlier_mae"] * 100) if d["earlier_mae"] else None
        drifted = degradation is not None and degradation > 25
        out.append({"monitor_date": str(d["monitor_date"]), "model_id": fm.short_model_id(d["model_name"], d["model_version"]),
                    "region": d["region_id"], "psi_score": None, "data_drift_detected": None,
                    "concept_drift_detected": drifted, "performance_degradation_pct": _r(degradation, 1),
                    "recommended_action": "Retrain on latest data" if drifted else "Monitor",
                    "retraining_triggered": False})
    return out


@router.get("/api/distributed-solar-forecasting/dashboard")
def dsfa_dashboard():
    raise HTTPException(status_code=503, detail="No rooftop/distributed solar forecast model is deployed in this "
                                                "build; this dashboard is intentionally not populated.")


@router.get("/api/demand-forecast-accuracy/dashboard")
def dfa_dashboard():
    metrics = fm.model_metrics("demand")
    p90 = {(a["region"], a["h"]): a["p90_abs_error"]
           for a in fm.backtest_grouped("demand", [("region_id", "region"), ("horizon_intervals", "h")])}
    daily = fm.backtest_grouped("demand", [(DATE_SQL, "d"), ("region_id", "region")], horizon=6)
    error_records = [{
        "date": str(a["d"]), "region": a["region"], "period": "DAILY", "horizon_min": 30,
        "forecast_mw": _r(a["avg_forecast"], 0), "actual_mw": _r(a["avg_actual"], 0), "error_mw": _r(a["bias"], 1),
        "error_pct": _r(a["bias"] / a["avg_actual"] * 100), "direction": "OVER" if a["bias"] > 0 else "UNDER"}
        for a in daily]
    horizon_summary = [{
        "region": m["region"], "horizon_min": fm.HORIZON_MIN.get(m["horizon_intervals"]), "mae_mw": _r(m["mae"], 1),
        "rmse_mw": _r(m["rmse"], 1), "mape_pct": _r(m["mape_pct"]), "bias_mw": _r(m["bias"], 1),
        "p90_error_mw": _r(p90.get((m["region"], m["horizon_intervals"])), 0),
        "skill_score": _r(m["skill_vs_persistence"], 3)} for m in metrics]
    top_feature = {}
    for f in fm.feature_importance("demand", top_n=1):
        top_feature[f["region"]] = f["feature"]
    seasonal = fm.backtest_grouped("demand", [("region_id", "region"), (fm.SEASON_SQL, "season"), (TOD_SQL, "tod")],
                                   horizon=12)
    seasonal_bias = [{
        "region": s["region"], "season": s["season"], "time_of_day": s["tod"], "avg_error_mw": _r(s["bias"], 1),
        "avg_error_pct": _r(s["bias"] / s["avg_actual"] * 100), "sample_count": s["n"],
        "primary_driver": f"top model feature: {top_feature.get(s['region'], 'n/a')}"} for s in seasonal]
    years = fm.training_window_years()
    benchmarks = []
    for m in (m for m in metrics if m["horizon_intervals"] == 12):
        common = {"region": m["region"], "training_data_years": years, "last_retrained": str(m["trained_at"])[:10]}
        benchmarks += [
            {**common, "model_name": "LightGBM (1h)", "model_type": "Gradient boosting", "mae_mw": _r(m["mae"], 1),
             "rmse_mw": _r(m["rmse"], 1), "mape_pct": _r(m["mape_pct"]), "features_used": "price/demand lags, "
             "cross-region prices, supply margin, interchange, calendar", "deployment_status": "PRODUCTION"},
            {**common, "model_name": "Persistence (1h)", "model_type": "Baseline", "mae_mw": _r(m["persistence_mae"], 1),
             "rmse_mw": _r(m.get("persistence_rmse"), 1), "mape_pct": _r(m.get("persistence_mape_pct")),
             "features_used": "latest observed demand", "deployment_status": "BASELINE"}]
    best = min(horizon_summary, key=lambda h: h["mape_pct"])
    return {**_meta(), "error_records": error_records, "horizon_summary": horizon_summary,
            "seasonal_bias": seasonal_bias, "model_benchmarks": benchmarks,
            "summary": {"best_model_mape_pct": best["mape_pct"], "best_horizon_min": best["horizon_min"],
                        "production_models": len({m["model_name"] for m in metrics}),
                        "seasonal_bias_records": len(seasonal_bias)}}


# =========================================================================
# Price forecast accuracy
# =========================================================================

@router.get("/api/electricity-market-forecasting-accuracy/dashboard")
def emfa_dashboard():
    price_m, demand_m = fm.model_metrics("price"), fm.model_metrics("demand")
    flags = {(str(f["d"]), f["region_id"]): f for f in fm.daily_spike_flags(48)}
    p_daily = fm.backtest_grouped("price", [(DATE_SQL, "d"), ("region_id", "region")], horizon=48)
    price_forecasts = [{
        "forecast_date": str(a["d"]), "region": a["region"], "horizon_type": "4h-ahead",
        "forecast_price_mwh": _r(a["avg_forecast"]), "actual_price_mwh": _r(a["avg_actual"]),
        "absolute_error_mwh": _r(a["mae"]), "pct_error": _r(a["mape_pct"]),
        "direction_correct": (a["directional_pct"] or 0) >= 50,
        "spike_predicted": bool(flags.get((str(a["d"]), a["region"]), {}).get("spike_predicted")),
        "spike_occurred": bool(flags.get((str(a["d"]), a["region"]), {}).get("spike_occurred")),
        "model_type": "LightGBM"} for a in p_daily]
    d_daily = fm.backtest_grouped("demand", [(DATE_SQL, "d"), ("region_id", "region")], horizon=48)
    demand_forecasts = [{
        "forecast_date": str(a["d"]), "region": a["region"], "horizon_type": "4h-ahead",
        "forecast_demand_mw": _r(a["avg_forecast"], 0), "actual_demand_mw": _r(a["avg_actual"], 0),
        "mae_mw": _r(a["mae"], 1), "rmse_mw": _r(a["rmse"], 1), "mape_pct": _r(a["mape_pct"]), "model_type": "LightGBM",
        "temperature_error_c": None, "season": _season(a["d"] if isinstance(a["d"], date) else date.fromisoformat(str(a["d"])))}
        for a in d_daily]
    event_predictions = [{
        "event_date": str(e["interval_datetime"])[:10], "region": e["region_id"], "event_type": "Price spike >$300",
        "forecast_severity": _r(e["predicted"] / 1000), "actual_severity": _r(e["actual"] / 1000),
        "prediction_lead_time_h": 1,
        "forecast_accuracy_pct": _r(max(0.0, 100 - abs(e["predicted"] - e["actual"]) / e["actual"] * 100), 1),
        "financial_impact_m": _r(e["cost_above_300_m_aud"], 3),
        "improvement_recommendation": "Flagged by spike classifier" if e["spike_flag"]
        else "Missed: add generator-outage and bid-stack features"} for e in fm.spike_events()]
    models = [{
        "model_id": fm.short_model_id(m["model_name"], m["model_version"]), "model_name": f"LightGBM {m['target']} {m['region']}",
        "model_type": "LightGBM", "target_variable": m["target"], "training_period_years": fm.training_window_years(),
        "last_retrained": str(m["trained_at"])[:10], "mape_pct": _r(m["mape_pct"]), "mae_value": _r(m["mae"]),
        "r_squared": _r(m["r_squared"], 3), "inference_time_ms": None, "features_count": None, "is_production": True}
        for m in price_m + demand_m if m["horizon_intervals"] == 12]
    p12 = [m for m in price_m if m["horizon_intervals"] == 12]
    recalls = [m["spike_recall"] for m in p12 if m.get("spike_recall") is not None]
    return {**_meta(), "price_forecasts": price_forecasts, "demand_forecasts": demand_forecasts,
            "renewable_forecasts": [], "event_predictions": event_predictions, "models": models, "improvement_trend": [],
            "summary": {"avg_price_mape_pct": _r(mean(m["mape_pct"] for m in p12 if m["mape_pct"] is not None), 1),
                        "avg_demand_mape_pct": _r(mean(m["mape_pct"] for m in demand_m if m["horizon_intervals"] == 12), 1),
                        "best_model_name": "LightGBM (per region)",
                        "spike_prediction_accuracy_pct": _r(mean(recalls) * 100, 1) if recalls else None,
                        "yoy_improvement_pct": None, "avg_renewable_error_pct": None}}


@router.get("/api/electricity-price-forecasting-models/dashboard")
def epf_models_dashboard():
    metrics = fm.model_metrics("price")
    years = fm.training_window_years()
    models = []
    for m in metrics:
        label = fm.HORIZON_LABEL.get(m["horizon_intervals"])
        base = {"region": m["region"], "horizon": label, "last_trained": str(m["trained_at"])[:10]}
        models += [
            {**base, "model_id": fm.short_model_id(m["model_name"], m["model_version"]), "model_name": f"LightGBM {label}",
             "model_type": "LightGBM (Huber)", "mae_aud_mwh": _r(m["mae"]), "rmse_aud_mwh": _r(m["rmse"]),
             "mape_pct": _r(m["mape_pct"]), "r2_score": _r(m["r_squared"], 3), "training_samples": m.get("n_train") or 0,
             "active": True},
            {**base, "model_id": f"persistence_{m['region'].lower()}_{label}", "model_name": f"Persistence {label}",
             "model_type": "Baseline", "mae_aud_mwh": _r(m["persistence_mae"]), "rmse_aud_mwh": _r(m.get("persistence_rmse")),
             "mape_pct": _r(m.get("persistence_mape_pct")), "r2_score": None, "training_samples": 0, "active": False}]
    daily = fm.backtest_grouped("price", [(DATE_SQL, "d"), ("region_id", "region"), ("horizon_intervals", "h")])
    forecast_accuracy = [{
        "date": str(a["d"]), "region": a["region"], "horizon": fm.HORIZON_LABEL.get(a["h"]),
        "actual_aud_mwh": _r(a["avg_actual"]), "forecast_aud_mwh": _r(a["avg_forecast"]), "error_aud_mwh": _r(a["bias"]),
        "error_pct": _r(a["bias"] / max(abs(a["avg_actual"]), 20.0) * 100),
        "within_10pct": abs(a["bias"]) <= 0.1 * max(abs(a["avg_actual"]), 20.0)} for a in daily]
    feature_importance = [{
        "model_id": fm.short_model_id(f["model_name"], f["model_version"]), "feature": f["feature"],
        "importance_score": _r(f["importance_share"], 4), "feature_category": fm.feature_category(f["feature"]),
        "rank": f["rank"]} for f in fm.feature_importance("price")]
    calibration = [{
        "region": c["region_id"], "decile": c["decile"], "predicted_probability": _r(c["predicted_probability"], 3),
        "actual_frequency": _r(c["actual_frequency"], 3),
        "calibration_error": _r(abs(c["predicted_probability"] - c["actual_frequency"]), 3), "sample_count": c["n"]}
        for c in fm.spike_calibration()]
    h12 = [m for m in metrics if m["horizon_intervals"] == 12]
    best = min(h12, key=lambda m: m["mae"])
    return {**_meta(training_window_years=years), "models": models, "ensemble_weights": [],
            "forecast_accuracy": forecast_accuracy, "feature_importance": feature_importance, "calibration": calibration,
            "summary": {"best_model": f"LightGBM 1h ({best['region']})", "best_model_mae_aud_mwh": _r(best["mae"]),
                        "avg_mape_pct": _r(mean(m["mape_pct"] for m in h12 if m["mape_pct"] is not None), 1),
                        "models_in_production": len({m["model_name"] for m in metrics}),
                        "horizons_beating_persistence": sum(1 for m in metrics if (m["skill_vs_persistence"] or 0) > 0),
                        "horizons_evaluated": len(metrics)}}


@router.get("/api/electricity-price-forecasting/dashboard")
def epfm_dashboard():
    metrics = fm.model_metrics("price")
    years = fm.training_window_years()
    models = [{
        "model_id": fm.short_model_id(m["model_name"], m["model_version"]) + f"/{fm.HORIZON_LABEL.get(m['horizon_intervals'])}",
        "model_name": f"LightGBM price {m['region']}", "model_type": "LightGBM (Huber)",
        "forecast_horizon": fm.HORIZON_LABEL.get(m["horizon_intervals"]), "region": m["region"], "mae_mwh": _r(m["mae"]),
        "rmse_mwh": _r(m["rmse"]), "mape_pct": _r(m["mape_pct"]), "r2_score": _r(m["r_squared"], 3),
        "training_data_years": years, "last_retrained_month": str(m["trained_at"])[:7]} for m in metrics]
    ids = {m["region"]: fm.short_model_id(m["model_name"], m["model_version"]) for m in metrics}
    forecast_accuracy = [{
        "model_id": ids.get(a["region_id"]), "region": a["region_id"], "year": a["year"], "month": a["month"],
        "mae_mwh": _r(a["mae"]), "rmse_mwh": _r(a["rmse"]), "mape_pct": _r(a["mape_pct"]),
        "spike_recall_pct": _r(a["spike_recall_pct"], 1), "spike_precision_pct": _r(a["spike_precision_pct"], 1),
        "negative_price_accuracy_pct": _r(a["negative_price_accuracy_pct"], 1)} for a in _monthly_price_accuracy()]
    feature_importance = [{
        "model_id": fm.short_model_id(f["model_name"], f["model_version"]), "feature_name": f["feature"],
        "importance_score": _r(f["importance_share"], 4), "feature_category": fm.feature_category(f["feature"])}
        for f in fm.feature_importance("price")]
    fva = fm.backtest_grouped("price", [("region_id", "region"), (YEAR_SQL, "year"), (QUARTER_SQL, "q")], horizon=12)
    forecast_vs_actual = [{
        "model_id": ids.get(a["region"]), "region": a["region"], "year": a["year"], "quarter": f"Q{a['q']}",
        "avg_forecast_mwh": _r(a["avg_forecast"]), "avg_actual_mwh": _r(a["avg_actual"]), "bias_mwh": _r(a["bias"]),
        "directional_accuracy_pct": _r(a["directional_pct"], 1), "within_10pct_accuracy_pct": _r(a["within_10pct"], 1)}
        for a in fva]
    today = datetime.now(timezone.utc).date()
    events = [(e, "Price Spike >$300", bool(e["spike_flag"])) for e in fm.spike_events(8)]
    events += [(e, "Negative Price", e["predicted"] < 0) for e in fm.negative_price_events(4)]
    extreme_events = [{
        "event_id": f"EE-{i + 1:03d}", "event_type": kind, "region": e["region_id"],
        "event_date_offset": (date.fromisoformat(str(e["interval_datetime"])[:10]) - today).days,
        "actual_price_mwh": _r(e["actual"]), "forecast_price_mwh": _r(e["predicted"]),
        "forecast_error_mwh": _r(abs(e["predicted"] - e["actual"])), "was_predicted": bool(hit),
        "model_id": ids.get(e["region_id"])} for i, (e, kind, hit) in enumerate(events)]
    h12 = [m for m in metrics if m["horizon_intervals"] == 12]
    best = min(h12, key=lambda m: m["mae"])
    recalls = [m["spike_recall"] for m in h12 if m.get("spike_recall") is not None]
    return {**_meta(), "models": models, "forecast_accuracy": forecast_accuracy, "feature_importance": feature_importance,
            "forecast_vs_actual": forecast_vs_actual, "extreme_events": extreme_events,
            "summary": {"total_models": len(models), "best_model_mae": _r(best["mae"]),
                        "best_model_name": f"LightGBM price {best['region']} (1h)",
                        "avg_mape_pct": _r(mean(m["mape_pct"] for m in h12 if m["mape_pct"] is not None), 1),
                        "best_spike_recall_pct": _r(max(recalls) * 100, 1) if recalls else 0.0,
                        "total_extreme_events_predicted": sum(1 for e in extreme_events if e["was_predicted"]),
                        "avg_r2_score": _r(mean(m["r_squared"] for m in h12), 3)}}


def _monthly_price_accuracy() -> list[dict]:
    return fm.rows(f"""
        SELECT region_id, {YEAR_SQL} AS year, {MONTH_SQL} AS month,
               avg(abs(predicted - actual)) AS mae, sqrt(avg(pow(predicted - actual, 2))) AS rmse,
               avg(CASE WHEN abs(actual) >= 20 THEN abs(predicted - actual) / abs(actual) END) * 100 AS mape_pct,
               count_if(actual > 300 AND spike_flag) * 100.0 / nullif(count_if(actual > 300), 0) AS spike_recall_pct,
               count_if(actual > 300 AND spike_flag) * 100.0
                   / nullif(count_if(spike_flag), 0) AS spike_precision_pct,
               count_if(actual < 0 AND predicted < 0) * 100.0 / nullif(count_if(actual < 0), 0) AS negative_price_accuracy_pct
        FROM {fm.GOLD}.forecast_backtest WHERE target = 'price' AND horizon_intervals = 12
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
    """, "monthly price accuracy")


# =========================================================================
# Long-range demand outlook (planning assumptions on top of real peaks)
# =========================================================================

@router.get("/api/nem-demand-forecast/dashboard")
def ndf_dashboard():
    raise HTTPException(status_code=503, detail="Long-range (multi-year) demand outlook needs ISP scenario inputs that "
                                                "are not part of this build; this dashboard is intentionally not populated.")


# =========================================================================
# Volatility regimes (real monthly price statistics)
# =========================================================================

REGIMES = ("LOW", "NORMAL", "HIGH", "EXTREME")


@router.get("/api/volatility-regime/dashboard")
def volatility_regime_dashboard():
    months = fm.monthly_volatility()
    for m in months:
        m["vi"] = float(m["price_std"] or 0) / max(abs(float(m["avg_price"] or 0)), 1.0)
    ranked = sorted(m["vi"] for m in months)
    cut = [ranked[int(len(ranked) * q)] for q in (0.25, 0.5, 0.75)] if ranked else [0, 0, 0]
    for m in months:
        m["regime"] = REGIMES[sum(m["vi"] > c for c in cut)]
    regimes, transitions = [], defaultdict(list)
    for region in sorted({m["region_id"] for m in months}):
        seq = [m for m in months if m["region_id"] == region]
        run_days = 0
        for i, m in enumerate(seq):
            run_days = run_days + m["days"] if i and seq[i - 1]["regime"] == m["regime"] else m["days"]
            if i and seq[i - 1]["regime"] != m["regime"]:
                transitions[(seq[i - 1]["regime"], m["regime"])].append(run_days)
            regimes.append({"month": m["month"], "region": region, "regime": m["regime"],
                            "avg_price_aud_mwh": _r(m["avg_price"]), "price_std_aud": _r(m["price_std"]),
                            "spike_count": m["spike_count"], "negative_price_hours": _r(m["negative_price_hours"], 1),
                            "volatility_index": _r(m["vi"], 3), "regime_duration_days": run_days})
    total = sum(len(v) for v in transitions.values()) or 1
    return {"timestamp": datetime.now(timezone.utc).isoformat(), "data_source": "gold.nem_prices_5min",
            "source_note": "Regimes are quartiles of monthly volatility index (price std / |mean price|) across region-months.",
            "regimes": regimes, "clusters": _spike_clusters(), "hedging": [],
            "transitions": [{"from_regime": f, "to_regime": t, "transition_count": len(v),
                             "avg_duration_before_transition_days": _r(mean(v), 1),
                             "probability_pct": _r(len(v) / total * 100, 1), "typical_trigger": "—"}
                            for (f, t), v in sorted(transitions.items())]}


def _spike_clusters() -> list[dict]:
    """Runs of consecutive market days with at least one >$300/MWh interval, per region."""
    days = fm.daily_price_extremes()
    clusters, current = [], None
    for d in days:
        is_spike = float(d["max_price"] or 0) > 300
        day = date.fromisoformat(str(d["d"]))
        contiguous = current and current["region"] == d["region_id"] and (day - current["end"]).days == 1
        if is_spike and contiguous:
            current.update(end=day, max_price=max(current["max_price"], d["max_price"]),
                           prices=current["prices"] + [d["avg_price"]], cost=current["cost"] + d["cost_above_300_m_aud"])
        elif is_spike:
            if current:
                clusters.append(current)
            current = {"region": d["region_id"], "start": day, "end": day, "max_price": d["max_price"],
                       "prices": [d["avg_price"]], "cost": d["cost_above_300_m_aud"]}
    if current:
        clusters.append(current)
    return [{"cluster_id": f"VC-{i + 1:03d}", "region": c["region"], "start_date": str(c["start"]),
             "end_date": str(c["end"]), "duration_days": (c["end"] - c["start"]).days + 1, "trigger": "UNCLASSIFIED",
             "max_price": _r(c["max_price"]), "avg_price": _r(mean(c["prices"])),
             "total_cost_impact_m_aud": _r(c["cost"], 3)}
            for i, c in enumerate(sorted(clusters, key=lambda c: -c["cost"])[:20])]


# =========================================================================
# Demand forecast models (registry + forecast vs actual)
# =========================================================================

@router.get("/api/demand-forecast-models/dashboard")
def demand_forecast_models_dashboard():
    metrics = [m for m in fm.model_metrics("demand") if m["horizon_intervals"] == 12]
    years = fm.training_window_years()
    models = []
    for m in metrics:
        common = {"region": m["region"], "training_data_years": years, "last_retrained": str(m["trained_at"])[:10]}
        rl = m["region"].lower()
        models += [{**common, "model_id": f"lgbm_{rl}", "name": "LightGBM", "mae_mw": _r(m["mae"], 1),
                    "rmse_mw": _r(m["rmse"], 1), "mape_pct": _r(m["mape_pct"]), "r_squared": _r(m["r_squared"], 4)},
                   {**common, "model_id": f"persistence_{rl}", "name": "Persistence baseline",
                    "mae_mw": _r(m["persistence_mae"], 1), "rmse_mw": _r(m.get("persistence_rmse"), 1),
                    "mape_pct": _r(m.get("persistence_mape_pct")), "r_squared": None}]
    hourly = fm.latest_backtest_day_hourly()
    forecasts = []
    for h in hourly:
        rl = h["region_id"].lower()
        base = {"region": h["region_id"], "forecast_date": str(h["d"]), "hour": h["hr"],
                "actual_mw": _r(h["actual"], 0), "temperature_degc": None}
        forecasts += [{**base, "model_id": f"lgbm_{rl}", "forecast_mw": _r(h["forecast"], 0),
                       "lower_bound_mw": _r(h["lower_80"], 0), "upper_bound_mw": _r(h["upper_80"], 0)},
                      {**base, "model_id": f"persistence_{rl}", "forecast_mw": _r(h["persistence"], 0),
                       "lower_bound_mw": None, "upper_bound_mw": None}]
    seasonal_patterns = [{"region": s["region_id"], "season": s["season"].upper(), "peak_demand_mw": _r(s["peak"], 0),
                          "avg_demand_mw": _r(s["avg_d"], 0), "min_demand_mw": _r(s["min_d"], 0),
                          "peak_hour": s["peak_hour"], "temp_sensitivity_mw_per_degc": None}
                         for s in fm.seasonal_demand_patterns()]
    feature_importance = [{"model_id": f"lgbm_{f['region'].lower()}", "feature": f["feature"],
                           "importance_score": _r(f["importance_share"], 4)} for f in fm.feature_importance("demand", 7)]
    return {**_meta(forecast_date=str(hourly[0]["d"]) if hourly else None), "models": models, "forecasts": forecasts,
            "seasonal_patterns": seasonal_patterns, "feature_importance": feature_importance}
