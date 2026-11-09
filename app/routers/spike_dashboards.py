"""Real spike-prediction, spot-price-forecast and negative-price dashboards.

Sources: live forecasts in gold.price_forecasts (UC models `ml.nem_price_forecast_*`, alias production),
the held-out back-test in gold.forecast_backtest / forecast_model_metrics, and real prices in
gold.nem_prices_5min. Fields with no ingested source are null or empty lists; nothing is simulated.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean

from . import forecast_metrics as fm
from .shared import _CATALOG, _no_real_data, _query_gold

GOLD = f"{_CATALOG}.gold"
SPIKE = 300.0


def _r(v, n: int = 2):
    return None if v is None else round(float(v), n)


def _thresholds() -> dict[str, float]:
    return {m["region"]: float(m["spike_threshold"]) for m in fm.model_metrics("price")
            if m["horizon_intervals"] == 12 and m.get("spike_threshold") is not None}


def _classification(horizon: int = 12) -> list[dict]:
    return fm.rows(f"""
        SELECT region_id, count_if(actual > {SPIKE}) AS spikes, count_if(spike_flag) AS flagged,
               count_if(actual > {SPIKE} AND spike_flag) AS tp, count_if(actual <= {SPIKE} AND spike_flag) AS fp,
               count_if(actual <= {SPIKE} AND NOT spike_flag) AS tn,
               min(interval_datetime) AS start_ts, max(interval_datetime) AS end_ts
        FROM {GOLD}.forecast_backtest WHERE target = 'price' AND horizon_intervals = {horizon}
        GROUP BY 1 ORDER BY 1
    """, "spike classification back-test")


# ---------------------------------------------------------------------------
# /api/spot-price-spike-prediction/dashboard  (SPPDashboard)
# ---------------------------------------------------------------------------

def spike_prediction_dashboard() -> dict:
    thr = _thresholds()
    live = _query_gold(f"""
        SELECT f.region_id, f.interval_datetime, f.forecast_run_at, f.horizon_intervals, f.predicted_rrp,
               f.spike_probability, f.prediction_lower_80, f.prediction_upper_80, a.rrp AS actual
        FROM {GOLD}.price_forecasts f
        LEFT JOIN {GOLD}.nem_prices_5min a ON a.region_id = f.region_id AND a.interval_datetime = f.interval_datetime
        WHERE f.model_name LIKE '%.ml.nem_price_forecast_%' AND f.horizon_intervals = 12
        ORDER BY f.forecast_run_at DESC, f.region_id LIMIT 200
    """) or []
    if not live:
        _no_real_data("Live spike predictions")
    predictions = []
    for i, p in enumerate(live):
        flag = p["spike_probability"] is not None and p["spike_probability"] >= thr.get(p["region_id"], 0.5)
        actual = p["actual"]
        predictions.append({
            "prediction_id": f"SPP-{i + 1:04d}", "region": p["region_id"],
            "dispatch_interval": str(p["interval_datetime"]).replace(" ", "T"),
            "predicted_spike_probability": _r(p["spike_probability"], 4), "predicted_price_aud_mwh": _r(p["predicted_rrp"]),
            "actual_price_aud_mwh": _r(actual), "threshold_aud_mwh": SPIKE,
            "correct_prediction": None if actual is None else (flag == (actual > SPIKE)),
            "confidence_interval_low": _r(p["prediction_lower_80"]), "confidence_interval_high": _r(p["prediction_upper_80"])})
    metrics = {m["region"]: m for m in fm.model_metrics("price") if m["horizon_intervals"] == 12}
    model_performance = []
    for c in _classification():
        tp, fp, tn, spikes = c["tp"], c["fp"], c["tn"], c["spikes"]
        prec = tp / (tp + fp) if tp + fp else None
        rec = tp / spikes if spikes else None
        model_performance.append({
            "model_name": "LightGBM spike classifier (1h)", "region": c["region_id"],
            "period": f"{str(c['start_ts'])[:10]} to {str(c['end_ts'])[:10]} (held-out)",
            "precision": _r(prec, 3), "recall": _r(rec, 3),
            "f1_score": _r(2 * prec * rec / (prec + rec), 3) if prec and rec else None,
            "auc_roc": _r(metrics.get(c["region_id"], {}).get("spike_auc"), 3),
            "false_positive_rate": _r(fp / (fp + tn), 4) if fp + tn else None,
            "spike_threshold_aud_mwh": SPIKE, "total_spikes": spikes, "predicted_spikes": c["flagged"]})
    shares = defaultdict(list)
    for f in fm.feature_importance("price", top_n=15):
        shares[f["feature"]].append(f["importance_share"])
    features = sorted(({"feature": k, "importance": _r(mean(v), 4), "category": fm.feature_category(k),
                        "spike_correlation": None, "lag_minutes": _lag_minutes(k)} for k, v in shares.items()),
                      key=lambda x: -x["importance"])[:12]
    latest_run = max((p["forecast_run_at"] for p in live), default=None)
    alerts = [{
        "alert_id": f"ALERT-{p['region_id']}-{str(p['interval_datetime'])[11:16].replace(':', '')}", "region": p["region_id"],
        "issued_at": str(p["forecast_run_at"]), "forecast_window_minutes": 60,
        "spike_probability": _r(p["spike_probability"], 4), "expected_price_aud_mwh": _r(p["predicted_rrp"]),
        "trigger_factors": [f["feature"] for f in features[:3]], "status": "ACTIVE"}
        for p in live if p["forecast_run_at"] == latest_run and p["spike_probability"] is not None
        and p["spike_probability"] >= thr.get(p["region_id"], 0.5)]
    history = _spike_history()
    flagged = [h for h in history if h["predicted"]]
    aucs = [m["auc_roc"] for m in model_performance if m["auc_roc"] is not None]
    total_spikes = sum(m["total_spikes"] for m in model_performance)
    total_tp = sum(c["tp"] for c in _classification())
    fprs = [m["false_positive_rate"] for m in model_performance if m["false_positive_rate"] is not None]
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(), "data_source": "model_backtest+live_forecasts",
        "source_note": fm.SOURCE_NOTE + " Spike = RRP > $300/MWh; decision thresholds tuned on a validation window.",
        "predictions": predictions, "model_performance": model_performance, "features": features,
        "alerts": alerts, "spike_history": history,
        "summary": {"best_model": "LightGBM spike classifier (1h)", "best_auc_roc": max(aucs) if aucs else 0.0,
                    "spike_detection_rate_pct": _r(total_tp / total_spikes * 100, 1) if total_spikes else None,
                    "false_positive_rate_pct": _r(mean(fprs) * 100, 2) if fprs else None,
                    "active_alerts": len(alerts), "total_spikes_ytd": len(history),
                    "avg_warning_lead_time_minutes": 60 if flagged else None},
    }


def _lag_minutes(feature: str) -> int | None:
    parts = feature.rsplit("_", 1)
    return int(parts[1]) * 5 if len(parts) == 2 and parts[1].isdigit() else None


def _spike_history() -> list[dict]:
    """Real >$300 hours in the back-test window, and whether the 1-hour-ahead classifier flagged them."""
    rows = fm.rows(f"""
        SELECT region_id, to_date(from_utc_timestamp(interval_datetime, {fm.AEST})) AS d,
               hour(from_utc_timestamp(interval_datetime, {fm.AEST})) AS hr,
               max(actual) AS max_price, count_if(actual > {SPIKE}) AS n, max(CAST(spike_flag AS INT)) AS flagged
        FROM {GOLD}.forecast_backtest WHERE target = 'price' AND horizon_intervals = 12 AND actual > {SPIKE}
        GROUP BY 1, 2, 3 ORDER BY max_price DESC LIMIT 40
    """, "spike history")
    return [{"region": r["region_id"], "date": str(r["d"]), "hour": r["hr"], "max_price_aud_mwh": _r(r["max_price"]),
             "duration_intervals": r["n"], "cause": "UNCLASSIFIED", "predicted": bool(r["flagged"]),
             "warning_lead_time_minutes": 60 if r["flagged"] else None} for r in rows]


# ---------------------------------------------------------------------------
# /api/spot-price-forecast/dashboard  (SPFDashboard)
# ---------------------------------------------------------------------------

def spot_price_forecast_dashboard() -> dict:
    metrics = fm.model_metrics("price")
    neg = {(r["region_id"], r["h"]): r["neg_recall"] for r in fm.rows(f"""
        SELECT region_id, horizon_intervals AS h,
               count_if(actual < 0 AND predicted < 0) * 100.0 / nullif(count_if(actual < 0), 0) AS neg_recall
        FROM {GOLD}.forecast_backtest WHERE target = 'price' GROUP BY 1, 2
    """, "negative-price recall")}
    models = [{
        "model_id": fm.short_model_id(m["model_name"], m["model_version"]) + f"/h{m['horizon_intervals']}",
        "model_name": f"LightGBM price {m['region']} ({fm.HORIZON_LABEL.get(m['horizon_intervals'])})",
        "model_type": "LightGBM", "region": m["region"], "horizon_min": fm.HORIZON_MIN.get(m["horizon_intervals"]),
        "mae_per_mwh": _r(m["mae"]), "rmse_per_mwh": _r(m["rmse"]), "mape_pct": _r(m["mape_pct"]),
        "r2_score": _r(m["r_squared"], 3),
        "spike_detection_recall_pct": _r((m.get("spike_recall") or 0) * 100, 1) if m.get("spike_events") else None,
        "negative_price_recall_pct": _r(neg.get((m["region"], m["horizon_intervals"])), 1),
        "training_period": f"{str(m['train_start'])[:10]} to {str(m['train_end'])[:10]}",
        "deployment_status": "PRODUCTION"} for m in metrics]
    hourly = fm.rows(f"""
        SELECT region_id, to_date(from_utc_timestamp(interval_datetime, {fm.AEST})) AS d,
               hour(from_utc_timestamp(interval_datetime, {fm.AEST})) AS hr, avg(actual) AS actual,
               avg(predicted) AS forecast, avg(lower_80) AS lo, avg(upper_80) AS hi, max(model_name) AS model_name
        FROM {GOLD}.forecast_backtest
        WHERE target = 'price' AND horizon_intervals = 12
          AND interval_datetime >= (SELECT max(interval_datetime) FROM {GOLD}.forecast_backtest) - INTERVAL 2 DAYS
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
    """, "hourly price back-test")
    forecasts = [{
        "date": str(h["d"]), "region": h["region_id"], "trading_interval": f"{h['hr']:02d}:00",
        "actual_price": _r(h["actual"]), "forecast_price": _r(h["forecast"]), "forecast_low": _r(h["lo"]),
        "forecast_high": _r(h["hi"]), "error_per_mwh": _r(h["forecast"] - h["actual"]),
        "model_used": h["model_name"].split(".")[-1],
        "price_regime": "SPIKE" if h["actual"] > SPIKE else "NEGATIVE" if h["actual"] < 0 else "NORMAL"} for h in hourly]
    features = [{"feature_name": f["feature"], "model_type": "LightGBM", "region": f["region"],
                 "importance_score": _r(f["importance_share"], 4), "rank": f["rank"],
                 "category": fm.feature_category(f["feature"])} for f in fm.feature_importance("price")]
    h12 = [m for m in metrics if m["horizon_intervals"] == 12]
    best = min(h12, key=lambda m: m["mae"])
    recalls = [m["spike_detection_recall_pct"] for m in models if m["spike_detection_recall_pct"] is not None]
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(), "data_source": "model_backtest", "source_note": fm.SOURCE_NOTE,
        "models": models, "forecasts": forecasts, "features": features, "drift": _price_drift(),
        "summary": {"best_mae_model": f"LightGBM price {best['region']} (1h)",
                    "avg_mape_pct": _r(mean(m["mape_pct"] for m in h12 if m["mape_pct"] is not None), 1),
                    "best_spike_recall_pct": max(recalls) if recalls else None,
                    "production_models": len({m["model_name"] for m in metrics}),
                    "total_forecasts": sum(int(m["n_test"]) for m in metrics)},
    }


def _price_drift() -> list[dict]:
    daily = fm.backtest_grouped("price", [(f"to_date(from_utc_timestamp(interval_datetime, {fm.AEST}))", "d"),
                                          ("region_id", "region")], horizon=12)
    by_region = defaultdict(list)
    for d in daily:
        by_region[d["region"]].append(d)
    out = []
    for region, days in by_region.items():
        baseline = mean(d["mae"] for d in days)
        for i in range(6, len(days)):
            mae7 = mean(d["mae"] for d in days[i - 6:i + 1])
            score = mae7 / baseline if baseline else None
            out.append({"model_id": f"nem_price_forecast_{region.lower()}", "date": str(days[i]["d"]),
                        "mae_rolling_7d": _r(mae7), "mae_baseline": _r(baseline), "drift_score": _r(score, 3),
                        "drift_alert": bool(score and score > 1.25),
                        "regime_shift": "elevated error" if score and score > 1.25 else "none"})
    return out


# ---------------------------------------------------------------------------
# /api/negative-price-events/dashboard  (NPEDashboard)
# ---------------------------------------------------------------------------

def negative_price_dashboard() -> dict:
    rows = fm.rows(f"""
        WITH n AS (SELECT region_id, interval_datetime, rrp, total_demand_mw,
                          CASE WHEN rrp < 0 THEN 1 ELSE 0 END AS neg FROM {GOLD}.nem_prices_5min),
        runs AS (SELECT *, sum(CASE WHEN neg = 0 THEN 1 ELSE 0 END)
                          OVER (PARTITION BY region_id ORDER BY interval_datetime) AS grp FROM n),
        longest AS (SELECT region_id, max(c) AS max_run FROM (
                      SELECT region_id, grp, count(*) AS c FROM runs WHERE neg = 1 GROUP BY 1, 2) GROUP BY 1)
        SELECT year(from_utc_timestamp(n.interval_datetime, {fm.AEST})) AS year, n.region_id,
               count_if(n.rrp < 0) AS neg_intervals, count(*) AS all_intervals,
               avg(CASE WHEN n.rrp < 0 THEN n.rrp END) AS avg_neg, min(n.rrp) AS deepest,
               sum(CASE WHEN n.rrp < 0 THEN n.total_demand_mw / 12 END) AS neg_energy_mwh, max(l.max_run) AS max_run
        FROM n JOIN longest l USING (region_id) GROUP BY 1, 2 ORDER BY 1, 2
    """, "negative price frequency")
    frequency = [{
        "year": r["year"], "region": r["region_id"], "negative_price_intervals": r["neg_intervals"],
        "negative_price_hours": _r(r["neg_intervals"] / 12, 1),
        "pct_of_year": _r(r["neg_intervals"] / r["all_intervals"] * 100, 2),
        "avg_negative_price": _r(r["avg_neg"]), "deepest_price": _r(r["deepest"]),
        "consecutive_negative_hrs_max": _r((r["max_run"] or 0) / 12, 2),
        "total_negative_energy_mwh": _r(r["neg_energy_mwh"], 0)} for r in rows]
    total_hours = sum(f["negative_price_hours"] or 0 for f in frequency)
    deepest = min(frequency, key=lambda f: f["deepest_price"] or 0)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(), "data_source": "gold.nem_prices_5min",
        "source_note": "Percentages are of the intervals in the data window (not a full calendar year). Drivers, "
                       "must-run plant economics and market-design options need data that is not ingested.",
        "frequency": frequency, "drivers": [], "battery_opportunity": [], "must_run": [], "market_design": [],
        "summary": {"total_negative_hours": _r(total_hours, 1),
                    "most_negative_region": max(frequency, key=lambda f: f["negative_price_hours"] or 0)["region"],
                    "deepest_price": deepest["deepest_price"], "deepest_price_region": deepest["region"],
                    "data_window_intervals": sum(r["all_intervals"] for r in rows)},
    }
