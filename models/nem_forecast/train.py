# Databricks notebook source
# MAGIC %md
# MAGIC # NEM price, spike and demand forecast — training and back-test
# MAGIC
# MAGIC **Execution evidence:** `evidence/captured_runs.py::MODEL_TRAINING` (job run 940474602766977: 10 models trained, back-tested vs persistence, registered in UC (v6); MLflow experiment /Shared/energy_copilot_nem_forecast); executed notebook with outputs in `notebooks/executed/`; checked by `tests/test_execution_evidence.py`.
# MAGIC
# MAGIC Trains on **real AEMO NEMWEB data** in `gold.nem_prices_5min` (landed by Pipeline 01 / job 36).
# MAGIC For each region it trains:
# MAGIC
# MAGIC | Model | Target | Algorithm |
# MAGIC |-------|--------|-----------|
# MAGIC | Price | RRP at t + h ($/MWh) | LightGBM, Huber loss |
# MAGIC | Spike | P(RRP at t + h > spike threshold) | LightGBM classifier (only when the training window has spikes) |
# MAGIC | Demand | total demand at t + h (MW) | LightGBM, L2 loss |
# MAGIC
# MAGIC Horizons default to 30 min / 1 h / 4 h. The split is chronological: the last `test_days` are held out
# MAGIC and never used for training. Every test-set metric is compared with a **persistence baseline**
# MAGIC (forecast = latest observed value), so the skill score shows whether the model beats "no model".
# MAGIC
# MAGIC Outputs:
# MAGIC * MLflow runs (params, metrics, feature importance) and UC models `{catalog}.ml.nem_{price|demand}_forecast_{region}`, alias `production`
# MAGIC * `gold.forecast_model_metrics`: one row per target × region × horizon (test set)
# MAGIC * `gold.forecast_backtest`: test-set predictions vs actuals, interval by interval
# MAGIC * `gold.forecast_feature_importance`

# COMMAND ----------

# Dependencies (lightgbm==4.5.0, scikit-learn) come from the job environment spec in resources/jobs.yml.

import json
import re
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from features import REGIONS, feature_columns, load_wide, region_features, stack_horizons
from mlflow.models import infer_signature
from sklearn.metrics import average_precision_score, roc_auc_score

if TYPE_CHECKING:  # provided by the Databricks notebook runtime
    spark: Any
    dbutils: Any

dbutils.widgets.text("catalog", "energy_copilot_catalog")
dbutils.widgets.text("lookback_days", "90")
dbutils.widgets.text("test_days", "14")
dbutils.widgets.text("horizons", "6,12,48")
dbutils.widgets.text("spike_threshold", "300")
dbutils.widgets.text("experiment", "/Shared/energy_copilot_nem_forecast")

CATALOG = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"Invalid catalog name: {CATALOG!r}")
LOOKBACK = int(dbutils.widgets.get("lookback_days"))
TEST_DAYS = int(dbutils.widgets.get("test_days"))
HORIZONS = tuple(int(h) for h in dbutils.widgets.get("horizons").split(","))
SPIKE = float(dbutils.widgets.get("spike_threshold"))
MAPE_FLOOR = 20.0  # $/MWh: price MAPE excludes |actual| < $20, where percentage error is meaningless

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(dbutils.widgets.get("experiment"))
TRAINED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
start_utc = (TRAINED_AT - timedelta(days=LOOKBACK)).strftime("%Y-%m-%d %H:%M:%S")
wide = load_wide(spark, CATALOG, start_utc)
TEST_START = wide.index.max() - pd.Timedelta(days=TEST_DAYS)
print(f"Data {wide.index.min()} → {wide.index.max()} UTC | test from {TEST_START} | horizons {HORIZONS}")

# COMMAND ----------


BASE_FEATURE = {"price": "rrp_lag_0", "demand": "demand_lag_0"}  # latest observed value = persistence
MAPE_FLOORS = {"price": MAPE_FLOOR, "demand": 100.0}  # MW: SA1 demand goes near zero/negative under rooftop solar
SPIKE_CLASS_WEIGHT = 10.0
SPIKE_ALERT_BUDGET = 0.01  # flag the top 1% of intervals (~3 alerts per region per day)
VAL_DAYS = 14  # last 14 days of the training window: spike threshold + interval calibration (test stays untouched)


class PooledSpikeClassifier:
    """One spike classifier trained on all five regions (region as a feature) so it sees every spike in the window;
    the decision threshold is tuned for F1 on the pooled validation slice."""

    def __init__(self, classifier, features: list[str], threshold: float):
        self.classifier, self.features, self.threshold = classifier, features, threshold

    def proba(self, df: pd.DataFrame, region_code: int) -> np.ndarray:
        X = df.reindex(columns=self.features)
        X["region_code"] = region_code
        return self.classifier.predict_proba(X)[:, 1]


class NemForecaster(mlflow.pyfunc.PythonModel):
    """Predicts the change from the latest observed value (so persistence is the floor), plus (for price) the pooled
    spike classifier with its validation-tuned threshold, and 80% intervals from validation residuals."""

    def __init__(self, regressor, spike, features, residual_q, target, region_code):
        self.regressor, self.spike, self.features = regressor, spike, features
        self.residual_q, self.target, self.region_code = residual_q, target, region_code

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        X = model_input[self.features]
        pred = X[BASE_FEATURE[self.target]].to_numpy() + self.regressor.predict(X)
        q = np.array([self.residual_q.get(int(h), (0.0, 0.0)) for h in model_input["horizon"]])
        out = pd.DataFrame({"predicted": pred, "lower_80": pred + q[:, 0], "upper_80": pred + q[:, 1]})
        prob = self.spike.proba(model_input, self.region_code) if self.spike is not None else np.full(len(X), np.nan)
        out["spike_probability"] = prob
        out["spike_flag"] = prob >= self.spike.threshold if self.spike is not None else False
        return out


def regression_metrics(y: np.ndarray, pred: np.ndarray, base: np.ndarray, mape_floor: float) -> dict:
    err, base_err = pred - y, base - y
    mae, base_mae = float(np.mean(np.abs(err))), float(np.mean(np.abs(base_err)))
    mask = np.abs(y) >= mape_floor
    return {
        "mae": mae, "rmse": float(np.sqrt(np.mean(err ** 2))), "bias": float(np.mean(err)),
        "mape_pct": float(np.mean(np.abs(err[mask] / y[mask])) * 100) if mask.any() else None,
        "persistence_mae": base_mae, "skill_vs_persistence": 1 - mae / base_mae if base_mae else None,
        "persistence_rmse": float(np.sqrt(np.mean(base_err ** 2))),
        "persistence_mape_pct": float(np.mean(np.abs(base_err[mask] / y[mask])) * 100) if mask.any() else None,
        "r_squared": float(1 - np.sum(err ** 2) / np.sum((y - y.mean()) ** 2)) if len(y) > 1 else None,
        "n_test": int(len(y)),
    }


def best_f1_threshold(y_true: np.ndarray, prob: np.ndarray) -> float:
    """Decision threshold that maximises F1 on the validation slice. Falls back to 0.5 when the validation slice has
    no spikes or no threshold achieves F1 > 0 (otherwise the search would return a meaningless near-zero cut-off)."""
    if not y_true.any():
        return 0.5
    best_t, best_f1 = 0.5, 0.0
    top = np.sort(np.unique(prob))[::-1][:2000]  # distinct high-probability values as candidate cut-offs
    for t in top:
        flagged = prob >= t
        tp = np.sum(flagged & y_true); fp = np.sum(flagged & ~y_true); fn = np.sum(~flagged & y_true)
        f1 = 2 * tp / (2 * tp + fp + fn) if tp else 0.0
        if f1 > best_f1:
            best_t, best_f1 = float(t), f1
    return best_t


def spike_metrics(y_true: np.ndarray, prob: np.ndarray, threshold: float) -> dict:
    flagged = prob >= threshold
    tp = int(np.sum(flagged & y_true)); fp = int(np.sum(flagged & ~y_true)); fn = int(np.sum(~flagged & y_true))
    out = {"spike_events": int(y_true.sum()), "spike_threshold": threshold,
           "spike_recall": tp / (tp + fn) if tp + fn else None,
           "spike_precision": tp / (tp + fp) if tp + fp else None, "spike_auc": None, "spike_avg_precision": None}
    if 0 < y_true.sum() < len(y_true):
        out["spike_auc"] = float(roc_auc_score(y_true, prob))
        out["spike_avg_precision"] = float(average_precision_score(y_true, prob))
    return out


def train_models(fit: pd.DataFrame, feats: list[str], target: str, y_col: str):
    params = {"objective": "regression_l1" if target == "price" else "regression", "n_estimators": 800,
              "learning_rate": 0.05, "num_leaves": 63, "min_child_samples": 50, "subsample": 0.8,
              "subsample_freq": 1, "colsample_bytree": 0.8, "verbose": -1}
    delta = fit[y_col] - fit[BASE_FEATURE[target]]
    reg = lgb.LGBMRegressor(**params).fit(fit[feats], delta)
    return reg, params


def train_pooled_spike(stacks: dict[str, pd.DataFrame]) -> tuple[PooledSpikeClassifier, dict]:
    frames = []
    for code, region in enumerate(REGIONS):
        df = stacks[region].dropna(subset=["y_price", "rrp_lag_0"]).copy()
        df["region_code"] = code
        frames.append(df)
    pooled = pd.concat(frames, ignore_index=True)
    feats = sorted({c for df in frames for c in feature_columns(df)} | {"region_code"})
    val_start = TEST_START - pd.Timedelta(days=VAL_DAYS)
    fit = pooled[pooled["issue_time"] < val_start]
    val = pooled[(pooled["issue_time"] >= val_start) & (pooled["issue_time"] < TEST_START)]
    y_fit, y_val = (fit["y_price"] > SPIKE).to_numpy(), (val["y_price"] > SPIKE).to_numpy()
    weight = SPIKE_CLASS_WEIGHT  # moderate up-weighting; the full neg/pos ratio (~770) saturates probabilities at 1.0
    clf = lgb.LGBMClassifier(n_estimators=400, learning_rate=0.05, num_leaves=31, min_child_samples=50,
                             scale_pos_weight=weight, subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
                             verbose=-1).fit(fit.reindex(columns=feats), y_fit)
    # Operating point = alert budget (a desk choice, fixed before seeing test results), not F1-tuned: F1-tuning picked
    # p == 1.0 because validation spikes were mostly continuations of an already-running spike.
    val_prob = clf.predict_proba(val.reindex(columns=feats))[:, 1]
    threshold = float(np.quantile(val_prob, 1 - SPIKE_ALERT_BUDGET))
    stats = {"spike_fit_events": int(y_fit.sum()), "spike_val_events": int(y_val.sum()),
             "spike_alert_budget": SPIKE_ALERT_BUDGET,
             "spike_scale_pos_weight": round(weight, 1), "spike_threshold": threshold}
    print(f"Pooled spike classifier: {stats}")
    return PooledSpikeClassifier(clf, feats, threshold), stats


def fit_target(stacked: pd.DataFrame, feats: list[str], target: str, region: str,
               spike: PooledSpikeClassifier | None) -> tuple[dict, list, list]:
    """Fit on train minus the last VAL_DAYS, calibrate on validation, evaluate once on the held-out test set."""
    y_col, base_col = f"y_{target}", f"persistence_{target}"
    data = stacked.dropna(subset=[y_col, base_col, BASE_FEATURE[target]])
    val_start = TEST_START - pd.Timedelta(days=VAL_DAYS)
    fit = data[data["issue_time"] < val_start]
    val = data[(data["issue_time"] >= val_start) & (data["issue_time"] < TEST_START)]
    test = data[data["issue_time"] >= TEST_START]
    reg, params = train_models(fit, feats, target, y_col)
    code = REGIONS.index(region)
    clf = spike if target == "price" else None

    def predict(df: pd.DataFrame) -> np.ndarray:
        return df[BASE_FEATURE[target]].to_numpy() + reg.predict(df[feats])

    val_resid = val[y_col].to_numpy() - predict(val)
    resid_q = {int(h): tuple(np.quantile(val_resid[(val["horizon"] == h).to_numpy()], [0.1, 0.9])) for h in HORIZONS}
    threshold = clf.threshold if clf is not None else None
    pred = predict(test)
    residuals = test[y_col].to_numpy() - pred
    prob = clf.proba(test, code) if clf is not None else np.full(len(test), np.nan)

    metric_rows = []
    for h in HORIZONS:
        m = (test["horizon"] == h).to_numpy()
        row = regression_metrics(test[y_col].to_numpy()[m], pred[m], test[base_col].to_numpy()[m], MAPE_FLOORS[target])
        if clf is not None and threshold is not None:
            row.update(spike_metrics(test[y_col].to_numpy()[m] > SPIKE, prob[m], threshold))
        lo, hi = resid_q[h]
        row["interval_coverage_80"] = float(np.mean((residuals[m] >= lo) & (residuals[m] <= hi)))
        metric_rows.append({"target": target, "region": region, "horizon_intervals": h, "n_train": len(fit),
                            "train_start": str(fit["issue_time"].min()), "train_end": str(fit["issue_time"].max()),
                            **row})
    lower = pred + np.array([resid_q[int(h)][0] for h in test["horizon"]])
    upper = pred + np.array([resid_q[int(h)][1] for h in test["horizon"]])
    backtest = pd.DataFrame({
        "target": target, "region_id": region, "horizon_intervals": test["horizon"].to_numpy(),
        "issue_time": test["issue_time"].to_numpy(), "interval_datetime": test["target_time"].to_numpy(),
        "actual": test[y_col].to_numpy(), "predicted": pred, "persistence": test[base_col].to_numpy(),
        "lower_80": lower, "upper_80": upper, "spike_probability": prob,
        "spike_flag": prob >= threshold if clf is not None else np.zeros(len(test), dtype=bool),
    })
    info = {"model": NemForecaster(reg, clf, feats, resid_q, target, code), "regressor": reg,
            "params": {**params, "target_transform": "delta_from_latest", "spike_threshold": threshold},
            "n_train": len(fit), "train_end": str(fit["issue_time"].max()), "sample": test[feats].head(50)}
    return info, metric_rows, [backtest]

# COMMAND ----------


def register(info: dict, target: str, region: str, metric_rows: list[dict]) -> tuple[str, str, str]:
    name = f"{CATALOG}.ml.nem_{target}_forecast_{region.lower()}"
    with mlflow.start_run(run_name=f"{target}_{region}", nested=True) as run:
        mlflow.log_params({**info["params"], "target": target, "region": region, "horizons": str(HORIZONS),
                           "n_train": info["n_train"], "train_end": info["train_end"],
                           "test_start": str(TEST_START), "lookback_days": LOOKBACK})
        for r in metric_rows:
            for k, v in r.items():
                if isinstance(v, (int, float)) and v is not None and k not in ("horizon_intervals", "n_train"):
                    mlflow.log_metric(f"h{r['horizon_intervals']}_{k}", float(v))
        sample = info["sample"]
        signature = infer_signature(sample, info["model"].predict(None, sample))
        mlflow.pyfunc.log_model("model", python_model=info["model"], signature=signature,
                                input_example=sample.head(5), registered_model_name=name,
                                pip_requirements=["lightgbm==4.5.0", "pandas", "numpy", "scikit-learn"])
        run_id = run.info.run_id
    client = mlflow.MlflowClient()
    version = max(int(v.version) for v in client.search_model_versions(f"name='{name}'"))
    client.set_registered_model_alias(name, "production", str(version))
    return name, str(version), run_id


metric_rows_all: list[dict] = []
backtests: list[pd.DataFrame] = []
importances: list[dict] = []
with mlflow.start_run(run_name=f"nem_forecast_{TRAINED_AT:%Y%m%d_%H%M}") as parent:
    stacks = {region: stack_horizons(region_features(wide, region), wide, region, HORIZONS) for region in REGIONS}
    spike_model, spike_stats = train_pooled_spike(stacks)
    mlflow.log_params(spike_stats)
    for region in REGIONS:
        stacked = stacks[region]
        feats = feature_columns(stacked)
        for target in ("price", "demand"):
            info, rows, bt = fit_target(stacked, feats, target, region, spike_model)
            name, version, run_id = register(info, target, region, rows)
            for r in rows:
                r.update(model_name=name, model_version=version, mlflow_run_id=run_id)
            for b in bt:
                b["model_name"], b["model_version"] = name, version
            metric_rows_all.extend(rows)
            backtests.extend(bt)
            gain = info["regressor"].booster_.feature_importance(importance_type="gain")
            importances.extend({"target": target, "region": region, "feature": f, "importance_gain": float(g),
                                "importance_share": float(g / gain.sum()), "model_name": name, "model_version": version}
                               for f, g in zip(feats, gain))
            print(f"{region} {target}: v{version} | " + ", ".join(
                f"h{r['horizon_intervals']} MAE {r['mae']:.1f} (persistence {r['persistence_mae']:.1f})" for r in rows))
    parent_run_id = parent.info.run_id

# COMMAND ----------

stamp = {"trained_at": TRAINED_AT, "test_start": TEST_START.to_pydatetime(), "parent_run_id": parent_run_id}
metrics_df = pd.DataFrame(metric_rows_all).assign(**stamp)
backtest_df = pd.concat(backtests, ignore_index=True).assign(trained_at=TRAINED_AT)
importance_df = pd.DataFrame(importances).assign(trained_at=TRAINED_AT)
for df, table in ((metrics_df, "forecast_model_metrics"), (backtest_df, "forecast_backtest"),
                  (importance_df, "forecast_feature_importance")):
    (spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.gold.{table}"))
    print(f"wrote {len(df)} rows to gold.{table}")

headline = (metrics_df[metrics_df["target"] == "price"]
            .groupby("horizon_intervals")[["mae", "persistence_mae", "spike_recall", "spike_precision"]].mean())
summary = {"parent_run_id": parent_run_id, "data_start": str(wide.index.min()), "data_end": str(wide.index.max()),
           "test_start": str(TEST_START), "models_registered": int(metrics_df["model_name"].nunique()),
           "pooled_spike_classifier": spike_stats,
           "price_by_horizon": json.loads(headline.round(3).to_json(orient="index"))}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
