"""
Price Forecast -- Backtesting Evaluation (Sprint 8a)
=====================================================
Loads the production model for each NEM region from MLflow Model Registry,
reconstructs the exact features defined in feature_engineering.py and
train.py, computes deep metrics with spike recall, per-horizon MAE,
volatility-regime breakdown, and AEMO pre-dispatch baseline comparison.

Definition-of-Done thresholds (must ALL pass to promote champion -> production):
  - Overall MAE  < $15/MWh
  - Spike MAE    < $200/MWh
  - MAPE         < 12%

Metrics computed per region:
  - Overall MAE, RMSE, MAPE
  - Spike recall  : % of actual spikes (>$300/MWh) predicted as spikes (>$300)
  - Spike MAE     : MAE on spike intervals only
  - Per-horizon MAE: one row per forecast_horizon (1,4,8,12,24,48)
  - Volatility-regime breakdown: MAE per regime (low/medium/high)
  - AEMO pre-dispatch baseline comparison (silver.dispatch_prices P5MIN)

Outputs:
  - gold.price_forecast_evaluation Delta table
  - MLflow artifact: price_forecast_eval_report.json
  - Promotion table when --promote flag is given and ALL regions pass DoD

Unit-test mode (spark=None):
  Uses synthetic NEM-like data (lognormal base + ~1% spike injection) so
  the module can be smoke-tested without a Spark / Databricks environment.

Usage
-----
  python -m models.price_forecast.evaluate --region NSW1
  python -m models.price_forecast.evaluate --all-regions
  python -m models.price_forecast.evaluate --all-regions --promote
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Constants -- aligned with feature_engineering.py and train.py
# ---------------------------------------------------------------------------

CATALOG        = "energy_copilot"
GOLD           = f"{CATALOG}.gold"
SILVER         = f"{CATALOG}.silver"
FEATURE_TABLE  = f"{GOLD}.feature_store_price"
DISPATCH_TABLE = f"{SILVER}.dispatch_prices"      # P5MIN column for baseline
EVAL_TABLE     = f"{GOLD}.price_forecast_evaluation"

NEM_REGIONS: List[str] = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

# Model registry URI: energy_copilot.ml.price_forecast_{region}@production
MODEL_REGISTRY_TMPL = "energy_copilot.ml.price_forecast_{region}"
MODEL_ALIAS         = "production"
CHAMPION_ALIAS      = "champion"
MLFLOW_EXPERIMENT   = "energy_copilot_price_evaluation"

# Spike threshold (matches train.py SPIKE_WEIGHT_THRESHOLD = 300 $/MWh)
EVAL_SPIKE_THRESHOLD = 300.0   # $/MWh

# Definition-of-Done (DoD) thresholds
DOD_MAE_THRESHOLD       = 15.0    # $/MWh
DOD_SPIKE_MAE_THRESHOLD = 200.0   # $/MWh
DOD_MAPE_THRESHOLD      = 12.0    # %

# Forecast horizons (5-min intervals) -- matches feature_engineering.FORECAST_HORIZONS
FORECAST_HORIZONS: List[int] = [1, 4, 8, 12, 24, 48]

# Volatility regimes -- matches feature_engineering.py thresholds
VOLATILITY_REGIMES: List[str] = ["low", "medium", "high"]
VOLATILITY_LOW_THRESHOLD  = 20.0   # $/MWh rolling std
VOLATILITY_HIGH_THRESHOLD = 80.0   # $/MWh rolling std

# AEST offset (UTC+10, NEM runs on AEST year-round)
AEST_OFFSET_HOURS = 10

# Spike history threshold (matches feature_engineering.SPIKE_HISTORY_THRESHOLD)
SPIKE_HISTORY_THRESHOLD = 300.0


# ---------------------------------------------------------------------------
# Synthetic data generation (unit-test mode)
# ---------------------------------------------------------------------------

def _make_synthetic_price_data(
    region: str = "NSW1",
    n_intervals: int = 8_640,   # ~30 days of 5-min data
    seed: int = 42,
) -> pd.DataFrame:
    """Generate synthetic NEM-like price data for unit-test / smoke-test mode.

    Price distribution:
      - Lognormal base: median ~$55/MWh (typical NEM off-peak)
      - ~1% spike injection: $300-$14,000/MWh (NEM market cap events)
      - ~0.5% negative injection: -$1,000 to -$50/MWh (excess renewables)
    """
    rng = np.random.default_rng(seed)
    base_dt = datetime(2025, 1, 1)
    timestamps = [base_dt + timedelta(minutes=5 * i) for i in range(n_intervals)]

    # Base price: lognormal, median ~$55/MWh
    prices = rng.lognormal(mean=4.0, sigma=0.5, size=n_intervals)

    # Inject spikes (~1%)
    n_spikes = max(1, int(n_intervals * 0.01))
    spike_idx = rng.choice(n_intervals, size=n_spikes, replace=False)
    prices[spike_idx] = rng.uniform(350, 14_000, size=n_spikes)

    # Inject negatives (~0.5%)
    n_neg = max(1, int(n_intervals * 0.005))
    neg_idx = rng.choice(n_intervals, size=n_neg, replace=False)
    prices[neg_idx] = rng.uniform(-1_000, -50, size=n_neg)

    demand = rng.normal(7_000, 1_500, size=n_intervals).clip(2_000, 14_000)

    return pd.DataFrame({
        "settlementdate": timestamps,
        "regionid":       region,
        "rrp":            prices,
        "totaldemand":    demand,
    })


def _engineer_features_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Reconstruct the EXACT features from feature_engineering.py and train.py
    using pandas (for unit-test mode / offline validation).

    Features reconstructed:
      Temporal (cyclical encoding):
        hour_sin, hour_cos, dow_sin, dow_cos, month_sin, month_cos
      Temporal (flags):
        is_peak_period (07-09 and 17-20 AEST)
        is_business_day (Mon-Fri)
        days_to_quarter_end
      Lag features for price (lags 1,2,3,6,12,24,48 x 5-min intervals):
        price_lag_{k}
      Rolling mean/std (30-min=6i, 1h=12i, 4h=48i, 24h=288i):
        price_roll_mean_*, price_roll_std_*
      Volatility regime (5-day = 1440-interval rolling std):
        price_std_5day
        volatility_regime one-hot (low/medium/high)
      Spike history:
        spike_count_24h (288-interval window)
        spike_count_7d  (2016-interval window)
      FCAS (zeros in unit-test mode):
        raise6sec_price_lag1, raise6sec_price_lag6
      Interconnector stress (zeros in unit-test mode):
        nem_ic_stress_mw
      Season one-hot: season_summer, season_autumn, season_winter, season_spring
      forecast_horizon (int): 1, 4, 8, 12, 24, 48
      rrp_target (float): future price at settlementdate + horizon*5min
    """
    df = df.copy()
    df["settlementdate"] = pd.to_datetime(df["settlementdate"])
    df = df.sort_values(["regionid", "settlementdate"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Temporal features
    # ------------------------------------------------------------------
    aest_hour = (df["settlementdate"].dt.hour + AEST_OFFSET_HOURS) % 24
    hour      = df["settlementdate"].dt.hour
    dow       = df["settlementdate"].dt.dayofweek   # Mon=0
    month     = df["settlementdate"].dt.month

    # Cyclical encoding (sin/cos)
    df["hour_sin"]  = np.sin(2 * np.pi * hour / 24)
    df["hour_cos"]  = np.cos(2 * np.pi * hour / 24)
    df["dow_sin"]   = np.sin(2 * np.pi * dow / 7)
    df["dow_cos"]   = np.cos(2 * np.pi * dow / 7)
    df["month_sin"] = np.sin(2 * np.pi * month / 12)
    df["month_cos"] = np.cos(2 * np.pi * month / 12)

    # Peak period: 07-09 and 17-20 AEST (matches feature_engineering.py)
    df["is_peak_period"] = (
        aest_hour.between(7, 8) | aest_hour.between(17, 19)
    ).astype(int)

    # Business day: Mon-Fri
    df["is_business_day"] = (dow < 5).astype(int)

    # Days to quarter end (matches feature_engineering.quarter_end_day logic)
    def _days_to_quarter_end(ts: pd.Timestamp) -> int:
        m, y = ts.month, ts.year
        if m <= 3:
            qend = pd.Timestamp(y, 3, 31)
        elif m <= 6:
            qend = pd.Timestamp(y, 6, 30)
        elif m <= 9:
            qend = pd.Timestamp(y, 9, 30)
        else:
            qend = pd.Timestamp(y, 12, 31)
        ts_date = ts.replace(hour=0, minute=0, second=0, microsecond=0)
        return max(0, (qend - ts_date).days)

    df["days_to_quarter_end"] = df["settlementdate"].apply(_days_to_quarter_end)

    # ------------------------------------------------------------------
    # Lag features for price (lags 1,2,3,6,12,24,48 x 5-min intervals)
    # ------------------------------------------------------------------
    for lag in [1, 2, 3, 6, 12, 24, 48]:
        df[f"price_lag_{lag}"] = df.groupby("regionid")["rrp"].shift(lag)

    # ------------------------------------------------------------------
    # Rolling mean/std (30-min=6i, 1h=12i, 4h=48i, 24h=288i)
    # ------------------------------------------------------------------
    window_map = {"30min": 6, "1h": 12, "4h": 48, "24h": 288}
    for label, win in window_map.items():
        df[f"price_roll_mean_{label}"] = df.groupby("regionid")["rrp"].transform(
            lambda x, w=win: x.shift(1).rolling(w, min_periods=1).mean()
        )
        df[f"price_roll_std_{label}"] = df.groupby("regionid")["rrp"].transform(
            lambda x, w=win: x.shift(1).rolling(w, min_periods=1).std().fillna(0.0)
        )

    # ------------------------------------------------------------------
    # Volatility regime (5-day = 1440 intervals rolling std)
    # ------------------------------------------------------------------
    df["price_std_5day"] = df.groupby("regionid")["rrp"].transform(
        lambda x: x.shift(1).rolling(1440, min_periods=1).std().fillna(0.0)
    )
    df["volatility_regime"] = pd.cut(
        df["price_std_5day"],
        bins=[-np.inf, VOLATILITY_LOW_THRESHOLD, VOLATILITY_HIGH_THRESHOLD, np.inf],
        labels=["low", "medium", "high"],
    ).astype(str)

    # ------------------------------------------------------------------
    # Spike history counts (24h=288i, 7d=2016i)
    # ------------------------------------------------------------------
    df["_spike_flag"] = (df["rrp"] > SPIKE_HISTORY_THRESHOLD).astype(int)
    for label, win in [("24h", 288), ("7d", 2016)]:
        df[f"spike_count_{label}"] = df.groupby("regionid")["_spike_flag"].transform(
            lambda x, w=win: x.shift(1).rolling(w, min_periods=1).sum().fillna(0.0)
        ).astype(int)
    df.drop(columns=["_spike_flag"], inplace=True)

    # ------------------------------------------------------------------
    # FCAS features (zeros in synthetic/unit-test mode)
    # Mirrors feature_engineering.build_fcas_features output columns
    # ------------------------------------------------------------------
    df["raise6sec_price_lag1"] = 0.0
    df["raise6sec_price_lag6"] = 0.0

    # ------------------------------------------------------------------
    # Interconnector stress (zeros in synthetic/unit-test mode)
    # Mirrors feature_engineering.build_interconnector_features output
    # ------------------------------------------------------------------
    df["nem_ic_stress_mw"] = 0.0

    # ------------------------------------------------------------------
    # Season one-hot (matches train.py encode_categoricals)
    # ------------------------------------------------------------------
    month_col = df["settlementdate"].dt.month
    df["season"] = "spring"
    df.loc[month_col.isin([12, 1, 2]), "season"] = "summer"
    df.loc[month_col.isin([3, 4, 5]),  "season"] = "autumn"
    df.loc[month_col.isin([6, 7, 8]),  "season"] = "winter"
    season_dummies = pd.get_dummies(df["season"], prefix="season", drop_first=False)
    df = pd.concat([df.drop(columns=["season"]), season_dummies], axis=1)

    # ------------------------------------------------------------------
    # Volatility regime one-hot (matches train.py encode_categoricals)
    # ------------------------------------------------------------------
    regime_dummies = pd.get_dummies(
        df["volatility_regime"], prefix="volatility_regime", drop_first=False
    )
    df = pd.concat([df.drop(columns=["volatility_regime"]), regime_dummies], axis=1)

    # ------------------------------------------------------------------
    # Explode forecast horizons and build rrp_target via price lookup
    # ------------------------------------------------------------------
    price_lookup = df.set_index(["regionid", "settlementdate"])["rrp"].to_dict()

    horizon_dfs = []
    for horizon in FORECAST_HORIZONS:
        hdf = df.copy()
        hdf["forecast_horizon"] = horizon
        future_timestamps = hdf["settlementdate"] + pd.Timedelta(minutes=5 * horizon)
        hdf["rrp_target"] = [
            float(price_lookup.get((row["regionid"], fts), float("nan")))
            for (_, row), fts in zip(hdf.iterrows(), future_timestamps)
        ]
        horizon_dfs.append(hdf)

    df_exploded = pd.concat(horizon_dfs, ignore_index=True)
    df_exploded = df_exploded.dropna(subset=["rrp_target"]).reset_index(drop=True)
    return df_exploded


# ---------------------------------------------------------------------------
# Load model from MLflow
# ---------------------------------------------------------------------------

def load_production_model(region: str) -> mlflow.pyfunc.PyFuncModel:
    """Load the model registered under alias production for a given region.

    Model URI: models:/energy_copilot.ml.price_forecast_{region}@production
    where {region} is the lowercased region code without trailing 1
    (e.g. NSW1 -> nsw, QLD1 -> qld).
    """
    region_slug = region.lower().replace("1", "")
    model_name  = MODEL_REGISTRY_TMPL.format(region=region_slug)
    model_uri   = f"models:/{model_name}@{MODEL_ALIAS}"
    logger.info("Loading model: %s", model_uri)
    try:
        model = mlflow.pyfunc.load_model(model_uri)
    except mlflow.exceptions.MlflowException as exc:
        raise RuntimeError(
            f"Could not load model {model_uri!r}. "
            "Ensure train.py has been run and the model registered with alias production."
        ) from exc
    return model


# ---------------------------------------------------------------------------
# Load test data from Spark feature store
# ---------------------------------------------------------------------------

def load_test_data_spark(spark, region: str):
    """Load held-out test data from gold.feature_store_price.

    Reads val_end/test_end boundaries from the production model MLflow run tags.
    Applies the same categorical encoding as train.py (season and
    volatility_regime are one-hot encoded).

    Returns (df, val_end, test_end).
    """
    import pyspark.sql.functions as F

    client      = MlflowClient()
    region_slug = region.lower().replace("1", "")
    model_name  = MODEL_REGISTRY_TMPL.format(region=region_slug)
    mv          = client.get_model_version_by_alias(model_name, MODEL_ALIAS)
    run         = client.get_run(mv.run_id)

    val_end_str  = run.data.tags.get("val_end")
    test_end_str = run.data.tags.get("test_end")
    if not (val_end_str and test_end_str):
        raise ValueError(
            f"val_end / test_end tags missing from MLflow run {mv.run_id}. "
            "Re-run train.py to regenerate the model with proper split tags."
        )
    val_end  = datetime.fromisoformat(val_end_str)
    test_end = datetime.fromisoformat(test_end_str)

    logger.info(
        "Loading test data for %s: [%s, %s)",
        region, val_end.date(), test_end.date(),
    )

    df_spark = (
        spark.table(FEATURE_TABLE)
        .filter(F.col("regionid") == region)
        .filter(F.col("settlementdate") >= F.lit(val_end.isoformat()))
        .filter(F.col("settlementdate") <  F.lit(test_end.isoformat()))
        .orderBy("settlementdate", "forecast_horizon")
    )
    df = df_spark.toPandas()

    # Season one-hot (mirrors train.py encode_categoricals)
    if "season" in df.columns:
        dummies = pd.get_dummies(df["season"], prefix="season", drop_first=False)
        df = pd.concat([df.drop(columns=["season"]), dummies], axis=1)

    # Volatility regime one-hot
    if "volatility_regime" in df.columns:
        dummies = pd.get_dummies(
            df["volatility_regime"], prefix="volatility_regime", drop_first=False
        )
        df = pd.concat([df.drop(columns=["volatility_regime"]), dummies], axis=1)

    return df, val_end, test_end


# ---------------------------------------------------------------------------
# AEMO pre-dispatch baseline from silver.dispatch_prices
# ---------------------------------------------------------------------------

def load_predispatch_baseline(
    spark,
    region: str,
    val_end: datetime,
    test_end: datetime,
) -> Optional[pd.DataFrame]:
    """Load AEMO P5MIN pre-dispatch prices from silver.dispatch_prices.

    The p5min column is the AEMO 5-minute ahead pre-dispatch price, used
    as the naive baseline to quantify model improvement over AEMO forecasts.
    Returns None if the table or column is unavailable.
    """
    import pyspark.sql.functions as F

    try:
        df_spark = (
            spark.table(DISPATCH_TABLE)
            .filter(F.col("regionid") == region)
            .filter(F.col("settlementdate") >= F.lit(val_end.isoformat()))
            .filter(F.col("settlementdate") <  F.lit(test_end.isoformat()))
            .select(
                F.col("settlementdate"),
                F.col("p5min").alias("baseline_pred"),
            )
        )
        df = df_spark.toPandas()
        return df if not df.empty else None
    except Exception as exc:    # noqa: BLE001
        logger.warning("Pre-dispatch baseline unavailable: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Core metric computation
# ---------------------------------------------------------------------------

def _safe_mean(arr: np.ndarray) -> float:
    """Return mean of arr, or nan if empty."""
    return float(np.mean(arr)) if len(arr) > 0 else float("nan")


def compute_core_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
) -> Dict[str, float]:
    """Compute overall MAE, RMSE, MAPE."""
    eps     = 1e-6
    abs_err = np.abs(y_true - y_pred)
    sq_err  = (y_true - y_pred) ** 2
    pct_err = abs_err / (np.abs(y_true) + eps)
    return {
        "mae_aud_mwh":  float(np.mean(abs_err)),
        "rmse_aud_mwh": float(np.sqrt(np.mean(sq_err))),
        "mape_pct":     float(np.mean(pct_err) * 100.0),
    }


def compute_spike_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
) -> Dict[str, float]:
    """Compute spike recall and spike MAE.

    Spike definition: actual price > EVAL_SPIKE_THRESHOLD ($300/MWh).
    Spike prediction: predicted price > EVAL_SPIKE_THRESHOLD.
    Spike recall  = TP / (TP + FN).
    Spike MAE     = MAE on real spike intervals (DoD: < $200/MWh).
    SPIKE_WEIGHT  = 3.0 was applied during training, so spike errors are
    penalised more heavily in training; this metric evaluates the result.
    """
    spike_mask      = y_true > EVAL_SPIKE_THRESHOLD
    pred_spike_mask = y_pred > EVAL_SPIKE_THRESHOLD

    tp = int((spike_mask & pred_spike_mask).sum())
    fn = int((spike_mask & ~pred_spike_mask).sum())

    spike_recall = tp / (tp + fn) if (tp + fn) > 0 else float("nan")
    spike_mae    = _safe_mean(np.abs(y_true[spike_mask] - y_pred[spike_mask]))

    return {
        "spike_recall_pct":  spike_recall * 100.0 if not np.isnan(spike_recall) else float("nan"),
        "spike_mae_aud_mwh": spike_mae,
        "n_true_spikes":     int(spike_mask.sum()),
        "n_pred_spikes":     int(pred_spike_mask.sum()),
    }


def compute_horizon_metrics(
    df: pd.DataFrame,
    y_true: np.ndarray,
    y_pred: np.ndarray,
) -> List[Dict]:
    """Per-horizon MAE for each forecast_horizon value (1,4,8,12,24,48).

    Returns one dict per horizon with mae_aud_mwh, rmse_aud_mwh, mape_pct,
    spike_recall_pct, spike_mae_aud_mwh, horizon_intervals, n_intervals.
    """
    horizon_rows = []
    for h in sorted(df["forecast_horizon"].unique()):
        mask = (df["forecast_horizon"] == h).values
        if mask.sum() == 0:
            continue
        yt, yp = y_true[mask], y_pred[mask]
        core_m  = compute_core_metrics(yt, yp)
        spike_m = compute_spike_metrics(yt, yp)
        horizon_rows.append({
            "horizon_intervals": int(h),
            "n_intervals":       int(mask.sum()),
            **core_m,
            **spike_m,
        })
    return horizon_rows


def compute_regime_metrics(
    df: pd.DataFrame,
    y_true: np.ndarray,
    y_pred: np.ndarray,
) -> List[Dict]:
    """MAE broken down by volatility_regime (low/medium/high).

    Handles both one-hot encoded regime columns (Spark/feature-store path)
    and the raw string column (synthetic unit-test path).
    """
    if "volatility_regime" in df.columns:
        regimes = df["volatility_regime"].astype(str)
    else:
        # One-hot representation: reconstruct string label
        regime_series = pd.Series("unknown", index=df.index)
        for r in VOLATILITY_REGIMES:
            col = f"volatility_regime_{r}"
            if col in df.columns:
                regime_series = regime_series.where(df[col] != 1, r)
        regimes = regime_series

    regime_rows = []
    for regime in VOLATILITY_REGIMES:
        mask = (regimes == regime).values
        if mask.sum() == 0:
            continue
        yt, yp = y_true[mask], y_pred[mask]
        regime_rows.append({
            "volatility_regime": regime,
            "mae_aud_mwh":       _safe_mean(np.abs(yt - yp)),
            "n_intervals":       int(mask.sum()),
        })
    return regime_rows


# ---------------------------------------------------------------------------
# DoD check
# ---------------------------------------------------------------------------

def check_dod(
    mae_aud_mwh: float,
    spike_mae_aud_mwh: float,
    mape_pct: float,
) -> bool:
    """Return True when all DoD thresholds are satisfied.

    spike_mae nan (no spikes in test set) is treated as passing the spike DoD
    so the overall result is not penalised for spike-free evaluation periods.
    """
    if np.isnan(mae_aud_mwh) or np.isnan(mape_pct):
        return False
    mae_ok   = mae_aud_mwh  < DOD_MAE_THRESHOLD
    mape_ok  = mape_pct     < DOD_MAPE_THRESHOLD
    spike_ok = np.isnan(spike_mae_aud_mwh) or (spike_mae_aud_mwh < DOD_SPIKE_MAE_THRESHOLD)
    return bool(mae_ok and mape_ok and spike_ok)


# ---------------------------------------------------------------------------
# Evaluate one region (Spark / production path)
# ---------------------------------------------------------------------------

def evaluate_region_spark(spark, region: str) -> Dict:
    """Full evaluation pipeline for one region using the Spark feature store."""
    model                      = load_production_model(region)
    df_test, val_end, test_end = load_test_data_spark(spark, region)

    # Align feature columns to model schema (handles column drift)
    model_feat_cols = model.metadata.get_input_schema().input_names()
    for col in model_feat_cols:
        if col not in df_test.columns:
            df_test[col] = 0.0
    X_test = df_test[model_feat_cols]

    y_true = df_test["rrp_target"].values
    y_pred = model.predict(X_test)

    core    = compute_core_metrics(y_true, y_pred)
    spike   = compute_spike_metrics(y_true, y_pred)
    horizon = compute_horizon_metrics(df_test, y_true, y_pred)
    regime  = compute_regime_metrics(df_test, y_true, y_pred)
    passed  = check_dod(core["mae_aud_mwh"], spike["spike_mae_aud_mwh"], core["mape_pct"])

    # AEMO pre-dispatch baseline comparison
    baseline_improvement_pct: Optional[float] = None
    baseline_df = load_predispatch_baseline(spark, region, val_end, test_end)
    if baseline_df is not None:
        joined = df_test.reset_index(drop=True).merge(
            baseline_df, on="settlementdate", how="inner"
        )
        if not joined.empty:
            bl_true = joined["rrp_target"].values
            bl_pred = joined["baseline_pred"].values
            baseline_mae = float(np.mean(np.abs(bl_true - bl_pred)))
            if baseline_mae > 0:
                baseline_improvement_pct = (
                    (baseline_mae - core["mae_aud_mwh"]) / baseline_mae * 100.0
                )
                logger.info(
                    "%s | Baseline MAE: %.2f | Model MAE: %.2f | Improvement: %.1f%%",
                    region, baseline_mae, core["mae_aud_mwh"],
                    baseline_improvement_pct,
                )

    result = {
        "region":          region,
        "eval_date":       datetime.utcnow().date().isoformat(),
        "n_test_rows":     int(len(df_test)),
        "passed_dod":      passed,
        "core_metrics":    core,
        "spike_metrics":   spike,
        "horizon_metrics": horizon,
        "regime_metrics":  regime,
        "vs_baseline_mae_improvement_pct": baseline_improvement_pct,
    }

    _log_region_summary(region, core, spike, passed)
    return result


# ---------------------------------------------------------------------------
# Evaluate one region (unit-test / offline path)
# ---------------------------------------------------------------------------

def evaluate_region_offline(region: str) -> Dict:
    """Evaluate using synthetic data (no Spark required).

    Generates lognormal + spike-injected price data, reconstructs all features
    with _engineer_features_pandas, and uses a naive persistence forecast as
    the model proxy (price_lag_1 = last observed price).

    Validates:
      - Feature reconstruction produces expected columns
      - Metric computation handles spikes, horizons, and regimes correctly
      - All metric functions are numerically stable
    """
    logger.info("Unit-test mode: evaluating %s with synthetic data", region)

    raw_df = _make_synthetic_price_data(region=region)
    df     = _engineer_features_pandas(raw_df)

    if df.empty:
        raise RuntimeError(f"Empty DataFrame after feature engineering for {region}")

    # Naive persistence: predict last observed price (price_lag_1)
    lag_col  = "price_lag_1"
    fill_val = float(df["rrp"].mean()) if "rrp" in df.columns else 50.0
    y_pred   = (
        df[lag_col].fillna(fill_val).values
        if lag_col in df.columns
        else np.full(len(df), fill_val)
    )

    y_true = df["rrp_target"].values
    valid  = ~(np.isnan(y_true) | np.isnan(y_pred))
    y_true, y_pred = y_true[valid], y_pred[valid]
    df_valid = df[valid].reset_index(drop=True)

    core    = compute_core_metrics(y_true, y_pred)
    spike   = compute_spike_metrics(y_true, y_pred)
    horizon = compute_horizon_metrics(df_valid, y_true, y_pred)
    regime  = compute_regime_metrics(df_valid, y_true, y_pred)
    passed  = check_dod(core["mae_aud_mwh"], spike["spike_mae_aud_mwh"], core["mape_pct"])

    result = {
        "region":          region,
        "eval_date":       datetime.utcnow().date().isoformat(),
        "n_test_rows":     int(len(df_valid)),
        "passed_dod":      passed,
        "core_metrics":    core,
        "spike_metrics":   spike,
        "horizon_metrics": horizon,
        "regime_metrics":  regime,
        "vs_baseline_mae_improvement_pct": None,
    }

    _log_region_summary(region, core, spike, passed)
    return result


def _log_region_summary(
    region: str,
    core:   Dict[str, float],
    spike:  Dict[str, float],
    passed: bool,
) -> None:
    """Print a structured evaluation summary for one region."""
    logger.info("=" * 60)
    logger.info("Region: %s  |  DoD: %s", region, "PASS" if passed else "FAIL")
    logger.info(
        "  MAE  : %.2f $/MWh  (threshold < %.1f)",
        core["mae_aud_mwh"], DOD_MAE_THRESHOLD,
    )
    logger.info("  RMSE : %.2f $/MWh", core["rmse_aud_mwh"])
    logger.info(
        "  MAPE : %.2f %%     (threshold < %.1f)",
        core["mape_pct"], DOD_MAPE_THRESHOLD,
    )
    logger.info(
        "  Spike Recall : %.1f %%  (n=%d true / %d pred spikes)",
        spike.get("spike_recall_pct", float("nan")),
        spike.get("n_true_spikes", 0),
        spike.get("n_pred_spikes", 0),
    )
    logger.info(
        "  Spike MAE    : %.2f $/MWh  (threshold < %.1f)",
        spike.get("spike_mae_aud_mwh", float("nan")),
        DOD_SPIKE_MAE_THRESHOLD,
    )


# ---------------------------------------------------------------------------
# MLflow logging -- price_{region}_{metric} naming convention
# ---------------------------------------------------------------------------

def log_region_to_mlflow(region: str, result: Dict) -> None:
    """Log per-region metrics to the active MLflow run.

    Naming convention: price_{region_lower}_{metric_name}
    Examples:
      price_nsw1_mae_aud_mwh, price_nsw1_spike_recall_pct,
      price_nsw1_mae_h1, price_nsw1_mae_regime_low,
      price_nsw1_vs_baseline_mae_improvement_pct
    """
    region_tag = region.lower()
    core       = result["core_metrics"]
    spike      = result["spike_metrics"]
    metrics: Dict[str, float] = {}

    # Core metrics
    for k, v in core.items():
        if isinstance(v, float) and not np.isnan(v):
            metrics[f"price_{region_tag}_{k}"] = v

    # Spike metrics
    for k, v in spike.items():
        if isinstance(v, float) and not np.isnan(v):
            metrics[f"price_{region_tag}_{k}"] = v
        elif isinstance(v, int):
            metrics[f"price_{region_tag}_{k}"] = float(v)

    # Per-horizon MAE
    for hrow in result.get("horizon_metrics", []):
        h   = hrow["horizon_intervals"]
        mae = hrow.get("mae_aud_mwh", float("nan"))
        if not np.isnan(mae):
            metrics[f"price_{region_tag}_mae_h{h}"] = mae

    # Volatility-regime MAE
    for rrow in result.get("regime_metrics", []):
        r   = rrow["volatility_regime"]
        mae = rrow.get("mae_aud_mwh", float("nan"))
        if not np.isnan(mae):
            metrics[f"price_{region_tag}_mae_regime_{r}"] = mae

    # Baseline improvement
    bl = result.get("vs_baseline_mae_improvement_pct")
    if bl is not None:
        bl_f = float(bl)
        if not np.isnan(bl_f):
            metrics[f"price_{region_tag}_vs_baseline_mae_improvement_pct"] = bl_f

    # DoD pass/fail (0.0 or 1.0 for MLflow numeric metric)
    metrics[f"price_{region_tag}_passed_dod"] = 1.0 if result["passed_dod"] else 0.0

    mlflow.log_metrics(metrics)
    logger.info("Logged %d MLflow metrics for %s", len(metrics), region)


# ---------------------------------------------------------------------------
# Save evaluation results to Delta table
# ---------------------------------------------------------------------------

def save_to_delta(spark, results: List[Dict]) -> None:
    """Write evaluation results to gold.price_forecast_evaluation.

    Schema:
      region (str), eval_date (str), horizon_intervals (int),
      mae_aud_mwh (double), rmse_aud_mwh (double), mape_pct (double),
      spike_recall_pct (double), spike_mae_aud_mwh (double),
      volatility_regime (str), passed_dod (boolean)

    Row types:
      horizon_intervals=-1, volatility_regime=all  ->  overall aggregate
      horizon_intervals=h,  volatility_regime=all  ->  per-horizon row
      horizon_intervals=-1, volatility_regime=r    ->  per-regime row
    """
    rows = []
    for r in results:
        region     = r["region"]
        eval_date  = r["eval_date"]
        passed_dod = bool(r["passed_dod"])
        core       = r["core_metrics"]
        spike      = r["spike_metrics"]

        # Overall aggregate row
        rows.append({
            "region":            region,
            "eval_date":         eval_date,
            "horizon_intervals": -1,
            "volatility_regime": "all",
            "mae_aud_mwh":       core["mae_aud_mwh"],
            "rmse_aud_mwh":      core["rmse_aud_mwh"],
            "mape_pct":          core["mape_pct"],
            "spike_recall_pct":  spike.get("spike_recall_pct", float("nan")),
            "spike_mae_aud_mwh": spike.get("spike_mae_aud_mwh", float("nan")),
            "passed_dod":        passed_dod,
        })

        # Per-horizon rows
        for hrow in r.get("horizon_metrics", []):
            rows.append({
                "region":            region,
                "eval_date":         eval_date,
                "horizon_intervals": hrow["horizon_intervals"],
                "volatility_regime": "all",
                "mae_aud_mwh":       hrow.get("mae_aud_mwh", float("nan")),
                "rmse_aud_mwh":      hrow.get("rmse_aud_mwh", float("nan")),
                "mape_pct":          hrow.get("mape_pct", float("nan")),
                "spike_recall_pct":  hrow.get("spike_recall_pct", float("nan")),
                "spike_mae_aud_mwh": hrow.get("spike_mae_aud_mwh", float("nan")),
                "passed_dod":        passed_dod,
            })

        # Per-regime rows
        for rrow in r.get("regime_metrics", []):
            rows.append({
                "region":            region,
                "eval_date":         eval_date,
                "horizon_intervals": -1,
                "volatility_regime": rrow["volatility_regime"],
                "mae_aud_mwh":       rrow.get("mae_aud_mwh", float("nan")),
                "rmse_aud_mwh":      float("nan"),
                "mape_pct":          float("nan"),
                "spike_recall_pct":  float("nan"),
                "spike_mae_aud_mwh": float("nan"),
                "passed_dod":        passed_dod,
            })

    eval_spark_df = spark.createDataFrame(pd.DataFrame(rows))
    (
        eval_spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(EVAL_TABLE)
    )
    logger.info("Evaluation results written to %s (%d rows)", EVAL_TABLE, len(rows))


# ---------------------------------------------------------------------------
# Promotion: set champion alias when ALL regions pass DoD
# ---------------------------------------------------------------------------

def promote_champion(results: List[Dict]) -> None:
    """Set the champion alias on each region model when ALL regions pass DoD.

    Prints a formatted promotion table.  The champion alias marks the model
    as ready for use; production remains the alias evaluate.py loads from.
    """
    all_pass = all(r["passed_dod"] for r in results)

    header = (
        f"\n{chr(39)}Region{chr(39):<2} {chr(39)}MAE{chr(39):>8} {chr(39)}MAPE{chr(39):>7} "
        f"{chr(39)}SpkMAE{chr(39):>9} {chr(39)}SpkRcl{chr(39):>8} {chr(39)}DoD{chr(39):>6}"
    )
    # Build header without embedded quotes
    col1, col2, col3, col4, col5, col6 = "Region", "MAE", "MAPE", "SpkMAE", "SpkRcl", "DoD"
    header_line = f"\n{col1:<8} {col2:>8} {col3:>7} {col4:>9} {col5:>8} {col6:>6}"
    separator   = "-" * 54
    print(header_line)
    print(separator)

    for r in results:
        core      = r["core_metrics"]
        spike     = r["spike_metrics"]
        status    = "PASS" if r["passed_dod"] else "FAIL"
        spike_mae = spike.get("spike_mae_aud_mwh", float("nan"))
        spike_rcl = spike.get("spike_recall_pct",  float("nan"))
        reg = r["region"]
        mae = core["mae_aud_mwh"]
        mape = core["mape_pct"]
        print(
            f"{reg:<8}"
            f" {mae:>7.2f}"
            f" {mape:>6.2f}%"
            f" {spike_mae:>8.2f}"
            f" {spike_rcl:>7.1f}%"
            f" {status:>6}"
        )

    print(separator)

    if all_pass:
        print("\nAll regions passed DoD -- setting champion alias.")
        client = MlflowClient()
        for r in results:
            region      = r["region"]
            region_slug = region.lower().replace("1", "")
            model_name  = MODEL_REGISTRY_TMPL.format(region=region_slug)
            try:
                mv = client.get_model_version_by_alias(model_name, MODEL_ALIAS)
                client.set_registered_model_alias(
                    model_name, CHAMPION_ALIAS, mv.version
                )
                logger.info(
                    "Promoted %s v%s to alias champion",
                    model_name, mv.version,
                )
            except Exception as exc:    # noqa: BLE001
                logger.warning("Could not promote %s: %s", model_name, exc)
    else:
        failing = [r["region"] for r in results if not r["passed_dod"]]
        print(f"\nPromotion blocked -- failing regions: {chr(44).join(failing)}")
        print(
            f"DoD: MAE < {DOD_MAE_THRESHOLD} $/MWh, "
            f"MAPE < {DOD_MAPE_THRESHOLD}%, "
            f"Spike MAE < {DOD_SPIKE_MAE_THRESHOLD} $/MWh"
        )


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------

def run_evaluation(
    spark=None,
    regions: Optional[List[str]] = None,
    promote: bool = False,
) -> List[Dict]:
    """Run the full evaluation pipeline.

    Parameters
    ----------
    spark   : SparkSession or None (triggers unit-test mode)
    regions : NEM region IDs to evaluate; defaults to all 5
    promote : if True, set champion alias when all regions pass DoD

    Returns
    -------
    List of per-region result dicts with metrics and DoD flags.
    """
    regions = regions or NEM_REGIONS
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    results: List[Dict] = []

    with mlflow.start_run(run_name="price_forecast_evaluation"):
        for region in regions:
            try:
                if spark is None:
                    result = evaluate_region_offline(region)
                else:
                    result = evaluate_region_spark(spark, region)

                log_region_to_mlflow(region, result)
                results.append(result)

            except Exception as exc:    # noqa: BLE001
                logger.error(
                    "Evaluation failed for %s: %s", region, exc, exc_info=True
                )

        # Save JSON report artifact
        report_path = "/tmp/price_forecast_eval_report.json"
        with open(report_path, "w") as fh:
            json.dump(results, fh, indent=2, default=str)
        mlflow.log_artifact(report_path, artifact_path="evaluation")

        # Write Delta table (Spark path only)
        if spark is not None and results:
            save_to_delta(spark, results)

        # Promotion decision
        if promote and results:
            promote_champion(results)

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate NEM price forecast models (Sprint 8a)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="Evaluate a single NEM region (e.g. NSW1).",
    )
    parser.add_argument(
        "--all-regions",
        action="store_true",
        default=False,
        help="Evaluate all 5 NEM regions.",
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        default=False,
        help="Set champion alias when all regions pass DoD.",
    )
    parser.add_argument(
        "--unit-test",
        action="store_true",
        default=False,
        help="Run in unit-test mode (spark=None, synthetic data).",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()

    if args.region and args.all_regions:
        logger.error("--region and --all-regions are mutually exclusive.")
        sys.exit(1)

    target_regions: Optional[List[str]] = None
    if args.region:
        if args.region not in NEM_REGIONS:
            logger.error(
                "Unknown region %r. Valid: %s", args.region, NEM_REGIONS
            )
            sys.exit(1)
        target_regions = [args.region]
    elif args.all_regions:
        target_regions = NEM_REGIONS
    else:
        target_regions = NEM_REGIONS

    if args.unit_test:
        spark_session = None
    else:
        try:
            from pyspark.sql import SparkSession as _SparkSession
            spark_session = _SparkSession.builder.getOrCreate()
        except ImportError:
            logger.warning("PySpark not available -- falling back to unit-test mode.")
            spark_session = None

    run_evaluation(
        spark=spark_session,
        regions=target_regions,
        promote=args.promote,
    )
