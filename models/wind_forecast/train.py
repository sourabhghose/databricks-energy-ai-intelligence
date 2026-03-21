"""
Wind Generation Forecast -- LightGBM Training
================================================
Trains one LightGBM model per NEM region (5 models) to predict total wind
generation (MW) at each dispatch interval for multiple forecast horizons.

Wind-specific design decisions:
  - Wind speed lags (1-12 intervals) capture momentum in the wind resource.
  - 4-hour rolling mean and std dev capture sustained wind events and variability.
  - Ramp rate (difference from 2 intervals ago) is a proxy for wind ramping events
    which are operationally significant for system security.
  - Capacity factor (wind_total_mw / installed_capacity_mw) normalises output
    against installed fleet size, improving cross-temporal generalisation.
  - High/low wind binary flags encode the cut-in (<3 m/s) and strong-wind (>10 m/s)
    regimes where the power curve shape changes substantially.
  - Unit test mode (spark=None) generates synthetic sine-wave wind data with
    realistic MW ranges (0-2000 MW) for CI/CD without a live Spark session.

Target column    : wind_generation_target (MW)
Feature source   : gold.feature_store_price (includes wind gen lags,
                   windspeed_100m current + NWP forecasts, calendar)
Model registry   : "wind_forecast_{region}" with alias "production"
MLflow experiment: /energy_copilot/wind_forecast_training

Split: 18 months train | 3 months val | 3 months test (chronological).
Hyperparameter tuning: Optuna 50 trials, TPE sampler + MedianPruner, MAE objective.
MAPE target: < 8% (wind is inherently harder to forecast than demand).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import optuna
import pandas as pd
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG       = "energy_copilot"
GOLD          = f"{CATALOG}.gold"
FEATURE_TABLE = f"{GOLD}.feature_store_price"

NEM_REGIONS: List[str] = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

MLFLOW_EXPERIMENT = "/energy_copilot/wind_forecast_training"
MODEL_NAME_TMPL   = "wind_forecast_{region}"

TRAIN_MONTHS = 18
VAL_MONTHS   = 3
TEST_MONTHS  = 3

OPTUNA_TRIALS = 50
OPTUNA_SEED   = 42

LABEL_COL          = "wind_generation_target"
FORECAST_HORIZONS: List[int] = [1, 4, 8, 12, 24, 48]

# Installed wind capacity (MW) per NEM region — used for capacity factor feature.
# Source: approximate fleet sizes based on AEMO Generation Information 2024.
INSTALLED_WIND_CAPACITY_MW: Dict[str, float] = {
    "NSW1": 3500.0,
    "QLD1": 2500.0,
    "VIC1": 2500.0,
    "SA1":  1800.0,
    "TAS1":  700.0,
}

# Wind speed thresholds (m/s)
WIND_HIGH_THRESHOLD = 10.0   # above this: strong wind regime
WIND_LOW_THRESHOLD  =  3.0   # below this: cut-in speed; turbines not generating

EXCLUDED_COLS = {
    "settlementdate", "settlement_date", "regionid", "rrp", "totaldemand",
    "rrp_target", LABEL_COL, "feature_timestamp", "season",
}

LGB_FIXED_PARAMS: Dict[str, Any] = {
    "objective":     "regression_l1",
    "metric":        "mae",
    "boosting_type": "gbdt",
    "n_jobs":        -1,
    "verbose":       -1,
    "random_state":  42,
}

MAPE_TARGET_PCT = 8.0   # wind is harder than demand; target < 8%


# ---------------------------------------------------------------------------
# Wind-specific feature engineering (pandas)
# ---------------------------------------------------------------------------

def add_wind_features(df: pd.DataFrame, region: str) -> pd.DataFrame:
    """
    Compute wind-specific engineered features on a pandas DataFrame.

    Requires columns: wind_speed_100m_ms (wind speed at hub height),
    wind_total_mw or gen_wind_mw (observed wind generation).
    Falls back gracefully when columns are absent (fills with 0 / NaN).

    Parameters
    ----------
    df : pd.DataFrame
        Feature DataFrame sorted chronologically within a single region.
    region : str
        NEM region identifier (e.g. 'NSW1').  Used for capacity factor lookup.

    Returns
    -------
    pd.DataFrame with additional columns added in-place.
    """
    df = df.copy()

    # --- Determine the wind speed column (may vary by feature store version) ---
    wind_speed_col = None
    for candidate in ("wind_speed_100m_ms", "windspeed_100m", "wind_speed_ms"):
        if candidate in df.columns:
            wind_speed_col = candidate
            break

    # --- Determine the wind generation column ---
    wind_gen_col = None
    for candidate in ("gen_wind_mw", "wind_total_mw", "wind_generation_mw"):
        if candidate in df.columns:
            wind_gen_col = candidate
            break

    # 1. Lags of wind speed (intervals 1-12 ~ 5 min to 1 hour)
    if wind_speed_col is not None:
        ws = df[wind_speed_col]
        for lag in range(1, 13):
            df[f"wind_lag_{lag}"] = ws.shift(lag)

        # 2. 4-hour rolling stats (48 x 5-min intervals = 4 h)
        df["wind_roll_mean_4h"] = ws.shift(1).rolling(window=48, min_periods=1).mean()
        df["wind_roll_std_4h"]  = ws.shift(1).rolling(window=48, min_periods=1).std().fillna(0.0)

        # 3. Ramp rate: difference between current and 2-interval-ago wind speed
        df["wind_ramp_rate"] = ws - ws.shift(2)

        # 5. High / low wind binary flags
        df["is_high_wind"] = (ws > WIND_HIGH_THRESHOLD).astype(int)
        df["is_low_wind"]  = (ws < WIND_LOW_THRESHOLD).astype(int)
    else:
        logger.warning("wind speed column not found; wind feature columns set to 0")
        for lag in range(1, 13):
            df[f"wind_lag_{lag}"] = 0.0
        df["wind_roll_mean_4h"] = 0.0
        df["wind_roll_std_4h"]  = 0.0
        df["wind_ramp_rate"]    = 0.0
        df["is_high_wind"]      = 0
        df["is_low_wind"]       = 0

    # 4. Wind capacity factor
    installed_mw = INSTALLED_WIND_CAPACITY_MW.get(region, 1000.0)
    if wind_gen_col is not None:
        df["wind_capacity_factor"] = (df[wind_gen_col] / installed_mw).clip(lower=0.0, upper=1.05)
    else:
        df["wind_capacity_factor"] = 0.0

    return df


# ---------------------------------------------------------------------------
# Synthetic data for unit-test mode (spark=None)
# ---------------------------------------------------------------------------

def _make_synthetic_wind_data(region: str, n_intervals: int = 52560) -> pd.DataFrame:
    """
    Generate synthetic wind generation data for unit-test / CI mode.

    Produces realistic-looking wind generation time series using a sine-wave
    base (diurnal + seasonal) plus Gaussian noise, clamped to [0, 2000] MW.

    Parameters
    ----------
    region : str
        NEM region identifier — used to scale installed capacity.
    n_intervals : int
        Number of 5-minute intervals to generate (default 52560 ≈ 6 months).

    Returns
    -------
    pd.DataFrame with columns matching the feature store schema.
    """
    rng   = np.random.default_rng(seed=42)
    t     = np.arange(n_intervals)

    # Diurnal cycle (peaks at night for wind in Australia) + seasonal cycle
    diurnal  = np.sin(2 * np.pi * t / (288))          # 288 intervals / day
    seasonal = np.sin(2 * np.pi * t / (52560))         # ~6-month cycle
    noise    = rng.normal(0, 0.12, n_intervals)

    # Wind speed (m/s) — roughly 5-15 m/s range
    wind_speed = 8.0 + 4.0 * diurnal + 2.0 * seasonal + noise * 2.0
    wind_speed = np.clip(wind_speed, 0.0, 25.0)

    # Wind generation roughly proportional to wind_speed^3 (power curve simplified)
    installed = INSTALLED_WIND_CAPACITY_MW.get(region, 2000.0)
    gen_wind  = installed * np.clip((wind_speed / 12.5) ** 3, 0.0, 1.0)
    gen_wind += rng.normal(0, installed * 0.05, n_intervals)
    gen_wind  = np.clip(gen_wind, 0.0, installed)

    start_dt  = datetime(2022, 1, 1)
    dates     = [start_dt + timedelta(minutes=5 * i) for i in range(n_intervals)]

    df = pd.DataFrame({
        "settlementdate":     dates,
        "regionid":           region,
        "wind_speed_100m_ms": wind_speed,
        "gen_wind_mw":        gen_wind,
        "totaldemand":        8000.0 + rng.normal(0, 500, n_intervals),
        "rrp":                50.0  + rng.normal(0, 30, n_intervals),
        "hour_of_day":        [(start_dt + timedelta(minutes=5 * i)).hour for i in range(n_intervals)],
        "day_of_week":        [(start_dt + timedelta(minutes=5 * i)).weekday() for i in range(n_intervals)],
        "month":              [(start_dt + timedelta(minutes=5 * i)).month for i in range(n_intervals)],
        "forecast_horizon":   1,
    })

    # Add the target column (shift gen_wind_mw forward by 1 interval as simple proxy)
    df[LABEL_COL] = df["gen_wind_mw"].shift(-1).fillna(method="ffill")
    return df


# ---------------------------------------------------------------------------
# Data loading and feature engineering
# ---------------------------------------------------------------------------

def add_wind_target(spark, df_spark):
    """
    Add wind_generation_target by self-joining future gen_wind_mw values.

    Cross-joins the base feature table with all forecast horizons, then
    looks up the actual wind generation at each future timestamp.
    """
    import pyspark.sql.types as T
    from pyspark.sql import functions as F

    horizons_df = spark.createDataFrame(
        [(h,) for h in FORECAST_HORIZONS],
        schema=T.StructType([T.StructField("forecast_horizon", T.IntegerType(), False)]),
    )
    df_x = df_spark.crossJoin(horizons_df)
    df_x = df_x.withColumn(
        "_target_ts",
        (F.col("settlementdate").cast("long") + F.col("forecast_horizon") * 300).cast("timestamp"),
    )
    future = df_spark.select(
        F.col("settlementdate").alias("_fts"),
        F.col("regionid").alias("_fr"),
        F.col("gen_wind_mw").alias(LABEL_COL),
    ).distinct()

    df_x = (
        df_x.join(
            future,
            (F.col("_target_ts") == F.col("_fts")) & (F.col("regionid") == F.col("_fr")),
            "left",
        )
        .drop("_target_ts", "_fts", "_fr")
        .filter(F.col(LABEL_COL).isNotNull())
    )
    return df_x


def prepare_region_data(
    spark,
    region:    str,
    train_end: datetime,
    val_end:   datetime,
    test_end:  datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load, label, and split data for a single NEM region.

    When spark is None, falls back to synthetic wind data for unit-test mode.

    Returns
    -------
    Tuple of (train_df, val_df, test_df) as pandas DataFrames.
    """
    if spark is None:
        logger.warning("Running in unit test mode with synthetic data")
        df = _make_synthetic_wind_data(region)
        df = add_wind_features(df, region)
        train_mask = df["settlementdate"] <  train_end
        val_mask   = (df["settlementdate"] >= train_end) & (df["settlementdate"] < val_end)
        test_mask  = (df["settlementdate"] >= val_end)   & (df["settlementdate"] < test_end)
        return df[train_mask].copy(), df[val_mask].copy(), df[test_mask].copy()

    from pyspark.sql import functions as F

    train_start = train_end - timedelta(days=TRAIN_MONTHS * 30)

    df_spark_base = (
        spark.table(FEATURE_TABLE)
        .filter(F.col("regionid") == region)
        .filter(F.col("settlementdate") >= F.lit(train_start.isoformat()))
        .filter(F.col("settlementdate") <  F.lit(test_end.isoformat()))
        .dropDuplicates(["settlementdate", "regionid"])
    )

    df_spark = add_wind_target(spark, df_spark_base)
    df = df_spark.toPandas()

    # One-hot encode season if present
    if "season" in df.columns:
        dummies = pd.get_dummies(df["season"], prefix="season", drop_first=False)
        df = pd.concat([df.drop(columns=["season"]), dummies], axis=1)

    # Sort chronologically before computing lags / rolling features
    df = df.sort_values("settlementdate").reset_index(drop=True)
    df = add_wind_features(df, region)

    train_mask = df["settlementdate"] <  train_end
    val_mask   = (df["settlementdate"] >= train_end) & (df["settlementdate"] < val_end)
    test_mask  = (df["settlementdate"] >= val_end)   & (df["settlementdate"] < test_end)

    return df[train_mask].copy(), df[val_mask].copy(), df[test_mask].copy()


def get_feature_columns(df: pd.DataFrame) -> List[str]:
    """Return sorted list of feature columns, excluding target and metadata cols."""
    return sorted([c for c in df.columns if c not in EXCLUDED_COLS])


# ---------------------------------------------------------------------------
# Evaluation metrics
# ---------------------------------------------------------------------------

def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """Compute MAE, RMSE, MAPE, and bias for wind generation forecasts."""
    eps     = 1.0   # avoid division by zero for near-zero wind generation
    abs_err = np.abs(y_true - y_pred)
    pct_err = abs_err / (np.abs(y_true) + eps)
    return {
        "mae":         float(np.mean(abs_err)),
        "rmse":        float(np.sqrt(np.mean(abs_err ** 2))),
        "mape_pct":    float(np.mean(pct_err) * 100.0),
        "bias_mw":     float(np.mean(y_pred - y_true)),
        "max_abs_err": float(np.max(abs_err)),
    }


# ---------------------------------------------------------------------------
# Optuna hyperparameter objective
# ---------------------------------------------------------------------------

def make_objective(X_train, y_train, X_val, y_val):
    """
    Return an Optuna objective function for LightGBM hyperparameter search.

    The search space covers the ranges specified in the sprint requirements.
    Uses MAE as the optimisation criterion (symmetric error for wind).
    """
    def objective(trial: optuna.Trial) -> float:
        params = {
            **LGB_FIXED_PARAMS,
            "n_estimators":      trial.suggest_int(  "n_estimators",      50,   800),
            "max_depth":         trial.suggest_int(  "max_depth",          3,     9),
            "learning_rate":     trial.suggest_float("learning_rate",      0.005, 0.3, log=True),
            "num_leaves":        trial.suggest_int(  "num_leaves",         15,  127),
            "min_child_samples": trial.suggest_int(  "min_child_samples",  10,  100),
            "subsample":         trial.suggest_float("subsample",          0.6,  1.0),
            "colsample_bytree":  trial.suggest_float("colsample_bytree",   0.6,  1.0),
        }
        dtrain = lgb.Dataset(X_train, label=y_train)
        dval   = lgb.Dataset(X_val,   label=y_val, reference=dtrain)
        booster = lgb.train(
            params, dtrain, valid_sets=[dval],
            callbacks=[
                lgb.early_stopping(50, verbose=False),
                lgb.log_evaluation(-1),
            ],
        )
        return float(np.mean(np.abs(y_val - booster.predict(X_val))))
    return objective


# ---------------------------------------------------------------------------
# Single-region training
# ---------------------------------------------------------------------------

def train_region(
    spark,
    region:    str,
    train_end: datetime,
    val_end:   datetime,
    test_end:  datetime,
) -> None:
    """
    Train, evaluate, and register a wind forecast model for one NEM region.

    Steps:
      1. Load + engineer features via prepare_region_data().
      2. Optuna hyperparameter search (50 trials, MedianPruner).
      3. Retrain final model on train + val combined.
      4. Evaluate on held-out test set, log metrics + feature importances.
      5. Register to MLflow Model Registry with alias "production".
    """
    logger.info("=== Wind forecast training: %s ===", region)
    df_train, df_val, df_test = prepare_region_data(
        spark, region, train_end, val_end, test_end
    )

    feat_cols = get_feature_columns(df_train)
    # Drop NaN rows introduced by lag/rolling feature computation
    df_train = df_train[feat_cols + [LABEL_COL]].dropna()
    df_val   = df_val[feat_cols   + [LABEL_COL]].dropna()
    df_test  = df_test[feat_cols  + [LABEL_COL]].dropna()

    X_train, y_train = df_train[feat_cols].values, df_train[LABEL_COL].values
    X_val,   y_val   = df_val[feat_cols].values,   df_val[LABEL_COL].values
    X_test,  y_test  = df_test[feat_cols].values,  df_test[LABEL_COL].values

    model_name = MODEL_NAME_TMPL.format(region=region.lower().replace("1", ""))

    # ------------------------------------------------------------------
    # Phase 1: Hyperparameter search
    # ------------------------------------------------------------------
    with mlflow.start_run(run_name=f"{region}_hyperopt"):
        mlflow.set_tag("region", region)
        mlflow.set_tag("phase",  "hyperparameter_tuning")
        mlflow.log_param("optuna_trials", OPTUNA_TRIALS)

        pruner = optuna.pruners.MedianPruner(n_startup_trials=5, n_warmup_steps=10)
        study  = optuna.create_study(
            direction="minimize",
            sampler=optuna.samplers.TPESampler(seed=OPTUNA_SEED),
            pruner=pruner,
        )
        study.optimize(
            make_objective(X_train, y_train, X_val, y_val),
            n_trials=OPTUNA_TRIALS,
        )
        best_params = {**LGB_FIXED_PARAMS, **study.best_params}
        mlflow.log_metric("best_val_mae", study.best_value)

    # ------------------------------------------------------------------
    # Phase 2: Final model on train + val
    # ------------------------------------------------------------------
    X_final = np.vstack([X_train, X_val])
    y_final = np.concatenate([y_train, y_val])

    with mlflow.start_run(run_name=f"{region}_final_model"):
        mlflow.set_tag("region",    region)
        mlflow.set_tag("phase",     "final_training")
        mlflow.set_tag("train_end", train_end.isoformat())
        mlflow.set_tag("val_end",   val_end.isoformat())
        mlflow.set_tag("test_end",  test_end.isoformat())

        # Log best Optuna params as best_param_* tags (for programmatic retrieval)
        for k, v in study.best_params.items():
            mlflow.set_tag(f"best_param_{k}", str(v))
        mlflow.log_params({f"param_{k}": v for k, v in study.best_params.items()})
        mlflow.log_param("feature_columns", json.dumps(feat_cols))
        mlflow.log_param("n_features", len(feat_cols))

        booster = lgb.train(
            best_params,
            lgb.Dataset(X_final, label=y_final),
        )

        # Clamp predictions to >= 0 (wind generation cannot be negative)
        y_pred_test = np.maximum(booster.predict(X_test), 0.0)
        test_metrics = compute_metrics(y_test, y_pred_test)
        mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})
        logger.info("Test metrics for %s: %s", region, test_metrics)

        # Log MAPE compliance tag
        mape = test_metrics["mape_pct"]
        mlflow.set_tag("mape_target_met",    str(mape < MAPE_TARGET_PCT))
        mlflow.set_tag("mape_target_pct",    str(MAPE_TARGET_PCT))
        mlflow.set_tag("mape_achieved_pct",  f"{mape:.2f}")

        # Log feature importances as JSON artifact
        importance_dict = dict(zip(feat_cols, booster.feature_importance(importance_type="gain").tolist()))
        importance_sorted = dict(sorted(importance_dict.items(), key=lambda x: x[1], reverse=True))
        with mlflow.start_run(run_name=f"{region}_feature_importance", nested=True):
            pass  # no-op nested run
        fi_path = f"/tmp/feature_importances_{region}.json"
        with open(fi_path, "w") as fh:
            json.dump(importance_sorted, fh, indent=2)
        mlflow.log_artifact(fi_path, artifact_path="feature_importances")
        logger.info(
            "Top-5 features for %s: %s",
            region,
            list(importance_sorted.keys())[:5],
        )

        # Register model
        signature = mlflow.models.infer_signature(
            pd.DataFrame(X_train[:5], columns=feat_cols),
            np.maximum(booster.predict(X_train[:5]), 0.0),
        )
        mlflow.lightgbm.log_model(
            booster,
            artifact_path="model",
            registered_model_name=model_name,
            signature=signature,
            input_example=pd.DataFrame(X_train[:3], columns=feat_cols),
        )

        client = MlflowClient()
        latest = max(int(v.version) for v in client.get_latest_versions(model_name))
        client.set_registered_model_alias(model_name, "production", str(latest))
        logger.info("Registered %s v%d with alias 'production'", model_name, latest)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def train_all_regions(spark=None) -> None:
    """
    Train wind forecast models for all 5 NEM regions.

    When spark is None, runs in unit-test mode with synthetic data.
    In production, spark should be an active SparkSession.
    """
    if spark is not None:
        from pyspark.sql import functions as F
        date_stats = (
            spark.table(FEATURE_TABLE)
            .agg(
                F.min("settlementdate").alias("min_dt"),
                F.max("settlementdate").alias("max_dt"),
            )
            .collect()[0]
        )
        data_start = date_stats["min_dt"]
    else:
        logger.warning("Running in unit test mode with synthetic data")
        data_start = datetime(2022, 1, 1)

    train_end = data_start + timedelta(days=TRAIN_MONTHS * 30)
    val_end   = train_end  + timedelta(days=VAL_MONTHS   * 30)
    test_end  = val_end    + timedelta(days=TEST_MONTHS  * 30)

    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    for region in NEM_REGIONS:
        train_region(spark, region, train_end, val_end, test_end)
    logger.info("All wind forecast models trained.")


if __name__ == "__main__":
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    except ImportError:
        spark = None
        logger.warning("PySpark not available — running in unit test mode")
    train_all_regions(spark)
