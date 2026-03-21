"""
Solar Generation Forecast -- LightGBM Training
================================================
Trains one LightGBM model per NEM region (5 models) to predict total solar
generation (MW) at each dispatch interval across multiple forecast horizons.

Solar-specific design decisions:
  - Night-time intervals (hour_of_day < 6 or >= 20) are excluded from training
    to prevent the model learning a trivial "predict 0" policy for dark hours.
    At inference time, the forecast pipeline zero-clamps predictions for dark
    hours directly without invoking the model.
  - Lags of solar AC power (1-12 intervals, ~5-60 min) capture inertia in
    cloud cover and irradiance conditions.
  - Capacity factor normalises output against the installed fleet size, which
    improves cross-temporal and cross-horizon generalisation.
  - Daylight-hour flag and sun angle proxy (|hour - 12| / 6) encode the
    position-in-day effect without requiring continuous solar geometry inputs.
  - Season sin/cos (based on day_of_year) encode the winter/summer solar cycle
    in a smooth, cyclically-continuous way suitable for tree models.
  - Cloud proxy (1 - radiation / max_possible) estimates cloud cover from the
    ratio of observed shortwave radiation to the peak clear-sky envelope.
  - Unit test mode (spark=None) generates synthetic solar data with zero at
    night and a bell curve during daylight hours for CI/CD runs.

Target column    : solar_generation_target (MW)
Feature source   : gold.feature_store_price
Model registry   : "solar_forecast_{region}" with alias "production"
MLflow experiment: /energy_copilot/solar_forecast_training

Split: 18 months train | 3 months val | 3 months test (chronological).
Hyperparameter tuning: Optuna 50 trials, TPE sampler + MedianPruner, MAE objective.
MAPE target: < 10% (solar is harder than demand due to cloud-cover variability).
"""

from __future__ import annotations

import json
import logging
import math
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

MLFLOW_EXPERIMENT = "/energy_copilot/solar_forecast_training"
MODEL_NAME_TMPL   = "solar_forecast_{region}"

TRAIN_MONTHS = 18
VAL_MONTHS   = 3
TEST_MONTHS  = 3

OPTUNA_TRIALS = 50
OPTUNA_SEED   = 42

LABEL_COL          = "solar_generation_target"
FORECAST_HORIZONS: List[int] = [1, 4, 8, 12, 24, 48]

# Night-time hours (AEST) where solar output is definitionally zero.
# Training rows for these hours are excluded to prevent a "predict-zero" bias.
# At inference, the forecast pipeline applies zero-clamping for dark hours.
NIGHT_HOURS = set(range(0, 6)) | set(range(20, 24))   # 8 pm – 6 am AEST

# Installed solar capacity (MW) per NEM region — large-scale + embedded distributed.
# Source: approximate fleet sizes based on AEMO Generation Information 2024.
INSTALLED_SOLAR_CAPACITY_MW: Dict[str, float] = {
    "NSW1": 4500.0,
    "QLD1": 4000.0,
    "VIC1": 3500.0,
    "SA1":  2500.0,
    "TAS1":  300.0,
}

# Peak clear-sky irradiance (W/m²) — standard value; used in cloud proxy
PEAK_IRRADIANCE_WM2 = 1000.0

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

MAPE_TARGET_PCT = 10.0   # solar is harder than demand; target < 10%


# ---------------------------------------------------------------------------
# Solar-specific feature engineering (pandas)
# ---------------------------------------------------------------------------

def add_solar_features(df: pd.DataFrame, region: str) -> pd.DataFrame:
    """
    Compute solar-specific engineered features on a pandas DataFrame.

    Requires columns: ac_power_mw or gen_solar_mw (observed solar generation),
    solar_radiation_wm2 or shortwave_radiation (observed irradiance),
    hour_of_day (0-23), day_of_year (1-366).

    Falls back gracefully when columns are absent (fills with 0 / NaN).

    Parameters
    ----------
    df : pd.DataFrame
        Feature DataFrame sorted chronologically within a single region.
        Must contain hour_of_day and day_of_year (or derivable from settlementdate).
    region : str
        NEM region identifier (e.g. 'NSW1').  Used for capacity factor lookup.

    Returns
    -------
    pd.DataFrame with additional columns added in-place.
    """
    df = df.copy()

    # --- Ensure hour_of_day is available ---
    if "hour_of_day" not in df.columns and "settlementdate" in df.columns:
        df["hour_of_day"] = pd.to_datetime(df["settlementdate"]).dt.hour

    # --- Ensure day_of_year is available ---
    if "day_of_year" not in df.columns and "settlementdate" in df.columns:
        df["day_of_year"] = pd.to_datetime(df["settlementdate"]).dt.dayofyear

    hour = df["hour_of_day"] if "hour_of_day" in df.columns else pd.Series(12, index=df.index)
    doy  = df["day_of_year"] if "day_of_year" in df.columns else pd.Series(180, index=df.index)

    # --- Determine the solar AC power / generation column for lags ---
    solar_gen_col = None
    for candidate in ("ac_power_mw", "gen_solar_mw", "solar_generation_mw", "solar_total_mw"):
        if candidate in df.columns:
            solar_gen_col = candidate
            break

    # --- Determine the irradiance column for cloud proxy ---
    radiation_col = None
    for candidate in ("solar_radiation_wm2", "shortwave_radiation", "shortwave_radiation_wm2"):
        if candidate in df.columns:
            radiation_col = candidate
            break

    # 1. Solar generation lags (intervals 1-12, i.e. 5-60 min)
    if solar_gen_col is not None:
        sg = df[solar_gen_col]
        for lag in range(1, 13):
            df[f"solar_lag_{lag}"] = sg.shift(lag)
    else:
        logger.warning("solar generation column not found; solar_lag_* columns set to 0")
        for lag in range(1, 13):
            df[f"solar_lag_{lag}"] = 0.0

    # 2. Solar capacity factor
    installed_mw = INSTALLED_SOLAR_CAPACITY_MW.get(region, 1000.0)
    if solar_gen_col is not None:
        df["solar_capacity_factor"] = (df[solar_gen_col] / installed_mw).clip(lower=0.0, upper=1.05)
    else:
        df["solar_capacity_factor"] = 0.0

    # 3. Daylight-hour flag: 1 if hour between 6 and 19 (AEST), else 0
    df["is_daylight_hour"] = ((hour >= 6) & (hour <= 19)).astype(int)

    # 4. Sun angle proxy: abs(hour_of_day - 12) / 6.0
    #    0.0 at noon (maximum sun), 1.0 at 6am/6pm (horizon)
    df["sun_angle_proxy"] = (hour - 12.0).abs() / 6.0

    # 5. Season sin/cos: smooth encoding of annual solar cycle
    df["season_sin"] = np.sin(2 * math.pi * doy / 365.0)
    df["season_cos"] = np.cos(2 * math.pi * doy / 365.0)

    # 6. Cloud proxy: 1 - (observed_radiation / max_possible_radiation)
    #    max_possible_radiation = PEAK_IRRADIANCE_WM2 * sin(sun_elevation_angle)
    #    We approximate sin(elevation) using the sun_angle_proxy inverted:
    #    sin_elevation ≈ max(0, 1 - sun_angle_proxy) = cos(sun_angle_proxy * π/2)
    sin_elevation = np.cos(df["sun_angle_proxy"] * (math.pi / 2)).clip(lower=0.0)
    max_possible  = PEAK_IRRADIANCE_WM2 * sin_elevation

    if radiation_col is not None:
        observed_rad = df[radiation_col].clip(lower=0.0)
        # Avoid division by zero when sun is below the horizon
        safe_max = max_possible.replace(0.0, np.nan)
        df["cloud_proxy"] = (1.0 - (observed_rad / safe_max)).clip(lower=0.0, upper=1.0)
        df["cloud_proxy"] = df["cloud_proxy"].fillna(1.0)   # night = fully "cloudy" proxy
    else:
        logger.warning("solar radiation column not found; cloud_proxy set to 0")
        df["cloud_proxy"] = 0.0

    # 7. Night zero-handling: for intervals where solar is definitionally zero,
    #    set the target directly to 0. (Complement of night exclusion during training;
    #    at inference time the pipeline also applies zero-clamping.)
    if LABEL_COL in df.columns:
        night_mask = (hour < 6) | (hour >= 20)
        df.loc[night_mask, LABEL_COL] = 0.0

    return df


# ---------------------------------------------------------------------------
# Synthetic data for unit-test mode (spark=None)
# ---------------------------------------------------------------------------

def _make_synthetic_solar_data(region: str, n_intervals: int = 52560) -> pd.DataFrame:
    """
    Generate synthetic solar generation data for unit-test / CI mode.

    Produces a time series with zero at night and a bell-curve (Gaussian-shaped)
    profile during daylight hours, plus Gaussian noise. Output is clamped to
    [0, installed_capacity_mw].

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
    rng        = np.random.default_rng(seed=42)
    start_dt   = datetime(2022, 1, 1)
    dates      = [start_dt + timedelta(minutes=5 * i) for i in range(n_intervals)]
    hours      = np.array([d.hour + d.minute / 60.0 for d in dates])
    doys       = np.array([d.timetuple().tm_yday for d in dates])

    installed  = INSTALLED_SOLAR_CAPACITY_MW.get(region, 2000.0)

    # Bell curve during daylight: peak at solar noon (~12:30 AEST mid-summer)
    # Seasonal modulation: peaks in summer (Jan), dips in winter (Jul)
    seasonal_scale = 0.75 + 0.25 * np.cos(2 * math.pi * (doys - 15) / 365.0)
    bell           = np.exp(-0.5 * ((hours - 12.5) / 3.0) ** 2)
    gen_solar      = installed * seasonal_scale * bell
    gen_solar     += rng.normal(0, installed * 0.04, n_intervals)
    gen_solar      = np.clip(gen_solar, 0.0, installed)

    # Zero at night (before 6am and after 8pm)
    night_mask = (hours < 6.0) | (hours >= 20.0)
    gen_solar[night_mask] = 0.0

    # Irradiance proportional to generation (simplified)
    radiation = (gen_solar / installed) * PEAK_IRRADIANCE_WM2
    radiation = np.clip(radiation + rng.normal(0, 20, n_intervals), 0.0, PEAK_IRRADIANCE_WM2)

    df = pd.DataFrame({
        "settlementdate":      dates,
        "regionid":            region,
        "gen_solar_mw":        gen_solar,
        "solar_radiation_wm2": radiation,
        "totaldemand":         7500.0 + rng.normal(0, 400, n_intervals),
        "rrp":                 45.0  + rng.normal(0, 25, n_intervals),
        "hour_of_day":         [d.hour for d in dates],
        "day_of_week":         [d.weekday() for d in dates],
        "month":               [d.month for d in dates],
        "day_of_year":         doys.tolist(),
        "forecast_horizon":    1,
    })

    # Add target column (shift gen_solar_mw forward by 1 interval)
    df[LABEL_COL] = df["gen_solar_mw"].shift(-1).fillna(method="ffill")
    return df


# ---------------------------------------------------------------------------
# Data loading and feature engineering
# ---------------------------------------------------------------------------

def add_solar_target(spark, df_spark):
    """
    Add solar_generation_target by self-joining future gen_solar_mw values.

    Cross-joins the base feature table with all forecast horizons, then
    looks up the actual solar generation at each future timestamp.
    Night-time rows are excluded from the result to prevent a trivial
    zero-prediction policy from dominating the model.
    """
    import pyspark.sql.types as T
    from pyspark.sql import functions as F

    horizons_df = spark.createDataFrame(
        [(h,) for h in FORECAST_HORIZONS],
        schema=T.StructType([T.StructField("forecast_horizon", T.IntegerType(), False)]),
    )
    df_x = df_spark.crossJoin(horizons_df).withColumn(
        "_ts",
        (F.col("settlementdate").cast("long") + F.col("forecast_horizon") * 300).cast("timestamp"),
    )
    future = df_spark.select(
        F.col("settlementdate").alias("_fts"),
        F.col("regionid").alias("_fr"),
        F.col("gen_solar_mw").alias(LABEL_COL),
    ).distinct()

    df_x = (
        df_x.join(
            future,
            (F.col("_ts") == F.col("_fts")) & (F.col("regionid") == F.col("_fr")),
            "left",
        )
        .drop("_ts", "_fts", "_fr")
        .filter(F.col(LABEL_COL).isNotNull())
        # Exclude night-time rows from training — solar generation is trivially zero
        .filter(~F.col("hour_of_day").isin(list(NIGHT_HOURS)))
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
    Load, label, engineer features, and split data for a single NEM region.

    When spark is None, falls back to synthetic solar data for unit-test mode.
    Night-time intervals are excluded from training splits.

    Returns
    -------
    Tuple of (train_df, val_df, test_df) as pandas DataFrames.
    """
    if spark is None:
        logger.warning("Running in unit test mode with synthetic data")
        df = _make_synthetic_solar_data(region)
        # Add solar-specific features before splitting
        df = df.sort_values("settlementdate").reset_index(drop=True)
        df = add_solar_features(df, region)
        # Exclude night rows from all splits (consistent with Spark path)
        df = df[~df["hour_of_day"].isin(NIGHT_HOURS)].copy()
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

    # add_solar_target already excludes night hours via PySpark filter
    df_spark = add_solar_target(spark, df_spark_base)
    df = df_spark.toPandas()

    # One-hot encode season if present
    if "season" in df.columns:
        dummies = pd.get_dummies(df["season"], prefix="season", drop_first=False)
        df = pd.concat([df.drop(columns=["season"]), dummies], axis=1)

    # Sort chronologically before computing lags / rolling features
    df = df.sort_values("settlementdate").reset_index(drop=True)
    df = add_solar_features(df, region)

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
    """
    Compute MAE, RMSE, MAPE, and bias for solar generation forecasts.

    Note: metrics are computed on daylight-only intervals (night rows excluded
    from training and evaluation to avoid trivial-zero inflation of accuracy).
    """
    eps     = 1.0   # avoid division by zero for near-zero solar output
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

    The search space matches the sprint specification for solar models.
    Uses MAE as the optimisation criterion (symmetric, robust to outliers).
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
        # Clamp to zero before computing MAE — solar cannot be negative
        y_pred_val = np.maximum(booster.predict(X_val), 0.0)
        return float(np.mean(np.abs(y_val - y_pred_val)))
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
    Train, evaluate, and register a solar forecast model for one NEM region.

    Steps:
      1. Load + engineer features via prepare_region_data() (night rows excluded).
      2. Optuna hyperparameter search (50 trials, MedianPruner, MAE objective).
      3. Retrain final model on train + val combined.
      4. Evaluate on held-out test set, log metrics + feature importances.
      5. Register to MLflow Model Registry with alias "production".
    """
    logger.info("=== Solar forecast training: %s ===", region)
    df_train, df_val, df_test = prepare_region_data(
        spark, region, train_end, val_end, test_end
    )

    feat_cols = get_feature_columns(df_train)
    # Drop NaN rows introduced by lag feature computation
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
        mlflow.log_param("optuna_trials",        OPTUNA_TRIALS)
        mlflow.set_tag("night_hours_excluded",   str(sorted(NIGHT_HOURS)))

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
        mlflow.set_tag("region",               region)
        mlflow.set_tag("phase",                "final_training")
        mlflow.set_tag("train_end",            train_end.isoformat())
        mlflow.set_tag("val_end",              val_end.isoformat())
        mlflow.set_tag("test_end",             test_end.isoformat())
        mlflow.set_tag("night_hours_excluded", str(sorted(NIGHT_HOURS)))

        # Log best Optuna params as best_param_* tags (for programmatic retrieval)
        for k, v in study.best_params.items():
            mlflow.set_tag(f"best_param_{k}", str(v))
        mlflow.log_params({f"param_{k}": v for k, v in study.best_params.items()})
        mlflow.log_param("feature_columns", json.dumps(feat_cols))
        mlflow.log_param("n_features",      len(feat_cols))

        booster = lgb.train(
            best_params,
            lgb.Dataset(X_final, label=y_final),
        )

        # Clamp predictions to >= 0 (solar generation cannot be negative)
        y_pred_test = np.maximum(booster.predict(X_test), 0.0)
        test_metrics = compute_metrics(y_test, y_pred_test)
        mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})
        logger.info("Test metrics for %s: %s", region, test_metrics)

        # Log MAPE compliance tag
        mape = test_metrics["mape_pct"]
        mlflow.set_tag("mape_target_met",   str(mape < MAPE_TARGET_PCT))
        mlflow.set_tag("mape_target_pct",   str(MAPE_TARGET_PCT))
        mlflow.set_tag("mape_achieved_pct", f"{mape:.2f}")

        # Log feature importances as JSON artifact
        importance_dict = dict(
            zip(feat_cols, booster.feature_importance(importance_type="gain").tolist())
        )
        importance_sorted = dict(
            sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
        )
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
        logger.info("Registered %s v%d as 'production'", model_name, latest)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def train_all_regions(spark=None) -> None:
    """
    Train solar forecast models for all 5 NEM regions.

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
    logger.info("All solar forecast models trained.")


if __name__ == "__main__":
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    except ImportError:
        spark = None
        logger.warning("PySpark not available — running in unit test mode")
    train_all_regions(spark)
