"""
Solar Generation Forecast -- Evaluation
==========================================
Evaluates the production solar forecast model per NEM region against the
held-out test set.  Reconstructs the exact same feature set used in train.py
(12 lags, 4h rolling stats, capacity factor, daylight flag, sun angle proxy,
season sin/cos, cloud proxy, and cyclic calendar encodings).

Night hours (NIGHT_HOURS = list(range(0, 6)) + list(range(20, 24))) are
excluded from training in train.py, and the corresponding zero-compliance
metric confirms that inference-time clamping works correctly.

Metrics computed per region:
  - Daytime-only MAPE (hours 6-19) -- PRIMARY metric, target < 10%
  - Clear-sky MAE  (cloud_proxy < 0.2 subset)
  - Zero-compliance: % of night predictions correctly clamped to 0
  - Overall MAE, RMSE
  - Seasonal MAPE: summer (DJF), autumn (MAM), winter (JJA), spring (SON)

Output:
  - gold.solar_forecast_evaluation Delta table
  - MLflow metrics logged as solar_{region}_{metric}
  - Promotion: if ALL regions daytime_mape < 10% -> alias "champion" is set

Model registry path: models:/energy_copilot.ml.solar_forecast_{region}@production

Unit test mode (spark=None):
  Generates synthetic daylight-cycle solar data (sin-curve over hours 6-18,
  zero otherwise) and validates the entire evaluate pipeline without errors.

CLI usage:
  python -m models.solar_forecast.evaluate --region NSW1
  python -m models.solar_forecast.evaluate --all-regions
  python -m models.solar_forecast.evaluate --all-regions --promote
"""

from __future__ import annotations

import argparse
import logging
import math
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Optional heavy imports (graceful for unit-test mode)
# ---------------------------------------------------------------------------
try:
    import mlflow
    import mlflow.lightgbm
    from mlflow.tracking import MlflowClient
    _MLFLOW_AVAILABLE = True
except ImportError:
    _MLFLOW_AVAILABLE = False

try:
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from pyspark.sql import SparkSession
    _SPARK_AVAILABLE = True
except ImportError:
    _SPARK_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG       = "energy_copilot"
GOLD          = f"{CATALOG}.gold"
FEATURE_TABLE = f"{GOLD}.feature_store_price"
EVAL_TABLE    = f"{GOLD}.solar_forecast_evaluation"

NEM_REGIONS: List[str] = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

MODEL_NAME_TMPL   = "energy_copilot.ml.solar_forecast_{region}"
MODEL_ALIAS       = "production"
CHAMPION_ALIAS    = "champion"
MLFLOW_EXPERIMENT = "energy_copilot_solar_evaluation"

LABEL_COL         = "solar_generation_target"
FORECAST_HORIZONS: List[int] = [1, 4, 8, 12, 24, 48]

# Night-time hours where solar output is definitionally zero.
# Must match train.py exactly.
NIGHT_HOURS: List[int] = list(range(0, 6)) + list(range(20, 24))
NIGHT_HOURS_SET = set(NIGHT_HOURS)

# Installed solar capacity (MW) per NEM region -- large-scale + embedded.
# Source: approximate fleet sizes based on AEMO Generation Information 2024.
INSTALLED_SOLAR_CAPACITY_MW: Dict[str, float] = {
    "NSW1": 4500.0,
    "QLD1": 4000.0,
    "VIC1": 3500.0,
    "SA1":  2500.0,
    "TAS1":  300.0,
}

# Peak clear-sky irradiance (W/m2) -- standard value for cloud proxy computation
PEAK_IRRADIANCE_WM2 = 1000.0

# Cloud proxy threshold for "clear sky" condition
CLEAR_SKY_CLOUD_PROXY_THRESHOLD = 0.2

# Daytime MAPE target for Definition-of-Done
MAPE_TARGET_PCT = 10.0

# Zero-clamping threshold (MW): night predictions below this are counted as 0
ZERO_CLAMP_THRESHOLD_MW = 0.5

# Season -> month mapping for seasonal MAPE
SEASON_MONTHS: Dict[str, List[int]] = {
    "summer": [12, 1, 2],    # DJF
    "autumn": [3,  4, 5],    # MAM
    "winter": [6,  7, 8],    # JJA
    "spring": [9, 10, 11],   # SON
}

EXCLUDED_COLS = {
    "settlementdate", "settlement_date", "regionid", "rrp", "totaldemand",
    "rrp_target", LABEL_COL, "feature_timestamp", "season",
}


# ---------------------------------------------------------------------------
# Solar-specific feature engineering (must mirror train.py exactly)
# ---------------------------------------------------------------------------

def add_solar_features(df: pd.DataFrame, region: str) -> pd.DataFrame:
    """
    Reconstruct the exact same solar-specific feature set used during training.

    Requires columns: gen_solar_mw (or alias), solar_radiation_wm2 (or alias),
    hour_of_day, day_of_year.  Falls back gracefully when absent.

    Features added
    --------------
    solar_lag_1 ... solar_lag_12  : solar AC power lags (5-60 min)
    solar_roll_mean_4h             : 4-hour rolling mean of solar output
    solar_roll_std_4h              : 4-hour rolling std dev of solar output
    solar_capacity_factor          : gen_solar_mw / installed capacity (clipped 0-1.05)
    is_daylight_hour               : 1 if hour 6-19, else 0
    sun_angle_proxy                : |hour - 12| / 6.0 (0=noon, 1=horizon)
    season_sin, season_cos         : smooth annual solar cycle encoding
    cloud_proxy                    : 1 - (observed_radiation / max_possible)
    hour_sin, hour_cos             : cyclic hour-of-day encoding
    dow_sin, dow_cos               : cyclic day-of-week encoding
    """
    df = df.copy()

    # Ensure temporal columns are available
    if "hour_of_day" not in df.columns and "settlementdate" in df.columns:
        df["hour_of_day"] = pd.to_datetime(df["settlementdate"]).dt.hour
    if "day_of_year" not in df.columns and "settlementdate" in df.columns:
        df["day_of_year"] = pd.to_datetime(df["settlementdate"]).dt.dayofyear
    if "day_of_week" not in df.columns and "settlementdate" in df.columns:
        df["day_of_week"] = pd.to_datetime(df["settlementdate"]).dt.dayofweek

    hour = df["hour_of_day"] if "hour_of_day" in df.columns else pd.Series(12, index=df.index)
    doy  = df["day_of_year"] if "day_of_year" in df.columns else pd.Series(180, index=df.index)

    # Resolve solar generation column
    solar_gen_col: Optional[str] = None
    for candidate in ("ac_power_mw", "gen_solar_mw", "solar_generation_mw", "solar_total_mw"):
        if candidate in df.columns:
            solar_gen_col = candidate
            break

    # Resolve irradiance column
    radiation_col: Optional[str] = None
    for candidate in ("solar_radiation_wm2", "shortwave_radiation", "shortwave_radiation_wm2"):
        if candidate in df.columns:
            radiation_col = candidate
            break

    # 1. Solar generation lags (intervals 1-12, i.e. 5-60 min)
    if solar_gen_col is not None:
        sg = df[solar_gen_col]
        for lag in range(1, 13):
            df[f"solar_lag_{lag}"] = sg.shift(lag)

        # 2. 4-hour rolling stats (48 x 5-min intervals = 4 h)
        df["solar_roll_mean_4h"] = sg.shift(1).rolling(window=48, min_periods=1).mean()
        df["solar_roll_std_4h"]  = sg.shift(1).rolling(window=48, min_periods=1).std().fillna(0.0)
    else:
        logger.warning("Solar generation column not found; solar lag columns set to 0")
        for lag in range(1, 13):
            df[f"solar_lag_{lag}"] = 0.0
        df["solar_roll_mean_4h"] = 0.0
        df["solar_roll_std_4h"]  = 0.0

    # 3. Solar capacity factor
    installed_mw = INSTALLED_SOLAR_CAPACITY_MW.get(region, 1000.0)
    if solar_gen_col is not None:
        df["solar_capacity_factor"] = (df[solar_gen_col] / installed_mw).clip(lower=0.0, upper=1.05)
    else:
        df["solar_capacity_factor"] = 0.0

    # 4. Daylight-hour flag (1 if 6 <= hour <= 19)
    df["is_daylight_hour"] = ((hour >= 6) & (hour <= 19)).astype(int)

    # 5. Sun angle proxy: abs(hour - 12) / 6.0  (0=noon, 1=6am/6pm)
    df["sun_angle_proxy"] = (hour - 12.0).abs() / 6.0

    # 6. Season sin/cos: smooth encoding of annual solar cycle
    df["season_sin"] = np.sin(2 * math.pi * doy / 365.0)
    df["season_cos"] = np.cos(2 * math.pi * doy / 365.0)

    # 7. Cloud proxy: 1 - (observed_radiation / max_possible_radiation)
    #    sin_elevation ~ cos(sun_angle_proxy * pi/2); clipped to [0, 1]
    sin_elevation = np.cos(df["sun_angle_proxy"] * (math.pi / 2.0)).clip(lower=0.0)
    max_possible  = PEAK_IRRADIANCE_WM2 * sin_elevation

    if radiation_col is not None:
        observed_rad = df[radiation_col].clip(lower=0.0)
        safe_max     = max_possible.where(max_possible > 0.0, other=np.nan)
        df["cloud_proxy"] = (1.0 - (observed_rad / safe_max)).clip(lower=0.0, upper=1.0)
        df["cloud_proxy"] = df["cloud_proxy"].fillna(1.0)  # night = fully "cloudy" proxy
    else:
        logger.warning("Solar radiation column not found; cloud_proxy set to 0")
        df["cloud_proxy"] = 0.0

    # 8. Cyclic hour and day-of-week encodings
    df["hour_sin"] = np.sin(2 * math.pi * hour / 24.0)
    df["hour_cos"] = np.cos(2 * math.pi * hour / 24.0)

    if "day_of_week" in df.columns:
        d = df["day_of_week"]
        df["dow_sin"] = np.sin(2 * math.pi * d / 7.0)
        df["dow_cos"] = np.cos(2 * math.pi * d / 7.0)

    return df


# ---------------------------------------------------------------------------
# Metric computation helpers
# ---------------------------------------------------------------------------

def _safe_mape(y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1.0) -> float:
    """MAPE with epsilon guard for near-zero values."""
    if len(y_true) == 0:
        return float("nan")
    abs_err = np.abs(y_true - y_pred)
    return float(np.mean(abs_err / (np.abs(y_true) + eps)) * 100.0)


def _safe_mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """MAE returning nan for empty arrays."""
    if len(y_true) == 0:
        return float("nan")
    return float(np.mean(np.abs(y_true - y_pred)))


def compute_zero_compliance(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    hour_arr: np.ndarray,
    threshold_mw: float = ZERO_CLAMP_THRESHOLD_MW,
) -> float:
    """
    Compute the zero-compliance metric for night-time predictions.

    Checks what percentage of night-hour intervals have predictions
    correctly clamped to (near-)zero.

    Parameters
    ----------
    y_true : np.ndarray
        Actual solar generation values (all intervals, not just night).
    y_pred : np.ndarray
        Predicted solar generation values (after zero-clamping by pipeline).
    hour_arr : np.ndarray
        Hour-of-day for each row.
    threshold_mw : float
        Predictions below this value are considered "zero" (default 0.5 MW).

    Returns
    -------
    float : percentage (0-100) of night predictions correctly near-zero.
    """
    night_mask = np.isin(hour_arr, NIGHT_HOURS)
    if not night_mask.any():
        return float("nan")
    night_preds = y_pred[night_mask]
    correct     = (night_preds < threshold_mw).sum()
    return float(correct / len(night_preds) * 100.0)


def compute_seasonal_mape(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    month_arr: np.ndarray,
    eps: float = 1.0,
) -> Dict[str, float]:
    """
    Compute per-season MAPE using daytime rows only.

    Seasons:
      summer = DJF (Dec-Jan-Feb)
      autumn = MAM (Mar-Apr-May)
      winter = JJA (Jun-Jul-Aug)
      spring = SON (Sep-Oct-Nov)

    Returns
    -------
    Dict[str, float] with keys: summer_mape_pct, autumn_mape_pct,
    winter_mape_pct, spring_mape_pct.
    """
    result: Dict[str, float] = {}
    for season, months in SEASON_MONTHS.items():
        mask = np.isin(month_arr, months)
        result[f"{season}_mape_pct"] = _safe_mape(y_true[mask], y_pred[mask], eps) if mask.any() else float("nan")
    return result


def compute_clearsky_mae(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    cloud_proxy: np.ndarray,
    threshold: float = CLEAR_SKY_CLOUD_PROXY_THRESHOLD,
) -> float:
    """
    Compute MAE on intervals where cloud_proxy < threshold (clear-sky conditions).

    Returns float("nan") if no clear-sky intervals are present.
    """
    mask = cloud_proxy < threshold
    return _safe_mae(y_true[mask], y_pred[mask]) if mask.any() else float("nan")


# ---------------------------------------------------------------------------
# Synthetic data for unit-test mode (spark=None)
# ---------------------------------------------------------------------------

def _make_synthetic_solar_data(region: str, n_intervals: int = 17520) -> pd.DataFrame:
    """
    Generate synthetic solar generation data for unit-test / CI mode.

    Produces a time series with zero at night and a Gaussian bell curve during
    daylight hours (hours 6-18), plus additive noise.  Output is clamped to
    [0, installed_capacity_mw].

    Parameters
    ----------
    region : str
        NEM region identifier -- used to scale installed capacity.
    n_intervals : int
        Number of 5-minute intervals (~61 days default for fast CI runs).

    Returns
    -------
    pd.DataFrame with columns matching the feature store schema.
    """
    rng       = np.random.default_rng(seed=42)
    start_dt  = datetime(2022, 1, 1)
    dates     = [start_dt + timedelta(minutes=5 * i) for i in range(n_intervals)]
    hours     = np.array([d.hour + d.minute / 60.0 for d in dates])
    doys      = np.array([d.timetuple().tm_yday     for d in dates])
    months    = np.array([d.month                   for d in dates])
    installed = INSTALLED_SOLAR_CAPACITY_MW.get(region, 2000.0)

    # Seasonal modulation (peak in Jan, trough in Jul for southern hemisphere)
    seasonal_scale = 0.75 + 0.25 * np.cos(2 * math.pi * (doys - 15) / 365.0)

    # Bell curve during daylight: peak at 12:30
    bell      = np.exp(-0.5 * ((hours - 12.5) / 3.0) ** 2)
    gen_solar = installed * seasonal_scale * bell
    gen_solar += rng.normal(0, installed * 0.04, n_intervals)
    gen_solar  = np.clip(gen_solar, 0.0, installed)

    # Zero at night (before 6am and from 8pm onwards)
    night_mask = (hours < 6.0) | (hours >= 20.0)
    gen_solar[night_mask] = 0.0

    # Simplified irradiance proportional to generation
    radiation = (gen_solar / installed) * PEAK_IRRADIANCE_WM2
    radiation = np.clip(radiation + rng.normal(0, 20, n_intervals), 0.0, PEAK_IRRADIANCE_WM2)

    df = pd.DataFrame({
        "settlementdate":      dates,
        "regionid":            region,
        "gen_solar_mw":        gen_solar,
        "solar_radiation_wm2": radiation,
        "totaldemand":         7500.0 + rng.normal(0, 400, n_intervals),
        "rrp":                 45.0   + rng.normal(0, 25,  n_intervals),
        "hour_of_day":         [d.hour     for d in dates],
        "day_of_week":         [d.weekday() for d in dates],
        "month":               months.tolist(),
        "day_of_year":         doys.tolist(),
        "forecast_horizon":    1,
    })

    # Target = next interval's gen_solar_mw (1-step-ahead proxy)
    df[LABEL_COL] = df["gen_solar_mw"].shift(-1).ffill()
    return df


# ---------------------------------------------------------------------------
# Data loading (Spark path)
# ---------------------------------------------------------------------------

def load_test_data_spark(spark: Any, region: str) -> pd.DataFrame:
    """
    Load the held-out test set for a region from the Gold feature store.

    Loads ALL rows (daytime and night) so that zero-compliance can be computed.
    The train split boundaries (val_end, test_end) are recovered from the
    MLflow run tags attached to the registered production model version.

    Parameters
    ----------
    spark : SparkSession
    region : str

    Returns
    -------
    pd.DataFrame ready for add_solar_features().
    """
    client   = MlflowClient()
    mn       = MODEL_NAME_TMPL.format(region=region)
    mv       = client.get_model_version_by_alias(mn, MODEL_ALIAS)
    run      = client.get_run(mv.run_id)
    val_end  = datetime.fromisoformat(run.data.tags["val_end"])
    test_end = datetime.fromisoformat(run.data.tags["test_end"])

    base = (
        spark.table(FEATURE_TABLE)
        .filter(F.col("regionid") == region)
        .filter(F.col("settlementdate") >= F.lit(val_end.isoformat()))
        .filter(F.col("settlementdate") <  F.lit(test_end.isoformat()))
        .dropDuplicates(["settlementdate", "regionid"])
    )

    # Add target column via self-join across all forecast horizons
    horizons_df = spark.createDataFrame(
        [(h,) for h in FORECAST_HORIZONS],
        schema=T.StructType([T.StructField("forecast_horizon", T.IntegerType(), False)]),
    )
    df_x = base.crossJoin(horizons_df).withColumn(
        "_target_ts",
        (F.col("settlementdate").cast("long") + F.col("forecast_horizon") * 300).cast("timestamp"),
    )
    future = base.select(
        F.col("settlementdate").alias("_fts"),
        F.col("regionid").alias("_fr"),
        F.col("gen_solar_mw").alias(LABEL_COL),
    ).distinct()

    df_x = (
        df_x.join(
            future,
            (F.col("_target_ts") == F.col("_fts")) & (F.col("regionid") == F.col("_fr")),
            "left",
        )
        .drop("_target_ts", "_fts", "_fr")
        .filter(F.col(LABEL_COL).isNotNull())
        # NOTE: we do NOT exclude night rows here -- we need them for zero-compliance.
        # They were excluded from TRAINING in train.py but are needed for evaluation.
    )
    df = df_x.toPandas()

    if "season" in df.columns:
        dummies = pd.get_dummies(df["season"], prefix="season", drop_first=False)
        df = pd.concat([df.drop(columns=["season"]), dummies], axis=1)

    return df


# ---------------------------------------------------------------------------
# Single-region evaluation
# ---------------------------------------------------------------------------

def evaluate_region(
    region: str,
    df: pd.DataFrame,
    model: Any,
) -> Dict[str, Any]:
    """
    Evaluate a solar forecast model for one NEM region.

    Steps:
    1. Feature engineering (matching train.py)
    2. Model prediction (with night-hour zero-clamping)
    3. Compute daytime MAPE, clear-sky MAE, zero-compliance, seasonal MAPE

    Parameters
    ----------
    region : str
        NEM region identifier.
    df : pd.DataFrame
        Full test DataFrame (daytime + night rows, before feature engineering).
    model : mlflow.pyfunc.PyFuncModel or None
        Production model.  None uses a persistence baseline in unit-test mode.

    Returns
    -------
    Dict with all evaluation metrics and a passed_dod flag.
    """
    logger.info("--- Evaluating solar forecast: %s ---", region)

    # Sort chronologically before computing lag/rolling features
    df = df.sort_values("settlementdate").reset_index(drop=True)
    df = add_solar_features(df, region)

    # Drop rows with NaN targets
    df = df.dropna(subset=[LABEL_COL]).reset_index(drop=True)

    # Ensure hour_of_day and month are available for masking
    if "hour_of_day" not in df.columns and "settlementdate" in df.columns:
        df["hour_of_day"] = pd.to_datetime(df["settlementdate"]).dt.hour
    if "month" not in df.columns and "settlementdate" in df.columns:
        df["month"] = pd.to_datetime(df["settlementdate"]).dt.month

    y_true   = df[LABEL_COL].values.astype(float)
    hour_arr = df["hour_of_day"].values.astype(int) if "hour_of_day" in df.columns else np.zeros(len(y_true), dtype=int)
    month_arr = df["month"].values.astype(int)       if "month"       in df.columns else np.ones(len(y_true), dtype=int)

    if model is not None:
        try:
            feat_cols = model.metadata.get_input_schema().input_names()
        except Exception:
            feat_cols = sorted([c for c in df.columns if c not in EXCLUDED_COLS])

        for c in feat_cols:
            if c not in df.columns:
                df[c] = 0.0

        df_feats = df[feat_cols].fillna(0.0)
        raw_pred = model.predict(df_feats)
    else:
        # Unit-test mode: persistence baseline
        if "gen_solar_mw" in df.columns:
            raw_pred = df["gen_solar_mw"].shift(1).fillna(0.0).values
        else:
            raw_pred = np.zeros(len(y_true))

    # Zero-clamp night hours in predictions (solar generation is always 0 at night)
    night_mask = np.isin(hour_arr, NIGHT_HOURS)
    y_pred     = np.where(night_mask, 0.0, np.maximum(raw_pred, 0.0))

    # Masks for sub-population metrics
    day_mask   = ~night_mask                              # daytime rows (hours 6-19)
    cloud_proxy_arr = df["cloud_proxy"].values if "cloud_proxy" in df.columns else np.ones(len(y_true))

    # --- Core metrics ---
    overall_mae  = float(np.mean(np.abs(y_true - y_pred)))
    overall_rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    daytime_mape = _safe_mape(y_true[day_mask], y_pred[day_mask])
    clearsky_mae = compute_clearsky_mae(
        y_true[day_mask], y_pred[day_mask],
        cloud_proxy_arr[day_mask],
    )
    zero_compliance = compute_zero_compliance(y_true, y_pred, hour_arr)
    seasonal_mapes  = compute_seasonal_mape(y_true[day_mask], y_pred[day_mask], month_arr[day_mask])

    passed_dod = bool(daytime_mape < MAPE_TARGET_PCT)

    def _sr(v: float, n: int = 4) -> Optional[float]:
        """Safe round: returns None for NaN."""
        return round(v, n) if not math.isnan(v) else None

    result: Dict[str, Any] = {
        "region":           region,
        "eval_date":        datetime.utcnow().date().isoformat(),
        "overall_mae_mw":   round(overall_mae,  3),
        "overall_rmse_mw":  round(overall_rmse, 3),
        "daytime_mape_pct": round(daytime_mape, 4),
        "clearsky_mae_mw":  _sr(clearsky_mae),
        "zero_compliance_pct": round(zero_compliance, 4) if not math.isnan(zero_compliance) else None,
        "summer_mape_pct":  _sr(seasonal_mapes.get("summer_mape_pct", float("nan"))),
        "autumn_mape_pct":  _sr(seasonal_mapes.get("autumn_mape_pct", float("nan"))),
        "winter_mape_pct":  _sr(seasonal_mapes.get("winter_mape_pct", float("nan"))),
        "spring_mape_pct":  _sr(seasonal_mapes.get("spring_mape_pct", float("nan"))),
        "passed_dod":       passed_dod,
        "n_rows_total":     int(len(y_true)),
        "n_rows_daytime":   int(day_mask.sum()),
        "n_rows_night":     int(night_mask.sum()),
    }
    logger.info(
        "%s | Daytime MAPE: %.2f%% | Overall MAE: %.1f MW | RMSE: %.1f MW | "
        "Zero-compliance: %.1f%% | Clear-sky MAE: %s | DoD: %s",
        region,
        result["daytime_mape_pct"],
        result["overall_mae_mw"],
        result["overall_rmse_mw"],
        result["zero_compliance_pct"] or float("nan"),
        f"{result['clearsky_mae_mw']:.1f} MW" if result["clearsky_mae_mw"] is not None else "N/A",
        "PASS" if passed_dod else "FAIL",
    )
    return result


# ---------------------------------------------------------------------------
# MLflow logging + promotion helpers
# ---------------------------------------------------------------------------

def log_metrics_to_mlflow(results: List[Dict[str, Any]]) -> None:
    """Log per-region solar evaluation metrics to the active MLflow run."""
    if not _MLFLOW_AVAILABLE:
        return
    flat: Dict[str, float] = {}
    for r in results:
        rg = r["region"]
        for key in (
            "overall_mae_mw", "overall_rmse_mw", "daytime_mape_pct", "clearsky_mae_mw",
            "zero_compliance_pct", "summer_mape_pct", "autumn_mape_pct",
            "winter_mape_pct", "spring_mape_pct",
        ):
            val = r.get(key)
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                flat[f"solar_{rg}_{key}"] = float(val)
        flat[f"solar_{rg}_passed_dod"] = float(r["passed_dod"])
    mlflow.log_metrics(flat)


def attempt_promotion(results: List[Dict[str, Any]], dry_run: bool = False) -> bool:
    """
    Set the 'champion' alias if ALL regions pass the daytime MAPE DoD threshold.

    Parameters
    ----------
    results : list of region result dicts
    dry_run : bool
        If True, logs what would happen but does not set aliases.

    Returns
    -------
    bool : True if all regions passed and champion was promoted.
    """
    if not _MLFLOW_AVAILABLE:
        logger.warning("MLflow not available -- skipping promotion")
        return False

    all_passed = all(r["passed_dod"] for r in results)
    if not all_passed:
        failed = [r["region"] for r in results if not r["passed_dod"]]
        logger.info("Promotion skipped -- failing regions: %s", failed)
        return False

    client = MlflowClient()
    logger.info(
        "ALL regions passed daytime MAPE < %.1f%% -- promoting to '%s'",
        MAPE_TARGET_PCT, CHAMPION_ALIAS,
    )
    for r in results:
        region = r["region"]
        mn     = MODEL_NAME_TMPL.format(region=region)
        try:
            mv = client.get_model_version_by_alias(mn, MODEL_ALIAS)
            if not dry_run:
                client.set_registered_model_alias(mn, CHAMPION_ALIAS, mv.version)
                logger.info("  %s v%s -> alias='%s'", mn, mv.version, CHAMPION_ALIAS)
            else:
                logger.info("  DRY RUN: would promote %s v%s -> '%s'", mn, mv.version, CHAMPION_ALIAS)
        except Exception as exc:
            logger.error("  Failed to promote %s: %s", mn, exc)

    return True


# ---------------------------------------------------------------------------
# Console output helpers
# ---------------------------------------------------------------------------

def _fmt_val(val: Optional[float], decimals: int = 2) -> str:
    """Format a metric value for the summary table."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "  N/A"
    return f"{val:.{decimals}f}"


def print_summary_table(results: List[Dict[str, Any]]) -> None:
    """
    Print a rich console evaluation table showing daytime vs nighttime performance.

    Sections:
    1. Per-region core metrics (daytime MAPE, clear-sky MAE, zero-compliance, DoD)
    2. Per-region seasonal MAPE breakdown (summer/autumn/winter/spring)
    3. NEM-wide capacity-weighted summary row
    """
    print()
    print("=" * 100)
    print(f"  SOLAR FORECAST EVALUATION REPORT  --  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 100)

    # ---- Section 1: Core metrics ----
    hdr1 = (
        f"{'Region':>6}  {'Daytime MAPE%':>14}  {'Overall MAE MW':>15}  {'Overall RMSE MW':>16}  "
        f"{'Clear-sky MAE':>14}  {'Zero-comp%':>11}  {'DoD':>5}"
    )
    print(hdr1)
    print("-" * len(hdr1))

    total_w = 0.0
    w_day_mape = w_mae = w_rmse = 0.0

    for r in results:
        cap_w    = INSTALLED_SOLAR_CAPACITY_MW.get(r["region"], 1000.0)
        total_w  += cap_w
        w_day_mape += r["daytime_mape_pct"] * cap_w
        w_mae      += r["overall_mae_mw"]   * cap_w
        w_rmse     += r["overall_rmse_mw"]  * cap_w

        print(
            f"{r['region']:>6}  "
            f"{_fmt_val(r['daytime_mape_pct'], 2):>14}  "
            f"{_fmt_val(r['overall_mae_mw'],   1):>15}  "
            f"{_fmt_val(r['overall_rmse_mw'],  1):>16}  "
            f"{_fmt_val(r.get('clearsky_mae_mw'), 1):>14}  "
            f"{_fmt_val(r.get('zero_compliance_pct'), 1):>11}  "
            f"{'PASS' if r['passed_dod'] else 'FAIL':>5}"
        )

    print("-" * len(hdr1))
    if total_w > 0:
        all_passed = all(r["passed_dod"] for r in results)
        print(
            f"{'NEM-wtd':>6}  "
            f"{_fmt_val(w_day_mape / total_w, 2):>14}  "
            f"{_fmt_val(w_mae / total_w, 1):>15}  "
            f"{_fmt_val(w_rmse / total_w, 1):>16}  "
            f"{'---':>14}  {'---':>11}  "
            f"{'ALL PASS' if all_passed else 'SOME FAIL':>5}"
        )

    # ---- Section 2: Seasonal MAPE ----
    print()
    hdr2 = (
        f"{'Region':>6}  {'Summer MAPE%':>13}  {'Autumn MAPE%':>13}  "
        f"{'Winter MAPE%':>13}  {'Spring MAPE%':>13}"
    )
    print("  Seasonal MAPE (daytime rows)")
    print(hdr2)
    print("-" * len(hdr2))
    for r in results:
        print(
            f"{r['region']:>6}  "
            f"{_fmt_val(r.get('summer_mape_pct'), 2):>13}  "
            f"{_fmt_val(r.get('autumn_mape_pct'), 2):>13}  "
            f"{_fmt_val(r.get('winter_mape_pct'), 2):>13}  "
            f"{_fmt_val(r.get('spring_mape_pct'), 2):>13}"
        )

    print("=" * 100)
    print(f"  Daytime MAPE DoD target: < {MAPE_TARGET_PCT}%   |   Night hours: {sorted(NIGHT_HOURS_SET)}")
    print()


# ---------------------------------------------------------------------------
# Delta table write (Spark path)
# ---------------------------------------------------------------------------

def write_results_to_delta(spark: Any, results: List[Dict[str, Any]]) -> None:
    """Append evaluation results to gold.solar_forecast_evaluation Delta table."""
    rows = []
    for r in results:
        rows.append({
            "region":             r["region"],
            "eval_date":          r["eval_date"],
            "overall_mae_mw":     float(r["overall_mae_mw"]),
            "overall_rmse_mw":    float(r["overall_rmse_mw"]),
            "daytime_mape_pct":   float(r["daytime_mape_pct"]),
            "clearsky_mae_mw":    float(r["clearsky_mae_mw"]) if r.get("clearsky_mae_mw") is not None else None,
            "zero_compliance_pct": float(r["zero_compliance_pct"]) if r.get("zero_compliance_pct") is not None else None,
            "summer_mape_pct":    float(r["summer_mape_pct"]) if r.get("summer_mape_pct") is not None else None,
            "autumn_mape_pct":    float(r["autumn_mape_pct"]) if r.get("autumn_mape_pct") is not None else None,
            "winter_mape_pct":    float(r["winter_mape_pct"]) if r.get("winter_mape_pct") is not None else None,
            "spring_mape_pct":    float(r["spring_mape_pct"]) if r.get("spring_mape_pct") is not None else None,
            "passed_dod":         bool(r["passed_dod"]),
        })

    schema = T.StructType([
        T.StructField("region",              T.StringType(),  False),
        T.StructField("eval_date",           T.StringType(),  False),
        T.StructField("overall_mae_mw",      T.DoubleType(),  True),
        T.StructField("overall_rmse_mw",     T.DoubleType(),  True),
        T.StructField("daytime_mape_pct",    T.DoubleType(),  True),
        T.StructField("clearsky_mae_mw",     T.DoubleType(),  True),
        T.StructField("zero_compliance_pct", T.DoubleType(),  True),
        T.StructField("summer_mape_pct",     T.DoubleType(),  True),
        T.StructField("autumn_mape_pct",     T.DoubleType(),  True),
        T.StructField("winter_mape_pct",     T.DoubleType(),  True),
        T.StructField("spring_mape_pct",     T.DoubleType(),  True),
        T.StructField("passed_dod",          T.BooleanType(), True),
    ])
    (
        spark.createDataFrame(rows, schema=schema)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(EVAL_TABLE)
    )
    logger.info("Solar evaluation results written to %s", EVAL_TABLE)


# ---------------------------------------------------------------------------
# Null context manager for when MLflow is unavailable
# ---------------------------------------------------------------------------

class _null_context:
    """A do-nothing context manager used when MLflow is not installed."""
    def __enter__(self) -> "_null_context":
        return self
    def __exit__(self, *args: Any) -> None:
        pass


# ---------------------------------------------------------------------------
# Main evaluation orchestrator
# ---------------------------------------------------------------------------

def run_evaluation(
    spark: Any = None,
    regions: Optional[List[str]] = None,
    promote: bool = False,
) -> List[Dict[str, Any]]:
    """
    Run solar forecast evaluation for the specified regions.

    When spark is None, operates in unit-test mode: synthetic solar data is
    generated (sin-bell curve over hours 6-18, zero at night) and a persistence
    baseline is used.  No MLflow registry or Spark connectivity is required.

    Parameters
    ----------
    spark : SparkSession or None
        Live Spark session.  None triggers unit-test mode.
    regions : list of str or None
        Regions to evaluate.  Defaults to all 5 NEM regions.
    promote : bool
        If True and all regions pass DoD, set the 'champion' alias.

    Returns
    -------
    List of per-region result dicts.
    """
    regions = regions or NEM_REGIONS
    unit_test_mode = (spark is None)

    if unit_test_mode:
        logger.warning("UNIT TEST MODE -- using synthetic solar data + persistence baseline")

    if _MLFLOW_AVAILABLE:
        mlflow.set_experiment(MLFLOW_EXPERIMENT)

    results: List[Dict[str, Any]] = []

    ctx = mlflow.start_run(run_name="solar_forecast_evaluation") if _MLFLOW_AVAILABLE else _null_context()
    with ctx:
        if _MLFLOW_AVAILABLE:
            mlflow.set_tag("unit_test_mode", str(unit_test_mode))
            mlflow.set_tag("regions",        str(regions))
            mlflow.set_tag("mape_target",    str(MAPE_TARGET_PCT))
            mlflow.set_tag("night_hours",    str(sorted(NIGHT_HOURS_SET)))

        for region in regions:
            try:
                if unit_test_mode:
                    df    = _make_synthetic_solar_data(region)
                    model = None
                else:
                    mn    = MODEL_NAME_TMPL.format(region=region)
                    model = mlflow.pyfunc.load_model(f"models:/{mn}@{MODEL_ALIAS}")
                    df    = load_test_data_spark(spark, region)

                result = evaluate_region(region, df, model)
                results.append(result)

            except Exception as exc:
                logger.error("Evaluation failed for %s: %s", region, exc, exc_info=True)

        if results:
            if _MLFLOW_AVAILABLE:
                log_metrics_to_mlflow(results)

            print_summary_table(results)

            if not unit_test_mode and spark is not None:
                write_results_to_delta(spark, results)

            if promote:
                promoted = attempt_promotion(results, dry_run=unit_test_mode)
                if _MLFLOW_AVAILABLE:
                    mlflow.set_tag("champion_promoted", str(promoted))

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate solar generation forecast models for one or all NEM regions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m models.solar_forecast.evaluate --region NSW1\n"
            "  python -m models.solar_forecast.evaluate --all-regions\n"
            "  python -m models.solar_forecast.evaluate --all-regions --promote\n"
            "  python -m models.solar_forecast.evaluate --all-regions --unit-test\n"
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--region",      choices=NEM_REGIONS, help="Evaluate a single NEM region.")
    group.add_argument("--all-regions", action="store_true", help="Evaluate all 5 NEM regions.")
    parser.add_argument(
        "--promote",
        action="store_true",
        default=False,
        help="Set 'champion' alias if ALL regions pass daytime MAPE DoD threshold.",
    )
    parser.add_argument(
        "--unit-test",
        action="store_true",
        default=False,
        help="Force unit-test mode (no Spark / MLflow registry).",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()

    if args.unit_test:
        spark_session = None
    else:
        try:
            spark_session = SparkSession.builder.getOrCreate()
        except Exception:
            logger.warning("Could not obtain SparkSession -- falling back to unit-test mode")
            spark_session = None

    selected_regions = NEM_REGIONS if args.all_regions else [args.region]

    run_evaluation(
        spark=spark_session,
        regions=selected_regions,
        promote=args.promote,
    )
