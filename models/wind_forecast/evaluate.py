"""
Wind Generation Forecast -- Evaluation
=========================================
Evaluates the production wind forecast model per NEM region against the
held-out test set.  Reconstructs the exact same feature set used in train.py
(12 lags, 4h rolling stats, ramp rate, capacity factor, high/low wind flags,
and cyclic calendar encodings) so that evaluation is always consistent with
training.

Metrics computed per region:
  - MAE (MW), RMSE (MW), MAPE (%)
  - Capacity-weighted bias (mean error / installed capacity)
  - Ramp accuracy: % of intervals where predicted ramp direction matches
    actual within a 50 MW deadband
  - High-wind MAPE (wind_speed > 10 m/s subset)
  - Low-wind MAPE  (wind_speed < 3 m/s subset)
  MAPE target: < 8.0%

Output:
  - gold.wind_forecast_evaluation Delta table (per-region rows + passed_dod flag)
  - MLflow metrics logged as wind_{region}_{metric}
  - Promotion: if ALL regions pass MAPE < 8% -> alias "champion" is set

Model registry path: models:/energy_copilot.ml.wind_forecast_{region}@production

Unit test mode (spark=None):
  Generates synthetic sine+noise wind data for all 5 regions and validates
  the entire evaluate pipeline runs without errors.

CLI usage:
  python -m models.wind_forecast.evaluate --region NSW1
  python -m models.wind_forecast.evaluate --all-regions
  python -m models.wind_forecast.evaluate --all-regions --promote
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
EVAL_TABLE    = f"{GOLD}.wind_forecast_evaluation"

NEM_REGIONS: List[str] = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

MODEL_NAME_TMPL    = "energy_copilot.ml.wind_forecast_{region}"
MODEL_ALIAS        = "production"
CHAMPION_ALIAS     = "champion"
MLFLOW_EXPERIMENT  = "energy_copilot_wind_evaluation"

LABEL_COL          = "wind_generation_target"
FORECAST_HORIZONS: List[int] = [1, 4, 8, 12, 24, 48]

# Installed wind capacity (MW) per NEM region.
# Source: approximate fleet sizes based on AEMO Generation Information 2024.
INSTALLED_WIND_CAPACITY_MW: Dict[str, float] = {
    "NSW1": 3500.0,
    "QLD1": 2500.0,
    "VIC1": 2500.0,
    "SA1":  1800.0,
    "TAS1":  700.0,
}

# Wind speed thresholds (m/s) -- must match train.py exactly
WIND_HIGH_THRESHOLD = 10.0   # strong-wind regime (turbines at rated power)
WIND_LOW_THRESHOLD  =  3.0   # cut-in speed (turbines not generating)

# Ramp deadband for ramp accuracy metric (MW)
RAMP_DEADBAND_MW = 50.0

# MAPE target for Definition-of-Done
MAPE_TARGET_PCT = 8.0

EXCLUDED_COLS = {
    "settlementdate", "settlement_date", "regionid", "rrp", "totaldemand",
    "rrp_target", LABEL_COL, "feature_timestamp", "season",
}


# ---------------------------------------------------------------------------
# Wind-specific feature engineering (must mirror train.py exactly)
# ---------------------------------------------------------------------------

def add_wind_features(df: pd.DataFrame, region: str) -> pd.DataFrame:
    """
    Reconstruct the exact same wind-specific feature set used during training.

    Requires columns: wind_speed_100m_ms (or alias), gen_wind_mw (or alias).
    Falls back gracefully with zeros when columns are absent.

    Features added
    --------------
    wind_lag_1 ... wind_lag_12 : wind speed lags (5-min to 60-min)
    wind_roll_mean_4h          : 4-hour rolling mean of wind speed
    wind_roll_std_4h           : 4-hour rolling std dev of wind speed
    wind_ramp_rate             : speed difference vs 2 intervals ago
    wind_capacity_factor       : gen_wind_mw / installed capacity (clipped 0-1.05)
    is_high_wind               : 1 if speed > 10 m/s, else 0
    is_low_wind                : 1 if speed < 3 m/s, else 0
    hour_sin, hour_cos         : cyclic hour-of-day encoding
    dow_sin, dow_cos           : cyclic day-of-week encoding
    month_sin, month_cos       : cyclic month-of-year encoding
    """
    df = df.copy()

    # Resolve wind speed column
    wind_speed_col: Optional[str] = None
    for candidate in ("wind_speed_100m_ms", "windspeed_100m", "wind_speed_ms"):
        if candidate in df.columns:
            wind_speed_col = candidate
            break

    # Resolve wind generation column
    wind_gen_col: Optional[str] = None
    for candidate in ("gen_wind_mw", "wind_total_mw", "wind_generation_mw"):
        if candidate in df.columns:
            wind_gen_col = candidate
            break

    # 1. Lags of wind speed (intervals 1-12, i.e. 5-60 min)
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
        logger.warning("Wind speed column not found; wind feature columns set to 0")
        for lag in range(1, 13):
            df[f"wind_lag_{lag}"] = 0.0
        df["wind_roll_mean_4h"] = 0.0
        df["wind_roll_std_4h"]  = 0.0
        df["wind_ramp_rate"]    = 0.0
        df["is_high_wind"]      = 0
        df["is_low_wind"]       = 0

    # 4. Wind capacity factor (normalised by installed fleet capacity)
    installed_mw = INSTALLED_WIND_CAPACITY_MW.get(region, 1000.0)
    if wind_gen_col is not None:
        df["wind_capacity_factor"] = (df[wind_gen_col] / installed_mw).clip(lower=0.0, upper=1.05)
    else:
        df["wind_capacity_factor"] = 0.0

    # 6. Cyclic calendar encodings -- ensure source columns are available
    if "hour_of_day" not in df.columns and "settlementdate" in df.columns:
        df["hour_of_day"] = pd.to_datetime(df["settlementdate"]).dt.hour
    if "day_of_week" not in df.columns and "settlementdate" in df.columns:
        df["day_of_week"] = pd.to_datetime(df["settlementdate"]).dt.dayofweek
    if "month" not in df.columns and "settlementdate" in df.columns:
        df["month"] = pd.to_datetime(df["settlementdate"]).dt.month

    if "hour_of_day" in df.columns:
        h = df["hour_of_day"]
        df["hour_sin"] = np.sin(2 * math.pi * h / 24.0)
        df["hour_cos"] = np.cos(2 * math.pi * h / 24.0)

    if "day_of_week" in df.columns:
        d = df["day_of_week"]
        df["dow_sin"] = np.sin(2 * math.pi * d / 7.0)
        df["dow_cos"] = np.cos(2 * math.pi * d / 7.0)

    if "month" in df.columns:
        m = df["month"]
        df["month_sin"] = np.sin(2 * math.pi * m / 12.0)
        df["month_cos"] = np.cos(2 * math.pi * m / 12.0)

    return df


# ---------------------------------------------------------------------------
# Metric computation helpers
# ---------------------------------------------------------------------------

def _safe_mape(y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1.0) -> float:
    """MAPE with epsilon guard for near-zero values."""
    abs_err = np.abs(y_true - y_pred)
    return float(np.mean(abs_err / (np.abs(y_true) + eps)) * 100.0)


def compute_ramp_accuracy(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    deadband_mw: float = RAMP_DEADBAND_MW,
) -> float:
    """
    Compute the percentage of intervals where predicted ramp direction matches actual.

    A ramp is classified as:
      +1 (up)   if the change exceeds  +deadband_mw
      -1 (down) if the change is below -deadband_mw
       0 (flat) if the change is within the deadband

    Parameters
    ----------
    y_true, y_pred : np.ndarray
        Actual and predicted wind generation time series (MW).
    deadband_mw : float
        Flat-zone deadband in MW (default 50 MW).

    Returns
    -------
    float : ramp accuracy percentage (0-100).
    """
    actual_diff = np.diff(y_true, prepend=y_true[0])
    pred_diff   = np.diff(y_pred, prepend=y_pred[0])

    def _sign(arr: np.ndarray) -> np.ndarray:
        return np.where(arr > deadband_mw, 1, np.where(arr < -deadband_mw, -1, 0))

    return float(np.mean(_sign(actual_diff) == _sign(pred_diff)) * 100.0)


def compute_core_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    region: str,
    df: pd.DataFrame,
    wind_speed_col: Optional[str],
) -> Dict[str, Any]:
    """
    Compute the full set of wind evaluation metrics for a single region.

    Parameters
    ----------
    y_true : np.ndarray
        Actual wind generation (MW).
    y_pred : np.ndarray
        Predicted wind generation (MW), already clamped >= 0.
    region : str
        NEM region identifier.
    df : pd.DataFrame
        Feature DataFrame aligned with y_true / y_pred.
    wind_speed_col : str or None
        Name of the wind speed column in df, or None.

    Returns
    -------
    Dict with keys: mae_mw, rmse_mw, mape_pct, bias_pct_of_capacity,
    ramp_accuracy_pct, high_wind_mape_pct, low_wind_mape_pct.
    """
    eps       = 1.0
    abs_err   = np.abs(y_true - y_pred)
    installed = INSTALLED_WIND_CAPACITY_MW.get(region, 1000.0)

    mae  = float(np.mean(abs_err))
    rmse = float(np.sqrt(np.mean(abs_err ** 2)))
    mape = _safe_mape(y_true, y_pred, eps)
    bias = float(np.mean(y_pred - y_true))
    bias_pct    = bias / installed * 100.0
    ramp_acc    = compute_ramp_accuracy(y_true, y_pred)

    # --- High / low wind MAPE ---
    high_wind_mape: float = float("nan")
    low_wind_mape:  float = float("nan")

    if wind_speed_col is not None and wind_speed_col in df.columns:
        ws        = df[wind_speed_col].values
        high_mask = ws > WIND_HIGH_THRESHOLD
        low_mask  = ws < WIND_LOW_THRESHOLD

        if high_mask.any():
            high_wind_mape = _safe_mape(y_true[high_mask], y_pred[high_mask], eps)
        if low_mask.any():
            low_wind_mape = _safe_mape(y_true[low_mask], y_pred[low_mask], eps)

    return {
        "mae_mw":               mae,
        "rmse_mw":              rmse,
        "mape_pct":             mape,
        "bias_pct_of_capacity": bias_pct,
        "ramp_accuracy_pct":    ramp_acc,
        "high_wind_mape_pct":   high_wind_mape,
        "low_wind_mape_pct":    low_wind_mape,
    }


# ---------------------------------------------------------------------------
# Synthetic data for unit-test mode (spark=None)
# ---------------------------------------------------------------------------

def _make_synthetic_wind_data(region: str, n_intervals: int = 17520) -> pd.DataFrame:
    """
    Generate synthetic wind generation data for unit-test / CI mode.

    Produces a realistic-looking wind generation time series using a sine-wave
    base (diurnal + seasonal) plus Gaussian noise, clamped to [0, installed_mw].

    Parameters
    ----------
    region : str
        NEM region identifier -- used to scale installed capacity.
    n_intervals : int
        Number of 5-minute intervals (~61 days default for fast CI runs).

    Returns
    -------
    pd.DataFrame with columns matching feature store schema.
    """
    rng       = np.random.default_rng(seed=42)
    t         = np.arange(n_intervals)
    installed = INSTALLED_WIND_CAPACITY_MW.get(region, 2000.0)

    # Diurnal wind cycle (peaks at night in Australia) + seasonal cycle
    diurnal  = np.sin(2 * math.pi * t / 288)    # 288 intervals/day
    seasonal = np.sin(2 * math.pi * t / 52560)  # ~6-month cycle
    noise    = rng.normal(0, 0.12, n_intervals)

    wind_speed = 8.0 + 4.0 * diurnal + 2.0 * seasonal + noise * 2.0
    wind_speed = np.clip(wind_speed, 0.0, 25.0)

    # Power curve approximation: P ~ v^3 (simplified, capped at rated power)
    gen_wind = installed * np.clip((wind_speed / 12.5) ** 3, 0.0, 1.0)
    gen_wind += rng.normal(0, installed * 0.05, n_intervals)
    gen_wind  = np.clip(gen_wind, 0.0, installed)

    start_dt = datetime(2022, 1, 1)
    dates    = [start_dt + timedelta(minutes=5 * i) for i in range(n_intervals)]

    df = pd.DataFrame({
        "settlementdate":     dates,
        "regionid":           region,
        "wind_speed_100m_ms": wind_speed,
        "gen_wind_mw":        gen_wind,
        "totaldemand":        8000.0 + rng.normal(0, 500, n_intervals),
        "rrp":                50.0   + rng.normal(0, 30,  n_intervals),
        "hour_of_day":        [d.hour       for d in dates],
        "day_of_week":        [d.weekday()   for d in dates],
        "month":              [d.month       for d in dates],
        "forecast_horizon":   1,
    })

    # Target = next interval's gen_wind_mw (1-step-ahead proxy)
    df[LABEL_COL] = df["gen_wind_mw"].shift(-1).ffill()
    return df


# ---------------------------------------------------------------------------
# Data loading (Spark path)
# ---------------------------------------------------------------------------

def load_test_data_spark(spark: Any, region: str) -> pd.DataFrame:
    """
    Load the held-out test set for a region from the Gold feature store.

    Uses val_end and test_end tags stored on the training MLflow run to
    reproduce the exact same temporal split used during training.

    Parameters
    ----------
    spark : SparkSession
    region : str

    Returns
    -------
    pd.DataFrame ready for add_wind_features().
    """
    client  = MlflowClient()
    mn      = MODEL_NAME_TMPL.format(region=region)
    mv      = client.get_model_version_by_alias(mn, MODEL_ALIAS)
    run     = client.get_run(mv.run_id)
    val_end  = datetime.fromisoformat(run.data.tags["val_end"])
    test_end = datetime.fromisoformat(run.data.tags["test_end"])

    base = (
        spark.table(FEATURE_TABLE)
        .filter(F.col("regionid") == region)
        .filter(F.col("settlementdate") >= F.lit(val_end.isoformat()))
        .filter(F.col("settlementdate") <  F.lit(test_end.isoformat()))
        .dropDuplicates(["settlementdate", "regionid"])
    )

    # Add target column by joining future gen_wind_mw values across all horizons
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
    Evaluate a wind forecast model for one NEM region.

    Parameters
    ----------
    region : str
        NEM region identifier.
    df : pd.DataFrame
        Raw feature DataFrame (feature engineering applied here).
    model : mlflow.pyfunc.PyFuncModel or None
        Loaded production model.  When None (unit-test mode), a naive
        persistence-forecast baseline is used instead.

    Returns
    -------
    Dict with per-region metrics, passed_dod flag, and metadata.
    """
    logger.info("--- Evaluating wind forecast: %s ---", region)

    # Sort chronologically before computing lag/rolling features
    df = df.sort_values("settlementdate").reset_index(drop=True)
    df = add_wind_features(df, region)

    # Resolve wind speed column for per-regime MAPE computations
    wind_speed_col: Optional[str] = None
    for candidate in ("wind_speed_100m_ms", "windspeed_100m", "wind_speed_ms"):
        if candidate in df.columns:
            wind_speed_col = candidate
            break

    # Drop rows with NaN targets (from lag computation at start of series)
    df = df.dropna(subset=[LABEL_COL]).reset_index(drop=True)
    y_true = df[LABEL_COL].values.astype(float)

    if model is not None:
        # Determine feature columns from the registered model's input schema
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
        # Unit-test mode: persistence baseline (last observed value)
        if "gen_wind_mw" in df.columns:
            raw_pred = df["gen_wind_mw"].shift(1).fillna(0.0).values
        else:
            raw_pred = np.zeros(len(y_true))

    # Clamp: wind generation cannot be negative
    y_pred = np.maximum(raw_pred, 0.0)

    metrics    = compute_core_metrics(y_true, y_pred, region, df, wind_speed_col)
    passed_dod = bool(metrics["mape_pct"] < MAPE_TARGET_PCT)

    def _safe_round(v: float, n: int = 4) -> Optional[float]:
        return round(v, n) if not math.isnan(v) else None

    result: Dict[str, Any] = {
        "region":               region,
        "eval_date":            datetime.utcnow().date().isoformat(),
        "mae_mw":               round(metrics["mae_mw"], 3),
        "rmse_mw":              round(metrics["rmse_mw"], 3),
        "mape_pct":             round(metrics["mape_pct"], 4),
        "bias_pct_of_capacity": round(metrics["bias_pct_of_capacity"], 4),
        "ramp_accuracy_pct":    round(metrics["ramp_accuracy_pct"], 4),
        "high_wind_mape_pct":   _safe_round(metrics["high_wind_mape_pct"]),
        "low_wind_mape_pct":    _safe_round(metrics["low_wind_mape_pct"]),
        "passed_dod":           passed_dod,
        "n_rows":               int(len(y_true)),
    }
    logger.info(
        "%s | MAE: %.1f MW | RMSE: %.1f MW | MAPE: %.2f%% | "
        "Bias: %.2f%% cap | Ramp acc: %.1f%% | DoD: %s",
        region,
        result["mae_mw"],
        result["rmse_mw"],
        result["mape_pct"],
        result["bias_pct_of_capacity"],
        result["ramp_accuracy_pct"],
        "PASS" if passed_dod else "FAIL",
    )
    return result


# ---------------------------------------------------------------------------
# MLflow logging + promotion helpers
# ---------------------------------------------------------------------------

def log_metrics_to_mlflow(results: List[Dict[str, Any]]) -> None:
    """Log per-region wind evaluation metrics to the active MLflow run."""
    if not _MLFLOW_AVAILABLE:
        return
    flat: Dict[str, float] = {}
    for r in results:
        rg = r["region"]
        for key in (
            "mae_mw", "rmse_mw", "mape_pct", "bias_pct_of_capacity",
            "ramp_accuracy_pct", "high_wind_mape_pct", "low_wind_mape_pct",
        ):
            val = r.get(key)
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                flat[f"wind_{rg}_{key}"] = float(val)
        flat[f"wind_{rg}_passed_dod"] = float(r["passed_dod"])
    mlflow.log_metrics(flat)


def attempt_promotion(results: List[Dict[str, Any]], dry_run: bool = False) -> bool:
    """
    Set the 'champion' alias if ALL regions pass the MAPE DoD threshold.

    Parameters
    ----------
    results : list of region result dicts
    dry_run : bool
        If True, log what would happen but do not set aliases.

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
    logger.info("ALL regions passed MAPE < %.1f%% -- promoting to '%s'", MAPE_TARGET_PCT, CHAMPION_ALIAS)

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
    """Format a float for the summary table, or 'N/A' if None."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "  N/A"
    return f"{val:.{decimals}f}"


def print_summary_table(results: List[Dict[str, Any]]) -> None:
    """Print a rich per-region evaluation table + NEM-wide weighted-average summary row."""
    col_w = 14
    header_fields = [
        ("Region",      6),
        ("MAE (MW)",    col_w),
        ("RMSE (MW)",   col_w),
        ("MAPE %",      col_w),
        ("Bias% Cap",   col_w),
        ("Ramp Acc%",   col_w),
        ("Hi-Wind MAPE", col_w + 2),
        ("Lo-Wind MAPE", col_w + 2),
        ("DoD",         6),
    ]
    header = "  ".join(f"{label:>{width}}" for label, width in header_fields)
    sep    = "-" * len(header)

    print()
    print("=" * len(header))
    print("  WIND FORECAST EVALUATION REPORT  --  " + datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
    print("=" * len(header))
    print(header)
    print(sep)

    total_w = 0.0
    w_mae = w_rmse = w_mape = w_bias = w_ramp = 0.0

    for r in results:
        cap_w   = INSTALLED_WIND_CAPACITY_MW.get(r["region"], 1000.0)
        total_w += cap_w
        w_mae   += r["mae_mw"]  * cap_w
        w_rmse  += r["rmse_mw"] * cap_w
        w_mape  += r["mape_pct"] * cap_w
        w_bias  += r["bias_pct_of_capacity"] * cap_w
        w_ramp  += r["ramp_accuracy_pct"] * cap_w

        dod_str = "PASS" if r["passed_dod"] else "FAIL"
        vals = [
            f"{r['region']:>6}",
            f"{_fmt_val(r['mae_mw'], 1):>{col_w}}",
            f"{_fmt_val(r['rmse_mw'], 1):>{col_w}}",
            f"{_fmt_val(r['mape_pct'], 2):>{col_w}}",
            f"{_fmt_val(r['bias_pct_of_capacity'], 2):>{col_w}}",
            f"{_fmt_val(r['ramp_accuracy_pct'], 1):>{col_w}}",
            f"{_fmt_val(r.get('high_wind_mape_pct'), 2):>{col_w+2}}",
            f"{_fmt_val(r.get('low_wind_mape_pct'), 2):>{col_w+2}}",
            f"{dod_str:>6}",
        ]
        print("  ".join(vals))

    print(sep)

    # NEM-wide capacity-weighted average row
    if total_w > 0:
        all_passed = all(r["passed_dod"] for r in results)
        nem_vals = [
            f"{'NEM-wtd':>6}",
            f"{_fmt_val(w_mae / total_w, 1):>{col_w}}",
            f"{_fmt_val(w_rmse / total_w, 1):>{col_w}}",
            f"{_fmt_val(w_mape / total_w, 2):>{col_w}}",
            f"{_fmt_val(w_bias / total_w, 2):>{col_w}}",
            f"{_fmt_val(w_ramp / total_w, 1):>{col_w}}",
            f"{'---':>{col_w+2}}",
            f"{'---':>{col_w+2}}",
            f"{'ALL PASS' if all_passed else 'SOME FAIL':>6}",
        ]
        print("  ".join(nem_vals))

    print("=" * len(header))
    print(f"  MAPE DoD target: < {MAPE_TARGET_PCT}%")
    print()


# ---------------------------------------------------------------------------
# Delta table write (Spark path)
# ---------------------------------------------------------------------------

def write_results_to_delta(spark: Any, results: List[Dict[str, Any]]) -> None:
    """Append evaluation results to gold.wind_forecast_evaluation Delta table."""
    rows = []
    for r in results:
        rows.append({
            "region":               r["region"],
            "eval_date":            r["eval_date"],
            "mae_mw":               float(r["mae_mw"]),
            "rmse_mw":              float(r["rmse_mw"]),
            "mape_pct":             float(r["mape_pct"]),
            "bias_pct_of_capacity": float(r["bias_pct_of_capacity"]),
            "ramp_accuracy_pct":    float(r["ramp_accuracy_pct"]),
            "high_wind_mape_pct":   float(r["high_wind_mape_pct"]) if r.get("high_wind_mape_pct") is not None else None,
            "low_wind_mape_pct":    float(r["low_wind_mape_pct"])  if r.get("low_wind_mape_pct")  is not None else None,
            "passed_dod":           bool(r["passed_dod"]),
        })

    schema = T.StructType([
        T.StructField("region",               T.StringType(),  False),
        T.StructField("eval_date",            T.StringType(),  False),
        T.StructField("mae_mw",               T.DoubleType(),  True),
        T.StructField("rmse_mw",              T.DoubleType(),  True),
        T.StructField("mape_pct",             T.DoubleType(),  True),
        T.StructField("bias_pct_of_capacity", T.DoubleType(),  True),
        T.StructField("ramp_accuracy_pct",    T.DoubleType(),  True),
        T.StructField("high_wind_mape_pct",   T.DoubleType(),  True),
        T.StructField("low_wind_mape_pct",    T.DoubleType(),  True),
        T.StructField("passed_dod",           T.BooleanType(), True),
    ])
    (
        spark.createDataFrame(rows, schema=schema)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(EVAL_TABLE)
    )
    logger.info("Wind evaluation results written to %s", EVAL_TABLE)


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
    Run wind forecast evaluation for the specified regions.

    When spark is None, operates in unit-test mode: synthetic wind data is
    generated for each region and a persistence-forecast baseline is used in
    place of the registered MLflow model.  This mode requires no external
    connectivity and is safe to run in CI/CD.

    Parameters
    ----------
    spark : SparkSession or None
        Live Spark session.  None triggers unit-test mode.
    regions : list of str or None
        Regions to evaluate.  Defaults to all 5 NEM regions.
    promote : bool
        If True and all regions pass DoD, set the 'champion' alias on every
        production model version.

    Returns
    -------
    List of per-region result dicts.
    """
    regions = regions or NEM_REGIONS
    unit_test_mode = (spark is None)

    if unit_test_mode:
        logger.warning("UNIT TEST MODE -- using synthetic wind data + persistence baseline")

    if _MLFLOW_AVAILABLE:
        mlflow.set_experiment(MLFLOW_EXPERIMENT)

    results: List[Dict[str, Any]] = []

    ctx = mlflow.start_run(run_name="wind_forecast_evaluation") if _MLFLOW_AVAILABLE else _null_context()
    with ctx:
        if _MLFLOW_AVAILABLE:
            mlflow.set_tag("unit_test_mode", str(unit_test_mode))
            mlflow.set_tag("regions",        str(regions))
            mlflow.set_tag("mape_target",    str(MAPE_TARGET_PCT))

        for region in regions:
            try:
                if unit_test_mode:
                    df    = _make_synthetic_wind_data(region)
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
        description="Evaluate wind generation forecast models for one or all NEM regions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m models.wind_forecast.evaluate --region NSW1\n"
            "  python -m models.wind_forecast.evaluate --all-regions\n"
            "  python -m models.wind_forecast.evaluate --all-regions --promote\n"
            "  python -m models.wind_forecast.evaluate --all-regions --unit-test\n"
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--region",      choices=NEM_REGIONS, help="Evaluate a single NEM region.")
    group.add_argument("--all-regions", action="store_true", help="Evaluate all 5 NEM regions.")
    parser.add_argument(
        "--promote",
        action="store_true",
        default=False,
        help="Set 'champion' alias if ALL regions pass MAPE DoD threshold.",
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
