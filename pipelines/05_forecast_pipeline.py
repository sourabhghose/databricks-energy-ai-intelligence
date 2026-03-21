# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 05 — ML Forecast Inference Pipeline
# MAGIC
# MAGIC **Schedule:** Every 5 minutes via Databricks Job (cron: `0 */5 * * * ?`)
# MAGIC **Cluster:** DBR 15.4.x-ml-scala2.12 (ML runtime required for scikit-learn / LightGBM)
# MAGIC
# MAGIC Loads 20 regional models (5 regions × price/demand/wind/solar) from MLflow UC registry
# MAGIC @production alias, runs batch inference over the latest 48 intervals from
# MAGIC gold.feature_store_price, and writes forecasts to gold.nem_forecasts_realtime via
# MAGIC MERGE upsert. Logs metrics to experiment energy_copilot_forecast_pipeline.
# MAGIC
# MAGIC **Graceful degradation:** If Gold tables are unavailable the pipeline falls back to
# MAGIC the last known forecast plus Gaussian noise (stale_mode=True tag).

import logging
import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG = "energy_copilot"
NEM_REGIONS: List[str] = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
FORECAST_TYPES: List[str] = ["price", "demand", "wind", "solar"]
MODEL_ALIAS = "production"

# Horizon definitions: (horizon_intervals, horizon_minutes)
# 1 interval = 5 minutes
FORECAST_HORIZONS: List[Tuple[int, int]] = [
    (1, 5),
    (2, 10),
    (4, 20),
    (6, 30),
    (12, 60),
    (24, 120),
]

NIGHT_HOURS: List[int] = list(range(0, 6)) + list(range(20, 24))

# Physical bounds for clamping
SOLAR_MIN = 0.0
DEMAND_MIN = 0.0
DEMAND_MAX = 50000.0   # MW — NEM physical upper bound
PRICE_MIN = -1000.0    # AUD/MWh — market floor (APC floor)
PRICE_MAX = 17000.0    # AUD/MWh — MPC headroom

INFERENCE_LOOKBACK_INTERVALS = 48  # rows from feature_store_price

MLFLOW_EXPERIMENT = "energy_copilot_forecast_pipeline"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("forecast_pipeline")


# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------

def _model_name(forecast_type: str, region: str) -> str:
    return f"{CATALOG}.ml.{forecast_type}_forecast_{region}"


def _load_single_model(forecast_type: str, region: str) -> Optional[Any]:
    """Load a single model from MLflow UC registry @production alias.

    Returns the pyfunc model on success, None on any failure (missing model,
    network issue, etc.).  Logs model version and last_updated when available.
    """
    name = _model_name(forecast_type, region)
    uri = f"models:/{name}@{MODEL_ALIAS}"
    try:
        model = mlflow.pyfunc.load_model(uri)
        # Attempt to read version metadata from the MLflow client
        try:
            client = mlflow.tracking.MlflowClient()
            mv = client.get_model_version_by_alias(name, MODEL_ALIAS)
            logger.info(
                f"Loaded {forecast_type}/{region} — version={mv.version} "
                f"last_updated={mv.last_updated_timestamp}"
            )
        except Exception:
            logger.info(f"Loaded {forecast_type}/{region} (version metadata unavailable)")
        return model
    except Exception as exc:
        logger.warning(f"Model not available: {uri} — {exc}")
        return None


def load_all_models() -> Dict[str, Dict[str, Optional[Any]]]:
    """Load all 20 regional models.  Missing models are recorded as None."""
    models: Dict[str, Dict[str, Optional[Any]]] = {ft: {} for ft in FORECAST_TYPES}
    for ft in FORECAST_TYPES:
        for region in NEM_REGIONS:
            models[ft][region] = _load_single_model(ft, region)
    return models


def count_missing_models(models: Dict[str, Dict[str, Optional[Any]]]) -> int:
    return sum(1 for ft in FORECAST_TYPES for region in NEM_REGIONS if models[ft][region] is None)


# ---------------------------------------------------------------------------
# Feature loading
# ---------------------------------------------------------------------------

def load_inference_features(spark: Any, region: str) -> Optional[pd.DataFrame]:
    """Load the latest 48 intervals from gold.feature_store_price for a region.

    Returns a pandas DataFrame ready for model.predict(), or None if unavailable.
    """
    try:
        table = f"{CATALOG}.gold.feature_store_price"
        df = (
            spark.table(table)
            .filter(f"region_id = '{region}'")
            .orderBy("interval_datetime", ascending=False)
            .limit(INFERENCE_LOOKBACK_INTERVALS)
            .toPandas()
        )
        if df.empty:
            logger.warning(f"No feature rows for region={region}")
            return None
        # Sort ascending for rolling/lag coherence
        df = df.sort_values("interval_datetime").reset_index(drop=True)
        return df
    except Exception as exc:
        logger.error(f"Feature load failed for region={region}: {exc}")
        return None


def make_synthetic_features(region: str, n_rows: int = 48) -> pd.DataFrame:
    """Synthetic feature data for unit-test mode (spark=None)."""
    rng = np.random.default_rng(seed=42)
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(n_rows):
        rows.append({
            "region_id": region,
            "interval_datetime": now,
            "forecast_horizon": 1,
            "hour_of_day": (now.hour + i) % 24,
            "day_of_week": now.weekday(),
            "month": now.month,
            "is_weekend": int(now.weekday() >= 5),
            "season": (now.month % 12) // 3,
            "price_lag_1": float(rng.uniform(50, 150)),
            "price_lag_12": float(rng.uniform(50, 150)),
            "price_lag_288": float(rng.uniform(50, 150)),
            "price_mean_4hr": float(rng.uniform(50, 150)),
            "demand_lag_12": float(rng.uniform(4000, 10000)),
            "demand_mean_4hr": float(rng.uniform(4000, 10000)),
            "temperature_c": float(rng.uniform(10, 40)),
            "wind_speed_100m": float(rng.uniform(0, 20)),
            "solar_radiation": float(rng.uniform(0, 800)),
            "gen_wind_mw": float(rng.uniform(0, 2000)),
            "gen_solar_mw": float(rng.uniform(0, 1000)),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Prediction helpers
# ---------------------------------------------------------------------------

def _sigmoid(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def spike_probability(predicted_rrp: float) -> float:
    """sigmoid(predicted_rrp / 300 - 1) clamped to [0, 1]."""
    raw = _sigmoid(predicted_rrp / 300.0 - 1.0)
    return float(np.clip(raw, 0.0, 1.0))


def confidence_score(horizon_intervals: int) -> float:
    """1 / (1 + horizon_intervals / 12) — decays from 1.0 toward 0 with horizon."""
    return float(1.0 / (1.0 + horizon_intervals / 12.0))


def clamp_prediction(value: float, forecast_type: str, hour: Optional[int] = None) -> float:
    """Apply physical clamping rules per forecast type."""
    if forecast_type == "solar":
        if hour is not None and hour in NIGHT_HOURS:
            return 0.0
        return float(np.clip(value, SOLAR_MIN, None))
    if forecast_type == "demand":
        return float(np.clip(value, DEMAND_MIN, DEMAND_MAX))
    if forecast_type == "price":
        return float(np.clip(value, PRICE_MIN, PRICE_MAX))
    # wind — clamp to non-negative
    return float(np.clip(value, 0.0, None))


def run_model_predict(
    model: Any,
    features_df: pd.DataFrame,
    forecast_type: str,
) -> List[float]:
    """Run model.predict and return a list of floats.  Never raises."""
    if model is None or features_df is None or features_df.empty:
        return []
    try:
        # Drop non-feature columns that models don't expect
        drop_cols = [c for c in ["region_id", "interval_datetime"] if c in features_df.columns]
        input_df = features_df.drop(columns=drop_cols, errors="ignore")
        raw = model.predict(input_df)
        if not hasattr(raw, "__len__"):
            raw = [raw]
        return [float(v) for v in raw]
    except Exception as exc:
        logger.error(f"model.predict failed ({forecast_type}): {exc}")
        return []


# ---------------------------------------------------------------------------
# Stale-mode fallback
# ---------------------------------------------------------------------------

def load_stale_forecast(spark: Any, region: str) -> Optional[pd.DataFrame]:
    """Load last known forecast from gold.nem_forecasts_realtime and add Gaussian noise."""
    try:
        df = (
            spark.table(f"{CATALOG}.gold.nem_forecasts_realtime")
            .filter(f"region = '{region}'")
            .orderBy("generated_at", ascending=False)
            .limit(len(FORECAST_HORIZONS) * len(FORECAST_TYPES))
            .toPandas()
        )
        if df.empty:
            return None
        rng = np.random.default_rng()
        df = df.copy()
        noise_scale = df["predicted_value"].abs() * 0.05  # 5% Gaussian noise
        df["predicted_value"] = df["predicted_value"] + rng.normal(0, noise_scale)
        df["generated_at"] = datetime.now(timezone.utc)
        logger.warning(f"Stale-mode active for region={region} — using last known forecast + noise")
        return df
    except Exception as exc:
        logger.error(f"Stale forecast load failed for region={region}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Forecast record assembly
# ---------------------------------------------------------------------------

def build_forecast_records(
    region: str,
    forecast_type: str,
    model_version: str,
    features_df: pd.DataFrame,
    predictions: List[float],
    generated_at: datetime,
) -> List[Dict[str, Any]]:
    """Build a list of forecast dicts — one per horizon."""
    records = []
    for idx, (horizon_intervals, horizon_minutes) in enumerate(FORECAST_HORIZONS):
        if idx >= len(predictions):
            break
        raw_value = predictions[idx]
        # Determine target hour for solar night-clamping
        target_ts = generated_at
        target_hour = target_ts.hour
        clamped_value = clamp_prediction(raw_value, forecast_type, hour=target_hour)

        # Uncertainty bands: ±10% widening linearly with horizon
        band_pct = 0.10 + (horizon_intervals / 48) * 0.10
        lower = clamp_prediction(clamped_value * (1 - band_pct), forecast_type, target_hour)
        upper = clamp_prediction(clamped_value * (1 + band_pct), forecast_type, target_hour)

        # Spike probability only for price models
        sp = spike_probability(clamped_value) if forecast_type == "price" else 0.0
        cs = confidence_score(horizon_intervals)

        records.append({
            "region": region,
            "interval_datetime": generated_at,
            "generated_at": generated_at,
            "model_type": forecast_type,
            "model_version": model_version,
            "horizon_intervals": horizon_intervals,
            "horizon_minutes": horizon_minutes,
            "predicted_value": clamped_value,
            "lower_bound": lower,
            "upper_bound": upper,
            "spike_probability": float(np.clip(sp, 0.0, 1.0)),
            "confidence_score": cs,
        })
    return records


# ---------------------------------------------------------------------------
# Delta MERGE writer
# ---------------------------------------------------------------------------

def write_forecasts_merge(spark: Any, records: List[Dict[str, Any]]) -> int:
    """Write forecast records to gold.nem_forecasts_realtime via MERGE upsert.

    Merge key: (region, forecast_horizon, generated_at)
    Returns number of records written.
    """
    if not records:
        return 0
    target_table = f"{CATALOG}.gold.nem_forecasts_realtime"
    source_df = spark.createDataFrame(pd.DataFrame(records))
    source_df.createOrReplaceTempView("_forecast_source")

    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING _forecast_source AS source
        ON  target.region           = source.region
        AND target.horizon_intervals = source.horizon_intervals
        AND target.generated_at     = source.generated_at
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    try:
        spark.sql(merge_sql)
        logger.info(f"MERGE complete: {len(records)} records into {target_table}")
        return len(records)
    except Exception as exc:
        logger.error(f"MERGE failed: {exc}")
        # Fall back to append
        try:
            source_df.write.format("delta").mode("append").saveAsTable(target_table)
            logger.warning("Fell back to append write for nem_forecasts_realtime")
            return len(records)
        except Exception as exc2:
            logger.error(f"Append fallback also failed: {exc2}")
            return 0


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_forecast_pipeline(spark: Optional[Any] = None) -> Dict[str, Any]:
    """Run the full forecast inference pipeline.

    Parameters
    ----------
    spark:
        Active SparkSession.  Pass None to run in unit-test mode with synthetic
        data and mock models — no Spark or MLflow calls are made.

    Returns
    -------
    dict with pipeline run metadata.
    """
    pipeline_start = time.time()
    generated_at = datetime.now(timezone.utc)
    unit_test_mode = spark is None

    logger.info(
        f"Forecast pipeline start: generated_at={generated_at.isoformat()} "
        f"unit_test_mode={unit_test_mode}"
    )

    # ------------------------------------------------------------------
    # Resolve Spark reference (notebook global vs. passed parameter)
    # ------------------------------------------------------------------
    if not unit_test_mode:
        try:
            _spark = spark or globals().get("spark")  # type: ignore[assignment]
            if _spark is None:
                raise NameError("spark not in globals")
        except NameError:
            logger.error("No Spark session — aborting pipeline")
            return {"error": "no_spark"}
    else:
        _spark = None

    # ------------------------------------------------------------------
    # Model loading (skip in unit-test mode — use mock predict)
    # ------------------------------------------------------------------
    if not unit_test_mode:
        models = load_all_models()
        models_missing = count_missing_models(models)
    else:
        # Mock models — each returns a simple constant array
        class _MockModel:
            def predict(self, df: pd.DataFrame) -> List[float]:
                return [100.0 + i * 5.0 for i in range(len(FORECAST_HORIZONS))]

        models = {ft: {r: _MockModel() for r in NEM_REGIONS} for ft in FORECAST_TYPES}
        models_missing = 0
        logger.info("Unit-test mode: using mock models")

    # ------------------------------------------------------------------
    # MLflow run
    # ------------------------------------------------------------------
    mlflow_run_id: Optional[str] = None
    stale_mode = False
    all_records: List[Dict[str, Any]] = []
    total_regions_processed = 0

    try:
        if not unit_test_mode:
            mlflow.set_experiment(MLFLOW_EXPERIMENT)
            mlflow_ctx = mlflow.start_run(run_name=f"forecast_{generated_at.strftime('%Y%m%dT%H%M')}")
        else:
            mlflow_ctx = None
    except Exception as exc:
        logger.warning(f"MLflow experiment setup failed (non-fatal): {exc}")
        mlflow_ctx = None

    try:
        for region in NEM_REGIONS:
            # ----------------------------------------------------------
            # Feature loading
            # ----------------------------------------------------------
            if unit_test_mode:
                features_df = make_synthetic_features(region)
                region_stale = False
            else:
                features_df = load_inference_features(_spark, region)
                region_stale = False
                if features_df is None:
                    logger.warning(f"Gold tables unavailable for {region} — trying stale fallback")
                    stale_records = load_stale_forecast(_spark, region)
                    if stale_records is not None:
                        all_records.extend(stale_records.to_dict("records"))
                        region_stale = True
                        stale_mode = True
                        total_regions_processed += 1
                    continue

            if not region_stale:
                # ------------------------------------------------------
                # Inference per forecast type
                # ------------------------------------------------------
                for ft in FORECAST_TYPES:
                    model = models[ft].get(region)
                    if model is None:
                        logger.warning(f"Skipping {ft}/{region} — model not loaded")
                        continue

                    preds = run_model_predict(model, features_df, ft)
                    if not preds:
                        continue

                    # Determine model_version string
                    mv_str = MODEL_ALIAS
                    try:
                        if not unit_test_mode:
                            client = mlflow.tracking.MlflowClient()
                            mv = client.get_model_version_by_alias(
                                _model_name(ft, region), MODEL_ALIAS
                            )
                            mv_str = str(mv.version)
                    except Exception:
                        pass

                    recs = build_forecast_records(
                        region=region,
                        forecast_type=ft,
                        model_version=mv_str,
                        features_df=features_df,
                        predictions=preds,
                        generated_at=generated_at,
                    )
                    all_records.extend(recs)

                total_regions_processed += 1

        # ----------------------------------------------------------------
        # Write output
        # ----------------------------------------------------------------
        total_written = 0
        if not unit_test_mode and all_records:
            total_written = write_forecasts_merge(_spark, all_records)
        elif unit_test_mode:
            total_written = len(all_records)
            logger.info(f"Unit-test mode: would write {total_written} forecast records")

        pipeline_duration = time.time() - pipeline_start

        # ----------------------------------------------------------------
        # MLflow metrics logging
        # ----------------------------------------------------------------
        if mlflow_ctx is not None:
            try:
                mlflow.log_metrics({
                    "total_regions_processed": float(total_regions_processed),
                    "total_forecasts_written": float(total_written),
                    "models_missing": float(models_missing),
                    "pipeline_duration_seconds": round(pipeline_duration, 2),
                })
                if stale_mode:
                    mlflow.set_tag("stale_mode", "True")
                mlflow.set_tag("generated_at", generated_at.isoformat())
            except Exception as exc:
                logger.warning(f"MLflow metric logging failed (non-fatal): {exc}")

        logger.info(
            f"Pipeline complete: regions_processed={total_regions_processed} "
            f"forecasts_written={total_written} "
            f"models_missing={models_missing} "
            f"stale_mode={stale_mode} "
            f"duration_s={pipeline_duration:.1f}"
        )

        return {
            "total_regions_processed": total_regions_processed,
            "total_forecasts_written": total_written,
            "models_missing": models_missing,
            "stale_mode": stale_mode,
            "pipeline_duration_seconds": pipeline_duration,
            "generated_at": generated_at.isoformat(),
        }

    finally:
        if mlflow_ctx is not None:
            try:
                mlflow.end_run()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Unit-test entry point
# ---------------------------------------------------------------------------

def run_unit_tests() -> None:
    """Verify the pipeline runs end-to-end with synthetic data and mock models."""
    logger.info("Running unit tests for forecast pipeline...")

    result = run_forecast_pipeline(spark=None)

    assert result.get("total_regions_processed") == len(NEM_REGIONS), (
        f"Expected {len(NEM_REGIONS)} regions processed, got {result.get('total_regions_processed')}"
    )
    expected_records = len(NEM_REGIONS) * len(FORECAST_TYPES) * len(FORECAST_HORIZONS)
    assert result.get("total_forecasts_written") == expected_records, (
        f"Expected {expected_records} records, got {result.get('total_forecasts_written')}"
    )

    # Verify clamping logic
    assert clamp_prediction(-500.0, "solar", hour=3) == 0.0, "Solar night clamp failed"
    assert clamp_prediction(-500.0, "price") == PRICE_MIN, "Price floor clamp failed"
    assert clamp_prediction(60000.0, "demand") == DEMAND_MAX, "Demand ceiling clamp failed"

    # Verify spike probability
    sp = spike_probability(300.0)
    assert 0.0 <= sp <= 1.0, f"spike_probability out of [0,1]: {sp}"
    assert spike_probability(0.0) < spike_probability(600.0), "spike_probability not monotone"

    # Verify confidence score
    cs_near = confidence_score(1)
    cs_far = confidence_score(24)
    assert cs_near > cs_far, "confidence_score should decrease with horizon"

    logger.info("All unit tests passed.")


# ---------------------------------------------------------------------------
# Notebook execution entry point
# ---------------------------------------------------------------------------

try:
    # When running as a Databricks notebook, `spark` is injected by the runtime.
    run_forecast_pipeline(spark=spark)  # type: ignore[name-defined]  # noqa: F821
except NameError:
    # No Spark session — run unit tests to validate logic
    logger.warning("No Spark session found — running in unit-test mode")
    run_unit_tests()
