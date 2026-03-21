"""
Anomaly Detection -- Training (Complete Implementation)
=======================================================
Two-stage anomaly detection for NEM electricity market events, trained on
21 months of gold.nem_prices_5min data (or gold.feature_store_price as
fallback when the dedicated prices table is not yet available).

Stage 1 -- Unsupervised (sklearn Pipeline):
  StandardScaler -> IsolationForest(contamination=0.01, n_estimators=100)
  Produces anomaly_score (decision_function) and is_anomaly_if (0/1).

Stage 2 -- Rule-based classifier:
  spike      : spot_price_aud_mwh >= 5000 $/MWh
  negative   : spot_price_aud_mwh <= -100 $/MWh
  separation : max regional price - min regional price >= 500 $/MWh per interval
  normal     : everything else

Combined output:
  is_anomaly = (isolation_forest == -1) OR (rule_based == 1)
  event_type : "spike" | "negative" | "separation" | "normal"

Features used:
  - spot_price_aud_mwh
  - total_demand_mw
  - hour_of_day  (0-23)
  - day_of_week  (0-6, Monday=0 via pandas)
  - region_id    (label-encoded integer)
  - price_roll_mean_12     : 12-interval (60-min) rolling mean of price
  - price_roll_std_12      : 12-interval rolling std of price
  - demand_roll_mean_12    : 12-interval rolling mean of demand
  - demand_roll_std_12     : 12-interval rolling std of demand

MLflow logging:
  - Params   : contamination, n_estimators, n_features, training_months, thresholds
  - Metrics  : anomaly_rate_pct, spike_events, negative_events, separation_events,
               permutation_importance_<feature> (permutation-based for IsolationForest)
  - Artifacts: sklearn Pipeline artifact, anomaly_thresholds.json, feature_list.json
  - Tags     : model_type, phase=final_training

Registration:
  energy_copilot.ml.anomaly_detection_nem@production

Unit-test mode:
  When PySpark is unavailable (no Spark session), the module can still be
  imported and functions called with a pandas DataFrame instead of loading
  from a Spark table.  Pass spark=None to train_anomaly_detector() to trigger
  unit-test mode, which reads from NEM_PRICES_FALLBACK_CSV if set, or
  generates synthetic data for smoke-testing.

Platform: Databricks Runtime 15.4 LTS+
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient
from sklearn.ensemble import IsolationForest
from sklearn.inspection import permutation_importance
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, StandardScaler

# PySpark imports are optional — wrap for graceful degradation in unit test mode
try:
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession
    _SPARK_AVAILABLE = True
except ImportError:
    _SPARK_AVAILABLE = False
    SparkSession = None  # type: ignore[assignment, misc]

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG         = "energy_copilot"
GOLD            = f"{CATALOG}.gold"

# Primary price table; fallback to feature_store_price when absent
NEM_PRICES_TABLE   = f"{GOLD}.nem_prices_5min"
FEATURE_STORE_TABLE = f"{GOLD}.feature_store_price"

MLFLOW_EXPERIMENT = "/energy_copilot/anomaly_detection_training"
MODEL_NAME        = f"{CATALOG}.ml.anomaly_detection_nem"

# Rule-based thresholds (AUD/MWh)
SPIKE_THRESHOLD      =  5_000.0
NEGATIVE_THRESHOLD   =   -100.0
SEPARATION_THRESHOLD =    500.0   # max(region price) - min(region price) per interval

# IsolationForest hyperparameters
IF_CONTAMINATION  = 0.01    # ~1% of intervals expected to be anomalous (NEM base rate)
IF_N_ESTIMATORS   = 100
IF_MAX_SAMPLES    = "auto"
IF_RANDOM_STATE   = 42

# Training window (most recent 21 months)
TRAINING_MONTHS = 21

# Rolling window for price/demand statistics (12 intervals = 60 min)
ROLLING_WINDOW  = 12

# Feature columns fed to IsolationForest (after engineering)
FEATURE_COLS: List[str] = [
    "spot_price_aud_mwh",
    "total_demand_mw",
    "hour_of_day",
    "day_of_week",
    "region_id_encoded",
    "price_roll_mean_12",
    "price_roll_std_12",
    "demand_roll_mean_12",
    "demand_roll_std_12",
]

# Column name maps for the two possible source tables
_PRICES_TABLE_COLS = {
    "settlementdate_col": "interval_datetime",
    "price_col":          "spot_price_aud_mwh",
    "demand_col":         "total_demand_mw",
    "region_col":         "region_id",
}
_FEATURE_STORE_COLS = {
    "settlementdate_col": "settlementdate",
    "price_col":          "rrp",
    "demand_col":         "totaldemand",
    "region_col":         "regionid",
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_training_data_spark(spark: Any) -> pd.DataFrame:  # spark: SparkSession
    """Load 21 months of price/demand data from Spark.

    Tries gold.nem_prices_5min first; falls back to gold.feature_store_price
    (deduplicated to one row per interval x region).

    Returns a pandas DataFrame with standardised columns:
      interval_datetime, region_id, spot_price_aud_mwh, total_demand_mw
    """
    cutoff = datetime.utcnow() - timedelta(days=TRAINING_MONTHS * 30)

    def _try_table(table: str, col_map: Dict[str, str]) -> Optional[pd.DataFrame]:
        try:
            df_spark = (
                spark.table(table)  # noqa: F821
                .select(
                    F.col(col_map["settlementdate_col"]).alias("interval_datetime"),
                    F.col(col_map["region_col"]).alias("region_id"),
                    F.col(col_map["price_col"]).alias("spot_price_aud_mwh"),
                    F.col(col_map["demand_col"]).alias("total_demand_mw"),
                )
                .filter(F.col("interval_datetime") >= F.lit(cutoff.isoformat()))
                .dropDuplicates(["interval_datetime", "region_id"])
                .orderBy("region_id", "interval_datetime")
            )
            return df_spark.toPandas()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not load from %s: %s", table, exc)
            return None

    df = _try_table(NEM_PRICES_TABLE, _PRICES_TABLE_COLS)
    if df is None or df.empty:
        logger.info("Falling back to feature_store_price ...")
        df = _try_table(FEATURE_STORE_TABLE, _FEATURE_STORE_COLS)

    if df is None or df.empty:
        raise RuntimeError(
            f"No data loaded from either {NEM_PRICES_TABLE} or {FEATURE_STORE_TABLE}. "
            "Ensure at least one table is populated with recent data."
        )

    logger.info("Loaded %d rows from Spark (cutoff: %s)", len(df), cutoff.date())
    return df


def _make_synthetic_data(n_rows: int = 50_000) -> pd.DataFrame:
    """Generate synthetic NEM-like data for unit-test / smoke-test mode.

    Produces a realistic price distribution: mostly $50-150/MWh with ~1%
    spike events above $300/MWh and ~0.5% negative events below -$50/MWh.
    """
    rng = np.random.default_rng(42)
    regions = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]
    n_per_region = n_rows // len(regions)

    dfs = []
    base_dt = datetime(2024, 1, 1)
    for region in regions:
        n      = n_per_region
        prices = rng.lognormal(mean=4.0, sigma=0.5, size=n)   # ~$55/MWh median
        # Inject spikes (~1%)
        spike_idx = rng.choice(n, size=max(1, int(n * 0.01)), replace=False)
        prices[spike_idx] = rng.uniform(5000, 14000, size=len(spike_idx))
        # Inject negatives (~0.5%)
        neg_idx = rng.choice(n, size=max(1, int(n * 0.005)), replace=False)
        prices[neg_idx] = rng.uniform(-1000, -100, size=len(neg_idx))

        demand = rng.normal(7000, 1500, size=n).clip(1000, 14000)
        timestamps = [base_dt + timedelta(minutes=5 * i) for i in range(n)]

        dfs.append(pd.DataFrame({
            "interval_datetime": timestamps,
            "region_id":         region,
            "spot_price_aud_mwh": prices,
            "total_demand_mw":    demand,
        }))

    return pd.concat(dfs, ignore_index=True)


# ---------------------------------------------------------------------------
# Feature engineering (pandas)
# ---------------------------------------------------------------------------

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add model features to the raw price/demand DataFrame.

    Input columns expected:
      interval_datetime (datetime-like), region_id (str),
      spot_price_aud_mwh (float), total_demand_mw (float)

    Output adds:
      hour_of_day, day_of_week, region_id_encoded,
      price_roll_mean_12, price_roll_std_12,
      demand_roll_mean_12, demand_roll_std_12
    """
    df = df.copy()
    df["interval_datetime"] = pd.to_datetime(df["interval_datetime"])
    df = df.sort_values(["region_id", "interval_datetime"]).reset_index(drop=True)

    # Time features
    df["hour_of_day"] = df["interval_datetime"].dt.hour
    df["day_of_week"] = df["interval_datetime"].dt.dayofweek   # Monday=0

    # Label-encode region_id
    le = LabelEncoder()
    df["region_id_encoded"] = le.fit_transform(df["region_id"])

    # Rolling features per region (12 intervals = 60 min; min_periods=1 to avoid NaN)
    for region, grp in df.groupby("region_id", sort=False):
        idx = grp.index
        df.loc[idx, "price_roll_mean_12"]  = (
            grp["spot_price_aud_mwh"].rolling(ROLLING_WINDOW, min_periods=1).mean()
        )
        df.loc[idx, "price_roll_std_12"]   = (
            grp["spot_price_aud_mwh"].rolling(ROLLING_WINDOW, min_periods=1).std().fillna(0.0)
        )
        df.loc[idx, "demand_roll_mean_12"] = (
            grp["total_demand_mw"].rolling(ROLLING_WINDOW, min_periods=1).mean()
        )
        df.loc[idx, "demand_roll_std_12"]  = (
            grp["total_demand_mw"].rolling(ROLLING_WINDOW, min_periods=1).std().fillna(0.0)
        )

    return df


# ---------------------------------------------------------------------------
# Rule-based event classifier
# ---------------------------------------------------------------------------

def classify_events_rule(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """Apply deterministic rules to classify each row.

    Separation is detected at the interval level: if the spread between the
    maximum and minimum regional price within the same dispatch interval
    exceeds SEPARATION_THRESHOLD, all rows in that interval are labelled
    "separation".

    Priority order (first match wins):
      1. spike      -- spot_price_aud_mwh >= SPIKE_THRESHOLD
      2. negative   -- spot_price_aud_mwh <= NEGATIVE_THRESHOLD
      3. separation -- interval price spread >= SEPARATION_THRESHOLD
      4. normal

    Returns
    -------
    event_type  : pd.Series[str]  -- label per row
    is_rule_based : pd.Series[int] -- 1 if any rule triggered, else 0
    """
    cond_spike    = df["spot_price_aud_mwh"] >= SPIKE_THRESHOLD
    cond_negative = df["spot_price_aud_mwh"] <= NEGATIVE_THRESHOLD

    # Separation: compute per-interval price spread and broadcast back
    interval_spread = (
        df.groupby("interval_datetime")["spot_price_aud_mwh"]
        .transform(lambda x: x.max() - x.min())
    )
    cond_separation = interval_spread >= SEPARATION_THRESHOLD

    event_type = pd.Series("normal", index=df.index)
    event_type = event_type.where(~cond_separation, "separation")
    event_type = event_type.where(~cond_negative,   "negative")
    event_type = event_type.where(~cond_spike,       "spike")

    is_rule_based = (
        cond_spike | cond_negative | cond_separation
    ).astype(int)

    return event_type, is_rule_based


# ---------------------------------------------------------------------------
# Permutation importance for IsolationForest
# ---------------------------------------------------------------------------

def compute_permutation_importance(
    pipeline: Pipeline,
    X:        np.ndarray,
    y_anomaly: np.ndarray,
    feature_names: List[str],
    n_repeats: int = 5,
) -> Dict[str, float]:
    """Compute permutation importance for the IsolationForest using the
    decision_function as the scoring signal.

    IsolationForest does not expose a native feature importance; permutation
    importance measures how much the anomaly score distribution shifts when
    each feature is randomly shuffled.

    Returns
    -------
    Dict mapping feature name -> mean importance score (higher = more important).
    """
    # Use the isolation forest's score_samples as the scoring function
    # (higher = more normal; we negate so higher importance = more informative)
    def _scorer(estimator: Pipeline, X_in: np.ndarray, y_in: np.ndarray) -> float:
        scores = estimator.decision_function(X_in)
        # Maximise: a feature is important if shuffling it degrades score variance
        return float(np.std(scores))

    try:
        result = permutation_importance(
            pipeline,
            X,
            y_anomaly,
            scoring=_scorer,
            n_repeats=n_repeats,
            random_state=IF_RANDOM_STATE,
            n_jobs=-1,
        )
        return {
            name: float(result.importances_mean[i])
            for i, name in enumerate(feature_names)
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Permutation importance computation failed: %s", exc)
        return {name: float("nan") for name in feature_names}


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train_anomaly_detector(
    spark: Optional[Any] = None,  # SparkSession or None for unit-test mode
) -> None:
    """Train and register the NEM anomaly detection model.

    When spark is None the function enters unit-test mode and uses synthetic
    data rather than querying Databricks tables.

    Steps
    -----
    1. Load data (Spark or synthetic)
    2. Engineer features
    3. Fit StandardScaler + IsolationForest Pipeline
    4. Apply rule-based classifier
    5. Combine: is_anomaly = IF anomaly OR rule-based anomaly
    6. Compute permutation importance
    7. Log everything to MLflow and register model with alias "production"
    """
    unit_test_mode = spark is None or not _SPARK_AVAILABLE

    if unit_test_mode:
        logger.info("Unit-test mode: loading synthetic NEM data ...")
        df_raw = _make_synthetic_data()
    else:
        logger.info("Loading 21 months of NEM prices from Spark ...")
        df_raw = load_training_data_spark(spark)

    logger.info("Loaded %d rows | %d unique regions | date range: %s to %s",
                len(df_raw),
                df_raw["region_id"].nunique(),
                df_raw["interval_datetime"].min(),
                df_raw["interval_datetime"].max())

    logger.info("Engineering features ...")
    df = engineer_features(df_raw)

    # Drop rows with NaN in feature columns (warm-up window artefacts)
    df_clean = df.dropna(subset=FEATURE_COLS).reset_index(drop=True)
    logger.info("Rows after NaN drop: %d (dropped %d)", len(df_clean), len(df) - len(df_clean))

    X = df_clean[FEATURE_COLS].values.astype(np.float32)

    # ------------------------------------------------------------------
    # Stage 1: IsolationForest wrapped in Pipeline(StandardScaler + IF)
    # ------------------------------------------------------------------
    pipeline = Pipeline([
        ("scaler",    StandardScaler()),
        ("isoforest", IsolationForest(
            contamination=IF_CONTAMINATION,
            n_estimators=IF_N_ESTIMATORS,
            max_samples=IF_MAX_SAMPLES,
            random_state=IF_RANDOM_STATE,
            n_jobs=-1,
        )),
    ])

    logger.info("Fitting IsolationForest (contamination=%.3f, n_estimators=%d) ...",
                IF_CONTAMINATION, IF_N_ESTIMATORS)
    pipeline.fit(X)

    df_clean["anomaly_score"] = pipeline.decision_function(X)
    df_clean["is_anomaly_if"] = (pipeline.predict(X) == -1).astype(int)

    # ------------------------------------------------------------------
    # Stage 2: Rule-based classification
    # ------------------------------------------------------------------
    event_type, is_rule_based = classify_events_rule(df_clean)
    df_clean["event_type"]    = event_type
    df_clean["is_rule_based"] = is_rule_based

    # Combined anomaly flag: IsolationForest OR rule-based
    df_clean["is_anomaly"] = (
        (df_clean["is_anomaly_if"] == 1) | (df_clean["is_rule_based"] == 1)
    ).astype(int)

    # ------------------------------------------------------------------
    # Summary statistics
    # ------------------------------------------------------------------
    anomaly_rate  = df_clean["is_anomaly"].mean()
    if_rate       = df_clean["is_anomaly_if"].mean()
    spike_count   = (df_clean["event_type"] == "spike").sum()
    neg_count     = (df_clean["event_type"] == "negative").sum()
    sep_count     = (df_clean["event_type"] == "separation").sum()

    logger.info(
        "Combined anomaly rate: %.3f%% | IF rate: %.3f%% | "
        "spikes: %d | negatives: %d | separations: %d",
        anomaly_rate * 100, if_rate * 100, spike_count, neg_count, sep_count,
    )

    # ------------------------------------------------------------------
    # Permutation-based feature importance
    # ------------------------------------------------------------------
    logger.info("Computing permutation importance (5 repeats) ...")
    perm_imp = compute_permutation_importance(
        pipeline=pipeline,
        X=X,
        y_anomaly=df_clean["is_anomaly"].values,
        feature_names=FEATURE_COLS,
        n_repeats=5,
    )
    logger.info("Permutation importance: %s", perm_imp)

    # ------------------------------------------------------------------
    # MLflow logging and model registration
    # ------------------------------------------------------------------
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name="anomaly_detector_training") as run:
        mlflow.set_tag("model_type", "IsolationForest + rule_classifier")
        mlflow.set_tag("phase",      "final_training")
        mlflow.set_tag("unit_test_mode", str(unit_test_mode))

        mlflow.log_params({
            "if_contamination":      IF_CONTAMINATION,
            "if_n_estimators":       IF_N_ESTIMATORS,
            "if_max_samples":        str(IF_MAX_SAMPLES),
            "rolling_window":        ROLLING_WINDOW,
            "spike_threshold":       SPIKE_THRESHOLD,
            "negative_threshold":    NEGATIVE_THRESHOLD,
            "separation_threshold":  SEPARATION_THRESHOLD,
            "training_months":       TRAINING_MONTHS,
            "n_features":            len(FEATURE_COLS),
            "n_train_rows":          int(len(df_clean)),
        })

        # Log feature importance as individual metrics for easy chart rendering
        mlflow.log_metrics({
            "train_rows":          int(len(df_clean)),
            "anomaly_rate_pct":    float(anomaly_rate * 100),
            "if_anomaly_rate_pct": float(if_rate * 100),
            "spike_events":        int(spike_count),
            "negative_events":     int(neg_count),
            "separation_events":   int(sep_count),
            **{f"perm_imp_{k}": float(v) for k, v in perm_imp.items()},
        })

        # Model artifact with signature
        sample_X = pd.DataFrame(X[:5], columns=FEATURE_COLS)
        signature = mlflow.models.infer_signature(
            sample_X,
            pipeline.decision_function(X[:5]),
        )
        mlflow.sklearn.log_model(
            pipeline,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
            signature=signature,
            input_example=pd.DataFrame(X[:3], columns=FEATURE_COLS),
        )

        # Config artifacts for auditing / rule transparency
        thresholds = {
            "spike_threshold":      SPIKE_THRESHOLD,
            "negative_threshold":   NEGATIVE_THRESHOLD,
            "separation_threshold": SEPARATION_THRESHOLD,
        }
        thresholds_path = "/tmp/anomaly_thresholds.json"
        with open(thresholds_path, "w") as fh:
            json.dump(thresholds, fh, indent=2)
        mlflow.log_artifact(thresholds_path, artifact_path="config")

        feature_list_path = "/tmp/anomaly_feature_list.json"
        with open(feature_list_path, "w") as fh:
            json.dump({"feature_cols": FEATURE_COLS, "perm_importance": perm_imp}, fh, indent=2)
        mlflow.log_artifact(feature_list_path, artifact_path="config")

        # ------------------------------------------------------------------
        # Register model to Unity Catalog with alias "production"
        # ------------------------------------------------------------------
        client = MlflowClient()

        # Ensure the registered model exists (handle 404 gracefully)
        try:
            client.get_registered_model(MODEL_NAME)
        except MlflowException as exc:
            if "RESOURCE_DOES_NOT_EXIST" in str(exc) or exc.error_code == "RESOURCE_DOES_NOT_EXIST":
                logger.info("Creating registered model: %s", MODEL_NAME)
                client.create_registered_model(MODEL_NAME)
            else:
                raise

        latest_versions = client.get_latest_versions(MODEL_NAME)
        if latest_versions:
            latest = str(max(int(v.version) for v in latest_versions))
            client.set_registered_model_alias(MODEL_NAME, "production", latest)
            logger.info(
                "Registered %s v%s with alias 'production'",
                MODEL_NAME, latest,
            )
        else:
            logger.warning(
                "No registered versions found for %s after log_model. "
                "Alias 'production' was not set.",
                MODEL_NAME,
            )

    logger.info("Anomaly detection training complete. MLflow run: %s", run.info.run_id)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if _SPARK_AVAILABLE:
        _spark = SparkSession.builder.getOrCreate()  # noqa: F821
    else:
        logger.warning("PySpark not available — running in unit-test mode with synthetic data.")
        _spark = None

    train_anomaly_detector(spark=_spark)
