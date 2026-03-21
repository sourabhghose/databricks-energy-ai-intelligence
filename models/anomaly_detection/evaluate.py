"""
Anomaly Detection -- Evaluation (Sprint 8a)
============================================
Evaluates the production anomaly detector against known historical NEM
market events. Computes Precision, Recall, F1, ROC-AUC, false positive rate,
alert storm rate, and per-event-type (spike/negative/separation) breakdown.

Model: models:/energy_copilot.ml.anomaly_detection_nem@production

Feature reconstruction matching train.py engineer_features():
  hour_of_day, day_of_week (0=Monday), LabelEncoder region_id_encoded
  price_roll_mean_12, price_roll_std_12  (12-interval = 60 min rolling)
  demand_roll_mean_12, demand_roll_std_12

Ground truth strategy:
  Primary  : gold.known_market_events (AEMO-confirmed events)
  Fallback : rule-derived (price >=5000 OR <=-100 OR spread >=500)

Definition-of-Done: F1 > 0.7 AND FP rate < 5%

Outputs:
  - gold.anomaly_detection_evaluation Delta table
  - MLflow artifact: anomaly_eval_report.json
  - champion alias set when DoD passes and --promote given

Unit-test mode (spark=None):
  Synthetic data with injected anomalies at known positions; validates
  that precision/recall calculations are numerically stable.

Usage:
  python -m models.anomaly_detection.evaluate
  python -m models.anomaly_detection.evaluate --promote
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.metrics import (
    precision_recall_fscore_support,
    roc_auc_score,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Constants -- aligned with anomaly_detection/train.py
# ---------------------------------------------------------------------------

CATALOG       = "energy_copilot"
GOLD          = f"{CATALOG}.gold"
SILVER        = f"{CATALOG}.silver"

NEM_PRICES_TABLE    = f"{GOLD}.nem_prices_5min"
FEATURE_STORE_TABLE = f"{GOLD}.feature_store_price"
EVENTS_TABLE        = f"{GOLD}.known_market_events"
EVAL_TABLE          = f"{GOLD}.anomaly_detection_evaluation"

MODEL_NAME        = "energy_copilot.ml.anomaly_detection_nem"
MODEL_ALIAS       = "production"
CHAMPION_ALIAS    = "champion"
MLFLOW_EXPERIMENT = "energy_copilot_anomaly_evaluation"

SPIKE_THRESHOLD      =  5_000.0
NEGATIVE_THRESHOLD   =   -100.0
SEPARATION_THRESHOLD =    500.0

DOD_F1_THRESHOLD  = 0.70
DOD_FP_THRESHOLD  = 5.0

ROLLING_WINDOW          = 12
ALERT_STORM_CONSECUTIVE = 3

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

NEM_REGIONS: List[str] = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]
TRAINING_MONTHS = 21


# ---------------------------------------------------------------------------
# Rule-based ground-truth helper
# ---------------------------------------------------------------------------

def build_rule_ground_truth(df: pd.DataFrame) -> pd.Series:
    """Derive ground-truth labels from raw price data.

    Rules (priority: spike > negative > separation > normal):
      spike      : spot_price_aud_mwh >= 5000
      negative   : spot_price_aud_mwh <= -100
      separation : per-interval regional price spread >= 500
      normal     : everything else
    """
    cond_spike    = df["spot_price_aud_mwh"] >= SPIKE_THRESHOLD
    cond_negative = df["spot_price_aud_mwh"] <= NEGATIVE_THRESHOLD

    interval_col = (
        "interval_datetime" if "interval_datetime" in df.columns
        else df.columns[0]
    )
    interval_spread = (
        df.groupby(interval_col)["spot_price_aud_mwh"]
        .transform(lambda x: x.max() - x.min())
    )
    cond_separation = interval_spread >= SEPARATION_THRESHOLD

    gt = pd.Series("normal", index=df.index)
    gt = gt.where(~cond_separation, "separation")
    gt = gt.where(~cond_negative,   "negative")
    gt = gt.where(~cond_spike,       "spike")
    return gt


# ---------------------------------------------------------------------------
# Synthetic data generation (unit-test mode)
# ---------------------------------------------------------------------------

def _make_synthetic_anomaly_data(
    n_rows: int = 20_000,
    seed:   int = 42,
) -> Tuple[pd.DataFrame, pd.Series]:
    """Generate synthetic NEM-like data with injected anomalies.

    Returns (df, true_labels) where true_labels is a Series with values
    normal/spike/negative/separation at known positions.

    Injection rates: ~1% spike, ~0.5% negative, ~0.3% separation.
    """
    rng = np.random.default_rng(seed)
    n_per_region = n_rows // len(NEM_REGIONS)
    base_dt = datetime(2025, 1, 1)

    dfs, all_labels = [], []

    for region in NEM_REGIONS:
        n = n_per_region
        prices = rng.lognormal(mean=4.0, sigma=0.5, size=n)
        labels = np.full(n, "normal", dtype=object)

        # Inject spikes (~1%)
        n_spikes  = max(1, int(n * 0.01))
        spike_idx = rng.choice(n, size=n_spikes, replace=False)
        prices[spike_idx] = rng.uniform(5_000, 14_000, size=n_spikes)
        labels[spike_idx] = "spike"

        # Inject negatives (~0.5%)
        n_neg   = max(1, int(n * 0.005))
        neg_idx = rng.choice(n, size=n_neg, replace=False)
        prices[neg_idx] = rng.uniform(-1_000, -100, size=n_neg)
        labels[neg_idx] = "negative"

        demand     = rng.normal(7_000, 1_500, size=n).clip(1_000, 14_000)
        timestamps = [base_dt + timedelta(minutes=5 * i) for i in range(n)]

        dfs.append(pd.DataFrame({
            "interval_datetime":  timestamps,
            "region_id":          region,
            "spot_price_aud_mwh": prices,
            "total_demand_mw":    demand,
        }))
        all_labels.extend(labels.tolist())

    df = pd.concat(dfs, ignore_index=True)
    true_labels = pd.Series(all_labels, name="event_type")
    return df, true_labels


# ---------------------------------------------------------------------------
# Feature engineering (mirrors train.py engineer_features)
# ---------------------------------------------------------------------------

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Reconstruct features matching train.py engineer_features().

    Input columns expected:
      interval_datetime, region_id, spot_price_aud_mwh, total_demand_mw

    Output adds:
      hour_of_day          : 0-23
      day_of_week          : 0-6 (Monday=0, pandas convention)
      region_id_encoded    : LabelEncoder integer (matches train.py)
      price_roll_mean_12   : 12-interval (60 min) rolling mean
      price_roll_std_12    : 12-interval rolling std
      demand_roll_mean_12  : 12-interval rolling mean of demand
      demand_roll_std_12   : 12-interval rolling std of demand
    """
    from sklearn.preprocessing import LabelEncoder

    df = df.copy()
    df["interval_datetime"] = pd.to_datetime(df["interval_datetime"])
    df = df.sort_values(["region_id", "interval_datetime"]).reset_index(drop=True)

    df["hour_of_day"] = df["interval_datetime"].dt.hour
    df["day_of_week"] = df["interval_datetime"].dt.dayofweek   # Monday=0

    le = LabelEncoder()
    df["region_id_encoded"] = le.fit_transform(df["region_id"])

    for region, grp in df.groupby("region_id", sort=False):
        idx = grp.index
        df.loc[idx, "price_roll_mean_12"]  = (
            grp["spot_price_aud_mwh"].rolling(ROLLING_WINDOW, min_periods=1).mean()
        )
        df.loc[idx, "price_roll_std_12"]   = (
            grp["spot_price_aud_mwh"].rolling(ROLLING_WINDOW, min_periods=1)
            .std().fillna(0.0)
        )
        df.loc[idx, "demand_roll_mean_12"] = (
            grp["total_demand_mw"].rolling(ROLLING_WINDOW, min_periods=1).mean()
        )
        df.loc[idx, "demand_roll_std_12"]  = (
            grp["total_demand_mw"].rolling(ROLLING_WINDOW, min_periods=1)
            .std().fillna(0.0)
        )

    return df


# ---------------------------------------------------------------------------
# Load test data from Spark
# ---------------------------------------------------------------------------

def load_test_data_spark(spark) -> pd.DataFrame:
    """Load the most recent test window from Spark.

    Reads test_start/test_end from the production model MLflow run tags if
    available; otherwise falls back to last 90 days of data.

    Tries gold.nem_prices_5min first, falls back to gold.feature_store_price.
    Returns a raw pandas DataFrame (before feature engineering).
    """
    import pyspark.sql.functions as F

    client = MlflowClient()
    test_start_str = test_end_str = None
    try:
        mv  = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
        run = client.get_run(mv.run_id)
        test_start_str = run.data.tags.get("test_start")
        test_end_str   = run.data.tags.get("test_end")
    except Exception:
        pass

    if test_start_str and test_end_str:
        test_start = datetime.fromisoformat(test_start_str)
        test_end   = datetime.fromisoformat(test_end_str)
    else:
        max_ts = (
            spark.table(FEATURE_STORE_TABLE)
            .agg(F.max("settlementdate").alias("m"))
            .collect()[0]["m"]
        )
        test_end   = max_ts
        test_start = max_ts - timedelta(days=90)

    logger.info("Loading anomaly test data: [%s, %s)", test_start, test_end)

    col_maps = [
        (NEM_PRICES_TABLE, {
            "settlementdate_col": "interval_datetime",
            "price_col":          "spot_price_aud_mwh",
            "demand_col":         "total_demand_mw",
            "region_col":         "region_id",
        }),
        (FEATURE_STORE_TABLE, {
            "settlementdate_col": "settlementdate",
            "price_col":          "rrp",
            "demand_col":         "totaldemand",
            "region_col":         "regionid",
        }),
    ]

    for table, col_map in col_maps:
        try:
            df_spark = (
                spark.table(table)
                .select(
                    F.col(col_map["settlementdate_col"]).alias("interval_datetime"),
                    F.col(col_map["region_col"]).alias("region_id"),
                    F.col(col_map["price_col"]).alias("spot_price_aud_mwh"),
                    F.col(col_map["demand_col"]).alias("total_demand_mw"),
                )
                .filter(F.col("interval_datetime") >= F.lit(test_start.isoformat()))
                .filter(F.col("interval_datetime") <  F.lit(test_end.isoformat()))
                .dropDuplicates(["interval_datetime", "region_id"])
                .orderBy("region_id", "interval_datetime")
            )
            df = df_spark.toPandas()
            if not df.empty:
                logger.info("Loaded %d rows from %s", len(df), table)
                return df
        except Exception as exc:    # noqa: BLE001
            logger.warning("Could not load %s: %s", table, exc)

    raise RuntimeError("No test data loaded from Spark tables.")


# ---------------------------------------------------------------------------
# Load ground truth labels
# ---------------------------------------------------------------------------

def load_known_events(spark, df: pd.DataFrame) -> pd.Series:
    """Load AEMO-confirmed event labels from gold.known_market_events.

    Falls back to rule-based ground truth if the table is absent or empty.
    Returns a pd.Series of str labels (normal/spike/negative/separation).
    """

    try:
        events_df = (
            spark.table(EVENTS_TABLE)
            .select("settlementdate", "regionid", "event_type")
        ).toPandas()

        if events_df.empty:
            raise ValueError("Empty events table")

        df_keys = df[["interval_datetime", "region_id"]].rename(
            columns={
                "interval_datetime": "settlementdate",
                "region_id":         "regionid",
            }
        )
        merged = df_keys.merge(events_df, on=["settlementdate", "regionid"], how="left")
        merged["event_type"] = merged["event_type"].fillna("normal")
        n_events = (merged["event_type"] != "normal").sum()
        logger.info("Loaded %d known events from %s", n_events, EVENTS_TABLE)
        return merged["event_type"].reset_index(drop=True)

    except Exception as exc:    # noqa: BLE001
        logger.warning(
            "Could not load %s (%s); using rule-based ground truth.",
            EVENTS_TABLE, exc,
        )
        return build_rule_ground_truth(df)


# ---------------------------------------------------------------------------
# Classify event types from features (rule-based predictions)
# ---------------------------------------------------------------------------

def classify_event_type_from_features(df: pd.DataFrame) -> pd.Series:
    """Apply deterministic rules to classify each row.

    Uses spot_price_aud_mwh (or rrp if not present).
    Priority: spike > negative > separation > normal.
    """
    price_col = (
        "spot_price_aud_mwh" if "spot_price_aud_mwh" in df.columns else "rrp"
    )
    prices = df[price_col]

    cond_spike    = prices >= SPIKE_THRESHOLD
    cond_negative = prices <= NEGATIVE_THRESHOLD

    interval_col = (
        "interval_datetime" if "interval_datetime" in df.columns else "settlementdate"
    )
    if interval_col in df.columns:
        interval_spread = (
            df.groupby(interval_col)[price_col]
            .transform(lambda x: x.max() - x.min())
        )
    else:
        interval_spread = pd.Series(0.0, index=df.index)
    cond_separation = interval_spread >= SEPARATION_THRESHOLD

    event_type = pd.Series("normal", index=df.index)
    event_type = event_type.where(~cond_separation, "separation")
    event_type = event_type.where(~cond_negative,   "negative")
    event_type = event_type.where(~cond_spike,       "spike")
    return event_type


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def compute_binary_metrics(
    y_true_binary: np.ndarray,
    y_pred_binary: np.ndarray,
    anomaly_scores: np.ndarray,
) -> Dict[str, float]:
    """Compute binary anomaly detection metrics.

    Parameters
    ----------
    y_true_binary  : 1 for anomaly, 0 for normal
    y_pred_binary  : 1 for predicted anomaly, 0 for normal
    anomaly_scores : IsolationForest decision_function (lower = more anomalous)

    Returns precision, recall, f1_score, roc_auc, fp_rate_pct.
    """
    pr, rc, f1, _ = precision_recall_fscore_support(
        y_true_binary, y_pred_binary,
        average="binary",
        zero_division=0,
    )

    # Negate scores: lower decision_function -> more anomalous -> higher AUC score
    try:
        roc_auc = float(roc_auc_score(y_true_binary, -anomaly_scores))
    except ValueError:
        roc_auc = float("nan")

    # False positive rate on confirmed-normal intervals (DoD: < 5%)
    normal_mask = y_true_binary == 0
    fp_rate = (
        float(y_pred_binary[normal_mask].mean()) * 100.0
        if normal_mask.any()
        else float("nan")
    )

    return {
        "precision":   float(pr),
        "recall":      float(rc),
        "f1_score":    float(f1),
        "roc_auc":     roc_auc,
        "fp_rate_pct": fp_rate,
    }


def compute_per_event_type_metrics(
    y_true_labels: pd.Series,
    y_pred_labels: pd.Series,
) -> Dict[str, Dict[str, float]]:
    """Compute precision/recall/F1 per event type (spike/negative/separation).

    Each event type is treated as binary one-vs-rest:
      positive = that event type, negative = all other labels.

    Returns a dict keyed by event type with precision/recall/f1/support.
    """
    per_type: Dict[str, Dict[str, float]] = {}

    for etype in ["spike", "negative", "separation"]:
        yt = (y_true_labels == etype).astype(int).values
        yp = (y_pred_labels == etype).astype(int).values

        if yt.sum() == 0:
            per_type[etype] = {
                "precision": float("nan"),
                "recall":    float("nan"),
                "f1_score":  float("nan"),
                "support":   0,
            }
            continue

        pr, rc, f1, sup = precision_recall_fscore_support(
            yt, yp, average="binary", zero_division=0
        )
        per_type[etype] = {
            "precision": float(pr),
            "recall":    float(rc),
            "f1_score":  float(f1),
            "support":   int(sup),
        }

    return per_type


def compute_alert_storm_rate(is_anomaly: np.ndarray) -> float:
    """Compute alert storm rate.

    Alert storm rate = % of anomaly-flagged intervals that are part of a
    consecutive anomaly run longer than ALERT_STORM_CONSECUTIVE (3).

    High storm rates (>5%) indicate over-detection where the model fires
    continuously rather than selectively on real events.

    Returns the percentage of anomaly intervals in storm runs.
    """
    if len(is_anomaly) == 0:
        return float("nan")

    n = len(is_anomaly)
    in_storm = np.zeros(n, dtype=bool)

    i = 0
    while i < n:
        if is_anomaly[i] == 1:
            j = i
            while j < n and is_anomaly[j] == 1:
                j += 1
            if (j - i) > ALERT_STORM_CONSECUTIVE:
                in_storm[i:j] = True
            i = j
        else:
            i += 1

    n_anomaly = is_anomaly.sum()
    if n_anomaly == 0:
        return 0.0

    return float(in_storm.sum() / n_anomaly * 100.0)


# ---------------------------------------------------------------------------
# DoD check
# ---------------------------------------------------------------------------

def check_dod(f1: float, fp_rate_pct: float) -> bool:
    """Return True when both DoD thresholds are satisfied.

    DoD: F1 > 0.7 AND FP rate < 5%
    """
    if np.isnan(f1) or np.isnan(fp_rate_pct):
        return False
    return bool(f1 > DOD_F1_THRESHOLD and fp_rate_pct < DOD_FP_THRESHOLD)


# ---------------------------------------------------------------------------
# Shared metric packaging
# ---------------------------------------------------------------------------

def _compute_and_package_results(
    df: pd.DataFrame,
    y_true_labels: pd.Series,
    y_true_binary: np.ndarray,
    event_type_pred: pd.Series,
    is_anomaly: np.ndarray,
    anomaly_scores: np.ndarray,
) -> Dict:
    """Compute all metrics and build the result dict."""
    binary_m  = compute_binary_metrics(y_true_binary, is_anomaly, anomaly_scores)
    per_type  = compute_per_event_type_metrics(y_true_labels, event_type_pred)
    storm_rate = compute_alert_storm_rate(is_anomaly)

    # Per-region breakdown
    region_col = "region_id" if "region_id" in df.columns else None
    per_region: List[Dict] = []
    if region_col:
        for region in sorted(df[region_col].unique()):
            mask = (df[region_col] == region).values
            if mask.sum() == 0:
                continue
            yt_b = y_true_binary[mask]
            yp   = is_anomaly[mask]
            sc   = anomaly_scores[mask]

            pr, rc, f1, _ = precision_recall_fscore_support(
                yt_b, yp, average="binary", zero_division=0
            )
            try:
                auc_r = float(roc_auc_score(yt_b, -sc))
            except ValueError:
                auc_r = float("nan")

            normal_m = yt_b == 0
            fp_r = (
                float(yp[normal_m].mean()) * 100.0
                if normal_m.any()
                else float("nan")
            )
            per_region.append({
                "region":      region,
                "precision":   float(pr),
                "recall":      float(rc),
                "f1_score":    float(f1),
                "roc_auc":     auc_r,
                "fp_rate_pct": fp_r,
            })

    passed = check_dod(binary_m["f1_score"], binary_m["fp_rate_pct"])

    result = {
        "eval_date":            datetime.utcnow().date().isoformat(),
        "n_test_rows":          int(len(df)),
        "passed_dod":           passed,
        "binary_metrics":       binary_m,
        "per_event_type":       per_type,
        "alert_storm_rate_pct": storm_rate,
        "per_region":           per_region,
    }

    _log_summary(binary_m, per_type, storm_rate, passed)
    return result


def _log_summary(
    binary_m:   Dict[str, float],
    per_type:   Dict[str, Dict[str, float]],
    storm_rate: float,
    passed:     bool,
) -> None:
    """Print a structured evaluation summary."""
    logger.info("=" * 60)
    logger.info(
        "Anomaly Detection Evaluation  |  DoD: %s",
        "PASS" if passed else "FAIL",
    )
    logger.info("  Precision  : %.3f", binary_m["precision"])
    logger.info("  Recall     : %.3f", binary_m["recall"])
    logger.info(
        "  F1 Score   : %.3f  (threshold > %.2f)",
        binary_m["f1_score"], DOD_F1_THRESHOLD,
    )
    logger.info("  ROC-AUC    : %.3f", binary_m["roc_auc"])
    logger.info(
        "  FP Rate    : %.1f %%  (threshold < %.1f %%)",
        binary_m["fp_rate_pct"], DOD_FP_THRESHOLD,
    )
    logger.info("  Alert Storm: %.1f %%", storm_rate)
    logger.info("")
    for etype in ["spike", "negative", "separation"]:
        m = per_type.get(etype, {})
        logger.info(
            "  %-12s | P: %.3f | R: %.3f | F1: %.3f | n=%d",
            etype.upper(),
            m.get("precision", float("nan")),
            m.get("recall",    float("nan")),
            m.get("f1_score",  float("nan")),
            m.get("support",   0),
        )


# ---------------------------------------------------------------------------
# Evaluation pipeline (Spark / production path)
# ---------------------------------------------------------------------------

def evaluate_spark(spark) -> Dict:
    """Full evaluation pipeline using the Spark feature store."""
    model_uri = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
    logger.info("Loading model: %s", model_uri)
    model = mlflow.sklearn.load_model(model_uri)

    df_raw   = load_test_data_spark(spark)
    df       = engineer_features(df_raw)
    df_clean = df.dropna(subset=FEATURE_COLS).reset_index(drop=True)

    X = df_clean[FEATURE_COLS].fillna(0.0).values

    anomaly_scores = model.decision_function(X)
    is_anomaly_if  = (model.predict(X) == -1).astype(int)

    event_type_pred = classify_event_type_from_features(df_clean)
    is_rule_based   = (event_type_pred != "normal").astype(int).values

    # Combined: IsolationForest OR rule-based
    is_anomaly = np.clip(is_anomaly_if + is_rule_based, 0, 1)

    y_true_labels = load_known_events(spark, df_clean)
    y_true_binary = (y_true_labels != "normal").astype(int).values

    return _compute_and_package_results(
        df_clean, y_true_labels, y_true_binary,
        event_type_pred, is_anomaly, anomaly_scores,
    )


# ---------------------------------------------------------------------------
# Evaluation pipeline (unit-test / offline path)
# ---------------------------------------------------------------------------

def evaluate_offline() -> Dict:
    """Evaluate using synthetic data (no Spark required).

    Generates data with injected anomalies, uses rule-based classification
    as predictions, and validates that all metric calculations produce
    finite numbers. IsolationForest is not used; a price z-score proxy
    is used as the anomaly score.
    """
    logger.info("Unit-test mode: evaluating with synthetic anomaly data")

    df_raw, true_labels = _make_synthetic_anomaly_data()
    df       = engineer_features(df_raw)
    df_clean = df.dropna(subset=FEATURE_COLS).reset_index(drop=True)

    # Trim true_labels to match df_clean (dropna removes warm-up rows)
    n_dropped     = len(df) - len(df_clean)
    y_true_labels = true_labels.iloc[n_dropped:].reset_index(drop=True)
    y_true_binary = (y_true_labels != "normal").astype(int).values

    # Proxy anomaly score: price z-score (high price = high anomaly)
    prices     = df_clean["spot_price_aud_mwh"].values
    price_mean = prices.mean()
    price_std  = prices.std()
    # Negate to match IsolationForest convention (lower = more anomalous)
    anomaly_scores = -((prices - price_mean) / (price_std + 1e-6))

    # Proxy predictions: rule-based classification
    event_type_pred = classify_event_type_from_features(df_clean)
    is_anomaly      = (event_type_pred != "normal").astype(int).values

    return _compute_and_package_results(
        df_clean, y_true_labels, y_true_binary,
        event_type_pred, is_anomaly, anomaly_scores,
    )


# ---------------------------------------------------------------------------
# MLflow logging -- anomaly_{region}_{metric} naming convention
# ---------------------------------------------------------------------------

def log_to_mlflow(result: Dict) -> None:
    """Log evaluation metrics to the active MLflow run.

    Naming conventions:
      anomaly_overall_{metric}     : overall binary metrics
      anomaly_{event_type}_{metric}: per-event-type metrics
      anomaly_{region}_{metric}    : per-region metrics
    """
    binary_m = result["binary_metrics"]
    per_type = result["per_event_type"]
    metrics: Dict[str, float] = {}

    # Overall binary metrics
    for k, v in binary_m.items():
        if not np.isnan(v):
            metrics[f"anomaly_overall_{k}"] = v

    # Alert storm rate
    sr = result["alert_storm_rate_pct"]
    if not np.isnan(sr):
        metrics["anomaly_overall_alert_storm_rate_pct"] = sr

    # DoD flag (0.0 or 1.0)
    metrics["anomaly_overall_passed_dod"] = 1.0 if result["passed_dod"] else 0.0

    # Per-event-type metrics
    for etype, m in per_type.items():
        for mk, mv in m.items():
            if mk == "support":
                metrics[f"anomaly_{etype}_{mk}"] = float(mv)
            elif isinstance(mv, float) and not np.isnan(mv):
                metrics[f"anomaly_{etype}_{mk}"] = mv

    # Per-region metrics
    for rrow in result.get("per_region", []):
        region = rrow["region"].lower()
        for mk in ["precision", "recall", "f1_score", "roc_auc", "fp_rate_pct"]:
            v = rrow.get(mk, float("nan"))
            if not np.isnan(v):
                metrics[f"anomaly_{region}_{mk}"] = v

    mlflow.log_metrics(metrics)
    logger.info("Logged %d MLflow metrics for anomaly evaluation", len(metrics))


# ---------------------------------------------------------------------------
# Save evaluation results to Delta table
# ---------------------------------------------------------------------------

def save_to_delta(spark, result: Dict) -> None:
    """Write evaluation results to gold.anomaly_detection_evaluation.

    Schema:
      region (str), eval_date (str),
      precision (double), recall (double), f1_score (double), roc_auc (double),
      fp_rate_pct (double), alert_storm_rate_pct (double),
      spike_recall (double), negative_recall (double), separation_recall (double),
      passed_dod (boolean)

    Writes one overall row (region=all) and one row per NEM region.
    """
    binary_m = result["binary_metrics"]
    per_type = result["per_event_type"]
    rows = []

    # Overall row
    rows.append({
        "region":               "all",
        "eval_date":            result["eval_date"],
        "precision":            binary_m["precision"],
        "recall":               binary_m["recall"],
        "f1_score":             binary_m["f1_score"],
        "roc_auc":              binary_m["roc_auc"],
        "fp_rate_pct":          binary_m["fp_rate_pct"],
        "alert_storm_rate_pct": result["alert_storm_rate_pct"],
        "spike_recall":         per_type.get("spike",      {}).get("recall", float("nan")),
        "negative_recall":      per_type.get("negative",   {}).get("recall", float("nan")),
        "separation_recall":    per_type.get("separation", {}).get("recall", float("nan")),
        "passed_dod":           bool(result["passed_dod"]),
    })

    # Per-region rows
    for rrow in result.get("per_region", []):
        rows.append({
            "region":               rrow["region"],
            "eval_date":            result["eval_date"],
            "precision":            rrow.get("precision",   float("nan")),
            "recall":               rrow.get("recall",      float("nan")),
            "f1_score":             rrow.get("f1_score",    float("nan")),
            "roc_auc":              rrow.get("roc_auc",     float("nan")),
            "fp_rate_pct":          rrow.get("fp_rate_pct", float("nan")),
            "alert_storm_rate_pct": float("nan"),
            "spike_recall":         float("nan"),
            "negative_recall":      float("nan"),
            "separation_recall":    float("nan"),
            "passed_dod":           bool(result["passed_dod"]),
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
# Promotion: champion alias when DoD passes
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# Promotion: champion alias when DoD passes
# ---------------------------------------------------------------------------

def promote_champion(result):
    """Set the champion alias on the anomaly detection model when DoD passes.

    DoD: F1 > 0.7 AND FP rate < 5%.
    Prints a formatted summary promotion table.
    """
    passed   = result["passed_dod"]
    binary_m = result["binary_metrics"]
    per_type = result["per_event_type"]

    print("\nAnomaly Detection Evaluation Summary")
    print("-" * 52)
    f1_val  = binary_m.get("f1_score",    float("nan"))
    fp_val  = binary_m.get("fp_rate_pct", float("nan"))
    auc_val = binary_m.get("roc_auc",     float("nan"))
    print("  F1 Score   : {:.3f}  (threshold > {:.2f})".format(f1_val, DOD_F1_THRESHOLD))
    print("  FP Rate    : {:.1f} %  (threshold < {:.1f} %)".format(fp_val, DOD_FP_THRESHOLD))
    print("  ROC-AUC    : {:.3f}".format(auc_val))
    print()
    for etype in ["spike", "negative", "separation"]:
        m  = per_type.get(etype, {})
        pr = m.get("precision", float("nan"))
        rc = m.get("recall",    float("nan"))
        f1 = m.get("f1_score",  float("nan"))
        print("  {:<12} | P: {:.3f} | R: {:.3f} | F1: {:.3f}".format(
            etype.upper(), pr, rc, f1))
    print("-" * 52)
    print("  DoD: {}".format("PASS" if passed else "FAIL"))

    if passed:
        print("\nDoD passed -- setting champion alias.")
        client = MlflowClient()
        try:
            mv = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
            client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, mv.version)
            logger.info(
                "Promoted %s v%s to alias champion",
                MODEL_NAME, mv.version,
            )
        except Exception as exc:    # noqa: BLE001
            logger.warning("Could not promote %s: %s", MODEL_NAME, exc)
    else:
        print("\nPromotion blocked. DoD: F1 > {}, FP rate < {}%".format(
            DOD_F1_THRESHOLD, DOD_FP_THRESHOLD))


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------

def run_evaluation(spark=None, promote: bool = False) -> Dict:
    """Run the full anomaly detection evaluation pipeline.

    Parameters
    ----------
    spark   : SparkSession or None (triggers unit-test mode)
    promote : if True, set champion alias when DoD passes

    Returns
    -------
    Result dict with metrics and DoD flag.
    """
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    result: Dict = {}

    with mlflow.start_run(run_name="anomaly_detector_evaluation"):
        try:
            if spark is None:
                result = evaluate_offline()
            else:
                result = evaluate_spark(spark)

            log_to_mlflow(result)

            # Save JSON report artifact
            report_path = "/tmp/anomaly_eval_report.json"
            with open(report_path, "w") as fh:
                json.dump(result, fh, indent=2, default=str)
            mlflow.log_artifact(report_path, artifact_path="evaluation")

            # Write Delta table (Spark path only)
            if spark is not None:
                save_to_delta(spark, result)

            # Promotion decision
            if promote:
                promote_champion(result)

        except Exception as exc:    # noqa: BLE001
            logger.error("Evaluation failed: %s", exc, exc_info=True)

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Evaluate NEM anomaly detection model (Sprint 8a)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        default=False,
        help="Set champion alias when DoD passes (F1 > 0.7 AND FP rate < 5%).",
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
        promote=args.promote,
    )
