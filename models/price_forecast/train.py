"""
Price Forecast -- LightGBM Training
=====================================
Trains one LightGBM model per NEM region (5 total) using features from
energy_copilot.gold.feature_store_price with forecast_horizon as an
explicit model feature (multi-horizon single model per region).

Hyperparameter optimisation: Optuna with 50 trials, TPE sampler.
Tracking: MLflow experiments on Databricks.
Model registration: MLflow Model Registry (Unity Catalog), initial alias
  "champion".  The "production" alias should be set manually after the
  model passes Definition-of-Done evaluation thresholds in evaluate.py.

Split strategy (no data leakage):
  - train : earliest 18 months of the feature store
  - val   : next 3 months (used by Optuna for early stopping signal)
  - test  : final 3 months (held out; evaluation done in evaluate.py)

The test set boundaries are saved as MLflow tags so that evaluate.py can
reproduce the exact same split without needing to re-read the whole table.

Spike weighting:
  Intervals where rrp_target > $300/MWh receive a sample weight of
  SPIKE_WEIGHT (3.0) during LightGBM training.  This improves recall
  on price spikes without requiring a separate model.

Usage
-----
  python train.py            # Databricks task or local with Spark session
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
import pyspark.sql.functions as F
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG       = "energy_copilot"
GOLD          = f"{CATALOG}.gold"
FEATURE_TABLE = f"{GOLD}.feature_store_price"

NEM_REGIONS: List[str] = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

MLFLOW_EXPERIMENT = "/energy_copilot/price_forecast_training"
MODEL_NAME_TMPL   = "price_forecast_{region}"

TRAIN_MONTHS = 18
VAL_MONTHS   = 3
TEST_MONTHS  = 3

OPTUNA_TRIALS = 50
OPTUNA_SEED   = 42

SPIKE_THRESHOLD = 500.0   # $/MWh -- used for per-segment MAE/MAPE metrics
SPIKE_WEIGHT    = 3.0     # sample weight multiplier for intervals where rrp_target > $300/MWh
SPIKE_WEIGHT_THRESHOLD = 300.0  # $/MWh threshold for applying SPIKE_WEIGHT

LABEL_COL     = "rrp_target"
EXCLUDED_COLS = {
    "settlementdate", "settlement_date", "regionid", "rrp", "totaldemand",
    LABEL_COL, "feature_timestamp", "season",   # season encoded separately below
    "volatility_regime",                         # encoded via one-hot below
}

# LightGBM fixed params (not tuned by Optuna)
LGB_FIXED_PARAMS: Dict[str, Any] = {
    "objective":       "regression_l1",   # MAE objective
    "metric":          "mae",
    "boosting_type":   "gbdt",
    "n_jobs":          -1,
    "verbose":         -1,
    "random_state":    42,
}


# ---------------------------------------------------------------------------
# Feature preparation helpers
# ---------------------------------------------------------------------------

def encode_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """One-hot encode low-cardinality string columns (season, volatility_regime).

    Label-encoding regionid is not needed because each model is trained on
    a single region, but the column is in EXCLUDED_COLS for safety.
    """
    for col in ("season", "volatility_regime"):
        if col in df.columns:
            dummies = pd.get_dummies(df[col], prefix=col, drop_first=False)
            df = pd.concat([df.drop(columns=[col]), dummies], axis=1)
    return df


def get_feature_columns(df: pd.DataFrame) -> List[str]:
    """Return the sorted list of feature columns, excluding labels and IDs."""
    return sorted([c for c in df.columns if c not in EXCLUDED_COLS])


def build_sample_weights(y: np.ndarray) -> np.ndarray:
    """Return per-sample weights: SPIKE_WEIGHT for spike intervals, 1.0 otherwise.

    Spike intervals are defined as rrp_target > SPIKE_WEIGHT_THRESHOLD ($/MWh).
    Higher weights on spikes cause LightGBM to penalise spike prediction errors
    more heavily, improving recall on extreme price events.
    """
    weights = np.ones(len(y), dtype=np.float32)
    weights[y > SPIKE_WEIGHT_THRESHOLD] = SPIKE_WEIGHT
    return weights


def prepare_region_data(
    spark: SparkSession,
    region: str,
    train_end: datetime,
    val_end: datetime,
    test_end: datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load features for a single NEM region from the feature store and split
    into train / val / test DataFrames.

    Returns
    -------
    df_train, df_val, df_test : pd.DataFrame
        Each split retains all columns (feature columns are selected later).
    """
    train_start = train_end - timedelta(days=TRAIN_MONTHS * 30)

    df_spark = (
        spark.table(FEATURE_TABLE)
        .filter(F.col("regionid") == region)
        .filter(F.col("settlementdate") >= F.lit(train_start.isoformat()))
        .filter(F.col("settlementdate") <  F.lit(test_end.isoformat()))
        .orderBy("settlementdate", "forecast_horizon")
    )
    df: pd.DataFrame = df_spark.toPandas()
    df = encode_categoricals(df)

    # Chronological split
    train_mask = df["settlementdate"] <  train_end
    val_mask   = (df["settlementdate"] >= train_end) & (df["settlementdate"] < val_end)
    test_mask  = (df["settlementdate"] >= val_end)   & (df["settlementdate"] < test_end)

    return df[train_mask], df[val_mask], df[test_mask]


# ---------------------------------------------------------------------------
# Metrics helpers
# ---------------------------------------------------------------------------

def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """Compute MAE, MAPE for all rows and separately for normal vs spike events.

    Spike is defined as rrp_target > SPIKE_THRESHOLD $/MWh.
    """
    eps = 1e-6
    abs_err = np.abs(y_true - y_pred)
    pct_err = abs_err / (np.abs(y_true) + eps)

    spike_mask  = y_true > SPIKE_THRESHOLD
    normal_mask = ~spike_mask

    metrics: Dict[str, float] = {
        "mae_all":             float(np.mean(abs_err)),
        "mape_all":            float(np.mean(pct_err) * 100.0),
        "mae_normal":          float(np.mean(abs_err[normal_mask]))  if normal_mask.any() else float("nan"),
        "mape_normal":         float(np.mean(pct_err[normal_mask]) * 100.0) if normal_mask.any() else float("nan"),
        "mae_spike":           float(np.mean(abs_err[spike_mask]))   if spike_mask.any() else float("nan"),
        "mape_spike":          float(np.mean(pct_err[spike_mask]) * 100.0) if spike_mask.any() else float("nan"),
        "n_spike_events_true": int(spike_mask.sum()),
    }
    return metrics


# ---------------------------------------------------------------------------
# Optuna objective with early stopping
# ---------------------------------------------------------------------------

def make_objective(
    X_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    X_val:   np.ndarray,
    y_val:   np.ndarray,
) -> Any:
    """Return an Optuna objective function for LightGBM hyperparameter search.

    Hyperparameter search space (per sprint requirements):
      num_leaves        : int   [20, 300]
      learning_rate     : float [1e-4, 0.3]  (log scale)
      n_estimators      : int   [200, 2000]
      min_child_samples : int   [5, 100]
      subsample         : float [0.5, 1.0]
      colsample_bytree  : float [0.5, 1.0]
      lambda_l1         : float [0, 10]
      lambda_l2         : float [0, 10]

    Early stopping (stopping_rounds=50) is attached so that trials with a
    high n_estimators terminate early when the val MAE plateaus.
    """
    def objective(trial: optuna.Trial) -> float:
        params = {
            **LGB_FIXED_PARAMS,
            "num_leaves":        trial.suggest_int("num_leaves",        20,   300),
            "learning_rate":     trial.suggest_float("learning_rate",   1e-4, 0.3,  log=True),
            "n_estimators":      trial.suggest_int("n_estimators",      200,  2000),
            "min_child_samples": trial.suggest_int("min_child_samples", 5,    100),
            "subsample":         trial.suggest_float("subsample",       0.5,  1.0),
            "colsample_bytree":  trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "lambda_l1":         trial.suggest_float("lambda_l1",       0.0,  10.0),
            "lambda_l2":         trial.suggest_float("lambda_l2",       0.0,  10.0),
        }

        dtrain = lgb.Dataset(X_train, label=y_train, weight=w_train)
        dval   = lgb.Dataset(X_val,   label=y_val,   reference=dtrain)

        # Pruning callback integrates with Optuna's MedianPruner;
        # early_stopping fires when val MAE does not improve for 50 rounds.
        callbacks = [
            lgb.early_stopping(stopping_rounds=50, verbose=False),
            lgb.log_evaluation(period=-1),
            optuna.integration.lightgbm.LightGBMPruningCallback(trial, "mae"),
        ]

        booster = lgb.train(
            params,
            dtrain,
            valid_sets=[dval],
            callbacks=callbacks,
        )

        y_pred = booster.predict(X_val)
        mae = float(np.mean(np.abs(y_val - y_pred)))
        return mae

    return objective


# ---------------------------------------------------------------------------
# Single-region training
# ---------------------------------------------------------------------------

def train_region(
    spark:      SparkSession,
    region:     str,
    train_end:  datetime,
    val_end:    datetime,
    test_end:   datetime,
) -> None:
    """Run the full training pipeline for a single NEM region.

    Steps:
      1. Load and split data
      2. Build spike sample weights for training set
      3. Optuna hyperparameter search (50 trials) with early stopping + pruning
      4. Log best params as MLflow run tags (prefix "best_param_")
      5. Final training on train + val combined with best params + spike weights
      6. Log metrics, feature importance, and model artifact to MLflow
      7. Register model to Unity Catalog with alias "champion"

    Note: The "production" alias should be set only after evaluate.py confirms
    the model passes Definition-of-Done thresholds (e.g. mae_all < $15/MWh on
    the held-out test set, spike recall >= 0.60).  Set via:
        mlflow_client.set_registered_model_alias(model_name, "production", version)
    """
    logger.info("=== Training region: %s ===", region)

    df_train, df_val, df_test = prepare_region_data(
        spark, region, train_end, val_end, test_end
    )

    feat_cols = get_feature_columns(df_train)
    logger.info("Feature count for %s: %d", region, len(feat_cols))

    X_train = df_train[feat_cols].values
    y_train = df_train[LABEL_COL].values
    w_train = build_sample_weights(y_train)   # spike weights

    X_val   = df_val[feat_cols].values
    y_val   = df_val[LABEL_COL].values

    X_test  = df_test[feat_cols].values
    y_test  = df_test[LABEL_COL].values

    model_name = MODEL_NAME_TMPL.format(region=region.lower().replace("1", ""))

    # ------------------------------------------------------------------
    # Hyperparameter search
    # ------------------------------------------------------------------
    with mlflow.start_run(run_name=f"{region}_hyperopt") as parent_run:
        mlflow.set_tag("region",     region)
        mlflow.set_tag("phase",      "hyperparameter_tuning")
        mlflow.set_tag("train_end",  train_end.isoformat())
        mlflow.set_tag("val_end",    val_end.isoformat())
        mlflow.set_tag("test_end",   test_end.isoformat())
        mlflow.log_param("feature_columns",  json.dumps(feat_cols))
        mlflow.log_param("train_rows",       len(X_train))
        mlflow.log_param("val_rows",         len(X_val))
        mlflow.log_param("test_rows",        len(X_test))
        mlflow.log_param("optuna_trials",    OPTUNA_TRIALS)
        mlflow.log_param("spike_weight",     SPIKE_WEIGHT)
        mlflow.log_param("spike_weight_threshold", SPIKE_WEIGHT_THRESHOLD)

        sampler = optuna.samplers.TPESampler(seed=OPTUNA_SEED)
        pruner  = optuna.pruners.MedianPruner(n_startup_trials=10, n_warmup_steps=50)
        study   = optuna.create_study(
            direction="minimize",
            sampler=sampler,
            pruner=pruner,
            study_name=f"price_forecast_{region}",
        )
        objective = make_objective(X_train, y_train, w_train, X_val, y_val)
        study.optimize(
            objective,
            n_trials=OPTUNA_TRIALS,
            show_progress_bar=False,
        )

        best_params = {**LGB_FIXED_PARAMS, **study.best_params}
        logger.info("Best params for %s: %s", region, best_params)

        # Log best Optuna params as individual params (for sorting/filtering)
        mlflow.log_params({f"best_{k}": v for k, v in study.best_params.items()})
        # Also log as run tags for easy retrieval by register_all_models.py
        for k, v in study.best_params.items():
            mlflow.set_tag(f"best_param_{k}", str(v))
        mlflow.log_metric("best_val_mae", study.best_value)

    # ------------------------------------------------------------------
    # Final model: retrain on train + val combined with best params
    # ------------------------------------------------------------------
    X_final = np.vstack([X_train, X_val])
    y_final = np.concatenate([y_train, y_val])
    w_final = build_sample_weights(y_final)

    dtrain_final = lgb.Dataset(X_final, label=y_final, weight=w_final)

    with mlflow.start_run(run_name=f"{region}_final_model") as run:
        mlflow.set_tag("region",    region)
        mlflow.set_tag("phase",     "final_training")
        mlflow.set_tag("train_end", train_end.isoformat())
        mlflow.set_tag("val_end",   val_end.isoformat())
        mlflow.set_tag("test_end",  test_end.isoformat())

        # Store best params both as logged params and as run tags
        mlflow.log_params({f"param_{k}": v for k, v in study.best_params.items()})
        for k, v in study.best_params.items():
            mlflow.set_tag(f"best_param_{k}", str(v))

        mlflow.log_param("feature_columns",         json.dumps(feat_cols))
        mlflow.log_param("spike_weight",            SPIKE_WEIGHT)
        mlflow.log_param("spike_weight_threshold",  SPIKE_WEIGHT_THRESHOLD)

        final_booster = lgb.train(
            best_params,
            dtrain_final,
        )

        # Evaluate on held-out test set (not used for model selection)
        y_pred_test = final_booster.predict(X_test)
        test_metrics = compute_metrics(y_test, y_pred_test)
        mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})
        logger.info("Test metrics for %s: %s", region, test_metrics)

        # Feature importance CSV artifact
        importance_df = pd.DataFrame({
            "feature":    feat_cols,
            "importance": final_booster.feature_importance(importance_type="gain"),
        }).sort_values("importance", ascending=False)
        importance_path = f"/tmp/feature_importance_{region}.csv"
        importance_df.to_csv(importance_path, index=False)
        mlflow.log_artifact(importance_path, artifact_path="feature_importance")

        # Model signature and artifact
        sample_input = pd.DataFrame(X_train[:5], columns=feat_cols)
        signature = mlflow.models.infer_signature(
            sample_input,
            final_booster.predict(X_train[:5]),
        )
        mlflow.lightgbm.log_model(
            final_booster,
            artifact_path="model",
            registered_model_name=model_name,
            signature=signature,
            input_example=pd.DataFrame(X_train[:3], columns=feat_cols),
        )

        # ------------------------------------------------------------------
        # Assign "champion" alias (not "production") for initial review.
        # "production" should be set only after evaluate.py confirms the
        # model passes Definition-of-Done thresholds:
        #   - test_mae_all         < 15 $/MWh   (or region-specific target)
        #   - test_mae_spike       < 200 $/MWh  (spike interval accuracy)
        #   - n_spike_events_true  > 0           (at least some spike events)
        # Set via:
        #   client.set_registered_model_alias(model_name, "production", version)
        # ------------------------------------------------------------------
        client = MlflowClient()
        latest_versions = client.get_latest_versions(model_name)
        if latest_versions:
            latest_version = str(max(int(v.version) for v in latest_versions))
            client.set_registered_model_alias(model_name, "champion", latest_version)
            logger.info(
                "Registered %s v%s with alias 'champion' (awaiting DoD evaluation before 'production')",
                model_name, latest_version,
            )
        else:
            logger.warning("No registered versions found for %s after log_model.", model_name)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def train_all_regions(spark: SparkSession) -> None:
    """Determine train/val/test boundaries from the feature store and train
    one model per NEM region sequentially.
    """
    date_stats = (
        spark.table(FEATURE_TABLE)
        .agg(
            F.min("settlementdate").alias("min_dt"),
            F.max("settlementdate").alias("max_dt"),
        )
        .collect()[0]
    )
    data_start = date_stats["min_dt"]
    data_end   = date_stats["max_dt"]

    train_end = data_start + timedelta(days=TRAIN_MONTHS * 30)
    val_end   = train_end  + timedelta(days=VAL_MONTHS   * 30)
    test_end  = val_end    + timedelta(days=TEST_MONTHS  * 30)

    logger.info(
        "Split: train [%s, %s) | val [%s, %s) | test [%s, %s)",
        data_start.date(), train_end.date(),
        train_end.date(), val_end.date(),
        val_end.date(), test_end.date(),
    )
    logger.info("Data available through: %s", data_end)

    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    for region in NEM_REGIONS:
        train_region(spark, region, train_end, val_end, test_end)

    logger.info("All regions trained successfully.")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()  # noqa: F821
    train_all_regions(spark)
