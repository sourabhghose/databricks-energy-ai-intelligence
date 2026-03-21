"""
Demand Forecast -- LightGBM Training
=======================================
Trains one LightGBM model per NEM region (5 models total) to predict
total_demand_mw at each dispatch interval for multiple forecast horizons.

Key differences from price_forecast/train.py:
  - Target column: totaldemand (MW) instead of rrp ($/MWh)
  - No spike-segmented metrics (demand spikes are less extreme)
  - Feature table: gold.feature_store_price is reused because it includes
    demand lags, rolling demand stats, weather, and calendar features -- all
    relevant for demand forecasting.  The label column is totaldemand shifted
    forward by horizon steps (demand_target).
  - Model names: "demand_forecast_{region}"
  - Evaluation metric emphasis: MAPE (target < 3%)

Split strategy: 18 months train, 3 months val, 3 months test (chronological).
Hyperparameter tuning: Optuna, 50 trials, TPE sampler, MAE objective.
Logging: MLflow experiment /energy_copilot/demand_forecast_training.
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
import pyspark.sql.types as T
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG       = "energy_copilot"
GOLD          = f"{CATALOG}.gold"
FEATURE_TABLE = f"{GOLD}.feature_store_price"   # reuse -- contains demand features

NEM_REGIONS: List[str] = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

MLFLOW_EXPERIMENT = "/energy_copilot/demand_forecast_training"
MODEL_NAME_TMPL   = "demand_forecast_{region}"

TRAIN_MONTHS = 18
VAL_MONTHS   = 3
TEST_MONTHS  = 3

OPTUNA_TRIALS = 50
OPTUNA_SEED   = 42

LABEL_COL     = "demand_target"   # added by explode_demand_horizons below
FORECAST_HORIZONS: List[int] = [1, 4, 8, 12, 24, 48]

EXCLUDED_COLS = {
    "settlementdate", "settlement_date", "regionid", "rrp", "totaldemand",
    "rrp_target", "demand_target", "feature_timestamp", "season",
}

LGB_FIXED_PARAMS: Dict[str, Any] = {
    "objective":     "regression_l1",
    "metric":        "mae",
    "boosting_type": "gbdt",
    "n_jobs":        -1,
    "verbose":       -1,
    "random_state":  42,
}


# ---------------------------------------------------------------------------
# Add demand_target label (totaldemand at future horizon)
# ---------------------------------------------------------------------------

def add_demand_target(spark: SparkSession, df_spark) -> "pyspark.sql.DataFrame":
    """
    Cross-join the base price/demand feature table with forecast horizons,
    then self-join to look up future totaldemand as the label.

    This mirrors feature_engineering.explode_horizons but for totaldemand.
    """
    horizons_df = spark.createDataFrame(
        [(h,) for h in FORECAST_HORIZONS],
        schema=T.StructType([T.StructField("forecast_horizon", T.IntegerType(), False)]),
    )

    df_x = df_spark.crossJoin(horizons_df)
    df_x = df_x.withColumn(
        "_target_ts",
        (
            F.col("settlementdate").cast("long")
            + F.col("forecast_horizon") * F.lit(300)
        ).cast("timestamp"),
    )

    future = df_spark.select(
        F.col("settlementdate").alias("_future_ts"),
        F.col("regionid").alias("_future_region"),
        F.col("totaldemand").alias("demand_target"),
    ).distinct()

    df_x = (
        df_x
        .join(
            future,
            (F.col("_target_ts") == F.col("_future_ts"))
            & (F.col("regionid") == F.col("_future_region")),
            how="left",
        )
        .drop("_target_ts", "_future_ts", "_future_region")
        .filter(F.col("demand_target").isNotNull())
    )
    return df_x


# ---------------------------------------------------------------------------
# Data loading and splitting
# ---------------------------------------------------------------------------

def prepare_region_data(
    spark:     SparkSession,
    region:    str,
    train_end: datetime,
    val_end:   datetime,
    test_end:  datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train_start = train_end - timedelta(days=TRAIN_MONTHS * 30)

    df_spark_base = (
        spark.table(FEATURE_TABLE)
        .filter(F.col("regionid") == region)
        .filter(F.col("settlementdate") >= F.lit(train_start.isoformat()))
        .filter(F.col("settlementdate") <  F.lit(test_end.isoformat()))
        # Use the unique rows from the price feature store (drop rrp_target horizon explode)
        .dropDuplicates(["settlementdate", "regionid"])
    )

    df_spark = add_demand_target(spark, df_spark_base)
    df = df_spark.toPandas()

    if "season" in df.columns:
        dummies = pd.get_dummies(df["season"], prefix="season", drop_first=False)
        df = pd.concat([df.drop(columns=["season"]), dummies], axis=1)

    train_mask = df["settlementdate"] <  train_end
    val_mask   = (df["settlementdate"] >= train_end) & (df["settlementdate"] < val_end)
    test_mask  = (df["settlementdate"] >= val_end)   & (df["settlementdate"] < test_end)

    return df[train_mask], df[val_mask], df[test_mask]


def get_feature_columns(df: pd.DataFrame) -> List[str]:
    return sorted([c for c in df.columns if c not in EXCLUDED_COLS])


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    eps      = 1.0   # avoid /0 for near-zero demand
    abs_err  = np.abs(y_true - y_pred)
    pct_err  = abs_err / (np.abs(y_true) + eps)
    return {
        "mae":          float(np.mean(abs_err)),
        "rmse":         float(np.sqrt(np.mean(abs_err ** 2))),
        "mape_pct":     float(np.mean(pct_err) * 100.0),
        "max_abs_err":  float(np.max(abs_err)),
    }


# ---------------------------------------------------------------------------
# Optuna objective
# ---------------------------------------------------------------------------

def make_objective(X_train, y_train, X_val, y_val):
    def objective(trial: optuna.Trial) -> float:
        params = {
            **LGB_FIXED_PARAMS,
            "num_leaves":        trial.suggest_int("num_leaves",        31,  512),
            "learning_rate":     trial.suggest_float("learning_rate",   1e-3, 0.3,  log=True),
            "max_depth":         trial.suggest_int("max_depth",         3,   12),
            "min_child_samples": trial.suggest_int("min_child_samples", 10,  200),
            "reg_alpha":         trial.suggest_float("reg_alpha",       1e-8, 10.0, log=True),
            "reg_lambda":        trial.suggest_float("reg_lambda",      1e-8, 10.0, log=True),
            "subsample":         trial.suggest_float("subsample",       0.5,  1.0),
            "n_estimators":      trial.suggest_int("n_estimators",      200, 2000),
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
    spark:     SparkSession,
    region:    str,
    train_end: datetime,
    val_end:   datetime,
    test_end:  datetime,
) -> None:
    logger.info("=== Demand forecast training: %s ===", region)
    df_train, df_val, df_test = prepare_region_data(
        spark, region, train_end, val_end, test_end
    )
    feat_cols = get_feature_columns(df_train)
    X_train, y_train = df_train[feat_cols].values, df_train[LABEL_COL].values
    X_val,   y_val   = df_val[feat_cols].values,   df_val[LABEL_COL].values
    X_test,  y_test  = df_test[feat_cols].values,  df_test[LABEL_COL].values

    model_name = MODEL_NAME_TMPL.format(region=region.lower().replace("1", ""))

    # Hyperparameter search
    with mlflow.start_run(run_name=f"{region}_hyperopt"):
        mlflow.set_tag("region", region)
        mlflow.set_tag("phase",  "hyperparameter_tuning")
        mlflow.log_param("optuna_trials", OPTUNA_TRIALS)

        study = optuna.create_study(
            direction="minimize",
            sampler=optuna.samplers.TPESampler(seed=OPTUNA_SEED),
        )
        study.optimize(make_objective(X_train, y_train, X_val, y_val), n_trials=OPTUNA_TRIALS)
        best_params = {**LGB_FIXED_PARAMS, **study.best_params}
        mlflow.log_metric("best_val_mae", study.best_value)

    # Final model on train + val
    X_final = np.vstack([X_train, X_val])
    y_final = np.concatenate([y_train, y_val])

    with mlflow.start_run(run_name=f"{region}_final_model") as run:
        mlflow.set_tag("region",    region)
        mlflow.set_tag("phase",     "final_training")
        mlflow.set_tag("train_end", train_end.isoformat())
        mlflow.set_tag("val_end",   val_end.isoformat())
        mlflow.set_tag("test_end",  test_end.isoformat())
        mlflow.log_params({f"param_{k}": v for k, v in study.best_params.items()})
        mlflow.log_param("feature_columns", json.dumps(feat_cols))

        booster = lgb.train(
            best_params,
            lgb.Dataset(X_final, label=y_final),
        )

        test_metrics = compute_metrics(y_test, booster.predict(X_test))
        mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})
        logger.info("Test metrics for %s: %s", region, test_metrics)

        # Log MAPE compliance
        mape = test_metrics["mape_pct"]
        mlflow.set_tag("mape_target_met", str(mape < 3.0))

        signature = mlflow.models.infer_signature(
            pd.DataFrame(X_train[:5], columns=feat_cols),
            booster.predict(X_train[:5]),
        )
        mlflow.lightgbm.log_model(
            booster,
            artifact_path="model",
            registered_model_name=model_name,
            signature=signature,
            input_example=pd.DataFrame(X_train[:3], columns=feat_cols),
        )

        client = MlflowClient()
        latest = max(
            int(v.version)
            for v in client.get_latest_versions(model_name)
        )
        client.set_registered_model_alias(model_name, "production", str(latest))
        logger.info("Registered %s v%d with alias 'production'", model_name, latest)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def train_all_regions(spark: SparkSession) -> None:
    date_stats = (
        spark.table(FEATURE_TABLE)
        .agg(
            F.min("settlementdate").alias("min_dt"),
            F.max("settlementdate").alias("max_dt"),
        )
        .collect()[0]
    )
    data_start = date_stats["min_dt"]
    train_end = data_start + timedelta(days=TRAIN_MONTHS * 30)
    val_end   = train_end  + timedelta(days=VAL_MONTHS   * 30)
    test_end  = val_end    + timedelta(days=TEST_MONTHS  * 30)

    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    for region in NEM_REGIONS:
        train_region(spark, region, train_end, val_end, test_end)
    logger.info("All demand forecast models trained.")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    train_all_regions(spark)
