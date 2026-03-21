# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # AUS Energy Copilot — Batch Scoring Notebook
# MAGIC # Loads all 21 ML models from MLflow UC registry and runs batch inference
# MAGIC # on the latest Gold feature data. Results written to gold.nem_forecasts_batch.
# MAGIC # Schedule: Run daily at 02:00 AEST via Databricks Job (job_05_forecast_pipeline)

# COMMAND ----------

from datetime import datetime

import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = spark.conf.get("spark.energy_copilot.catalog", "energy_copilot")
REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
MODEL_TYPES = ["price_forecast", "demand_forecast", "wind_forecast", "solar_forecast"]
FORECAST_HORIZONS = [1, 2, 4, 6, 12, 24]  # intervals (5-min each)
NIGHT_HOURS = list(range(0, 6)) + list(range(20, 24))

# COMMAND ----------

def load_all_models():
    """Load all 20 regional forecast models + 1 anomaly model from MLflow UC registry."""
    client = MlflowClient()
    models = {}
    failed = []

    for mtype in MODEL_TYPES:
        for region in REGIONS:
            model_uri = f"models:/{CATALOG}.ml.{mtype}_{region}@production"
            try:
                model = mlflow.pyfunc.load_model(model_uri)
                version = client.get_model_version_by_alias(
                    f"{CATALOG}.ml.{mtype}_{region}", "production"
                ).version
                models[(mtype, region)] = {"model": model, "version": version}
                print(f"  Loaded {mtype}_{region} v{version}")
            except Exception as e:
                failed.append(f"{mtype}_{region}: {e}")
                print(f"  Missing {mtype}_{region}: {e}")

    # Anomaly model
    try:
        anom_uri = f"models:/{CATALOG}.ml.anomaly_detection_nem@production"
        models[("anomaly_detection", "NEM")] = {
            "model": mlflow.pyfunc.load_model(anom_uri), "version": "latest"
        }
        print("  Loaded anomaly_detection_nem")
    except Exception as e:
        failed.append(f"anomaly_detection_nem: {e}")

    print(f"\nLoaded {len(models)}/21 models. Failures: {len(failed)}")
    return models, failed

print("Loading ML models from MLflow UC registry...")
ALL_MODELS, LOAD_FAILURES = load_all_models()

# COMMAND ----------

def load_inference_features(region: str, lookback_intervals: int = 48) -> pd.DataFrame:
    """Load the latest N intervals of features from gold.feature_store_price."""
    df = spark.table(f"{CATALOG}.gold.feature_store_price") \
        .filter(F.col("regionid") == region) \
        .orderBy(F.col("settlementdate").desc()) \
        .limit(lookback_intervals) \
        .toPandas()
    df = df.sort_values("settlementdate").reset_index(drop=True)
    return df

print("Loading inference features for all regions...")
INFERENCE_FEATURES = {region: load_inference_features(region) for region in REGIONS}

# COMMAND ----------

def clamp_prediction(predicted: float, model_type: str, hour: int) -> float:
    """Apply physical bounds to model predictions."""
    if model_type == "solar_forecast" and hour in NIGHT_HOURS:
        return 0.0
    if model_type == "demand_forecast":
        return max(0.0, min(50_000.0, predicted))
    if model_type == "price_forecast":
        return max(-1_000.0, min(17_000.0, predicted))
    return max(0.0, predicted)  # generation models: non-negative

def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + np.exp(-x))

results = []
run_ts = datetime.utcnow().isoformat() + "Z"

for (mtype, region), info in ALL_MODELS.items():
    if region == "NEM":
        continue  # anomaly model handled separately

    features_df = INFERENCE_FEATURES.get(region)
    if features_df is None or features_df.empty:
        print(f"  No features for {mtype}/{region}")
        continue

    for horizon in FORECAST_HORIZONS:
        try:
            feat_row = features_df.iloc[[-1]].copy()
            feat_row["forecast_horizon"] = horizon
            raw_pred = float(info["model"].predict(feat_row).iloc[0])
            hour = pd.to_datetime(features_df["settlementdate"].iloc[-1]).hour
            pred = clamp_prediction(raw_pred, mtype, hour)

            spike_prob = sigmoid(pred / 300 - 1) if mtype == "price_forecast" else None
            confidence = 1.0 / (1.0 + horizon / 12.0)

            results.append({
                "region": region,
                "model_type": mtype,
                "model_version": info["version"],
                "generated_at": run_ts,
                "horizon_intervals": horizon,
                "horizon_minutes": horizon * 5,
                "predicted_value": pred,
                "lower_bound": pred * 0.85,
                "upper_bound": pred * 1.15,
                "spike_probability": spike_prob,
                "confidence_score": confidence,
            })
        except Exception as e:
            print(f"  Inference failed for {mtype}/{region} h={horizon}: {e}")

print(f"Generated {len(results)} forecast records")

# COMMAND ----------

if results:
    schema = T.StructType([
        T.StructField("region", T.StringType()),
        T.StructField("model_type", T.StringType()),
        T.StructField("model_version", T.StringType()),
        T.StructField("generated_at", T.StringType()),
        T.StructField("horizon_intervals", T.IntegerType()),
        T.StructField("horizon_minutes", T.IntegerType()),
        T.StructField("predicted_value", T.DoubleType()),
        T.StructField("lower_bound", T.DoubleType()),
        T.StructField("upper_bound", T.DoubleType()),
        T.StructField("spike_probability", T.DoubleType()),
        T.StructField("confidence_score", T.DoubleType()),
    ])

    results_df = spark.createDataFrame(results, schema=schema)

    results_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{CATALOG}.gold.nem_forecasts_batch")

    print(f"Written {results_df.count()} records to {CATALOG}.gold.nem_forecasts_batch")
else:
    print("No results to write")

# COMMAND ----------

# Summary
print("=" * 60)
print("BATCH SCORING COMPLETE")
print("=" * 60)
print(f"Models loaded: {len(ALL_MODELS)}/21")
print(f"Records written: {len(results)}")
print(f"Run timestamp: {run_ts}")
if LOAD_FAILURES:
    print(f"Failed models ({len(LOAD_FAILURES)}):")
    for f in LOAD_FAILURES:
        print(f"  - {f}")
