# Run evidence — model_training / task `train`

| field | value |
|---|---|
| job run id | `940474602766977` |
| task run id | `243588330407321` |
| job | NEM forecast training (price, spike, demand) |
| notebook | `/Workspace/Users/sourabh.ghose@databricks.com/.bundle/energy-copilot/dev/files/models/nem_forecast/train` |
| started | 2026-11-09 04:41:39 UTC |
| result | **SUCCESS** |
| run page | https://fevm-serverless-stable-2b9lj3.cloud.databricks.com/?o=7474644340067319#job/1751095846419/run/243588330407321 |

Exported from the Databricks Jobs API with cell outputs as they were produced during the run.

## Cell 2

```python
# Dependencies (lightgbm==4.5.0, scikit-learn) come from the job environment spec in resources/jobs.yml.

import json
import re
from datetime import datetime, timedelta, timezone

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.metrics import average_precision_score, roc_auc_score

from features import REGIONS, feature_columns, load_wide, region_features, stack_horizons

dbutils.widgets.text("catalog", "energy_copilot_catalog")
dbutils.widgets.text("lookback_days", "90")
dbutils.widgets.text("test_days", "14")
dbutils.widgets.text("horizons", "6,12,48")
dbutils.widgets.text("spike_threshold", "300")
dbutils.widgets.text("experiment", "/Shared/energy_copilot_nem_forecast")

CATALOG = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"Invalid catalog name: {CATALOG!r}")
# … 13 more lines
```

**Output:**

```text
/databricks/python/lib/python3.11/site-packages/mlflow/protos/service_pb2.py:11: UserWarning: google.protobuf.service module is deprecated. RPC implementations should provide code generator plugins which generate code specific to the RPC implementation. service.py will be removed in Jan 2025
  from google.protobuf import service as _service
Data 2026-08-11 04:45:00 → 2026-11-09 04:40:00 UTC | test from 2026-10-26 04:40:00 | horizons (6, 12, 48)
```

## Cell 3

```python
BASE_FEATURE = {"price": "rrp_lag_0", "demand": "demand_lag_0"}  # latest observed value = persistence
MAPE_FLOORS = {"price": MAPE_FLOOR, "demand": 100.0}  # MW: SA1 demand goes near zero/negative under rooftop solar
SPIKE_CLASS_WEIGHT = 10.0
SPIKE_ALERT_BUDGET = 0.01  # flag the top 1% of intervals (~3 alerts per region per day)
VAL_DAYS = 14  # last 14 days of the training window: spike threshold + interval calibration (test stays untouched)


class PooledSpikeClassifier:
    """One spike classifier trained on all five regions (region as a feature) so it sees every spike in the window;
    the decision threshold is tuned for F1 on the pooled validation slice."""

    def __init__(self, classifier, features: list[str], threshold: float):
        self.classifier, self.features, self.threshold = classifier, features, threshold

    def proba(self, df: pd.DataFrame, region_code: int) -> np.ndarray:
        X = df.reindex(columns=self.features)
        X["region_code"] = region_code
        return self.classifier.predict_proba(X)[:, 1]


class NemForecaster(mlflow.pyfunc.PythonModel):
    """Predicts the change from the latest observed value (so persistence is the floor), plus (for price) the pooled
    spike classifier with its validation-tuned threshold, and 80% intervals from validation residuals."""

    def __init__(self, regressor, spike, features, residual_q, target, region_code):
# … 140 more lines
```

## Cell 4

```python
def register(info: dict, target: str, region: str, metric_rows: list[dict]) -> tuple[str, str, str]:
    name = f"{CATALOG}.ml.nem_{target}_forecast_{region.lower()}"
    with mlflow.start_run(run_name=f"{target}_{region}", nested=True) as run:
        mlflow.log_params({**info["params"], "target": target, "region": region, "horizons": str(HORIZONS),
                           "n_train": info["n_train"], "train_end": info["train_end"],
                           "test_start": str(TEST_START), "lookback_days": LOOKBACK})
        for r in metric_rows:
            for k, v in r.items():
                if isinstance(v, (int, float)) and v is not None and k not in ("horizon_intervals", "n_train"):
                    mlflow.log_metric(f"h{r['horizon_intervals']}_{k}", float(v))
        sample = info["sample"]
        signature = infer_signature(sample, info["model"].predict(None, sample))
        mlflow.pyfunc.log_model("model", python_model=info["model"], signature=signature,
                                input_example=sample.head(5), registered_model_name=name,
                                pip_requirements=["lightgbm==4.5.0", "pandas", "numpy", "scikit-learn"])
        run_id = run.info.run_id
    client = mlflow.MlflowClient()
    version = max(int(v.version) for v in client.search_model_versions(f"name='{name}'"))
    client.set_registered_model_alias(name, "production", str(version))
    return name, str(version), run_id


metric_rows_all, backtests, importances = [], [], []
with mlflow.start_run(run_name=f"nem_forecast_{TRAINED_AT:%Y%m%d_%H%M}") as parent:
    stacks = {region: stack_horizons(region_features(wide, region), wide, region, HORIZONS) for region in REGIONS}
# … 21 more lines
```

**Output:**

```text
Pooled spike classifier: {'spike_fit_events': 353, 'spike_val_events': 231, 'spike_alert_budget': 0.01, 'spike_scale_pos_weight': 10.0, 'spike_threshold': 1.0}
/databricks/python/lib/python3.11/site-packages/mlflow/types/utils.py:393: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
/databricks/python/lib/python3.11/site-packages/_distutils_hack/__init__.py:31: UserWarning: Setuptools is replacing distutils. Support for replacing an already imported distutils is deprecated. In the future, this condition will fail. Register concerns at https://github.com/pypa/setuptools/issues/new?template=distutils-deprecation.yml
  warnings.warn(
Registered model 'serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_nsw1' already exists. Creating a new version of this model...
Created version '6' of model 'serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_nsw1'.
NSW1 price: v6 | h6 MAE 10.8 (persistence 11.2), h12 MAE 12.8 (persistence 14.6), h48 MAE 18.0 (persistence 28.4)
/databricks/python/lib/python3.11/site-packages/mlflow/types/utils.py:393: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
Registered model 'serverless_stable_2b9lj3_catalog.ml.nem_demand_forecast_nsw1' already exists. Creating a new version of this model...
Created version '6' of model 'serverless_stable_2b9lj3_catalog.ml.nem_demand_forecast_nsw1'.
NSW1 demand: v6 | h6 MAE 132.6 (persistence 205.2), h12 MAE 204.5 (persistence 382.0), h48 MAE 493.2 (persistence 1236.1)
/databricks/python/lib/python3.11/site-packages/mlflow/types/utils.py:393: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
Registered model 'serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_qld1' already exists. Creating a new version of this model...
Created version '6' of model 'serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_qld1'.
QLD1 price: v6 | h6 MAE 9.6 (persistence 9.9), h12 MAE 12.0 (persistence 13.5), h48 MAE 19.8 (persistence 32.2)
/databricks/python/lib/python3.11/site-packages/mlflow/types/utils.py:393: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
Registered model 'serverless_stable_2b9lj3_catalog.ml.nem_demand_forecast_qld1' already exists. Creating a new version of this model...
Created version '6' of model 'serverless_stable_2b9lj3_catalog.ml.nem_demand_forecast_qld1'.
QLD1 demand: v6 | h6 MAE 102.4 (persistence 167.9), h12 MAE 168.3 (persistence 313.2), h48 MAE 433.1 (persistence 1010.1)
/databricks/python/lib/python3.11/site-packages/mlflow/types/utils.py:393: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
Registered model 'serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_vic1' already exists. Creating a new version of this model...
Created version '6' of model 'serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_vic1'.
VIC1 price: v6 | h6 MAE 12.9 (persistence 13.3), h12 MAE 16.1 (persistence 18.4), h48 MAE 26.2 (persistence 40.0)
/databricks/python/lib/python3.11/site-packages/mlflow/types/utils.py:393: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
Registered model 'serverless_stable_2b9lj3_catalog.ml.nem_demand_forecast_vic1' already exists. Creating a new version of this model...
Created version '6' of model 'serverless_stable_2b9lj3_catalog.ml.nem_demand_forecast_vic1'.
VIC1 demand: v6 | h6 MAE 106.1 (persistence 168.8), h12 MAE 166.8 (persistence 301.5), h48 MAE 404.5 (persistence 876.6)
/databricks/python/lib/python3.11/site-packages/mlflow/types/utils.py:393: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
Registered model 'serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_sa1' already exists. Creating a new version of this model...
Created version '6' of model 'serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_sa1'.
SA1 price: v6 | h6 MAE 32.4 (persistence 29.6), h12 MAE 38.1 (persistence 39.9), h48 MAE 52.1 (persistence 80.0)
/databricks/python/lib/python3.11/site-packages/mlflow/types/utils.py:393: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
Registered model 'serverless_stable_2b9lj3_catalog.ml.nem_demand_forecast_sa1' already exists. Creating a new version of this model...
Created version '6' of model 'serverless_stable_2b9lj3_catalog.ml.nem_demand_forecast_sa1'.
SA1 demand: v6 | h6 MAE 51.8 (persistence 76.8), h12 MAE 77.3 (persistence 133.9), h48 MAE 161.3 (persistence 391.7)
/databricks/python/lib/python3.11/site-packages/mlflow/types/utils.py:393: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
Registered model 'serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_tas1' already exists. Creating a new version of this model...
Created version '6' of model 'serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_tas1'.
TAS1 price: v6 | h6 MAE 17.1 (persistence 13.6), h12 MAE 20.1 (persistence 18.1), h48 MAE 31.7 (persistence 36.7)
/databricks/python/lib/python3.11/site-packages/mlflow/types/utils.py:393: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
Registered model 'serverless_stable_2b9lj3_catalog.ml.nem_demand_forecast_tas1' already exists. Creating a new version of this model...
Created version '6' of model 'serverless_stable_2b9lj3_catalog.ml.nem_demand_forecast_tas1'.
TAS1 demand: v6 | h6 MAE 20.0 (persistence 24.6), h12 MAE 27.9 (persistence 42.6), h48 MAE 55.2 (persistence 124.8)
```

## Cell 5

```python
stamp = {"trained_at": TRAINED_AT, "test_start": TEST_START.to_pydatetime(), "parent_run_id": parent_run_id}
metrics_df = pd.DataFrame(metric_rows_all).assign(**stamp)
backtest_df = pd.concat(backtests, ignore_index=True).assign(trained_at=TRAINED_AT)
importance_df = pd.DataFrame(importances).assign(trained_at=TRAINED_AT)
for df, table in ((metrics_df, "forecast_model_metrics"), (backtest_df, "forecast_backtest"),
                  (importance_df, "forecast_feature_importance")):
    (spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.gold.{table}"))
    print(f"wrote {len(df)} rows to gold.{table}")

headline = (metrics_df[metrics_df["target"] == "price"]
            .groupby("horizon_intervals")[["mae", "persistence_mae", "spike_recall", "spike_precision"]].mean())
summary = {"parent_run_id": parent_run_id, "data_start": str(wide.index.min()), "data_end": str(wide.index.max()),
           "test_start": str(TEST_START), "models_registered": int(metrics_df["model_name"].nunique()),
           "pooled_spike_classifier": spike_stats,
           "price_by_horizon": json.loads(headline.round(3).to_json(orient="index"))}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
```

**Output:**

```text
{"parent_run_id": "24865aa8c780473ebc17bdc4506c6891", "data_start": "2026-08-11 04:45:00", "data_end": "2026-11-09 04:40:00", "test_start": "2026-10-26 04:40:00", "models_registered": 10, "pooled_spike_classifier": {"spike_fit_events": 353, "spike_val_events": 231, "spike_alert_budget": 0.01, "spike_scale_pos_weight": 10.0, "spike_threshold": 1.0}, "price_by_horizon": {"6": {"mae": 16.526, "persistence_mae": 15.524, "spike_recall": 0.0, "spike_precision": 0.0}, "12": {"mae": 19.845, "persistence_mae": 20.903, "spike_recall": 0.0, "spike_precision": 0.0}, "48": {"mae": 29.544, "persistence_mae": 43.448, "spike_recall": 0.25, "spike_precision": 0.001}}}
```
