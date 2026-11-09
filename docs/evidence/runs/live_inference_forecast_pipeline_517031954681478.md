# Run evidence — live_inference / task `forecast_pipeline`

| field | value |
|---|---|
| job run id | `481037869311618` |
| task run id | `517031954681478` |
| job | [dev sourabh_ghose] [dev] 05 — Forecast Pipeline |
| notebook | `/Workspace/Users/sourabh.ghose@databricks.com/.bundle/energy-copilot/dev/files/pipelines/05_forecast_pipeline` |
| started | 2026-11-09 09:02:03 UTC |
| result | **SUCCESS** |
| run page | https://fevm-serverless-stable-2b9lj3.cloud.databricks.com/?o=7474644340067319#job/670211769373148/run/517031954681478 |

Exported from the Databricks Jobs API with cell outputs as they were produced during the run.

## Cell 2

```python
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import mlflow
import pandas as pd
from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../models/nem_forecast"))
from features import INTERVAL, REGIONS, load_wide, region_features  # models/nem_forecast/features.py

dbutils.widgets.text("catalog", "energy_copilot_catalog")
dbutils.widgets.text("horizons", "6,12,48")
dbutils.widgets.text("max_input_age_minutes", "30")

CATALOG = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"Invalid catalog name: {CATALOG!r}")
HORIZONS = tuple(int(h) for h in dbutils.widgets.get("horizons").split(","))
MAX_AGE = timedelta(minutes=int(dbutils.widgets.get("max_input_age_minutes")))
RUN_AT = datetime.now(timezone.utc).replace(tzinfo=None)

mlflow.set_registry_uri("databricks-uc")
# … 4 more lines
```

**Output:**

```text
/databricks/python/lib/python3.11/site-packages/mlflow/protos/service_pb2.py:11: UserWarning: google.protobuf.service module is deprecated. RPC implementations should provide code generator plugins which generate code specific to the RPC implementation. service.py will be removed in Jan 2025
  from google.protobuf import service as _service
```

## Cell 3

```python
def load_model(target: str, region: str):
    name = f"{CATALOG}.ml.nem_{target}_forecast_{region.lower()}"
    version = mlflow.MlflowClient().get_model_version_by_alias(name, "production").version
    return name, str(version), mlflow.pyfunc.load_model(f"models:/{name}@production")


def forecast_region(region: str) -> tuple[list[dict], list[dict]]:
    feats = region_features(wide, region).loc[[latest]]
    X = pd.concat([feats.assign(horizon=h) for h in HORIZONS], ignore_index=True)
    price_rows, demand_rows = [], []
    for target, sink in (("price", price_rows), ("demand", demand_rows)):
        name, version, model = load_model(target, region)
        out = model.predict(X)
        for h, (_, p) in zip(HORIZONS, out.iterrows()):
            base = {"forecast_run_at": RUN_AT, "interval_datetime": (latest + h * INTERVAL).to_pydatetime(),
                    "region_id": region, "horizon_intervals": h, "prediction_lower_80": float(p["lower_80"]),
                    "prediction_upper_80": float(p["upper_80"]), "model_version": version, "model_name": name,
                    "_updated_at": RUN_AT}
            if target == "price":
                spike = p["spike_probability"]
                sink.append({**base, "predicted_rrp": float(p["predicted"]),
                             "spike_probability": None if pd.isna(spike) else float(spike)})
            else:
                sink.append({**base, "predicted_demand_mw": float(p["predicted"])})
    return price_rows, demand_rows
# … 7 more lines
```

## Cell 4

```python
for rows, table in ((price_all, "price_forecasts"), (demand_all, "demand_forecasts")):
    target_schema = spark.table(f"{CATALOG}.gold.{table}").schema
    df = spark.createDataFrame(pd.DataFrame(rows).astype(object).where(pd.DataFrame(rows).notna(), None))
    df = df.select(*[F.col(c.name).cast(c.dataType).alias(c.name) for c in target_schema])
    df.write.mode("append").saveAsTable(f"{CATALOG}.gold.{table}")

summary = {
    "run_at_utc": str(RUN_AT), "issued_from_interval_utc": str(latest),
    "price_forecasts": len(price_all), "demand_forecasts": len(demand_all),
    "sample": [{k: (str(v) if isinstance(v, datetime) else v) for k, v in r.items()
                if k in ("region_id", "horizon_intervals", "interval_datetime", "predicted_rrp", "spike_probability",
                         "model_version")} for r in price_all[:: len(HORIZONS)]],
}
print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
```

**Output:**

```text
{"run_at_utc": "2026-11-09 09:02:25.534830", "issued_from_interval_utc": "2026-11-09 09:00:00", "price_forecasts": 15, "demand_forecasts": 15, "sample": [{"interval_datetime": "2026-11-09 09:30:00", "region_id": "NSW1", "horizon_intervals": 6, "model_version": "6", "predicted_rrp": 108.72689068444309, "spike_probability": 0.0}, {"interval_datetime": "2026-11-09 09:30:00", "region_id": "QLD1", "horizon_intervals": 6, "model_version": "6", "predicted_rrp": 104.80602291659464, "spike_probability": 0.0}, {"interval_datetime": "2026-11-09 09:30:00", "region_id": "VIC1", "horizon_intervals": 6, "model_version": "6", "predicted_rrp": 103.84407722753276, "spike_probability": 0.0}, {"interval_datetime": "2026-11-09 09:30:00", "region_id": "SA1", "horizon_intervals": 6, "model_version": "6", "predicted_rrp": 99.08496299780683, "spike_probability": 0.0}, {"interval_datetime": "2026-11-09 09:30:00", "region_id": "TAS1", "horizon_intervals": 6, "model_version": "6", "predicted_rrp": 98.06968820283996, "spike_probability": 0.0}]}
```
