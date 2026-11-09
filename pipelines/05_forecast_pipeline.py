# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 05 — NEM price, spike and demand forecasts (every 5 minutes)
# MAGIC
# MAGIC Loads the `production` alias of each Unity Catalog model trained by `models/nem_forecast/train.py`
# MAGIC (`{catalog}.ml.nem_price_forecast_{region}`, `{catalog}.ml.nem_demand_forecast_{region}`), builds the
# MAGIC latest features from real `gold.nem_prices_5min` data, and appends forecasts to `gold.price_forecasts`
# MAGIC and `gold.demand_forecasts`.
# MAGIC
# MAGIC There is **no synthetic fallback**. If a model is missing or the input data is stale, the job fails so
# MAGIC the problem is visible, rather than writing made-up forecasts.

# COMMAND ----------

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import mlflow
import pandas as pd
from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../models/nem_forecast"))
from features import (  # models/nem_forecast/features.py
    INTERVAL,
    REGIONS,
    load_wide,
    region_features,
)

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
wide = load_wide(spark, CATALOG, (RUN_AT - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S"))
latest = wide.dropna(how="all").index.max()
if RUN_AT - latest.to_pydatetime() > MAX_AGE:
    raise RuntimeError(f"Input data is stale: latest interval {latest} UTC, run at {RUN_AT} UTC")

# COMMAND ----------


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


price_all, demand_all = [], []
for region in REGIONS:
    p_rows, d_rows = forecast_region(region)
    price_all.extend(p_rows)
    demand_all.extend(d_rows)

# COMMAND ----------

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
