# Databricks notebook source
# MAGIC %md
# MAGIC # E15: Create Constraint Forecast Tables
# MAGIC Creates Delta table for ML-based constraint binding predictions.

# COMMAND ----------

catalog = "energy_copilot_catalog"
sp_id = "67aaaa6b-778c-4c8b-b2f0-9f9b9728b3bb"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS energy_copilot_catalog.gold.constraint_forecasts (
# MAGIC   forecast_id STRING,
# MAGIC   constraint_id STRING,
# MAGIC   constraint_type STRING,
# MAGIC   region STRING,
# MAGIC   forecast_datetime TIMESTAMP,
# MAGIC   target_datetime TIMESTAMP,
# MAGIC   horizon_hours INT,
# MAGIC   binding_probability DOUBLE,
# MAGIC   expected_marginal_value DOUBLE,
# MAGIC   price_impact_estimate DOUBLE,
# MAGIC   confidence_level STRING,
# MAGIC   features_json STRING,
# MAGIC   model_version STRING,
# MAGIC   created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );

# COMMAND ----------

spark.sql(f"GRANT MODIFY ON TABLE {catalog}.gold.constraint_forecasts TO `{sp_id}`")
spark.sql(f"GRANT SELECT ON TABLE {catalog}.gold.constraint_forecasts TO `{sp_id}`")
print("Grants applied to constraint_forecasts table")
