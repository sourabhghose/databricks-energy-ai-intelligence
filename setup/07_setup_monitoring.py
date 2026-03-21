# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 07 — Lakehouse Monitoring for ML Models
# MAGIC
# MAGIC Sets up Databricks Lakehouse Monitoring (via `databricks-sdk`) on every gold
# MAGIC forecast table and the daily market summary table.
# MAGIC
# MAGIC **Monitors created:**
# MAGIC - `energy_copilot.gold.price_forecasts`   → REGRESSION monitor
# MAGIC - `energy_copilot.gold.demand_forecasts`  → REGRESSION monitor
# MAGIC - `energy_copilot.gold.wind_forecasts`    → REGRESSION monitor
# MAGIC - `energy_copilot.gold.solar_forecasts`   → REGRESSION monitor
# MAGIC - `energy_copilot.gold.daily_market_summary` → CUSTOM monitor
# MAGIC
# MAGIC Run order: after 00–06 setup notebooks.
# MAGIC Re-running is safe — `ResourceAlreadyExists` is handled gracefully.

# COMMAND ----------

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.catalog import (
    MonitorCronSchedule,
    MonitorCustomMetric,
    MonitorCustomMetricType,
    MonitorInferenceLog,
    MonitorInferenceLogProblemType,
    MonitorSnapshot,
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------


class _JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    _SKIP = frozenset(
        {
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "name",
            "taskName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        obj: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if record.exc_info:
            obj["exception"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k not in self._SKIP:
                obj[k] = v
        return json.dumps(obj, default=str)


def _configure_logging(level: str = "INFO") -> logging.Logger:
    """Configure and return a structured-JSON logger."""
    lg = logging.getLogger("energy_copilot.monitoring_setup")
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(_JsonFormatter())
        lg.addHandler(h)
    lg.setLevel(getattr(logging, level.upper(), logging.INFO))
    lg.propagate = False
    return lg


logger = _configure_logging()

# COMMAND ----------

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CATALOG: str = os.environ.get("DATABRICKS_CATALOG", "energy_copilot")

# Daily schedule: refresh monitors at 07:00 UTC (17:00 AEST) each day
DAILY_SCHEDULE = MonitorCronSchedule(quartz_cron_expression="0 0 7 * * ?", timezone_id="UTC")

# Baseline tables hold test-set predictions produced at training time.
# Schema mirrors the forecast gold tables, with an additional `split` column.
BASELINE_TABLE_TPL = "{catalog}.ml.{model_type}_forecast_baseline"

# Output profile tables written by the monitor
PROFILE_TABLE_TPL = "{catalog}.ml.{model_type}_monitor_profile"


# COMMAND ----------

# ---------------------------------------------------------------------------
# Forecast model descriptor
# ---------------------------------------------------------------------------


@dataclass
class ForecastMonitorConfig:
    """Configuration for one regression forecast monitor."""

    model_type: str                  # price | demand | wind | solar
    gold_table: str                  # fully-qualified gold forecast table
    timestamp_col: str               # column holding the prediction timestamp
    prediction_col: str              # column holding the predicted value
    label_col: str                   # column holding the actuals (may be NULL until actuals land)
    model_id_col: str                # column holding the model run/version identifier
    slicing_exprs: List[str]         # expressions used for sliced drift metrics


FORECAST_MONITORS: List[ForecastMonitorConfig] = [
    ForecastMonitorConfig(
        model_type="price",
        gold_table=f"{CATALOG}.gold.price_forecasts",
        timestamp_col="forecast_time",
        prediction_col="predicted_rrp",
        label_col="actual_rrp",
        model_id_col="model_id",
        slicing_exprs=["region_id", "horizon_hours"],
    ),
    ForecastMonitorConfig(
        model_type="demand",
        gold_table=f"{CATALOG}.gold.demand_forecasts",
        timestamp_col="forecast_time",
        prediction_col="predicted_demand_mw",
        label_col="actual_demand_mw",
        model_id_col="model_id",
        slicing_exprs=["region_id", "horizon_hours"],
    ),
    ForecastMonitorConfig(
        model_type="wind",
        gold_table=f"{CATALOG}.gold.wind_forecasts",
        timestamp_col="forecast_time",
        prediction_col="predicted_wind_mw",
        label_col="actual_wind_mw",
        model_id_col="model_id",
        slicing_exprs=["region_id", "horizon_hours"],
    ),
    ForecastMonitorConfig(
        model_type="solar",
        gold_table=f"{CATALOG}.gold.solar_forecasts",
        timestamp_col="forecast_time",
        prediction_col="predicted_solar_mw",
        label_col="actual_solar_mw",
        model_id_col="model_id",
        slicing_exprs=["region_id", "horizon_hours"],
    ),
]

# COMMAND ----------

# ---------------------------------------------------------------------------
# Custom metrics for drift thresholds
# ---------------------------------------------------------------------------
# Jensen-Shannon divergence > 0.1 on predicted_value distribution (per region/horizon)
# MAE increase > 25% vs baseline
# These are expressed as SQL expressions evaluated within the monitor profile run.
# ---------------------------------------------------------------------------


def _build_regression_custom_metrics(
    prediction_col: str,
    label_col: str,
) -> List[MonitorCustomMetric]:
    """
    Return custom metric definitions for a regression forecast monitor.

    Metrics:
    - ``js_divergence_alert``: fires when Jensen-Shannon divergence on the
      predicted value distribution exceeds 0.1 relative to the baseline window.
    - ``mae_relative_increase``: fractional increase in MAE versus the baseline
      window; values > 0.25 indicate a 25%+ regression.

    Args:
        prediction_col: Name of the column holding model predictions.
        label_col:      Name of the column holding ground-truth actuals.

    Returns:
        List of ``MonitorCustomMetric`` objects ready to pass to
        ``w.quality_monitors.create()``.
    """
    return [
        MonitorCustomMetric(
            name="js_divergence_alert",
            type=MonitorCustomMetricType.AGGREGATE,
            # JS divergence proxy: symmetric KL using log-ratio of density estimates.
            # Lakehouse Monitoring evaluates this SQL in the context of the profile run.
            definition=(
                f"CASE WHEN avg(ln(({prediction_col} + 1e-9) / (baseline_{prediction_col} + 1e-9))) "
                f"+ avg(ln((baseline_{prediction_col} + 1e-9) / ({prediction_col} + 1e-9))) > 0.2 "
                f"THEN 1 ELSE 0 END"
            ),
            input_columns=[prediction_col],
            output_data_type="INT",
        ),
        MonitorCustomMetric(
            name="mae_relative_increase",
            type=MonitorCustomMetricType.AGGREGATE,
            # Fractional increase in MAE vs baseline MAE stored in profile table.
            # Alert fires when this value exceeds 0.25 (i.e. > 25% increase).
            definition=(
                f"(avg(abs({prediction_col} - {label_col})) - baseline_mae) "
                f"/ NULLIF(baseline_mae, 0)"
            ),
            input_columns=[prediction_col, label_col],
            output_data_type="DOUBLE",
        ),
    ]


# COMMAND ----------

# ---------------------------------------------------------------------------
# Custom metrics for daily_market_summary (CUSTOM problem type)
# ---------------------------------------------------------------------------


def _build_summary_custom_metrics() -> List[MonitorCustomMetric]:
    """
    Return custom metric definitions for the daily market summary monitor.

    Metrics:
    - ``generation_success_rate``: fraction of rows where generation_succeeded=true;
      alert fires when this drops below 0.95.
    - ``avg_word_count``: mean word count per summary; healthy range is 400-600 words.

    Returns:
        List of ``MonitorCustomMetric`` objects.
    """
    return [
        MonitorCustomMetric(
            name="generation_success_rate",
            type=MonitorCustomMetricType.AGGREGATE,
            definition="avg(CAST(generation_succeeded AS INT))",
            input_columns=["generation_succeeded"],
            output_data_type="DOUBLE",
        ),
        MonitorCustomMetric(
            name="avg_word_count",
            type=MonitorCustomMetricType.AGGREGATE,
            definition="avg(word_count)",
            input_columns=["word_count"],
            output_data_type="DOUBLE",
        ),
    ]


# COMMAND ----------

# ---------------------------------------------------------------------------
# Helper — create or skip a monitor
# ---------------------------------------------------------------------------


def _create_or_skip(
    w: WorkspaceClient,
    table_name: str,
    create_kwargs: Dict[str, Any],
) -> None:
    """
    Attempt to create a Lakehouse Monitor on *table_name*.

    If the monitor already exists (``ResourceAlreadyExists``), log a warning
    and continue rather than raising an exception so this notebook is safely
    idempotent.

    Args:
        w:             Authenticated ``WorkspaceClient`` instance.
        table_name:    Fully-qualified Delta table name to monitor.
        create_kwargs: Keyword arguments forwarded to
                       ``w.quality_monitors.create()``.
    """
    try:
        result = w.quality_monitors.create(table_name=table_name, **create_kwargs)
        logger.info(
            "monitor_created",
            extra={"table": table_name, "monitor_name": str(result)},
        )
    except ResourceAlreadyExists:
        logger.warning(
            "monitor_already_exists",
            extra={"table": table_name, "action": "skipped"},
        )
    except Exception:
        logger.exception("monitor_create_failed", extra={"table": table_name})
        raise


# COMMAND ----------

# ---------------------------------------------------------------------------
# Main setup logic
# ---------------------------------------------------------------------------


def setup_forecast_monitors(w: WorkspaceClient) -> None:
    """
    Create REGRESSION Lakehouse Monitors for all four forecast model types.

    Each monitor is configured with:
    - Daily refresh schedule (07:00 UTC)
    - Baseline table pointing to test-set predictions stored at training time
    - Output profile written to ``energy_copilot.ml.<model_type>_monitor_profile``
    - Slicing by ``region_id`` and ``horizon_hours``
    - Custom drift metrics (JS divergence + MAE increase)

    Args:
        w: Authenticated ``WorkspaceClient`` instance.
    """
    for cfg in FORECAST_MONITORS:
        baseline_table = BASELINE_TABLE_TPL.format(
            catalog=CATALOG, model_type=cfg.model_type
        )
        output_schema = f"{CATALOG}.ml"

        logger.info(
            "setting_up_forecast_monitor",
            extra={
                "model_type": cfg.model_type,
                "table": cfg.gold_table,
                "baseline": baseline_table,
                "output_schema": output_schema,
            },
        )

        custom_metrics = _build_regression_custom_metrics(
            prediction_col=cfg.prediction_col,
            label_col=cfg.label_col,
        )

        _create_or_skip(
            w=w,
            table_name=cfg.gold_table,
            create_kwargs=dict(
                assets_dir=f"/Shared/energy_copilot/monitors/{cfg.model_type}",
                output_schema_name=output_schema,
                inference_log=MonitorInferenceLog(
                    problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_REGRESSION,
                    prediction_col=cfg.prediction_col,
                    label_col=cfg.label_col,
                    timestamp_col=cfg.timestamp_col,
                    model_id_col=cfg.model_id_col,
                    granularities=["1 day", "1 week"],
                ),
                baseline_table_name=baseline_table,
                slicing_exprs=cfg.slicing_exprs,
                custom_metrics=custom_metrics,
                schedule=DAILY_SCHEDULE,
            ),
        )

    logger.info(
        "forecast_monitors_setup_complete",
        extra={"count": len(FORECAST_MONITORS)},
    )


def setup_market_summary_monitor(w: WorkspaceClient) -> None:
    """
    Create a CUSTOM Lakehouse Monitor on ``energy_copilot.gold.daily_market_summary``.

    Tracks:
    - ``generation_success_rate``: should remain > 0.95
    - ``avg_word_count``: healthy range 400–600 words

    Args:
        w: Authenticated ``WorkspaceClient`` instance.
    """
    table_name = f"{CATALOG}.gold.daily_market_summary"
    output_schema = f"{CATALOG}.ml"

    logger.info(
        "setting_up_summary_monitor",
        extra={"table": table_name, "output_schema": output_schema},
    )

    custom_metrics = _build_summary_custom_metrics()

    _create_or_skip(
        w=w,
        table_name=table_name,
        create_kwargs=dict(
            assets_dir="/Shared/energy_copilot/monitors/market_summary",
            output_schema_name=output_schema,
            snapshot=MonitorSnapshot(),
            slicing_exprs=["CAST(summary_date AS STRING)"],
            custom_metrics=custom_metrics,
            schedule=DAILY_SCHEDULE,
        ),
    )

    logger.info("summary_monitor_setup_complete", extra={"table": table_name})


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run setup

# COMMAND ----------

w = WorkspaceClient()

logger.info(
    "starting_monitoring_setup",
    extra={"catalog": CATALOG, "workspace_host": w.config.host},
)

setup_forecast_monitors(w)
setup_market_summary_monitor(w)

logger.info(
    "all_monitors_configured",
    extra={
        "monitors": [cfg.gold_table for cfg in FORECAST_MONITORS]
        + [f"{CATALOG}.gold.daily_market_summary"]
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify monitors exist

# COMMAND ----------

all_tables = [cfg.gold_table for cfg in FORECAST_MONITORS] + [
    f"{CATALOG}.gold.daily_market_summary"
]

print("Monitor status summary")
print("=" * 60)
for tbl in all_tables:
    try:
        info = w.quality_monitors.get(table_name=tbl)
        status = getattr(info, "status", "UNKNOWN")
        print(f"  OK  {tbl}  [{status}]")
    except Exception as exc:
        print(f"  ERR {tbl}  — {exc}")
print("=" * 60)
print("Setup complete. Monitors refresh daily at 07:00 UTC.")
