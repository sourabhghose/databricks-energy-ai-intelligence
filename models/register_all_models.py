"""
Bulk Model Registration -- MLflow Unity Catalog
================================================
Registers all Energy Copilot ML models to the Unity Catalog model registry:

  - 20 regional forecast models: 5 regions x 4 model types
      price_forecast_<region>    (NSW1, QLD1, SA1, TAS1, VIC1)
      demand_forecast_<region>
      wind_forecast_<region>
      solar_forecast_<region>
  - 1 anomaly detection model:
      anomaly_detection_nem

For each model the script:
  1. Searches the MLflow experiment for the latest completed "final_model" run
  2. Creates the registered model in energy_copilot.ml.<model_name> if it
     does not already exist (handles 404 / ResourceDoesNotExist gracefully)
  3. Creates a new model version pointing at the run's model artifact
  4. Sets the specified alias (default: "production") on the new version
  5. Prints a formatted summary table

Usage
-----
  export DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
  export DATABRICKS_TOKEN=dapi...

  # Register all models with alias "production"
  python models/register_all_models.py --experiment-prefix energy_copilot

  # Register all models with a custom alias
  python models/register_all_models.py --experiment-prefix energy_copilot --set-alias champion

  # Dry-run (print what would be registered without actually doing it)
  python models/register_all_models.py --experiment-prefix energy_copilot --dry-run

Requirements
------------
  DATABRICKS_HOST  -- Databricks workspace URL (e.g. https://adb-xxxx.azuredatabricks.net)
  DATABRICKS_TOKEN -- Databricks personal access token or service principal token
  mlflow >= 2.11   -- for Unity Catalog set_registered_model_alias support
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

import mlflow
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG    = "energy_copilot"
ML_SCHEMA  = f"{CATALOG}.ml"

NEM_REGIONS: List[str] = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

# Map of model_type -> experiment suffix and model name template
MODEL_TYPES = {
    "price":   {
        "experiment_suffix": "price_forecast_training",
        "model_tmpl":        "price_forecast_{region}",
    },
    "demand":  {
        "experiment_suffix": "demand_forecast_training",
        "model_tmpl":        "demand_forecast_{region}",
    },
    "wind":    {
        "experiment_suffix": "wind_forecast_training",
        "model_tmpl":        "wind_forecast_{region}",
    },
    "solar":   {
        "experiment_suffix": "solar_forecast_training",
        "model_tmpl":        "solar_forecast_{region}",
    },
}

ANOMALY_EXPERIMENT_SUFFIX = "anomaly_detection_training"
ANOMALY_MODEL_NAME        = "anomaly_detection_nem"

# Artifact sub-path where the model is logged inside a run
MODEL_ARTIFACT_PATH = "model"

# Primary MAE metric key used across all experiments (logged as test_mae_all
# for forecast models; anomaly_rate_pct for anomaly model)
METRIC_KEY_FORECAST = "test_mae_all"
METRIC_KEY_ANOMALY  = "anomaly_rate_pct"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class RegistrationResult:
    """Result of a single model registration attempt."""
    model_name:  str
    run_id:      str
    metric_key:  str
    metric_mae:  Optional[float]
    alias_set:   str
    status:      str     # "ok" | "skipped" | "error"
    message:     str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_env_vars() -> None:
    """Raise EnvironmentError if the required Databricks env vars are absent."""
    missing = [v for v in ("DATABRICKS_HOST", "DATABRICKS_TOKEN") if not os.getenv(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Set them before running this script:\n"
            "  export DATABRICKS_HOST=https://<workspace>.azuredatabricks.net\n"
            "  export DATABRICKS_TOKEN=dapi..."
        )


def _configure_mlflow() -> None:
    """Point MLflow at the Databricks workspace specified by env vars."""
    host  = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    mlflow.set_tracking_uri("databricks")
    os.environ["MLFLOW_TRACKING_URI"] = "databricks"
    logger.info("MLflow tracking URI set to Databricks workspace: %s", host)


def _get_latest_final_run(
    client: MlflowClient,
    experiment_path: str,
) -> Optional[mlflow.entities.Run]:
    """Return the most recent run tagged with phase=final_training in the
    given experiment.  Returns None if no matching run is found.
    """
    try:
        experiment = client.get_experiment_by_name(experiment_path)
    except MlflowException:
        logger.warning("Experiment not found: %s", experiment_path)
        return None

    if experiment is None:
        logger.warning("Experiment not found: %s", experiment_path)
        return None

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string="tags.phase = 'final_training'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    if not runs:
        logger.warning("No final_training runs in experiment: %s", experiment_path)
        return None

    return runs[0]


def _ensure_registered_model(client: MlflowClient, uc_model_name: str) -> None:
    """Create the registered model if it does not already exist.

    Handles mlflow.exceptions.MlflowException with RESOURCE_DOES_NOT_EXIST
    (404 equivalent) by creating the model rather than re-raising.
    """
    try:
        client.get_registered_model(uc_model_name)
        logger.debug("Registered model already exists: %s", uc_model_name)
    except MlflowException as exc:
        if "RESOURCE_DOES_NOT_EXIST" in str(exc) or exc.error_code == "RESOURCE_DOES_NOT_EXIST":
            logger.info("Creating registered model: %s", uc_model_name)
            client.create_registered_model(
                uc_model_name,
                description=(
                    f"Energy Copilot ML model — {uc_model_name}. "
                    "Auto-registered by models/register_all_models.py."
                ),
            )
        else:
            raise


def _register_model_version(
    client:          MlflowClient,
    uc_model_name:   str,
    run_id:          str,
    alias:           str,
    metric_key:      str,
    dry_run:         bool,
) -> RegistrationResult:
    """Register a new model version from a run and set the alias.

    Parameters
    ----------
    client         : MlflowClient configured for Databricks.
    uc_model_name  : Fully qualified UC name, e.g. energy_copilot.ml.price_forecast_nsw
    run_id         : MLflow run ID containing the model artifact.
    alias          : Alias to assign (e.g. "production" or "champion").
    metric_key     : Name of the primary metric to surface in the summary table.
    dry_run        : If True, skip actual API calls; just print what would happen.

    Returns
    -------
    RegistrationResult
    """
    # Fetch the metric value for the summary table
    try:
        run = client.get_run(run_id)
        metric_val = run.data.metrics.get(metric_key)
    except MlflowException as exc:
        logger.warning("Could not fetch run %s: %s", run_id, exc)
        metric_val = None

    if dry_run:
        logger.info("[DRY-RUN] Would register %s from run %s with alias '%s'",
                    uc_model_name, run_id, alias)
        return RegistrationResult(
            model_name=uc_model_name,
            run_id=run_id,
            metric_key=metric_key,
            metric_mae=metric_val,
            alias_set=alias,
            status="skipped",
            message="dry-run",
        )

    try:
        _ensure_registered_model(client, uc_model_name)

        # Construct the artifact URI pointing to the model subdirectory
        artifact_uri = f"runs:/{run_id}/{MODEL_ARTIFACT_PATH}"

        mv = client.create_model_version(
            name=uc_model_name,
            source=artifact_uri,
            run_id=run_id,
        )
        version_str = str(mv.version)
        logger.info("Created model version %s for %s", version_str, uc_model_name)

        client.set_registered_model_alias(uc_model_name, alias, version_str)
        logger.info("Set alias '%s' on %s v%s", alias, uc_model_name, version_str)

        return RegistrationResult(
            model_name=uc_model_name,
            run_id=run_id,
            metric_key=metric_key,
            metric_mae=metric_val,
            alias_set=alias,
            status="ok",
        )

    except MlflowException as exc:
        logger.error("Failed to register %s: %s", uc_model_name, exc)
        return RegistrationResult(
            model_name=uc_model_name,
            run_id=run_id,
            metric_key=metric_key,
            metric_mae=metric_val,
            alias_set=alias,
            status="error",
            message=str(exc),
        )


# ---------------------------------------------------------------------------
# Main registration logic
# ---------------------------------------------------------------------------

def register_all_models(
    experiment_prefix: str,
    alias:             str,
    dry_run:           bool,
) -> List[RegistrationResult]:
    """Register all 21 Energy Copilot ML models to MLflow Unity Catalog.

    Scans each experiment for the latest final_training run, registers the
    model version to energy_copilot.ml.<model_name>, and sets the alias.

    Parameters
    ----------
    experiment_prefix : str
        Prefix used for all MLflow experiment paths, e.g. "energy_copilot".
        Experiments are expected at /<prefix>/<experiment_suffix>.
    alias : str
        Alias to assign to each new model version.
    dry_run : bool
        If True, skip all write operations and just print what would happen.

    Returns
    -------
    List[RegistrationResult]
        One entry per model, regardless of success or failure.
    """
    client  = MlflowClient()
    results: List[RegistrationResult] = []

    # ------------------------------------------------------------------
    # 20 regional forecast models (5 regions x 4 types)
    # ------------------------------------------------------------------
    for model_type, cfg in MODEL_TYPES.items():
        experiment_path = f"/{experiment_prefix}/{cfg['experiment_suffix']}"

        for region in NEM_REGIONS:
            region_slug  = region.lower().replace("1", "")
            model_name   = cfg["model_tmpl"].format(region=region_slug)
            uc_name      = f"{ML_SCHEMA}.{model_name}"

            logger.info("Processing %s ...", uc_name)

            run = _get_latest_final_run(client, experiment_path)
            if run is None:
                results.append(RegistrationResult(
                    model_name=uc_name,
                    run_id="N/A",
                    metric_key=METRIC_KEY_FORECAST,
                    metric_mae=None,
                    alias_set=alias,
                    status="error",
                    message=f"No final_training run found in {experiment_path}",
                ))
                continue

            # Filter to the run for this specific region using tags
            region_runs = client.search_runs(
                experiment_ids=[
                    client.get_experiment_by_name(experiment_path).experiment_id
                ],
                filter_string=f"tags.phase = 'final_training' AND tags.region = '{region}'",
                order_by=["start_time DESC"],
                max_results=1,
            )
            if region_runs:
                run = region_runs[0]
            else:
                logger.warning(
                    "No region-specific run found for %s in %s; using latest final_training run.",
                    region, experiment_path,
                )

            result = _register_model_version(
                client=client,
                uc_model_name=uc_name,
                run_id=run.info.run_id,
                alias=alias,
                metric_key=METRIC_KEY_FORECAST,
                dry_run=dry_run,
            )
            results.append(result)

    # ------------------------------------------------------------------
    # Anomaly detection model (1 model, NEM-wide)
    # ------------------------------------------------------------------
    anomaly_experiment = f"/{experiment_prefix}/{ANOMALY_EXPERIMENT_SUFFIX}"
    anomaly_uc_name    = f"{ML_SCHEMA}.{ANOMALY_MODEL_NAME}"

    logger.info("Processing %s ...", anomaly_uc_name)
    anomaly_run = _get_latest_final_run(client, anomaly_experiment)

    # Anomaly train.py uses run_name="anomaly_detector_training", not "final_model"
    # Fall back to the latest run in the experiment if no final_training tag
    if anomaly_run is None:
        try:
            exp = client.get_experiment_by_name(anomaly_experiment)
            if exp:
                all_runs = client.search_runs(
                    experiment_ids=[exp.experiment_id],
                    order_by=["start_time DESC"],
                    max_results=1,
                )
                anomaly_run = all_runs[0] if all_runs else None
        except MlflowException:
            anomaly_run = None

    if anomaly_run is None:
        results.append(RegistrationResult(
            model_name=anomaly_uc_name,
            run_id="N/A",
            metric_key=METRIC_KEY_ANOMALY,
            metric_mae=None,
            alias_set=alias,
            status="error",
            message=f"No runs found in {anomaly_experiment}",
        ))
    else:
        result = _register_model_version(
            client=client,
            uc_model_name=anomaly_uc_name,
            run_id=anomaly_run.info.run_id,
            alias=alias,
            metric_key=METRIC_KEY_ANOMALY,
            dry_run=dry_run,
        )
        results.append(result)

    return results


# ---------------------------------------------------------------------------
# Summary table printer
# ---------------------------------------------------------------------------

def print_summary_table(results: List[RegistrationResult]) -> None:
    """Print a formatted summary table to stdout.

    Columns: model_name | run_id (truncated) | metric_mae | alias_set | status
    """
    col_widths = {
        "model_name": max(len("model_name"), max(len(r.model_name) for r in results)),
        "run_id":     12,
        "metric":     12,
        "alias":      12,
        "status":     8,
    }

    header = (
        f"{'model_name':<{col_widths['model_name']}}  "
        f"{'run_id':<{col_widths['run_id']}}  "
        f"{'metric_value':<{col_widths['metric']}}  "
        f"{'alias_set':<{col_widths['alias']}}  "
        f"{'status':<{col_widths['status']}}"
    )
    divider = "-" * len(header)

    print("\n" + divider)
    print(header)
    print(divider)

    ok_count    = 0
    error_count = 0
    skip_count  = 0

    for r in results:
        run_short    = r.run_id[:12] if r.run_id != "N/A" else "N/A"
        metric_str   = f"{r.metric_mae:.4f}" if r.metric_mae is not None else "N/A"
        status_label = r.status.upper()

        print(
            f"{r.model_name:<{col_widths['model_name']}}  "
            f"{run_short:<{col_widths['run_id']}}  "
            f"{metric_str:<{col_widths['metric']}}  "
            f"{r.alias_set:<{col_widths['alias']}}  "
            f"{status_label:<{col_widths['status']}}"
            + (f"  [{r.message}]" if r.message else "")
        )

        if r.status == "ok":      ok_count    += 1
        elif r.status == "error": error_count += 1
        else:                     skip_count  += 1

    print(divider)
    print(
        f"Total: {len(results)} models | "
        f"OK: {ok_count} | "
        f"Errors: {error_count} | "
        f"Skipped/dry-run: {skip_count}"
    )
    print(divider + "\n")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk-register all Energy Copilot ML models to MLflow Unity Catalog.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--experiment-prefix",
        default="energy_copilot",
        help=(
            "Prefix for MLflow experiment paths, e.g. 'energy_copilot'. "
            "Experiments are looked up as /<prefix>/<experiment_suffix>. "
            "Default: energy_copilot"
        ),
    )
    parser.add_argument(
        "--set-alias",
        default="production",
        dest="alias",
        help=(
            "Alias to assign to each newly registered model version. "
            "Use 'champion' to stage for review before promoting to 'production'. "
            "Default: production"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print what would be registered without making any API calls.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity. Default: INFO",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point.  Returns exit code (0 = success, 1 = any errors)."""
    args = _parse_args(argv)
    logging.getLogger().setLevel(args.log_level)

    _require_env_vars()
    _configure_mlflow()

    logger.info(
        "Starting bulk registration: prefix=%s, alias=%s, dry_run=%s",
        args.experiment_prefix, args.alias, args.dry_run,
    )

    results = register_all_models(
        experiment_prefix=args.experiment_prefix,
        alias=args.alias,
        dry_run=args.dry_run,
    )

    print_summary_table(results)

    error_count = sum(1 for r in results if r.status == "error")
    if error_count > 0:
        logger.warning("%d model(s) failed to register. See table above.", error_count)
        return 1

    logger.info("All models registered successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
