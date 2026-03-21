# Databricks notebook source
"""
setup/load_generator_registry.py
=================================
Loads the AEMO NEM Registration and Exemption List into:
  - energy_copilot.silver.generator_registry   (Delta, overwrite)
  - energy_copilot.silver.region_weather_mapping (Delta, overwrite)

Data source:
  Primary  — AEMO XLSX download from the registration page
  Fallback — local CSV specified via --local-csv argument or GENERATOR_REGISTRY_CSV env var

Fuel-type canonical mapping (fuel_source_primary + tech_type_primary):
  Black Coal / Brown Coal                         → "coal"
  Natural Gas / Coal Seam Methane / CMMG / CCMG  → "gas"
  Water / Run of River                            → "hydro"
  Wind                                            → "wind"
  Solar / Photovoltaic                            → "solar_utility"
  Battery Storage                                 → "battery"
  Pump Storage / Pumped Hydro                     → "pumps"
  Liquid Fuel / Kerosene / Diesel                 → "liquid"
  Biomass / Bagasse                               → "biomass"
  Everything else                                 → "other"

Usage:
  # On Databricks (Spark available):
  python setup/load_generator_registry.py

  # With a local CSV fallback:
  python setup/load_generator_registry.py --local-csv /path/to/aemo_registrations.csv

  # Dry-run (parse + print, no writes):
  python setup/load_generator_registry.py --dry-run
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# Structured / JSON logging
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    _SKIP = frozenset({
        "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno",
        "funcName", "created", "msecs", "relativeCreated", "thread",
        "threadName", "processName", "process", "message", "name", "taskName",
    })

    def format(self, record: logging.LogRecord) -> str:
        obj: Dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
        }
        if record.exc_info:
            obj["exception"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k not in self._SKIP:
                obj[k] = v
        return json.dumps(obj, default=str)


def _configure_logging(level: str = "INFO") -> logging.Logger:
    lg = logging.getLogger("load_generator_registry")
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(_JsonFormatter())
        lg.addHandler(h)
    lg.setLevel(getattr(logging, level.upper(), logging.INFO))
    return lg


logger = _configure_logging(os.environ.get("LOG_LEVEL", "INFO"))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AEMO_REGISTRATION_URL = (
    "https://aemo.com.au/aemo/apps/api/report/NEM_REGISTRATION_AND_EXEMPTION_LIST"
)
# The main AEMO registration page (used as fallback discovery)
AEMO_REGISTRATION_PAGE = (
    "https://www.aemo.com.au/energy-systems/electricity/"
    "national-electricity-market-nem/participate-in-the-market/registration"
)

GENERATOR_REGISTRY_TABLE = "energy_copilot.silver.generator_registry"
REGION_WEATHER_TABLE = "energy_copilot.silver.region_weather_mapping"

# Expected column names in the AEMO NEM Registration Excel (sheet "Generators and Scheduled Loads")
# AEMO publishes these with mixed capitalisation; we normalise on read.
AEMO_COLUMN_MAP: Dict[str, str] = {
    "duid":                   "duid",
    "station name":           "station_name",
    "fuel source - primary":  "fuel_source_primary",
    "fuel source - descriptor": "fuel_source_descriptor",
    "technology type - primary": "tech_type_primary",
    "technology type - descriptor": "tech_type_descriptor",
    "region":                 "region_id",
    "reg cap (mw)":           "reg_cap_mw",
    "max cap (mw)":           "max_cap_mw",
    "min stable level":       "min_stable_mw",
    "dispatch type":          "dispatch_type",
    "participant id":         "participant_id",
    "classification":         "classification",
    "connection point id":    "connection_point_id",
    "scheduling status":      "scheduling_status",
    "commissioning date":     "commissioning_date",
    "deregistration date":    "deregistration_date",
}

# ---------------------------------------------------------------------------
# NEM region → weather station mapping (from Implementation Plan §3.3)
# ---------------------------------------------------------------------------

REGION_WEATHER_MAPPING: List[Dict] = [
    {
        "region_id":        "NSW1",
        "region_name":      "New South Wales",
        "representative_cities": "Sydney, Newcastle, Wollongong",
        "latitude":         -33.87,
        "longitude":        151.21,
        "city_label":       "Sydney",
        "population_weight": 1.0,
    },
    {
        "region_id":        "QLD1",
        "region_name":      "Queensland",
        "representative_cities": "Brisbane, Townsville, Cairns",
        "latitude":         -27.47,
        "longitude":        153.03,
        "city_label":       "Brisbane",
        "population_weight": 1.0,
    },
    {
        "region_id":        "VIC1",
        "region_name":      "Victoria",
        "representative_cities": "Melbourne, Geelong",
        "latitude":         -37.81,
        "longitude":        144.96,
        "city_label":       "Melbourne",
        "population_weight": 1.0,
    },
    {
        "region_id":        "SA1",
        "region_name":      "South Australia",
        "representative_cities": "Adelaide, Port Augusta",
        "latitude":         -34.93,
        "longitude":        138.60,
        "city_label":       "Adelaide",
        "population_weight": 1.0,
    },
    {
        "region_id":        "TAS1",
        "region_name":      "Tasmania",
        "representative_cities": "Hobart, Launceston",
        "latitude":         -42.88,
        "longitude":        147.33,
        "city_label":       "Hobart",
        "population_weight": 1.0,
    },
]

# ---------------------------------------------------------------------------
# Canonical fuel type mapping
# ---------------------------------------------------------------------------

# Keys are lowercased fuel_source_primary values (or tech_type_primary where
# fuel source is ambiguous). The mapper is applied in priority order.
_FUEL_SOURCE_MAP: Dict[str, str] = {
    # Coal
    "black coal":              "coal",
    "brown coal":              "coal",
    "coal":                    "coal",
    # Gas
    "natural gas":             "gas",
    "gas":                     "gas",
    "coal seam methane":       "gas",
    "coal mine waste gas":     "gas",
    "cmmg":                    "gas",
    "ccmg":                    "gas",
    "landfill gas":            "gas",
    # Hydro
    "water":                   "hydro",
    "run of river":            "hydro",
    "hydro":                   "hydro",
    # Wind
    "wind":                    "wind",
    # Solar
    "solar":                   "solar_utility",
    "photovoltaic":            "solar_utility",
    # Battery
    "battery storage":         "battery",
    "battery":                 "battery",
    # Pumped storage
    "pump storage":            "pumps",
    "pumped hydro":            "pumps",
    "pumped storage":          "pumps",
    # Liquid fuels
    "liquid fuel":             "liquid",
    "liquid":                  "liquid",
    "kerosene":                "liquid",
    "diesel":                  "liquid",
    "distillate":              "liquid",
    "fuel oil":                "liquid",
    # Biomass
    "biomass":                 "biomass",
    "bagasse":                 "biomass",
    "wood":                    "biomass",
    "agricultural waste":      "biomass",
}

_TECH_TYPE_MAP: Dict[str, str] = {
    # Battery (tech type clarifies when fuel source is generic)
    "battery storage":         "battery",
    # Pump storage
    "pump storage":            "pumps",
    "pumped hydro":            "pumps",
    # Solar
    "photovoltaic":            "solar_utility",
    # Wind
    "wind turbine":            "wind",
    "wind":                    "wind",
    # Hydro
    "hydro - dam":             "hydro",
    "run of river":            "hydro",
}

CANONICAL_FUEL_TYPES = frozenset({
    "coal", "gas", "hydro", "wind", "solar_utility",
    "battery", "pumps", "liquid", "biomass", "other",
})


def map_fuel_type(fuel_source: str, tech_type: str) -> str:
    """
    Map raw AEMO fuel source and technology type strings to a canonical
    fuel_type label.  fuel_source_primary takes priority; tech_type_primary
    is used as a tiebreaker when fuel_source is ambiguous.
    """
    fs = (fuel_source or "").strip().lower()
    tt = (tech_type or "").strip().lower()

    # Direct fuel source lookup
    if fs in _FUEL_SOURCE_MAP:
        return _FUEL_SOURCE_MAP[fs]

    # Partial-match fuel source (handles descriptors like "Natural Gas / OCGT")
    for key, val in _FUEL_SOURCE_MAP.items():
        if key in fs:
            return val

    # Fall back to tech type
    if tt in _TECH_TYPE_MAP:
        return _TECH_TYPE_MAP[tt]

    for key, val in _TECH_TYPE_MAP.items():
        if key in tt:
            return val

    return "other"

# ---------------------------------------------------------------------------
# CO2 intensity coefficients by fuel type (tonne CO2e / MWh sent-out)
# Source: Australian Government — NGA Factors 2023
# ---------------------------------------------------------------------------

_CO2_INTENSITY: Dict[str, float] = {
    "coal":          0.91,   # average for black coal plant
    "gas":           0.55,   # average for CCGT; OCGT higher ~0.72 but use blended
    "hydro":         0.004,
    "wind":          0.012,
    "solar_utility": 0.041,
    "battery":       0.0,    # zero at point of discharge; upstream charged separately
    "pumps":         0.0,    # pumped hydro, similar to hydro
    "liquid":        0.70,
    "biomass":       0.22,   # biogenic; often counted as near-zero in policy
    "other":         0.50,   # conservative default
}


def co2_intensity(fuel_type: str) -> float:
    return _CO2_INTENSITY.get(fuel_type, _CO2_INTENSITY["other"])

# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _download_aemo_xlsx() -> Optional[bytes]:
    """
    Attempt to download the AEMO NEM Registration and Exemption List XLSX.
    Returns raw bytes on success, or None on failure.
    """
    try:
        import requests
    except ImportError:
        logger.warning("requests library not available; skipping download")
        return None

    # Try the direct API endpoint first (faster, no HTML parsing needed)
    urls_to_try = [AEMO_REGISTRATION_URL]

    # Add a common direct-download URL pattern as secondary attempt
    urls_to_try.append(
        "https://aemo.com.au/aemo/apps/api/report/NEM_REGISTRATION_AND_EXEMPTION_LIST"
    )

    headers = {
        "User-Agent": "AUSEnergyCopilot/1.0 (automated data pipeline; contact: ops@example.com)",
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/octet-stream",
    }

    for url in urls_to_try:
        try:
            logger.info("Attempting AEMO download", extra={"url": url})
            resp = requests.get(url, headers=headers, timeout=60)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if (
                "spreadsheetml" in content_type
                or "octet-stream" in content_type
                or "excel" in content_type
                or resp.content[:4] == b"PK\x03\x04"  # ZIP magic bytes (XLSX is a ZIP)
            ):
                logger.info(
                    "AEMO XLSX downloaded",
                    extra={"url": url, "size_bytes": len(resp.content)},
                )
                return resp.content
            else:
                logger.warning(
                    "Unexpected content type; skipping",
                    extra={"url": url, "content_type": content_type},
                )
        except Exception as exc:
            logger.warning(
                "Download failed",
                extra={"url": url, "error": str(exc)},
            )

    return None


def _parse_xlsx(raw_bytes: bytes) -> "pd.DataFrame":
    """Parse AEMO XLSX into a normalised pandas DataFrame."""
    import pandas as pd

    # The AEMO workbook has multiple sheets; the main one is usually
    # "Generators and Scheduled Loads" (or similar)
    xl = pd.ExcelFile(io.BytesIO(raw_bytes))
    target_sheet = None
    for sheet in xl.sheet_names:
        if "generator" in sheet.lower() or "scheduled" in sheet.lower():
            target_sheet = sheet
            break
    if target_sheet is None:
        # Fall back to the second sheet (index 1) which is typically the data sheet
        target_sheet = xl.sheet_names[1] if len(xl.sheet_names) > 1 else xl.sheet_names[0]
        logger.warning(
            "Could not identify generator sheet by name; using fallback",
            extra={"sheet": target_sheet, "available_sheets": xl.sheet_names},
        )

    logger.info("Parsing AEMO XLSX", extra={"sheet": target_sheet})
    df = pd.read_excel(
        io.BytesIO(raw_bytes),
        sheet_name=target_sheet,
        header=None,
    )
    return df


def _normalise_xlsx_columns(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Find the header row (first row containing 'DUID'), normalise column names,
    and return a clean DataFrame.
    """

    # Find the header row
    header_row_idx = None
    for idx, row in df.iterrows():
        row_str = [str(c).strip().lower() for c in row.values]
        if "duid" in row_str:
            header_row_idx = idx
            break

    if header_row_idx is None:
        raise ValueError("Could not find header row containing 'DUID' in XLSX")

    logger.info("Found XLSX header row", extra={"row_index": header_row_idx})

    # Rebuild DataFrame with correct header
    headers = [str(c).strip() for c in df.iloc[header_row_idx].values]
    data = df.iloc[header_row_idx + 1 :].reset_index(drop=True)
    data.columns = headers

    # Drop fully-empty rows
    data = data.dropna(how="all")

    # Normalise column names to snake_case using AEMO_COLUMN_MAP
    col_map: Dict[str, str] = {}
    for col in data.columns:
        col_lower = col.strip().lower()
        if col_lower in AEMO_COLUMN_MAP:
            col_map[col] = AEMO_COLUMN_MAP[col_lower]
        else:
            # Keep original but snake_case it
            col_map[col] = col_lower.replace(" ", "_").replace("-", "_").replace("/", "_")

    data = data.rename(columns=col_map)
    logger.info(
        "Normalised XLSX columns",
        extra={"num_columns": len(data.columns), "num_rows": len(data)},
    )
    return data


def _load_local_csv(path: str) -> "pd.DataFrame":
    """Load a local CSV file (AEMO registration export format)."""
    import pandas as pd

    logger.info("Loading local CSV fallback", extra={"path": path})
    # AEMO CSV exports sometimes have leading/trailing whitespace in headers
    df = pd.read_csv(path, dtype=str, skipinitialspace=True)
    df.columns = [c.strip() for c in df.columns]

    # Normalise column names
    col_map: Dict[str, str] = {}
    for col in df.columns:
        col_lower = col.strip().lower()
        if col_lower in AEMO_COLUMN_MAP:
            col_map[col] = AEMO_COLUMN_MAP[col_lower]
        else:
            col_map[col] = col_lower.replace(" ", "_").replace("-", "_").replace("/", "_")

    df = df.rename(columns=col_map)
    logger.info(
        "Loaded local CSV",
        extra={"num_rows": len(df), "path": path},
    )
    return df

# ---------------------------------------------------------------------------
# Data transformation
# ---------------------------------------------------------------------------

def _transform_to_registry(raw_df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Transform the raw AEMO registration DataFrame into the canonical
    silver.generator_registry schema.

    Returns a pandas DataFrame ready for writing to Delta.
    """
    import pandas as pd

    now_utc = datetime.now(timezone.utc)
    rows = []

    required_cols = {"duid"}
    missing = required_cols - set(raw_df.columns)
    if missing:
        raise ValueError(f"Required columns missing from source data: {missing}")

    for _, row in raw_df.iterrows():
        duid = str(row.get("duid", "")).strip()
        if not duid or duid.lower() in {"nan", "none", ""}:
            continue

        fuel_source = str(row.get("fuel_source_primary", "")).strip()
        tech_type   = str(row.get("tech_type_primary", "")).strip()

        # Handle "nan" strings from pandas
        fuel_source = "" if fuel_source.lower() == "nan" else fuel_source
        tech_type   = "" if tech_type.lower() == "nan" else tech_type

        fuel_type = map_fuel_type(fuel_source, tech_type)

        def _float(val) -> Optional[float]:
            try:
                v = float(str(val).replace(",", "").strip())
                return None if (v != v) else v  # NaN check
            except (ValueError, TypeError):
                return None

        def _str(val) -> Optional[str]:
            s = str(val).strip()
            return None if s.lower() in {"nan", "none", ""} else s

        def _date(val) -> Optional[str]:
            s = str(val).strip()
            if s.lower() in {"nan", "none", ""}:
                return None
            try:
                # AEMO dates are usually DD/MM/YYYY or YYYY-MM-DD
                import re
                if re.match(r"\d{2}/\d{2}/\d{4}", s):
                    parts = s.split("/")
                    return f"{parts[2]}-{parts[1]}-{parts[0]}"
                return s[:10]  # keep ISO format as-is
            except Exception:
                return None

        scheduling_status = _str(row.get("scheduling_status", ""))
        is_scheduled      = scheduling_status == "SCHEDULED" if scheduling_status else None
        is_semi_scheduled = scheduling_status == "SEMI-SCHEDULED" if scheduling_status else None
        is_non_scheduled  = scheduling_status == "NON-SCHEDULED" if scheduling_status else None

        dereg_date = _date(row.get("deregistration_date", ""))
        is_active  = dereg_date is None  # active if no deregistration date

        max_cap  = _float(row.get("max_cap_mw")) or _float(row.get("reg_cap_mw"))
        reg_cap  = _float(row.get("reg_cap_mw"))

        rows.append({
            "duid":                   duid,
            "station_name":           _str(row.get("station_name")),
            "region_id":              _str(row.get("region_id")),
            "fuel_type":              fuel_type,
            "fuel_source_primary":    fuel_source or None,
            "technology_type":        tech_type or None,
            "registered_capacity_mw": reg_cap,
            "max_capacity_mw":        max_cap,
            "min_capacity_mw":        _float(row.get("min_stable_mw")),
            "participant_id":         _str(row.get("participant_id")),
            "classification":         _str(row.get("classification")),
            "dispatch_type":          _str(row.get("dispatch_type")),
            "co2e_intensity":         co2_intensity(fuel_type),
            "is_scheduled":           is_scheduled,
            "is_semi_scheduled":      is_semi_scheduled,
            "is_non_scheduled":       is_non_scheduled,
            "connection_point_id":    _str(row.get("connection_point_id")),
            "commissioning_date":     _date(row.get("commissioning_date")),
            "deregistration_date":    dereg_date,
            "is_active":              is_active,
            "_last_updated":          now_utc.isoformat(),
        })

    result = pd.DataFrame(rows)
    logger.info(
        "Transformation complete",
        extra={
            "total_rows": len(result),
            "fuel_type_distribution": result["fuel_type"].value_counts().to_dict()
            if len(result) > 0 else {},
        },
    )
    return result


def _build_region_weather_df() -> "pd.DataFrame":
    """Return the hardcoded NEM region ↔ weather station mapping as a DataFrame."""
    import pandas as pd

    now_utc = datetime.now(timezone.utc)
    rows = []
    for r in REGION_WEATHER_MAPPING:
        rows.append({
            **r,
            "_last_updated": now_utc.isoformat(),
        })
    df = pd.DataFrame(rows)
    logger.info(
        "Built region-weather mapping",
        extra={"num_regions": len(df)},
    )
    return df

# ---------------------------------------------------------------------------
# Delta write helpers
# ---------------------------------------------------------------------------

def _write_to_delta_spark(
    df: "pd.DataFrame",
    table_name: str,
    spark,
    dry_run: bool = False,
) -> None:
    """Write a pandas DataFrame to a Unity Catalog Delta table via PySpark."""
    logger.info(
        "Writing to Delta table",
        extra={"table": table_name, "rows": len(df), "dry_run": dry_run},
    )
    if dry_run:
        logger.info("DRY RUN — skipping write", extra={"table": table_name})
        return

    spark_df = spark.createDataFrame(df)
    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    logger.info("Delta write complete", extra={"table": table_name, "rows": len(df)})


def _write_to_delta_local(
    df: "pd.DataFrame",
    path: str,
    dry_run: bool = False,
) -> None:
    """
    Write to a local Delta-compatible directory using delta-rs (deltalake package).
    Falls back to Parquet if delta-rs is not available.
    Used when running outside Databricks without a Spark session.
    """
    if dry_run:
        logger.info("DRY RUN — skipping local write", extra={"path": path})
        return

    try:
        import pyarrow as pa
        from deltalake.writer import write_deltalake

        pa_table = pa.Table.from_pandas(df)
        write_deltalake(path, pa_table, mode="overwrite")
        logger.info("Local Delta write complete", extra={"path": path, "rows": len(df)})
    except ImportError:
        # Fall back to Parquet
        parquet_path = path.rstrip("/") + ".parquet"
        df.to_parquet(parquet_path, index=False)
        logger.warning(
            "deltalake not available; wrote as Parquet instead",
            extra={"path": parquet_path},
        )

# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run(
    local_csv: Optional[str] = None,
    dry_run: bool = False,
    output_dir: Optional[str] = None,
) -> None:
    """
    Main entry point.

    1. Download AEMO XLSX (or load local CSV fallback)
    2. Parse + normalise
    3. Apply fuel_type mapping
    4. Write silver.generator_registry (overwrite)
    5. Write silver.region_weather_mapping (overwrite)
    """
    import pandas as pd

    # ------------------------------------------------------------------
    # Step 1: Acquire raw data
    # ------------------------------------------------------------------
    raw_df: Optional[pd.DataFrame] = None

    if local_csv:
        logger.info("Using local CSV override", extra={"path": local_csv})
        try:
            raw_df = _load_local_csv(local_csv)
        except Exception as exc:
            logger.error(
                "Failed to load local CSV",
                extra={"path": local_csv, "error": str(exc)},
            )
            sys.exit(1)
    else:
        # Try live download
        xlsx_bytes = _download_aemo_xlsx()
        if xlsx_bytes is not None:
            try:
                raw_xlsx = _parse_xlsx(xlsx_bytes)
                raw_df   = _normalise_xlsx_columns(raw_xlsx)
            except Exception as exc:
                logger.warning(
                    "XLSX parse failed; will attempt CSV fallback if provided",
                    extra={"error": str(exc)},
                )

    if raw_df is None:
        # Check for env-var fallback
        env_csv = os.environ.get("GENERATOR_REGISTRY_CSV")
        if env_csv and Path(env_csv).exists():
            logger.info(
                "Using GENERATOR_REGISTRY_CSV env-var fallback",
                extra={"path": env_csv},
            )
            try:
                raw_df = _load_local_csv(env_csv)
            except Exception as exc:
                logger.error(
                    "Env-var CSV fallback also failed",
                    extra={"path": env_csv, "error": str(exc)},
                )
                sys.exit(1)
        else:
            logger.error(
                "No data source available. "
                "Provide --local-csv or set GENERATOR_REGISTRY_CSV env var.",
            )
            sys.exit(1)

    # ------------------------------------------------------------------
    # Step 2: Transform
    # ------------------------------------------------------------------
    try:
        registry_df      = _transform_to_registry(raw_df)
        region_weather_df = _build_region_weather_df()
    except Exception as exc:
        logger.error("Transformation failed", extra={"error": str(exc)})
        raise

    # ------------------------------------------------------------------
    # Step 3: Write
    # ------------------------------------------------------------------
    spark = None
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            # Try to get or create (when running as a standalone script on cluster)
            try:
                spark = SparkSession.builder.getOrCreate()
            except Exception:
                spark = None
    except ImportError:
        pass

    if spark is not None:
        logger.info("Spark session available; writing to Unity Catalog Delta")
        _write_to_delta_spark(registry_df,      GENERATOR_REGISTRY_TABLE, spark, dry_run)
        _write_to_delta_spark(region_weather_df, REGION_WEATHER_TABLE,     spark, dry_run)
    else:
        # Running locally without Spark — write to local directory
        base = output_dir or os.path.join(tempfile.gettempdir(), "energy_copilot")
        reg_path = os.path.join(base, "silver", "generator_registry")
        rw_path  = os.path.join(base, "silver", "region_weather_mapping")
        os.makedirs(reg_path, exist_ok=True)
        os.makedirs(rw_path,  exist_ok=True)
        logger.warning(
            "No Spark session; writing locally",
            extra={"registry_path": reg_path, "weather_path": rw_path},
        )
        _write_to_delta_local(registry_df,       reg_path, dry_run)
        _write_to_delta_local(region_weather_df, rw_path,  dry_run)

    # Summary
    logger.info(
        "load_generator_registry complete",
        extra={
            "registry_rows":       len(registry_df),
            "region_weather_rows": len(region_weather_df),
            "dry_run":             dry_run,
        },
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load AEMO generator registry into silver Delta tables",
    )
    parser.add_argument(
        "--local-csv",
        default=os.environ.get("GENERATOR_REGISTRY_CSV"),
        help=(
            "Path to a local AEMO registration CSV (fallback when download fails). "
            "Overrides GENERATOR_REGISTRY_CSV env var."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and transform data but do not write to Delta",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Local output directory when running without Spark "
            "(default: system temp dir / energy_copilot)"
        ),
    )
    args = parser.parse_args()
    run(
        local_csv=args.local_csv,
        dry_run=args.dry_run,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
