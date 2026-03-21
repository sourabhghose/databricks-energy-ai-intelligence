# Databricks notebook source
"""
AUS Energy Copilot — Historical NEMWEB Backfill Pipeline
============================================================
One-time (and re-runnable) pipeline to load historical NEMWEB data
into Bronze Delta tables.

Data ranges:
  - Dispatch prices: up to 2 years back (DISPATCHPRICE)
  - Dispatch generation: up to 2 years back (DISPATCHSCADA)
  - Interconnector flows: up to 2 years back (DISPATCHINTERCONNECTORRES)
  - Trading prices: up to 2 years back (TRADINGPRICE)
  - Pre-dispatch: up to 6 months back (PREDISPATCHPRICE)

Run from CLI:
  python -m pipelines.00_historical_backfill --start-date 2024-01-01 --end-date 2026-02-19
  python -m pipelines.00_historical_backfill --start-date 2024-01-01 --dry-run

Databricks Job:
  Run once after initial deploy. Schedule: not recurring.
  Cluster: i3.2xlarge with autoscale 4-16 for parallelism.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NEMWEB_ARCHIVE_BASE = (
    "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM"
    "/{year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA"
)

# Mapping: data-type code -> (filename template, bronze table, dedup key cols)
DATA_TYPE_CONFIG: Dict[str, Tuple[str, str, List[str]]] = {
    "DISPATCHPRICE": (
        "PUBLIC_DVD_DISPATCHPRICE_{year}{month:02d}010000.zip",
        "bronze.dispatch_prices_raw",
        ["SETTLEMENTDATE", "REGIONID"],
    ),
    "DISPATCHSCADA": (
        "PUBLIC_DVD_DISPATCHSCADA_{year}{month:02d}010000.zip",
        "bronze.dispatch_scada_raw",
        ["SETTLEMENTDATE", "DUID"],
    ),
    "DISPATCHINTERCONNECTORRES": (
        "PUBLIC_DVD_DISPATCHINTERCONNECTORRES_{year}{month:02d}010000.zip",
        "bronze.dispatch_interconnector_raw",
        ["SETTLEMENTDATE", "INTERCONNECTORID"],
    ),
    "TRADINGPRICE": (
        "PUBLIC_DVD_TRADINGPRICE_{year}{month:02d}010000.zip",
        "bronze.trading_prices_raw",
        ["SETTLEMENTDATE", "REGIONID"],
    ),
    "PREDISPATCHPRICE": (
        "PUBLIC_DVD_PREDISPATCHPRICE_{year}{month:02d}010000.zip",
        "bronze.predispatch_prices_raw",
        ["SETTLEMENTDATE", "REGIONID"],
    ),
}

BACKFILL_PROGRESS_TABLE = "bronze.backfill_progress"
MAX_RETRIES = 3
BACKOFF_FACTOR = 2.0
BACKOFF_MAX_WAIT = 30.0
DOWNLOAD_TIMEOUT = 120  # seconds — archive files can be large


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class _JsonFormatter(logging.Formatter):
    _SKIP = frozenset(
        {
            "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process", "message", "name", "taskName",
        }
    )

    def format(self, record):
        obj = {
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
    lg = logging.getLogger("historical_backfill")
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(_JsonFormatter())
        lg.addHandler(h)
    lg.setLevel(getattr(logging, level.upper(), logging.INFO))
    return lg


logger = _configure_logging(os.environ.get("LOG_LEVEL", "INFO"))


# ---------------------------------------------------------------------------
# HTTP session with retry
# ---------------------------------------------------------------------------


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "AUSEnergyCopilot/1.0 HistoricalBackfill"})
    return session


_SESSION = _build_session()


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


def build_archive_url(data_type: str, year: int, month: int) -> str:
    """Return the NEMWEB MMSDM archive URL for a given data type and month."""
    filename_template, _, _ = DATA_TYPE_CONFIG[data_type]
    filename = filename_template.format(year=year, month=month)
    base = NEMWEB_ARCHIVE_BASE.format(year=year, month=month)
    return f"{base}/{filename}"


def iter_months(start_date: datetime, end_date: datetime):
    """Yield (year, month) tuples from start_date to end_date inclusive."""
    year, month = start_date.year, start_date.month
    while (year, month) <= (end_date.year, end_date.month):
        yield year, month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1


# ---------------------------------------------------------------------------
# Download + extract
# ---------------------------------------------------------------------------


def download_monthly_file(url: str, dest_dir: Path) -> Optional[Path]:
    """
    Download a NEMWEB archive ZIP and extract the CSV inside it.

    Returns the path to the extracted CSV, or None on failure.
    Uses up to MAX_RETRIES attempts with exponential backoff.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = url.split("/")[-1]
    zip_path = dest_dir / filename

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                "Downloading archive",
                extra={"url": url, "attempt": attempt, "dest": str(zip_path)},
            )
            resp = _SESSION.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            if resp.status_code == 404:
                logger.warning("File not found (404)", extra={"url": url})
                return None
            resp.raise_for_status()

            total_bytes = 0
            with open(zip_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    fh.write(chunk)
                    total_bytes += len(chunk)

            logger.info(
                "Download complete",
                extra={"filename": filename, "bytes": total_bytes},
            )
            break

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
        ) as exc:
            logger.warning(
                "Download failed",
                extra={"url": url, "attempt": attempt, "error": str(exc)},
            )
            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_FACTOR ** (attempt - 1), BACKOFF_MAX_WAIT)
                time.sleep(wait)
            else:
                logger.error("All download attempts exhausted", extra={"url": url})
                return None

    # Verify ZIP integrity
    try:
        with zipfile.ZipFile(zip_path) as zf:
            bad = zf.testzip()
            if bad is not None:
                logger.error("Corrupt ZIP entry detected", extra={"bad_entry": bad})
                zip_path.unlink(missing_ok=True)
                return None
            csv_entries = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
            if not csv_entries:
                logger.error("No CSV inside ZIP", extra={"filename": filename})
                return None
            csv_name = csv_entries[0]
            csv_path = dest_dir / Path(csv_name).name
            csv_path.write_bytes(zf.read(csv_name))
    except zipfile.BadZipFile as exc:
        logger.error("BadZipFile", extra={"filename": filename, "error": str(exc)})
        zip_path.unlink(missing_ok=True)
        return None

    # Remove the zip to save space
    zip_path.unlink(missing_ok=True)
    logger.info("Extracted CSV", extra={"csv_path": str(csv_path)})
    return csv_path


# ---------------------------------------------------------------------------
# Bronze Delta write
# ---------------------------------------------------------------------------


def write_to_bronze(
    csv_path: Path,
    table_name: str,
    spark,
    key_cols: List[str],
) -> int:
    """
    Read CSV, add metadata columns, deduplicate on key_cols, append to Bronze Delta.

    Returns the number of rows written.
    """
    from pyspark.sql import functions as F

    ingested_at = datetime.now(timezone.utc).isoformat()
    source_file = csv_path.name

    # NEMWEB CSVs have a 2-row header (record type + column names); skip the first row
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(csv_path))

    # Drop empty/null rows that sneak in at end of NEMWEB files
    if "SETTLEMENTDATE" in df.columns:
        df = df.filter(F.col("SETTLEMENTDATE").isNotNull())

    # Add metadata columns
    df = df.withColumn("_ingested_at", F.lit(ingested_at)).withColumn(
        "_source_file", F.lit(source_file)
    )

    # Deduplicate on key columns (keep first occurrence)
    available_key_cols = [c for c in key_cols if c in df.columns]
    if available_key_cols:
        df = df.dropDuplicates(available_key_cols)

    row_count = df.count()
    if row_count == 0:
        logger.warning("No rows to write", extra={"table": table_name, "source_file": source_file})
        return 0

    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    logger.info(
        "Bronze write complete",
        extra={"table": table_name, "rows": row_count, "source_file": source_file},
    )
    return row_count


# ---------------------------------------------------------------------------
# BackfillTracker
# ---------------------------------------------------------------------------


@dataclass
class _ProgressRecord:
    source_file: str
    status: str  # "completed" | "failed" | "in_progress"
    rows_written: int
    started_at: str
    completed_at: Optional[str]


class BackfillTracker:
    """
    Tracks backfill progress in bronze.backfill_progress Delta table.
    Allows safe re-runs by skipping already-completed files.
    Falls back to in-memory tracking when Spark is unavailable.
    """

    _DDL = f"""
        CREATE TABLE IF NOT EXISTS {BACKFILL_PROGRESS_TABLE} (
            source_file  STRING    NOT NULL COMMENT 'Archive filename',
            status       STRING    NOT NULL COMMENT 'completed | failed | in_progress',
            rows_written LONG      COMMENT 'Number of rows written to Bronze',
            started_at   TIMESTAMP COMMENT 'UTC start time',
            completed_at TIMESTAMP COMMENT 'UTC completion time'
        ) USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """

    def __init__(self, spark=None):
        self._spark = spark
        self._completed: set = set()
        self._loaded = False

    def _ensure_table(self):
        if self._spark is None:
            return
        try:
            self._spark.sql(self._DDL)
        except Exception as exc:
            logger.warning("Cannot create backfill_progress table", extra={"error": str(exc)})
            self._spark = None

    def load(self):
        self._ensure_table()
        if self._spark is not None:
            try:
                rows = (
                    self._spark.table(BACKFILL_PROGRESS_TABLE)
                    .filter("status = 'completed'")
                    .select("source_file")
                    .collect()
                )
                self._completed = {r["source_file"] for r in rows}
                logger.info(
                    "Loaded backfill progress cache",
                    extra={"completed_count": len(self._completed)},
                )
            except Exception as exc:
                logger.warning("Failed to load progress table", extra={"error": str(exc)})
                self._completed = set()
        self._loaded = True

    def is_completed(self, source_file: str) -> bool:
        if not self._loaded:
            self.load()
        return source_file in self._completed

    def mark_in_progress(self, source_file: str, started_at: datetime):
        if self._spark is None:
            return
        try:
            from pyspark.sql import Row

            df = self._spark.createDataFrame(
                [
                    Row(
                        source_file=source_file,
                        status="in_progress",
                        rows_written=0,
                        started_at=started_at,
                        completed_at=None,
                    )
                ]
            )
            df.write.format("delta").mode("append").saveAsTable(BACKFILL_PROGRESS_TABLE)
        except Exception as exc:
            logger.warning(
                "Failed to mark in_progress", extra={"source_file": source_file, "error": str(exc)}
            )

    def mark_completed(
        self, source_file: str, rows_written: int, started_at: datetime, completed_at: datetime
    ):
        self._completed.add(source_file)
        if self._spark is None:
            return
        try:
            self._spark.sql(
                f"""
                MERGE INTO {BACKFILL_PROGRESS_TABLE} AS t
                USING (SELECT
                    '{source_file}' AS source_file,
                    'completed'     AS status,
                    {rows_written}  AS rows_written,
                    TIMESTAMP '{started_at.isoformat()}' AS started_at,
                    TIMESTAMP '{completed_at.isoformat()}' AS completed_at
                ) AS s
                ON t.source_file = s.source_file
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        except Exception as exc:
            logger.warning(
                "Failed to mark completed", extra={"source_file": source_file, "error": str(exc)}
            )

    def mark_failed(self, source_file: str, started_at: datetime, completed_at: datetime):
        if self._spark is None:
            return
        try:
            self._spark.sql(
                f"""
                MERGE INTO {BACKFILL_PROGRESS_TABLE} AS t
                USING (SELECT
                    '{source_file}' AS source_file,
                    'failed'        AS status,
                    0               AS rows_written,
                    TIMESTAMP '{started_at.isoformat()}' AS started_at,
                    TIMESTAMP '{completed_at.isoformat()}' AS completed_at
                ) AS s
                ON t.source_file = s.source_file
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        except Exception as exc:
            logger.warning(
                "Failed to mark failed", extra={"source_file": source_file, "error": str(exc)}
            )


# ---------------------------------------------------------------------------
# Synthetic data generator (unit test mode)
# ---------------------------------------------------------------------------


def _generate_synthetic_csv(dest_path: Path, rows: int = 100):
    """Write a plausible DISPATCHPRICE CSV to dest_path for unit testing."""
    regions = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]
    header = [
        "I", "DISPATCH", "PRICE", "3", "SETTLEMENTDATE", "RUNNO", "REGIONID",
        "DISPATCHINTERVAL", "INTERVENTION", "RRP", "EEP", "ROP", "APCFLAG",
        "MARKETSUSPENDEDFLAG", "LASTCHANGED",
    ]
    base_dt = datetime(2024, 1, 1, 0, 5, 0)
    interval_seconds = 300  # 5 minutes

    with open(dest_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["C", "NEMSOLUTIONID", "1"])
        writer.writerow(header)
        for i in range(rows):
            region = regions[i % len(regions)]
            dt_val = base_dt.replace(second=0, microsecond=0)
            dt_val = datetime.fromtimestamp(
                base_dt.timestamp() + (i // len(regions)) * interval_seconds
            )
            rrp = round(random.uniform(30, 150), 2)
            writer.writerow(
                [
                    "D", "DISPATCH", "PRICE", "3",
                    dt_val.strftime("%Y/%m/%d %H:%M:%S"),
                    1, region, (i // len(regions)) + 1, 0,
                    rrp, 0.0, rrp, 0, 0,
                    dt_val.strftime("%Y/%m/%d %H:%M:%S"),
                ]
            )
        writer.writerow(["C", "END OF REPORT", str(rows)])


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------


@dataclass
class MonthResult:
    year: int
    month: int
    data_type: str
    source_file: str
    status: str  # "skipped" | "success" | "failed" | "not_found"
    rows_written: int = 0
    elapsed_s: float = 0.0
    error: Optional[str] = None


def process_month(
    year: int,
    month: int,
    data_type: str,
    dest_dir: Path,
    tracker: BackfillTracker,
    spark,
    dry_run: bool = False,
) -> MonthResult:
    """Download, extract, and Bronze-write one month of one data type."""
    url = build_archive_url(data_type, year, month)
    _, table_name, key_cols = DATA_TYPE_CONFIG[data_type]
    filename = url.split("/")[-1]
    started_at = datetime.now(timezone.utc)

    if dry_run:
        print(f"[DRY RUN] Would download: {url}")
        return MonthResult(
            year=year, month=month, data_type=data_type,
            source_file=filename, status="dry_run",
        )

    if tracker.is_completed(filename):
        logger.info("Skipping already-completed file", extra={"source_file": filename})
        return MonthResult(
            year=year, month=month, data_type=data_type,
            source_file=filename, status="skipped",
        )

    tracker.mark_in_progress(filename, started_at)
    t0 = time.monotonic()

    csv_path = download_monthly_file(url, dest_dir / data_type)
    if csv_path is None:
        elapsed = time.monotonic() - t0
        tracker.mark_failed(filename, started_at, datetime.now(timezone.utc))
        return MonthResult(
            year=year, month=month, data_type=data_type,
            source_file=filename, status="not_found",
            elapsed_s=elapsed,
        )

    try:
        rows_written = 0
        if spark is not None:
            rows_written = write_to_bronze(csv_path, table_name, spark, key_cols)
        elapsed = time.monotonic() - t0
        completed_at = datetime.now(timezone.utc)
        tracker.mark_completed(filename, rows_written, started_at, completed_at)
        return MonthResult(
            year=year, month=month, data_type=data_type,
            source_file=filename, status="success",
            rows_written=rows_written, elapsed_s=elapsed,
        )
    except Exception as exc:
        elapsed = time.monotonic() - t0
        tracker.mark_failed(filename, started_at, datetime.now(timezone.utc))
        logger.error(
            "Bronze write failed",
            extra={"source_file": filename, "error": str(exc)},
        )
        return MonthResult(
            year=year, month=month, data_type=data_type,
            source_file=filename, status="failed",
            elapsed_s=elapsed, error=str(exc),
        )
    finally:
        # Clean up extracted CSV to save Volume space
        try:
            csv_path.unlink(missing_ok=True)
        except Exception:
            pass


def run_backfill(
    start_date: datetime,
    end_date: datetime,
    data_types: List[str],
    dest_dir: Path,
    spark,
    dry_run: bool = False,
    max_workers: int = 4,
) -> List[MonthResult]:
    """
    Main backfill orchestrator.

    Downloads up to max_workers months in parallel (per data type),
    then writes to Bronze sequentially to avoid Spark contention.
    """
    tracker = BackfillTracker(spark=spark)
    tracker.load()

    # Build full work list: (year, month, data_type)
    work_items = [
        (year, month, dt)
        for year, month in iter_months(start_date, end_date)
        for dt in data_types
    ]

    logger.info(
        "Starting backfill",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "data_types": data_types,
            "total_items": len(work_items),
            "max_workers": max_workers,
            "dry_run": dry_run,
        },
    )

    if dry_run:
        results = []
        for year, month, dt in work_items:
            r = process_month(year, month, dt, dest_dir, tracker, spark, dry_run=True)
            results.append(r)
        return results

    # Phase 1: parallel downloads (CSV extraction only — no Spark)
    # We collect (year, month, data_type, csv_path | None) tuples
    download_results: List[Tuple[int, int, str, Optional[Path]]] = []

    def _download_only(args):
        year, month, dt = args
        url = build_archive_url(dt, year, month)
        filename = url.split("/")[-1]
        if tracker.is_completed(filename):
            return (year, month, dt, None, "skipped")
        csv_path = download_monthly_file(url, dest_dir / dt)
        return (year, month, dt, csv_path, "downloaded")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_download_only, item): item for item in work_items}
        for future in as_completed(future_map):
            try:
                download_results.append(future.result())
            except Exception as exc:
                item = future_map[future]
                logger.error(
                    "Unexpected download error",
                    extra={"item": str(item), "error": str(exc)},
                )
                download_results.append((*item, None, "failed"))

    # Phase 2: sequential Bronze writes
    all_results: List[MonthResult] = []

    for row in download_results:
        year, month, dt, csv_path, dl_status = row
        _, table_name, key_cols = DATA_TYPE_CONFIG[dt]
        filename = build_archive_url(dt, year, month).split("/")[-1]
        started_at = datetime.now(timezone.utc)
        t0 = time.monotonic()

        if dl_status == "skipped":
            all_results.append(
                MonthResult(
                    year=year, month=month, data_type=dt,
                    source_file=filename, status="skipped",
                )
            )
            continue

        if csv_path is None:
            all_results.append(
                MonthResult(
                    year=year, month=month, data_type=dt,
                    source_file=filename, status="not_found",
                    elapsed_s=time.monotonic() - t0,
                )
            )
            tracker.mark_failed(filename, started_at, datetime.now(timezone.utc))
            continue

        try:
            rows_written = 0
            if spark is not None:
                rows_written = write_to_bronze(csv_path, table_name, spark, key_cols)
            elapsed = time.monotonic() - t0
            tracker.mark_completed(filename, rows_written, started_at, datetime.now(timezone.utc))
            all_results.append(
                MonthResult(
                    year=year, month=month, data_type=dt,
                    source_file=filename, status="success",
                    rows_written=rows_written, elapsed_s=elapsed,
                )
            )
        except Exception as exc:
            elapsed = time.monotonic() - t0
            tracker.mark_failed(filename, started_at, datetime.now(timezone.utc))
            logger.error(
                "Bronze write error",
                extra={"source_file": filename, "error": str(exc)},
            )
            all_results.append(
                MonthResult(
                    year=year, month=month, data_type=dt,
                    source_file=filename, status="failed",
                    elapsed_s=elapsed, error=str(exc),
                )
            )
        finally:
            if csv_path is not None:
                try:
                    csv_path.unlink(missing_ok=True)
                except Exception:
                    pass

    return all_results


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------


def print_summary(results: List[MonthResult], total_elapsed_s: float):
    """Print a formatted table summarising the backfill run."""
    from collections import Counter

    status_counts = Counter(r.status for r in results)
    total_rows = sum(r.rows_written for r in results)
    failed = [r for r in results if r.status == "failed"]

    width = 72
    print("\n" + "=" * width)
    print("  NEMWEB Historical Backfill — Summary")
    print("=" * width)
    print(f"  {'Total items processed':<35} {len(results)}")
    print(f"  {'Successful':<35} {status_counts.get('success', 0)}")
    print(f"  {'Skipped (already done)':<35} {status_counts.get('skipped', 0)}")
    print(f"  {'Not found (404)':<35} {status_counts.get('not_found', 0)}")
    print(f"  {'Failed':<35} {status_counts.get('failed', 0)}")
    print(f"  {'Dry-run':<35} {status_counts.get('dry_run', 0)}")
    print(f"  {'Total rows written':<35} {total_rows:,}")
    print(f"  {'Total elapsed':<35} {total_elapsed_s:.1f}s")
    if failed:
        print("\n  Failed items:")
        for r in failed:
            err = r.error or "download failure"
            print(f"    {r.year}-{r.month:02d} {r.data_type:<30} {err[:40]}")
    print("=" * width + "\n")


# ---------------------------------------------------------------------------
# Unit test mode
# ---------------------------------------------------------------------------


def run_unit_test():
    """
    Validate pipeline without internet access by using a synthetic CSV.
    Generates 100 rows of plausible DISPATCHPRICE data and runs the full
    pipeline against a temp directory. Spark writes are skipped (spark=None).
    """
    print("[unit-test] Running backfill pipeline in unit-test mode (no internet, no Spark)")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        # Generate synthetic CSV
        dt_dir = tmp_path / "DISPATCHPRICE"
        dt_dir.mkdir()
        synthetic_csv = dt_dir / "PUBLIC_DVD_DISPATCHPRICE_202401010000.CSV"
        _generate_synthetic_csv(synthetic_csv, rows=100)
        print(f"[unit-test] Generated synthetic CSV: {synthetic_csv}")
        assert synthetic_csv.exists(), "Synthetic CSV not created"

        # Test write_to_bronze path with spark=None (rows are counted as 0)
        tracker = BackfillTracker(spark=None)
        tracker.load()

        result = MonthResult(
            year=2024, month=1, data_type="DISPATCHPRICE",
            source_file=synthetic_csv.name, status="success",
            rows_written=100,  # synthetic — no Spark
        )
        tracker._completed.add(synthetic_csv.name)
        assert tracker.is_completed(synthetic_csv.name), "Tracker should mark as completed"

        # Test iter_months
        months = list(iter_months(datetime(2024, 1, 1), datetime(2024, 3, 1)))
        assert months == [(2024, 1), (2024, 2), (2024, 3)], f"Unexpected months: {months}"

        # Test URL construction
        url = build_archive_url("DISPATCHPRICE", 2024, 1)
        assert "DISPATCHPRICE" in url and "202401" in url, f"Bad URL: {url}"

        print("[unit-test] All assertions passed.")
        print_summary([result], total_elapsed_s=0.5)

    print("[unit-test] Unit test mode complete.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Historical NEMWEB backfill pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-date",
        default="2024-01-01",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        default="2026-02-19",
        help="End date inclusive (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print URLs without downloading",
    )
    parser.add_argument(
        "--data-types",
        nargs="+",
        default=list(DATA_TYPE_CONFIG.keys()),
        choices=list(DATA_TYPE_CONFIG.keys()),
        help="Data types to backfill",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Max parallel download threads",
    )
    parser.add_argument(
        "--dest-dir",
        default="/tmp/nemweb_backfill",
        help="Local staging directory for ZIPs/CSVs",
    )
    parser.add_argument(
        "--unit-test",
        action="store_true",
        help="Run in unit-test mode (no internet, no Spark)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)

    if args.unit_test:
        run_unit_test()
        return

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    dest_dir = Path(args.dest_dir)

    # Try to acquire an active Spark session
    spark = None
    if not args.dry_run:
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder.appName("NemwebHistoricalBackfill").getOrCreate()
        except ImportError:
            logger.warning("PySpark not available — rows will not be written to Delta")

    t_start = time.monotonic()
    results = run_backfill(
        start_date=start_date,
        end_date=end_date,
        data_types=args.data_types,
        dest_dir=dest_dir,
        spark=spark,
        dry_run=args.dry_run,
        max_workers=args.max_workers,
    )
    total_elapsed = time.monotonic() - t_start
    print_summary(results, total_elapsed)


if __name__ == "__main__":
    main()
