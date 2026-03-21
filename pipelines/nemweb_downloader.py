# Databricks notebook source
"""
nemweb_downloader.py — NEMWEB polling and download utility.

Polls NEMWEB /REPORTS/CURRENT/ every 60s, tracks processed files in Delta,
downloads new ZIPs, extracts CSVs to /Volumes/energy_copilot/bronze/nemweb_raw/.
Exponential backoff (max 5 retries). Structured JSON logging.

Usage:
  python nemweb_downloader.py          # continuous
  python nemweb_downloader.py --once   # single pass
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NEMWEB_BASE_URL = "https://www.nemweb.com.au/REPORTS/CURRENT/"
NEMWEB_HOME_URL = "https://www.nemweb.com.au/"

# ---------------------------------------------------------------------------
# File type registry — maps data-type codes to URL patterns and Bronze tables
# ---------------------------------------------------------------------------

NEMWEB_FILE_TYPES: Dict[str, Dict] = {
    "DISPATCHPRICE": {
        "url_pattern": "PUBLIC_DVD_DISPATCHPRICE_{year}{month:02d}010000",
        "table": "bronze.dispatch_prices_raw",
        "key_cols": ["SETTLEMENTDATE", "REGIONID"],
    },
    "DISPATCHSCADA": {
        "url_pattern": "PUBLIC_DVD_DISPATCHSCADA_{year}{month:02d}010000",
        "table": "bronze.dispatch_scada_raw",
        "key_cols": ["SETTLEMENTDATE", "DUID"],
    },
    "DISPATCHINTERCONNECTORRES": {
        "url_pattern": "PUBLIC_DVD_DISPATCHINTERCONNECTORRES_{year}{month:02d}010000",
        "table": "bronze.dispatch_interconnector_raw",
        "key_cols": ["SETTLEMENTDATE", "INTERCONNECTORID"],
    },
    "TRADINGPRICE": {
        "url_pattern": "PUBLIC_DVD_TRADINGPRICE_{year}{month:02d}010000",
        "table": "bronze.trading_prices_raw",
        "key_cols": ["SETTLEMENTDATE", "REGIONID"],
    },
    "PREDISPATCHPRICE": {
        "url_pattern": "PUBLIC_DVD_PREDISPATCHPRICE_{year}{month:02d}010000",
        "table": "bronze.predispatch_prices_raw",
        "key_cols": ["SETTLEMENTDATE", "REGIONID"],
    },
}

# ---------------------------------------------------------------------------
# Download metrics
# ---------------------------------------------------------------------------


@dataclass
class DownloadMetrics:
    """Tracks aggregate statistics for a single run_once() call."""

    files_attempted: int = 0
    files_succeeded: int = 0
    files_failed: int = 0
    total_bytes_downloaded: int = 0
    total_rows_extracted: int = 0

    def print_summary(self):
        """Print a human-readable summary table."""
        print("\n" + "-" * 50)
        print("  NEMWEB Downloader — Run Summary")
        print("-" * 50)
        print(f"  {'Files attempted':<30} {self.files_attempted}")
        print(f"  {'Files succeeded':<30} {self.files_succeeded}")
        print(f"  {'Files failed':<30} {self.files_failed}")
        print(
            f"  {'Total downloaded':<30} {self.total_bytes_downloaded / 1024 / 1024:.2f} MB"
        )
        print(f"  {'Total rows extracted':<30} {self.total_rows_extracted:,}")
        print("-" * 50 + "\n")


REPORT_CONFIG: Dict[str, Tuple[str, List[str]]] = {
    "Dispatch_SCADA":                 ("Dispatch_SCADA/",      [r"^PUBLIC_DISPATCHSCADA_\d{12}_\d{16}\.ZIP$"]),
    "DispatchIS_Reports":             ("DispatchIS_Reports/",  [r"^PUBLIC_DISPATCHIS_\d{12}_\d{16}\.ZIP$"]),
    "Dispatch_Reports_Price":         ("Dispatch_Reports/",    [r"^PUBLIC_DISPATCHPRICE_\d{12}_\d{16}\.ZIP$"]),
    "Dispatch_Reports_Interconnector":("Dispatch_Reports/",    [r"^PUBLIC_DISPATCHINTERCONNECTORRES_\d{12}_\d{16}\.ZIP$"]),
    "TradingIS_Reports":              ("TradingIS_Reports/",   [r"^PUBLIC_TRADINGIS_\d{12}_\d{16}\.ZIP$"]),
    "Predispatch_Reports":            ("Predispatch_Reports/", [r"^PUBLIC_PREDISPATCHIS_\d{12}_\d{16}\.ZIP$"]),
    "Next_Day_Dispatch":              ("Next_Day_Dispatch/",   [r"^PUBLIC_NEXT_DAY_DISPATCH_\d{12}_\d{16}\.ZIP$"]),
}

VOLUME_BASE_PATH = Path("/Volumes/energy_copilot/bronze/nemweb_raw")
PROCESSED_FILES_TABLE = "energy_copilot.bronze.nemweb_processed_files"
POLL_INTERVAL_SECONDS = 60
MAX_RETRIES = 5
BACKOFF_FACTOR = 2.0
BACKOFF_MAX_WAIT = 60.0


class _JsonFormatter(logging.Formatter):
    _SKIP = frozenset({"msg","args","levelname","levelno","pathname","filename","module",
                       "exc_info","exc_text","stack_info","lineno","funcName","created",
                       "msecs","relativeCreated","thread","threadName","processName","process",
                       "message","name","taskName"})
    def format(self, record):
        obj = {"timestamp": datetime.now(timezone.utc).isoformat(), "level": record.levelname,
               "message": record.getMessage(), "module": record.module}
        if record.exc_info:
            obj["exception"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k not in self._SKIP:
                obj[k] = v
        return json.dumps(obj, default=str)


def _configure_logging(level="INFO"):
    lg = logging.getLogger("nemweb_downloader")
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(_JsonFormatter())
        lg.addHandler(h)
    lg.setLevel(getattr(logging, level.upper(), logging.INFO))
    return lg


logger = _configure_logging(os.environ.get("LOG_LEVEL", "INFO"))


# ---------------------------------------------------------------------------
# URL health check
# ---------------------------------------------------------------------------


def check_nemweb_availability() -> bool:
    """
    Perform a HEAD request to the NEMWEB home page.

    Returns True if the site responds with a 2xx/3xx status code,
    False on connection failure or non-success status.
    Called at downloader startup to give early warning of outages.
    """
    try:
        session = _build_session()
        resp = session.head(NEMWEB_HOME_URL, timeout=10, allow_redirects=True)
        available = resp.status_code < 400
        logger.info(
            "NEMWEB availability check",
            extra={"url": NEMWEB_HOME_URL, "status_code": resp.status_code, "available": available},
        )
        return available
    except Exception as exc:
        logger.warning(
            "NEMWEB availability check failed",
            extra={"url": NEMWEB_HOME_URL, "error": str(exc)},
        )
        return False


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR,
                  status_forcelist=[429,500,502,503,504], allowed_methods=["HEAD","GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "AUSEnergyCopilot/1.0"})
    return session


class ProcessedFilesTracker:
    """Tracks processed ZIPs in Delta table; falls back to in-memory when Spark unavailable."""

    _DDL = """
        CREATE TABLE IF NOT EXISTS energy_copilot.bronze.nemweb_processed_files (
            filename      STRING    NOT NULL COMMENT 'NEMWEB ZIP filename',
            report_type   STRING    NOT NULL COMMENT 'Report type key',
            downloaded_at TIMESTAMP COMMENT 'UTC download timestamp',
            csv_path      STRING    COMMENT 'JSON array of extracted CSV paths',
            size_bytes    LONG      COMMENT 'ZIP size in bytes'
        ) USING DELTA
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true')
    """

    def __init__(self, spark=None):
        self._spark = spark
        self._cache: Set[str] = set()
        self._loaded = False

    def _ensure_table(self):
        if self._spark is None: return
        try: self._spark.sql(self._DDL)
        except Exception as exc:
            logger.warning("Cannot create processed-files table", extra={"error": str(exc)})
            self._spark = None

    def load(self):
        self._ensure_table()
        if self._spark is not None:
            try:
                rows = self._spark.table(PROCESSED_FILES_TABLE).select("filename").collect()
                self._cache = {r["filename"] for r in rows}
                logger.info("Loaded processed-files cache", extra={"count": len(self._cache)})
            except Exception as exc:
                logger.warning("Delta read failed; empty cache", extra={"error": str(exc)})
                self._cache = set()
        self._loaded = True

    def is_processed(self, filename: str) -> bool:
        if not self._loaded: self.load()
        return filename in self._cache

    def mark_processed(self, filename: str, report_type: str, csv_path: str, size_bytes: int):
        self._cache.add(filename)
        if self._spark is None: return
        try:
            from pyspark.sql import Row
            df = self._spark.createDataFrame([Row(filename=filename, report_type=report_type,
                                                   downloaded_at=datetime.now(timezone.utc),
                                                   csv_path=csv_path, size_bytes=size_bytes)])
            df.write.format("delta").mode("append").saveAsTable(PROCESSED_FILES_TABLE)
        except Exception as exc:
            logger.error("Failed to persist to Delta", extra={"filename": filename, "error": str(exc)})


class NemwebDownloader:
    """Poll NEMWEB, download new ZIPs, extract CSVs to Databricks Volume."""

    def __init__(self, spark=None, volume_base_path=None, poll_interval=POLL_INTERVAL_SECONDS):
        self._spark = spark
        self._volume_base = volume_base_path or VOLUME_BASE_PATH
        self._poll_interval = poll_interval
        self._session = _build_session()
        self._tracker = ProcessedFilesTracker(spark=spark)

    def run_continuous(self):
        logger.info("Starting NEMWEB polling", extra={"interval_s": self._poll_interval})
        while True:
            try: self.run_once()
            except KeyboardInterrupt: raise
            except Exception as exc:
                logger.error("Unhandled exception; retrying", extra={"error": str(exc)})
            time.sleep(self._poll_interval)

    def run_once(self) -> Dict[str, int]:
        # Check NEMWEB availability before starting the cycle
        if not check_nemweb_availability():
            logger.error("NEMWEB is not reachable; skipping this cycle")
            return {}

        metrics = DownloadMetrics()
        results = {}
        for report_type, (subdir, patterns) in REPORT_CONFIG.items():
            url = urljoin(NEMWEB_BASE_URL, subdir)
            try:
                count = self._process_directory(report_type, url, patterns, metrics)
                results[report_type] = count
            except Exception as exc:
                logger.error("Directory error", extra={"report_type": report_type, "error": str(exc)})
                results[report_type] = 0

        logger.info("Cycle complete", extra={"total_new": sum(results.values())})
        metrics.print_summary()
        return results

    def _process_directory(
        self, report_type, directory_url, patterns, metrics: Optional[DownloadMetrics] = None
    ) -> int:
        available = self._list_directory(directory_url)
        compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
        new = 0
        for filename in available:
            if not any(rx.match(filename) for rx in compiled): continue
            if self._tracker.is_processed(filename): continue
            if metrics is not None:
                metrics.files_attempted += 1
            success = self._download_and_extract(
                urljoin(directory_url, filename), filename, report_type, metrics
            )
            if success:
                new += 1
                if metrics is not None:
                    metrics.files_succeeded += 1
            else:
                if metrics is not None:
                    metrics.files_failed += 1
        return new

    def _list_directory(self, url) -> List[str]:
        resp = self._get_with_backoff(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        return [link["href"].split("/")[-1] for link in soup.find_all("a", href=True)
                if link["href"].upper().endswith(".ZIP")]

    def _verify_zip_checksum(self, zip_bytes: bytes, filename: str) -> bool:
        """
        Verify ZIP integrity using zipfile.ZipFile.testzip().

        Returns True if the ZIP is valid. On corruption, logs the error
        and returns False so the caller can delete and retry.
        """
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                bad_entry = zf.testzip()
                if bad_entry is not None:
                    logger.error(
                        "Checksum failure — corrupt ZIP entry",
                        extra={"filename": filename, "bad_entry": bad_entry},
                    )
                    return False
            return True
        except zipfile.BadZipFile as exc:
            logger.error(
                "BadZipFile during checksum verification",
                extra={"filename": filename, "error": str(exc)},
            )
            return False

    def _download_and_extract(
        self,
        zip_url: str,
        filename: str,
        report_type: str,
        metrics: Optional[DownloadMetrics] = None,
    ) -> bool:
        logger.info("Downloading", extra={"filename": filename})

        for attempt in range(1, 3):  # up to 2 attempts (initial + 1 retry on corruption)
            try:
                resp = self._get_with_backoff(zip_url)
                zip_bytes = resp.content
            except Exception as exc:
                logger.error("Download error", extra={"filename": filename, "error": str(exc)})
                return False

            # Checksum verification — retry once if corrupt
            if not self._verify_zip_checksum(zip_bytes, filename):
                if attempt < 2:
                    logger.warning(
                        "Corrupt ZIP — retrying download",
                        extra={"filename": filename, "attempt": attempt},
                    )
                    continue
                else:
                    logger.error(
                        "ZIP still corrupt after retry — skipping",
                        extra={"filename": filename},
                    )
                    return False

            # ZIP is valid — extract
            break
        else:
            return False

        try:
            out_dir = self._volume_base / report_type
            out_dir.mkdir(parents=True, exist_ok=True)
            row_count = 0
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                csv_entries = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
                if not csv_entries: return False
                written = []
                for entry in csv_entries:
                    dest = out_dir / Path(entry).name
                    csv_data = zf.read(entry)
                    dest.write_bytes(csv_data)
                    written.append(str(dest))
                    # Count data rows (subtract header lines)
                    row_count += max(0, csv_data.count(b"\n") - 2)
            if metrics is not None:
                metrics.total_bytes_downloaded += len(zip_bytes)
                metrics.total_rows_extracted += row_count
            self._tracker.mark_processed(filename, report_type, json.dumps(written), len(zip_bytes))
            logger.info("ZIP processed", extra={"filename": filename, "csv_count": len(written), "approx_rows": row_count})
            return True
        except zipfile.BadZipFile as exc:
            logger.error("Corrupt ZIP", extra={"filename": filename, "error": str(exc)}); return False
        except OSError as exc:
            logger.error("Filesystem error", extra={"filename": filename, "error": str(exc)}); return False

    def _get_with_backoff(self, url) -> requests.Response:
        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self._session.get(url, timeout=30)
                resp.raise_for_status()
                return resp
            except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as exc:
                last_exc = exc
                logger.warning("Request failed", extra={"url": url, "attempt": attempt})
                if attempt < MAX_RETRIES:
                    time.sleep(min(BACKOFF_FACTOR**(attempt-1), BACKOFF_MAX_WAIT))
        raise RuntimeError(f"GET {url} failed after {MAX_RETRIES} attempts") from last_exc


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--once", action="store_true")
    p.add_argument("--interval", type=int, default=POLL_INTERVAL_SECONDS)
    p.add_argument("--volume-path", default=str(VOLUME_BASE_PATH))
    args = p.parse_args()
    spark = None
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
    except ImportError:
        pass
    d = NemwebDownloader(spark=spark, volume_base_path=Path(args.volume_path), poll_interval=args.interval)
    if args.once: d.run_once()
    else: d.run_continuous()


if __name__ == "__main__":
    main()
