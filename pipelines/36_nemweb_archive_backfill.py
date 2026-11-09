# Databricks notebook source
# MAGIC %md
# MAGIC # 36 — NEMWEB archive backfill (real history for model training)
# MAGIC
# MAGIC **Execution evidence:** `evidence/captured_runs.py::BACKFILL` (job run 161620000649581: rows_written 128,860, days_failed 0); executed notebook with outputs in `notebooks/executed/`; checked by `tests/test_execution_evidence.py`.
# MAGIC
# MAGIC Downloads AEMO's daily DispatchIS bundles from `/REPORTS/ARCHIVE/DispatchIS_Reports/`
# MAGIC (each holds the 288 five-minute files for one market day), parses them with the same
# MAGIC parser as the live ingest (`nemweb_parse.py`) and appends the rows to
# MAGIC `{catalog}.bronze.nemweb_dispatch_price`. Any 5-minute file already in bronze is skipped, so
# MAGIC this notebook and the live feed can never double-load an interval.
# MAGIC
# MAGIC Afterwards run job 01's `bronze_to_gold_dq` task (or the next scheduled run does it): every
# MAGIC backfilled row goes through the same data-quality rules before it reaches gold.

# COMMAND ----------

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from nemweb_parse import iter_archive_day, to_bronze_row  # pipelines/nemweb_parse.py
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

dbutils.widgets.text("catalog", "energy_copilot_catalog")
dbutils.widgets.text("days", "90")
dbutils.widgets.text("workers", "6")

CATALOG = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"Invalid catalog name: {CATALOG!r}")
DAYS = int(dbutils.widgets.get("days"))
WORKERS = int(dbutils.widgets.get("workers"))

BRONZE = f"{CATALOG}.bronze.nemweb_dispatch_price"
ARCHIVE_URL = "https://www.nemweb.com.au/REPORTS/ARCHIVE/DispatchIS_Reports/"
DAY_PATTERN = re.compile(r'href="([^"]*?(PUBLIC_DISPATCHIS_(\d{8})\.zip))"', re.IGNORECASE)

BRONZE_SCHEMA = StructType([
    StructField("_source_file", StringType()), StructField("_ingested_at", TimestampType()),
    StructField("i_type", StringType()), StructField("settlement_date", StringType()),
    StructField("run_no", StringType()), StructField("regionid", StringType()),
    StructField("rrp", StringType()), StructField("intervention", StringType()),
    StructField("lastchanged", StringType()), StructField("settlement_datetime", StringType()),
    StructField("total_demand", StringType()), StructField("available_generation", StringType()),
    StructField("net_interchange", StringType()),
])

# COMMAND ----------


def build_session() -> requests.Session:
    retry = Retry(total=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504))
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=WORKERS))
    session.headers.update({"User-Agent": "energy-copilot-nemweb-backfill/1.0"})
    return session


session = build_session()
listing = session.get(ARCHIVE_URL, timeout=60)
listing.raise_for_status()
days = sorted({(d, urljoin(ARCHIVE_URL, href)) for href, _, d in DAY_PATTERN.findall(listing.text)})[-DAYS:]
landed = {r[0] for r in spark.sql(f"SELECT DISTINCT _source_file FROM {BRONZE}").collect()}
print(f"Archive days selected: {len(days)} ({days[0][0]} → {days[-1][0]}) | files already landed: {len(landed)}")

# COMMAND ----------


def fetch_day(day: str, url: str) -> tuple[str, list[tuple], int, str | None]:
    """Download one daily bundle and return bronze rows for the 5-minute files not yet landed."""
    try:
        resp = session.get(url, timeout=180)
        resp.raise_for_status()
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        rows, skipped = [], 0
        for inner, records in iter_archive_day(resp.content):
            if inner in landed:
                skipped += 1
                continue
            rows.extend(to_bronze_row(r, ingested_at) for r in records)
        return day, rows, skipped, None
    except Exception as exc:  # logged per day and reported in the summary; other days continue
        return day, [], 0, f"{type(exc).__name__}: {str(exc)[:200]}"


written, failed, skipped_files, batch = 0, [], 0, []
with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    futures = [pool.submit(fetch_day, d, u) for d, u in days]
    for fut in as_completed(futures):
        day, rows, skipped, err = fut.result()
        skipped_files += skipped
        if err:
            failed.append({"day": day, "error": err})
            print(f"WARN {day}: {err}")
            continue
        batch.extend(rows)
        if len(batch) >= 20_000:  # write in chunks to keep driver memory flat
            spark.createDataFrame(batch, schema=BRONZE_SCHEMA).write.mode("append").saveAsTable(BRONZE)
            written += len(batch)
            batch = []
if batch:
    spark.createDataFrame(batch, schema=BRONZE_SCHEMA).write.mode("append").saveAsTable(BRONZE)
    written += len(batch)

# COMMAND ----------

coverage = spark.sql(f"""
  SELECT min(settlement_datetime) AS first_interval, max(settlement_datetime) AS last_interval,
         count(*) AS rows, count(DISTINCT _source_file) AS files
  FROM {BRONZE}
""").collect()[0].asDict()
summary = {
    "days_requested": len(days),
    "days_failed": len(failed),
    "failed": failed[:10],
    "rows_written": written,
    "files_skipped_already_landed": skipped_files,
    "bronze_coverage_aest": coverage,
}
print(json.dumps(summary, indent=2, default=str))
if len(failed) > len(days) // 10:
    raise RuntimeError(f"More than 10% of archive days failed: {failed[:5]}")
dbutils.notebook.exit(json.dumps(summary, default=str))
