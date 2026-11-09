# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 01 — NEMWEB Dispatch Price Ingest (live, every 5 minutes)
# MAGIC
# MAGIC Polls AEMO NEMWEB `/REPORTS/CURRENT/DispatchIS_Reports/`, downloads any 5-minute
# MAGIC DispatchIS ZIPs not yet landed, parses the `DISPATCH,PRICE` and `DISPATCH,REGIONSUM`
# MAGIC records and appends them to `{catalog}.bronze.nemweb_dispatch_price`.
# MAGIC
# MAGIC Job 34 (`13_nemweb_bronze_to_gold`) then applies data-quality rules and merges
# MAGIC valid rows into `{catalog}.gold.nem_prices_5min`.
# MAGIC
# MAGIC The notebook exits with a JSON summary (files fetched, rows written, latest interval)
# MAGIC so every run leaves auditable evidence in the job run output.

# COMMAND ----------

import json
import re
import zipfile
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from nemweb_parse import PriceRecord, parse_dispatchis_zip, to_bronze_row  # pipelines/nemweb_parse.py

dbutils.widgets.text("catalog", "energy_copilot_catalog")
dbutils.widgets.text("max_files", "288")  # bootstrap window: 288 files = last 24h

CATALOG = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"Invalid catalog name: {CATALOG!r}")
MAX_FILES = int(dbutils.widgets.get("max_files"))

BRONZE = f"{CATALOG}.bronze.nemweb_dispatch_price"
LISTING_URL = "https://www.nemweb.com.au/REPORTS/CURRENT/DispatchIS_Reports/"
FILE_PATTERN = re.compile(r'href="([^"]*?(PUBLIC_DISPATCHIS_\d{12}_\d+\.zip))"', re.IGNORECASE)
NEM_REGIONS = frozenset({"NSW1", "QLD1", "VIC1", "SA1", "TAS1"})
EXTRA_COLUMNS = {  # added to the original bronze schema; all raw strings
    "settlement_datetime": "Full SETTLEMENTDATE (market time, UTC+10) YYYY/MM/DD HH:MM:SS",
    "total_demand": "REGIONSUM.TOTALDEMAND (MW)",
    "available_generation": "REGIONSUM.AVAILABLEGENERATION (MW)",
    "net_interchange": "REGIONSUM.NETINTERCHANGE (MW)",
}

# COMMAND ----------
# MAGIC %md ## Schema evolution — add the columns this ingest needs (idempotent)

# COMMAND ----------

existing_cols = {f.name for f in spark.table(BRONZE).schema.fields}
for col, comment in EXTRA_COLUMNS.items():
    if col not in existing_cols:
        spark.sql(f"ALTER TABLE {BRONZE} ADD COLUMNS ({col} STRING COMMENT '{comment}')")
        print(f"Added column {col}")

# COMMAND ----------
# MAGIC %md ## Discover new files

# COMMAND ----------


def build_session() -> requests.Session:
    retry = Retry(total=4, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504))
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "energy-copilot-nemweb-ingest/1.0"})
    return session


def list_remote_files(session: requests.Session) -> list[tuple[str, str]]:
    resp = session.get(LISTING_URL, timeout=30)
    resp.raise_for_status()
    found = {name: urljoin(LISTING_URL, href) for href, name in FILE_PATTERN.findall(resp.text)}
    return sorted(found.items())  # file names sort chronologically (timestamp in name)


session = build_session()
remote = list_remote_files(session)
landed = {r[0] for r in spark.sql(f"SELECT DISTINCT _source_file FROM {BRONZE}").collect()}
pending = [(name, url) for name, url in remote if name not in landed][-MAX_FILES:]
print(f"Remote files: {len(remote)} | already landed: {len(landed)} | to fetch: {len(pending)}")

# COMMAND ----------
# MAGIC %md ## Download and parse

# COMMAND ----------


records: list[PriceRecord] = []
failed_files: list[dict[str, str]] = []
for name, url in pending:
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        records.extend(parse_dispatchis_zip(name, resp.content))
    except (requests.RequestException, zipfile.BadZipFile) as exc:
        failed_files.append({"file": name, "error": str(exc)[:200]})
        print(f"WARN failed {name}: {exc}")

print(f"Parsed {len(records)} price rows from {len(pending) - len(failed_files)} files")

# COMMAND ----------
# MAGIC %md ## Append to bronze

# COMMAND ----------

BRONZE_SCHEMA = StructType([
    StructField("_source_file", StringType()),
    StructField("_ingested_at", TimestampType()),
    StructField("i_type", StringType()),
    StructField("settlement_date", StringType()),
    StructField("run_no", StringType()),
    StructField("regionid", StringType()),
    StructField("rrp", StringType()),
    StructField("intervention", StringType()),
    StructField("lastchanged", StringType()),
    StructField("settlement_datetime", StringType()),
    StructField("total_demand", StringType()),
    StructField("available_generation", StringType()),
    StructField("net_interchange", StringType()),
])

ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
rows = [to_bronze_row(r, ingested_at) for r in records]
if rows:
    spark.createDataFrame(rows, schema=BRONZE_SCHEMA).write.mode("append").saveAsTable(BRONZE)

latest = max((r.settlement_datetime for r in records), default=None)
summary = {
    "status": "success" if rows or not pending else "failed",
    "files_listed": len(remote),
    "files_fetched": len(pending) - len(failed_files),
    "files_failed": len(failed_files),
    "rows_written": len(rows),
    "regions": sorted({r.region_id for r in records} & NEM_REGIONS),
    "latest_settlement_datetime_aest": latest,
    "target": BRONZE,
}
print(json.dumps(summary, indent=2))
if pending and not rows:
    raise RuntimeError(f"Fetched {len(pending)} files but wrote no rows: {failed_files[:3]}")
dbutils.notebook.exit(json.dumps(summary))
