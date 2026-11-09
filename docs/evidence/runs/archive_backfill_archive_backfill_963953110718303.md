# Run evidence — archive_backfill / task `archive_backfill`

| field | value |
|---|---|
| job run id | `161620000649581` |
| task run id | `963953110718303` |
| job | [dev sourabh_ghose] [dev] 36 — Historical NEMWEB Backfill |
| notebook | `/Workspace/Users/sourabh.ghose@databricks.com/.bundle/energy-copilot/dev/files/pipelines/36_nemweb_archive_backfill` |
| started | 2026-11-09 02:48:27 UTC |
| result | **SUCCESS** |
| run page | https://fevm-serverless-stable-2b9lj3.cloud.databricks.com/?o=7474644340067319#job/522353803163446/run/963953110718303 |

Exported from the Databricks Jobs API with cell outputs as they were produced during the run.

## Cell 2

```python
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from nemweb_parse import iter_archive_day, to_bronze_row  # pipelines/nemweb_parse.py

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
# … 11 more lines
```

## Cell 3

```python
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
```

**Output:**

```text
Archive days selected: 90 (20260810 → 20261107) | files already landed: 589
```

## Cell 4

```python
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
# … 10 more lines
```

## Cell 5

```python
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
```

**Output:**

```text
{"days_requested": 90, "days_failed": 0, "failed": [], "rows_written": 128860, "files_skipped_already_landed": 148, "bronze_coverage_aest": {"first_interval": "2026/08/10 00:05:00", "last_interval": "2026/11/09 12:50:00", "rows": 131805, "files": 26361}}
```
