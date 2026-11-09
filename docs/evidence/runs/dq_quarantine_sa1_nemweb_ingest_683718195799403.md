# Run evidence — dq_quarantine_sa1 / task `nemweb_ingest`

| field | value |
|---|---|
| job run id | `645430909659288` |
| task run id | `683718195799403` |
| job | [dev sourabh_ghose] [dev] 01 — NEMWEB Ingest |
| notebook | `/Workspace/Users/sourabh.ghose@databricks.com/.bundle/energy-copilot/dev/files/pipelines/01_nemweb_price_ingest` |
| started | 2026-11-09 01:40:04 UTC |
| result | **SUCCESS** |
| run page | https://fevm-serverless-stable-2b9lj3.cloud.databricks.com/?o=7474644340067319#job/741084016505266/run/683718195799403 |

Exported from the Databricks Jobs API with cell outputs as they were produced during the run.

## Cell 2

```python
import csv
import io
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

dbutils.widgets.text("catalog", "energy_copilot_catalog")
dbutils.widgets.text("max_files", "288")  # bootstrap window: 288 files = last 24h

CATALOG = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(f"Invalid catalog name: {CATALOG!r}")
MAX_FILES = int(dbutils.widgets.get("max_files"))

BRONZE = f"{CATALOG}.bronze.nemweb_dispatch_price"
LISTING_URL = "https://www.nemweb.com.au/REPORTS/CURRENT/DispatchIS_Reports/"
FILE_PATTERN = re.compile(r'href="([^"]*?(PUBLIC_DISPATCHIS_\d{12}_\d+\.zip))"', re.IGNORECASE)
# … 7 more lines
```

## Cell 4

```python
existing_cols = {f.name for f in spark.table(BRONZE).schema.fields}
for col, comment in EXTRA_COLUMNS.items():
    if col not in existing_cols:
        spark.sql(f"ALTER TABLE {BRONZE} ADD COLUMNS ({col} STRING COMMENT '{comment}')")
        print(f"Added column {col}")
```

## Cell 6

```python
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
```

**Output:**

```text
Remote files: 575 | already landed: 287 | to fetch: 288
```

## Cell 8

```python
@dataclass(frozen=True)
class PriceRecord:
    source_file: str
    settlement_datetime: str
    run_no: str
    region_id: str
    rrp: str
    intervention: str
    last_changed: str
    total_demand: str | None
    available_generation: str | None
    net_interchange: str | None


def parse_dispatchis(name: str, payload: bytes) -> list[PriceRecord]:
    """Parse DISPATCH,PRICE rows and enrich with DISPATCH,REGIONSUM using each record's I-header."""
    headers: dict[str, list[str]] = {}
    prices: list[dict[str, str]] = []
    regionsum: dict[tuple[str, str, str, str], dict[str, str]] = {}
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        for member in zf.namelist():
            text = zf.read(member).decode("utf-8", errors="replace")
            for row in csv.reader(io.StringIO(text)):
                if len(row) < 4 or row[1] != "DISPATCH" or row[2] not in ("PRICE", "REGIONSUM"):
                    continue
# … 40 more lines
```

**Output:**

```text
Parsed 1440 price rows from 288 files
```

## Cell 10

```python
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
rows = [
    (r.source_file, ingested_at, "DISPATCH,PRICE", r.settlement_datetime[:10], r.run_no, r.region_id,
     r.rrp, r.intervention, r.last_changed, r.settlement_datetime, r.total_demand,
     r.available_generation, r.net_interchange)
    for r in records
]
if rows:
    spark.createDataFrame(rows, schema=BRONZE_SCHEMA).write.mode("append").saveAsTable(BRONZE)
# … 16 more lines
```

**Output:**

```text
{"status": "success", "files_listed": 575, "files_fetched": 288, "files_failed": 0, "rows_written": 1440, "regions": ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"], "latest_settlement_datetime_aest": "2026/11/09 05:25:00", "target": "serverless_stable_2b9lj3_catalog.bronze.nemweb_dispatch_price"}
```
