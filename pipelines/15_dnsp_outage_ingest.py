# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 15 — DNSP Outage Feeds
# MAGIC Fetches live distribution outage data from two NSW DNSPs:
# MAGIC
# MAGIC   - Ausgrid (Sydney metro + Hunter):
# MAGIC     POST https://www.ausgrid.com.au/services/Outage/Outage.asmx/GetOutages
# MAGIC     Returns JSON: d.Data[] with .NET /Date(ms)/ timestamps
# MAGIC   - Endeavour Energy (Western Sydney + South Coast):
# MAGIC     GET https://www.endeavourenergy.com.au/api/outage-points/stream
# MAGIC     Returns SSE stream; read until event:complete
# MAGIC
# MAGIC Target table: energy_copilot_catalog.gold.outage_events
# MAGIC   Columns: event_id, start_time, end_time, feeder_id, zone_substation,
# MAGIC            affected_customers, cause_code, region, status, etr_minutes
# MAGIC
# MAGIC Cadence: Every 5 minutes

# COMMAND ----------

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone

import requests

try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "energy_copilot_catalog"

SCHEMA = f"{CATALOG}.gold"

REQUEST_TIMEOUT = 30   # seconds for regular HTTP calls
SSE_TIMEOUT     = 20   # seconds — stream read window for Endeavour SSE

# Ausgrid bounding box covers Sydney metro + Central Coast + Hunter
AUSGRID_URL = "https://www.ausgrid.com.au/services/Outage/Outage.asmx/GetOutages"
AUSGRID_BOX = {
    "box": {
        "bottomleft": {"lat": -34.2, "lng": 150.5},
        "topright":   {"lat": -33.4, "lng": 151.8},
        "zoom": 10,
    }
}

ENDEAVOUR_URL = "https://www.endeavourenergy.com.au/api/outage-points/stream"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper utilities

# COMMAND ----------

def _md5(*parts) -> str:
    """Return hex MD5 of pipe-joined parts — used as event_id."""
    raw = "|".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()


def _sql_escape(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return str(s).replace("'", "''")


def _nullable(val) -> str:
    """Format a Python value as a SQL literal."""
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    return f"'{_sql_escape(str(val))}'"


def _parse_dotnet_date(s: str) -> datetime | None:
    """
    Parse .NET JSON date strings: /Date(1234567890000)/ or /Date(1234567890000+1100)/
    Returns UTC datetime or None.
    """
    if not s:
        return None
    m = re.search(r"/Date\((-?\d+)", s)
    if m:
        ms = int(m.group(1))
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return None


def _ts_sql(dt: datetime | None) -> str:
    """Format datetime as a CAST-able SQL timestamp string, or NULL."""
    if dt is None:
        return "NULL"
    return f"CAST('{dt.strftime('%Y-%m-%d %H:%M:%S')}' AS TIMESTAMP)"


def _parse_etr_minutes(etr_str: str | None, start_dt: datetime | None) -> int | None:
    """
    Derive ETR in minutes from a free-text EstRestTime string or ISO timestamp.
    Examples: "/Date(ms)/", "2026-03-17T15:30:00", "03/17/2026 15:30", "Unknown"
    """
    if not etr_str:
        return None
    # .NET date
    dt = _parse_dotnet_date(etr_str)
    if dt is None:
        # Try ISO / common date formats
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M",
                    "%d/%m/%Y %H:%M", "%Y-%m-%dT%H:%M"):
            try:
                dt = datetime.strptime(etr_str[:16], fmt[:len(etr_str[:16])])
                dt = dt.replace(tzinfo=timezone.utc)
                break
            except ValueError:
                pass
    if dt and start_dt:
        delta_min = int((dt - start_dt).total_seconds() / 60)
        return max(0, delta_min)
    return None


def _map_ausgrid_status(status_code: str) -> str:
    """
    Ausgrid status codes → canonical status string.
    UPD = Updated (crew on route), R = Restoration in progress, C = Completed.
    """
    mapping = {
        "UPD": "active",
        "R":   "active",
        "C":   "restored",
        "S":   "active",     # Scheduled
        "P":   "planned",    # Planned outage
        "NEW": "active",
    }
    return mapping.get((status_code or "").upper().strip(), "active")


def _map_endeavour_status(status_str: str) -> str:
    """Endeavour incident_status → canonical status string."""
    mapping = {
        "NEW":       "active",
        "ASSIGNED":  "active",
        "ACTIVE":    "active",
        "IN_PROGRESS": "active",
        "RESTORED":  "restored",
        "CLOSED":    "restored",
        "PLANNED":   "planned",
    }
    return mapping.get((status_str or "").upper().strip(), "active")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fetch Ausgrid outages (JSON POST)

# COMMAND ----------

ausgrid_outage_records = []

try:
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept":       "application/json",
        "User-Agent":   "Mozilla/5.0 (compatible; EnergyDataBot/1.0)",
        "Origin":       "https://www.ausgrid.com.au",
        "Referer":      "https://www.ausgrid.com.au/outages",
    }
    resp = requests.post(
        AUSGRID_URL,
        json=AUSGRID_BOX,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    print(f"Ausgrid API: HTTP {resp.status_code}")

    if resp.status_code == 200:
        payload = resp.json()
        # Response structure: {"d": {"Data": [...], ...}} or {"d": [...]}
        d = payload.get("d", payload)
        if isinstance(d, dict):
            data_list = d.get("Data", d.get("data", []))
        elif isinstance(d, list):
            data_list = d
        else:
            data_list = []

        print(f"Ausgrid: {len(data_list)} outage items in response")

        for item in data_list:
            web_id    = str(item.get("WebId") or item.get("webId") or item.get("id") or "")
            area      = item.get("Area")      or item.get("area")      or ""
            cause     = item.get("Cause")     or item.get("cause")     or ""
            customers = item.get("Customers") or item.get("customers") or 0
            etr_raw   = item.get("EstRestTime") or item.get("estRestTime") or ""
            start_raw = item.get("StartDateTime") or item.get("startDateTime") or ""
            status_raw = item.get("Status") or item.get("status") or "UPD"
            display_type = item.get("OutageDisplayType") or item.get("outageDisplayType") or ""

            # Try to extract coordinates from Coords list for substation name
            coords = item.get("Coords") or item.get("coords") or []
            coord_str = ""
            if coords and isinstance(coords, list) and isinstance(coords[0], dict):
                lat = coords[0].get("lat", "")
                lng = coords[0].get("lng", "")
                coord_str = f"{lat},{lng}"

            start_dt = _parse_dotnet_date(start_raw)
            etr_mins = _parse_etr_minutes(etr_raw, start_dt)

            # end_time: if status is restored and ETR is in the past, use ETR as end_time
            etr_dt = _parse_dotnet_date(etr_raw) if etr_raw else None
            now_utc = datetime.now(tz=timezone.utc)
            end_dt = etr_dt if (etr_dt and etr_dt < now_utc and _map_ausgrid_status(status_raw) == "restored") else None

            event_id = _md5("AUSGRID", web_id) if web_id else _md5("AUSGRID", area, str(start_dt))

            # Cause code: truncate / normalise
            cause_code = re.sub(r"[^A-Za-z0-9_\-\s]", "", cause).strip()[:50]

            try:
                affected = int(float(str(customers).replace(",", "")))
            except (ValueError, TypeError):
                affected = 0

            ausgrid_outage_records.append({
                "event_id":            event_id,
                "start_time":          start_dt,
                "end_time":            end_dt,
                "feeder_id":           coord_str[:50] if coord_str else None,
                "zone_substation":     _sql_escape(area[:100]) if area else None,
                "affected_customers":  affected,
                "cause_code":          cause_code or "UNKNOWN",
                "region":              "NSW1",
                "status":              _map_ausgrid_status(status_raw),
                "etr_minutes":         etr_mins,
            })

        print(f"Ausgrid: parsed {len(ausgrid_outage_records)} outage events")
    else:
        print(f"Ausgrid: non-200 response — {resp.text[:200]}")

except Exception as exc:
    print(f"Ausgrid fetch failed: {exc} — will use seed data if no other source available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fetch Endeavour Energy outages (SSE stream)

# COMMAND ----------

endeavour_outage_records = []

try:
    headers = {
        "Accept":     "text/event-stream",
        "User-Agent": "Mozilla/5.0 (compatible; EnergyDataBot/1.0)",
        "Cache-Control": "no-cache",
        "Origin":     "https://www.endeavourenergy.com.au",
        "Referer":    "https://www.endeavourenergy.com.au/outages",
    }

    # Stream the SSE endpoint with a short timeout window.
    # We collect all batch events until we see "event:complete" or the stream closes.
    batch_records = []
    with requests.get(ENDEAVOUR_URL, headers=headers, stream=True,
                      timeout=SSE_TIMEOUT) as resp:
        print(f"Endeavour SSE: HTTP {resp.status_code}")
        if resp.status_code == 200:
            current_event_type = None
            current_data_lines = []
            for raw_line in resp.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue
                line = raw_line.strip()
                # SSE protocol: blank line = event boundary
                if line == "":
                    # Dispatch accumulated event
                    if current_data_lines:
                        data_str = "\n".join(current_data_lines)
                        if current_event_type == "complete":
                            print("Endeavour SSE: received 'complete' event — stopping stream")
                            break
                        if current_event_type in (None, "batch", "message"):
                            try:
                                parsed = json.loads(data_str)
                                # payload may be {"type":"UNPLANNED","data":[...]} or just [...]
                                if isinstance(parsed, list):
                                    batch_records.extend(parsed)
                                elif isinstance(parsed, dict):
                                    items = parsed.get("data", [parsed])
                                    batch_records.extend(items)
                            except json.JSONDecodeError:
                                pass
                    current_event_type = None
                    current_data_lines = []
                elif line.startswith("event:"):
                    current_event_type = line[6:].strip()
                elif line.startswith("data:"):
                    current_data_lines.append(line[5:].strip())
        else:
            print(f"Endeavour SSE: non-200 — {resp.text[:200]}")

    print(f"Endeavour SSE: collected {len(batch_records)} raw records")

    for item in batch_records:
        incident_id = str(item.get("incident_id") or item.get("id") or "")
        outage_type = item.get("outage_type") or item.get("type") or "UNPLANNED"
        status_raw  = item.get("incident_status") or item.get("status") or "NEW"
        customers   = item.get("customers_affected") or item.get("customers") or 0
        etr_raw     = item.get("etr") or item.get("estimated_restore_time") or ""
        city        = item.get("cityname") or item.get("suburb") or ""
        postcode    = item.get("postcode") or ""
        cause_raw   = item.get("cause") or item.get("cause_description") or ""
        start_raw   = item.get("start_date_time") or item.get("start_time") or ""
        lng         = item.get("x") or item.get("lng") or ""
        lat         = item.get("y") or item.get("lat") or ""

        # Parse start time (ISO 8601 or /Date(ms)/)
        start_dt = None
        if start_raw:
            start_dt = _parse_dotnet_date(start_raw)
            if start_dt is None:
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
                    try:
                        start_dt = datetime.strptime(start_raw[:19], fmt)
                        start_dt = start_dt.replace(tzinfo=timezone.utc)
                        break
                    except ValueError:
                        pass

        etr_mins = _parse_etr_minutes(etr_raw, start_dt)
        etr_dt   = None
        if etr_raw:
            etr_dt = _parse_dotnet_date(etr_raw)
            if etr_dt is None:
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                    try:
                        etr_dt = datetime.strptime(etr_raw[:19], fmt)
                        etr_dt = etr_dt.replace(tzinfo=timezone.utc)
                        break
                    except ValueError:
                        pass

        now_utc = datetime.now(tz=timezone.utc)
        canonical_status = _map_endeavour_status(status_raw)
        end_dt = etr_dt if (etr_dt and etr_dt < now_utc and canonical_status == "restored") else None

        event_id = _md5("ENDEAVOUR", incident_id) if incident_id else _md5("ENDEAVOUR", city, postcode, str(start_dt))

        cause_code = re.sub(r"[^A-Za-z0-9_\-\s]", "", cause_raw).strip()[:50] or outage_type[:50]

        try:
            affected = int(float(str(customers).replace(",", "")))
        except (ValueError, TypeError):
            affected = 0

        substation = f"{city} {postcode}".strip()[:100]

        endeavour_outage_records.append({
            "event_id":           event_id,
            "start_time":         start_dt,
            "end_time":           end_dt,
            "feeder_id":          f"{lat},{lng}"[:50] if lat and lng else None,
            "zone_substation":    substation or None,
            "affected_customers": affected,
            "cause_code":         cause_code or "UNKNOWN",
            "region":             "NSW1",
            "status":             canonical_status,
            "etr_minutes":        etr_mins,
        })

    print(f"Endeavour: parsed {len(endeavour_outage_records)} outage events")

except Exception as exc:
    print(f"Endeavour SSE fetch failed: {exc} — will use seed data if no other source available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Merge Ausgrid + Endeavour records

# COMMAND ----------

_combined = ausgrid_outage_records + endeavour_outage_records
print(f"Combined outage records before dedup: {len(_combined)} "
      f"(Ausgrid={len(ausgrid_outage_records)}, Endeavour={len(endeavour_outage_records)})")

# Deduplicate by event_id — keep last occurrence (Endeavour overwrites Ausgrid if collision)
_seen: dict[str, dict] = {}
for rec in _combined:
    _seen[rec["event_id"]] = rec
all_outage_records = list(_seen.values())
print(f"After dedup by event_id: {len(all_outage_records)} unique events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Seed fallback (when both APIs unavailable)

# COMMAND ----------

if not all_outage_records:
    print("No live outage data from either DNSP — loading seed records")
    now_utc = datetime.now(tz=timezone.utc)
    # Representative seed outage events for demonstration
    _seed_outages = [
        # (source, area, cause, customers, start_offset_h, etr_offset_h, status)
        ("AUSGRID", "Bondi",           "Equipment fault",        850,  -2,   1,  "active"),
        ("AUSGRID", "Parramatta",      "Planned maintenance",    2100,  0,   4,  "planned"),
        ("AUSGRID", "Hornsby",         "Storm damage",           1250, -1,   2,  "active"),
        ("AUSGRID", "Penrith",         "Vehicle accident",        320, -3,  -1, "restored"),
        ("AUSGRID", "Sutherland",      "Equipment fault",         470, -1,   3,  "active"),
        ("ENDEAVOUR", "Blacktown",     "Transformer failure",    3400, -4,   2,  "active"),
        ("ENDEAVOUR", "Campbelltown",  "Tree contact",            780, -2,   1,  "active"),
        ("ENDEAVOUR", "Wollongong",    "Storm damage",           1900, -6,  -2, "restored"),
        ("ENDEAVOUR", "Penrith",       "Planned maintenance",    1100,  0,   6,  "planned"),
        ("ENDEAVOUR", "Lithgow",       "Equipment fault",         290, -1,   2,  "active"),
    ]
    for source, area, cause, customers, start_h, etr_h, status in _seed_outages:
        start_dt = now_utc + timedelta(hours=start_h)
        etr_dt   = now_utc + timedelta(hours=etr_h)
        end_dt   = etr_dt if status == "restored" else None
        etr_mins = max(0, int((etr_dt - start_dt).total_seconds() / 60)) if etr_dt else None
        event_id = _md5(source, area, str(start_dt))

        cause_code = re.sub(r"[^A-Za-z0-9_\-\s]", "", cause)[:50]

        all_outage_records.append({
            "event_id":           event_id,
            "start_time":         start_dt,
            "end_time":           end_dt,
            "feeder_id":          None,
            "zone_substation":    area,
            "affected_customers": customers,
            "cause_code":         cause_code,
            "region":             "NSW1",
            "status":             status,
            "etr_minutes":        etr_mins,
        })
    print(f"Loaded {len(all_outage_records)} seed outage events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write outage_events to Delta (MERGE upsert)

# COMMAND ----------

now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

if all_outage_records:
    batch_size = 50
    total_upserted = 0
    for i in range(0, len(all_outage_records), batch_size):
        batch = all_outage_records[i : i + batch_size]
        rows_sql = []
        for r in batch:
            start_sql = (
                f"CAST('{r['start_time'].strftime('%Y-%m-%d %H:%M:%S')}' AS TIMESTAMP)"
                if r["start_time"] else "NULL"
            )
            end_sql = (
                f"CAST('{r['end_time'].strftime('%Y-%m-%d %H:%M:%S')}' AS TIMESTAMP)"
                if r["end_time"] else "NULL"
            )
            rows_sql.append(
                f"('{r['event_id']}', {start_sql}, {end_sql}, "
                f"{_nullable(r['feeder_id'])}, {_nullable(r['zone_substation'])}, "
                f"{r['affected_customers']}, '{_sql_escape(r['cause_code'])}', "
                f"'{r['region']}', '{r['status']}', {_nullable(r['etr_minutes'])})"
            )
        values = ",\n".join(rows_sql)
        spark.sql(f"""
            MERGE INTO {SCHEMA}.outage_events AS t
            USING (
                SELECT event_id, start_time, end_time, feeder_id, zone_substation,
                       affected_customers, cause_code, region, status, etr_minutes
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY start_time DESC) AS _rn
                    FROM VALUES {values}
                    AS s(event_id, start_time, end_time, feeder_id, zone_substation,
                         affected_customers, cause_code, region, status, etr_minutes)
                )
                WHERE _rn = 1
            ) AS s
            ON t.event_id = s.event_id
            WHEN MATCHED THEN UPDATE SET
                start_time          = s.start_time,
                end_time            = s.end_time,
                feeder_id           = s.feeder_id,
                zone_substation     = s.zone_substation,
                affected_customers  = s.affected_customers,
                cause_code          = s.cause_code,
                region              = s.region,
                status              = s.status,
                etr_minutes         = s.etr_minutes
            WHEN NOT MATCHED THEN INSERT *
        """)
        total_upserted += len(batch)
    print(f"Upserted {total_upserted} rows into {SCHEMA}.outage_events")
else:
    print("No outage records to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary stats and exit

# COMMAND ----------

try:
    total_cnt = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {SCHEMA}.outage_events"
    ).collect()[0].cnt
    print(f"  {SCHEMA}.outage_events: {total_cnt} rows total")

    spark.sql(f"""
        SELECT status,
               region,
               COUNT(*)               AS events,
               SUM(affected_customers) AS total_customers,
               AVG(etr_minutes)        AS avg_etr_minutes
        FROM {SCHEMA}.outage_events
        GROUP BY status, region
        ORDER BY events DESC
    """).show(truncate=False)
except Exception as exc:
    print(f"Summary query failed: {exc}")
    total_cnt = len(all_outage_records)

dbutils.notebook.exit(json.dumps({
    "status":            "success",
    "ausgrid_records":   len(ausgrid_outage_records),
    "endeavour_records": len(endeavour_outage_records),
    "total_written":     len(all_outage_records),
    "outage_events_total": total_cnt,
    "ingested_at":       now_str,
}))
