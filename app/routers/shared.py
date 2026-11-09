"""Shared helpers, constants, and utilities used across all router modules."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, NoReturn, Optional

import functools
import inspect

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    _SKIP = frozenset({
        "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno",
        "funcName", "created", "msecs", "relativeCreated", "thread",
        "threadName", "processName", "process", "message", "name", "taskName",
    })

    def format(self, record: logging.LogRecord) -> str:
        obj: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "message":   record.getMessage(),
            "module":    record.module,
        }
        if record.exc_info:
            obj["exception"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k not in self._SKIP:
                obj[k] = v
        return json.dumps(obj, default=str)


def _configure_logging(level: str = "INFO") -> logging.Logger:
    lg = logging.getLogger("energy_copilot")
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(_JsonFormatter())
        lg.addHandler(h)
    lg.setLevel(getattr(logging, level.upper(), logging.INFO))
    lg.propagate = False
    return lg


logger = _configure_logging(os.environ.get("LOG_LEVEL", "INFO"))

# ---------------------------------------------------------------------------
# Environment / config
# ---------------------------------------------------------------------------
ALLOW_ORIGINS: List[str] = os.getenv("ALLOW_ORIGINS", "*").split(",")
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "60"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))
_rate_limit_store: dict[str, list[float]] = defaultdict(list)

# ---------------------------------------------------------------------------
# Simple in-memory TTL cache
# ---------------------------------------------------------------------------
_cache: Dict[str, Dict[str, Any]] = {}


def _cache_get(key: str) -> Optional[Any]:
    entry = _cache.get(key)
    if entry is None:
        return None
    if time.monotonic() > entry["expires_at"]:
        del _cache[key]
        return None
    return entry["data"]


def _cache_set(key: str, data: Any, ttl_seconds: float = 3600.0) -> None:
    _cache[key] = {"data": data, "expires_at": time.monotonic() + ttl_seconds}


# ---------------------------------------------------------------------------
# Data source tracking — lets endpoints report which backend served the data
# ---------------------------------------------------------------------------
_local = threading.local()
_query_stats: Dict[str, int] = defaultdict(int)  # global counters


def _set_last_source(source: str, elapsed_ms: float, sql_snippet: str = "") -> None:
    """Record which backend served the last query (per-thread)."""
    _local.last_source = source
    _local.last_elapsed_ms = elapsed_ms
    _query_stats[source] += 1
    logger.info("query_source=%s elapsed_ms=%.1f rows_from=%s sql=%s",
                source, elapsed_ms, source, sql_snippet[:80])


def _get_last_source() -> str:
    """Return the source that served the most recent query in this thread."""
    return getattr(_local, "last_source", "unknown")


def _get_last_elapsed_ms() -> float:
    return getattr(_local, "last_elapsed_ms", -1.0)


def _get_query_stats() -> Dict[str, int]:
    """Return cumulative query counts by source since process start."""
    return dict(_query_stats)


# ---------------------------------------------------------------------------
# NEM constants
# ---------------------------------------------------------------------------
_NEM_REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
_REGION_BASE_PRICES = {"NSW1": 72.5, "QLD1": 65.3, "VIC1": 55.8, "SA1": 88.1, "TAS1": 42.0}
_AEST = timezone(timedelta(hours=10))


# ---------------------------------------------------------------------------
# SQL query helper — query gold tables via Databricks Statement Execution API
# ---------------------------------------------------------------------------
_CATALOG = os.environ.get("DATABRICKS_CATALOG", "energy_copilot_catalog")
_SQL_WAREHOUSE_ID = os.environ.get("SQL_WAREHOUSE_ID", "")

# WorkspaceClient singleton — used for both SQL and Lakebase cred generation
_workspace_client = None
_ws_client_lock = threading.Lock()
_ws_unavailable_until: float = 0.0


def _get_workspace_client():
    """Lazily create a singleton WorkspaceClient. Thread-safe."""
    global _workspace_client, _ws_unavailable_until
    if _ws_unavailable_until and time.monotonic() < _ws_unavailable_until:
        return None
    if _workspace_client is not None:
        return _workspace_client
    with _ws_client_lock:
        if _workspace_client is not None:
            return _workspace_client
        if _ws_unavailable_until and time.monotonic() < _ws_unavailable_until:
            return None
        try:
            from databricks.sdk import WorkspaceClient
            _workspace_client = WorkspaceClient()
            logger.info("WorkspaceClient initialized: host=%s", _workspace_client.config.host)
            return _workspace_client
        except Exception as exc:
            logger.warning("Cannot initialize WorkspaceClient: %s", exc)
            _ws_unavailable_until = time.monotonic() + 30.0
            return None


def _get_sql_connection():
    """Compatibility shim — returns truthy if SQL warehouse is reachable."""
    return _get_workspace_client()


# ---------------------------------------------------------------------------
# Lakebase (Postgres) query helper — sub-10ms reads when provisioned
# ---------------------------------------------------------------------------
_lakebase_pool = None
_lakebase_last_error: Optional[str] = None


_lakebase_token = None
_lakebase_token_expires = 0.0


def _get_lakebase_token():
    """Generate or return cached OAuth token for Lakebase Postgres."""
    global _lakebase_token, _lakebase_token_expires
    if _lakebase_token and time.monotonic() < _lakebase_token_expires:
        return _lakebase_token
    instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME")
    if not instance_name:
        return None
    try:
        import uuid

        w = _get_workspace_client()
        if w is None:
            return None
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[instance_name],
        )
        _lakebase_token = cred.token
        _lakebase_token_expires = time.monotonic() + 50 * 60  # refresh 10 min before expiry
        logger.info("Lakebase OAuth token generated for %s", instance_name)
        return _lakebase_token
    except Exception as exc:
        logger.warning("Lakebase token generation failed: %s", exc)
        return None


def _get_lakebase_pool():
    """Lazily create a psycopg3 connection pool for Lakebase with OAuth token.

    Prefers PG* env vars auto-injected by Databricks Apps when a database
    resource is registered, falling back to LAKEBASE_* manual config.
    Uses psycopg3 (not psycopg2) because Lakebase on port 443 requires
    TLS handling that psycopg2 cannot negotiate.  psycopg3 + hostaddr works.
    """
    global _lakebase_pool, _lakebase_last_error
    if _lakebase_pool is not None:
        return _lakebase_pool

    # Prefer PG* env vars (auto-set by Databricks Apps resource), fall back to LAKEBASE_*
    host = os.environ.get("PGHOST") or os.environ.get("LAKEBASE_HOST")
    if not host:
        _lakebase_last_error = "Neither PGHOST nor LAKEBASE_HOST set"
        return None

    token = _get_lakebase_token()
    if not token:
        _lakebase_last_error = "Token generation failed"
        return None

    try:
        import socket

        from psycopg_pool import ConnectionPool

        # Get username: PGUSER (Apps-injected) > SDK > LAKEBASE_USER
        username = os.environ.get("PGUSER")
        if not username:
            try:
                from databricks.sdk import WorkspaceClient
                username = WorkspaceClient().current_user.me().user_name
            except Exception:
                username = os.environ.get("LAKEBASE_USER", "databricks")

        port = int(os.environ.get("PGPORT") or os.environ.get("LAKEBASE_PORT", "443"))
        dbname = os.environ.get("PGDATABASE") or os.environ.get("LAKEBASE_DATABASE", "energy_copilot_db")
        sslmode = os.environ.get("PGSSLMODE", "require")

        # Resolve DNS to IP — required for psycopg3 hostaddr param
        ip = socket.gethostbyname(host)
        logger.info("Lakebase pool connecting: host=%s ip=%s port=%d db=%s user=%s sslmode=%s",
                     host, ip, port, dbname, username, sslmode)

        conninfo = (
            f"host={host} hostaddr={ip} port={port} dbname={dbname} "
            f"user={username} password={token} sslmode={sslmode} connect_timeout=10"
        )

        _lakebase_pool = ConnectionPool(
            conninfo=conninfo,
            min_size=1,
            max_size=5,
            open=False,
        )
        _lakebase_pool.open(wait=False)
        _lakebase_last_error = None
        logger.info("Lakebase psycopg3 pool created and opened: %s -> %s", host, ip)
        return _lakebase_pool
    except Exception as exc:
        _lakebase_last_error = f"{type(exc).__name__}: {exc}"
        logger.warning("Lakebase pool creation failed: %s", _lakebase_last_error)
        return None


def _get_lakebase_last_error() -> Optional[str]:
    return _lakebase_last_error


def _query_lakebase(sql: str, params: Optional[tuple] = None) -> Optional[List[Dict[str, Any]]]:
    """Run a query against Lakebase (Postgres) via psycopg3 pool."""
    cache_key = f"lb:{hash(sql)}:{params}"
    cached = _cache_get(cache_key)
    if cached is not None:
        _set_last_source("lakebase-cache", 0.0, sql)
        return cached

    pool = _get_lakebase_pool()
    if pool is None:
        return None

    t0 = time.monotonic()
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                result = [dict(zip(columns, row)) for row in rows]
                elapsed = (time.monotonic() - t0) * 1000
                _set_last_source("lakebase", elapsed, sql)
                if result:
                    _cache_set(cache_key, result, ttl_seconds=10)
                return result
    except Exception as exc:
        elapsed = (time.monotonic() - t0) * 1000
        logger.warning("Lakebase query failed (%.1fms): %s — %s", elapsed, exc, sql[:120])
        _set_last_source("lakebase-error", elapsed, sql)
        # Token may have expired — reset pool so next call gets fresh token
        global _lakebase_pool, _lakebase_token_expires
        _lakebase_pool = None
        _lakebase_token_expires = 0.0
        return None


_lakebase_freshness: Optional[float] = None  # monotonic time of last freshness check
_lakebase_fresh: bool = False


def _is_lakebase_fresh(max_lag_minutes: float = 30.0) -> bool:
    """Check if Lakebase synced data is fresh enough (cached for 60s)."""
    global _lakebase_freshness, _lakebase_fresh
    now = time.monotonic()
    if _lakebase_freshness is not None and now - _lakebase_freshness < 60:
        return _lakebase_fresh
    _lakebase_freshness = now
    row = _query_lakebase(
        "SELECT MAX(interval_datetime) AS latest FROM gold.nem_prices_5min_dedup_synced"
    )
    if not row or not row[0].get("latest"):
        _lakebase_fresh = False
        return False
    latest = row[0]["latest"]
    if hasattr(latest, "timestamp"):
        from datetime import timezone as _tz
        age_min = (datetime.now(_tz.utc) - latest.replace(tzinfo=_tz.utc)).total_seconds() / 60
    else:
        _lakebase_fresh = False
        return False
    _lakebase_fresh = age_min <= max_lag_minutes
    if not _lakebase_fresh:
        logger.info("Lakebase stale: latest=%s age=%.0fmin (max=%s)", latest, age_min, max_lag_minutes)
    return _lakebase_fresh


def _query_lakebase_fresh(sql: str, params=None, max_lag_minutes: float = 30.0) -> Optional[List[Dict[str, Any]]]:
    """Query Lakebase only if synced data is fresh enough, else return None."""
    if not _is_lakebase_fresh(max_lag_minutes):
        return None
    return _query_lakebase(sql, params)


def _query_with_fallback(sql_lb: str, sql_wh: str, params_lb=None, params_wh=None) -> Optional[List[Dict[str, Any]]]:
    """Try Lakebase first (if fresh), fall back to SQL Warehouse."""
    result = _query_lakebase_fresh(sql_lb, params_lb)
    if result is not None:
        return result
    return _query_gold(sql_wh, params_wh)


def _query_snapshot(endpoint_path: str, region: str = "ALL") -> Optional[Dict[str, Any]]:
    """Read a pre-computed JSON snapshot for an endpoint from dashboard_snapshots.

    Tries Lakebase first (sub-10ms), then SQL Warehouse fallback.
    Returns parsed JSON dict or None on miss/error.
    """
    cache_key = f"snap:{endpoint_path}:{region}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # Try Lakebase first (psycopg3 pool)
    pool = _get_lakebase_pool()
    if pool is not None:
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT payload_json FROM gold.dashboard_snapshots_synced WHERE endpoint_path = %s AND region = %s",
                        (endpoint_path, region),
                    )
                    row = cur.fetchone()
                    if row:
                        result = json.loads(row[0])
                        _cache_set(cache_key, result, ttl_seconds=30)
                        return result
        except Exception as exc:
            logger.debug("Lakebase snapshot miss for %s: %s", endpoint_path, exc)

    # Fallback to SQL Warehouse
    try:
        rows = _query_gold(
            f"SELECT payload_json FROM {_CATALOG}.gold.dashboard_snapshots "
            f"WHERE endpoint_path = '{endpoint_path}' AND region = '{region}' LIMIT 1"
        )
        if rows and rows[0].get("payload_json"):
            result = json.loads(rows[0]["payload_json"])
            _cache_set(cache_key, result, ttl_seconds=30)
            return result
    except Exception as exc:
        logger.debug("SQL snapshot miss for %s: %s", endpoint_path, exc)

    return None


def _query_gold(sql: str, params: Optional[dict] = None) -> Optional[List[Dict[str, Any]]]:
    """Run a SQL query via Statement Execution API and return list of dicts."""
    cache_key = f"sql:{hash(sql)}:{params}"
    cached = _cache_get(cache_key)
    if cached is not None:
        _set_last_source("sql-warehouse-cache", 0.0, sql)
        return cached

    w = _get_workspace_client()
    if w is None:
        return None
    t0 = time.monotonic()
    try:
        from databricks.sdk.service.sql import Format, Disposition
        resp = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=_SQL_WAREHOUSE_ID,
            wait_timeout="30s",
            format=Format.JSON_ARRAY,
            disposition=Disposition.INLINE,
        )
        state_val = resp.status.state.value if hasattr(resp.status.state, 'value') else str(resp.status.state)
        if state_val != "SUCCEEDED":
            err = resp.status.error if resp.status else "unknown"
            logger.warning("SQL statement failed: state=%s error=%s sql=%s",
                           state_val, err, sql[:120])
            return None
        manifest = resp.manifest
        result_data = resp.result
        logger.info("SQL result: manifest=%s result_data=%s sql=%s",
                    bool(manifest), bool(result_data), sql[:80])
        if not manifest or not manifest.schema or not manifest.schema.columns:
            return []
        columns = [col.name for col in manifest.schema.columns]
        # Build type converters from schema
        _INT_TYPES = {"INT", "INTEGER", "LONG", "BIGINT", "SMALLINT", "TINYINT", "SHORT"}
        _FLOAT_TYPES = {"DOUBLE", "FLOAT", "DECIMAL", "REAL", "NUMERIC"}
        _BOOL_TYPES = {"BOOLEAN", "BOOL"}
        col_types = []
        for col in manifest.schema.columns:
            tn = getattr(col, "type_name", None)
            # SDK returns an enum; extract .value if present (e.g. ColumnInfoTypeName.DOUBLE → "DOUBLE")
            type_name = (tn.value if hasattr(tn, "value") else str(tn or "")).upper()
            if type_name in _INT_TYPES:
                col_types.append("int")
            elif type_name in _FLOAT_TYPES:
                col_types.append("float")
            elif type_name in _BOOL_TYPES:
                col_types.append("bool")
            else:
                col_types.append("str")

        def _coerce(val, typ):
            if val is None:
                return None
            try:
                if typ == "int":
                    return int(val)
                if typ == "float":
                    return float(val)
                if typ == "bool":
                    return str(val).lower() in ("true", "1")
            except (ValueError, TypeError):
                pass
            return val

        rows = []
        if result_data and result_data.data_array:
            for row in result_data.data_array:
                rows.append({col: _coerce(val, typ) for col, val, typ in zip(columns, row, col_types)})
        elapsed = (time.monotonic() - t0) * 1000
        _set_last_source("sql-warehouse", elapsed, sql)
        logger.info("SQL query returned %d rows in %.0fms", len(rows), elapsed)
        if rows:
            _cache_set(cache_key, rows, ttl_seconds=25)
        return rows
    except Exception as exc:
        elapsed = (time.monotonic() - t0) * 1000
        logger.warning("SQL query failed (%.1fms): %s — %s", elapsed, exc, sql[:120])
        _set_last_source("sql-warehouse-error", elapsed, sql)
        return None


def _no_real_data(what: str) -> NoReturn:
    """Fail loudly instead of serving illustrative numbers when no real data could be read."""
    logger.warning("no real data for %s — returning 503 (illustrative fallback disabled)", what)
    raise HTTPException(status_code=503, detail=f"{what}: no real data available right now (the query failed or "
                                                "returned no rows). Illustrative fallbacks are disabled.")


SAMPLE_HEADER = "X-Data-Source"


def _sample_data(reason: str):
    """Mark an endpoint whose payload contains illustrative (non-real) values.

    Adds ``data_source: "sample"`` and ``sample_reason`` to dict payloads and an ``X-Data-Source: sample`` header to
    every response, so the UI can show a "sample data" banner. Use only where no real source is ingested yet.
    """
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            out = await fn(*args, **kwargs) if inspect.iscoroutinefunction(fn) else fn(*args, **kwargs)
            if isinstance(out, JSONResponse):
                out.headers[SAMPLE_HEADER] = "sample"
                return out
            body = jsonable_encoder(out)
            if isinstance(body, dict):
                body = {**body, "data_source": "sample", "sample_reason": reason}
            return JSONResponse(content=body, headers={SAMPLE_HEADER: "sample"})
        return wrapper
    return decorator


async def _query_gold_async(sql: str, params: Optional[dict] = None) -> Optional[List[Dict[str, Any]]]:
    """Non-blocking version of _query_gold — runs in a thread pool."""
    import asyncio
    return await asyncio.to_thread(_query_gold, sql, params)


# ---------------------------------------------------------------------------
# SQL write helpers — INSERT / UPDATE / DELETE via SQL Warehouse
# ---------------------------------------------------------------------------

def _execute_gold(sql: str, params: Optional[dict] = None) -> bool:
    """Execute an INSERT/UPDATE/DELETE via Statement Execution API. Returns True on success."""
    w = _get_workspace_client()
    if w is None:
        return False
    t0 = time.monotonic()
    try:
        resp = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=_SQL_WAREHOUSE_ID,
            wait_timeout="30s",
        )
        elapsed = (time.monotonic() - t0) * 1000
        state_val = resp.status.state.value if hasattr(resp.status.state, 'value') else str(resp.status.state)
        if state_val == "SUCCEEDED":
            logger.info("SQL write (%.1fms): %s", elapsed, sql[:120])
            return True
        err = resp.status.error if resp.status else "unknown"
        logger.warning("SQL write failed (%.1fms): state=%s error=%s sql=%s",
                       elapsed, state_val, err, sql[:120])
        return False
    except Exception as exc:
        elapsed = (time.monotonic() - t0) * 1000
        logger.warning("SQL write failed (%.1fms): %s — %s", elapsed, exc, sql[:120])
        return False


def _insert_gold(table: str, data: dict) -> bool:
    """INSERT a single row into a gold table from a dict."""
    cols = ", ".join(data.keys())
    vals = ", ".join(f"'{_sql_escape(v)}'" if isinstance(v, str)
                     else "NULL" if v is None
                     else str(v) for v in data.values())
    sql = f"INSERT INTO {table} ({cols}) VALUES ({vals})"
    return _execute_gold(sql)


def _insert_gold_batch(table: str, rows: List[dict]) -> int:
    """INSERT multiple rows in a single SQL statement. Returns count of rows inserted."""
    if not rows:
        return 0
    cols = ", ".join(rows[0].keys())

    def _fmt_val(v):
        if isinstance(v, str):
            return f"'{_sql_escape(v)}'"
        if v is None:
            return "NULL"
        return str(v)

    # Batch in chunks of 50 to stay within SQL statement size limits
    inserted = 0
    chunk_size = 50
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        value_rows = ", ".join(
            "(" + ", ".join(_fmt_val(v) for v in row.values()) + ")"
            for row in chunk
        )
        sql = f"INSERT INTO {table} ({cols}) VALUES {value_rows}"
        if _execute_gold(sql):
            inserted += len(chunk)
    return inserted


def _update_gold(table: str, data: dict, where: str) -> bool:
    """UPDATE rows in a gold table. `where` is a raw SQL WHERE clause."""
    set_clause = ", ".join(
        f"{k} = '{_sql_escape(v)}'" if isinstance(v, str)
        else f"{k} = NULL" if v is None
        else f"{k} = {v}"
        for k, v in data.items()
    )
    sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
    return _execute_gold(sql)


def _sql_escape(value: str) -> str:
    """Minimal SQL string escape to prevent injection."""
    return value.replace("'", "''").replace("\\", "\\\\")


def _invalidate_cache(prefix: str) -> None:
    """Remove all cache entries whose key starts with `prefix`."""
    keys_to_remove = [k for k in _cache if k.startswith(prefix)]
    for k in keys_to_remove:
        del _cache[k]
