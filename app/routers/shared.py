"""Shared helpers, constants, and utilities used across all router modules."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional


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
# SQL query helper — query gold tables via Databricks SQL
# ---------------------------------------------------------------------------
_CATALOG = "energy_copilot_catalog"
_sql_connection = None


def _get_sql_connection():
    """Lazily create a Databricks SQL connection using SDK auth."""
    global _sql_connection
    if _sql_connection is not None:
        try:
            _sql_connection.cursor().execute("SELECT 1")
            return _sql_connection
        except Exception:
            _sql_connection = None

    try:
        from databricks.sdk import WorkspaceClient
        from databricks import sql as dbsql

        w = WorkspaceClient()
        host = w.config.host.rstrip("/").replace("https://", "")
        token = w.config.authenticate().get("Authorization", "").replace("Bearer ", "")

        # Find the first available SQL warehouse
        warehouses = list(w.warehouses.list())
        wh_id = None
        for wh in warehouses:
            if wh.state and str(wh.state).upper() in ("RUNNING", "STARTING"):
                wh_id = wh.id
                break
        if not wh_id and warehouses:
            wh_id = warehouses[0].id
        if not wh_id:
            logger.warning("No SQL warehouse found")
            return None

        _sql_connection = dbsql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{wh_id}",
            access_token=token,
        )
        logger.info("SQL connection established to %s warehouse %s", host, wh_id)
        return _sql_connection
    except Exception as exc:
        logger.warning("Cannot establish SQL connection: %s", exc)
        return None


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
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
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
    """Run a SQL query and return list of dicts, or None on failure."""
    cache_key = f"sql:{hash(sql)}:{params}"
    cached = _cache_get(cache_key)
    if cached is not None:
        _set_last_source("sql-warehouse-cache", 0.0, sql)
        return cached

    conn = _get_sql_connection()
    if conn is None:
        return None
    t0 = time.monotonic()
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]
        elapsed = (time.monotonic() - t0) * 1000
        _set_last_source("sql-warehouse", elapsed, sql)
        # Only cache non-empty results to avoid caching transient misses
        if result:
            _cache_set(cache_key, result, ttl_seconds=25)
        return result
    except Exception as exc:
        elapsed = (time.monotonic() - t0) * 1000
        logger.warning("SQL query failed (%.1fms): %s — %s", elapsed, exc, sql[:120])
        _set_last_source("sql-warehouse-error", elapsed, sql)
        return None
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
