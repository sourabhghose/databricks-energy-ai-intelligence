"""Shared helpers, constants, and utilities used across all router modules."""

from __future__ import annotations

import json
import logging
import os
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


def _query_gold(sql: str, params: Optional[dict] = None) -> Optional[List[Dict[str, Any]]]:
    """Run a SQL query and return list of dicts, or None on failure."""
    cache_key = f"sql:{hash(sql)}:{params}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    conn = _get_sql_connection()
    if conn is None:
        return None
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]
        # Only cache non-empty results to avoid caching transient misses
        if result:
            _cache_set(cache_key, result, ttl_seconds=25)
        return result
    except Exception as exc:
        logger.warning("SQL query failed: %s — %s", exc, sql[:120])
        return None
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
