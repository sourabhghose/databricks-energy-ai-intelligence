"""Deal Capture & Portfolio Management — Phase 2 (PRD 15.1).

Endpoints for trade CRUD, portfolio management, counterparties,
amendment audit trail, and bulk CSV import.
"""
from __future__ import annotations

import csv
import io
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, Query, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .shared import (
    _CATALOG,
    _NEM_REGIONS,
    _execute_gold,
    _insert_gold,
    _insert_gold_batch,
    _invalidate_cache,
    _query_gold,
    _query_lakebase_fresh,
    _sql_escape,
    _update_gold,
    logger,
)

router = APIRouter()

_SCHEMA = f"{_CATALOG}.gold"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_TRADE_TYPES = {"SPOT", "FORWARD", "SWAP", "FUTURE", "OPTION", "PPA", "REC"}
_PROFILES = {"FLAT", "PEAK", "OFF_PEAK", "SUPER_PEAK"}
_STATUSES = {"DRAFT", "CONFIRMED", "SETTLED", "CANCELLED", "PENDING_APPROVAL"}
_BUY_SELL = {"BUY", "SELL"}

# NEM peak hours: 07:00–22:00 AEST (weekdays)
_PEAK_START, _PEAK_END = 7, 22
# Super peak: 15:00–20:00 AEST (weekdays)
_SUPER_PEAK_START, _SUPER_PEAK_END = 15, 20


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TradeCreate(BaseModel):
    trade_type: str = Field(..., description="SPOT|FORWARD|SWAP|FUTURE|OPTION|PPA|REC")
    region: str = Field(..., description="NEM region: NSW1|QLD1|VIC1|SA1|TAS1")
    buy_sell: str = Field(..., description="BUY or SELL")
    volume_mw: float = Field(..., gt=0)
    price: float = Field(..., description="$/MWh")
    start_date: date
    end_date: date
    profile: str = Field("FLAT", description="FLAT|PEAK|OFF_PEAK|SUPER_PEAK")
    status: str = Field("DRAFT", description="DRAFT|CONFIRMED|SETTLED|CANCELLED")
    counterparty_id: Optional[str] = None
    portfolio_id: Optional[str] = None
    notes: Optional[str] = None
    created_by: str = Field("system")


class TradeUpdate(BaseModel):
    volume_mw: Optional[float] = None
    price: Optional[float] = None
    status: Optional[str] = None
    counterparty_id: Optional[str] = None
    portfolio_id: Optional[str] = None
    notes: Optional[str] = None
    amended_by: str = Field("system")


class PortfolioCreate(BaseModel):
    name: str
    owner: Optional[str] = None
    description: Optional[str] = None


class CounterpartyCreate(BaseModel):
    name: str
    credit_rating: Optional[str] = None
    credit_limit_aud: Optional[float] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _new_id() -> str:
    return str(uuid.uuid4())


def _profile_factor(profile: str, dt: datetime) -> float:
    """Return profile factor for a 5-min interval. 1.0 = active, 0.0 = inactive."""
    hour = dt.hour
    weekday = dt.weekday()  # 0=Mon, 6=Sun
    is_weekday = weekday < 5

    if profile == "FLAT":
        return 1.0
    elif profile == "PEAK":
        return 1.0 if (is_weekday and _PEAK_START <= hour < _PEAK_END) else 0.0
    elif profile == "OFF_PEAK":
        return 0.0 if (is_weekday and _PEAK_START <= hour < _PEAK_END) else 1.0
    elif profile == "SUPER_PEAK":
        return 1.0 if (is_weekday and _SUPER_PEAK_START <= hour < _SUPER_PEAK_END) else 0.0
    return 1.0


def _generate_legs(trade_id: str, start_date: date, end_date: date,
                   volume_mw: float, price: float, profile: str) -> List[dict]:
    """Generate daily legs for a trade. Each leg = one settlement day."""
    legs = []
    current = start_date
    while current <= end_date:
        leg_id = _new_id()
        # Compute average profile factor for the day
        factors = [_profile_factor(profile, datetime(current.year, current.month, current.day, h))
                   for h in range(24)]
        avg_factor = sum(factors) / len(factors) if factors else 1.0
        if avg_factor > 0:
            legs.append({
                "leg_id": leg_id,
                "trade_id": trade_id,
                "settlement_date": str(current),
                "interval_start": f"{current} 00:00:00",
                "interval_end": f"{current} 23:55:00",
                "volume_mw": volume_mw,
                "price": price,
                "profile_factor": round(avg_factor, 4),
            })
        current += timedelta(days=1)
    return legs


# =========================================================================
# TRADE CRUD
# =========================================================================

@router.get("/api/deals/trades")
async def list_trades(
    region: Optional[str] = None,
    status: Optional[str] = None,
    trade_type: Optional[str] = None,
    portfolio_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(100, le=500),
    offset: int = Query(0, ge=0),
):
    """List trades with optional filters and pagination."""
    # --- Lakebase fast path ---
    try:
        lb_where = ["1=1"]
        if region:
            lb_where.append(f"t.region = '{region}'")
        if status:
            lb_where.append(f"t.status = '{status}'")
        if trade_type:
            lb_where.append(f"t.trade_type = '{trade_type}'")
        if start_date:
            lb_where.append(f"t.start_date >= '{start_date}'")
        if end_date:
            lb_where.append(f"t.end_date <= '{end_date}'")
        lb_wc = " AND ".join(lb_where)

        if portfolio_id:
            lb_sql = (
                f"SELECT t.*, c.name as counterparty_name "
                f"FROM gold.trades_synced t "
                f"LEFT JOIN gold.counterparties_synced c ON t.counterparty_id = c.counterparty_id "
                f"INNER JOIN gold.portfolio_trades_synced pt ON t.trade_id = pt.trade_id "
                f"WHERE pt.portfolio_id = '{portfolio_id}' AND {lb_wc} "
                f"ORDER BY t.created_at DESC LIMIT {limit} OFFSET {offset}"
            )
        else:
            lb_sql = (
                f"SELECT t.*, c.name as counterparty_name "
                f"FROM gold.trades_synced t "
                f"LEFT JOIN gold.counterparties_synced c ON t.counterparty_id = c.counterparty_id "
                f"WHERE {lb_wc} "
                f"ORDER BY t.created_at DESC LIMIT {limit} OFFSET {offset}"
            )
        lb_rows = _query_lakebase_fresh(lb_sql)
        if lb_rows is not None:
            lb_count_sql = f"SELECT COUNT(*) as cnt FROM gold.trades_synced t WHERE {lb_wc}"
            lb_count = _query_lakebase_fresh(lb_count_sql)
            total = lb_count[0]["cnt"] if lb_count else len(lb_rows)
            for r in lb_rows:
                for k in ("created_at", "updated_at", "start_date", "end_date"):
                    if k in r and r[k] is not None:
                        r[k] = str(r[k])
            return {"trades": lb_rows, "total": total, "limit": limit, "offset": offset, "data_source": "lakebase"}
    except Exception as exc:
        logger.warning("Lakebase list_trades failed: %s", exc)

    # --- SQL Warehouse fallback ---
    where_parts = ["1=1"]
    if region:
        where_parts.append(f"t.region = '{region}'")
    if status:
        where_parts.append(f"t.status = '{status}'")
    if trade_type:
        where_parts.append(f"t.trade_type = '{trade_type}'")
    if start_date:
        where_parts.append(f"t.start_date >= '{start_date}'")
    if end_date:
        where_parts.append(f"t.end_date <= '{end_date}'")

    where_clause = " AND ".join(where_parts)

    if portfolio_id:
        sql = (
            f"SELECT t.*, c.name as counterparty_name "
            f"FROM {_SCHEMA}.trades t "
            f"LEFT JOIN {_SCHEMA}.counterparties c ON t.counterparty_id = c.counterparty_id "
            f"INNER JOIN {_SCHEMA}.portfolio_trades pt ON t.trade_id = pt.trade_id "
            f"WHERE pt.portfolio_id = '{portfolio_id}' AND {where_clause} "
            f"ORDER BY t.created_at DESC LIMIT {limit} OFFSET {offset}"
        )
    else:
        sql = (
            f"SELECT t.*, c.name as counterparty_name "
            f"FROM {_SCHEMA}.trades t "
            f"LEFT JOIN {_SCHEMA}.counterparties c ON t.counterparty_id = c.counterparty_id "
            f"WHERE {where_clause} "
            f"ORDER BY t.created_at DESC LIMIT {limit} OFFSET {offset}"
        )

    rows = _query_gold(sql)
    if rows is None:
        return {"trades": [], "total": 0, "data_source": "unavailable"}

    # Get total count
    count_sql = (
        f"SELECT COUNT(*) as cnt FROM {_SCHEMA}.trades t WHERE {where_clause}"
    )
    count_rows = _query_gold(count_sql)
    total = count_rows[0]["cnt"] if count_rows else len(rows)

    for r in rows:
        for k in ("created_at", "updated_at", "start_date", "end_date"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])

    return {"trades": rows, "total": total, "limit": limit, "offset": offset}


@router.get("/api/deals/trades/{trade_id}")
async def get_trade(trade_id: str):
    """Get a single trade with its legs."""
    # --- Lakebase fast path ---
    try:
        lb_rows = _query_lakebase_fresh(
            f"SELECT t.*, c.name as counterparty_name "
            f"FROM gold.trades_synced t "
            f"LEFT JOIN gold.counterparties_synced c ON t.counterparty_id = c.counterparty_id "
            f"WHERE t.trade_id = '{trade_id}'"
        )
        if lb_rows is not None:
            if not lb_rows:
                return JSONResponse(status_code=404, content={"error": "Trade not found"})
            trade = lb_rows[0]
            for k in ("created_at", "updated_at", "start_date", "end_date"):
                if k in trade and trade[k] is not None:
                    trade[k] = str(trade[k])
            lb_legs = _query_lakebase_fresh(
                f"SELECT * FROM gold.trade_legs_synced WHERE trade_id = '{trade_id}' "
                f"ORDER BY settlement_date"
            )
            legs = lb_legs if lb_legs else []
            for leg in legs:
                for k in ("settlement_date", "interval_start", "interval_end"):
                    if k in leg and leg[k] is not None:
                        leg[k] = str(leg[k])
            trade["legs"] = legs
            trade["data_source"] = "lakebase"
            return trade
    except Exception as exc:
        logger.warning("Lakebase get_trade failed: %s", exc)

    # --- SQL Warehouse fallback ---
    rows = _query_gold(
        f"SELECT t.*, c.name as counterparty_name "
        f"FROM {_SCHEMA}.trades t "
        f"LEFT JOIN {_SCHEMA}.counterparties c ON t.counterparty_id = c.counterparty_id "
        f"WHERE t.trade_id = '{trade_id}'"
    )
    if not rows:
        return JSONResponse(status_code=404, content={"error": "Trade not found"})

    trade = rows[0]
    for k in ("created_at", "updated_at", "start_date", "end_date"):
        if k in trade and trade[k] is not None:
            trade[k] = str(trade[k])

    # Fetch legs
    legs = _query_gold(
        f"SELECT * FROM {_SCHEMA}.trade_legs WHERE trade_id = '{trade_id}' "
        f"ORDER BY settlement_date"
    )
    if legs:
        for leg in legs:
            for k in ("settlement_date", "interval_start", "interval_end"):
                if k in leg and leg[k] is not None:
                    leg[k] = str(leg[k])
    else:
        legs = []

    trade["legs"] = legs
    return trade


@router.post("/api/deals/trades")
async def create_trade(body: TradeCreate):
    """Create a new trade and auto-generate settlement legs."""
    # Validate
    if body.trade_type not in _TRADE_TYPES:
        return JSONResponse(status_code=400, content={"error": f"Invalid trade_type. Must be one of: {_TRADE_TYPES}"})
    if body.region not in _NEM_REGIONS:
        return JSONResponse(status_code=400, content={"error": f"Invalid region. Must be one of: {_NEM_REGIONS}"})
    if body.buy_sell not in _BUY_SELL:
        return JSONResponse(status_code=400, content={"error": "buy_sell must be BUY or SELL"})
    if body.profile not in _PROFILES:
        return JSONResponse(status_code=400, content={"error": f"Invalid profile. Must be one of: {_PROFILES}"})
    if body.end_date < body.start_date:
        return JSONResponse(status_code=400, content={"error": "end_date must be >= start_date"})

    # --- E12: Pre-trade credit check ---
    credit_warning = None
    if body.counterparty_id:
        try:
            from .risk import _credit_check_core
            days = (body.end_date - body.start_date).days or 1
            notional = body.volume_mw * body.price * days
            check = _credit_check_core(body.counterparty_id, notional, days)
            if check.get("status") == "block":
                return JSONResponse(status_code=403, content={
                    "error": "Credit limit exceeded",
                    "credit_check": check,
                })
            if check.get("status") == "warn":
                credit_warning = check
        except Exception as exc:
            logger.warning("Credit check failed: %s", exc)

    trade_id = _new_id()
    now = _now_ts()

    # --- E13: Check approval rules ---
    days = (body.end_date - body.start_date).days or 1
    notional_aud = body.volume_mw * body.price * days
    approval_request_id = None
    trade_status = body.status if body.status in _STATUSES else "DRAFT"

    try:
        rules = _query_gold(
            f"SELECT * FROM {_SCHEMA}.approval_rules "
            f"WHERE is_active = true AND event_type = 'trade_create' "
            f"AND min_notional_aud <= {notional_aud} "
            f"ORDER BY min_notional_aud DESC"
        )
        if rules:
            # Filter by trade_type if rule specifies one
            matching = [
                r for r in rules
                if not r.get("trade_type") or r["trade_type"] == body.trade_type
            ]
            if matching:
                top_rule = matching[0]  # highest threshold match
                trade_status = "PENDING_APPROVAL"
                approval_request_id = _new_id()
                _insert_gold(f"{_SCHEMA}.approval_requests", {
                    "request_id": approval_request_id,
                    "trade_id": trade_id,
                    "rule_id": top_rule["approval_rule_id"],
                    "event_type": "trade_create",
                    "status": "PENDING",
                    "notional_aud": notional_aud,
                    "submitted_by": body.created_by,
                    "submitted_at": now,
                    "decided_by": "",
                    "decided_at": None,
                    "decision_reason": "",
                })
    except Exception as exc:
        logger.warning("Approval rule check failed: %s", exc)

    trade_data = {
        "trade_id": trade_id,
        "trade_type": body.trade_type,
        "region": body.region,
        "buy_sell": body.buy_sell,
        "volume_mw": body.volume_mw,
        "price": body.price,
        "start_date": str(body.start_date),
        "end_date": str(body.end_date),
        "profile": body.profile,
        "status": trade_status,
        "counterparty_id": body.counterparty_id or "",
        "portfolio_id": body.portfolio_id or "",
        "notes": body.notes or "",
        "created_by": body.created_by,
        "created_at": now,
        "updated_at": now,
    }

    ok = _insert_gold(f"{_SCHEMA}.trades", trade_data)
    if not ok:
        return JSONResponse(status_code=500, content={"error": "Failed to insert trade"})

    # Generate legs (daily granularity) — batch insert for performance
    legs = _generate_legs(trade_id, body.start_date, body.end_date,
                          body.volume_mw, body.price, body.profile)
    legs_inserted = _insert_gold_batch(f"{_SCHEMA}.trade_legs", legs)
    leg_errors = len(legs) - legs_inserted

    # Auto-assign to portfolio if specified
    if body.portfolio_id:
        _insert_gold(f"{_SCHEMA}.portfolio_trades", {
            "portfolio_id": body.portfolio_id,
            "trade_id": trade_id,
        })

    _invalidate_cache("sql:")
    result: Dict[str, Any] = {
        "trade_id": trade_id,
        "legs_created": len(legs) - leg_errors,
        "leg_errors": leg_errors,
        "status": "created",
    }
    if approval_request_id:
        result["approval_required"] = True
        result["approval_request_id"] = approval_request_id
        result["trade_status"] = "PENDING_APPROVAL"
    if credit_warning:
        result["credit_warning"] = credit_warning
    return result


@router.put("/api/deals/trades/{trade_id}")
async def update_trade(trade_id: str, body: TradeUpdate):
    """Amend trade fields. Creates audit trail entries."""
    # Fetch current trade
    rows = _query_gold(f"SELECT * FROM {_SCHEMA}.trades WHERE trade_id = '{trade_id}'")
    if not rows:
        return JSONResponse(status_code=404, content={"error": "Trade not found"})

    current = rows[0]
    now = _now_ts()
    updates: Dict[str, Any] = {"updated_at": now}
    amendments = []

    for field in ("volume_mw", "price", "status", "counterparty_id", "portfolio_id", "notes"):
        new_val = getattr(body, field, None)
        if new_val is not None:
            old_val = current.get(field)
            if str(new_val) != str(old_val):
                updates[field] = new_val
                amendments.append({
                    "amendment_id": _new_id(),
                    "trade_id": trade_id,
                    "field_changed": field,
                    "old_value": str(old_val) if old_val is not None else "",
                    "new_value": str(new_val),
                    "amended_by": body.amended_by,
                    "amended_at": now,
                })

    if len(updates) <= 1:
        return {"trade_id": trade_id, "amendments": 0, "message": "No changes detected"}

    ok = _update_gold(f"{_SCHEMA}.trades", updates, f"trade_id = '{trade_id}'")
    if not ok:
        return JSONResponse(status_code=500, content={"error": "Failed to update trade"})

    for amend in amendments:
        _insert_gold(f"{_SCHEMA}.trade_amendments", amend)

    _invalidate_cache("sql:")
    return {"trade_id": trade_id, "amendments": len(amendments), "status": "amended"}


@router.delete("/api/deals/trades/{trade_id}")
async def delete_trade(trade_id: str, amended_by: str = "system"):
    """Soft-delete a trade (set status=CANCELLED)."""
    rows = _query_gold(f"SELECT status FROM {_SCHEMA}.trades WHERE trade_id = '{trade_id}'")
    if not rows:
        return JSONResponse(status_code=404, content={"error": "Trade not found"})

    now = _now_ts()
    ok = _update_gold(f"{_SCHEMA}.trades",
                      {"status": "CANCELLED", "updated_at": now},
                      f"trade_id = '{trade_id}'")
    if not ok:
        return JSONResponse(status_code=500, content={"error": "Failed to cancel trade"})

    _insert_gold(f"{_SCHEMA}.trade_amendments", {
        "amendment_id": _new_id(),
        "trade_id": trade_id,
        "field_changed": "status",
        "old_value": str(rows[0].get("status", "")),
        "new_value": "CANCELLED",
        "amended_by": amended_by,
        "amended_at": now,
    })

    _invalidate_cache("sql:")
    return {"trade_id": trade_id, "status": "CANCELLED"}


@router.post("/api/deals/trades/bulk-import")
async def bulk_import_trades(file: UploadFile = File(...)):
    """Bulk import trades from a CSV file.

    Expected columns: trade_type, region, buy_sell, volume_mw, price,
    start_date, end_date, profile, counterparty_id, portfolio_id, notes
    """
    content = await file.read()
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    success = 0
    errors = []
    for i, row in enumerate(reader, start=1):
        try:
            body = TradeCreate(
                trade_type=row.get("trade_type", "SWAP").strip().upper(),
                region=row.get("region", "NSW1").strip().upper(),
                buy_sell=row.get("buy_sell", "BUY").strip().upper(),
                volume_mw=float(row.get("volume_mw", 0)),
                price=float(row.get("price", 0)),
                start_date=date.fromisoformat(row.get("start_date", "2026-01-01").strip()),
                end_date=date.fromisoformat(row.get("end_date", "2026-12-31").strip()),
                profile=row.get("profile", "FLAT").strip().upper(),
                counterparty_id=row.get("counterparty_id", "").strip() or None,
                portfolio_id=row.get("portfolio_id", "").strip() or None,
                notes=row.get("notes", "").strip() or None,
                created_by="csv-import",
            )
            result = await create_trade(body)
            if isinstance(result, JSONResponse):
                errors.append({"row": i, "error": "validation/insert failed"})
            else:
                success += 1
        except Exception as exc:
            errors.append({"row": i, "error": str(exc)[:200]})

    return {"imported": success, "errors": len(errors), "error_details": errors[:20]}


# =========================================================================
# TRADE LEGS
# =========================================================================

@router.get("/api/deals/trades/{trade_id}/legs")
async def list_trade_legs(trade_id: str):
    """List all settlement legs for a trade."""
    # --- Lakebase fast path ---
    try:
        lb_rows = _query_lakebase_fresh(
            f"SELECT * FROM gold.trade_legs_synced WHERE trade_id = '{trade_id}' "
            f"ORDER BY settlement_date"
        )
        if lb_rows is not None:
            for r in lb_rows:
                for k in ("settlement_date", "interval_start", "interval_end"):
                    if k in r and r[k] is not None:
                        r[k] = str(r[k])
            return {"legs": lb_rows, "count": len(lb_rows), "data_source": "lakebase"}
    except Exception as exc:
        logger.warning("Lakebase list_trade_legs failed: %s", exc)

    # --- SQL Warehouse fallback ---
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.trade_legs WHERE trade_id = '{trade_id}' "
        f"ORDER BY settlement_date"
    )
    if rows is None:
        rows = []
    for r in rows:
        for k in ("settlement_date", "interval_start", "interval_end"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])
    return {"legs": rows, "count": len(rows)}


# =========================================================================
# AMENDMENTS (audit trail)
# =========================================================================

@router.get("/api/deals/trades/{trade_id}/amendments")
async def list_amendments(trade_id: str):
    """Get the full amendment audit trail for a trade."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.trade_amendments "
        f"WHERE trade_id = '{trade_id}' ORDER BY amended_at DESC"
    )
    if rows is None:
        rows = []
    for r in rows:
        if "amended_at" in r and r["amended_at"] is not None:
            r["amended_at"] = str(r["amended_at"])
    return {"amendments": rows, "count": len(rows)}


# =========================================================================
# PORTFOLIOS
# =========================================================================

@router.get("/api/deals/portfolios")
async def list_portfolios():
    """List all portfolios with trade counts."""
    # --- Lakebase fast path ---
    try:
        lb_rows = _query_lakebase_fresh(
            "SELECT p.*, "
            "(SELECT COUNT(*) FROM gold.portfolio_trades_synced pt WHERE pt.portfolio_id = p.portfolio_id) as trade_count "
            "FROM gold.portfolios_synced p ORDER BY p.name"
        )
        if lb_rows is not None:
            for r in lb_rows:
                if "created_at" in r and r["created_at"] is not None:
                    r["created_at"] = str(r["created_at"])
            return {"portfolios": lb_rows, "data_source": "lakebase"}
    except Exception as exc:
        logger.warning("Lakebase list_portfolios failed: %s", exc)

    # --- SQL Warehouse fallback ---
    rows = _query_gold(
        f"SELECT p.*, "
        f"(SELECT COUNT(*) FROM {_SCHEMA}.portfolio_trades pt WHERE pt.portfolio_id = p.portfolio_id) as trade_count "
        f"FROM {_SCHEMA}.portfolios p ORDER BY p.name"
    )
    if rows is None:
        rows = []
    for r in rows:
        if "created_at" in r and r["created_at"] is not None:
            r["created_at"] = str(r["created_at"])
    return {"portfolios": rows}


@router.post("/api/deals/portfolios")
async def create_portfolio(body: PortfolioCreate):
    """Create a new portfolio."""
    pid = _new_id()
    now = _now_ts()
    ok = _insert_gold(f"{_SCHEMA}.portfolios", {
        "portfolio_id": pid,
        "name": body.name,
        "owner": body.owner or "",
        "description": body.description or "",
        "created_at": now,
    })
    if not ok:
        return JSONResponse(status_code=500, content={"error": "Failed to create portfolio"})
    return {"portfolio_id": pid, "status": "created"}


@router.get("/api/deals/portfolios/{portfolio_id}")
async def get_portfolio(portfolio_id: str):
    """Get portfolio detail with trade summary."""
    # --- Lakebase fast path ---
    try:
        lb_rows = _query_lakebase_fresh(
            f"SELECT * FROM gold.portfolios_synced WHERE portfolio_id = '{portfolio_id}'"
        )
        if lb_rows is not None:
            if not lb_rows:
                return JSONResponse(status_code=404, content={"error": "Portfolio not found"})
            portfolio = lb_rows[0]
            if "created_at" in portfolio and portfolio["created_at"] is not None:
                portfolio["created_at"] = str(portfolio["created_at"])
            lb_trades = _query_lakebase_fresh(
                f"SELECT t.trade_type, t.region, t.buy_sell, t.status, "
                f"COUNT(*) as count, SUM(t.volume_mw) as total_mw "
                f"FROM gold.trades_synced t "
                f"INNER JOIN gold.portfolio_trades_synced pt ON t.trade_id = pt.trade_id "
                f"WHERE pt.portfolio_id = '{portfolio_id}' AND t.status != 'CANCELLED' "
                f"GROUP BY t.trade_type, t.region, t.buy_sell, t.status"
            )
            portfolio["trade_summary"] = lb_trades or []
            portfolio["data_source"] = "lakebase"
            return portfolio
    except Exception as exc:
        logger.warning("Lakebase get_portfolio failed: %s", exc)

    # --- SQL Warehouse fallback ---
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.portfolios WHERE portfolio_id = '{portfolio_id}'"
    )
    if not rows:
        return JSONResponse(status_code=404, content={"error": "Portfolio not found"})

    portfolio = rows[0]
    if "created_at" in portfolio and portfolio["created_at"] is not None:
        portfolio["created_at"] = str(portfolio["created_at"])

    # Trade summary
    trades = _query_gold(
        f"SELECT t.trade_type, t.region, t.buy_sell, t.status, "
        f"COUNT(*) as count, SUM(t.volume_mw) as total_mw "
        f"FROM {_SCHEMA}.trades t "
        f"INNER JOIN {_SCHEMA}.portfolio_trades pt ON t.trade_id = pt.trade_id "
        f"WHERE pt.portfolio_id = '{portfolio_id}' AND t.status != 'CANCELLED' "
        f"GROUP BY t.trade_type, t.region, t.buy_sell, t.status"
    )
    portfolio["trade_summary"] = trades or []
    return portfolio


@router.get("/api/deals/portfolios/{portfolio_id}/position")
async def portfolio_position(portfolio_id: str):
    """Net MW position by region and quarter."""
    # --- Lakebase fast path ---
    try:
        lb_rows = _query_lakebase_fresh(
            f"SELECT t.region, "
            f"'Q' || EXTRACT(QUARTER FROM t.start_date)::INT || ' ' || EXTRACT(YEAR FROM t.start_date)::INT as quarter, "
            f"SUM(CASE WHEN t.buy_sell = 'BUY' THEN t.volume_mw ELSE -t.volume_mw END) as net_mw, "
            f"SUM(t.volume_mw) as gross_mw, "
            f"COUNT(*) as trade_count "
            f"FROM gold.trades_synced t "
            f"INNER JOIN gold.portfolio_trades_synced pt ON t.trade_id = pt.trade_id "
            f"WHERE pt.portfolio_id = '{portfolio_id}' AND t.status != 'CANCELLED' "
            f"GROUP BY t.region, EXTRACT(QUARTER FROM t.start_date), EXTRACT(YEAR FROM t.start_date) "
            f"ORDER BY t.region, EXTRACT(YEAR FROM t.start_date), EXTRACT(QUARTER FROM t.start_date)"
        )
        if lb_rows is not None:
            return {"positions": lb_rows, "portfolio_id": portfolio_id, "data_source": "lakebase"}
    except Exception as exc:
        logger.warning("Lakebase portfolio_position failed: %s", exc)

    # --- SQL Warehouse fallback ---
    rows = _query_gold(
        f"SELECT t.region, "
        f"CONCAT('Q', QUARTER(t.start_date), ' ', YEAR(t.start_date)) as quarter, "
        f"SUM(CASE WHEN t.buy_sell = 'BUY' THEN t.volume_mw ELSE -t.volume_mw END) as net_mw, "
        f"SUM(t.volume_mw) as gross_mw, "
        f"COUNT(*) as trade_count "
        f"FROM {_SCHEMA}.trades t "
        f"INNER JOIN {_SCHEMA}.portfolio_trades pt ON t.trade_id = pt.trade_id "
        f"WHERE pt.portfolio_id = '{portfolio_id}' AND t.status != 'CANCELLED' "
        f"GROUP BY t.region, QUARTER(t.start_date), YEAR(t.start_date) "
        f"ORDER BY t.region, YEAR(t.start_date), QUARTER(t.start_date)"
    )
    return {"positions": rows or [], "portfolio_id": portfolio_id}


@router.get("/api/deals/portfolios/{portfolio_id}/pnl")
async def portfolio_pnl(portfolio_id: str):
    """Realized + unrealized P&L by region.

    Unrealized = (current spot - trade price) * volume * direction.
    """
    # Get trades
    trades = _query_gold(
        f"SELECT t.region, t.buy_sell, t.volume_mw, t.price, t.status "
        f"FROM {_SCHEMA}.trades t "
        f"INNER JOIN {_SCHEMA}.portfolio_trades pt ON t.trade_id = pt.trade_id "
        f"WHERE pt.portfolio_id = '{portfolio_id}' AND t.status != 'CANCELLED'"
    )
    if not trades:
        return {"pnl": [], "portfolio_id": portfolio_id}

    # Get latest spot prices
    spot_rows = _query_gold(
        f"SELECT region_id, AVG(rrp) as avg_rrp FROM {_CATALOG}.gold.nem_prices_5min "
        f"WHERE interval_datetime >= current_timestamp() - INTERVAL 1 HOUR "
        f"GROUP BY region_id"
    )
    spot_map = {r["region_id"]: r["avg_rrp"] for r in (spot_rows or [])}

    pnl_by_region: Dict[str, Dict[str, float]] = {}
    for t in trades:
        region = t["region"]
        if region not in pnl_by_region:
            pnl_by_region[region] = {"realized": 0.0, "unrealized": 0.0, "net_mw": 0.0}

        direction = 1.0 if t["buy_sell"] == "BUY" else -1.0
        vol = t["volume_mw"] * direction
        pnl_by_region[region]["net_mw"] += vol

        spot = spot_map.get(region, t["price"])
        if t["status"] == "SETTLED":
            pnl_by_region[region]["realized"] += (spot - t["price"]) * t["volume_mw"] * direction
        else:
            pnl_by_region[region]["unrealized"] += (spot - t["price"]) * t["volume_mw"] * direction

    pnl_list = [
        {"region": r, "realized": round(v["realized"], 2),
         "unrealized": round(v["unrealized"], 2),
         "total": round(v["realized"] + v["unrealized"], 2),
         "net_mw": round(v["net_mw"], 1)}
        for r, v in pnl_by_region.items()
    ]
    return {"pnl": pnl_list, "portfolio_id": portfolio_id}


@router.get("/api/deals/portfolios/{portfolio_id}/exposure")
async def portfolio_exposure(portfolio_id: str):
    """Exposure heatmap data: region x month grid of MW exposure."""
    # --- Lakebase fast path ---
    try:
        lb_rows = _query_lakebase_fresh(
            f"SELECT t.region, "
            f"TO_CHAR(t.start_date, 'YYYY-MM') as month, "
            f"SUM(CASE WHEN t.buy_sell = 'BUY' THEN t.volume_mw ELSE -t.volume_mw END) as net_mw "
            f"FROM gold.trades_synced t "
            f"INNER JOIN gold.portfolio_trades_synced pt ON t.trade_id = pt.trade_id "
            f"WHERE pt.portfolio_id = '{portfolio_id}' AND t.status != 'CANCELLED' "
            f"GROUP BY t.region, TO_CHAR(t.start_date, 'YYYY-MM') "
            f"ORDER BY t.region, month"
        )
        if lb_rows is not None:
            return {"exposure": lb_rows, "portfolio_id": portfolio_id, "data_source": "lakebase"}
    except Exception as exc:
        logger.warning("Lakebase portfolio_exposure failed: %s", exc)

    # --- SQL Warehouse fallback ---
    rows = _query_gold(
        f"SELECT t.region, "
        f"CONCAT(YEAR(t.start_date), '-', LPAD(MONTH(t.start_date), 2, '0')) as month, "
        f"SUM(CASE WHEN t.buy_sell = 'BUY' THEN t.volume_mw ELSE -t.volume_mw END) as net_mw "
        f"FROM {_SCHEMA}.trades t "
        f"INNER JOIN {_SCHEMA}.portfolio_trades pt ON t.trade_id = pt.trade_id "
        f"WHERE pt.portfolio_id = '{portfolio_id}' AND t.status != 'CANCELLED' "
        f"GROUP BY t.region, YEAR(t.start_date), MONTH(t.start_date) "
        f"ORDER BY t.region, month"
    )
    return {"exposure": rows or [], "portfolio_id": portfolio_id}


@router.post("/api/deals/portfolios/{portfolio_id}/trades")
async def add_trade_to_portfolio(portfolio_id: str, trade_id: str = Query(...)):
    """Add a trade to a portfolio."""
    ok = _insert_gold(f"{_SCHEMA}.portfolio_trades", {
        "portfolio_id": portfolio_id,
        "trade_id": trade_id,
    })
    if not ok:
        return JSONResponse(status_code=500, content={"error": "Failed to add trade to portfolio"})
    _invalidate_cache("sql:")
    return {"status": "added", "portfolio_id": portfolio_id, "trade_id": trade_id}


@router.delete("/api/deals/portfolios/{portfolio_id}/trades/{trade_id}")
async def remove_trade_from_portfolio(portfolio_id: str, trade_id: str):
    """Remove a trade from a portfolio."""
    ok = _execute_gold(
        f"DELETE FROM {_SCHEMA}.portfolio_trades "
        f"WHERE portfolio_id = '{portfolio_id}' AND trade_id = '{trade_id}'"
    )
    if not ok:
        return JSONResponse(status_code=500, content={"error": "Failed to remove trade"})
    _invalidate_cache("sql:")
    return {"status": "removed", "portfolio_id": portfolio_id, "trade_id": trade_id}


# =========================================================================
# COUNTERPARTIES
# =========================================================================

@router.get("/api/deals/counterparties")
async def list_counterparties():
    """List all counterparties."""
    # --- Lakebase fast path ---
    try:
        lb_rows = _query_lakebase_fresh(
            "SELECT * FROM gold.counterparties_synced ORDER BY name"
        )
        if lb_rows is not None:
            for r in lb_rows:
                if "created_at" in r and r["created_at"] is not None:
                    r["created_at"] = str(r["created_at"])
            return {"counterparties": lb_rows, "data_source": "lakebase"}
    except Exception as exc:
        logger.warning("Lakebase list_counterparties failed: %s", exc)

    # --- SQL Warehouse fallback ---
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.counterparties ORDER BY name"
    )
    if rows is None:
        rows = []
    for r in rows:
        if "created_at" in r and r["created_at"] is not None:
            r["created_at"] = str(r["created_at"])
    return {"counterparties": rows}


@router.post("/api/deals/counterparties")
async def create_counterparty(body: CounterpartyCreate):
    """Create a new counterparty."""
    cid = _new_id()
    now = _now_ts()
    ok = _insert_gold(f"{_SCHEMA}.counterparties", {
        "counterparty_id": cid,
        "name": body.name,
        "credit_rating": body.credit_rating or "",
        "credit_limit_aud": body.credit_limit_aud if body.credit_limit_aud is not None else 0.0,
        "status": "ACTIVE",
        "created_at": now,
    })
    if not ok:
        return JSONResponse(status_code=500, content={"error": "Failed to create counterparty"})
    return {"counterparty_id": cid, "status": "created"}


# =========================================================================
# APPROVAL WORKFLOWS (E13)
# =========================================================================

class ApprovalRuleCreate(BaseModel):
    rule_name: str
    event_type: str = Field("trade_create", description="trade_create|trade_amend|trade_cancel")
    trade_type: Optional[str] = None
    min_notional_aud: float
    required_approvers: int = Field(1, ge=1)
    approver_role: str = Field("RISK_MANAGER", description="RISK_MANAGER|TRADING_HEAD|CFO")


@router.get("/api/deals/approvals/rules")
async def list_approval_rules():
    """List active approval rules."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.approval_rules "
        f"WHERE is_active = true ORDER BY min_notional_aud DESC"
    )
    if rows is None:
        rows = []
    for r in rows:
        for k in ("created_at", "updated_at"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])
    return {"rules": rows}


@router.post("/api/deals/approvals/rules")
async def create_approval_rule(body: ApprovalRuleCreate):
    """Create a new approval rule."""
    rule_id = _new_id()
    now = _now_ts()
    ok = _insert_gold(f"{_SCHEMA}.approval_rules", {
        "approval_rule_id": rule_id,
        "rule_name": body.rule_name,
        "event_type": body.event_type,
        "trade_type": body.trade_type or "",
        "min_notional_aud": body.min_notional_aud,
        "required_approvers": body.required_approvers,
        "approver_role": body.approver_role,
        "is_active": True,
        "created_at": now,
        "updated_at": now,
    })
    if not ok:
        return JSONResponse(status_code=500, content={"error": "Failed to create rule"})
    return {"approval_rule_id": rule_id, "status": "created"}


@router.get("/api/deals/approvals/pending")
async def list_pending_approvals():
    """List all pending approval requests with trade details."""
    rows = _query_gold(
        f"SELECT ar.*, t.trade_type, t.region, t.buy_sell, t.volume_mw, t.price, "
        f"t.start_date, t.end_date, t.profile, t.counterparty_id, "
        f"rl.rule_name, rl.approver_role "
        f"FROM {_SCHEMA}.approval_requests ar "
        f"INNER JOIN {_SCHEMA}.trades t ON ar.trade_id = t.trade_id "
        f"LEFT JOIN {_SCHEMA}.approval_rules rl ON ar.rule_id = rl.approval_rule_id "
        f"WHERE ar.status = 'PENDING' "
        f"ORDER BY ar.submitted_at DESC"
    )
    if rows is None:
        rows = []
    for r in rows:
        for k in ("submitted_at", "decided_at", "start_date", "end_date"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])
    return {"pending": rows, "count": len(rows)}


@router.put("/api/deals/approvals/{request_id}/approve")
async def approve_request(
    request_id: str,
    decided_by: str = Query("approver"),
    reason: str = Query(""),
):
    """Approve a pending trade request."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.approval_requests "
        f"WHERE request_id = '{_sql_escape(request_id)}'"
    )
    if not rows:
        return JSONResponse(status_code=404, content={"error": "Request not found"})
    req = rows[0]
    if req.get("status") != "PENDING":
        return JSONResponse(status_code=400, content={"error": f"Request already {req.get('status')}"})

    # Maker-checker: approver != submitter
    if decided_by == req.get("submitted_by") and decided_by != "system":
        return JSONResponse(status_code=400, content={"error": "Maker-checker: approver cannot be the submitter"})

    now = _now_ts()
    # Update approval request
    _update_gold(f"{_SCHEMA}.approval_requests", {
        "status": "APPROVED",
        "decided_by": decided_by,
        "decided_at": now,
        "decision_reason": reason,
    }, f"request_id = '{_sql_escape(request_id)}'")

    # Update trade status to CONFIRMED
    trade_id = req.get("trade_id", "")
    _update_gold(f"{_SCHEMA}.trades", {
        "status": "CONFIRMED",
        "updated_at": now,
    }, f"trade_id = '{_sql_escape(trade_id)}'")

    _invalidate_cache("sql:")
    return {
        "request_id": request_id,
        "trade_id": trade_id,
        "status": "APPROVED",
        "decided_by": decided_by,
    }


@router.put("/api/deals/approvals/{request_id}/reject")
async def reject_request(
    request_id: str,
    decided_by: str = Query("approver"),
    reason: str = Query(""),
):
    """Reject a pending trade request."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.approval_requests "
        f"WHERE request_id = '{_sql_escape(request_id)}'"
    )
    if not rows:
        return JSONResponse(status_code=404, content={"error": "Request not found"})
    req = rows[0]
    if req.get("status") != "PENDING":
        return JSONResponse(status_code=400, content={"error": f"Request already {req.get('status')}"})

    now = _now_ts()
    _update_gold(f"{_SCHEMA}.approval_requests", {
        "status": "REJECTED",
        "decided_by": decided_by,
        "decided_at": now,
        "decision_reason": reason,
    }, f"request_id = '{_sql_escape(request_id)}'")

    # Cancel the trade
    trade_id = req.get("trade_id", "")
    _update_gold(f"{_SCHEMA}.trades", {
        "status": "CANCELLED",
        "updated_at": now,
    }, f"trade_id = '{_sql_escape(trade_id)}'")

    _invalidate_cache("sql:")
    return {
        "request_id": request_id,
        "trade_id": trade_id,
        "status": "REJECTED",
        "decided_by": decided_by,
    }
