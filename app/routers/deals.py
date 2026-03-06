"""Deal Capture & Portfolio Management — Phase 2 (PRD 15.1).

Endpoints for trade CRUD, portfolio management, counterparties,
amendment audit trail, and bulk CSV import.
"""
from __future__ import annotations

import csv
import io
import uuid
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, UploadFile, File
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .shared import (
    _CATALOG, _NEM_REGIONS, _query_gold, _query_with_fallback,
    _insert_gold, _insert_gold_batch, _update_gold, _execute_gold,
    _invalidate_cache, logger,
)

router = APIRouter()

_SCHEMA = f"{_CATALOG}.gold"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_TRADE_TYPES = {"SPOT", "FORWARD", "SWAP", "FUTURE", "OPTION", "PPA", "REC"}
_PROFILES = {"FLAT", "PEAK", "OFF_PEAK", "SUPER_PEAK"}
_STATUSES = {"DRAFT", "CONFIRMED", "SETTLED", "CANCELLED"}
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

    trade_id = _new_id()
    now = _now_ts()

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
        "status": body.status if body.status in _STATUSES else "DRAFT",
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
    return {
        "trade_id": trade_id,
        "legs_created": len(legs) - leg_errors,
        "leg_errors": leg_errors,
        "status": "created",
    }


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
