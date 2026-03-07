"""Forward Curve Construction Engine — E1 Enhancement.

Bootstraps NEM forward electricity curves from ASX futures data,
applies peak/off-peak shaping and seasonal adjustments.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query

from .shared import (
    _query_gold,
    _CATALOG,
    _insert_gold_batch,
    _execute_gold,
    _sql_escape,
    _NEM_REGIONS,
    logger,
)

router = APIRouter()

# ---------------------------------------------------------------------------
# NEM Seasonal Shape Factors (monthly multiplier vs annual average)
# Based on typical NEM patterns: summer premium Dec-Feb, winter shoulder Jun-Aug
# ---------------------------------------------------------------------------
SEASONAL_FACTORS: Dict[int, float] = {
    1: 1.12, 2: 1.08, 3: 0.95, 4: 0.88, 5: 0.92, 6: 1.02,
    7: 1.05, 8: 1.00, 9: 0.93, 10: 0.90, 11: 0.95, 12: 1.10,
}

# Peak-to-base ratio by region (peak hours ~7am-10pm weekdays)
PEAK_RATIO: Dict[str, float] = {
    "NSW1": 1.22, "QLD1": 1.18, "VIC1": 1.20, "SA1": 1.25, "TAS1": 1.10,
}

# Quarter → month mapping
QUARTER_MONTHS: Dict[int, List[int]] = {
    1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12],
}

# Peak weighting: fraction of hours in peak period (~62% of weekday hours 7-22)
_PEAK_WEIGHT = 0.45  # approx proportion of total hours that are peak
_OFFPEAK_WEIGHT = 1.0 - _PEAK_WEIGHT


def _fetch_asx_futures(region: Optional[str] = None, as_of: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """Fetch ASX futures settlement prices from gold table."""
    where_parts = []
    if region:
        where_parts.append(f"region = '{_sql_escape(region)}'")
    if as_of:
        where_parts.append(f"trade_date <= '{_sql_escape(as_of)}'")

    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    rows = _query_gold(f"""
        SELECT contract_code, region, contract_type, quarter, year,
               settlement_price, trade_date
        FROM {_CATALOG}.gold.asx_futures_eod
        {where}
        ORDER BY trade_date DESC, region, year, quarter
    """)
    return rows if rows and len(rows) >= 1 else None


def _build_forward_curve(
    region: str = "NSW1",
    profile: str = "FLAT",
    curve_date: Optional[str] = None,
    num_quarters: int = 8,
) -> List[Dict[str, Any]]:
    """Build a forward curve for a region+profile from ASX futures data.

    Returns list of {month, price_mwh, source, quarter} dicts.
    """
    if curve_date is None:
        curve_date = date.today().isoformat()

    # 1. Fetch ASX futures for this region
    futures = _fetch_asx_futures(region=region, as_of=curve_date)

    if not futures:
        # Return illustrative curve if no data
        return _illustrative_curve(region, profile, curve_date, num_quarters)

    # Get latest trade_date's data
    latest_date = futures[0]["trade_date"]
    latest = [r for r in futures if str(r["trade_date"]) == str(latest_date) and r["region"] == region]

    if not latest:
        # Try without region filter (data may have different region codes)
        latest = [r for r in futures if str(r["trade_date"]) == str(latest_date)]

    if not latest:
        return _illustrative_curve(region, profile, curve_date, num_quarters)

    # 2. Build quarterly base prices from futures
    quarterly_prices: Dict[str, float] = {}  # "Q1_2026" -> price
    for row in latest:
        qtr = str(row.get("quarter", ""))
        yr = str(row.get("year", ""))
        ct = row.get("contract_type", "Base")
        sp = float(row.get("settlement_price", 0) or 0)
        if sp <= 0:
            continue

        # Normalize key to "Q1_2026" format
        # Data may have quarter="Q1-2026" (already includes year) or quarter="1"
        if qtr.startswith("Q") and len(qtr) > 2:
            # e.g. "Q1-2026" → "Q1_2026"
            key = qtr.replace("-", "_")
        else:
            # e.g. quarter="1", year="2026" → "Q1_2026"
            key = f"Q{qtr}_{yr}"

        # Only use Base contracts for the flat curve bootstrap
        if ct in ("Base", "BASE", "base", "Baseload"):
            quarterly_prices[key] = sp

    if not quarterly_prices:
        # If no Base contracts, use whatever we have
        for row in latest:
            sp = float(row.get("settlement_price", 0) or 0)
            if sp <= 0:
                continue
            qtr = str(row.get("quarter", "1"))
            yr = str(row.get("year", "2026"))
            if qtr.startswith("Q") and len(qtr) > 2:
                key = qtr.replace("-", "_")
            else:
                key = f"Q{qtr}_{yr}"
            quarterly_prices[key] = sp

    if not quarterly_prices:
        return _illustrative_curve(region, profile, curve_date, num_quarters)

    # 3. Determine starting quarter from curve_date
    cd = date.fromisoformat(curve_date) if isinstance(curve_date, str) else curve_date
    start_q = (cd.month - 1) // 3 + 1
    start_y = cd.year

    # 4. Decompose quarterly → monthly with seasonal shaping
    points: List[Dict[str, Any]] = []
    last_known_price = list(quarterly_prices.values())[-1]

    for qi in range(num_quarters):
        q = ((start_q - 1 + qi) % 4) + 1
        y = start_y + (start_q - 1 + qi) // 4

        qkey = f"Q{q}_{y}"
        # Try various key formats
        qprice = quarterly_prices.get(qkey)
        if qprice is None:
            qprice = quarterly_prices.get(f"Q{q}-{y}")
        if qprice is None:
            qprice = quarterly_prices.get(f"{q}_{y}")

        if qprice is not None:
            source = "ASX_BOOTSTRAP"
            last_known_price = qprice
        else:
            # Extrapolate with trend decay
            decay = 0.995 ** (qi * 3)  # slight contango decay
            qprice = last_known_price * decay
            source = "EXTRAPOLATED"

        # Monthly decomposition within this quarter
        months = QUARTER_MONTHS[q]
        # Normalize seasonal factors for this quarter
        q_factors = [SEASONAL_FACTORS[m] for m in months]
        q_avg_factor = sum(q_factors) / len(q_factors)

        for m in months:
            month_str = f"{y}-{m:02d}"
            seasonal_ratio = SEASONAL_FACTORS[m] / q_avg_factor
            base_price = qprice * seasonal_ratio

            if source == "ASX_BOOTSTRAP":
                month_source = "SHAPED"
            else:
                month_source = source

            # Apply profile shaping
            if profile == "PEAK":
                price = base_price * PEAK_RATIO.get(region, 1.20)
            elif profile == "OFF_PEAK":
                peak_price = base_price * PEAK_RATIO.get(region, 1.20)
                price = (base_price - _PEAK_WEIGHT * peak_price) / _OFFPEAK_WEIGHT
                price = max(price, base_price * 0.6)  # floor at 60% of base
            else:  # FLAT
                price = base_price

            points.append({
                "month": month_str,
                "price_mwh": round(price, 2),
                "source": month_source,
                "quarter": qkey,
            })

    return points


def _illustrative_curve(
    region: str, profile: str, curve_date: str, num_quarters: int = 8,
) -> List[Dict[str, Any]]:
    """Generate an illustrative curve when no ASX data is available."""
    base_prices = {"NSW1": 85.0, "QLD1": 72.0, "VIC1": 68.0, "SA1": 95.0, "TAS1": 55.0}
    base = base_prices.get(region, 80.0)

    cd = date.fromisoformat(curve_date) if isinstance(curve_date, str) else curve_date
    start_q = (cd.month - 1) // 3 + 1
    start_y = cd.year

    points: List[Dict[str, Any]] = []
    for qi in range(num_quarters):
        q = ((start_q - 1 + qi) % 4) + 1
        y = start_y + (start_q - 1 + qi) // 4
        qkey = f"Q{q}_{y}"

        for m in QUARTER_MONTHS[q]:
            month_str = f"{y}-{m:02d}"
            price = base * SEASONAL_FACTORS[m]
            if profile == "PEAK":
                price *= PEAK_RATIO.get(region, 1.20)
            elif profile == "OFF_PEAK":
                peak_p = price * PEAK_RATIO.get(region, 1.20)
                price = max((price - _PEAK_WEIGHT * peak_p) / _OFFPEAK_WEIGHT, price * 0.6)

            points.append({
                "month": month_str,
                "price_mwh": round(price, 2),
                "source": "ILLUSTRATIVE",
                "quarter": qkey,
            })
    return points


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/api/curves/term-structure")
def curves_term_structure(
    region: str = Query("NSW1", description="NEM region"),
    profile: str = Query("FLAT", description="FLAT, PEAK, or OFF_PEAK"),
    as_of: Optional[str] = Query(None, description="Curve date (YYYY-MM-DD), default today"),
):
    """Forward curve term structure for a region+profile. Monthly prices for next 8 quarters."""
    curve_date = as_of or date.today().isoformat()
    points = _build_forward_curve(region=region, profile=profile, curve_date=curve_date)
    return {
        "region": region,
        "profile": profile,
        "as_of": curve_date,
        "points": points,
        "data_source": points[0]["source"] if points else "none",
    }


@router.get("/api/curves/compare")
def curves_compare(
    regions: str = Query("NSW1,QLD1,VIC1,SA1,TAS1", description="Comma-separated regions"),
    profile: str = Query("FLAT", description="FLAT, PEAK, or OFF_PEAK"),
    as_of: Optional[str] = Query(None, description="Curve date"),
):
    """Compare forward curves across multiple regions."""
    curve_date = as_of or date.today().isoformat()
    region_list = [r.strip() for r in regions.split(",") if r.strip()]
    curves: Dict[str, List[Dict[str, Any]]] = {}
    for r in region_list:
        curves[r] = _build_forward_curve(region=r, profile=profile, curve_date=curve_date)
    return {
        "regions": region_list,
        "profile": profile,
        "as_of": curve_date,
        "curves": curves,
    }


@router.get("/api/curves/history")
def curves_history(
    region: str = Query("NSW1"),
    profile: str = Query("FLAT"),
    months_back: int = Query(5, description="Number of historical curve dates to show"),
):
    """Historical curve evolution — how the forward curve changed over time."""
    # Get distinct trade dates from ASX futures
    rows = _query_gold(f"""
        SELECT DISTINCT trade_date
        FROM {_CATALOG}.gold.asx_futures_eod
        WHERE region = '{_sql_escape(region)}'
        ORDER BY trade_date DESC
        LIMIT {months_back + 1}
    """)

    if not rows or len(rows) < 1:
        # Try without region filter
        rows = _query_gold(f"""
            SELECT DISTINCT trade_date
            FROM {_CATALOG}.gold.asx_futures_eod
            ORDER BY trade_date DESC
            LIMIT {months_back + 1}
        """)

    snapshots = []
    if rows:
        for row in rows[:months_back]:
            td = str(row["trade_date"])
            points = _build_forward_curve(region=region, profile=profile, curve_date=td, num_quarters=6)
            snapshots.append({"curve_date": td, "points": points})
    else:
        # Illustrative fallback with slightly different base prices
        today = date.today()
        for i in range(months_back):
            d = date(today.year, max(today.month - i, 1), 1)
            points = _illustrative_curve(region, profile, d.isoformat(), 6)
            # Shift prices slightly to show evolution
            for p in points:
                p["price_mwh"] = round(p["price_mwh"] * (1 + i * 0.015), 2)
            snapshots.append({"curve_date": d.isoformat(), "points": points})

    return {
        "region": region,
        "profile": profile,
        "snapshots": snapshots,
    }


@router.post("/api/curves/snapshot")
def curves_snapshot():
    """Persist current forward curves to gold.forward_curves for all regions and profiles."""
    curve_date = date.today().isoformat()
    curve_id_base = str(uuid.uuid4())[:8]
    all_rows: List[Dict[str, Any]] = []

    for region in _NEM_REGIONS:
        for profile in ["FLAT", "PEAK", "OFF_PEAK"]:
            points = _build_forward_curve(region=region, profile=profile, curve_date=curve_date)
            for pt in points:
                all_rows.append({
                    "curve_id": f"{curve_id_base}-{region}-{profile}",
                    "curve_date": curve_date,
                    "region": region,
                    "profile": profile,
                    "quarter": pt["quarter"],
                    "month": pt["month"],
                    "price_mwh": pt["price_mwh"],
                    "source": pt["source"],
                    "model_version": "1.0",
                    "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                })

    inserted = _insert_gold_batch(f"{_CATALOG}.gold.forward_curves", all_rows)
    return {
        "status": "ok" if inserted > 0 else "error",
        "curve_date": curve_date,
        "rows_inserted": inserted,
        "regions": len(_NEM_REGIONS),
        "profiles": 3,
    }


@router.get("/api/curves/snapshots")
def curves_snapshots(
    region: str = Query("NSW1"),
    profile: str = Query("FLAT"),
    curve_date: Optional[str] = Query(None, description="Filter by curve date"),
):
    """List saved curve snapshots from gold.forward_curves."""
    where_parts = [
        f"region = '{_sql_escape(region)}'",
        f"profile = '{_sql_escape(profile)}'",
    ]
    if curve_date:
        where_parts.append(f"curve_date = '{_sql_escape(curve_date)}'")

    where = " AND ".join(where_parts)

    rows = _query_gold(f"""
        SELECT curve_id, curve_date, region, profile, quarter, month,
               price_mwh, source, model_version, created_at
        FROM {_CATALOG}.gold.forward_curves
        WHERE {where}
        ORDER BY curve_date DESC, month
        LIMIT 500
    """)

    if not rows:
        return {"snapshots": [], "region": region, "profile": profile}

    # Group by curve_date
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        cd = str(r["curve_date"])
        if cd not in grouped:
            grouped[cd] = []
        grouped[cd].append({
            "month": r["month"],
            "price_mwh": float(r["price_mwh"]),
            "source": r["source"],
            "quarter": r["quarter"],
        })

    snapshots = [{"curve_date": cd, "points": pts} for cd, pts in grouped.items()]
    return {"snapshots": snapshots, "region": region, "profile": profile}
