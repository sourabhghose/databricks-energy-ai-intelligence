"""WS6: Environmental Certificates & Carbon Exposure.

LGC/ACCU/STC portfolio management, certificate balances, carbon exposure, and bundled PPA valuation.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .shared import (
    _CATALOG,
    _insert_gold,
    _invalidate_cache,
    _query_gold,
    _sql_escape,
)

router = APIRouter()
_SCHEMA = f"{_CATALOG}.gold"

# Certificate market prices (indicative fallbacks)
_CERT_PRICES_DEFAULT = {"LGC": 47.50, "ACCU": 32.00, "STC": 39.80}

# Emissions factors by fuel fallback (tCO2e/MWh)
_EMISSION_FACTORS_DEFAULT = {
    "coal_black": 0.90, "coal_brown": 1.15, "gas_ccgt": 0.37,
    "gas_ocgt": 0.55, "distillate": 0.70,
}


def _get_cert_prices() -> dict:
    """Get certificate prices from lgc_spot_prices, falling back to defaults."""
    rows = _query_gold(
        f"SELECT trade_date, price_aud FROM {_SCHEMA}.lgc_spot_prices "
        f"ORDER BY trade_date DESC LIMIT 1"
    )
    prices = dict(_CERT_PRICES_DEFAULT)
    if rows and rows[0].get("price_aud"):
        prices["LGC"] = round(float(rows[0]["price_aud"]), 2)
    return prices


def _get_emission_factors() -> dict:
    """Get emission factors from emissions_factors table (kg→tCO2/MWh)."""
    rows = _query_gold(
        f"SELECT fuel_type, total_kg_co2_mwh FROM {_SCHEMA}.emissions_factors"
    )
    if rows:
        factors = {}
        for r in rows:
            fuel = r.get("fuel_type", "")
            kg = float(r.get("total_kg_co2_mwh", 0) or 0)
            factors[fuel] = round(kg / 1000.0, 4)  # kg → tonnes
        return factors if factors else dict(_EMISSION_FACTORS_DEFAULT)
    return dict(_EMISSION_FACTORS_DEFAULT)


# ---------------------------------------------------------------------------
# Core functions (exported for Copilot tools)
# ---------------------------------------------------------------------------

def _get_lgc_position_core() -> Dict[str, Any]:
    """Get LGC/ACCU/STC portfolio position and liability status."""
    holdings = _query_gold(
        f"SELECT certificate_type, status, SUM(quantity) as total_qty, "
        f"SUM(total_value) as total_value, AVG(unit_price) as avg_price "
        f"FROM {_SCHEMA}.environmental_portfolio "
        f"GROUP BY certificate_type, status ORDER BY certificate_type, status"
    )

    balances = _query_gold(
        f"SELECT certificate_type, vintage_year, opening_balance, acquired, "
        f"surrendered, closing_balance, liability, surplus_deficit "
        f"FROM {_SCHEMA}.certificate_balances ORDER BY certificate_type, vintage_year"
    )

    # Aggregate by cert type
    by_type: Dict[str, Dict[str, Any]] = {}
    for h in (holdings or []):
        ct = h["certificate_type"]
        if ct not in by_type:
            by_type[ct] = {"held": 0, "committed": 0, "surrendered": 0, "total_value": 0}
        status = h.get("status", "HELD")
        qty = int(h.get("total_qty", 0) or 0)
        val = float(h.get("total_value", 0) or 0)
        if status == "HELD":
            by_type[ct]["held"] += qty
        elif status == "COMMITTED":
            by_type[ct]["committed"] += qty
        elif status == "SURRENDERED":
            by_type[ct]["surrendered"] += qty
        by_type[ct]["total_value"] += val

    return {
        "position_by_type": by_type,
        "holdings_detail": holdings or [],
        "balances": [{**b, "as_of_date": str(b.get("as_of_date", ""))} for b in (balances or [])],
        "market_prices": _get_cert_prices(),
    }


def _get_carbon_exposure_core(region: Optional[str] = None) -> Dict[str, Any]:
    """Calculate carbon exposure based on generation portfolio."""
    emission_factors = _get_emission_factors()

    # Get generation mix
    where = f"AND region_id = '{_sql_escape(region)}'" if region else ""
    gen = _query_gold(
        f"SELECT fuel_type, AVG(total_mw) as avg_mw "
        f"FROM {_SCHEMA}.nem_generation_by_fuel "
        f"WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS {where} "
        f"GROUP BY fuel_type"
    )

    exposure = []
    total_emissions = 0
    for g in (gen or []):
        fuel = g["fuel_type"]
        mw = float(g.get("avg_mw", 0) or 0)
        ef = emission_factors.get(fuel, 0)
        annual_mwh = mw * 8760
        annual_tco2 = annual_mwh * ef
        total_emissions += annual_tco2
        if ef > 0:
            exposure.append({
                "fuel_type": fuel,
                "avg_mw": round(mw, 1),
                "emission_factor": ef,
                "annual_mwh": round(annual_mwh, 0),
                "annual_tco2": round(annual_tco2, 0),
            })

    return {
        "region": region or "ALL",
        "exposure": sorted(exposure, key=lambda x: x["annual_tco2"], reverse=True),
        "total_annual_tco2": round(total_emissions, 0),
        "estimated_cost_aud": round(total_emissions * 32, 0),  # $32/tCO2 carbon price assumption
    }


def _value_bundled_ppa_core(strike_price: float, volume_mw: float,
                             technology: str, region: str,
                             term_years: int = 10,
                             lgc_price: Optional[float] = None) -> Dict[str, Any]:
    """Value a bundled PPA (electricity + LGCs)."""
    if lgc_price is None:
        lgc_price = _get_cert_prices().get("LGC", 47.50)
    # Energy value from forward curve
    from .curves import _build_forward_curve
    curve = _build_forward_curve(region=region, profile="FLAT")
    if curve:
        avg_forward = sum(p["price_mwh"] for p in curve) / len(curve)
    else:
        avg_forward = 75.0

    # Capacity factor by technology
    cf = {"solar_utility": 0.22, "wind": 0.35, "solar_rooftop": 0.15}.get(technology, 0.30)

    annual_mwh = volume_mw * cf * 8760
    annual_lgcs = round(annual_mwh)  # 1 LGC per MWh

    # Energy NPV
    energy_npv = 0
    lgc_npv = 0
    discount_rate = 0.065
    annual_cashflows = []
    for yr in range(1, term_years + 1):
        df = 1 / (1 + discount_rate) ** yr
        energy_cf = annual_mwh * (avg_forward - strike_price) * df
        lgc_cf = annual_lgcs * lgc_price * df * (0.98 ** yr)  # LGC price decline
        energy_npv += energy_cf
        lgc_npv += lgc_cf
        annual_cashflows.append({
            "year": yr,
            "energy_cashflow": round(energy_cf, 0),
            "lgc_cashflow": round(lgc_cf, 0),
            "total_cashflow": round(energy_cf + lgc_cf, 0),
        })

    total_npv = energy_npv + lgc_npv
    return {
        "strike_price": strike_price,
        "volume_mw": volume_mw,
        "technology": technology,
        "region": region,
        "term_years": term_years,
        "capacity_factor": cf,
        "annual_mwh": round(annual_mwh, 0),
        "annual_lgcs": annual_lgcs,
        "avg_forward_price": round(avg_forward, 2),
        "lgc_price": lgc_price,
        "energy_npv": round(energy_npv, 0),
        "lgc_npv": round(lgc_npv, 0),
        "total_npv": round(total_npv, 0),
        "bundled_premium_pct": round(lgc_npv / max(abs(energy_npv), 1) * 100, 1),
        "annual_cashflows": annual_cashflows,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/environmentals/dashboard")
async def environmentals_dashboard():
    """Environmental portfolio overview."""
    position = _get_lgc_position_core()
    carbon = _get_carbon_exposure_core()

    # LGC registry summary (accredited capacity by fuel)
    lgc_reg = _query_gold(
        f"SELECT fuel_type, COUNT(*) as count, SUM(capacity_mw) as total_mw "
        f"FROM {_SCHEMA}.lgc_registry GROUP BY fuel_type"
    )

    return {
        "certificate_position": position["position_by_type"],
        "market_prices": _get_cert_prices(),
        "lgc_registry_summary": lgc_reg or [],
        "carbon_exposure_summary": {
            "total_annual_tco2": carbon["total_annual_tco2"],
            "estimated_cost_aud": carbon["estimated_cost_aud"],
        },
        "balances": position["balances"],
    }


@router.get("/api/environmentals/portfolio")
async def environmental_portfolio(certificate_type: Optional[str] = None,
                                   status: Optional[str] = None):
    """Get environmental certificate holdings."""
    parts = []
    if certificate_type:
        parts.append(f"certificate_type = '{_sql_escape(certificate_type)}'")
    if status:
        parts.append(f"status = '{_sql_escape(status)}'")
    where = f"WHERE {' AND '.join(parts)}" if parts else ""

    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.environmental_portfolio {where} "
        f"ORDER BY certificate_type, vintage_year DESC LIMIT 100"
    )
    return {
        "holdings": [{**r, "acquired_date": str(r.get("acquired_date", "")),
                       "expiry_date": str(r.get("expiry_date", ""))}
                      for r in (rows or [])]
    }


@router.post("/api/environmentals/portfolio")
async def add_holding(request: Request):
    """Add a new certificate holding."""
    body = await request.json()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    qty = int(body.get("quantity", 100))
    price = float(body.get("unit_price", _get_cert_prices().get(body.get("certificate_type", "LGC"), 40)))
    data = {
        "holding_id": str(uuid.uuid4()),
        "certificate_type": body.get("certificate_type", "LGC"),
        "vintage_year": int(body.get("vintage_year", 2026)),
        "quantity": qty,
        "unit_price": price,
        "total_value": round(qty * price, 2),
        "status": "HELD",
        "source": body.get("source", "Market Purchase"),
        "acquired_date": body.get("acquired_date", str(date.today())),
        "expiry_date": body.get("expiry_date", str(date.today() + timedelta(days=730))),
    }
    ok = _insert_gold(f"{_SCHEMA}.environmental_portfolio", data)
    if ok:
        _invalidate_cache("sql:")
        return {"status": "created", "holding_id": data["holding_id"]}
    return JSONResponse(status_code=500, content={"error": "Failed to add holding"})


@router.get("/api/environmentals/balances")
async def certificate_balances():
    """Get certificate balance sheet."""
    rows = _query_gold(
        f"SELECT * FROM {_SCHEMA}.certificate_balances "
        f"ORDER BY certificate_type, vintage_year"
    )
    return {"balances": [{**r, "as_of_date": str(r.get("as_of_date", ""))}
                          for r in (rows or [])]}


@router.post("/api/environmentals/bundled-ppa")
async def value_bundled_ppa(request: Request):
    """Value a bundled PPA (electricity + LGCs)."""
    body = await request.json()
    return _value_bundled_ppa_core(
        strike_price=float(body.get("strike_price", 55)),
        volume_mw=float(body.get("volume_mw", 100)),
        technology=body.get("technology", "solar_utility"),
        region=body.get("region", "NSW1"),
        term_years=int(body.get("term_years", 10)),
        lgc_price=float(body.get("lgc_price", _get_cert_prices()["LGC"])),
    )
