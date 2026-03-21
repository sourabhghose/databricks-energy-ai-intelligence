"""Regulatory Asset Base (RAB) Roll-Forward & WACC Sensitivity.

Tracks RAB movements — capex additions, disposals, depreciation,
and inflation indexing — plus allowed revenue sensitivity to WACC.
"""
from __future__ import annotations

import random as _r

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex"]
_RAB_BASE = {
    "AusNet Services": 7_200.0,   # $M
    "Ergon Energy":    9_100.0,
    "Energex":         8_300.0,
}
_CAPEX_CATEGORIES = [
    "Network Augmentation", "Asset Replacement", "Connections",
    "Demand Management", "IT & Technology", "Land & Buildings", "Other Capex",
]


@router.get("/api/rab/summary")
async def rab_summary(
    dnsp: str = Query("AusNet Services", description="DNSP name"),
) -> JSONResponse:
    """Current-year RAB movement summary for a DNSP."""
    _r.seed(560)
    base = _RAB_BASE.get(dnsp, 7_200.0)
    capex_add = round(_r.uniform(base * 0.04, base * 0.08), 2)
    disposals = round(_r.uniform(base * 0.005, base * 0.015), 2)
    depreciation = round(_r.uniform(base * 0.025, base * 0.040), 2)
    inflation = round(base * _r.uniform(0.03, 0.055), 2)
    closing = round(base + capex_add - disposals - depreciation + inflation, 2)
    return JSONResponse({
        "dnsp": dnsp,
        "total_rab_m_aud": base,
        "ytd_capex_additions_m_aud": capex_add,
        "ytd_disposals_m_aud": disposals,
        "depreciation_m_aud": depreciation,
        "inflation_indexing_m_aud": inflation,
        "closing_rab_forecast_m_aud": closing,
        "data_source": "synthetic",
    })


@router.get("/api/rab/rollforward")
async def rab_rollforward(
    dnsp: str = Query("AusNet Services", description="DNSP name"),
) -> JSONResponse:
    """5-year RAB roll-forward schedule."""
    _r.seed(561)
    base = _RAB_BASE.get(dnsp, 7_200.0)
    rollforward = []
    opening = base
    for i, yr in enumerate(range(2022, 2027)):
        capex = round(_r.uniform(opening * 0.04, opening * 0.08), 2)
        disposals = round(_r.uniform(opening * 0.005, opening * 0.012), 2)
        depreciation = round(_r.uniform(opening * 0.025, opening * 0.038), 2)
        inflation = round(opening * _r.uniform(0.025, 0.055), 2)
        closing = round(opening + capex - disposals - depreciation + inflation, 2)
        rollforward.append({
            "year": yr,
            "opening_rab": opening,
            "capex_additions": capex,
            "disposals": disposals,
            "depreciation": depreciation,
            "inflation_index": inflation,
            "closing_rab": closing,
            "dnsp": dnsp,
        })
        opening = closing
    return JSONResponse({"rollforward": rollforward})


@router.get("/api/rab/capex-categories")
async def rab_capex_categories(
    dnsp: str = Query("AusNet Services", description="DNSP name"),
) -> JSONResponse:
    """Capex by category — approved allowance vs actual spend."""
    _r.seed(562)
    base = _RAB_BASE.get(dnsp, 7_200.0)
    categories = []
    for cat in _CAPEX_CATEGORIES:
        approved = round(_r.uniform(base * 0.005, base * 0.018), 2)
        actual = round(approved * _r.uniform(0.80, 1.15), 2)
        variance_pct = round((actual - approved) / approved * 100, 1)
        rab_eligible = round(_r.uniform(75.0, 100.0), 1)
        risk = "Low" if abs(variance_pct) < 5 else ("Medium" if abs(variance_pct) < 15 else "High")
        categories.append({
            "category": cat,
            "approved_m_aud": approved,
            "actual_m_aud": actual,
            "variance_pct": variance_pct,
            "rab_eligible_pct": rab_eligible,
            "inclusion_risk": risk,
        })
    return JSONResponse({"categories": categories, "dnsp": dnsp})


@router.get("/api/rab/wacc-sensitivity")
async def rab_wacc_sensitivity(
    dnsp: str = Query("AusNet Services", description="DNSP name"),
) -> JSONResponse:
    """Allowed revenue sensitivity across 5 WACC scenarios (5%–9%)."""
    _r.seed(563)
    base_rab = _RAB_BASE.get(dnsp, 7_200.0)
    base_wacc = 0.0685
    base_revenue = round(base_rab * base_wacc + base_rab * 0.032, 2)
    scenarios = []
    for wacc_pct in [5.0, 6.0, 7.0, 8.0, 9.0]:
        wacc = wacc_pct / 100
        revenue = round(base_rab * wacc + base_rab * 0.032, 2)
        delta = round(revenue - base_revenue, 2)
        scenarios.append({
            "wacc_pct": wacc_pct,
            "allowed_revenue_m_aud": revenue,
            "delta_vs_base_m_aud": delta,
        })
    return JSONResponse({"scenarios": scenarios, "base_wacc_pct": round(base_wacc * 100, 2), "dnsp": dnsp})
