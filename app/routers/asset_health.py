"""Asset Health & Lifecycle Management.

Asset condition monitoring, risk matrix, and replacement forecasting
for distribution network assets across Australian DNSPs.
"""
from __future__ import annotations

import random as _r
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()

_ASSET_TYPES = [
    "Zone Substation Transformer", "HV Feeder Cable", "Distribution Transformer",
    "Pole & Cross Arm", "Switchgear", "Protection Relay", "Capacitor Bank",
]
_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy"]
_RISK_RATINGS = ["Critical", "High", "Medium", "Low"]
_CONSEQUENCES = ["Catastrophic", "Major", "Moderate", "Minor"]
_LIKELIHOODS = ["Almost Certain", "Likely", "Possible", "Unlikely"]


@router.get("/api/asset-health/summary")
async def asset_health_summary() -> JSONResponse:
    """Asset health KPI summary across all DNSPs."""
    _r.seed(500)
    total = _r.randint(18000, 25000)
    high_risk = _r.randint(800, 2000)
    critical = _r.randint(50, 300)
    return JSONResponse({
        "total_assets": total,
        "high_risk_count": high_risk,
        "critical_count": critical,
        "avg_health_score": round(_r.uniform(62.0, 78.0), 1),
        "replacement_capex_m_aud": round(_r.uniform(420.0, 680.0), 2),
        "assets_past_eol": _r.randint(1200, 3500),
        "data_source": "synthetic",
    })


@router.get("/api/asset-health/assets")
async def asset_health_assets(
    dnsp: Optional[str] = Query(None, description="Filter by DNSP name"),
    risk_rating: Optional[str] = Query(None, description="Critical/High/Medium/Low"),
) -> JSONResponse:
    """Asset register with health scores and risk ratings."""
    _r.seed(501)
    today_dt = date.today()
    mock = []
    for i in range(60):
        d = dnsp or _r.choice(_DNSPS)
        rr = risk_rating or _r.choices(_RISK_RATINGS, weights=[5, 25, 45, 25])[0]
        age = _r.randint(5, 55)
        health = round(_r.uniform(20.0, 95.0), 1)
        if rr == "Critical":
            health = round(_r.uniform(10.0, 35.0), 1)
        elif rr == "High":
            health = round(_r.uniform(30.0, 55.0), 1)
        eol_year = 2026 + _r.randint(-10, 15)
        last_insp = today_dt - timedelta(days=_r.randint(30, 730))
        mock.append({
            "asset_id": f"AST-{d[:3].upper()}-{i+1:05d}",
            "asset_type": _r.choice(_ASSET_TYPES),
            "dnsp": d,
            "zone_substation": f"ZSS-{_r.randint(1, 50):02d}",
            "age_years": age,
            "health_score": health,
            "risk_rating": rr,
            "eol_year": eol_year,
            "replacement_cost_k_aud": round(_r.uniform(50.0, 8500.0), 1),
            "last_inspection": last_insp.isoformat(),
        })
    return JSONResponse({"assets": mock, "count": len(mock)})


@router.get("/api/asset-health/risk-matrix")
async def asset_health_risk_matrix() -> JSONResponse:
    """Risk matrix combining consequence and likelihood ratings."""
    _r.seed(502)
    matrix = []
    for atype in _ASSET_TYPES:
        consequence = _r.choice(_CONSEQUENCES)
        likelihood = _r.choice(_LIKELIHOODS)
        risk_score = round(_r.uniform(2.0, 20.0), 1)
        count = _r.randint(10, 500)
        matrix.append({
            "asset_type": atype,
            "consequence": consequence,
            "likelihood": likelihood,
            "risk_score": risk_score,
            "count": count,
            "total_replacement_m_aud": round(count * _r.uniform(0.05, 8.5), 2),
        })
    return JSONResponse({"matrix": matrix})


@router.get("/api/asset-health/replacement-forecast")
async def asset_health_replacement_forecast() -> JSONResponse:
    """5-year capital replacement forecast."""
    _r.seed(503)
    forecast = []
    cumulative = 0.0
    current_year = 2026
    for i in range(5):
        yr = current_year + i
        assets_due = _r.randint(120, 450)
        capex = round(_r.uniform(80.0, 220.0), 2)
        cumulative = round(cumulative + capex, 2)
        forecast.append({
            "year": yr,
            "assets_due": assets_due,
            "capex_m_aud": capex,
            "cumulative_m_aud": cumulative,
        })
    return JSONResponse({"forecast": forecast})
