"""AER Efficiency Benchmarking — Opex, Partial Productivity & Reset Readiness.

Provides peer benchmarking of distribution network service provider efficiency
against AER frontier comparisons, partial productivity measures, and reset
submission readiness indicators for the 2025-2030 regulatory period.
"""
from __future__ import annotations

import random as _r

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy", "SA Power Networks"]


# =========================================================================
# GET /api/benchmarking/summary
# =========================================================================

@router.get("/api/benchmarking/summary")
async def benchmarking_summary() -> JSONResponse:
    """Own-DNSP efficiency summary relative to industry frontier and peers."""
    _r.seed(760)
    own_score = round(_r.uniform(68.0, 84.0), 1)
    peer_avg = round(_r.uniform(62.0, 78.0), 1)
    return JSONResponse({
        "own_efficiency_score": own_score,
        "peer_avg_efficiency": peer_avg,
        "frontier_efficiency": 100.0,
        "opex_per_customer_aud": round(_r.uniform(420.0, 620.0), 2),
        "opex_per_km_aud": round(_r.uniform(8_200.0, 14_500.0), 2),
        "industry_avg_opex_per_customer": round(_r.uniform(480.0, 580.0), 2),
        "industry_avg_opex_per_km": round(_r.uniform(9_500.0, 12_800.0), 2),
        "trend_direction": _r.choice(["Improving", "Stable", "Declining"]),
        "reset_year": 2025,
        "dnsp_name": "AusNet Services",
        "data_source": "synthetic",
    })


# =========================================================================
# GET /api/benchmarking/peers
# =========================================================================

@router.get("/api/benchmarking/peers")
async def benchmarking_peers() -> JSONResponse:
    """Per-DNSP efficiency and cost benchmarking across the NEM."""
    _r.seed(761)
    peer_data = [
        ("AusNet Services",    True,   75.0,   490.0,  10_200.0),
        ("Ergon Energy",       False, 140.0,   580.0,   8_400.0),
        ("Energex",            False,  80.0,   445.0,  13_100.0),
        ("Ausgrid",            False,  70.0,   410.0,  14_800.0),
        ("Essential Energy",   False, 160.0,   620.0,   7_900.0),
        ("SA Power Networks",  False,  90.0,   510.0,  11_500.0),
        ("CitiPower",          False,  55.0,   380.0,  16_200.0),
        ("Powercor",           False,  95.0,   525.0,  10_800.0),
    ]
    peers = []
    for rank, (dnsp, is_self, saidi_t, opex_cust_base, opex_km_base) in enumerate(peer_data, start=1):
        opex_cust = round(opex_cust_base * _r.uniform(0.92, 1.08), 2)
        opex_km = round(opex_km_base * _r.uniform(0.92, 1.08), 2)
        saidi = round(saidi_t * _r.uniform(0.80, 1.20), 2)
        eff = round(_r.uniform(58.0, 96.0), 1)
        peers.append({
            "dnsp_name": dnsp,
            "opex_per_customer": opex_cust,
            "opex_per_km": opex_km,
            "saidi_minutes": saidi,
            "reliability_rank": rank,
            "efficiency_score": eff,
            "is_self": is_self,
        })
    return JSONResponse({"peers": peers, "count": len(peers)})


# =========================================================================
# GET /api/benchmarking/partial-productivity
# =========================================================================

@router.get("/api/benchmarking/partial-productivity")
async def benchmarking_partial_productivity() -> JSONResponse:
    """Partial productivity index measures vs industry quartiles."""
    _r.seed(762)
    measures = [
        ("Opex per Customer Served",  490.0,  380.0,  510.0,  640.0, "AUD/customer"),
        ("Opex per Circuit km",     10_200.0, 7_800.0, 10_500.0, 13_900.0, "AUD/km"),
        ("Opex per MWh Distributed",  18.50,  12.00,  17.50,   24.00, "AUD/MWh"),
    ]
    result = []
    for name, own_base, p25, p50, p75, unit in measures:
        own_val = round(own_base * _r.uniform(0.90, 1.10), 2)
        # Derive own percentile based on where own_val sits vs quartiles
        if own_val <= p25:
            own_pct = round(_r.uniform(5.0, 25.0), 1)
        elif own_val <= p50:
            own_pct = round(_r.uniform(26.0, 50.0), 1)
        elif own_val <= p75:
            own_pct = round(_r.uniform(51.0, 74.0), 1)
        else:
            own_pct = round(_r.uniform(75.0, 95.0), 1)
        trend = round(_r.uniform(-4.5, 4.5), 2)
        result.append({
            "measure_name": name,
            "unit": unit,
            "own_value": own_val,
            "industry_p25": p25,
            "industry_p50": p50,
            "industry_p75": p75,
            "own_percentile": own_pct,
            "trend_3yr": trend,
        })
    return JSONResponse({"measures": result, "count": len(result)})


# =========================================================================
# GET /api/benchmarking/reset-readiness
# =========================================================================

@router.get("/api/benchmarking/reset-readiness")
async def benchmarking_reset_readiness() -> JSONResponse:
    """Regulatory reset submission readiness by area."""
    _r.seed(763)
    areas = [
        (
            "Demand Forecasting",
            _r.randint(65, 92),
            ["Load growth uncertainty in outer suburbs", "EV uptake modelling assumptions challenged by AER"],
            ["Engage independent demand forecaster", "Submit sensitivity analysis with base case"],
            "Required",
        ),
        (
            "Opex Efficiency Assessment",
            _r.randint(55, 82),
            ["Benchmarking methodology contested in prior reset", "Shadow opex model not yet calibrated to 2025 data"],
            ["Conduct SFA/DEA analysis with updated peer data", "Commission AER-preferred benchmarking consultant"],
            "Required",
        ),
        (
            "Capex Justification",
            _r.randint(60, 88),
            ["Asset condition data gaps in rural network", "Prudency test threshold tightened since 2020 reset"],
            ["Complete asset condition survey by June 2026", "Map capex line items to network need triggers"],
            "Required",
        ),
        (
            "Reliability & Quality of Supply",
            _r.randint(72, 95),
            ["SAIDI exclusion methodology under AER review", "Climate-adjusted targets not yet agreed"],
            ["Engage with AER on exclusion framework", "Prepare bushfire adjustment evidence pack"],
            "Recommended",
        ),
        (
            "DER & Network Integration",
            _r.randint(48, 76),
            ["Hosting capacity assessment not complete for all feeders", "Export management scheme design pending"],
            ["Accelerate feeder-level hosting capacity mapping", "Align with AEMC DER integration rule changes"],
            "Required",
        ),
        (
            "Customer Engagement",
            _r.randint(70, 90),
            ["Customer preference survey sample size below AER guidance", "Vulnerable customer consultation incomplete"],
            ["Expand survey to 3,000+ residential customers", "Hold targeted workshops with concession card holders"],
            "Recommended",
        ),
    ]
    result = []
    for area, score, risks, actions, engagement in areas:
        result.append({
            "area": area,
            "readiness_score": score,
            "key_risks": risks,
            "recommended_actions": actions,
            "consultant_engagement": engagement,
        })
    return JSONResponse({"reset_readiness": result, "count": len(result), "reset_year": 2025})
