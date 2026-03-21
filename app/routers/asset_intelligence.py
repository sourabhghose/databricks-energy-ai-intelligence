"""Asset Intelligence — Health Scoring, Expenditure Justification & Cross-System Integration.

Combines asset condition data from Maximo, SAP, and GIS to produce composite
health scores, AER expenditure justifications, and network criticality rankings
for Australian DNSP asset portfolios.
"""
from __future__ import annotations

import random as _r
from datetime import date

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy", "SA Power Networks"]

_ASSET_CLASSES = ["Transformer", "Switchgear", "Cable", "Pole", "Protection"]
_CONSEQUENCES = ["High", "Medium", "Low"]
_ACTIONS = ["Replace", "Refurbish", "Monitor", "Inspect"]
_CRITICALITIES = ["High", "Medium", "Low"]
_STRENGTH = ["Strong", "Moderate", "Weak"]


# =========================================================================
# GET /api/asset-intel/summary
# =========================================================================

@router.get("/api/asset-intel/summary")
async def asset_intel_summary() -> JSONResponse:
    """Portfolio-level asset health and regulatory spend summary."""
    _r.seed(740)
    return JSONResponse({
        "total_assets": 84_230,
        "avg_health_score": round(_r.uniform(62.0, 78.0), 1),
        "high_risk_pct": round(_r.uniform(8.5, 18.0), 1),
        "critical_count": _r.randint(120, 480),
        "total_replacement_value_m": round(_r.uniform(4_200.0, 6_800.0), 1),
        "assets_past_eol": _r.randint(3_800, 9_200),
        "benchmarked_efficiency_pct": round(_r.uniform(72.0, 91.0), 1),
        "regulatory_spend_justified_pct": round(_r.uniform(84.0, 97.0), 1),
        "data_source": "synthetic",
    })


# =========================================================================
# GET /api/asset-intel/health-scores
# =========================================================================

@router.get("/api/asset-intel/health-scores")
async def asset_intel_health_scores() -> JSONResponse:
    """Individual asset health scores with failure probability and recommended actions."""
    _r.seed(741)
    assets = []
    today = date.today()
    for i in range(40):
        dnsp = _r.choice(_DNSPS)
        asset_class = _r.choice(_ASSET_CLASSES)
        age = _r.randint(5, 55)
        health = round(max(5.0, 100.0 - age * _r.uniform(0.8, 2.2) + _r.uniform(-10, 10)), 1)
        health = min(health, 100.0)
        fail_prob = round(max(0.5, (100 - health) * _r.uniform(0.3, 0.8)), 1)
        consequence = (
            "High" if health < 40 else
            "Medium" if health < 65 else
            "Low"
        )
        action = (
            "Replace" if health < 35 else
            "Refurbish" if health < 55 else
            "Monitor" if health < 75 else
            "Inspect"
        )
        eol_year = today.year + max(1, int((health / 100) * 25))
        capex = round(_r.uniform(50, 2_500), 0)
        assets.append({
            "asset_id": f"AST-{dnsp[:3].upper()}-{i+1:04d}",
            "asset_class": asset_class,
            "dnsp": dnsp,
            "age_years": age,
            "health_score": health,
            "failure_prob_pct": fail_prob,
            "consequence": consequence,
            "recommended_action": action,
            "estimated_eol_year": eol_year,
            "capex_required_k": capex,
        })
    assets.sort(key=lambda x: x["health_score"])
    return JSONResponse({"assets": assets, "count": len(assets)})


# =========================================================================
# GET /api/asset-intel/expenditure-justification
# =========================================================================

@router.get("/api/asset-intel/expenditure-justification")
async def asset_intel_expenditure_justification() -> JSONResponse:
    """AER expenditure justification by asset category."""
    _r.seed(742)
    categories = [
        ("Zone Substations",      85.0, 92.0),
        ("Distribution Transformers", 42.0, 45.0),
        ("Underground Cable",     68.0, 71.0),
        ("Overhead Lines",        55.0, 58.0),
        ("Protection Systems",    28.0, 30.0),
        ("SCADA & Control",       22.0, 24.0),
        ("Metering Infrastructure", 18.0, 19.5),
        ("Pole & Structures",     38.0, 40.0),
    ]
    evidence_options = [
        "Asset age profile analysis demonstrates replacement urgency",
        "Failure rate data supports investment case over 5-year horizon",
        "N-1 security compliance requires augmentation by 2027",
        "Bushfire risk mitigation underpins network hardening spend",
        "DER integration requirements drive protection system upgrades",
        "Customer reliability targets necessitate cable replacement program",
        "AER RIN data confirms peer-benchmarked efficiency of capital spend",
    ]
    result = []
    for category, actual, allowed in categories:
        ratio = round(actual / allowed, 3)
        aer_justified = ratio <= 1.05
        strength = (
            "Strong" if ratio < 0.95 else
            "Moderate" if ratio <= 1.0 else
            "Weak"
        )
        result.append({
            "category": category,
            "actual_spend_m": actual,
            "allowed_spend_m": allowed,
            "efficiency_ratio": ratio,
            "aer_justified": aer_justified,
            "justification_strength": strength,
            "key_evidence": _r.choice(evidence_options),
        })
    return JSONResponse({"expenditure": result, "count": len(result)})


# =========================================================================
# GET /api/asset-intel/cross-system
# =========================================================================

@router.get("/api/asset-intel/cross-system")
async def asset_intel_cross_system() -> JSONResponse:
    """Cross-system asset view combining Maximo, SAP, and GIS data."""
    _r.seed(743)
    assets = []
    for i in range(30):
        dnsp = _r.choice(_DNSPS)
        maximo_score = round(_r.uniform(30.0, 100.0), 1)
        sap_cost = round(_r.uniform(12.0, 980.0), 2)
        criticality = _r.choice(_CRITICALITIES)
        outage_mins = round(_r.uniform(0.0, 420.0), 1)
        composite = round(
            (maximo_score * 0.4) +
            ({"High": 40, "Medium": 25, "Low": 10}[criticality]) +
            min(20, outage_mins / 25.0),
            1
        )
        assets.append({
            "asset_id": f"AST-{dnsp[:3].upper()}-{i+1:04d}",
            "maximo_condition_score": maximo_score,
            "sap_cost_ytd_k": sap_cost,
            "gis_network_criticality": criticality,
            "outage_contribution_mins": outage_mins,
            "composite_priority_score": min(composite, 100.0),
        })
    assets.sort(key=lambda x: x["composite_priority_score"], reverse=True)
    return JSONResponse({"assets": assets, "count": len(assets)})


# =========================================================================
# GET /api/asset-intel/ml-predictions
# =========================================================================

_AUSNET_LOCATIONS = [
    ("Dandenong", "Dandenong Zone Substation"),
    ("Ringwood", "Ringwood 66kV Feeder"),
    ("Frankston", "Frankston Distribution Network"),
    ("Geelong", "Geelong South Transformer"),
    ("Ballarat", "Ballarat Ring Main"),
    ("Bendigo", "Bendigo Zone Substation"),
    ("Pakenham", "Pakenham Growth Corridor"),
    ("Cranbourne", "Cranbourne Industrial Feeder"),
    ("Mornington", "Mornington Peninsula 22kV"),
    ("Warnambool", "Warrnambool West Feeder"),
    ("Hamilton", "Hamilton Zone Substation"),
    ("Sale", "Sale 66kV Bus"),
    ("Traralgon", "Traralgon Distribution"),
    ("Wodonga", "Wodonga Industrial"),
    ("Shepparton", "Shepparton 22kV Feeder"),
    ("Mildura", "Mildura Zone Substation"),
    ("Sunbury", "Sunbury Ring Circuit"),
    ("Melton", "Melton Growth Area"),
    ("Werribee", "Werribee South Feeder"),
    ("Laverton", "Laverton Industrial Park"),
    ("Bayswater", "Bayswater 11kV Network"),
    ("Ferntree Gully", "Ferntree Gully Overhead"),
    ("Lilydale", "Lilydale 22kV Feeder"),
    ("Healesville", "Healesville BMO Span"),
    ("Yea", "Yea Zone Substation"),
]

_ML_ASSET_CLASSES = ["Transformer", "Switchgear", "Cable", "Pole", "Protection", "Zone Substation"]
_ML_ACTIONS = ["Replace", "Refurbish", "Monitor", "Inspect"]
_TOP_DRIVERS = [
    "Asset Age (years)", "Last Inspection Score", "Fault History (5yr)",
    "Load Factor %", "Environment Severity", "Overload Events",
]


@router.get("/api/asset-intel/ml-predictions")
async def asset_intel_ml_predictions() -> JSONResponse:
    """XGBoost failure predictions per asset — MLflow model metadata included."""
    _r.seed(745)

    feature_importances = [
        {"feature": "Asset Age (years)", "importance": 0.31},
        {"feature": "Last Inspection Score", "importance": 0.22},
        {"feature": "Fault History (5yr)", "importance": 0.18},
        {"feature": "Load Factor %", "importance": 0.12},
        {"feature": "Environment Severity", "importance": 0.09},
        {"feature": "Overload Events", "importance": 0.05},
        {"feature": "Other Features (18)", "importance": 0.03},
    ]

    predictions = []
    for i, (suburb, location) in enumerate(_AUSNET_LOCATIONS):
        asset_class = _r.choice(_ML_ASSET_CLASSES)
        failure_prob_12m = round(_r.uniform(0.05, 0.95), 2)
        failure_prob_3m = round(failure_prob_12m * _r.uniform(0.25, 0.55), 2)
        if failure_prob_12m > 0.7:
            risk_tier = "Critical"
        elif failure_prob_12m > 0.4:
            risk_tier = "High"
        elif failure_prob_12m > 0.2:
            risk_tier = "Medium"
        else:
            risk_tier = "Low"
        confidence = round(_r.uniform(0.82, 0.97), 2)
        top_driver = _r.choice(_TOP_DRIVERS)
        action = (
            "Replace" if failure_prob_12m > 0.7 else
            "Refurbish" if failure_prob_12m > 0.4 else
            "Monitor" if failure_prob_12m > 0.2 else
            "Inspect"
        )
        cost_if_fail = round(_r.uniform(0.4, 4.8), 2)
        predictions.append({
            "asset_id": f"AST-AUS-{i+1:04d}",
            "asset_class": asset_class,
            "location": location,
            "suburb": suburb,
            "failure_prob_12m": failure_prob_12m,
            "failure_prob_3m": failure_prob_3m,
            "risk_tier": risk_tier,
            "confidence": confidence,
            "top_driver": top_driver,
            "recommended_action": action,
            "cost_if_fail_m": cost_if_fail,
        })

    predictions.sort(key=lambda x: x["failure_prob_12m"], reverse=True)

    risk_dist: dict[str, int] = {"Critical": 0, "High": 0, "Medium": 0, "Low": 0}
    for p in predictions:
        risk_dist[p["risk_tier"]] += 1

    return JSONResponse({
        "model_metadata": {
            "model_name": "asset_failure_xgb_v3",
            "mlflow_run_id": "a3f8b2c1d4e567890abcdef1234567890abcdef12",
            "registered_model_version": 3,
            "accuracy": 0.923,
            "precision": 0.891,
            "recall": 0.887,
            "f1": 0.889,
            "auc_roc": 0.961,
            "training_date": "2026-03-15",
            "features_used": 24,
            "training_samples": 48230,
        },
        "feature_importances": feature_importances,
        "predictions": predictions,
        "risk_distribution": risk_dist,
    })
