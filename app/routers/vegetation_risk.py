"""Vegetation Risk & Electrical Line Clearance Compliance.

Tracks powerline vegetation clearance compliance under the Electricity Safety
(Bushfire Mitigation) Regulations, span-level risk scores, ELC inspection
schedules, and seasonal bushfire risk forecasting across the network.
"""
from __future__ import annotations

import random as _r
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from .shared import _query_gold, logger

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy", "SA Power Networks"]

_CLEARANCE_STATUSES = ["Compliant", "Non-Compliant", "Overdue", "Scheduled"]
_ZONES = ["BMO", "Non-BMO"]
_FUEL_LOADS = ["High", "Medium", "Low"]
_MITIGATION_STATUSES = ["On Track", "Behind", "Complete"]

_BUSHFIRE_ZONES = [
    ("Dandenong Ranges", True),
    ("Yarra Valley", True),
    ("Kinglake", True),
    ("Macedon Ranges", True),
    ("Otway Ranges", True),
    ("Strathbogie Ranges", True),
    ("East Gippsland", True),
    ("Latrobe Valley", False),
    ("Mornington Peninsula", False),
    ("Bellarine Peninsula", False),
    ("Geelong Metro", False),
    ("Melbourne CBD", False),
]


# =========================================================================
# GET /api/veg-risk/summary
# =========================================================================

@router.get("/api/veg-risk/summary")
async def veg_risk_summary() -> JSONResponse:
    """Network-wide vegetation risk and ELC compliance summary."""
    _r.seed(800)
    total_km = 47_200
    bmo_km = _r.randint(12_400, 18_600)
    return JSONResponse({
        "total_network_km": total_km,
        "bmo_zone_km": bmo_km,
        "high_risk_spans": _r.randint(1_800, 4_200),
        "critical_risk_spans": _r.randint(280, 840),
        "inspection_compliance_pct": round(_r.uniform(88.5, 97.8), 1),
        "avg_risk_score": round(_r.uniform(32.0, 58.0), 1),
        "outages_from_vegetation_ytd": _r.randint(180, 640),
        "clearance_work_due_km": round(_r.uniform(620.0, 2_400.0), 1),
        "data_source": "synthetic",
    })


# =========================================================================
# GET /api/veg-risk/spans
# =========================================================================

@router.get("/api/veg-risk/spans")
async def veg_risk_spans() -> JSONResponse:
    """Span-level vegetation risk scores and inspection status."""
    _r.seed(801)
    today = date.today()
    spans = []
    for i in range(40):
        zone = _r.choice(_ZONES)
        risk = round(_r.uniform(5.0, 100.0), 1)
        encroachment = round(_r.uniform(0.0, 2.8), 2)
        last_inspection = today - timedelta(days=_r.randint(30, 730))
        # BMO zones require annual inspection; Non-BMO every 3 years
        inspection_cycle_days = 365 if zone == "BMO" else 1095
        next_due = last_inspection + timedelta(days=inspection_cycle_days)
        overdue = next_due < today
        if overdue and encroachment > 1.5:
            status = "Overdue"
        elif next_due < today + timedelta(days=60):
            status = "Scheduled"
        elif encroachment > 1.8:
            status = "Non-Compliant"
        else:
            status = "Compliant"
        feeder_num = _r.randint(1, 30)
        spans.append({
            "span_id": f"SPN-{i+1:04d}",
            "feeder_id": f"FDR-{feeder_num:03d}",
            "zone": zone,
            "vegetation_risk_score": risk,
            "canopy_encroachment_m": encroachment,
            "last_inspection_date": last_inspection.isoformat(),
            "next_inspection_due": next_due.isoformat(),
            "clearance_status": status,
            "satellite_change_detected": _r.random() > 0.65,
            "outage_history_3yr": _r.randint(0, 5),
        })
    spans.sort(key=lambda x: x["vegetation_risk_score"], reverse=True)
    return JSONResponse({"spans": spans, "count": len(spans)})


# =========================================================================
# GET /api/veg-risk/elc-compliance
# =========================================================================

@router.get("/api/veg-risk/elc-compliance")
async def veg_risk_elc_compliance() -> JSONResponse:
    """Electrical line clearance compliance by DNSP."""
    _r.seed(802)
    dnsp_params = [
        ("AusNet Services",    18_400, 0.944),
        ("Ergon Energy",       24_800, 0.912),
        ("Energex",            22_100, 0.958),
        ("Ausgrid",            19_600, 0.961),
        ("Essential Energy",   31_200, 0.887),
        ("SA Power Networks",  16_800, 0.933),
    ]
    result = []
    for dnsp, total_spans, compliance_rate in dnsp_params:
        compliant = int(total_spans * compliance_rate * _r.uniform(0.98, 1.02))
        compliant = min(compliant, total_spans)
        compliance_pct = round(compliant / total_spans * 100, 1)
        overdue = int(total_spans * _r.uniform(0.008, 0.045))
        works_done = int(total_spans * _r.uniform(0.12, 0.28))
        works_outstanding = int(total_spans * _r.uniform(0.04, 0.12))
        reg_risk = (
            "High" if compliance_pct < 90.0 else
            "Medium" if compliance_pct < 95.0 else
            "Low"
        )
        result.append({
            "dnsp": dnsp,
            "total_spans": total_spans,
            "compliant_spans": compliant,
            "compliance_pct": compliance_pct,
            "overdue_inspections": overdue,
            "clearance_works_completed": works_done,
            "clearance_works_outstanding": works_outstanding,
            "regulatory_risk": reg_risk,
        })
    return JSONResponse({"elc_compliance": result, "count": len(result)})


# =========================================================================
# GET /api/veg-risk/bushfire-forecast
# =========================================================================

@router.get("/api/veg-risk/bushfire-forecast")
async def veg_risk_bushfire_forecast() -> JSONResponse:
    """Seasonal bushfire risk forecast by network zone."""
    _r.seed(803)
    result = []
    for zone_name, is_bmo in _BUSHFIRE_ZONES:
        current_risk = round(_r.uniform(20.0, 60.0) if not is_bmo else _r.uniform(45.0, 88.0), 1)
        peak_risk = round(min(current_risk * _r.uniform(1.1, 1.6), 100.0), 1)
        fuel_load = (
            "High" if peak_risk > 72 else
            "Medium" if peak_risk > 48 else
            "Low"
        )
        line_km = round(_r.uniform(80.0, 680.0), 1) if is_bmo else round(_r.uniform(20.0, 180.0), 1)
        high_risk_assets = _r.randint(12, 180) if is_bmo else _r.randint(2, 45)
        mitigation = (
            "Complete" if current_risk < 35 else
            "On Track" if _r.random() > 0.3 else
            "Behind"
        )
        result.append({
            "zone_name": zone_name,
            "bmo_zone": is_bmo,
            "risk_score_current": current_risk,
            "risk_score_peak_season": peak_risk,
            "fuel_load_rating": fuel_load,
            "powerline_length_km": line_km,
            "high_risk_assets": high_risk_assets,
            "mitigation_plan_status": mitigation,
        })
    return JSONResponse({"zones": result, "count": len(result)})


# =========================================================================
# GET /api/veg-risk/ml-scores
# =========================================================================

_ML_VEG_LOCATIONS = [
    ("Kinglake", "Kinglake BMO Ring Circuit"),
    ("Yarra Ranges", "Yarra Junction 22kV Feeder"),
    ("Dandenong Ranges", "Belgrave Heights Span"),
    ("Macedon", "Macedon Ranges 11kV Spur"),
    ("Healesville", "Healesville–Chum Creek"),
    ("Warburton", "Warburton Valley Feeder"),
    ("Gembrook", "Gembrook 11kV Rural Spur"),
    ("Cockatoo", "Cockatoo–Emerald Line"),
    ("Marysville", "Marysville BMO Feeder"),
    ("Toolangi", "Toolangi Forest Span"),
    ("Narbethong", "Narbethong 11kV Section"),
    ("Eildon", "Eildon Pondage Circuit"),
    ("Alexandra", "Alexandra Distribution Feeder"),
    ("Mansfield", "Mansfield 22kV Rural"),
    ("Yea", "Yea River Crossing Span"),
    ("Strathewen", "Strathewen BMO Span"),
    ("Hurstbridge", "Hurstbridge 11kV Spur"),
    ("Christmas Hills", "Christmas Hills Rural Feeder"),
    ("Kangaroo Ground", "Kangaroo Ground 22kV"),
    ("St Andrews", "St Andrews BMO Line"),
]

_VEG_TREE_SPECIES = ["Mountain Ash", "Alpine Ash", "Manna Gum", "Stringybark", "Wattle Regrowth", "Native Eucalypt", "Blackwood"]
_VEG_ACTIONS = ["Emergency Trim", "Priority Trim", "Schedule Trim", "Inspect & Monitor"]
_VEG_TOP_FEATURES = ["fire_history_score", "inspection_age_days", "tree_species_risk", "last_trim_months", "wind_exposure", "soil_moisture"]


@router.get("/api/veg-risk/ml-scores")
async def veg_risk_ml_scores() -> JSONResponse:
    """ML-scored span risk classification — XGBoost Classifier with MLflow metadata."""
    _r.seed(805)

    feature_importances = [
        {"feature": "Fire History Score", "importance": 0.28},
        {"feature": "Inspection Age (days)", "importance": 0.21},
        {"feature": "Tree Species Risk", "importance": 0.17},
        {"feature": "Last Trim (months)", "importance": 0.14},
        {"feature": "Wind Exposure", "importance": 0.11},
        {"feature": "Soil Moisture Index", "importance": 0.05},
        {"feature": "Slope %", "importance": 0.04},
    ]

    spans = []
    for i, (suburb, location) in enumerate(_ML_VEG_LOCATIONS):
        span_length = _r.randint(180, 820)
        fire_history = round(_r.uniform(1.0, 9.8), 1)
        inspection_age = _r.randint(45, 580)
        last_trim = _r.randint(3, 36)
        ml_confidence = round(_r.uniform(0.71, 0.98), 2)

        # Derive risk tier from composite score
        raw_score = (fire_history / 10.0) * 0.35 + min(inspection_age / 600, 1.0) * 0.30 + (last_trim / 36.0) * 0.20 + _r.uniform(0, 0.15)
        if raw_score > 0.70:
            risk_tier = "Critical"
            action = "Emergency Trim"
        elif raw_score > 0.50:
            risk_tier = "High"
            action = "Priority Trim"
        elif raw_score > 0.30:
            risk_tier = "Medium"
            action = "Schedule Trim"
        else:
            risk_tier = "Low"
            action = "Inspect & Monitor"

        spans.append({
            "span_id": f"SP-{4000 + i + 1:04d}",
            "location": location,
            "suburb": suburb,
            "span_length_m": span_length,
            "risk_tier": risk_tier,
            "ml_confidence": ml_confidence,
            "fire_history_score": fire_history,
            "inspection_age_days": inspection_age,
            "last_trim_months": last_trim,
            "top_feature": _r.choice(_VEG_TOP_FEATURES),
            "action": action,
        })

    spans.sort(key=lambda x: (
        {"Critical": 3, "High": 2, "Medium": 1, "Low": 0}[x["risk_tier"]],
        x["ml_confidence"]
    ), reverse=True)

    risk_counts: dict[str, int] = {"Critical": 0, "High": 0, "Medium": 0, "Low": 0}
    for s in spans:
        risk_counts[s["risk_tier"]] += 1

    # Simulate "newly flagged" spans not in prior manual assessment
    new_critical = max(1, risk_counts["Critical"] - 1)

    return JSONResponse({
        "model_metadata": {
            "model_name": "veg_risk_classifier_v2",
            "mlflow_run_id": "b7d3c9e2f1a845670bcde234567890abcdef5678",
            "algorithm": "XGBoost Classifier",
            "accuracy": 0.887,
            "f1_macro": 0.863,
            "training_date": "2026-02-28",
            "features": ["span_length_m", "tree_species_risk", "soil_moisture", "fire_history_score",
                         "inspection_age_days", "wind_exposure", "slope_pct", "last_trim_months"],
        },
        "risk_summary": {
            **risk_counts,
            "total_spans_scored": len(spans),
            "model_flagged_new_critical": new_critical,
        },
        "top_risk_spans": spans,
        "feature_importances": feature_importances,
    })
