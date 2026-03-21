"""DER Export Management — Dynamic Operating Envelopes (DOE).

Tracks distributed energy resource export limits, feeder solar penetration,
curtailment events, and DOE settings across distribution feeders.
"""
from __future__ import annotations

import random as _r
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()

_DNSPS = ["AusNet Services", "Ergon Energy", "Energex", "Ausgrid", "Essential Energy"]
_DOE_TYPES = ["Static", "Dynamic", "Flexible"]
_CURTAILMENT_CAUSES = [
    "Feeder Thermal Limit", "Voltage Rise", "Protection Coordination",
    "Network Congestion", "Reactive Power Constraint",
]
_FEEDER_STATUSES = ["Active", "Monitoring", "Constrained", "Under Review"]


@router.get("/api/der-export/summary")
async def der_export_summary() -> JSONResponse:
    """DER export and DOE compliance summary."""
    _r.seed(620)
    return JSONResponse({
        "feeders_with_doe": _r.randint(80, 350),
        "total_solar_capacity_mw": round(_r.uniform(1200.0, 4500.0), 1),
        "avg_export_limit_kw": round(_r.uniform(2.5, 10.0), 2),
        "curtailment_events_30d": _r.randint(50, 500),
        "curtailment_energy_mwh": round(_r.uniform(50.0, 800.0), 1),
        "doe_compliance_pct": round(_r.uniform(88.0, 99.0), 1),
        "data_source": "synthetic",
    })


@router.get("/api/der-export/feeders")
async def der_export_feeders(
    dnsp: Optional[str] = Query(None, description="Filter by DNSP"),
) -> JSONResponse:
    """Feeder-level DER export and DOE status."""
    _r.seed(621)
    target_dnsps = [dnsp] if dnsp else _DNSPS
    feeders = []
    for d in target_dnsps:
        for i in range(12):
            solar_pen = round(_r.uniform(15.0, 185.0), 1)
            doe_active = solar_pen > 80.0 or _r.random() > 0.3
            hosting_remaining = round(_r.uniform(0.0, 500.0) if not doe_active else _r.uniform(0.0, 150.0), 1)
            feeders.append({
                "feeder_id": f"FDR-{d[:3].upper()}-{i+1:03d}",
                "zone_substation": f"ZSS-{_r.randint(1, 50):02d}",
                "dnsp": d,
                "solar_penetration_pct": solar_pen,
                "doe_active": doe_active,
                "export_limit_kw": round(_r.uniform(1.5, 10.0), 2) if doe_active else None,
                "curtailment_events_30d": _r.randint(0, 30) if doe_active else 0,
                "hosting_capacity_remaining_kw": hosting_remaining,
                "status": _r.choice(_FEEDER_STATUSES),
            })
    return JSONResponse({"feeders": feeders, "count": len(feeders)})


@router.get("/api/der-export/curtailment-events")
async def der_export_curtailment_events() -> JSONResponse:
    """Curtailment events in the last 30 days."""
    _r.seed(622)
    today_dt = datetime.now(timezone.utc)
    events = []
    feeder_ids = [f"FDR-{d[:3].upper()}-{i:03d}" for d in _DNSPS for i in range(1, 8)]
    for i in range(40):
        ts = today_dt - timedelta(days=_r.randint(0, 30), hours=_r.randint(0, 23))
        duration = _r.randint(5, 180)
        events.append({
            "event_id": f"CUR-{i+1:05d}",
            "feeder_id": _r.choice(feeder_ids),
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "duration_min": duration,
            "energy_curtailed_kwh": round(_r.uniform(10.0, 2500.0), 1),
            "cause": _r.choice(_CURTAILMENT_CAUSES),
            "customers_affected": _r.randint(5, 300),
        })
    events.sort(key=lambda x: x["timestamp"], reverse=True)
    return JSONResponse({"events": events, "count": len(events)})


@router.get("/api/der-export/doe-settings")
async def der_export_doe_settings() -> JSONResponse:
    """Dynamic operating envelope settings by feeder."""
    _r.seed(623)
    today_dt = date.today()
    settings = []
    for d in _DNSPS:
        for i in range(8):
            doe_type = _r.choice(_DOE_TYPES)
            default_limit = round(_r.uniform(2.0, 10.0), 2)
            settings.append({
                "feeder_id": f"FDR-{d[:3].upper()}-{i+1:03d}",
                "doe_type": doe_type,
                "default_limit_kw": default_limit,
                "peak_limit_kw": round(default_limit * _r.uniform(0.5, 0.85), 2) if doe_type != "Static" else default_limit,
                "off_peak_limit_kw": round(default_limit * _r.uniform(1.0, 1.5), 2) if doe_type != "Static" else default_limit,
                "last_updated": (today_dt - timedelta(days=_r.randint(1, 180))).isoformat(),
            })
    return JSONResponse({"settings": settings, "count": len(settings)})
