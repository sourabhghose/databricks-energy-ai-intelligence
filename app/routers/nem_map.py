"""NEM Infrastructure Map — facility locations, layers, and detail endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from routers.shared import _query_gold, _CATALOG, logger

router = APIRouter()

# ---------------------------------------------------------------------------
# Fuel type colour palette (returned to frontend for consistency)
# ---------------------------------------------------------------------------
FUEL_COLORS = {
    "wind": "#22c55e",
    "solar_utility": "#eab308",
    "solar_rooftop": "#fbbf24",
    "coal_black": "#1e293b",
    "coal_brown": "#78350f",
    "gas_ccgt": "#3b82f6",
    "gas_ocgt": "#60a5fa",
    "gas_steam": "#93c5fd",
    "gas_recip": "#93c5fd",
    "hydro": "#06b6d4",
    "pumped_hydro": "#0891b2",
    "battery": "#a855f7",
    "bioenergy": "#84cc16",
    "distillate": "#f97316",
    "region_centroid": "#6b7280",
    "gas_hub": "#ef4444",
    "rez": "#f97316",
    "isp_transmission": "#ec4899",
    "unknown": "#9ca3af",
}


# ---------------------------------------------------------------------------
# GET /api/map/facilities
# ---------------------------------------------------------------------------
@router.get("/api/map/facilities")
async def get_map_facilities(
    fuel_type: Optional[str] = Query(None, description="Filter by fuel_type"),
    region: Optional[str] = Query(None, description="Filter by region_id"),
    layer_type: Optional[str] = Query(None, description="Filter by layer_type (generator, rez, isp, gas_hub, region)"),
    min_capacity_mw: float = Query(0, description="Minimum capacity MW"),
):
    """Return all facility locations with optional filters."""
    clauses = ["1=1"]
    if fuel_type:
        clauses.append(f"fuel_type = '{fuel_type}'")
    if region:
        clauses.append(f"region_id = '{region}'")
    if layer_type:
        clauses.append(f"layer_type = '{layer_type}'")
    if min_capacity_mw > 0:
        clauses.append(f"capacity_mw >= {min_capacity_mw}")

    where = " AND ".join(clauses)
    sql = f"SELECT * FROM {_CATALOG}.gold.facility_locations WHERE {where} ORDER BY capacity_mw DESC LIMIT 5000"

    rows = _query_gold(sql)
    if rows is None:
        rows = _mock_facilities(fuel_type, region, layer_type, min_capacity_mw)

    return {"facilities": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# GET /api/map/layers
# ---------------------------------------------------------------------------
@router.get("/api/map/layers")
async def get_map_layers():
    """Return available map layers with counts and colours."""
    sql = f"""
    SELECT layer_type, fuel_type, COUNT(*) AS cnt, ROUND(SUM(capacity_mw), 0) AS total_mw
    FROM {_CATALOG}.gold.facility_locations
    GROUP BY layer_type, fuel_type
    ORDER BY cnt DESC
    """
    rows = _query_gold(sql)
    if rows is None:
        rows = _mock_layers()
        return {"layers": rows}

    layers = []
    for r in rows:
        ft = r.get("fuel_type", "unknown")
        layers.append({
            "layer_type": r.get("layer_type", "generator"),
            "fuel_type": ft,
            "count": int(r.get("cnt", 0)),
            "total_mw": float(r.get("total_mw", 0)),
            "color": FUEL_COLORS.get(ft, "#9ca3af"),
        })
    return {"layers": layers}


# ---------------------------------------------------------------------------
# GET /api/map/facility/{duid}
# ---------------------------------------------------------------------------
@router.get("/api/map/facility/{duid}")
async def get_map_facility_detail(duid: str):
    """Return detail for a single facility including recent generation if available."""
    sql = f"SELECT * FROM {_CATALOG}.gold.facility_locations WHERE duid = '{duid}' LIMIT 1"
    rows = _query_gold(sql)
    if not rows:
        return {"error": "Facility not found", "duid": duid}

    facility = rows[0]

    # Try to get recent generation data
    gen_sql = f"""
    SELECT interval_datetime, generation_mw
    FROM {_CATALOG}.gold.nem_generation_by_fuel
    WHERE duid = '{duid}'
    ORDER BY interval_datetime DESC
    LIMIT 288
    """
    gen_rows = _query_gold(gen_sql)
    generation = []
    if gen_rows:
        generation = [
            {"timestamp": str(r.get("interval_datetime", "")), "mw": float(r.get("generation_mw", 0))}
            for r in gen_rows
        ]

    return {
        "facility": facility,
        "generation": generation,
    }


# ---------------------------------------------------------------------------
# Mock fallbacks
# ---------------------------------------------------------------------------
import random as _r

def _mock_facilities(fuel_type=None, region=None, layer_type=None, min_cap=0):
    """Generate mock facility locations across Australia for dev/demo."""
    _r.seed(42)
    FUELS = ["wind", "solar_utility", "coal_black", "gas_ccgt", "hydro", "battery"]
    REGIONS = {"NSW1": (-32.5, 149.0), "QLD1": (-24.0, 150.0), "VIC1": (-37.0, 145.0), "SA1": (-34.0, 138.5), "TAS1": (-42.0, 146.5)}
    STATE_MAP = {"NSW1": "NSW", "QLD1": "QLD", "VIC1": "VIC", "SA1": "SA", "TAS1": "TAS"}

    facs = []
    for i in range(200):
        reg = _r.choice(list(REGIONS.keys()))
        ft = _r.choice(FUELS)
        base_lat, base_lng = REGIONS[reg]
        cap = round(_r.uniform(5, 500), 1)

        if fuel_type and ft != fuel_type:
            continue
        if region and reg != region:
            continue
        if layer_type and layer_type != "generator":
            continue
        if cap < min_cap:
            continue

        facs.append({
            "duid": f"MOCK_{i:04d}",
            "station_name": f"Mock Station {i}",
            "lat": round(base_lat + _r.uniform(-2, 2), 4),
            "lng": round(base_lng + _r.uniform(-3, 3), 4),
            "state": STATE_MAP[reg],
            "region_id": reg,
            "fuel_type": ft,
            "capacity_mw": cap,
            "status": "operating",
            "layer_type": "generator",
        })
    return facs


def _mock_layers():
    """Mock layer summary."""
    return [
        {"layer_type": "generator", "fuel_type": "wind", "count": 85, "total_mw": 12500, "color": "#22c55e"},
        {"layer_type": "generator", "fuel_type": "solar_utility", "count": 72, "total_mw": 9800, "color": "#eab308"},
        {"layer_type": "generator", "fuel_type": "coal_black", "count": 18, "total_mw": 22000, "color": "#1e293b"},
        {"layer_type": "generator", "fuel_type": "gas_ccgt", "count": 25, "total_mw": 8500, "color": "#3b82f6"},
        {"layer_type": "generator", "fuel_type": "hydro", "count": 30, "total_mw": 7800, "color": "#06b6d4"},
        {"layer_type": "generator", "fuel_type": "battery", "count": 15, "total_mw": 2100, "color": "#a855f7"},
        {"layer_type": "rez", "fuel_type": "rez", "count": 14, "total_mw": 35200, "color": "#f97316"},
        {"layer_type": "isp", "fuel_type": "isp_transmission", "count": 10, "total_mw": 13700, "color": "#ec4899"},
        {"layer_type": "gas_hub", "fuel_type": "gas_hub", "count": 5, "total_mw": 0, "color": "#ef4444"},
        {"layer_type": "region", "fuel_type": "region_centroid", "count": 5, "total_mw": 0, "color": "#6b7280"},
    ]
