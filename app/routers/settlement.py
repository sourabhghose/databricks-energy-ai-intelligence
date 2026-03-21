"""Settlement Back Office — AEMO settlement ingestion, true-up, disputes, finance."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Query
from pydantic import BaseModel, Field

from .shared import (
    _CATALOG,
    _execute_gold,
    _insert_gold,
    _query_gold,
    _sql_escape,
    logger,
)

router = APIRouter()

# =========================================================================
# SETTLEMENT BACK OFFICE ENDPOINTS
# =========================================================================

# --- Pydantic Models ---

class SettlementRunCreate(BaseModel):
    run_type: str = Field(..., description="PRELIM, FINAL, R1, R2, R3")
    billing_period: str = Field(..., description="YYYY-MM")
    region: str = Field(...)
    run_date: str = Field(...)
    aemo_total_aud: float = 0
    internal_total_aud: float = 0
    prior_run_id: Optional[str] = None
    materiality_threshold_aud: float = 5000.0
    notes: str = ""
    created_by: str = "app_user"

class ChargeCreate(BaseModel):
    run_id: str
    charge_type: str
    charge_code: str = ""
    region: str = ""
    interval_date: str = ""
    aemo_amount_aud: float = 0
    internal_amount_aud: float = 0
    trade_id: Optional[str] = None
    description: str = ""

class DisputeCreateV2(BaseModel):
    region: str
    settlement_date: str
    dispute_type: str
    aemo_amount_aud: float
    internal_amount_aud: float
    description: str = ""
    raised_by: str = "app_user"
    priority: str = "MEDIUM"
    billing_run_no: Optional[str] = None
    charge_id: Optional[str] = None
    assigned_to: Optional[str] = None
    due_date: Optional[str] = None
    aemo_case_ref: Optional[str] = None

class JournalGenerateRequest(BaseModel):
    run_id: str
    entity: str = "AUS_ENERGY"
    journal_type: str = "SETTLEMENT"

class GlMappingCreate(BaseModel):
    entity: str = "AUS_ENERGY"
    charge_type: str
    debit_account_code: str
    debit_account_name: str
    credit_account_code: str
    credit_account_name: str


# --- Dispute State Machine ---
_VALID_TRANSITIONS = {
    "DRAFT": ["SUBMITTED"],
    "SUBMITTED": ["UNDER_REVIEW"],
    "UNDER_REVIEW": ["ACCEPTED", "REJECTED"],
    "ACCEPTED": ["CLOSED"],
    "REJECTED": ["DRAFT", "CLOSED"],
    "CLOSED": [],
}


# =========================================================================
# Settlement Reconciliation (moved from sidebar.py)
# =========================================================================

def _build_settlement_reconciliation(days_back: int = 7) -> Dict:
    """Build settlement reconciliation — AEMO settlement vs internal trade positions."""
    now = datetime.now(timezone.utc)

    energy_rows = _query_gold(f"""
        SELECT region_id,
               SUM(rrp * total_demand_mw * 5 / 60) AS energy_settlement_aud,
               SUM(total_demand_mw * 5 / 60) AS total_energy_mwh,
               COUNT(*) AS intervals
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE interval_datetime >= current_timestamp() - INTERVAL {days_back} DAYS
        GROUP BY region_id
    """)

    fcas_rows = _query_gold(f"""
        SELECT region_id,
               SUM(total_recovery_aud) AS fcas_aud
        FROM {_CATALOG}.gold.nem_settlement_summary
        GROUP BY region_id
    """)

    sra_rows = _query_gold(f"""
        SELECT interconnector_id,
               SUM(total_irsr_aud) AS irsr_aud,
               AVG(avg_mw_flow) AS avg_flow
        FROM {_CATALOG}.gold.nem_sra_residues
        GROUP BY interconnector_id
    """)

    trade_rows = _query_gold(f"""
        SELECT t.region, t.buy_sell,
               SUM(l.volume_mw * l.hours * l.price_aud_mwh) AS notional_aud
        FROM {_CATALOG}.gold.trades t
        JOIN {_CATALOG}.gold.trade_legs l ON t.trade_id = l.trade_id
        WHERE t.status IN ('CONFIRMED', 'SETTLED')
          AND l.start_date >= current_date() - INTERVAL {days_back} DAYS
        GROUP BY t.region, t.buy_sell
    """)

    total_aemo = 0
    total_internal = 0
    variances_by_region = []

    if energy_rows:
        fcas_by_region = {}
        if fcas_rows:
            for f in fcas_rows:
                fcas_by_region[f["region_id"]] = abs(float(f.get("fcas_aud") or 0))

        trade_by_region = {}
        if trade_rows:
            for t in trade_rows:
                r = t["region"]
                val = float(t.get("notional_aud") or 0)
                if t["buy_sell"] == "BUY":
                    trade_by_region[r] = trade_by_region.get(r, 0) - val
                else:
                    trade_by_region[r] = trade_by_region.get(r, 0) + val

        for row in energy_rows:
            r = row["region_id"]
            aemo_energy = float(row.get("energy_settlement_aud") or 0)
            aemo_fcas = fcas_by_region.get(r, 0)
            aemo_total = aemo_energy + aemo_fcas
            internal = abs(trade_by_region.get(r, 0))

            total_aemo += aemo_total
            total_internal += internal
            variance = aemo_total - internal
            variance_pct = (variance / max(abs(aemo_total), 1)) * 100

            variances_by_region.append({
                "region": r,
                "aemo_aud": round(aemo_total, 2),
                "internal_aud": round(internal, 2),
                "variance_aud": round(variance, 2),
                "variance_pct": round(variance_pct, 2),
                "status": "BREACH" if abs(variance) > 1000 else "OK",
            })

    variance = total_aemo - total_internal
    threshold_breaches = sum(1 for v in variances_by_region if v["status"] == "BREACH")

    residues = []
    if sra_rows:
        for s in sra_rows:
            flow = float(s.get("avg_flow") or 0)
            irsr = float(s.get("irsr_aud") or 0)
            residues.append({
                "interconnector_id": s["interconnector_id"],
                "flow_mw": round(flow, 1),
                "price_differential": round(irsr / max(abs(flow) * 24 * days_back, 1), 2),
                "settlement_residue_aud": round(irsr, 2),
                "direction": "EXPORT" if flow >= 0 else "IMPORT",
            })

    return {
        "days_back": days_back,
        "total_aemo_settlement": round(total_aemo, 2),
        "total_internal_settlement": round(total_internal, 2),
        "variance": round(variance, 2),
        "variance_pct": round((variance / max(abs(total_aemo), 1)) * 100, 2),
        "variances_by_region": variances_by_region,
        "threshold_breaches": threshold_breaches,
        "residues": residues,
        "timestamp": now.isoformat(),
    }


# =========================================================================
# Settlement Runs (6 endpoints)
# =========================================================================

@router.get("/api/settlement/runs", summary="List settlement runs", tags=["Settlement"])
async def list_settlement_runs(
    run_type: str = Query(None),
    status: str = Query(None),
    region: str = Query(None),
    billing_period: str = Query(None),
    limit: int = Query(50),
):
    where = []
    if run_type:
        where.append(f"run_type = '{_sql_escape(run_type)}'")
    if status:
        where.append(f"status = '{_sql_escape(status)}'")
    if region:
        where.append(f"region = '{_sql_escape(region)}'")
    if billing_period:
        where.append(f"billing_period = '{_sql_escape(billing_period)}'")
    wc = ("WHERE " + " AND ".join(where)) if where else ""
    rows = _query_gold(
        f"SELECT * FROM {_CATALOG}.gold.settlement_runs {wc} ORDER BY run_date DESC LIMIT {min(limit, 200)}"
    )
    if rows:
        for r in rows:
            for k in ("run_date", "created_at", "updated_at"):
                if k in r and r[k] is not None:
                    r[k] = str(r[k])
    return {"runs": rows or [], "count": len(rows or [])}


@router.get("/api/settlement/runs/comparison", summary="Compare two settlement runs", tags=["Settlement"])
async def compare_settlement_runs(
    run_id_a: str = Query(...),
    run_id_b: str = Query(...),
):
    """Compare charges between two runs (e.g. PRELIM vs FINAL)."""
    rows_a = _query_gold(
        f"SELECT charge_type, region, SUM(aemo_amount_aud) AS aemo, SUM(internal_amount_aud) AS internal "
        f"FROM {_CATALOG}.gold.settlement_charges WHERE run_id = '{_sql_escape(run_id_a)}' "
        f"GROUP BY charge_type, region ORDER BY charge_type, region"
    )
    rows_b = _query_gold(
        f"SELECT charge_type, region, SUM(aemo_amount_aud) AS aemo, SUM(internal_amount_aud) AS internal "
        f"FROM {_CATALOG}.gold.settlement_charges WHERE run_id = '{_sql_escape(run_id_b)}' "
        f"GROUP BY charge_type, region ORDER BY charge_type, region"
    )

    def _to_map(rows):
        m = {}
        if rows:
            for r in rows:
                key = f"{r['charge_type']}|{r['region']}"
                m[key] = {"aemo": float(r.get("aemo") or 0), "internal": float(r.get("internal") or 0)}
        return m

    map_a, map_b = _to_map(rows_a), _to_map(rows_b)
    all_keys = sorted(set(list(map_a.keys()) + list(map_b.keys())))
    comparisons = []
    for key in all_keys:
        ct, reg = key.split("|")
        a = map_a.get(key, {"aemo": 0, "internal": 0})
        b = map_b.get(key, {"aemo": 0, "internal": 0})
        comparisons.append({
            "charge_type": ct,
            "region": reg,
            "run_a_aemo": round(a["aemo"], 2),
            "run_b_aemo": round(b["aemo"], 2),
            "delta_aemo": round(b["aemo"] - a["aemo"], 2),
            "run_a_internal": round(a["internal"], 2),
            "run_b_internal": round(b["internal"], 2),
            "delta_internal": round(b["internal"] - a["internal"], 2),
        })
    return {"run_id_a": run_id_a, "run_id_b": run_id_b, "comparisons": comparisons}


@router.get("/api/settlement/runs/{run_id}", summary="Settlement run detail", tags=["Settlement"])
async def get_settlement_run(run_id: str):
    rows = _query_gold(
        f"SELECT * FROM {_CATALOG}.gold.settlement_runs WHERE run_id = '{_sql_escape(run_id)}' LIMIT 1"
    )
    if not rows:
        return {"error": "Run not found"}
    run = rows[0]
    for k in ("run_date", "created_at", "updated_at"):
        if k in run and run[k] is not None:
            run[k] = str(run[k])
    charges = _query_gold(
        f"SELECT charge_type, COUNT(*) AS cnt, SUM(aemo_amount_aud) AS total_aemo, "
        f"SUM(variance_aud) AS total_var "
        f"FROM {_CATALOG}.gold.settlement_charges WHERE run_id = '{_sql_escape(run_id)}' "
        f"GROUP BY charge_type"
    )
    run["charge_summary"] = charges or []
    return run


@router.post("/api/settlement/runs", summary="Create settlement run", tags=["Settlement"])
async def create_settlement_run(body: SettlementRunCreate):
    run_id = str(uuid.uuid4())
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    variance = round(body.aemo_total_aud - body.internal_total_aud, 2)
    variance_pct = round((variance / max(abs(body.aemo_total_aud), 1)) * 100, 2)
    auto_accept = abs(variance) < body.materiality_threshold_aud
    status = "ACCEPTED" if auto_accept else "PENDING"

    _insert_gold(f"{_CATALOG}.gold.settlement_runs", {
        "run_id": run_id,
        "run_type": body.run_type,
        "billing_period": body.billing_period,
        "region": body.region,
        "run_date": body.run_date,
        "status": status,
        "aemo_total_aud": body.aemo_total_aud,
        "internal_total_aud": body.internal_total_aud,
        "variance_aud": variance,
        "variance_pct": variance_pct,
        "prior_run_id": body.prior_run_id or "",
        "auto_accept": auto_accept,
        "materiality_threshold_aud": body.materiality_threshold_aud,
        "notes": body.notes,
        "created_by": body.created_by,
        "created_at": now_str,
        "updated_at": now_str,
    })
    return {"status": "created", "run_id": run_id, "auto_accept": auto_accept, "variance_aud": variance}


@router.put("/api/settlement/runs/{run_id}/status", summary="Update run status", tags=["Settlement"])
async def update_run_status(
    run_id: str,
    status: str = Query(...),
    notes: str = Query(None),
):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    set_parts = [f"status = '{_sql_escape(status)}'", f"updated_at = '{now_str}'"]
    if notes:
        set_parts.append(f"notes = '{_sql_escape(notes)}'")
    _execute_gold(
        f"UPDATE {_CATALOG}.gold.settlement_runs SET {', '.join(set_parts)} "
        f"WHERE run_id = '{_sql_escape(run_id)}'"
    )
    return {"status": "updated", "run_id": run_id}


@router.get("/api/settlement/runs/{run_id}/charges", summary="Charges for a run", tags=["Settlement"])
async def get_run_charges(run_id: str, charge_type: str = Query(None)):
    where = f"WHERE run_id = '{_sql_escape(run_id)}'"
    if charge_type:
        where += f" AND charge_type = '{_sql_escape(charge_type)}'"
    rows = _query_gold(
        f"SELECT * FROM {_CATALOG}.gold.settlement_charges {where} ORDER BY interval_date, charge_type"
    )
    if rows:
        for r in rows:
            for k in ("interval_date", "created_at"):
                if k in r and r[k] is not None:
                    r[k] = str(r[k])
    return {"charges": rows or [], "count": len(rows or [])}


# =========================================================================
# Charges (3 endpoints)
# =========================================================================

@router.post("/api/settlement/charges", summary="Create charge", tags=["Settlement"])
async def create_charge(body: ChargeCreate):
    charge_id = str(uuid.uuid4())
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    variance = round(body.aemo_amount_aud - body.internal_amount_aud, 2)
    mapped = "MAPPED" if body.trade_id else "UNMAPPED"
    _insert_gold(f"{_CATALOG}.gold.settlement_charges", {
        "charge_id": charge_id,
        "run_id": body.run_id,
        "charge_type": body.charge_type,
        "charge_code": body.charge_code,
        "region": body.region,
        "interval_date": body.interval_date,
        "aemo_amount_aud": body.aemo_amount_aud,
        "internal_amount_aud": body.internal_amount_aud,
        "variance_aud": variance,
        "mapped_status": mapped,
        "trade_id": body.trade_id or "",
        "description": body.description,
        "created_at": now_str,
    })
    return {"status": "created", "charge_id": charge_id, "variance_aud": variance}


@router.post("/api/settlement/charges/batch", summary="Batch insert charges", tags=["Settlement"])
async def create_charges_batch(charges: List[ChargeCreate] = Body(...)):
    if len(charges) > 200:
        return {"error": "Maximum 200 charges per batch"}
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    results = []
    for ch in charges:
        cid = str(uuid.uuid4())
        variance = round(ch.aemo_amount_aud - ch.internal_amount_aud, 2)
        mapped = "MAPPED" if ch.trade_id else "UNMAPPED"
        _insert_gold(f"{_CATALOG}.gold.settlement_charges", {
            "charge_id": cid,
            "run_id": ch.run_id,
            "charge_type": ch.charge_type,
            "charge_code": ch.charge_code,
            "region": ch.region,
            "interval_date": ch.interval_date,
            "aemo_amount_aud": ch.aemo_amount_aud,
            "internal_amount_aud": ch.internal_amount_aud,
            "variance_aud": variance,
            "mapped_status": mapped,
            "trade_id": ch.trade_id or "",
            "description": ch.description,
            "created_at": now_str,
        })
        results.append({"charge_id": cid, "variance_aud": variance})
    return {"status": "created", "count": len(results), "charges": results}


@router.put("/api/settlement/charges/{charge_id}/map", summary="Map charge to trade", tags=["Settlement"])
async def map_charge(charge_id: str, trade_id: str = Query(...)):
    _execute_gold(
        f"UPDATE {_CATALOG}.gold.settlement_charges "
        f"SET trade_id = '{_sql_escape(trade_id)}', mapped_status = 'MAPPED' "
        f"WHERE charge_id = '{_sql_escape(charge_id)}'"
    )
    return {"status": "mapped", "charge_id": charge_id, "trade_id": trade_id}


# =========================================================================
# True-Up (3 endpoints)
# =========================================================================

@router.get("/api/settlement/trueup/summary", summary="True-up variance summary", tags=["Settlement"])
async def trueup_summary(billing_period: str = Query(None)):
    """Variance across run versions (PRELIM → FINAL → R1 → R2 → R3) for a billing period."""
    where = ""
    if billing_period:
        where = f"WHERE billing_period = '{_sql_escape(billing_period)}'"
    rows = _query_gold(
        f"SELECT billing_period, run_type, region, "
        f"SUM(aemo_total_aud) AS aemo_total, SUM(internal_total_aud) AS internal_total, "
        f"SUM(variance_aud) AS variance, AVG(variance_pct) AS avg_variance_pct "
        f"FROM {_CATALOG}.gold.settlement_runs {where} "
        f"GROUP BY billing_period, run_type, region "
        f"ORDER BY billing_period DESC, run_type, region"
    )
    return {"trueup": rows or [], "count": len(rows or [])}


@router.get("/api/settlement/trueup/material", summary="Material variance runs", tags=["Settlement"])
async def trueup_material(threshold_aud: float = Query(5000.0)):
    """Runs exceeding materiality threshold."""
    rows = _query_gold(
        f"SELECT * FROM {_CATALOG}.gold.settlement_runs "
        f"WHERE ABS(variance_aud) >= {threshold_aud} AND status NOT IN ('ACCEPTED') "
        f"ORDER BY ABS(variance_aud) DESC LIMIT 100"
    )
    if rows:
        for r in rows:
            for k in ("run_date", "created_at", "updated_at"):
                if k in r and r[k] is not None:
                    r[k] = str(r[k])
    return {"material_runs": rows or [], "count": len(rows or []), "threshold_aud": threshold_aud}


@router.post("/api/settlement/trueup/accept", summary="Auto-accept run", tags=["Settlement"])
async def trueup_accept(run_id: str = Query(...)):
    """Mark a run as auto-accepted after review."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _execute_gold(
        f"UPDATE {_CATALOG}.gold.settlement_runs "
        f"SET status = 'ACCEPTED', auto_accept = true, updated_at = '{now_str}' "
        f"WHERE run_id = '{_sql_escape(run_id)}'"
    )
    return {"status": "accepted", "run_id": run_id}


# =========================================================================
# Reconciliation (existing, moved from sidebar.py)
# =========================================================================

@router.get("/api/settlement/reconciliation", summary="Settlement reconciliation", tags=["Settlement"])
async def settlement_reconciliation(days_back: int = Query(7, ge=1, le=90)):
    """AEMO settlement vs internal trade positions with variance analysis."""
    return _build_settlement_reconciliation(days_back)


@router.get("/api/settlement/residues", summary="Settlement residues", tags=["Settlement"])
async def settlement_residues(interconnector: str = Query(None)):
    """Inter-regional settlement residues from SRA data."""
    recon = _build_settlement_reconciliation(30)
    residues = recon.get("residues", [])
    if interconnector:
        residues = [r for r in residues if interconnector.lower() in r["interconnector_id"].lower()]
    return residues


# =========================================================================
# Enhanced Disputes (5 endpoints)
# =========================================================================

@router.get("/api/settlement/disputes", summary="List settlement disputes", tags=["Settlement"])
async def list_disputes(
    region: str = Query(None),
    status: str = Query(None),
    workflow_state: str = Query(None),
    priority: str = Query(None),
    limit: int = Query(50),
):
    where_parts = []
    if region:
        where_parts.append(f"region = '{_sql_escape(region)}'")
    if status:
        where_parts.append(f"status = '{_sql_escape(status)}'")
    if workflow_state:
        where_parts.append(f"workflow_state = '{_sql_escape(workflow_state)}'")
    if priority:
        where_parts.append(f"priority = '{_sql_escape(priority)}'")
    where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    rows = _query_gold(
        f"SELECT * FROM {_CATALOG}.gold.settlement_disputes "
        f"{where_clause} ORDER BY raised_at DESC LIMIT {min(limit, 200)}"
    )
    if rows:
        for r in rows:
            for k in ("settlement_date", "raised_at", "resolved_at", "due_date", "updated_at"):
                if k in r and r[k] is not None:
                    r[k] = str(r[k])
    return {"disputes": rows or [], "count": len(rows or [])}


@router.get("/api/settlement/disputes/summary", summary="Dispute summary", tags=["Settlement"])
async def dispute_summary():
    """Counts by status + total variance."""
    rows = _query_gold(
        f"SELECT status, workflow_state, COUNT(*) AS cnt, SUM(ABS(variance_aud)) AS total_variance "
        f"FROM {_CATALOG}.gold.settlement_disputes "
        f"GROUP BY status, workflow_state"
    )
    by_status: Dict[str, Any] = {}
    by_workflow: Dict[str, int] = {}
    total_variance = 0.0
    total_count = 0
    if rows:
        for r in rows:
            s = r["status"]
            c = int(r["cnt"])
            v = float(r.get("total_variance") or 0)
            if s not in by_status:
                by_status[s] = {"count": 0, "total_variance_aud": 0}
            by_status[s]["count"] += c
            by_status[s]["total_variance_aud"] = round(by_status[s]["total_variance_aud"] + v, 2)
            ws = r.get("workflow_state") or "LEGACY"
            by_workflow[ws] = by_workflow.get(ws, 0) + c
            total_variance += v
            total_count += c
    return {
        "by_status": by_status,
        "by_workflow_state": by_workflow,
        "total_disputes": total_count,
        "total_variance_aud": round(total_variance, 2),
    }


@router.post("/api/settlement/disputes/v2", summary="Create dispute v2", tags=["Settlement"])
async def create_dispute_v2(body: DisputeCreateV2):
    """Create dispute with enhanced workflow fields."""
    dispute_id = str(uuid.uuid4())
    variance = round(body.aemo_amount_aud - body.internal_amount_aud, 2)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _insert_gold(f"{_CATALOG}.gold.settlement_disputes", {
        "dispute_id": dispute_id,
        "region": body.region,
        "settlement_date": body.settlement_date,
        "dispute_type": body.dispute_type,
        "aemo_amount_aud": body.aemo_amount_aud,
        "internal_amount_aud": body.internal_amount_aud,
        "variance_aud": variance,
        "status": "OPEN",
        "description": body.description,
        "resolution_notes": "",
        "raised_by": body.raised_by,
        "raised_at": now_str,
        "workflow_state": "DRAFT",
        "priority": body.priority,
        "billing_run_no": body.billing_run_no or "",
        "charge_id": body.charge_id or "",
        "assigned_to": body.assigned_to or "",
        "due_date": body.due_date or "",
        "aemo_case_ref": body.aemo_case_ref or "",
        "updated_at": now_str,
    })
    return {"status": "created", "dispute_id": dispute_id, "variance_aud": variance, "workflow_state": "DRAFT"}


@router.post("/api/settlement/disputes", summary="Create dispute (legacy)", tags=["Settlement"])
async def create_dispute(
    region: str = Query(...),
    settlement_date: str = Query(...),
    dispute_type: str = Query(...),
    aemo_amount_aud: float = Query(...),
    internal_amount_aud: float = Query(...),
    description: str = Query(""),
    raised_by: str = Query("app_user"),
):
    """Legacy dispute creation — backward compatible."""
    dispute_id = str(uuid.uuid4())
    variance = round(aemo_amount_aud - internal_amount_aud, 2)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _insert_gold(f"{_CATALOG}.gold.settlement_disputes", {
        "dispute_id": dispute_id,
        "region": region,
        "settlement_date": settlement_date,
        "dispute_type": dispute_type,
        "aemo_amount_aud": aemo_amount_aud,
        "internal_amount_aud": internal_amount_aud,
        "variance_aud": variance,
        "status": "OPEN",
        "description": description,
        "resolution_notes": "",
        "raised_by": raised_by,
        "raised_at": now_str,
        "workflow_state": "DRAFT",
        "updated_at": now_str,
    })
    return {"status": "created", "dispute_id": dispute_id, "variance_aud": variance}


@router.put("/api/settlement/disputes/{dispute_id}", summary="Update dispute", tags=["Settlement"])
async def update_dispute(
    dispute_id: str,
    status: str = Query(None),
    resolution_notes: str = Query(None),
):
    """Update a dispute status and/or resolution notes (legacy)."""
    set_parts = []
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if status:
        set_parts.append(f"status = '{_sql_escape(status)}'")
        if status == "RESOLVED":
            set_parts.append("resolved_at = current_timestamp()")
    if resolution_notes is not None:
        set_parts.append(f"resolution_notes = '{_sql_escape(resolution_notes)}'")
    if not set_parts:
        return {"status": "no_changes"}
    set_parts.append(f"updated_at = '{now_str}'")
    _execute_gold(
        f"UPDATE {_CATALOG}.gold.settlement_disputes "
        f"SET {', '.join(set_parts)} "
        f"WHERE dispute_id = '{_sql_escape(dispute_id)}'"
    )
    return {"status": "updated", "dispute_id": dispute_id}


@router.put("/api/settlement/disputes/{dispute_id}/transition", summary="Transition dispute state", tags=["Settlement"])
async def transition_dispute(
    dispute_id: str,
    target_state: str = Query(...),
):
    """Validated state-machine transition for dispute workflow."""
    rows = _query_gold(
        f"SELECT workflow_state FROM {_CATALOG}.gold.settlement_disputes "
        f"WHERE dispute_id = '{_sql_escape(dispute_id)}' LIMIT 1"
    )
    if not rows:
        return {"error": "Dispute not found"}
    current = rows[0].get("workflow_state") or "DRAFT"
    allowed = _VALID_TRANSITIONS.get(current, [])
    if target_state not in allowed:
        return {
            "error": f"Invalid transition: {current} → {target_state}",
            "allowed_transitions": allowed,
        }
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    set_parts = [f"workflow_state = '{_sql_escape(target_state)}'", f"updated_at = '{now_str}'"]
    if target_state == "CLOSED":
        set_parts.append("resolved_at = current_timestamp()")
        set_parts.append("status = 'RESOLVED'")
    elif target_state == "SUBMITTED":
        set_parts.append("status = 'OPEN'")
    elif target_state == "UNDER_REVIEW":
        set_parts.append("status = 'UNDER_REVIEW'")
    elif target_state == "ACCEPTED":
        set_parts.append("status = 'RESOLVED'")
    elif target_state == "REJECTED":
        set_parts.append("status = 'ESCALATED'")
    _execute_gold(
        f"UPDATE {_CATALOG}.gold.settlement_disputes SET {', '.join(set_parts)} "
        f"WHERE dispute_id = '{_sql_escape(dispute_id)}'"
    )
    return {"status": "transitioned", "dispute_id": dispute_id, "from": current, "to": target_state}


@router.post("/api/settlement/disputes/{dispute_id}/evidence", summary="Attach evidence", tags=["Settlement"])
async def attach_evidence(
    dispute_id: str,
    evidence_type: str = Query(..., description="SCREENSHOT, CSV, EMAIL, AEMO_RESPONSE, NOTE"),
    filename: str = Query(""),
    content_json: str = Body("{}"),
    uploaded_by: str = Query("app_user"),
):
    evidence_id = str(uuid.uuid4())
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _insert_gold(f"{_CATALOG}.gold.settlement_evidence", {
        "evidence_id": evidence_id,
        "dispute_id": dispute_id,
        "evidence_type": evidence_type,
        "filename": filename,
        "content_json": content_json,
        "uploaded_by": uploaded_by,
        "uploaded_at": now_str,
    })
    # Update dispute evidence_ids
    existing = _query_gold(
        f"SELECT evidence_ids FROM {_CATALOG}.gold.settlement_disputes "
        f"WHERE dispute_id = '{_sql_escape(dispute_id)}' LIMIT 1"
    )
    current_ids = ""
    if existing and existing[0].get("evidence_ids"):
        current_ids = existing[0]["evidence_ids"]
    new_ids = f"{current_ids},{evidence_id}" if current_ids else evidence_id
    _execute_gold(
        f"UPDATE {_CATALOG}.gold.settlement_disputes SET evidence_ids = '{new_ids}' "
        f"WHERE dispute_id = '{_sql_escape(dispute_id)}'"
    )
    return {"status": "attached", "evidence_id": evidence_id, "dispute_id": dispute_id}


@router.get("/api/settlement/disputes/{dispute_id}/evidence", summary="List evidence", tags=["Settlement"])
async def list_evidence(dispute_id: str):
    rows = _query_gold(
        f"SELECT * FROM {_CATALOG}.gold.settlement_evidence "
        f"WHERE dispute_id = '{_sql_escape(dispute_id)}' ORDER BY uploaded_at DESC"
    )
    if rows:
        for r in rows:
            if "uploaded_at" in r and r["uploaded_at"] is not None:
                r["uploaded_at"] = str(r["uploaded_at"])
    return {"evidence": rows or [], "count": len(rows or [])}


@router.get("/api/settlement/disputes/{dispute_id}/timeline", summary="Dispute timeline (CDF)", tags=["Settlement"])
async def dispute_timeline(dispute_id: str):
    """Audit trail from Delta Change Data Feed."""
    try:
        rows = _query_gold(
            f"SELECT _change_type, _commit_version, _commit_timestamp, "
            f"status, workflow_state, resolution_notes, assigned_to "
            f"FROM table_changes('{_CATALOG}.gold.settlement_disputes', 1) "
            f"WHERE dispute_id = '{_sql_escape(dispute_id)}' "
            f"ORDER BY _commit_version"
        )
        if rows:
            for r in rows:
                if "_commit_timestamp" in r and r["_commit_timestamp"] is not None:
                    r["_commit_timestamp"] = str(r["_commit_timestamp"])
        return {"timeline": rows or [], "dispute_id": dispute_id}
    except Exception as exc:
        logger.warning("CDF query failed for dispute %s: %s", dispute_id, exc)
        return {"timeline": [], "dispute_id": dispute_id, "note": "CDF not available"}


# =========================================================================
# Finance (5 endpoints)
# =========================================================================

@router.post("/api/settlement/journals/generate", summary="Generate GL journals", tags=["Settlement"])
async def generate_journals(body: JournalGenerateRequest):
    """Generate GL journal entries from a settlement run using GL mappings."""
    charges = _query_gold(
        f"SELECT charge_type, region, SUM(aemo_amount_aud) AS total_aemo "
        f"FROM {_CATALOG}.gold.settlement_charges WHERE run_id = '{_sql_escape(body.run_id)}' "
        f"GROUP BY charge_type, region"
    )
    if not charges:
        return {"error": "No charges found for run", "run_id": body.run_id}

    mappings = _query_gold(
        f"SELECT * FROM {_CATALOG}.gold.settlement_gl_mapping "
        f"WHERE entity = '{_sql_escape(body.entity)}' AND is_active = true"
    )
    mapping_by_type = {}
    if mappings:
        for m in mappings:
            mapping_by_type[m["charge_type"]] = m

    run_rows = _query_gold(
        f"SELECT billing_period FROM {_CATALOG}.gold.settlement_runs "
        f"WHERE run_id = '{_sql_escape(body.run_id)}' LIMIT 1"
    )
    period = run_rows[0]["billing_period"] if run_rows else "UNKNOWN"

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    journals_created = []
    for ch in charges:
        ct = ch["charge_type"]
        amount = float(ch.get("total_aemo") or 0)
        m = mapping_by_type.get(ct)
        if not m:
            continue
        jid = str(uuid.uuid4())
        # Debit entry
        _insert_gold(f"{_CATALOG}.gold.settlement_journals", {
            "journal_id": jid,
            "run_id": body.run_id,
            "journal_type": body.journal_type,
            "entity": body.entity,
            "period": period,
            "account_code": m["debit_account_code"],
            "account_name": m["debit_account_name"],
            "debit_aud": round(abs(amount), 2),
            "credit_aud": 0,
            "charge_type": ct,
            "region": ch["region"],
            "posted": False,
            "description": f"{ct} settlement — {ch['region']}",
            "created_at": now_str,
        })
        # Credit entry
        jid2 = str(uuid.uuid4())
        _insert_gold(f"{_CATALOG}.gold.settlement_journals", {
            "journal_id": jid2,
            "run_id": body.run_id,
            "journal_type": body.journal_type,
            "entity": body.entity,
            "period": period,
            "account_code": m["credit_account_code"],
            "account_name": m["credit_account_name"],
            "debit_aud": 0,
            "credit_aud": round(abs(amount), 2),
            "charge_type": ct,
            "region": ch["region"],
            "posted": False,
            "description": f"{ct} settlement — {ch['region']}",
            "created_at": now_str,
        })
        journals_created.append({"charge_type": ct, "region": ch["region"], "amount_aud": round(abs(amount), 2)})

    return {"status": "generated", "run_id": body.run_id, "journals_count": len(journals_created) * 2, "entries": journals_created}


@router.get("/api/settlement/journals", summary="List journals", tags=["Settlement"])
async def list_journals(
    period: str = Query(None),
    entity: str = Query(None),
    posted: bool = Query(None),
    run_id: str = Query(None),
    limit: int = Query(100),
):
    where = []
    if period:
        where.append(f"period = '{_sql_escape(period)}'")
    if entity:
        where.append(f"entity = '{_sql_escape(entity)}'")
    if posted is not None:
        where.append(f"posted = {str(posted).lower()}")
    if run_id:
        where.append(f"run_id = '{_sql_escape(run_id)}'")
    wc = ("WHERE " + " AND ".join(where)) if where else ""
    rows = _query_gold(
        f"SELECT * FROM {_CATALOG}.gold.settlement_journals {wc} "
        f"ORDER BY created_at DESC LIMIT {min(limit, 500)}"
    )
    if rows:
        for r in rows:
            for k in ("posted_at", "created_at"):
                if k in r and r[k] is not None:
                    r[k] = str(r[k])
    total_debit = sum(float(r.get("debit_aud") or 0) for r in (rows or []))
    total_credit = sum(float(r.get("credit_aud") or 0) for r in (rows or []))
    return {
        "journals": rows or [],
        "count": len(rows or []),
        "total_debit_aud": round(total_debit, 2),
        "total_credit_aud": round(total_credit, 2),
    }


@router.put("/api/settlement/journals/{journal_id}/post", summary="Post journal", tags=["Settlement"])
async def post_journal(journal_id: str, posted_by: str = Query("app_user")):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _execute_gold(
        f"UPDATE {_CATALOG}.gold.settlement_journals "
        f"SET posted = true, posted_at = '{now_str}', posted_by = '{_sql_escape(posted_by)}' "
        f"WHERE journal_id = '{_sql_escape(journal_id)}'"
    )
    return {"status": "posted", "journal_id": journal_id}


@router.get("/api/settlement/finance/statement", summary="Settlement statement", tags=["Settlement"])
async def finance_statement(
    start_date: str = Query(None),
    end_date: str = Query(None),
    region: str = Query(None),
):
    """Settlement statement for a date range — summarises runs, charges, journals."""
    where_run = []
    if start_date:
        where_run.append(f"run_date >= '{_sql_escape(start_date)}'")
    if end_date:
        where_run.append(f"run_date <= '{_sql_escape(end_date)}'")
    if region:
        where_run.append(f"region = '{_sql_escape(region)}'")
    wc = ("WHERE " + " AND ".join(where_run)) if where_run else ""

    runs = _query_gold(
        f"SELECT run_type, region, COUNT(*) AS run_count, "
        f"SUM(aemo_total_aud) AS total_aemo, SUM(internal_total_aud) AS total_internal, "
        f"SUM(variance_aud) AS total_variance "
        f"FROM {_CATALOG}.gold.settlement_runs {wc} GROUP BY run_type, region"
    )

    charges = _query_gold(
        f"SELECT charge_type, SUM(aemo_amount_aud) AS total_aemo, SUM(variance_aud) AS total_variance "
        f"FROM {_CATALOG}.gold.settlement_charges c "
        f"JOIN {_CATALOG}.gold.settlement_runs r ON c.run_id = r.run_id "
        f"{wc.replace('run_date', 'r.run_date').replace('region', 'r.region') if wc else ''} "
        f"GROUP BY charge_type"
    )

    journals = _query_gold(
        f"SELECT journal_type, SUM(debit_aud) AS total_debit, SUM(credit_aud) AS total_credit "
        f"FROM {_CATALOG}.gold.settlement_journals "
        f"{'WHERE posted = true' if not wc else ''} "
        f"GROUP BY journal_type"
    )

    return {
        "runs_summary": runs or [],
        "charges_by_type": charges or [],
        "journals_by_type": journals or [],
        "period": {"start": start_date, "end": end_date, "region": region},
    }


@router.get("/api/settlement/finance/accruals", summary="Month-end accruals", tags=["Settlement"])
async def finance_accruals(period: str = Query(None)):
    """Month-end accruals — unposted settlement amounts."""
    where = ""
    if period:
        where = f"AND period = '{_sql_escape(period)}'"
    rows = _query_gold(
        f"SELECT period, charge_type, region, "
        f"SUM(debit_aud) AS accrued_debit, SUM(credit_aud) AS accrued_credit "
        f"FROM {_CATALOG}.gold.settlement_journals "
        f"WHERE posted = false {where} "
        f"GROUP BY period, charge_type, region "
        f"ORDER BY period DESC, charge_type"
    )
    total_accrued = sum(float(r.get("accrued_debit") or 0) for r in (rows or []))
    return {"accruals": rows or [], "total_accrued_aud": round(total_accrued, 2)}


# =========================================================================
# GL Mappings (3 endpoints)
# =========================================================================

@router.get("/api/settlement/gl-mappings", summary="List GL mappings", tags=["Settlement"])
async def list_gl_mappings(entity: str = Query(None)):
    where = ""
    if entity:
        where = f"WHERE entity = '{_sql_escape(entity)}'"
    rows = _query_gold(
        f"SELECT * FROM {_CATALOG}.gold.settlement_gl_mapping {where} ORDER BY charge_type"
    )
    if rows:
        for r in rows:
            for k in ("created_at", "updated_at"):
                if k in r and r[k] is not None:
                    r[k] = str(r[k])
    return {"mappings": rows or [], "count": len(rows or [])}


@router.post("/api/settlement/gl-mappings", summary="Create/update GL mapping", tags=["Settlement"])
async def upsert_gl_mapping(body: GlMappingCreate):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    existing = _query_gold(
        f"SELECT mapping_id FROM {_CATALOG}.gold.settlement_gl_mapping "
        f"WHERE entity = '{_sql_escape(body.entity)}' AND charge_type = '{_sql_escape(body.charge_type)}' "
        f"AND is_active = true LIMIT 1"
    )
    if existing:
        mid = existing[0]["mapping_id"]
        _execute_gold(
            f"UPDATE {_CATALOG}.gold.settlement_gl_mapping SET "
            f"debit_account_code = '{_sql_escape(body.debit_account_code)}', "
            f"debit_account_name = '{_sql_escape(body.debit_account_name)}', "
            f"credit_account_code = '{_sql_escape(body.credit_account_code)}', "
            f"credit_account_name = '{_sql_escape(body.credit_account_name)}', "
            f"updated_at = '{now_str}' "
            f"WHERE mapping_id = '{mid}'"
        )
        return {"status": "updated", "mapping_id": mid}
    mid = str(uuid.uuid4())
    _insert_gold(f"{_CATALOG}.gold.settlement_gl_mapping", {
        "mapping_id": mid,
        "entity": body.entity,
        "charge_type": body.charge_type,
        "debit_account_code": body.debit_account_code,
        "debit_account_name": body.debit_account_name,
        "credit_account_code": body.credit_account_code,
        "credit_account_name": body.credit_account_name,
        "is_active": True,
        "created_at": now_str,
        "updated_at": now_str,
    })
    return {"status": "created", "mapping_id": mid}


@router.delete("/api/settlement/gl-mappings/{mapping_id}", summary="Deactivate GL mapping", tags=["Settlement"])
async def delete_gl_mapping(mapping_id: str):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _execute_gold(
        f"UPDATE {_CATALOG}.gold.settlement_gl_mapping "
        f"SET is_active = false, updated_at = '{now_str}' "
        f"WHERE mapping_id = '{_sql_escape(mapping_id)}'"
    )
    return {"status": "deactivated", "mapping_id": mapping_id}
