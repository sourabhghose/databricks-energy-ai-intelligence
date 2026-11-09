"""AER efficiency benchmarking endpoints (DNSP opex, partial productivity, reset readiness).

These dashboards need the AER Economic Benchmarking RIN dataset (opex, customer numbers, circuit length,
productivity indices per DNSP), which is not ingested in this build. Rather than serve illustrative numbers,
every endpoint returns HTTP 503 with an explanation, and the frontend shows a "not available" notice.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

router = APIRouter()

_UNAVAILABLE = ("AER economic benchmarking (RIN) data is not ingested in this build, so DNSP opex, "
                "productivity and reset-readiness benchmarks are not shown. Ingest the AER Economic "
                "Benchmarking RIN workbooks to enable this dashboard.")


def _unavailable() -> None:
    raise HTTPException(status_code=503, detail=_UNAVAILABLE)


@router.get("/api/benchmarking/summary")
async def benchmarking_summary() -> None:
    _unavailable()


@router.get("/api/benchmarking/peers")
async def benchmarking_peers() -> None:
    _unavailable()


@router.get("/api/benchmarking/partial-productivity")
async def benchmarking_partial_productivity() -> None:
    _unavailable()


@router.get("/api/benchmarking/reset-readiness")
async def benchmarking_reset_readiness() -> None:
    _unavailable()
