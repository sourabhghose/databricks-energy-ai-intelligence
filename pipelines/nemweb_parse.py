"""Shared parser for AEMO NEMWEB DispatchIS files (live CURRENT feed and daily ARCHIVE bundles).

A DispatchIS ZIP holds one CSV with several record types. Each type starts with an ``I`` header row
and continues with ``D`` data rows. We keep ``DISPATCH,PRICE`` and enrich it with ``DISPATCH,REGIONSUM``
(demand, available generation, net interchange), matched on (SETTLEMENTDATE, RUNNO, REGIONID, INTERVENTION).
"""
from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass


@dataclass(frozen=True)
class PriceRecord:
    source_file: str
    settlement_datetime: str
    run_no: str
    region_id: str
    rrp: str
    intervention: str
    last_changed: str
    total_demand: str | None
    available_generation: str | None
    net_interchange: str | None


def _key(rec: dict[str, str]) -> tuple[str | None, ...]:
    return rec.get("SETTLEMENTDATE"), rec.get("RUNNO"), rec.get("REGIONID"), rec.get("INTERVENTION")


def parse_dispatchis_csv(name: str, text: str) -> list[PriceRecord]:
    headers: dict[str, list[str]] = {}
    prices: list[dict[str, str]] = []
    regionsum: dict[tuple[str | None, ...], dict[str, str]] = {}
    for row in csv.reader(io.StringIO(text)):
        if len(row) < 4 or row[1] != "DISPATCH" or row[2] not in ("PRICE", "REGIONSUM"):
            continue
        if row[0] == "I":
            headers[row[2]] = row
        elif row[0] == "D" and row[2] in headers:
            rec = dict(zip(headers[row[2]], row))
            if row[2] == "PRICE":
                prices.append(rec)
            else:
                regionsum[_key(rec)] = rec
    out = []
    for p in prices:
        rs = regionsum.get(_key(p), {})
        out.append(PriceRecord(
            source_file=name,
            settlement_datetime=p.get("SETTLEMENTDATE", ""),
            run_no=p.get("RUNNO", ""),
            region_id=p.get("REGIONID", ""),
            rrp=p.get("RRP", ""),
            intervention=p.get("INTERVENTION", ""),
            last_changed=p.get("LASTCHANGED", ""),
            total_demand=rs.get("TOTALDEMAND"),
            available_generation=rs.get("AVAILABLEGENERATION"),
            net_interchange=rs.get("NETINTERCHANGE"),
        ))
    return out


def parse_dispatchis_zip(name: str, payload: bytes) -> list[PriceRecord]:
    """Parse one 5-minute DispatchIS ZIP (as published in /REPORTS/CURRENT/DispatchIS_Reports/)."""
    records: list[PriceRecord] = []
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        for member in zf.namelist():
            records.extend(parse_dispatchis_csv(name, zf.read(member).decode("utf-8", errors="replace")))
    return records


def iter_archive_day(payload: bytes):
    """Yield (inner_zip_name, records) for each 5-minute file inside a daily ARCHIVE bundle."""
    with zipfile.ZipFile(io.BytesIO(payload)) as day:
        for inner in sorted(day.namelist()):
            if inner.lower().endswith(".zip"):
                yield inner, parse_dispatchis_zip(inner, day.read(inner))


def to_bronze_row(r: PriceRecord, ingested_at) -> tuple:
    """Row tuple in the column order of bronze.nemweb_dispatch_price (see BRONZE_SCHEMA in the notebooks)."""
    return (r.source_file, ingested_at, "DISPATCH,PRICE", r.settlement_datetime[:10], r.run_no, r.region_id,
            r.rrp, r.intervention, r.last_changed, r.settlement_datetime, r.total_demand,
            r.available_generation, r.net_interchange)
