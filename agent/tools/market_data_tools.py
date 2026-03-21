"""
agent/tools/market_data_tools.py
=================================
Python stubs for the 6 market-data Unity Catalog function tools used by the
AUS Energy Copilot Mosaic AI agent.

Each function is decorated with @tool so it can be registered directly with
a LangChain agent executor OR wrapped as a Databricks Unity Catalog function.

All functions connect to the Gold layer via the Databricks SQL connector.
Connection details are resolved from environment variables.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from databricks import sql as dbsql
from langchain_core.tools import tool

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _connection():
    """Return a fresh databricks-sql-connector connection."""
    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def _query(sql: str) -> list[dict]:
    """Execute *sql* and return rows as a list of dicts."""
    conn = _connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


_CATALOG = os.getenv("DATABRICKS_CATALOG", "energy_copilot")


# ---------------------------------------------------------------------------
# Tool 1: get_latest_prices
# ---------------------------------------------------------------------------

@tool
def get_latest_prices(region: Optional[str] = None) -> dict:
    """
    Retrieve the most recent 5-minute dispatch price (RRP in AUD/MWh) for each
    NEM region from the gold layer.

    Args:
        region: Optional NEM region code to filter results.
                Valid values: NSW1, QLD1, VIC1, SA1, TAS1.
                If omitted, returns the latest price for all 5 regions.

    Returns:
        A dict with keys:
          - "data": list of records, each containing:
              - "region"           (str)   NEM region identifier
              - "settlement_date"  (str)   ISO-8601 dispatch interval end time (AEST)
              - "rrp"              (float) Regional Reference Price in AUD/MWh
              - "total_demand"     (float) Regional operational demand in MW
          - "retrieved_at"  (str) UTC timestamp of query execution

    Raises:
        RuntimeError: If the Databricks SQL query fails.

    Example:
        >>> get_latest_prices("NSW1")
        {"data": [{"region": "NSW1", "settlement_date": "2026-02-19T10:30:00",
                   "rrp": 87.42, "total_demand": 9823.1}],
         "retrieved_at": "2026-02-19T00:30:01Z"}
    """
    where = f"WHERE regionid = '{region}'" if region else ""
    rows = _query(
        f"""
        SELECT regionid AS region,
               settlementdate AS settlement_date,
               rrp,
               totaldemand AS total_demand
        FROM {_CATALOG}.gold.nem_prices_5min
        {where}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY regionid ORDER BY settlementdate DESC) = 1
        ORDER BY regionid
        """
    )
    from datetime import datetime, timezone
    return {"data": rows, "retrieved_at": datetime.now(timezone.utc).isoformat()}


# ---------------------------------------------------------------------------
# Tool 2: get_price_history
# ---------------------------------------------------------------------------

@tool
def get_price_history(
    region: str,
    start: str,
    end: str,
    interval: str = "5min",
) -> list:
    """
    Retrieve historical 5-minute (or aggregated) NEM dispatch prices for a
    specific region over a date/time range.

    Args:
        region:   NEM region code (NSW1, QLD1, VIC1, SA1, TAS1).
        start:    ISO-8601 start datetime, e.g. "2026-02-01T00:00:00".
                  Interpreted as AEST if no timezone suffix is provided.
        end:      ISO-8601 end datetime, e.g. "2026-02-01T23:59:59".
        interval: Aggregation interval. One of:
                    "5min"  — raw 5-minute dispatch intervals (default)
                    "30min" — 30-minute trading intervals (averaged)
                    "1h"    — hourly averages
                    "1d"    — daily averages

    Returns:
        A list of records ordered by settlement_date, each containing:
          - "settlement_date"  (str)   ISO-8601 interval timestamp
          - "rrp"              (float) Average RRP for the interval (AUD/MWh)
          - "total_demand"     (float) Average operational demand (MW)
          - "interval_count"   (int)   Number of 5-minute intervals aggregated

    Raises:
        ValueError: If region is not a valid NEM region code.
        RuntimeError: If the query returns no data for the requested range.

    Example:
        >>> get_price_history("VIC1", "2026-02-01T06:00:00", "2026-02-01T12:00:00")
        [{"settlement_date": "2026-02-01T06:05:00", "rrp": 102.3, ...}, ...]
    """
    valid_regions = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}
    if region not in valid_regions:
        raise ValueError(f"Invalid region '{region}'. Must be one of {valid_regions}.")

    # Build time-truncation expression based on requested interval
    trunc_map = {
        "5min": "date_trunc('minute', settlementdate)",
        "30min": "date_trunc('minute', settlementdate - INTERVAL 30 MINUTES * (minute(settlementdate) % 30))",
        "1h": "date_trunc('hour', settlementdate)",
        "1d": "date_trunc('day', settlementdate)",
    }
    trunc_expr = trunc_map.get(interval, "date_trunc('minute', settlementdate)")

    rows = _query(
        f"""
        SELECT
            {trunc_expr} AS settlement_date,
            AVG(rrp)         AS rrp,
            AVG(totaldemand) AS total_demand,
            COUNT(*)         AS interval_count
        FROM {_CATALOG}.gold.nem_prices_5min
        WHERE regionid = '{region}'
          AND settlementdate BETWEEN '{start}' AND '{end}'
        GROUP BY {trunc_expr}
        ORDER BY settlement_date
        """
    )
    return rows


# ---------------------------------------------------------------------------
# Tool 3: get_generation_mix
# ---------------------------------------------------------------------------

@tool
def get_generation_mix(region: str, start: str, end: str) -> dict:
    """
    Retrieve the generation mix by fuel type for a NEM region over a time range.

    Args:
        region: NEM region code (NSW1, QLD1, VIC1, SA1, TAS1).
        start:  ISO-8601 start datetime.
        end:    ISO-8601 end datetime.

    Returns:
        A dict containing:
          - "region"      (str)  The requested region.
          - "start"       (str)  Actual start of returned data.
          - "end"         (str)  Actual end of returned data.
          - "fuel_totals" (dict) Mapping of fuel_type -> total_gwh over the period.
          - "timeseries"  (list) Per-interval breakdown:
              [{"settlement_date": str, "fuel_type": str, "generation_mw": float}, ...]

    Fuel types include: BLACK_COAL, BROWN_COAL, NATURAL_GAS, LIQUID_FUEL,
    PUMPS, HYDRO, WIND, SOLAR_UTILITY, SOLAR_ROOFTOP, BATTERY_DISCHARGING,
    BATTERY_CHARGING.

    Example:
        >>> get_generation_mix("SA1", "2026-02-19T08:00:00", "2026-02-19T10:00:00")
        {"region": "SA1", "fuel_totals": {"WIND": 1234.5, "SOLAR_UTILITY": 678.2, ...}, ...}
    """
    rows = _query(
        f"""
        SELECT settlementdate AS settlement_date,
               fuel_type,
               SUM(generation_mw) AS generation_mw
        FROM {_CATALOG}.gold.nem_generation_by_fuel
        WHERE regionid = '{region}'
          AND settlementdate BETWEEN '{start}' AND '{end}'
        GROUP BY settlementdate, fuel_type
        ORDER BY settlementdate, fuel_type
        """
    )

    # Aggregate fuel totals for the period (convert MW * 5min to GWh)
    fuel_totals: dict[str, float] = {}
    for row in rows:
        ft = row["fuel_type"]
        fuel_totals[ft] = fuel_totals.get(ft, 0.0) + row["generation_mw"] * (5 / 60)

    return {
        "region": region,
        "start": start,
        "end": end,
        "fuel_totals_gwh": {k: round(v, 3) for k, v in fuel_totals.items()},
        "timeseries": rows,
    }


# ---------------------------------------------------------------------------
# Tool 4: get_interconnector_flows
# ---------------------------------------------------------------------------

@tool
def get_interconnector_flows(interconnector_id: Optional[str] = None) -> dict:
    """
    Retrieve current or most recent interconnector flow data for NEM
    interconnectors.

    NEM interconnectors and their typical directions:
      - QNI     : QLD1 <-> NSW1
      - VIC1-NSW1: VIC1 <-> NSW1
      - MELLINKBANK-NSW: VIC1 <-> NSW1 (additional path)
      - HEYWOOD : VIC1 <-> SA1
      - MURRAYLINK: VIC1 <-> SA1 (DC link)
      - BASSLINK : VIC1 <-> TAS1

    Args:
        interconnector_id: Optional interconnector identifier to filter to a
                           single link. If omitted, returns data for all
                           interconnectors.

    Returns:
        A dict with keys:
          - "data": list of records:
              - "interconnector_id"  (str)   Interconnector identifier
              - "settlement_date"    (str)   ISO-8601 timestamp of reading
              - "mw_flow"            (float) Flow in MW. Positive = export from
                                             FromRegion; negative = import.
              - "export_limit"       (float) Current MW export capability
              - "import_limit"       (float) Current MW import capability
              - "utilisation_pct"    (float) |flow| / applicable_limit * 100
          - "retrieved_at": (str) UTC timestamp of query execution

    Example:
        >>> get_interconnector_flows("BASSLINK")
        {"data": [{"interconnector_id": "BASSLINK", "mw_flow": -420.0, ...}], ...}
    """
    where = f"AND interconnectorid = '{interconnector_id}'" if interconnector_id else ""
    rows = _query(
        f"""
        SELECT interconnectorid AS interconnector_id,
               settlementdate AS settlement_date,
               mwflow AS mw_flow,
               exportlimit AS export_limit,
               importlimit AS import_limit
        FROM {_CATALOG}.gold.nem_interconnectors
        WHERE 1=1 {where}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY interconnectorid ORDER BY settlementdate DESC) = 1
        ORDER BY interconnectorid
        """
    )
    # Annotate utilisation
    for row in rows:
        flow = abs(row["mw_flow"])
        limit = row["export_limit"] if row["mw_flow"] >= 0 else abs(row["import_limit"])
        row["utilisation_pct"] = round(flow / limit * 100, 1) if limit else None

    from datetime import datetime, timezone
    return {"data": rows, "retrieved_at": datetime.now(timezone.utc).isoformat()}


# ---------------------------------------------------------------------------
# Tool 5: get_active_constraints
# ---------------------------------------------------------------------------

@tool
def get_active_constraints(region: str) -> list:
    """
    Retrieve network constraints currently active or binding in a NEM region.

    Binding constraints indicate transmission limitations that restrict
    generator output or interconnector flows and can cause price divergence
    between regions.

    Args:
        region: NEM region code (NSW1, QLD1, VIC1, SA1, TAS1).

    Returns:
        A list of constraint records, each containing:
          - "constraintid"    (str)   AEMO constraint set identifier
          - "settlement_date" (str)   Dispatch interval when constraint was active
          - "rhs"             (float) Right-hand side (constraint limit in MW)
          - "marginal_value"  (float) Shadow price of the constraint (AUD/MWh)
          - "violation_mw"    (float) MW by which constraint was violated (0 if not)
          - "generic_name"    (str)   Human-readable constraint description

    An empty list indicates no active binding constraints for the region.

    Example:
        >>> get_active_constraints("SA1")
        [{"constraintid": "V>>SA_HEYWOOD_1", "marginal_value": 15.2, ...}]
    """
    rows = _query(
        f"""
        SELECT constraintid,
               settlementdate AS settlement_date,
               rhs,
               marginalvalue AS marginal_value,
               violationdegree AS violation_mw,
               generic_constraint_name AS generic_name
        FROM {_CATALOG}.gold.nem_constraints
        WHERE regionid = '{region}'
          AND settlementdate >= CURRENT_TIMESTAMP - INTERVAL 30 MINUTES
          AND marginalvalue != 0
        ORDER BY ABS(marginalvalue) DESC
        """
    )
    return rows


# ---------------------------------------------------------------------------
# Tool 6: get_fcas_prices
# ---------------------------------------------------------------------------

@tool
def get_fcas_prices(region: str, service: str, start: str, end: str) -> list:
    """
    Retrieve FCAS (Frequency Control Ancillary Services) prices for a specific
    service type and region over a time range.

    FCAS services (8 markets):
      Raise contingency: RAISE6SEC, RAISE60SEC, RAISE5MIN, RAISEREG
      Lower contingency: LOWER6SEC, LOWER60SEC, LOWER5MIN, LOWERREG

    Args:
        region:  NEM region code (NSW1, QLD1, VIC1, SA1, TAS1).
                 Note: FCAS prices are NEM-wide but can vary slightly by region
                 due to Causer Pays arrangements.
        service: FCAS service type. One of:
                 RAISE6SEC, RAISE60SEC, RAISE5MIN, RAISEREG,
                 LOWER6SEC, LOWER60SEC, LOWER5MIN, LOWERREG.
        start:   ISO-8601 start datetime.
        end:     ISO-8601 end datetime.

    Returns:
        A list of FCAS price records ordered by settlement_date, each containing:
          - "settlement_date"  (str)   Dispatch interval timestamp
          - "service"          (str)   FCAS service type
          - "rrp"              (float) FCAS Regional Reference Price (AUD/MW/hr)
          - "local_price"      (float) Region-specific price where applicable
          - "cleared_volume"   (float) Cleared FCAS volume in MW

    Example:
        >>> get_fcas_prices("NSW1", "RAISE6SEC", "2026-02-19T00:00:00",
        ...                 "2026-02-19T06:00:00")
        [{"settlement_date": "2026-02-19T00:05:00", "rrp": 12.40, ...}, ...]
    """
    valid_services = {
        "RAISE6SEC", "RAISE60SEC", "RAISE5MIN", "RAISEREG",
        "LOWER6SEC", "LOWER60SEC", "LOWER5MIN", "LOWERREG",
    }
    if service not in valid_services:
        raise ValueError(f"Invalid service '{service}'. Must be one of {valid_services}.")

    rows = _query(
        f"""
        SELECT settlementdate AS settlement_date,
               service,
               rrp,
               localprice AS local_price,
               clearedvolume AS cleared_volume
        FROM {_CATALOG}.gold.nem_fcas_prices
        WHERE regionid = '{region}'
          AND service = '{service}'
          AND settlementdate BETWEEN '{start}' AND '{end}'
        ORDER BY settlementdate
        """
    )
    return rows
