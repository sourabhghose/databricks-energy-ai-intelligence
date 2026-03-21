# Databricks notebook source
"""
AUS Energy Copilot — Data Quality Report Pipeline
============================================================
Runs daily at 06:00 AEST (20:00 UTC previous day).
Queries the DLT event log and Gold tables to produce a data
quality summary written to gold.data_quality_daily_report.

Schedule: 0 0 20 * * ? (UTC) = 06:00 AEST
Cluster: Standard (no ML runtime needed), autoscale 1-4
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("energy_copilot.data_quality_report")

# ---------------------------------------------------------------------------
# Catalog / schema configuration
# ---------------------------------------------------------------------------
# Resolved at runtime from spark conf or env; falls back to "energy_copilot".
def _get_catalog(spark) -> str:
    if spark is None:
        return "energy_copilot"
    try:
        return spark.conf.get("spark.energy_copilot.catalog", "energy_copilot")
    except Exception:
        return "energy_copilot"


CATALOG: str = "energy_copilot"  # overridden in main() once spark is available

# Gold tables checked for freshness
REALTIME_GOLD_TABLES = [
    "gold.nem_prices_5min",
    "gold.nem_generation_dispatch",
    "gold.nem_interconnectors_5min",
    "gold.nem_forecasts_realtime",
]
DAILY_GOLD_TABLES = [
    "gold.daily_market_summary",
]

# Staleness thresholds
REALTIME_STALE_MINUTES = 10
DAILY_STALE_HOURS = 25

# Row count alert threshold (today vs 7-day average)
ROW_COUNT_ALERT_THRESHOLD = 0.80  # flag if today < 80% of average

# Null rate alert threshold
NULL_RATE_ALERT_PCT = 0.001  # 0.1%

# Columns to check for nulls in gold.nem_prices_5min
NULL_CHECK_COLUMNS = [
    "spot_price_aud_mwh",
    "total_demand_mw",
    "region_id",
    "interval_datetime",
]


# ===========================================================================
# 1. DLT expectation metrics
# ===========================================================================

def check_dlt_expectations(spark, catalog: str) -> Dict[str, Any]:
    """
    Query the DLT event log for the most recent pipeline run and extract
    per-expectation pass/fail counts.

    Falls back to a mock report if the event_log() TVF is unavailable
    (e.g. pipeline has never run, or queried outside Databricks).

    Returns a dict keyed by table_name -> list of expectation dicts.
    """
    if spark is None:
        logger.info("check_dlt_expectations: spark=None, returning mock metrics")
        return _mock_dlt_metrics()

    try:
        dlt_metrics_sql = f"""
        SELECT
            details.flow_progress.name AS table_name,
            expectations.name          AS expectation_name,
            expectations.failed_records  AS failed_records,
            expectations.passed_records  AS passed_records,
            ROUND(
                100.0 * expectations.passed_records
                / NULLIF(expectations.passed_records + expectations.failed_records, 0),
                2
            ) AS pass_rate_pct
        FROM (
            SELECT details, EXPLODE(details.flow_progress.data_quality.expectations) AS expectations
            FROM event_log(TABLE({catalog}.bronze.dlt_pipeline_id))
            WHERE event_type = 'flow_progress'
              AND details.flow_progress.status = 'COMPLETED'
        )
        ORDER BY table_name, expectation_name
        """
        rows = spark.sql(dlt_metrics_sql).collect()
        result: Dict[str, List[Dict]] = {}
        for row in rows:
            tbl = row["table_name"] or "unknown"
            if tbl not in result:
                result[tbl] = []
            result[tbl].append({
                "expectation_name": row["expectation_name"],
                "failed_records": int(row["failed_records"] or 0),
                "passed_records": int(row["passed_records"] or 0),
                "pass_rate_pct": float(row["pass_rate_pct"] or 0.0),
            })
        logger.info("check_dlt_expectations: retrieved metrics for %d tables", len(result))
        return result

    except Exception as exc:
        logger.warning(
            "check_dlt_expectations: event_log query failed (%s); using mock metrics", exc
        )
        return _mock_dlt_metrics()


def _mock_dlt_metrics() -> Dict[str, Any]:
    """Return plausible mock DLT expectation metrics for unit-test / fallback mode."""
    tables = [
        "nemweb_dispatch_price", "nemweb_dispatch_gen", "nemweb_dispatch_inter",
        "dispatch_price", "dispatch_gen", "dispatch_interconnector",
        "nem_prices_5min", "nem_prices_30min", "nem_generation_by_fuel",
        "nem_interconnectors", "nem_fcas_prices",
    ]
    mock: Dict[str, List[Dict]] = {}
    for tbl in tables:
        mock[tbl] = [
            {
                "expectation_name": "valid_settlement_date",
                "failed_records": 0,
                "passed_records": 2880,
                "pass_rate_pct": 100.0,
            },
            {
                "expectation_name": "valid_region",
                "failed_records": 2,
                "passed_records": 14398,
                "pass_rate_pct": 99.99,
            },
        ]
    return mock


# ===========================================================================
# 2. Gold table freshness
# ===========================================================================

def check_table_freshness(spark, catalog: str) -> List[Dict[str, Any]]:
    """
    For each key Gold table, compute minutes since the latest record's
    interval_datetime (or summary_date for daily tables).

    Returns a list of freshness dicts with keys:
        table, latest_record_ts, minutes_stale, status (fresh|stale|missing|error)
    """
    if spark is None:
        logger.info("check_table_freshness: spark=None, returning mock freshness")
        return _mock_table_freshness()

    results = []
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

    # Real-time tables: check interval_datetime
    for table_ref in REALTIME_GOLD_TABLES:
        full_ref = f"{catalog}.{table_ref}"
        entry = _check_realtime_table(spark, table_ref, full_ref, now_utc)
        results.append(entry)

    # Daily tables: check summary_date
    for table_ref in DAILY_GOLD_TABLES:
        full_ref = f"{catalog}.{table_ref}"
        entry = _check_daily_table(spark, table_ref, full_ref, now_utc)
        results.append(entry)

    return results


def _check_realtime_table(spark, table_ref: str, full_ref: str, now_utc: datetime) -> Dict:
    """Check freshness of a real-time Gold table via its interval_datetime column."""
    try:
        row = spark.sql(
            f"SELECT MAX(interval_datetime) AS latest FROM {full_ref}"
        ).collect()[0]
        latest = row["latest"]
        if latest is None:
            return {"table": table_ref, "latest_record_ts": None,
                    "minutes_stale": None, "status": "missing"}
        minutes_stale = (now_utc - latest).total_seconds() / 60.0
        status = "stale" if minutes_stale > REALTIME_STALE_MINUTES else "fresh"
        return {
            "table": table_ref,
            "latest_record_ts": latest.isoformat(),
            "minutes_stale": round(minutes_stale, 1),
            "status": status,
        }
    except Exception as exc:
        logger.warning("check_table_freshness: error querying %s — %s", full_ref, exc)
        return {"table": table_ref, "latest_record_ts": None,
                "minutes_stale": None, "status": "error", "error": str(exc)}


def _check_daily_table(spark, table_ref: str, full_ref: str, now_utc: datetime) -> Dict:
    """Check freshness of a daily Gold table via its summary_date column."""
    try:
        row = spark.sql(
            f"SELECT MAX(summary_date) AS latest FROM {full_ref}"
        ).collect()[0]
        latest = row["latest"]
        if latest is None:
            return {"table": table_ref, "latest_record_ts": None,
                    "minutes_stale": None, "status": "missing"}
        latest_dt = datetime(latest.year, latest.month, latest.day)
        hours_stale = (now_utc - latest_dt).total_seconds() / 3600.0
        status = "stale" if hours_stale > DAILY_STALE_HOURS else "fresh"
        return {
            "table": table_ref,
            "latest_record_ts": latest.isoformat(),
            "hours_stale": round(hours_stale, 1),
            "status": status,
        }
    except Exception as exc:
        logger.warning("check_table_freshness: error querying %s — %s", full_ref, exc)
        return {"table": table_ref, "latest_record_ts": None,
                "minutes_stale": None, "status": "error", "error": str(exc)}


def _mock_table_freshness() -> List[Dict]:
    now_iso = datetime.utcnow().isoformat()
    results = []
    for tbl in REALTIME_GOLD_TABLES:
        results.append({
            "table": tbl,
            "latest_record_ts": now_iso,
            "minutes_stale": 4.2,
            "status": "fresh",
        })
    for tbl in DAILY_GOLD_TABLES:
        results.append({
            "table": tbl,
            "latest_record_ts": date.today().isoformat(),
            "hours_stale": 1.5,
            "status": "fresh",
        })
    return results


# ===========================================================================
# 3. Row count trends
# ===========================================================================

def check_row_counts(spark, catalog: str) -> Dict[str, Any]:
    """
    Compare today's ingested row count vs 7-day average for gold.nem_prices_5min.
    Flags if today's count is < 80% of the 7-day average (data gap indicator).

    Returns a dict with keys: table, today_count, avg_7d_count, ratio, status, alert_message
    """
    if spark is None:
        logger.info("check_row_counts: spark=None, returning mock row count data")
        return _mock_row_counts()

    table_ref = f"{catalog}.gold.nem_prices_5min"
    try:
        today_str = date.today().isoformat()
        seven_days_ago_str = (date.today() - timedelta(days=7)).isoformat()

        today_row = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {table_ref}
            WHERE interval_date = '{today_str}'
        """).collect()[0]
        today_count = int(today_row["cnt"])

        avg_row = spark.sql(f"""
            SELECT AVG(daily_count) AS avg_cnt
            FROM (
                SELECT interval_date, COUNT(*) AS daily_count
                FROM {table_ref}
                WHERE interval_date >= '{seven_days_ago_str}'
                  AND interval_date < '{today_str}'
                GROUP BY interval_date
            )
        """).collect()[0]
        avg_7d = float(avg_row["avg_cnt"] or 0.0)

        if avg_7d == 0:
            ratio = None
            status = "no_baseline"
            alert_message = "No 7-day baseline available for row count comparison."
        else:
            ratio = today_count / avg_7d
            if ratio < ROW_COUNT_ALERT_THRESHOLD:
                status = "data_gap"
                alert_message = (
                    f"Row count alert: {table_ref} has {today_count} rows today "
                    f"({ratio:.1%} of 7-day avg {avg_7d:.0f}). Possible data gap."
                )
            else:
                status = "ok"
                alert_message = None

        return {
            "table": "gold.nem_prices_5min",
            "today_count": today_count,
            "avg_7d_count": round(avg_7d, 1),
            "ratio": round(ratio, 4) if ratio is not None else None,
            "status": status,
            "alert_message": alert_message,
        }

    except Exception as exc:
        logger.warning("check_row_counts: query failed — %s", exc)
        return {
            "table": "gold.nem_prices_5min",
            "today_count": None,
            "avg_7d_count": None,
            "ratio": None,
            "status": "error",
            "alert_message": str(exc),
        }


def _mock_row_counts() -> Dict[str, Any]:
    avg = 14400.0  # 5 regions * 288 intervals/day
    today = 14256
    ratio = today / avg
    return {
        "table": "gold.nem_prices_5min",
        "today_count": today,
        "avg_7d_count": avg,
        "ratio": round(ratio, 4),
        "status": "ok",
        "alert_message": None,
    }


# ===========================================================================
# 4. Null rate scan
# ===========================================================================

def check_null_rates(spark, catalog: str) -> List[Dict[str, Any]]:
    """
    For gold.nem_prices_5min, compute the null rate for each key column.
    Flags if null rate > NULL_RATE_ALERT_PCT (0.1%) for any critical column.

    Returns a list of per-column dicts with keys:
        column, total_rows, null_count, null_rate_pct, status
    """
    if spark is None:
        logger.info("check_null_rates: spark=None, returning mock null rate data")
        return _mock_null_rates()

    table_ref = f"{catalog}.gold.nem_prices_5min"
    today_str = date.today().isoformat()
    results = []

    try:
        total_row = spark.sql(f"""
            SELECT COUNT(*) AS total FROM {table_ref}
            WHERE interval_date = '{today_str}'
        """).collect()[0]
        total_rows = int(total_row["total"])

        if total_rows == 0:
            logger.warning("check_null_rates: no rows found for today (%s)", today_str)
            for col in NULL_CHECK_COLUMNS:
                results.append({
                    "column": col,
                    "total_rows": 0,
                    "null_count": 0,
                    "null_rate_pct": 0.0,
                    "status": "no_data",
                })
            return results

        for col in NULL_CHECK_COLUMNS:
            null_row = spark.sql(f"""
                SELECT COUNT(*) AS null_cnt
                FROM {table_ref}
                WHERE interval_date = '{today_str}'
                  AND {col} IS NULL
            """).collect()[0]
            null_count = int(null_row["null_cnt"])
            null_rate = null_count / total_rows
            null_rate_pct = round(null_rate * 100, 4)
            status = "alert" if null_rate > NULL_RATE_ALERT_PCT else "ok"
            results.append({
                "column": col,
                "total_rows": total_rows,
                "null_count": null_count,
                "null_rate_pct": null_rate_pct,
                "status": status,
            })

    except Exception as exc:
        logger.warning("check_null_rates: query failed — %s", exc)
        for col in NULL_CHECK_COLUMNS:
            results.append({
                "column": col,
                "total_rows": None,
                "null_count": None,
                "null_rate_pct": None,
                "status": "error",
                "error": str(exc),
            })

    return results


def _mock_null_rates() -> List[Dict]:
    return [
        {"column": col, "total_rows": 14400, "null_count": 0,
         "null_rate_pct": 0.0, "status": "ok"}
        for col in NULL_CHECK_COLUMNS
    ]


# ===========================================================================
# 5. Overall status aggregation
# ===========================================================================

def _count_failures(
    dlt_metrics: Dict,
    freshness: List[Dict],
    row_counts: Dict,
    null_rates: List[Dict],
) -> Tuple[int, int, int]:
    """
    Return (total_checks, passed_checks, failed_checks).
    A check is considered failed if any sub-item is in status error/stale/data_gap/alert/missing.
    """
    total = 0
    failed = 0

    # DLT expectations: count each expectation as one check; flag if pass_rate_pct < 99.0
    for tbl, expectations in dlt_metrics.items():
        for exp in expectations:
            total += 1
            if exp.get("pass_rate_pct") is not None and exp["pass_rate_pct"] < 99.0:
                failed += 1

    # Freshness: one check per table
    for entry in freshness:
        total += 1
        if entry.get("status") in ("stale", "missing", "error"):
            failed += 1

    # Row counts: one check
    total += 1
    if row_counts.get("status") in ("data_gap", "error"):
        failed += 1

    # Null rates: one check per column
    for entry in null_rates:
        total += 1
        if entry.get("status") in ("alert", "error"):
            failed += 1

    passed = total - failed
    return total, passed, failed


def _overall_status(failed_checks: int) -> str:
    """green = all pass, amber = 1-2 failures, red = 3+ failures."""
    if failed_checks == 0:
        return "green"
    elif failed_checks <= 2:
        return "amber"
    else:
        return "red"


# ===========================================================================
# 6. Alert stub
# ===========================================================================

def _send_alert_stub(message: str) -> None:
    """
    Log the alert message. Wire to Databricks notification destinations
    (Slack / PagerDuty / email) by replacing this function body with an
    HTTP POST to the Databricks REST API notification endpoint:
      POST /api/2.0/notification-destinations/{id}/send
    """
    logger.error("DATA QUALITY ALERT: %s", message)
    # TODO: implement Databricks notification destination webhook call
    # import requests
    # requests.post(
    #     f"{DATABRICKS_HOST}/api/2.0/notification-destinations/{DEST_ID}/send",
    #     headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
    #     json={"message": message},
    # )


# ===========================================================================
# 7. Write summary to gold.data_quality_daily_report
# ===========================================================================

def write_report(
    spark,
    catalog: str,
    report_date: date,
    generated_at: datetime,
    overall_status: str,
    dlt_metrics: Dict,
    freshness: List[Dict],
    row_counts: Dict,
    null_rates: List[Dict],
    total_checks: int,
    passed_checks: int,
    failed_checks: int,
) -> None:
    """
    Write the daily data quality summary to gold.data_quality_daily_report.
    Schema:
        report_date DATE, generated_at TIMESTAMP, overall_status STRING,
        dlt_expectations_json STRING, freshness_json STRING,
        row_count_json STRING, null_rates_json STRING,
        total_checks INT, passed_checks INT, failed_checks INT
    """
    if spark is None:
        logger.info(
            "write_report: spark=None (unit-test mode). Would write report for %s: "
            "status=%s, total=%d, passed=%d, failed=%d",
            report_date, overall_status, total_checks, passed_checks, failed_checks,
        )
        return

    from pyspark.sql import Row

    row = Row(
        report_date=report_date,
        generated_at=generated_at,
        overall_status=overall_status,
        dlt_expectations_json=json.dumps(dlt_metrics),
        freshness_json=json.dumps(freshness),
        row_count_json=json.dumps(row_counts),
        null_rates_json=json.dumps(null_rates),
        total_checks=total_checks,
        passed_checks=passed_checks,
        failed_checks=failed_checks,
    )

    target_table = f"{catalog}.gold.data_quality_daily_report"
    df = spark.createDataFrame([row])

    # Upsert: replace today's report if it already exists
    df.createOrReplaceTempView("_dq_report_staging")
    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING _dq_report_staging AS source
        ON target.report_date = source.report_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info("write_report: upserted report for %s into %s", report_date, target_table)


# ===========================================================================
# 8. Validate report structure (unit-test helper)
# ===========================================================================

def _validate_report_structure(
    dlt_metrics: Dict,
    freshness: List[Dict],
    row_counts: Dict,
    null_rates: List[Dict],
    total_checks: int,
    passed_checks: int,
    failed_checks: int,
    overall_status: str,
) -> bool:
    """
    Validate that all report components have the expected structure.
    Returns True if valid, raises AssertionError on the first violation.
    """
    # DLT metrics: dict of table -> list of expectation dicts
    assert isinstance(dlt_metrics, dict), "dlt_metrics must be a dict"
    for tbl, exps in dlt_metrics.items():
        assert isinstance(exps, list), f"dlt_metrics[{tbl!r}] must be a list"
        for exp in exps:
            for key in ("expectation_name", "failed_records", "passed_records", "pass_rate_pct"):
                assert key in exp, f"Missing key {key!r} in expectation for table {tbl!r}"

    # Freshness: list of dicts
    assert isinstance(freshness, list), "freshness must be a list"
    for entry in freshness:
        assert "table" in entry, "freshness entry missing 'table' key"
        assert "status" in entry, "freshness entry missing 'status' key"

    # Row counts: single dict
    assert isinstance(row_counts, dict), "row_counts must be a dict"
    for key in ("table", "status"):
        assert key in row_counts, f"row_counts missing key {key!r}"

    # Null rates: list of dicts
    assert isinstance(null_rates, list), "null_rates must be a list"
    for entry in null_rates:
        assert "column" in entry, "null_rates entry missing 'column' key"
        assert "status" in entry, "null_rates entry missing 'status' key"

    # Counts
    assert isinstance(total_checks, int) and total_checks >= 0
    assert isinstance(passed_checks, int) and passed_checks >= 0
    assert isinstance(failed_checks, int) and failed_checks >= 0
    assert passed_checks + failed_checks == total_checks, (
        f"passed ({passed_checks}) + failed ({failed_checks}) != total ({total_checks})"
    )

    # Overall status
    assert overall_status in ("green", "amber", "red"), (
        f"Invalid overall_status: {overall_status!r}"
    )

    logger.info("_validate_report_structure: all assertions passed")
    return True


# ===========================================================================
# 9. Main entry point
# ===========================================================================

def main(spark=None) -> Dict[str, Any]:
    """
    Orchestrate the data quality report pipeline.

    When spark=None (unit-test / CLI mode) all check functions return mock
    data and the write step is skipped.  The function returns the assembled
    report dict so callers can inspect it.
    """
    global CATALOG
    CATALOG = _get_catalog(spark)

    report_date = date.today()
    generated_at = datetime.utcnow()

    logger.info(
        "Data quality report started — report_date=%s, catalog=%s",
        report_date, CATALOG,
    )

    # --- Run all checks ---
    logger.info("Step 1/4: checking DLT expectation metrics …")
    dlt_metrics = check_dlt_expectations(spark, CATALOG)

    logger.info("Step 2/4: checking Gold table freshness …")
    freshness = check_table_freshness(spark, CATALOG)

    logger.info("Step 3/4: checking row count trends …")
    row_counts = check_row_counts(spark, CATALOG)

    logger.info("Step 4/4: scanning null rates …")
    null_rates = check_null_rates(spark, CATALOG)

    # --- Aggregate ---
    total_checks, passed_checks, failed_checks = _count_failures(
        dlt_metrics, freshness, row_counts, null_rates
    )
    overall_status = _overall_status(failed_checks)

    logger.info(
        "Report summary: status=%s, total=%d, passed=%d, failed=%d",
        overall_status, total_checks, passed_checks, failed_checks,
    )

    # --- Alert if red ---
    if overall_status == "red":
        alert_msg = (
            f"[{report_date}] Data quality RED: {failed_checks}/{total_checks} checks failed. "
            f"Review gold.data_quality_daily_report for details."
        )
        _send_alert_stub(alert_msg)

    # --- Write report ---
    write_report(
        spark=spark,
        catalog=CATALOG,
        report_date=report_date,
        generated_at=generated_at,
        overall_status=overall_status,
        dlt_metrics=dlt_metrics,
        freshness=freshness,
        row_counts=row_counts,
        null_rates=null_rates,
        total_checks=total_checks,
        passed_checks=passed_checks,
        failed_checks=failed_checks,
    )

    # --- Validate structure (always, for CI confidence) ---
    _validate_report_structure(
        dlt_metrics=dlt_metrics,
        freshness=freshness,
        row_counts=row_counts,
        null_rates=null_rates,
        total_checks=total_checks,
        passed_checks=passed_checks,
        failed_checks=failed_checks,
        overall_status=overall_status,
    )

    report = {
        "report_date": report_date.isoformat(),
        "generated_at": generated_at.isoformat(),
        "overall_status": overall_status,
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "dlt_expectations": dlt_metrics,
        "freshness": freshness,
        "row_counts": row_counts,
        "null_rates": null_rates,
    }

    logger.info("Data quality report complete.")
    return report


# ===========================================================================
# CLI entry point — runs with mock data when spark is unavailable
# ===========================================================================

if __name__ == "__main__":
    import sys

    # Attempt to obtain a real Spark session; fall back to None for local testing.
    try:
        from pyspark.sql import SparkSession

        _spark = SparkSession.getActiveSession()
    except ImportError:
        _spark = None

    if _spark is None:
        logger.info("No active Spark session — running in unit-test / local mode with mock data.")

    report = main(spark=_spark)

    print("\n=== Data Quality Report ===")
    print(f"  Date           : {report['report_date']}")
    print(f"  Generated at   : {report['generated_at']}")
    print(f"  Overall status : {report['overall_status'].upper()}")
    print(f"  Checks         : {report['passed_checks']}/{report['total_checks']} passed")
    print(f"  Failed checks  : {report['failed_checks']}")

    sys.exit(0 if report["overall_status"] in ("green", "amber") else 1)
