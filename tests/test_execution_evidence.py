"""Checks on the execution evidence captured from real Databricks job runs (evidence/captured_runs.py).

The captured outputs are verbatim cell outputs of job runs on fevm-serverless-stable-2b9lj3 (see
notebooks/executed/). These tests tie each run to the source file it executed and assert the facts the
README, EVIDENCE.md and the deck rely on, so the claims are checked rather than only stated.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from evidence import captured_runs as runs

ROOT = Path(__file__).resolve().parents[1]
TRAIN_LINE = re.compile(
    r"(?P<region>NSW1|QLD1|SA1|TAS1|VIC1) (?P<target>price|demand): v(?P<version>\d+) \| "
    r"h6 MAE (?P<m6>[\d.]+) \(persistence (?P<p6>[\d.]+)\), h12 MAE (?P<m12>[\d.]+) \(persistence (?P<p12>[\d.]+)\), "
    r"h48 MAE (?P<m48>[\d.]+) \(persistence (?P<p48>[\d.]+)\)")


def text(run: dict) -> str:
    return "\n".join(run["cell_outputs"])


def json_summary(run: dict) -> dict:
    """The JSON summary each pipeline prints as its final cell output."""
    for cell in reversed(run["cell_outputs"]):
        start = cell.find("{")
        if start >= 0:
            try:
                return json.loads(cell[start:])
            except json.JSONDecodeError:
                continue
    raise AssertionError("no JSON summary in run output")


@pytest.mark.parametrize("name", sorted(runs.ALL_RUNS))
def test_every_run_succeeded_and_its_source_is_in_this_repo(name: str) -> None:
    run = runs.ALL_RUNS[name]
    assert run["result"] == "SUCCESS"
    assert (ROOT / run["source_file"]).exists(), run["source_file"]
    assert (ROOT / run["executed_notebook"]).exists(), run["executed_notebook"]
    assert run["cell_outputs"], "run has no captured output"


# --- Ingestion: real AEMO NEMWEB rows landed ------------------------------------------------------------------

def test_live_ingest_landed_rows_from_aemo() -> None:
    out = text(runs.INGEST_LIVE)
    assert "Remote files:" in out and "Parsed 5 price rows from 1 files" in out
    summary = json_summary(runs.INGEST_LIVE)
    assert summary["rows_written"] == 5
    assert summary["regions"] == ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]


def test_live_batch_passed_data_quality_and_freshness() -> None:
    summary = json_summary(runs.INGEST_LIVE_DQ)
    assert summary["rows_merged"] == summary["rows_checked"] == 5
    assert summary["rows_quarantined"] == 0
    assert summary["freshness"] == "PASS"


def test_archive_backfill_landed_90_days() -> None:
    summary = json_summary(runs.BACKFILL)
    assert summary["days_requested"] == 90 and summary["days_failed"] == 0
    assert summary["rows_written"] == 128_860


def test_data_quality_rule_quarantined_bad_rows() -> None:
    out = text(runs.DQ_QUARANTINE)
    assert "Checked 2875 rows | quarantined 102" in out
    assert json_summary(runs.DQ_QUARANTINE)["rule_failures"]["demand_non_negative"] == 102


# --- Models: trained, back-tested against persistence, registered in Unity Catalog --------------------------

def training_results() -> list[dict]:
    return [m.groupdict() for m in TRAIN_LINE.finditer(text(runs.MODEL_TRAINING))]


def test_training_log_covers_all_ten_models() -> None:
    results = training_results()
    assert {(r["region"], r["target"]) for r in results} == {
        (reg, tgt) for reg in ("NSW1", "QLD1", "SA1", "TAS1", "VIC1") for tgt in ("price", "demand")}
    assert json_summary(runs.MODEL_TRAINING)["models_registered"] == 10
    assert "Created version" in text(runs.MODEL_TRAINING)


def test_demand_models_beat_persistence_at_every_horizon() -> None:
    for r in (r for r in training_results() if r["target"] == "demand"):
        for h in ("6", "12", "48"):
            assert float(r[f"m{h}"]) < float(r[f"p{h}"]), (r["region"], h)


def test_price_models_beat_persistence_at_4h() -> None:
    for r in (r for r in training_results() if r["target"] == "price"):
        assert float(r["m48"]) < float(r["p48"]), r["region"]


def test_mlflow_training_runs_were_logged() -> None:
    out = text(runs.EVIDENCE_VERIFICATION)
    assert "MLflow training runs in /Shared/energy_copilot_nem_forecast" in out
    assert len(re.findall(r"\b[0-9a-f]{32}\b .*FINISHED", out)) >= 10


def test_live_inference_used_registered_models() -> None:
    summary = json_summary(runs.LIVE_INFERENCE)
    assert summary["price_forecasts"] == 15 and summary["demand_forecasts"] == 15
    assert all(s["model_version"] == "6" for s in summary["sample"])


# --- AI: assistant conversation with tool results, Genie answer with returned rows -------------------------

def test_assistant_conversation_used_live_tools() -> None:
    out = text(runs.EVIDENCE_VERIFICATION)
    calls = re.findall(r"TOOL CALL \(round \d+\): (\w+)", out)
    assert calls == ["query_spot_prices", "query_spot_prices", "query_price_forecasts", "query_price_forecasts"]
    assert out.count('"source": "gold.nem_prices_5min') == 2
    assert out.count("UC model serverless_stable_2b9lj3_catalog.ml.nem_price_forecast_") == 2
    assert "ASSISTANT (" in out and "$115.90/MWh" in out  # tool returned latest_rrp 115.89764 for SA1
    assert '"latest_rrp": 115.89764' in out


def test_genie_answered_with_sql_and_rows_matching_an_independent_query() -> None:
    out = text(runs.EVIDENCE_VERIFICATION)
    assert "SQL GENERATED BY GENIE:" in out and "gold`.`nem_prices_5min`" in out
    assert "[['NSW1', '87', '23200.0']]" in out
    assert re.search(r"\|NSW1\s*\|87\s*\|23200\.0", out), "independent check did not match"


# --- Governance: policies applied and registered in Unity Catalog ------------------------------------------

def test_governance_policies_applied_and_registered() -> None:
    applied = text(runs.UC_GOVERNANCE)
    assert "OK  row filter on gold.trades" in applied
    assert "OK  column mask on gold.counterparties.credit_limit_aud" in applied
    registered = text(runs.EVIDENCE_VERIFICATION)
    assert "|row_filter |trades" in registered and "tools.trades_row_filter" in registered
    assert re.search(r"\|sourabh\.ghose@databricks\.com\|Hedge Book\s*\|14", registered)
    assert re.search(r"\|sourabh\.ghose@databricks\.com\|Trading Book\|28", registered)
