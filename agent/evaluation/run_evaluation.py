"""
agent/evaluation/run_evaluation.py
=====================================
Evaluation harness for the AUS Energy Copilot agent.

Runs all 50 evaluation pairs from eval_dataset.py against the agent,
scores responses using a combination of rule-based checks and an LLM judge,
and writes results to both MLflow and a local JSON report.

Scoring rubric:
  - key_fact_coverage  (0-1.0): Fraction of required key_facts present in response
  - tool_usage         (0-1.0): Whether appropriate tools were called
  - decline_correctness(0-1.0): For OUT_OF_SCOPE: did agent decline? For others: did it not?
  - response_quality   (0-1.0): LLM judge score for accuracy, relevance, and tone
  - overall_score      (0-1.0): Weighted average

Usage:
  python -m agent.evaluation.run_evaluation [--categories FACTUAL_MARKET FORECAST]
                                            [--output report.json]
                                            [--dry-run]
                                            [--max-pairs N]
                                            [--output-file PATH]

Prerequisites:
  ANTHROPIC_API_KEY, DATABRICKS_HOST, DATABRICKS_TOKEN env vars set.
  Agent tools connected to a running Databricks SQL warehouse.

Mock mode (CI-safe):
  When ANTHROPIC_API_KEY starts with "sk-ant-dummy" or "sk-ant-test", the LLM
  judge is replaced with a mock that always returns score=0.8, enabling the
  evaluation to run in CI without spending real API tokens.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import mlflow
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Import agent and eval dataset
# ---------------------------------------------------------------------------

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # project root

from agent.copilot_agent import build_agent_executor
from agent.evaluation.eval_dataset import ALL_EVAL_PAIRS, CATEGORY_COUNTS, EvalPair

# ---------------------------------------------------------------------------
# Mock mode detection
# ---------------------------------------------------------------------------

def _is_mock_api_key() -> bool:
    """Return True when the API key is a dummy/test key (CI mode)."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    return api_key.startswith("sk-ant-dummy") or api_key.startswith("sk-ant-test")


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def score_key_fact_coverage(response: str, key_facts: list[str]) -> float:
    """
    Return the fraction of key_facts that appear (case-insensitive) in response.
    """
    if not key_facts:
        return 1.0
    response_lower = response.lower()
    hits = sum(1 for fact in key_facts if fact.lower() in response_lower)
    return round(hits / len(key_facts), 3)


def score_tool_usage(intermediate_steps: list[dict], category: str) -> float:
    """
    Score whether appropriate tools were used based on category.
    OUT_OF_SCOPE questions should have 0 tool calls.
    All others should have at least 1 tool call.
    """
    num_tools_called = len(intermediate_steps)
    if category == "OUT_OF_SCOPE":
        # For declined queries, no tool calls expected
        return 1.0 if num_tools_called == 0 else 0.5
    else:
        return 1.0 if num_tools_called >= 1 else 0.0


def score_decline_correctness(response: str, should_decline: bool) -> float:
    """
    Score whether the agent correctly handled out-of-scope questions.
    For should_decline=True, check for decline indicators.
    For should_decline=False, check the agent did NOT refuse.
    """
    decline_indicators = [
        "cannot", "can't", "outside", "not able", "not something i",
        "not a question", "don't provide", "unable to", "not within",
        "scope", "financial adviser", "trading advice",
    ]
    response_lower = response.lower()
    has_decline = any(ind in response_lower for ind in decline_indicators)

    if should_decline:
        return 1.0 if has_decline else 0.0
    else:
        # Should NOT have declined a legitimate question
        return 0.0 if has_decline else 1.0


def _mock_llm_judge_score() -> float:
    """Mock LLM judge for CI/dry-run mode. Always returns 0.8."""
    return 0.8


def score_with_llm_judge(
    question: str,
    response: str,
    category: str,
    anthropic_client,
) -> float:
    """
    Use Claude as an LLM judge to score response quality on a 0-4 scale,
    then normalise to 0.0-1.0.

    When the API key is a dummy/test key (CI mode), returns a fixed mock
    score of 0.8 to avoid spending real API tokens.

    Criteria:
      4 - Excellent: accurate, complete, properly cites data timestamps, appropriate tone
      3 - Good: mostly accurate, minor omissions, acceptable tone
      2 - Adequate: partially correct or missing key details
      1 - Poor: inaccurate, unhelpful, or inappropriate
      0 - Failure: wrong answer, harmful content, or failed to decline when required
    """
    if _is_mock_api_key():
        logger.debug("Mock LLM judge active (dummy API key): returning score=0.8")
        return _mock_llm_judge_score()

    judge_prompt = f"""You are evaluating an AI assistant response for an Australian energy market platform.

QUESTION: {question}

CATEGORY: {category}

RESPONSE: {response}

Score the response on a scale of 0-4 based on:
- Accuracy of market data and facts
- Completeness (covers all aspects of the question)
- Data citation (response references data timestamps when using live data)
- Appropriate tone (concise, trader-friendly, no made-up data)
- For OUT_OF_SCOPE: did it politely decline without being unhelpful?

Respond with ONLY a JSON object in this exact format:
{{"score": <0-4 integer>, "reason": "<one sentence>"}}"""

    try:
        message = anthropic_client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=100,
            messages=[{"role": "user", "content": judge_prompt}],
        )
        raw = message.content[0].text.strip()
        # Parse JSON response
        result = json.loads(raw)
        score = int(result.get("score", 2))
        return round(min(max(score, 0), 4) / 4.0, 3)
    except Exception as exc:
        logger.warning("LLM judge failed: %s", exc)
        return 0.5  # Default to neutral score on failure


def compute_overall_score(
    key_fact_coverage: float,
    tool_usage: float,
    decline_correctness: float,
    response_quality: float,
) -> float:
    """
    Weighted average of component scores.
    Weights: key_fact=0.3, tool_usage=0.2, decline=0.2, quality=0.3
    """
    return round(
        0.30 * key_fact_coverage
        + 0.20 * tool_usage
        + 0.20 * decline_correctness
        + 0.30 * response_quality,
        3,
    )


# ---------------------------------------------------------------------------
# Run single eval pair
# ---------------------------------------------------------------------------

def run_eval_pair(
    pair: EvalPair,
    executor,
    anthropic_client,
    timeout_seconds: float = 60.0,
) -> dict:
    """
    Run a single evaluation pair and return a result dict.
    """
    start_time = time.monotonic()

    try:
        result = executor.invoke(
            {"input": pair.question, "chat_history": []},
        )
        elapsed = time.monotonic() - start_time
        response_text = result.get("output", "")
        intermediate_steps = [
            {
                "tool": step[0].tool,
                "tool_input": step[0].tool_input,
                "observation": str(step[1])[:300],
            }
            for step in result.get("intermediate_steps", [])
        ]
        error = None

    except Exception as exc:
        elapsed = time.monotonic() - start_time
        response_text = f"ERROR: {exc}"
        intermediate_steps = []
        error = str(exc)
        logger.warning("Agent failed on question '%s': %s", pair.question[:60], exc)

    # Scoring
    key_fact_score = score_key_fact_coverage(response_text, pair.key_facts)
    tool_score = score_tool_usage(intermediate_steps, pair.category)
    decline_score = score_decline_correctness(response_text, pair.should_decline)
    quality_score = score_with_llm_judge(pair.question, response_text, pair.category, anthropic_client)
    overall = compute_overall_score(key_fact_score, tool_score, decline_score, quality_score)

    return {
        "question": pair.question,
        "category": pair.category,
        "expected_type": pair.expected_type,
        "should_decline": pair.should_decline,
        "notes": pair.notes,
        "response": response_text,
        "intermediate_steps": intermediate_steps,
        "scores": {
            "key_fact_coverage": key_fact_score,
            "tool_usage": tool_score,
            "decline_correctness": decline_score,
            "response_quality": quality_score,
            "overall": overall,
        },
        "elapsed_seconds": round(elapsed, 2),
        "error": error,
    }


# ---------------------------------------------------------------------------
# Evaluation harness
# ---------------------------------------------------------------------------

def run_evaluation(
    categories: Optional[list[str]] = None,
    output_path: Optional[Path] = None,
    mlflow_experiment: Optional[str] = None,
    dry_run: bool = False,
    max_pairs: Optional[int] = None,
    output_file: Optional[Path] = None,
) -> dict:
    """
    Run the full evaluation suite and return an aggregated report dict.

    Args:
        categories:        List of category names to evaluate (None = all).
        output_path:       Path to write JSON report (None = no file output).
                           Legacy alias; prefer output_file.
        mlflow_experiment: MLflow experiment name for tracking.
        dry_run:           When True, skip all MLflow logging calls.
        max_pairs:         Limit evaluation to the first N pairs.
        output_file:       Path to write the CI-format JSON summary report.

    Returns:
        Dict with "summary", "by_category", and "results" keys.
    """
    import anthropic

    # Filter eval pairs by category
    pairs = ALL_EVAL_PAIRS
    if categories:
        pairs = [p for p in pairs if p.category in categories]

    # Limit to first N pairs when requested (fast CI mode)
    if max_pairs is not None and max_pairs > 0:
        pairs = pairs[:max_pairs]
        logger.info("max_pairs=%d applied; evaluating %d pairs", max_pairs, len(pairs))

    logger.info("Running evaluation on %d pairs (categories: %s, dry_run=%s)",
                len(pairs), categories or "ALL", dry_run)

    # Build agent
    executor = build_agent_executor(streaming=False, verbose=False)
    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    # MLflow setup (skipped in dry-run mode)
    if not dry_run:
        if mlflow_experiment:
            mlflow.set_experiment(mlflow_experiment)
        else:
            mlflow.set_experiment("/Users/energy-copilot-team/copilot-evaluation")

    results = []

    def _do_run():
        for i, pair in enumerate(pairs, 1):
            logger.info("[%d/%d] Category=%s | %s", i, len(pairs),
                        pair.category, pair.question[:70])
            result = run_eval_pair(pair, executor, anthropic_client)
            results.append(result)

            # Log per-pair metrics to MLflow (skip in dry-run)
            if not dry_run:
                mlflow.log_metric(f"pair_{i:02d}_overall", result["scores"]["overall"])

        # Aggregate scores
        all_scores = [r["scores"]["overall"] for r in results]
        avg_overall = round(sum(all_scores) / len(all_scores), 3) if all_scores else 0.0

        by_category: dict = {}
        for r in results:
            cat = r["category"]
            by_category.setdefault(cat, []).append(r["scores"]["overall"])

        category_averages = {
            cat: round(sum(scores) / len(scores), 3)
            for cat, scores in by_category.items()
        }

        # Log aggregate metrics (skip in dry-run)
        if not dry_run:
            mlflow.log_metric("avg_overall_score", avg_overall)
            for cat, score in category_averages.items():
                mlflow.log_metric(f"avg_{cat.lower()}", score)

        pass_threshold = 0.70
        pass_count = sum(1 for s in all_scores if s >= pass_threshold)
        pass_rate = round(pass_count / len(all_scores), 3) if all_scores else 0.0

        if not dry_run:
            mlflow.log_metric("pass_rate_70pct", pass_rate)

        logger.info("=== EVALUATION COMPLETE ===")
        logger.info("Overall average score: %.3f", avg_overall)
        logger.info("Pass rate (>= 0.70): %.1f%% (%d/%d)", pass_rate * 100,
                    pass_count, len(all_scores))
        for cat, score in sorted(category_averages.items()):
            logger.info("  %s: %.3f", cat, score)

        # Determine effective output path (output_file takes priority)
        effective_output_path = output_file or output_path

        report = {
            "run_at": datetime.now(timezone.utc).isoformat(),
            "num_pairs": len(pairs),
            "dry_run": dry_run,
            "summary": {
                "avg_overall_score": avg_overall,
                "pass_rate_70pct": pass_rate,
                "pass_count": pass_count,
                "total_count": len(all_scores),
            },
            "by_category": category_averages,
            "results": results,
            # Top-level CI-friendly fields for the GitHub Actions check step
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_pairs": len(pairs),
            "passed": pass_count,
            "failed": len(all_scores) - pass_count,
            "pass_rate": pass_rate,
            "scores_by_dimension": {
                "key_fact_coverage": round(
                    sum(r["scores"]["key_fact_coverage"] for r in results) / len(results), 3
                ) if results else 0.0,
                "tool_usage": round(
                    sum(r["scores"]["tool_usage"] for r in results) / len(results), 3
                ) if results else 0.0,
                "decline_correctness": round(
                    sum(r["scores"]["decline_correctness"] for r in results) / len(results), 3
                ) if results else 0.0,
                "response_quality": round(
                    sum(r["scores"]["response_quality"] for r in results) / len(results), 3
                ) if results else 0.0,
            },
        }

        if effective_output_path:
            effective_output_path = Path(effective_output_path)
            effective_output_path.parent.mkdir(parents=True, exist_ok=True)
            effective_output_path.write_text(json.dumps(report, indent=2, default=str))
            logger.info("Report written to: %s", effective_output_path)
            if not dry_run:
                mlflow.log_artifact(str(effective_output_path))

        return report

    if dry_run:
        # Skip MLflow context manager entirely in dry-run mode
        report = _do_run()
    else:
        with mlflow.start_run(run_name=f"eval-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"):
            mlflow.log_param("num_pairs", len(pairs))
            mlflow.log_param("categories", json.dumps(categories or "ALL"))
            mlflow.log_param("llm_model", "claude-sonnet-4-5")
            report = _do_run()

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run AUS Energy Copilot evaluation")
    parser.add_argument(
        "--categories",
        nargs="+",
        choices=list(CATEGORY_COUNTS.keys()),
        help="Restrict evaluation to specific categories",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to write JSON evaluation report (legacy flag)",
    )
    parser.add_argument(
        "--experiment",
        type=str,
        default=None,
        help="MLflow experiment name",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help=(
            "Skip all MLflow logging calls. Useful for CI where MLflow is not "
            "configured. Also activates mock LLM judge when API key is a dummy key."
        ),
    )
    parser.add_argument(
        "--max-pairs",
        type=int,
        default=None,
        metavar="N",
        help="Limit evaluation to the first N Q&A pairs (fast CI check).",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Write a CI-format JSON summary report to this path. "
            "Report includes: timestamp, total_pairs, passed, failed, "
            "pass_rate, scores_by_dimension."
        ),
    )
    args = parser.parse_args()

    # Default output path when neither --output nor --output-file is given
    default_output = (
        args.output_file
        or args.output
        or Path(__file__).parent / "results" / "eval_report.json"
    )

    report = run_evaluation(
        categories=args.categories,
        output_path=default_output if not args.output_file else args.output,
        mlflow_experiment=args.experiment,
        dry_run=args.dry_run,
        max_pairs=args.max_pairs,
        output_file=args.output_file,
    )

    print(f"\nOverall score: {report['summary']['avg_overall_score']:.3f}")
    print(f"Pass rate (>=70%): {report['summary']['pass_rate_70pct']*100:.1f}%")
    if args.dry_run:
        print("[dry-run] MLflow logging was skipped.")
    if args.output_file:
        print(f"CI report written to: {args.output_file}")
