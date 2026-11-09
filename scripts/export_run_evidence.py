"""Export Databricks job-run notebooks (with their real cell outputs) as reviewable evidence.

For each run ID, every task run is exported via the Jobs API (`databricks jobs export-run --views-to-export ALL`):
  * <name>_<task_run_id>.html  — the notebook exactly as it ran, outputs included (opens in a browser)
  * <name>_<task_run_id>.md    — a plain-text transcript: run metadata, then each cell's code and printed output

Usage:
    python scripts/export_run_evidence.py --profile <cli-profile> --out docs/evidence/runs \
        --run ingest_dq=230199744739803 --run training=940474602766977 ...
Auth is handled by the Databricks CLI profile; no token is read by this script.
"""
from __future__ import annotations

import argparse
import base64
import json
import re
import subprocess
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
MAX_CODE_LINES = 25
MAX_OUTPUT_CHARS = 20000


def cli(profile: str, *args: str) -> dict:
    out = subprocess.run(["databricks", *args, "--profile", profile, "-o", "json"],
                         capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def notebook_model(html: str) -> dict:
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", html)
    if not m:
        raise ValueError("exported HTML has no embedded notebook model")
    return json.loads(urllib.parse.unquote(base64.b64decode(m.group(1)).decode("utf-8")))


def render_output(results: dict | None) -> str:
    """Flatten a command's results (stdout, return values, tables, errors) to plain text."""
    if not results:
        return ""
    kind, data = results.get("type"), results.get("data")
    if kind == "listResults":
        return "".join(render_output(r) for r in data or [])
    if kind in ("ansi", "text", "raw", "html"):
        text = data if isinstance(data, str) else json.dumps(data, default=str)
        return ANSI.sub("", re.sub(r"<[^>]+>", "", text) if kind == "html" else text)
    if kind == "table":
        cols = [c.get("name") for c in results.get("schema", [])]
        rows = [" | ".join(str(v) for v in row) for row in (data or [])[:50]]
        return " | ".join(cols) + "\n" + "\n".join(rows) + "\n"
    if kind == "error":
        return "ERROR: " + ANSI.sub("", str(results.get("summary") or results.get("cause") or data))
    return ""


def transcript(name: str, run: dict, task: dict, model: dict) -> str:
    started = datetime.fromtimestamp(task["start_time"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [f"# Run evidence — {name} / task `{task['task_key']}`", "",
             "| field | value |", "|---|---|",
             f"| job run id | `{run['run_id']}` |", f"| task run id | `{task['run_id']}` |",
             f"| job | {run.get('run_name', '')} |", f"| notebook | `{task.get('notebook_task', {}).get('notebook_path', '')}` |",
             f"| started | {started} |", f"| result | **{task['state'].get('result_state')}** |",
             f"| run page | {task.get('run_page_url', '')} |", "",
             "Exported from the Databricks Jobs API with cell outputs as they were produced during the run.", ""]
    for i, cmd in enumerate(model.get("commands", []), start=1):
        code = (cmd.get("command") or "").strip()
        if code.startswith("%md") or not code:
            continue
        code_lines = code.splitlines()
        shown = "\n".join(code_lines[:MAX_CODE_LINES]) + (f"\n# … {len(code_lines) - MAX_CODE_LINES} more lines"
                                                          if len(code_lines) > MAX_CODE_LINES else "")
        output = render_output(cmd.get("results")).strip()
        lines += [f"## Cell {i}", "", "```python", shown, "```", ""]
        if output:
            clipped = output[:MAX_OUTPUT_CHARS] + ("\n… (truncated)" if len(output) > MAX_OUTPUT_CHARS else "")
            lines += ["**Output:**", "", "```text", clipped, "```", ""]
    return "\n".join(lines)


def export(profile: str, name: str, run_id: str, out: Path) -> list[Path]:
    run = cli(profile, "jobs", "get-run", run_id)
    written = []
    for task in run.get("tasks", []):
        exported = cli(profile, "jobs", "export-run", str(task["run_id"]), "--views-to-export", "ALL")
        html = exported["views"][0]["content"]
        stem = f"{name}_{task['task_key']}_{task['run_id']}"
        (out / f"{stem}.html").write_text(html, encoding="utf-8")
        (out / f"{stem}.md").write_text(transcript(name, run, task, notebook_model(html)), encoding="utf-8")
        written += [out / f"{stem}.md", out / f"{stem}.html"]
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--profile", required=True)
    ap.add_argument("--out", type=Path, default=Path("docs/evidence/runs"))
    ap.add_argument("--run", action="append", required=True, help="name=job_run_id (repeatable)")
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for spec in args.run:
        name, run_id = spec.split("=", 1)
        for path in export(args.profile, name, run_id, args.out):
            print(f"wrote {path}")


if __name__ == "__main__":
    main()
