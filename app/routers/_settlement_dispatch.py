"""Settlement tool handlers extracted to avoid stale-bytecode scoping issues."""
import json
from .shared import _query_gold, _CATALOG


def dispatch(name: str, arguments: dict) -> str:
    """Handle get_settlement_runs, get_settlement_trueup, get_constraint_forecast."""
    if name == "get_settlement_runs":
        run_type = arguments.get("run_type") or ""
        status = arguments.get("status") or ""
        region = arguments.get("region") or ""
        billing_period = arguments.get("billing_period") or ""
        rows = _query_gold(
            f"SELECT {_CATALOG}.ai_tools.get_settlement_runs("
            f"'{run_type}', '{status}', '{region}', '{billing_period}') AS result"
        )
        if rows and rows[0].get("result"):
            return rows[0]["result"]
        return json.dumps({"runs": []})

    elif name == "get_settlement_trueup":
        threshold = float(arguments.get("threshold_aud", 5000))
        rows = _query_gold(
            f"SELECT {_CATALOG}.ai_tools.get_settlement_trueup({threshold}) AS result"
        )
        if rows and rows[0].get("result"):
            return rows[0]["result"]
        return json.dumps({"material_runs": []})

    elif name == "get_constraint_forecast":
        region = arguments.get("region", "NSW1")
        days = int(arguments.get("days", 7))
        rows = _query_gold(
            f"SELECT {_CATALOG}.ai_tools.get_constraint_forecast('{region}', {days}) AS result"
        )
        if rows and rows[0].get("result"):
            return rows[0]["result"]
        return json.dumps({"message": f"No constraint data for {region} in last {days} days"})

    return json.dumps({"error": f"Unknown settlement tool: {name}"})
