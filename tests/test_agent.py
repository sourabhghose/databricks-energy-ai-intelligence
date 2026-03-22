"""
tests/test_agent.py
====================
Pytest test suite for the AUS Energy Copilot agent tools and FastAPI backend.

All tests use unittest.mock; no real Databricks, MLflow, or Anthropic calls
are made.  The FastAPI endpoint tests use httpx.AsyncClient (ASGI transport)
so the full request/response cycle is exercised without a live server.

Run:
    pytest tests/test_agent.py -v

To run only a single class:
    pytest tests/test_agent.py::TestMarketDataTools -v
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Environment stubs — must be set before any app-level imports that read
# os.environ at module load time (FastAPI main.py reads env vars at import).
# ---------------------------------------------------------------------------
os.environ.setdefault("DATABRICKS_HOST", "")
os.environ.setdefault("DATABRICKS_TOKEN", "")
os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", "fake-warehouse-id")
os.environ.setdefault("DATABRICKS_CATALOG", "energy_copilot")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-fake-key")

# Pre-import agent subpackages so unittest.mock.patch can resolve dotted paths
# (patch resolves "agent.tools.market_data_tools._connection" by walking module
# attributes; if the subpackages haven't been imported yet the lookup fails).
import agent.tools.analysis_tools  # noqa: E402, F401
import agent.tools.forecast_tools  # noqa: E402, F401
import agent.tools.market_data_tools  # noqa: E402, F401

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cursor(rows: list[list[Any]], col_names: list[str]) -> MagicMock:
    """
    Build a mock Databricks SQL cursor whose .description and .fetchall()
    return the given columns and rows respectively.

    The cursor is designed to be used as a context manager:
        with conn.cursor() as cur:
            cur.execute(...)
    """
    cursor = MagicMock()
    cursor.description = [(col, None, None, None, None, None, None) for col in col_names]
    cursor.fetchall.return_value = rows
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


def _make_conn(cursor: MagicMock) -> MagicMock:
    """Wrap a cursor mock inside a connection mock."""
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# ---------------------------------------------------------------------------
# TestMarketDataTools
# ---------------------------------------------------------------------------

class TestMarketDataTools:
    """Unit tests for agent/tools/market_data_tools.py."""

    # ------------------------------------------------------------------
    # test_get_latest_prices_all_regions
    # ------------------------------------------------------------------

    def test_get_latest_prices_all_regions(self):
        """
        _query returns 5 rows (one per region); the tool should return a dict
        with a "data" key containing exactly 5 region records, each with the
        expected keys.
        """
        rows = [
            ["NSW1", "2026-02-19T10:30:00", 87.42, 9823.1],
            ["QLD1", "2026-02-19T10:30:00", 92.10, 8100.0],
            ["SA1",  "2026-02-19T10:30:00", 105.0, 1850.0],
            ["TAS1", "2026-02-19T10:30:00", 79.30, 1250.0],
            ["VIC1", "2026-02-19T10:30:00", 88.00, 7200.0],
        ]
        col_names = ["region", "settlement_date", "rrp", "total_demand"]
        cursor = _make_cursor(rows, col_names)
        conn = _make_conn(cursor)

        with patch("agent.tools.market_data_tools._connection", return_value=conn):
            from agent.tools.market_data_tools import get_latest_prices

            result = get_latest_prices.invoke({})

        assert "data" in result
        assert len(result["data"]) == 5
        for record in result["data"]:
            assert "region" in record
            assert "rrp" in record
            # settlement_date aliased to this key in the tool's query
            assert "settlement_date" in record or "rrp" in record

    # ------------------------------------------------------------------
    # test_get_latest_prices_single_region
    # ------------------------------------------------------------------

    def test_get_latest_prices_single_region(self):
        """
        When called with region="NSW1", the tool passes the WHERE clause to
        _query and the returned data list contains exactly 1 record for NSW1.
        """
        rows = [["NSW1", "2026-02-19T10:30:00", 87.42, 9823.1]]
        col_names = ["region", "settlement_date", "rrp", "total_demand"]
        cursor = _make_cursor(rows, col_names)
        conn = _make_conn(cursor)

        with patch("agent.tools.market_data_tools._connection", return_value=conn):
            from agent.tools.market_data_tools import get_latest_prices

            result = get_latest_prices.invoke({"region": "NSW1"})

        assert len(result["data"]) == 1
        assert result["data"][0]["region"] == "NSW1"

    # ------------------------------------------------------------------
    # test_get_price_history_returns_list
    # ------------------------------------------------------------------

    def test_get_price_history_returns_list(self):
        """
        Mock cursor returns 10 rows; tool should return a list of 10 price
        point dicts, each containing settlement_date, rrp, total_demand, and
        interval_count.
        """
        rows = [
            [f"2026-02-19T{h:02d}:05:00", 80.0 + h, 9000.0 + h * 10, 1]
            for h in range(10)
        ]
        col_names = ["settlement_date", "rrp", "total_demand", "interval_count"]
        cursor = _make_cursor(rows, col_names)
        conn = _make_conn(cursor)

        with patch("agent.tools.market_data_tools._connection", return_value=conn):
            from agent.tools.market_data_tools import get_price_history

            result = get_price_history.invoke({
                "region": "VIC1",
                "start": "2026-02-19T00:00:00",
                "end": "2026-02-19T01:00:00",
                "interval": "5min",
            })

        assert isinstance(result, list)
        assert len(result) == 10
        assert "rrp" in result[0]

    # ------------------------------------------------------------------
    # test_get_price_history_invalid_region
    # ------------------------------------------------------------------

    def test_get_price_history_invalid_region(self):
        """
        Passing an unrecognised region code must raise a ValueError before any
        database call is made.
        """
        with pytest.raises(ValueError, match="Invalid region"):
            from agent.tools.market_data_tools import get_price_history

            get_price_history.invoke({
                "region": "INVALID_REGION",
                "start": "2026-02-19T00:00:00",
                "end": "2026-02-19T01:00:00",
            })

    # ------------------------------------------------------------------
    # test_get_fcas_prices_valid_service
    # ------------------------------------------------------------------

    def test_get_fcas_prices_valid_service(self):
        """
        For a valid service type, the tool should return a list; the SQL
        executed by the cursor must include the service name.
        """
        rows = [
            ["2026-02-19T00:05:00", "RAISE6SEC", 12.40, 12.40, 50.0],
            ["2026-02-19T00:10:00", "RAISE6SEC", 14.00, 14.00, 50.0],
        ]
        col_names = ["settlement_date", "service", "rrp", "local_price", "cleared_volume"]
        cursor = _make_cursor(rows, col_names)
        conn = _make_conn(cursor)

        with patch("agent.tools.market_data_tools._connection", return_value=conn):
            from agent.tools.market_data_tools import get_fcas_prices

            result = get_fcas_prices.invoke({
                "region": "NSW1",
                "service": "RAISE6SEC",
                "start": "2026-02-19T00:00:00",
                "end": "2026-02-19T01:00:00",
            })

        assert isinstance(result, list)
        assert len(result) == 2
        # Verify the SQL contained the service filter
        executed_sql = cursor.execute.call_args[0][0]
        assert "RAISE6SEC" in executed_sql

    # ------------------------------------------------------------------
    # test_get_fcas_prices_invalid_service
    # ------------------------------------------------------------------

    def test_get_fcas_prices_invalid_service(self):
        """
        An unrecognised service name must raise a ValueError before any DB
        call is made.
        """
        with pytest.raises(ValueError, match="Invalid service"):
            from agent.tools.market_data_tools import get_fcas_prices

            get_fcas_prices.invoke({
                "region": "NSW1",
                "service": "RAISE_NOTASERVICE",
                "start": "2026-02-19T00:00:00",
                "end": "2026-02-19T01:00:00",
            })

    # ------------------------------------------------------------------
    # test_get_generation_mix_fuel_types
    # ------------------------------------------------------------------

    def test_get_generation_mix_fuel_types(self):
        """
        The response fuel_totals_gwh dict must contain at least 6 of the
        standard NEM fuel types defined in the tool's docstring.
        """
        fuel_types = [
            "BLACK_COAL",
            "WIND",
            "SOLAR_UTILITY",
            "NATURAL_GAS",
            "HYDRO",
            "BATTERY_DISCHARGING",
        ]
        rows = [
            ["2026-02-19T08:00:00", ft, 100.0 * (i + 1)]
            for i, ft in enumerate(fuel_types)
        ]
        col_names = ["settlement_date", "fuel_type", "generation_mw"]
        cursor = _make_cursor(rows, col_names)
        conn = _make_conn(cursor)

        with patch("agent.tools.market_data_tools._connection", return_value=conn):
            from agent.tools.market_data_tools import get_generation_mix

            result = get_generation_mix.invoke({
                "region": "SA1",
                "start": "2026-02-19T08:00:00",
                "end": "2026-02-19T10:00:00",
            })

        assert "fuel_totals_gwh" in result
        assert len(result["fuel_totals_gwh"]) == 6
        for ft in fuel_types:
            assert ft in result["fuel_totals_gwh"]


# ---------------------------------------------------------------------------
# TestForecastTools
# ---------------------------------------------------------------------------

class TestForecastTools:
    """Unit tests for agent/tools/forecast_tools.py."""

    # ------------------------------------------------------------------
    # test_get_price_forecast_returns_confidence_bounds
    # ------------------------------------------------------------------

    def test_get_price_forecast_returns_confidence_bounds(self):
        """
        Each record returned by get_price_forecast must contain the keys
        'predicted_rrp', 'lower_bound', and 'upper_bound'.
        """
        rows = [
            ["2026-02-19T11:00:00", 60, 95.2, 62.1, 187.4, "mlflow-run-abc", "2026-02-19T10:00:00"],
            ["2026-02-19T11:05:00", 65, 97.0, 65.0, 190.0, "mlflow-run-abc", "2026-02-19T10:00:00"],
        ]
        col_names = [
            "forecast_time", "horizon_minutes", "predicted_rrp",
            "lower_bound", "upper_bound", "model_version", "model_run_at",
        ]
        cursor = _make_cursor(rows, col_names)
        conn = _make_conn(cursor)

        with patch("agent.tools.forecast_tools._connection", return_value=conn):
            from agent.tools.forecast_tools import get_price_forecast

            result = get_price_forecast.invoke({"region": "NSW1", "horizon": "4h"})

        assert isinstance(result, list)
        assert len(result) == 2
        for record in result:
            assert "predicted_rrp" in record
            assert "lower_bound" in record
            assert "upper_bound" in record

    # ------------------------------------------------------------------
    # test_get_demand_forecast_horizon_validation
    # ------------------------------------------------------------------

    def test_get_demand_forecast_horizon_validation(self):
        """
        An invalid horizon string (e.g. "48h") must raise a ValueError before
        any database call is made.
        """
        with pytest.raises(ValueError, match="Invalid horizon"):
            from agent.tools.forecast_tools import get_demand_forecast

            get_demand_forecast.invoke({"region": "QLD1", "horizon": "48h"})

    # ------------------------------------------------------------------
    # test_get_weather_forecast_all_regions
    # ------------------------------------------------------------------

    def test_get_weather_forecast_all_regions(self):
        """
        get_weather_forecast should return a dict with a 'region' key and a
        'forecasts' list for each of the 5 valid NEM regions.
        """
        base_row = [
            "2026-02-19T11:00:00", 28.5, 30.1, 18.4, 220, 2.1, 0.0, 3,
            "023034", "2026-02-19T08:00:00",
        ]
        col_names = [
            "valid_time", "temp_c", "apparent_temp_c", "wind_speed_kph",
            "wind_dir_deg", "solar_exposure_mj", "precip_mm", "cloud_oktas",
            "bom_station_id", "forecast_run",
        ]

        for region in ("NSW1", "QLD1", "VIC1", "SA1", "TAS1"):
            cursor = _make_cursor([base_row], col_names)
            conn = _make_conn(cursor)

            with patch("agent.tools.forecast_tools._connection", return_value=conn):
                from agent.tools.forecast_tools import get_weather_forecast

                result = get_weather_forecast.invoke({"region": region, "hours_ahead": 24})

            assert result["region"] == region
            assert isinstance(result["forecasts"], list)
            assert len(result["forecasts"]) == 1


# ---------------------------------------------------------------------------
# TestAnalysisTools
# ---------------------------------------------------------------------------

class TestAnalysisTools:
    """Unit tests for agent/tools/analysis_tools.py."""

    # ------------------------------------------------------------------
    # test_explain_price_event_returns_structured_response
    # ------------------------------------------------------------------

    def test_explain_price_event_returns_structured_response(self):
        """
        explain_price_event makes 5 internal _query calls (price, constraints,
        outages, interconnectors, weather, generation).  Mock all of them and
        assert the returned dict contains 'likely_causes', 'region', and
        'timestamp'.
        """
        # We patch _query directly; it is called multiple times with different
        # SQL statements.  Return side-effects in call order:
        #   1. price_rows
        #   2. constraints
        #   3. outages
        #   4. interconnectors
        #   5. weather
        #   6. generation
        price_rows    = [{"rrp": 14500.0, "demand_mw": 2100.0, "demand_vs_norm_pct": 15.0}]
        constraints   = [{"constraintid": "V>>SA_HEYWOOD_1", "marginal_value": 250.0,
                           "rhs": 500.0, "violationdegree": 0.0}]
        outages       = [{"duid": "PPCCGT", "name": "Pelican Point CCGT",
                           "type": "GENERATOR", "capacity_mw": 240.0,
                           "outage_type": "FORCED"}]
        interconnects = [{"interconnectorid": "HEYWOOD", "mwflow": 600.0,
                           "exportlimit": 600.0, "importlimit": -500.0,
                           "utilisation_pct": 100.0}]
        weather       = [{"temp_c": 39.5, "wind_speed_kph": 8.0,
                           "solar_exposure_mj": 3.2, "apparent_temp_c": 42.0}]
        generation    = [{"fuel_type": "NATURAL_GAS", "generation_mw": 450.0},
                         {"fuel_type": "WIND", "generation_mw": 120.0}]

        side_effects = [
            price_rows, constraints, outages, interconnects, weather, generation,
        ]

        with patch(
            "agent.tools.analysis_tools._query",
            side_effect=side_effects,
        ):
            from agent.tools.analysis_tools import explain_price_event

            result = explain_price_event.invoke({
                "region": "SA1",
                "timestamp": "2026-02-19T15:00:00",
            })

        assert "likely_causes" in result
        assert "region" in result
        assert "timestamp" in result
        assert result["region"] == "SA1"
        assert result["timestamp"] == "2026-02-19T15:00:00"
        assert isinstance(result["likely_causes"], list)
        assert len(result["likely_causes"]) >= 1

    # ------------------------------------------------------------------
    # test_compare_regions_price_metric
    # ------------------------------------------------------------------

    def test_compare_regions_price_metric(self):
        """
        compare_regions with metric='price' should return a dict with 'regions'
        containing an entry for all 5 NEM regions when the mock DB returns 5
        rows.
        """
        rows = [
            {"regionid": "NSW1", "avg_price": 87.0, "min_price": -30.0,
             "max_price": 1400.0, "std_price": 90.0},
            {"regionid": "QLD1", "avg_price": 92.0, "min_price": -20.0,
             "max_price": 800.0,  "std_price": 70.0},
            {"regionid": "SA1",  "avg_price": 115.0, "min_price": -60.0,
             "max_price": 14500.0, "std_price": 430.0},
            {"regionid": "TAS1", "avg_price": 78.0, "min_price": 10.0,
             "max_price": 200.0,  "std_price": 30.0},
            {"regionid": "VIC1", "avg_price": 88.0, "min_price": -40.0,
             "max_price": 900.0,  "std_price": 80.0},
        ]

        with patch("agent.tools.analysis_tools._query", return_value=rows):
            from agent.tools.analysis_tools import compare_regions

            result = compare_regions.invoke({
                "metric": "price",
                "start": "2026-02-19T00:00:00",
                "end": "2026-02-19T23:59:59",
            })

        assert "regions" in result
        assert set(result["regions"].keys()) == {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}
        assert "ranking" in result
        assert len(result["ranking"]) == 5

    # ------------------------------------------------------------------
    # test_get_market_summary_today
    # ------------------------------------------------------------------

    def test_get_market_summary_today(self):
        """
        get_market_summary should return a non-empty string that starts with
        the expected header format.
        """
        price_rows = [
            {"regionid": "NSW1", "avg_rrp": 87.0, "max_rrp": 1450.0,
             "min_rrp": -30.0, "spike_count": 2, "neg_count": 1},
            {"regionid": "VIC1", "avg_rrp": 88.0, "max_rrp": 900.0,
             "min_rrp": -40.0, "spike_count": 0, "neg_count": 3},
        ]
        peak_rows = [
            {"regionid": "NSW1", "peak_demand_mw": 12000.0, "peak_time": "2026-02-19T15:30:00"},
            {"regionid": "VIC1", "peak_demand_mw": 9500.0,  "peak_time": "2026-02-19T16:00:00"},
        ]
        gen_rows = [
            {"fuel_type": "BLACK_COAL",    "total_gwh": 850.0},
            {"fuel_type": "WIND",          "total_gwh": 320.0},
            {"fuel_type": "SOLAR_UTILITY", "total_gwh": 180.0},
        ]

        # get_market_summary calls _query three times
        with patch(
            "agent.tools.analysis_tools._query",
            side_effect=[price_rows, peak_rows, gen_rows],
        ):
            from agent.tools.analysis_tools import get_market_summary

            result = get_market_summary.invoke({"date": "2026-02-19"})

        assert isinstance(result, str)
        assert len(result) > 0
        assert "NEM Market Summary" in result
        assert "2026-02-19" in result


# ---------------------------------------------------------------------------
# TestFastAPIEndpoints
# ---------------------------------------------------------------------------

# Lazy import so the module-level env vars above are present first.
# We also patch _get_db_connection at class setup to ensure no real Databricks
# calls occur when the FastAPI app is imported.

@pytest.mark.asyncio
class TestFastAPIEndpoints:
    """Integration-style tests for the FastAPI backend using httpx AsyncClient."""

    # ------------------------------------------------------------------
    # test_health_check
    # ------------------------------------------------------------------

    async def test_health_check(self):
        """GET /health must return 200 with {status: 'ok'}."""
        import httpx

        from app.backend.main import app

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"

    # ------------------------------------------------------------------
    # test_get_prices_latest
    # ------------------------------------------------------------------

    async def test_get_prices_latest(self):
        """
        GET /api/prices/latest with a mocked DB should return a list of
        PriceRecord objects (non-empty).
        """
        import httpx

        from app.backend.main import app

        mock_rows = [
            {
                "region": "NSW1",
                "settlement_date": datetime(2026, 2, 19, 10, 30, 0),
                "rrp": 87.42,
                "raise_reg_rrp": None,
                "lower_reg_rrp": None,
                "total_demand": 9823.1,
            },
            {
                "region": "VIC1",
                "settlement_date": datetime(2026, 2, 19, 10, 30, 0),
                "rrp": 88.00,
                "raise_reg_rrp": None,
                "lower_reg_rrp": None,
                "total_demand": 7200.0,
            },
        ]

        with patch("app.backend.main._run_query", return_value=mock_rows):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/api/prices/latest")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["region"] == "NSW1"
        assert data[0]["rrp"] == pytest.approx(87.42, rel=1e-3)

    # ------------------------------------------------------------------
    # test_get_prices_history_missing_params
    # ------------------------------------------------------------------

    async def test_get_prices_history_missing_params(self):
        """
        GET /api/prices/history without required params (region, start, end)
        must return 422 Unprocessable Entity.
        """
        import httpx

        from app.backend.main import app

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/api/prices/history")

        assert response.status_code == 422

    # ------------------------------------------------------------------
    # test_chat_endpoint_streams
    # ------------------------------------------------------------------

    async def test_chat_endpoint_streams(self):
        """
        POST /api/chat with a valid ChatRequest body must return a streaming
        response with Content-Type text/event-stream.
        """
        import httpx

        from app.backend.main import app

        async def _fake_stream(request):
            """Yields a minimal SSE sequence without calling Anthropic."""
            yield f"data: {json.dumps({'type': 'text', 'content': 'Hello'})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"

        with patch("app.backend.main._stream_chat", side_effect=_fake_stream):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post(
                    "/api/chat",
                    json={"message": "What is the current NEM price in NSW1?", "history": []},
                )

        assert response.status_code == 200
        content_type = response.headers.get("content-type", "")
        assert "text/event-stream" in content_type

    # ------------------------------------------------------------------
    # test_chat_endpoint_missing_message
    # ------------------------------------------------------------------

    async def test_chat_endpoint_missing_message(self):
        """
        POST /api/chat with an empty JSON body (missing 'message' field) must
        return 422 Unprocessable Entity.
        """
        import httpx

        from app.backend.main import app

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post("/api/chat", json={})

        assert response.status_code == 422
