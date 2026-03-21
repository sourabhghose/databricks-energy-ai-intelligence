"""
Shared pytest fixtures for AUS Energy Copilot.

All fixtures avoid hard-importing PySpark so the test suite remains runnable
in CI environments where PySpark is not installed.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# FastAPI TestClient fixture — shared by all test classes that use
# ``def test_*(self, client)`` style.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def client():
    """Return a synchronous FastAPI TestClient for the main app."""
    os.environ.setdefault("DATABRICKS_HOST", "https://dummy.azuredatabricks.net")
    os.environ.setdefault("DATABRICKS_TOKEN", "dapi-dummy-token")
    os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", "dummy-warehouse-id")
    os.environ.setdefault("DATABRICKS_CATALOG", "energy_copilot")
    os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-dummy-key")

    from fastapi.testclient import TestClient

    from app.backend.main import app  # noqa: E402

    with TestClient(app) as c:
        yield c

# ---------------------------------------------------------------------------
# Integration-test gate
# ---------------------------------------------------------------------------
# Import this constant in any test module that has slow/expensive integration tests:
#   from tests.conftest import IS_INTEGRATION
#   pytestmark = pytest.mark.skipif(not IS_INTEGRATION, reason="integration only")
IS_INTEGRATION = bool(os.environ.get("ENERGY_COPILOT_INTEGRATION_TEST"))


# ---------------------------------------------------------------------------
# Autouse fixture: inject dummy environment variables for every test
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def databricks_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set dummy Databricks and Anthropic credentials for all tests.

    This prevents ``ImportError`` or ``ValueError`` in modules that read
    these variables at import / class-init time (e.g. db.py).
    """
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token-000000000000")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key-00000000000000")


# ---------------------------------------------------------------------------
# PySpark mock fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_spark() -> MagicMock:
    """Return a MagicMock that quacks like a SparkSession.

    The mock provides the three most commonly used SparkSession methods:
    - ``.table(name)``        → returns a MagicMock DataFrame
    - ``.sql(query)``         → returns a MagicMock DataFrame
    - ``.createDataFrame()``  → returns a MagicMock DataFrame

    Tests that need richer behaviour can configure the returned mock
    further, e.g.::

        mock_spark.table.return_value.count.return_value = 42

    The fixture does **not** import PySpark; a ``try/except ImportError``
    guard is applied so the fixture is always available.
    """
    try:
        # Attempt to import to make type-checking tools happy; not required.
        from pyspark.sql import SparkSession as _SparkSession  # noqa: F401
    except ImportError:
        pass

    spark = MagicMock(name="SparkSession")

    # Each of these methods returns a fresh DataFrame-like MagicMock so
    # callers can chain further operations (e.g. .filter(), .count(), etc.)
    _mock_df = MagicMock(name="DataFrame")
    spark.table.return_value = _mock_df
    spark.sql.return_value = _mock_df
    spark.createDataFrame.return_value = _mock_df

    return spark


# ---------------------------------------------------------------------------
# Anthropic client mock fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def auth_headers() -> dict:
    """Return headers dict with a dummy API key.

    When ENERGY_COPILOT_API_KEY is not set (the default in tests) the
    verify_api_key dependency is a no-op, so any key value works.  Tests
    that use this fixture will still pass in both auth-enabled and
    auth-disabled configurations.
    """
    return {"X-API-Key": "test-key"}


@pytest.fixture()
def mock_anthropic_client() -> MagicMock:
    """Return a MagicMock that quacks like an ``anthropic.Anthropic`` client.

    The mock pre-configures ``client.messages.create()`` to return a
    response object whose ``.content`` list contains a single
    ``MagicMock`` with ``.text = "Test response"``.

    Usage example::

        def test_something(mock_anthropic_client, monkeypatch):
            monkeypatch.setattr("agent.copilot_agent.anthropic.Anthropic",
                                lambda **kw: mock_anthropic_client)
            ...
    """
    try:
        import anthropic as _anthropic  # noqa: F401
    except ImportError:
        pass

    client = MagicMock(name="AnthropicClient")

    # Build the mock response object that messages.create() returns.
    _content_block = MagicMock(name="TextBlock")
    _content_block.text = "Test response"

    _mock_response = MagicMock(name="Message")
    _mock_response.content = [_content_block]
    _mock_response.stop_reason = "end_turn"
    _mock_response.usage = MagicMock(input_tokens=10, output_tokens=5)

    client.messages.create.return_value = _mock_response

    return client
