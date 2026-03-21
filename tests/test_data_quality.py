"""
tests/test_data_quality.py
==========================
Data quality assertion tests for the AUS Energy Copilot medallion tables.

Tests use mock PySpark DataFrames by default (no cluster required).
Set ENERGY_COPILOT_INTEGRATION_TEST=1 to run against live Unity Catalog tables
using a real Spark session.

Test groups
-----------
TestGoldPriceTableQuality
  test_no_null_region_ids            — region_id has no nulls
  test_region_ids_valid_values       — region_id in {NSW1, QLD1, VIC1, SA1, TAS1}
  test_prices_within_nem_limits      — spot_price_aud_mwh between -1000 and 17000
  test_no_duplicate_intervals        — no duplicate (interval_datetime, region_id) pairs
  test_interval_date_matches_datetime — interval_date == CAST(interval_datetime AS DATE)

TestGoldFcasPricesQuality
  test_fcas_service_values_valid     — fcas_service in the 8-value canonical set
  test_no_negative_fcas_prices       — price_aud_mwh >= 0

TestSilverDispatchGenQuality
  test_scada_values_reasonable       — SCADAVALUE between -9999 and 99999
  test_no_future_intervals           — no interval_datetime > now + 10 minutes

TestGeneratorRegistryQuality
  test_canonical_fuel_types          — fuel_type in the canonical set
  test_no_null_duids                 — DUID is not null
  test_capacity_positive             — max_capacity_mw > 0

Run (unit):
  pytest tests/test_data_quality.py -v

Run (integration, requires Databricks cluster):
  ENERGY_COPILOT_INTEGRATION_TEST=1 pytest tests/test_data_quality.py -v
"""

from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Environment flags
# ---------------------------------------------------------------------------

IS_INTEGRATION = os.environ.get("ENERGY_COPILOT_INTEGRATION_TEST", "0") == "1"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NEM_REGIONS     = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}
NEM_PRICE_MIN   = -1000.0
NEM_PRICE_MAX   = 17000.0   # market price cap (MPC) as of 2024; using 17000 for headroom
NEM_PRICE_FLOOR = -1000.0   # market floor price

FCAS_SERVICES = {
    "RAISE6SEC",
    "RAISE60SEC",
    "RAISE5MIN",
    "RAISEREG",
    "LOWER6SEC",
    "LOWER60SEC",
    "LOWER5MIN",
    "LOWERREG",
}

SCADA_VALUE_MIN = -9999.0
SCADA_VALUE_MAX =  99999.0

CANONICAL_FUEL_TYPES = {
    "coal", "gas", "hydro", "wind", "solar_utility",
    "battery", "pumps", "liquid", "biomass", "other",
}

FUTURE_GRACE_MINUTES = 10  # allow up to 10 minutes in the future


# ---------------------------------------------------------------------------
# Mock DataFrame helpers (Spark-like interface backed by pandas)
# ---------------------------------------------------------------------------


class _MockColumn:
    """
    Minimal Spark Column-like wrapper to let assertions use df.filter(col(...))
    patterns without a real Spark session.  Only the subset of operations used
    in this test file is implemented.
    """

    def __init__(self, series: pd.Series, name: str = ""):
        self._s   = series
        self._name = name

    # Arithmetic / comparison
    def __gt__(self, other):  return _MockColumn(self._s > other)
    def __ge__(self, other):  return _MockColumn(self._s >= other)
    def __lt__(self, other):  return _MockColumn(self._s < other)
    def __le__(self, other):  return _MockColumn(self._s <= other)
    def __eq__(self, other):  return _MockColumn(self._s == other)
    def __ne__(self, other):  return _MockColumn(self._s != other)
    def __and__(self, other): return _MockColumn(self._s & other._s)
    def __or__(self, other):  return _MockColumn(self._s | other._s)
    def __invert__(self):     return _MockColumn(~self._s)

    def isNull(self):
        return _MockColumn(self._s.isna())

    def isNotNull(self):
        return _MockColumn(self._s.notna())

    def isin(self, *values):
        # Accept both isin({...}) and isin(v1, v2, ...)
        if len(values) == 1 and isinstance(values[0], (set, list, frozenset)):
            return _MockColumn(self._s.isin(values[0]))
        return _MockColumn(self._s.isin(list(values)))

    def cast(self, dtype):
        if dtype == "date" or dtype == "DateType":
            return _MockColumn(pd.to_datetime(self._s).dt.date, self._name)
        return self

    @property
    def _bool_series(self) -> pd.Series:
        return self._s.astype(bool)


class MockDataFrame:
    """
    Minimal PySpark DataFrame-like object backed by a pandas DataFrame.
    Supports filter(), select(), count(), distinct(), and the column accessor col().
    """

    def __init__(self, pandas_df: pd.DataFrame):
        self._df = pandas_df.copy().reset_index(drop=True)

    # ------------------------------------------------------------------
    # Column accessor
    # ------------------------------------------------------------------

    def col(self, name: str) -> _MockColumn:
        """Return a _MockColumn for the named column."""
        return _MockColumn(self._df[name], name)

    # Allow attribute-style access df.column_name for test ergonomics
    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._df.columns:
            return _MockColumn(self._df[name], name)
        raise AttributeError(f"Column '{name}' not found in MockDataFrame")

    # ------------------------------------------------------------------
    # PySpark-like API
    # ------------------------------------------------------------------

    def filter(self, condition: _MockColumn) -> "MockDataFrame":
        mask = condition._bool_series
        return MockDataFrame(self._df[mask])

    def count(self) -> int:
        return len(self._df)

    def select(self, *cols) -> "MockDataFrame":
        names = []
        for c in cols:
            if isinstance(c, str):
                names.append(c)
            elif isinstance(c, _MockColumn):
                names.append(c._name)
            else:
                names.append(str(c))
        return MockDataFrame(self._df[names])

    def distinct(self) -> "MockDataFrame":
        return MockDataFrame(self._df.drop_duplicates())

    def dropDuplicates(self, subset: Optional[List[str]] = None) -> "MockDataFrame":
        return MockDataFrame(self._df.drop_duplicates(subset=subset))

    def groupBy(self, *cols) -> "_MockGroupBy":
        names = [c if isinstance(c, str) else c._name for c in cols]
        return _MockGroupBy(self._df, names)

    def collect(self) -> List[Dict[str, Any]]:
        return self._df.to_dict(orient="records")

    def withColumn(self, name: str, col: _MockColumn) -> "MockDataFrame":
        new_df = self._df.copy()
        new_df[name] = col._s.values
        return MockDataFrame(new_df)

    @property
    def columns(self) -> List[str]:
        return list(self._df.columns)

    def toPandas(self) -> pd.DataFrame:
        return self._df.copy()


class _MockGroupBy:
    def __init__(self, df: pd.DataFrame, keys: List[str]):
        self._df   = df
        self._keys = keys

    def count(self) -> MockDataFrame:
        result = self._df.groupby(self._keys).size().reset_index(name="count")
        return MockDataFrame(result)


def col(name: str) -> _MockColumn:
    """Module-level col() factory — mirrors pyspark.sql.functions.col()."""
    # Returns a deferred column; when used with a specific DataFrame the
    # series will be resolved inside MockDataFrame.col().
    # For stand-alone use we return a placeholder; tests must use df.col(name).
    raise RuntimeError(
        "Use df.col(name) in tests instead of the module-level col(). "
        "This function exists for documentation only."
    )


# ---------------------------------------------------------------------------
# Mock data factories
# ---------------------------------------------------------------------------

import numpy as np

_RNG = np.random.default_rng(seed=42)


def _make_gold_price_df(
    n_rows: int = 100,
    include_nulls: bool = False,
    include_bad_region: bool = False,
    include_out_of_range_price: bool = False,
    include_duplicates: bool = False,
    include_date_mismatch: bool = False,
) -> MockDataFrame:
    """Build a mock gold.nem_prices_5min DataFrame."""
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    base    = datetime(2025, 1, 1, 0, 0, 0)

    rows = []
    for i in range(n_rows):
        region = regions[i % len(regions)]
        dt     = base + timedelta(minutes=5 * i)
        price  = float(_RNG.uniform(30, 200))
        rows.append({
            "interval_datetime":   dt,
            "interval_date":       dt.date(),
            "region_id":           region,
            "spot_price_aud_mwh":  price,
        })

    df = pd.DataFrame(rows)

    if include_nulls:
        df.loc[0, "region_id"] = None

    if include_bad_region:
        df.loc[1, "region_id"] = "INVALID_REGION"

    if include_out_of_range_price:
        df.loc[2, "spot_price_aud_mwh"] = 99_999.0  # above MPC

    if include_duplicates:
        # Add a duplicate row (same interval + region)
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)

    if include_date_mismatch:
        # Force the date column to disagree with datetime
        df.loc[3, "interval_date"] = (
            pd.Timestamp(df.loc[3, "interval_datetime"]) + pd.Timedelta(days=1)
        ).date()

    return MockDataFrame(df)


def _make_gold_fcas_df(
    include_bad_service: bool = False,
    include_negative_price: bool = False,
) -> MockDataFrame:
    """Build a mock gold.nem_fcas_prices DataFrame."""
    services = list(FCAS_SERVICES)
    regions  = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    base     = datetime(2025, 1, 1, 0, 0, 0)

    rows = []
    for i, service in enumerate(services):
        for j, region in enumerate(regions):
            dt    = base + timedelta(minutes=5 * i)
            price = float(_RNG.uniform(0, 50))
            rows.append({
                "interval_datetime": dt,
                "region_id":         region,
                "fcas_service":      service,
                "price_aud_mwh":     price,
            })

    df = pd.DataFrame(rows)

    if include_bad_service:
        df.loc[0, "fcas_service"] = "INVALID_SERVICE"

    if include_negative_price:
        df.loc[1, "price_aud_mwh"] = -5.0

    return MockDataFrame(df)


def _make_silver_dispatch_gen_df(
    include_out_of_range_scada: bool = False,
    include_future_intervals: bool = False,
) -> MockDataFrame:
    """Build a mock silver.dispatch_gen DataFrame."""
    base = datetime(2025, 1, 1, 0, 0, 0)
    rows = []
    for i in range(50):
        dt   = base + timedelta(minutes=5 * i)
        mw   = float(_RNG.uniform(0, 500))
        duid = f"DUID{i:04d}"
        rows.append({
            "SETTLEMENTDATE": dt,
            "DUID":           duid,
            "SCADAVALUE":     mw,
        })

    df = pd.DataFrame(rows)

    if include_out_of_range_scada:
        df.loc[0, "SCADAVALUE"] = 999_999.0

    if include_future_intervals:
        # Set one row to 30 minutes in the future
        far_future = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=30)
        df.loc[1, "SETTLEMENTDATE"] = far_future

    return MockDataFrame(df)


def _make_generator_registry_df(
    include_bad_fuel_type: bool = False,
    include_null_duid: bool = False,
    include_zero_capacity: bool = False,
) -> MockDataFrame:
    """Build a mock silver.generator_registry DataFrame."""
    fuel_types = list(CANONICAL_FUEL_TYPES)
    rows = []
    for i in range(30):
        ft  = fuel_types[i % len(fuel_types)]
        cap = float(_RNG.uniform(50, 1500))
        rows.append({
            "duid":             f"DUID{i:04d}",
            "station_name":     f"Station {i}",
            "fuel_type":        ft,
            "max_capacity_mw":  cap,
            "region_id":        "NSW1",
        })

    df = pd.DataFrame(rows)

    if include_bad_fuel_type:
        df.loc[0, "fuel_type"] = "UNKNOWN_FUEL"

    if include_null_duid:
        df.loc[1, "duid"] = None

    if include_zero_capacity:
        df.loc[2, "max_capacity_mw"] = 0.0

    return MockDataFrame(df)


# ---------------------------------------------------------------------------
# Integration: Spark session fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark_session():
    """Return an active Spark session (integration mode only)."""
    if not IS_INTEGRATION:
        pytest.skip("Integration test skipped (set ENERGY_COPILOT_INTEGRATION_TEST=1)")
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# TestGoldPriceTableQuality
# ---------------------------------------------------------------------------


class TestGoldPriceTableQuality(unittest.TestCase):
    """Data quality tests for gold.nem_prices_5min (or equivalent)."""

    def _get_df(self, **kwargs) -> MockDataFrame:
        return _make_gold_price_df(**kwargs)

    # ------------------------------------------------------------------
    # test_no_null_region_ids
    # ------------------------------------------------------------------

    def test_no_null_region_ids(self):
        """region_id column must contain no null values."""
        df      = self._get_df(include_nulls=False)
        null_df = df.filter(df.col("region_id").isNull())
        self.assertEqual(
            null_df.count(),
            0,
            "Expected zero null region_id values in clean dataset",
        )

    def test_no_null_region_ids_detects_nulls(self):
        """Confirms the check correctly detects nulls when they exist."""
        df      = self._get_df(include_nulls=True)
        null_df = df.filter(df.col("region_id").isNull())
        self.assertGreater(
            null_df.count(),
            0,
            "Dataset seeded with nulls should have at least one null region_id",
        )

    # ------------------------------------------------------------------
    # test_region_ids_valid_values
    # ------------------------------------------------------------------

    def test_region_ids_valid_values(self):
        """All region_id values must be in the set {NSW1, QLD1, VIC1, SA1, TAS1}."""
        df         = self._get_df(include_bad_region=False)
        invalid_df = df.filter(~df.col("region_id").isin(NEM_REGIONS))
        self.assertEqual(
            invalid_df.count(),
            0,
            "All region_ids should be valid NEM regions",
        )

    def test_region_ids_valid_values_detects_invalid(self):
        """Confirms invalid region IDs are detected."""
        df         = self._get_df(include_bad_region=True)
        invalid_df = df.filter(~df.col("region_id").isin(NEM_REGIONS))
        self.assertGreater(
            invalid_df.count(),
            0,
            "Expected at least one invalid region_id to be flagged",
        )

    # ------------------------------------------------------------------
    # test_prices_within_nem_limits
    # ------------------------------------------------------------------

    def test_prices_within_nem_limits(self):
        """spot_price_aud_mwh must be between -1000 and 17000 $/MWh."""
        df = self._get_df(include_out_of_range_price=False)
        out_of_range = df.filter(
            (df.col("spot_price_aud_mwh") < NEM_PRICE_FLOOR) |
            (df.col("spot_price_aud_mwh") > NEM_PRICE_MAX)
        )
        self.assertEqual(
            out_of_range.count(),
            0,
            "All prices should be within NEM limits [-1000, 17000]",
        )

    def test_prices_within_nem_limits_detects_violation(self):
        """Confirms out-of-range prices are detected."""
        df = self._get_df(include_out_of_range_price=True)
        out_of_range = df.filter(
            (df.col("spot_price_aud_mwh") < NEM_PRICE_FLOOR) |
            (df.col("spot_price_aud_mwh") > NEM_PRICE_MAX)
        )
        self.assertGreater(
            out_of_range.count(),
            0,
            "Expected at least one out-of-range price to be flagged",
        )

    # ------------------------------------------------------------------
    # test_no_duplicate_intervals
    # ------------------------------------------------------------------

    def test_no_duplicate_intervals(self):
        """There should be no duplicate (interval_datetime, region_id) pairs."""
        df = self._get_df(include_duplicates=False)
        # Dedup and compare counts
        total   = df.count()
        deduped = df.dropDuplicates(["interval_datetime", "region_id"]).count()
        self.assertEqual(
            total,
            deduped,
            "Duplicate (interval_datetime, region_id) pairs detected in clean dataset",
        )

    def test_no_duplicate_intervals_detects_duplicates(self):
        """Confirms the test catches duplicate (interval_datetime, region_id)."""
        df = self._get_df(include_duplicates=True)
        # Count all rows vs deduplicated
        total   = df.count()
        deduped = df.dropDuplicates(["interval_datetime", "region_id"]).count()
        self.assertGreater(
            total - deduped,
            0,
            "Expected at least one duplicate (interval_datetime, region_id) pair",
        )

    # ------------------------------------------------------------------
    # test_interval_date_matches_datetime
    # ------------------------------------------------------------------

    def test_interval_date_matches_datetime(self):
        """
        interval_date should equal CAST(interval_datetime AS DATE) for all rows.
        """
        df = self._get_df(include_date_mismatch=False)
        pandas_df = df.toPandas()
        expected_dates = pd.to_datetime(pandas_df["interval_datetime"]).dt.date
        mismatches     = (pandas_df["interval_date"] != expected_dates).sum()
        self.assertEqual(
            mismatches,
            0,
            "interval_date should match the date component of interval_datetime",
        )

    def test_interval_date_matches_datetime_detects_mismatch(self):
        """Confirms date mismatches are detected."""
        df        = self._get_df(include_date_mismatch=True)
        pandas_df = df.toPandas()
        expected_dates = pd.to_datetime(pandas_df["interval_datetime"]).dt.date
        mismatches     = (pandas_df["interval_date"] != expected_dates).sum()
        self.assertGreater(
            mismatches,
            0,
            "Expected at least one interval_date / interval_datetime mismatch",
        )


# ---------------------------------------------------------------------------
# TestGoldFcasPricesQuality
# ---------------------------------------------------------------------------


class TestGoldFcasPricesQuality(unittest.TestCase):
    """Data quality tests for gold.nem_fcas_prices."""

    def _get_df(self, **kwargs) -> MockDataFrame:
        return _make_gold_fcas_df(**kwargs)

    # ------------------------------------------------------------------
    # test_fcas_service_values_valid
    # ------------------------------------------------------------------

    def test_fcas_service_values_valid(self):
        """fcas_service must be one of the 8 canonical FCAS service names."""
        df      = self._get_df(include_bad_service=False)
        invalid = df.filter(~df.col("fcas_service").isin(FCAS_SERVICES))
        self.assertEqual(
            invalid.count(),
            0,
            f"All fcas_service values should be in {FCAS_SERVICES}",
        )

    def test_fcas_service_values_valid_detects_invalid(self):
        """Confirms invalid FCAS service names are caught."""
        df      = self._get_df(include_bad_service=True)
        invalid = df.filter(~df.col("fcas_service").isin(FCAS_SERVICES))
        self.assertGreater(
            invalid.count(),
            0,
            "Expected at least one invalid fcas_service to be flagged",
        )

    # ------------------------------------------------------------------
    # test_no_negative_fcas_prices
    # ------------------------------------------------------------------

    def test_no_negative_fcas_prices(self):
        """FCAS prices (price_aud_mwh) must be non-negative (>= 0)."""
        df       = self._get_df(include_negative_price=False)
        negative = df.filter(df.col("price_aud_mwh") < 0)
        self.assertEqual(
            negative.count(),
            0,
            "FCAS prices should never be negative",
        )

    def test_no_negative_fcas_prices_detects_negatives(self):
        """Confirms negative FCAS prices are detected."""
        df       = self._get_df(include_negative_price=True)
        negative = df.filter(df.col("price_aud_mwh") < 0)
        self.assertGreater(
            negative.count(),
            0,
            "Expected at least one negative FCAS price to be flagged",
        )


# ---------------------------------------------------------------------------
# TestSilverDispatchGenQuality
# ---------------------------------------------------------------------------


class TestSilverDispatchGenQuality(unittest.TestCase):
    """Data quality tests for silver.dispatch_gen (SCADA generation data)."""

    def _get_df(self, **kwargs) -> MockDataFrame:
        return _make_silver_dispatch_gen_df(**kwargs)

    # ------------------------------------------------------------------
    # test_scada_values_reasonable
    # ------------------------------------------------------------------

    def test_scada_values_reasonable(self):
        """SCADAVALUE must be between -9999 and 99999 MW."""
        df           = self._get_df(include_out_of_range_scada=False)
        out_of_range = df.filter(
            (df.col("SCADAVALUE") < SCADA_VALUE_MIN) |
            (df.col("SCADAVALUE") > SCADA_VALUE_MAX)
        )
        self.assertEqual(
            out_of_range.count(),
            0,
            f"SCADAVALUE should be between {SCADA_VALUE_MIN} and {SCADA_VALUE_MAX}",
        )

    def test_scada_values_reasonable_detects_outliers(self):
        """Confirms unreasonably large SCADA values are caught."""
        df           = self._get_df(include_out_of_range_scada=True)
        out_of_range = df.filter(
            (df.col("SCADAVALUE") < SCADA_VALUE_MIN) |
            (df.col("SCADAVALUE") > SCADA_VALUE_MAX)
        )
        self.assertGreater(
            out_of_range.count(),
            0,
            "Expected at least one out-of-range SCADAVALUE to be flagged",
        )

    # ------------------------------------------------------------------
    # test_no_future_intervals
    # ------------------------------------------------------------------

    def test_no_future_intervals(self):
        """
        SETTLEMENTDATE must not be more than 10 minutes in the future.
        (Allows small clock-skew in the 5-minute dispatch cycle.)
        """
        df          = self._get_df(include_future_intervals=False)
        pandas_df   = df.toPandas()
        now_utc     = datetime.now(timezone.utc).replace(tzinfo=None)
        cutoff      = now_utc + timedelta(minutes=FUTURE_GRACE_MINUTES)

        future_rows = pandas_df[
            pd.to_datetime(pandas_df["SETTLEMENTDATE"]) > pd.Timestamp(cutoff)
        ]
        self.assertEqual(
            len(future_rows),
            0,
            f"No SETTLEMENTDATE should be > now + {FUTURE_GRACE_MINUTES} minutes",
        )

    def test_no_future_intervals_detects_future_rows(self):
        """Confirms rows with far-future timestamps are flagged."""
        df          = self._get_df(include_future_intervals=True)
        pandas_df   = df.toPandas()
        now_utc     = datetime.now(timezone.utc).replace(tzinfo=None)
        cutoff      = now_utc + timedelta(minutes=FUTURE_GRACE_MINUTES)

        future_rows = pandas_df[
            pd.to_datetime(pandas_df["SETTLEMENTDATE"]) > pd.Timestamp(cutoff)
        ]
        self.assertGreater(
            len(future_rows),
            0,
            "Expected at least one future SETTLEMENTDATE to be flagged",
        )


# ---------------------------------------------------------------------------
# TestGeneratorRegistryQuality
# ---------------------------------------------------------------------------


class TestGeneratorRegistryQuality(unittest.TestCase):
    """Data quality tests for silver.generator_registry."""

    def _get_df(self, **kwargs) -> MockDataFrame:
        return _make_generator_registry_df(**kwargs)

    # ------------------------------------------------------------------
    # test_canonical_fuel_types
    # ------------------------------------------------------------------

    def test_canonical_fuel_types(self):
        """fuel_type must be a member of the canonical fuel type set."""
        df      = self._get_df(include_bad_fuel_type=False)
        invalid = df.filter(~df.col("fuel_type").isin(CANONICAL_FUEL_TYPES))
        self.assertEqual(
            invalid.count(),
            0,
            f"All fuel_type values should be in {CANONICAL_FUEL_TYPES}",
        )

    def test_canonical_fuel_types_detects_invalid(self):
        """Confirms non-canonical fuel types are caught."""
        df      = self._get_df(include_bad_fuel_type=True)
        invalid = df.filter(~df.col("fuel_type").isin(CANONICAL_FUEL_TYPES))
        self.assertGreater(
            invalid.count(),
            0,
            "Expected at least one non-canonical fuel_type to be flagged",
        )

    # ------------------------------------------------------------------
    # test_no_null_duids
    # ------------------------------------------------------------------

    def test_no_null_duids(self):
        """duid must not be null for any registry entry."""
        df       = self._get_df(include_null_duid=False)
        null_ids = df.filter(df.col("duid").isNull())
        self.assertEqual(
            null_ids.count(),
            0,
            "duid should never be null in the generator registry",
        )

    def test_no_null_duids_detects_nulls(self):
        """Confirms null duids are flagged."""
        df       = self._get_df(include_null_duid=True)
        null_ids = df.filter(df.col("duid").isNull())
        self.assertGreater(
            null_ids.count(),
            0,
            "Expected at least one null duid to be flagged",
        )

    # ------------------------------------------------------------------
    # test_capacity_positive
    # ------------------------------------------------------------------

    def test_capacity_positive(self):
        """max_capacity_mw must be strictly greater than 0 for all entries."""
        df          = self._get_df(include_zero_capacity=False)
        bad_cap     = df.filter(df.col("max_capacity_mw") <= 0)
        self.assertEqual(
            bad_cap.count(),
            0,
            "max_capacity_mw should be positive (> 0) for all generators",
        )

    def test_capacity_positive_detects_zero(self):
        """Confirms zero or negative capacity values are caught."""
        df      = self._get_df(include_zero_capacity=True)
        bad_cap = df.filter(df.col("max_capacity_mw") <= 0)
        self.assertGreater(
            bad_cap.count(),
            0,
            "Expected at least one zero/negative max_capacity_mw to be flagged",
        )


# ---------------------------------------------------------------------------
# Integration tests — only run with ENERGY_COPILOT_INTEGRATION_TEST=1
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not IS_INTEGRATION, reason="Requires live Databricks cluster")
class TestGoldPriceTableQualityIntegration(unittest.TestCase):
    """Integration versions of the Gold price quality tests."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.getOrCreate()
        cls.df    = cls.spark.table("energy_copilot.gold.nem_prices_5min")

    def test_no_null_region_ids(self):
        from pyspark.sql import functions as F
        null_count = self.df.filter(F.col("region_id").isNull()).count()
        self.assertEqual(null_count, 0, "No null region_ids in production table")

    def test_region_ids_valid_values(self):
        from pyspark.sql import functions as F
        invalid = self.df.filter(~F.col("region_id").isin(list(NEM_REGIONS))).count()
        self.assertEqual(invalid, 0)

    def test_prices_within_nem_limits(self):
        from pyspark.sql import functions as F
        oob = self.df.filter(
            (F.col("spot_price_aud_mwh") < NEM_PRICE_FLOOR) |
            (F.col("spot_price_aud_mwh") > NEM_PRICE_MAX)
        ).count()
        self.assertEqual(oob, 0)

    def test_no_duplicate_intervals(self):
        total   = self.df.count()
        deduped = self.df.dropDuplicates(["interval_datetime", "region_id"]).count()
        self.assertEqual(total, deduped, "Duplicate intervals found in production table")

    def test_interval_date_matches_datetime(self):
        from pyspark.sql import functions as F
        mismatches = self.df.filter(
            F.col("interval_date") != F.col("interval_datetime").cast("date")
        ).count()
        self.assertEqual(mismatches, 0, "interval_date disagrees with interval_datetime")


@pytest.mark.skipif(not IS_INTEGRATION, reason="Requires live Databricks cluster")
class TestGoldFcasPricesQualityIntegration(unittest.TestCase):
    """Integration versions of the Gold FCAS price quality tests."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.getOrCreate()
        cls.df    = cls.spark.table("energy_copilot.gold.nem_fcas_prices")

    def test_fcas_service_values_valid(self):
        from pyspark.sql import functions as F
        invalid = self.df.filter(
            ~F.col("fcas_service").isin(list(FCAS_SERVICES))
        ).count()
        self.assertEqual(invalid, 0)

    def test_no_negative_fcas_prices(self):
        from pyspark.sql import functions as F
        negative = self.df.filter(F.col("price_aud_mwh") < 0).count()
        self.assertEqual(negative, 0, "FCAS prices must be non-negative")


@pytest.mark.skipif(not IS_INTEGRATION, reason="Requires live Databricks cluster")
class TestSilverDispatchGenQualityIntegration(unittest.TestCase):
    """Integration versions of the Silver dispatch_gen quality tests."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.getOrCreate()
        cls.df    = cls.spark.table("energy_copilot.silver.dispatch_gen")

    def test_scada_values_reasonable(self):
        from pyspark.sql import functions as F
        oob = self.df.filter(
            (F.col("SCADAVALUE") < SCADA_VALUE_MIN) |
            (F.col("SCADAVALUE") > SCADA_VALUE_MAX)
        ).count()
        self.assertEqual(oob, 0)

    def test_no_future_intervals(self):
        from pyspark.sql import functions as F
        from pyspark.sql.functions import current_timestamp
        future_count = self.df.filter(
            F.col("SETTLEMENTDATE") > current_timestamp() + F.expr(f"INTERVAL {FUTURE_GRACE_MINUTES} MINUTES")
        ).count()
        self.assertEqual(future_count, 0, f"No intervals more than {FUTURE_GRACE_MINUTES} min in the future")


@pytest.mark.skipif(not IS_INTEGRATION, reason="Requires live Databricks cluster")
class TestGeneratorRegistryQualityIntegration(unittest.TestCase):
    """Integration versions of the generator_registry quality tests."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.getOrCreate()
        cls.df    = cls.spark.table("energy_copilot.silver.generator_registry")

    def test_canonical_fuel_types(self):
        from pyspark.sql import functions as F
        invalid = self.df.filter(
            ~F.col("fuel_type").isin(list(CANONICAL_FUEL_TYPES))
        ).count()
        self.assertEqual(invalid, 0)

    def test_no_null_duids(self):
        from pyspark.sql import functions as F
        null_duids = self.df.filter(F.col("duid").isNull()).count()
        self.assertEqual(null_duids, 0)

    def test_capacity_positive(self):
        from pyspark.sql import functions as F
        bad_cap = self.df.filter(F.col("max_capacity_mw") <= 0).count()
        self.assertEqual(bad_cap, 0, "max_capacity_mw must be > 0")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(argv=["", "-v"])
