"""
tests/test_pipelines.py
=======================
Unit tests for the NEMWEB downloader and related pipeline utilities.

Tests are designed to run without a live Databricks cluster or internet
connection using unittest.mock.

Test groups
-----------
TestNemwebDownloader
  test_list_directory_parses_zip_links     — HTML with ZIP anchors yields correct filenames
  test_list_directory_filters_non_zip      — Non-ZIP hrefs are excluded
  test_download_and_extract_success        — Valid in-memory ZIP is extracted; mark_processed called
  test_download_and_extract_bad_zip        — Corrupt bytes return False without raising
  test_run_once_skips_processed            — Already-processed files are not re-downloaded
  test_backoff_retries                     — ConnectionError retried; 5th attempt succeeds

TestProcessedFilesTracker
  test_is_processed_returns_false_for_unknown  — Fresh tracker returns False for any filename
  test_mark_processed_adds_to_cache            — mark_processed causes is_processed to return True
  test_load_without_spark_uses_empty_cache     — No Spark session → loads without error, empty cache

Run:
  pytest tests/test_pipelines.py -v
"""

from __future__ import annotations

import io
import json
import sys
import unittest
import zipfile
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Ensure the project root is on the path so we can import the pipelines module
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Import the module under test — adjust path if needed
# pipelines/nemweb_downloader.py lives at project root / pipelines /
_PIPELINES_DIR = _PROJECT_ROOT / "pipelines"
if str(_PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(_PIPELINES_DIR))

import nemweb_downloader as nd
from nemweb_downloader import (
    NemwebDownloader,
    ProcessedFilesTracker,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_html_with_links(hrefs: List[str]) -> str:
    """Build minimal HTML containing anchor tags for the given hrefs."""
    anchors = "\n".join(f'<a href="{h}">{h}</a>' for h in hrefs)
    return f"<html><body>{anchors}</body></html>"


def _make_zip_bytes(csv_filename: str = "data.csv", csv_content: str = "a,b\n1,2\n") -> bytes:
    """Return raw bytes of a valid in-memory ZIP containing one CSV file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_filename, csv_content)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# TestNemwebDownloader
# ---------------------------------------------------------------------------


class TestNemwebDownloader(unittest.TestCase):
    """Unit tests for NemwebDownloader using mocked HTTP responses."""

    def _make_downloader(self, spark=None, tmp_path=None) -> NemwebDownloader:
        """Return a NemwebDownloader wired to a temp directory."""
        path = Path(tmp_path) if tmp_path else Path("/tmp/test_nemweb")
        d = NemwebDownloader(spark=spark, volume_base_path=path, poll_interval=0)
        return d

    # ------------------------------------------------------------------
    # test_list_directory_parses_zip_links
    # ------------------------------------------------------------------

    def test_list_directory_parses_zip_links(self):
        """
        _list_directory should return the base filenames of all ZIP hrefs
        found in the HTML response body.
        """
        zip_names = [
            "PUBLIC_DISPATCHSCADA_202501011200_0000000001234567.ZIP",
            "PUBLIC_DISPATCHSCADA_202501011205_0000000001234568.ZIP",
        ]
        # HTML has both absolute paths and bare names
        hrefs = [f"/REPORTS/CURRENT/Dispatch_SCADA/{z}" for z in zip_names]
        html  = _make_html_with_links(hrefs)

        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status = MagicMock()

        downloader = self._make_downloader()

        with patch.object(downloader, "_get_with_backoff", return_value=mock_response):
            result = downloader._list_directory("https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/")

        self.assertEqual(
            sorted(result),
            sorted(zip_names),
            msg="Expected exactly the ZIP filenames from the HTML links",
        )

    # ------------------------------------------------------------------
    # test_list_directory_filters_non_zip
    # ------------------------------------------------------------------

    def test_list_directory_filters_non_zip(self):
        """
        Non-ZIP hrefs (HTML, TXT, directories) must not appear in the result.
        """
        hrefs = [
            "PUBLIC_DISPATCHSCADA_202501011200_0000000001234567.ZIP",  # keep
            "index.html",                                               # skip
            "README.txt",                                               # skip
            "PUBLIC_DISPATCHSCADA_202501011205_0000000001234568.ZIP",  # keep
            "/some/path/",                                              # skip (directory)
        ]
        html = _make_html_with_links(hrefs)

        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status = MagicMock()

        downloader = self._make_downloader()
        with patch.object(downloader, "_get_with_backoff", return_value=mock_response):
            result = downloader._list_directory("https://example.com/dir/")

        expected = {
            "PUBLIC_DISPATCHSCADA_202501011200_0000000001234567.ZIP",
            "PUBLIC_DISPATCHSCADA_202501011205_0000000001234568.ZIP",
        }
        self.assertEqual(
            set(result),
            expected,
            msg=f"Expected only ZIP files; got {result}",
        )

    # ------------------------------------------------------------------
    # test_download_and_extract_success
    # ------------------------------------------------------------------

    def test_download_and_extract_success(self):
        """
        A valid ZIP response should be extracted and mark_processed should be
        called exactly once with the correct filename.
        """
        import tempfile

        zip_filename = "PUBLIC_DISPATCHSCADA_202501011200_0000000001234567.ZIP"
        csv_content  = "I,DISPATCH,UNIT_SCADA,1\nD,DUID,SCADAVALUE\nD,BW01,200.5\n"
        zip_bytes    = _make_zip_bytes("dispatch_scada.csv", csv_content)

        mock_response        = MagicMock()
        mock_response.content = zip_bytes
        mock_response.raise_for_status = MagicMock()

        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = self._make_downloader(tmp_path=tmpdir)

            with patch.object(downloader, "_get_with_backoff", return_value=mock_response):
                with patch.object(downloader._tracker, "mark_processed") as mock_mark:
                    success = downloader._download_and_extract(
                        zip_url=f"https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/{zip_filename}",
                        filename=zip_filename,
                        report_type="Dispatch_SCADA",
                    )

        self.assertTrue(success, "Expected _download_and_extract to return True on success")
        mock_mark.assert_called_once()
        call_args = mock_mark.call_args
        self.assertEqual(
            call_args.args[0],
            zip_filename,
            msg="mark_processed should be called with the ZIP filename",
        )

    # ------------------------------------------------------------------
    # test_download_and_extract_bad_zip
    # ------------------------------------------------------------------

    def test_download_and_extract_bad_zip(self):
        """
        Corrupt bytes (not a valid ZIP) should cause _download_and_extract to
        return False without raising an exception.
        """
        corrupt_bytes = b"this is not a zip file at all GARBAGE"

        mock_response         = MagicMock()
        mock_response.content = corrupt_bytes
        mock_response.raise_for_status = MagicMock()

        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = self._make_downloader(tmp_path=tmpdir)
            with patch.object(downloader, "_get_with_backoff", return_value=mock_response):
                try:
                    result = downloader._download_and_extract(
                        zip_url="https://www.nemweb.com.au/test.ZIP",
                        filename="test.ZIP",
                        report_type="Dispatch_SCADA",
                    )
                except Exception as exc:
                    self.fail(
                        f"_download_and_extract raised an unexpected exception: {exc}"
                    )

        self.assertFalse(result, "Expected False for corrupt ZIP bytes")

    # ------------------------------------------------------------------
    # test_run_once_skips_processed
    # ------------------------------------------------------------------

    def test_run_once_skips_processed(self):
        """
        When the tracker reports a file as already processed, _download_and_extract
        must never be called for that file.
        """
        zip_filename = "PUBLIC_DISPATCHSCADA_202501011200_0000000001234567.ZIP"
        html         = _make_html_with_links([zip_filename])

        mock_list_resp = MagicMock()
        mock_list_resp.text = html
        mock_list_resp.raise_for_status = MagicMock()

        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = self._make_downloader(tmp_path=tmpdir)

            # Mark the file as already processed in the tracker
            downloader._tracker._cache.add(zip_filename)
            downloader._tracker._loaded = True

            with patch.object(downloader, "_get_with_backoff", return_value=mock_list_resp):
                with patch.object(
                    downloader, "_download_and_extract"
                ) as mock_download:
                    downloader._process_directory(
                        "Dispatch_SCADA",
                        "https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
                        [r"^PUBLIC_DISPATCHSCADA_\d{12}_\d{16}\.ZIP$"],
                    )

        mock_download.assert_not_called()

    # ------------------------------------------------------------------
    # test_backoff_retries
    # ------------------------------------------------------------------

    def test_backoff_retries(self):
        """
        _get_with_backoff should retry on ConnectionError and succeed on the
        5th call (after 4 failures).  Exactly 5 total calls to session.get
        should be made.
        """
        import requests as req_lib

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        # Patch time.sleep to avoid actual delays during testing
        with patch("time.sleep"):
            downloader = self._make_downloader()

            # Build side_effect: fail 4 times, succeed on 5th
            side_effects = [req_lib.exceptions.ConnectionError("timeout")] * 4 + [mock_response]
            downloader._session.get = MagicMock(side_effect=side_effects)

            result = downloader._get_with_backoff("https://www.nemweb.com.au/test")

        self.assertEqual(
            downloader._session.get.call_count,
            5,
            msg="Expected exactly 5 total calls (4 failures + 1 success)",
        )
        self.assertIs(result, mock_response)

    def test_backoff_exhausted_raises(self):
        """
        When all retries are exhausted, _get_with_backoff must raise RuntimeError.
        """
        import requests as req_lib

        with patch("time.sleep"):
            downloader = self._make_downloader()
            downloader._session.get = MagicMock(
                side_effect=req_lib.exceptions.ConnectionError("always fails")
            )
            with self.assertRaises(RuntimeError):
                downloader._get_with_backoff("https://www.nemweb.com.au/test")

        self.assertEqual(downloader._session.get.call_count, nd.MAX_RETRIES)


# ---------------------------------------------------------------------------
# TestProcessedFilesTracker
# ---------------------------------------------------------------------------


class TestProcessedFilesTracker(unittest.TestCase):
    """Unit tests for ProcessedFilesTracker."""

    # ------------------------------------------------------------------
    # test_is_processed_returns_false_for_unknown
    # ------------------------------------------------------------------

    def test_is_processed_returns_false_for_unknown(self):
        """
        A freshly created tracker (no Spark, empty cache) should return False
        for any filename it has never seen.
        """
        tracker = ProcessedFilesTracker(spark=None)
        # Force loaded=True so we don't trigger load() which might access Spark
        tracker._loaded = True

        self.assertFalse(
            tracker.is_processed("UNKNOWN_FILE.ZIP"),
            "Fresh tracker should return False for an unseen file",
        )
        self.assertFalse(
            tracker.is_processed("PUBLIC_DISPATCHSCADA_202501011200_0000000001234567.ZIP"),
            "Fresh tracker should return False for a new dispatch SCADA file",
        )

    # ------------------------------------------------------------------
    # test_mark_processed_adds_to_cache
    # ------------------------------------------------------------------

    def test_mark_processed_adds_to_cache(self):
        """
        After calling mark_processed for a filename, is_processed should
        return True for that filename.
        """
        tracker = ProcessedFilesTracker(spark=None)
        tracker._loaded = True

        filename = "PUBLIC_DISPATCHSCADA_202501011200_0000000001234567.ZIP"
        self.assertFalse(tracker.is_processed(filename))

        tracker.mark_processed(
            filename=filename,
            report_type="Dispatch_SCADA",
            csv_path=json.dumps(["/vol/Dispatch_SCADA/dispatch_scada.csv"]),
            size_bytes=102400,
        )

        self.assertTrue(
            tracker.is_processed(filename),
            "is_processed should return True after mark_processed",
        )

    def test_mark_processed_only_affects_specified_file(self):
        """
        Marking one file as processed should not affect other filenames.
        """
        tracker = ProcessedFilesTracker(spark=None)
        tracker._loaded = True

        file_a = "PUBLIC_DISPATCHSCADA_202501011200_0000000001234567.ZIP"
        file_b = "PUBLIC_DISPATCHSCADA_202501011205_0000000001234568.ZIP"

        tracker.mark_processed(file_a, "Dispatch_SCADA", "[]", 1024)

        self.assertTrue(tracker.is_processed(file_a))
        self.assertFalse(
            tracker.is_processed(file_b),
            "Unmarked file_b should still return False",
        )

    # ------------------------------------------------------------------
    # test_load_without_spark_uses_empty_cache
    # ------------------------------------------------------------------

    def test_load_without_spark_uses_empty_cache(self):
        """
        When spark=None, calling load() should complete without error and
        result in an empty cache.
        """
        tracker = ProcessedFilesTracker(spark=None)
        self.assertFalse(tracker._loaded)

        try:
            tracker.load()
        except Exception as exc:
            self.fail(f"load() raised an unexpected exception with spark=None: {exc}")

        self.assertTrue(tracker._loaded, "tracker._loaded should be True after load()")
        self.assertEqual(len(tracker._cache), 0, "Cache should be empty without Spark")

    def test_load_with_failing_spark_falls_back_to_empty_cache(self):
        """
        If the Spark session raises an exception when reading the Delta table,
        the tracker should fall back to an empty cache without raising.
        """
        mock_spark = MagicMock()
        mock_spark.sql = MagicMock(return_value=None)  # CREATE TABLE OK
        mock_spark.table = MagicMock(side_effect=Exception("Delta read error"))

        tracker = ProcessedFilesTracker(spark=mock_spark)

        try:
            tracker.load()
        except Exception as exc:
            self.fail(f"load() should not raise even when Spark fails: {exc}")

        self.assertTrue(tracker._loaded)
        self.assertEqual(
            len(tracker._cache),
            0,
            "Cache should be empty after Spark Delta read failure",
        )

    def test_mark_processed_with_spark_appends_to_delta(self):
        """
        When a Spark session is present, mark_processed should attempt to
        write to the Delta table via spark.createDataFrame().write.
        """
        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_df.write = mock_write
        mock_write.format = MagicMock(return_value=mock_write)
        mock_write.mode  = MagicMock(return_value=mock_write)
        mock_write.saveAsTable = MagicMock(return_value=None)

        mock_spark = MagicMock()
        mock_spark.sql = MagicMock()
        mock_spark.createDataFrame = MagicMock(return_value=mock_df)
        # Simulate existing processed files table (returns empty result set)
        mock_empty = MagicMock()
        mock_empty.select = MagicMock(return_value=mock_empty)
        mock_empty.collect = MagicMock(return_value=[])
        mock_spark.table = MagicMock(return_value=mock_empty)

        tracker = ProcessedFilesTracker(spark=mock_spark)
        tracker._loaded = True

        filename = "PUBLIC_DISPATCHSCADA_202501011200_0000000001234567.ZIP"
        tracker.mark_processed(
            filename=filename,
            report_type="Dispatch_SCADA",
            csv_path=json.dumps(["/vol/test.csv"]),
            size_bytes=2048,
        )

        mock_spark.createDataFrame.assert_called_once()
        mock_write.saveAsTable.assert_called_once()
        self.assertIn(
            nd.PROCESSED_FILES_TABLE,
            mock_write.saveAsTable.call_args[0][0],
        )

    def test_is_processed_triggers_load_if_not_loaded(self):
        """
        Calling is_processed before load() should trigger an automatic load
        (loaded flag should be True afterwards).
        """
        tracker = ProcessedFilesTracker(spark=None)
        self.assertFalse(tracker._loaded)

        # This should trigger load() internally
        result = tracker.is_processed("any_file.ZIP")

        self.assertFalse(result)
        self.assertTrue(tracker._loaded, "_loaded should be True after auto-triggered load()")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(argv=["", "-v"])
