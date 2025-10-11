"""Tests for CSV export assets."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from orchestration.assets_export_csv import _md5, export_csv_snapshot
from tests.constants import MD5_HASH_LENGTH, TEST_DATA_ROWS_2


class TestCSVExportUtilities:
    """Test CSV export utility functions."""

    def test_md5_hash(self) -> None:
        """Test MD5 hash calculation."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test content")
            temp_path = Path(f.name)

        try:
            result = _md5(temp_path)
            assert len(result) == MD5_HASH_LENGTH  # MD5 hash length
            assert result.isalnum()  # Should be alphanumeric
        finally:
            temp_path.unlink()

    def test_md5_nonexistent_file(self) -> None:
        """Test MD5 calculation on non-existent file."""
        temp_path = Path("/nonexistent/file.csv")
        with pytest.raises(FileNotFoundError):
            _md5(temp_path)


class TestCSVExportAsset:
    """Test the CSV export asset."""

    def test_export_csv_snapshot_success(self) -> None:
        """Test successful CSV export."""
        # Create temporary directories
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            export_dir = temp_path / "exports"
            results_dir = temp_path / "results"

            # Create a test database with sample data
            db_file = temp_path / "test.duckdb"
            with duckdb.connect(str(db_file)) as con:
                con.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
                con.execute("INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')")

            with (
                patch("orchestration.assets_export_csv.DUCKDB_PATH", str(db_file)),
                patch("orchestration.assets_export_csv.EXPORT_DIR", export_dir),
                patch("orchestration.assets_export_csv.RESULTS_DIR", results_dir),
                patch("orchestration.assets_export_csv.EXPORT_FINANCE_TABLE", "test_table"),
            ):
                result = export_csv_snapshot()

                # Check return value
                assert result.value["rows"] == TEST_DATA_ROWS_2  # type: ignore[attr-defined]
                assert "md5" in result.value  # type: ignore[attr-defined]

    def test_export_csv_snapshot_empty_table(self) -> None:
        """Test CSV export with empty table."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            export_dir = temp_path / "exports"
            results_dir = temp_path / "results"

            # Create empty table
            db_file = temp_path / "empty.duckdb"
            with duckdb.connect(str(db_file)) as con:
                con.execute("CREATE TABLE empty_table (id INTEGER)")

            with (
                patch("orchestration.assets_export_csv.DUCKDB_PATH", str(db_file)),
                patch("orchestration.assets_export_csv.EXPORT_DIR", export_dir),
                patch("orchestration.assets_export_csv.RESULTS_DIR", results_dir),
                patch("orchestration.assets_export_csv.EXPORT_FINANCE_TABLE", "empty_table"),
            ):
                result = export_csv_snapshot()

                # Check return value for empty table
                assert result.value["rows"] == 0  # type: ignore[attr-defined]
                assert "md5" in result.value  # type: ignore[attr-defined]

    def test_export_csv_snapshot_nonexistent_table(self) -> None:
        """Test CSV export with non-existent table."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            export_dir = temp_path / "exports"
            results_dir = temp_path / "results"

            with (
                patch("orchestration.assets_export_csv.DUCKDB_PATH", ":memory:"),
                patch("orchestration.assets_export_csv.EXPORT_DIR", export_dir),
                patch("orchestration.assets_export_csv.RESULTS_DIR", results_dir),
                patch("orchestration.assets_export_csv.EXPORT_FINANCE_TABLE", "nonexistent_table"),
                pytest.raises(duckdb.CatalogException),  # DuckDB will raise an error
            ):
                export_csv_snapshot()
