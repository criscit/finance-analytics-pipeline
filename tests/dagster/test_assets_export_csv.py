"""Tests for CSV export assets."""

import json
import tempfile
from pathlib import Path
from typing import Any
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

    @patch("orchestration.assets_export_csv.OUT")
    @patch("orchestration.assets_export_csv.META")
    @patch("orchestration.assets_export_csv.DB")
    @patch("orchestration.assets_export_csv.TABLE")
    def test_export_csv_snapshot_success(
        self, mock_table: Any, mock_db: Any, mock_meta: Any, mock_out: Any
    ) -> None:
        """Test successful CSV export."""
        # Create temporary directories
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            mock_out.return_value = temp_path / "csv"
            mock_meta.return_value = temp_path / "meta"
            mock_db.return_value = ":memory:"
            mock_table.return_value = "test_table"

            # Create a test database with sample data
            with duckdb.connect(":memory:") as con:
                con.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
                con.execute("INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')")

                # Export the database to a temporary file
                db_file = temp_path / "test.duckdb"
                con.execute(f"EXPORT DATABASE '{db_file}'")

            mock_db.return_value = str(db_file)

            result = export_csv_snapshot()

            # Check that files were created
            csv_dir = mock_out.return_value / "test_table"
            assert csv_dir.exists()

            # Check manifest was created
            meta_dir = mock_meta.return_value / "test_table"
            manifest_file = meta_dir / "manifest.json"
            assert manifest_file.exists()

            # Check manifest content
            with manifest_file.open("r") as f:
                manifest = json.load(f)

            assert manifest["table"] == "test_table"
            assert manifest["row_count"] == TEST_DATA_ROWS_2
            assert "md5" in manifest
            assert "created_at_utc" in manifest

            # Check return value
            assert result.value["rows"] == TEST_DATA_ROWS_2  # type: ignore[attr-defined]
            assert "md5" in result.value  # type: ignore[attr-defined]

    @patch("orchestration.assets_export_csv.OUT")
    @patch("orchestration.assets_export_csv.META")
    @patch("orchestration.assets_export_csv.DB")
    @patch("orchestration.assets_export_csv.TABLE")
    def test_export_csv_snapshot_empty_table(
        self, mock_table: Any, mock_db: Any, mock_meta: Any, mock_out: Any
    ) -> None:
        """Test CSV export with empty table."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            mock_out.return_value = temp_path / "csv"
            mock_meta.return_value = temp_path / "meta"
            mock_db.return_value = ":memory:"
            mock_table.return_value = "empty_table"

            # Create empty table
            with duckdb.connect(":memory:") as con:
                con.execute("CREATE TABLE empty_table (id INTEGER)")
                db_file = temp_path / "empty.duckdb"
                con.execute(f"EXPORT DATABASE '{db_file}'")

            mock_db.return_value = str(db_file)

            result = export_csv_snapshot()

            # Check return value for empty table
            assert result.value["rows"] == 0  # type: ignore[attr-defined]
            assert "md5" in result.value  # type: ignore[attr-defined]

    @patch("orchestration.assets_export_csv.OUT")
    @patch("orchestration.assets_export_csv.META")
    @patch("orchestration.assets_export_csv.DB")
    @patch("orchestration.assets_export_csv.TABLE")
    def test_export_csv_snapshot_nonexistent_table(
        self, mock_table: Any, mock_db: Any, mock_meta: Any, mock_out: Any
    ) -> None:
        """Test CSV export with non-existent table."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            mock_out.return_value = temp_path / "csv"
            mock_meta.return_value = temp_path / "meta"
            mock_db.return_value = ":memory:"
            mock_table.return_value = "nonexistent_table"

            with pytest.raises((RuntimeError, duckdb.Error)):  # DuckDB will raise an error
                export_csv_snapshot()
