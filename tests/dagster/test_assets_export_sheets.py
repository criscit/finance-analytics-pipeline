"""Tests for Google Sheets export assets."""

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from orchestration.assets_export_sheets import (
    convert_cell,
    load_runtime_config,
    read_all_rows,
    read_table_columns,
)
from tests.constants import TEST_DATA_ROWS_2


class TestGoogleSheetsUtilities:
    """Test Google Sheets utility functions."""

    def test_convert_cell_string(self) -> None:
        """Test cell conversion for string values."""
        assert convert_cell("test") == "test"
        assert convert_cell("") == ""
        assert convert_cell(None) == "None"

    def test_convert_cell_numeric(self) -> None:
        """Test cell conversion for numeric values."""
        assert convert_cell(123) == "123"
        assert convert_cell(123.45) == "123.45"
        assert convert_cell(0) == "0"

    def test_convert_cell_datetime(self) -> None:
        """Test cell conversion for datetime values."""
        from datetime import datetime

        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = convert_cell(dt)
        assert "2024-01-01T12:00:00" in result

    def test_load_runtime_config(self) -> None:
        """Test configuration loading."""
        with patch.dict(
            "os.environ",
            {
                "GOOGLE_SHEET_ID": "test_sheet_id",
                "GOOGLE_SHEET_RANGE": "Test!A1",
                "GOOGLE_SA_JSON": "/path/to/sa.json",
                "EXPORT_TABLE": "test_table",
                "DUCKDB_PATH": "/path/to/db.duckdb",
            },
        ):
            config = load_runtime_config()

            assert config["sheet_id"] == "test_sheet_id"
            assert config["range_name"] == "Test!A1"
            assert config["sa_path"] == "/path/to/sa.json"
            assert config["table"] == "test_table"
            assert config["db_path"] == "/path/to/db.duckdb"

    def test_load_runtime_config_defaults(self) -> None:
        """Test configuration loading with defaults."""
        with patch.dict("os.environ", {}, clear=True):
            config = load_runtime_config()

            assert config["sheet_id"] is None
            assert config["range_name"] == "Exports!A1"
            assert config["sa_path"] == "/app/credentials/finance-sheets-writer-prod-sa.json"
            assert config["table"] == "prod_imart.view_bank_transactions"
            assert config["db_path"] == "/app/data/warehouse/warehouse.duckdb"


class TestDuckDBFunctions:
    """Test DuckDB read functions."""

    def test_read_table_columns(self) -> None:
        """Test reading table columns."""
        import duckdb

        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            with duckdb.connect(db_path) as con:
                con.execute(
                    "CREATE TABLE test_table (id INTEGER, name VARCHAR, created_at TIMESTAMP)"
                )

            columns = read_table_columns(db_path, "test_table")
            assert columns == ["id", "name", "created_at"]
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_read_all_rows(self) -> None:
        """Test reading all rows from table."""
        import duckdb

        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            with duckdb.connect(db_path) as con:
                con.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
                con.execute("INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')")

            rows, columns = read_all_rows(db_path, "test_table")

            assert columns == ["id", "name"]
            assert len(rows) == TEST_DATA_ROWS_2
            assert rows[0] == (1, "test1")
            assert rows[1] == (2, "test2")
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_read_all_rows_empty_table(self) -> None:
        """Test reading from empty table."""
        import duckdb

        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            with duckdb.connect(db_path) as con:
                con.execute("CREATE TABLE empty_table (id INTEGER)")

            rows, columns = read_all_rows(db_path, "empty_table")

            assert columns == ["id"]
            assert len(rows) == 0
        finally:
            Path(db_path).unlink(missing_ok=True)


class TestGoogleSheetsExportAsset:
    """Test the Google Sheets export asset."""

    @patch("orchestration.assets_export_sheets.load_runtime_config")
    @patch("orchestration.assets_export_sheets.read_all_rows")
    @patch("orchestration.assets_export_sheets.append_to_sheet")
    def test_export_to_google_sheets_success(
        self, mock_append: Any, mock_read_rows: Any, mock_config: Any
    ) -> None:
        """Test successful Google Sheets export."""
        from orchestration.assets_export_sheets import export_to_google_sheets

        # Mock configuration
        mock_config.return_value = {
            "sheet_id": "test_sheet_id",
            "range_name": "Test!A1",
            "sa_path": "/path/to/sa.json",
            "table": "test_table",
            "db_path": "/path/to/db.duckdb",
        }

        # Mock data
        mock_read_rows.return_value = ([(1, "test1"), (2, "test2")], ["id", "name"])

        # Mock service account file exists
        with patch("pathlib.Path.exists", return_value=True):
            result = export_to_google_sheets()

        # Verify calls
        mock_read_rows.assert_called_once_with("/path/to/db.duckdb", "test_table")
        mock_append.assert_called_once()

        # Check the values passed to append_to_sheet
        call_args = mock_append.call_args
        assert call_args[0][0] == "/path/to/sa.json"  # sa_path
        assert call_args[0][1] == "test_sheet_id"  # sheet_id
        assert call_args[0][2] == "Test!A1"  # range_name

        values = call_args[0][3]  # values
        assert values[0] == ["id", "name"]  # header row
        assert values[1] == ["1", "test1"]  # data row 1
        assert values[2] == ["2", "test2"]  # data row 2

        # Check return value
        assert result.value["appended"] == TEST_DATA_ROWS_2  # type: ignore[attr-defined]

    @patch("orchestration.assets_export_sheets.load_runtime_config")
    def test_export_to_google_sheets_no_sheet_id(self, mock_config: Any) -> None:
        """Test export with missing sheet ID."""
        from orchestration.assets_export_sheets import export_to_google_sheets

        mock_config.return_value = {
            "sheet_id": None,
            "range_name": "Test!A1",
            "sa_path": "/path/to/sa.json",
            "table": "test_table",
            "db_path": "/path/to/db.duckdb",
        }

        with pytest.raises(ValueError, match="GOOGLE_SHEET_ID environment variable is required"):
            export_to_google_sheets()

    @patch("orchestration.assets_export_sheets.load_runtime_config")
    def test_export_to_google_sheets_no_sa_file(self, mock_config: Any) -> None:
        """Test export with missing service account file."""
        from orchestration.assets_export_sheets import export_to_google_sheets

        mock_config.return_value = {
            "sheet_id": "test_sheet_id",
            "range_name": "Test!A1",
            "sa_path": "/nonexistent/sa.json",
            "table": "test_table",
            "db_path": "/path/to/db.duckdb",
        }

        with (
            patch("pathlib.Path.exists", return_value=False),
            pytest.raises(ValueError, match="Google service account file not found"),
        ):
            export_to_google_sheets()

    @patch("orchestration.assets_export_sheets.load_runtime_config")
    @patch("orchestration.assets_export_sheets.read_all_rows")
    def test_export_to_google_sheets_no_data(self, mock_read_rows: Any, mock_config: Any) -> None:
        """Test export with no data."""
        from orchestration.assets_export_sheets import export_to_google_sheets

        mock_config.return_value = {
            "sheet_id": "test_sheet_id",
            "range_name": "Test!A1",
            "sa_path": "/path/to/sa.json",
            "table": "test_table",
            "db_path": "/path/to/db.duckdb",
        }

        # Mock empty data
        mock_read_rows.return_value = ([], ["id", "name"])

        with patch("pathlib.Path.exists", return_value=True):
            result = export_to_google_sheets()

        # Should return early with no data
        assert result.value["appended"] == 0  # type: ignore[attr-defined]
