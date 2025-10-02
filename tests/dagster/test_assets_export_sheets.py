"""Tests for Google Sheets export assets."""

from typing import Any
from unittest.mock import patch

import pytest

from orchestration.assets_export_sheets import (
    load_runtime_config,
)


class TestGoogleSheetsUtilities:
    """Test Google Sheets utility functions."""

    def test_load_runtime_config(self) -> None:
        """Test configuration loading."""
        with patch.dict(
            "os.environ",
            {
                "GOOGLE_SPREADSHEET_ID": "test_sheet_id",
                "GOOGLE_SHEET_NAME": "Test",
                "GOOGLE_TABLE_NAME": "Test Table",
                "GOOGLE_SA_JSON_PATH": "/path/to/sa.json",
                "EXPORT_FINANCE_TABLE": "test_table",
                "DUCKDB_PATH": "/path/to/db.duckdb",
            },
        ):
            config = load_runtime_config()

            assert config["google_spreadsheet_id"] == "test_sheet_id"
            assert config["google_sheet_name"] == "Test"
            assert config["google_table_name"] == "Test Table"
            assert config["google_sa_json_path"] == "/path/to/sa.json"
            assert config["export_finance_table"] == "test_table"
            assert config["duckdb_path"] == "/path/to/db.duckdb"

    def test_load_runtime_config_defaults(self) -> None:
        """Test configuration loading with defaults."""
        with patch.dict("os.environ", {}, clear=True):
            config = load_runtime_config()

            assert config["google_spreadsheet_id"] is None
            assert config["google_sheet_name"] == "Spendings"
            assert config["google_table_name"] == "Spendings Log"
            assert (
                config["google_sa_json_path"]
                == "/app/credentials/finance-sheets-writer-prod-sa.json"
            )
            assert config["export_finance_table"] == "prod_imart.view_bank_transactions"
            assert config["duckdb_path"] == "/app/data/warehouse/analytics.duckdb"


class TestGoogleSheetsExportAsset:
    """Test the Google Sheets export asset."""

    @patch("orchestration.assets_export_sheets.load_runtime_config")
    def test_export_to_google_sheets_no_sheet_id(self, mock_config: Any) -> None:
        """Test export with missing sheet ID."""
        from orchestration.assets_export_sheets import export_to_google_sheets

        mock_config.return_value = {
            "google_spreadsheet_id": None,
            "google_sheet_name": "Test",
            "google_table_name": "Test Table",
            "google_sa_json_path": "/path/to/sa.json",
            "export_finance_table": "test_table",
            "duckdb_path": "/path/to/db.duckdb",
        }

        with pytest.raises(
            ValueError, match="GOOGLE_SPREADSHEET_ID environment variable is required"
        ):
            export_to_google_sheets()

    @patch("orchestration.assets_export_sheets.load_runtime_config")
    def test_export_to_google_sheets_no_sa_file(self, mock_config: Any) -> None:
        """Test export with missing service account file."""
        from orchestration.assets_export_sheets import export_to_google_sheets

        mock_config.return_value = {
            "google_spreadsheet_id": "test_sheet_id",
            "google_sheet_name": "Test",
            "google_table_name": "Test Table",
            "google_sa_json_path": "/nonexistent/sa.json",
            "export_finance_table": "test_table",
            "duckdb_path": "/path/to/db.duckdb",
        }

        with (
            patch("pathlib.Path.exists", return_value=False),
            pytest.raises(ValueError, match="Google service account file not found"),
        ):
            export_to_google_sheets()
