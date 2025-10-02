"""Tests for Google Sheets module."""

from unittest.mock import Mock, patch

from src.google_sheets import GoogleSheetsTableManager

# Test constants
EXISTING_SHEET_ID = 123
OTHER_SHEET_ID = 456
NEW_SHEET_ID = 789


class TestGoogleSheetsTableManager:
    """Test cases for GoogleSheetsTableManager."""

    @patch("src.google_sheets.Credentials")
    @patch("src.google_sheets.build")
    def test_init(self, mock_build: Mock, mock_credentials: Mock) -> None:
        """Test GoogleSheetsTableManager initialization."""
        mock_creds = Mock()
        mock_credentials.from_service_account_file.return_value = mock_creds
        mock_service = Mock()
        mock_build.return_value = mock_service

        manager = GoogleSheetsTableManager("test_credentials.json")

        mock_credentials.from_service_account_file.assert_called_once_with(
            "test_credentials.json", scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        mock_build.assert_called_once_with("sheets", "v4", credentials=mock_creds)
        assert manager.sheets_service == mock_service

    @patch("src.google_sheets.Credentials")
    @patch("src.google_sheets.build")
    def test_get_or_create_sheet_existing(self, mock_build: Mock, mock_credentials: Mock) -> None:
        """Test ensure_sheet_exists when sheet already exists."""
        mock_service = Mock()
        mock_build.return_value = mock_service

        # Mock spreadsheet response
        mock_response = {
            "sheets": [{"properties": {"title": "Spendings", "sheetId": EXISTING_SHEET_ID}}]
        }
        mock_service.spreadsheets().get.return_value.execute.return_value = mock_response

        manager = GoogleSheetsTableManager("test_credentials.json")
        result = manager.get_or_create_sheet("test_spreadsheet_id", "Spendings")

        assert result == EXISTING_SHEET_ID
        mock_service.spreadsheets().get.assert_called_once_with(spreadsheetId="test_spreadsheet_id")

    @patch("src.google_sheets.Credentials")
    @patch("src.google_sheets.build")
    def test_get_or_create_sheet_new(self, mock_build: Mock, mock_credentials: Mock) -> None:
        """Test ensure_sheet_exists when creating new sheet."""
        mock_service = Mock()
        mock_build.return_value = mock_service

        # Mock spreadsheet response with no matching sheet
        mock_response = {
            "sheets": [{"properties": {"title": "Other Sheet", "sheetId": OTHER_SHEET_ID}}]
        }
        mock_service.spreadsheets().get.return_value.execute.return_value = mock_response

        # Mock batch update response
        mock_batch_response = {"replies": [{"addSheet": {"properties": {"sheetId": NEW_SHEET_ID}}}]}
        mock_service.spreadsheets().batchUpdate.return_value.execute.return_value = (
            mock_batch_response
        )

        manager = GoogleSheetsTableManager("test_credentials.json")
        result = manager.get_or_create_sheet("test_spreadsheet_id", "Spendings")

        assert result == NEW_SHEET_ID
        mock_service.spreadsheets().batchUpdate.assert_called_once()
