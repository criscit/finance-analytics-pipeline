"""Tests for DuckDB utilities module."""

from unittest.mock import Mock, patch

from src.duckdb_utils import (
    convert_cell_value,
    get_duckdb_to_sheets_column_mapping,
    get_ordered_columns_for_sheets,
    prepare_data_for_sheets,
    prepare_ordered_data_for_sheets,
    read_table_data_with_ordered_columns,
)


class TestDuckDBUtils:
    """Test cases for DuckDB utilities."""

    def test_convert_cell_value_string(self) -> None:
        """Test converting string values."""
        result = convert_cell_value("test_string")
        assert result == "test_string"

    def test_convert_cell_value_number(self) -> None:
        """Test converting numeric values."""
        result = convert_cell_value(123.45)
        assert result == "123.45"

    def test_convert_cell_value_none(self) -> None:
        """Test converting None values."""
        result = convert_cell_value(None)
        assert result == ""

    def test_convert_cell_value_date(self) -> None:
        """Test converting date values."""
        from datetime import date

        test_date = date(2025, 1, 15)
        result = convert_cell_value(test_date)
        assert result == "2025-01-15"

    def test_prepare_data_for_sheets(self) -> None:
        """Test preparing data for Google Sheets (without headers)."""
        rows = [
            ("2025-01-15", "Chas", "Food", "Grocery", 45.50, "USD", 45.50, 45.50),
            ("2025-01-16", "Sberbank", "Transport", "Taxi", 12.30, "USD", 12.30, 12.30),
        ]
        cols = ["Date", "Bank", "Category", "Description", "Amount", "Currency", "RUB", "USD"]

        result = prepare_data_for_sheets(rows, cols)

        expected = [
            ["2025-01-15", "Chas", "Food", "Grocery", "45.5", "USD", "45.5", "45.5"],
            ["2025-01-16", "Sberbank", "Transport", "Taxi", "12.3", "USD", "12.3", "12.3"],
        ]

        assert result == expected

    def test_get_ordered_columns_for_sheets(self) -> None:
        """Test getting ordered columns for Google Sheets."""
        result = get_ordered_columns_for_sheets()
        expected = [
            "Date",
            "Platform Name",
            "Category",
            "Description",
            "Amount, Currency",
            "Currency",
            "Amount, RUB",
            "Amount, USD",
            "Executed Rate, RUB",
            "Executed Rate, USD",
            "Close Rate, RUB",
            "Close Rate, USD",
        ]
        assert result == expected

    def test_get_duckdb_to_sheets_column_mapping(self) -> None:
        """Test DuckDB to Google Sheets column mapping."""
        result = get_duckdb_to_sheets_column_mapping()
        expected = {
            "date": "Date",
            "platform_name": "Platform Name",
            "category": "Category",
            "description": "Description",
            "amount_currency": "Amount, Currency",
            "currency": "Currency",
            "amount_rub": "Amount, RUB",
            "amount_usd": "Amount, USD",
            "executed_rate_rub": "Executed Rate, RUB",
            "executed_rate_usd": "Executed Rate, USD",
            "close_rate_rub": "Close Rate, RUB",
            "close_rate_usd": "Close Rate, USD",
        }
        assert result == expected

    def test_prepare_ordered_data_for_sheets_complete_data(self) -> None:
        """Test preparing ordered data with complete DuckDB columns."""
        # Simulate DuckDB data with columns in random order (6 columns for simplicity)
        rows = [
            ("T Bank", "2025-01-15", "USD", "Food", "Grocery Store", 45.50),
            ("Bybit", "2025-01-16", "USD", "Transport", "Taxi Ride", 12.30),
        ]
        # DuckDB columns in different order than expected Google Sheets order
        cols = [
            "platform_name",
            "date",
            "currency",
            "category",
            "description",
            "amount_currency",
        ]

        result = prepare_ordered_data_for_sheets("test.db", "schema", "transactions", rows, cols)

        # Should include all 12 expected columns with missing ones as empty
        assert len(result[0]) == 12  # Header row
        assert result[0][0] == "Date"
        assert result[0][1] == "Platform Name"
        assert result[1][0] == "2025-01-15"
        assert result[1][1] == "T Bank"

    def test_prepare_ordered_data_for_sheets_missing_columns(self) -> None:
        """Test preparing ordered data with missing DuckDB columns."""
        rows = [("2025-01-15", "T Bank", 45.50)]  # Missing some columns
        # Only some columns present
        cols = ["date", "platform_name", "amount_currency"]

        result = prepare_ordered_data_for_sheets("test.db", "schema", "transactions", rows, cols)

        # Should include all 12 expected columns with missing ones as empty
        assert len(result[0]) == 12  # Header row
        assert result[1][0] == "2025-01-15"
        assert result[1][1] == "T Bank"
        assert result[1][2] == ""  # category - missing
        assert result[1][4] == "45.5"  # amount_currency

    def test_prepare_ordered_data_for_sheets_extra_columns(self) -> None:
        """Test preparing ordered data with extra DuckDB columns not in mapping."""
        rows = [
            (
                "2025-01-15",
                "T Bank",
                "Food",
                "Grocery",
                45.50,
                "USD",
                "extra_col1",
                "extra_col2",
            )
        ]
        # Extra columns that aren't in our mapping
        cols = [
            "date",
            "platform_name",
            "category",
            "description",
            "amount_currency",
            "currency",
            "extra_col1",
            "extra_col2",
        ]

        result = prepare_ordered_data_for_sheets("test.db", "schema", "transactions", rows, cols)

        # Should include all 12 expected columns, extra columns ignored
        assert len(result[0]) == 12  # Header row
        assert result[1][0] == "2025-01-15"
        assert result[1][1] == "T Bank"
        assert result[1][2] == "Food"

    def test_prepare_ordered_data_for_sheets_date_conversion(self) -> None:
        """Test that date values are properly converted in ordered export."""
        from datetime import date

        rows = [(date(2025, 1, 15), "T Bank", "Food", "Grocery", 45.50, "USD")]
        cols = [
            "date",
            "platform_name",
            "category",
            "description",
            "amount_currency",
            "currency",
        ]

        result = prepare_ordered_data_for_sheets("test.db", "schema", "transactions", rows, cols)

        # Should include all 12 expected columns
        assert len(result[0]) == 12  # Header row
        assert result[1][0] == "2025-01-15"  # Date properly converted

    @patch("src.duckdb_utils.connect_readonly")
    def test_read_table_data_with_ordered_columns(self, mock_connect: Mock) -> None:
        """Test reading table data with proper column ordering."""
        # Mock the database connection and execution
        mock_con = Mock()
        mock_connect.return_value.__enter__.return_value = mock_con

        # Mock the execute result (12 columns matching the new structure)
        mock_rows = [
            (
                "2025-01-15",
                "T Bank",
                "Food",
                "Grocery Store",
                45.50,
                "USD",
                45.50,
                45.50,
                None,
                None,
                None,
                None,
            ),
            (
                "2025-01-16",
                "Bybit",
                "Transport",
                "Taxi Ride",
                12.30,
                "USD",
                12.30,
                12.30,
                None,
                None,
                None,
                None,
            ),
        ]
        mock_con.execute.return_value.fetchall.return_value = mock_rows

        result = read_table_data_with_ordered_columns("test.db", "schema", "transactions")

        # Verify the data conversion
        assert len(result) == 2
        assert result[0][0] == "2025-01-15"
        assert result[0][1] == "T Bank"

        # Verify the query was constructed correctly
        mock_con.execute.assert_called_once()
        query = mock_con.execute.call_args[0][0]
        assert '"date" as "Date"' in query
        assert '"platform_name" as "Platform Name"' in query
        assert '"category" as "Category"' in query
        assert '"description" as "Description"' in query
        assert '"amount_currency" as "Amount, Currency"' in query
        assert '"currency" as "Currency"' in query
        assert '"amount_rub" as "Amount, RUB"' in query
        assert '"amount_usd" as "Amount, USD"' in query
        assert 'from "schema"."transactions"' in query
        assert "order by" in query.lower()
        assert '"transacted_at"' in query
