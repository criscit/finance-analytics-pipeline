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
        assert result == "None"

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
        expected = ["Date", "Bank Name", "Category", "Description", "Amount, Currency", "Currency"]
        assert result == expected

    def test_get_duckdb_to_sheets_column_mapping(self) -> None:
        """Test DuckDB to Google Sheets column mapping."""
        result = get_duckdb_to_sheets_column_mapping()
        expected = {
            "transaction_dt": "Date",
            "bank_nm": "Bank Name",
            "category_nm": "Category",
            "description": "Description",
            "transaction_amt": "Amount, Currency",
            "transaction_currency_cd": "Currency",
        }
        assert result == expected

    def test_prepare_ordered_data_for_sheets_complete_data(self) -> None:
        """Test preparing ordered data with complete DuckDB columns."""
        # Simulate DuckDB data with columns in random order
        rows = [
            ("Chase Bank", "2025-01-15", "USD", "Food", "Grocery Store", 45.50),
            ("Sberbank", "2025-01-16", "USD", "Transport", "Taxi Ride", 12.30),
        ]
        # DuckDB columns in different order than expected Google Sheets order
        cols = [
            "bank_nm",
            "transaction_dt",
            "transaction_currency_cd",
            "category_nm",
            "description",
            "transaction_amt",
        ]

        result = prepare_ordered_data_for_sheets("test.db", "schema", "transactions", rows, cols)

        expected = [
            ["Date", "Bank Name", "Category", "Description", "Amount, Currency", "Currency"],
            ["2025-01-15", "Chase Bank", "Food", "Grocery Store", "45.5", "USD"],
            ["2025-01-16", "Sberbank", "Transport", "Taxi Ride", "12.3", "USD"],
        ]

        assert result == expected

    def test_prepare_ordered_data_for_sheets_missing_columns(self) -> None:
        """Test preparing ordered data with missing DuckDB columns."""
        rows = [("2025-01-15", "Chase Bank", 45.50)]  # Missing some columns
        # Only some columns present
        cols = ["transaction_dt", "bank_nm", "transaction_amt"]

        result = prepare_ordered_data_for_sheets("test.db", "schema", "transactions", rows, cols)

        expected = [
            ["Date", "Bank Name", "Category", "Description", "Amount, Currency", "Currency"],
            [
                "2025-01-15",
                "Chase Bank",
                "",
                "",
                "45.5",
                "",
            ],  # Missing columns filled with empty strings
        ]

        assert result == expected

    def test_prepare_ordered_data_for_sheets_extra_columns(self) -> None:
        """Test preparing ordered data with extra DuckDB columns not in mapping."""
        rows = [
            (
                "2025-01-15",
                "Chase Bank",
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
            "transaction_dt",
            "bank_nm",
            "category_nm",
            "description",
            "transaction_amt",
            "transaction_currency_cd",
            "extra_col1",
            "extra_col2",
        ]

        result = prepare_ordered_data_for_sheets("test.db", "schema", "transactions", rows, cols)

        expected = [
            ["Date", "Bank Name", "Category", "Description", "Amount, Currency", "Currency"],
            ["2025-01-15", "Chase Bank", "Food", "Grocery", "45.5", "USD"],
        ]

        assert result == expected

    def test_prepare_ordered_data_for_sheets_date_conversion(self) -> None:
        """Test that date values are properly converted in ordered export."""
        from datetime import date

        rows = [(date(2025, 1, 15), "Chase Bank", "Food", "Grocery", 45.50, "USD")]
        cols = [
            "transaction_dt",
            "bank_nm",
            "category_nm",
            "description",
            "transaction_amt",
            "transaction_currency_cd",
        ]

        result = prepare_ordered_data_for_sheets("test.db", "schema", "transactions", rows, cols)

        expected = [
            ["Date", "Bank Name", "Category", "Description", "Amount, Currency", "Currency"],
            ["2025-01-15", "Chase Bank", "Food", "Grocery", "45.5", "USD"],
        ]

        assert result == expected

    @patch("src.duckdb_utils.connect_readonly")
    def test_read_table_data_with_ordered_columns(self, mock_connect: Mock) -> None:
        """Test reading table data with proper column ordering."""
        # Mock the database connection and execution
        mock_con = Mock()
        mock_connect.return_value.__enter__.return_value = mock_con

        # Mock the execute result
        mock_rows = [
            ("2025-01-15", "Chase Bank", "Food", "Grocery Store", 45.50, "USD"),
            ("2025-01-16", "Sberbank", "Transport", "Taxi Ride", 12.30, "USD"),
        ]
        mock_con.execute.return_value.fetchall.return_value = mock_rows

        result = read_table_data_with_ordered_columns("test.db", "schema", "transactions")

        expected = [
            ["2025-01-15", "Chase Bank", "Food", "Grocery Store", "45.5", "USD"],
            ["2025-01-16", "Sberbank", "Transport", "Taxi Ride", "12.3", "USD"],
        ]

        assert result == expected

        # Verify the query was constructed correctly
        mock_con.execute.assert_called_once()
        query = mock_con.execute.call_args[0][0]
        assert '"transaction_dt" as "Date"' in query
        assert '"bank_nm" as "Bank Name"' in query
        assert '"category_nm" as "Category"' in query
        assert '"description" as "Description"' in query
        assert '"transaction_amt" as "Amount, Currency"' in query
        assert '"transaction_currency_cd" as "Currency"' in query
        assert 'from "schema"."transactions"' in query
