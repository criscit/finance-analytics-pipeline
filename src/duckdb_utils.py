"""DuckDB database utilities module."""

from typing import Any

import duckdb

from src.utils import qident, qtable


def connect_readonly(db_path: str) -> duckdb.DuckDBPyConnection:
    """Create read-only DuckDB connection."""
    return duckdb.connect(database=db_path, read_only=True)


def get_table_columns(db_path: str, schema: str, table: str) -> list[str]:
    """Get column names for the specified table."""
    with connect_readonly(db_path) as con:
        rows = con.execute(f"describe {qtable(schema, table)}").fetchall()
    return [r[0] for r in rows]


def read_table_data(
    db_path: str, schema: str, table: str
) -> tuple[list[tuple[Any, ...]], list[str]]:
    """Read all rows from the specified table."""
    cols = get_table_columns(db_path, schema, table)
    query = f"select * from {qtable(schema, table)}"

    with connect_readonly(db_path) as con:
        rows = con.execute(query).fetchall()
    return rows, cols


def read_table_data_with_ordered_columns(db_path: str, schema: str, table: str) -> list[list[str]]:
    """Read table data with proper column ordering for Google Sheets."""
    # Get the mapping and ordered columns
    column_mapping = get_duckdb_to_sheets_column_mapping()

    # Build the SELECT query with ordered columns
    select_columns = []
    for duckdb_col, sheets_col in column_mapping.items():
        select_columns.append(f"{qident(duckdb_col)} as {qident(sheets_col)}")

    if not select_columns:
        raise ValueError(f"No matching columns found for table {qtable(schema, table)}")

    query = f"select {', '.join(select_columns)} from {qtable(schema, table)}"

    with connect_readonly(db_path) as con:
        rows = con.execute(query).fetchall()

    # Convert to list of lists with proper formatting
    values = []
    for row in rows:
        converted_row = [convert_cell_value(cell) for cell in row]
        values.append(converted_row)

    return values


def convert_cell_value(value: Any) -> str:
    """Convert cell value to string for Google Sheets."""
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


def prepare_data_for_sheets(rows: list[tuple[Any, ...]], cols: list[str]) -> list[list[str]]:
    """Prepare data for Google Sheets export (without headers)."""
    values = []

    # Data rows only (no headers)
    for row in rows:
        converted_row = [convert_cell_value(cell) for cell in row]
        values.append(converted_row)

    return values


def get_ordered_columns_for_sheets() -> list[str]:
    """Get the ordered column names for Google Sheets export."""
    return ["Date", "Bank Name", "Category", "Description", "Amount, Currency", "Currency"]


def get_duckdb_to_sheets_column_mapping() -> dict[str, str]:
    """Get mapping from DuckDB column names to Google Sheets column names."""
    return {
        "transaction_dt": "Date",
        "bank_nm": "Bank Name",
        "category_nm": "Category",
        "description": "Description",
        "transaction_amt": "Amount, Currency",
        "transaction_currency_cd": "Currency",
    }


def prepare_ordered_data_for_sheets(
    db_path: str, schema: str, table: str, rows: list[tuple[Any, ...]], cols: list[str]
) -> list[list[str]]:
    """
    Prepare data for Google Sheets export with proper column ordering.

    Args:
        db_path: Path to DuckDB database
        schema: Schema name
        table: Table name to export
        rows: Raw data rows from DuckDB
        cols: Column names from DuckDB (in DuckDB order)

    Returns:
        List of rows with properly ordered columns for Google Sheets
    """
    # Get the mapping and ordered columns
    column_mapping = get_duckdb_to_sheets_column_mapping()
    ordered_sheets_columns = get_ordered_columns_for_sheets()

    # Create mapping from DuckDB column index to sheets column name
    duckdb_to_sheets_idx = {}
    for i, duckdb_col in enumerate(cols):
        if duckdb_col in column_mapping:
            sheets_col = column_mapping[duckdb_col]
            duckdb_to_sheets_idx[sheets_col] = i

    # Create ordered data
    values = [ordered_sheets_columns]  # Header row

    # Process data rows
    for row in rows:
        ordered_row = []
        for sheets_col in ordered_sheets_columns:
            if sheets_col in duckdb_to_sheets_idx:
                duckdb_idx = duckdb_to_sheets_idx[sheets_col]
                cell_value = convert_cell_value(row[duckdb_idx])
                ordered_row.append(cell_value)
            else:
                # Handle missing columns gracefully
                ordered_row.append("")
        values.append(ordered_row)

    return values
