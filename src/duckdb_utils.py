"""DuckDB database utilities module."""

from typing import Any

import duckdb

from src.utils import build_load_key_expr, qident, qtable


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


def ensure_meta_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create metadata schema and tables if they don't exist.

    Creates:
        - prod_meta schema
        - prod_raw schema
        - prod_meta.ingest_ledger table for tracking ingested files
    """
    con.execute("create schema if not exists prod_meta;")
    con.execute("create schema if not exists prod_raw;")
    con.execute(
        """
        create table if not exists prod_meta.ingest_ledger (
            bank_nm varchar(32),
            table_nm varchar(128),
            file_path varchar(512),
            file_size bigint,
            md5 varchar(32),
            processed_at timestamp,
            archived_at timestamp
        )
        """
    )


def ensure_raw_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    file_cols: list[str],
) -> None:
    """
    Create raw table with TEXT-only columns plus __load_key and processed_at.

    Args:
        con: DuckDB connection
        schema: Schema name (typically 'prod_raw')
        table: Table name
        file_cols: List of column names from source file
    """
    cols_ddl = ", ".join(f"{qident(c)} text" for c in file_cols)
    con.execute(f"create schema if not exists {qident(schema)};")
    con.execute(
        f"""
        create table if not exists {qtable(schema, table)} (
          __load_key varchar(32),
          {cols_ddl},
          processed_at timestamp
        );
        """
    )
    con.execute(f"create index if not exists ix_{table}_bk on {qtable(schema, table)}(__load_key);")


def table_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    """
    Get column names from a database table using pragma table_info.

    Args:
        con: DuckDB connection
        schema: Schema name
        table: Table name

    Returns:
        List of column names
    """
    rows = con.execute(f"pragma table_info({qtable(schema, table)})").fetchall()
    return [r[1] for r in rows]  # (cid, name, type, notnull, dflt, pk)


def ingest_parquet_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    parquet_path: str,
) -> int:
    """
    Insert DISTINCT rows from parquet into raw table, aligning columns and casting to text.

    The __load_key is derived via build_load_key_expr to create deterministic row hashes.

    Args:
        con: DuckDB connection
        schema: Target schema name
        table: Target table name
        parquet_path: Path to source parquet file

    Returns:
        Number of rows inserted
    """
    read_expr = f"read_parquet('{parquet_path}')"
    file_cols = [r[0] for r in con.execute(f"describe select * from {read_expr}").fetchall()]

    ensure_raw_table(con, schema, table, file_cols)

    tgt_cols = table_columns(con, schema, table)  # includes __load_key, processed_at
    load_key_expr = build_load_key_expr(file_cols)

    select_list = []
    for c in tgt_cols:
        if c == "__load_key":
            select_list.append(f"{load_key_expr} as {qident(c)}")
        elif c == "processed_at":
            select_list.append(f"(current_timestamp at time zone 'UTC') as {qident(c)}")
        else:
            select_list.append(
                f"cast({qident(c)} as text) as {qident(c)}"
                if c in file_cols
                else f"NULL as {qident(c)}"
            )

    result = con.execute(
        f"""
        insert into {qtable(schema, table)}
        select distinct {", ".join(select_list)}
        from {read_expr};
        """
    )
    return int(result.rowcount)
