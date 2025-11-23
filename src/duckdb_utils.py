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


def read_table_data_with_ordered_columns_generic(
    db_path: str,
    schema: str,
    table: str,
    column_mapping: dict[str, str],
    order_by: str | None = None,
) -> list[list[str]]:
    """
    Read table data with proper column ordering for Google Sheets using a custom column mapping.

    Args:
        db_path: Path to DuckDB database
        schema: Schema name
        table: Table name
        column_mapping: Dictionary mapping DuckDB column names to Google Sheets column names
        order_by: Optional column name to sort results by (e.g., 'transacted_at')

    Returns:
        List of rows with properly ordered columns for Google Sheets
    """
    # Build the SELECT query with ordered columns
    select_columns = []
    for duckdb_col, sheets_col in column_mapping.items():
        select_columns.append(f"{qident(duckdb_col)} as {qident(sheets_col)}")

    if not select_columns:
        raise ValueError(f"No matching columns found for table {qtable(schema, table)}")

    query = f"select {', '.join(select_columns)} from {qtable(schema, table)}"
    if order_by:
        query += f" order by {qident(order_by)}"

    with connect_readonly(db_path) as con:
        rows = con.execute(query).fetchall()

    # Convert to list of lists with proper formatting
    values = []
    for row in rows:
        converted_row = [convert_cell_value(cell) for cell in row]
        values.append(converted_row)

    return values


def read_table_data_with_ordered_columns(db_path: str, schema: str, table: str) -> list[list[str]]:
    """Read transaction table data with proper column ordering for Google Sheets.

    Results are sorted by transacted_at column.
    """
    column_mapping = get_duckdb_to_sheets_column_mapping()
    return read_table_data_with_ordered_columns_generic(
        db_path, schema, table, column_mapping, order_by="transacted_at"
    )


def read_assets_table_data_with_ordered_columns(
    db_path: str, schema: str, table: str
) -> list[list[str]]:
    """Read assets table data with proper column ordering for Google Sheets."""
    column_mapping = get_duckdb_to_sheets_assets_column_mapping()
    return read_table_data_with_ordered_columns_generic(db_path, schema, table, column_mapping)


def convert_cell_value(value: Any) -> str:
    """Convert cell value to string for Google Sheets."""
    from datetime import date, datetime

    if value is None:
        return ""

    # Handle datetime objects - extract just the date part
    if isinstance(value, datetime):
        return value.date().isoformat()

    # Handle date objects - format as YYYY-MM-DD
    if isinstance(value, date):
        return value.isoformat()

    # Handle other objects with isoformat (e.g., time)
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
    """Get the ordered column names for Google Sheets transactions export."""
    return [
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


def get_duckdb_to_sheets_column_mapping() -> dict[str, str]:
    """
    Get mapping from DuckDB column names to Google Sheets column names for transactions.

    Maps the imart column names (snake_case) to display names for Google Sheets.
    """
    return {
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


def get_ordered_columns_for_assets_sheets() -> list[str]:
    """Get the ordered column names for Google Sheets assets export."""
    return [
        "Type",
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
        "APY, %",
        "Source",
        "Comments",
    ]


def get_duckdb_to_sheets_assets_column_mapping() -> dict[str, str]:
    """
    Get mapping from DuckDB column names to Google Sheets column names for assets.

    Maps the imart column names (snake_case) to display names for Google Sheets.
    """
    return {
        "type": "Type",
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
        "apy_pct": "APY, %",
        "source": "Source",
        "comments": "Comments",
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


def get_recently_ingested_tables(db_path: str, hours: int = 24) -> dict[str, Any]:
    """
    Get list of tables that were ingested in the last N hours.

    Args:
        db_path: Path to DuckDB database
        hours: Number of hours to look back (default 24)

    Returns:
        Dictionary with:
        - tables_to_process: List of dicts with source_system_nm, table_nm, selector_nm, file_count, latest_ingestion
        - total_tables: Count of tables
        - detected_at: ISO timestamp
    """
    con = connect_readonly(db_path)

    try:
        # Check if ingest_ledger exists
        ledger_exists = con.execute(
            """
            select 1
            from information_schema.tables
            where table_schema = 'prod_meta'
                and table_name = 'ingest_ledger'
            """
        ).fetchone()

        if not ledger_exists:
            return {
                "tables_to_process": [],
                "total_tables": 0,
                "detected_at": None,
            }

        # Get recent ingestions
        recent_ingestions = con.execute(
            f"""
            select
                source_system_nm,
                table_nm,
                count(*) as file_count,
                max(processed_at) as latest_ingestion
            from prod_meta.ingest_ledger
            where processed_at >= current_timestamp - interval '{hours} hours'
            group by source_system_nm, table_nm
            order by latest_ingestion desc
            """
        ).fetchall()

        # Build result list
        tables_to_process = []
        for row in recent_ingestions:
            source_system_nm, table_nm, file_count, latest_ingestion = row
            tables_to_process.append(
                {
                    "source_system_nm": source_system_nm,
                    "table_nm": table_nm,
                    "selector_nm": f"run_{table_nm}",
                    "file_count": file_count,
                    "latest_ingestion": latest_ingestion.isoformat() if latest_ingestion else None,
                }
            )

        from datetime import datetime

        return {
            "tables_to_process": tables_to_process,
            "total_tables": len(tables_to_process),
            "detected_at": datetime.now().isoformat(),
        }

    finally:
        con.close()


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
            source_system_nm varchar(32),
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


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    """
    Check if a table exists in the database.

    Args:
        con: DuckDB connection
        schema: Schema name
        table: Table name

    Returns:
        True if table exists, False otherwise
    """
    result = con.execute(
        """
        select 1
        from information_schema.tables
        where table_schema = ?
            and table_name = ?
        """,
        [schema, table],
    ).fetchone()
    return result is not None


def restore_raw_table_from_parquets(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    parquet_dir: str,
) -> int:
    """
    Restore a raw table from all cached parquet files.

    This is an idempotent recovery mechanism that rebuilds raw tables
    from the parquet cache when tables are missing but data exists.

    Args:
        con: DuckDB connection
        schema: Target schema name (typically 'prod_raw')
        table: Target table name
        parquet_dir: Directory containing cached parquet files for this table

    Returns:
        Total number of rows restored

    Raises:
        FileNotFoundError: If parquet_dir doesn't exist or has no parquet files
    """
    from pathlib import Path

    parquet_path = Path(parquet_dir)
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")

    parquet_files = list(parquet_path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in: {parquet_dir}")

    # Drop existing table to ensure clean state
    con.execute(f"drop table if exists {qtable(schema, table)}")

    total_rows = 0
    for parquet_file in parquet_files:
        rows = ingest_parquet_to_duckdb(con, schema, table, str(parquet_file))
        total_rows += rows

    return total_rows


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
