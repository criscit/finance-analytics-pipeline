"""Database utilities for the finance analytics pipeline."""

from pathlib import Path
from typing import Any

import duckdb

from .utils import qtable


def get_file_columns(con: duckdb.DuckDBPyConnection, path: Path) -> list[str]:
    """Get column names from a CSV file."""
    rows = con.execute(
        "describe select * from read_csv_auto(?, header=true)",
        [str(path)],
    ).fetchall()
    return [r[0] for r in rows]


def get_table_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    """Get column names from a database table."""
    rows = con.execute(f"pragma table_info({qtable(schema, table)})").fetchall()
    return [r[1] for r in rows]


def read_table_columns(db_path: str, table: str) -> list[str]:
    """Get column names for the specified table."""
    with duckdb.connect(db_path, read_only=True) as con:
        rows = con.execute(f"describe {table}").fetchall()
    return [r[0] for r in rows]


def read_all_rows(db_path: str, table: str) -> tuple[list[tuple[Any, ...]], list[str]]:
    """Read all rows from the specified table."""
    cols = read_table_columns(db_path, table)
    query = f"select * from {table}"

    with duckdb.connect(db_path, read_only=True) as con:
        rows = con.execute(query).fetchall()
    return rows, cols
