# file: orchestration/dagster_project/src/assets_export_sheets.py
import os
from pathlib import Path
from typing import Any

import duckdb
from dagster import Output, asset, get_dagster_logger
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ----------------------------
# Config & clients
# ----------------------------
def load_runtime_config() -> dict[str, Any]:
    """Read environment variables at runtime."""
    return {
        "sheet_id": os.getenv("GOOGLE_SHEET_ID"),
        "range_name": os.getenv("GOOGLE_SHEET_RANGE", "Exports!A1"),
        "sa_path": os.getenv(
            "GOOGLE_SA_JSON", "/app/credentials/finance-sheets-writer-prod-sa.json"
        ),
        "table": os.getenv("EXPORT_TABLE", "prod_imart.view_bank_transactions"),
        "db_path": os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb"),
    }


def sheets_values_client(sa_path: str) -> Any:
    """Create Google Sheets API client."""
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)  # type: ignore
    return build("sheets", "v4", credentials=creds).spreadsheets().values()


# ----------------------------
# DuckDB: READ-ONLY functions
# ----------------------------
def _connect_ro(db_path: str) -> duckdb.DuckDBPyConnection:
    """Create read-only DuckDB connection."""
    return duckdb.connect(database=db_path, read_only=True)


def read_table_columns(db_path: str, table: str) -> list[str]:
    """Get column names for the specified table."""
    with _connect_ro(db_path) as con:
        rows = con.execute(f"describe {table}").fetchall()
    return [r[0] for r in rows]


def read_all_rows(db_path: str, table: str) -> tuple[list[tuple[Any, ...]], list[str]]:
    """Read all rows from the specified table."""
    cols = read_table_columns(db_path, table)
    query = f"select * from {table}"

    with _connect_ro(db_path) as con:
        rows = con.execute(query).fetchall()
    return rows, cols


# ----------------------------
# Google Sheets: WRITE functions
# ----------------------------
def append_to_sheet(sa_path: str, sheet_id: str, range_name: str, values: list[list[Any]]) -> None:
    """Append data to Google Sheets."""
    svc = sheets_values_client(sa_path)
    svc.append(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


# ----------------------------
# Pure helpers (no I/O)
# ----------------------------
def convert_cell(value: Any) -> str:
    """Convert cell value to string for Google Sheets."""
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


# ----------------------------
# Main Asset
# ----------------------------
@asset(deps=["run_ge_checkpoints"])
def export_to_google_sheets() -> Output[dict[str, int]]:
    """Export data from DuckDB to Google Sheets."""
    log = get_dagster_logger()
    cfg = load_runtime_config()

    # Validate required environment variables
    if not cfg["sheet_id"]:
        raise ValueError("GOOGLE_SHEET_ID environment variable is required but not set")
    if not Path(cfg["sa_path"]).exists():
        raise ValueError(f"Google service account file not found at: {cfg['sa_path']}")

    log.info("Exporting to Google Sheet ID: %s", cfg["sheet_id"])
    log.info("Using range: %s", cfg["range_name"])
    log.info("Using service account file: %s", cfg["sa_path"])

    # Read all data from the table
    rows, cols = read_all_rows(cfg["db_path"], cfg["table"])

    if not rows:
        log.info("No data found in table %s", cfg["table"])
        return Output({"appended": 0}, metadata={"appended": 0})

    # Prepare values (header + rows)
    values = [cols] + [[convert_cell(cell) for cell in row] for row in rows]

    # Write to Google Sheets
    append_to_sheet(cfg["sa_path"], cfg["sheet_id"], cfg["range_name"], values)

    log.info("Successfully exported %d rows to Google Sheets", len(rows))
    return Output({"appended": len(rows)}, metadata={"appended": len(rows)})
