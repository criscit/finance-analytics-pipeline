# file: orchestration/assets_export_sheets.py
import os
from pathlib import Path
from typing import Any

from dagster import Output, asset, get_dagster_logger

from src.duckdb_utils import read_table_data_with_ordered_columns
from src.google_sheets import GoogleSheetsTableManager


def load_runtime_config() -> dict[str, Any]:
    """Read environment variables at runtime."""
    return {
        "google_spreadsheet_id": os.getenv("GOOGLE_SPREADSHEET_ID"),
        "google_sa_json_path": os.getenv(
            "GOOGLE_SA_JSON_PATH", "/app/credentials/finance-sheets-writer-prod-sa.json"
        ),
        "export_finance_table": os.getenv(
            "EXPORT_FINANCE_TABLE", "prod_imart.view_bank_transactions"
        ),
        "duckdb_path": os.getenv("DUCKDB_PATH", "/app/data/warehouse/analytics.duckdb"),
        "google_table_name": os.getenv("GOOGLE_TABLE_NAME", "Spendings Log"),
        "google_sheet_name": os.getenv("GOOGLE_SHEET_NAME", "Spendings"),
    }


@asset(deps=["run_ge_checkpoints"])
def export_to_google_sheets() -> Output[dict[str, int]]:
    """Export data from DuckDB to Google Sheets with table management."""
    log = get_dagster_logger()
    cfg = load_runtime_config()

    # Validate required environment variables
    if not cfg["google_spreadsheet_id"]:
        raise ValueError("GOOGLE_SPREADSHEET_ID environment variable is required but not set")
    if not Path(cfg["google_sa_json_path"]).exists():
        raise ValueError(f"Google service account file not found at: {cfg['google_sa_json_path']}")

    log.info("Exporting to Google Spreadsheet ID: %s", cfg["google_spreadsheet_id"])
    log.info("Using table name: %s", cfg["google_table_name"])
    log.info("Using sheet name: %s", cfg["google_sheet_name"])
    log.info("Using service account file: %s", cfg["google_sa_json_path"])

    # Initialize Google Sheets manager
    sheets_manager = GoogleSheetsTableManager(cfg["google_sa_json_path"])

    # Parse schema and table from the table name (format: schema.table)
    schema, table = cfg["export_finance_table"].split(".", 1)

    # Read all data from the DuckDB table with proper column ordering
    values = read_table_data_with_ordered_columns(cfg["duckdb_path"], schema, table)

    if not values:
        log.info("No data found in table %s", cfg["export_finance_table"])
        return Output({"appended": 0}, metadata={"appended": 0})

    # Append data to Google Sheets (creates sheet and table if needed)
    table_id = sheets_manager.append_rows(
        spreadsheet_id=cfg["google_spreadsheet_id"],
        sheet_name=cfg["google_sheet_name"],
        table_name=cfg["google_table_name"],
        sample_data=values,
    )

    log.info("Successfully exported %d rows to Google Sheets table: %s", len(values), table_id)
    return Output(
        {"appended": len(values)}, metadata={"appended": len(values), "table_id": table_id}
    )
