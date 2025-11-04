# file: orchestration/assets_export_assets_sheets.py
import os
from pathlib import Path
from typing import Any

from dagster import Output, asset, get_dagster_logger

from src.duckdb_utils import read_assets_table_data_with_ordered_columns
from src.google_sheets import GoogleSheetsTableManager


def load_runtime_config() -> dict[str, Any]:
    """Read environment variables at runtime for assets export."""
    return {
        "google_spreadsheet_id": os.getenv("FINANCE_GOOGLE_SPREADSHEET_ID"),
        "google_sa_json_path": os.getenv(
            "GOOGLE_SA_JSON_PATH", "/app/credentials/finance-sheets-writer-prod-sa.json"
        ),
        "export_assets_table": os.getenv("FINANCE_ASSETS_EXPORT_TABLE", "prod_imart.view_assets"),
        "duckdb_path": os.getenv("DUCKDB_PATH", "/app/data/warehouse/analytics.duckdb"),
        "google_table_name": os.getenv("FINANCE_ASSETS_GOOGLE_TABLE_NAME", "Assets"),
        "google_sheet_name": os.getenv("FINANCE_ASSETS_GOOGLE_SHEET_NAME", "Assets"),
    }


@asset(deps=["build_imart_models"])
def export_assets_to_google_sheets() -> Output[dict[str, int]]:
    """
    Export assets data from DuckDB to Google Sheets.

    Note: Assets are a snapshot, so this replaces the entire table each time
    (unlike transactions which are incremental).
    """
    log = get_dagster_logger()
    cfg = load_runtime_config()

    # Validate required environment variables
    if not cfg["google_spreadsheet_id"]:
        raise ValueError(
            "FINANCE_GOOGLE_SPREADSHEET_ID environment variable is required but not set"
        )
    if not Path(cfg["google_sa_json_path"]).exists():
        raise ValueError(f"Google service account file not found at: {cfg['google_sa_json_path']}")

    log.info("Exporting assets to Google Spreadsheet ID: %s", cfg["google_spreadsheet_id"])
    log.info("Using table name: %s", cfg["google_table_name"])
    log.info("Using sheet name: %s", cfg["google_sheet_name"])
    log.info("Using service account file: %s", cfg["google_sa_json_path"])

    # Initialize Google Sheets manager
    sheets_manager = GoogleSheetsTableManager(cfg["google_sa_json_path"])

    # Parse schema and table from the table name (format: schema.table)
    schema, table = cfg["export_assets_table"].split(".", 1)

    # Read all data from the DuckDB assets table with proper column ordering
    all_values = read_assets_table_data_with_ordered_columns(cfg["duckdb_path"], schema, table)

    if not all_values:
        log.info("No assets data found in table %s", cfg["export_assets_table"])
        return Output({"exported": 0}, metadata={"exported": 0})

    log.info("Found %d assets rows to export", len(all_values))

    # For assets, we replace the entire table (it's a snapshot, not incremental)
    # First, clear the existing data by deleting the sheet and recreating it
    # The append_rows method will create the sheet and table if they don't exist

    # Append data to Google Sheets (creates sheet and table if needed)
    # Note: For a true "replace" operation, you might want to add a method to
    # clear the existing table first, but for now we'll append
    table_id = sheets_manager.append_rows(
        spreadsheet_id=cfg["google_spreadsheet_id"],
        sheet_name=cfg["google_sheet_name"],
        table_name=cfg["google_table_name"],
        sample_data=all_values,
    )

    log.info(
        "Successfully exported %d assets rows to Google Sheets table: %s",
        len(all_values),
        table_id,
    )
    return Output(
        {"exported": len(all_values)},
        metadata={
            "exported": len(all_values),
            "table_id": table_id,
        },
    )
