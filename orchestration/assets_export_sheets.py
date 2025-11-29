# file: orchestration/assets_export_sheets.py
import os
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, Output, asset

from src.duckdb_utils import read_table_data_with_ordered_columns
from src.google_sheets import GoogleSheetsTableManager
from src.utils import filter_new_transactions, get_max_transaction_datetimes_by_platform


def load_runtime_config() -> dict[str, Any]:
    """Read environment variables at runtime."""
    return {
        "google_spreadsheet_id": os.getenv("FINANCE_GOOGLE_SPREADSHEET_ID"),
        "google_sa_json_path": os.getenv(
            "GOOGLE_SA_JSON_PATH", "/app/credentials/finance-sheets-writer-prod-sa.json"
        ),
        "export_finance_table": os.getenv(
            "FINANCE_HISTORY_EXPORT_TABLE", "prod_imart.view_transactions"
        ),
        "duckdb_path": os.getenv("DUCKDB_PATH", "/app/data/warehouse/analytics.duckdb"),
        "google_table_name": os.getenv("FINANCE_HISTORY_GOOGLE_TABLE_NAME", "Spendings Log"),
        "google_sheet_name": os.getenv("FINANCE_HISTORY_GOOGLE_SHEET_NAME", "Spendings"),
    }


@asset(deps=["build_imart_models"])
def export_to_google_sheets(context: AssetExecutionContext) -> Output[dict[str, int]]:
    """Export data from DuckDB to Google Sheets with table management."""
    cfg = load_runtime_config()

    # Validate required environment variables
    if not cfg["google_spreadsheet_id"]:
        raise ValueError(
            "FINANCE_GOOGLE_SPREADSHEET_ID environment variable is required but not set"
        )
    if not Path(cfg["google_sa_json_path"]).exists():
        raise ValueError(f"Google service account file not found at: {cfg['google_sa_json_path']}")

    context.log.info("Exporting to Google Spreadsheet ID: %s", cfg["google_spreadsheet_id"])
    context.log.info("Using table name: %s", cfg["google_table_name"])
    context.log.info("Using sheet name: %s", cfg["google_sheet_name"])
    context.log.info("Using service account file: %s", cfg["google_sa_json_path"])

    # Initialize Google Sheets manager
    sheets_manager = GoogleSheetsTableManager(cfg["google_sa_json_path"])

    # Parse schema and table from the table name (format: schema.table)
    schema, table = cfg["export_finance_table"].split(".", 1)

    # Read all data from the DuckDB table with proper column ordering
    all_values = read_table_data_with_ordered_columns(cfg["duckdb_path"], schema, table)

    if not all_values:
        context.log.info("No data found in table %s", cfg["export_finance_table"])
        return Output({"appended": 0}, metadata={"appended": 0})

    # Read existing data from Google Sheets to get max transaction datetimes per platform
    context.log.info(
        "Reading existing data from Google Sheets to determine max transaction datetimes per platform"
    )
    existing_data = sheets_manager.read_existing_data(
        spreadsheet_id=cfg["google_spreadsheet_id"],
        sheet_name=cfg["google_sheet_name"],
        table_name=cfg["google_table_name"],
    )

    # Get max transaction datetimes by platform from existing data
    max_datetimes_by_platform = get_max_transaction_datetimes_by_platform(existing_data)
    context.log.info(
        "Found max transaction datetimes for %d platforms: %s",
        len(max_datetimes_by_platform),
        max_datetimes_by_platform,
    )

    # Filter new data to only include transactions with datetimes greater than max datetime per platform
    filtered_values = filter_new_transactions(all_values, max_datetimes_by_platform)

    if not filtered_values:
        context.log.info(
            "No new transactions to append (all transactions already exist or are older)"
        )
        return Output({"appended": 0}, metadata={"appended": 0, "filtered_from": len(all_values)})

    context.log.info(
        "Filtered %d new transactions from %d total transactions",
        len(filtered_values),
        len(all_values),
    )

    # Append filtered data to Google Sheets (creates sheet and table if needed)
    table_id = sheets_manager.append_rows(
        spreadsheet_id=cfg["google_spreadsheet_id"],
        sheet_name=cfg["google_sheet_name"],
        table_name=cfg["google_table_name"],
        sample_data=filtered_values,
    )

    context.log.info(
        "Successfully exported %d new rows to Google Sheets table: %s",
        len(filtered_values),
        table_id,
    )
    return Output(
        {"appended": len(filtered_values)},
        metadata={
            "appended": len(filtered_values),
            "filtered_from": len(all_values),
            "table_id": table_id,
        },
    )
