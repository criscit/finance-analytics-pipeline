# file: orchestration/assets_export_assets_sheets.py
import os
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, Output, asset

from src.duckdb_utils import read_assets_table_data_with_ordered_columns
from src.google_sheets import GoogleSheetsTableManager, SourceFilterConfig

# Source column index in the assets data (0-based): 13 = "Source" column
ASSETS_SOURCE_COLUMN_INDEX = 13
ASSETS_SOURCE_VALUE = "finance-analytics-pipeline"
ASSETS_NUM_COLUMNS = 15  # 15 columns including Source


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
def export_assets_to_google_sheets(context: AssetExecutionContext) -> Output[dict[str, int]]:
    """
    Export assets data from DuckDB to Google Sheets.

    Assets are a snapshot - this deletes all rows with source='finance-analytics-pipeline'
    and inserts fresh data. Other sources remain untouched.
    """
    cfg = load_runtime_config()

    # Validate required environment variables
    if not cfg["google_spreadsheet_id"]:
        raise ValueError(
            "FINANCE_GOOGLE_SPREADSHEET_ID environment variable is required but not set"
        )
    if not Path(cfg["google_sa_json_path"]).exists():
        raise ValueError(f"Google service account file not found at: {cfg['google_sa_json_path']}")

    context.log.info("Exporting assets to Google Spreadsheet ID: %s", cfg["google_spreadsheet_id"])
    context.log.info("Using table name: %s", cfg["google_table_name"])
    context.log.info("Using sheet name: %s", cfg["google_sheet_name"])
    context.log.info("Using service account file: %s", cfg["google_sa_json_path"])

    # Initialize Google Sheets manager
    sheets_manager = GoogleSheetsTableManager(cfg["google_sa_json_path"])

    # Parse schema and table from the table name (format: schema.table)
    schema, table = cfg["export_assets_table"].split(".", 1)

    # Read all data from the DuckDB assets table with proper column ordering
    raw_values = read_assets_table_data_with_ordered_columns(cfg["duckdb_path"], schema, table)

    # Ensure rows have Source column set to the expected value and match sheet width
    all_values: list[list[str]] = []
    for row in raw_values:
        padded_row = list(row)
        if len(padded_row) < ASSETS_NUM_COLUMNS:
            padded_row.extend([""] * (ASSETS_NUM_COLUMNS - len(padded_row)))
        padded_row[ASSETS_SOURCE_COLUMN_INDEX] = ASSETS_SOURCE_VALUE
        all_values.append(padded_row)

    if not all_values:
        context.log.info("No assets data found in table %s", cfg["export_assets_table"])
        return Output({"exported": 0}, metadata={"exported": 0})

    context.log.info("Found %d assets rows to export", len(all_values))

    # Delete existing rows with our source and insert new data
    source_filter = SourceFilterConfig(
        column_index=ASSETS_SOURCE_COLUMN_INDEX,
        value=ASSETS_SOURCE_VALUE,
        num_columns=ASSETS_NUM_COLUMNS,
    )
    exported_count = sheets_manager.replace_rows_by_source(
        spreadsheet_id=cfg["google_spreadsheet_id"],
        sheet_name=cfg["google_sheet_name"],
        source_filter=source_filter,
        new_data=all_values,
    )

    context.log.info(
        "Successfully exported %d assets rows to Google Sheets",
        exported_count,
    )
    return Output(
        {"exported": exported_count},
        metadata={
            "exported": exported_count,
        },
    )
