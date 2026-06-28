"""Google Sheets table management module."""

import uuid
from dataclasses import dataclass
from typing import Any, ClassVar

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from src.logging_config import logger


@dataclass
class SourceFilterConfig:
    """Configuration for source-based row filtering operations."""

    column_index: int
    value: str
    num_columns: int = 14


class GoogleSheetsTableManager:
    """Manages Google Sheets table operations with a single service instance."""

    SCOPES: ClassVar[list[str]] = ["https://www.googleapis.com/auth/spreadsheets"]

    def __init__(self, credentials_path: str):
        """Initialize the Google Sheets service."""
        self.creds = Credentials.from_service_account_file(credentials_path, scopes=self.SCOPES)  # type: ignore[no-untyped-call]
        self.sheets_service = build("sheets", "v4", credentials=self.creds)

    def get_or_create_sheet(self, spreadsheet_id: str, sheet_name: str) -> int:
        """
        Get existing sheet ID or create a new sheet if it doesn't exist.

        Args:
            spreadsheet_id (str): The ID of the Google Spreadsheet
            sheet_name (str): Name of the sheet to get or create

        Returns:
            int: Sheet ID
        """
        try:
            ss = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheet = next((s for s in ss["sheets"] if s["properties"]["title"] == sheet_name), None)

            if sheet:
                return int(sheet["properties"]["sheetId"])
            r = (
                self.sheets_service.spreadsheets()
                .batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
                )
                .execute()
            )
            return int(r["replies"][0]["addSheet"]["properties"]["sheetId"])

        except Exception as e:
            logger.error("Error getting or creating sheet: {}", e)
            raise

    def find_table_by_name(self, spreadsheet_id: str, sheet_id: int, table_name: str) -> str | None:
        """
        Find a table with the given name in the specified sheet.

        Args:
            spreadsheet_id (str): The ID of the Google Spreadsheet
            sheet_id (int): The ID of the sheet to check
            table_name (str): Name of the table to check for

        Returns:
            str or None: Table ID if table exists, None otherwise
        """
        try:
            spreadsheet = (
                self.sheets_service.spreadsheets()
                .get(spreadsheetId=spreadsheet_id, includeGridData=False)
                .execute()
            )

            # Check each sheet for tables
            for sheet in spreadsheet.get("sheets", []):
                if sheet["properties"]["sheetId"] == sheet_id:
                    tables = sheet.get("tables", [])

                    # Check if any table has the matching name
                    for table in tables:
                        if table.get("name") == table_name:
                            return str(table.get("tableId"))

            return None

        except Exception as e:
            logger.error("Error finding table: {}", e)
            return None

    def create_table(self, spreadsheet_id: str, sheet_id: int, table_name: str) -> str:
        """
        Create a table structure with predefined columns for financial data.

        Args:
            spreadsheet_id (str): The ID of the Google Spreadsheet
            sheet_id (int): The ID of the sheet
            table_name (str): Name of the table to create

        Returns:
            str: Table ID of the created table
        """
        # Generate a unique table ID
        table_id = f"table_{uuid.uuid4().hex[:8]}"

        add_table_req = {
            "requests": [
                {"clearBasicFilter": {"sheetId": sheet_id}},
                {
                    "addTable": {
                        "table": {
                            "name": table_name,
                            "tableId": table_id,
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": 0,
                                "startColumnIndex": 0,
                                "endColumnIndex": 12,
                            },
                            "columnProperties": [
                                {
                                    "columnIndex": 0,
                                    "columnName": "Date",
                                    "columnType": "DATE",
                                    "format": {
                                        "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
                                    },
                                },
                                {
                                    "columnIndex": 1,
                                    "columnName": "Platform Name",
                                    "columnType": "DROPDOWN",
                                    "dataValidationRule": {
                                        "condition": {
                                            "type": "ONE_OF_LIST",
                                            "values": [
                                                {"userEnteredValue": "T Bank"},
                                                {"userEnteredValue": "BakAi Bank"},
                                                {"userEnteredValue": "Binance"},
                                                {"userEnteredValue": "Bybit"},
                                                {"userEnteredValue": "Telegram Wallet"},
                                            ],
                                        },
                                    },
                                },
                                {"columnIndex": 2, "columnName": "Category", "columnType": "TEXT"},
                                {
                                    "columnIndex": 3,
                                    "columnName": "Description",
                                    "columnType": "TEXT",
                                },
                                {
                                    "columnIndex": 4,
                                    "columnName": "Amount, Currency",
                                    "columnType": "DOUBLE",
                                    "format": {
                                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}
                                    },
                                },
                                {"columnIndex": 5, "columnName": "Currency", "columnType": "TEXT"},
                                {
                                    "columnIndex": 6,
                                    "columnName": "Amount, RUB",
                                    "columnType": "DOUBLE",
                                    "format": {
                                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}
                                    },
                                },
                                {
                                    "columnIndex": 7,
                                    "columnName": "Amount, USD",
                                    "columnType": "DOUBLE",
                                    "format": {
                                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}
                                    },
                                },
                                {
                                    "columnIndex": 8,
                                    "columnName": "Executed Rate, RUB",
                                    "columnType": "DOUBLE",
                                    "format": {
                                        "numberFormat": {
                                            "type": "NUMBER",
                                            "pattern": "#,##0.000000",
                                        }
                                    },
                                },
                                {
                                    "columnIndex": 9,
                                    "columnName": "Executed Rate, USD",
                                    "columnType": "DOUBLE",
                                    "format": {
                                        "numberFormat": {
                                            "type": "NUMBER",
                                            "pattern": "#,##0.000000",
                                        }
                                    },
                                },
                                {
                                    "columnIndex": 10,
                                    "columnName": "Close Rate, RUB",
                                    "columnType": "DOUBLE",
                                    "format": {
                                        "numberFormat": {
                                            "type": "NUMBER",
                                            "pattern": "#,##0.000000",
                                        }
                                    },
                                },
                                {
                                    "columnIndex": 11,
                                    "columnName": "Close Rate, USD",
                                    "columnType": "DOUBLE",
                                    "format": {
                                        "numberFormat": {
                                            "type": "NUMBER",
                                            "pattern": "#,##0.000000",
                                        }
                                    },
                                },
                            ],
                        }
                    }
                },
            ]
        }

        try:
            reply = (
                self.sheets_service.spreadsheets()
                .batchUpdate(
                    spreadsheetId=spreadsheet_id, body={"requests": add_table_req["requests"]}
                )
                .execute()
            )
            return str(reply["replies"][1]["addTable"]["table"]["tableId"])

        except Exception as e:
            logger.error("Error creating table structure: {}", e)
            raise

    def _format_date_column(self, spreadsheet_id: str, sheet_id: int, updated_range: str) -> None:
        """
        Apply date formatting to column A for the updated range.

        Args:
            spreadsheet_id: The ID of the Google Spreadsheet
            sheet_id: The ID of the sheet
            updated_range: The range that was just updated (e.g., "Sheet1!A2:F10")
        """
        try:
            # Parse the updated range to get row numbers
            # Format: "SheetName!A2:F10"
            if "!" in updated_range:
                range_part = updated_range.split("!")[1]
                # Extract start and end row numbers
                # Range format: A2:F10
                start_cell = range_part.split(":")[0]
                end_cell = range_part.split(":")[1]

                # Extract row numbers (e.g., "A2" -> 2, "F10" -> 10)
                import re

                start_match = re.search(r"\d+", start_cell)
                end_match = re.search(r"\d+", end_cell)
                if not start_match or not end_match:
                    raise ValueError(f"Invalid range format: {updated_range}")

                start_row = int(start_match.group())
                end_row = int(end_match.group())

                # Apply date format to column A for the updated rows
                # Row indices are 0-based in the API
                format_request = {
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": start_row - 1,
                                    "endRowIndex": end_row,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": 1,
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
                                    }
                                },
                                "fields": "userEnteredFormat.numberFormat",
                            }
                        }
                    ]
                }

                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id, body=format_request
                ).execute()

                logger.info("Applied date formatting to rows {} to {}", start_row, end_row)
        except Exception as e:
            logger.warning("Could not apply date formatting: {}", e)
            # Don't raise - formatting is nice to have but not critical

    def read_existing_data(
        self, spreadsheet_id: str, sheet_name: str, table_name: str
    ) -> list[list[str]]:
        """
        Read existing data from Google Sheets table.

        Args:
            spreadsheet_id (str): The ID of the Google Spreadsheet
            sheet_name (str): Name of the sheet
            table_name (str): Name of the table to read from

        Returns:
            list[list[str]]: Existing data rows (without headers)
        """
        try:
            # Get or create sheet
            sheet_id = self.get_or_create_sheet(spreadsheet_id, sheet_name)

            # Check if table exists
            existing_table_id = self.find_table_by_name(spreadsheet_id, sheet_id, table_name)
            if not existing_table_id:
                logger.info("Table '{}' does not exist, returning empty data", table_name)
                return []

            # Get the sheet name for reading data
            spreadsheet = (
                self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            )
            actual_sheet_name = None
            for sheet in spreadsheet.get("sheets", []):
                if sheet["properties"]["sheetId"] == sheet_id:
                    actual_sheet_name = sheet["properties"]["title"]
                    break

            if not actual_sheet_name:
                raise Exception(f"Could not find sheet with ID {sheet_id}")

            # Read all data from the sheet (12 columns: A-L)
            range_name = f"{actual_sheet_name}!A:L"
            result = (
                self.sheets_service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=range_name)
                .execute()
            )

            values = result.get("values", [])
            if not values:
                return []

            # Skip header row and return data rows
            return values[1:] if len(values) > 1 else []

        except Exception as e:
            logger.error("Error reading existing data: {}", e)
            return []

    def append_rows(
        self, spreadsheet_id: str, sheet_name: str, table_name: str, sample_data: list[list[Any]]
    ) -> str:
        """
        Get or create table and append data to it.

        Args:
            spreadsheet_id (str): The ID of the Google Spreadsheet
            sheet_name (str): Name of the sheet to create/use
            table_name (str): Name of the table to create
            sample_data (List[List[Any]]): Data to append

        Returns:
            str: Table ID
        """
        try:
            # Get or create sheet
            sheet_id = self.get_or_create_sheet(spreadsheet_id, sheet_name)

            # Check if table already exists
            existing_table_id = self.find_table_by_name(spreadsheet_id, sheet_id, table_name)
            if existing_table_id:
                logger.info("Table '{}' already exists with ID: {}", table_name, existing_table_id)
                table_id = existing_table_id
            else:
                # Create the table
                table_id = self.create_table(spreadsheet_id, sheet_id, table_name)
                logger.info("Created new table '{}' with ID: {}", table_name, table_id)

            # Get the sheet name for appending data
            spreadsheet = (
                self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            )
            actual_sheet_name = None
            for sheet in spreadsheet.get("sheets", []):
                if sheet["properties"]["sheetId"] == sheet_id:
                    actual_sheet_name = sheet["properties"]["title"]
                    break

            if not actual_sheet_name:
                raise Exception(f"Could not find sheet with ID {sheet_id}")

            # Convert data to the format expected by Google Sheets API
            values = []
            for row_data in sample_data:
                values.append(row_data)

            # Use append to add data to the end of the table (12 columns: A-L)
            range_name = f"{actual_sheet_name}!A:L"

            body = {"values": values}

            result = (
                self.sheets_service.spreadsheets()
                .values()
                .append(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body=body,
                )
                .execute()
            )

            # Apply date formatting to column A (Date column) for all data rows
            # Get the range of rows that were just appended
            updated_range = result.get("updates", {}).get("updatedRange", "")
            if updated_range:
                # Format the date column (column A) with proper date format
                self._format_date_column(spreadsheet_id, sheet_id, updated_range)

            logger.info("Appended {} rows of data to table '{}'", len(sample_data), table_name)
            return table_id

        except Exception as e:
            logger.error("Error appending data: {}", e)
            raise

    def delete_rows_by_source(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        source_column_index: int,
        source_value: str,
    ) -> int:
        """
        Delete all rows where the source column matches the given value.

        Args:
            spreadsheet_id: The ID of the Google Spreadsheet
            sheet_name: Name of the sheet
            source_column_index: 0-based index of the source column
            source_value: Value to match for deletion

        Returns:
            int: Number of rows deleted
        """
        try:
            sheet_id = self.get_or_create_sheet(spreadsheet_id, sheet_name)

            # Get the actual sheet name
            spreadsheet = (
                self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            )
            actual_sheet_name = None
            for sheet in spreadsheet.get("sheets", []):
                if sheet["properties"]["sheetId"] == sheet_id:
                    actual_sheet_name = sheet["properties"]["title"]
                    break

            if not actual_sheet_name:
                raise Exception(f"Could not find sheet with ID {sheet_id}")

            # Read all data from the sheet
            range_name = f"{actual_sheet_name}!A:Z"
            result = (
                self.sheets_service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=range_name)
                .execute()
            )

            values = result.get("values", [])
            if not values or len(values) <= 1:
                logger.info("No data rows to delete")
                return 0

            # Find rows to delete (match source value), collect row indices in reverse order
            rows_to_delete = []
            for i, row in enumerate(values[1:], start=2):  # Start from 2 (skip header, 1-indexed)
                if len(row) > source_column_index and row[source_column_index] == source_value:
                    rows_to_delete.append(i)

            if not rows_to_delete:
                logger.info("No rows found with source='{}'", source_value)
                return 0

            # Delete rows in reverse order to maintain correct indices
            rows_to_delete.sort(reverse=True)
            delete_requests = []
            for row_num in rows_to_delete:
                delete_requests.append(
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": row_num - 1,  # 0-indexed
                                "endIndex": row_num,
                            }
                        }
                    }
                )

            if delete_requests:
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id, body={"requests": delete_requests}
                ).execute()

            logger.info("Deleted {} rows with source='{}'", len(rows_to_delete), source_value)
            return len(rows_to_delete)

        except Exception as e:
            logger.error("Error deleting rows by source: {}", e)
            raise

    def replace_rows_by_source(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        source_filter: SourceFilterConfig,
        new_data: list[list[Any]],
    ) -> int:
        """
        Delete all rows with matching source value and insert new rows.

        Args:
            spreadsheet_id: The ID of the Google Spreadsheet
            sheet_name: Name of the sheet
            source_filter: Configuration for source column filtering
            new_data: New rows to insert (without header)

        Returns:
            int: Number of rows inserted
        """
        # Delete existing rows with this source
        deleted = self.delete_rows_by_source(
            spreadsheet_id, sheet_name, source_filter.column_index, source_filter.value
        )
        logger.info("Deleted {} existing rows before inserting new data", deleted)

        if not new_data:
            logger.info("No new data to insert")
            return 0

        # Get sheet info
        sheet_id = self.get_or_create_sheet(spreadsheet_id, sheet_name)
        spreadsheet = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        actual_sheet_name = None
        for sheet in spreadsheet.get("sheets", []):
            if sheet["properties"]["sheetId"] == sheet_id:
                actual_sheet_name = sheet["properties"]["title"]
                break

        if not actual_sheet_name:
            raise Exception(f"Could not find sheet with ID {sheet_id}")

        # Append new data
        col_letter = chr(ord("A") + source_filter.num_columns - 1)
        range_name = f"{actual_sheet_name}!A:{col_letter}"

        self.sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": new_data},
        ).execute()

        logger.info("Inserted {} new rows with source='{}'", len(new_data), source_filter.value)
        return len(new_data)
