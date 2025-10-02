"""Google Sheets table management module."""

import uuid
from typing import Any, ClassVar

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


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
            print(f"Error getting or creating sheet: {e}")
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
            print(f"Error finding table: {e}")
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
                                "endColumnIndex": 6,
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
                                    "columnName": "Bank Name",
                                    "columnType": "DROPDOWN",
                                    "dataValidationRule": {
                                        "condition": {
                                            "type": "ONE_OF_LIST",
                                            "values": [
                                                {"userEnteredValue": "Chas"},
                                                {"userEnteredValue": "Sberbank"},
                                                {"userEnteredValue": "Tinkoff"},
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
            print(f"Error creating table structure: {e}")
            raise

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
                print(f"Table '{table_name}' already exists with ID: {existing_table_id}")
                table_id = existing_table_id
            else:
                # Create the table
                table_id = self.create_table(spreadsheet_id, sheet_id, table_name)
                print(f"Created new table '{table_name}' with ID: {table_id}")

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

            # Use append to add data to the end of the table
            range_name = f"{actual_sheet_name}!A:F"

            body = {"values": values}

            (
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

            print(f"Appended {len(sample_data)} rows of data to table '{table_name}'")
            return table_id

        except Exception as e:
            print(f"Error appending data: {e}")
            raise
