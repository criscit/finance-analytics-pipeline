# file: orchestration/dagster_project/src/assets_export_sheets.py
import os
from pathlib import Path
from typing import Any

import duckdb
from dagster import Output, asset, get_dagster_logger
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Environment variables will be read inside functions to ensure they're available at runtime

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def sheets_client() -> Any:
    sa_path = os.getenv(
        "GOOGLE_SA_JSON",
        "/app/credentials/finance-sheets-writer-prod-sa.json",
    )
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)  # type: ignore
    return build("sheets", "v4", credentials=creds).spreadsheets().values()


def get_bookmark(con: Any) -> tuple[str, str]:
    table = os.getenv("EXPORT_TABLE", "prod_imart.view_bank_transactions")
    con.execute(
        """
      create table if not exists prod_meta.export_bookmark(
        dataset text primary key,
        last_ts timestamp,
        last_id text
      )
    """,
    )
    row = con.execute(
        "select last_ts, last_id from prod_meta.export_bookmark where dataset=?",
        [table],
    ).fetchone()
    if row:
        return row[0], row[1]
    return "1970-01-01 00:00:00", ""


def set_bookmark(con: Any, last_ts: str, last_id: str) -> None:
    table = os.getenv("EXPORT_TABLE", "prod_imart.view_bank_transactions")
    con.execute(
        """
      insert into prod_meta.export_bookmark(dataset, last_ts, last_id)
      values (?,?,?)
      on conflict(dataset) do update set last_ts=excluded.last_ts, last_id=excluded.last_id
    """,
        [table, last_ts, last_id],
    )


@asset(deps=["run_ge_checkpoints"])
def export_to_google_sheets() -> Output[dict[str, int]]:
    log = get_dagster_logger()

    # Read environment variables at runtime
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    range_name = os.getenv("GOOGLE_SHEET_RANGE", "Exports!A1")
    sa_path = os.getenv(
        "GOOGLE_SA_JSON",
        "/app/credentials/finance-sheets-writer-prod-sa.json",
    )
    table = os.getenv("EXPORT_TABLE", "prod_imart.view_bank_transactions")
    db_path = os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb")

    # Validate required environment variables
    if not sheet_id:
        msg = "GOOGLE_SHEET_ID environment variable is required but not set"
        raise ValueError(msg)

    if not Path(sa_path).exists():
        msg = f"Google service account file not found at: {sa_path}"
        raise ValueError(msg)

    log.info("Exporting to Google Sheet ID: %s", sheet_id)
    log.info("Using range: %s", range_name)
    log.info("Using service account file: %s", sa_path)

    con = duckdb.connect(db_path)
    last_ts, last_id = get_bookmark(con)

    query = f"""
      with src as (select * from {table})
      select * from src
      where __ingested_at > timestamp '{last_ts}'
         or (__ingested_at = timestamp '{last_ts}' and transaction_bk > '{last_id}')
      order by __ingested_at, transaction_bk
    """
    rows = con.execute(query).fetchall()
    cols = [c[0] for c in con.execute(f"describe {table}").fetchall()]

    if not rows:
        con.close()
        return Output({"appended": 0}, metadata={"appended": 0})

    # Prepare body (include header on first write if the sheet is empty)
    # Convert datetime objects to strings for JSON serialization
    def convert_datetime_to_string(value: Any) -> str:
        if hasattr(value, "isoformat"):  # datetime object
            return str(value.isoformat())
        return str(value)

    values = [cols] + [[convert_datetime_to_string(cell) for cell in row] for row in rows]

    svc = sheets_client()
    svc.append(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()

    # Update bookmark to last row's (__ingested_at, transaction_bk)
    last_updated_at, last_row_id = (
        rows[-1][cols.index("__ingested_at")],
        rows[-1][cols.index("transaction_bk")],
    )
    set_bookmark(con, last_updated_at, last_row_id)
    con.close()

    return Output({"appended": len(rows)}, metadata={"appended": len(rows)})
