# file: orchestration/dagster_project/src/assets_export_sheets.py
import os, duckdb
from dagster import asset, get_dagster_logger, Output, MetadataValue
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

DB   = os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb")
SA   = os.getenv("GOOGLE_SA_JSON", "/app/credentials/finance-sheets-writer-prod-sa.json")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
RANGE    = os.getenv("GOOGLE_SHEET_RANGE", "Exports!A1")
TABLE    = os.getenv("EXPORT_TABLE", "marts.daily_snapshot")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def sheets_client():
    creds = Credentials.from_service_account_file(SA, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds).spreadsheets().values()

def get_bookmark(con):
    con.execute("""
      create table if not exists meta_export_bookmark(
        dataset text primary key,
        last_ts timestamp,
        last_id bigint
      )
    """)
    row = con.execute("select last_ts, last_id from meta_export_bookmark where dataset=?", [TABLE]).fetchone()
    if row:
        return row[0], row[1]
    return "1970-01-01 00:00:00", -1

def set_bookmark(con, last_ts, last_id):
    con.execute("""
      insert into meta_export_bookmark(dataset, last_ts, last_id)
      values (?,?,?)
      on conflict(dataset) do update set last_ts=excluded.last_ts, last_id=excluded.last_id
    """, [TABLE, last_ts, last_id])

@asset(deps=["run_ge_checkpoints"])
def export_to_google_sheets():
    log = get_dagster_logger()
    con = duckdb.connect(DB)
    last_ts, last_id = get_bookmark(con)

    query = f"""
      with src as (select * from {TABLE})
      select * from src
      where updated_at > TIMESTAMP '{last_ts}'
         or (updated_at = TIMESTAMP '{last_ts}' and id > {last_id})
      order by updated_at, id
    """
    rows = con.execute(query).fetchall()
    cols = [c[0] for c in con.execute(f"describe {TABLE}").fetchall()]

    if not rows:
        con.close()
        return Output({"appended": 0}, metadata={"appended": 0})

    # Prepare body (include header on first write if the sheet is empty)
    values = [cols] + [list(r) for r in rows]

    svc = sheets_client()
    svc.append(spreadsheetId=SHEET_ID, range=RANGE,
               valueInputOption="USER_ENTERED", body={"values": values}).execute()

    # Update bookmark to last row's (updated_at, id)
    last_updated_at, last_row_id = rows[-1][cols.index("updated_at")], rows[-1][cols.index("id")]
    set_bookmark(con, last_updated_at, last_row_id)
    con.close()

    return Output({"appended": len(rows)}, metadata={"appended": len(rows)})



