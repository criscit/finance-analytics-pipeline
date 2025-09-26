# file: orchestration/dagster_project/src/assets_export_sheets.py
import os, duckdb
from dagster import asset, get_dagster_logger, Output, MetadataValue
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Environment variables will be read inside functions to ensure they're available at runtime

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def sheets_client():
    sa_path = os.getenv("GOOGLE_SA_JSON", "/app/credentials/finance-sheets-writer-prod-sa.json")
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds).spreadsheets().values()

def get_bookmark(con):
    table = os.getenv("EXPORT_TABLE", "marts.daily_snapshot")
    con.execute("""
      create table if not exists meta_export_bookmark(
        dataset text primary key,
        last_ts timestamp,
        last_id bigint
      )
    """)
    row = con.execute("select last_ts, last_id from meta_export_bookmark where dataset=?", [table]).fetchone()
    if row:
        return row[0], row[1]
    return "1970-01-01 00:00:00", -1

def set_bookmark(con, last_ts, last_id):
    table = os.getenv("EXPORT_TABLE", "marts.daily_snapshot")
    con.execute("""
      insert into meta_export_bookmark(dataset, last_ts, last_id)
      values (?,?,?)
      on conflict(dataset) do update set last_ts=excluded.last_ts, last_id=excluded.last_id
    """, [table, last_ts, last_id])

@asset(deps=["run_ge_checkpoints"])
def export_to_google_sheets():
    log = get_dagster_logger()
    
    # Read environment variables at runtime
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    range_name = os.getenv("GOOGLE_SHEET_RANGE", "Exports!A1")
    sa_path = os.getenv("GOOGLE_SA_JSON", "/app/credentials/finance-sheets-writer-prod-sa.json")
    table = os.getenv("EXPORT_TABLE", "marts.daily_snapshot")
    db_path = os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb")
    
    # Validate required environment variables
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID environment variable is required but not set")
    
    if not os.path.exists(sa_path):
        raise ValueError(f"Google service account file not found at: {sa_path}")
    
    log.info(f"Exporting to Google Sheet ID: {sheet_id}")
    log.info(f"Using range: {range_name}")
    log.info(f"Using service account file: {sa_path}")
    
    con = duckdb.connect(db_path)
    last_ts, last_id = get_bookmark(con)

    query = f"""
      with src as (select * from {table})
      select * from src
      where updated_at > TIMESTAMP '{last_ts}'
         or (updated_at = TIMESTAMP '{last_ts}' and id > {last_id})
      order by updated_at, id
    """
    rows = con.execute(query).fetchall()
    cols = [c[0] for c in con.execute(f"describe {table}").fetchall()]

    if not rows:
        con.close()
        return Output({"appended": 0}, metadata={"appended": 0})

    # Prepare body (include header on first write if the sheet is empty)
    # Convert datetime objects to strings for JSON serialization
    def convert_datetime_to_string(value):
        if hasattr(value, 'isoformat'):  # datetime object
            return value.isoformat()
        return value
    
    values = [cols] + [[convert_datetime_to_string(cell) for cell in row] for row in rows]

    svc = sheets_client()
    svc.append(spreadsheetId=sheet_id, range=range_name,
               valueInputOption="USER_ENTERED", body={"values": values}).execute()

    # Update bookmark to last row's (updated_at, id)
    last_updated_at, last_row_id = rows[-1][cols.index("updated_at")], rows[-1][cols.index("id")]
    set_bookmark(con, last_updated_at, last_row_id)
    con.close()

    return Output({"appended": len(rows)}, metadata={"appended": len(rows)})



