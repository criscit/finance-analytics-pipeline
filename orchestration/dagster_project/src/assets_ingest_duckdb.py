# file: orchestration/dagster_project/src/assets_ingest_duckdb.py
import os, time, hashlib, duckdb
from pathlib import Path
from dagster import asset

RAW = Path(os.getenv("RAW_PATH", "/app/data/raw"))
DB  = os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb")
STABILITY_S = int(os.getenv("RAW_STABILITY_SECONDS", "8"))

def _stable(p: Path) -> bool:
    s1 = p.stat().st_size
    time.sleep(STABILITY_S)
    return s1 == p.stat().st_size

@asset
def ingest_csv_to_duckdb():
    con = duckdb.connect(DB)
    con.execute("""
        create table if not exists meta_ingest_ledger(
          filename text primary key,
          size bigint,
          md5 text,
          ingested_at timestamp default now()
        )
    """)
    for f in RAW.glob("*.csv"):
        if not _stable(f):
            continue
        md5 = hashlib.md5(f.read_bytes()).hexdigest()
        size = f.stat().st_size
        if con.execute(
            "select 1 from meta_ingest_ledger where filename=? and size=? and md5=?",
            [f.name, size, md5]
        ).fetchone():
            continue
        table = f.stem.split("_")[0]  # simple routing by prefix
        con.execute(f"create table if not exists stg_{table} as select * from read_csv_auto('{f}') limit 0")
        con.execute(f"insert into stg_{table} select * from read_csv_auto('{f}', header=true)")
        con.execute("insert into meta_ingest_ledger(filename,size,md5) values (?,?,?)", [f.name, size, md5])
    con.close()



