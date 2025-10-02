# file: orchestration/dagster_project/src/assets_export_csv.py
import datetime
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import duckdb
from dagster import MetadataValue, Output, asset

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb")
FINANCE_DATA_DIR = Path(os.getenv("FINANCE_DATA_DIR_CONTAINER", "/app/data/finance"))
EXPORT_DIR = FINANCE_DATA_DIR / "Archive" / "Bank" / "Exports"
RESULTS_DIR = FINANCE_DATA_DIR / "Results"
EXPORT_FINANCE_TABLE = os.getenv("EXPORT_FINANCE_TABLE", "prod_imart.view_bank_transactions")


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@asset(deps=["run_ge_checkpoints"])
def export_csv_snapshot() -> Output[dict[str, Any]]:
    # Create timestamp for folder and file naming
    now = datetime.datetime.utcnow()
    date_folder = now.strftime("%Y%m%d")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    # Replace dots with underscores in table name
    table_name = EXPORT_FINANCE_TABLE.replace(".", "_")

    # Create date-based folder structure
    date_dir = EXPORT_DIR / date_folder
    date_dir.mkdir(parents=True, exist_ok=True)

    # Create file names with timestamp
    csv_filename = f"{table_name}_{timestamp}.csv"
    manifest_filename = f"{table_name}_manifest_{timestamp}.json"

    csv_path = date_dir / csv_filename
    manifest_path = date_dir / manifest_filename

    # Results file path
    results_path = RESULTS_DIR / "bank_transactions.csv"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Export data from database
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    con.execute(
        f"COPY (select * from {EXPORT_FINANCE_TABLE}) TO '{csv_path.as_posix()}' WITH (HEADER, DELIMITER ',')",
    )
    # Also export to Results folder
    con.execute(
        f"COPY (select * from {EXPORT_FINANCE_TABLE}) TO '{results_path.as_posix()}' WITH (HEADER, DELIMITER ',')",
    )
    row_count = con.execute(f"select count(*) from {EXPORT_FINANCE_TABLE}").fetchone()[0]
    con.close()

    # Calculate checksums
    checksum = _md5(csv_path)
    results_checksum = _md5(results_path)

    # Create manifest metadata
    meta = {
        "table": EXPORT_FINANCE_TABLE,
        "csv_path": str(csv_path),
        "results_path": str(results_path),
        "row_count": row_count,
        "md5": checksum,
        "results_md5": results_checksum,
        "created_at_utc": now.isoformat(timespec="seconds") + "Z",
        "date_folder": date_folder,
        "timestamp": timestamp,
    }

    # Write manifest file
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    return Output(
        value={
            "rows": row_count,
            "md5": checksum,
            "results_md5": results_checksum,
            "timestamp": timestamp,
        },
        metadata={
            "csv": MetadataValue.path(str(csv_path)),
            "results": MetadataValue.path(str(results_path)),
            "manifest": MetadataValue.path(str(manifest_path)),
            "row_count": row_count,
            "md5": checksum,
            "results_md5": results_checksum,
            "date_folder": date_folder,
            "timestamp": timestamp,
        },
    )
