# file: orchestration/dagster_project/src/assets_export_csv.py
import datetime
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

import duckdb
from dagster import MetadataValue, Output, asset

DB = os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb")
OUT = Path(os.getenv("EXPORT_CSV_IN_CONTAINER", "/app/data/exports/csv"))
META = Path(os.getenv("EXPORT_META_PATH", "/app/data/exports/metadata"))
TABLE = os.getenv("EXPORT_TABLE", "prod_imart.view_bank_transactions")


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@asset(deps=["run_ge_checkpoints"])
def export_csv_snapshot() -> Output[dict[str, Any]]:
    dt = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    name = TABLE.replace(".", "_")
    out_dir = OUT / name / f"dt={dt}"
    meta_dir = META / name / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "part-00000.csv"
    latest = OUT / name / "latest.csv"
    manifest = meta_dir / "manifest.json"

    con = duckdb.connect(DB, read_only=True)
    con.execute(
        f"COPY (select * from {TABLE}) TO '{csv_path.as_posix()}' WITH (HEADER, DELIMITER ',')",
    )
    row_count = con.execute(f"select count(*) from {TABLE}").fetchone()[0]
    con.close()

    checksum = _md5(csv_path)
    latest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(csv_path, latest)

    meta = {
        "table": TABLE,
        "csv_path": str(csv_path),
        "latest_path": str(latest),
        "row_count": row_count,
        "md5": checksum,
        "created_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    with manifest.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    return Output(
        value={"rows": row_count, "md5": checksum},
        metadata={
            "csv": MetadataValue.path(str(csv_path)),
            "latest": MetadataValue.path(str(latest)),
            "manifest": MetadataValue.path(str(manifest)),
            "row_count": row_count,
            "md5": checksum,
        },
    )
