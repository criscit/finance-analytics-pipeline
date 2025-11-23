# file: orchestration/dagster_project/src/assets_export_csv.py
import datetime
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import duckdb
from dagster import AssetExecutionContext, MetadataValue, Output, asset

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb")
FINANCE_DATA_DIR = Path(os.getenv("FINANCE_DATA_DIR_CONTAINER", "/app/data/finance"))
EXPORT_DIR = FINANCE_DATA_DIR / "Archive" / "Bank" / "Exports"
RESULTS_DIR = FINANCE_DATA_DIR / "Results"
FINANCE_HISTORY_EXPORT_TABLE = os.getenv(
    "FINANCE_HISTORY_EXPORT_TABLE", "prod_imart.view_transactions"
)
FINANCE_ASSETS_EXPORT_TABLE = os.getenv("FINANCE_ASSETS_EXPORT_TABLE", "prod_imart.view_assets")


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _export_table_snapshot(
    context: AssetExecutionContext,
    *,
    export_table: str,
    results_filename: str,
    snapshot_label: str,
    order_by: str | None = None,
) -> Output[dict[str, Any]]:
    """Shared helper that exports DuckDB tables to timestamped CSV snapshots."""
    now = datetime.datetime.now(datetime.UTC)
    date_folder = now.strftime("%Y%m%d")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    table_name = export_table.replace(".", "_")
    date_dir = EXPORT_DIR / date_folder
    date_dir.mkdir(parents=True, exist_ok=True)

    csv_filename = f"{table_name}_{timestamp}.csv"
    manifest_filename = f"{table_name}_manifest_{timestamp}.json"

    csv_path = date_dir / csv_filename
    manifest_path = date_dir / manifest_filename

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results_path = RESULTS_DIR / results_filename

    context.log.info(
        "Exporting %s snapshot from %s (order by %s)",
        snapshot_label,
        export_table,
        order_by or "N/A",
    )
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    order_clause = f" order by {order_by}" if order_by else ""
    export_query = f"select * from {export_table}{order_clause}"
    con.execute(
        f"COPY ({export_query}) TO '{csv_path.as_posix()}' WITH (HEADER, DELIMITER ',')",
    )
    con.execute(
        f"COPY ({export_query}) TO '{results_path.as_posix()}' WITH (HEADER, DELIMITER ',')",
    )
    result = con.execute(f"select count(*) from {export_table}").fetchone()
    row_count = result[0] if result else 0
    con.close()

    checksum = _md5(csv_path)
    results_checksum = _md5(results_path)

    meta = {
        "table": export_table,
        "csv_path": str(csv_path),
        "results_path": str(results_path),
        "row_count": row_count,
        "md5": checksum,
        "results_md5": results_checksum,
        "created_at_utc": now.isoformat(timespec="seconds") + "Z",
        "date_folder": date_folder,
        "timestamp": timestamp,
        "snapshot_label": snapshot_label,
    }

    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    return Output(
        value={
            "rows": row_count,
            "md5": checksum,
            "results_md5": results_checksum,
            "timestamp": timestamp,
            "snapshot_label": snapshot_label,
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
            "snapshot_label": snapshot_label,
        },
    )


@asset(deps=["build_imart_models"])
def export_csv_snapshot(context: AssetExecutionContext) -> Output[dict[str, Any]]:
    """Historical transactions export."""
    return _export_table_snapshot(
        context,
        export_table=FINANCE_HISTORY_EXPORT_TABLE,
        results_filename="transactions.csv",
        snapshot_label="transactions",
        order_by="transacted_at",
    )


@asset(deps=["build_imart_models"])
def export_assets_csv_snapshot(context: AssetExecutionContext) -> Output[dict[str, Any]]:
    """Assets snapshot export."""
    return _export_table_snapshot(
        context,
        export_table=FINANCE_ASSETS_EXPORT_TABLE,
        results_filename="assets.csv",
        snapshot_label="assets",
    )
