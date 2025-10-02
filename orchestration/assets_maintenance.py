"""Assets responsible for post-run file lifecycle management."""

from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
from dagster import MetadataValue, Output, asset, get_dagster_logger

RAW_ROOT = Path(os.getenv("FINANCE_DATA_DIR_CONTAINER", "/app/data/finance"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb")
EXPORT_DIR = RAW_ROOT / "Archive" / "Bank" / "Exports"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _cleanup_export_directory() -> dict[str, int]:
    """Clean up export directory keeping only the 10 most recent date folders."""
    logger = get_dagster_logger()

    if not EXPORT_DIR.exists():
        logger.info("Export directory %s does not exist, skipping cleanup", EXPORT_DIR)
        return {"deleted_count": 0, "kept_count": 0}

    # Get all date folders (assuming they are named with date format like YYYYMMDD)
    date_folders = []
    for item in EXPORT_DIR.iterdir():
        if item.is_dir():
            try:
                # Try to parse as date to validate format
                datetime.strptime(item.name, "%Y%m%d")
                date_folders.append((item, item.stat().st_mtime))
            except ValueError:
                # Skip folders that don't match date format
                logger.warning("Skipping non-date folder: %s", item.name)
                continue

    # Sort by modification time (newest first)
    date_folders.sort(key=lambda x: x[1], reverse=True)

    # Keep only the 10 most recent folders
    folders_to_keep = date_folders[:10]
    folders_to_delete = date_folders[10:]

    deleted_count = 0
    for folder_path, _ in folders_to_delete:
        try:
            shutil.rmtree(folder_path)
            deleted_count += 1
            logger.info("Deleted old export folder: %s", folder_path.name)
        except Exception as e:
            logger.error("Failed to delete folder %s: %s", folder_path, e)

    logger.info(
        "Export cleanup completed: kept %d folders, deleted %d folders",
        len(folders_to_keep),
        deleted_count,
    )

    return {"deleted_count": deleted_count, "kept_count": len(folders_to_keep)}


@asset(deps=["export_csv_snapshot", "export_to_google_sheets"])
def pipeline_maintenance() -> Output[dict[str, Any]]:
    """Move successfully processed files from To Parse to Archive folder tree and cleanup export directory."""

    logger = get_dagster_logger()
    summary: list[dict[str, str]] = []
    with duckdb.connect(DUCKDB_PATH) as con:
        rows = con.execute(
            """
            select src_path, bank, file_name
            from prod_meta.ingest_ledger
            where archived_at is null
            order by ingested_at
            """,
        ).fetchall()

        for src_path, bank, _file_name in rows:
            src = RAW_ROOT / src_path
            if not src.exists():
                logger.warning("File %s missing at archive time", src)
                continue

            # Replace "To Parse" with "Archive" in the path
            archived_path = src_path.replace("To Parse", "Archive")
            target = RAW_ROOT / archived_path
            _ensure_parent(target)

            shutil.move(src, target)

            con.execute(
                """
                update prod_meta.ingest_ledger
                set archived_at = current_timestamp at time zone 'UTC'
                where src_path = ?
                """,
                [src_path],
            )

            summary.append(
                {
                    "file": src_path,
                    "archived_to": archived_path,
                    "bank": bank,
                },
            )
            logger.info("Archived %s to %s", src_path, target)

    # Clean up export directory
    cleanup_result = _cleanup_export_directory()

    return Output(
        {
            "archived_files": summary,
            "export_cleanup": cleanup_result,
        },
        metadata={
            "archived_count": len(summary),
            "archived_files": MetadataValue.json(summary),
            "export_cleanup": MetadataValue.json(cleanup_result),
        },
    )
