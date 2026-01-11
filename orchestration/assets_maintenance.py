"""Assets responsible for post-run file lifecycle management."""

import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
from dagster import AssetExecutionContext, MetadataValue, Output, asset

RAW_ROOT = Path(os.getenv("FINANCE_DATA_DIR_CONTAINER", "/app/data/finance"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb")
EXPORT_DIR = RAW_ROOT / "Archive" / "Bank" / "Exports"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _is_assets_or_earn_folder(file_path: str) -> bool:
    """Check if file is in an 'assets' or 'earn' folder (case-insensitive)."""
    path_lower = file_path.lower()
    path_parts = Path(path_lower).parts
    return any(part in ("assets", "earn") for part in path_parts)


def _parse_p2p_filename(filename: str) -> tuple[str, str] | None:
    """
    Parse Telegram P2P filename to extract date range.

    Expected format: p2p-order-history_YYYY-MM-DD_YYYY-MM-DD.ext
    Returns: (start_date, end_date) or None if not a P2P file
    """
    pattern = r"p2p-order-history_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})"
    match = re.search(pattern, filename)
    if match:
        return (match.group(1), match.group(2))
    return None


def _handle_p2p_consolidation(
    context: AssetExecutionContext, file_path: str, src: Path, raw_root: Path
) -> bool:
    """
    Handle Telegram P2P file consolidation.

    If a newer file with the same start date but later end date exists,
    delete the older file(s) from "To Parse" folder.

    Returns: True if the file should be archived normally, False if it was deleted
    """
    parsed = _parse_p2p_filename(src.name)
    if not parsed:
        return True  # Not a P2P file, handle normally

    current_start, current_end = parsed

    # Find the parent "To Parse" directory to search for other P2P files
    to_parse_dir = src.parent

    # Find all P2P files in the same directory
    p2p_files = []
    for file in to_parse_dir.glob("p2p-order-history_*"):
        if file == src:
            continue
        parsed_other = _parse_p2p_filename(file.name)
        if parsed_other:
            other_start, other_end = parsed_other
            p2p_files.append((file, other_start, other_end))

    # Check if there's a newer file with the same start date but later end date
    is_current_newest = True
    files_to_delete = []

    for other_file, other_start, other_end in p2p_files:
        if other_start == current_start:
            # Same start date - compare end dates
            if other_end > current_end:
                # Found a newer file, current file should be deleted
                is_current_newest = False
                context.log.info(
                    "Found newer P2P file %s (ends %s) vs current %s (ends %s)",
                    other_file.name,
                    other_end,
                    src.name,
                    current_end,
                )
                break
            if current_end > other_end:
                # Current file is newer, mark older file for deletion
                files_to_delete.append(other_file)

    if not is_current_newest:
        # Delete current file as it's superseded by a newer one
        try:
            src.unlink()
            context.log.info("Deleted superseded P2P file: %s", src.name)
        except Exception as e:
            context.log.error("Failed to delete superseded P2P file %s: %s", src.name, e)
        return False  # Don't archive this file

    # Delete older files that are superseded by the current file
    for old_file in files_to_delete:
        try:
            old_file.unlink()
            context.log.info("Deleted superseded P2P file: %s", old_file.name)
        except Exception as e:
            context.log.error("Failed to delete superseded P2P file %s: %s", old_file.name, e)

    return True  # Archive the current file normally


def _cleanup_export_directory(context: AssetExecutionContext) -> dict[str, int]:
    """Clean up export directory keeping only the 10 most recent date folders."""
    if not EXPORT_DIR.exists():
        context.log.info("Export directory %s does not exist, skipping cleanup", EXPORT_DIR)
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
                context.log.warning("Skipping non-date folder: %s", item.name)
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
            context.log.info("Deleted old export folder: %s", folder_path.name)
        except Exception as e:
            context.log.error("Failed to delete folder %s: %s", folder_path, e)

    context.log.info(
        "Export cleanup completed: kept %d folders, deleted %d folders",
        len(folders_to_keep),
        deleted_count,
    )

    return {"deleted_count": deleted_count, "kept_count": len(folders_to_keep)}


def _archive_assets_earn_folders(context: AssetExecutionContext, con: Any) -> list[dict[str, str]]:
    """Archive all files from assets/earn folders immediately."""
    archived_summary: list[dict[str, str]] = []

    # Find all "assets" and "earn" folders in "To Parse" directory
    to_parse_dir = RAW_ROOT / "To Parse"
    if not to_parse_dir.exists():
        context.log.info("To Parse directory does not exist, skipping assets/earn archival")
        return archived_summary

    # Recursively find all files in assets/earn folders
    for item in to_parse_dir.rglob("*"):
        if not item.is_file():
            continue

        # Get relative path from RAW_ROOT
        relative_path = str(item.relative_to(RAW_ROOT))

        # Check if this file is in an assets or earn folder
        if not _is_assets_or_earn_folder(relative_path):
            continue

        # Replace "To Parse" with "Archive" in the path
        archived_path = relative_path.replace("To Parse", "Archive")
        target = RAW_ROOT / archived_path
        _ensure_parent(target)

        try:
            shutil.move(item, target)

            # Update ingest_ledger if this file was tracked
            con.execute(
                """
                update prod_meta.ingest_ledger
                set
                    archived_at = current_timestamp at time zone 'UTC'
                where
                    file_path = ?
                    and archived_at is null
                """,
                [relative_path],
            )

            archived_summary.append(
                {
                    "file": relative_path,
                    "archived_to": archived_path,
                    "type": "assets_earn_auto",
                },
            )
            context.log.info("Auto-archived assets/earn file: %s to %s", relative_path, target)
        except Exception as e:
            context.log.error("Failed to archive assets/earn file %s: %s", relative_path, e)

    return archived_summary


@asset(
    deps=[
        "export_csv_snapshot",
        "export_assets_csv_snapshot",
        "export_to_google_sheets",
        "export_assets_to_google_sheets",
    ]
)
def pipeline_maintenance(context: AssetExecutionContext) -> Output[dict[str, Any]]:
    """Move successfully processed files from To Parse to Archive folder tree and cleanup export directory."""
    summary: list[dict[str, str]] = []
    deleted_p2p_files: list[str] = []

    with duckdb.connect(DUCKDB_PATH) as con:
        # First, handle assets/earn folders - archive everything
        assets_earn_summary = _archive_assets_earn_folders(context, con)
        summary.extend(assets_earn_summary)

        # Then handle regular processed files from ingest_ledger
        rows = con.execute(
            """
            select
                source_system_nm,
                table_nm,
                file_path
            from
                prod_meta.ingest_ledger
            where
                archived_at is null
            order by
                processed_at desc
            """,
        ).fetchall()

        for source_system_nm, table_nm, file_path in rows:
            src = RAW_ROOT / file_path
            if not src.exists():
                context.log.warning("File %s missing at archive time", src)
                continue

            # Handle Telegram P2P file consolidation
            should_archive = _handle_p2p_consolidation(context, file_path, src, RAW_ROOT)
            if not should_archive:
                # File was deleted as it's superseded by a newer P2P file
                deleted_p2p_files.append(file_path)
                # Update ingest_ledger to mark as archived (even though deleted)
                con.execute(
                    """
                    update prod_meta.ingest_ledger
                    set
                        archived_at = current_timestamp at time zone 'UTC'
                    where
                        file_path = ?
                    """,
                    [file_path],
                )
                continue

            # Replace "To Parse" with "Archive" in the path
            archived_path = file_path.replace("To Parse", "Archive")
            target = RAW_ROOT / archived_path
            _ensure_parent(target)

            try:
                shutil.move(src, target)

                con.execute(
                    """
                    update prod_meta.ingest_ledger
                    set
                        archived_at = current_timestamp at time zone 'UTC'
                    where
                        file_path = ?
                    """,
                    [file_path],
                )

                summary.append(
                    {
                        "file": file_path,
                        "archived_to": archived_path,
                        "bank": source_system_nm,
                        "table": table_nm,
                    },
                )
                context.log.info("Archived %s to %s", file_path, target)
            except Exception as e:
                context.log.error("Failed to archive file %s: %s", file_path, e)

    # Clean up export directory
    cleanup_result = _cleanup_export_directory(context)

    return Output(
        {
            "archived_files": summary,
            "deleted_p2p_files": deleted_p2p_files,
            "export_cleanup": cleanup_result,
        },
        metadata={
            "archived_count": len(summary),
            "deleted_p2p_count": len(deleted_p2p_files),
            "archived_files": MetadataValue.json(summary),
            "deleted_p2p_files": MetadataValue.json(deleted_p2p_files),
            "export_cleanup": MetadataValue.json(cleanup_result),
        },
    )
