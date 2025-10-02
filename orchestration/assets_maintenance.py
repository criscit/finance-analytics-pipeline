"""Assets responsible for post-run file lifecycle management."""

from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from dagster import MetadataValue, Output, asset, get_dagster_logger

RAW_ROOT = Path(os.getenv("RAW_PATH", "/app/data/raw"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse/warehouse.duckdb")


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


@asset(deps=["export_csv_snapshot", "export_to_google_sheets"])
def archive_processed_files() -> Output[list[dict[str, str]]]:
    """Move successfully exported files into the Archive folder tree."""

    logger = get_dagster_logger()
    summary: list[dict[str, str]] = []
    with duckdb.connect(DUCKDB_PATH) as con:
        rows = con.execute(
            """
            select filename, original_name, bank, source_kind
            from prod_meta.ingest_ledger
            where archived_at is null
            order by ingested_at
            """,
        ).fetchall()

        for stored_path, original_name, bank, source_kind in rows:
            src = RAW_ROOT / stored_path
            if not src.exists():
                logger.warning("File %s missing at archive time", src)
                continue

            if source_kind == "legacy_results":
                archive_dir = RAW_ROOT / "Archive" / "Results"
            else:
                archive_dir = RAW_ROOT / "Archive" / "Bank" / bank

            partition = datetime.now(tz=UTC).strftime("%Y-%m-%d")
            target_dir = archive_dir / partition
            _ensure_parent(target_dir / original_name)
            target = target_dir / original_name
            shutil.move(src, target)

            con.execute(
                """
                update prod_meta.ingest_ledger
                set archived_at = current_timestamp at time zone 'UTC', archived_path = ?
                where filename = ?
                """,
                [target.relative_to(RAW_ROOT).as_posix(), stored_path],
            )

            summary.append(
                {
                    "file": stored_path,
                    "archived_to": target.relative_to(RAW_ROOT).as_posix(),
                    "bank": bank,
                },
            )
            logger.info("Archived %s to %s", stored_path, target)

    return Output(
        summary,
        metadata={
            "archived_count": len(summary),
            "archived_files": MetadataValue.json(summary),
        },
    )
