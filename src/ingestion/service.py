"""Reusable ingestion workflow for financial data sources."""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from src.datacontract_io.contracts import DataContract
from src.datacontract_io.paths import resolve_contract_path, resolve_raw_parquet_path
from src.datacontract_io.readers import make_reader
from src.datacontract_io.writers import RawParquetWriter
from src.duckdb_utils import ensure_meta_schema, ingest_parquet_to_duckdb
from src.utils import extract_table_name_from_dir, is_file_stable, md5_hash, qident, utc_now_str


@dataclass(frozen=True)
class IngestionContext:
    """Runtime configuration that is shared across ingestion sources."""

    duckdb_path: str
    finance_data_root: Path
    stability_seconds: int = 8
    supported_extensions: frozenset[str] = field(
        default_factory=lambda: frozenset({".csv", ".xlsx", ".parquet"})
    )


@dataclass
class PendingFileInfo:
    path: Path
    md5: str
    rel_path: str
    size: int


@dataclass
class FileCheckResult:
    status: str
    reason: str | None = None
    info: PendingFileInfo | None = None


@dataclass(frozen=True)
class LeafIngestionOptions:
    latest_only: bool = False
    merge_pending: bool = False


@dataclass(frozen=True)
class IngestionSourceConfig:
    """
    Describes how to traverse a source tree and label its directories in logs and metrics.
    """

    name: str
    root_path: Path
    source_label: str
    leaf_label: str
    structure_hint: str
    leaf_options_factory: Callable[[Path], LeafIngestionOptions] | None = None


def _default_leaf_options(_: Path) -> LeafIngestionOptions:
    return LeafIngestionOptions()


def _prepare_file_ingestion(
    con: duckdb.DuckDBPyConnection,
    file_path: Path,
    context: IngestionContext,
    log: Any,
) -> FileCheckResult:
    """Common validation before ingesting a file."""
    if not file_path.is_file():
        return FileCheckResult(status="skipped", reason="not a file")

    file_ext = file_path.suffix.lower()
    if file_ext not in context.supported_extensions:
        reason = "pdf_file" if file_ext == ".pdf" else f"unsupported_extension: {file_ext}"
        if file_ext == ".pdf":
            log.warning("Skipping PDF file (not supported for ingestion): %s", file_path)
        else:
            log.warning(
                "Skipping unsupported file type '%s': %s",
                file_ext or "(no extension)",
                file_path,
            )
        return FileCheckResult(status="unsupported", reason=reason)

    if not is_file_stable(file_path, context.stability_seconds):
        log.info("Skipping unstable file: %s", file_path)
        return FileCheckResult(status="skipped", reason="unstable")

    file_md5 = md5_hash(file_path)
    already_ingested = con.execute(
        "select 1 from prod_meta.ingest_ledger where md5 = ?",
        [file_md5],
    ).fetchone()
    if already_ingested:
        log.info("Already ingested MD5=%s; skipping %s", file_md5, file_path)
        return FileCheckResult(status="skipped", reason="already ingested")

    try:
        rel = file_path.relative_to(context.finance_data_root).as_posix()
    except ValueError:
        rel = file_path.as_posix()

    size = file_path.stat().st_size
    info = PendingFileInfo(path=file_path, md5=file_md5, rel_path=rel, size=size)
    return FileCheckResult(status="pending", info=info)


def _validate_dataframe_columns(
    contract: DataContract,
    df: pd.DataFrame,
    file_path: Path,
) -> pd.DataFrame:
    """Ensure DataFrame columns match the contract definition."""
    expected = contract.expected_columns()
    missing = [col for col in expected if col not in df.columns]
    unexpected = [col for col in df.columns if col not in expected]

    if missing or unexpected:
        raise ValueError(
            f"Column mismatch for {file_path}: "
            f"missing={missing or '[]'}, unexpected={unexpected or '[]'}"
        )

    return df[expected].copy()


def _merge_and_ingest_pending_files(  # noqa: PLR0913
    con: duckdb.DuckDBPyConnection,
    contract: DataContract,
    pending_files: list[PendingFileInfo],
    dataframes: list[pd.DataFrame],
    source_name: str,
    log: Any,
) -> tuple[bool, int]:
    """Merge validated DataFrames, ingest once, and update ledger for each file."""
    if not pending_files:
        return True, 0

    merged_df = pd.concat(dataframes, ignore_index=True)

    md5_hasher = hashlib.md5()
    for info in pending_files:
        md5_hasher.update(info.md5.encode())
    combined_md5 = md5_hasher.hexdigest()

    writer = RawParquetWriter(contract)
    table_nm = contract.target_table.name
    parquet_path = resolve_raw_parquet_path(table_nm, md5_hash=combined_md5)
    parquet_file = Path(parquet_path)
    reuse_existing = parquet_file.exists()

    try:
        con.execute("begin;")
        if reuse_existing:
            log.info(
                "Merged parquet cache hit for MD5=%s at %s; reusing existing file",
                combined_md5,
                parquet_path,
            )
        else:
            parquet_path = writer.write(merged_df, md5_hash=combined_md5, coerce_schema=False)
            log.info(
                "Created merged parquet %s from %d file(s) (%d rows)",
                parquet_path,
                len(pending_files),
                len(merged_df),
            )
        rows_inserted = ingest_parquet_to_duckdb(con, "prod_raw", table_nm, parquet_path)

        for info in pending_files:
            con.execute(
                """
                insert into prod_meta.ingest_ledger(
                    source_system_nm,
                    table_nm,
                    file_path,
                    file_size,
                    md5,
                    processed_at
                )
                values (?,?,?,?,?, cast(? as timestamp))
                """,
                [source_name, table_nm, info.rel_path, info.size, info.md5, utc_now_str()],
            )

        con.execute("commit;")
        log.info(
            "Ingested %d merged file(s) -> prod_raw.%s (rows=%d)",
            len(pending_files),
            table_nm,
            rows_inserted,
        )
        return True, rows_inserted

    except Exception as exc:  # pragma: no cover - defensive
        con.execute("rollback;")
        log.exception(
            "Failed to ingest merged files for %s using %d source files: %s",
            table_nm,
            len(pending_files),
            exc,
        )
        return False, 0


def _parquet_from_source_if_needed(
    contract: DataContract,
    src_path: Path,
    file_md5: str,
    log: Any,
) -> str:
    """
    For CSV/XLSX: return cached parquet if exists, else read via contract & write.
    For parquet input: just return the same path.
    """
    if src_path.suffix.lower() == ".parquet":
        log.info("Using existing parquet: %s", src_path)
        return str(src_path)

    parquet_path = resolve_raw_parquet_path(contract.target_table.name, md5_hash=file_md5)

    if Path(parquet_path).exists():
        log.info("Parquet cache hit for MD5=%s at %s", file_md5, parquet_path)
        return parquet_path

    reader = make_reader(contract)
    df = reader.read(str(src_path))

    writer = RawParquetWriter(contract)
    out_path = writer.write(df, md5_hash=file_md5, coerce_schema=False)
    log.info("Created parquet: %s (rows=%d)", out_path, len(df))
    return out_path


def _process_file_once(  # noqa: PLR0913
    con: duckdb.DuckDBPyConnection,
    contract: DataContract,
    file_path: Path,
    source_name: str,
    context: IngestionContext,
    log: Any,
) -> dict[str, Any]:
    """Single-file workflow with idempotency by MD5 of file contents."""
    check = _prepare_file_ingestion(con, file_path, context, log)
    if check.status != "pending" or check.info is None:
        return {"status": check.status, "reason": check.reason}

    info = check.info
    table_nm = contract.target_table.name

    try:
        con.execute("begin;")

        parquet_path = _parquet_from_source_if_needed(contract, info.path, info.md5, log)
        rows_inserted = ingest_parquet_to_duckdb(con, "prod_raw", table_nm, parquet_path)

        con.execute(
            """
            insert into prod_meta.ingest_ledger(
                source_system_nm,
                table_nm,
                file_path,
                file_size,
                md5,
                processed_at
            )
            values (?,?,?,?,?, cast(? as timestamp))
            """,
            [source_name, table_nm, info.rel_path, info.size, info.md5, utc_now_str()],
        )

        con.execute("commit;")
        log.info("Ingested %s -> prod_raw.%s (rows=%d)", info.path, table_nm, rows_inserted)
        return {
            "status": "success",
            "rows": rows_inserted,
            "parquet": parquet_path,
            "md5": info.md5,
        }

    except Exception as exc:  # pragma: no cover - defensive
        con.execute("rollback;")
        log.exception("Failed to process %s: %s", info.path, exc)
        return {"status": "error", "reason": str(exc)}


def _validate_contract_for_table(
    table_nm: str,
    data_type_dir: Path,
    supported_extensions: frozenset[str],
    log: Any,
) -> str | None:
    """
    Validate that a contract exists for the table.
    Returns contract_path if valid, None if should skip.
    Raises FileNotFoundError if contract missing for supported files.
    """
    contract_path = resolve_contract_path(table_nm)
    if Path(contract_path).exists():
        return contract_path

    all_files = list(data_type_dir.rglob("*.*"))
    supported_files = [f for f in all_files if f.suffix.lower() in supported_extensions]

    if supported_files:
        error_msg = (
            "CRITICAL: Data contract not found for "
            f"{table_nm} at {contract_path}. Contract is required because "
            f"{len(supported_files)} supported file(s) found: "
            f"{[f.name for f in supported_files[:3]]}"
        )
        log.error(error_msg)
        raise FileNotFoundError(error_msg)

    log.info(
        "No contract found for %s and no supported files to process "
        "(only PDFs or other unsupported types). Skipping.",
        table_nm,
    )
    return None


def _should_drop_table_for_fresh_load(  # noqa: PLR0913
    con: duckdb.DuckDBPyConnection,
    contract: DataContract,
    data_type_dir: Path,
    seen_tables: set[str],
    context: IngestionContext,
    log: Any,
) -> bool:
    """
    Determine if table should be dropped for fresh ingestion.
    Only drops if table hasn't been seen this run and there are files to process.
    """
    table_nm = contract.target_table.name

    if table_nm in seen_tables:
        return False

    file_pattern = contract.metadata.get("file_pattern", "*.csv")
    matching_files = list(data_type_dir.rglob(file_pattern))

    for file_path in matching_files:
        if not file_path.is_file():
            continue

        file_ext = file_path.suffix.lower()
        if file_ext not in context.supported_extensions:
            continue

        if not is_file_stable(file_path, context.stability_seconds):
            continue

        file_md5 = md5_hash(file_path)
        already_ingested = con.execute(
            "select 1 from prod_meta.ingest_ledger where md5 = ?",
            [file_md5],
        ).fetchone()

        if not already_ingested:
            log.info("Found new files to ingest for %s - will drop table for fresh load", table_nm)
            return True

    log.info("No new files to ingest for %s - keeping existing table intact", table_nm)
    return False


def _ensure_fresh_table(
    con: duckdb.DuckDBPyConnection,
    table_nm: str,
    seen_tables: set[str],
    log: Any,
) -> None:
    """Drop table and mark as seen in this run."""
    if table_nm not in seen_tables:
        con.execute(f"drop table if exists prod_raw.{qident(table_nm)}")
        _clear_raw_parquet_cache(table_nm, log)
        seen_tables.add(table_nm)
        log.info("Dropped table prod_raw.%s for fresh ingestion", table_nm)


def _clear_raw_parquet_cache(table_nm: str, log: Any) -> None:
    """Remove cached parquet files for a table to keep only the next ingest output."""
    raw_dir = Path(resolve_raw_parquet_path(table_nm))
    if not raw_dir.exists():
        return

    removed = 0
    for path in raw_dir.iterdir():
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            removed += 1
        except OSError as exc:  # pragma: no cover - defensive
            log.warning("Failed to remove cached parquet artifact %s: %s", path, exc)

    if removed:
        log.info("Cleared %d cached parquet artifact(s) for %s", removed, table_nm)


def _process_table_files(  # noqa: PLR0913, PLR0912, PLR0915
    con: duckdb.DuckDBPyConnection,
    contract: DataContract,
    data_type_dir: Path,
    source_name: str,
    context: IngestionContext,
    leaf_options: LeafIngestionOptions,
    log: Any,
) -> tuple[int, int, int, int]:
    """Process all files for a given table."""
    file_pattern = contract.metadata.get("file_pattern", "*.csv")
    log.info("Looking for files matching '%s' in %s", file_pattern, data_type_dir)

    matching_files = sorted(data_type_dir.rglob(file_pattern))
    log.info("Found %d files matching pattern in %s", len(matching_files), data_type_dir)

    latest_supported: Path | None = None
    if leaf_options.latest_only:
        supported_candidates = [
            f
            for f in matching_files
            if f.is_file() and f.suffix.lower() in context.supported_extensions
        ]
        if supported_candidates:
            supported_candidates.sort(key=lambda path: path.name, reverse=True)
            latest_supported = supported_candidates[0]
            log.info(
                "Selected latest supported file '%s' for ingestion in %s",
                latest_supported.name,
                data_type_dir,
            )
        else:
            log.info(
                "No supported files found in %s for latest-only ingestion; "
                "all files will be evaluated normally",
                data_type_dir,
            )

    ingested = skipped = errors = unsupported = 0
    pending_files: list[PendingFileInfo] = []
    pending_frames: list[pd.DataFrame] = []
    reader = make_reader(contract) if leaf_options.merge_pending else None

    for file_path in matching_files:
        if (
            leaf_options.latest_only
            and latest_supported is not None
            and file_path.is_file()
            and file_path.suffix.lower() in context.supported_extensions
            and file_path != latest_supported
        ):
            log.info(
                "Skipping older supported file '%s' because newer file '%s' will be ingested",
                file_path.name,
                latest_supported.name,
            )
            skipped += 1
            continue

        if leaf_options.merge_pending and reader is not None:
            check = _prepare_file_ingestion(con, file_path, context, log)
            if check.status == "pending" and check.info is not None:
                try:
                    df = reader.read(str(file_path))
                    df = _validate_dataframe_columns(contract, df, file_path)
                except Exception as exc:  # pragma: no cover - defensive
                    log.exception("Failed to read %s: %s", file_path, exc)
                    errors += 1
                    continue

                pending_files.append(check.info)
                pending_frames.append(df)
                continue

            if check.status == "skipped":
                skipped += 1
            elif check.status == "unsupported":
                unsupported += 1
            else:
                errors += 1
            continue

        result = _process_file_once(
            con,
            contract,
            file_path,
            source_name,
            context,
            log,
        )
        status = result.get("status")
        if status == "success":
            ingested += 1
        elif status == "skipped":
            skipped += 1
        elif status == "unsupported":
            unsupported += 1
        else:
            errors += 1

    if leaf_options.merge_pending and pending_files:
        success, _ = _merge_and_ingest_pending_files(
            con,
            contract,
            pending_files,
            pending_frames,
            source_name,
            log,
        )
        if success:
            ingested += len(pending_files)
        else:
            errors += len(pending_files)

    return ingested, skipped, unsupported, errors


def run_ingestion(
    config: IngestionSourceConfig,
    context: IngestionContext,
    log: Any,
) -> dict[str, int]:
    """
    Execute the ingestion workflow for the given source configuration.
    Returns ingestion metrics that can be emitted as Dagster metadata.
    """
    root_path = config.root_path
    root_path.mkdir(parents=True, exist_ok=True)
    if not root_path.exists():
        raise ValueError(f"No To Parse folder found at {root_path}")

    con = duckdb.connect(context.duckdb_path)
    ensure_meta_schema(con)

    ingested = skipped = errors = unsupported = 0
    seen_tables: set[str] = set()
    options_factory = config.leaf_options_factory or _default_leaf_options

    log.info("Starting %s ingestion from %s", config.name.lower(), root_path)

    source_dirs = sorted(p for p in root_path.iterdir() if p.is_dir())
    log.info(
        "Found %d %s directories: %s",
        len(source_dirs),
        config.source_label,
        [d.name for d in source_dirs],
    )

    for source_dir in source_dirs:
        source_nm = source_dir.name
        log.info("Processing %s directory: %s", config.source_label, source_nm)

        leaf_dirs = sorted(p for p in source_dir.iterdir() if p.is_dir())
        if not leaf_dirs:
            log.warning(
                "No %s subdirectories found in %s. Expected structure: %s",
                config.leaf_label,
                source_dir,
                config.structure_hint.format(source=source_nm),
            )
            continue

        log.info(
            "Found %d %s directories in %s: %s",
            len(leaf_dirs),
            config.leaf_label,
            source_nm,
            [d.name for d in leaf_dirs],
        )

        for leaf_dir in leaf_dirs:
            table_nm = extract_table_name_from_dir(leaf_dir, root_path)

            contract_path = _validate_contract_for_table(
                table_nm,
                leaf_dir,
                context.supported_extensions,
                log,
            )
            if not contract_path:
                continue

            try:
                contract = DataContract.from_yaml(contract_path)
                log.info("Loaded contract for table %s", table_nm)
            except Exception as exc:  # pragma: no cover - defensive
                log.exception("Failed to load contract %s: %s", contract_path, exc)
                continue

            should_drop = _should_drop_table_for_fresh_load(
                con,
                contract,
                leaf_dir,
                seen_tables,
                context,
                log,
            )
            if should_drop:
                _ensure_fresh_table(con, contract.target_table.name, seen_tables, log)
            else:
                seen_tables.add(contract.target_table.name)

            leaf_options = options_factory(leaf_dir)
            i, s, u, err_count = _process_table_files(
                con,
                contract,
                leaf_dir,
                source_nm,
                context,
                leaf_options,
                log,
            )
            ingested += i
            skipped += s
            unsupported += u
            errors += err_count

    con.close()
    log.info(
        "%s ingestion complete. Ingested=%d, Skipped=%d, Unsupported=%d, Errors=%d",
        config.name,
        ingested,
        skipped,
        unsupported,
        errors,
    )

    return {
        "ingested": ingested,
        "skipped": skipped,
        "unsupported": unsupported,
        "errors": errors,
    }
