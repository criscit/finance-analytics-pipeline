# file: orchestration/dagster_project/src/assets_ingest.py
import os
from pathlib import Path
from typing import Any

import duckdb
from dagster import Output, asset, get_dagster_logger

from src.datacontract_io.contracts import DataContract
from src.datacontract_io.paths import resolve_contract_path, resolve_raw_parquet_path
from src.datacontract_io.readers import make_reader
from src.datacontract_io.writers import RawParquetWriter
from src.duckdb_utils import ensure_meta_schema, ingest_parquet_to_duckdb
from src.utils import extract_table_name_from_dir, is_file_stable, md5_hash, qident, utc_now_str

FINANCE_DATA_DIR_CONTAINER = Path(os.getenv("FINANCE_DATA_DIR_CONTAINER", "/app/data/finance"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse/analytics.duckdb")
STABILITY_S = 8

INPUT_PATH = FINANCE_DATA_DIR_CONTAINER / "To Parse" / "Bank"

# Supported file extensions for ingestion
SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".parquet"}

# -------------------------
# Helpers
# -------------------------


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

    # IMPORTANT: use the already computed MD5 (no double-hash)
    table_nm = contract.target_table.name
    parquet_path = resolve_raw_parquet_path(table_nm, md5_hash=file_md5)

    if Path(parquet_path).exists():
        log.info("Parquet cache hit for MD5=%s at %s", file_md5, parquet_path)
        return parquet_path

    reader = make_reader(contract)
    df = reader.read(str(src_path))

    writer = RawParquetWriter(contract)
    out_path = writer.write(df, md5_hash=file_md5, coerce_schema=False)
    log.info("Created parquet: %s (rows=%d)", out_path, len(df))
    return out_path


def _process_file_once(
    con: duckdb.DuckDBPyConnection,
    contract: DataContract,
    file_path: Path,
    bank_nm: str,
    log: Any,
) -> dict[str, Any]:
    """
    Single-file workflow with idempotency by MD5 of file contents.
    Transaction covers ingest + ledger write.
    """
    if not file_path.is_file():
        return {"status": "skipped", "reason": "not a file"}

    # Check for unsupported file types
    file_ext = file_path.suffix.lower()
    if file_ext not in SUPPORTED_EXTENSIONS:
        reason = "pdf_file" if file_ext == ".pdf" else f"unsupported_extension: {file_ext}"
        if file_ext == ".pdf":
            log.warning("⚠️  Skipping PDF file (not supported for ingestion): %s", file_path)
        else:
            log.warning(
                "⚠️  Skipping unsupported file type '%s': %s",
                file_ext or "(no extension)",
                file_path,
            )
        return {"status": "unsupported", "reason": reason}

    if not is_file_stable(file_path, STABILITY_S):
        log.info("Skipping unstable file: %s", file_path)
        return {"status": "skipped", "reason": "unstable"}

    file_md5 = md5_hash(file_path)  # hash of file contents

    # Idempotency via ledger
    if con.execute("select 1 from prod_meta.ingest_ledger where md5 = ?", [file_md5]).fetchone():
        log.info("Already ingested MD5=%s; skipping %s", file_md5, file_path)
        return {"status": "skipped", "reason": "already ingested"}

    table_nm = contract.target_table.name

    try:
        con.execute("begin;")

        parquet_path = _parquet_from_source_if_needed(contract, file_path, file_md5, log)
        rows_inserted = ingest_parquet_to_duckdb(con, "prod_raw", table_nm, parquet_path)

        rel = file_path.relative_to(FINANCE_DATA_DIR_CONTAINER).as_posix()
        file_size = file_path.stat().st_size

        con.execute(
            """
            insert into prod_meta.ingest_ledger(
                bank_nm,
                table_nm,
                file_path,
                file_size, 
                md5, 
                processed_at
            )
            values (?,?,?,?,?, cast(? as timestamp))
            """,
            [bank_nm, table_nm, rel, file_size, file_md5, utc_now_str()],
        )

        con.execute("commit;")
        log.info("✅ Ingested %s -> prod_raw.%s (rows=%d)", file_path, table_nm, rows_inserted)
        return {
            "status": "success",
            "rows": rows_inserted,
            "parquet": parquet_path,
            "md5": file_md5,
        }

    except Exception as e:
        con.execute("rollback;")
        log.exception("Failed to process %s: %s", file_path, e)
        return {"status": "error", "reason": str(e)}


# -------------------------
# Asset Helpers
# -------------------------


def _validate_contract_for_table(table_nm: str, data_type_dir: Path, log: Any) -> str | None:
    """
    Validate that a contract exists for the table.
    Returns contract_path if valid, None if should skip.
    Raises FileNotFoundError if contract missing for supported files.
    """
    contract_path = resolve_contract_path(table_nm)
    if Path(contract_path).exists():
        return contract_path

    # Check if there are any potentially ingestible files before failing
    all_files = list(data_type_dir.rglob("*.*"))
    supported_files = [f for f in all_files if f.suffix.lower() in SUPPORTED_EXTENSIONS]

    if supported_files:
        error_msg = f"CRITICAL: Data contract not found for {table_nm} at {contract_path}. Contract is required because {len(supported_files)} supported file(s) found: {[f.name for f in supported_files[:3]]}"
        log.error(error_msg)
        raise FileNotFoundError(error_msg)

    log.info(
        "No contract found for %s and no supported files to process (only PDFs or other unsupported types). Skipping.",
        table_nm,
    )
    return None


def _should_drop_table_for_fresh_load(
    con: duckdb.DuckDBPyConnection,
    contract: DataContract,
    data_type_dir: Path,
    seen_tables: set[str],
    log: Any,
) -> bool:
    """
    Determine if table should be dropped for fresh ingestion.

    Only drops if:
    1. Table hasn't been seen this run
    2. AND there are actually files to process (not already ingested)

    This prevents dropping tables when no new data will be loaded,
    which would leave tables missing if parquets already processed.

    Returns:
        True if table should be dropped, False otherwise
    """
    table_nm = contract.target_table.name

    if table_nm in seen_tables:
        return False

    # Check if there are any files that would actually be ingested
    file_pattern = contract.metadata.get("file_pattern", "*.csv")
    matching_files = list(data_type_dir.rglob(file_pattern))

    # Check if any files are NOT yet in ledger (would be ingested)
    has_new_files = False
    for file_path in matching_files:
        if not file_path.is_file():
            continue

        file_ext = file_path.suffix.lower()
        if file_ext not in SUPPORTED_EXTENSIONS:
            continue

        if not is_file_stable(file_path, STABILITY_S):
            continue

        file_md5 = md5_hash(file_path)
        already_ingested = con.execute(
            "select 1 from prod_meta.ingest_ledger where md5 = ?",
            [file_md5],
        ).fetchone()

        if not already_ingested:
            has_new_files = True
            break

    if has_new_files:
        log.info(
            "Found new files to ingest for %s - will drop table for fresh load",
            table_nm,
        )
        return True
    log.info(
        "No new files to ingest for %s - keeping existing table intact",
        table_nm,
    )
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
        seen_tables.add(table_nm)
        log.info("Dropped table prod_raw.%s for fresh ingestion", table_nm)


def _process_table_files(
    con: duckdb.DuckDBPyConnection,
    contract: DataContract,
    data_type_dir: Path,
    bank_nm: str,
    log: Any,
) -> tuple[int, int, int, int]:
    """
    Process all files for a given table.
    Returns tuple of (ingested, skipped, unsupported, errors).
    """
    file_pattern = contract.metadata.get("file_pattern", "*.csv")
    log.info("Looking for files matching '%s' in %s", file_pattern, data_type_dir)

    matching_files = list(data_type_dir.rglob(file_pattern))
    log.info("Found %d files matching pattern in %s", len(matching_files), data_type_dir)

    ingested = skipped = errors = unsupported = 0
    for file_path in matching_files:
        result = _process_file_once(con, contract, file_path, bank_nm, log)
        status = result.get("status")
        if status == "success":
            ingested += 1
        elif status == "skipped":
            skipped += 1
        elif status == "unsupported":
            unsupported += 1
        else:
            errors += 1

    return ingested, skipped, unsupported, errors


# -------------------------
# Asset
# -------------------------


@asset
def ingest_transactions() -> Output[dict[str, int]]:
    """
    Unified ingestion using data contracts and MD5-based parquet caching.

    Source root: FINANCE_DATA_DIR_CONTAINER/To Parse/Bank/<bank>/**/*
    Contract per table: data/contracts/{table_nm}.yaml
    Parquet cache:      data/finance/raw/{table_nm}/{md5}.parquet
    DuckDB RAW:         prod_raw.<table_nm> (TEXT-only + __load_key, processed_at)
    Ledger:             prod_meta.ingest_ledger (idempotency by file MD5)
    """
    log = get_dagster_logger()

    INPUT_PATH.mkdir(parents=True, exist_ok=True)
    if not INPUT_PATH.exists():
        raise ValueError(f"No To Parse folder found at {INPUT_PATH}")

    con = duckdb.connect(DUCKDB_PATH)
    ensure_meta_schema(con)

    ingested = skipped = errors = unsupported = 0
    seen_tables: set[str] = set()

    log.info("Starting unified ingestion from %s", INPUT_PATH)

    # Bank → data_type dir → files
    bank_dirs = sorted(p for p in INPUT_PATH.iterdir() if p.is_dir())
    log.info("Found %d bank directories: %s", len(bank_dirs), [d.name for d in bank_dirs])

    for bank_dir in bank_dirs:
        bank_nm = bank_dir.name
        log.info("Processing bank directory: %s", bank_nm)

        data_type_dirs = sorted(p for p in bank_dir.iterdir() if p.is_dir())
        if not data_type_dirs:
            log.warning(
                "⚠️  No data type subdirectories found in %s. Expected structure: Bank/%s/<data_type>/",
                bank_nm,
                bank_nm,
            )
            continue

        log.info(
            "Found %d data type directories in %s: %s",
            len(data_type_dirs),
            bank_nm,
            [d.name for d in data_type_dirs],
        )

        for data_type_dir in data_type_dirs:
            table_nm = extract_table_name_from_dir(data_type_dir, INPUT_PATH)

            contract_path = _validate_contract_for_table(table_nm, data_type_dir, log)
            if not contract_path:
                continue

            try:
                contract = DataContract.from_yaml(contract_path)
                log.info("Loaded contract for table %s", table_nm)
            except Exception as exc:
                log.exception("Failed to load contract %s: %s", contract_path, exc)
                continue

            # Only drop table if we're actually going to load new data
            should_drop = _should_drop_table_for_fresh_load(
                con, contract, data_type_dir, seen_tables, log
            )
            if should_drop:
                _ensure_fresh_table(con, contract.target_table.name, seen_tables, log)
            else:
                # Mark as seen to avoid checking again
                seen_tables.add(contract.target_table.name)

            i, s, u, err_count = _process_table_files(con, contract, data_type_dir, bank_nm, log)
            ingested += i
            skipped += s
            unsupported += u
            errors += err_count

    con.close()
    log.info(
        "Ingestion complete. Ingested=%d, Skipped=%d, Unsupported=%d, Errors=%d",
        ingested,
        skipped,
        unsupported,
        errors,
    )

    return Output(
        {"ingested": ingested, "skipped": skipped, "unsupported": unsupported, "errors": errors},
        metadata={
            "ingested": ingested,
            "skipped": skipped,
            "unsupported": unsupported,
            "errors": errors,
        },
    )
