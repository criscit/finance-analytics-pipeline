# file: orchestration/dagster_project/src/assets_ingest.py
import hashlib
import os
import re
import time
from pathlib import Path

import duckdb
from dagster import Output, asset, get_dagster_logger

RAW_PATH = Path(os.getenv("RAW_PATH", "/app/data/prod_raw"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse/analytics.duckdb")
STABILITY_S = int(os.getenv("RAW_STABILITY_SECONDS", "8"))

INPUT_PATH = RAW_PATH / "To Parse" / "Bank"

# -------------------------
# Helpers
# -------------------------


def _stable(path: Path) -> bool:
    try:
        s1 = path.stat().st_size
        time.sleep(STABILITY_S)
        return s1 == path.stat().st_size
    except FileNotFoundError:
        return False


def _md5(path: Path) -> str:
    hash = hashlib.md5()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1 << 20), b""):
            hash.update(chunk)
    return hash.hexdigest()


def qident(name: str) -> str:
    """Quote an identifier for DuckDB (schema/table/column)."""
    return '"' + name.replace('"', '""') + '"'


def qtable(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def _extract_table_name_from_dir(dir_path: Path) -> str:
    """
    INPUT_PATH/T-Bank/transactions  -> t_bank_transactions
    """
    rel = dir_path.relative_to(INPUT_PATH)  # e.g. Path('T-Bank/transactions')
    parts = rel.parts  # ('T-Bank','transactions')
    raw_name = "_".join(parts).lower()
    table_name = re.sub(r"[^a-z0-9_]", "_", raw_name)
    table_name = re.sub(r"_+", "_", table_name).strip("_")
    return table_name or "unknown"


def _file_columns(con: duckdb.DuckDBPyConnection, path: Path) -> list[str]:
    """
    Infer CSV columns without Pandas:
    DuckDB: DESCRIBE SELECT * FROM read_csv_auto(...)  -> first column is column_name
    """
    rows = con.execute(
        "describe select * from read_csv_auto(?, header=true)",
        [str(path)],
    ).fetchall()
    return [r[0] for r in rows]  # column_name


def _table_columns(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
) -> list[str]:
    rows = con.execute(f"pragma table_info({qtable(schema, table)})").fetchall()
    # rows: (cid, name, type, notnull, dflt, pk)
    return [r[1] for r in rows]


def build_load_key_expr(file_cols: list[str]) -> str:
    """
    Deterministic hash of the whole row:
    - stable order (sorted column names)
    - NULL-safe with explicit tag
    - length-prefix each value to avoid delimiter collisions
    """
    columns = sorted(file_cols)
    pieces = []
    for col in columns:
        col_qid = qident(col)
        val = f"coalesce({col_qid}, '<NULL>')"
        # len:value pattern: LENGTH||':'||value
        pieces.append(f"cast(length({val}) as varchar) || ':' || {val}")
    concat_all = " || '|' || ".join(pieces)
    return f"md5({concat_all})"


def _ensure_raw_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    path: Path,
) -> None:
    """
    Create prod_raw.<table> with explicit TEXT columns = file headers,
    plus BK + metadata. No CTAS typing here.
    """
    file_cols = _file_columns(con, path)
    cols_ddl = ",\n  ".join(f"{qident(c)} text" for c in file_cols)

    con.execute(f"create schema if not exists {qident(schema)};")
    con.execute(
        f"""
      create table if not exists {qtable(schema, table)} (
        __load_key varchar(32),
        {cols_ddl},
        __ingested_at timestamp
      );
    """,
    )
    # handy lookup
    con.execute(
        f"""
      create index if not exists ix_{table}_bk
      on {qtable(schema, table)}(__load_key);
    """,
    )


def _append_file(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    path: Path,
) -> int:
    """
    Append rows with a staging read of the CSV.
    RAW stays TEXT-only; BK built from raw strings; metadata added.
    Only inserts rows with transaction_bks that don't already exist in the target table.

    Returns:
        int: Number of rows actually inserted (excluding duplicates)
    """
    _ensure_raw_table(con, schema, table, path)

    tgt_cols = _table_columns(con, schema, table)  # RAW table cols (include BK+meta)
    file_cols = _file_columns(con, path)  # CSV headers

    load_key_expr = build_load_key_expr(file_cols)

    # Build SELECT list aligned to target table order
    aligned = []
    for c in tgt_cols:
        if c == "__load_key":
            aligned.append(f"{load_key_expr} as {qident(c)}")
        elif c == "__ingested_at":
            aligned.append(
                f"(current_timestamp at time zone 'UTC') AS {qident(c)}",
            )
        else:
            # data columns: pass through if exists, else NULL
            aligned.append(
                (f"{qident(c)} as {qident(c)}" if c in file_cols else f"NULL AS {qident(c)}"),
            )

    try:
        result = con.execute(
            f"""
            insert into {qtable(schema, table)}
            select distinct
                {", ".join(aligned)}
            from
                read_csv_auto(?, header=true, all_varchar=true)
            ;
            """,
            [str(path)],
        )
        return int(result.rowcount)
    except Exception:
        raise


# -------------------------
# Asset
# -------------------------


@asset
def ingest_csv_to_duckdb() -> Output[dict[str, int]]:
    """
    Ingest CSVs:
      - RAW_PATH/To Parse/Bank/<bank>/**/*.csv  → prod_raw.<derived_table_name>
    Ledger: prod_meta.ingest_ledger(src_path, md5)
    """
    log = get_dagster_logger()
    INPUT_PATH.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("create schema if not exists prod_meta;")
    con.execute("create schema if not exists prod_raw;")
    con.execute(
        """
        create table if not exists prod_meta.ingest_ledger (
            bank varchar(31),
            table_name varchar(127),
            file_name varchar(255),
            src_path varchar(511),
            size bigint,
            md5 varchar(32),
            ingested_at timestamp
        )
        """,
    )

    ingested_count = 0
    skipped = 0

    def process_file(bank: str, table_name: str, path: Path) -> None:
        nonlocal ingested_count, skipped
        if not path.is_file() or path.suffix.lower() != ".csv":
            return
        if not _stable(path):
            log.info("Skipping unstable file: %s", path)
            return

        rel = path.relative_to(RAW_PATH)
        src_path = rel.as_posix()  # normalize slashes
        md5 = _md5(path)
        size = path.stat().st_size

        # Dedup by (src_path, md5)
        if con.execute(
            "select 1 from prod_meta.ingest_ledger where src_path = ? and md5 = ?",
            [src_path, md5],
        ).fetchone():
            skipped += 1
            return

        ingested_at = time.strftime("%Y-%m-%d %H:%M:%S")

        # Atomic per-file ingestion
        con.execute("begin;")

        try:
            _append_file(con, "prod_raw", table_name, path)
            con.execute(
                """
                insert into prod_meta.ingest_ledger(src_path, bank, table_name,file_name, size, md5, ingested_at)
                values (?,?,?,?,?,?,cast(? as timestamp))
                """,
                [src_path, bank, table_name, path.name, size, md5, ingested_at],
            )
            con.execute("commit;")
        except Exception as e:
            con.execute("rollback;")
            log.exception("Failed to ingest %s: %s", path, e)
            raise

        ingested_count += 1
        log.info("Ingested %s -> %s", path, qtable("prod_raw", table_name))

    if not INPUT_PATH.exists():
        log.error("No To Parse folder found at %s", INPUT_PATH)
        msg = f"No To Parse folder found at {INPUT_PATH}"
        raise ValueError(msg)

    for bank_dir in sorted([p for p in INPUT_PATH.iterdir() if p.is_dir()]):
        bank = bank_dir.name
        for bank_data_type_dir in sorted([p for p in bank_dir.iterdir() if p.is_dir()]):
            table_name = _extract_table_name_from_dir(bank_data_type_dir)
            con.execute(f"drop table if exists prod_raw.{qident(table_name)}")
            for file_path in bank_data_type_dir.rglob("*.csv"):
                process_file(bank, table_name, file_path)
    con.close()
    log.info("Ingestion complete. New files: %s, skipped: %s", ingested_count, skipped)
    return Output(
        {"ingested": ingested_count, "skipped": skipped},
        metadata={"ingested": ingested_count, "skipped": skipped},
    )
