"""Shared utility functions for the finance analytics pipeline."""

import hashlib
import re
from pathlib import Path


def qident(name: str) -> str:
    """Quote an identifier for DuckDB (schema/table/column)."""
    return '"' + name.replace('"', '""') + '"'


def qtable(schema: str, table: str) -> str:
    """Quote a table name for DuckDB."""
    return f"{qident(schema)}.{qident(table)}"


def md5_hash(path: Path) -> str:
    """Calculate MD5 hash of a file."""
    hash_obj = hashlib.md5()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1 << 20), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def extract_table_name_from_dir(dir_path: Path, base_path: Path) -> str:
    """Extract table name from directory path."""
    rel = dir_path.relative_to(base_path)
    parts = rel.parts
    raw_name = "_".join(parts).lower()
    table_name = re.sub(r"[^a-z0-9_]", "_", raw_name)
    table_name = re.sub(r"_+", "_", table_name).strip("_")
    return table_name or "unknown"


def build_load_key_expr(file_cols: list[str]) -> str:
    """Build deterministic hash expression for load keys."""
    columns = sorted(file_cols)
    pieces = []
    for col in columns:
        col_qid = qident(col)
        val = f"coalesce({col_qid}, '<NULL>')"
        pieces.append(f"cast(length({val}) as varchar) || ':' || {val}")
    concat_all = " || '|' || ".join(pieces)
    return f"md5({concat_all})"
