"""Shared utility functions for the finance analytics pipeline."""

import hashlib
import re
import time
from datetime import UTC, date, datetime
from pathlib import Path

# Minimum row length for transaction data (Date, Platform Name)
MIN_TRANSACTION_ROW_LENGTH = 2


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


def parse_date_string(date_str: str) -> datetime | None:
    """Parse date string from Google Sheets to datetime object."""
    if not date_str:
        return None

    # Try different date formats that might come from Google Sheets
    date_formats = [
        "%Y-%m-%d",  # 2025-01-15
        "%m/%d/%Y",  # 01/15/2025
        "%d/%m/%Y",  # 15/01/2025
        "%Y-%m-%d %H:%M:%S",  # 2025-01-15 10:30:00
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue

    return None


def get_max_transaction_dates_by_bank(existing_data: list[list[str]]) -> dict[str, date]:
    """
    Get maximum transaction date for each bank from existing Google Sheets data.

    Args:
        existing_data: List of data rows from Google Sheets (without headers)
                      Expected format: [Date, Platform Name, Category, Description, Amount, Currency]

    Returns:
        dict: Bank name -> max transaction date
    """
    max_dates: dict[str, date] = {}

    for row in existing_data:
        if len(row) < MIN_TRANSACTION_ROW_LENGTH:  # Need at least Date and Platform Name
            continue

        date_str = row[0]
        bank_name = row[1]

        parsed_date = parse_date_string(date_str)
        if parsed_date is None:
            continue

        if bank_name not in max_dates or parsed_date > max_dates[bank_name]:
            max_dates[bank_name] = parsed_date

    return max_dates


def filter_new_transactions(
    new_data: list[list[str]], max_dates_by_bank: dict[str, date]
) -> list[list[str]]:
    """
    Filter new transaction data to only include rows with dates greater than max date per bank.

    Args:
        new_data: New transaction data from DuckDB
                 Expected format: [Date, Platform Name, Category, Description, Amount, Currency]
        max_dates_by_bank: Dictionary of bank name -> max transaction date

    Returns:
        list: Filtered new data rows
    """
    filtered_data = []

    for row in new_data:
        if len(row) < MIN_TRANSACTION_ROW_LENGTH:  # Need at least Date and Platform Name
            continue

        date_str = row[0]
        bank_name = row[1]

        parsed_date = parse_date_string(date_str)
        if parsed_date is None:
            continue

        # If no existing data for this bank, include all transactions
        if bank_name not in max_dates_by_bank:
            filtered_data.append(row)
            continue

        # Only include if date is greater than max date for this bank
        if parsed_date > max_dates_by_bank[bank_name]:
            filtered_data.append(row)

    return filtered_data


def is_file_stable(path: Path, stability_seconds: int = 8) -> bool:
    """
    Check if file size is stable for specified seconds.

    Args:
        path: Path to file to check
        stability_seconds: Number of seconds to wait and check

    Returns:
        bool: True if file size didn't change, False otherwise
    """
    try:
        s1 = path.stat().st_size
        time.sleep(stability_seconds)
        return s1 == path.stat().st_size
    except FileNotFoundError:
        return False


def utc_now_str() -> str:
    """Return current UTC timestamp as string in 'YYYY-MM-DD HH:MM:SS' format."""
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
