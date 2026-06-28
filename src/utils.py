"""Shared utility functions for the finance analytics pipeline."""

import hashlib
import re
import time
from datetime import UTC, datetime
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
    """
    Parse date/datetime string from Google Sheets to datetime object.

    Supports various formats including ISO 8601 datetime strings with timezone.
    For backwards compatibility with existing date-only data.
    """
    if not date_str:
        return None

    date_str = date_str.strip()

    # First try parsing as ISO format datetime (most common for new data).
    # Supports both "2025-01-15T10:30:00+03:00" and "2025-01-15 10:30:00+03:00".
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        pass

    # Try different date/datetime formats that might come from Google Sheets
    date_formats = [
        "%Y-%m-%d %H:%M:%S",  # 2025-01-15 10:30:00 (space-separated datetime)
        "%Y-%m-%d",  # 2025-01-15 (date only)
        "%m/%d/%Y",  # 01/15/2025 (US format)
        "%d/%m/%Y",  # 15/01/2025 (European format)
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return None


def get_max_transaction_datetimes_by_platform(
    existing_data: list[list[str]],
) -> dict[str, datetime]:
    """
    Get maximum transaction datetime for each platform from existing Google Sheets data.

    Args:
        existing_data: List of data rows from Google Sheets (without headers)
                      Expected format: [DateTime, Platform Name, Category, Description, Amount, Currency]

    Returns:
        dict: Platform name -> max transaction datetime
    """
    max_datetimes: dict[str, datetime] = {}

    for row in existing_data:
        if len(row) < MIN_TRANSACTION_ROW_LENGTH:  # Need at least DateTime and Platform Name
            continue

        datetime_str = row[0]
        platform_name = row[1]

        parsed_datetime = parse_date_string(datetime_str)
        if parsed_datetime is None:
            continue

        if platform_name not in max_datetimes or parsed_datetime > max_datetimes[platform_name]:
            max_datetimes[platform_name] = parsed_datetime

    return max_datetimes


def filter_new_transactions(
    new_data: list[list[str]], max_datetimes_by_platform: dict[str, datetime]
) -> list[list[str]]:
    """
    Filter new transaction data to only include rows with datetimes greater than max datetime per platform.

    Args:
        new_data: New transaction data from DuckDB
                 Expected format: [DateTime, Platform Name, Category, Description, Amount, Currency]
        max_datetimes_by_platform: Dictionary of platform name -> max transaction datetime

    Returns:
        list: Filtered new data rows
    """
    filtered_data = []

    for row in new_data:
        if len(row) < MIN_TRANSACTION_ROW_LENGTH:  # Need at least DateTime and Platform Name
            continue

        datetime_str = row[0]
        platform_name = row[1]

        parsed_datetime = parse_date_string(datetime_str)
        if parsed_datetime is None:
            continue

        # If no existing data for this platform, include all transactions
        if platform_name not in max_datetimes_by_platform:
            filtered_data.append(row)
            continue

        # Only include if datetime is greater than max datetime for this platform
        if parsed_datetime > max_datetimes_by_platform[platform_name]:
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
