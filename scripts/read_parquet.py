#!/usr/bin/env python3
"""
Simple CLI tool to read and inspect Parquet files using PyArrow.

Usage:
    python scripts/read_parquet.py <file_path> [options]

Examples:
    # Show schema only
    python scripts/read_parquet.py data/raw/bakai_transactions/file.parquet --schema

    # Show first 10 rows
    python scripts/read_parquet.py data/raw/bakai_transactions/file.parquet --rows 10

    # Show all data
    python scripts/read_parquet.py data/raw/bakai_transactions/file.parquet --all
"""

import argparse
import sys
from pathlib import Path

import pyarrow.parquet as pq


def read_parquet_file(
    file_path: str, show_schema: bool = False, num_rows: int | None = None, show_all: bool = False
) -> None:
    """Read and display parquet file contents."""
    path = Path(file_path)

    if not path.exists():
        print(f"❌ Error: File not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    if path.suffix.lower() != ".parquet":
        print(f"⚠️  Warning: File doesn't have .parquet extension: {file_path}", file=sys.stderr)

    try:
        # Read parquet file
        table = pq.read_table(file_path)

        print(f"📄 File: {file_path}")
        print(f"📊 Total rows: {table.num_rows:,}")
        print(f"📋 Total columns: {table.num_columns}")
        print()

        # Show schema
        if show_schema or (not show_all and num_rows is None):
            print("=" * 80)
            print("SCHEMA")
            print("=" * 80)
            print(table.schema)
            print()

        # Show data
        if show_all or num_rows is not None:
            print("=" * 80)
            print("DATA")
            print("=" * 80)

            if show_all:
                df = table.to_pandas()
                print(df.to_string())
            elif num_rows:
                df = table.slice(0, num_rows).to_pandas()
                print(df.to_string())
            print()

        # Show metadata if available
        metadata = table.schema.metadata
        if metadata:
            print("=" * 80)
            print("METADATA")
            print("=" * 80)
            for key, value in metadata.items():
                print(f"{key.decode()}: {value.decode()}")
            print()

    except Exception as e:
        print(f"❌ Error reading parquet file: {e}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Read and inspect Parquet files using PyArrow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "file_path",
        help="Path to the parquet file to read",
    )

    parser.add_argument(
        "-s",
        "--schema",
        action="store_true",
        help="Show only the schema (default if no other option specified)",
    )

    parser.add_argument(
        "-r",
        "--rows",
        type=int,
        metavar="N",
        help="Show first N rows of data",
    )

    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Show all rows (use with caution for large files)",
    )

    args = parser.parse_args()

    read_parquet_file(
        file_path=args.file_path,
        show_schema=args.schema,
        num_rows=args.rows,
        show_all=args.all,
    )


if __name__ == "__main__":
    main()
