# datacontract_io/writers.py
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import DataContract
from .paths import resolve_parquet_path, resolve_raw_parquet_path
from .schema import enforce_schema


class Writer:
    def __init__(self, contract: DataContract):
        self.contract = contract


class ParquetWriter(Writer):
    def write(self, df: pd.DataFrame, *, ds: str | None = None, coerce_schema: bool = True) -> str:
        if coerce_schema:
            df = enforce_schema(df, self.contract)
        output_dir = resolve_parquet_path(self.contract, ds=ds)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        path = str(Path(output_dir) / "part-00000.parquet")
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, path)
        return path


class RawParquetWriter(Writer):
    """Writer for raw parquet files (before DuckDB ingestion)."""

    def write(
        self,
        df: pd.DataFrame,
        *,
        output_path: str | None = None,
        md5_hash: str | None = None,
        coerce_schema: bool = False,
    ) -> str:
        """
        Write to raw parquet path with schema aligned to contract.

        For raw layer:
        - Columns are selected and ordered according to contract
        - Types are mapped from contract definitions to PyArrow types
        - If coerce_schema=True, enforce_schema() is applied first (type casting, null validation)

        Args:
            df: DataFrame to write
            output_path: Full output path. If provided, overrides md5_hash.
            md5_hash: MD5 hash to use in filename. Ignored if output_path provided.
            coerce_schema: Whether to enforce full schema validation (type casting, null checks)

        Returns:
            Path to written parquet file
        """
        if coerce_schema:
            df = enforce_schema(df, self.contract)

        if output_path:
            path = output_path
        else:
            table_name = self.contract.target_table.name
            path = resolve_raw_parquet_path(table_name, md5_hash=md5_hash)

        # Ensure directory exists
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        # Select and order columns according to contract
        expected_cols = self.contract.expected_columns()
        missing_cols = [c for c in expected_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns from contract: {missing_cols}")

        df_aligned = df[expected_cols].copy()

        # Build PyArrow schema from contract
        # Map contract types to PyArrow types
        def contract_type_to_pyarrow(col_type: str) -> pa.DataType:
            """Map contract type to PyArrow type."""
            if col_type in ("varchar", "string", "text"):
                return pa.string()
            if col_type in ("int", "bigint", "integer"):
                return pa.int64()
            if col_type in ("float", "double"):
                return pa.float64()
            if col_type in ("decimal", "numeric") or col_type in ("date", "timestamp", "datetime"):
                return pa.string()  # Keep as string in raw layer
            if col_type in ("bool", "boolean"):
                return pa.bool_()
            return pa.string()  # Default to string

        schema_fields = [
            (col_spec.name, contract_type_to_pyarrow(col_spec.type))
            for col_spec in self.contract.columns
        ]
        schema = pa.schema(schema_fields)

        # Convert DataFrame to PyArrow table with explicit schema
        table = pa.Table.from_pandas(df_aligned, schema=schema, preserve_index=False)
        pq.write_table(table, path)
        return path
