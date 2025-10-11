# datacontract_io/__init__.py
"""
Data Contract I/O package for reading, writing, and validating data against contracts.
"""

from .cleaning import clean_df, drop_all_null_rows, normalize_empties
from .contracts import ColumnSpec, DataContract, TargetTable
from .errors import ContractError, ReadError, SchemaError, WriteError
from .paths import resolve_parquet_path
from .readers import CSVReader, ExcelReader, make_reader
from .schema import enforce_schema
from .writers import ParquetWriter

__all__ = [
    # Core contracts
    "DataContract",
    "ColumnSpec",
    "TargetTable",
    # Readers
    "CSVReader",
    "ExcelReader",
    "make_reader",
    # Writers
    "ParquetWriter",
    # Schema enforcement
    "enforce_schema",
    # Cleaning utilities
    "clean_df",
    "normalize_empties",
    "drop_all_null_rows",
    # Errors
    "ContractError",
    "ReadError",
    "WriteError",
    "SchemaError",
    # Path utilities
    "resolve_parquet_path",
]
