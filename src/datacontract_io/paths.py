# datacontract_io/paths.py
import os
from datetime import date
from pathlib import Path

from .contracts import DataContract


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def resolve_parquet_path(contract: DataContract, *, ds: str | None = None) -> str:
    base = env("DATA_WAREHOUSE_DIR", "data/warehouse")
    schema = contract.target_table.schema
    name = contract.target_table.name
    ds = ds or date.today().isoformat()
    # e.g., data/warehouse/prod_raw/bakai_transactions/ds=2025-10-10/
    return str(Path(base) / schema / name / f"ds={ds}")


def resolve_raw_parquet_path(table_name: str, *, md5_hash: str | None = None) -> str:
    """
    Resolve path for raw parquet files before DuckDB ingestion.

    Args:
        table_name: Target table name
        md5_hash: MD5 hash of source file content. If provided, returns full file path.
                  If None, returns directory path.

    Returns:
        Full file path if md5_hash provided, otherwise directory path
        e.g., /app/data/raw/t_bank_transactions/abc123def456.parquet
    """
    base = "/app/data"
    dir_path = Path(base) / "raw" / table_name

    if md5_hash:
        return str(dir_path / f"{md5_hash}.parquet")
    return str(dir_path)


def resolve_contract_path(table_name: str) -> str:
    """
    Resolve path to data contract YAML file.

    Uses DATA_CONTRACTS_PATH if set, otherwise derives from FINANCE_DATA_DIR_CONTAINER.

    Args:
        table_name: Name of the table (e.g., 't_bank_transactions')

    Returns:
        Full path to contract YAML file (e.g., /app/data/contracts/t_bank_transactions.yaml)
    """
    # Use explicit contracts path if set
    contracts_dir = os.environ.get("DATA_CONTRACTS_PATH")

    if not contracts_dir:
        # Fallback: derive from FINANCE_DATA_DIR_CONTAINER
        base = env("FINANCE_DATA_DIR_CONTAINER", "/app/data/finance")
        # Extract the parent directory (e.g., /app/data or data)
        data_dir = Path(base).parent
        contracts_dir = str(data_dir / "contracts")

    return str(Path(contracts_dir) / f"{table_name}.yaml")
