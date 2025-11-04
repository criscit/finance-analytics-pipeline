"""
Bootstrap functions for Great Expectations checkpoints.

This module provides functions to run GX validations using DataFrame-based approach,
bypassing SQLAlchemy/DuckDB compatibility issues by reading data directly from DuckDB
and validating with Pandas execution engine.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import great_expectations as gx
import pandas as pd

from orchestration.quality_gx.checkpoints import get_checkpoint_configs
from src.logging_config import logger

if TYPE_CHECKING:
    from typing import Protocol

    class AbstractDataContext(Protocol):
        """Protocol for GX DataContext."""

        data_sources: Any
        suites: Any


# Constants
_EXPECTED_TABLE_NAME_PARTS = 2


def _get_duckdb_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """
    Get DuckDB connection to the warehouse database.

    Args:
        read_only: If True, opens connection in read-only mode (default: True)

    Returns:
        DuckDB connection object
    """
    duckdb_path = os.getenv("DUCKDB_PATH")
    if not duckdb_path:
        # Default: resolve relative to project root
        project_root = Path(__file__).parent.parent.parent
        duckdb_path = str(project_root / "data" / "warehouse" / "analytics.duckdb")

    return duckdb.connect(duckdb_path, read_only=read_only)


def _ensure_raw_table_exists(schema: str, table: str) -> bool:
    """
    Ensure raw table exists, attempting to restore from parquets if missing.

    This is a defensive recovery mechanism that:
    1. Checks if table exists
    2. If missing, tries to restore from cached parquet files
    3. If no parquets exist, returns False (table truly doesn't exist)

    Args:
        schema: Schema name (e.g., 'prod_raw')
        table: Table name (e.g., 't_bank_transactions')

    Returns:
        True if table exists or was successfully restored, False otherwise
    """
    try:
        from src.duckdb_utils import restore_raw_table_from_parquets, table_exists

        # Check if table exists (read-only connection)
        conn_readonly = _get_duckdb_connection(read_only=True)
        exists = table_exists(conn_readonly, schema, table)
        conn_readonly.close()

        if exists:
            logger.info("✓ Table %s.%s exists", schema, table)
            return True

        logger.warning(
            "Table %s.%s does not exist - attempting to restore from parquets...", schema, table
        )

        # Try to restore from parquets (requires writable connection)
        # Get parquet directory from environment
        import os
        from pathlib import Path

        raw_data_dir = os.getenv("RAW_DATA_DIR")
        if not raw_data_dir:
            project_root = Path(__file__).parent.parent.parent
            raw_data_dir = str(project_root / "data" / "raw")

        parquet_dir = Path(raw_data_dir) / table

        if not parquet_dir.exists():
            logger.error(
                "Cannot restore %s.%s - parquet directory not found: %s", schema, table, parquet_dir
            )
            return False

        # Open writable connection for restoration
        conn_writable = _get_duckdb_connection(read_only=False)

        # Restore table from all parquets in directory
        rows_restored = restore_raw_table_from_parquets(
            conn_writable, schema, table, str(parquet_dir)
        )
        conn_writable.close()

        logger.info(
            "✓ Successfully restored %s.%s from parquets (%s rows)", schema, table, rows_restored
        )
        return True

    except FileNotFoundError as e:
        logger.error("Cannot restore %s.%s: %s", schema, table, e)
        return False
    except Exception as e:
        logger.error("Error checking/restoring table %s.%s: %s", schema, table, e)
        return False


def _read_table_to_dataframe(table_name: str) -> pd.DataFrame:
    """
    Read a table from DuckDB directly to Pandas DataFrame.

    Includes defensive check to restore missing tables from parquet cache.

    Args:
        table_name: Fully qualified table name (e.g., "prod_raw.t_bank_transactions")

    Returns:
        Pandas DataFrame with table data

    Raises:
        RuntimeError: If table doesn't exist and cannot be restored
    """
    logger.info("Reading table '%s' from DuckDB...", table_name)

    # Parse schema and table name
    parts = table_name.split(".")
    if len(parts) != _EXPECTED_TABLE_NAME_PARTS:
        raise ValueError(f"Table name must be schema.table format, got: {table_name}")

    schema, table = parts

    # For raw tables, ensure they exist (restore if needed)
    if schema == "prod_raw" and not _ensure_raw_table_exists(schema, table):
        raise RuntimeError(
            f"Table '{table_name}' does not exist and cannot be restored from parquets. "
            f"This likely means no data has been ingested yet."
        )

    try:
        conn = _get_duckdb_connection()
        df = conn.execute(f"SELECT * FROM {table_name}").df()
        conn.close()

        logger.info("✓ Read %s rows from '%s'", len(df), table_name)
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to read table '{table_name}' from DuckDB: {e}") from e


def _create_gx_batch_for_dataframe(context: Any, validation_name: str, df: pd.DataFrame) -> Any:
    """
    Create a GX batch from a DataFrame using temporary Pandas datasource.

    Args:
        context: GX context
        validation_name: Name of the validation (used for unique naming)
        df: Pandas DataFrame to validate

    Returns:
        GX Batch object ready for validation
    """
    # Create a temporary Pandas datasource and asset for this validation
    datasource_name = f"temp_pandas_{validation_name}"
    try:
        # Try to get existing datasource
        datasource = context.data_sources.get(datasource_name)
    except Exception:
        # Create new Pandas datasource
        datasource = context.data_sources.add_pandas(datasource_name)

    # Add dataframe asset
    asset_name = f"df_{validation_name}"
    try:
        asset = datasource.get_asset(asset_name)
    except Exception:
        asset = datasource.add_dataframe_asset(name=asset_name)

    # Create batch definition with the DataFrame
    batch_def_name = f"{asset_name}_batch"
    try:
        batch_definition = asset.get_batch_definition(batch_def_name)
    except Exception:
        batch_definition = asset.add_batch_definition_whole_dataframe(batch_def_name)

    # Get batch with actual DataFrame
    batch_params = {"dataframe": df}
    return batch_definition.get_batch(batch_parameters=batch_params)


def _log_validation_result(validation_name: str, result: Any) -> bool:
    """
    Log validation result and return success status.

    Args:
        validation_name: Name of the validation
        result: GX validation result

    Returns:
        True if validation passed, False otherwise
    """
    if not result.success:
        logger.warning(
            "✗ Validation '%s' failed: %s/%s expectations failed",
            validation_name,
            result.statistics["unsuccessful_expectations"],
            result.statistics["evaluated_expectations"],
        )
        return False

    logger.info(
        "✓ Validation '%s' passed: %s/%s expectations succeeded",
        validation_name,
        result.statistics["successful_expectations"],
        result.statistics["evaluated_expectations"],
    )
    return True


def run_checkpoint_with_dataframes(
    checkpoint_name: str,
    filter_tables: set[str] | None = None,
) -> dict[str, Any]:
    """
    Run a checkpoint by reading data from DuckDB and validating with Pandas.

    This approach bypasses SQLAlchemy/DuckDB engine issues by:
    1. Reading data directly from DuckDB to DataFrame
    2. Using GX's Pandas execution engine for validation
    3. Running expectations against the DataFrame

    Args:
        checkpoint_name: Name of the checkpoint to run
        filter_tables: Optional set of table names to validate (e.g., {'t_bank_transactions', 'bakai_transactions'})
                      If provided, only validations for these tables will run

    Returns:
        Dictionary with success status and validation results

    Raises:
        RuntimeError: If checkpoint validation fails
        ValueError: If checkpoint config not found
    """
    # Get checkpoint configuration
    all_configs = get_checkpoint_configs()
    if checkpoint_name not in all_configs:
        raise ValueError(
            f"Checkpoint '{checkpoint_name}' not found. " f"Available: {list(all_configs.keys())}"
        )

    checkpoint_config = all_configs[checkpoint_name]
    logger.info("Running checkpoint: %s", checkpoint_name)
    logger.info("Description: %s", checkpoint_config.description)

    # Get GX context
    gx_dir = os.getenv("GE_DIR")
    if not gx_dir:
        project_root = Path(__file__).parent.parent.parent
        gx_dir = str(project_root / "data" / "quality" / "gx")

    context = gx.get_context(context_root_dir=gx_dir)  # type: ignore[attr-defined]

    # Run validations for each table in the checkpoint
    all_results = []
    all_success = True

    for validation_config in checkpoint_config.validations:
        # Filter validations based on ingested tables
        # Extract table name from duckdb_table (e.g., "prod_raw.t_bank_transactions" -> "t_bank_transactions")
        table_name = validation_config.duckdb_table.split(".")[-1]

        if filter_tables is not None and table_name not in filter_tables:
            logger.info(
                "Skipping validation '%s' - table '%s' not in filter set",
                validation_config.name,
                table_name,
            )
            continue
        logger.info("Validating: %s", validation_config.name)
        logger.info("  Table: %s", validation_config.duckdb_table)
        logger.info("  Suite: %s", validation_config.suite_name)

        try:
            # Read data from DuckDB
            df = _read_table_to_dataframe(validation_config.duckdb_table)

            # Get or create expectation suite
            from orchestration.quality_gx.suites import get_or_create_suite

            suite = get_or_create_suite(context, validation_config.suite_name)

            # Create GX batch from DataFrame
            batch = _create_gx_batch_for_dataframe(context, validation_config.name, df)

            # Create validator
            validator = context.get_validator(
                batch=batch,
                expectation_suite=suite,
            )

            # Run validation
            result = validator.validate()
            all_results.append(result)

            # Log result and track overall success
            if not _log_validation_result(validation_config.name, result):
                all_success = False

        except Exception as e:
            logger.error("Error running validation '%s': %s", validation_config.name, e)
            raise RuntimeError(
                f"Validation '{validation_config.name}' encountered an error: {e}"
            ) from e

    if not all_success:
        raise RuntimeError(
            f"Checkpoint '{checkpoint_name}' failed validation. " "Check logs for detailed results."
        )

    logger.info("✓ Checkpoint '%s' passed all validations", checkpoint_name)

    return {
        "success": all_success,
        "checkpoint_name": checkpoint_name,
        "validation_count": len(all_results),
        "results": all_results,
    }
