# file: orchestration/assets_quality_ge.py
"""
Great Expectations Data Quality Assets (Code-First, GX 1.7+)

This module defines Dagster assets that run Great Expectations checkpoints
using a code-first approach. All checkpoints are bootstrapped programmatically
at runtime, eliminating reliance on YAML checkpoint files.

Architecture:
- Checkpoints, suites, and validation definitions are defined in code
- Bootstrap happens automatically before each checkpoint run
- GX stores artifacts as JSON (can be gitignored or committed)
"""
import contextlib
import os
from pathlib import Path
from typing import Any

import great_expectations as gx
from dagster import AssetExecutionContext, Failure, MetadataValue, Output, asset

from orchestration.quality_gx.bootstrap import run_checkpoint_with_dataframes
from src.logging_config import logger

# Support both local and container environments
GE_DIR = os.getenv("GE_DIR", None)
if not GE_DIR:
    # Default: resolve relative to project root (parent of orchestration directory)
    project_root = Path(__file__).parent.parent
    GE_DIR = str(project_root / "quality" / "gx")

# Set DUCKDB_PATH for GE datasource connection if not already set
DUCKDB_PATH = os.getenv("DUCKDB_PATH", None)
if not DUCKDB_PATH:
    project_root = Path(__file__).parent.parent
    DUCKDB_PATH = str(project_root / "data" / "warehouse" / "analytics.duckdb")

# Normalize path for GX datasource (convert Windows backslashes to forward slashes)
DUCKDB_PATH_NORMALIZED = DUCKDB_PATH.replace("\\", "/") if DUCKDB_PATH else ""

# Set environment variables for GX datasource
os.environ["DUCKDB_PATH"] = DUCKDB_PATH_NORMALIZED
os.environ["DUCKDB_URL"] = f"duckdb:///{DUCKDB_PATH_NORMALIZED}"


def _get_data_context() -> Any:
    """
    Get Great Expectations context with diagnostic logging.

    Returns:
        Configured GX DataContext

    Raises:
        FileNotFoundError: If GE_DIR doesn't exist
    """
    if not GE_DIR:
        raise FileNotFoundError("GE_DIR environment variable is not set")

    root = Path(GE_DIR).resolve()
    if not root.exists():
        raise FileNotFoundError(
            f"Great Expectations root not found at {root}. "
            f"Set GE_DIR or ensure the folder is mounted into the container."
        )

    logger.info("Loading GE context from: %s", root)
    logger.info("DUCKDB_URL: %s", os.getenv("DUCKDB_URL"))

    context = gx.get_context(context_root_dir=str(root))  # type: ignore[attr-defined]

    # Log GX version and configuration
    with contextlib.suppress(Exception):
        logger.info("great_expectations version: %s", gx.__version__)

    return context


def _run_checkpoint(checkpoint_name: str, filter_tables: set[str] | None = None) -> dict[str, Any]:
    """
    Run a checkpoint by reading data from DuckDB and validating with Pandas.

    This function uses a DataFrame-based approach that bypasses SQLAlchemy/DuckDB
    compatibility issues. It:
    1. Reads data directly from DuckDB to DataFrame
    2. Creates GX validator with the DataFrame
    3. Runs validations
    4. Returns results

    Args:
        checkpoint_name: Name of the checkpoint to run
        filter_tables: Optional set of table names to validate (e.g., {'t_bank_transactions', 'bakai_transactions'})
                      If provided, only validations for these tables will run

    Returns:
        Dictionary with success status and validation results

    Raises:
        RuntimeError: If checkpoint validation fails
    """
    logger.info("Running checkpoint: %s", checkpoint_name)

    # Run checkpoint using DataFrame-based approach
    result = run_checkpoint_with_dataframes(checkpoint_name, filter_tables=filter_tables)

    return {
        "success": result["success"],
        "checkpoint_name": result["checkpoint_name"],
        "validation_count": result["validation_count"],
    }


def _handle_checkpoint_failure(checkpoint_name: str, error: Exception) -> None:
    """Handle checkpoint validation failures."""
    raise Failure(
        description=f"GE checkpoint '{checkpoint_name}' failed validation: {error!s}",
        metadata={"checkpoint": MetadataValue.text(checkpoint_name)},
    ) from error


def _handle_unexpected_exception(checkpoint_name: str, error: Exception) -> None:
    """Handle unexpected exceptions during checkpoint execution."""
    raise Failure(
        description=f"Unexpected error while running GE checkpoint '{checkpoint_name}': {error!s}",
        metadata={"checkpoint": MetadataValue.text(checkpoint_name)},
    ) from error


@asset(deps=["ingest_bank"])
def run_ge_raw_checkpoints(context: AssetExecutionContext) -> Output[dict[str, str]]:
    """
    Run Great Expectations checkpoints on raw tables.

    Only validates tables that were actually ingested (have data in last 24 hours).
    This checkpoint ensures:
    - Tables exist and have data
    - Critical columns are present
    - Load keys are unique (no duplicates)
    - Data is recent
    """
    from src.duckdb_utils import get_recently_ingested_tables

    checkpoint_name = "check_raw"

    # Get list of ingested tables from ledger
    ingestion_info = get_recently_ingested_tables(DUCKDB_PATH_NORMALIZED, hours=24)
    tables_to_process = ingestion_info.get("tables_to_process", [])
    ingested_table_names = {t["table_nm"] for t in tables_to_process}

    context.log.info("Tables ingested in last 24h: %s", ingested_table_names)

    if not ingested_table_names:
        context.log.info("No tables ingested recently - skipping raw quality checks")
        return Output(
            {"status": "skipped", "checkpoint": checkpoint_name, "reason": "no_recent_ingestions"},
            metadata={
                "checkpoint": MetadataValue.text(checkpoint_name),
                "status": MetadataValue.text("skipped"),
                "validation_count": MetadataValue.int(0),
            },
        )

    try:
        result = _run_checkpoint(checkpoint_name, filter_tables=ingested_table_names)
    except RuntimeError as error:  # pragma: no cover - runtime error path
        context.log.error("GE raw checkpoint failed: %s", error)
        _handle_checkpoint_failure(checkpoint_name, error)
    except Exception as error:  # pragma: no cover - runtime error path
        context.log.error("Unexpected error in GE raw checkpoint: %s", error)
        _handle_unexpected_exception(checkpoint_name, error)

    context.log.info("Raw table GE checkpoint completed successfully")
    return Output(
        {"status": "success", "checkpoint": checkpoint_name},
        metadata={
            "checkpoint": MetadataValue.text(checkpoint_name),
            "status": MetadataValue.text("success"),
            "validation_count": MetadataValue.int(result.get("validation_count", 0)),
        },
    )


@asset(deps=["build_dbt_models"])
def run_ge_mart_checkpoints(context: AssetExecutionContext) -> Output[dict[str, str]]:
    """
    Run Great Expectations checkpoints on mart tables.

    Validates business logic integrity in mart-level data.
    This checkpoint ensures:
    - Exchange rates are populated
    - Amount calculations are reasonable
    - No unexpected null values in critical business fields
    """
    checkpoint_name = "check_mart"

    try:
        result = _run_checkpoint(checkpoint_name)
    except RuntimeError as error:  # pragma: no cover - runtime error path
        context.log.error("GE mart checkpoint failed: %s", error)
        _handle_checkpoint_failure(checkpoint_name, error)
    except Exception as error:  # pragma: no cover - runtime error path
        context.log.error("Unexpected error in GE mart checkpoint: %s", error)
        _handle_unexpected_exception(checkpoint_name, error)

    context.log.info("Mart table GE checkpoint completed successfully")
    return Output(
        {"status": "success", "checkpoint": checkpoint_name},
        metadata={
            "checkpoint": MetadataValue.text(checkpoint_name),
            "status": MetadataValue.text("success"),
            "validation_count": MetadataValue.int(result.get("validation_count", 0)),
        },
    )
