"""
Checkpoint Definitions

This module defines all checkpoint configurations as code.
Checkpoints orchestrate validation definitions and actions.

Uses DataFrame-based approach to avoid SQLAlchemy/DuckDB compatibility issues.
"""

from dataclasses import dataclass


@dataclass
class ValidationConfig:
    """Configuration for a single validation (suite + data source)."""

    name: str
    suite_name: str
    duckdb_table: str  # Fully qualified table name (e.g., "prod_raw.t_bank_transactions")
    description: str = ""


@dataclass
class CheckpointConfig:
    """Configuration for a checkpoint."""

    name: str
    validations: list[ValidationConfig]
    description: str = ""


# Checkpoint configurations
RAW_CHECKPOINT = CheckpointConfig(
    name="check_raw",
    description="Validates raw tables before dbt transformation",
    validations=[
        ValidationConfig(
            name="raw_load_t_bank_transactions",
            suite_name="raw.t_bank_transactions.data_presence_check",
            duckdb_table="prod_raw.t_bank_transactions",
            description="Data presence and integrity checks for raw T-Bank transactions",
        ),
        ValidationConfig(
            name="raw_load_bakai_transactions",
            suite_name="raw.bakai_transactions.data_presence_check",
            duckdb_table="prod_raw.bakai_transactions",
            description="Data presence and integrity checks for raw Bakai transactions",
        ),
    ],
)

MART_CHECKPOINT = CheckpointConfig(
    name="check_mart",
    description="Validates mart tables for business logic integrity",
    validations=[
        ValidationConfig(
            name="mart_bakai_transactions_exchange_rate",
            suite_name="mart.bakai_transactions.exchange_rate_null",
            duckdb_table="prod_mart.bakai_transactions",
            description="Exchange rate must be NULL - alerts when non-null values found",
        ),
    ],
)


def get_checkpoint_configs() -> dict[str, CheckpointConfig]:
    """
    Get all checkpoint configurations.

    Returns:
        Dictionary mapping checkpoint names to their configurations
    """
    return {
        RAW_CHECKPOINT.name: RAW_CHECKPOINT,
        MART_CHECKPOINT.name: MART_CHECKPOINT,
    }
