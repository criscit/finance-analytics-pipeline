"""
Great Expectations Quality Framework - DataFrame-Based Approach (GX 1.7+)

This package manages data quality checks using Great Expectations 1.7+.
Uses DataFrame-based validation to bypass SQLAlchemy/DuckDB compatibility issues.
"""

from orchestration.quality_gx.bootstrap import run_checkpoint_with_dataframes
from orchestration.quality_gx.checkpoints import (
    CheckpointConfig,
    ValidationConfig,
    get_checkpoint_configs,
)

__all__ = [
    "run_checkpoint_with_dataframes",
    "CheckpointConfig",
    "ValidationConfig",
    "get_checkpoint_configs",
]
