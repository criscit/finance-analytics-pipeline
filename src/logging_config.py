"""
Centralized logging configuration using loguru.

This module provides a singleton logger instance configured with:
- Structured logging with context binding
- File rotation and retention
- Color-coded console output

Note: For Dagster assets, use context.log instead of this logger.
This logger is intended for business logic in src/.
"""

import sys
from pathlib import Path
from typing import Any

from loguru import logger

# Remove default logger
logger.remove()

# Configure console output with color coding
logger.add(
    sys.stderr,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    ),
    level="INFO",
    colorize=True,
    backtrace=True,
    diagnose=True,
)

# Configure file output with rotation
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logger.add(
    log_dir / "finance_pipeline_{time:YYYY-MM-DD}.log",
    format=(
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | " "{name}:{function}:{line} | {message}"
    ),
    level="DEBUG",
    rotation="00:00",  # Rotate at midnight
    retention="30 days",  # Keep logs for 30 days
    compression="zip",  # Compress old logs
    backtrace=True,
    diagnose=True,
)


def get_logger(name: str | None = None) -> Any:
    """
    Get a logger instance with optional name binding.

    Args:
        name: Optional name to bind to the logger for context

    Returns:
        Configured loguru logger instance
    """
    if name:
        return logger.bind(context=name)
    return logger


def bind_context(**kwargs: Any) -> Any:
    """
    Bind additional context to the logger.

    Usage:
        log = bind_context(table="prod_raw.transactions", source="bakai")
        log.info("Processing transactions")

    Args:
        **kwargs: Key-value pairs to bind as context

    Returns:
        Logger instance with bound context
    """
    return logger.bind(**kwargs)


# Export the logger and utility functions
__all__ = ["logger", "get_logger", "bind_context"]
