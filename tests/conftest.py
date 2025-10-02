"""Shared test fixtures and utilities."""

import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import duckdb
import pytest


@pytest.fixture
def temp_db() -> Generator[Path, None, None]:
    """Create a temporary DuckDB database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = Path(f.name)

    try:
        yield db_path
    finally:
        db_path.unlink(missing_ok=True)


@pytest.fixture
def sample_csv_data() -> str:
    """Provide sample CSV data for testing."""
    return """id,name,amount,date
1,test1,100.50,2024-01-01
2,test2,200.75,2024-01-02
3,test3,300.25,2024-01-03"""


@pytest.fixture
def sample_transaction_data() -> list[tuple[Any, ...]]:
    """Provide sample transaction data for testing."""
    return [
        (1, "test1", 100.50, "2024-01-01 12:00:00"),
        (2, "test2", -200.75, "2024-01-01 13:00:00"),
        (3, "test3", 300.25, "2024-01-01 14:00:00"),
    ]


@pytest.fixture
def mock_environment() -> Any:
    """Mock environment variables for testing."""
    import os
    from unittest.mock import patch

    env_vars = {
        "RAW_PATH": "/tmp/test_raw",
        "DUCKDB_PATH": "/tmp/test.duckdb",
        "EXPORT_CSV_IN_CONTAINER": "/tmp/test_exports/csv",
        "EXPORT_META_PATH": "/tmp/test_exports/metadata",
        "EXPORT_TABLE": "test_table",
        "GOOGLE_SHEET_ID": "test_sheet_id",
        "GOOGLE_SHEET_RANGE": "Test!A1",
        "GOOGLE_SA_JSON": "/tmp/test_sa.json",
    }

    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def test_database(temp_db: Path) -> duckdb.DuckDBPyConnection:
    """Create a test database with sample data."""
    con = duckdb.connect(str(temp_db))

    # Create schemas
    con.execute("CREATE SCHEMA IF NOT EXISTS prod_raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS core")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts")
    con.execute("CREATE SCHEMA IF NOT EXISTS prod_meta")

    # Create test tables
    con.execute(
        """
        CREATE TABLE prod_raw.t_bank_transactions (
            __load_key VARCHAR(32),
            id INTEGER,
            name VARCHAR,
            amount DECIMAL,
            __ingested_at TIMESTAMP
        )
    """
    )

    con.execute(
        """
        CREATE TABLE staging.stg_load_t_bank_transactions (
            id INTEGER,
            name VARCHAR,
            amount DECIMAL,
            __ingested_at TIMESTAMP
        )
    """
    )

    con.execute(
        """
        CREATE TABLE core.core_load_t_bank_transactions (
            id INTEGER,
            name VARCHAR,
            amount DECIMAL,
            transaction_type VARCHAR,
            __ingested_at TIMESTAMP
        )
    """
    )

    con.execute(
        """
        CREATE TABLE marts.mart_load_t_bank_transactions (
            id INTEGER,
            name VARCHAR,
            amount DECIMAL,
            transaction_type VARCHAR,
            __ingested_at TIMESTAMP
        )
    """
    )

    # Insert sample data
    con.execute(
        """
        INSERT INTO prod_raw.t_bank_transactions VALUES 
        ('key1', 1, 'test1', 100.50, '2024-01-01 12:00:00'),
        ('key2', 2, 'test2', -200.75, '2024-01-01 13:00:00'),
        ('key3', 3, 'test3', 300.25, '2024-01-01 14:00:00')
    """
    )

    yield con
    con.close()
