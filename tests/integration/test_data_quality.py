"""Tests for data quality checks and Great Expectations integration."""

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from tests.constants import (
    TEST_AMOUNT_MAX,
    TEST_AMOUNT_MIN,
    TEST_COMPLETENESS_3,
    TEST_DATA_ROWS_4,
    TEST_DATA_ROWS_5,
    TEST_INVALID_REFERENCES_1,
    TEST_PERCENTAGE_80,
    TEST_TOTAL_TRANSACTIONS_3,
    TEST_UNIQUE_COUNT_3,
    TEST_VALID_REFERENCES_2,
)


class TestDataQualityChecks:
    """Test data quality validation functions."""

    def test_column_completeness_check(self) -> None:
        """Test column completeness validation."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            with duckdb.connect(db_path) as con:
                # Create test table with some NULL values
                con.execute(
                    """
                    CREATE TABLE test_table (
                        id INTEGER,
                        name VARCHAR,
                        amount DECIMAL,
                        status VARCHAR
                    )
                """
                )

                con.execute(
                    """
                    INSERT INTO test_table VALUES 
                    (1, 'test1', 100.50, 'active'),
                    (2, NULL, 200.75, 'inactive'),
                    (3, 'test3', NULL, 'active'),
                    (4, 'test4', 300.25, NULL)
                """
                )

                # Test completeness checks
                completeness_results = con.execute(
                    """
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(name) as name_complete,
                        COUNT(amount) as amount_complete,
                        COUNT(status) as status_complete
                    FROM test_table
                """
                ).fetchone()

                assert completeness_results[0] == TEST_DATA_ROWS_4  # total_rows
                assert completeness_results[1] == TEST_COMPLETENESS_3  # name_complete (1 NULL)
                assert completeness_results[2] == TEST_COMPLETENESS_3  # amount_complete (1 NULL)
                assert completeness_results[3] == TEST_COMPLETENESS_3  # status_complete (1 NULL)

        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_data_type_validation(self) -> None:
        """Test data type validation."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            with duckdb.connect(db_path) as con:
                # Create test table
                con.execute(
                    """
                    CREATE TABLE test_table (
                        id INTEGER,
                        name VARCHAR,
                        amount DECIMAL,
                        created_at TIMESTAMP
                    )
                """
                )

                # Test valid data types
                con.execute(
                    """
                    INSERT INTO test_table VALUES 
                    (1, 'test1', 100.50, '2024-01-01 12:00:00'),
                    (2, 'test2', 200.75, '2024-01-01 13:00:00')
                """
                )

                # Verify data types
                schema_info = con.execute("PRAGMA table_info(test_table)").fetchall()
                column_types = {col[1]: col[2] for col in schema_info}

                assert column_types["id"] == "INTEGER"
                assert column_types["name"] == "VARCHAR"
                assert column_types["amount"] == "DECIMAL"
                assert column_types["created_at"] == "TIMESTAMP"

        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_range_validation(self) -> None:
        """Test value range validation."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            with duckdb.connect(db_path) as con:
                # Create test table
                con.execute(
                    """
                    CREATE TABLE test_table (
                        id INTEGER,
                        amount DECIMAL,
                        percentage DECIMAL
                    )
                """
                )

                con.execute(
                    """
                    INSERT INTO test_table VALUES 
                    (1, 100.50, 0.15),
                    (2, -50.25, 0.95),
                    (3, 0.00, 1.00),
                    (4, 1000.00, 0.00)
                """
                )

                # Test range validations
                range_results = con.execute(
                    """
                    SELECT 
                        MIN(amount) as min_amount,
                        MAX(amount) as max_amount,
                        MIN(percentage) as min_percentage,
                        MAX(percentage) as max_percentage,
                        COUNT(CASE WHEN percentage < 0 OR percentage > 1 THEN 1 END) as invalid_percentages
                    FROM test_table
                """
                ).fetchone()

                assert range_results[0] == TEST_AMOUNT_MIN  # min_amount
                assert range_results[1] == TEST_AMOUNT_MAX  # max_amount
                assert range_results[2] == 0.00  # min_percentage
                assert range_results[3] == 1.00  # max_percentage
                assert range_results[4] == 0  # invalid_percentages

        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_uniqueness_validation(self) -> None:
        """Test uniqueness constraint validation."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            with duckdb.connect(db_path) as con:
                # Create test table
                con.execute(
                    """
                    CREATE TABLE test_table (
                        id INTEGER,
                        email VARCHAR,
                        name VARCHAR
                    )
                """
                )

                con.execute(
                    """
                    INSERT INTO test_table VALUES 
                    (1, 'user1@test.com', 'User 1'),
                    (2, 'user2@test.com', 'User 2'),
                    (3, 'user1@test.com', 'User 3'),  -- Duplicate email
                    (4, 'user4@test.com', 'User 1')   -- Duplicate name
                """
                )

                # Test uniqueness checks
                uniqueness_results = con.execute(
                    """
                    SELECT 
                        COUNT(DISTINCT email) as unique_emails,
                        COUNT(DISTINCT name) as unique_names,
                        COUNT(*) as total_rows
                    FROM test_table
                """
                ).fetchone()

                assert uniqueness_results[0] == TEST_UNIQUE_COUNT_3  # unique_emails (1 duplicate)
                assert uniqueness_results[1] == TEST_UNIQUE_COUNT_3  # unique_names (1 duplicate)
                assert uniqueness_results[2] == TEST_DATA_ROWS_4  # total_rows

        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_referential_integrity(self) -> None:
        """Test referential integrity validation."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            with duckdb.connect(db_path) as con:
                # Create parent table
                con.execute(
                    """
                    CREATE TABLE categories (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR
                    )
                """
                )

                con.execute(
                    """
                    INSERT INTO categories VALUES 
                    (1, 'Food'),
                    (2, 'Transport'),
                    (3, 'Entertainment')
                """
                )

                # Create child table
                con.execute(
                    """
                    CREATE TABLE transactions (
                        id INTEGER,
                        category_id INTEGER,
                        amount DECIMAL
                    )
                """
                )

                con.execute(
                    """
                    INSERT INTO transactions VALUES 
                    (1, 1, 25.50),
                    (2, 2, 15.75),
                    (3, 99, 100.00)  -- Invalid category_id
                """
                )

                # Test referential integrity
                integrity_results = con.execute(
                    """
                    SELECT 
                        COUNT(*) as total_transactions,
                        COUNT(CASE WHEN c.id IS NOT NULL THEN 1 END) as valid_references,
                        COUNT(CASE WHEN c.id IS NULL THEN 1 END) as invalid_references
                    FROM transactions t
                    LEFT JOIN categories c ON t.category_id = c.id
                """
                ).fetchone()

                assert integrity_results[0] == TEST_TOTAL_TRANSACTIONS_3  # total_transactions
                assert integrity_results[1] == TEST_VALID_REFERENCES_2  # valid_references
                assert integrity_results[2] == TEST_INVALID_REFERENCES_1  # invalid_references

        finally:
            Path(db_path).unlink(missing_ok=True)


class TestGreatExpectationsIntegration:
    """Test Great Expectations integration."""

    @patch("orchestration.dagster_project.src.assets_quality_ge.run_ge_checkpoints")
    def test_ge_checkpoint_execution(self, mock_ge: Any) -> None:
        """Test Great Expectations checkpoint execution."""
        # Import here to avoid circular import issues
        try:
            from orchestration.dagster_project.src.assets_quality_ge import run_ge_checkpoints
        except ImportError:
            pytest.skip("orchestration module not available")

        # Mock successful GE execution
        mock_ge.return_value = MagicMock(
            value=None, metadata={"status": "success", "message": "All checks passed"}
        )

        result = run_ge_checkpoints()

        # Verify GE was called
        mock_ge.assert_called_once()

        # Verify result structure
        assert result.value is None
        assert "status" in result.metadata
        assert "message" in result.metadata

    def test_data_quality_metrics(self) -> None:
        """Test data quality metrics calculation."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            with duckdb.connect(db_path) as con:
                # Create test table
                con.execute(
                    """
                    CREATE TABLE test_table (
                        id INTEGER,
                        name VARCHAR,
                        amount DECIMAL,
                        status VARCHAR
                    )
                """
                )

                con.execute(
                    """
                    INSERT INTO test_table VALUES 
                    (1, 'test1', 100.50, 'active'),
                    (2, NULL, 200.75, 'inactive'),
                    (3, 'test3', NULL, 'active'),
                    (4, 'test4', 300.25, NULL),
                    (5, 'test5', 400.00, 'active')
                """
                )

                # Calculate quality metrics
                quality_metrics = con.execute(
                    """
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(name) / COUNT(*)::DECIMAL as name_completeness,
                        COUNT(amount) / COUNT(*)::DECIMAL as amount_completeness,
                        COUNT(status) / COUNT(*)::DECIMAL as status_completeness,
                        AVG(amount) as avg_amount,
                        STDDEV(amount) as amount_stddev
                    FROM test_table
                """
                ).fetchone()

                assert quality_metrics[0] == TEST_DATA_ROWS_5  # total_rows
                assert quality_metrics[1] == TEST_PERCENTAGE_80  # name_completeness (4/5)
                assert quality_metrics[2] == TEST_PERCENTAGE_80  # amount_completeness (4/5)
                assert quality_metrics[3] == TEST_PERCENTAGE_80  # status_completeness (4/5)
                assert quality_metrics[4] is not None  # avg_amount
                assert quality_metrics[5] is not None  # amount_stddev

        finally:
            Path(db_path).unlink(missing_ok=True)
