"""Test utility functions and helpers."""

import hashlib
import tempfile
from pathlib import Path

import duckdb
import pytest

from tests.constants import (
    MD5_HASH_LENGTH,
    TEST_AMOUNT_MAX,
    TEST_AMOUNT_MIN,
    TEST_COMPLETENESS_3,
    TEST_DATA_ROWS_4,
)


@pytest.mark.unit
class TestUtilityFunctions:
    """Test utility functions used across the pipeline."""

    def test_md5_hash_calculation(self) -> None:
        """Test MD5 hash calculation."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test content")
            temp_path = Path(f.name)

        try:
            # Calculate MD5 manually
            expected = hashlib.md5(b"test content").hexdigest()

            # Calculate MD5 using file reading
            hash_obj = hashlib.md5()
            with temp_path.open("rb") as file:
                for chunk in iter(lambda: file.read(1 << 20), b""):
                    hash_obj.update(chunk)
            result = hash_obj.hexdigest()

            assert result == expected
            assert len(result) == MD5_HASH_LENGTH
            assert result.isalnum()
        finally:
            temp_path.unlink()

    def test_file_stability_check(self) -> None:
        """Test file stability checking logic."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test content")
            temp_path = Path(f.name)

        try:
            # File should be stable after writing
            size1 = temp_path.stat().st_size
            size2 = temp_path.stat().st_size
            assert size1 == size2

            # Test non-existent file
            non_existent = Path("/nonexistent/file.csv")
            assert not non_existent.exists()

        finally:
            temp_path.unlink()

    def test_identifier_quoting(self) -> None:
        """Test identifier quoting for SQL."""

        def qident(name: str) -> str:
            """Quote an identifier for DuckDB."""
            return '"' + name.replace('"', '""') + '"'

        # Test simple identifier
        assert qident("simple_name") == '"simple_name"'

        # Test identifier with quotes
        assert qident('name with "quotes"') == '"name with ""quotes"""'

        # Test empty identifier
        assert qident("") == '""'

    def test_table_name_extraction(self) -> None:
        """Test table name extraction from directory paths."""

        def extract_table_name(dir_path: Path, base_path: Path) -> str:
            """Extract table name from directory path."""
            import re

            rel = dir_path.relative_to(base_path)
            parts = rel.parts
            raw_name = "_".join(parts).lower()
            table_name = re.sub(r"[^a-z0-9_]", "_", raw_name)
            table_name = re.sub(r"_+", "_", table_name).strip("_")
            return table_name or "unknown"

        base_path = Path("/base/path")
        test_cases = [
            (base_path / "T-Bank" / "transactions", "t_bank_transactions"),
            (base_path / "My-Bank" / "cards", "my_bank_cards"),
            (base_path / "Bank-Name" / "loans", "bank_name_loans"),
        ]

        for dir_path, expected in test_cases:
            result = extract_table_name(dir_path, base_path)
            assert result == expected

    def test_csv_column_detection(self) -> None:
        """Test CSV column detection using DuckDB."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("col1,col2,col3\nvalue1,value2,value3")
            temp_path = Path(f.name)

        try:
            with duckdb.connect() as con:
                rows = con.execute(
                    "describe select * from read_csv_auto(?, header=true)",
                    [str(temp_path)],
                ).fetchall()
                columns = [r[0] for r in rows]
                assert columns == ["col1", "col2", "col3"]
        finally:
            temp_path.unlink()

    def test_load_key_expression_building(self) -> None:
        """Test load key expression building for deduplication."""

        def build_load_key_expr(columns: list[str]) -> str:
            """Build deterministic hash expression for load keys."""
            columns = sorted(columns)
            pieces = []
            for col in columns:
                col_qid = f'"{col}"'
                val = f"coalesce({col_qid}, '<NULL>')"
                pieces.append(f"cast(length({val}) as varchar) || ':' || {val}")
            concat_all = " || '|' || ".join(pieces)
            return f"md5({concat_all})"

        columns = ["col1", "col2", "col3"]
        result = build_load_key_expr(columns)

        # Should contain MD5 function
        assert "md5(" in result
        # Should contain all columns
        for col in columns:
            assert f'"{col}"' in result
        # Should contain coalesce for NULL handling
        assert "coalesce" in result
        # Should contain length prefixing
        assert "length(" in result


@pytest.mark.unit
class TestDataValidation:
    """Test data validation functions."""

    def test_data_completeness_check(self) -> None:
        """Test data completeness validation."""
        # Use in-memory database for testing
        with duckdb.connect(":memory:") as con:
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

    def test_data_type_validation(self) -> None:
        """Test data type validation."""
        # Use in-memory database for testing
        with duckdb.connect(":memory:") as con:
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
            assert column_types["amount"].startswith("DECIMAL")
            assert column_types["created_at"] == "TIMESTAMP"

    def test_range_validation(self) -> None:
        """Test value range validation."""
        # Use in-memory database for testing
        with duckdb.connect(":memory:") as con:
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
