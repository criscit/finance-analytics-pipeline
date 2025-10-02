"""Tests for ingestion assets and utilities."""

import hashlib
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest

# Import here to avoid circular import issues
try:
    from orchestration.assets_ingest import (
        _extract_table_name_from_dir,
        _file_columns,
        _md5,
        _stable,
        build_load_key_expr,
        qident,
        qtable,
    )
except ImportError:
    # Skip tests if module not available
    pytest.skip("orchestration module not available")


class TestUtilityFunctions:
    """Test utility functions for ingestion."""

    def test_qident(self) -> None:
        """Test identifier quoting."""
        assert qident("simple_name") == '"simple_name"'
        assert qident('name with "quotes"') == '"name with ""quotes"""'
        assert qident("") == '""'

    def test_qtable(self) -> None:
        """Test table name quoting."""
        assert qtable("schema", "table") == '"schema"."table"'
        assert qtable("my_schema", "my_table") == '"my_schema"."my_table"'

    def test_md5(self) -> None:
        """Test MD5 hash calculation."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test content")
            temp_path = Path(f.name)

        try:
            result = _md5(temp_path)
            expected = hashlib.md5(b"test content").hexdigest()
            assert result == expected
        finally:
            temp_path.unlink()

    def test_stable_file(self) -> None:
        """Test file stability check."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test content")
            temp_path = Path(f.name)

        try:
            # File should be stable after writing
            assert _stable(temp_path) is True
        finally:
            temp_path.unlink()

    def test_stable_nonexistent_file(self) -> None:
        """Test stability check on non-existent file."""
        temp_path = Path("/nonexistent/file.csv")
        assert _stable(temp_path) is False

    def test_extract_table_name_from_dir(self) -> None:
        """Test table name extraction from directory path."""
        base_path = Path("/base/path")
        test_cases = [
            (base_path / "T-Bank" / "transactions", "t_bank_transactions"),
            (base_path / "My-Bank" / "cards", "my_bank_cards"),
            (base_path / "Bank-Name" / "loans", "bank_name_loans"),
            (base_path / "Test@Bank" / "data", "test_bank_data"),
        ]

        for dir_path, expected in test_cases:
            result = _extract_table_name_from_dir(dir_path)
            assert result == expected

    def test_build_load_key_expr(self) -> None:
        """Test load key expression building."""
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


class TestFileColumns:
    """Test file column detection."""

    def test_file_columns_csv(self) -> None:
        """Test column detection from CSV file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("col1,col2,col3\nvalue1,value2,value3")
            temp_path = Path(f.name)

        try:
            with duckdb.connect() as con:
                columns = _file_columns(con, temp_path)
                assert columns == ["col1", "col2", "col3"]
        finally:
            temp_path.unlink()

    def test_file_columns_empty_csv(self) -> None:
        """Test column detection from empty CSV."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("")
            temp_path = Path(f.name)

        try:
            with duckdb.connect() as con:
                columns = _file_columns(con, temp_path)
                assert columns == []
        finally:
            temp_path.unlink()


class TestIngestionAsset:
    """Test the main ingestion asset."""

    @patch("orchestration.assets_ingest.INPUT_PATH")
    @patch("orchestration.assets_ingest.DUCKDB_PATH")
    def test_ingest_csv_to_duckdb_no_input_path(
        self, mock_db_path: Any, mock_input_path: Any
    ) -> None:
        """Test ingestion when input path doesn't exist."""
        from orchestration.assets_ingest import ingest_csv_to_duckdb

        mock_input_path.exists.return_value = False
        mock_db_path.return_value = ":memory:"

        with pytest.raises(ValueError, match="No To Parse folder found"):
            ingest_csv_to_duckdb()

    @patch("orchestration.assets_ingest.INPUT_PATH")
    @patch("orchestration.assets_ingest.DUCKDB_PATH")
    def test_ingest_csv_to_duckdb_success(self, mock_db_path: Any, mock_input_path: Any) -> None:
        """Test successful ingestion."""
        from orchestration.assets_ingest import ingest_csv_to_duckdb

        # Mock input path exists
        mock_input_path.exists.return_value = True
        mock_input_path.mkdir.return_value = None

        # Create a temporary database
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            mock_db_path.return_value = db_path

            # Mock directory structure
            bank_dir = MagicMock()
            bank_dir.name = "T-Bank"
            bank_dir.is_dir.return_value = True
            bank_dir.iterdir.return_value = []

            mock_input_path.iterdir.return_value = [bank_dir]

            result = ingest_csv_to_duckdb()

            assert result.value["ingested"] == 0  # type: ignore[attr-defined]
            assert result.value["skipped"] == 0  # type: ignore[attr-defined]
        finally:
            Path(db_path).unlink(missing_ok=True)
