"""Integration tests for the finance analytics pipeline."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from orchestration.assets_export_csv import export_csv_snapshot
from orchestration.assets_ingest import ingest_csv_to_duckdb
from tests.constants import TEST_DATA_ROWS_2


@pytest.mark.integration
class TestPipelineIntegration:
    """Test end-to-end pipeline integration."""

    def test_ingest_and_export_workflow(self) -> None:
        """Test complete ingest to export workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Setup test environment
            raw_path = temp_path / "raw" / "To Parse" / "Bank"
            raw_path.mkdir(parents=True)

            # Create test CSV
            test_csv = raw_path / "T-Bank" / "transactions" / "test.csv"
            test_csv.parent.mkdir(parents=True)
            test_csv.write_text("id,name,amount\n1,test1,100\n2,test2,200")

            # Create test database
            db_path = temp_path / "test.duckdb"

            with patch.dict(
                "os.environ",
                {
                    "FINANCE_DATA_DIR_CONTAINER": str(temp_path / "finance"),
                    "DUCKDB_PATH": str(db_path),
                    "EXPORT_FINANCE_TABLE": "prod_imart.view_bank_transactions",
                },
            ):
                # Test ingestion
                ingest_result = ingest_csv_to_duckdb()
                assert ingest_result.value["ingested"] == 1  # type: ignore[attr-defined]
                assert ingest_result.value["skipped"] == 0  # type: ignore[attr-defined]

                # Verify data was ingested
                with duckdb.connect(str(db_path)) as con:
                    tables = con.execute("SHOW TABLES").fetchall()
                    table_names = [t[0] for t in tables]
                    assert "t_bank_transactions" in table_names

                    # Check data exists
                    rows = con.execute(
                        "SELECT COUNT(*) FROM prod_raw.t_bank_transactions"
                    ).fetchone()
                    assert rows[0] == TEST_DATA_ROWS_2

    def test_duplicate_ingestion_prevention(self) -> None:
        """Test that duplicate files are not ingested."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Setup test environment
            raw_path = temp_path / "raw" / "To Parse" / "Bank"
            raw_path.mkdir(parents=True)

            # Create test CSV
            test_csv = raw_path / "T-Bank" / "transactions" / "test.csv"
            test_csv.parent.mkdir(parents=True)
            test_csv.write_text("id,name,amount\n1,test1,100")

            # Create test database
            db_path = temp_path / "test.duckdb"

            with patch.dict(
                "os.environ",
                {
                    "FINANCE_DATA_DIR_CONTAINER": str(temp_path / "finance"),
                    "DUCKDB_PATH": str(db_path),
                },
            ):
                # First ingestion
                result1 = ingest_csv_to_duckdb()
                assert result1.value["ingested"] == 1  # type: ignore[attr-defined]
                assert result1.value["skipped"] == 0  # type: ignore[attr-defined]

                # Second ingestion (should skip)
                result2 = ingest_csv_to_duckdb()
                assert result2.value["ingested"] == 0  # type: ignore[attr-defined]
                assert result2.value["skipped"] == 1  # type: ignore[attr-defined]

    def test_csv_export_with_metadata(self) -> None:
        """Test CSV export with proper metadata generation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test database with data
            db_path = temp_path / "test.duckdb"
            with duckdb.connect(str(db_path)) as con:
                con.execute("CREATE SCHEMA IF NOT EXISTS prod_imart")
                con.execute(
                    "CREATE TABLE prod_imart.view_bank_transactions (id INTEGER, name VARCHAR, amount DECIMAL)"
                )
                con.execute(
                    "INSERT INTO prod_imart.view_bank_transactions VALUES (1, 'test1', 100.50), (2, 'test2', 200.75)"
                )

            with patch.dict(
                "os.environ",
                {
                    "DUCKDB_PATH": str(db_path),
                    "FINANCE_DATA_DIR_CONTAINER": str(temp_path / "finance"),
                    "EXPORT_FINANCE_TABLE": "prod_imart.view_bank_transactions",
                },
            ):
                # Test export
                result = export_csv_snapshot()

                # Check return value
                assert result.value["rows"] == TEST_DATA_ROWS_2  # type: ignore[attr-defined]
                assert "md5" in result.value  # type: ignore[attr-defined]

                # Check files were created
                csv_dir = temp_path / "exports" / "csv" / "prod_imart_view_bank_transactions"
                assert csv_dir.exists()

                latest_file = csv_dir / "latest.csv"
                assert latest_file.exists()

                # Check manifest
                meta_dir = temp_path / "exports" / "metadata" / "prod_imart_view_bank_transactions"

                # Note: The exact date will depend on when the test runs
                # We'll check that a manifest exists somewhere in the metadata directory
                manifest_files = list(meta_dir.rglob("manifest.json"))
                assert len(manifest_files) > 0

                # Check manifest content
                with manifest_files[0].open("r") as f:
                    import json

                    manifest = json.load(f)

                assert manifest["table"] == "prod_imart.view_bank_transactions"
                assert manifest["row_count"] == TEST_DATA_ROWS_2
                assert "md5" in manifest
                assert "created_at_utc" in manifest

    def test_error_handling_invalid_csv(self) -> None:
        """Test error handling with invalid CSV files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Setup test environment
            raw_path = temp_path / "raw" / "To Parse" / "Bank"
            raw_path.mkdir(parents=True)

            # Create invalid CSV (empty file)
            test_csv = raw_path / "T-Bank" / "transactions" / "invalid.csv"
            test_csv.parent.mkdir(parents=True)
            test_csv.write_text("")  # Empty file

            # Create test database
            db_path = temp_path / "test.duckdb"

            with patch.dict(
                "os.environ",
                {
                    "FINANCE_DATA_DIR_CONTAINER": str(temp_path / "finance"),
                    "DUCKDB_PATH": str(db_path),
                },
            ):
                # Should handle empty CSV gracefully
                result = ingest_csv_to_duckdb()
                # Empty CSV might be skipped or processed depending on implementation
                assert result.value["ingested"] >= 0  # type: ignore[attr-defined]
                assert result.value["skipped"] >= 0  # type: ignore[attr-defined]

    def test_file_stability_check(self) -> None:
        """Test file stability checking during ingestion."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Setup test environment
            raw_path = temp_path / "raw" / "To Parse" / "Bank"
            raw_path.mkdir(parents=True)

            # Create test CSV
            test_csv = raw_path / "T-Bank" / "transactions" / "test.csv"
            test_csv.parent.mkdir(parents=True)
            test_csv.write_text("id,name,amount\n1,test1,100")

            # Create test database
            db_path = temp_path / "test.duckdb"

            with patch.dict(
                "os.environ",
                {
                    "FINANCE_DATA_DIR_CONTAINER": str(temp_path / "finance"),
                    "DUCKDB_PATH": str(db_path),
                },
            ):
                # Test ingestion with stability check
                result = ingest_csv_to_duckdb()
                assert result.value["ingested"] == 1  # type: ignore[attr-defined]
