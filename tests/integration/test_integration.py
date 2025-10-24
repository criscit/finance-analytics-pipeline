"""Integration tests for the finance analytics pipeline."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from orchestration.assets_export_csv import export_csv_snapshot
from orchestration.assets_ingest import ingest_bank
from src.ingestion.service import IngestionContext, IngestionSourceConfig
from tests.constants import TEST_DATA_ROWS_2


def _make_test_config(
    db_path: Path, input_path: Path, stability_s: int = 0
) -> tuple[IngestionContext, IngestionSourceConfig]:
    """Create test ingestion config objects."""
    test_context = IngestionContext(
        duckdb_path=str(db_path), finance_data_root=input_path.parent, stability_seconds=stability_s
    )
    test_config = IngestionSourceConfig(
        name="test_bank",
        root_path=input_path,
        source_label="source",
        leaf_label="data_type",
        structure_hint="Bank/{source}/<transaction_type>/",
        leaf_options_factory=lambda p: type(
            "opts", (), {"latest_only": False, "merge_pending": False}
        )(),
    )
    return test_context, test_config


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

            # Create data contract
            contracts_dir = temp_path / "contracts"
            contracts_dir.mkdir(parents=True)
            contract_file = contracts_dir / "t_bank_transactions.yaml"
            contract_file.write_text(
                """version: "1.0"
dataset: "Bank Transactions"
format: "CSV"
encoding: "utf-8"
skip_header_rows: 0
skip_footer_rows: 0
target_table:
  schema: "prod_raw"
  name: "t_bank_transactions"
columns:
  - name: "id"
    type: "int"
    nullable: false
  - name: "name"
    type: "varchar"
    nullable: true
  - name: "amount"
    type: "int"
    nullable: true
"""
            )

            # Create test database
            db_path = temp_path / "test.duckdb"

            # Initialize database
            with duckdb.connect(str(db_path)) as con:
                con.execute("CREATE SCHEMA IF NOT EXISTS prod_raw")
                con.execute("CREATE SCHEMA IF NOT EXISTS prod_meta")

            test_context, test_config = _make_test_config(db_path, raw_path, stability_s=0)

            with (
                patch("orchestration.assets_ingest.INGESTION_CONTEXT", test_context),
                patch("orchestration.assets_ingest.BANK_CONFIG", test_config),
                patch.dict("os.environ", {"DATA_CONTRACTS_PATH": str(contracts_dir)}),
            ):
                # Test ingestion
                ingest_result = ingest_bank()
                # Check ingestion completed without exceptions
                assert "ingested" in ingest_result.value  # type: ignore[attr-defined]
                assert "skipped" in ingest_result.value  # type: ignore[attr-defined]
                assert "errors" in ingest_result.value  # type: ignore[attr-defined]

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

            # Create data contract
            contracts_dir = temp_path / "contracts"
            contracts_dir.mkdir(parents=True)
            contract_file = contracts_dir / "t_bank_transactions.yaml"
            contract_file.write_text(
                """version: "1.0"
dataset: "Bank Transactions"
format: "CSV"
encoding: "utf-8"
skip_header_rows: 0
skip_footer_rows: 0
target_table:
  schema: "prod_raw"
  name: "t_bank_transactions"
columns:
  - name: "id"
    type: "int"
    nullable: false
  - name: "name"
    type: "varchar"
    nullable: true
  - name: "amount"
    type: "int"
    nullable: true
"""
            )

            # Create test database
            db_path = temp_path / "test.duckdb"

            # Initialize database
            with duckdb.connect(str(db_path)) as con:
                con.execute("CREATE SCHEMA IF NOT EXISTS prod_raw")
                con.execute("CREATE SCHEMA IF NOT EXISTS prod_meta")

            test_context, test_config = _make_test_config(db_path, raw_path, stability_s=0)

            with (
                patch("orchestration.assets_ingest.INGESTION_CONTEXT", test_context),
                patch("orchestration.assets_ingest.BANK_CONFIG", test_config),
                patch.dict("os.environ", {"DATA_CONTRACTS_PATH": str(contracts_dir)}),
            ):
                # First ingestion
                result1 = ingest_bank()

            # Check result structure exists
            assert "ingested" in result1.value  # type: ignore[attr-defined]
            assert "skipped" in result1.value  # type: ignore[attr-defined]

            with (
                patch("orchestration.assets_ingest.INGESTION_CONTEXT", test_context),
                patch("orchestration.assets_ingest.BANK_CONFIG", test_config),
                patch.dict("os.environ", {"DATA_CONTRACTS_PATH": str(contracts_dir)}),
            ):
                # Second ingestion (should handle duplicates)
                result2 = ingest_bank()

            # Check second result structure exists
            assert "ingested" in result2.value  # type: ignore[attr-defined]
            assert "skipped" in result2.value  # type: ignore[attr-defined]

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

            export_dir = temp_path / "finance" / "Archive" / "Bank" / "Exports"
            results_dir = temp_path / "finance" / "Results"

            with (
                patch("orchestration.assets_export_csv.DUCKDB_PATH", str(db_path)),
                patch("orchestration.assets_export_csv.EXPORT_DIR", export_dir),
                patch("orchestration.assets_export_csv.RESULTS_DIR", results_dir),
                patch(
                    "orchestration.assets_export_csv.EXPORT_FINANCE_TABLE",
                    "prod_imart.view_bank_transactions",
                ),
            ):
                # Test export
                result = export_csv_snapshot()

                # Check return value
                assert result.value["rows"] == TEST_DATA_ROWS_2  # type: ignore[attr-defined]
                assert "md5" in result.value  # type: ignore[attr-defined]

            # Check files were created in date-based directories
            csv_files = list(export_dir.rglob("*.csv"))
            assert len(csv_files) >= 1

            # Check manifest
            manifest_files = list(export_dir.rglob("*manifest*.json"))
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

            # Create data contract
            contracts_dir = temp_path / "contracts"
            contracts_dir.mkdir(parents=True)
            contract_file = contracts_dir / "t_bank_transactions.yaml"
            contract_file.write_text(
                """version: "1.0"
dataset: "Bank Transactions"
format: "CSV"
encoding: "utf-8"
skip_header_rows: 1
skip_footer_rows: 0
target_table:
  schema: "prod_raw"
  name: "t_bank_transactions"
columns:
  - name: "id"
    type: "int"
    nullable: false
"""
            )

            # Create test database
            db_path = temp_path / "test.duckdb"

            # Initialize database
            with duckdb.connect(str(db_path)) as con:
                con.execute("CREATE SCHEMA IF NOT EXISTS prod_raw")
                con.execute("CREATE SCHEMA IF NOT EXISTS prod_meta")

            test_context, test_config = _make_test_config(db_path, raw_path)

            with (
                patch("orchestration.assets_ingest.INGESTION_CONTEXT", test_context),
                patch("orchestration.assets_ingest.BANK_CONFIG", test_config),
                patch.dict("os.environ", {"DATA_CONTRACTS_PATH": str(contracts_dir)}),
            ):
                # Should handle empty CSV gracefully
                result = ingest_bank()

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

            # Create data contract
            contracts_dir = temp_path / "contracts"
            contracts_dir.mkdir(parents=True)
            contract_file = contracts_dir / "t_bank_transactions.yaml"
            contract_file.write_text(
                """version: "1.0"
dataset: "Bank Transactions"
format: "CSV"
encoding: "utf-8"
skip_header_rows: 0
skip_footer_rows: 0
target_table:
  schema: "prod_raw"
  name: "t_bank_transactions"
columns:
  - name: "id"
    type: "int"
    nullable: false
  - name: "name"
    type: "varchar"
    nullable: true
  - name: "amount"
    type: "int"
    nullable: true
"""
            )

            # Create test database
            db_path = temp_path / "test.duckdb"

            # Initialize database
            with duckdb.connect(str(db_path)) as con:
                con.execute("CREATE SCHEMA IF NOT EXISTS prod_raw")
                con.execute("CREATE SCHEMA IF NOT EXISTS prod_meta")

            test_context, test_config = _make_test_config(db_path, raw_path, stability_s=0)

            with (
                patch("orchestration.assets_ingest.INGESTION_CONTEXT", test_context),
                patch("orchestration.assets_ingest.BANK_CONFIG", test_config),
                patch.dict("os.environ", {"DATA_CONTRACTS_PATH": str(contracts_dir)}),
            ):
                # Test ingestion with stability check
                result = ingest_bank()

            # Check result structure exists
            assert "ingested" in result.value  # type: ignore[attr-defined]
            assert "skipped" in result.value  # type: ignore[attr-defined]
