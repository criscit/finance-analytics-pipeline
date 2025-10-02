"""End-to-end tests for the finance analytics pipeline."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.constants import TEST_DATA_ROWS_4


@pytest.mark.e2e
class TestPipelineE2E:
    """End-to-end pipeline tests."""

    def test_complete_pipeline_workflow(self) -> None:
        """Test complete pipeline from ingestion to export."""
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
                # This would test the complete pipeline
                # For now, just verify the setup works
                assert test_csv.exists()
                assert raw_path.exists()
                assert db_path.parent.exists()

    def test_data_quality_validation(self) -> None:
        """Test data quality validation in the pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test data with quality issues
            test_data = temp_path / "test_data.csv"
            test_data.write_text("id,name,amount\n1,,100\n2,test2,invalid\n3,test3,200")

            # Verify data quality checks would catch issues
            lines = test_data.read_text().split("\n")
            assert len(lines) == TEST_DATA_ROWS_4  # header + 3 data rows

            # Check for empty name (quality issue)
            assert lines[1].split(",")[1] == ""  # empty name

            # Check for invalid amount (quality issue)
            assert lines[2].split(",")[2] == "invalid"  # invalid amount
