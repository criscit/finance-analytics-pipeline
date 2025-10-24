"""Tests for Great Expectations quality assets."""

from typing import Any
from unittest.mock import patch

from orchestration.assets_quality_ge import run_ge_raw_checkpoints


class TestGreatExpectationsAsset:
    """Test the Great Expectations quality asset."""

    @patch("orchestration.assets_quality_ge._run_checkpoint")
    @patch("src.duckdb_utils.get_recently_ingested_tables")
    def test_run_ge_raw_checkpoints_success(
        self, mock_get_tables: Any, mock_run_checkpoint: Any
    ) -> None:
        """Test successful GE checkpoint run."""
        # Mock recently ingested tables
        mock_get_tables.return_value = {
            "tables_to_process": [{"table_nm": "test_table", "selector_nm": "run_test_table"}]
        }

        # Mock successful checkpoint run
        mock_run_checkpoint.return_value = {
            "success": True,
            "checkpoint_name": "check_raw",
            "validation_count": 5,
        }

        result = run_ge_raw_checkpoints()

        # Should return success
        assert result.value["status"] == "success"  # type: ignore[attr-defined]
        assert result.value["checkpoint"] == "check_raw"  # type: ignore[attr-defined]

    @patch("orchestration.assets_quality_ge._run_checkpoint")
    @patch("src.duckdb_utils.get_recently_ingested_tables")
    @patch("orchestration.assets_quality_ge.get_dagster_logger")
    def test_run_ge_checkpoints_logging(
        self, mock_logger: Any, mock_get_tables: Any, mock_run_checkpoint: Any
    ) -> None:
        """Test that GE checkpoint logs appropriately."""
        mock_log = mock_logger.return_value

        # Mock recently ingested tables
        mock_get_tables.return_value = {
            "tables_to_process": [{"table_nm": "test_table", "selector_nm": "run_test_table"}]
        }

        # Mock successful checkpoint run
        mock_run_checkpoint.return_value = {
            "success": True,
            "checkpoint_name": "check_raw",
            "validation_count": 5,
        }

        run_ge_raw_checkpoints()

        # Verify logging calls (should be multiple info calls)
        assert mock_log.info.call_count >= 1

    @patch("orchestration.assets_quality_ge._run_checkpoint")
    @patch("src.duckdb_utils.get_recently_ingested_tables")
    def test_run_ge_checkpoints_metadata(
        self, mock_get_tables: Any, mock_run_checkpoint: Any
    ) -> None:
        """Test GE checkpoint metadata structure."""
        # Mock recently ingested tables
        mock_get_tables.return_value = {
            "tables_to_process": [{"table_nm": "test_table", "selector_nm": "run_test_table"}]
        }

        # Mock successful checkpoint run
        mock_run_checkpoint.return_value = {
            "success": True,
            "checkpoint_name": "check_raw",
            "validation_count": 5,
        }

        result = run_ge_raw_checkpoints()

        # Check metadata structure
        assert "status" in result.metadata  # type: ignore[attr-defined]
        assert "checkpoint" in result.metadata  # type: ignore[attr-defined]
        # Metadata values are MetadataValue objects, not raw strings
        from dagster import MetadataValue

        assert isinstance(result.metadata["status"], MetadataValue)  # type: ignore[attr-defined]
        assert isinstance(result.metadata["checkpoint"], MetadataValue)  # type: ignore[attr-defined]
