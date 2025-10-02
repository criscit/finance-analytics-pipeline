"""Tests for Great Expectations quality assets."""

from typing import Any
from unittest.mock import patch

from orchestration.assets_quality_ge import run_ge_checkpoints


class TestGreatExpectationsAsset:
    """Test the Great Expectations quality asset."""

    def test_run_ge_checkpoints_success(self) -> None:
        """Test successful GE checkpoint run."""
        result = run_ge_checkpoints()

        # Should return success (currently mocked)
        assert result.value is None  # type: ignore[attr-defined]
        assert result.metadata["status"] == "success"  # type: ignore[attr-defined]
        assert "message" in result.metadata  # type: ignore[attr-defined]

    @patch("orchestration.assets_quality_ge.get_dagster_logger")
    def test_run_ge_checkpoints_logging(self, mock_logger: Any) -> None:
        """Test that GE checkpoint logs appropriately."""
        mock_log = mock_logger.return_value

        run_ge_checkpoints()

        # Verify logging calls
        mock_log.info.assert_called_once()
        log_message = mock_log.info.call_args[0][0]
        assert "Skipping GE checkpoints" in log_message

    def test_run_ge_checkpoints_metadata(self) -> None:
        """Test GE checkpoint metadata structure."""
        result = run_ge_checkpoints()

        # Check metadata structure
        assert "status" in result.metadata  # type: ignore[attr-defined]
        assert "message" in result.metadata  # type: ignore[attr-defined]
        assert result.metadata["status"] == "success"  # type: ignore[attr-defined]
        assert isinstance(result.metadata["message"], str)  # type: ignore[attr-defined]
