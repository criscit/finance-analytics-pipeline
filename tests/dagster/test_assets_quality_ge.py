"""Tests for Great Expectations quality assets."""

from typing import Any
from unittest.mock import patch

from orchestration.assets_quality_ge import run_ge_raw_checkpoints


class TestGreatExpectationsAsset:
    """Test the Great Expectations quality asset."""

    @patch("orchestration.assets_quality_ge.GE_DIR", "/tmp/gx")
    @patch("orchestration.assets_quality_ge.os.chdir")
    @patch("orchestration.assets_quality_ge.subprocess.run")
    def test_run_ge_raw_checkpoints_success(self, mock_run: Any, mock_chdir: Any) -> None:
        """Test successful GE checkpoint run."""
        from unittest.mock import MagicMock

        mock_run.return_value = MagicMock(returncode=0, stdout="Success", stderr="")
        result = run_ge_raw_checkpoints()

        # Should return success
        assert result.value["status"] == "success"  # type: ignore[attr-defined]
        assert result.value["checkpoint"] == "check_raw"  # type: ignore[attr-defined]

    @patch("orchestration.assets_quality_ge.GE_DIR", "/tmp/gx")
    @patch("orchestration.assets_quality_ge.os.chdir")
    @patch("orchestration.assets_quality_ge.subprocess.run")
    @patch("orchestration.assets_quality_ge.get_dagster_logger")
    def test_run_ge_checkpoints_logging(
        self, mock_logger: Any, mock_run: Any, mock_chdir: Any
    ) -> None:
        """Test that GE checkpoint logs appropriately."""
        from unittest.mock import MagicMock

        mock_log = mock_logger.return_value
        mock_run.return_value = MagicMock(returncode=0, stdout="Success", stderr="")

        run_ge_raw_checkpoints()

        # Verify logging calls
        mock_log.info.assert_called_once()

    @patch("orchestration.assets_quality_ge.GE_DIR", "/tmp/gx")
    @patch("orchestration.assets_quality_ge.os.chdir")
    @patch("orchestration.assets_quality_ge.subprocess.run")
    def test_run_ge_checkpoints_metadata(self, mock_run: Any, mock_chdir: Any) -> None:
        """Test GE checkpoint metadata structure."""
        from unittest.mock import MagicMock

        mock_run.return_value = MagicMock(returncode=0, stdout="Success", stderr="")
        result = run_ge_raw_checkpoints()

        # Check metadata structure
        assert "status" in result.metadata  # type: ignore[attr-defined]
        assert "checkpoint" in result.metadata  # type: ignore[attr-defined]
        # Metadata values are MetadataValue objects, not raw strings
        from dagster import MetadataValue

        assert isinstance(result.metadata["status"], MetadataValue)  # type: ignore[attr-defined]
        assert isinstance(result.metadata["checkpoint"], MetadataValue)  # type: ignore[attr-defined]
