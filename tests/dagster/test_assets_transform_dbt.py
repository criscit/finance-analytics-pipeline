"""Tests for dbt transform assets."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from orchestration.assets_transform_dbt import dbt_build_models
from tests.constants import LOG_TRUNCATION_LENGTH, SUBPROCESS_CALLS_2


class TestDBTTransformAsset:
    """Test the dbt transform asset."""

    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_success(self, mock_run: Any) -> None:
        """Test successful dbt build."""
        # Mock successful subprocess calls
        mock_run.return_value = MagicMock(returncode=0, stdout="dbt build successful", stderr="")

        result = dbt_build_models()

        # Verify subprocess.run was called twice (deps and build)
        assert mock_run.call_count == SUBPROCESS_CALLS_2

        # Check first call (dbt deps)
        deps_call = mock_run.call_args_list[0]
        assert deps_call[1]["cwd"] == "/app/transform"
        assert deps_call[1]["capture_output"] is True
        assert deps_call[1]["text"] is True
        assert deps_call[1]["check"] is False

        # Check second call (dbt build)
        build_call = mock_run.call_args_list[1]
        assert build_call[1]["cwd"] == "/app/transform"
        assert build_call[1]["capture_output"] is True
        assert build_call[1]["text"] is True
        assert build_call[1]["check"] is False

        # Check return value
        assert result.value is None  # type: ignore[attr-defined]
        assert "dbt_build_log" in result.metadata  # type: ignore[attr-defined]

    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_deps_failure(self, mock_run: Any) -> None:
        """Test dbt build failure during deps."""
        # Mock deps failure
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="deps failed")

        with pytest.raises(RuntimeError, match="deps failed"):
            dbt_build_models()

    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_build_failure(self, mock_run: Any) -> None:
        """Test dbt build failure during build."""

        # Mock successful deps, failed build
        def side_effect(*args: Any, **kwargs: Any) -> Any:
            if "deps" in args[0]:
                return MagicMock(returncode=0, stdout="deps successful", stderr="")
            return MagicMock(returncode=1, stdout="build output", stderr="build failed")

        mock_run.side_effect = side_effect

        with pytest.raises(RuntimeError, match="dbt build failed"):
            dbt_build_models()

    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_long_output(self, mock_run: Any) -> None:
        """Test dbt build with long output (truncation)."""
        # Mock successful build with long output
        long_output = "x" * 3000  # Longer than 2000 char limit
        mock_run.return_value = MagicMock(returncode=0, stdout=long_output, stderr="")

        result = dbt_build_models()

        # Check that output is truncated in metadata
        build_log = result.metadata["dbt_build_log"]  # type: ignore[attr-defined]
        assert len(build_log) <= LOG_TRUNCATION_LENGTH
        assert build_log.endswith("x" * LOG_TRUNCATION_LENGTH)  # Should be truncated to last chars

    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_short_output(self, mock_run: Any) -> None:
        """Test dbt build with short output (no truncation)."""
        # Mock successful build with short output
        short_output = "dbt build successful"
        mock_run.return_value = MagicMock(returncode=0, stdout=short_output, stderr="")

        result = dbt_build_models()

        # Check that output is not truncated
        build_log = result.metadata["dbt_build_log"]  # type: ignore[attr-defined]
        assert build_log == short_output
