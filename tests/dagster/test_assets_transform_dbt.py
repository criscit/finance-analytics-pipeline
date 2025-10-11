"""Tests for dbt transform assets."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from orchestration.assets_transform_dbt import build_dbt_models
from tests.constants import SUBPROCESS_CALLS_2

# Constants
BUILD_LOG_TRUNCATION_LENGTH = 500


class TestDBTTransformAsset:
    """Test the dbt transform asset."""

    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_success(self, mock_run: Any) -> None:
        """Test successful dbt build."""
        from unittest.mock import MagicMock

        from dagster import build_asset_context

        # Mock successful subprocess calls
        mock_run.return_value = MagicMock(returncode=0, stdout="dbt build successful", stderr="")

        # Mock the detect_ingested_tables input
        mock_detect_result = {
            "tables_to_process": [
                {"bank_nm": "test_bank", "table_nm": "test_table", "selector_nm": "run_test_table"}
            ]
        }

        context = build_asset_context()
        result = build_dbt_models(context, mock_detect_result)

        # Verify subprocess.run was called at least twice (deps and build)
        assert mock_run.call_count >= SUBPROCESS_CALLS_2

        # Check return value structure
        assert "build_results" in result.value  # type: ignore[attr-defined]
        assert "overall_status" in result.value  # type: ignore[attr-defined]

    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_deps_failure(self, mock_run: Any) -> None:
        """Test dbt build failure during deps."""
        from dagster import build_asset_context

        # Mock deps failure
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="deps failed")

        mock_detect_result = {
            "tables_to_process": [
                {"bank_nm": "test_bank", "table_nm": "test_table", "selector_nm": "run_test_table"}
            ]
        }

        context = build_asset_context()
        with pytest.raises(RuntimeError, match="deps failed"):
            build_dbt_models(context, mock_detect_result)

    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_build_failure(self, mock_run: Any) -> None:
        """Test dbt build failure during build."""
        from dagster import build_asset_context

        # Mock successful deps, failed build
        def side_effect(*args: Any, **kwargs: Any) -> Any:
            if "deps" in args[0]:
                return MagicMock(returncode=0, stdout="deps successful", stderr="")
            return MagicMock(returncode=1, stdout="build output", stderr="build failed")

        mock_run.side_effect = side_effect

        mock_detect_result = {
            "tables_to_process": [
                {"bank_nm": "test_bank", "table_nm": "test_table", "selector_nm": "run_test_table"}
            ]
        }

        context = build_asset_context()
        result = build_dbt_models(context, mock_detect_result)
        # Build failures are now caught and returned as status, not raised
        assert result.value["build_results"]["test_table"]["status"] == "failed"  # type: ignore[attr-defined]

    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_long_output(self, mock_run: Any) -> None:
        """Test dbt build with long output (truncation)."""
        from dagster import build_asset_context

        # Mock successful build with long output
        long_output = "x" * 3000  # Longer than 2000 char limit
        mock_run.return_value = MagicMock(returncode=0, stdout=long_output, stderr="")

        mock_detect_result = {
            "tables_to_process": [
                {"bank_nm": "test_bank", "table_nm": "test_table", "selector_nm": "run_test_table"}
            ]
        }

        context = build_asset_context()
        result = build_dbt_models(context, mock_detect_result)

        # Check that output is truncated in build results
        build_log = result.value["build_results"]["test_table"]["build_log"]  # type: ignore[attr-defined]
        assert len(build_log) <= BUILD_LOG_TRUNCATION_LENGTH  # Truncated to last 500 chars

    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_short_output(self, mock_run: Any) -> None:
        """Test dbt build with short output (no truncation)."""
        from dagster import build_asset_context

        # Mock successful build with short output
        short_output = "dbt build successful"
        mock_run.return_value = MagicMock(returncode=0, stdout=short_output, stderr="")

        mock_detect_result = {
            "tables_to_process": [
                {"bank_nm": "test_bank", "table_nm": "test_table", "selector_nm": "run_test_table"}
            ]
        }

        context = build_asset_context()
        result = build_dbt_models(context, mock_detect_result)

        # Check that output is not truncated
        build_log = result.value["build_results"]["test_table"]["build_log"]  # type: ignore[attr-defined]
        assert build_log == short_output[-500:]
