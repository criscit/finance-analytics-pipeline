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

    @patch("src.duckdb_utils.get_recently_ingested_tables")
    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_success(self, mock_run: Any, mock_get_tables: Any) -> None:
        """Test successful dbt build."""
        from dagster import build_asset_context

        # Mock the get_recently_ingested_tables call
        mock_get_tables.return_value = {
            "tables_to_process": [
                {
                    "source_system_nm": "test_bank",
                    "table_nm": "test_table",
                    "selector_nm": "run_test_table",
                }
            ]
        }

        # Mock successful subprocess calls
        mock_run.return_value = MagicMock(returncode=0, stdout="dbt build successful", stderr="")

        context = build_asset_context()
        result = build_dbt_models(context)

        # Verify subprocess.run was called at least twice (deps and build)
        assert mock_run.call_count >= SUBPROCESS_CALLS_2

        # Check return value structure matches actual implementation
        assert "build_results" in result.value  # type: ignore[attr-defined]
        assert "tables_processed" in result.value  # type: ignore[attr-defined]
        assert "total_tables" in result.value  # type: ignore[attr-defined]

    @patch("src.duckdb_utils.get_recently_ingested_tables")
    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_deps_failure(self, mock_run: Any, mock_get_tables: Any) -> None:
        """Test dbt build failure during deps."""
        from dagster import build_asset_context

        # Mock the get_recently_ingested_tables call
        mock_get_tables.return_value = {
            "tables_to_process": [
                {
                    "source_system_nm": "test_bank",
                    "table_nm": "test_table",
                    "selector_nm": "run_test_table",
                }
            ]
        }

        # Mock deps failure
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="deps failed")

        context = build_asset_context()
        # The function raises RuntimeError on deps failure
        with pytest.raises(RuntimeError, match="dbt deps failed"):
            build_dbt_models(context)

    @patch("src.duckdb_utils.get_recently_ingested_tables")
    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_build_failure(self, mock_run: Any, mock_get_tables: Any) -> None:
        """Test dbt build failure during build."""
        from dagster import build_asset_context

        # Mock the get_recently_ingested_tables call
        mock_get_tables.return_value = {
            "tables_to_process": [
                {
                    "source_system_nm": "test_bank",
                    "table_nm": "test_table",
                    "selector_nm": "run_test_table",
                }
            ]
        }

        # Mock successful deps, failed build
        def side_effect(*args: Any, **kwargs: Any) -> Any:
            if "deps" in args[0]:
                return MagicMock(returncode=0, stdout="deps successful", stderr="")
            return MagicMock(returncode=1, stdout="build output", stderr="build failed")

        mock_run.side_effect = side_effect

        context = build_asset_context()
        # The function raises RuntimeError on build failure
        with pytest.raises(RuntimeError, match="dbt build failed"):
            build_dbt_models(context)

    @patch("src.duckdb_utils.get_recently_ingested_tables")
    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_long_output(self, mock_run: Any, mock_get_tables: Any) -> None:
        """Test dbt build with long output (truncation)."""
        from dagster import build_asset_context

        # Mock the get_recently_ingested_tables call
        mock_get_tables.return_value = {
            "tables_to_process": [
                {
                    "source_system_nm": "test_bank",
                    "table_nm": "test_table",
                    "selector_nm": "run_test_table",
                }
            ]
        }

        # Mock successful build with long output
        long_output = "x" * 3000  # Longer than 2000 char limit
        mock_run.return_value = MagicMock(returncode=0, stdout=long_output, stderr="")

        context = build_asset_context()
        result = build_dbt_models(context)

        # Check that output is truncated in build results
        build_log = result.value["build_results"]["test_table"]["build_log"]  # type: ignore[attr-defined]
        assert len(build_log) <= BUILD_LOG_TRUNCATION_LENGTH  # Truncated to last 500 chars

    @patch("src.duckdb_utils.get_recently_ingested_tables")
    @patch("orchestration.assets_transform_dbt.subprocess.run")
    def test_dbt_build_models_short_output(self, mock_run: Any, mock_get_tables: Any) -> None:
        """Test dbt build with short output (no truncation)."""
        from dagster import build_asset_context

        # Mock the get_recently_ingested_tables call
        mock_get_tables.return_value = {
            "tables_to_process": [
                {
                    "source_system_nm": "test_bank",
                    "table_nm": "test_table",
                    "selector_nm": "run_test_table",
                }
            ]
        }

        # Mock successful build with short output
        short_output = "dbt build successful"
        mock_run.return_value = MagicMock(returncode=0, stdout=short_output, stderr="")

        context = build_asset_context()
        result = build_dbt_models(context)

        # Check that output is not truncated
        build_log = result.value["build_results"]["test_table"]["build_log"]  # type: ignore[attr-defined]
        assert build_log == short_output[-500:]
