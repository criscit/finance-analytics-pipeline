# file: orchestration/assets_simple_table_driven.py
"""
Simple table-driven orchestration.
Uses ingest_ledger to detect loaded tables and maps to bank tags for model selection.
"""

import os
import subprocess
from typing import Any

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from src.logging_config import logger

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse/analytics.duckdb")
DBT_DIR = os.getenv("DBT_DIR", "/app/transform/dbt")


@asset(
    name="build_dbt_models",
    description="Build stg, core, and mart layers using dbt selectors based on loaded tables",
    deps=["run_ge_raw_checkpoints"],
)
def build_dbt_models(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    """
    Build staging, core, and mart models using dbt selectors based on loaded tables.
    Does NOT include imart (integration_marts) layer - that's built separately.
    Use table detection to determine which selectors to use.
    """
    from src.duckdb_utils import get_recently_ingested_tables

    profiles_dir = os.getenv("DBT_PROFILES_DIR", f"{DBT_DIR}/profiles")

    # Get recently ingested tables
    ingestion_info = get_recently_ingested_tables(DUCKDB_PATH, hours=24)
    tables_to_process = ingestion_info.get("tables_to_process", [])

    if not tables_to_process:
        logger.info("No tables to process - skipping model building")
        return Output(
            {"build_results": {}, "status": "no_tables"},
            metadata={"status": MetadataValue.text("no_tables_to_process")},
        )

    # Install dbt dependencies
    logger.info("Installing dbt dependencies...")
    deps_result = subprocess.run(
        ["dbt", "deps"],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )

    if deps_result.returncode != 0:
        raise RuntimeError(f"dbt deps failed: {deps_result.stderr}")

    build_results = {}

    # Process each bank that has loaded tables
    for table_config in tables_to_process:
        logger.info(
            "Building models for %s (stg, core, mart layers only)", table_config["table_nm"]
        )

        # Build models using selector (excludes imart layer by schema)
        build_result = subprocess.run(
            [
                "dbt",
                "build",
                "--selector",
                table_config["selector_nm"],
                "--exclude",
                "config.schema:imart",
                "--profiles-dir",
                profiles_dir,
            ],
            cwd=DBT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )

        if build_result.returncode != 0:
            logger.error("Failed to build models for %s", table_config["table_nm"])
            logger.error("STDERR: %s", build_result.stderr)
            logger.error("STDOUT (last 1000 chars): %s", build_result.stdout[-1000:])
            raise RuntimeError(
                f"dbt build failed for {table_config['table_nm']}: {build_result.stderr or build_result.stdout[-500:]}"
            )

        build_results[table_config["table_nm"]] = {
            "status": "success",
            "selector_used": table_config["selector_nm"],
            "build_log": build_result.stdout[-500:],
        }
        logger.info("Successfully built models for %s", table_config["table_nm"])

    logger.info("Model building completed successfully for all %s tables", len(tables_to_process))

    return Output(
        {
            "build_results": build_results,
            "tables_processed": list(build_results.keys()),
            "total_tables": len(tables_to_process),
        },
        metadata={
            "tables_processed": MetadataValue.text(", ".join(build_results.keys())),
            "total_tables": MetadataValue.int(len(tables_to_process)),
        },
    )


@asset(
    name="build_imart_models",
    description="Build integration mart (imart) layer that combines data from multiple sources",
    deps=["run_ge_mart_checkpoints"],
)
def build_imart_models(context: AssetExecutionContext) -> Output[dict[str, Any]]:
    """
    Build integration_marts (imart) layer models.
    This layer combines data from different banks/sources into unified views.
    Runs after all staging and mart quality checks pass.
    """
    profiles_dir = os.getenv("DBT_PROFILES_DIR", f"{DBT_DIR}/profiles")

    logger.info("Building integration_marts (imart) layer...")

    # Build only the imart schema
    build_result = subprocess.run(
        [
            "dbt",
            "build",
            "--select",
            "config.schema:imart",
            "--profiles-dir",
            profiles_dir,
        ],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )

    if build_result.returncode != 0:
        logger.error("Failed to build imart models")
        logger.error("STDERR: %s", build_result.stderr)
        logger.error("STDOUT: %s", build_result.stdout)
        raise RuntimeError(
            f"dbt build failed for imart layer: {build_result.stderr or build_result.stdout[-500:]}"
        )

    logger.info("Successfully built imart layer")
    return Output(
        {"status": "success", "build_log": build_result.stdout[-500:]},
        metadata={
            "status": MetadataValue.text("success"),
            "build_log": MetadataValue.text(build_result.stdout[-500:]),
        },
    )
