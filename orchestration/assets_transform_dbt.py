# file: orchestration/assets_simple_table_driven.py
"""
Simple table-driven orchestration.
Uses ingest_ledger to detect loaded tables and maps to bank tags for model selection.
"""

import os
import subprocess
from datetime import datetime
from typing import Any

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from src.duckdb_utils import connect_readonly

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse/analytics.duckdb")
DBT_DIR = os.getenv("DBT_DIR", "/app/transform/dbt")


@asset(
    name="detect_ingested_tables",
    description="Detect which tables were loaded using ingest_ledger",
    deps=["run_ge_raw_checkpoints"],
)
def detect_ingested_tables(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    """
    Detect which tables were loaded using ingest_ledger.
    Map table names to bank tags for model selection.
    """
    log = context.log
    con = connect_readonly(DUCKDB_PATH)

    try:
        # Check if ingest_ledger exists
        ledger_exists = con.execute(
            """
            select 1
            from
                information_schema.tables 
            where
                table_schema = 'prod_meta'
                and table_name = 'ingest_ledger'
        """
        ).fetchone()

        if not ledger_exists:
            log.warning("No ingest_ledger found - no tables to process")
            return Output(
                {"loaded_tables": [], "banks_to_process": []},
                metadata={"status": MetadataValue.text("no_ledger_found")},
            )

        # Get recent ingestions (last 24 hours)
        recent_ingestions = con.execute(
            """
            select 
                bank_nm,
                table_nm,
                count(*) as file_count,
                max(processed_at) as latest_ingestion
            from
                prod_meta.ingest_ledger 
            where
                processed_at >= current_timestamp - interval '24 hours'
            group by
                bank_nm,
                table_nm
            order by
                latest_ingestion desc
        """
        ).fetchall()

        # Find which tables were loaded and map to bank tags
        tables_to_process = []

        for row in recent_ingestions:
            bank_nm, table_nm, file_count, latest_ingestion = row

            tables_to_process.append(
                {
                    "bank_nm": bank_nm,
                    "table_nm": table_nm,
                    "selector_nm": "run_" + table_nm,
                    "file_count": file_count,
                    "latest_ingestion": latest_ingestion.isoformat() if latest_ingestion else None,
                }
            )

            log.info("Found loaded within 24 hours table: %s with %s files", table_nm, file_count)

        return Output(
            {
                "tables_to_process": tables_to_process,
                "total_tables": len(tables_to_process),
                "detected_at": datetime.now().isoformat(),
            },
            metadata={
                "tables_to_process": MetadataValue.text(
                    ", ".join([t["table_nm"] for t in tables_to_process])
                ),
                "total_tables": MetadataValue.int(len(tables_to_process)),
            },
        )

    except Exception as e:
        log.error("Failed to detect loaded tables: %s", e)
        raise
    finally:
        con.close()


@asset(
    name="build_dbt_models", description="Build models using dbt selectors based on loaded tables"
)
def build_dbt_models(
    context: AssetExecutionContext, detect_ingested_tables: dict[str, Any]
) -> Output[dict[str, Any]]:
    """
    Build models using dbt selectors based on loaded tables.
    Use table detection to determine which selectors to use.
    """
    log = context.log

    profiles_dir = os.getenv("DBT_PROFILES_DIR", f"{DBT_DIR}/profiles")

    try:
        tables_to_process = detect_ingested_tables.get("tables_to_process", [])

        if not tables_to_process:
            log.info("No tables to process - skipping model building")
            return Output(
                {"build_results": {}, "status": "no_tables"},
                metadata={"status": MetadataValue.text("no_tables_to_process")},
            )

        # Install dbt dependencies
        log.info("Installing dbt dependencies...")
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
            log.info("Building models for %s", table_config["table_nm"])

            try:
                # Build models using selector
                build_result = subprocess.run(
                    [
                        "dbt",
                        "build",
                        "--selector",
                        table_config["selector_nm"],
                        "--profiles-dir",
                        profiles_dir,
                    ],
                    cwd=DBT_DIR,
                    capture_output=True,
                    text=True,
                    check=False,
                )

                if build_result.returncode == 0:
                    build_results[table_config["table_nm"]] = {
                        "status": "success",
                        "selector_used": table_config["selector_nm"],
                        "build_log": build_result.stdout[-500:],
                    }
                    log.info("Successfully built models for %s", table_config["table_nm"])
                else:
                    build_results[table_config["table_nm"]] = {
                        "status": "failed",
                        "selector_used": table_config["selector_nm"],
                        "error": build_result.stderr,
                        "build_log": build_result.stdout[-500:],
                    }
                    log.error("Failed to build models for %s", table_config["table_nm"])
                    log.error("STDERR: %s", build_result.stderr)
                    log.error("STDOUT (last 1000 chars): %s", build_result.stdout[-1000:])

            except Exception as e:
                build_results[table_config["table_nm"]] = {"status": "error", "error": str(e)}
                log.error("Error building models for %s: %s", table_config["table_nm"], e)

        # Calculate overall status
        successful_tables = [
            table for table, result in build_results.items() if result["status"] == "success"
        ]
        failed_tables = [
            table
            for table, result in build_results.items()
            if result["status"] in ["failed", "error"]
        ]

        overall_status = (
            "success"
            if len(failed_tables) == 0
            else "partial" if len(successful_tables) > 0 else "failed"
        )

        log.info(
            "Model building completed: %s successful, %s failed",
            len(successful_tables),
            len(failed_tables),
        )

        return Output(
            {
                "build_results": build_results,
                "successful_tables": successful_tables,
                "failed_tables": failed_tables,
                "overall_status": overall_status,
                "total_tables": len(tables_to_process),
            },
            metadata={
                "successful_tables": MetadataValue.text(", ".join(successful_tables)),
                "failed_tables": MetadataValue.text(", ".join(failed_tables)),
                "overall_status": MetadataValue.text(overall_status),
                "total_tables": MetadataValue.int(len(tables_to_process)),
            },
        )

    except Exception as e:
        log.error("Model building failed: %s", e)
        return Output(
            {"build_results": {}, "status": "error", "error": str(e)},
            metadata={"status": MetadataValue.text("error"), "error": MetadataValue.text(str(e))},
        )
