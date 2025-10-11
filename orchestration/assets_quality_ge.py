# file: orchestration/dagster_project/src/assets_quality_ge.py
import os
import subprocess

from dagster import MetadataValue, Output, asset, get_dagster_logger

GE_DIR = "/app/quality/gx"


@asset(deps=["ingest_transactions"])
def run_ge_raw_checkpoints() -> Output[dict[str, str]]:
    """
    Run Great Expectations checkpoints on raw tables to ensure data is present
    before dbt processing begins.
    """
    log = get_dagster_logger()

    try:
        # Change to GE directory
        os.chdir(GE_DIR)

        # Run raw table checkpoint
        result = subprocess.run(
            ["great_expectations", "checkpoint", "run", "check_raw"],
            capture_output=True,
            text=True,
            check=True,
        )

        log.info("Raw table GE checkpoint completed successfully")
        return Output(
            {"status": "success", "checkpoint": "check_raw"},
            metadata={
                "ge_output": MetadataValue.text(result.stdout),
                "checkpoint": "check_raw",
                "status": "success",
            },
        )

    except subprocess.CalledProcessError as e:
        log.error("GE raw checkpoint failed: %s", e.stderr)
        return Output(
            {"status": "failed", "checkpoint": "check_raw"},
            metadata={
                "ge_output": MetadataValue.text(f"Error: {e.stderr}"),
                "checkpoint": "check_raw",
                "status": "failed",
            },
        )
    except Exception as e:
        log.error("Unexpected error in GE raw checkpoint: %s", str(e))
        return Output(
            {"status": "error", "checkpoint": "check_raw"},
            metadata={
                "ge_output": MetadataValue.text(f"Unexpected error: {e!s}"),
                "checkpoint": "check_raw",
                "status": "error",
            },
        )


@asset(deps=["build_dbt_models"])
def run_ge_staging_checkpoints() -> Output[dict[str, str]]:
    """
    Run Great Expectations checkpoints on staging tables after dbt processing.
    """
    log = get_dagster_logger()

    try:
        # Change to GE directory
        os.chdir(GE_DIR)

        # Run staging table checkpoint
        result = subprocess.run(
            ["great_expectations", "checkpoint", "run", "check_staging"],
            capture_output=True,
            text=True,
            check=True,
        )

        log.info("Staging table GE checkpoint completed successfully")
        return Output(
            {"status": "success", "checkpoint": "check_staging"},
            metadata={
                "ge_output": MetadataValue.text(result.stdout),
                "checkpoint": "check_staging",
                "status": "success",
            },
        )

    except subprocess.CalledProcessError as e:
        log.error("GE staging checkpoint failed: %s", e.stderr)
        return Output(
            {"status": "failed", "checkpoint": "check_staging"},
            metadata={
                "ge_output": MetadataValue.text(f"Error: {e.stderr}"),
                "checkpoint": "check_staging",
                "status": "failed",
            },
        )
    except Exception as e:
        log.error("Unexpected error in GE staging checkpoint: %s", str(e))
        return Output(
            {"status": "error", "checkpoint": "check_staging"},
            metadata={
                "ge_output": MetadataValue.text(f"Unexpected error: {e!s}"),
                "checkpoint": "check_staging",
                "status": "error",
            },
        )
