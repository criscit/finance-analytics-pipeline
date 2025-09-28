# file: orchestration/dagster_project/src/assets_transform_dbt.py
import subprocess

from dagster import MetadataValue, Output, asset

DBT_DIR = "/app/transform"


@asset(deps=["ingest_csv_to_duckdb"])
def dbt_build_models() -> Output[None]:
    # Assumes profiles.yml is set to use DUCKDB_PATH
    out = subprocess.run(
        ["dbt", "deps"],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr)
    out = subprocess.run(
        ["dbt", "build", "--profiles-dir", DBT_DIR],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0:
        msg = f"dbt build failed. stdout: {out.stdout}, stderr: {out.stderr}"
        raise RuntimeError(
            msg,
        )
    return Output(
        None,
        metadata={"dbt_build_log": MetadataValue.text(out.stdout[-2000:])},
    )
