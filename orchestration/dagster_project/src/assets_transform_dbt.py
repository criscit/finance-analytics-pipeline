# file: orchestration/dagster_project/src/assets_transform_dbt.py
import subprocess, os
from dagster import asset, Output, MetadataValue

DBT_DIR = "/app/transform"

@asset(deps=["ingest_csv_to_duckdb"])
def dbt_build_models():
    # Assumes profiles.yml is set to use DUCKDB_PATH
    out = subprocess.run(["dbt", "deps"], cwd=DBT_DIR, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr)
    out = subprocess.run(["dbt", "build", "--profiles-dir", DBT_DIR], cwd=DBT_DIR, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"dbt build failed. stdout: {out.stdout}, stderr: {out.stderr}")
    return Output(None, metadata={"dbt_build_log": MetadataValue.text(out.stdout[-2000:])})
