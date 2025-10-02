# file: orchestration/dagster_project/src/repo.py
from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job

from .assets_export_csv import export_csv_snapshot
from .assets_export_sheets import export_to_google_sheets
from .assets_ingest import ingest_csv_to_duckdb
from .assets_maintenance import pipeline_maintenance
from .assets_quality_ge import run_ge_checkpoints
from .assets_transform_dbt import dbt_build_models

all_assets = [
    ingest_csv_to_duckdb,
    dbt_build_models,
    run_ge_checkpoints,
    export_csv_snapshot,
    export_to_google_sheets,
    pipeline_maintenance,
]

montly_job = define_asset_job(
    name="montly_export",
    selection=AssetSelection.assets(ingest_csv_to_duckdb)
    .downstream()  # dbt
    .downstream()  # GE
    .downstream(),  # exports
)

montly_schedule = ScheduleDefinition(
    job=montly_job,
    cron_schedule="0 6 1 * *",  # 06:00 1st day of the month
)

defs = Definitions(
    assets=all_assets,
    schedules=[montly_schedule],
)
