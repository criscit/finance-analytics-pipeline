# file: orchestration/dagster_project/src/repo.py
from dagster import Definitions, ScheduleDefinition, define_asset_job, AssetSelection
from .assets_ingest_duckdb import ingest_csv_to_duckdb
from .assets_transform_dbt import dbt_build_models
from .assets_quality_ge import run_ge_checkpoints
from .assets_export_csv import export_csv_snapshot
from .assets_export_sheets import export_to_google_sheets

all_assets = [ingest_csv_to_duckdb, dbt_build_models, run_ge_checkpoints, export_csv_snapshot, export_to_google_sheets]

daily_job = define_asset_job(
    name="daily_pipeline",
    selection=AssetSelection.assets(ingest_csv_to_duckdb)
    .downstream()  # dbt
    .downstream()  # GE
    .downstream(), # exports
)

daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 6 * * *",  # 06:00 daily
)

defs = Definitions(
    assets=all_assets,
    schedules=[daily_schedule],
)



