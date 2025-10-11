# file: orchestration/dagster_project/src/repo.py
from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job

from .assets_export_csv import export_csv_snapshot
from .assets_export_sheets import export_to_google_sheets
from .assets_ingest import ingest_transactions
from .assets_maintenance import pipeline_maintenance
from .assets_quality_ge import run_ge_raw_checkpoints, run_ge_staging_checkpoints
from .assets_transform_dbt import build_dbt_models, detect_ingested_tables

all_assets = [
    ingest_transactions,
    run_ge_raw_checkpoints,
    detect_ingested_tables,
    build_dbt_models,
    run_ge_staging_checkpoints,
    export_csv_snapshot,
    export_to_google_sheets,
    pipeline_maintenance,
]

monthly_job = define_asset_job(
    name="monthly_export",
    selection=AssetSelection.assets(
        ingest_transactions,
        run_ge_raw_checkpoints,
        detect_ingested_tables,
        build_dbt_models,
        run_ge_staging_checkpoints,
        export_csv_snapshot,
        export_to_google_sheets,
        pipeline_maintenance,
    ),
)

monthly_schedule = ScheduleDefinition(
    job=monthly_job,
    cron_schedule="0 6 1 * *",  # 06:00 1st day of the month
)

defs = Definitions(
    assets=all_assets,
    schedules=[monthly_schedule],
)
