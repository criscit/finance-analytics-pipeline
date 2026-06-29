# file: orchestration/dagster_project/src/repo.py
from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job

from .assets_export_assets_sheets import export_assets_to_google_sheets
from .assets_export_csv import export_assets_csv_snapshot, export_csv_snapshot
from .assets_export_sheets import export_to_google_sheets
from .assets_ingest import ingest_bank, ingest_crypto, ingest_marketplace
from .assets_maintenance import pipeline_maintenance
from .assets_quality_ge import (
    run_ge_mart_checkpoints,
    run_ge_raw_checkpoints,
)
from .assets_transform_dbt import build_dbt_models, build_imart_models

all_assets = [
    ingest_bank,
    ingest_crypto,
    ingest_marketplace,
    run_ge_raw_checkpoints,
    build_dbt_models,
    run_ge_mart_checkpoints,
    build_imart_models,
    export_csv_snapshot,
    export_assets_csv_snapshot,
    export_to_google_sheets,
    export_assets_to_google_sheets,
    pipeline_maintenance,
]

# -------------------------
# Pipeline 1: Build Pipeline
# Ingest (crypto → marketplace → bank) → QE raw checks → build dbt models (stg, core, mart) → QE mart checks
# -------------------------
build_finance_data_pipeline = define_asset_job(
    name="build_finance_data_pipeline",
    description="Build pipeline: ingest data, run quality checks, build dbt models (stg, core, mart layers only)",
    selection=AssetSelection.assets(
        ingest_bank,
        ingest_crypto,
        ingest_marketplace,
        run_ge_raw_checkpoints,
        build_dbt_models,
        run_ge_mart_checkpoints,
    ),
)

# -------------------------
# Pipeline 2: Export Pipeline
# Build imart layer → export data to CSV and Google Sheets
# -------------------------
export_pipeline = define_asset_job(
    name="export_pipeline",
    description="Export pipeline: build imart layer and export data to CSV and Google Sheets",
    selection=AssetSelection.assets(
        build_imart_models,
        export_csv_snapshot,
        export_assets_csv_snapshot,
        export_to_google_sheets,
        export_assets_to_google_sheets,
    ),
)

# -------------------------
# Pipeline 3: Maintenance Pipeline
# Archive processed files and cleanup old exports
# -------------------------
maintenance_pipeline = define_asset_job(
    name="maintenance_pipeline",
    description="Maintenance pipeline: archive processed files and cleanup old exports",
    selection=AssetSelection.assets(
        pipeline_maintenance,
    ),
)

# -------------------------
# Combined Monthly Job
# Runs all three pipelines in sequence: build → export → maintenance
# -------------------------
monthly_job = define_asset_job(
    name="monthly_full_pipeline",
    description="Complete monthly pipeline: build (stg/core/mart) → export (imart + data) → maintenance",
    selection=AssetSelection.assets(
        ingest_bank,
        ingest_crypto,
        ingest_marketplace,
        run_ge_raw_checkpoints,
        build_dbt_models,
        run_ge_mart_checkpoints,
        build_imart_models,
        export_csv_snapshot,
        export_assets_csv_snapshot,
        export_to_google_sheets,
        export_assets_to_google_sheets,
        pipeline_maintenance,
    ),
)

monthly_schedule = ScheduleDefinition(
    job=monthly_job,
    cron_schedule="0 6 1 * *",  # 06:00 1st day of the month
)

defs = Definitions(
    assets=all_assets,
    jobs=[
        build_finance_data_pipeline,
        export_pipeline,
        maintenance_pipeline,
        monthly_job,
    ],
    schedules=[monthly_schedule],
)
