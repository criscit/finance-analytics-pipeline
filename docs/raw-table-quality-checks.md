# Raw Table Quality Checks

## Overview

This document describes the Great Expectations (GE) quality checks implemented to ensure raw tables contain data before dbt processing begins.

## Data Flow

```
CSV Files → ingest_csv_to_duckdb → run_ge_raw_checkpoints → dbt_build_models → run_ge_staging_checkpoints
```

## Raw Table Checks

The `run_ge_raw_checkpoints` asset validates the following for `prod_raw.t_bank_transactions`:

### Data Presence Checks
- **Table has data**: Ensures at least 1 row exists
- **Recent data**: Data ingested within last 7 days
- **Reasonable volume**: Between 1 and 1,000,000 rows

### Data Quality Checks
- **Load key exists**: `__load_key` column is present
- **Load key is unique**: No duplicate load keys
- **Load key is not null**: All load keys have values
- **Ingestion timestamp exists**: `processed_at` column is present and not null

## Implementation Files

### Expectations Suite
- **File**: `quality/gx/expectations/raw/t_bank_transactions/data_presence_check.yml`
- **Purpose**: Defines the quality rules for raw tables

### Checkpoint Configuration
- **File**: `quality/gx/checkpoints/check_raw.yml`
- **Purpose**: Configures how to run the raw table checks

### Dagster Asset
- **File**: `orchestration/assets_quality_ge.py`
- **Asset**: `run_ge_raw_checkpoints`
- **Dependencies**: `ingest_csv_to_duckdb`
- **Purpose**: Executes the raw table quality checks

## Pipeline Integration

The raw table quality checks are integrated into the pipeline as follows:

1. **Data Ingestion**: CSV files are ingested into `prod_raw` tables
2. **Raw Quality Check**: GE validates raw table data quality
3. **dbt Processing**: Only proceeds if raw table checks pass
4. **Staging Quality Check**: GE validates staging table data quality

## Error Handling

If raw table quality checks fail:
- The pipeline stops before dbt processing
- Error details are logged and returned in asset metadata
- No downstream processing occurs until raw data issues are resolved

## Testing

Tests are located in `tests/dagster/test_assets_quality_ge_raw.py` and cover:
- Successful checkpoint execution
- Subprocess errors
- Unexpected errors
