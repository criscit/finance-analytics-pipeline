# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A local, lake-less data pipeline for personal finance analytics using:
- **Dagster**: Orchestration and asset management
- **DuckDB**: Single-file analytics warehouse
- **dbt-duckdb**: SQL transformations
- **Great Expectations**: Data quality validation
- **Poetry**: Dependency management

The pipeline runs entirely locally in Docker with no cloud dependencies (except optional Google Sheets export).

### Docker Configuration

**IMPORTANT**: The project codebase is bind-mounted into the Docker container via `docker-compose.yml`. This means:
- Code changes on the host are immediately available in the container
- **No rebuild needed** for Python code, dbt models, or data contract changes
- **Restart required** (no rebuild) for `.env` changes: `docker compose restart`
- The container automatically picks up file changes (selectors, models, Python modules, etc.)
- Only rebuild (`docker compose up --build -d`) when changing:
  - Dependencies in `pyproject.toml`
  - `Dockerfile` or `docker-compose.yml`
  - System-level configuration

## Commands

### Local Development
```bash
# Install dependencies
poetry install

# Set up pre-commit hooks
make setup

# Run linting
poetry run ruff check .
make lint

# Format code
poetry run black .
make fmt

# Type checking
poetry run mypy src/

# Run all tests
poetry run pytest
make test

# Run specific test suites
make test-unit         # Unit tests only
make test-integration  # Integration tests
make test-dagster      # Dagster asset tests
make test-e2e          # End-to-end tests

# Run specific test file
poetry run pytest tests/unit/test_duckdb_utils.py

# Run specific test
poetry run pytest tests/unit/test_duckdb_utils.py::test_ensure_meta_schema -v
```

### Docker Pipeline
```bash
# Start Dagster UI (http://localhost:3000)
docker compose up --build -d
make up

# View logs
docker compose logs worker -f

# Stop services
docker compose down
make down

# Rebuild after dependency changes
docker compose up --build -d
```

### dbt Commands
```bash
# IMPORTANT: Always use this command pattern (works from project root)
# poetry run dbt <subcommand> --project-dir transform/dbt --profiles-dir transform/dbt/profiles [other args]

# Build all models (staging → core → marts)
poetry run dbt run --project-dir transform/dbt --profiles-dir transform/dbt/profiles
make dbt-build

# Build specific model
poetry run dbt run --project-dir transform/dbt --profiles-dir transform/dbt/profiles --select stg_load_bakai_transactions

# Build model and downstream dependencies
poetry run dbt run --project-dir transform/dbt --profiles-dir transform/dbt/profiles --select stg_load_bakai_transactions+

# Test models
poetry run dbt test --project-dir transform/dbt --profiles-dir transform/dbt/profiles
make dbt-test

# List models in a selector
poetry run dbt ls --project-dir transform/dbt --profiles-dir transform/dbt/profiles --selector run_bakai_transactions

# Parse project (useful after adding models/macros)
poetry run dbt parse --project-dir transform/dbt --profiles-dir transform/dbt/profiles

# Lint dbt models
make dbt-lint
```

### DuckDB Inspection
```bash
# Connect to warehouse
poetry run python -c "import duckdb; con = duckdb.connect('data/warehouse/analytics.duckdb'); con.execute('show tables').show()"

# Query data
poetry run python -c "import duckdb; con = duckdb.connect('data/warehouse/analytics.duckdb'); con.execute('select * from prod_raw.bakai_transactions limit 5').show()"

# List all schemas
poetry run python -c "import duckdb; con = duckdb.connect('data/warehouse/analytics.duckdb'); con.execute('show schemas').show()"
```

## Architecture

### Pipeline Flow

The pipeline consists of 4 main orchestrated jobs:

1. **Build Pipeline** (`build_finance_data_pipeline`):
   - Ingest bank & crypto data → Quality checks (raw) → dbt models (stg/core/mart) → Quality checks (marts)

2. **Export Pipeline** (`export_pipeline`):
   - Build integration marts → Export to CSV & Google Sheets

3. **Maintenance Pipeline** (`maintenance_pipeline`):
   - Archive processed files and cleanup old exports

4. **Monthly Full Pipeline** (`monthly_full_pipeline`):
   - Runs all 3 pipelines in sequence on the 1st of each month at 06:00

### Data Contracts

All data sources are defined via YAML contracts in `data/contracts/`. Contracts specify:
- Column schema (name, type, nullable, format)
- Source metadata (system, dataset, owner)
- File parsing rules (skip_header_rows, skip_footer_rows, delimiter)
- Target table (schema, name)

**Key constraint**: Contracts are REQUIRED before files can be ingested. The ingestion service validates that a contract exists before processing files.

Example contract location: `data/contracts/bakai_transactions.yaml`

### Ingestion System

The ingestion system (`src/ingestion/service.py`) is a reusable, configurable workflow:

**Directory Structure**:
- Bank data: `{FINANCE_DIR}/To Parse/Bank/{source}/{data_type}/`
- Crypto data: `{FINANCE_DIR}/To Parse/Crypto/{platform}/{transaction_type}/`

**Key Features**:
- **Idempotent**: MD5-based deduplication via `prod_meta.ingest_ledger`
- **File stability checks**: Waits 8 seconds before processing to ensure uploads complete
- **Caching**: Converts CSV/XLSX to Parquet cache (stored in `data/raw/`)
- **Flexible**: Supports latest-only mode (for snapshots like "Assets") or merge-pending mode (for transaction history)
- **Contract-driven**: Reads schema and parsing rules from data contracts

**Table Naming**:
Tables are named by directory path: `{source}_{transaction_type}` (e.g., `bybit_spot_orders`, `bakai_transactions`)

**Fresh Loads**:
When new files are detected for a table, the ingestion service drops the existing raw table and re-ingests from scratch (using cached parquet files if available).

### dbt Layering

Models follow a medallion architecture:

- **Staging** (`prod_stg` schema): Raw data cleaning and type casting
  - Pattern: `stg_load_{source}_{data_type}.sql`
  - Example: `stg_load_bakai_transactions.sql`
  - Uses `get_stg_columns_list_map()` macro to read column definitions from `seeds/mappings/transactions_column_map.csv`

- **Core** (`prod_core` schema): Business logic and standardization
  - Pattern: `core_load_{source}_{data_type}.sql`
  - Passes through properly typed columns from staging
  - Applies regexp cleaning for special varchar fields (e.g., "$1.23" → 1.23)

- **Marts** (`prod_mart` schema): Aggregated/denormalized for analytics
  - Pattern: `mart_load_{source}_{data_type}.sql`
  - Materialized as tables for performance

- **Integration Marts** (`prod_imart` schema): Cross-source unified views (VIEWs)
  - Example: `imart_bind_transactions.sql` (combines T Bank + Bakai Bank + Telegram + Crypto)
  - Materialized as views for flexibility

All models materialize as tables except integration marts (views).

**Important**: The staging layer uses the `get_stg_columns_list_map()` macro, which reads column definitions from `seeds/mappings/transactions_column_map.csv`. This macro outputs properly named and typed columns, so the core layer should pass them through without additional casting - **except** for special varchar fields (like Telegram's "$1.23" or "5.4% APY" formatted strings) that still need regexp cleaning to extract numeric values.

### DuckDB Schemas

- `prod_meta`: Metadata tables (ingest_ledger for file tracking, export_bookmarks for incremental exports)
- `prod_raw`: Raw ingested data (text columns only + `__load_key`, `processed_at`)
- `prod_stg`: Staging tables from dbt
- `prod_core`: Core tables from dbt
- `prod_mart`: Mart tables from dbt
- `prod_imart`: Integration mart views from dbt
- `seed`: dbt seeds (tab-delimited mappings)

### Data Quality

Great Expectations checkpoints run at two stages:
1. **After ingestion** (`run_ge_raw_checkpoints`): Validates raw data
2. **After marts** (`run_ge_mart_checkpoints`): Validates final analytics tables

Configuration: `data/quality/gx/great_expectations.yml`

### Export System

**CSV Exports** (`orchestration/assets_export_csv.py`):
- Creates dated snapshots: `{FINANCE_DIR}/exports/csv/{date}/data.csv`
- Maintains `latest.csv` symlink/copy
- Includes `manifest.json` with row count, MD5, timestamps

**Google Sheets Exports** (`orchestration/assets_export_sheets.py`):
- Incremental exports using high-watermark (latest transaction_dt)
- Bookmark stored in `prod_meta.export_bookmarks`
- Column mapping: `get_duckdb_to_sheets_column_mapping()` in `src/duckdb_utils.py`

## Key Patterns

### Adding a New Data Source

1. **Create data contract** in `data/contracts/{source}_{type}.yaml`:
   - Define columns, types, parsing rules
   - Set target table: `prod_raw.{source}_{type}`

2. **Place data files** in appropriate directory:
   - Bank: `{FINANCE_DIR}/To Parse/Bank/{source}/{type}/`
   - Crypto: `{FINANCE_DIR}/To Parse/Crypto/{source}/{type}/`

3. **Create dbt models**:
   - Staging: `transform/dbt/models/staging/{source}/stg_load_{source}_{type}.sql`
   - Core: `transform/dbt/models/core/{source}/core_load_{source}_{type}.sql`
   - Mart: `transform/dbt/models/marts/{source}/mart_load_{source}_{type}.sql`

4. **Add column mappings** to `transform/dbt/seeds/mappings/transactions_column_map.csv`

5. **Run ingestion** to validate contract and ingest data

### Working with DuckDB Tables

Use helper functions from `src/duckdb_utils.py`:
- `ensure_meta_schema(con)`: Initialize schemas
- `ingest_parquet_to_duckdb(con, schema, table, path)`: Load parquet
- `table_exists(con, schema, table)`: Check table existence
- `get_table_columns(db_path, schema, table)`: Get column list

Use utilities from `src/utils.py`:
- `qident(name)`: Quote identifier for SQL
- `qtable(schema, table)`: Quote schema.table
- `md5_hash(file_path)`: MD5 of file contents
- `is_file_stable(path, seconds)`: Check file hasn't changed

### Testing Strategy

- **Unit tests** (`tests/unit/`): Test utilities and helpers
- **Dagster asset tests** (`tests/dagster/`): Mock-based asset testing
- **Integration tests** (`tests/integration/`): Full pipeline with test data
- **E2E tests** (`tests/e2e/`): Complete workflow validation

All tests use `conftest.py` fixtures for DuckDB connections and sample data.

**Test markers**:
- `@pytest.mark.unit`: Quick unit tests
- `@pytest.mark.integration`: Tests requiring DuckDB/dbt
- `@pytest.mark.dagster`: Dagster asset tests
- `@pytest.mark.e2e`: Full pipeline tests

## Environment Variables

Required variables in `.env`:

```bash
# Data directories
FINANCE_DIR_HOST=G:/USER/DATA/FINDNA  # Windows path with forward slashes
FINANCE_DATA_DIR_CONTAINER=/app/data/finance

# Export configuration
FINANCE_HISTORY_EXPORT_TABLE=prod_imart.view_transactions

# Google Sheets (optional)
GOOGLE_SA_JSON_PATH=/app/data/credentials/finance-sheets-writer-prod-sa.json
FINANCE_GOOGLE_SPREADSHEET_ID=your_spreadsheet_id
FINANCE_HISTORY_GOOGLE_SHEET_NAME=Spendings
FINANCE_HISTORY_GOOGLE_TABLE_NAME="Spendings Log"

# DuckDB
DUCKDB_PATH=/app/data/warehouse/analytics.duckdb

# dbt
DBT_DIR=/app/transform/dbt
DBT_PROFILES_DIR=/app/transform/dbt/profiles
```

## Code Quality Standards

- **Line length**: 100 characters (enforced by Black and Ruff)
- **Python version**: 3.11+
- **Type hints**: Required on all functions (mypy strict mode)
- **Imports**: Sorted and organized (Ruff)
- **Docstrings**: Required for all public functions
- **Constants**: Use `UPPER_SNAKE_CASE` for module-level constants
- **String formatting**: Prefer f-strings over `.format()` or `%` formatting
- **Pre-commit hooks**: Installed via `make setup` to run checks before each commit

Run `poetry run ruff check . && poetry run black . && poetry run mypy src/` before committing, or use `make lint` and `make fmt`.

## Project Structure

```
├── orchestration/
│   └── dagster_project/       # Dagster assets, jobs, and schedules
├── transform/
│   └── dbt/                   # dbt models (staging → core → marts)
│       ├── models/
│       │   ├── staging/       # Raw data cleaning
│       │   ├── core/          # Business logic
│       │   ├── marts/         # Analytics tables
│       │   └── integration_marts/  # Cross-source views
│       ├── profiles/          # dbt profiles configuration
│       └── seeds/             # Column mappings and reference data
├── src/
│   ├── ingestion/             # Contract-driven ingestion service
│   ├── export/                # CSV and Google Sheets exporters
│   ├── duckdb_utils.py        # DuckDB helper functions
│   └── utils.py               # General utilities
├── tests/
│   ├── unit/                  # Unit tests
│   ├── integration/           # Integration tests
│   ├── dagster/               # Dagster asset tests
│   ├── e2e/                   # End-to-end tests
│   └── fixtures/              # Shared test fixtures
├── data/
│   ├── contracts/             # YAML data contracts
│   ├── raw/                   # Parquet cache
│   ├── warehouse/             # DuckDB database file
│   └── quality/               # Great Expectations configuration
├── docs/                      # Documentation and reports
├── credentials/               # Service account credentials
├── docker-compose.yml         # Docker services configuration
├── Dockerfile                 # Python 3.11 + Poetry setup
├── pyproject.toml             # Poetry dependencies
├── Makefile                   # Common development commands
└── .env                       # Environment variables (not in git)
```

## Commit & Pull Request Guidelines

- Use imperative, capitalized commit messages (`Add mart layer for crypto`, not `added mart layer`)
- Squash WIP commits before review
- Split cross-cutting changes into focused commits
- In PRs:
  - Outline affected assets/models
  - Call out `.env` or schema changes
  - Note `make lint` and relevant `make test-*` results
  - Link issues or Dagster incidents
  - Include screenshots for UI-facing updates

## Important Notes

- **Single DuckDB file**: All data lives in `data/warehouse/analytics.duckdb`
- **No Parquet lake**: Parquet files in `data/raw/` are just ingestion cache
- **Idempotent design**: Re-running ingestion with same files is safe (MD5 dedup)
- **Windows paths**: Always use forward slashes in `.env`, even on Windows
- **Docker volume sharing**: Ensure Docker Desktop has access to your data drive
- **Bind mounts**: Code is bind-mounted to container - no rebuild needed for code/config changes (only for dependency/Docker changes)
- **Security**: Keep `.env` and `credentials/` out of git; use `env.example` as template
