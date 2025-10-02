# Finance Analytics Pipeline

A local, lake-less data pipeline using Dagster + DuckDB + dbt-duckdb + Great Expectations with Poetry for dependency management.

## Features

- **Ingest**: Local CSV files from host path into DuckDB (no Parquet lake)
- **Transform**: dbt-duckdb builds staging → core → marts in a single DuckDB file
- **Quality**: Great Expectations checkpoints against staged/mart tables
- **Export**: 
  - Google Sheets exporter (incremental using high-watermark stored in DuckDB)
  - CSV snapshot exporter (full daily snapshot with latest.csv + manifest.json)
- **Orchestration**: Dagster assets & schedules
- **Storage**: One .duckdb file (warehouse) + exported CSVs on host path

## Quick Start

### 1. Prerequisites

- Docker Desktop installed and running
- Poetry installed for local development
- Google Cloud service account JSON file

### 2. Setup

1. **Clone and install dependencies:**
   ```bash
   poetry install
   ```

2. **Configure environment:**
   ```bash
   cp env.example .env
   # Edit .env with your actual paths and Google Sheet ID
   ```

3. **Set up Google Sheets integration:**
   - Create a service account in Google Cloud Console
   - Download the JSON key file to `credentials/finance-sheets-writer-prod-sa.json`
   - Share your target Google Sheet with the service account email

4. **Create required directories:**
   ```bash
   mkdir -p data/warehouse credentials
   ```

### 3. Windows Path Configuration

Ensure Docker Desktop → Settings → Resources → File Sharing includes your drive (e.g., G: drive).

Update `.env` with your actual paths:
```env
FINANCE_DIR_HOST=G:/USER/DATA/FINDNA
```

### 4. Run the Pipeline

1. **Start the services:**
   ```bash
   docker compose up --build -d
   ```

2. **Access Dagster UI:**
   - Open http://localhost:3000
   - Run the `daily_pipeline` job manually or wait for the scheduled run (06:00 daily)

3. **Add sample data:**
   - Copy `data/sample/events_sample.csv` to your `IMPORT_CSV_PATH`
   - The pipeline will automatically detect and process new CSV files

### 5. Verify Results

- **DuckDB tables**: Check that staging, core, and marts tables are populated
- **CSV exports**: Look for files in `FINANCE_DIR_HOST/exports/csv` with dated snapshots and `latest.csv`
- **Google Sheets**: Verify incremental data appears in your target sheet
- **Manifest**: Check `manifest.json` files for metadata (row count, MD5, timestamps)

## Project Structure

```
├── docker-compose.yml          # Docker services configuration
├── pyproject.toml             # Poetry dependencies
├── env.example                # Environment variables template
├── infra/
│   └── Dockerfile             # Python 3.11 + Poetry setup
├── orchestration/
│   └── dagster_project/       # Dagster assets and schedules
├── transform/                 # dbt models (staging → core → marts)
├── quality/
│   └── great_expectations/    # Data quality checks
├── export/
│   └── tests/                 # Test files
├── data/
│   ├── warehouse/             # DuckDB storage
│   └── sample/               # Sample CSV files
└── credentials/              # Google service account JSON
```

## Development

### Local Development with Poetry

```bash
# Install dependencies
poetry install

# Run linting
poetry run ruff check .
poetry run black .

# Run tests
poetry run pytest export/tests/
```

### Adding New Data Sources

1. Add CSV files to your `IMPORT_CSV_PATH`
2. Files are automatically routed to `stg_{prefix}` tables based on filename prefix
3. Create corresponding dbt models in `transform/models/`
4. Update Great Expectations expectations as needed

### Customizing Exports

- **CSV exports**: Modify `EXPORT_FINANCE_TABLE` in `.env` to change the source table
- **Google Sheets**: Update `GOOGLE_SHEET_NAME` and `GOOGLE_TABLE_NAME` to change the target sheet and table
- **Schedules**: Modify the cron schedule in `orchestration/dagster_project/src/repo.py`

## Troubleshooting

### Common Issues

1. **File sharing errors**: Ensure Docker Desktop has access to your drive
2. **Google Sheets auth**: Verify service account JSON is correct and sheet is shared
3. **Path issues**: Use forward slashes in `.env` even on Windows
4. **Port conflicts**: Change port 3000 in `docker-compose.yml` if needed

### Logs

```bash
# View container logs
docker compose logs pipeline-worker
docker compose logs dagster-daemon

# Follow logs in real-time
docker compose logs -f pipeline-worker
```

## Architecture Notes

- **Idempotent**: Ingestion ledger prevents duplicate processing
- **Incremental**: Google Sheets exports use bookmarks to avoid duplicates
- **Stable**: File stability checks prevent processing incomplete uploads
- **Quality**: Great Expectations validates data before exports
- **Local**: No cloud dependencies, runs entirely on your machine



