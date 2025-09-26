# Finance Analytics Pipeline - Runbook

## Emergency Procedures

### Pipeline Failure Recovery

1. **Check service status:**
   ```bash
   docker compose ps
   docker compose logs pipeline-worker
   docker compose logs dagster-daemon
   ```

2. **Restart services:**
   ```bash
   docker compose restart
   # Or full restart
   docker compose down && docker compose up --build -d
   ```

3. **Check Dagster UI:**
   - Open http://localhost:3000
   - Review asset materialization history
   - Check for failed runs and error messages

### Data Quality Issues

1. **Great Expectations failures:**
   ```bash
   # Check GE validation results
   ls quality/great_expectations/validations/
   
   # Review specific validation
   cat quality/great_expectations/validations/[validation_id]/[asset_name]/[checkpoint_name].json
   ```

2. **Reset GE checkpoint:**
   ```bash
   # Clear validation history (if needed)
   rm -rf quality/great_expectations/validations/*
   ```

### Export Issues

1. **Google Sheets export failures:**
   - Verify service account JSON is valid
   - Check sheet permissions (service account email must have edit access)
   - Review export bookmark in DuckDB:
     ```sql
     SELECT * FROM meta_export_bookmark;
     ```

2. **CSV export issues:**
   - Check export paths in .env file
   - Verify Docker volume mounts
   - Review manifest.json files for errors

## Backfill Procedures

### Reset Export Bookmarks

1. **Reset Google Sheets bookmark:**
   ```sql
   DELETE FROM meta_export_bookmark WHERE dataset = 'marts.daily_snapshot';
   ```

2. **Reset ingestion ledger (if needed):**
   ```sql
   DELETE FROM meta_ingest_ledger WHERE filename = 'your_file.csv';
   ```

### Manual Data Processing

1. **Process specific CSV files:**
   ```bash
   # Copy files to import path
   cp your_file.csv $IMPORT_CSV_PATH/
   
   # Trigger manual run in Dagster UI
   # Or run specific assets via CLI
   ```

2. **Force dbt rebuild:**
   ```bash
   cd transform
   dbt run --full-refresh
   ```

## Key Rotation

### Google Service Account

1. **Generate new service account key:**
   - Go to Google Cloud Console
   - Create new service account or rotate existing key
   - Download new JSON file

2. **Update credentials:**
   ```bash
   # Replace the service account file
   cp new-finance-sheets-writer-prod-sa.json credentials/finance-sheets-writer-prod-sa.json
   
   # Restart services
   docker compose restart
   ```

3. **Update sheet permissions:**
   - Share target Google Sheet with new service account email
   - Remove old service account access

### Environment Variables

1. **Update .env file:**
   ```bash
   # Edit .env with new values
   nano .env
   
   # Restart services
   docker compose down && docker compose up -d
   ```

## Monitoring

### Health Checks

1. **Service health:**
   ```bash
   docker compose ps
   curl http://localhost:3000/health
   ```

2. **Database connectivity:**
   ```bash
   docker compose exec pipeline-worker python -c "import duckdb; print('DuckDB OK')"
   ```

3. **Export verification:**
   ```bash
   # Check latest CSV
   ls -la data/exports/csv/marts_daily_snapshot/latest.csv
   
   # Check manifest
   cat data/exports/metadata/marts_daily_snapshot/dt=*/manifest.json
   ```

### Log Analysis

1. **Dagster logs:**
   ```bash
   docker compose logs pipeline-worker | grep ERROR
   docker compose logs dagster-daemon | grep ERROR
   ```

2. **dbt logs:**
   ```bash
   docker compose logs pipeline-worker | grep dbt
   ```

3. **Great Expectations logs:**
   ```bash
   docker compose logs pipeline-worker | grep "great_expectations"
   ```

## Troubleshooting

### Common Issues

1. **Volume mount issues (Windows):**
   - Ensure Docker Desktop has access to your drive
   - Use forward slashes in .env paths
   - Check Docker Desktop → Settings → Resources → File Sharing

2. **Port conflicts:**
   - Change port 3000 in docker-compose.yml if needed
   - Update README.md with new port

3. **Memory issues:**
   - Increase Docker Desktop memory allocation
   - Monitor DuckDB file size

### Performance Tuning

1. **DuckDB optimization:**
   ```sql
   -- Analyze tables for better query planning
   ANALYZE;
   
   -- Check table sizes
   SELECT table_name, row_count FROM duckdb_tables();
   ```

2. **dbt performance:**
   ```bash
   # Increase dbt threads in profiles.yml
   # threads: 8  # instead of 4
   ```

## Maintenance

### Regular Tasks

1. **Weekly:**
   - Review Dagster UI for failed runs
   - Check export file sizes and counts
   - Verify Google Sheets data freshness

2. **Monthly:**
   - Clean up old export files
   - Review and update Great Expectations rules
   - Backup DuckDB file

3. **Quarterly:**
   - Rotate service account keys
   - Review and update dependencies
   - Performance analysis and optimization



