# Data Model

## Database Schema

The pipeline uses a single DuckDB database with the following schema structure:

### Raw Data Tables (stg_*)
Staging tables created from CSV files with minimal processing.

#### stg_events
```sql
CREATE TABLE stg_events (
    id INTEGER,
    user_id INTEGER,
    event_type VARCHAR,
    event_ts TIMESTAMP,
    metric_1 INTEGER,
    metric_2 INTEGER,
    created_at TIMESTAMP
);
```

#### stg_sales
```sql
CREATE TABLE stg_sales (
    sale_id INTEGER,
    product_id INTEGER,
    user_id INTEGER,
    sale_date TIMESTAMP,
    amount DECIMAL,
    quantity INTEGER,
    created_at TIMESTAMP
);
```

#### stg_inventory
```sql
CREATE TABLE stg_inventory (
    item_id INTEGER,
    product_name VARCHAR,
    category VARCHAR,
    stock_quantity INTEGER,
    unit_price DECIMAL,
    updated_at TIMESTAMP,
    created_at TIMESTAMP
);
```

### Core Tables (core_*)
Business logic tables with deduplication and data cleaning.

#### core_events
```sql
CREATE TABLE core_events (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    event_type VARCHAR NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    metric_1 INTEGER NOT NULL,
    metric_2 INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL
);
```

### Mart Tables (marts.*)
Final export-ready tables for downstream consumption.

#### marts.daily_snapshot
```sql
CREATE TABLE marts.daily_snapshot (
    id INTEGER,
    user_id INTEGER,
    event_type VARCHAR,
    updated_at TIMESTAMP,
    metric_1 INTEGER,
    metric_2 INTEGER
);
```

### Metadata Tables
System tables for pipeline management.

#### meta_ingest_ledger
```sql
CREATE TABLE meta_ingest_ledger (
    filename TEXT PRIMARY KEY,
    size BIGINT,
    md5 TEXT,
    ingested_at TIMESTAMP DEFAULT now()
);
```

#### meta_export_bookmark
```sql
CREATE TABLE meta_export_bookmark (
    dataset TEXT PRIMARY KEY,
    last_ts TIMESTAMP,
    last_id BIGINT
);
```

## Data Types

### Standard Types
- **INTEGER**: Numeric identifiers and counts
- **VARCHAR**: Text fields with variable length
- **TIMESTAMP**: Date and time values
- **DECIMAL**: Monetary amounts and precise numbers
- **TEXT**: Long text fields and metadata

### Constraints
- **PRIMARY KEY**: Unique identifiers
- **NOT NULL**: Required fields
- **UNIQUE**: Unique values within tables
- **CHECK**: Value range validations

## Data Quality Rules

### Great Expectations Expectations

#### stg_events
```yaml
expectations:
  - expect_column_to_exist:
      column: id
  - expect_column_values_to_not_be_null:
      column: id
  - expect_column_values_to_not_be_null:
      column: user_id
  - expect_column_values_to_not_be_null:
      column: event_type
  - expect_column_values_to_be_in_set:
      column: event_type
      value_set: ['page_view', 'click', 'purchase', 'signup']
  - expect_column_values_to_be_between:
      column: metric_1
      min_value: 0
      max_value: 1000
```

#### marts.daily_snapshot
```yaml
expectations:
  - expect_column_values_to_not_be_null:
      column: id
  - expect_column_values_to_not_be_null:
      column: user_id
  - expect_column_values_to_not_be_null:
      column: event_type
  - expect_column_values_to_be_between:
      column: metric_1
      min_value: 0
  - expect_column_values_to_be_between:
      column: metric_2
      min_value: 0
```

## dbt Tests

### Source Tests
```yaml
sources:
  - name: raw
    tables:
      - name: stg_events
        columns:
          - name: id
            tests:
              - not_null
              - unique
          - name: user_id
            tests:
              - not_null
          - name: event_type
            tests:
              - not_null
              - accepted_values:
                  values: ['page_view', 'click', 'purchase', 'signup']
```

### Model Tests
```yaml
models:
  - name: daily_snapshot
    columns:
      - name: id
        tests:
          - not_null
          - unique
      - name: user_id
        tests:
          - not_null
      - name: event_type
        tests:
          - not_null
```

## Data Lineage

### Staging → Core → Marts
```
CSV Files → stg_events → core_events → marts.daily_snapshot
```

### Transformation Logic
1. **Staging**: Raw data ingestion with type casting
2. **Core**: Business logic, deduplication, data cleaning
3. **Marts**: Final export-ready tables with renamed columns

### Column Mappings
- `event_ts` → `updated_at` (for export compatibility)
- Metric columns preserved as-is
- User and event identifiers maintained

## Export Schema

### CSV Export Format
```csv
id,user_id,event_type,updated_at,metric_1,metric_2
1,101,page_view,2024-01-01 10:00:00,100,50
2,102,click,2024-01-01 10:05:00,150,75
```

### Google Sheets Format
- Same column structure as CSV
- Incremental updates only
- Header row included on first write

## Data Retention

### Ingestion Ledger
- Permanent record of all processed files
- Used for idempotency checks
- Includes file metadata (size, MD5, timestamp)

### Export Bookmarks
- Tracks last exported record per dataset
- Enables incremental exports
- Prevents duplicate data in Google Sheets

### Export Files
- Daily snapshots with date partitioning
- `latest.csv` always points to most recent data
- Manifest files include metadata and checksums

## Performance Considerations

### Indexing
- Primary keys on all tables
- Timestamp columns for time-based queries
- User ID columns for user-based analytics

### Partitioning
- Export files partitioned by date
- DuckDB automatic columnar storage
- Efficient compression for analytics workloads

### Query Optimization
- dbt models use table materialization
- DuckDB automatic query optimization
- Parallel processing where applicable



