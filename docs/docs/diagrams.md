# Diagrams

## System Architecture

```mermaid
graph TB
    subgraph "Host System"
        CSV[CSV Files]
        EXPORT[Export Directory]
        CREDS[Credentials]
    end
    
    subgraph "Docker Container"
        subgraph "Dagster Orchestration"
            ASSETS[Assets]
            SCHED[Schedule]
        end
        
        subgraph "Data Processing"
            INGEST[CSV Ingest]
            DBT[dbt Transform]
            GE[Great Expectations]
        end
        
        subgraph "Storage"
            DUCKDB[DuckDB Warehouse]
            LEDGER[Ingestion Ledger]
            BOOKMARK[Export Bookmark]
        end
        
        subgraph "Exports"
            CSVEXP[CSV Export]
            SHEETS[Google Sheets]
        end
    end
    
    subgraph "External"
        GSHEET[Google Sheets]
    end
    
    CSV --> INGEST
    INGEST --> DUCKDB
    INGEST --> LEDGER
    DUCKDB --> DBT
    DBT --> GE
    GE --> CSVEXP
    GE --> SHEETS
    CSVEXP --> EXPORT
    SHEETS --> GSHEET
    SHEETS --> BOOKMARK
    CREDS --> SHEETS
```

## Data Flow

```mermaid
sequenceDiagram
    participant CSV as CSV Files
    participant Ingest as Ingest Asset
    participant DuckDB as DuckDB
    participant dbt as dbt Transform
    participant GE as Great Expectations
    participant CSVExp as CSV Export
    participant Sheets as Google Sheets
    
    CSV->>Ingest: New CSV files
    Ingest->>DuckDB: Load to staging tables
    Ingest->>DuckDB: Update ingestion ledger
    
    dbt->>DuckDB: Read staging tables
    dbt->>DuckDB: Write core/marts tables
    
    GE->>DuckDB: Validate data quality
    GE->>GE: Generate validation report
    
    CSVExp->>DuckDB: Read marts tables
    CSVExp->>CSVExp: Write CSV snapshots
    CSVExp->>CSVExp: Generate manifests
    
    Sheets->>DuckDB: Read new data
    Sheets->>Sheets: Append to Google Sheets
    Sheets->>DuckDB: Update export bookmark
```

## Asset Dependencies

```mermaid
graph TD
    A[ingest_csv_to_duckdb] --> B[dbt_build_models]
    B --> C[run_ge_checkpoints]
    C --> D[export_csv_snapshot]
    C --> E[export_to_google_sheets]
    
    A --> F[meta_ingest_ledger]
    E --> G[meta_export_bookmark]
    
    style A fill:#e1f5fe
    style B fill:#f3e5f5
    style C fill:#fff3e0
    style D fill:#e8f5e8
    style E fill:#e8f5e8
```

## File System Layout

```mermaid
graph TD
    subgraph "Host Paths"
        H1[G:/USER/DATA/FINDNA/]
        H2[G:/USER/DATA/FINDNA/RESULT/]
    end
    
    subgraph "Container Paths"
        C1[/app/data/raw/]
        C2[/app/data/warehouse/]
        C3[/app/data/exports/csv/]
        C4[/app/data/exports/metadata/]
    end
    
    H1 --> C1
    H2 --> C3
    H2 --> C4
```

## Volume Mounts

```mermaid
graph LR
    subgraph "Host"
        A[IMPORT_CSV_PATH]
        B[EXPORT_CSV_PATH]
        C[credentials/]
        D[data/warehouse/]
    end
    
    subgraph "Container"
        E[/app/data/raw]
        F[/app/data/exports/csv]
        G[/app/data/exports/metadata]
        H[/app/credentials]
        I[/app/data/warehouse]
    end
    
    A --> E
    B --> F
    B --> G
    C --> H
    D --> I
```

## Database Schema

```mermaid
erDiagram
    stg_events {
        integer id
        integer user_id
        varchar event_type
        timestamp event_ts
        integer metric_1
        integer metric_2
        timestamp created_at
    }
    
    core_events {
        integer id PK
        integer user_id
        varchar event_type
        timestamp event_ts
        integer metric_1
        integer metric_2
        timestamp created_at
    }
    
    daily_snapshot {
        integer id
        integer user_id
        varchar event_type
        timestamp updated_at
        integer metric_1
        integer metric_2
    }
    
    meta_ingest_ledger {
        text filename PK
        bigint size
        text md5
        timestamp ingested_at
    }
    
    meta_export_bookmark {
        text dataset PK
        timestamp last_ts
        bigint last_id
    }
    
    stg_events ||--|| core_events : transforms
    core_events ||--|| daily_snapshot : exports
```

## Export Process

```mermaid
graph TD
    A[Read marts.daily_snapshot] --> B[Generate CSV]
    B --> C[Calculate MD5]
    C --> D[Write to dated folder]
    D --> E[Copy to latest.csv]
    E --> F[Generate manifest.json]
    
    G[Read new data] --> H[Prepare for Sheets]
    H --> I[Append to Google Sheets]
    I --> J[Update bookmark]
    
    style A fill:#e1f5fe
    style G fill:#e1f5fe
    style F fill:#e8f5e8
    style J fill:#e8f5e8
```

## Error Handling Flow

```mermaid
graph TD
    A[File Stability Check] --> B{File Stable?}
    B -->|No| C[Skip File]
    B -->|Yes| D[Check Ledger]
    D --> E{Already Processed?}
    E -->|Yes| F[Skip File]
    E -->|No| G[Process File]
    G --> H[Update Ledger]
    
    I[Data Quality Check] --> J{Quality Pass?}
    J -->|No| K[Fail Pipeline]
    J -->|Yes| L[Continue Export]
    
    M[Export Bookmark] --> N{New Data?}
    N -->|No| O[Skip Export]
    N -->|Yes| P[Export Data]
    P --> Q[Update Bookmark]
```

## Monitoring Dashboard

```mermaid
graph TB
    subgraph "Dagster UI"
        A[Asset Status]
        B[Run History]
        C[Logs]
        D[Lineage Graph]
    end
    
    subgraph "Health Checks"
        E[Container Status]
        F[Database Connectivity]
        G[Export Verification]
    end
    
    subgraph "Logs"
        H[Dagster Logs]
        I[dbt Logs]
        J[GE Logs]
    end
    
    A --> E
    B --> H
    C --> I
    D --> J
```



