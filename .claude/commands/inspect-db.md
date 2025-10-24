---
description: Inspect DuckDB warehouse tables and schemas
---

Inspect the DuckDB analytics warehouse at data/warehouse/analytics.duckdb.

Show:
1. All schemas in the database
2. Tables in each schema (prod_raw, prod_stg, prod_core, prod_mart, prod_imart, prod_meta)
3. Row counts for each table
4. Sample of recent data from prod_meta.ingest_ledger

Use the Read tool to check if the database exists first, then use appropriate DuckDB Python commands.
