---
description: Guide for adding a new data source
---

You need to help add a new data source to the pipeline. Ask the user:

1. **Source details**:
   - What is the source name? (e.g., "revolut", "binance")
   - What type of data? (e.g., "transactions", "assets", "deposits")
   - Is it Bank or Crypto?

2. **File format**:
   - CSV or XLSX?
   - Delimiter if CSV? (comma, tab, semicolon)
   - How many header rows to skip?
   - How many footer rows to skip?

3. **Column schema**:
   - What columns exist in the file?
   - What are their data types? (text, integer, decimal, date, datetime)
   - Date/datetime format? (e.g., "%Y-%m-%d", "%d/%m/%Y %H:%M")
   - Which columns are nullable?

Once you have this information:
1. Create the data contract in data/contracts/{source}_{type}.yaml
2. Create staging model in transform/dbt/models/staging/{source}/stg_load_{source}_{type}.sql
3. Create the directory structure for file placement
4. Provide instructions on where to place files and how to test
