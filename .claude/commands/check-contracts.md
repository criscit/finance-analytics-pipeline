---
description: List all data contracts and their status
---

List all data contracts in data/contracts/ and check their status:

1. Find all .yaml files in data/contracts/
2. For each contract, show:
   - Source and data type
   - Target table name
   - Expected file location based on contract
   - Whether raw table exists in DuckDB (prod_raw schema)
3. Identify any contracts without corresponding raw tables (not yet ingested)
