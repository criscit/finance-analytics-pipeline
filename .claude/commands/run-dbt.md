---
description: Run dbt models (staging -> core -> marts)
---

Run dbt transformations. Ask the user if they want to:

1. Run all models
2. Run specific model(s)
3. Run a model and its downstream dependencies
4. Run a specific selector

Then execute the appropriate dbt command using the standard pattern:

- All models: `poetry run dbt run --project-dir transform/dbt --profiles-dir transform/dbt/profiles`
- Specific model: `poetry run dbt run --project-dir transform/dbt --profiles-dir transform/dbt/profiles --select {model_name}`
- Model + downstream: `poetry run dbt run --project-dir transform/dbt --profiles-dir transform/dbt/profiles --select {model_name}+`
- Specific selector: `poetry run dbt build --project-dir transform/dbt --profiles-dir transform/dbt/profiles --selector {selector_name}`

After running:
- Report models built successfully
- Show any errors encountered
- Suggest fixes for common dbt issues
