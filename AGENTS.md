# Repository Guidelines

## Project Structure & Module Organization
- `src/`: Core ingestion, DuckDB, Google Sheets, and utility modules; keep reusable code here.
- `orchestration/dagster_project/`: Dagster assets and schedules that orchestrate the daily pipeline.
- `transform/dbt/models/`: dbt layers (`staging`, `core`, `marts`) plus profiles in `transform/dbt/profiles/`.
- `tests/`: Layered suites (`unit`, `integration`, `dagster`, `e2e`) with shared fixtures in `tests/fixtures/`.
- `docs/` / `reports/`: Reference material and generated analytics; store new documentation or run artifacts here.

## Build, Test, and Development Commands
- `make setup`: Install Poetry dependencies (incl. dev) and register pre-commit hooks.
- `make up` / `make down`: Start or stop the Docker stack (Dagster UI, worker, DuckDB).
- `make lint` / `make fmt`: Run Ruff, Black, and mypy in check or auto-fix mode.
- `make test` plus scope variants (`test-unit`, `test-integration`, `test-dagster`, `test-e2e`): Execute pytest suites with `-m` selectors.
- `make dbt-build` or `make dbt-test`: Build models and run dbt tests from `transform/dbt`.

## Coding Style & Naming Conventions
- Python 3.11 with strict mypy; annotate public functions and keep interfaces typed.
- Follow Black/Ruff defaults (line length 100), f-strings, sorted imports, and constants in `UPPER_SNAKE_CASE`.
- Name Dagster assets, dbt models, and tests after domain nouns (`events_transactions`, `test_events_transactions.py`).
- Read secrets via helpers in `src/utils.py`; keep `.env` keys out of code.

## Testing Guidelines
- Pytest looks under `tests/`; name files `test_*` and classes `Test*` per `pytest.ini`.
- Tag suites with `@pytest.mark.unit`, `integration`, `dagster`, or `e2e` to align with Make targets.
- For new dbt models, run `make dbt-build` then `make test-integration` and update `tests/fixtures/` as needed.
- Add regression tests for bug fixes and keep coverage high on new modules.

## Commit & Pull Request Guidelines
- Match the existing history with imperative, capitalized subjects (`Add mart layer for crypto`).
- Squash WIP commits before review; split cross-cutting changes into focused commits.
- In PRs, outline affected assets/models, call out `.env` or schema changes, and note `make lint` plus relevant `make test-*` results.
- Link issues or Dagster incidents and include screenshots for UI-facing updates.

## Security & Configuration Tips
- Copy `env.example` to `.env`; store secrets under `credentials/` and exclude DuckDB artifacts in `data/warehouse/`.
- Ensure `.env` paths use forward slashes (Docker on Windows) before `make up`.
- Prefer `poetry run python src/scripts/...` helpers for ad hoc work so orchestration code stays auditable.
