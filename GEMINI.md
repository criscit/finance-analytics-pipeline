# Project Overview

This project is a local data pipeline for personal finance analytics. It uses Dagster for orchestration, DuckDB as a data warehouse, dbt for data transformation, and Great Expectations for data quality. The pipeline ingests CSV files from various financial platforms (banks and crypto exchanges), transforms and unifies the data using dbt, runs data quality checks, and then exports the consolidated data to Google Sheets and CSV snapshots.

The entire pipeline is designed to run locally using Docker and is orchestrated by a single `docker-compose.yml` file that starts a Dagster webserver.

## Key Technologies

*   **Orchestration**: Dagster
*   **Data Warehouse**: DuckDB
*   **Transformation**: dbt
*   **Data Quality**: Great Expectations
*   **Dependency Management**: Poetry
*   **Containerization**: Docker

# Building and Running

1.  **Install Dependencies**:
    ```bash
    poetry install
    ```

2.  **Configure Environment**:
    Copy `env.example` to `.env` and update the paths and Google Sheet ID.

3.  **Run the Pipeline**:
    ```bash
    docker compose up --build -d
    ```

4.  **Access Dagster UI**:
    Open [http://localhost:3000](http://localhost:3000) in your browser to view and manage the pipeline.

# Development Conventions

*   **Linting**: The project uses `ruff` for linting. Run `poetry run ruff check .` to check for issues.
*   **Formatting**: The project uses `black` for code formatting. Run `poetry run black .` to format the code.
*   **Testing**: The project uses `pytest` for testing. Run `poetry run pytest` to run the tests.
*   **Type Checking**: The project uses `mypy` for static type checking. Run `poetry run mypy .` to check for type errors.
*   **Pre-commit Hooks**: The project uses `pre-commit` to run checks before each commit.

# Project Structure

*   `orchestration/`: Contains the Dagster assets, jobs, and schedules that define the pipeline.
*   `transform/dbt/`: Contains the dbt models for transforming the raw data into a unified format.
*   `src/`: Contains Python source code for ingestion, export, and other utilities.
*   `data/`: Contains the DuckDB warehouse, raw data, and other data-related files.
*   `docker-compose.yml`: Defines the Docker services for running the pipeline.
*   `pyproject.toml`: Defines the project dependencies and development tools.
