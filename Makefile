# Finance Analytics Pipeline - Makefile

.PHONY: help up down logs lint fmt test dbt-build docs demo clean install-hooks

help: ## Show this help message
	@echo "Finance Analytics Pipeline - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# Development setup
setup: ## Install dependencies and setup pre-commit hooks
	poetry install --with dev
	poetry run pre-commit install

install-hooks: ## Install pre-commit hooks
	poetry run pre-commit install

# Docker operations
up: ## Start the pipeline services
	docker compose up --build -d

down: ## Stop the pipeline services
	docker compose down

logs: ## Show logs from pipeline services
	docker compose logs -f pipeline-worker

# Code quality
lint: ## Run linting (ruff + black + mypy)
	poetry run ruff check .
	poetry run black --check .
	poetry run mypy .

fmt: ## Format code (ruff + black)
	poetry run black .
	poetry run ruff check --fix .

# Testing
test: ## Run all tests
	poetry run pytest tests/ -v

test-unit: ## Run unit tests only
	poetry run pytest -m unit -v

test-dagster: ## Run dagster tests only
	poetry run pytest -m dagster -v

test-integration: ## Run integration tests only
	poetry run pytest -m integration -v

test-e2e: ## Run e2e tests only
	poetry run pytest -m e2e -v

# dbt operations
dbt-deps: ## Install dbt dependencies
	cd transform/dbt && poetry run dbt deps --lock

dbt-build: ## Run dbt build
	cd transform/dbt && poetry run dbt build

dbt-build-ci: ## Run dbt build with CI profiles
	cd transform/dbt && poetry run dbt build --profiles-dir . --profile local_duckdb --target ci

dbt-test: ## Run dbt tests
	cd transform/dbt && poetry run dbt test

dbt-test-ci: ## Run dbt tests with CI profiles
	cd transform/dbt && poetry run dbt test --profiles-dir . --profile local_duckdb --target ci

dbt-lint: ## Run dbt linting
	cd transform/dbt && poetry run dbt parse

# CI pipeline (fast fail → deeper checks)
ci-fast: ## Fast CI checks (lint + mypy + unit tests)
	poetry run ruff format && poetry run ruff check
	poetry run mypy
	poetry run pytest -m "unit or dagster"

ci-full: ## Full CI pipeline
	$(MAKE) ci-fast
	$(MAKE) dbt-deps
	$(MAKE) dbt-build-ci
	poetry run pytest -m integration

# Documentation
docs: ## Generate documentation (if mkdocs is configured)
	@echo "Documentation generation not yet configured"

# Demo
demo: ## Run demo with sample data
	@echo "Copying sample data to import path..."
	@if [ -z "$$IMPORT_CSV_PATH" ]; then echo "Please set IMPORT_CSV_PATH environment variable"; exit 1; fi
	cp sample_data/*.csv $$IMPORT_CSV_PATH/
	@echo "Sample data copied. Run 'make up' to start the pipeline."

# Cleanup
clean: ## Clean up generated files
	docker compose down -v
	rm -rf data/warehouse/*.duckdb
	rm -rf data/exports/csv/*
	rm -rf data/exports/metadata/*
	rm -rf quality/great_expectations/uncommitted/
	rm -rf quality/great_expectations/validations/
	rm -rf transform/dbt/target/
	rm -rf transform/dbt/dbt_packages/
	rm -rf transform/dbt/logs/
	rm -rf transform/dbt/*.log
	rm -rf transform/dbt/manifest.json
	rm -rf transform/dbt/run_results.json
	rm -rf transform/dbt/semantic_manifest.json
	rm -rf transform/dbt/perf_info.json
	rm -rf transform/dbt/partial_parse.msgpack
	rm -rf transform/dbt/graph.gpickle
	rm -rf transform/dbt/graph_summary.json