# Finance Analytics Pipeline - Makefile

.PHONY: help up down logs lint fmt test dbt-build docs demo clean

help: ## Show this help message
	@echo "Finance Analytics Pipeline - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

up: ## Start the pipeline services
	docker compose up --build -d

down: ## Stop the pipeline services
	docker compose down

logs: ## Show logs from pipeline services
	docker compose logs -f pipeline-worker

lint: ## Run linting checks
	poetry run ruff check .
	poetry run black --check .

fmt: ## Format code
	poetry run black .
	poetry run ruff check --fix .

test: ## Run tests
	poetry run pytest export/tests/ -v

dbt-build: ## Run dbt build locally (requires DUCKDB_PATH env var)
	cd transform && dbt deps
	cd transform && dbt build

docs: ## Generate documentation (if mkdocs is configured)
	@echo "Documentation generation not yet configured"

demo: ## Run demo with sample data
	@echo "Copying sample data to import path..."
	@if [ -z "$$IMPORT_CSV_PATH" ]; then echo "Please set IMPORT_CSV_PATH environment variable"; exit 1; fi
	cp sample_data/*.csv $$IMPORT_CSV_PATH/
	@echo "Sample data copied. Run 'make up' to start the pipeline."

clean: ## Clean up generated files
	docker compose down -v
	rm -rf data/warehouse/*.duckdb
	rm -rf data/exports/csv/*
	rm -rf data/exports/metadata/*
	rm -rf quality/great_expectations/uncommitted/
	rm -rf quality/great_expectations/validations/
	rm -rf transform/target/
	rm -rf transform/dbt_packages/



