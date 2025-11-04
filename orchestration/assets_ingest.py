"""Dagster assets for ingesting finance data into DuckDB."""

from __future__ import annotations

import os
from pathlib import Path

from dagster import Output, asset, get_dagster_logger

from src.ingestion import (
    IngestionContext,
    IngestionSourceConfig,
    LeafIngestionOptions,
    run_ingestion,
)

FINANCE_DATA_DIR_CONTAINER = Path(os.getenv("FINANCE_DATA_DIR_CONTAINER", "/app/data/finance"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse/analytics.duckdb")
STABILITY_S = 8

BANK_INPUT_PATH = FINANCE_DATA_DIR_CONTAINER / "To Parse" / "Bank"
CRYPTO_INPUT_PATH = FINANCE_DATA_DIR_CONTAINER / "To Parse" / "Crypto"

INGESTION_CONTEXT = IngestionContext(
    duckdb_path=DUCKDB_PATH,
    finance_data_root=FINANCE_DATA_DIR_CONTAINER,
    stability_seconds=STABILITY_S,
)


def _bank_leaf_options(leaf_dir: Path) -> LeafIngestionOptions:
    """Configure bank ingestion behaviour - merge all files into consolidated parquet."""
    return LeafIngestionOptions(latest_only=False, merge_pending=True)


def _crypto_leaf_options(leaf_dir: Path) -> LeafIngestionOptions:
    """Configure crypto ingestion behaviour per transaction type directory."""
    name = leaf_dir.name.lower()
    is_snapshot = name in {"assets", "earn"}
    return LeafIngestionOptions(latest_only=is_snapshot, merge_pending=not is_snapshot)


BANK_CONFIG = IngestionSourceConfig(
    name="Bank",
    root_path=BANK_INPUT_PATH,
    source_label="bank",
    leaf_label="data type",
    structure_hint="Bank/{source}/<data_type>/",
    leaf_options_factory=_bank_leaf_options,
)

CRYPTO_CONFIG = IngestionSourceConfig(
    name="Crypto",
    root_path=CRYPTO_INPUT_PATH,
    source_label="platform",
    leaf_label="transaction type",
    structure_hint="Crypto/{source}/<transaction_type>/",
    leaf_options_factory=_crypto_leaf_options,
)


def _run_asset_ingestion(config: IngestionSourceConfig) -> Output[dict[str, int]]:
    log = get_dagster_logger()
    metrics = run_ingestion(config, INGESTION_CONTEXT, log)
    return Output(metrics, metadata=metrics)


@asset(name="ingest_bank", deps=["ingest_crypto"])
def ingest_bank() -> Output[dict[str, int]]:
    """Ingest bank statements and exports into the raw DuckDB schema."""
    return _run_asset_ingestion(BANK_CONFIG)


@asset(name="ingest_crypto")
def ingest_crypto() -> Output[dict[str, int]]:
    """Ingest crypto exchange exports into the raw DuckDB schema."""
    return _run_asset_ingestion(CRYPTO_CONFIG)
