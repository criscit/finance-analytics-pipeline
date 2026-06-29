"""Dagster assets for ingesting finance data into DuckDB."""

import os
from pathlib import Path

from dagster import AssetExecutionContext, Failure, Output, asset

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
MARKETPLACE_INPUT_PATH = FINANCE_DATA_DIR_CONTAINER / "To Parse" / "Marketplace"

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


def _marketplace_leaf_options(leaf_dir: Path) -> LeafIngestionOptions:
    """Configure marketplace ingestion - merge transaction-history files like bank."""
    return LeafIngestionOptions(latest_only=False, merge_pending=True)


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

MARKETPLACE_CONFIG = IngestionSourceConfig(
    name="Marketplace",
    root_path=MARKETPLACE_INPUT_PATH,
    source_label="marketplace",
    leaf_label="data type",
    structure_hint="Marketplace/{source}/<data_type>/",
    leaf_options_factory=_marketplace_leaf_options,
)


def _run_asset_ingestion(
    config: IngestionSourceConfig, context: AssetExecutionContext
) -> Output[dict[str, int]]:
    metrics = run_ingestion(config, INGESTION_CONTEXT, context.log)
    if metrics.get("errors"):
        raise Failure(
            description=(
                f"{config.name} ingestion encountered {metrics['errors']} error(s); "
                "see logs for details"
            ),
            metadata=metrics,
        )
    return Output(metrics, metadata=metrics)


@asset(name="ingest_bank", deps=["ingest_marketplace"])
def ingest_bank(context: AssetExecutionContext) -> Output[dict[str, int]]:
    """Ingest bank statements and exports into the raw DuckDB schema."""
    return _run_asset_ingestion(BANK_CONFIG, context)


@asset(name="ingest_marketplace", deps=["ingest_crypto"])
def ingest_marketplace(context: AssetExecutionContext) -> Output[dict[str, int]]:
    """Ingest marketplace (WB, Ozon) exports into the raw DuckDB schema.

    Chained after crypto (and before bank) to serialize writes: DuckDB allows only one
    read-write process per database file, so the ingest assets run in sequence, not in parallel.
    """
    return _run_asset_ingestion(MARKETPLACE_CONFIG, context)


@asset(name="ingest_crypto")
def ingest_crypto(context: AssetExecutionContext) -> Output[dict[str, int]]:
    """Ingest crypto exchange exports into the raw DuckDB schema."""
    return _run_asset_ingestion(CRYPTO_CONFIG, context)
