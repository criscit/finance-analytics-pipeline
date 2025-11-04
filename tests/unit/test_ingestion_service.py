"""Unit tests for ingestion service workflows."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest

from src.datacontract_io.contracts import ColumnSpec, DataContract, TargetTable
from src.duckdb_utils import ensure_meta_schema
from src.ingestion.service import (
    IngestionContext,
    LeafIngestionOptions,
    MergePendingState,
    PendingFileInfo,
    ProcessingContext,
    _merge_and_ingest_pending_files,
    _prepare_file_ingestion,
    _process_file_once,
    _process_table_files,
)
from src.utils import md5_hash
from tests.constants import MD5_HASH_LENGTH, TEST_DATA_ROWS_3


def _contract(table_name: str = "t_ingestion_test") -> DataContract:
    columns = [
        ColumnSpec(name="id", type="varchar", nullable=False),
        ColumnSpec(name="name", type="varchar", nullable=False),
        ColumnSpec(name="amount", type="varchar", nullable=False),
    ]
    return DataContract(
        version="1.0",
        dataset="finance",
        description=None,
        owner=None,
        domain=None,
        source_system="fixtures",
        format="CSV",
        encoding="utf-8",
        skip_header_rows=0,
        skip_footer_rows=0,
        target_table=TargetTable(schema="prod_raw", name=table_name),
        columns=columns,
        metadata={"file_pattern": "*.csv"},
    )


def _processing_context(
    db_path: Path, finance_root: Path
) -> tuple[ProcessingContext, duckdb.DuckDBPyConnection]:
    con = duckdb.connect(str(db_path))
    ensure_meta_schema(con)
    ctx = IngestionContext(
        duckdb_path=str(db_path),
        finance_data_root=finance_root,
        stability_seconds=0,
    )
    return ProcessingContext(con=con, context=ctx, log=MagicMock()), con


def _patch_raw_parquet_paths(monkeypatch: pytest.MonkeyPatch, raw_cache_dir: Path) -> None:
    def _fake_resolve(table_name: str, md5_hash: str | None = None) -> str:
        table_dir = raw_cache_dir / table_name
        if md5_hash:
            return str(table_dir / f"{md5_hash}.parquet")
        return str(table_dir)

    monkeypatch.setattr("src.ingestion.service.resolve_raw_parquet_path", _fake_resolve)
    monkeypatch.setattr("src.datacontract_io.paths.resolve_raw_parquet_path", _fake_resolve)
    monkeypatch.setattr("src.datacontract_io.writers.resolve_raw_parquet_path", _fake_resolve)


@pytest.mark.unit
def test_prepare_file_ingestion_skips_when_md5_is_recorded(tmp_path: Path) -> None:
    finance_root = tmp_path / "finance"
    source_dir = finance_root / "bank" / "transactions"
    source_dir.mkdir(parents=True)

    ingestion_file = source_dir / "snapshot.csv"
    ingestion_file.write_text("id,name\n1,A\n", encoding="utf-8")

    pctx, con = _processing_context(tmp_path / "ledger.duckdb", finance_root)
    try:
        file_md5 = md5_hash(ingestion_file)
        con.execute(
            "insert into prod_meta.ingest_ledger values (?,?,?,?,?,?,?)",
            [
                "bank",
                "t_bank_transactions",
                ingestion_file.relative_to(finance_root).as_posix(),
                ingestion_file.stat().st_size,
                file_md5,
                "2024-01-01 00:00:00",
                None,
            ],
        )

        result = _prepare_file_ingestion(pctx, ingestion_file)

        assert result.status == "skipped"
        assert result.reason == "already ingested"
        assert result.info is None
    finally:
        con.close()


@pytest.mark.unit
def test_process_file_once_ingests_and_records_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    finance_root = tmp_path / "finance"
    source_dir = finance_root / "bank" / "transactions"
    source_dir.mkdir(parents=True)
    input_file = source_dir / "fresh_snapshot.csv"
    input_file.write_text(
        "id,name,amount\n1,A,100.0\n2,B,200.5\n3,C,300.25\n",
        encoding="utf-8",
    )

    raw_cache_dir = tmp_path / "raw_cache"
    _patch_raw_parquet_paths(monkeypatch, raw_cache_dir)

    pctx, con = _processing_context(tmp_path / "process.duckdb", finance_root)
    try:
        contract = _contract()
        result = _process_file_once(pctx, contract, input_file, "banking_app")

        assert result["status"] == "success"
        assert len(result["md5"]) == MD5_HASH_LENGTH
        assert Path(result["parquet"]).exists()
        assert Path(result["parquet"]).parent == raw_cache_dir / contract.target_table.name

        table_count_row = con.execute(
            f"select count(*) from prod_raw.{contract.target_table.name}"
        ).fetchone()
        assert table_count_row is not None
        table_count = table_count_row[0]
        assert table_count == TEST_DATA_ROWS_3
        assert result["rows"] in (-1, table_count)

        ledger_rows = con.execute(
            "select source_system_nm, table_nm, file_path, md5 from prod_meta.ingest_ledger"
        ).fetchall()
        assert ledger_rows == [
            (
                "banking_app",
                contract.target_table.name,
                input_file.relative_to(finance_root).as_posix(),
                result["md5"],
            )
        ]

        repeat = _process_file_once(pctx, contract, input_file, "banking_app")
        assert repeat == {"status": "skipped", "reason": "already ingested"}
    finally:
        con.close()


@pytest.mark.unit
def test_process_table_files_latest_only_prefers_newest(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    finance_root = tmp_path / "finance"
    leaf_dir = finance_root / "bank" / "transactions"
    leaf_dir.mkdir(parents=True)

    older_file = leaf_dir / "report_20240101.csv"
    newer_file = leaf_dir / "report_20240102.csv"
    older_file.write_text("id,name,amount\n1,A,10\n", encoding="utf-8")
    newer_file.write_text("id,name,amount\n1,B,20\n", encoding="utf-8")

    pctx = ProcessingContext(
        con=MagicMock(),
        context=IngestionContext(
            duckdb_path=str(tmp_path / "latest.duckdb"),
            finance_data_root=finance_root,
            stability_seconds=0,
        ),
        log=MagicMock(),
    )
    contract = _contract()
    processed: list[Path] = []

    def _fake_process_file_once(
        _pctx: object,
        _contract: object,
        file_path: Path,
        *_: object,
        **__: object,
    ) -> dict[str, str]:
        processed.append(file_path)
        return {"status": "success"}

    monkeypatch.setattr("src.ingestion.service._process_file_once", _fake_process_file_once)

    ingested, skipped, unsupported, errors = _process_table_files(
        pctx,
        contract,
        leaf_dir,
        "banking_app",
        LeafIngestionOptions(latest_only=True),
    )

    assert processed == [newer_file]
    assert (ingested, skipped, unsupported, errors) == (1, 1, 0, 0)


@pytest.mark.unit
def test_merge_and_ingest_pending_files_merges_batches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    finance_root = tmp_path / "finance"
    raw_cache_dir = tmp_path / "raw_cache"
    contract = _contract("t_merge_unit")

    _patch_raw_parquet_paths(monkeypatch, raw_cache_dir)

    pctx, con = _processing_context(tmp_path / "merge.duckdb", finance_root)
    try:
        merge_state = MergePendingState(
            files=[
                PendingFileInfo(
                    path=finance_root / "bank/a.csv",
                    md5="a" * MD5_HASH_LENGTH,
                    rel_path="bank/a.csv",
                    size=12,
                ),
                PendingFileInfo(
                    path=finance_root / "bank/b.csv",
                    md5="b" * MD5_HASH_LENGTH,
                    rel_path="bank/b.csv",
                    size=18,
                ),
            ],
            dataframes=[
                pd.DataFrame({"id": ["1", "2"], "name": ["A1", "A2"], "amount": ["10", "20"]}),
                pd.DataFrame({"id": ["3", "4"], "name": ["B1", "B2"], "amount": ["30", "40"]}),
            ],
        )

        success, rows = _merge_and_ingest_pending_files(pctx, contract, merge_state, "banking_app")

        assert success is True
        assert rows in (-1, 4)
        parquet_files = list((raw_cache_dir / contract.target_table.name).glob("*.parquet"))
        assert len(parquet_files) == 1

        table_rows_row = con.execute(
            f"select count(*) from prod_raw.{contract.target_table.name}"
        ).fetchone()
        assert table_rows_row is not None
        table_rows = table_rows_row[0]
        assert table_rows == 4

        ledger_rows = con.execute(
            "select file_path, md5 from prod_meta.ingest_ledger order by file_path"
        ).fetchall()
        assert ledger_rows == [
            ("bank/a.csv", "a" * MD5_HASH_LENGTH),
            ("bank/b.csv", "b" * MD5_HASH_LENGTH),
        ]
    finally:
        con.close()
