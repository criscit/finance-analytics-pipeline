# datacontract_io/contracts.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REQUIRED_TOP = [
    "version",
    "dataset",
    "format",
    "encoding",
    "target_table",
    "columns",
    "skip_header_rows",
    "skip_footer_rows",
]
REQUIRED_TARGET = ["schema", "name"]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str
    nullable: bool
    source_column_index: int | None = None
    description: str | None = None
    format: str | None = None


@dataclass(frozen=True)
class TargetTable:
    schema: str
    name: str


@dataclass(frozen=True)
class DataContract:
    version: str
    dataset: str
    description: str | None
    owner: str | None
    domain: str | None
    source_system: str | None
    format: str  # "CSV" or "XLSX"
    encoding: str  # e.g., "utf-8"
    skip_header_rows: int  # Number of rows to skip from the top
    skip_footer_rows: int  # Number of rows to skip from the bottom
    target_table: TargetTable
    columns: list[ColumnSpec]
    metadata: dict[str, Any]

    @staticmethod
    def from_yaml(path: str) -> DataContract:
        with Path(path).open(encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        for k in REQUIRED_TOP:
            if k not in raw:
                raise ValueError(f"Missing top-level key in contract: {k}")

        tgt = raw["target_table"]
        for k in REQUIRED_TARGET:
            if k not in tgt:
                raise ValueError(f"Missing target_table.{k} in contract")

        cols = []
        for c in raw["columns"]:
            cols.append(
                ColumnSpec(
                    name=c["name"],
                    type=c.get("type", "varchar"),
                    nullable=bool(c.get("nullable", True)),
                    source_column_index=c.get("source_column_index"),
                    description=c.get("description"),
                    format=c.get("format"),
                )
            )

        # Merge top-level delimiter into metadata if present
        metadata = raw.get("metadata", {})
        if "delimiter" in raw and "delimiter" not in metadata:
            metadata["delimiter"] = raw["delimiter"]

        return DataContract(
            version=raw["version"],
            dataset=raw["dataset"],
            description=raw.get("description"),
            owner=raw.get("owner"),
            domain=raw.get("domain"),
            source_system=raw.get("source_system"),
            format=raw["format"].upper(),
            encoding=raw["encoding"],
            skip_header_rows=raw.get("skip_header_rows", 0),
            skip_footer_rows=raw.get("skip_footer_rows", 0),
            target_table=TargetTable(schema=tgt["schema"], name=tgt["name"]),
            columns=cols,
            metadata=metadata,
        )

    def csv_delimiter(self) -> str:
        delimiter = self.metadata.get("delimiter", ",")
        return str(delimiter)

    def expected_columns(self) -> list[str]:
        """Return list of expected column names in order."""
        return [c.name for c in self.columns]
