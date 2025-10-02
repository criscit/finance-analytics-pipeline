# file: export/tests/test_latest_csv_contract.py
from __future__ import annotations

import csv

from .conftest import ExportArtifacts


def test_latest_csv_contract(export_artifacts: ExportArtifacts) -> None:
    """Validate the header contract for ``latest.csv`` exports."""

    latest = export_artifacts.latest
    assert latest.exists(), "latest.csv should exist for contract validation"

    with latest.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file, delimiter=";")
        header = next(reader)

    assert (
        header == export_artifacts.expected_header
    ), f"Header mismatch: {header} != {export_artifacts.expected_header}"
