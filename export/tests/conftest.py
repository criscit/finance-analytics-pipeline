"""Fixtures for export tests."""

from __future__ import annotations

import csv
import hashlib
import json
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pytest

EXPORT_SUBDIR = "marts_bank_finance_analytics"
EXPECTED_HEADER = [
    "Дата операции",
    "Дата платежа",
    "Номер карты",
    "Статус",
    "Сумма операции",
    "Валюта операции",
    "Сумма платежа",
    "Валюта платежа",
    "Кэшбэк",
    "Категория",
    "MCC",
    "Описание",
    "Бонусы (включая кэшбэк)",
    "Округление на инвесткопилку",
    "Сумма операции с округлением",
]


@dataclass(frozen=True)
class ExportArtifacts:
    """Container for the CSV and manifest artefacts used in tests."""

    latest: Path
    manifest: Path
    expected_header: list[str]


def _ensure_real_export() -> ExportArtifacts | None:
    """Return paths to real export artefacts if they exist on disk."""

    latest = Path("data/exports/csv") / EXPORT_SUBDIR / "latest.csv"
    manifest_root = Path("data/exports/metadata") / EXPORT_SUBDIR

    if not latest.exists() or not manifest_root.exists():
        return None

    manifests: Iterable[Path] = manifest_root.glob("*/manifest.json")
    try:
        manifest = next(iter(sorted(manifests)))
    except StopIteration:
        return None

    return ExportArtifacts(latest=latest, manifest=manifest, expected_header=EXPECTED_HEADER)


def _md5(path: Path) -> str:
    """Compute an MD5 checksum for the supplied file."""

    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_synthetic_export(tmp_path: Path) -> ExportArtifacts:
    """Create a synthetic export folder structure for tests to use."""

    exports_root = tmp_path / "data" / "exports"
    csv_root = exports_root / "csv" / EXPORT_SUBDIR
    metadata_root = exports_root / "metadata" / EXPORT_SUBDIR

    snapshot_dir = csv_root / "dt=2024-01-01"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir = metadata_root / "dt=2024-01-01"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    snapshot_csv = snapshot_dir / "part-00000.csv"
    latest_csv = csv_root / "latest.csv"

    rows = [
        EXPECTED_HEADER,
        [
            "20.09.2025 17:09:04",
            "20.09.2025",
            "",
            "OK",
            "-11405,00",
            "RUB",
            "-11405,00",
            "RUB",
            "",
            "Переводы",
            "",
            "Данила К.",
            "0,00",
            "0,00",
            "11405,00",
        ],
        [
            "20.09.2025 15:45:59",
            "20.09.2025",
            "*4822",
            "OK",
            "-2210,00",
            "RUB",
            "-2210,00",
            "RUB",
            "21",
            "Маркетплейсы",
            "5300",
            "Aliexpress.ru",
            "21,00",
            "0,00",
            "2210,00",
        ],
        [
            "20.09.2025 15:36:52",
            "20.09.2025",
            "*4822",
            "OK",
            "-412,00",
            "RUB",
            "-412,00",
            "RUB",
            "4",
            "Маркетплейсы",
            "5300",
            "Aliexpress.ru",
            "4,00",
            "0,00",
            "412,00",
        ],
        [
            "20.09.2025 15:31:57",
            "20.09.2025",
            "*4822",
            "OK",
            "-1880,00",
            "RUB",
            "-1880,00",
            "RUB",
            "18",
            "Маркетплейсы",
            "5300",
            "Aliexpress.ru",
            "18,00",
            "0,00",
            "1880,00",
        ],
    ]

    with snapshot_csv.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file, delimiter=";", quoting=csv.QUOTE_ALL)
        writer.writerows(rows)

    shutil.copy2(snapshot_csv, latest_csv)

    manifest = metadata_dir / "manifest.json"
    manifest_data = {
        "table": "prod_imart.t_bank_transactions",
        "csv_path": str(snapshot_csv),
        "latest_path": str(latest_csv),
        "row_count": len(rows) - 1,  # exclude header row
        "md5": _md5(snapshot_csv),
        "created_at_utc": datetime(2024, 1, 1, 0, 0, 0).isoformat() + "Z",
    }

    with manifest.open("w", encoding="utf-8") as manifest_file:
        json.dump(manifest_data, manifest_file, indent=2)

    return ExportArtifacts(latest=latest_csv, manifest=manifest, expected_header=EXPECTED_HEADER)


@pytest.fixture
def export_artifacts(tmp_path: Path) -> ExportArtifacts:
    """Provide paths to CSV and manifest artefacts for tests."""

    real_artifacts = _ensure_real_export()
    if real_artifacts is not None:
        return real_artifacts

    return _build_synthetic_export(tmp_path)
