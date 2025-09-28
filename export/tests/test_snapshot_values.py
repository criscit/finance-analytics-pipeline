# file: export/tests/test_snapshot_values.py
from __future__ import annotations

import csv
import json
from decimal import Decimal, InvalidOperation

from .conftest import ExportArtifacts

MD5_HASH_LENGTH = 32
EXPECTED_TABLE = "prod_imart.t_bank_transactions"


def _to_decimal(value: str) -> Decimal:
    """Convert comma-based decimal strings to :class:`Decimal` values."""

    if value is None or value == "":
        return Decimal("0")

    normalised = value.replace(" ", "").replace(",", ".")
    try:
        return Decimal(normalised)
    except InvalidOperation as error:  # pragma: no cover
        raise AssertionError(f"Value {value!r} is not a valid decimal") from error


def test_latest_csv_exists(export_artifacts: ExportArtifacts) -> None:
    """Test that latest.csv exists and is readable."""

    latest = export_artifacts.latest
    assert latest.exists(), "latest.csv not found"

    with latest.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file, delimiter=";")
        rows = list(reader)

    assert rows, "latest.csv is empty"
    assert rows[0] == export_artifacts.expected_header, "latest.csv header mismatch"


def test_manifest_exists(export_artifacts: ExportArtifacts) -> None:
    """Test that manifest.json exists and has required fields."""

    manifest = export_artifacts.manifest
    assert manifest.exists(), "manifest.json not found"

    with manifest.open("r", encoding="utf-8") as manifest_file:
        data = json.load(manifest_file)

    required_fields = ["table", "row_count", "md5", "created_at_utc"]
    for field in required_fields:
        assert field in data, f"Missing field {field} in manifest"

    assert data["row_count"] > 0, "Row count should be positive"
    assert len(data["md5"]) == MD5_HASH_LENGTH, "MD5 should be 32 characters"
    assert data["table"] == EXPECTED_TABLE


def test_csv_row_count_matches_manifest(export_artifacts: ExportArtifacts) -> None:
    """Test that CSV row count matches the manifest metadata."""

    latest = export_artifacts.latest
    manifest = export_artifacts.manifest

    with manifest.open("r", encoding="utf-8") as manifest_file:
        manifest_data = json.load(manifest_file)

    with latest.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file, delimiter=";")
        rows = list(reader)

    csv_row_count = max(len(rows) - 1, 0)
    assert csv_row_count == manifest_data["row_count"]


def test_transaction_amounts_are_consistent(export_artifacts: ExportArtifacts) -> None:
    """Validate the numeric relationships in the export rows."""

    latest = export_artifacts.latest
    if not latest.exists():
        return  # The fixture guarantees creation, but guard defensively.

    with latest.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file, delimiter=";")
        rows = list(reader)

    if not rows:
        return

    cashback_positive = 0
    debit_transactions = 0

    for row in rows:
        amount = _to_decimal(row["Сумма операции"])
        payment_amount = _to_decimal(row["Сумма платежа"])
        rounded_amount = _to_decimal(row["Сумма операции с округлением"])
        cashback = _to_decimal(row["Кэшбэк"])
        bonuses = _to_decimal(row["Бонусы (включая кэшбэк)"])

        assert payment_amount == amount, "Payment amount should match operation amount"
        assert rounded_amount == abs(
            amount
        ), "Rounded amount should equal the absolute operation amount"
        assert row["Валюта операции"] == row["Валюта платежа"], "Currencies should align"

        if row.get("MCC"):
            assert row["MCC"].isdigit(), f"MCC should be numeric when present, got {row['MCC']}"

        if cashback > 0:
            cashback_positive += 1
        if amount < 0:
            debit_transactions += 1

        assert bonuses >= 0, "Bonuses should not be negative"

    assert debit_transactions > 0, "Expected at least one debit transaction"
    assert cashback_positive > 0, "Expected at least one cashback entry"
