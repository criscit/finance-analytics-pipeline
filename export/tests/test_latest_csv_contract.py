# file: export/tests/test_latest_csv_contract.py
import csv
from pathlib import Path


def test_latest_csv_contract() -> None:
    latest = Path("data/exports/csv/marts_bank_finance_analytics/latest.csv")
    assert latest.exists(), "latest.csv not produced"
    with latest.open(newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        header = next(r)
    expected = ["id", "user_id", "event_type", "updated_at", "metric_1", "metric_2"]
    assert header == expected, f"Header mismatch: {header} != {expected}"
