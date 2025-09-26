# file: export/tests/test_snapshot_values.py
from pathlib import Path
import csv
import json

def test_latest_csv_exists():
    """Test that latest.csv exists and is readable"""
    latest = Path("data/exports/csv/marts_daily_snapshot/latest.csv")
    assert latest.exists(), "latest.csv not found"
    
    # Test that file is readable
    with latest.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
        assert len(rows) > 0, "latest.csv is empty"

def test_manifest_exists():
    """Test that manifest.json exists and has required fields"""
    manifest_pattern = Path("data/exports/metadata/marts_daily_snapshot")
    manifests = list(manifest_pattern.glob("*/manifest.json"))
    assert len(manifests) > 0, "No manifest.json files found"
    
    manifest = manifests[0]
    with manifest.open("r", encoding="utf-8") as f:
        data = json.load(f)
    
    required_fields = ["table", "row_count", "md5", "created_at_utc"]
    for field in required_fields:
        assert field in data, f"Missing field {field} in manifest"
    
    assert data["row_count"] > 0, "Row count should be positive"
    assert len(data["md5"]) == 32, "MD5 should be 32 characters"

def test_csv_row_count_matches_manifest():
    """Test that CSV row count matches manifest"""
    latest = Path("data/exports/csv/marts_daily_snapshot/latest.csv")
    manifest_pattern = Path("data/exports/metadata/marts_daily_snapshot")
    manifests = list(manifest_pattern.glob("*/manifest.json"))
    
    if not manifests:
        return  # Skip if no manifests
    
    manifest = manifests[0]
    with manifest.open("r", encoding="utf-8") as f:
        manifest_data = json.load(f)
    
    # Count rows in CSV (excluding header)
    with latest.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
        csv_row_count = len(rows) - 1  # Subtract header
    
    assert csv_row_count == manifest_data["row_count"], \
        f"CSV row count ({csv_row_count}) doesn't match manifest ({manifest_data['row_count']})"

def test_metric_values_sanity():
    """Test basic sanity checks on metric values"""
    latest = Path("data/exports/csv/marts_daily_snapshot/latest.csv")
    if not latest.exists():
        return  # Skip if no data
    
    with latest.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    if not rows:
        return  # Skip if no data
    
    for row in rows:
        # Check that metric values are numeric and non-negative
        metric_1 = float(row.get("metric_1", 0))
        metric_2 = float(row.get("metric_2", 0))
        
        assert metric_1 >= 0, f"metric_1 should be non-negative, got {metric_1}"
        assert metric_2 >= 0, f"metric_2 should be non-negative, got {metric_2}"



