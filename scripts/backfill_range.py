#!/usr/bin/env python3
"""
Backfill script for replaying pipeline runs by date range.
This script can be used to reprocess data for specific date ranges.
"""

import os
import sys
import argparse
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

def run_dagster_asset(asset_name: str, start_date: str, end_date: str):
    """Run a specific Dagster asset with date parameters"""
    print(f"Running asset {asset_name} for date range {start_date} to {end_date}")
    
    # This would typically use Dagster's CLI or API
    # For now, we'll just print the command that would be run
    cmd = [
        "dagster", "asset", "materialize",
        "--select", asset_name,
        "--partition", f"{start_date}:{end_date}"
    ]
    print(f"Command: {' '.join(cmd)}")
    
    # In a real implementation, you would run:
    # subprocess.run(cmd, check=True)

def backfill_range(start_date: str, end_date: str, assets: list = None):
    """Backfill pipeline for a date range"""
    if assets is None:
        assets = [
            "ingest_csv_to_duckdb",
            "dbt_build_models", 
            "run_ge_checkpoints",
            "export_csv_snapshot",
            "export_to_google_sheets"
        ]
    
    print(f"Starting backfill from {start_date} to {end_date}")
    print(f"Assets to run: {', '.join(assets)}")
    
    # Parse dates
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        print(f"\nProcessing date: {date_str}")
        
        for asset in assets:
            try:
                run_dagster_asset(asset, date_str, date_str)
            except Exception as e:
                print(f"Error running {asset} for {date_str}: {e}")
                # Continue with other assets
        
        current_dt += timedelta(days=1)
    
    print(f"\nBackfill completed for {start_date} to {end_date}")

def main():
    parser = argparse.ArgumentParser(description="Backfill pipeline for date range")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--assets", nargs="+", help="Specific assets to run")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be run without executing")
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("DRY RUN MODE - No actual execution")
        print(f"Would backfill from {args.start_date} to {args.end_date}")
        if args.assets:
            print(f"Would run assets: {', '.join(args.assets)}")
        return
    
    try:
        backfill_range(args.start_date, args.end_date, args.assets)
    except Exception as e:
        print(f"Backfill failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()



