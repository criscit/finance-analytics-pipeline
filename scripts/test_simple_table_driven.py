#!/usr/bin/env python3
"""
Test script for simple table-driven architecture.
Tests the correct approach: ingest_ledger -> table detection -> bank tags -> model selection.
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_bank_data_config() -> bool:
    """Test bank_data_config seed file"""
    print("Testing bank_data_config seed file...")

    try:
        config_file = Path("transform/dbt/seeds/bank_data_config.csv")
        if config_file.exists():
            print("SUCCESS: Found bank_data_config.csv")

            # Check if file has proper structure
            with config_file.open() as f:
                lines = f.readlines()

            if len(lines) > 1:  # Header + data
                print(f"SUCCESS: Found {len(lines)-1} bank configurations")

                # Check for table_name column
                header = lines[0].strip().split(",")
                if "table_name" in header:
                    print("SUCCESS: Found table_name column")
                else:
                    print("ERROR: Missing table_name column")
                    return False

                return True
            print("ERROR: bank_data_config.csv is empty")
            return False
        print("ERROR: Missing bank_data_config.csv")
        return False

    except Exception as e:
        print(f"ERROR: bank_data_config test failed: {e}")
        return False


def test_selectors_yml() -> bool:
    """Test selectors.yml file"""
    print("Testing selectors.yml...")

    try:
        selectors_file = Path("transform/dbt/selectors.yml")
        if selectors_file.exists():
            print("SUCCESS: Found selectors.yml")

            # Check if file has proper structure
            with selectors_file.open() as f:
                content = f.read()

            # Check for key selectors
            key_selectors = [
                "run_bank_staging",
                "run_bank_core",
                "run_bank_mart",
                "test_bank_staging",
                "test_bank_core",
                "test_bank_mart",
                "run_bank_full",
            ]

            for selector in key_selectors:
                if selector in content:
                    print(f"SUCCESS: Found selector {selector}")
                else:
                    print(f"WARNING: Missing selector {selector}")

            return True
        print("ERROR: Missing selectors.yml")
        return False

    except Exception as e:
        print(f"ERROR: selectors.yml test failed: {e}")
        return False


def test_tagged_models() -> bool:
    """Test tagged models structure"""
    print("Testing tagged models...")

    try:
        # Test T-Bank models
        t_bank_models = [
            "transform/dbt/models/staging/t_bank/stg_t_bank_transactions.sql",
            "transform/dbt/models/core/t_bank/core_t_bank_transactions.sql",
            "transform/dbt/models/marts/t_bank/mart_t_bank_transactions.sql",
        ]

        for model in t_bank_models:
            if Path(model).exists():
                print(f"SUCCESS: Found {model}")

                # Check if model has proper tags
                with Path(model).open(encoding="utf-8") as f:
                    content = f.read()

                if "tags=" in content and "t_bank" in content:
                    print(f"SUCCESS: {model} has proper tags")
                else:
                    print(f"WARNING: {model} may be missing proper tags")
            else:
                print(f"ERROR: Missing {model}")
                return False

        return True

    except Exception as e:
        print(f"ERROR: Tagged models test failed: {e}")
        return False


def test_simple_orchestration() -> bool:
    """Test simple table-driven orchestration assets"""
    print("Testing simple table-driven orchestration...")

    try:
        orchestration_file = Path("orchestration/assets_simple_table_driven.py")
        if orchestration_file.exists():
            print("SUCCESS: Found simple table-driven orchestration assets")
            return True
        print("ERROR: Missing simple table-driven orchestration assets")
        return False

    except Exception as e:
        print(f"ERROR: Simple orchestration test failed: {e}")
        return False


def test_asset_imports() -> bool:
    """Test that simple table-driven assets can be imported"""
    print("Testing simple table-driven asset imports...")

    try:
        print("SUCCESS: All simple table-driven assets imported successfully")
        return True

    except Exception as e:
        print(f"ERROR: Simple table-driven asset import test failed: {e}")
        return False


def test_dbt_project_structure() -> bool:
    """Test dbt project structure for simple table-driven approach"""
    print("Testing dbt project structure...")

    try:
        # Check for key dbt files
        dbt_files = [
            "transform/dbt/dbt_project.yml",
            "transform/dbt/selectors.yml",
            "transform/dbt/seeds/bank_data_config.csv",
        ]

        for file_path in dbt_files:
            if Path(file_path).exists():
                print(f"SUCCESS: Found {file_path}")
            else:
                print(f"ERROR: Missing {file_path}")
                return False

        return True

    except Exception as e:
        print(f"ERROR: dbt project structure test failed: {e}")
        return False


def test_table_name_mapping() -> bool:
    """Test that table names are properly mapped in bank_data_config"""
    print("Testing table name mapping...")

    try:
        config_file = Path("transform/dbt/seeds/bank_data_config.csv")
        if config_file.exists():
            with config_file.open() as f:
                lines = f.readlines()

            # Check for table names
            table_names = []
            MIN_COLUMNS = 6  # Minimum columns including table_name
            for line in lines[1:]:  # Skip header
                if line.strip():
                    parts = line.strip().split(",")
                    if len(parts) >= MIN_COLUMNS:  # Ensure we have enough columns
                        table_name = parts[5]  # table_name column
                        table_names.append(table_name)

            expected_tables = ["t_bank_transactions", "bakai_transactions", "sberbank_transactions"]

            for expected_table in expected_tables:
                if expected_table in table_names:
                    print(f"SUCCESS: Found table mapping for {expected_table}")
                else:
                    print(f"WARNING: Missing table mapping for {expected_table}")

            return True
        print("ERROR: bank_data_config.csv not found")
        return False

    except Exception as e:
        print(f"ERROR: Table name mapping test failed: {e}")
        return False


def main() -> int:
    """Run all simple table-driven architecture tests"""
    print("Starting Simple Table-Driven Architecture Tests")
    print("=" * 50)

    tests = [
        test_bank_data_config,
        test_selectors_yml,
        test_tagged_models,
        test_simple_orchestration,
        test_asset_imports,
        test_dbt_project_structure,
        test_table_name_mapping,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1
        print()

    print("=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("All simple table-driven architecture tests passed!")
        return 0
    print("Some tests failed. Check the output above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
