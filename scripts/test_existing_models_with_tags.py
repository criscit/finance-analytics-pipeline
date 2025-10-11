#!/usr/bin/env python3
"""
Test script for existing models with added tags.
Tests that we've properly tagged the existing models.
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_staging_model_tags() -> bool:
    """Test staging model has proper tags"""
    print("Testing staging model tags...")

    try:
        staging_file = Path("transform/dbt/models/staging/t_bank/stg_load_t_bank_transactions.sql")
        if staging_file.exists():
            with staging_file.open(encoding="utf-8") as f:
                content = f.read()

            if "tags=['t_bank', 'layer_stg', 'stg_tests']" in content:
                print("SUCCESS: Staging model has proper tags")
                return True
            print("ERROR: Staging model missing proper tags")
            return False
        print("ERROR: Staging model not found")
        return False

    except Exception as e:
        print(f"ERROR: Staging model test failed: {e}")
        return False


def test_core_model_tags() -> bool:
    """Test core model has proper tags"""
    print("Testing core model tags...")

    try:
        core_file = Path("transform/dbt/models/core/t_bank/core_load_t_bank_transactions.sql")
        if core_file.exists():
            with core_file.open(encoding="utf-8") as f:
                content = f.read()

            if "tags=['t_bank', 'layer_core', 'core_tests']" in content:
                print("SUCCESS: Core model has proper tags")
                return True
            print("ERROR: Core model missing proper tags")
            return False
        print("ERROR: Core model not found")
        return False

    except Exception as e:
        print(f"ERROR: Core model test failed: {e}")
        return False


def test_mart_model_tags() -> bool:
    """Test mart model has proper tags"""
    print("Testing mart model tags...")

    try:
        mart_file = Path("transform/dbt/models/marts/t_bank/mart_load_t_bank_transactions.sql")
        if mart_file.exists():
            with mart_file.open(encoding="utf-8") as f:
                content = f.read()

            if "tags=['t_bank', 'layer_mart', 'mart_tests']" in content:
                print("SUCCESS: Mart model has proper tags")
                return True
            print("ERROR: Mart model missing proper tags")
            return False
        print("ERROR: Mart model not found")
        return False

    except Exception as e:
        print(f"ERROR: Mart model test failed: {e}")
        return False


def test_integration_model_tags() -> bool:
    """Test integration model has proper tags"""
    print("Testing integration model tags...")

    try:
        integration_file = Path(
            "transform/dbt/models/integration_marts/imart_bind_bank_transactions.sql"
        )
        if integration_file.exists():
            with integration_file.open(encoding="utf-8") as f:
                content = f.read()

            if "tags=['t_bank', 'layer_integration', 'integration_tests']" in content:
                print("SUCCESS: Integration model has proper tags")
                return True
            print("ERROR: Integration model missing proper tags")
            return False
        print("ERROR: Integration model not found")
        return False

    except Exception as e:
        print(f"ERROR: Integration model test failed: {e}")
        return False


def test_selectors_include_integration() -> bool:
    """Test selectors include integration layer"""
    print("Testing selectors include integration...")

    try:
        selectors_file = Path("transform/dbt/selectors.yml")
        if selectors_file.exists():
            with selectors_file.open() as f:
                content = f.read()

            if "run_bank_integration" in content and "test_bank_integration" in content:
                print("SUCCESS: Selectors include integration layer")
                return True
            print("ERROR: Selectors missing integration layer")
            return False
        print("ERROR: Selectors file not found")
        return False

    except Exception as e:
        print(f"ERROR: Selectors test failed: {e}")
        return False


def test_bank_config_includes_integration() -> bool:
    """Test bank_config includes integration layer"""
    print("Testing bank_config includes integration...")

    try:
        config_file = Path("transform/dbt/seeds/bank_data_config.csv")
        if config_file.exists():
            with config_file.open() as f:
                content = f.read()

            if "integration_tests" in content and "layer_integration" in content:
                print("SUCCESS: Bank config includes integration layer")
                return True
            print("ERROR: Bank config missing integration layer")
            return False
        print("ERROR: Bank config file not found")
        return False

    except Exception as e:
        print(f"ERROR: Bank config test failed: {e}")
        return False


def main() -> int:
    """Run all existing models with tags tests"""
    print("Starting Existing Models with Tags Tests")
    print("=" * 50)

    tests = [
        test_staging_model_tags,
        test_core_model_tags,
        test_mart_model_tags,
        test_integration_model_tags,
        test_selectors_include_integration,
        test_bank_config_includes_integration,
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
        print("All existing models with tags tests passed!")
        return 0
    print("Some tests failed. Check the output above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
