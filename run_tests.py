#!/usr/bin/env python3
"""Test runner script for the finance analytics pipeline."""

import subprocess
import sys
from pathlib import Path


def run_tests() -> int:
    """Run all tests in the project."""
    print("Running finance analytics pipeline tests...")

    # Test directories to run
    test_dirs = ["export/tests", "orchestration/tests", "transform/tests", "quality/tests"]

    all_passed = True

    for test_dir in test_dirs:
        if Path(test_dir).exists():
            print(f"\n{'='*50}")
            print(f"Running tests in {test_dir}")
            print(f"{'='*50}")

            try:
                result = subprocess.run(
                    ["poetry", "run", "pytest", test_dir, "-v", "--tb=short"],
                    capture_output=True,
                    text=True,
                    check=False,
                )

                print(result.stdout)
                if result.stderr:
                    print("STDERR:", result.stderr)

                if result.returncode != 0:
                    all_passed = False
                    print(f"❌ Tests in {test_dir} failed")
                else:
                    print(f"✅ Tests in {test_dir} passed")

            except Exception as e:
                print(f"❌ Error running tests in {test_dir}: {e}")
                all_passed = False
        else:
            print(f"⚠️  Test directory {test_dir} not found, skipping")

    print(f"\n{'='*50}")
    if all_passed:
        print("🎉 All tests passed!")
        return 0
    print("❌ Some tests failed!")
    return 1


if __name__ == "__main__":
    sys.exit(run_tests())
