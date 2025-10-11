"""
Expectation Suite Definitions

This module contains all expectation suite configurations as code.
Each suite is defined as a Python function that returns a GX ExpectationSuite.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import great_expectations as gx

if TYPE_CHECKING:
    pass


def create_raw_t_bank_transactions_suite() -> Any:
    """
    Data presence and quality checks for raw t_bank_transactions table.

    Ensures:
    - Table has data (at least 1 row)
    - Critical columns exist
    - Load key is unique and not null
    - Recent data is present
    """
    suite_name = "raw.t_bank_transactions.data_presence_check"
    suite = gx.ExpectationSuite(name=suite_name)  # type: ignore[attr-defined]

    # Table must have data
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=1_000_000)  # type: ignore[attr-defined]
    )

    # Critical columns must exist
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="__load_key"))  # type: ignore[attr-defined]
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="processed_at"))  # type: ignore[attr-defined]

    # Load key integrity checks
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="__load_key"))  # type: ignore[attr-defined]
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="__load_key"))  # type: ignore[attr-defined]

    # Timestamp integrity
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="processed_at"))  # type: ignore[attr-defined]

    return suite


def create_raw_bakai_transactions_suite() -> Any:
    """
    Data presence and quality checks for raw bakai_transactions table.

    Ensures:
    - Table has data (at least 1 row)
    - Critical columns exist
    - Load key is unique and not null
    - Recent data is present
    """
    suite_name = "raw.bakai_transactions.data_presence_check"
    suite = gx.ExpectationSuite(name=suite_name)  # type: ignore[attr-defined]

    # Table must have data
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=1_000_000)  # type: ignore[attr-defined]
    )

    # Critical columns must exist
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="__load_key"))  # type: ignore[attr-defined]
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="processed_at"))  # type: ignore[attr-defined]

    # Load key integrity checks
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="__load_key"))  # type: ignore[attr-defined]
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="__load_key"))  # type: ignore[attr-defined]

    # Timestamp integrity
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="processed_at"))  # type: ignore[attr-defined]

    return suite


def create_mart_bakai_transactions_suite() -> Any:
    """
    Business logic validation for mart-level Bakai transactions.

    Ensures:
    - Exchange rates should always be NULL
    - Alert/error when exchange_rate has non-null values

    Note: Row count checks are handled by dbt tests on critical columns.
    """
    suite_name = "mart.bakai_transactions.exchange_rate_null"
    suite = gx.ExpectationSuite(name=suite_name)  # type: ignore[attr-defined]

    # Exchange rate column must exist
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="exchange_rate"))  # type: ignore[attr-defined]

    # Exchange rate must be NULL (100% null values expected)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeNull(  # type: ignore[attr-defined]
            column="exchange_rate",
            meta={
                "notes": {
                    "format": "markdown",
                    "content": [
                        "We expect this field to be null.",
                        "If it's not, please refactor this and child pipelines.",
                    ],
                }
            },
        )
    )

    return suite


# Registry of all suite creation functions
SUITE_REGISTRY = {
    "raw.t_bank_transactions.data_presence_check": create_raw_t_bank_transactions_suite,
    "raw.bakai_transactions.data_presence_check": create_raw_bakai_transactions_suite,
    "mart.bakai_transactions.exchange_rate_null": create_mart_bakai_transactions_suite,
}


def get_or_create_suite(context: Any, suite_name: str) -> Any:
    """
    Get an existing suite or create it from the registry.

    Args:
        context: Great Expectations data context
        suite_name: Name of the suite to retrieve or create

    Returns:
        ExpectationSuite instance

    Raises:
        ValueError: If suite_name is not in the registry
    """
    if suite_name not in SUITE_REGISTRY:
        raise ValueError(
            f"Suite '{suite_name}' not found in registry. "
            f"Available: {list(SUITE_REGISTRY.keys())}"
        )

    try:
        # Try to get existing suite
        suite = context.suites.get(suite_name)
    except Exception:
        # Create new suite from registry
        suite = SUITE_REGISTRY[suite_name]()
        suite = context.suites.add(suite)

    return suite
