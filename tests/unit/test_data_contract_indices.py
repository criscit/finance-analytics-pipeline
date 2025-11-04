from __future__ import annotations

from pathlib import Path

import pytest
import yaml

CONTRACT_DIR = Path("data/contracts")


def _contract_paths() -> list[Path]:
    return sorted(CONTRACT_DIR.glob("*.yaml"))


@pytest.mark.unit
@pytest.mark.parametrize("contract_path", _contract_paths(), ids=lambda p: p.stem)
def test_contract_source_indices_are_zero_based_and_sequential(contract_path: Path) -> None:
    data = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    columns = data.get("columns", [])
    indices = [
        col.get("source_column_index")
        for col in columns
        if col.get("source_column_index") is not None
    ]

    if not indices:
        pytest.skip("Contract has no indexed columns")

    assert min(indices) == 0, "Expected zero-based indices (min should be 0)"
    unique_sorted = sorted(set(indices))
    assert unique_sorted == list(
        range(len(unique_sorted))
    ), "Indices must be contiguous and zero-based"
