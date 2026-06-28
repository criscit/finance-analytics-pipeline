"""Source registry: maps each source key to its SourceConfig."""

from __future__ import annotations

from scripts.fetch.core import SourceConfig
from scripts.fetch.sources import ozon, tbank, wb


def build_sources() -> dict[str, SourceConfig]:
    """Return the supported sources keyed by their CLI name."""
    return {source.name: source for source in (tbank.SOURCE, ozon.SOURCE, wb.SOURCE)}
