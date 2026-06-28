"""Playwright transaction fetchers (T-Bank, Ozon, Wildberries).

Run via the package entry point::

    poetry run python -m scripts.fetch                 # all sources, headed
    poetry run python -m scripts.fetch --sources ozon  # one source
"""

from __future__ import annotations
