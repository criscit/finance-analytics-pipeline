"""Playwright browser lifecycle for a fetch run.

Either launches a persistent real-Chrome profile (the default) or attaches to an
already-running Chrome over CDP, then yields a ready page. Only a browser we
launched ourselves is closed on exit.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

from scripts.fetch.core import CHROME_ARGS, CHROME_CHANNEL, PAGE_TIMEOUT_MS, ensure_dir, logger


@dataclass
class RunConfig:
    """Browser launch settings for one fetch run."""

    headless: bool
    profile_dir: Path
    cdp: str | None


@contextlib.contextmanager
def browser_page(config: RunConfig) -> Iterator[Page]:
    """Yield a ready Page, launching a persistent Chrome profile or attaching over CDP."""
    with sync_playwright() as playwright:
        launched_context = None
        if config.cdp:
            # Attach to YOUR already-running Chrome (started with --remote-debugging-port).
            # Reuses its real cookies/sessions/extensions; we never close it.
            logger.info("Connecting to existing Chrome over CDP: {}", config.cdp)
            browser = playwright.chromium.connect_over_cdp(config.cdp)
            context = (
                browser.contexts[0]
                if browser.contexts
                else browser.new_context(accept_downloads=True)
            )
        else:
            ensure_dir(config.profile_dir)
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(config.profile_dir),
                channel=CHROME_CHANNEL,
                headless=config.headless,
                accept_downloads=True,
                args=list(CHROME_ARGS),
            )
            launched_context = context
        context.set_default_timeout(PAGE_TIMEOUT_MS)
        page = context.pages[0] if context.pages else context.new_page()
        try:
            yield page
        finally:
            if launched_context is not None:
                launched_context.close()  # only close the browser we launched ourselves
