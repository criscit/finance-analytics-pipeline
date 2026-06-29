"""Playwright browser lifecycle for a fetch run.

Either launches a persistent real-Chrome profile (the default) or attaches to an
already-running Chrome over CDP, then yields a ready page. Only a browser we
launched ourselves is closed on exit.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import Page, sync_playwright

from scripts.fetch.core import (
    CHROME_ARGS,
    CHROME_CHANNEL,
    DEFAULT_CDP_PORT,
    PAGE_TIMEOUT_MS,
    ensure_dir,
    logger,
)


@dataclass
class RunConfig:
    """Browser launch settings for one fetch run."""

    headless: bool
    profile_dir: Path
    cdp: str | None
    cdp_auto: bool


def _cdp_version_url(cdp_url: str) -> str:
    return f"{cdp_url.rstrip('/')}/json/version"


def _cdp_is_available(cdp_url: str) -> bool:
    try:
        with urllib.request.urlopen(_cdp_version_url(cdp_url), timeout=1):
            return True
    except (OSError, urllib.error.URLError):
        return False


def _resolve_chrome_executable() -> str:
    env_path = os.environ.get("FETCH_CHROME_PATH")
    if env_path:
        return env_path

    candidates = [
        Path(os.environ.get("PROGRAMFILES", ""))
        / "Google"
        / "Chrome"
        / "Application"
        / "chrome.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", ""))
        / "Google"
        / "Chrome"
        / "Application"
        / "chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", ""))
        / "Google"
        / "Chrome"
        / "Application"
        / "chrome.exe",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)

    for name in ("chrome", "google-chrome", "chromium", "chromium-browser"):
        found = shutil.which(name)
        if found:
            return found

    raise FileNotFoundError(
        "Chrome executable not found. Set FETCH_CHROME_PATH to the Chrome executable path."
    )


def _cdp_port(cdp_url: str) -> int:
    return urlparse(cdp_url).port or DEFAULT_CDP_PORT


def _ensure_cdp_chrome(cdp_url: str, profile_dir: Path) -> None:
    """Start Chrome for CDP if it is not already listening."""
    if _cdp_is_available(cdp_url):
        logger.info("Chrome CDP is already available: {}", cdp_url)
        return

    ensure_dir(profile_dir)
    chrome = _resolve_chrome_executable()
    port = _cdp_port(cdp_url)
    logger.info("Starting Chrome for CDP on port {} with profile {}", port, profile_dir)
    subprocess.Popen(
        [
            chrome,
            f"--remote-debugging-port={port}",
            f"--user-data-dir={profile_dir}",
            *CHROME_ARGS,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(30):
        if _cdp_is_available(cdp_url):
            return
        time.sleep(0.5)
    raise TimeoutError(f"Chrome did not expose CDP at {cdp_url}")


@contextlib.contextmanager
def browser_page(config: RunConfig) -> Iterator[Page]:
    """Yield a ready Page, launching a persistent Chrome profile or attaching over CDP."""
    if config.cdp and config.cdp_auto:
        _ensure_cdp_chrome(config.cdp, config.profile_dir)

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
