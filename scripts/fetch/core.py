"""Shared infrastructure for the source fetchers.

Holds the cross-source pieces: runtime dataclasses, generic page helpers, login
detection/gate, the incremental cutoff lookup and small parsing utilities. Each
source module under ``scripts/fetch/sources`` builds on these.
"""

from __future__ import annotations

import contextlib
import csv
import os
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv
from playwright.sync_api import Download, Locator, Page
from playwright.sync_api import Error as PlaywrightError

from src.logging_config import get_logger
from src.utils import get_max_transaction_datetimes_by_platform

logger = get_logger("fetch_sources")

# Repo root: scripts/fetch/core.py -> parents[2].
PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Canonical browser profile for fetch automation. Do not create alternate repo-local profiles
# like data/profiles or data/chrome_*; T-Bank login state is expected to live here.
DEFAULT_PROFILE_DIR = PROJECT_ROOT / "data" / "browser_profile"
# Finance-specific CDP port. Other local automation projects (e.g. crypto-wallet-analysis) use
# 9222; sharing it makes this fetcher attach to the WRONG Chrome (wrong profile, not logged in).
# Keep this unique so we always launch/reuse our own profile's Chrome.
DEFAULT_CDP_PORT = 9224
DEFAULT_CDP_URL = f"http://localhost:{DEFAULT_CDP_PORT}"
CHROME_CHANNEL = "chrome"
CHROME_ARGS: tuple[str, ...] = (
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-sync",
    "--disable-features=ChromeWhatsNewUI,SigninIntercept,IdentityInDiceWebSigninInterception",
)

LOGIN_POLL_SECONDS = 3
PAGE_TIMEOUT_MS = 60_000
ACTION_TIMEOUT_MS = 20_000
# Navigation: cap how long we wait for a page to load, and how many times we abort a stalled
# load (window.stop()) and reload before giving up and proceeding anyway. SPA pages (T-Bank)
# often never reach networkidle, so we wait for the "load" event, not full network silence.
NAV_TIMEOUT_MS = 45_000
LOAD_TIMEOUT_MS = 15_000
NAV_ATTEMPTS = 3

RU_MONTHS: dict[str, int] = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


# --------------------------------------------------------------------------------------
# Runtime dataclasses
# --------------------------------------------------------------------------------------
@dataclass
class SourceConfig:
    """Everything needed to fetch one source."""

    name: str
    platform_name: str  # must match the platform label used in Results/transactions.csv
    url: str
    out_subdir: tuple[str, ...]
    login_markers: tuple[str, ...]  # URL substrings that indicate a login/auth page
    fetch: Callable[[FetchCtx], Path | None]
    logged_in_url_markers: tuple[str, ...] = ()
    # CSS selector for an element present ONLY when NOT logged in (e.g. the login form/button).
    # Filled in from real page HTML; when set it takes priority over the URL heuristic.
    auth_selector: str = ""


@dataclass
class FetchCtx:
    """Per-source runtime context passed to a fetch routine."""

    page: Page
    source: SourceConfig
    cutoff: datetime | None
    out_dir: Path


# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------
def stamp() -> str:
    """Filesystem-safe timestamp for generated filenames."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_dir(path: Path) -> None:
    """Create a directory (and parents) if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)


def settle(page: Page, timeout_ms: int = LOAD_TIMEOUT_MS) -> None:
    """Best-effort wait for the network to go idle (short; SPAs may never fully settle)."""
    with contextlib.suppress(Exception):
        page.wait_for_load_state("networkidle", timeout=timeout_ms)


def goto(page: Page, url: str, *, attempts: int = NAV_ATTEMPTS) -> None:
    """Navigate to ``url``, waiting for the load event.

    If a load stalls past ``LOAD_TIMEOUT_MS`` (common on SPAs that keep connections open), abort
    the in-flight load with ``window.stop()`` and reload, up to ``attempts`` times. After the last
    attempt we proceed regardless so the caller's own waits (e.g. an expected XHR) can run.
    """
    for attempt in range(1, attempts + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            page.wait_for_load_state("load", timeout=LOAD_TIMEOUT_MS)
            return
        except PlaywrightError as exc:
            logger.warning(
                "[nav] {}/{}: {} stalled loading {}; stopping and reloading.",
                attempt,
                attempts,
                type(exc).__name__,
                url,
            )
            with contextlib.suppress(Exception):
                page.evaluate("window.stop()")  # abort the stuck in-flight load before retrying
    logger.warning(
        "[nav] Proceeding despite incomplete load of {} after {} attempts.", url, attempts
    )


def _url_has_none(page: Page, markers: Sequence[str]) -> bool:
    url = page.url.lower()
    return not any(marker in url for marker in markers)


def text_of(scope: Locator, selector: str) -> str:
    """First matching element's trimmed inner text, or '' when absent."""
    if not selector:
        return ""
    loc = scope.locator(selector)
    if loc.count() == 0:
        return ""
    return (loc.first.inner_text() or "").strip()


def parse_amount(text: str) -> str:
    """Normalise a money string like '1 405,00 ₽' into '1405.00' (sign preserved)."""
    if not text:
        return ""
    text = text.replace("−", "-")
    cleaned = re.sub(r"[^\d,.\-]", "", text)
    return cleaned.replace(",", ".")


def amount_key(amount: str) -> str:
    """Absolute-value key used to match a wallet debit against a receipt."""
    return amount.lstrip("-")


def parse_ru_date(text: str) -> datetime | None:
    """Parse a Russian date like '12 января 2026' into a datetime (date only)."""
    if not text:
        return None
    match = re.search(r"(\d{1,2})\s+([а-яё]+)\s+(\d{4})", text.lower())
    if not match:
        return None
    day, month_name, year = match.groups()
    month = RU_MONTHS.get(month_name)
    if not month:
        return None
    with contextlib.suppress(ValueError):
        return datetime(int(year), month, int(day))
    return None


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    """Write a list of dict rows to a UTF-8 CSV (header from the first row's keys)."""
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _read_csv_rows(path: Path) -> list[list[str]]:
    """Read rows from a CSV file, excluding its header."""
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
        reader = csv.reader(handle, dialect)
        next(reader, None)
        return list(reader)


def datetime_to_epoch_ms(value: datetime) -> int:
    """Convert a timezone-aware or local timestamp to epoch milliseconds."""
    return int(value.timestamp() * 1000)


def floor_datetime_to_day(value: datetime) -> datetime:
    """Round a cutoff down to the start of its calendar day."""
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def filename_from_content_disposition(value: str | None, fallback: str) -> str:
    """Extract a safe filename from a Content-Disposition header."""
    if not value:
        return fallback
    match = re.search(r"filename\*=UTF-8''([^;]+)", value, flags=re.IGNORECASE)
    if match:
        name = unquote(match.group(1))
    else:
        match = re.search(r'filename="?([^";]+)', value, flags=re.IGNORECASE)
        name = match.group(1) if match else fallback
    return name.replace("/", "-").replace("\\", "-").replace(":", "-")


def dump_discovery(page: Page, out_dir: Path, name: str) -> None:
    """Save the current page HTML + screenshot so selectors can be discovered later."""
    ensure_dir(out_dir)
    marker = stamp()
    with contextlib.suppress(Exception):
        (out_dir / f"_discovery_{name}_{marker}.html").write_text(page.content(), encoding="utf-8")
    with contextlib.suppress(Exception):
        page.screenshot(path=str(out_dir / f"_discovery_{name}_{marker}.png"), full_page=True)


def try_download(page: Page, selectors: Sequence[str]) -> Download | None:
    """Click the first matching control that produces a download; return it (or None)."""
    for selector in selectors:
        loc = page.locator(selector)
        if loc.count() == 0:
            continue
        try:
            with page.expect_download(timeout=ACTION_TIMEOUT_MS) as info:
                loc.first.click()
            return info.value
        except Exception:  # selector may be wrong / no download — try the next candidate
            continue
    return None


# --------------------------------------------------------------------------------------
# Login detection / gate
# --------------------------------------------------------------------------------------
def is_logged_in(page: Page, source: SourceConfig) -> bool:
    """Not logged in if the URL looks like a login page or a known auth element is visible."""
    url = page.url.lower()
    if any(marker.lower() in url for marker in source.logged_in_url_markers):
        return True
    if not _url_has_none(page, source.login_markers):
        return False
    if not source.auth_selector:
        return True

    auth_locator = page.locator(source.auth_selector)
    for index in range(auth_locator.count()):
        with contextlib.suppress(Exception):
            if auth_locator.nth(index).is_visible(timeout=1_000):
                return False
    return True


def ensure_logged_in(page: Page, source: SourceConfig) -> None:
    """Open the source URL and block until the session is logged in."""
    current_host = urlparse(page.url).netloc
    source_host = urlparse(source.url).netloc
    if current_host != source_host or not is_logged_in(page, source):
        goto(page, source.url)
    if is_logged_in(page, source):
        logger.info("[{}] Already logged in.", source.name)
        return
    logger.warning(
        "[{}] Not logged in. Please log in in the browser window; "
        "waiting until login is detected (Ctrl+C to abort).",
        source.name,
    )
    while not is_logged_in(page, source):
        time.sleep(LOGIN_POLL_SECONDS)
    logger.success("[{}] Login detected. Continuing.", source.name)


# --------------------------------------------------------------------------------------
# Paths / incremental cutoff
# --------------------------------------------------------------------------------------
def resolve_finance_root() -> Path:
    """Resolve FINANCE_DIR_HOST from .env."""
    load_dotenv(PROJECT_ROOT / ".env")
    raw = os.environ.get("FINANCE_DIR_HOST", "data/finance")
    base = Path(raw)
    if not base.is_absolute():
        base = (PROJECT_ROOT / base).resolve()
    return base


def resolve_to_parse_root() -> Path:
    """Resolve the host 'To Parse' folder from FINANCE_DIR_HOST in .env."""
    return resolve_finance_root() / "To Parse"


def get_results_csv_cutoff(platform_name: str, finance_root: Path) -> datetime | None:
    """Latest transaction timestamp for a platform from Results/transactions.csv."""
    results_path = finance_root / "Results" / "transactions.csv"
    rows = _read_csv_rows(results_path)
    if not rows:
        return None
    return get_max_transaction_datetimes_by_platform(rows).get(platform_name)


def get_source_cutoff(platform_name: str, finance_root: Path) -> datetime | None:
    """Resolve the incremental cutoff for a source."""
    return get_results_csv_cutoff(platform_name, finance_root)
