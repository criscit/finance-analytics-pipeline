#!/usr/bin/env python3
"""Fetch transactions from T-Bank, Ozon and Wildberries into the pipeline's "To Parse" folder.

The script drives a headed Playwright browser that uses a *persistent profile*, so your login is
remembered between runs. Normal runs just open each source URL and download with no interaction.
If a source is not logged in, the script pauses and waits until you log in manually in the opened
browser window, then continues automatically.

Downloaded / scraped files are written into the contract-driven ``To Parse`` directories that the
existing ingestion pipeline already reads, so nothing downstream needs to change.

Per source:

* **T-Bank** and **Ozon Bank** (``finance.ozon.ru``): open the operations page and try to trigger a
  file export/download.
* **Wildberries**: read the wallet history (``/lk/mywallet?type=history``), keep debits of type
  "Оплата по СБП", then match each amount against the receipts section (``/lk/receipts/get``) to
  recover the product name (each receipt opens in a new tab). The result is written to a CSV.

The exact per-site selectors must be confirmed against the live, authenticated pages. When a routine
cannot find what it expects, it saves a ``_discovery_*`` HTML + screenshot dump into the target
folder so the selectors can be filled in quickly.

Usage::

    poetry run python scripts/fetch_sources.py                 # all sources, headed
    poetry run python scripts/fetch_sources.py --sources ozon  # one source
    poetry run python scripts/fetch_sources.py --headless      # no UI (cannot log in)
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import csv
import os
import re
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlencode, urlparse

from dotenv import load_dotenv
from playwright.sync_api import Download, Locator, Page, sync_playwright

from src.logging_config import get_logger
from src.utils import get_max_transaction_datetimes_by_platform

logger = get_logger("fetch_sources")

# --------------------------------------------------------------------------------------
# Paths / constants
# --------------------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Canonical browser profile for fetch automation. Do not create alternate repo-local profiles
# like data/profiles or data/chrome_*; T-Bank login state is expected to live here.
DEFAULT_PROFILE_DIR = PROJECT_ROOT / "data" / "browser_profile"
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
T_BANK_EXPORT_API = (
    "https://www.tbank.ru/mybank/api/operations/timeline/public/legacy/v1/export_operations"
)
T_BANK_EXPORT_FORMAT = "csv"
T_BANK_PLATFORM_NAME = "T Bank"
# Path fragment of the authenticated operations XHR the SPA fires once loaded; it carries the
# sessionid we reuse for the export call. We wait for it instead of reading resources once.
T_BANK_TIMELINE_MARKER = "/operations/timeline/public/legacy/v1/"

# Source entry URLs. Adjust here if a site changes its layout.
# NOTE: T-Bank operations page expects you to choose a period, then click "Скачать" (CSV).
# The period picker selectors must be confirmed live; the download click is handled via
# EXPORT_SELECTORS ("скачать"). Until the period step is wired, it exports the default range.
URL_T_BANK = "https://www.tbank.ru/mybank/operations/"
URL_OZON = "https://finance.ozon.ru/lk/operations"
URL_WB_WALLET = "https://www.wildberries.ru/lk/mywallet?type=history"
URL_WB_RECEIPTS = "https://www.wildberries.ru/lk/receipts/get"

# Candidate controls that trigger a statement/transactions export on a bank-like page.
# Playwright matches these in order; the first that yields a download wins. VERIFY per site.
EXPORT_SELECTORS: tuple[str, ...] = (
    "text=/выписк/i",
    "text=/выгруз/i",
    "text=/экспорт/i",
    "text=/скачать/i",
    "[data-test-id*='export']",
    "[data-test-id*='download']",
)

# Wildberries selectors (VERIFY all against the live, logged-in DOM).
WB_SBP_MARKER = "СБП"
WB_WALLET_ROW = "[class*='history'] [class*='item'], [class*='wallet'] tr"
WB_WALLET_TYPE = "[class*='type'], [class*='operation'], [class*='descr']"
WB_WALLET_DATE = "[class*='date']"
WB_WALLET_AMOUNT = "[class*='sum'], [class*='amount']"
WB_RECEIPT_ROW = "[class*='receipt'] [class*='item'], [class*='receipt'] tr"
WB_RECEIPT_AMOUNT = "[class*='sum'], [class*='amount']"
WB_RECEIPT_ITEM_NAME = "[class*='name'], [class*='product'], [class*='good']"

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
# Source configuration
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


@dataclass
class RunConfig:
    """Browser launch settings for one fetch run."""

    headless: bool
    profile_dir: Path
    cdp: str | None


# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------
def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _settle(page: Page) -> None:
    """Best-effort wait for the page to stop loading."""
    with contextlib.suppress(Exception):
        page.wait_for_load_state("networkidle", timeout=PAGE_TIMEOUT_MS)


def _url_has_none(page: Page, markers: Sequence[str]) -> bool:
    url = page.url.lower()
    return not any(marker in url for marker in markers)


def _text(scope: Locator, selector: str) -> str:
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
    cleaned = re.sub(r"[^\d,.\-]", "", text)
    return cleaned.replace(",", ".")


def _amount_key(amount: str) -> str:
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


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    _ensure_dir(path.parent)
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


def _datetime_to_epoch_ms(value: datetime) -> int:
    """Convert a timezone-aware or local timestamp to epoch milliseconds."""
    return int(value.timestamp() * 1000)


def _floor_datetime_to_day(value: datetime) -> datetime:
    """Round a cutoff down to the start of its calendar day."""
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def _filename_from_content_disposition(value: str | None, fallback: str) -> str:
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


def _dump_discovery(page: Page, out_dir: Path, name: str) -> None:
    """Save the current page HTML + screenshot so selectors can be discovered later."""
    _ensure_dir(out_dir)
    stamp = _stamp()
    with contextlib.suppress(Exception):
        (out_dir / f"_discovery_{name}_{stamp}.html").write_text(page.content(), encoding="utf-8")
    with contextlib.suppress(Exception):
        page.screenshot(path=str(out_dir / f"_discovery_{name}_{stamp}.png"), full_page=True)


# --------------------------------------------------------------------------------------
# Login detection
# --------------------------------------------------------------------------------------
def is_logged_in(page: Page, source: SourceConfig) -> bool:
    """Not logged in if the URL looks like a login page or a known auth element is visible."""
    if not _url_has_none(page, source.login_markers):
        return False
    return not (source.auth_selector and page.locator(source.auth_selector).count() > 0)


# --------------------------------------------------------------------------------------
# Fetch routine: bank-like export (T-Bank, Ozon Bank)
# --------------------------------------------------------------------------------------
def _try_download(page: Page, selectors: Sequence[str]) -> Download | None:
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


def fetch_export(ctx: FetchCtx) -> Path | None:
    """Statement-style source: open the operations page and try to export a file."""
    ctx.page.goto(ctx.source.url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    _settle(ctx.page)
    download = _try_download(ctx.page, EXPORT_SELECTORS)
    if download is None:
        _dump_discovery(ctx.page, ctx.out_dir, ctx.source.name)
        logger.warning(
            "[{}] No export control matched yet — saved a discovery dump in {} "
            "so the export selector can be confirmed.",
            ctx.source.name,
            ctx.out_dir,
        )
        return None
    suggested = download.suggested_filename or f"{ctx.source.name}.csv"
    target = ctx.out_dir / f"{ctx.source.name}_{_stamp()}_{suggested}"
    _ensure_dir(ctx.out_dir)
    download.save_as(str(target))
    return target


def _t_bank_seed_url(page: Page, timeout_ms: int = PAGE_TIMEOUT_MS) -> str:
    """Wait until the operations SPA has issued an authenticated timeline XHR; return its URL.

    The operations page is a single-page app, so the request that carries the ``sessionid`` we
    reuse for the export only appears after the framework boots and fetches the timeline. Polling
    via ``wait_for_function`` avoids racing a not-yet-loaded page (the old code read resources once
    and failed immediately).
    """
    handle = page.wait_for_function(
        """(marker) => {
            const match = performance.getEntriesByType('resource')
                .map((entry) => entry.name)
                .reverse()
                .find((name) => name.includes(marker) && name.includes('sessionid='));
            return match || null;
        }""",
        arg=T_BANK_TIMELINE_MARKER,
        timeout=timeout_ms,
    )
    seed_url = handle.json_value()
    if not seed_url:
        raise RuntimeError("Could not find an authenticated T-Bank operations API request")
    return str(seed_url)


def _t_bank_export_params(page: Page, cutoff: datetime | None) -> dict[str, str]:
    """Build authenticated T-Bank export params from page resources and the CSV cutoff."""
    seed_url = _t_bank_seed_url(page)
    qs = parse_qs(urlparse(seed_url).query)
    session_ids = qs.get("sessionid")
    if not session_ids:
        raise RuntimeError("Could not recover T-Bank sessionid from page resources")

    start_ms = _datetime_to_epoch_ms(_floor_datetime_to_day(cutoff)) if cutoff else 0
    now_ms = _datetime_to_epoch_ms(datetime.now().astimezone())
    return {
        "appName": qs.get("appName", ["supreme"])[0],
        "appVersion": qs.get("appVersion", ["0.0.1"])[0],
        "origin": qs.get("origin", ["web,ib5,platform"])[0],
        "sessionid": session_ids[0],
        "start": str(start_ms),
        "end": str(now_ms),
        "format": T_BANK_EXPORT_FORMAT,
    }


def fetch_t_bank(ctx: FetchCtx) -> Path | None:
    """T-Bank: call the authenticated export API directly and save the CSV response."""
    ctx.page.goto(ctx.source.url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    _settle(ctx.page)

    params = _t_bank_export_params(ctx.page, ctx.cutoff)
    export_url = f"{T_BANK_EXPORT_API}?{urlencode(params)}"
    result = ctx.page.evaluate(
        """async (url) => {
            const response = await fetch(url, { credentials: 'include' });
            const headers = {};
            response.headers.forEach((value, key) => { headers[key] = value; });
            const buffer = await response.arrayBuffer();
            const bytes = new Uint8Array(buffer);
            let binary = '';
            const chunk = 0x8000;
            for (let offset = 0; offset < bytes.length; offset += chunk) {
                binary += String.fromCharCode(...bytes.subarray(offset, offset + chunk));
            }
            return {
                ok: response.ok,
                status: response.status,
                statusText: response.statusText,
                headers,
                byteLength: bytes.length,
                base64: btoa(binary),
                preview: new TextDecoder('utf-8', { fatal: false }).decode(bytes.slice(0, 300)),
            };
        }""",
        export_url,
    )
    if not result["ok"] or result["byteLength"] == 0:
        _dump_discovery(ctx.page, ctx.out_dir, ctx.source.name)
        raise RuntimeError(
            "T-Bank export failed "
            f"status={result['status']} {result['statusText']} preview={result['preview']!r}"
        )

    suggested = _filename_from_content_disposition(
        result["headers"].get("content-disposition"),
        f"t_bank_operations_{_stamp()}.csv",
    )
    if not suggested.lower().endswith(".csv"):
        suggested = f"{suggested}.csv"
    target = ctx.out_dir / suggested
    _ensure_dir(ctx.out_dir)
    target.write_bytes(base64.b64decode(result["base64"]))
    return target


# --------------------------------------------------------------------------------------
# Fetch routine: Wildberries (wallet "Оплата по СБП" debits + product name from receipts)
# --------------------------------------------------------------------------------------
def _wb_collect_sbp_debits(ctx: FetchCtx) -> list[dict[str, str]]:
    page = ctx.page
    page.goto(URL_WB_WALLET, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    _settle(page)
    rows = page.locator(WB_WALLET_ROW)
    count = rows.count()
    if count == 0:
        _dump_discovery(page, ctx.out_dir, "wb_wallet")
        logger.warning("[wb] No wallet rows matched '{}' — saved discovery dump.", WB_WALLET_ROW)
        return []

    debits: list[dict[str, str]] = []
    for index in range(count):
        row = rows.nth(index)
        type_text = _text(row, WB_WALLET_TYPE)
        if WB_SBP_MARKER not in type_text:
            continue
        date_text = _text(row, WB_WALLET_DATE)
        debit_dt = parse_ru_date(date_text)
        if ctx.cutoff and debit_dt and debit_dt <= ctx.cutoff:
            continue
        debits.append(
            {
                "date": date_text,
                "amount_rub": parse_amount(_text(row, WB_WALLET_AMOUNT)),
                "type": type_text,
            }
        )
    return debits


def _wb_open_receipt_name(page: Page, receipt: Locator) -> str:
    """Click a receipt (opens a new tab) and read the product name from it."""
    try:
        with page.context.expect_page(timeout=ACTION_TIMEOUT_MS) as popup_info:
            receipt.click()
    except Exception:
        return ""
    popup = popup_info.value
    _settle(popup)
    name = _text(popup.locator("body"), WB_RECEIPT_ITEM_NAME)
    with contextlib.suppress(Exception):
        popup.close()
    return name


def _wb_receipt_names_by_amount(ctx: FetchCtx) -> dict[str, str]:
    page = ctx.page
    page.goto(URL_WB_RECEIPTS, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    _settle(page)
    receipts = page.locator(WB_RECEIPT_ROW)
    count = receipts.count()
    if count == 0:
        _dump_discovery(page, ctx.out_dir, "wb_receipts")
        logger.warning("[wb] No receipts matched '{}' — saved discovery dump.", WB_RECEIPT_ROW)
        return {}

    mapping: dict[str, str] = {}
    for index in range(count):
        receipt = receipts.nth(index)
        amount_key = _amount_key(parse_amount(_text(receipt, WB_RECEIPT_AMOUNT)))
        if not amount_key:
            continue
        name = _wb_open_receipt_name(page, receipt)
        if name:
            mapping[amount_key] = name
    return mapping


def fetch_wb(ctx: FetchCtx) -> Path | None:
    """Wildberries: SBP debits from the wallet, enriched with product names from receipts."""
    debits = _wb_collect_sbp_debits(ctx)
    if not debits:
        logger.info("[wb] No new 'Оплата по СБП' debits after cutoff {}.", ctx.cutoff)
        return None

    names = _wb_receipt_names_by_amount(ctx)
    for debit in debits:
        debit["item"] = names.get(_amount_key(debit["amount_rub"]), "")

    target = ctx.out_dir / f"wb_{_stamp()}.csv"
    _write_csv(target, debits)
    return target


# --------------------------------------------------------------------------------------
# Source registry
# --------------------------------------------------------------------------------------
def build_sources() -> dict[str, SourceConfig]:
    return {
        "t_bank": SourceConfig(
            name="t_bank",
            platform_name="T Bank",
            url=URL_T_BANK,
            out_subdir=("Bank", "T-Bank", "transactions"),
            # observed: logged-out -> redirect to id.tbank.ru/auth/...
            login_markers=("id.tbank.ru", "id.tinkoff.ru", "/auth", "/login"),
            fetch=fetch_t_bank,
        ),
        "ozon": SourceConfig(
            name="ozon",
            platform_name="Ozon",
            url=URL_OZON,
            out_subdir=("Bank", "Ozon", "transactions"),
            # observed: logged-out stays on finance.ozon.ru/ and shows a "Войти" button
            login_markers=("id.ozon.ru", "/signin"),
            auth_selector='[data-testid="obi-test-id-borderless-button"]:has-text("Войти")',
            fetch=fetch_export,
        ),
        "wb": SourceConfig(
            name="wb",
            platform_name="Wildberries",
            url=URL_WB_WALLET,
            out_subdir=("Marketplace", "Wildberries", "orders"),
            # observed: logged-out -> redirect to id.wb.ru/login/...
            login_markers=("id.wb.ru", "/login"),
            fetch=fetch_wb,
        ),
    }


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


# --------------------------------------------------------------------------------------
# Login gate
# --------------------------------------------------------------------------------------
def ensure_logged_in(page: Page, source: SourceConfig) -> None:
    page.goto(source.url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    _settle(page)
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
# Driver
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


def run(selected: list[str], config: RunConfig) -> int:
    sources = build_sources()
    finance_root = resolve_finance_root()
    to_parse_root = finance_root / "To Parse"
    logger.info("Target 'To Parse' root: {}", to_parse_root)

    failures = 0
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
            _ensure_dir(config.profile_dir)
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

        for key in selected:
            source = sources[key]
            out_dir = to_parse_root.joinpath(*source.out_subdir)
            _ensure_dir(out_dir)
            cutoff = get_source_cutoff(source.platform_name, finance_root)
            logger.info("=== {} (cutoff: {}) ===", source.name, cutoff or "full history")
            try:
                ensure_logged_in(page, source)
                result = source.fetch(FetchCtx(page, source, cutoff, out_dir))
            except Exception:  # keep going with the other sources
                failures += 1
                logger.exception("[{}] ERROR", source.name)
                continue
            if result is None:
                logger.warning("[{}] No file produced.", source.name)
            else:
                logger.success("[{}] Saved -> {}", source.name, result)

        if launched_context is not None:
            launched_context.close()  # only close the browser we launched ourselves
    return failures


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sources",
        default="all",
        help="Comma-separated subset of: t_bank,ozon,wb (default: all)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run without a visible browser (cannot complete manual logins).",
    )
    parser.add_argument(
        "--profile-dir",
        default=str(DEFAULT_PROFILE_DIR),
        help=(
            "Persistent real-Chrome profile for fetch automation "
            f"(default: {DEFAULT_PROFILE_DIR}). Keep T-Bank login here."
        ),
    )
    parser.add_argument(
        "--cdp",
        default=os.environ.get("FETCH_CDP_URL", ""),
        help=(
            "Attach to an already-running Chrome over CDP instead of launching a profile, "
            "e.g. http://localhost:9222 (start Chrome with --remote-debugging-port=9222)."
        ),
    )
    return parser.parse_args(argv)


def _force_utf8_stdio() -> None:
    """Print UTF-8 so Cyrillic output/filenames don't crash on a cp1252 Windows console."""
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(AttributeError):
            stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]


def main(argv: Sequence[str] | None = None) -> int:
    _force_utf8_stdio()
    args = parse_args(argv)
    known = list(build_sources().keys())
    selected = known if args.sources == "all" else [s.strip() for s in args.sources.split(",")]
    unknown = [s for s in selected if s not in known]
    if unknown:
        logger.error("Unknown source(s): {}. Known: {}", unknown, known)
        return 2
    return run(
        selected,
        RunConfig(
            headless=args.headless,
            profile_dir=Path(args.profile_dir),
            cdp=args.cdp or None,
        ),
    )


if __name__ == "__main__":
    raise SystemExit(main())
