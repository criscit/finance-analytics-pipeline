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
import contextlib
import csv
import os
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from playwright.sync_api import Download, Locator, Page, sync_playwright

# --------------------------------------------------------------------------------------
# Paths / constants
# --------------------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PROFILE_DIR = PROJECT_ROOT / "data" / "browser_profile"
DEFAULT_WAREHOUSE = PROJECT_ROOT / "data" / "warehouse" / "analytics.duckdb"

# View the marts unify into; used to derive the per-source incremental cutoff.
EXPORT_TABLE = "prod_imart.view_transactions"

LOGIN_POLL_SECONDS = 3
PAGE_TIMEOUT_MS = 60_000
ACTION_TIMEOUT_MS = 20_000

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
    platform_name: str  # must match the platform label used in EXPORT_TABLE for the cutoff
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
        print(
            f"  [{ctx.source.name}] No export control matched yet — saved a discovery dump "
            f"in {ctx.out_dir} so the export selector can be confirmed."
        )
        return None
    suggested = download.suggested_filename or f"{ctx.source.name}.csv"
    target = ctx.out_dir / f"{ctx.source.name}_{_stamp()}_{suggested}"
    _ensure_dir(ctx.out_dir)
    download.save_as(str(target))
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
        print(f"  [wb] No wallet rows matched '{WB_WALLET_ROW}' — saved discovery dump.")
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
        print(f"  [wb] No receipts matched '{WB_RECEIPT_ROW}' — saved discovery dump.")
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
        print(f"  [wb] No new 'Оплата по СБП' debits after cutoff {ctx.cutoff}.")
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
            fetch=fetch_export,
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


# --------------------------------------------------------------------------------------
# Cutoff
# --------------------------------------------------------------------------------------
def get_cutoff(platform_name: str, warehouse: Path) -> datetime | None:
    """Latest transaction timestamp already stored for a platform (None on first run / errors)."""
    if not warehouse.exists():
        return None
    try:
        con = duckdb.connect(str(warehouse), read_only=True)
    except (duckdb.Error, OSError):
        return None
    try:
        row = con.execute(
            f"select max(transacted_at) from {EXPORT_TABLE} where platform_name = ?",
            [platform_name],
        ).fetchone()
    except duckdb.Error:
        return None
    finally:
        con.close()
    if row is None or row[0] is None:
        return None
    return row[0] if isinstance(row[0], datetime) else None


# --------------------------------------------------------------------------------------
# Login gate
# --------------------------------------------------------------------------------------
def ensure_logged_in(page: Page, source: SourceConfig) -> None:
    page.goto(source.url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    _settle(page)
    if is_logged_in(page, source):
        print(f"  [{source.name}] Already logged in.")
        return
    print(f"\n>>> [{source.name}] Not logged in. Please log in in the browser window.")
    print(">>> Waiting until login is detected... (Ctrl+C to abort)")
    while not is_logged_in(page, source):
        time.sleep(LOGIN_POLL_SECONDS)
    print(f">>> [{source.name}] Login detected. Continuing.\n")


# --------------------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------------------
def resolve_to_parse_root() -> Path:
    """Resolve the host 'To Parse' folder from FINANCE_DIR_HOST in .env."""
    load_dotenv(PROJECT_ROOT / ".env")
    raw = os.environ.get("FINANCE_DIR_HOST", "data/finance")
    base = Path(raw)
    if not base.is_absolute():
        base = (PROJECT_ROOT / base).resolve()
    return base / "To Parse"


def run(
    selected: list[str],
    *,
    headless: bool,
    profile_dir: Path,
    warehouse: Path,
    cdp: str | None,
) -> int:
    sources = build_sources()
    to_parse_root = resolve_to_parse_root()
    print(f"Target 'To Parse' root: {to_parse_root}")

    failures = 0
    with sync_playwright() as playwright:
        launched_context = None
        if cdp:
            # Attach to YOUR already-running Chrome (started with --remote-debugging-port).
            # Reuses its real cookies/sessions/extensions; we never close it.
            print(f"Connecting to existing Chrome over CDP: {cdp}")
            browser = playwright.chromium.connect_over_cdp(cdp)
            context = (
                browser.contexts[0]
                if browser.contexts
                else browser.new_context(accept_downloads=True)
            )
        else:
            _ensure_dir(profile_dir)
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                headless=headless,
                accept_downloads=True,
            )
            launched_context = context
        context.set_default_timeout(PAGE_TIMEOUT_MS)
        page = context.pages[0] if context.pages else context.new_page()

        for key in selected:
            source = sources[key]
            out_dir = to_parse_root.joinpath(*source.out_subdir)
            _ensure_dir(out_dir)
            cutoff = get_cutoff(source.platform_name, warehouse)
            print(f"\n=== {source.name} (cutoff: {cutoff or 'full history'}) ===")
            try:
                ensure_logged_in(page, source)
                result = source.fetch(FetchCtx(page, source, cutoff, out_dir))
            except Exception as exc:  # keep going with the other sources
                failures += 1
                print(f"  [{source.name}] ERROR: {exc}")
                continue
            if result is None:
                print(f"  [{source.name}] No file produced.")
            else:
                print(f"  [{source.name}] Saved -> {result}")

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
    parser.add_argument("--profile-dir", default=str(DEFAULT_PROFILE_DIR))
    parser.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    parser.add_argument(
        "--cdp",
        default=os.environ.get("FETCH_CDP_URL", ""),
        help=(
            "Attach to an already-running Chrome over CDP instead of launching a profile, "
            "e.g. http://localhost:9222 (start Chrome with --remote-debugging-port=9222)."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    known = list(build_sources().keys())
    selected = known if args.sources == "all" else [s.strip() for s in args.sources.split(",")]
    unknown = [s for s in selected if s not in known]
    if unknown:
        print(f"Unknown source(s): {unknown}. Known: {known}")
        return 2
    return run(
        selected,
        headless=args.headless,
        profile_dir=Path(args.profile_dir),
        warehouse=Path(args.warehouse),
        cdp=args.cdp or None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
