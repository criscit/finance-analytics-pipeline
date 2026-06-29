"""Wildberries: WB Кошелёк operations enriched from the operation card and fiscal receipts.

Two-stage flow (2026 only):

1. Wallet ``История операций``: open each operation card (a modal) and record the real type,
   status, date+time, commission, signed amount and the operation id. Covers both пополнения
   and списания.
2. For every **списание** (negative amount), match a fiscal receipt in ``Чеки``
   (``/lk/receipts/get``) by amount — the wallet and receipt timestamps diverge, but amounts
   match exactly — open it (new tab) and emit **one row per product** from its items table.

The wallet is guarded by a JS anti-bot interstitial and a wallet password overlay, both handled
by :func:`_wb_goto` / :func:`_wait_for_wallet_ready`.
"""

from __future__ import annotations

import contextlib
import re
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import Locator, Page

from scripts.fetch.core import (
    ACTION_TIMEOUT_MS,
    LOGIN_POLL_SECONDS,
    RU_MONTHS,
    FetchCtx,
    SourceConfig,
    dump_discovery,
    goto,
    logger,
    parse_amount,
    settle,
    stamp,
    text_of,
    write_csv,
)

URL_WB_WALLET = "https://www.wildberries.ru/lk/mywallet?type=history"
URL_WB_RECEIPTS = "https://www.wildberries.ru/lk/receipts/get"

# WB guards the wallet behind two in-page gates that carry no login-URL marker, so the URL
# heuristic alone wrongly reports "logged in" while either is up:
#   * Anti-bot: a JS interstitial (/__wbaas/challenges/antibot/, "Проверяем браузер") that
#     SELF-SOLVES in a real profile — we just wait it out, up to ANTIBOT_TIMEOUT_MS.
#   * Password: an in-page "Введите пароль" PIN/2FA confirm overlay that CANNOT self-solve —
#     it needs the user to type the wallet password in the browser, so we poll indefinitely.
WB_ANTIBOT = "#c_cont, b#s-key[data-site-key]"
WB_PASSWORD_GATE = "[class*='transfer-block--confirm']"
# Any of these means the wallet is not ready to read; used as the auth_selector below.
WB_NOT_READY = f"{WB_ANTIBOT}, {WB_PASSWORD_GATE}"
ANTIBOT_TIMEOUT_MS = 90_000

# Only keep 2026 operations. Year-less date headers (e.g. "27 июня") are assumed to be 2026.
WB_MIN_DATE = datetime(2026, 1, 1)

# Wildberries DOM. Class suffixes (e.g. --MeOoq) are build-hashed and churn, so match on the
# stable prefix only. Confirmed against live, logged-in discovery dumps.
# Wallet "История операций" list:
WB_WALLET_CONTAINER = "[class*='transactions--']"  # list wrapper (note the plural)
WB_WALLET_SECTION = "section"  # one per date group, inside the container
WB_WALLET_DATE = "[class*='dateTitle--']"  # group header date, e.g. "27 июня"
WB_WALLET_ROW = "[class*='transaction--']"  # one per operation, inside a section
# Operation card (modal opened by clicking a row):
WB_CARD = "[class*='transactionDetailsPopup--']"
WB_CARD_TYPE = "[class*='typeWrap--']"  # real type, e.g. "Оплата по СБП"
WB_CARD_AMOUNT = "[class*='amountPopup--']"  # signed amount, e.g. minus 3 446 RUB
WB_CARD_DETAIL_ROW = "[class*='detailRow--']"  # label/value pair rows
WB_CARD_DETAIL_LABEL = "[class*='detailLabel--']"
WB_CARD_DETAIL_VALUE = "[class*='detailValue--']"
WB_CARD_CLOSE = "[class*='mo-modal__close']"
# Receipts list (/lk/receipts/get, tab "Чеки"):
WB_RECEIPT_ITEM = "[data-testid='receipt-item']"
WB_RECEIPT_DATE = "[class*='receipt-page-date']"
WB_RECEIPT_SUM = "[data-testid='receipt-sum']"
WB_RECEIPT_OPEN = "[class*='open-btn--']"
# Receipt page (receipt.wb.ru) items table:
WB_PRODUCT_ITEM = "[class*='products-item']"
WB_PRODUCT_NUMBER = "[class*='products-cell_number']"
WB_PRODUCT_NAME = "[class*='products-cell_name'] [class*='products-prop-value']"
WB_PRODUCT_PRICE = "[class*='products-cell_price']"
WB_PRODUCT_COUNT = "[class*='products-cell_count']"
WB_PRODUCT_COST = "[class*='products-cell_cost']"

CARD_LOAD_ATTEMPTS = 12


# --------------------------------------------------------------------------------------
# Gates / navigation
# --------------------------------------------------------------------------------------
def _gate_visible(page: Page, selector: str) -> bool:
    """True while ANY element matching ``selector`` is rendered and visible.

    WB ships several hidden ``transfer-block--confirm`` templates, so checking only ``.first``
    misses the one real, visible confirm/password modal — we must scan every match.
    """
    loc = page.locator(selector)
    try:
        count = loc.count()
    except Exception:
        return False
    for index in range(count):
        with contextlib.suppress(Exception):
            if loc.nth(index).is_visible(timeout=500):
                return True
    return False


def _wait_for_wallet_ready(page: Page) -> None:
    """Wait until both the anti-bot and the password gate clear before reading the DOM.

    The anti-bot self-solves (bounded by ANTIBOT_TIMEOUT_MS); the password prompt needs the
    user to type the wallet PIN in the browser, so it is polled indefinitely.
    """
    antibot_deadline = time.monotonic() + ANTIBOT_TIMEOUT_MS / 1000
    warned_password = False
    while True:
        if _gate_visible(page, WB_PASSWORD_GATE):
            if not warned_password:
                logger.warning(
                    "[wb] Wallet confirm prompt detected (password / SMS code / 'Подтвердите, что "
                    "профиль ваш'). Complete it in the browser window; waiting until it clears."
                )
                warned_password = True
            time.sleep(LOGIN_POLL_SECONDS)
            continue
        if _gate_visible(page, WB_ANTIBOT):
            if time.monotonic() > antibot_deadline:
                logger.warning("[wb] Anti-bot still present after timeout; proceeding anyway.")
                return
            logger.info("[wb] Anti-bot challenge active; waiting for it to clear…")
            time.sleep(LOGIN_POLL_SECONDS)
            continue
        return


def _wb_goto(page: Page, url: str) -> None:
    """Navigate to a WB page and wait past the anti-bot / password gates before reading."""
    goto(page, url)
    _wait_for_wallet_ready(page)
    settle(page)


# --------------------------------------------------------------------------------------
# Date parsing
# --------------------------------------------------------------------------------------
def _parse_wb_datetime(text: str) -> datetime | None:
    """Parse WB date strings like '27 июня', '26 января 2025' or '27 июня, суббота, 12:01'.

    Year and time are optional: a missing year defaults to the current year, a missing time to
    midnight.
    """
    if not text:
        return None
    date_match = re.search(r"(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?", text.lower())
    if not date_match:
        return None
    day, month_name, year = date_match.groups()
    month = RU_MONTHS.get(month_name)
    if not month:
        return None
    time_match = re.search(r"(\d{1,2}):(\d{2})", text)
    hour, minute = (int(time_match.group(1)), int(time_match.group(2))) if time_match else (0, 0)
    resolved_year = int(year) if year else datetime.now().year
    with contextlib.suppress(ValueError):
        return datetime(resolved_year, month, int(day), hour, minute)
    return None


# --------------------------------------------------------------------------------------
# Operation card
# --------------------------------------------------------------------------------------
def _open_card(page: Page, row: Locator) -> bool:
    """Click an operation row and wait for its detail modal to fully load.

    The detail rows stream in via XHR, so we wait until their count stabilizes (two consecutive
    equal, non-zero reads) rather than returning on the first row — otherwise fields like Статус
    and Комиссия are read before they render.
    """
    try:
        row.click(timeout=ACTION_TIMEOUT_MS)
        page.wait_for_selector(WB_CARD, state="visible", timeout=ACTION_TIMEOUT_MS)
    except Exception:
        logger.warning("[wb] Could not open an operation card; skipping it.")
        return False
    card = page.locator(WB_CARD).first
    previous = -1
    for _ in range(CARD_LOAD_ATTEMPTS):
        page.wait_for_timeout(400)
        count = card.locator(WB_CARD_DETAIL_ROW).count()
        if count > 0 and count == previous and text_of(card, WB_CARD_TYPE):
            return True
        previous = count
    return True


def _close_card(page: Page) -> None:
    """Dismiss the operation modal and wait until it is fully removed from the DOM.

    A clean teardown guarantees the next card opens fresh rather than re-reading stale content.
    """
    for attempt in range(2):
        with contextlib.suppress(Exception):
            if attempt == 0:
                page.keyboard.press("Escape")
            else:
                page.locator(WB_CARD_CLOSE).first.click(timeout=2_000)
        for _ in range(10):
            if page.locator(WB_CARD).count() == 0:
                return
            page.wait_for_timeout(300)
    logger.warning("[wb] Operation card did not close; subsequent reads may be stale.")


def _clean_commission(value: str) -> str:
    """Normalise a commission value ('0 0', 'Без комиссии', '5 ₽') to a plain number string."""
    if not value or "без комисс" in value.lower():
        return "0"
    amount = parse_amount(value)
    with contextlib.suppress(ValueError):
        return f"{float(amount):g}"
    return amount or "0"


def _read_card(page: Page) -> dict[str, str]:
    """Read type/status/datetime/commission/amount/op_id from the open operation modal."""
    card = page.locator(WB_CARD).first
    details: dict[str, str] = {}
    rows = card.locator(WB_CARD_DETAIL_ROW)
    for index in range(rows.count()):
        row = rows.nth(index)
        label = text_of(row, WB_CARD_DETAIL_LABEL).rstrip(":").strip()
        if label:
            details[label] = text_of(row, WB_CARD_DETAIL_VALUE)

    card_dt = _parse_wb_datetime(details.get("Дата и время", ""))
    return {
        "transaction_dttm": card_dt.isoformat(sep=" ") if card_dt else "",
        "type": text_of(card, WB_CARD_TYPE),
        "status": details.get("Статус", ""),
        "commission_rub": _clean_commission(details.get("Комиссия", "")),
        "order_amt_rub": parse_amount(text_of(card, WB_CARD_AMOUNT)),
        "op_id": details.get("Идентификатор операции", ""),
        "raw_details": "\n".join(f"{key}: {value}" for key, value in details.items()),
    }


def _collect_operation_cards(ctx: FetchCtx) -> list[dict[str, str]]:
    """Open every 2026 operation card on the wallet page and return their fields."""
    page = ctx.page
    _wb_goto(page, URL_WB_WALLET)
    # The transaction list renders via XHR after the gates clear; wait for the first row.
    with contextlib.suppress(Exception):
        page.wait_for_selector(WB_WALLET_ROW, timeout=ACTION_TIMEOUT_MS)
    sections = page.locator(WB_WALLET_CONTAINER).locator(WB_WALLET_SECTION)
    section_count = sections.count()
    if section_count == 0:
        dump_discovery(page, ctx.out_dir, "wb_wallet")
        logger.warning(
            "[wb] No wallet sections matched '{}' — saved discovery dump.", WB_WALLET_ROW
        )
        return []

    floor = WB_MIN_DATE
    if ctx.cutoff and ctx.cutoff > floor:
        floor = ctx.cutoff

    cards: list[dict[str, str]] = []
    for s_index in range(section_count):
        section = sections.nth(s_index)
        section_dt = _parse_wb_datetime(text_of(section, WB_WALLET_DATE))
        if section_dt and section_dt < floor:
            continue  # whole date group is older than the cutoff
        rows = section.locator(WB_WALLET_ROW)
        for r_index in range(rows.count()):
            if not _open_card(page, rows.nth(r_index)):
                continue
            card = _read_card(page)
            _close_card(page)
            cards.append(card)
            logger.info(
                "[wb] card: {} {} {}", card["transaction_dttm"], card["type"], card["order_amt_rub"]
            )
    return cards


# --------------------------------------------------------------------------------------
# Receipts
# --------------------------------------------------------------------------------------
def _load_receipt_index(page: Page) -> list[dict[str, str]]:
    """Load the receipts list as ``{index, amount_abs, date_text}`` rows for amount matching."""
    _wb_goto(page, URL_WB_RECEIPTS)
    items = page.locator(WB_RECEIPT_ITEM)
    receipts: list[dict[str, str]] = []
    for index in range(items.count()):
        item = items.nth(index)
        receipts.append(
            {
                "index": str(index),
                "amount_abs": parse_amount(text_of(item, WB_RECEIPT_SUM)).lstrip("-"),
                "date_text": text_of(item, WB_RECEIPT_DATE),
            }
        )
    logger.info("[wb] loaded {} receipts.", len(receipts))
    return receipts


def _match_receipt(
    operation: dict[str, str], receipts: list[dict[str, str]], used: set[int]
) -> int | None:
    """Index of the unused receipt matching the debit by amount, tiebroken by nearest date."""
    target = operation["order_amt_rub"].lstrip("-")
    if not target:
        return None
    candidates = [r for r in receipts if r["amount_abs"] == target and int(r["index"]) not in used]
    if not candidates:
        return None
    op_dt = _parse_wb_datetime(operation["transaction_dttm"])
    if op_dt is None or len(candidates) == 1:
        return int(candidates[0]["index"])

    def date_distance(receipt: dict[str, str]) -> float:
        receipt_dt = _parse_wb_datetime(receipt["date_text"])
        return abs((receipt_dt - op_dt).total_seconds()) if receipt_dt else float("inf")

    return int(min(candidates, key=date_distance)["index"])


def _read_receipt_items(page: Page, receipt_index: int) -> tuple[str, list[dict[str, str]]]:
    """Click a receipt's 'Открыть' (new tab), parse its items table, return (url, items)."""
    open_button = page.locator(WB_RECEIPT_ITEM).nth(receipt_index).locator(WB_RECEIPT_OPEN)
    try:
        with page.context.expect_page(timeout=ACTION_TIMEOUT_MS) as popup_info:
            open_button.first.click(timeout=ACTION_TIMEOUT_MS)
        receipt_page = popup_info.value
        receipt_page.wait_for_load_state("domcontentloaded", timeout=ACTION_TIMEOUT_MS * 2)
        settle(receipt_page)
    except Exception:
        logger.warning("[wb] Receipt #{} did not open.", receipt_index)
        return "", []

    url = receipt_page.url
    items: list[dict[str, str]] = []
    products = receipt_page.locator(WB_PRODUCT_ITEM)
    for index in range(products.count()):
        product = products.nth(index)
        items.append(
            {
                "order_item_num": text_of(product, WB_PRODUCT_NUMBER) or str(index + 1),
                "item": text_of(product, WB_PRODUCT_NAME),
                "item_price_rub": parse_amount(text_of(product, WB_PRODUCT_PRICE)),
                "item_qty": text_of(product, WB_PRODUCT_COUNT),
                "item_amt_rub": parse_amount(text_of(product, WB_PRODUCT_COST)),
            }
        )
    with contextlib.suppress(Exception):
        receipt_page.close()
    return url, items


# --------------------------------------------------------------------------------------
# Assembly
# --------------------------------------------------------------------------------------
def _build_rows(ctx: FetchCtx, cards: list[dict[str, str]]) -> list[dict[str, str]]:
    """Combine operation cards with receipt items: one row per product for every списание."""
    receipts = _load_receipt_index(ctx.page)
    used: set[int] = set()
    rows: list[dict[str, str]] = []

    for card in cards:
        receipt_url = ""
        items: list[dict[str, str]] = []
        if card["order_amt_rub"].startswith("-"):  # any debit -> look for a receipt
            match_index = _match_receipt(card, receipts, used)
            if match_index is not None:
                used.add(match_index)
                receipt_url, items = _read_receipt_items(ctx.page, match_index)

        base_row = {
            "transaction_dttm": card["transaction_dttm"],
            "type": card["type"],
            "status": card["status"],
            "commission_rub": card["commission_rub"],
            "order_amt_rub": card["order_amt_rub"],
            "order_item_num": "",
            "item": "",
            "item_price_rub": "",
            "item_qty": "",
            "item_amt_rub": "",
            "receipt_url": receipt_url,
            "op_id": card["op_id"],
            "raw_details": card["raw_details"],
        }
        if not items:
            rows.append(base_row)
            continue
        for item in items:
            row = dict(base_row)
            row.update(item)
            rows.append(row)
    return rows


def fetch(ctx: FetchCtx) -> Path | None:
    """WB Кошелёк operations (2026) enriched with receipt items, one row per product."""
    cards = _collect_operation_cards(ctx)
    if not cards:
        logger.info("[wb] No wallet operations on/after cutoff {}.", ctx.cutoff or WB_MIN_DATE)
        return None

    rows = _build_rows(ctx, cards)
    target = ctx.out_dir / f"wb_{stamp()}.csv"
    write_csv(target, rows)
    return target


SOURCE = SourceConfig(
    name="wb",
    platform_name="WB",
    url=URL_WB_WALLET,
    out_subdir=("Marketplace", "WB", "transactions"),
    # observed: logged-out -> redirect to id.wb.ru/login/...
    login_markers=("id.wb.ru", "/login"),
    # The anti-bot interstitial and the "Введите пароль" overlay are both served at the wallet URL
    # with no login marker, so the URL heuristic alone wrongly reports "logged in". Treat either as
    # a not-ready signal: while one is visible, is_logged_in() returns False and ensure_logged_in()
    # keeps polling (giving the user time to clear the anti-bot / enter the wallet password).
    auth_selector=WB_NOT_READY,
    fetch=fetch,
)
