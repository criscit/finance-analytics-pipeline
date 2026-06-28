"""Wildberries: "Оплата по СБП" wallet debits enriched with product names.

Reads the wallet history, keeps SBP debits after the cutoff, then matches each
amount against the receipts section to recover the product name (each receipt
opens in a new tab). Selectors are not yet confirmed against the live DOM.
"""

from __future__ import annotations

import contextlib
from pathlib import Path

from playwright.sync_api import Locator, Page

from scripts.fetch.core import (
    ACTION_TIMEOUT_MS,
    FetchCtx,
    SourceConfig,
    amount_key,
    dump_discovery,
    goto,
    logger,
    parse_amount,
    parse_ru_date,
    settle,
    stamp,
    text_of,
    write_csv,
)

URL_WB_WALLET = "https://www.wildberries.ru/lk/mywallet?type=history"
URL_WB_RECEIPTS = "https://www.wildberries.ru/lk/receipts/get"

# Wildberries selectors (VERIFY all against the live, logged-in DOM).
WB_SBP_MARKER = "СБП"
WB_WALLET_ROW = "[class*='history'] [class*='item'], [class*='wallet'] tr"
WB_WALLET_TYPE = "[class*='type'], [class*='operation'], [class*='descr']"
WB_WALLET_DATE = "[class*='date']"
WB_WALLET_AMOUNT = "[class*='sum'], [class*='amount']"
WB_RECEIPT_ROW = "[class*='receipt'] [class*='item'], [class*='receipt'] tr"
WB_RECEIPT_AMOUNT = "[class*='sum'], [class*='amount']"
WB_RECEIPT_ITEM_NAME = "[class*='name'], [class*='product'], [class*='good']"


def _collect_sbp_debits(ctx: FetchCtx) -> list[dict[str, str]]:
    page = ctx.page
    goto(page, URL_WB_WALLET)
    rows = page.locator(WB_WALLET_ROW)
    count = rows.count()
    if count == 0:
        dump_discovery(page, ctx.out_dir, "wb_wallet")
        logger.warning("[wb] No wallet rows matched '{}' — saved discovery dump.", WB_WALLET_ROW)
        return []

    debits: list[dict[str, str]] = []
    for index in range(count):
        row = rows.nth(index)
        type_text = text_of(row, WB_WALLET_TYPE)
        if WB_SBP_MARKER not in type_text:
            continue
        date_text = text_of(row, WB_WALLET_DATE)
        debit_dt = parse_ru_date(date_text)
        if ctx.cutoff and debit_dt and debit_dt <= ctx.cutoff:
            continue
        debits.append(
            {
                "date": date_text,
                "amount_rub": parse_amount(text_of(row, WB_WALLET_AMOUNT)),
                "type": type_text,
            }
        )
    return debits


def _open_receipt_name(page: Page, receipt: Locator) -> str:
    """Click a receipt (opens a new tab) and read the product name from it."""
    try:
        with page.context.expect_page(timeout=ACTION_TIMEOUT_MS) as popup_info:
            receipt.click()
    except Exception:
        return ""
    popup = popup_info.value
    settle(popup)
    name = text_of(popup.locator("body"), WB_RECEIPT_ITEM_NAME)
    with contextlib.suppress(Exception):
        popup.close()
    return name


def _receipt_names_by_amount(ctx: FetchCtx) -> dict[str, str]:
    page = ctx.page
    goto(page, URL_WB_RECEIPTS)
    receipts = page.locator(WB_RECEIPT_ROW)
    count = receipts.count()
    if count == 0:
        dump_discovery(page, ctx.out_dir, "wb_receipts")
        logger.warning("[wb] No receipts matched '{}' — saved discovery dump.", WB_RECEIPT_ROW)
        return {}

    mapping: dict[str, str] = {}
    for index in range(count):
        receipt = receipts.nth(index)
        key = amount_key(parse_amount(text_of(receipt, WB_RECEIPT_AMOUNT)))
        if not key:
            continue
        name = _open_receipt_name(page, receipt)
        if name:
            mapping[key] = name
    return mapping


def fetch(ctx: FetchCtx) -> Path | None:
    """SBP debits from the wallet, enriched with product names from receipts."""
    debits = _collect_sbp_debits(ctx)
    if not debits:
        logger.info("[wb] No new 'Оплата по СБП' debits after cutoff {}.", ctx.cutoff)
        return None

    names = _receipt_names_by_amount(ctx)
    for debit in debits:
        debit["item"] = names.get(amount_key(debit["amount_rub"]), "")

    target = ctx.out_dir / f"wb_{stamp()}.csv"
    write_csv(target, debits)
    return target


SOURCE = SourceConfig(
    name="wb",
    platform_name="Wildberries",
    url=URL_WB_WALLET,
    out_subdir=("Marketplace", "Wildberries", "orders"),
    # observed: logged-out -> redirect to id.wb.ru/login/...
    login_markers=("id.wb.ru", "/login"),
    fetch=fetch,
)
