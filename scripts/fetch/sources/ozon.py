"""Ozon Bank: scrape operations and enrich purchases with order item names."""

from __future__ import annotations

import contextlib
import re
from datetime import datetime
from pathlib import Path

from playwright.sync_api import Locator, Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from scripts.fetch.core import (
    ACTION_TIMEOUT_MS,
    FetchCtx,
    SourceConfig,
    dump_discovery,
    logger,
    parse_amount,
    settle,
    stamp,
    write_csv,
)

URL_OZON = "https://finance.ozon.ru/lk/operations"

OPERATION_LIST = "[role=dialog] .virtual-list-wrapper"
OPERATION_ROW = "[role=dialog] .operation"
ORDER_LINK_TEXT = "Посмотреть заказ"
MAX_SCROLL_STEPS = 250
SCROLL_IDLE_LIMIT = 5
DETAIL_OPERATION_TYPE_INDEX = 2
DETAIL_LOAD_ATTEMPTS = 12
ITEM_STATUS_VALUES = {
    "Отменен",
    "Отменён",
    "Получен",
}


def _operation_list(page: Page) -> Locator:
    return page.locator(OPERATION_LIST).first


def _open_operations_list(page: Page, out_dir: Path) -> bool:
    if _operation_list(page).count() > 0:
        return True

    page.goto(URL_OZON, wait_until="domcontentloaded", timeout=ACTION_TIMEOUT_MS * 2)
    page.wait_for_timeout(5_000)
    if _operation_list(page).count() > 0:
        return True

    button = page.locator("[data-testid='history-all-operations-button']").first
    if button.count() > 0:
        button.evaluate("(e) => e.click()")
        page.wait_for_selector(OPERATION_LIST, timeout=ACTION_TIMEOUT_MS)
        return True

    dump_discovery(page, out_dir, "ozon_operations_list_not_open")
    logger.warning(
        "[ozon] Operations list is not open and the all-operations button was not found."
    )
    return False


def _operation_datetime(text: str) -> datetime | None:
    match = re.search(r"(\d{2})\.(\d{2})\.(\d{4}),\s*(\d{2}):(\d{2})", text)
    if not match:
        return None
    day, month, year, hour, minute = match.groups()
    with contextlib.suppress(ValueError):
        return datetime(int(year), int(month), int(day), int(hour), int(minute))
    return None


def _detail_lines(page: Page) -> list[str]:
    text = page.locator("[role=dialog]").last.inner_text(timeout=ACTION_TIMEOUT_MS)
    return [line.strip() for line in text.splitlines() if line.strip()]


def _wait_for_detail_lines(page: Page) -> list[str]:
    for _ in range(DETAIL_LOAD_ATTEMPTS):
        lines = _detail_lines(page)
        text = "\n".join(lines)
        if _operation_datetime(text) and _amount_from_lines(lines):
            return lines
        page.wait_for_timeout(500)
    return _detail_lines(page)


def _amount_from_lines(lines: list[str]) -> str:
    for line in lines:
        if "₽" in line and re.search(r"[+\-−]", line):
            return parse_amount(line)
    return ""


def _order_url(page: Page) -> str:
    link = page.locator("a", has_text=ORDER_LINK_TEXT).last
    if link.count() == 0:
        return ""
    with contextlib.suppress(Exception):
        return link.get_attribute("href") or ""
    return ""


def _item_status(lines: list[str], price_index: int, block_end_index: int) -> str:
    for candidate in lines[price_index + 1 : block_end_index]:
        if candidate in ITEM_STATUS_VALUES:
            return candidate
    for candidate in reversed(lines[max(0, price_index - 16) : price_index]):
        if candidate in ITEM_STATUS_VALUES:
            return candidate
    return ""


def _extract_order_items(lines: list[str]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    ignored = {
        "В корзину",
        "Оценить товар",
        "Оценить заказ",
        "Оставить чаевые",
        "Связаться с продавцом",
    }
    attribute_re = re.compile(r"^[А-ЯA-ZЁа-яa-zё ]+:\s*")
    for index, line in enumerate(lines):
        if line != "В корзину":
            continue

        price_index = None
        for candidate_index in range(index - 1, max(-1, index - 8), -1):
            if "₽" in lines[candidate_index]:
                price_index = candidate_index
                break
        if price_index is None:
            continue

        fallback = ""
        for candidate in lines[price_index + 1 : index]:
            if candidate in ignored or candidate in ITEM_STATUS_VALUES or "₽" in candidate:
                continue
            if not fallback:
                fallback = candidate
            if attribute_re.match(candidate):
                continue
            if candidate and not any(item["item"] == candidate for item in items):
                items.append(
                    {
                        "order_item_num": str(len(items) + 1),
                        "item_status": _item_status(lines, price_index, index),
                        "item": candidate,
                        "item_amt_rub": parse_amount(lines[price_index]),
                    }
                )
                break
        if fallback and not any(item["item"] == fallback for item in items):
            items.append(
                {
                    "order_item_num": str(len(items) + 1),
                    "item_status": _item_status(lines, price_index, index),
                    "item": fallback,
                    "item_amt_rub": parse_amount(lines[price_index]),
                }
            )
    return items


def _click_order_and_read_items(page: Page) -> tuple[str, list[dict[str, str]]]:
    order_url = _order_url(page)
    if not order_url:
        return "", []

    try:
        logger.info("[ozon] clicking order page {}", order_url)
        with page.context.expect_page(timeout=ACTION_TIMEOUT_MS) as popup_info:
            page.locator("a", has_text=ORDER_LINK_TEXT).last.click(timeout=ACTION_TIMEOUT_MS)
        order_page = popup_info.value
        order_page.wait_for_load_state("domcontentloaded", timeout=ACTION_TIMEOUT_MS * 2)
        settle(order_page)
        lines = [
            line.strip()
            for line in order_page.locator("body")
            .inner_text(timeout=ACTION_TIMEOUT_MS)
            .splitlines()
            if line.strip()
        ]
    except PlaywrightTimeoutError:
        logger.warning("[ozon] Order page did not open after clicking {}", order_url)
        return order_url, []
    except Exception:
        logger.exception("[ozon] Failed to read order page {}", order_url)
        return order_url, []
    finally:
        if "order_page" in locals():
            with contextlib.suppress(Exception):
                order_page.close()

    return order_url, _extract_order_items(lines)


def _operation_category(operation_text: str) -> str:
    lines = [line.strip() for line in operation_text.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def _read_operation_detail(page: Page, category: str) -> list[dict[str, str]]:
    lines = _wait_for_detail_lines(page)
    detail_text = "\n".join(lines)
    status = lines[DETAIL_OPERATION_TYPE_INDEX] if len(lines) > DETAIL_OPERATION_TYPE_INDEX else ""
    if "возврат" in status.lower() or "возврат" in category.lower():
        logger.info("[ozon] skipping refund operation: {}", status or category)
        return []

    order_items: list[dict[str, str]] = []
    order_url = ""
    if "успешная покупка" in status.lower():
        order_url, order_items = _click_order_and_read_items(page)

    operation_dt = _operation_datetime(detail_text)
    base_row = {
        "transaction_dttm": operation_dt.isoformat(sep=" ") if operation_dt else "",
        "order_amt_rub": _amount_from_lines(lines),
        "item_amt_rub": "",
        "status": status,
        "category": category,
        "order_item_num": "",
        "item_status": "",
        "item": "",
        "order_url": order_url,
        "raw_details": detail_text,
    }
    if not order_items:
        return [base_row]

    rows: list[dict[str, str]] = []
    for item in order_items:
        row = dict(base_row)
        row.update(item)
        rows.append(row)
    return rows


def _back_to_operations_list(page: Page) -> None:
    page.go_back(wait_until="domcontentloaded", timeout=ACTION_TIMEOUT_MS)
    page.wait_for_selector(OPERATION_LIST, timeout=ACTION_TIMEOUT_MS)


def _scroll_position(page: Page) -> tuple[float, float]:
    top, height = _operation_list(page).evaluate("(e) => [e.scrollTop, e.scrollHeight]")
    return float(top), float(height)


def _scroll_to_top(page: Page) -> None:
    box = page.locator("[role=dialog]").last.bounding_box()
    if box is None:
        return
    page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
    for _ in range(8):
        page.mouse.wheel(0, -1200)
        page.wait_for_timeout(250)


def _scroll_down(page: Page) -> tuple[float, float]:
    box = page.locator("[role=dialog]").last.bounding_box()
    if box is None:
        return _scroll_position(page)
    page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] - 80)
    page.mouse.wheel(0, 650)
    page.wait_for_timeout(900)
    return _scroll_position(page)


def _visible_operation_key(page: Page, index: int) -> tuple[str, str] | None:
    current_count = page.locator(OPERATION_ROW).count()
    if index >= current_count:
        return None
    operation = page.locator(OPERATION_ROW).nth(index)
    try:
        text = operation.inner_text(timeout=ACTION_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        logger.warning("[ozon] Visible operation {} disappeared; scrolling on.", index)
        return None
    parent_style = operation.evaluate(
        "(e) => e.parentElement ? e.parentElement.getAttribute('style') || '' : ''"
    )
    return text, parent_style


def _click_visible_operation(page: Page, index: int) -> bool:
    operation = page.locator(OPERATION_ROW).nth(index)
    try:
        operation.click(timeout=ACTION_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        logger.warning("[ozon] Visible operation {} could not be clicked; scrolling on.", index)
        return False
    page.wait_for_url(re.compile(r"/lk/operations/[^/?#]+"), timeout=ACTION_TIMEOUT_MS)
    return True


def _new_visible_operation_text(
    page: Page,
    index: int,
    seen_visible_operations: set[str],
) -> str | None:
    operation_key = _visible_operation_key(page, index)
    if operation_key is None:
        return None
    operation_text, parent_style = operation_key
    if not operation_text.strip():
        return ""

    visible_key = "\x1f".join([operation_text, parent_style])
    if visible_key in seen_visible_operations:
        return ""
    seen_visible_operations.add(visible_key)
    return operation_text


def _append_new_rows(
    rows: list[dict[str, str]],
    seen_rows: set[str],
    detail_rows: list[dict[str, str]],
) -> None:
    for row in detail_rows:
        row_key = "\x1f".join(
            [
                row["transaction_dttm"],
                row["order_amt_rub"],
                row["item_amt_rub"],
                row["status"],
                row["category"],
                row["order_item_num"],
                row["item_status"],
                row["item"],
                row["order_url"],
            ]
        )
        if row_key in seen_rows:
            continue
        seen_rows.add(row_key)
        rows.append(row)
        logger.info(
            "[ozon] collected {} {} {} #{}",
            row["transaction_dttm"],
            row["order_amt_rub"],
            row["status"],
            row["order_item_num"],
        )


def _collect_operations(ctx: FetchCtx) -> list[dict[str, str]]:
    page = ctx.page
    seen_urls: set[str] = set()
    seen_visible_operations: set[str] = set()
    rows: list[dict[str, str]] = []
    idle_scrolls = 0
    last_position = (-1.0, -1.0)
    _scroll_to_top(page)

    for _ in range(MAX_SCROLL_STEPS):
        operation_count = page.locator(OPERATION_ROW).count()
        if operation_count == 0:
            dump_discovery(page, ctx.out_dir, "ozon_operations_list")
            logger.warning("[ozon] No operation rows matched '{}'.", OPERATION_ROW)
            return rows

        for index in range(operation_count):
            operation_text = _new_visible_operation_text(page, index, seen_visible_operations)
            if operation_text is None:
                break
            if not operation_text:
                continue

            if not _click_visible_operation(page, index):
                break
            detail_rows = _read_operation_detail(page, _operation_category(operation_text))
            _append_new_rows(rows, seen_urls, detail_rows)
            _back_to_operations_list(page)

            if not detail_rows:
                continue
            first_row = detail_rows[0]
            if ctx.cutoff and first_row["transaction_dttm"]:
                operation_dt = datetime.fromisoformat(first_row["transaction_dttm"])
                if operation_dt <= ctx.cutoff:
                    return rows

        position = _scroll_down(page)
        if position == last_position:
            idle_scrolls += 1
        else:
            idle_scrolls = 0
        last_position = position
        if idle_scrolls >= SCROLL_IDLE_LIMIT:
            break

    return rows


def fetch(ctx: FetchCtx) -> Path | None:
    """Scrape Ozon Bank operation history and enrich purchases with order items."""
    if not _open_operations_list(ctx.page, ctx.out_dir):
        return None

    rows = _collect_operations(ctx)
    if not rows:
        logger.info("[ozon] No operations collected.")
        return None

    target = ctx.out_dir / f"ozon_{stamp()}.csv"
    write_csv(target, rows)
    return target


SOURCE = SourceConfig(
    name="ozon",
    platform_name="Ozon",
    url=URL_OZON,
    out_subdir=("Marketplace", "Ozon", "transactions"),
    # observed: logged-out stays on finance.ozon.ru/ and shows a sign-in or PIN form
    login_markers=("id.ozon.ru", "/signin"),
    logged_in_url_markers=("/lk/",),
    auth_selector=(
        '[data-testid="obi-test-id-borderless-button"]:has-text("Войти"), '
        '[data-testid="pincode-form-desktop"]'
    ),
    fetch=fetch,
)
