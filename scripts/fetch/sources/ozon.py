"""Ozon Bank: statement-style export from the operations page.

Opens the operations page and clicks the first candidate export control that
yields a download. Selectors are not yet confirmed against the live page, so a
miss saves a discovery dump for the selector to be filled in.
"""

from __future__ import annotations

from pathlib import Path

from scripts.fetch.core import (
    FetchCtx,
    SourceConfig,
    dump_discovery,
    ensure_dir,
    goto,
    logger,
    stamp,
    try_download,
)

URL_OZON = "https://finance.ozon.ru/lk/operations"

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


def fetch(ctx: FetchCtx) -> Path | None:
    """Open the operations page and try to export a statement file."""
    goto(ctx.page, ctx.source.url)
    download = try_download(ctx.page, EXPORT_SELECTORS)
    if download is None:
        dump_discovery(ctx.page, ctx.out_dir, ctx.source.name)
        logger.warning(
            "[{}] No export control matched yet — saved a discovery dump in {} "
            "so the export selector can be confirmed.",
            ctx.source.name,
            ctx.out_dir,
        )
        return None
    suggested = download.suggested_filename or f"{ctx.source.name}.csv"
    target = ctx.out_dir / f"{ctx.source.name}_{stamp()}_{suggested}"
    ensure_dir(ctx.out_dir)
    download.save_as(str(target))
    return target


SOURCE = SourceConfig(
    name="ozon",
    platform_name="Ozon",
    url=URL_OZON,
    out_subdir=("Bank", "Ozon", "transactions"),
    # observed: logged-out stays on finance.ozon.ru/ and shows a "Войти" button
    login_markers=("id.ozon.ru", "/signin"),
    auth_selector='[data-testid="obi-test-id-borderless-button"]:has-text("Войти")',
    fetch=fetch,
)
