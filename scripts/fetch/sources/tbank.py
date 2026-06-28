"""T-Bank: export operations via the authenticated timeline API.

We open the operations SPA and wait for its authenticated timeline request to
*complete successfully* — that response carries the session id. Reusing that id
we call the export endpoint directly and save the CSV response.
"""

from __future__ import annotations

import base64
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

from playwright.sync_api import Response

from scripts.fetch.core import (
    PAGE_TIMEOUT_MS,
    FetchCtx,
    SourceConfig,
    datetime_to_epoch_ms,
    dump_discovery,
    ensure_dir,
    filename_from_content_disposition,
    floor_datetime_to_day,
    goto,
    logger,
    stamp,
)

URL_T_BANK = "https://www.tbank.ru/mybank/operations/"
T_BANK_EXPORT_API = (
    "https://www.tbank.ru/mybank/api/operations/timeline/public/legacy/v1/export_operations"
)
T_BANK_EXPORT_FORMAT = "csv"
# Path fragment of the authenticated operations XHR the SPA fires once loaded; it carries the
# sessionid we reuse for the export call.
T_BANK_TIMELINE_MARKER = "/operations/timeline/public/legacy/v1/"


def _is_timeline_response(response: Response) -> bool:
    """True for a *successful* authenticated timeline request (the one carrying the sessionid)."""
    return T_BANK_TIMELINE_MARKER in response.url and "sessionid=" in response.url and response.ok


def _export_params(seed_url: str, cutoff: datetime | None) -> dict[str, str]:
    """Build export params from a captured timeline URL and the incremental cutoff."""
    qs = parse_qs(urlparse(seed_url).query)
    session_ids = qs.get("sessionid")
    if not session_ids:
        raise RuntimeError("Could not recover T-Bank sessionid from the timeline request")

    start_ms = datetime_to_epoch_ms(floor_datetime_to_day(cutoff)) if cutoff else 0
    now_ms = datetime_to_epoch_ms(datetime.now().astimezone())
    return {
        "appName": qs.get("appName", ["supreme"])[0],
        "appVersion": qs.get("appVersion", ["0.0.1"])[0],
        "origin": qs.get("origin", ["web,ib5,platform"])[0],
        "sessionid": session_ids[0],
        "start": str(start_ms),
        "end": str(now_ms),
        "format": T_BANK_EXPORT_FORMAT,
    }


def fetch(ctx: FetchCtx) -> Path | None:
    """Capture the authenticated session, call the export API, save the CSV response."""
    page = ctx.page
    # Wait for the timeline DATA request to complete (not just the page shell) so we reuse a fully
    # authenticated session id. networkidle never settles on this SPA, and reading performance
    # entries raced an early, pre-auth call (the export then 401'd with an auth error).
    with page.expect_response(_is_timeline_response, timeout=PAGE_TIMEOUT_MS) as info:
        goto(page, ctx.source.url)
    params = _export_params(info.value.url, ctx.cutoff)
    logger.info(
        "[t_bank] Export range start={} end={} (cutoff={}).",
        params["start"],
        params["end"],
        ctx.cutoff,
    )

    export_url = f"{T_BANK_EXPORT_API}?{urlencode(params)}"
    result = page.evaluate(
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
        dump_discovery(page, ctx.out_dir, ctx.source.name)
        raise RuntimeError(
            "T-Bank export failed "
            f"status={result['status']} {result['statusText']} preview={result['preview']!r}"
        )
    # The endpoint answers 200 with a JSON error body when the session is not accepted; treat any
    # JSON/error payload as a failure instead of saving it as a bogus CSV.
    content_type = result["headers"].get("content-type", "")
    if "json" in content_type.lower() or result["preview"].lstrip().startswith("{"):
        dump_discovery(page, ctx.out_dir, ctx.source.name)
        raise RuntimeError(
            f"T-Bank export returned an error payload instead of CSV: {result['preview']!r}"
        )

    suggested = filename_from_content_disposition(
        result["headers"].get("content-disposition"),
        f"t_bank_operations_{stamp()}.csv",
    )
    if not suggested.lower().endswith(".csv"):
        suggested = f"{suggested}.csv"
    target = ctx.out_dir / suggested
    ensure_dir(ctx.out_dir)
    target.write_bytes(base64.b64decode(result["base64"]))
    return target


SOURCE = SourceConfig(
    name="t_bank",
    platform_name="T Bank",
    url=URL_T_BANK,
    out_subdir=("Bank", "T-Bank", "transactions"),
    # observed: logged-out -> redirect to id.tbank.ru/auth/...
    login_markers=("id.tbank.ru", "id.tinkoff.ru", "/auth", "/login"),
    fetch=fetch,
)
