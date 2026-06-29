"""CLI entry point for the transaction fetchers.

Drives a headed Playwright Chrome with a persistent profile, so logins are
remembered between runs. If a source is not logged in, the run pauses until you
log in manually in the opened window, then continues. Output is written into the
contract-driven ``To Parse`` directories the ingestion pipeline already reads.

Usage::

    poetry run python -m scripts.fetch                 # all sources, headed
    poetry run python -m scripts.fetch --sources ozon  # one source
    poetry run python -m scripts.fetch --headless      # no UI (cannot log in)
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from scripts.fetch.browser import RunConfig, browser_page
from scripts.fetch.core import (
    DEFAULT_CDP_URL,
    DEFAULT_PROFILE_DIR,
    FetchCtx,
    ensure_dir,
    ensure_logged_in,
    get_source_cutoff,
    logger,
    resolve_finance_root,
)
from scripts.fetch.sources import build_sources


def run(selected: list[str], config: RunConfig) -> int:
    """Fetch each selected source in turn; return the number of failures."""
    sources = build_sources()
    finance_root = resolve_finance_root()
    to_parse_root = finance_root / "To Parse"
    logger.info("Target 'To Parse' root: {}", to_parse_root)

    failures = 0
    with browser_page(config) as page:
        for key in selected:
            source = sources[key]
            out_dir = to_parse_root.joinpath(*source.out_subdir)
            ensure_dir(out_dir)
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
    return failures


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    known = ",".join(build_sources())
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sources",
        default="all",
        help=f"Comma-separated subset of: {known} (default: all)",
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
    parser.add_argument(
        "--cdp-auto",
        action="store_true",
        help=("Deprecated; CDP auto-start is now the default unless --headless is used."),
    )
    return parser.parse_args(argv)


def _force_utf8_stdio() -> None:
    """Emit UTF-8 so Cyrillic output/filenames don't crash on a cp1252 Windows console."""
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
    cdp = args.cdp or (None if args.headless else DEFAULT_CDP_URL)
    return run(
        selected,
        RunConfig(
            headless=args.headless,
            profile_dir=Path(args.profile_dir),
            cdp=cdp,
            cdp_auto=bool(cdp),
        ),
    )


if __name__ == "__main__":
    raise SystemExit(main())
