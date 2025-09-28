# Orchestration package

"""Project level helpers and compatibility fixes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

# Constants
MIN_COMPATIBLE_ARGCOUNT = 4
EXPECTED_ARGS_COUNT = 3

if TYPE_CHECKING:
    from collections.abc import Callable


def _apply_rx_schedule_patch() -> None:
    """Apply a backwards compatible patch for RxPY 1.x.

    Dagster 1.9 expects the RxPY 3.x ``BaseObserver.schedule`` signature,
    which accepts ``(scheduler, state, action)``. The project pins RxPY 1.6
    for Python 3.11 compatibility and that version exposes a ``schedule``
    method that only accepts ``(scheduler, state)``. When Dagster passes an
    extra ``action`` argument, the interpreter raises ``TypeError``.  To avoid
    touching the dependency graph we provide a small compatibility shim that
    gracefully handles the extended call signature.
    """

    try:
        from rx.core import observerbase as observer_module
    except Exception:  # pragma: no cover - RxPY might not be installed yet.
        return

    original_schedule = getattr(observer_module.ObserverBase, "schedule", None)
    if original_schedule is None:
        return

    # Only patch implementations that still use the two-argument signature.
    try:
        argcount = original_schedule.__code__.co_argcount
    except AttributeError:  # pragma: no cover - built-in/extension functions.
        argcount = None

    if argcount is not None and argcount >= MIN_COMPATIBLE_ARGCOUNT:
        # Already compatible with the expected call signature.
        return

    def _patched_schedule(self: Any, *args: Any, **kwargs: Any) -> Any:
        """Compatibility wrapper that tolerates the extra ``action`` argument."""

        try:
            return original_schedule(self, *args, **kwargs)
        except TypeError:
            if len(args) == EXPECTED_ARGS_COUNT and not kwargs:
                scheduler, state, action = args
                schedule_fn: Callable[..., Any] | None = getattr(
                    scheduler,
                    "schedule",
                    None,
                )
                if callable(schedule_fn) and callable(action):
                    try:
                        return schedule_fn(action, state)
                    except TypeError:
                        return schedule_fn(action)
            raise

    observer_module.ObserverBase.schedule = _patched_schedule


_apply_rx_schedule_patch()
