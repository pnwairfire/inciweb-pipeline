"""Prefect-agnostic domain-event port.

Pipeline code announces meaningful occurrences with ``emit(name, **data)``.
By default this is a no-op, so the package runs identically on its own (CLI,
tests). A host (e.g. a Prefect wrapper) installs an emitter with
``set_event_emitter`` to receive and route those events; passing ``None``
restores the no-op.

This module deliberately knows nothing about how events are delivered — no
Prefect, no logging, no I/O.
"""

from typing import Any, Callable, Optional

_emitter: Optional[Callable[[str, dict[str, Any]], None]] = None


def set_event_emitter(
    emitter: Optional[Callable[[str, dict[str, Any]], None]],
) -> None:
    """Install the emitter that receives domain events, or None to disable."""
    global _emitter
    _emitter = emitter


def emit(name: str, **data: Any) -> None:
    """Announce a domain event. No-op unless an emitter is installed."""
    if _emitter is not None:
        _emitter(name, data)
