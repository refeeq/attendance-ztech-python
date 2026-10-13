"""Work around ZKTeco timestamps that are not real calendar dates.

The device packs the day into a 31-day field. A stored punch can therefore
decode as 31 April or 31 February. pyzk builds a ``datetime`` inside
``get_attendance()`` and lets that ``ValueError`` abort the whole download,
so one bad row drops every other punch from the terminal.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger("AttendanceZTech")


def tolerate_impossible_zk_dates(conn: Any, device_id: Any = None) -> None:
    """Skip impossible punch dates instead of failing the download."""
    if getattr(conn, "_zk_dates_patched", False):
        return

    label = f"device {device_id}" if device_id is not None else "device"
    patched = False
    for name in ("_ZK__decode_time", "_ZK__decode_timehex"):
        original = getattr(conn, name, None)
        if not callable(original):
            continue

        def _safe(payload: Any, _original: Callable[..., Any] = original) -> Any:
            try:
                return _original(payload)
            except (ValueError, OverflowError):
                return None

        setattr(conn, name, _safe)
        patched = True
    if not patched:
        return

    conn._zk_dates_patched = True
    original_get = conn.get_attendance

    def get_attendance() -> list:
        rows = original_get() or []
        kept = []
        skipped = 0
        for row in rows:
            if getattr(row, "timestamp", None) is None:
                skipped += 1
                continue
            kept.append(row)
        if skipped:
            logger.warning(
                "⚠️ [%s] skipped %s punch(es) with an impossible device date",
                label,
                skipped,
            )
        return kept

    conn.get_attendance = get_attendance
