"""
storage.py
Durable SQLite-backed attendance queue and small key/value state store.

Why this exists
---------------
Previously the running daemon kept attendance records only in a
``multiprocessing.Manager().list()`` and cleared the whole list after a
successful HTTP push. Any record appended between the snapshot and the clear
was silently lost, and a crash / power-loss / hung process discarded the
entire buffer.

This module gives the project a single, durable source of truth:

* ``enqueue_many`` is idempotent on the natural key
  ``(device_id, user_id, timestamp, status, punch)`` so re-pulls (boot sync,
  end-of-day catch-up, manual sync_all) never create duplicates.
* Records are only marked synced after the ERP confirms a 2xx response.
* SQLite WAL mode allows the device subprocesses, the pusher thread, and
  ad-hoc CLI scripts (``sync_all.py``, ``boot_sync_30d.py``) to share the
  same queue safely across processes.
* A small ``sync_state`` table holds run-to-run state (e.g. last EoD date)
  so daily safety-net behavior survives restarts.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

logger = logging.getLogger("AttendanceZTech.Storage")

DEFAULT_DB_PATH = "data/attendance_queue.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS attendance_queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id       INTEGER NOT NULL,
    user_id         INTEGER NOT NULL,
    timestamp       TEXT    NOT NULL,
    status          INTEGER NOT NULL DEFAULT 0,
    punch           INTEGER NOT NULL DEFAULT 0,
    synced          INTEGER NOT NULL DEFAULT 0,
    sync_attempts   INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    last_error      TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(device_id, user_id, timestamp, status, punch)
);

CREATE INDEX IF NOT EXISTS idx_aq_unsynced
    ON attendance_queue(synced, id);

CREATE INDEX IF NOT EXISTS idx_aq_synced_created
    ON attendance_queue(synced, created_at);

CREATE TABLE IF NOT EXISTS sync_state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def _open_connection(db_path: str) -> sqlite3.Connection:
    parent = os.path.dirname(db_path)
    if parent:
        Path(parent).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        timeout=30.0,
        isolation_level=None,        # autocommit; we drive transactions explicitly
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    # WAL gives concurrent readers + a single writer with crash safety on local disk.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


class AttendanceQueue:
    """Process- and thread-safe SQLite-backed queue.

    Each public method opens a short-lived connection. SQLite (WAL) handles
    cross-process locking; an in-process ``threading.Lock`` serializes
    same-process writers to keep contention predictable.

    The instance only stores ``db_path`` so it can be safely passed across
    ``multiprocessing.Process`` boundaries (no live file handles to leak).
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = str(db_path or DEFAULT_DB_PATH)
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(_SCHEMA)

    @contextmanager
    def _conn(self):
        with self._lock:
            conn = _open_connection(self.db_path)
            try:
                yield conn
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    @staticmethod
    def _normalize(record: Dict[str, Any]) -> Optional[tuple]:
        """Coerce a record into the canonical tuple, or return None if invalid."""
        try:
            return (
                int(record["device_id"]),
                int(record["user_id"]),
                str(record["timestamp"]),
                int(record.get("status") or 0),
                int(record.get("punch") or 0),
            )
        except Exception as exc:
            logger.warning("Skipping malformed record: %s | %s", exc, record)
            return None

    # ------------------------------------------------------------------ enqueue
    def enqueue_many(self, records: Iterable[Dict[str, Any]]) -> int:
        """Insert records idempotently. Returns count of NEW rows."""
        rows = [r for r in (self._normalize(rec) for rec in records) if r]
        if not rows:
            return 0
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                before = conn.execute(
                    "SELECT COUNT(*) FROM attendance_queue;"
                ).fetchone()[0]
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO attendance_queue
                        (device_id, user_id, timestamp, status, punch)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    rows,
                )
                after = conn.execute(
                    "SELECT COUNT(*) FROM attendance_queue;"
                ).fetchone()[0]
                conn.execute("COMMIT;")
                return max(0, int(after) - int(before))
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise

    def enqueue_one(self, record: Dict[str, Any]) -> bool:
        return self.enqueue_many([record]) > 0

    # ------------------------------------------------------------------ consume
    def fetch_unsynced(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Return up to ``limit`` unsynced rows in FIFO order."""
        limit = max(1, int(limit))
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, device_id, user_id, timestamp, status, punch
                  FROM attendance_queue
                 WHERE synced = 0
                 ORDER BY id ASC
                 LIMIT ?;
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def count_unsynced(self) -> int:
        with self._conn() as conn:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM attendance_queue WHERE synced = 0;"
                ).fetchone()[0]
            )

    def mark_synced(self, ids: Sequence[int]) -> int:
        ids = [int(i) for i in ids]
        if not ids:
            return 0
        with self._conn() as conn:
            placeholders = ",".join("?" for _ in ids)
            conn.execute("BEGIN IMMEDIATE;")
            try:
                conn.execute(
                    f"""
                    UPDATE attendance_queue
                       SET synced = 1,
                           last_attempt_at = datetime('now'),
                           last_error = NULL
                     WHERE id IN ({placeholders});
                    """,
                    ids,
                )
                conn.execute("COMMIT;")
                return len(ids)
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise

    def mark_attempt_failed(self, ids: Sequence[int], err: str) -> None:
        ids = [int(i) for i in ids]
        if not ids:
            return
        message = (err or "")[:500]
        with self._conn() as conn:
            placeholders = ",".join("?" for _ in ids)
            conn.execute("BEGIN IMMEDIATE;")
            try:
                conn.execute(
                    f"""
                    UPDATE attendance_queue
                       SET sync_attempts = sync_attempts + 1,
                           last_attempt_at = datetime('now'),
                           last_error = ?
                     WHERE id IN ({placeholders});
                    """,
                    [message, *ids],
                )
                conn.execute("COMMIT;")
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise

    # ----------------------------------------------------------------- maintain
    def purge_synced_older_than(self, days: int = 14) -> int:
        days = max(1, int(days))
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                cur = conn.execute(
                    "DELETE FROM attendance_queue "
                    "WHERE synced = 1 AND created_at < ?;",
                    (cutoff,),
                )
                conn.execute("COMMIT;")
                return int(cur.rowcount or 0)
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise

    def stats(self) -> Dict[str, int]:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT
                  SUM(CASE WHEN synced = 0 THEN 1 ELSE 0 END) AS pending,
                  SUM(CASE WHEN synced = 1 THEN 1 ELSE 0 END) AS synced,
                  COUNT(*)                                       AS total,
                  COALESCE(MAX(sync_attempts), 0)                AS max_attempts
                  FROM attendance_queue;
                """
            ).fetchone()
        return {
            "pending": int(row["pending"] or 0),
            "synced": int(row["synced"] or 0),
            "total": int(row["total"] or 0),
            "max_attempts": int(row["max_attempts"] or 0),
        }

    # ----------------------------------------------------------------- KV state
    def get_state(self, key: str) -> Optional[str]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT value FROM sync_state WHERE key = ?;", (key,)
            ).fetchone()
        return row["value"] if row else None

    def set_state(self, key: str, value: Any) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO sync_state(key, value, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = datetime('now');
                """,
                (str(key), str(value)),
            )
