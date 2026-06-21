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
import re
import shutil
import sqlite3
import subprocess
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple

logger = logging.getLogger("AttendanceZTech.Storage")

DEFAULT_DB_PATH = "data/attendance_queue.db"
QueueEnsureAction = Literal["ok", "recovered", "reset", "failed"]

_CORRUPT_RE = re.compile(
    r"malformed|corrupt|not a database|file is not a database|disk image",
    re.IGNORECASE,
)


def is_sqlite_corruption_error(exc: BaseException) -> bool:
    """True when ``exc`` looks like SQLite file corruption."""
    return bool(_CORRUPT_RE.search(str(exc)))


def _resolve_db_path(db_path: str) -> Path:
    return Path(str(db_path)).expanduser().resolve()


def _db_related_files(db_path: str) -> List[Path]:
    """Main DB file plus WAL/SHM siblings when present."""
    main = _resolve_db_path(db_path)
    paths = [main]
    for suffix in ("-wal", "-shm"):
        sibling = Path(str(main) + suffix)
        if sibling.is_file():
            paths.append(sibling)
    return paths


def _recovery_lock_path(db_path: str) -> Path:
    return _resolve_db_path(db_path).parent / ".queue_recovery.lock"


@contextmanager
def _recovery_lock(db_path: str, wait_s: float = 120.0):
    """Serialize queue recovery across PM2 restart storms."""
    lock_path = _recovery_lock_path(db_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(1.0, wait_s)
    fh = open(lock_path, "a+", encoding="utf-8")
    acquired = False
    try:
        import fcntl

        while time.monotonic() < deadline:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except BlockingIOError:
                time.sleep(0.25)
        if not acquired:
            raise TimeoutError(
                f"Timed out waiting for queue recovery lock ({lock_path})"
            )
        yield
    finally:
        if acquired:
            try:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
        try:
            fh.close()
        except Exception:
            pass


def verify_sqlite_queue_db(db_path: str) -> tuple[bool, str]:
    """Return ``(True, 'ok')`` if ``db_path`` is absent or passes ``PRAGMA quick_check``.

    Call this before constructing ``AttendanceQueue`` so a corrupted queue
    file does not trap the daemon in a tight PM2 restart / log-spam loop.
    """
    path = Path(str(db_path)).expanduser()
    if not path.is_file():
        return True, "ok"
    try:
        uri = path.resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=15.0)
    except sqlite3.Error as exc:
        return False, str(exc)
    try:
        rows = list(conn.execute("PRAGMA quick_check;"))
    except sqlite3.Error as exc:
        return False, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    if not rows:
        return False, "PRAGMA quick_check returned no rows"
    messages = [str(r[0]) for r in rows]
    if len(messages) == 1 and messages[0] == "ok":
        return True, "ok"
    return False, "; ".join(messages)


def checkpoint_wal(db_path: str, truncate: bool = True) -> None:
    """Merge WAL pages into the main DB file (reduces corruption risk)."""
    mode = "TRUNCATE" if truncate else "PASSIVE"
    path = _resolve_db_path(db_path)
    if not path.is_file():
        return
    conn = sqlite3.connect(str(path), timeout=30.0)
    try:
        conn.execute(f"PRAGMA wal_checkpoint({mode});")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _quarantine_db_files(db_path: str, label: str) -> Path:
    """Move the queue DB (+ WAL/SHM) into a timestamped backup folder."""
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    safe_label = re.sub(r"[^A-Za-z0-9._-]+", "_", label)[:40] or "corrupt"
    backup_dir = _resolve_db_path(db_path).parent / "queue_backups" / f"{stamp}_{safe_label}"
    backup_dir.mkdir(parents=True, exist_ok=False)
    moved = 0
    for src in _db_related_files(db_path):
        if not src.is_file():
            continue
        dest = backup_dir / src.name
        shutil.move(str(src), str(dest))
        moved += 1
    if moved == 0:
        shutil.rmtree(backup_dir, ignore_errors=True)
    return backup_dir


def _sqlite3_cli_available() -> Optional[str]:
    for candidate in ("sqlite3",):
        path = shutil.which(candidate)
        if path:
            return path
    return None


def _attempt_sqlite_recover(source_db: Path, dest_db: Path) -> tuple[bool, str]:
    """Use the ``sqlite3`` CLI ``.recover`` to rebuild a damaged database."""
    cli = _sqlite3_cli_available()
    if not cli:
        return False, "sqlite3 CLI not installed"
    if not source_db.is_file():
        return False, f"source missing: {source_db}"
    dest_db.parent.mkdir(parents=True, exist_ok=True)
    if dest_db.exists():
        dest_db.unlink()
    try:
        with open(dest_db, "wb") as out_fh:
            proc = subprocess.run(
                [cli, str(source_db), ".recover"],
                stdout=out_fh,
                stderr=subprocess.PIPE,
                check=False,
                timeout=600,
            )
        if proc.returncode != 0:
            err = (proc.stderr or b"").decode("utf-8", errors="replace")[:500]
            if dest_db.exists():
                dest_db.unlink()
            return False, err or f"sqlite3 .recover exit {proc.returncode}"
    except subprocess.TimeoutExpired:
        if dest_db.exists():
            dest_db.unlink()
        return False, "sqlite3 .recover timed out"
    except Exception as exc:
        if dest_db.exists():
            dest_db.unlink()
        return False, str(exc)

    ok, msg = verify_sqlite_queue_db(str(dest_db))
    if not ok:
        if dest_db.exists():
            dest_db.unlink()
        return False, f"recovered file failed quick_check: {msg}"
    return True, "ok"


def recover_or_reset_queue_db(db_path: str, reason: str) -> Tuple[QueueEnsureAction, str]:
    """Backup a damaged queue, try ``.recover``, else start with an empty DB."""
    path = _resolve_db_path(db_path)
    with _recovery_lock(str(path)):
        ok, msg = verify_sqlite_queue_db(str(path))
        if ok:
            return "ok", "queue already healthy"

        backup_dir: Optional[Path] = None
        if path.is_file() or Path(str(path) + "-wal").is_file():
            try:
                backup_dir = _quarantine_db_files(str(path), reason)
                logger.warning(
                    "Quarantined corrupt queue files to %s (%s)",
                    backup_dir,
                    msg,
                )
            except Exception as exc:
                logger.error("Failed to quarantine corrupt queue: %s", exc)
                for sibling in _db_related_files(str(path)):
                    try:
                        sibling.unlink(missing_ok=True)
                    except Exception:
                        pass

        if backup_dir and backup_dir.is_dir():
            corrupt_copy = backup_dir / path.name
            recovered_copy = backup_dir / f"{path.stem}_recovered{path.suffix}"
            rec_ok, rec_msg = _attempt_sqlite_recover(corrupt_copy, recovered_copy)
            if rec_ok:
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(recovered_copy), str(path))
                ok2, msg2 = verify_sqlite_queue_db(str(path))
                if ok2:
                    return (
                        "recovered",
                        f"restored from backup via sqlite3 .recover "
                        f"(backup: {backup_dir})",
                    )
                logger.error(
                    "Recovered DB failed verification after install: %s", msg2
                )
                for sibling in _db_related_files(str(path)):
                    try:
                        sibling.unlink(missing_ok=True)
                    except Exception:
                        pass
            else:
                logger.warning("sqlite3 .recover failed: %s", rec_msg)

        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_file():
            ok3, msg3 = verify_sqlite_queue_db(str(path))
            if ok3:
                return "ok", "queue healthy after cleanup"
        return (
            "reset",
            f"removed corrupt queue; fresh DB will be created on next open "
            f"(backup: {backup_dir or 'none'})",
        )


def ensure_sqlite_queue_db(db_path: str) -> Tuple[bool, QueueEnsureAction, str]:
    """Verify the queue DB and auto-repair when corrupt."""
    ok, msg = verify_sqlite_queue_db(db_path)
    if ok:
        return True, "ok", "ok"
    try:
        action, detail = recover_or_reset_queue_db(db_path, msg[:120])
    except Exception as exc:
        logger.exception("Queue auto-repair failed")
        return False, "failed", str(exc)
    if action == "failed":
        return False, "failed", detail
    ok2, msg2 = verify_sqlite_queue_db(db_path)
    if ok2 or action == "reset":
        return True, action, detail if action != "ok" else msg2
    return False, "failed", f"still unusable after {action}: {msg2}"


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

    def count_synced(self) -> int:
        with self._conn() as conn:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM attendance_queue WHERE synced = 1;"
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
    def purge_synced_older_than(self, days: int = 0) -> int:
        """Delete synced records older than ``days`` days.

        Pass ``days <= 0`` to **disable** purging entirely. The durable queue
        is then a permanent historical record of every attendance event the
        system has ever seen, which is the project's default behaviour.
        """
        days = int(days)
        if days <= 0:
            return 0
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
