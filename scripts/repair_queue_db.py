#!/usr/bin/env python3
"""
repair_queue_db.py
Verify the attendance SQLite queue and auto-repair if corrupt.

Safe to run while PM2 is stopped (recommended) or running — recovery uses
an exclusive lock so only one repair runs at a time.

Usage (on a school server):
  cd /path/to/attendance-ztech-python
  ./venv/bin/python scripts/repair_queue_db.py
"""

from __future__ import annotations

import json
import os
import sys

# Project root is parent of scripts/
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)
os.chdir(PROJECT_DIR)

from storage import (  # noqa: E402
    DEFAULT_DB_PATH,
    AttendanceQueue,
    checkpoint_wal,
    ensure_sqlite_queue_db,
    verify_sqlite_queue_db,
)


def main() -> int:
    config_path = os.path.join(PROJECT_DIR, "config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as fh:
            config = json.load(fh)
    except Exception as exc:
        print(f"ERROR: cannot read {config_path}: {exc}")
        return 1

    db_path = str((config.get("sync") or {}).get("db_path", DEFAULT_DB_PATH))
    if not os.path.isabs(db_path):
        db_path = os.path.join(PROJECT_DIR, db_path)

    print(f"Queue database: {db_path}")
    ok_before, msg_before = verify_sqlite_queue_db(db_path)
    if ok_before:
        print("Status: OK (quick_check passed)")
        try:
            checkpoint_wal(db_path)
            print("WAL checkpoint: done")
        except Exception as exc:
            print(f"WAL checkpoint warning: {exc}")
        stats = AttendanceQueue(db_path).stats()
        print(
            f"Records: pending={stats['pending']} synced={stats['synced']} "
            f"total={stats['total']}"
        )
        return 0

    print(f"Status: DAMAGED ({msg_before})")
    print("Attempting automatic repair…")
    ok, action, detail = ensure_sqlite_queue_db(db_path)
    if not ok:
        print(f"Repair FAILED: {detail}")
        return 1

    print(f"Repair OK — action={action}")
    print(detail)
    AttendanceQueue(db_path)
    stats = AttendanceQueue(db_path).stats()
    print(
        f"Records after repair: pending={stats['pending']} "
        f"synced={stats['synced']} total={stats['total']}"
    )
    if action == "reset":
        print()
        print(
            "Queue was reset to empty. Run a device backfill, e.g.:"
        )
        print(f"  {sys.executable} sync_all.py --from YYYY-MM-DD --to YYYY-MM-DD")
        print("  or scripts/sync_60_days.sh")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
