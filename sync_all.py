#!/usr/bin/env python3
"""
sync_all.py
Pull attendance logs from each configured ZKTeco device and feed them into the
durable local queue (``storage.AttendanceQueue``). The always-on daemon
(``main.py``) drains that queue to the ERP with retries, so this script's job
is just "make sure no device-resident records are missing from the queue".

Behavior
--------
* Idempotent: re-runs never produce duplicates because enqueue uses the
  natural key ``(device_id, user_id, timestamp, status, punch)``.
* Optional date filter via ``--from`` / ``--to`` (inclusive, by date).
* As a fallback (e.g. when the daemon is not running yet at boot time),
  also pushes directly to the ERP in chunks. Successfully pushed rows are
  marked synced so the daemon won't re-send them.
* Exits non-zero (2) on partial failure so ``boot_sync_30d.py`` and any
  CI / cron caller can detect problems instead of trusting "exit 0".
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

import httpx
from zk import ZK

from storage import DEFAULT_DB_PATH, AttendanceQueue, ensure_sqlite_queue_db


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Pull all attendance logs from ZKTeco devices, enqueue into the "
            "local durable queue, and push to the ERP."
        )
    )
    p.add_argument("--from", dest="from_date",
                   help="Start date (YYYY-MM-DD). If omitted, fetch all logs.")
    p.add_argument("--to", dest="to_date",
                   help="End date (YYYY-MM-DD). If omitted, up to now.")
    p.add_argument("--device-id", type=int,
                   help="Only sync a specific device_id from config.json.")
    p.add_argument("--chunk", type=int, default=500,
                   help="Batch size to push per request (default: 500).")
    p.add_argument("--retries", type=int, default=3,
                   help="HTTP retries per batch (default: 3).")
    p.add_argument("--no-push", action="store_true",
                   help="Only enqueue locally; skip direct ERP push (let the "
                        "daemon drain the queue).")
    p.add_argument("--log-level", default=None,
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="Override log level for this run.")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open("config.json", "r") as f:
        return json.load(f)


def setup_logging(level_name: Optional[str], cfg: dict) -> None:
    # Keep sync_all chatty by default for desktop/manual runs. If the operator
    # passes --log-level, honor it.
    level_str = level_name or "INFO"
    logging.basicConfig(
        level=getattr(logging, str(level_str).upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def parse_date(d: Optional[str]) -> Optional[datetime]:
    if not d:
        return None
    return datetime.strptime(d, "%Y-%m-%d")


def parse_date_end(d: Optional[str]) -> Optional[datetime]:
    """Inclusive end-of-day for ``--to`` date filters."""
    dt = parse_date(d)
    if dt is None:
        return None
    return dt + timedelta(days=1) - timedelta(seconds=1)


def progress(msg: str) -> None:
    """Operator-facing line on stdout (desktop / manual runs)."""
    print(msg, flush=True)


class ProgressHeartbeat:
    """Print elapsed-time updates while a blocking device call runs."""

    def __init__(self, phase: str, interval_s: float = 10.0) -> None:
        self._phase = phase
        self._interval_s = interval_s
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._started = 0.0

    def set_phase(self, phase: str) -> None:
        self._phase = phase
        self._started = time.monotonic()

    def __enter__(self) -> "ProgressHeartbeat":
        self._started = time.monotonic()

        def _loop() -> None:
            while not self._stop.wait(self._interval_s):
                elapsed = int(time.monotonic() - self._started)
                progress(
                    f"   … still {self._phase} ({elapsed}s elapsed)"
                )

        self._thread = threading.Thread(target=_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


def device_ping_ok(ip: str, wait_s: int = 2) -> bool:
    """Quick reachability check before opening a ZKTeco session."""
    try:
        rc = subprocess.call(
            ["ping", "-c", "1", "-W", str(max(1, wait_s)), ip],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return rc == 0
    except Exception:
        return False


def in_range(
    ts: datetime,
    start: Optional[datetime],
    end: Optional[datetime],
) -> bool:
    if start and ts < start:
        return False
    if end and ts > end:
        return False
    return True


def chunked(iterable: List[dict], size: int) -> Iterable[List[dict]]:
    size = max(1, size)
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def _safe_password(device: dict) -> int:
    pw = device.get("password", 0)
    if isinstance(pw, str):
        if not pw:
            return 0
        try:
            return int(pw)
        except ValueError:
            return 0
    try:
        return int(pw or 0)
    except Exception:
        return 0


def push_batch(
    endpoint: str,
    batch: Iterable[dict],
    retries: int = 3,
    timeout: int = 60,
) -> bool:
    payload = {"Json": list(batch)}
    attempt = 0
    backoff = 2
    while attempt <= retries:
        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.post(
                    endpoint,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
            if 200 <= resp.status_code < 300:
                logging.info(
                    f"Pushed {len(payload['Json'])} records (HTTP "
                    f"{resp.status_code})."
                )
                return True
            logging.error(
                f"Push failed (HTTP {resp.status_code}): "
                f"{resp.text[:300]}"
            )
        except Exception as exc:
            logging.error(f"HTTP push error: {exc}")
        attempt += 1
        if attempt <= retries:
            logging.info(
                f"Retrying in {backoff}s (attempt {attempt}/{retries})..."
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    return False


def collect_device_logs(device: dict) -> Optional[List[dict]]:
    """Fetch ALL logs from a device.

    ZKTeco devices always send their full on-device history; the date window
    is applied afterward in Python. ``get_attendance()`` can take many minutes
    on busy devices — heartbeats keep the operator informed.

    Returns ``None`` if the device could not be read (connection errors, etc.).
    Returns ``[]`` if the read succeeded but the device had no attendance rows.
    """
    dev_id = device["device_id"]
    ip = device["ip_address"]
    port = int(device.get("port", 4370) or 4370)
    timeout = int(device.get("timeout", 100) or 100)

    progress(f"   → Checking network reachability ({ip})...")
    if not device_ping_ok(ip):
        progress(
            f"   ✗ Device not reachable via ping ({ip}). "
            f"Check power, cable/Wi‑Fi, and IP in config.json."
        )
        logging.error(f"[{dev_id}] ping failed for {ip}")
        return None
    progress(f"   → Ping OK. Opening ZKTeco session on {ip}:{port}...")

    zk = ZK(
        ip,
        port=port,
        timeout=timeout,
        password=_safe_password(device),
        force_udp=bool(device.get("force_udp", False)),
        ommit_ping=bool(device.get("ommit_ping", False)),
    )
    conn = None
    try:
        with ProgressHeartbeat(f"connecting to {ip}:{port}"):
            conn = zk.connect()
        if not conn:
            progress(
                f"   ✗ Could not connect to {ip}:{port} "
                f"(timeout={timeout}s). Check port/password."
            )
            logging.error(f"[{dev_id}] connect() returned None")
            return None

        progress(
            f"   → Connected. Downloading ALL punches stored on device "
            f"(not just 7/60 days — device protocol limitation)..."
        )
        progress(
            "   → This step can take 5–20+ minutes on busy devices; "
            "heartbeat lines below mean it is still working."
        )
        with ProgressHeartbeat(f"downloading attendance from {ip}"):
            conn.enable_device()
            logs = conn.get_attendance() or []

        progress(f"   → Download complete: {len(logs):,} punch(es) on device.")
        logging.info(f"[{dev_id}] Retrieved {len(logs)} raw logs from {ip}")

        out: List[dict] = []
        for log in logs:
            try:
                out.append(
                    {
                        "device_id": dev_id,
                        "user_id": int(log.user_id),
                        "timestamp": log.timestamp.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "status": int(getattr(log, "status", 0) or 0),
                        "punch": int(getattr(log, "punch", 0) or 0),
                    }
                )
            except Exception as exc:
                logging.warning(f"[{dev_id}] skip malformed log: {exc}")
        return out
    except Exception as exc:
        progress(f"   ✗ Device read failed: {exc}")
        logging.error(f"[{dev_id}] Error collecting logs: {exc}")
        return None
    finally:
        if conn is not None:
            try:
                conn.disconnect()
                logging.debug(f"[{dev_id}] Disconnected from {ip}")
            except Exception as exc:
                logging.warning(f"[{dev_id}] disconnect issue: {exc}")


def _ids_for_records(
    queue: AttendanceQueue,
    records: List[dict],
) -> List[int]:
    """Look up the queue ids for the given records (matches natural key)."""
    if not records:
        return []
    ids: List[int] = []
    sql = (
        "SELECT id FROM attendance_queue "
        "WHERE device_id=? AND user_id=? AND timestamp=? "
        "  AND status=? AND punch=? AND synced=0;"
    )
    with queue._conn() as conn:                              # noqa: SLF001
        for r in records:
            n = AttendanceQueue._normalize(r)                # noqa: SLF001
            if n is None:
                continue
            row = conn.execute(sql, n).fetchone()
            if row is not None:
                ids.append(int(row["id"]))
    return ids


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    config = load_config()
    setup_logging(args.log_level, config)

    endpoint = str(config.get("endpoint", "")).strip()
    if not endpoint:
        logging.error("config.json missing 'endpoint'")
        return 2

    devices = list(config.get("devices") or [])
    if args.device_id is not None:
        devices = [d for d in devices if d.get("device_id") == args.device_id]
        if not devices:
            logging.error(f"No device with device_id={args.device_id}")
            return 2

    start_dt = parse_date(args.from_date)
    end_dt = parse_date_end(args.to_date)

    sync_cfg = config.get("sync") or {}
    db_path = str(sync_cfg.get("db_path", DEFAULT_DB_PATH))
    ok_sql, sql_action, sql_detail = ensure_sqlite_queue_db(db_path)
    if not ok_sql:
        logging.critical(
            "SQLite queue database is corrupt and auto-repair failed (%s). "
            "Path: %s. Run scripts/repair_queue_db.py on the server.",
            sql_detail,
            db_path,
        )
        return 2
    if sql_action in ("recovered", "reset"):
        logging.warning(
            "SQLite queue was auto-repaired before sync (%s): %s",
            sql_action,
            sql_detail,
        )
    queue = AttendanceQueue(db_path)

    overall_ok = True
    total_collected = 0
    total_enqueued = 0
    total_pushed = 0
    devices_ok = 0
    devices_failed = 0
    n_devices = len(devices)

    range_label = "all dates"
    if args.from_date or args.to_date:
        range_label = f"{args.from_date or '…'} → {args.to_date or '…'}"

    progress("")
    progress("=" * 62)
    progress("  ATTENDANCE SYNC — pulling from biometric devices")
    progress("=" * 62)
    progress(f"  Devices      : {n_devices}")
    progress(f"  Date range   : {range_label}")
    progress(
        f"  ERP push     : "
        f"{'skipped (daemon will push)' if args.no_push else 'direct from this run'}"
    )
    progress("")

    try:
        pending_before = queue.count_unsynced()
    except Exception:
        pending_before = -1

    for idx, device in enumerate(devices, start=1):
        dev_id = device.get("device_id")
        dev_ip = device.get("ip_address", "?")
        progress(f"── Device {idx}/{n_devices} │ ID {dev_id} │ {dev_ip} ──")
        logging.info(f"[{dev_id}] Starting device {idx}/{n_devices}")

        try:
            raw_logs = collect_device_logs(device)
        except Exception as exc:
            logging.error(f"[{dev_id}] collect failed: {exc}")
            progress(f"   ✗ FAILED — could not read device: {exc}")
            overall_ok = False
            devices_failed += 1
            continue

        if raw_logs is None:
            progress("   ✗ FAILED — device unreachable or read error")
            overall_ok = False
            devices_failed += 1
            continue

        raw_count = len(raw_logs)
        if not raw_logs:
            progress("   ○ Device read OK — no punches stored on device")
            devices_ok += 1
            continue

        if start_dt or end_dt:
            if len(raw_logs) > 1000:
                progress(
                    f"   → Filtering {len(raw_logs):,} punches to date range "
                    f"{args.from_date or '…'} → {args.to_date or '…'}..."
                )
            filtered: List[dict] = []
            for rec in raw_logs:
                try:
                    ts = datetime.strptime(
                        rec["timestamp"], "%Y-%m-%d %H:%M:%S"
                    )
                    if in_range(ts, start_dt, end_dt):
                        filtered.append(rec)
                except Exception as exc:
                    logging.warning(
                        f"[{dev_id}] timestamp parse error: {exc}"
                    )
            logs = filtered
            logging.info(
                f"[{dev_id}] Filtered: {len(logs)} / {len(raw_logs)}"
            )
        else:
            logs = raw_logs

        try:
            logs.sort(key=lambda r: r["timestamp"])
        except Exception:
            pass

        in_range_count = len(logs)
        total_collected += in_range_count

        if not logs:
            progress(
                f"   ○ Read {raw_count:,} punch(es) on device — "
                f"0 in selected date range"
            )
            devices_ok += 1
            continue

        try:
            new = queue.enqueue_many(logs)
            total_enqueued += new
            already = in_range_count - new
            pending_now = queue.count_unsynced()
            logging.info(
                f"[{dev_id}] Enqueued {new} new (of {in_range_count}), "
                f"pending={pending_now}"
            )
            progress(f"   ✓ Read from device     : {raw_count:,} total punch(es)")
            progress(f"   ✓ In date range        : {in_range_count:,}")
            progress(f"   ✓ New in local logbook : {new:,}")
            if already > 0:
                progress(f"   · Already in logbook : {already:,} (skipped)")
            progress(f"   · Logbook pending now : {pending_now:,}")
            devices_ok += 1
        except Exception as exc:
            logging.error(f"[{dev_id}] enqueue failed: {exc}")
            progress(f"   ✗ FAILED — could not save to logbook: {exc}")
            overall_ok = False
            devices_failed += 1
            continue

        if args.no_push:
            progress(
                f"   → Running totals: collected={total_collected:,}  "
                f"new={total_enqueued:,}  "
                f"devices done={idx}/{n_devices}"
            )
            progress("")
            continue

        # Direct push fallback so this script is useful even if the daemon
        # is not running yet (boot order, manual recovery, etc.). Mark rows
        # synced once the ERP confirms 2xx, so the daemon won't re-send.
        ids_unsynced = _ids_for_records(queue, logs)
        if not ids_unsynced:
            progress("   · Nothing to push for this device (already synced)")
            progress("")
            continue

        with queue._conn() as conn:                          # noqa: SLF001
            placeholders = ",".join("?" for _ in ids_unsynced)
            rows = conn.execute(
                f"SELECT id, device_id, user_id, timestamp, status, punch "
                f"  FROM attendance_queue "
                f" WHERE id IN ({placeholders}) AND synced = 0 "
                f" ORDER BY id ASC;",
                ids_unsynced,
            ).fetchall()
        records_with_ids = [dict(r) for r in rows]

        chunk_size = max(1, int(args.chunk or 500))
        n_batches = (len(records_with_ids) + chunk_size - 1) // chunk_size
        device_ok = True
        device_pushed = 0
        for batch_num, batch in enumerate(
            chunked(records_with_ids, chunk_size), start=1
        ):
            payload = [
                {
                    "device_id": r["device_id"],
                    "user_id": r["user_id"],
                    "timestamp": r["timestamp"],
                    "status": r["status"],
                    "punch": r["punch"],
                }
                for r in batch
            ]
            progress(
                f"   ↑ Pushing batch {batch_num}/{n_batches} "
                f"({len(batch)} records) to ERP..."
            )
            ok = push_batch(
                endpoint, payload, retries=int(args.retries or 3)
            )
            if ok:
                queue.mark_synced([r["id"] for r in batch])
                device_pushed += len(batch)
                total_pushed += len(batch)
                progress(
                    f"   ✓ Batch {batch_num}/{n_batches} OK "
                    f"({device_pushed:,}/{len(records_with_ids):,} for device)"
                )
            else:
                device_ok = False
                logging.error(
                    f"[{dev_id}] batch push failed; remaining records stay "
                    f"in the queue for the daemon to retry."
                )
                progress(f"   ✗ Batch {batch_num}/{n_batches} FAILED")
                break

        if not device_ok:
            overall_ok = False

        progress(
            f"   → Running totals: collected={total_collected:,}  "
            f"new={total_enqueued:,}  pushed={total_pushed:,}  "
            f"devices done={idx}/{n_devices}"
        )
        progress("")

    pending = -1
    synced_total = -1
    try:
        pending = queue.count_unsynced()
        synced_total = queue.count_synced()
    except Exception:
        pass

    progress("=" * 62)
    progress("  DEVICE PULL COMPLETE")
    progress("=" * 62)
    progress(f"  Devices OK / failed     : {devices_ok} / {devices_failed}")
    progress(f"  Punches in date range   : {total_collected:,}")
    progress(f"  New rows in logbook     : {total_enqueued:,}")
    if not args.no_push:
        progress(f"  Pushed to ERP (this run): {total_pushed:,}")
    if pending >= 0:
        progress(f"  Logbook pending (ERP)   : {pending:,}")
    if synced_total >= 0:
        progress(f"  Logbook already synced  : {synced_total:,}")
    if pending_before >= 0 and pending >= 0:
        delta_pending = pending - pending_before
        if delta_pending > 0:
            progress(
                f"  Net new pending         : +{delta_pending:,} "
                f"(was {pending_before:,} before this run)"
            )
    progress(
        f"  Status                  : "
        f"{'SUCCESS' if overall_ok else 'PARTIAL / ERRORS — see lines above'}"
    )
    progress("")

    logging.info(
        f"SYNC COMPLETE collected={total_collected} "
        f"newly_enqueued={total_enqueued} pushed={total_pushed} "
        f"pending_after={pending} overall_ok={overall_ok}"
    )
    return 0 if overall_ok else 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logging.error("Interrupted")
        sys.exit(130)
    except Exception as exc:
        logging.exception(f"Fatal error: {exc}")
        sys.exit(1)
