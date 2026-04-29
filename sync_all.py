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
import sys
import time
from datetime import datetime
from typing import Iterable, List, Optional

import httpx
from zk import ZK

from storage import DEFAULT_DB_PATH, AttendanceQueue


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


def parse_date(d: Optional[str]) -> Optional[datetime]:
    if not d:
        return None
    return datetime.strptime(d, "%Y-%m-%d")


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


def collect_device_logs(device: dict) -> List[dict]:
    """Fetch ALL logs from a device and return as a list of dicts."""
    zk = ZK(
        device["ip_address"],
        port=int(device.get("port", 4370) or 4370),
        timeout=int(device.get("timeout", 100) or 100),
        password=_safe_password(device),
        force_udp=bool(device.get("force_udp", False)),
        ommit_ping=bool(device.get("ommit_ping", False)),
    )
    conn = None
    try:
        logging.info(
            f"[{device['device_id']}] Connecting to {device['ip_address']}..."
        )
        conn = zk.connect()
        if not conn:
            logging.error(
                f"[{device['device_id']}] connect() returned None"
            )
            return []
        conn.enable_device()
        logs = conn.get_attendance() or []
        logging.info(
            f"[{device['device_id']}] Retrieved {len(logs)} raw logs."
        )
        out: List[dict] = []
        for log in logs:
            try:
                out.append(
                    {
                        "device_id": device["device_id"],
                        "user_id": int(log.user_id),
                        "timestamp": log.timestamp.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "status": int(getattr(log, "status", 0) or 0),
                        "punch": int(getattr(log, "punch", 0) or 0),
                    }
                )
            except Exception as exc:
                logging.warning(
                    f"[{device['device_id']}] skip malformed log: {exc}"
                )
        return out
    except Exception as exc:
        logging.error(
            f"[{device['device_id']}] Error collecting logs: {exc}"
        )
        return []
    finally:
        if conn is not None:
            try:
                conn.disconnect()
                logging.info(f"[{device['device_id']}] Disconnected.")
            except Exception as exc:
                logging.warning(
                    f"[{device['device_id']}] disconnect issue: {exc}"
                )


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
    end_dt = parse_date(args.to_date)

    sync_cfg = config.get("sync") or {}
    queue = AttendanceQueue(sync_cfg.get("db_path", DEFAULT_DB_PATH))

    overall_ok = True
    total_collected = 0
    total_enqueued = 0
    total_pushed = 0

    for idx, device in enumerate(devices, start=1):
        logging.info(
            f"[{device.get('device_id')}] Starting device {idx}/{len(devices)}"
        )
        try:
            raw_logs = collect_device_logs(device)
        except Exception as exc:
            logging.error(
                f"[{device.get('device_id')}] collect failed: {exc}"
            )
            overall_ok = False
            continue

        if not raw_logs:
            continue

        if start_dt or end_dt:
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
                        f"[{device.get('device_id')}] timestamp parse "
                        f"error: {exc}"
                    )
            logs = filtered
            logging.info(
                f"[{device.get('device_id')}] Filtered: {len(logs)} / "
                f"{len(raw_logs)}"
            )
        else:
            logs = raw_logs

        try:
            logs.sort(key=lambda r: r["timestamp"])
        except Exception:
            pass

        total_collected += len(logs)
        if not logs:
            continue

        try:
            new = queue.enqueue_many(logs)
            total_enqueued += new
            logging.info(
                f"[{device.get('device_id')}] Enqueued {new} new records "
                f"(of {len(logs)})."
            )
            try:
                pending_now = queue.count_unsynced()
                logging.info(
                    f"[{device.get('device_id')}] Queue pending now: "
                    f"{pending_now}"
                )
            except Exception:
                pass
        except Exception as exc:
            logging.error(
                f"[{device.get('device_id')}] enqueue failed: {exc}"
            )
            overall_ok = False
            continue

        if args.no_push:
            continue

        # Direct push fallback so this script is useful even if the daemon
        # is not running yet (boot order, manual recovery, etc.). Mark rows
        # synced once the ERP confirms 2xx, so the daemon won't re-send.
        ids_unsynced = _ids_for_records(queue, logs)
        if not ids_unsynced:
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
        device_ok = True
        for batch in chunked(records_with_ids, chunk_size):
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
            ok = push_batch(
                endpoint, payload, retries=int(args.retries or 3)
            )
            if ok:
                queue.mark_synced([r["id"] for r in batch])
                total_pushed += len(batch)
            else:
                device_ok = False
                logging.error(
                    f"[{device.get('device_id')}] batch push failed; "
                    f"remaining records remain in the local queue and the "
                    f"daemon will retry them."
                )
                break

        if not device_ok:
            overall_ok = False

    pending = -1
    try:
        pending = queue.count_unsynced()
    except Exception:
        pass

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
