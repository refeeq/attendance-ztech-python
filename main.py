"""
main.py
Attendance ZTech daemon.

Pipeline
--------
1. One subprocess per ZKTeco device streams real-time punches into a durable
   SQLite queue (``storage.AttendanceQueue``). Each subprocess auto-reconnects
   on any error with exponential backoff and never exits silently.
2. A background pusher thread drains the queue to the ERP HTTP endpoint with
   retries + backoff. Records are only marked synced after the ERP confirms
   a 2xx response, so failures never lose data.
3. A watchdog respawns dead device subprocesses every ~30s, and a slower
   "scheduled reconnect" cycle replaces all subprocesses periodically (covers
   the case where ZKTeco's TCP stack silently drops the live capture).
4. End-of-Day (23:55-23:59) re-pulls each device's stored punches and
   re-enqueues them idempotently as a safety net for any RT punches missed
   by network glitches. Boot recovery does the same with a 3-day lookback
   if the daemon was offline for a while.
5. ``boot_sync_30d.py`` (a separate oneshot service at boot time) provides
   60-day historical backfill via ``sync_all.py``; that path also feeds the
   same durable queue.

The queue is the single source of truth for "did we ship it?". Restarts,
crashes, and power-loss never discard pending records.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from multiprocessing import Process
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from zk import ZK

from storage import (
    DEFAULT_DB_PATH,
    AttendanceQueue,
    checkpoint_wal,
    ensure_sqlite_queue_db,
    is_sqlite_corruption_error,
    verify_sqlite_queue_db,
)
from telegram_notifier import TelegramNotifier


# ---------------------------------------------------------------------------
# Logging (rotating files + console)
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    log_dir = Path("logs")
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log = logging.getLogger("AttendanceZTech")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    log.propagate = False

    def _add_rotating(path: Path, max_mb: int = 10, backups: int = 5) -> None:
        try:
            handler = RotatingFileHandler(
                str(path),
                maxBytes=max_mb * 1024 * 1024,
                backupCount=backups,
                encoding="utf-8",
            )
            handler.setFormatter(formatter)
            log.addHandler(handler)
        except Exception as exc:
            sys.stderr.write(f"[logging] failed to attach {path}: {exc}\n")

    _add_rotating(log_dir / "attendance.log")
    _add_rotating(Path("log.txt"), max_mb=10, backups=3)

    desktop = Path(os.path.expanduser("~/Desktop"))
    if desktop.exists():
        try:
            d = desktop / "AttendanceZTech Logs"
            d.mkdir(exist_ok=True)
            _add_rotating(d / "attendance.log", max_mb=10, backups=3)
        except Exception:
            pass

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)

    # httpx logs every request at INFO (including URLs with secrets).
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    return log


logger = setup_logging()
logger.info(
    "=== Attendance ZTech System Started ===\n"
    f"Timestamp: {datetime.now()}\n"
    f"Python: {sys.version.split()[0]}\n"
    f"CWD: {os.getcwd()}\n"
    "======================================="
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CONFIG_PATH = os.environ.get("ATTENDANCE_CONFIG", "config.json")


def load_config() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as exc:
        logger.error(f"Failed to load {CONFIG_PATH}: {exc}")
        sys.exit(1)


config = load_config()

ENDPOINT: str = str(config.get("endpoint", "")).strip()
DEVICES: List[dict] = list(config.get("devices") or [])
SYSTEM_NAME: str = str(config.get("name", "Attendance System"))

_LOG_LEVEL = str(config.get("log_level", "INFO")).upper()
try:
    logger.setLevel(getattr(logging, _LOG_LEVEL))
except Exception:
    logger.setLevel(logging.INFO)

# legacy: minimum pending count that triggers an immediate push
LEGACY_BUFFER_LIMIT = max(1, int(config.get("buffer_limit", 50) or 50))

_SYNC_CFG: dict = config.get("sync") or {}
PUSH_BATCH_SIZE      = max(1,  int(_SYNC_CFG.get("batch_size", 200)))
PUSH_INTERVAL_S      = max(1,  int(_SYNC_CFG.get("push_interval_s", 15)))
PUSH_TIMEOUT_S       = max(5,  int(_SYNC_CFG.get("push_timeout_s", 60)))
PUSH_RETRIES         = max(1,  int(_SYNC_CFG.get("push_retries", 5)))
PURGE_DAYS           = int(_SYNC_CFG.get("purge_synced_after_days", 0))
# Telegram "data push OK" during backlog drain: at most one message per interval
# so large queue drain does not hit Telegram 429.
TG_DATA_PUSH_PROGRESS_INTERVAL_S = 300
WATCHDOG_INTERVAL_S  = max(5,  int(_SYNC_CFG.get("watchdog_interval_s", 30)))
RECONNECT_INTERVAL_S = max(60, int(_SYNC_CFG.get("reconnect_interval_min", 15)) * 60)
EOD_LOOKBACK_DAYS    = max(1,  int(_SYNC_CFG.get("eod_lookback_days", 1)))
BOOT_RECOVERY_DAYS   = max(1,  int(_SYNC_CFG.get("boot_recovery_days", 3)))
POST_DB_RESET_RECOVERY_DAYS = max(
    BOOT_RECOVERY_DAYS,
    int(_SYNC_CFG.get("post_db_reset_recovery_days", 60)),
)
WAL_CHECKPOINT_INTERVAL_S = max(
    300, int(_SYNC_CFG.get("wal_checkpoint_interval_s", 3600))
)
DB_INTEGRITY_CHECK_INTERVAL_S = max(
    3600, int(_SYNC_CFG.get("db_integrity_check_interval_s", 86400))
)
DB_PATH              = str(_SYNC_CFG.get("db_path", DEFAULT_DB_PATH))

if not ENDPOINT:
    logger.error("config.json missing 'endpoint'. Refusing to start.")
    sys.exit(1)
if not DEVICES:
    logger.error("config.json has no devices configured. Refusing to start.")
    sys.exit(1)

_telegram_cfg = config.get("telegram", {}) or {}
telegram_notifier = TelegramNotifier(
    bot_token=_telegram_cfg.get("bot_token", ""),
    chat_id=_telegram_cfg.get("chat_id", ""),
    enabled=bool(_telegram_cfg.get("enabled", False)),
    notification_settings=_telegram_cfg.get("notifications", {}),
    system_name=SYSTEM_NAME,
)

_retention = (
    "forever" if PURGE_DAYS <= 0 else f"{PURGE_DAYS}d after sync"
)
logger.info(
    f"Config: devices={len(DEVICES)} endpoint={ENDPOINT} "
    f"batch_size={PUSH_BATCH_SIZE} push_interval_s={PUSH_INTERVAL_S} "
    f"reconnect_interval_s={RECONNECT_INTERVAL_S} db={DB_PATH} "
    f"retention={_retention} "
    f"telegram={'ON' if telegram_notifier.enabled else 'OFF'}"
)


# ---------------------------------------------------------------------------
# Telegram (with per-kind throttling so the chat never floods)
# ---------------------------------------------------------------------------

_tg_lock = threading.Lock()
_tg_last_sent: Dict[str, float] = {}


def tg_send(
    message: str,
    *,
    kind: str = "default",
    min_interval_s: int = 0,
    retries: int = 3,
    backoff_s: int = 2,
) -> None:
    """Send a Telegram message, prefixing with the system name once.

    ``min_interval_s`` throttles by ``kind`` so high-frequency events
    (per-batch push, per-watchdog respawn) cannot spam the chat.
    """
    if not telegram_notifier.enabled:
        return
    if min_interval_s > 0:
        with _tg_lock:
            now = time.time()
            last = _tg_last_sent.get(kind, 0.0)
            if now - last < min_interval_s:
                return
            _tg_last_sent[kind] = now

    if "<b>" in message and f"{SYSTEM_NAME} -" not in message:
        message = message.replace("<b>", f"<b>{SYSTEM_NAME} - ", 1)

    for i in range(retries):
        try:
            telegram_notifier.send_message_sync(message)
            return
        except Exception as exc:
            logger.warning(
                f"Telegram send failed (attempt {i + 1}/{retries}): {exc}"
            )
            time.sleep(backoff_s * (i + 1))


# ---------------------------------------------------------------------------
# Network readiness helpers
# ---------------------------------------------------------------------------

def wait_for_network(max_wait_s: int = 120) -> bool:
    """Best-effort: return True as soon as any common host is reachable."""
    start = time.time()
    while time.time() - start < max_wait_s:
        for host, port in (
            ("api.telegram.org", 443),
            ("google.com", 443),
            ("1.1.1.1", 53),
            ("8.8.8.8", 53),
        ):
            try:
                with socket.create_connection((host, port), timeout=3):
                    return True
            except OSError:
                continue
        time.sleep(3)
    return False


def any_device_ping_ok(hosts: List[str], max_wait_s: int = 60) -> bool:
    start = time.time()
    while time.time() - start < max_wait_s:
        for h in hosts:
            if not h:
                continue
            try:
                rc = subprocess.call(
                    ["ping", "-c", "1", "-W", "1", h],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                if rc == 0:
                    return True
            except Exception:
                pass
        time.sleep(3)
    return False


# ---------------------------------------------------------------------------
# Queue (parent-side instance)
# ---------------------------------------------------------------------------

_db_ready, _db_action, _db_detail = ensure_sqlite_queue_db(DB_PATH)
if not _db_ready:
    _fix = (
        f"SQLite queue unusable after auto-repair: {_db_detail}. "
        f"File: {DB_PATH}. Check disk space and permissions, then run "
        f"scripts/repair_queue_db.py or sync_all for backfill."
    )
    logger.critical(_fix)
    try:
        tg_send(
            f"🔴 <b>Queue database repair failed</b>\n"
            f"<code>{_db_detail[:400]}</code>\n"
            f"Path: <code>{DB_PATH}</code>\n"
            "Daemon refusing to start.",
            kind="sqlite_corrupt",
            min_interval_s=0,
        )
    except Exception:
        pass
    sys.exit(1)

if _db_action == "recovered":
    logger.warning("SQLite queue auto-recovered: %s", _db_detail)
    try:
        tg_send(
            f"🟡 <b>Queue database auto-recovered</b>\n"
            f"<code>{_db_detail[:400]}</code>\n"
            f"Path: <code>{DB_PATH}</code>\n"
            "Daemon starting normally.",
            kind="sqlite_recovered",
            min_interval_s=0,
        )
    except Exception:
        pass
elif _db_action == "reset":
    logger.warning(
        "SQLite queue was reset (empty). Backfill will run: %s", _db_detail
    )
    try:
        tg_send(
            f"🟠 <b>Queue database reset</b>\n"
            f"Corrupt local queue was quarantined and replaced with a "
            f"fresh empty database.\n"
            f"<code>{_db_detail[:400]}</code>\n"
            f"Path: <code>{DB_PATH}</code>\n"
            f"A {POST_DB_RESET_RECOVERY_DAYS}-day device backfill will run "
            f"at startup.",
            kind="sqlite_reset",
            min_interval_s=0,
        )
    except Exception:
        pass

queue = AttendanceQueue(DB_PATH)
_queue_db_needs_extended_recovery = _db_action == "reset"


def _repair_queue_if_corrupt(context: str) -> bool:
    """Try to heal the queue in-process; return True if service can continue."""
    global _queue_db_needs_extended_recovery
    logger.critical(
        "SQLite corruption detected during %s — attempting auto-repair", context
    )
    try:
        tg_send(
            f"🟠 <b>Queue corruption at runtime</b>\n"
            f"Context: <code>{context}</code>\n"
            f"Attempting automatic repair…",
            kind="sqlite_runtime_repair",
            min_interval_s=0,
        )
    except Exception:
        pass
    ok, action, detail = ensure_sqlite_queue_db(DB_PATH)
    if not ok:
        logger.critical("Runtime queue repair failed: %s", detail)
        return False
    logger.warning("Runtime queue repair succeeded (%s): %s", action, detail)
    if action == "reset":
        _queue_db_needs_extended_recovery = True
    try:
        tg_send(
            f"✅ <b>Queue auto-repair OK</b>\n"
            f"Action: <code>{action}</code>\n"
            f"<code>{detail[:400]}</code>",
            kind="sqlite_runtime_repair_ok",
            min_interval_s=0,
        )
    except Exception:
        pass
    return True


# ---------------------------------------------------------------------------
# Device helpers
# ---------------------------------------------------------------------------

def _safe_password(device: dict) -> int:
    """Coerce a config password into an int; pyzk expects an int."""
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


def _record_from_zk(log_obj: Any, device_id: Any) -> Optional[Dict[str, Any]]:
    try:
        return {
            "device_id": int(device_id),
            "user_id": int(log_obj.user_id),
            "timestamp": log_obj.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "status": int(getattr(log_obj, "status", 0) or 0),
            "punch": int(getattr(log_obj, "punch", 0) or 0),
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# ERP push
# ---------------------------------------------------------------------------

def push_with_retries(
    records: List[dict],
    retries: Optional[int] = None,
    timeout: Optional[int] = None,
) -> Tuple[bool, Optional[str]]:
    """POST records to the ERP. Returns (ok, last_error)."""
    if not records:
        return True, None
    retries = retries or PUSH_RETRIES
    timeout = timeout or PUSH_TIMEOUT_S
    payload = {"Json": records}
    delay = 2
    last_err: Optional[str] = None
    for attempt in range(1, retries + 1):
        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.post(
                    ENDPOINT,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
            if 200 <= resp.status_code < 300:
                return True, None
            last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
            logger.warning(
                f"Push got {resp.status_code} (attempt {attempt}/{retries})"
            )
        except Exception as exc:
            last_err = f"{type(exc).__name__}: {exc}"
            logger.warning(
                f"Push exception (attempt {attempt}/{retries}): {exc}"
            )
        if attempt < retries:
            time.sleep(min(delay, 60))
            delay = min(delay * 2, 60)
    return False, last_err


def pusher_loop(stop_event: threading.Event) -> None:
    """Drain the queue to the ERP. Runs as a daemon thread in main process."""
    logger.info("📤 Pusher thread started")
    last_push_attempt = 0.0
    last_purge = 0.0
    consecutive_failures = 0

    while not stop_event.is_set():
        try:
            now = time.time()

            try:
                pending = queue.count_unsynced()
            except Exception as exc:
                logger.error(f"Pusher: count_unsynced failed: {exc}")
                if is_sqlite_corruption_error(exc):
                    _repair_queue_if_corrupt("pusher count_unsynced")
                stop_event.wait(timeout=5)
                continue

            if pending == 0:
                if PURGE_DAYS > 0 and now - last_purge >= 3600:
                    try:
                        deleted = queue.purge_synced_older_than(PURGE_DAYS)
                        if deleted:
                            logger.info(
                                f"🧽 Purged {deleted} synced records "
                                f"older than {PURGE_DAYS}d"
                            )
                    except Exception as exc:
                        logger.warning(f"Purge error: {exc}")
                        if is_sqlite_corruption_error(exc):
                            _repair_queue_if_corrupt("pusher purge")
                    last_purge = now
                stop_event.wait(timeout=1.0)
                continue

            should_push = (
                pending >= LEGACY_BUFFER_LIMIT
                or (now - last_push_attempt >= PUSH_INTERVAL_S)
            )
            if not should_push:
                stop_event.wait(timeout=1.0)
                continue

            last_push_attempt = now

            try:
                records = queue.fetch_unsynced(PUSH_BATCH_SIZE)
            except Exception as exc:
                logger.error(f"Pusher: fetch_unsynced failed: {exc}")
                if is_sqlite_corruption_error(exc):
                    _repair_queue_if_corrupt("pusher fetch_unsynced")
                stop_event.wait(timeout=5)
                continue

            if not records:
                continue

            ids = [r["id"] for r in records]
            payload = [
                {
                    "device_id": r["device_id"],
                    "user_id": r["user_id"],
                    "timestamp": r["timestamp"],
                    "status": r["status"],
                    "punch": r["punch"],
                }
                for r in records
            ]

            ok, err = push_with_retries(payload)
            if ok:
                try:
                    queue.mark_synced(ids)
                except Exception as exc:
                    logger.error(
                        f"Pusher: mark_synced failed (will retry): {exc}"
                    )
                    if is_sqlite_corruption_error(exc):
                        _repair_queue_if_corrupt("pusher mark_synced")
                    stop_event.wait(timeout=5)
                    continue

                logger.info(
                    f"✅ Synced {len(ids)} records "
                    f"(pending after: {max(0, pending - len(ids))})"
                )
                if consecutive_failures > 0:
                    tg_send(
                        f"✅ <b>Sync Recovered</b>\n"
                        f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}\n"
                        f"🧾 Records: {len(ids)}\n"
                        f"📦 Pending: {max(0, pending - len(ids))}",
                        kind="recovery",
                        min_interval_s=60,
                    )
                consecutive_failures = 0
                if telegram_notifier.is_notification_enabled("data_push"):
                    push_msg = telegram_notifier.data_push_message_html(
                        len(ids), True, records=records
                    )
                    pending_after = max(0, pending - len(ids))
                    if pending_after == 0:
                        tg_send(
                            push_msg,
                            kind="data_push_complete",
                            min_interval_s=0,
                        )
                    else:
                        tg_send(
                            push_msg,
                            kind="data_push_progress",
                            min_interval_s=TG_DATA_PUSH_PROGRESS_INTERVAL_S,
                        )
            else:
                try:
                    queue.mark_attempt_failed(ids, str(err))
                except Exception as exc2:
                    logger.error(f"Pusher: mark_attempt_failed error: {exc2}")
                consecutive_failures += 1
                logger.error(
                    f"❌ Sync failed (attempt #{consecutive_failures}, "
                    f"pending={pending}): {err}"
                )
                if telegram_notifier.enabled and (
                    telegram_notifier.is_notification_enabled("data_push")
                    or telegram_notifier.is_notification_enabled("errors")
                ):
                    fail_msg = telegram_notifier.data_push_message_html(
                        len(ids),
                        False,
                        records=records,
                        error=str(err)[:500],
                    )
                    tg_send(
                        fail_msg,
                        kind="push_failure",
                        min_interval_s=300,
                    )
                # Cool-off so we don't hammer a broken endpoint.
                cool_off = min(60, 5 + 5 * min(consecutive_failures, 6))
                stop_event.wait(timeout=cool_off)
        except Exception as exc:
            logger.exception(f"Pusher loop unexpected error: {exc}")
            stop_event.wait(timeout=5)

    logger.info("📤 Pusher thread stopped")


# ---------------------------------------------------------------------------
# Per-device real-time capture (subprocess target)
# ---------------------------------------------------------------------------

def capture_real_time_logs(device: dict, db_path: str) -> None:
    """Connect to a single ZKTeco device and stream punches into the queue.

    Runs as a daemon subprocess. Wrapped in a never-die outer loop so a
    single ZK / network hiccup does not silently kill capture; the parent
    watchdog also respawns the process if it ever exits.
    """
    sub_logger = logging.getLogger("AttendanceZTech")
    if not sub_logger.handlers:                       # spawn-mode children
        setup_logging()
        sub_logger = logging.getLogger("AttendanceZTech")

    device_id = device.get("device_id", "?")
    ip = device.get("ip_address", "?")
    port = int(device.get("port", 4370) or 4370)
    pwd = _safe_password(device)
    local_queue = AttendanceQueue(db_path)

    backoff = 5
    while True:
        conn = None
        try:
            sub_logger.info(
                f"🔌 [device {device_id}] Connecting to {ip}:{port}"
            )
            zk = ZK(
                ip,
                port=port,
                timeout=50,
                password=pwd,
                force_udp=False,
                ommit_ping=False,
            )
            conn = zk.connect()
            if not conn:
                raise RuntimeError("connect() returned None")
            conn.enable_device()
            sub_logger.info(
                f"✅ [device {device_id}] Connected, entering live capture"
            )
            backoff = 5

            for attendance in conn.live_capture():
                if attendance is None:
                    continue
                record = _record_from_zk(attendance, device_id)
                if record is None:
                    sub_logger.warning(
                        f"⚠️ [device {device_id}] skip malformed attendance"
                    )
                    continue
                try:
                    if local_queue.enqueue_one(record):
                        sub_logger.info(
                            f"🕘 [device {device_id}] punch user="
                            f"{record['user_id']} @ {record['timestamp']}"
                        )
                except Exception as exc:
                    sub_logger.error(
                        f"❌ [device {device_id}] enqueue failed: {exc}"
                    )
        except Exception as exc:
            sub_logger.error(
                f"❌ [device {device_id}] capture error: {exc}; "
                f"reconnecting in {backoff}s"
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)
        finally:
            if conn is not None:
                try:
                    conn.disconnect()
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# End-of-Day reconciliation (catches RT punches missed by network glitches)
# ---------------------------------------------------------------------------

def _eod_one_device(device: dict, target_dates: set) -> int:
    device_id = device.get("device_id", "?")
    ip = device.get("ip_address", "?")
    port = int(device.get("port", 4370) or 4370)
    pwd = _safe_password(device)

    logger.info(f"🧹 [device {device_id}] EoD pull from {ip}:{port}")
    zk = ZK(
        ip,
        port=port,
        timeout=100,
        password=pwd,
        force_udp=False,
        ommit_ping=False,
    )
    conn = zk.connect()
    if not conn:
        raise RuntimeError("connect() returned None")
    try:
        conn.enable_device()
        logs = conn.get_attendance() or []
    finally:
        try:
            conn.disconnect()
        except Exception:
            pass

    if not logs:
        logger.info(f"ℹ️ [device {device_id}] No logs found")
        return 0

    records: List[Dict[str, Any]] = []
    for log in logs:
        try:
            ts_date = log.timestamp.strftime("%Y-%m-%d")
        except Exception:
            continue
        if ts_date not in target_dates:
            continue
        rec = _record_from_zk(log, device_id)
        if rec is not None:
            records.append(rec)

    new_count = queue.enqueue_many(records)
    logger.info(
        f"🧹 [device {device_id}] EoD scanned={len(logs)} "
        f"matched={len(records)} new={new_count}"
    )
    return new_count


def end_of_day_task(lookback_days: Optional[int] = None) -> bool:
    lookback = max(1, int(lookback_days or EOD_LOOKBACK_DAYS))
    today = date.today()
    target_dates = {
        (today - timedelta(days=i)).isoformat() for i in range(lookback)
    }
    logger.info(
        f"🧹 EoD start (lookback={lookback}d, dates={sorted(target_dates)})"
    )
    tg_send(
        f"🧹 <b>End-of-Day Started</b>\n"
        f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"🖥️ Devices: {len(DEVICES)}\n"
        f"📅 Lookback: {lookback}d",
        kind="eod_start",
        min_interval_s=60,
    )

    ok = 0
    fail = 0
    total_new = 0
    for d in DEVICES:
        try:
            total_new += _eod_one_device(d, target_dates)
            ok += 1
        except Exception as exc:
            fail += 1
            logger.error(
                f"❌ EoD device {d.get('device_id')} error: {exc}"
            )

    tg_send(
        f"🧹 <b>End-of-Day Complete</b>\n"
        f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"✅ OK devices: {ok}\n"
        f"❌ Failed devices: {fail}\n"
        f"🧾 New records enqueued: {total_new}",
        kind="eod_done",
        min_interval_s=60,
    )
    return fail == 0


# ---------------------------------------------------------------------------
# Process supervisor / watchdog
# ---------------------------------------------------------------------------

def spawn_capture_process(device: dict) -> Process:
    p = Process(
        target=capture_real_time_logs,
        args=(device, DB_PATH),
        name=f"capture-{device.get('device_id')}",
        daemon=True,
    )
    p.start()
    logger.info(
        f"▶️ Capture process started for device "
        f"{device.get('device_id')} (PID {p.pid})"
    )
    return p


def supervise_processes(processes: Dict[Any, Process]) -> None:
    """Restart any device subprocess that has died."""
    for d in DEVICES:
        did = d.get("device_id")
        p = processes.get(did)
        if p is None or not p.is_alive():
            if p is not None:
                exitcode = p.exitcode
                logger.warning(
                    f"🩺 Watchdog: device {did} not alive "
                    f"(exitcode={exitcode}); respawning"
                )
                tg_send(
                    f"⚠️ <b>Worker Restarted</b>\n"
                    f"🖥️ Device: {did}\n"
                    f"🔧 Exit: {exitcode}",
                    kind=f"worker_restart_{did}",
                    min_interval_s=300,
                )
                try:
                    p.terminate()
                    p.join(timeout=5)
                except Exception:
                    pass
            try:
                processes[did] = spawn_capture_process(d)
            except Exception as exc:
                logger.error(
                    f"❌ Failed to spawn capture for device {did}: {exc}"
                )


def stop_processes(
    processes: Dict[Any, Process], timeout: int = 5
) -> None:
    for _did, p in list(processes.items()):
        try:
            p.terminate()
        except Exception:
            pass
    for _did, p in list(processes.items()):
        try:
            p.join(timeout=timeout)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Boot recovery (rate-limited so a crash-loop doesn't thrash the devices)
# ---------------------------------------------------------------------------

def maybe_run_boot_recovery(force_extended: bool = False) -> None:
    global _queue_db_needs_extended_recovery
    lookback = BOOT_RECOVERY_DAYS
    if force_extended or _queue_db_needs_extended_recovery:
        lookback = POST_DB_RESET_RECOVERY_DAYS
        logger.info(
            f"⚙️ Extended boot recovery after queue reset "
            f"(lookback={lookback}d)"
        )
        _queue_db_needs_extended_recovery = False
    else:
        last_recovery = queue.get_state("last_boot_recovery_at")
        if last_recovery:
            try:
                last_dt = datetime.fromisoformat(last_recovery)
                if datetime.now() - last_dt < timedelta(minutes=30):
                    logger.info(
                        f"⚙️ Skipping boot recovery (last ran {last_recovery})"
                    )
                    return
            except Exception:
                pass
        logger.info(
            f"⚙️ Boot EoD recovery (lookback={lookback}d)"
        )

    try:
        end_of_day_task(lookback_days=lookback)
    except Exception as exc:
        logger.exception(f"Boot recovery error: {exc}")
    finally:
        try:
            queue.set_state(
                "last_boot_recovery_at",
                datetime.now().replace(microsecond=0).isoformat(),
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("🚀 Boot checks: waiting for network...")
    if not wait_for_network(120):
        logger.warning("Network/DNS not ready after 120s; continuing anyway")
    else:
        logger.info("✅ Network/DNS OK")

    device_hosts = [d.get("ip_address") for d in DEVICES if d.get("ip_address")]
    logger.info("🔎 Waiting for at least one device to ping...")
    if not any_device_ping_ok(device_hosts, 60):
        logger.warning("No devices answered ping in 60s; continuing anyway")
    else:
        logger.info("✅ At least one device reachable")

    try:
        pending_at_boot = queue.count_unsynced()
    except Exception:
        pending_at_boot = -1

    tg_send(
        f"🚀 <b>Attendance ZTech Started</b>\n"
        f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"🖥️ Devices: {len(DEVICES)}\n"
        f"🌐 Endpoint: {ENDPOINT}\n"
        f"📦 Pending records: {pending_at_boot}",
        kind="boot",
    )

    stop_event = threading.Event()

    def _signal(sig, _frame):
        logger.info(f"⏹️ Signal {sig} received; shutting down")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _signal)
        except Exception:
            pass

    pusher = threading.Thread(
        target=pusher_loop, args=(stop_event,), name="pusher", daemon=True
    )
    pusher.start()

    processes: Dict[Any, Process] = {}
    supervise_processes(processes)

    maybe_run_boot_recovery(force_extended=_db_action == "reset")

    last_reconnect = time.time()
    last_watchdog = 0.0
    last_eod_attempt = 0.0
    last_wal_checkpoint = time.time()
    last_integrity_check = time.time()
    last_eod_done_marker: Optional[str] = queue.get_state("last_eod_date")

    try:
        logger.info("⏰ Entering main loop")
        while not stop_event.is_set():
            try:
                now = time.time()
                now_dt = datetime.now()
                today_iso = date.today().isoformat()

                if now - last_watchdog >= WATCHDOG_INTERVAL_S:
                    supervise_processes(processes)
                    last_watchdog = now

                if now - last_reconnect >= RECONNECT_INTERVAL_S:
                    logger.info(
                        "🔁 Scheduled reconnect of all device workers"
                    )
                    stop_processes(processes)
                    processes.clear()
                    supervise_processes(processes)
                    last_reconnect = now

                if now - last_wal_checkpoint >= WAL_CHECKPOINT_INTERVAL_S:
                    try:
                        checkpoint_wal(DB_PATH)
                        last_wal_checkpoint = now
                    except Exception as exc:
                        logger.warning(f"WAL checkpoint failed: {exc}")
                        if is_sqlite_corruption_error(exc):
                            _repair_queue_if_corrupt("wal checkpoint")

                if now - last_integrity_check >= DB_INTEGRITY_CHECK_INTERVAL_S:
                    last_integrity_check = now
                    ok_db, db_msg = verify_sqlite_queue_db(DB_PATH)
                    if not ok_db:
                        _repair_queue_if_corrupt(
                            f"scheduled integrity check: {db_msg}"
                        )

                # Daily EoD: anywhere in 23:55-23:59, run once per day,
                # retry every minute within the window if it fails.
                if (
                    now_dt.hour == 23
                    and 55 <= now_dt.minute <= 59
                    and last_eod_done_marker != today_iso
                    and (now - last_eod_attempt >= 60)
                ):
                    last_eod_attempt = now
                    try:
                        if end_of_day_task(lookback_days=EOD_LOOKBACK_DAYS):
                            last_eod_done_marker = today_iso
                            queue.set_state("last_eod_date", today_iso)
                    except Exception as exc:
                        logger.exception(f"EoD task error: {exc}")

                stop_event.wait(timeout=1.0)
            except Exception as exc:
                logger.exception(f"Main loop iteration error: {exc}")
                tg_send(
                    f"❌ <b>Main Loop Error</b>\n"
                    f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}\n"
                    f"🔧 {str(exc)[:200]}",
                    kind="main_loop_err",
                    min_interval_s=600,
                )
                stop_event.wait(timeout=2.0)
    finally:
        logger.info("🛑 Stopping...")
        stop_event.set()
        try:
            pusher.join(timeout=10)
        except Exception:
            pass
        stop_processes(processes)
        try:
            stats = queue.stats()
        except Exception:
            stats = {}
        logger.info(f"👋 Stopped. Queue stats: {stats}")
        tg_send(
            f"👋 <b>System Stopped</b>\n"
            f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}\n"
            f"📦 Pending: {stats.get('pending', '?')}",
            kind="shutdown",
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("⏹️ Interrupted by user")
    except Exception as exc:
        logger.exception(f"💥 Fatal error: {exc}")
        try:
            tg_send(
                f"💥 <b>Fatal Error</b>\n"
                f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}\n"
                f"🔧 {str(exc)[:300]}",
                kind="fatal",
            )
        except Exception:
            pass
        sys.exit(1)
