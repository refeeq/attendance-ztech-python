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
   by network glitches. Capture workers are paused for that pull so the
   device is free.
5. Morning ERP sync (08:00, today only) re-pulls the current day's punches
   so late-arrival data is on the ERP when staff need it. Runs once per
   day; if the daemon was down at 08:00 it catch-up runs after it returns.
   After enqueue it waits for the pusher to drain pending rows.
6. On every daemon start live capture begins immediately. A background
   thread then pulls ``boot_sync_days`` (default 60) of history into the
   queue; the pusher drains those rows to the ERP without blocking punches.
   The legacy ``boot_sync_30d.py`` oneshot still exists for systemd/cron.

The queue is the single source of truth for "did we ship it?". Restarts,
crashes, and power-loss never discard pending records.
"""

from __future__ import annotations

import json
import logging
import multiprocessing
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
from zk.base import ZK_helper
from zk.exception import ZKErrorResponse

from storage import (
    DEFAULT_DB_PATH,
    AttendanceQueue,
    checkpoint_wal,
    ensure_sqlite_queue_db,
    is_manual_backfill_active,
    is_sqlite_corruption_error,
    verify_sqlite_queue_db,
)
from error_alerts import (
    announce_error_alerts,
    attach_error_alerts,
    tap_capture_stdout,
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
_RAW_PURGE_DAYS      = int(_SYNC_CFG.get("purge_synced_after_days", 90))
# Retention cleanup cadence: default once a month. Prefer purge_interval_days
# when set; otherwise purge_interval_s (legacy). Minimum 1 day / 5 minutes.
_DEFAULT_PURGE_INTERVAL_S = 30 * 24 * 3600
if "purge_interval_days" in _SYNC_CFG:
    PURGE_INTERVAL_S = max(
        86400, int(_SYNC_CFG.get("purge_interval_days", 30)) * 86400
    )
elif "purge_interval_s" in _SYNC_CFG:
    PURGE_INTERVAL_S = max(300, int(_SYNC_CFG["purge_interval_s"]))
else:
    PURGE_INTERVAL_S = _DEFAULT_PURGE_INTERVAL_S
PURGE_VACUUM_MIN_DELETED = max(
    1, int(_SYNC_CFG.get("purge_vacuum_min_deleted", 100))
)
# Telegram "data push OK" during backlog drain: at most one message per interval
# so large queue drain does not hit Telegram 429.
TG_DATA_PUSH_PROGRESS_INTERVAL_S = 300
WATCHDOG_INTERVAL_S  = max(5,  int(_SYNC_CFG.get("watchdog_interval_s", 30)))
RECONNECT_INTERVAL_S = max(60, int(_SYNC_CFG.get("reconnect_interval_min", 30)) * 60)
# pyzk uses one timeout for the handshake and every later read. Keep the
# handshake short so a busy terminal fails fast, then widen it for transfers.
DEVICE_CONNECT_TIMEOUT_S = max(5, int(_SYNC_CFG.get("device_connect_timeout_s", 15)))
DEVICE_IO_TIMEOUT_S = max(10, int(_SYNC_CFG.get("device_io_timeout_s", 60)))
CAPTURE_MAX_BACKOFF_S = max(5, int(_SYNC_CFG.get("capture_max_backoff_s", 60)))
EOD_LOOKBACK_DAYS    = max(1,  int(_SYNC_CFG.get("eod_lookback_days", 1)))
MORNING_SYNC_HOUR = max(0, min(23, int(_SYNC_CFG.get("morning_sync_hour", 8))))
MORNING_SYNC_MINUTE = max(
    0, min(59, int(_SYNC_CFG.get("morning_sync_minute", 0)))
)
MORNING_SYNC_WINDOW_MIN = max(
    1, int(_SYNC_CFG.get("morning_sync_window_min", 5))
)
MORNING_SYNC_DRAIN_TIMEOUT_S = max(
    30, int(_SYNC_CFG.get("morning_sync_drain_timeout_s", 300))
)
BOOT_RECOVERY_DAYS   = max(1,  int(_SYNC_CFG.get("boot_recovery_days", 3)))
BOOT_SYNC_DAYS       = max(
    BOOT_RECOVERY_DAYS,
    int(_SYNC_CFG.get("boot_sync_days", 60)),
)
# Never retain less than the boot-sync window or a restart would re-enqueue
# (and re-push) punches we just deleted.
if _RAW_PURGE_DAYS > 0 and _RAW_PURGE_DAYS < BOOT_SYNC_DAYS:
    logger.warning(
        "purge_synced_after_days (%s) is shorter than boot_sync_days (%s); "
        "raising retention to %sd so boot sync cannot re-push deleted rows",
        _RAW_PURGE_DAYS,
        BOOT_SYNC_DAYS,
        BOOT_SYNC_DAYS,
    )
    PURGE_DAYS = BOOT_SYNC_DAYS
else:
    PURGE_DAYS = _RAW_PURGE_DAYS
POST_DB_RESET_RECOVERY_DAYS = max(
    BOOT_SYNC_DAYS,
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
    "forever" if PURGE_DAYS <= 0 else f"{PURGE_DAYS}d (punch timestamp)"
)
_purge_every = (
    "off"
    if PURGE_DAYS <= 0
    else f"every {max(1, PURGE_INTERVAL_S // 86400)}d"
)
logger.info(
    f"Config: devices={len(DEVICES)} endpoint={ENDPOINT} "
    f"batch_size={PUSH_BATCH_SIZE} push_interval_s={PUSH_INTERVAL_S} "
    f"reconnect_interval_s={RECONNECT_INTERVAL_S} db={DB_PATH} "
    f"boot_sync_days={BOOT_SYNC_DAYS} eod_lookback_days={EOD_LOOKBACK_DAYS} "
    f"morning_sync={MORNING_SYNC_HOUR:02d}:{MORNING_SYNC_MINUTE:02d} "
    f"retention={_retention} purge={_purge_every} "
    f"telegram={'ON' if telegram_notifier.enabled else 'OFF'}"
)

error_alert_handler = attach_error_alerts(
    logger, config, SYSTEM_NAME, DEVICES
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
    notif_key = {
        "morning_sync_start": "morning_sync",
        "morning_sync_done": "morning_sync",
    }.get(kind)
    if notif_key and not telegram_notifier.is_notification_enabled(notif_key):
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

# Capture workers the watchdog must not respawn (device is mid history-pull).
_paused_device_ids: set = set()
_paused_lock = threading.Lock()
_processes_lock = threading.Lock()
# Set while boot-sync / EoD holds a device, so the 15-min reconnect does
# not kill live capture on the other terminals.
_history_pull_active = threading.Event()


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


def _notify_cleanup(
    result: Optional[Dict[str, Any]],
    *,
    error: Optional[str],
    reason: str,
) -> None:
    success = error is None
    if success and not telegram_notifier.is_notification_enabled("cleanup"):
        return
    if not success and not (
        telegram_notifier.is_notification_enabled("cleanup")
        or telegram_notifier.is_notification_enabled("errors")
    ):
        return
    payload = result or {}
    message = telegram_notifier.cleanup_message_html(
        success=success,
        days=int(payload.get("days") or PURGE_DAYS),
        deleted=int(payload.get("deleted") or 0),
        remaining=int(payload.get("remaining") or 0),
        pending=int(payload.get("pending") or 0),
        cutoff=str(payload.get("cutoff") or ""),
        oldest_kept=str(payload.get("oldest_kept") or ""),
        bytes_before=int(payload.get("bytes_before") or 0),
        bytes_after=int(payload.get("bytes_after") or 0),
        duration_s=float(payload.get("duration_s") or 0.0),
        vacuumed=bool(payload.get("vacuumed")),
        reason=reason,
        error=error,
    )
    tg_send(
        message,
        kind="cleanup_error" if error else f"cleanup_{reason}",
        min_interval_s=0 if (reason == "startup" or error) else 3600,
    )


def run_retention_cleanup(
    *, reason: str, force_vacuum: bool = False
) -> Optional[Dict[str, Any]]:
    """Drop synced local punches older than ``PURGE_DAYS`` and alert."""
    if PURGE_DAYS <= 0:
        return None
    try:
        result = queue.purge_synced_older_than(PURGE_DAYS, vacuum=False)
        deleted = int(result.get("deleted") or 0)
        should_vacuum = deleted > 0 and (
            force_vacuum or deleted >= PURGE_VACUUM_MIN_DELETED
        )
        if should_vacuum:
            try:
                checkpoint_wal(DB_PATH)
                queue.vacuum()
                result["vacuumed"] = True
                result["bytes_after"] = queue.db_size_bytes()
            except Exception as exc:
                logger.warning("VACUUM after purge failed: %s", exc)
        if deleted:
            logger.info(
                "🧽 Purged %s synced records older than %sd "
                "(remaining=%s pending=%s vacuumed=%s %.1fs) [%s]",
                f"{deleted:,}",
                PURGE_DAYS,
                result.get("remaining"),
                result.get("pending"),
                result.get("vacuumed"),
                result.get("duration_s"),
                reason,
            )
        else:
            logger.info(
                "🧽 Cleanup (%s): no synced punches older than %sd "
                "(remaining=%s)",
                reason,
                PURGE_DAYS,
                result.get("remaining"),
            )
        try:
            queue.set_state(
                "last_purge_at",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            )
            queue.set_state("last_purge_deleted", deleted)
        except Exception:
            pass
        if deleted or reason == "startup" or result.get("vacuumed"):
            _notify_cleanup(result, error=None, reason=reason)
        return result
    except Exception as exc:
        logger.warning("Purge error (%s): %s", reason, exc)
        if is_sqlite_corruption_error(exc):
            _repair_queue_if_corrupt(f"retention purge ({reason})")
        _notify_cleanup(None, error=str(exc)[:500], reason=reason)
        return None


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


class DeviceConnectError(RuntimeError):
    pass


def _zk_sock(conn: Any) -> Optional[socket.socket]:
    return getattr(conn, "_ZK__sock", None)


def _enable_keepalive(sock: socket.socket) -> None:
    """Detect a vanished terminal within ~1 min instead of never.

    live_capture() treats every recv timeout as "no punch yet", so without
    keepalive a dead peer leaves the worker waiting forever.
    """
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        for name, value in (
            ("TCP_KEEPIDLE", 30),
            ("TCP_KEEPINTVL", 10),
            ("TCP_KEEPCNT", 3),
        ):
            opt = getattr(socket, name, None)
            if opt is not None:
                sock.setsockopt(socket.IPPROTO_TCP, opt, value)
    except OSError:
        pass


def _describe_connect_failure(
    ip: str, port: int, exc: BaseException, udp: bool
) -> str:
    detail = str(exc) or type(exc).__name__
    if "ping" in detail:
        return f"device not answering ping ({detail})"
    if udp:
        return f"UDP handshake failed ({detail})"
    try:
        rc = ZK_helper(ip, port).test_tcp()
    except Exception:
        rc = -1
    if rc not in (0, -1):
        return (
            f"TCP {port} refused/unreachable ({os.strerror(rc)}) although "
            f"ping works; SDK port closed or comm settings changed"
        )
    if "timed out" in detail:
        return (
            f"TCP {port} open but no ZK handshake reply in "
            f"{DEVICE_CONNECT_TIMEOUT_S}s; the terminal's SDK sessions are "
            f"busy or its comm stack is hung"
        )
    return detail


def _zk_close(conn: Any) -> None:
    """Send CMD_EXIT so the terminal frees the session, then drop the socket.

    ZKTeco terminals allow only a few SDK sessions; a socket that is closed
    without CMD_EXIT can keep a slot busy until the terminal times it out.
    """
    if conn is None:
        return
    sock = _zk_sock(conn)
    if getattr(conn, "is_connect", False):
        try:
            if sock is not None:
                sock.settimeout(3)
            conn.disconnect()
        except Exception:
            pass
    if sock is not None:
        try:
            sock.close()
        except Exception:
            pass


def _zk_open(
    device: dict,
    *,
    force_udp: bool = False,
    io_timeout: int = DEVICE_IO_TIMEOUT_S,
) -> Any:
    ip = str(device.get("ip_address", ""))
    port = int(device.get("port", 4370) or 4370)
    zk = ZK(
        ip,
        port=port,
        timeout=DEVICE_CONNECT_TIMEOUT_S,
        password=_safe_password(device),
        force_udp=force_udp,
        ommit_ping=False,
    )
    try:
        conn = zk.connect()
        if not conn:
            raise RuntimeError("connect() returned None")
    except Exception as exc:
        _zk_close(zk)
        raise DeviceConnectError(
            _describe_connect_failure(ip, port, exc, force_udp)
        ) from exc
    sock = _zk_sock(conn)
    if sock is not None:
        if not force_udp:
            _enable_keepalive(sock)
        sock.settimeout(io_timeout)
    conn._ZK__timeout = io_timeout
    _make_free_data_tolerant(conn)
    return conn


def _make_free_data_tolerant(conn: Any) -> None:
    """Don't discard a completed download because CMD_FREE_DATA was misread.

    pyzk calls free_data() only after every chunk has been received and
    validated. On TCP its "broken ACK" reassembly can leave that reply
    misaligned, and pyzk then raises "can't free data" and throws the
    whole transfer away. The terminal releases the buffer on disconnect
    anyway; if the stream really is out of sync the next command fails
    and the caller retries on a fresh connection.
    """
    original = conn.free_data

    def free_data() -> bool:
        try:
            return original()
        except ZKErrorResponse:
            return False

    conn.free_data = free_data


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
    last_purge = time.time()  # startup already purged; wait one interval
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

            if PURGE_DAYS > 0 and now - last_purge >= PURGE_INTERVAL_S:
                run_retention_cleanup(reason="scheduled", force_vacuum=False)
                last_purge = now

            if pending == 0:
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
    stopping = {"flag": False}

    # Forked children inherit the parent's SIGTERM handler, which only sets
    # the parent's stop_event copy. Without this, terminate() is ignored and
    # every restart leaks a worker that keeps its device session open.
    def _on_term(_sig: int, _frame: Any) -> None:
        if not stopping["flag"]:
            stopping["flag"] = True
            raise SystemExit(0)

    signal.signal(signal.SIGTERM, _on_term)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    parent = multiprocessing.parent_process()

    def _orphaned() -> bool:
        return parent is not None and not parent.is_alive()

    sub_logger = logging.getLogger("AttendanceZTech")
    if not sub_logger.handlers:                       # spawn-mode children
        setup_logging()
        sub_logger = logging.getLogger("AttendanceZTech")

    device_id = device.get("device_id", "?")
    alerts = attach_error_alerts(sub_logger, config, SYSTEM_NAME, DEVICES)
    tap_capture_stdout(device_id, alerts)
    ip = device.get("ip_address", "?")
    port = int(device.get("port", 4370) or 4370)
    preferred_udp = bool(device.get("force_udp", False))
    local_queue = AttendanceQueue(db_path)

    backoff = 5
    failures = 0
    while not stopping["flag"]:
        if _orphaned():
            sub_logger.warning(
                f"⚠️ [device {device_id}] parent daemon gone; worker exiting"
            )
            break
        # After two straight failures alternate transports: some terminals
        # hang the TCP SDK stack while UDP still answers, and vice versa.
        use_udp = preferred_udp ^ (failures >= 2 and failures % 2 == 0)
        transport = "UDP" if use_udp else "TCP"
        conn = None
        try:
            sub_logger.info(
                f"🔌 [device {device_id}] Connecting to {ip}:{port} "
                f"({transport})"
            )
            conn = _zk_open(device, force_udp=use_udp)
            conn.enable_device()
            sub_logger.info(
                f"✅ [device {device_id}] Connected ({transport}), "
                f"entering live capture"
            )
            if alerts is not None:
                alerts.note_recovered(device_id)
            backoff = 5
            failures = 0

            for attendance in conn.live_capture(new_timeout=10):
                if stopping["flag"]:
                    break
                if attendance is None:
                    if _orphaned():
                        break
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
            if not stopping["flag"] and not _orphaned():
                raise ConnectionError("live capture stream ended")
        except Exception as exc:
            failures += 1
            reason = str(exc) or type(exc).__name__
            # A single blip (e.g. the terminal still busy right after a
            # history pull closed its session) is retried quickly and not
            # alerted; only a repeat failure is reported as an error.
            if failures == 1:
                wait_s = 2
                sub_logger.warning(
                    f"⚠️ [device {device_id}] capture hiccup: [{transport}] "
                    f"{reason}; retrying in {wait_s}s"
                )
            else:
                wait_s = backoff
                backoff = min(backoff * 2, CAPTURE_MAX_BACKOFF_S)
                sub_logger.error(
                    f"❌ [device {device_id}] capture error: [{transport}] "
                    f"{reason}; reconnecting in {wait_s}s"
                )
            _zk_close(conn)
            conn = None
            deadline = time.time() + wait_s
            while time.time() < deadline and not _orphaned():
                time.sleep(1)
        finally:
            _zk_close(conn)


# ---------------------------------------------------------------------------
# End-of-Day reconciliation (catches RT punches missed by network glitches)
# ---------------------------------------------------------------------------

def _eod_one_device(device: dict, target_dates: set) -> int:
    device_id = device.get("device_id", "?")
    ip = device.get("ip_address", "?")
    port = int(device.get("port", 4370) or 4370)
    preferred_udp = bool(device.get("force_udp", False))

    logger.info(f"🧹 [device {device_id}] EoD pull from {ip}:{port}")
    # UDP transfers in small datagrams and avoids the TCP chunk
    # reassembly that most often breaks, so it is the last resort.
    attempts = (preferred_udp, preferred_udp, not preferred_udp)
    logs: List[Any] = []
    for n, use_udp in enumerate(attempts, 1):
        transport = "UDP" if use_udp else "TCP"
        conn = None
        try:
            conn = _zk_open(device, force_udp=use_udp, io_timeout=100)
            conn.enable_device()
            logs = conn.get_attendance() or []
            break
        except Exception as exc:
            if n == len(attempts):
                raise
            logger.warning(
                f"⚠️ [device {device_id}] history pull attempt {n}/"
                f"{len(attempts)} over {transport} failed ({exc}); "
                f"retrying on a fresh connection"
            )
        finally:
            _zk_close(conn)
        time.sleep(3)

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


def wait_for_queue_drain(
    timeout_s: int,
    stop_event: Optional[threading.Event] = None,
) -> Tuple[bool, int]:
    """Poll until the pusher has no pending rows, or ``timeout_s`` elapses.

    Does not POST to the ERP itself — ``pusher_loop`` owns HTTP. Returns
    ``(drained, remaining)`` where ``remaining`` is the last unsynced count
    (``-1`` if the count could not be read).
    """
    deadline = time.time() + max(1, int(timeout_s))
    remaining = -1
    while time.time() < deadline:
        if stop_event is not None and stop_event.is_set():
            break
        try:
            remaining = queue.count_unsynced()
        except Exception as exc:
            logger.warning(f"Drain wait: count_unsynced failed: {exc}")
            remaining = -1
            break
        if remaining == 0:
            return True, 0
        if stop_event is not None:
            stop_event.wait(timeout=1.0)
        else:
            time.sleep(1.0)
    if remaining < 0:
        try:
            remaining = queue.count_unsynced()
        except Exception:
            remaining = -1
    return False, remaining


def end_of_day_task(
    lookback_days: Optional[int] = None,
    *,
    purpose: str = "eod",
    processes: Optional[Dict[Any, Process]] = None,
    stop_event: Optional[threading.Event] = None,
) -> bool:
    is_boot = purpose == "boot"
    is_morning = purpose == "morning"
    if is_morning:
        lookback = 1
    else:
        lookback = max(1, int(lookback_days or EOD_LOOKBACK_DAYS))
    today = date.today()
    target_dates = {
        (today - timedelta(days=i)).isoformat() for i in range(lookback)
    }
    if is_boot:
        label = f"{lookback}-Day Boot Sync"
        emoji = "🚀"
        kind_start = "boot_sync_start"
        kind_done = "boot_sync_done"
    elif is_morning:
        label = "Morning ERP Sync"
        emoji = "🌅"
        kind_start = "morning_sync_start"
        kind_done = "morning_sync_done"
    else:
        label = "End-of-Day"
        emoji = "🧹"
        kind_start = "eod_start"
        kind_done = "eod_done"

    if is_manual_backfill_active(DB_PATH):
        logger.info(
            f"⏭️ {label} deferred — a manual device backfill is running"
        )
        return False

    # Boot may already hold this flag (set before the thread starts so
    # morning/EoD cannot race it). Any other job must wait.
    already_pulling = _history_pull_active.is_set()
    if already_pulling and not is_boot:
        logger.info(
            f"⏭️ {label} deferred — another history pull is already running"
        )
        return False

    _history_pull_active.set()
    logger.info(
        f"{emoji} {label} start (lookback={lookback}d, "
        f"dates={sorted(target_dates)[0]} … {sorted(target_dates)[-1]})"
    )
    tg_send(
        f"{emoji} <b>{label} Started</b>\n"
        f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"🖥️ Devices: {len(DEVICES)}\n"
        f"📅 Lookback: {lookback}d\n"
        f"📆 {sorted(target_dates)[0]} → {sorted(target_dates)[-1]}\n"
        f"🟢 Live capture stays running.",
        kind=kind_start,
        min_interval_s=60,
    )

    ok = 0
    fail = 0
    total_new = 0
    try:
        for d in DEVICES:
            if stop_event is not None and stop_event.is_set():
                logger.warning(f"{label} aborted (shutdown)")
                fail += 1
                break
            try:
                total_new += _pull_one_device_history(
                    d, target_dates, processes
                )
                ok += 1
            except Exception as exc:
                fail += 1
                logger.error(
                    f"❌ {label} device {d.get('device_id')} error: {exc}"
                )
    finally:
        if not already_pulling:
            _history_pull_active.clear()

    drain_line = "📤 Pending rows are pushing to ERP in the background."
    if is_morning and fail == 0:
        logger.info(
            f"{emoji} {label} waiting up to "
            f"{MORNING_SYNC_DRAIN_TIMEOUT_S}s for ERP drain"
        )
        drained, remaining = wait_for_queue_drain(
            MORNING_SYNC_DRAIN_TIMEOUT_S, stop_event
        )
        if drained:
            drain_line = "📤 Queue drained — today's punches are on the ERP."
            logger.info(f"{emoji} {label} ERP drain complete")
        else:
            drain_line = (
                f"⚠️ Drain still pending after "
                f"{MORNING_SYNC_DRAIN_TIMEOUT_S}s "
                f"({remaining} unsynced). Pusher will keep retrying."
            )
            logger.warning(
                f"{emoji} {label} ERP drain timed out "
                f"(remaining={remaining}); pusher continues"
            )

    tg_send(
        f"{emoji} <b>{label} Complete</b>\n"
        f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"✅ OK devices: {ok}\n"
        f"❌ Failed devices: {fail}\n"
        f"🧾 New records enqueued: {total_new}\n"
        f"{drain_line}",
        kind=kind_done,
        min_interval_s=60,
    )
    return fail == 0


# ---------------------------------------------------------------------------
# Process supervisor / watchdog
# ---------------------------------------------------------------------------

def _stop_process(p: Process, timeout: float = 5.0) -> None:
    try:
        if p.is_alive():
            p.terminate()
            p.join(timeout=timeout)
        if p.is_alive():
            logger.warning(
                f"🔪 {p.name} (PID {p.pid}) ignored SIGTERM; killing"
            )
            p.kill()
            p.join(timeout=3)
    except Exception:
        pass


def kill_stale_workers() -> int:
    """Kill leftover daemon/worker processes from earlier runs.

    Older builds leaked capture workers on every restart; each one still
    holds an SDK session, and terminals with few slots then stop answering
    new connections even though ping works.
    """
    try:
        import psutil
    except ImportError:
        logger.warning("psutil missing; cannot clean up stale capture workers")
        return 0
    try:
        me = psutil.Process()
        my_cmd = me.cmdline()
        my_cwd = me.cwd()
        skip = {me.pid} | {p.pid for p in me.parents()}
    except Exception:
        return 0
    victims = []
    for p in psutil.process_iter(["pid", "cmdline", "cwd"]):
        try:
            if p.pid in skip or p.info["cwd"] != my_cwd:
                continue
            cmd = p.info["cmdline"] or []
            # fork children share our argv; spawn/forkserver children run
            # "python -c 'from multiprocessing...'" from the same cwd.
            if cmd == my_cmd or (
                cmd and cmd[0] == my_cmd[0] and "multiprocessing" in " ".join(cmd)
            ):
                victims.append(p)
        except Exception:
            continue
    if not victims:
        return 0
    logger.warning(
        f"🧟 Found {len(victims)} stale attendance process(es) from a previous "
        f"run holding device sessions: {[p.pid for p in victims]}; stopping them"
    )
    for p in victims:
        try:
            p.terminate()
        except Exception:
            pass
    _gone, alive = psutil.wait_procs(victims, timeout=5)
    for p in alive:
        try:
            p.kill()
        except Exception:
            pass
    psutil.wait_procs(alive, timeout=3)
    return len(victims)


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


def _pause_capture_device(processes: Dict[Any, Process], device: dict) -> None:
    """Stop live capture on one device so get_attendance() can use the session."""
    did = device.get("device_id")
    with _paused_lock:
        _paused_device_ids.add(did)
    with _processes_lock:
        p = processes.pop(did, None)
    if p is None:
        return
    logger.info(f"⏸️ Pausing live capture on device {did} for history pull")
    _stop_process(p)


def _resume_capture_device(processes: Dict[Any, Process], device: dict) -> None:
    did = device.get("device_id")
    try:
        with _processes_lock:
            processes[did] = spawn_capture_process(device)
    except Exception as exc:
        logger.error(f"❌ Failed to resume capture for device {did}: {exc}")
    finally:
        with _paused_lock:
            _paused_device_ids.discard(did)


def _pull_one_device_history(
    device: dict,
    target_dates: set,
    processes: Optional[Dict[Any, Process]],
) -> int:
    """Download stored punches. Keep live capture if a second session works.

    ZKTeco terminals often allow only one SDK connection. Try the pull while
    live capture is running; if that fails, pause just this device, retry,
    then resume. Other devices stay in live capture the whole time.
    """
    try:
        return _eod_one_device(device, target_dates)
    except Exception as first_exc:
        if is_sqlite_corruption_error(first_exc):
            if not _repair_queue_if_corrupt(
                f"history pull device {device.get('device_id')}"
            ):
                raise
            return _eod_one_device(device, target_dates)
        if processes is None:
            raise
        logger.warning(
            f"⚠️ [device {device.get('device_id')}] history pull failed "
            f"while live capture was running ({first_exc}); "
            f"pausing this device only and retrying"
        )
        _pause_capture_device(processes, device)
        try:
            time.sleep(3)
            return _eod_one_device(device, target_dates)
        finally:
            time.sleep(3)
            _resume_capture_device(processes, device)


def supervise_processes(processes: Dict[Any, Process]) -> None:
    """Restart any device subprocess that has died."""
    for d in DEVICES:
        did = d.get("device_id")
        with _paused_lock:
            if did in _paused_device_ids:
                continue
        with _processes_lock:
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
                _stop_process(p)
            try:
                with _processes_lock:
                    processes[did] = spawn_capture_process(d)
            except Exception as exc:
                logger.error(
                    f"❌ Failed to spawn capture for device {did}: {exc}"
                )


def stop_processes(
    processes: Dict[Any, Process], timeout: int = 5
) -> None:
    with _processes_lock:
        items = list(processes.items())
        processes.clear()
    for _did, p in items:
        try:
            p.terminate()
        except Exception:
            pass
    deadline = time.time() + timeout
    for _did, p in items:
        try:
            p.join(timeout=max(0.1, deadline - time.time()))
        except Exception:
            pass
    for _did, p in items:
        _stop_process(p, timeout=0.1)


# ---------------------------------------------------------------------------
# Boot recovery (rate-limited so a crash-loop doesn't thrash the devices)
# ---------------------------------------------------------------------------

def maybe_run_boot_recovery(
    force_extended: bool = False,
    processes: Optional[Dict[Any, Process]] = None,
    stop_event: Optional[threading.Event] = None,
) -> None:
    """Pull ``boot_sync_days`` of device history into the queue on startup.

    Safe to run in a background thread alongside live capture. Per-device
    pull will pause only the terminal that needs a free SDK session.
    """
    global _queue_db_needs_extended_recovery
    _history_pull_active.set()
    try:
        _run_boot_recovery(
            force_extended=force_extended,
            processes=processes,
            stop_event=stop_event,
        )
    finally:
        _history_pull_active.clear()


def _run_boot_recovery(
    force_extended: bool = False,
    processes: Optional[Dict[Any, Process]] = None,
    stop_event: Optional[threading.Event] = None,
) -> None:
    global _queue_db_needs_extended_recovery
    lookback = BOOT_SYNC_DAYS
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
                        f"⚙️ Skipping boot sync (last succeeded {last_recovery})"
                    )
                    return
            except Exception:
                pass
        logger.info(f"⚙️ Boot sync (lookback={lookback}d, background)")

    ok = False
    try:
        ok = bool(
            end_of_day_task(
                lookback_days=lookback,
                purpose="boot",
                processes=processes,
                stop_event=stop_event,
            )
        )
    except Exception as exc:
        logger.exception(f"Boot recovery error: {exc}")
    if ok:
        try:
            queue.set_state(
                "last_boot_recovery_at",
                datetime.now().replace(microsecond=0).isoformat(),
            )
        except Exception:
            pass
    else:
        logger.warning(
            "Boot sync did not finish cleanly; next restart will retry"
        )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    kill_stale_workers()
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

    cleanup = None
    if PURGE_DAYS > 0:
        logger.info(
            "🧽 Startup retention cleanup (keep last %sd, vacuum after)...",
            PURGE_DAYS,
        )
        cleanup = run_retention_cleanup(reason="startup", force_vacuum=True)
    else:
        logger.info("🧽 Local retention disabled (purge_synced_after_days=0)")

    try:
        pending_at_boot = queue.count_unsynced()
    except Exception:
        pending_at_boot = -1

    if PURGE_DAYS > 0:
        retain_line = f"🧽 Retention: last {PURGE_DAYS}d"
        if cleanup:
            retain_line += (
                f" · removed {int(cleanup.get('deleted') or 0):,}"
                f" · left {int(cleanup.get('remaining') or 0):,}"
            )
    else:
        retain_line = "🧽 Retention: keep forever"

    tg_send(
        f"🚀 <b>Attendance ZTech Started</b>\n"
        f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"🖥️ Devices: {len(DEVICES)}\n"
        f"🌐 Endpoint: {ENDPOINT}\n"
        f"📦 Pending records: {pending_at_boot}\n"
        f"{retain_line}",
        kind="boot",
    )
    announce_error_alerts(error_alert_handler)

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

    boot_sync = threading.Thread(
        target=maybe_run_boot_recovery,
        kwargs={
            "force_extended": _db_action == "reset",
            "processes": processes,
            "stop_event": stop_event,
        },
        name="boot-sync",
        daemon=True,
    )
    # Hold the history-pull lock before the thread is scheduled so the
    # main loop cannot start morning/EoD on the same devices.
    _history_pull_active.set()
    boot_sync.start()
    logger.info("🟢 Live capture is running; 60-day boot sync is in background")

    last_reconnect = time.time()
    last_watchdog = 0.0
    last_eod_attempt = 0.0
    last_morning_attempt = 0.0
    last_wal_checkpoint = time.time()
    last_integrity_check = time.time()
    last_eod_done_marker: Optional[str] = queue.get_state("last_eod_date")
    last_morning_done_marker: Optional[str] = queue.get_state(
        "last_morning_sync_date"
    )

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

                if (
                    now - last_reconnect >= RECONNECT_INTERVAL_S
                    and not _history_pull_active.is_set()
                ):
                    logger.info(
                        "🔁 Scheduled reconnect of all device workers"
                    )
                    stop_processes(processes)
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

                # Daily morning ERP sync: from 08:00 (configurable), run
                # once per day. The first 5 minutes are the primary window
                # (retry every 60s). After that a catch-up still fires if
                # the daemon was down at 08:00, retried every 15 minutes
                # so a dead device does not get hammered all day.
                morning_start = now_dt.replace(
                    hour=MORNING_SYNC_HOUR,
                    minute=MORNING_SYNC_MINUTE,
                    second=0,
                    microsecond=0,
                )
                morning_window_end = morning_start + timedelta(
                    minutes=MORNING_SYNC_WINDOW_MIN
                )
                morning_in_window = (
                    morning_start <= now_dt < morning_window_end
                )
                morning_retry_s = 60 if morning_in_window else 900
                morning_due = (
                    last_morning_done_marker != today_iso
                    and now_dt >= morning_start
                    and (now - last_morning_attempt >= morning_retry_s)
                    and not _history_pull_active.is_set()
                )
                if morning_due:
                    last_morning_attempt = now
                    if morning_in_window:
                        logger.info("🌅 Morning ERP sync window")
                    else:
                        logger.info(
                            "🌅 Morning ERP sync catch-up "
                            f"(missed the {MORNING_SYNC_HOUR:02d}:"
                            f"{MORNING_SYNC_MINUTE:02d} window)"
                        )
                    try:
                        if end_of_day_task(
                            lookback_days=1,
                            purpose="morning",
                            processes=processes,
                            stop_event=stop_event,
                        ):
                            last_morning_done_marker = today_iso
                            queue.set_state(
                                "last_morning_sync_date", today_iso
                            )
                    except Exception as exc:
                        logger.exception(f"Morning sync error: {exc}")

                # Daily EoD: anywhere in 23:55-23:59, run once per day,
                # retry every minute within the window if it fails.
                # Live capture stays up; only a busy device is paused.
                if (
                    now_dt.hour == 23
                    and 55 <= now_dt.minute <= 59
                    and last_eod_done_marker != today_iso
                    and (now - last_eod_attempt >= 60)
                    and not _history_pull_active.is_set()
                ):
                    last_eod_attempt = now
                    try:
                        if end_of_day_task(
                            lookback_days=EOD_LOOKBACK_DAYS,
                            processes=processes,
                            stop_event=stop_event,
                        ):
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
        stop_processes(processes)
        try:
            pusher.join(timeout=10)
        except Exception:
            pass
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
