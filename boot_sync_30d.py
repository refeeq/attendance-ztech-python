#!/usr/bin/env python3
"""
boot_sync_30d.py
Run on system startup. Drives ``sync_all.py`` over a configurable historical
window so any attendance recorded while the server was offline lands in the
durable local queue (``storage.AttendanceQueue``); the always-on daemon then
drains that queue to the ERP.

The legacy filename keeps existing systemd units / cron entries working. The
default window is 60 days, configurable via ``config.json``::

    "sync": { "boot_sync_days": 60 }
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import socket
import subprocess
import sys
import time

from telegram_notifier import TelegramNotifier

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BootSync")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config(project_dir: str) -> dict:
    path = os.path.join(project_dir, "config.json")
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as exc:
        logger.error(f"Failed to load {path}: {exc}")
        return {}


def tg_send_safe(
    notifier: "TelegramNotifier",
    html: str,
    retries: int = 3,
    backoff_s: int = 2,
) -> None:
    if not notifier or not getattr(notifier, "enabled", False):
        return
    for i in range(retries):
        try:
            notifier.send_message_sync(html)
            return
        except Exception as exc:
            logger.error(
                f"Telegram send failed (attempt {i + 1}/{retries}): {exc}"
            )
            time.sleep(backoff_s * (i + 1))


def tg_send_with_name(
    notifier: "TelegramNotifier",
    message: str,
    retries: int = 3,
    backoff_s: int = 2,
) -> None:
    if not notifier or not getattr(notifier, "enabled", False):
        return
    if (
        f"{notifier.system_name} -" not in message
        and "<b>" in message
    ):
        message = message.replace(
            "<b>", f"<b>{notifier.system_name} - ", 1
        )
    tg_send_safe(notifier, message, retries, backoff_s)


def wait_for_network(max_wait_s: int = 120) -> bool:
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


def resolve_python(project_dir: str) -> str:
    """Prefer the project's venv interpreter; fall back to current."""
    candidates = [
        os.path.join(project_dir, "venv", "bin", "python"),
        os.path.join(project_dir, "venv", "Scripts", "python.exe"),
        os.path.join(project_dir, ".venv", "bin", "python"),
        os.path.join(project_dir, ".venv", "Scripts", "python.exe"),
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    logger.warning(
        f"venv python not found in {project_dir}; falling back to "
        f"{sys.executable}"
    )
    return sys.executable


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    project_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_config(project_dir)

    sync_cfg = config.get("sync") or {}
    boot_sync_days = max(1, int(sync_cfg.get("boot_sync_days", 60)))

    # The legacy script used "to=tomorrow" so today is fully covered.
    to_date = datetime.date.today() + datetime.timedelta(days=1)
    from_date = to_date - datetime.timedelta(days=boot_sync_days)

    tg_cfg = config.get("telegram") or {}
    system_name = config.get("name", "Attendance System")
    notifier = TelegramNotifier(
        bot_token=tg_cfg.get("bot_token", ""),
        chat_id=tg_cfg.get("chat_id", ""),
        enabled=bool(tg_cfg.get("enabled", False)),
        notification_settings=tg_cfg.get("notifications", {}),
        system_name=system_name,
    )

    sync_script = os.path.join(project_dir, "sync_all.py")
    if not os.path.isfile(sync_script):
        logger.error(f"sync_all.py not found at {sync_script}")
        sys.exit(1)

    python_bin = resolve_python(project_dir)

    logger.info(f"Boot sync range: {from_date} -> {to_date}")
    logger.info(f"CWD: {project_dir}")
    logger.info("Waiting for network/DNS up to 120s...")
    if wait_for_network(120):
        logger.info("Network/DNS OK")
    else:
        logger.warning(
            "Network/DNS still not ready after 120s; proceeding anyway."
        )

    cmd = [
        python_bin,
        sync_script,
        "--from", from_date.isoformat(),
        "--to",   to_date.isoformat(),
        "--log-level", "INFO",
    ]

    start_msg = (
        f"🚀 <b>{boot_sync_days}-Day Boot Sync Started</b>\n\n"
        f"📅 <b>Date Range:</b> {from_date} → {to_date}\n"
        f"📊 <b>Duration:</b> {boot_sync_days} days\n"
        f"🔄 <b>Status:</b> Starting historical data sync..."
    )
    tg_send_with_name(notifier, start_msg)

    start_time = datetime.datetime.now()
    logger.info("Starting boot sync subprocess...")

    attempts = 0
    max_attempts = 2
    last_err: Exception = None  # type: ignore[assignment]

    while attempts < max_attempts:
        attempts += 1
        try:
            subprocess.check_call(cmd, cwd=project_dir)
            duration = datetime.datetime.now() - start_time
            ok_msg = (
                f"✅ <b>{boot_sync_days}-Day Boot Sync Completed</b>\n\n"
                f"📅 <b>Date Range:</b> {from_date} → {to_date}\n"
                f"⏱️ <b>Duration:</b> "
                f"{duration.total_seconds():.1f} seconds\n"
                f"✅ <b>Status:</b> Historical data sync completed"
            )
            logger.info("Boot sync finished successfully.")
            tg_send_with_name(notifier, ok_msg)
            sys.exit(0)
        except subprocess.CalledProcessError as exc:
            last_err = exc
            logger.error(
                f"Boot sync failed (attempt {attempts}/{max_attempts}, "
                f"exit={exc.returncode}): {exc}"
            )
            if attempts < max_attempts:
                logger.info("Retrying in 10 seconds...")
                time.sleep(10)
        except Exception as exc:
            last_err = exc
            logger.error(
                f"Unexpected error (attempt {attempts}/{max_attempts}): "
                f"{exc}"
            )
            if attempts < max_attempts:
                logger.info("Retrying in 10 seconds...")
                time.sleep(10)

    duration = datetime.datetime.now() - start_time
    err_msg = (
        f"❌ <b>{boot_sync_days}-Day Boot Sync Failed</b>\n\n"
        f"📅 <b>Date Range:</b> {from_date} → {to_date}\n"
        f"⏱️ <b>Duration:</b> {duration.total_seconds():.1f} seconds\n"
        f"❌ <b>Status:</b> Historical data sync failed\n"
        f"🔧 <b>Error:</b> {last_err}"
    )
    logger.error(
        f"Boot sync failed after {max_attempts} attempts: {last_err}"
    )
    tg_send_with_name(notifier, err_msg)
    sys.exit(1)


if __name__ == "__main__":
    main()
