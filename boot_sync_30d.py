#!/usr/bin/env python3
"""
boot_sync_30d.py
Run on system startup: sync the last 60 days of attendance logs
to the API using sync_all.py logic.
"""

import subprocess
import datetime
import os
import sys
import json
import logging
import time
import socket
from telegram_notifier import TelegramNotifier

# ------------- Logging -------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BootSync60d")

# ------------- Helpers -------------

def load_telegram_config():
    try:
        with open("config.json", "r") as f:
            cfg = json.load(f)
        return cfg
    except Exception as e:
        logger.error(f"Failed to load config.json: {e}")
        return {}

def tg_send_safe(notifier: "TelegramNotifier", html: str, retries: int = 3, backoff_s: int = 2):
    if not notifier or not getattr(notifier, "enabled", False):
        return
    for i in range(retries):
        try:
            notifier.send_message_sync(html)
            return
        except Exception as e:
            logger.error(f"Telegram send failed (attempt {i+1}/{retries}): {e}")
            time.sleep(backoff_s * (i + 1))

def tg_send_with_name(notifier: "TelegramNotifier", message: str, retries: int = 3, backoff_s: int = 2):
    """
    Send a Telegram message with system name prefix.
    """
    if not notifier or not getattr(notifier, "enabled", False):
        return
    # Add system name to the message if it's not already there
    if f"{notifier.system_name} -" not in message and "<b>" in message:
        # Find the first <b> tag and add system name
        message = message.replace("<b>", f"<b>{notifier.system_name} - ", 1)
    tg_send_safe(notifier, message, retries, backoff_s)

def wait_for_network(max_wait_s: int = 120) -> bool:
    """Wait until DNS & outbound connectivity work (best-effort)."""
    start = time.time()
    while time.time() - start < max_wait_s:
        try:
            socket.gethostbyname("api.telegram.org")
            socket.gethostbyname("google.com")
            with socket.create_connection(("8.8.8.8", 53), timeout=3):
                return True
        except OSError:
            time.sleep(3)
    return False

def ensure_exec(path: str, what: str):
    if not os.path.isfile(path):
        logger.error(f"{what} not found: {path}")
        sys.exit(1)
    if not os.access(path, os.X_OK):
        logger.warning(f"{what} is not marked executable, attempting to continue: {path}")

# ------------- Main -------------

def main():
    # Dates
    to_date = datetime.date.today()+datetime.timedelta(days=1)
    from_date = to_date - datetime.timedelta(days=60)

    # Telegram
    config = load_telegram_config()
    tg_cfg = config.get("telegram", {})
    system_name = config.get("name", "Attendance System")
    notifier = TelegramNotifier(
        bot_token=tg_cfg.get("bot_token", ""),
        chat_id=tg_cfg.get("chat_id", ""),
        enabled=tg_cfg.get("enabled", False),
        notification_settings=tg_cfg.get("notifications", {}),
        system_name=system_name
    )

    # Paths
    project_dir = os.path.dirname(os.path.abspath(__file__))
    venv_python = os.path.join(project_dir, "venv", "bin", "python")
    sync_script = os.path.join(project_dir, "sync_all.py")

    logger.info(f"Boot sync range: {from_date} -> {to_date}")
    logger.info(f"CWD: {project_dir}")
    logger.info("Waiting for network/DNS up to 120s...")
    if wait_for_network(120):
        logger.info("Network/DNS OK")
    else:
        logger.warning("Network/DNS still not ready after 120s; proceeding anyway (may affect Telegram/API).")

    # Validate paths
    ensure_exec(venv_python, "venv python")
    if not os.path.isfile(sync_script):
        logger.error(f"sync_all.py not found at {sync_script}")
        sys.exit(1)

    # Command
    cmd = [
        venv_python,
        sync_script,
        "--from", from_date.isoformat(),
        "--to", to_date.isoformat(),
        "--log-level", "INFO",
    ]

    start_msg = (
        f"🚀 <b>60-Day Boot Sync Started</b>\n\n"
        f"📅 <b>Date Range:</b> {from_date} → {to_date}\n"
        f"📊 <b>Duration:</b> 60 days\n"
        f"🔄 <b>Status:</b> Starting historical data sync..."
    )
    tg_send_with_name(notifier, start_msg)

    start_time = datetime.datetime.now()
    logger.info("Starting 60-day boot sync subprocess...")
    print(f"Running 60-day boot sync: {from_date} -> {to_date}", flush=True)

    # Try once; on failure, wait and retry once
    attempts = 0
    max_attempts = 2
    last_err = None

    while attempts < max_attempts:
        attempts += 1
        try:
            subprocess.check_call(cmd, cwd=project_dir)
            # Success
            duration = datetime.datetime.now() - start_time
            ok_msg = (
                f"✅ <b>60-Day Boot Sync Completed</b>\n\n"
                f"📅 <b>Date Range:</b> {from_date} → {to_date}\n"
                f"⏱️ <b>Duration:</b> {duration.total_seconds():.1f} seconds\n"
                f"✅ <b>Status:</b> Historical data sync completed"
            )
            logger.info("Boot sync finished successfully.")
            print("Boot sync finished successfully.", flush=True)
            tg_send_with_name(notifier, ok_msg)
            sys.exit(0)
        except subprocess.CalledProcessError as e:
            last_err = e
            logger.error(f"Boot sync failed (attempt {attempts}/{max_attempts}): {e}")
            if attempts < max_attempts:
                logger.info("Retrying in 10 seconds...")
                time.sleep(10)
        except Exception as e:
            last_err = e
            logger.error(f"Unexpected error (attempt {attempts}/{max_attempts}): {e}")
            if attempts < max_attempts:
                logger.info("Retrying in 10 seconds...")
                time.sleep(10)

    # If here, all attempts failed
    duration = datetime.datetime.now() - start_time
    err_msg = (
        f"❌ <b>60-Day Boot Sync Failed</b>\n\n"
        f"📅 <b>Date Range:</b> {from_date} → {to_date}\n"
        f"⏱️ <b>Duration:</b> {duration.total_seconds():.1f} seconds\n"
        f"❌ <b>Status:</b> Historical data sync failed\n"
        f"🔧 <b>Error:</b> {last_err}"
    )
    print(f"Boot sync failed: {last_err}", flush=True)
    logger.error(f"Boot sync failed after {max_attempts} attempts: {last_err}")
    tg_send_with_name(notifier, err_msg)
    sys.exit(1)


if __name__ == "__main__":
    main()
