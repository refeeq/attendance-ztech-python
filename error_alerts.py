"""Separate Telegram bot for attendance sync failures.

The status bot stays for routine messages (startup, successful pushes, end of
day). This channel is only for failures: a device that stops capturing, an
ERP push that is rejected, a history pull that did not finish, a dead worker.

The first occurrence of a problem is sent immediately. A reconnect storm
(the same device timing out many times a minute) is one incident: further
copies are counted, and a reminder goes out after ``repeat_after_s`` while
it is still broken. A recovery message is sent when that device connects
again.
"""

from __future__ import annotations

import html
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from telegram_notifier import TelegramNotifier

_CAPTURE_RE = re.compile(r"\[device ([^\]]+)\].*capture error:\s*(.*)", re.I)
_RECONNECT_RE = re.compile(r";\s*reconnecting in \d+s", re.I)
_HISTORY_RE = re.compile(
    r"(Boot Sync|Morning ERP Sync|End-of-Day) device (\S+) error:\s*(.*)",
    re.I,
)
_WORKER_RE = re.compile(
    r"device (\S+) not alive(?: \(exitcode=([^)]*)\))?",
    re.I,
)
_ENQUEUE_RE = re.compile(r"\[device ([^\]]+)\].*enqueue failed:\s*(.*)", re.I)
_SYNC_RE = re.compile(r"Sync failed \(([^)]*)\):\s*(.*)", re.I)
_TELEGRAM_NOISE = (
    "send failed",
    "rate limit",
    "failed to send",
    "rejected message",
    "error sending telegram",
)


def resolve_alert_settings(config: Optional[dict]) -> Dict[str, Any]:
    """Merge ``telegram_alerts`` with TELEGRAM_ALERTS_* environment variables."""
    raw = dict((config or {}).get("telegram_alerts") or {})
    token = os.environ.get("TELEGRAM_ALERTS_BOT_TOKEN", "").strip()
    chat = os.environ.get("TELEGRAM_ALERTS_CHAT_ID", "").strip()
    enabled_env = os.environ.get("TELEGRAM_ALERTS_ENABLED", "").strip().lower()
    if token:
        raw["bot_token"] = token
    if chat:
        raw["chat_id"] = chat
    if enabled_env in ("1", "true", "yes", "on"):
        raw["enabled"] = True
    elif enabled_env in ("0", "false", "no", "off"):
        raw["enabled"] = False
    return raw


def alerts_configured(settings: Dict[str, Any]) -> bool:
    if not settings.get("enabled"):
        return False
    token = str(settings.get("bot_token") or "").strip()
    chat = str(settings.get("chat_id") or "").strip()
    if not token or not chat:
        return False
    if "YOUR_" in token or token.endswith("HERE"):
        return False
    return True


def _device_ips(devices: Optional[List[dict]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for device in devices or []:
        did = str(device.get("device_id", "")).strip()
        ip = str(device.get("ip_address") or "").strip()
        if did:
            out[did] = ip
    return out


def _clean_reason(text: str) -> str:
    text = _RECONNECT_RE.sub("", text or "")
    text = re.sub(r"^\s*❌\s*", "", text).strip()
    return text[:500]


class _StdoutTap:
    """Drop pyzk's ``trying to complete broken ACK`` prints.

    pyzk prints that line whenever the trailing 16-byte ACK of a chunked
    TCP transfer arrives split across reads; it then reads the rest and
    carries on. It is not an error, so it is neither logged nor alerted.
    """

    _NOISE = ("broken ACK",)

    def __init__(self, raw: Any, device_id: Any):
        self._raw = raw
        self._device_id = device_id
        self._buf = ""

    def write(self, s: Any) -> int:
        if not isinstance(s, str) or not s:
            return 0
        self._buf += s
        out = []
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if not any(n in line for n in self._NOISE):
                out.append(line + "\n")
        if len(self._buf) > 8192:
            out.append(self._buf)
            self._buf = ""
        if out:
            try:
                self._raw.write("".join(out))
            except Exception:
                pass
        return len(s)

    def flush(self) -> None:
        try:
            self._raw.flush()
        except Exception:
            pass

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)


class ErrorAlertHandler(logging.Handler):
    """Send ERROR / CRITICAL (and a few sync warnings) to the alert bot."""

    def __init__(
        self,
        notifier: TelegramNotifier,
        system_name: str,
        devices: Optional[List[dict]] = None,
        repeat_after_s: int = 600,
        *,
        sync: bool = False,
    ):
        super().__init__(level=logging.INFO)
        self._notifier = notifier
        self.system_name = system_name or "Attendance"
        self._ips = _device_ips(devices)
        self.repeat_after_s = max(60, int(repeat_after_s or 600))
        self._sync = sync
        self._lock = threading.Lock()
        self._state: Dict[str, Dict[str, Any]] = {}

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if record.name.startswith("AttendanceZTech.Telegram"):
                return
            if record.name.startswith("AttendanceZTech.Alert"):
                return
            text, key = self._decide(record.getMessage(), record.levelno)
        except Exception:
            return
        if text and key:
            self._dispatch(key, text)

    def report(self, message: str) -> None:
        """Alert on a line that did not come from the logger (pyzk stdout)."""
        try:
            text, key = self._decide(message, logging.ERROR)
        except Exception:
            return
        if text and key:
            self._dispatch(key, text)

    def note_recovered(self, device_id: Any) -> None:
        """Live capture on this device is connected again."""
        key = f"capture:{device_id}"
        try:
            with self._lock:
                st = self._state.get(key)
                if not st or not st.get("open"):
                    return
                label = self._device_label(str(device_id))
                since = st.get("opened_at") or ""
                st["open"] = False
                st["inflight"] = True
                st["suppressed"] = 0
                body = [datetime.now().strftime("%H:%M:%S")]
                if since:
                    body.append(f"It had been down since {since}.")
                body.append("Live punches are being recorded again.")
                text = self._html(f"{label} capture restored", body, ok=True)
        except Exception:
            return
        self._dispatch(key, text)

    def send_test(self) -> bool:
        text = self._html(
            "Error alerts test",
            [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "This chat will receive sync and device failures.",
                "The first failure is sent immediately. Repeats of the same "
                f"failure are grouped, with a reminder every "
                f"{self.repeat_after_s // 60} min while it continues.",
            ],
            ok=True,
        )
        return bool(self._notifier.send_message_sync(text))

    def _dispatch(self, key: str, text: str) -> None:
        if self._sync:
            self._deliver(key, text)
            return
        try:
            threading.Thread(
                target=self._deliver,
                args=(key, text),
                name="tg-alert",
                daemon=True,
            ).start()
        except Exception as exc:
            sys.stderr.write(f"[error-alerts] could not start send: {exc}\n")
            with self._lock:
                st = self._state.get(key)
                if st:
                    st["inflight"] = False

    def _deliver(self, key: str, text: str) -> None:
        ok = False
        try:
            ok = bool(self._notifier.send_message_sync(text))
        except Exception as exc:
            sys.stderr.write(f"[error-alerts] send failed: {exc}\n")
        with self._lock:
            st = self._state.get(key)
            if not st:
                return
            st["inflight"] = False
            st["last_sent"] = time.monotonic()
            if ok:
                st.pop("retry_in", None)
            else:
                st["retry_in"] = 60

    def _decide(
        self, message: str, level: int
    ) -> Tuple[Optional[str], Optional[str]]:
        key, title, detail = self._classify(message, level)
        if not key:
            return None, None
        now = time.monotonic()
        clock = datetime.now().strftime("%H:%M:%S")
        with self._lock:
            st = self._state.get(key)
            if st and st.get("inflight"):
                st["suppressed"] = int(st.get("suppressed") or 0) + 1
                st["last_text"] = detail
                return None, None
            if st is None or not st.get("open"):
                self._state[key] = {
                    "open": True,
                    "inflight": True,
                    "last_sent": now,
                    "suppressed": 0,
                    "opened_at": clock,
                    "last_text": detail,
                }
                return self._html(title, [clock, detail]), key

            wait = int(st.get("retry_in") or self.repeat_after_s)
            elapsed = now - float(st.get("last_sent") or 0)
            st["last_text"] = detail
            if elapsed < wait:
                st["suppressed"] = int(st.get("suppressed") or 0) + 1
                return None, None

            extra = int(st.get("suppressed") or 0) + 1
            since = st.get("opened_at") or ""
            st["suppressed"] = 0
            st["inflight"] = True
            lines = [f"{extra} more since {since}".strip(), f"Latest: {detail}"]
            return self._html(f"{title} — still failing", lines), key

    def _classify(
        self, message: str, level: int
    ) -> Tuple[Optional[str], str, str]:
        if self._telegram_noise(message):
            return None, "", ""

        match = _CAPTURE_RE.search(message)
        if match:
            did = match.group(1).strip()
            reason = _clean_reason(match.group(2))
            label = self._device_label(did)
            return (
                f"capture:{did}",
                f"{label} capture down",
                reason or "connection failed",
            )

        match = _ENQUEUE_RE.search(message)
        if match:
            did = match.group(1).strip()
            return (
                f"enqueue:{did}",
                f"{self._device_label(did)} punch not saved",
                _clean_reason(match.group(2)) or "enqueue failed",
            )

        match = _HISTORY_RE.search(message)
        if match:
            did = match.group(2).strip().rstrip(":")
            kind = match.group(1)
            return (
                f"history:{did}",
                f"{self._device_label(did)} {kind} failed",
                _clean_reason(match.group(3)) or "history pull failed",
            )

        match = _SYNC_RE.search(message)
        if match or "Pusher:" in message or "Pusher loop" in message:
            detail = _clean_reason(message)
            if match:
                detail = _clean_reason(match.group(2)) or detail
                pending = match.group(1)
                if pending:
                    detail = f"{detail} ({pending})"
            return ("erp-push", "ERP sync failed", detail or "push failed")

        match = _WORKER_RE.search(message)
        if match:
            did = match.group(1).strip()
            code = (match.group(2) or "").strip()
            detail = f"Process exited ({code}). Watchdog is restarting it." if code else (
                "Capture process died. Watchdog is restarting it."
            )
            return (
                f"worker:{did}",
                f"{self._device_label(did)} capture process died",
                detail,
            )

        low = message.lower()
        if "sqlite" in low and any(
            word in low for word in ("corrupt", "unusable", "repair failed")
        ):
            return ("sqlite", "Attendance queue database error", _clean_reason(message))
        if "no devices answered" in low:
            return (
                "devices-unreachable",
                "No attendance terminal answered",
                _clean_reason(message),
            )
        if "network/dns not ready" in low:
            return ("network", "Network not ready", _clean_reason(message))
        if "drain timed out" in low or "erp drain timed out" in low:
            return (
                "drain-timeout",
                "Punches still waiting to reach ERP",
                _clean_reason(message),
            )
        if "failed to spawn" in low or "failed to resume capture" in low:
            return ("worker-spawn", "Could not start device capture", _clean_reason(message))
        if "fatal error" in low or "main loop" in low:
            return ("fatal", "Attendance service error", _clean_reason(message))

        if level >= logging.ERROR:
            norm = re.sub(r"\d+", "N", message)
            norm = re.sub(r"\s+", " ", norm).strip().lower()[:180]
            return ("other:" + norm, "Attendance error", _clean_reason(message))
        return None, "", ""

    def _telegram_noise(self, message: str) -> bool:
        low = message.lower()
        if "telegram" not in low:
            return False
        return any(part in low for part in _TELEGRAM_NOISE)

    def _device_label(self, device_id: str) -> str:
        ip = self._ips.get(str(device_id), "")
        if ip:
            return f"Device {device_id} ({ip})"
        return f"Device {device_id}"

    def _html(self, title: str, lines: List[str], ok: bool = False) -> str:
        mark = "✅" if ok else "❌"
        body = "\n".join(html.escape(line) for line in lines if line)
        return (
            f"{mark} <b>{html.escape(self.system_name)}</b>\n"
            f"<b>{html.escape(title)}</b>\n"
            f"{body}"
        )


_off_logged = False


def attach_error_alerts(
    log: logging.Logger,
    config: Optional[dict],
    system_name: str,
    devices: Optional[List[dict]] = None,
) -> Optional[ErrorAlertHandler]:
    """Attach the alert handler once. Safe to call from the parent and workers."""
    global _off_logged
    for handler in list(log.handlers):
        if isinstance(handler, ErrorAlertHandler):
            return handler

    settings = resolve_alert_settings(config)
    if not alerts_configured(settings):
        if not _off_logged:
            _off_logged = True
            log.info(
                "Error alerts OFF. Set telegram_alerts.enabled, bot_token, and "
                "chat_id (a different bot from status updates), then restart."
            )
        return None

    repeat = int(settings.get("repeat_after_s") or 600)
    notifier = TelegramNotifier(
        bot_token=str(settings.get("bot_token") or "").strip(),
        chat_id=str(settings.get("chat_id") or "").strip(),
        enabled=True,
        system_name=system_name,
    )
    handler = ErrorAlertHandler(
        notifier,
        system_name,
        devices,
        repeat_after_s=repeat,
    )
    log.addHandler(handler)
    log.info(
        "Error alerts ON (separate Telegram bot, reminder every %s min)",
        handler.repeat_after_s // 60,
    )
    return handler


def announce_error_alerts(handler: Optional[ErrorAlertHandler]) -> None:
    """One message when the daemon starts, so the alert chat is confirmed live."""
    if handler is None:
        return
    text = handler._html(
        "Error watch is on",
        [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Device capture failures, broken terminal links, and ERP sync "
            "errors are sent here.",
            "The first one arrives immediately. If the same failure keeps "
            f"happening, a reminder follows every {handler.repeat_after_s // 60} "
            "min until it recovers.",
        ],
        ok=True,
    )
    try:
        handler._notifier.send_message_sync(text)
    except Exception as exc:
        sys.stderr.write(f"[error-alerts] startup ping failed: {exc}\n")


def tap_capture_stdout(
    device_id: Any, handler: Optional[ErrorAlertHandler] = None
) -> None:
    current = sys.stdout
    if isinstance(current, _StdoutTap):
        return
    sys.stdout = _StdoutTap(current, device_id)


def _self_check() -> None:
    sent: List[str] = []

    class _Fake:
        def send_message_sync(self, message: str, parse_mode: str = "HTML") -> bool:
            sent.append(message)
            return True

    handler = ErrorAlertHandler(
        _Fake(),  # type: ignore[arg-type]
        "PACE_ATTENDANCE",
        [{"device_id": 3, "ip_address": "10.30.141.5"}],
        repeat_after_s=600,
        sync=True,
    )

    def emit(level: int, msg: str) -> None:
        handler.emit(
            logging.LogRecord("AttendanceZTech", level, "", 0, msg, (), None)
        )

    emit(
        logging.ERROR,
        "❌ [device 3] capture error: timed out; reconnecting in 120s",
    )
    assert len(sent) == 1, sent
    assert "10.30.141.5" in sent[0]
    assert "timed out" in sent[0]
    emit(
        logging.ERROR,
        "❌ [device 3] capture error: timed out; reconnecting in 5s",
    )
    assert len(sent) == 1, sent
    handler._state["capture:3"]["last_sent"] -= 601
    emit(
        logging.ERROR,
        "❌ [device 3] capture error: [Errno 104] Connection reset by peer; reconnecting in 120s",
    )
    assert len(sent) == 2, sent
    assert "more since" in sent[-1]
    handler.note_recovered(3)
    assert len(sent) == 3, sent
    assert "restored" in sent[-1]
    handler.note_recovered(3)
    assert len(sent) == 3
    emit(logging.ERROR, "Telegram send failed (attempt 1/3): boom")
    assert len(sent) == 3
    emit(
        logging.ERROR,
        "❌ Sync failed (attempt #2, pending=10): HTTP 500",
    )
    assert len(sent) == 4
    assert "ERP sync failed" in sent[-1]
    emit(logging.INFO, "🕘 [device 3] punch user=1 @ now")
    assert len(sent) == 4
    print("error alert self-check ok")


if __name__ == "__main__":
    _self_check()
