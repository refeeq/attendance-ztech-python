import asyncio
import functools
import html
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx


def _format_punch_details_lines(
    records: List[Dict[str, Any]], max_lines: int = 20
) -> str:
    """One line per punch: device, user id (sent to ERP), time."""
    if not records:
        return ""
    lines: List[str] = []
    for r in records[:max_lines]:
        did = r.get("device_id", "?")
        uid = r.get("user_id", "?")
        ts = html.escape(str(r.get("timestamp", "?")))
        pch = r.get("punch")
        extra = f" · punch {pch}" if pch is not None else ""
        lines.append(f"• Device {did} · User {uid} · {ts}{extra}")
    rest = len(records) - max_lines
    if rest > 0:
        lines.append(f"… and {rest} more")
    return "\n".join(lines)


def apply_telegram_env_overrides(telegram_cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID from the environment over config."""
    out: Dict[str, Any] = dict(telegram_cfg or {})
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if token:
        out["bot_token"] = token
    if chat:
        out["chat_id"] = chat
    return out


class TelegramNotifier:
    """
    Telegram bot notifier for attendance system status updates.
    """

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        enabled: bool = True,
        notification_settings: Optional[Dict[str, bool]] = None,
        system_name: str = "Attendance System",
    ):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = enabled
        self.notification_settings = notification_settings or {}
        self.system_name = system_name
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.logger = logging.getLogger("AttendanceZTech.Telegram")

    def is_notification_enabled(self, notification_type: str) -> bool:
        """Check if a specific notification type is enabled."""
        if not self.enabled:
            return False
        return self.notification_settings.get(notification_type, True)

    def _post_message_sync(self, message: str, parse_mode: str = "HTML") -> bool:
        """Send message via synchronous HTTP (safe from any thread)."""
        if not self.enabled or not self.bot_token or not self.chat_id:
            return False

        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": parse_mode,
        }

        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.post(url, json=payload)
            try:
                response_data = response.json()
            except ValueError:
                self.logger.error(
                    f"Failed to parse Telegram response JSON. "
                    f"Status: {response.status_code}, Response: {response.text}"
                )
                return False

            if response.status_code != 200:
                self.logger.error(
                    f"Failed to send Telegram message. "
                    f"Status: {response.status_code}, Response: {response.text}"
                )
                return False

            if response_data.get("ok") is True:
                self.logger.debug("Telegram message sent successfully")
                return True

            self.logger.error(
                "Telegram API rejected message. "
                f"Error code: {response_data.get('error_code')}, "
                f"Description: {response_data.get('description')}, "
                f"Full response: {response.text}"
            )
            return False

        except Exception as e:
            self.logger.error(f"Error sending Telegram message: {e}")
            return False

    async def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        """
        Send a message to Telegram (async wrapper around sync HTTP).
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, functools.partial(self._post_message_sync, message, parse_mode)
        )

    def send_message_sync(self, message: str, parse_mode: str = "HTML") -> bool:
        """Synchronous send; uses thread-safe HTTP client."""
        return self._post_message_sync(message, parse_mode)

    def data_push_message_html(
        self,
        record_count: int,
        success: bool,
        *,
        records: Optional[List[Dict[str, Any]]] = None,
        error: Optional[str] = None,
        device_id: Optional[int] = None,
    ) -> str:
        """Build HTML for a data push result (success or error). Does not send."""
        header_emoji = "✅" if success else "❌"
        title_suffix = "Success" if success else "Error"
        device_suffix = f" (Device: {device_id})" if device_id else ""

        details_block = ""
        if records:
            block = _format_punch_details_lines(records)
            if block:
                details_block = f"\n\n👤 <b>Punches</b>\n{block}"

        err_block = ""
        if error:
            err_block = (
                f"\n\n🔧 <b>Error:</b> "
                f"<code>{html.escape(str(error)[:600])}</code>"
            )

        status_line = (
            "✅ <b>Status:</b> Uploaded"
            if success
            else "❌ <b>Status:</b> Failed"
        )

        return (
            f"{header_emoji} <b>{self.system_name} - Data Push {title_suffix}</b>\n\n"
            f"⌚ <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📄 <b>Records:</b> {record_count}{device_suffix}\n"
            f"{status_line}"
            f"{err_block}"
            f"{details_block}"
        ).strip()

    def send_data_push_notification_sync(
        self,
        record_count: int,
        success: bool,
        device_id: Optional[int] = None,
        records: Optional[List[Dict[str, Any]]] = None,
        error: Optional[str] = None,
    ) -> bool:
        """Notify ERP sync outcome; optional per-record punch lines."""
        if not self.is_notification_enabled("data_push"):
            return False

        message = self.data_push_message_html(
            record_count,
            success,
            records=records,
            error=error,
            device_id=device_id,
        )
        return self.send_message_sync(message)

    async def send_startup_notification(
        self, device_count: int, endpoint: str
    ) -> bool:
        """Send startup notification."""
        if not self.is_notification_enabled("startup"):
            return False

        message = f"""
🚀 <b>{self.system_name} Started</b>

📅 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📱 <b>Devices:</b> {device_count}
🌐 <b>Endpoint:</b> {endpoint}

✅ System is now monitoring attendance devices
        """
        return await self.send_message(message.strip())

    async def send_end_of_day_notification(
        self, device_id: int, record_count: int, success: bool
    ) -> bool:
        """Send end-of-day data push notification."""
        if not self.is_notification_enabled("end_of_day"):
            return False

        status_emoji = "✅" if success else "❌"
        status_text = "Successfully" if success else "Failed to"

        message = f"""
🌅 <b>{self.system_name} - End-of-Day Data Push</b>

📅 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📱 <b>Device:</b> {device_id}
📊 <b>Records:</b> {record_count}
{status_emoji} <b>Status:</b> {status_text} push data to server
        """
        return await self.send_message(message.strip())

    async def send_data_push_notification(
        self,
        record_count: int,
        success: bool,
        device_id: Optional[int] = None,
        records: Optional[List[Dict[str, Any]]] = None,
        error: Optional[str] = None,
    ) -> bool:
        """Send data push notification."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            functools.partial(
                self.send_data_push_notification_sync,
                record_count,
                success,
                device_id,
                records,
                error,
            ),
        )

    async def send_error_notification(
        self,
        error_type: str,
        error_message: str,
        device_id: Optional[int] = None,
    ) -> bool:
        """Send error notification."""
        if not self.is_notification_enabled("errors"):
            return False

        device_info = f" (Device: {device_id})" if device_id else ""

        message = f"""
❌ <b>{self.system_name} - Error Alert</b>

📅 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🔧 <b>Type:</b> {error_type}{device_info}
📝 <b>Message:</b> {error_message}
        """
        return await self.send_message(message.strip())

    async def send_device_status_notification(
        self, device_id: int, status: str, details: str = ""
    ) -> bool:
        """Send device status notification."""
        if not self.is_notification_enabled("device_status"):
            return False

        status_emoji = (
            "✅"
            if "success" in status.lower() or "connected" in status.lower()
            else "⚠️"
        )

        message = f"""
📱 <b>{self.system_name} - Device Status Update</b>

📅 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🔧 <b>Device:</b> {device_id}
{status_emoji} <b>Status:</b> {status}
{f"📝 <b>Details:</b> {details}" if details else ""}
        """
        return await self.send_message(message.strip())

    async def send_daily_summary(
        self,
        total_records: int,
        successful_pushes: int,
        failed_pushes: int,
        devices_status: Dict[int, str],
    ) -> bool:
        """Send daily summary notification."""
        if not self.is_notification_enabled("end_of_day"):
            return False

        success_rate = (
            (successful_pushes / (successful_pushes + failed_pushes) * 100)
            if (successful_pushes + failed_pushes) > 0
            else 0
        )

        message = f"""
📊 <b>{self.system_name} - Daily Summary Report</b>

📅 <b>Date:</b> {datetime.now().strftime('%Y-%m-%d')}
📈 <b>Total Records:</b> {total_records}
✅ <b>Successful Pushes:</b> {successful_pushes}
❌ <b>Failed Pushes:</b> {failed_pushes}
📊 <b>Success Rate:</b> {success_rate:.1f}%

📱 <b>Device Status:</b>
"""

        for dev_id, status in devices_status.items():
            status_emoji = "✅" if "connected" in status.lower() else "❌"
            message += f"• Device {dev_id}: {status_emoji} {status}\n"

        return await self.send_message(message.strip())

    def test_connection(self) -> bool:
        """Test Telegram bot connection."""
        if not self.enabled or not self.bot_token or not self.chat_id:
            return False

        test_message = (
            f"🧪 <b>{self.system_name} - Test Message</b>\n\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"✅ Telegram bot is working correctly!"
        )
        return self.send_message_sync(test_message)
