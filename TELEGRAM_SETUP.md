# Telegram Bot Integration Setup

This document explains how to set up and use the Telegram bot integration for the Attendance ZTech system.

## Overview

The Telegram bot integration provides real-time notifications about:
- System startup and shutdown
- 08:00 morning ERP completeness pass (today's punches)
- 24-hour end-of-day data pushing process
- Real-time data pushes when buffer limit is reached
- Device connection status
- Error notifications
- Local queue cleanup (90-day retention)
- Daily summary reports

## Setup Instructions

### 1. Create a Telegram Bot

1. Open Telegram and search for `@BotFather`
2. Send `/newbot` command
3. Follow the instructions to create your bot:
   - Choose a name for your bot (e.g., "Attendance ZTech Bot")
   - Choose a username for your bot (e.g., "attendance_ztech_bot")
4. Copy the bot token provided by BotFather (looks like: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`)

### 2. Get Your Chat ID

1. Start a chat with your newly created bot
2. Send any message to the bot (e.g., "Hello")
3. Visit this URL in your browser (replace `YOUR_BOT_TOKEN` with your actual token):
   ```
   https://api.telegram.org/botYOUR_BOT_TOKEN/getUpdates
   ```
4. Look for your chat ID in the response. It will be in the format:
   ```json
   "chat": {
     "id": 123456789,
     "first_name": "Your Name"
   }
   ```
5. Copy the chat ID number (e.g., `123456789`)

### 3. Update Configuration

Edit your `config.json` file and update the Telegram section:

```json
{
  "telegram": {
    "bot_token": "123456789:ABCdefGHIjklMNOpqrsTUVwxyz",
    "chat_id": "123456789",
    "enabled": true,
    "notifications": {
      "startup": true,
      "end_of_day": true,
      "morning_sync": true,
      "data_push": true,
      "errors": true,
      "device_status": true,
      "cleanup": true
    }
  }
}
```

### 3b. Separate bot for sync errors

Status messages (startup, successful pushes, end of day) stay on the bot above.
Capture timeouts, broken device links, and ERP sync failures go to a **second**
bot so that chat is only problems.

1. In BotFather, send `/newbot` again and create a different bot (for example
   `Pace Attendance Alerts`).
2. Open that bot and send it any message.
3. Open `https://api.telegram.org/bot<NEW_BOT_TOKEN>/getUpdates` and copy the
   chat `id`.
4. On the server, edit the `config.json` next to `main.py`:

```json
"telegram_alerts": {
  "enabled": true,
  "bot_token": "NEW_BOT_TOKEN",
  "chat_id": "YOUR_CHAT_ID",
  "repeat_after_s": 600
}
```

`repeat_after_s` is how long to wait before reminding you that the **same**
failure is still happening. The first failure is always sent immediately.
Device 3 timing out every second becomes one message, then a reminder with a
count, then a "capture restored" message when it reconnects.

5. Test, then restart the service:

```bash
python test_telegram.py --alerts
pm2 restart attendance-sync --update-env
```

In `pm2 logs` you should see `Error alerts ON`. The alert chat gets one
"Error watch is on" message at startup.

You can set `TELEGRAM_ALERTS_BOT_TOKEN`, `TELEGRAM_ALERTS_CHAT_ID`, and
`TELEGRAM_ALERTS_ENABLED=1` instead of putting the token in `config.json`.

### 4. Install Dependencies

Make sure you have the required dependencies installed:

```bash
pip install -r requirements.txt
```

### 5. Test the Integration

Run the test script to verify everything is working:

```bash
python test_telegram.py
```

This will send test notifications to your Telegram chat to confirm the setup is correct.

## Notification Types

### System Notifications
- **Startup**: Sent when the system starts monitoring devices
- **Shutdown**: Sent when the system is stopped (user termination or error)
- **Error**: Sent when critical errors occur

### Data Push Notifications
- **Real-time Push**: Sent when buffer limit is reached and data is pushed to server
- **Morning ERP Sync**: Sent at 08:00 when today's punches are re-pulled and drained to the ERP
- **End-of-Day Push**: Sent during the 24-hour data collection process
- **Push Success/Failure**: Detailed status of each data push operation

### Device Status Notifications
- **Connection Status**: When devices connect or disconnect
- **Device Errors**: When specific device errors occur

### Queue Cleanup
- **Startup cleanup**: How many synced punches older than the retention window were removed, remaining rows, and disk size before/after VACUUM
- **Scheduled cleanup**: Sent when the monthly (or configured) pass actually deleted rows
- **Cleanup failure**: Sent if the delete/VACUUM fails (also honors the `errors` flag)

### Daily Summary
- **End-of-Day Summary**: Complete summary of the day's operations
- **Success/Failure Statistics**: Overview of data push success rates

## Configuration Options

You can customize which notifications you receive by modifying the `notifications` section in `config.json`:

```json
"notifications": {
  "startup": true,        // System startup notifications
  "end_of_day": true,     // 24-hour data push notifications
  "morning_sync": true,   // 08:00 today-only ERP completeness pass
  "data_push": true,      // Real-time data push notifications
  "errors": true,         // Error notifications
  "device_status": true,  // Device connection status
  "cleanup": true         // Local 90-day queue cleanup
}
```

Set any notification type to `false` to disable it.

## Troubleshooting

### Bot Not Responding
1. Check that the bot token is correct
2. Verify the chat ID is correct
3. Make sure you've started a conversation with the bot
4. Check that `enabled` is set to `true` in config.json

### Messages Not Received
1. Run the test script: `python test_telegram.py`
2. Check the console output for error messages
3. Verify your internet connection
4. Check if the bot is blocked or restricted

### Configuration Issues
1. Ensure JSON syntax is valid in config.json
2. Check that all required fields are present
3. Verify the bot token format (should contain a colon)
4. Make sure chat ID is a number (not a string)

## Example Notifications

### System Startup
```
🚀 Attendance ZTech System Started

📅 Time: 2024-01-15 09:00:00
📱 Devices: 1
🌐 Endpoint: https://pcbs.paceeducation.com/erp-api/sync/empAttSync.php
📦 Buffer Limit: 3
✅ Status: System is now monitoring attendance devices
```

### End-of-Day Data Push
```
🌅 End-of-Day Data Push

📅 Time: 2024-01-15 23:59:00
📱 Device: 1
📊 Records: 25
✅ Status: Successfully pushed end-of-day data
```

### Data Push Success
```
📦 Data Push Success

📅 Time: 2024-01-15 14:30:00
📊 Records: 3
✅ Status: Successfully pushed to server
```

## Security Notes

- Keep your bot token secure and never share it publicly
- The bot token provides full access to your bot
- Consider using environment variables for production deployments
- Regularly rotate your bot token if compromised

## Support

If you encounter issues with the Telegram integration:

1. Check the logs in `log.txt` for detailed error messages
2. Run the test script to verify configuration
3. Ensure all dependencies are properly installed
4. Verify your Telegram bot setup with BotFather

The system will continue to function normally even if Telegram notifications fail, so your attendance monitoring will not be affected.
