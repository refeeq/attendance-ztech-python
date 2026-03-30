# 📘 Attendance ZTech — Complete Documentation & User Guide

> **Version:** 1.0 · **Last updated:** February 2026  
> A comprehensive guide to understanding, deploying, debugging, and controlling the Attendance ZTech system.

---

## Table of Contents

1. [The Story — What Is This Project?](#1-the-story--what-is-this-project)
2. [How It Works — The Big Picture](#2-how-it-works--the-big-picture)
3. [File-by-File Breakdown](#3-file-by-file-breakdown)
4. [Configuration — `config.json` Explained](#4-configuration--configjson-explained)
5. [Deployment Guide](#5-deployment-guide)
6. [How the System Runs Day-to-Day](#6-how-the-system-runs-day-to-day)
7. [Telegram Notifications](#7-telegram-notifications)
8. [Logs & Debugging](#8-logs--debugging)
9. [Control & Management](#9-control--management)
10. [Utility Scripts](#10-utility-scripts)
11. [Troubleshooting Cookbook](#11-troubleshooting-cookbook)
12. [Architecture Diagram](#12-architecture-diagram)
13. [FAQ](#13-faq)

---

## 1. The Story — What Is This Project?

Imagine you have a building (or several buildings) where employees walk in every morning and tap their fingers on a biometric device mounted on the wall. That device is a **ZKTeco attendance machine**. It records *who* tapped and *when*.

Now, someone in HR needs to see all those records on a website or an ERP system, far away from the machine itself. **This project is the bridge.** It is a small Python program that:

1. **Connects** to one or more ZKTeco devices over the local network.
2. **Listens** for new attendance punches in **real time** — the moment someone taps, the data flows.
3. **Collects** those punches into a small basket (buffer).
4. **Pushes** that basket to your server (an API endpoint) whenever it fills up, or once a minute, whichever comes first.
5. **Sends you Telegram messages** so you always know what's happening — even from your phone.

Think of it as a **courier service**: the biometric device is the warehouse, your server is the customer, and this program is the delivery truck that keeps going back and forth automatically, every day, all day, with zero manual effort.

---

## 2. How It Works — The Big Picture

Here's what happens from the moment you start the system to when you finally shut it down:

### 🚀 Startup Sequence

```
1. System boots up
2. Waits up to 120 seconds for the network/internet to be ready
3. Pings the ZKTeco devices (waits up to 60 seconds for at least one to respond)
4. Sends a "System Started" Telegram message
5. Creates a shared buffer (a temporary basket for attendance records)
6. Spawns one process PER DEVICE to capture attendance in real time
7. Enters the main loop
```

### 🔄 Main Loop (runs forever)

Every second, the main loop checks three things:

| Check | What happens | How often |
|---|---|---|
| **Reconnect timer** | Kills all device processes and restarts them (fresh connections) | Every **15 minutes** |
| **End-of-Day timer** | Fetches ALL of today's logs from each device and pushes them | Once a day at **23:59** |
| **Buffer overflow** | If the buffer has ≥ `buffer_limit` records, push them to the server | Whenever the buffer fills up |

### 🛑 Shutdown

When you press `Ctrl+C` or the system shuts down:

1. All device processes are terminated.
2. Any remaining records in the buffer are flushed (pushed) to the server.
3. A "System Stopped" Telegram message is sent.
4. The program exits.

---

## 3. File-by-File Breakdown

| File | Purpose | When it runs |
|---|---|---|
| `main.py` | **The brain.** Real-time capture, buffering, pushing, end-of-day collection, 15-minute reconnect cycle. | Always — this is the main service |
| `config.json` | **The settings file.** Devices, server endpoint, buffer size, Telegram config. | Read on startup by every script |
| `sync_all.py` | **Manual full sync tool.** Fetches ALL logs from devices (optionally filtered by date range) and pushes them in batches. | Run manually when you need a historical sync |
| `boot_sync_30d.py` | **Boot-time sync.** Automatically runs `sync_all.py` for the last 30 days whenever the system reboots. | Run automatically on system boot (via systemd/cron) |
| `telegram_notifier.py` | **Telegram messenger.** The class that sends formatted HTML messages to your Telegram chat. | Used by `main.py` and `boot_sync_30d.py` |
| `test_telegram.py` | **Telegram test script.** Sends test messages to verify your Telegram setup is correct. | Run manually to test Telegram |
| `Dockerfile` | **Docker packaging.** Builds a Docker image of the system for containerized deployment. | Used during Docker deployment |
| `requirements.txt` | **Python dependencies.** Lists `pyzk`, `httpx`, `psutil`, `colorama`, `python-telegram-bot`. | Used during installation |
| `TELEGRAM_SETUP.md` | **Telegram setup guide.** Step-by-step instructions for configuring the Telegram bot. | Reference document |
| `.gitignore` | **Git ignore rules.** Keeps logs, virtual environments, and system files out of version control. | Used by Git |
| `log.txt` | **Local log file.** Rolling text log of all system activity. | Written to continuously |
| `logs/attendance.log` | **Structured log file.** Same content as `log.txt` but in the `logs/` directory. | Written to continuously |

---

## 4. Configuration — `config.json` Explained

This is the **single most important file** you'll edit. Here's what every field means:

```json
{
  "endpoint": "https://your-server.com/erp-api/sync/empAttSync.php",
  "name": "PMBS-attendance",
  "devices": [
    {
      "device_id": 1,
      "ip_address": "10.50.141.32",
      "port": 4370,
      "password": "4546"
    }
  ],
  "buffer_limit": 3,
  "log_level": "INFO",
  "telegram": {
    "bot_token": "YOUR_BOT_TOKEN_HERE",
    "chat_id": "YOUR_CHAT_ID_HERE",
    "enabled": true,
    "notifications": {
      "startup": true,
      "end_of_day": true,
      "data_push": true,
      "errors": true,
      "device_status": true
    }
  }
}
```

### Field Reference

| Field | Type | Description |
|---|---|---|
| `endpoint` | String | The full URL of the API where attendance data is POSTed. The server must accept a JSON body with `{"Json": [...records...]}` format. |
| `name` | String | A friendly name for this instance (e.g., "PMBS-attendance"). Used in Telegram messages and logs to identify which system is talking. |
| `devices` | Array | List of ZKTeco devices to connect to. You can have one or many. |
| `devices[].device_id` | Number | A unique ID you assign to identify this device. Sent along with each attendance record. |
| `devices[].ip_address` | String | The local network IP address of the ZKTeco device (e.g., `"10.50.141.32"`). |
| `devices[].port` | Number | The port the device listens on. Almost always **4370** (default ZKTeco port). |
| `devices[].password` | String | The device communication password. Default is `0` if your device has no password set. |
| `buffer_limit` | Number | How many attendance records to collect before pushing to the server. Lower = more frequent pushes. Higher = fewer API calls. **Recommended:** 3–10 for real-time use, 50–100 for batch-heavy environments. |
| `log_level` | String | Logging verbosity: `"DEBUG"`, `"INFO"`, `"WARNING"`, or `"ERROR"`. Use `"DEBUG"` when troubleshooting. |
| `telegram.bot_token` | String | Your Telegram bot token from @BotFather. |
| `telegram.chat_id` | String | The Telegram chat/group ID where notifications will be sent. |
| `telegram.enabled` | Boolean | `true` to enable Telegram notifications, `false` to disable them entirely. |
| `telegram.notifications.*` | Boolean | Fine-grained control over which notification types to send. Set any to `false` to mute that category. |

### Adding Multiple Devices

To monitor more than one device, just add more objects to the `devices` array:

```json
"devices": [
  {
    "device_id": 1,
    "ip_address": "10.50.141.32",
    "port": 4370,
    "password": "4546"
  },
  {
    "device_id": 2,
    "ip_address": "10.50.141.33",
    "port": 4370,
    "password": "0"
  },
  {
    "device_id": 3,
    "ip_address": "192.168.1.100",
    "port": 4370,
    "password": ""
  }
]
```

Each device gets its own dedicated process for real-time capture.

---

## 5. Deployment Guide

### Option A: Direct Deployment on a Linux/Mac Server (Recommended)

This is the simplest approach. You run the Python script directly on a machine that is on the **same network** as the ZKTeco devices.

#### Step 1: Clone the project

```bash
git clone <your-repo-url> /opt/attendance-ztech
cd /opt/attendance-ztech
```

#### Step 2: Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

#### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

#### Step 4: Edit configuration

```bash
nano config.json
# Set your endpoint, device IPs, Telegram tokens, etc.
```

#### Step 5: Test it manually

```bash
python main.py
```

You should see logs like:
```
=== Attendance ZTech System Started ===
Configured devices: 1 | Endpoint: https://... | Buffer limit: 3 | Telegram: ENABLED
✅ Network/DNS looks OK
✅ Ping OK for at least one device
🔌 Starting RT capture for device 1 (10.50.141.32:4370)
```

Press `Ctrl+C` to stop.

#### Step 6: Set up as a systemd service (auto-start on boot)

Create a service file:

```bash
sudo nano /etc/systemd/system/attendance-ztech.service
```

Paste this content:

```ini
[Unit]
Description=Attendance ZTech System
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/attendance-ztech
ExecStart=/opt/attendance-ztech/venv/bin/python /opt/attendance-ztech/main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable attendance-ztech.service
sudo systemctl start attendance-ztech.service
```

#### Step 7: Set up boot sync (30-day historical sync on reboot)

Create another service:

```bash
sudo nano /etc/systemd/system/attendance-boot-sync.service
```

```ini
[Unit]
Description=Attendance ZTech 30-Day Boot Sync
After=network-online.target attendance-ztech.service
Wants=network-online.target

[Service]
Type=oneshot
User=root
WorkingDirectory=/opt/attendance-ztech
ExecStart=/opt/attendance-ztech/venv/bin/python /opt/attendance-ztech/boot_sync_30d.py
RemainAfterExit=no

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable attendance-boot-sync.service
```

---

### Option B: Docker Deployment

The project includes a `Dockerfile` that packages everything into a container.

#### Build the image

```bash
docker build -t attendance-ztech .
```

#### Run the container

```bash
docker run -d \
  --name attendance-ztech \
  --restart=always \
  --network=host \
  attendance-ztech
```

> **Important:** `--network=host` is required so the container can reach the ZKTeco devices on the local network. Without it, the container is isolated and cannot communicate with the devices.

#### View logs

```bash
docker logs -f attendance-ztech
```

#### Stop / Restart

```bash
docker stop attendance-ztech
docker start attendance-ztech
docker restart attendance-ztech
```

---

### Option C: Windows Deployment

The README references Windows-specific files (`install_service.bat`, `windows_service.py`, `view_logs.bat`) for running as a Windows Service. If deploying on Windows:

1. Install Python 3.7+ and check "Add Python to PATH".
2. Right-click `install_service.bat` → "Run as administrator".
3. The system installs as a Windows Service that starts on boot.

---

## 6. How the System Runs Day-to-Day

Once deployed, the system runs 24/7 without intervention. Here is its daily timeline:

```
┌─────────────────────────────────────────────────────────────────┐
│                    A TYPICAL DAY                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  00:00  System running, real-time capture active                │
│         ↕ Every 15 min: reconnect to all devices                │
│         ↕ Attendance punches flow in real time                  │
│         ↕ Buffer fills → push to server                         │
│         ↕ Every 60 sec: periodic flush (if buffer has data)     │
│                                                                 │
│  08:00  Employees start arriving, punches increase              │
│         → Each punch logged and pushed within seconds           │
│                                                                 │
│  17:00  Employees leave, punches spike again                    │
│                                                                 │
│  23:59  END-OF-DAY TASK TRIGGERS                                │
│         → System fetches ALL of today's logs from each device   │
│         → Pushes everything to the server (catches any misses)  │
│         → Sends Telegram summary                                │
│                                                                 │
│  00:00  New day begins, cycle repeats                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Why the 15-minute reconnect?

ZKTeco devices sometimes drop connections silently. By reconnecting every 15 minutes, the system ensures that a dropped connection doesn't go unnoticed for long. It kills the old device processes and starts fresh ones.

### Why the End-of-Day task?

Real-time capture is great, but network hiccups can cause some punches to be missed. The End-of-Day (EoD) task at 23:59 reads **all** of today's logs directly from the device memory, guaranteeing that nothing is lost. Your server API should handle duplicates gracefully (idempotent).

### What is the buffer?

The buffer is a shared list (using Python's `multiprocessing.Manager`) that all device processes write to. When it reaches the `buffer_limit` count, it is flushed (pushed) to the server. There is also a **time-based flush** inside each device process (every 60 seconds) and a **size-based flush** in the main loop as a safety net.

---

## 7. Telegram Notifications

The system sends real-time Telegram messages for important events. Here are the notification types:

| Event | Emoji | When it fires |
|---|---|---|
| System Started | 🚀 | When `main.py` boots up |
| System Stopped | 👋 | When the system shuts down gracefully |
| System Error | ❌ | When an unexpected error occurs in the main loop |
| Data Push Success | ✅ | Every time records are successfully pushed to the server |
| Data Push Failed | ❌ | When an API push fails (HTTP error or network issue) |
| End-of-Day Started | 🧹 | When the 23:59 EoD task begins |
| End-of-Day Complete | 🧹 | When the EoD task finishes (summary of OK/Failed devices) |
| 30-Day Boot Sync Started | 🚀 | When `boot_sync_30d.py` starts |
| 30-Day Boot Sync Complete | ✅ | When the boot sync finishes |
| Shutdown (Ctrl+C) | ⏹️ | When you manually stop the system |

### Controlling Notifications

In `config.json`, you can turn individual notification types on/off:

```json
"notifications": {
  "startup": true,       // 🚀 System start messages
  "end_of_day": true,    // 🧹 EoD messages
  "data_push": true,     // ✅/❌ Push success/failure
  "errors": true,        // ❌ Error alerts
  "device_status": true  // 📱 Device connection status
}
```

To **completely disable** all Telegram messages:

```json
"telegram": {
  "enabled": false
}
```

### Testing Telegram

```bash
python test_telegram.py
```

This sends 5 test messages to your chat — one for each notification type.

---

## 8. Logs & Debugging

### Where are the logs?

The system writes to **four** log destinations simultaneously:

| Location | Purpose |
|---|---|
| `logs/attendance.log` | Primary structured log file |
| `log.txt` | Local convenience log (same content) |
| `~/Desktop/AttendanceZTech Logs/attendance.log` | Desktop log (only if a Desktop folder exists) |
| **Console (stdout)** | Printed to the terminal if running interactively |

### Log format

```
2026-02-15 11:30:00 - INFO - ✅ Push success (3 records)
2026-02-15 11:30:15 - ERROR - ❌ Push failed HTTP 500: Internal Server Error
2026-02-15 11:45:00 - INFO - 🔁 Scheduled 15-min reconnect...
```

### Understanding the log emojis

| Emoji | Meaning |
|---|---|
| 🚀 | System startup |
| ✅ | Success (connection, push, etc.) |
| ❌ | Error or failure |
| ⚠️ | Warning (non-critical issue) |
| 🔌 | Device connection attempt |
| 🔁 | Reconnect cycle |
| 🕘 | New attendance punch recorded |
| 📤 | Pushing data to server |
| ⏱️ | Periodic flush |
| 🧹 | End-of-Day task |
| 📊 | Data statistics |
| ℹ️ | Informational |
| ⏹️ | System stopped by user |
| 👋 | System shutdown complete |
| 🧺 | Buffer initialized |

### Log levels

Set the `log_level` in `config.json`:

| Level | What you see |
|---|---|
| `DEBUG` | **Everything.** Very verbose. Use for troubleshooting. |
| `INFO` | Normal operations: connections, pushes, reconnects. **(Default)** |
| `WARNING` | Only warnings and errors. |
| `ERROR` | Only errors. Very quiet. |

### Reading logs in real time

```bash
# Follow the main log
tail -f logs/attendance.log

# Follow the local log
tail -f log.txt

# Search for errors
grep "ERROR" logs/attendance.log

# Search for a specific device
grep "device 1" logs/attendance.log

# Count today's pushes
grep "$(date '+%Y-%m-%d')" logs/attendance.log | grep "Push success" | wc -l
```

### Docker logs

```bash
docker logs -f attendance-ztech         # Follow logs
docker logs --tail 100 attendance-ztech  # Last 100 lines
docker logs --since 1h attendance-ztech  # Last hour
```

---

## 9. Control & Management

### Starting and stopping

#### Systemd (Linux)

```bash
# Start
sudo systemctl start attendance-ztech

# Stop
sudo systemctl stop attendance-ztech

# Restart
sudo systemctl restart attendance-ztech

# Check status
sudo systemctl status attendance-ztech

# View service logs
sudo journalctl -u attendance-ztech -f
```

#### Docker

```bash
docker start attendance-ztech
docker stop attendance-ztech
docker restart attendance-ztech
docker ps  # Check if running
```

#### Manual (foreground)

```bash
cd /opt/attendance-ztech
source venv/bin/activate
python main.py          # Runs in foreground, Ctrl+C to stop
```

### Changing configuration while running

1. Edit `config.json` with your changes.
2. **Restart** the service — configuration is only loaded at startup.

```bash
sudo systemctl restart attendance-ztech
# or
docker restart attendance-ztech
```

### Running a manual historical sync

If you need to sync data for a specific date range (e.g., because the system was offline for a few days):

```bash
cd /opt/attendance-ztech
source venv/bin/activate

# Sync last 7 days
python sync_all.py --from 2026-02-08 --to 2026-02-15

# Sync all available data (no date filter)
python sync_all.py

# Sync a specific device only
python sync_all.py --device-id 1 --from 2026-01-01 --to 2026-02-15

# Sync with smaller batches and more retries
python sync_all.py --from 2026-02-01 --to 2026-02-15 --chunk 100 --retries 5

# Sync with debug logging
python sync_all.py --from 2026-02-01 --to 2026-02-15 --log-level DEBUG
```

#### `sync_all.py` command-line options

| Option | Default | Description |
|---|---|---|
| `--from` | None (all data) | Start date (YYYY-MM-DD) |
| `--to` | None (up to now) | End date (YYYY-MM-DD) |
| `--device-id` | All devices | Sync only a specific device |
| `--chunk` | 500 | Batch size per API request |
| `--retries` | 3 | Number of retries per failed API batch |
| `--log-level` | From config | Override log level (`DEBUG`/`INFO`/`WARNING`/`ERROR`) |

---

## 10. Utility Scripts

### `boot_sync_30d.py` — 30-Day Boot Sync

**What it does:** Runs automatically on system startup. Calls `sync_all.py` with `--from` set to 30 days ago and `--to` set to tomorrow. This ensures that any data missed while the system was off gets synced.

**How it works:**
1. Waits for the network to be ready (up to 120 seconds).
2. Finds the virtual environment Python (`venv/bin/python`) and `sync_all.py`.
3. Runs the sync as a subprocess.
4. Retries once on failure (with a 10-second delay).
5. Sends Telegram notifications about start/success/failure.

**When to use manually:** If the system was offline for a while and you want to catch up on the last 30 days:

```bash
python boot_sync_30d.py
```

### `test_telegram.py` — Telegram Integration Test

**What it does:** Sends 5 test messages to your Telegram chat — one for each notification type (startup, data push, end-of-day, error, device status).

**When to use:**
- After initial setup to verify Telegram is configured correctly.
- After changing the `bot_token` or `chat_id`.
- When Telegram messages suddenly stop arriving.

```bash
python test_telegram.py
```

---

## 11. Troubleshooting Cookbook

### Problem: No data appearing on the server

1. **Check the logs:**
   ```bash
   grep "Push" logs/attendance.log | tail -20
   ```
2. **Is the endpoint correct?** Verify `endpoint` in `config.json`.
3. **Can the server be reached?**
   ```bash
   curl -X POST https://your-server.com/your-endpoint -H "Content-Type: application/json" -d '{"Json":[]}'
   ```
4. **Is the buffer filling up?** Lower `buffer_limit` to `1` for testing.

### Problem: "Failed to connect" to device

1. **Ping the device:**
   ```bash
   ping 10.50.141.32
   ```
2. **Check the port:**
   ```bash
   nc -zv 10.50.141.32 4370
   ```
3. **Verify the password:** The `password` field in `config.json` must match the device's communication password.
4. **Is the device on?** Check physically.
5. **Network isolation:** The machine running this script must be on the **same network** (or have routing) to the device.

### Problem: Telegram messages not arriving

1. **Run the test:**
   ```bash
   python test_telegram.py
   ```
2. **Check `enabled`:** Ensure `telegram.enabled` is `true` in `config.json`.
3. **Verify the bot token:** Make sure it's correct and the bot hasn't been deleted.
4. **Verify the chat ID:** Make sure it matches the chat where you want notifications.
5. **Check DNS:** Try `ping api.telegram.org`.

### Problem: System keeps disconnecting from devices

This is **normal behavior**. The system reconnects every 15 minutes by design. However, if you see constant connection failures:

1. **Check network stability** between the machine and the devices.
2. **Reduce the number of concurrent connections** — some devices only support 1-2 connections at a time.
3. **Check device firmware** — older firmware may have buggy TCP/IP stacks.

### Problem: Duplicate records on the server

The End-of-Day task re-fetches all of today's records, which may include records already pushed in real-time. **Your server API should handle duplicates.** Implement idempotency on the server side by checking for duplicate `(device_id, user_id, timestamp)` combinations.

### Problem: System using too much memory

1. Lower the `buffer_limit` so data is pushed more frequently.
2. Check if device processes are accumulating (they should be killed/recreated every 15 minutes).
3. Use `ps aux | grep python` to check running processes.

### Problem: Service won't start after reboot

```bash
# Check service status
sudo systemctl status attendance-ztech

# Check for errors in journal
sudo journalctl -u attendance-ztech --no-pager | tail -50

# Common fixes:
# - Ensure the venv exists and has all packages installed
# - Ensure config.json is valid JSON
# - Ensure the working directory in the service file is correct
```

---

## 12. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        YOUR SERVER / ERP                           │
│                   (receives attendance JSON)                       │
│                  POST /erp-api/sync/empAttSync.php                 │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ HTTPS
                               │
┌──────────────────────────────┴──────────────────────────────────────┐
│                     ATTENDANCE ZTECH SYSTEM                        │
│                      (this Python project)                         │
│                                                                    │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                        main.py                                │  │
│  │                                                               │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │  │
│  │  │  Process 1   │  │  Process 2   │  │  Process N   │         │  │
│  │  │  (Device 1)  │  │  (Device 2)  │  │  (Device N)  │         │  │
│  │  │  RT Capture  │  │  RT Capture  │  │  RT Capture  │         │  │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘         │  │
│  │         │                 │                 │                  │  │
│  │         └─────────────────┼─────────────────┘                 │  │
│  │                           │                                   │  │
│  │                    ┌──────▼──────┐                             │  │
│  │                    │   SHARED    │                             │  │
│  │                    │   BUFFER    │ ──→ push_to_server()        │  │
│  │                    │ (Manager)   │                             │  │
│  │                    └─────────────┘                             │  │
│  │                                                               │  │
│  │  ┌─────────────────────────────────────────────────────────┐  │  │
│  │  │ Main Loop (every 1s):                                   │  │  │
│  │  │   • 15-min reconnect check                              │  │  │
│  │  │   • 23:59 End-of-Day check                              │  │  │
│  │  │   • Buffer overflow check                               │  │  │
│  │  └─────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  ┌──────────────────┐  ┌──────────────────┐                        │
│  │ telegram_notifier │  │   config.json     │                       │
│  │   (sends alerts)  │  │   (all settings)  │                       │
│  └────────┬──────────┘  └──────────────────┘                        │
│           │                                                        │
└───────────┼────────────────────────────────────────────────────────┘
            │ HTTPS
            ▼
   ┌────────────────┐
   │   TELEGRAM      │
   │   (your chat)   │
   └────────────────┘

            ▲ TCP (port 4370)
            │
┌───────────┴───────────────────────────────────────────────────────┐
│                     ZKTeco DEVICES                                │
│                                                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │  Device 1    │  │  Device 2    │  │  Device N    │             │
│  │  10.50.x.x   │  │  10.50.x.x   │  │  192.168.x.x │            │
│  │  Port 4370   │  │  Port 4370   │  │  Port 4370   │            │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│                                                                   │
│  (Biometric fingerprint/card readers mounted on walls)            │
└───────────────────────────────────────────────────────────────────┘
```

---

## 13. FAQ

### Q: What happens if the internet goes down?

**A:** The system keeps capturing attendance data into the shared buffer. When the internet comes back, the buffer is pushed to the server on the next flush cycle. The End-of-Day task at 23:59 will also catch any missed records.

### Q: What happens if a ZKTeco device goes offline?

**A:** The process for that device will log an error and exit. On the next 15-minute reconnect cycle, the main loop will try to reconnect. The device's internal memory stores all punches, so no data is lost — it will be captured on the next successful connection.

### Q: Can I add a new device without restarting?

**A:** No. You must edit `config.json` and restart the service. The configuration is loaded once at startup.

### Q: What format is the data pushed to the server?

**A:** A JSON POST request with this structure:

```json
{
  "Json": [
    {
      "device_id": 1,
      "user_id": 12345,
      "timestamp": "2026-02-15 09:00:00",
      "status": 0,
      "punch": 0
    },
    {
      "device_id": 1,
      "user_id": 67890,
      "timestamp": "2026-02-15 09:01:00",
      "status": 0,
      "punch": 0
    }
  ]
}
```

### Q: What do `status` and `punch` mean?

**A:** These are ZKTeco-specific fields:
- `status`: Verification mode (0 = fingerprint, 1 = password, 2 = card, etc.)
- `punch`: Punch state (0 = check-in, 1 = check-out, etc.) — depends on device configuration.

### Q: How much memory/CPU does this use?

**A:** Very little. Each device process is lightweight. On a typical Raspberry Pi or small server, you can comfortably monitor 5-10 devices with under 100MB of RAM.

### Q: Can I run this on a Raspberry Pi?

**A:** Yes! The system is lightweight enough to run on a Raspberry Pi 3 or newer. Just ensure it's on the same network as your ZKTeco devices.

### Q: What Python version do I need?

**A:** Python 3.7 or newer. The Dockerfile uses Python 3.8.

### Q: How do I update the system?

**A:** Pull the latest code and restart:

```bash
cd /opt/attendance-ztech
git pull
source venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart attendance-ztech
```

---

## Quick Reference Card

| Action | Command |
|---|---|
| Start service | `sudo systemctl start attendance-ztech` |
| Stop service | `sudo systemctl stop attendance-ztech` |
| Restart service | `sudo systemctl restart attendance-ztech` |
| Check status | `sudo systemctl status attendance-ztech` |
| View live logs | `tail -f logs/attendance.log` |
| Manual sync (7 days) | `python sync_all.py --from 2026-02-08 --to 2026-02-15` |
| Manual sync (all data) | `python sync_all.py` |
| Boot sync (30 days) | `python boot_sync_30d.py` |
| Test Telegram | `python test_telegram.py` |
| Debug mode | Set `"log_level": "DEBUG"` in `config.json` and restart |
| Docker start | `docker start attendance-ztech` |
| Docker logs | `docker logs -f attendance-ztech` |

---

*Built with ❤️ for Pace Education · Attendance ZTech Python System*
