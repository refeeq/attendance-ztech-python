# Attendance ZTech

> A small, self-healing program that takes every fingerprint punch from a
> school's biometric devices and reliably delivers it to the ERP — no matter
> what the network, the devices, or the server does.

This README is written so anyone, including someone who has never seen Python
before, can understand exactly what this project does, how it works, why it is
built the way it is, and how to operate it on a school server.

---

## Table of Contents

1. [The Story](#1-the-story)
2. [The Big Picture in One Diagram](#2-the-big-picture-in-one-diagram)
3. [Meet the Cast — what each file is for](#3-meet-the-cast)
4. [A Day in the Life of One Fingerprint Punch](#4-a-day-in-the-life-of-one-fingerprint-punch)
5. [The Three Safety Nets](#5-the-three-safety-nets)
6. [The Local Logbook — the durable queue](#6-the-local-logbook)
7. [The Watchdog — how it heals itself](#7-the-watchdog)
8. [The `config.json` File, field by field](#8-the-configjson-file)
9. [Installing on a school server](#9-installing-on-a-school-server)
10. [A Typical Day, hour by hour](#10-a-typical-day-hour-by-hour)
11. [Telegram alerts](#11-telegram-alerts)
12. [Logs — where they are, how to read them](#12-logs)
13. [Health checks an admin can run](#13-health-checks)
14. [Failure scenarios — and how the system recovers automatically](#14-failure-scenarios)
15. [Common operations](#15-common-operations)
16. [Troubleshooting cookbook](#16-troubleshooting)
17. [FAQ](#17-faq)
18. [Architecture diagram](#18-architecture-diagram)
19. [Quick reference card](#19-quick-reference-card)

---

## 1. The Story

Imagine a school in the morning. Children, teachers and staff walk in and tap
their finger on a small black-and-white box mounted near the door. That box is
a **ZKTeco biometric device**. Every tap becomes a record:

> "User #1042 punched at 08:32:11 today."

Now imagine the school office on the other side of the building. The HR /
academic team uses a website (the **ERP**) to see who came in, who is late,
who is absent. The ERP lives on a server somewhere on the internet
(`https://pmbs.paceeducation.com/erp-api/sync/empAttSync.php`, for example).

Between the wall device and the ERP there is a problem:

* The biometric device speaks an old industrial protocol on the local network.
* The ERP speaks modern HTTPS / JSON.
* Networks fail. Devices reboot. Power flickers. Servers restart.
* Nine schools, nine servers, no one wants to babysit them.

**This project is the bridge.** It is one small program that runs on a
small Linux/Windows machine inside the school, on the same network as the
biometric devices. It talks to the devices in their language, and to the ERP
in its language. It does this 24×7, completely automatically, and is built so
that **no punch is ever lost**, even if everything around it misbehaves.

Think of it as a **courier service**:

* The biometric device is a **warehouse** that records goods.
* The ERP is the **customer** waiting for the goods.
* This program is the **delivery truck** that drives back and forth.
* The local computer has a small **logbook** (a tiny database file) that knows
  exactly which goods have been picked up, which have been delivered, and
  which are still in the truck. If the truck crashes, the logbook is still
  there when the new truck starts.

That logbook is the most important upgrade in this project. Older versions did
not have it — they kept their list of pending punches in memory only, so a
crash, a reboot, or a brief power dip could erase records that had not yet
been delivered. **The logbook fixes that permanently.**

---

## 2. The Big Picture in One Diagram

```
                ┌──────────────────────────────────┐
                │           ERP server             │
                │  https://…/empAttSync.php        │
                └──────────────▲───────────────────┘
                               │ HTTPS POST {"Json":[…]}
                               │
┌──────────────────────────────┴───────────────────────────────────────┐
│                School server (this project runs here)                │
│                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                │
│  │ Capture #1   │  │ Capture #2   │  │ Capture #N   │                │
│  │  (Device 1)  │  │  (Device 2)  │  │  (Device N)  │                │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                │
│         │ enqueue         │ enqueue         │ enqueue                │
│         └─────────────────┼─────────────────┘                        │
│                           │                                          │
│              ┌────────────▼─────────────┐                            │
│              │  data/attendance_queue   │  ◄── the durable logbook   │
│              │  .db   (SQLite, WAL)     │                            │
│              └────────────▲─────────────┘                            │
│                           │ fetch unsynced                           │
│                           │ mark synced after 2xx                    │
│              ┌────────────┴─────────────┐                            │
│              │  Pusher thread           │ ── HTTPS ──► ERP           │
│              └──────────────────────────┘                            │
│                                                                      │
│  ┌──────────────────┐  ┌──────────────────┐                          │
│  │ Watchdog (30s)   │  │ End-of-Day       │   ┌────────────────┐     │
│  │ respawns dead    │  │ 23:55–23:59      │ ─►│ Telegram alerts│     │
│  │ capture workers  │  │ catch-up safety  │   └────────────────┘     │
│  └──────────────────┘  └──────────────────┘                          │
└──────────────────────────────▲───────────────────────────────────────┘
                               │ TCP port 4370 (ZK protocol)
        ┌──────────────────────┴──────────────────────┐
        │                Biometric devices            │
        │  ┌───────────┐  ┌───────────┐  ┌──────────┐ │
        │  │ Device 1  │  │ Device 2  │  │ Device N │ │
        │  └───────────┘  └───────────┘  └──────────┘ │
        └─────────────────────────────────────────────┘
```

If you understand only this picture, you understand the whole project.

---

## 3. Meet the Cast

Each file has one job. You don't need to read the code to use the system.

| File | Plain-English role |
|---|---|
| `main.py` | **The driver.** The always-on program that connects to the devices, listens for punches, runs the truck, and orchestrates everything. This is the service that systemd / Docker keeps alive 24×7. |
| `storage.py` | **The logbook.** A tiny library that owns the local database file. Every punch goes through it. It promises: "if I told you I saved a record, it is permanently saved, even if the machine loses power right now." |
| `sync_all.py` | **The fetch tool.** A command you can run once to ask each device "give me your last N days of punches" and feed them into the logbook. Useful for backfills and one-off reconciliations. |
| `boot_sync_30d.py` | **The boot recovery tool.** Runs automatically when the machine starts up. Calls `sync_all.py` for the last 60 days so any punches from while the machine was off are not lost. |
| `telegram_notifier.py` | **The messenger.** Sends short status messages to a Telegram chat / group so a human can see at a glance "today's sync is healthy." |
| `test_telegram.py` | **A test button.** Run it once after setting up Telegram to confirm messages actually land in your chat. |
| `config.json` | **The settings file.** All school-specific values: ERP URL, device list, Telegram token, etc. |
| `Dockerfile` | **A boxed copy of the system.** Lets you run the project in a Docker container instead of installing Python yourself. |
| `requirements.txt` | **The shopping list of Python libraries** the project needs. |
| `data/attendance_queue.db` | **The actual logbook file**, created automatically. Plain SQLite. Don't delete it casually — it remembers what hasn't been delivered yet. |
| `logs/`, `log.txt` | **The diary.** Auto-rotating text files that record everything the system did. |
| `DOCUMENTATION.md` | The longer reference manual (more options, deployment recipes, troubleshooting cookbook). |
| `TELEGRAM_SETUP.md` | Step-by-step for creating the Telegram bot. |

---

## 4. A Day in the Life of One Fingerprint Punch

Let's follow a single punch, end to end.

> 08:32:11 — Aisha (User #1042) taps her finger on the device by Gate A.

1. **The device records it.** Internally the ZKTeco device stores
   `(user=1042, time=08:32:11, status=…, punch=…)` in its own memory.
2. **The capture worker hears it.** This project has one small worker process
   running per device, all day long, holding an open "live capture"
   connection. The device pushes the new punch to that worker within
   milliseconds.
3. **The worker writes it to the local logbook.** A single SQLite `INSERT OR
   IGNORE` lands the record on disk. If the same record is ever offered
   again (because of a retry, a boot-time recovery, or the nightly catch-up),
   it is silently ignored — the database is **idempotent** on the natural key
   `(device_id, user_id, timestamp, status, punch)`. No duplicates ever.
4. **The pusher thread picks it up.** Every few seconds the pusher asks the
   logbook "give me everything still marked unsynced" and packages those
   records into a single HTTPS request to the ERP:

   ```json
   POST https://…/empAttSync.php
   {
     "Json": [
       {"device_id": 1, "user_id": 1042,
        "timestamp": "2026-04-26 08:32:11", "status": 0, "punch": 0},
       …
     ]
   }
   ```
5. **The ERP replies.**
   * If the ERP returns a successful HTTP 2xx → the pusher tells the logbook
     "these record IDs are delivered" and the rows are marked `synced = 1`.
   * If the ERP returns an error, or the network drops, or the request times
     out → **nothing is marked synced**, the rows stay in the logbook, and
     the pusher will retry on the next cycle with exponential backoff. A
     Telegram alert is sent (rate-limited so the chat is not spammed).
6. **The logbook self-cleans.** Once a row has been synced for more than 14
   days (configurable), it is purged so the database stays small.

That is the entire happy path. Now look at every step where something can go
wrong — and notice that none of them lose the punch:

* If the **capture worker crashes**, the watchdog respawns it within 30s.
* If the **machine reboots**, the records on disk are still there. The pusher
  picks up where it left off as soon as the daemon starts again.
* If the **ERP is down**, the queue grows. When the ERP is back, all pending
  records flow through.
* If the **network is down**, same as above.
* If the **device was unreachable for hours**, the device's own memory holds
  the punches; the End-of-Day catch-up at 23:55 (and the boot-sync after
  reboots) re-pulls them from the device into the logbook, idempotently.

---

## 5. The Three Safety Nets

There are three independent paths that get punches into the logbook. They
overlap deliberately. Even if one path is down, another covers it.

### Net A — Real-Time Capture (the main road)

While both the device and the daemon are online and connected, every punch
flows in within milliseconds. This is what you want 99% of the time.

### Net B — End-of-Day Catch-Up (the nightly broom)

Every night between **23:55 and 23:59**, the daemon asks each device "send me
all attendance you have stored". It filters down to today (and a configurable
small lookback window), and feeds them all into the logbook. Because enqueue
is idempotent, anything already in the logbook is skipped; anything missed
during the day is added. The catch-up window has 5 retry minutes, so even if
one minute is busy or fails, the next minute will succeed.

### Net C — Boot Sync (the recovery truck)

Every time the school server boots, `boot_sync_30d.py` runs once. It asks
each device for the last 60 days of attendance and feeds them into the
logbook. This protects you from:

* The machine being off for a few days (power outage, weekend shutdown).
* A long ERP outage that requires a manual re-push.
* A fresh reinstall of the project on a new machine.

The daemon also performs a smaller 3-day "boot recovery" the moment it
starts, in case the dedicated boot-sync didn't run for some reason.

> **Why three nets?** Because in nine schools, weird things happen. Power
> goes out. Devices freeze. Networks die at exactly 23:59. The combination
> of three independent, idempotent paths means **the system catches its own
> mistakes** without anyone needing to log in.

---

## 6. The Local Logbook

The logbook is a single file: `data/attendance_queue.db`. It is a normal
**SQLite** database. You can open it in any SQLite browser if you like.

It stores two things:

### Table `attendance_queue`

| Column | What it means |
|---|---|
| `id` | Auto-incrementing row number. |
| `device_id` | Which device the punch came from. |
| `user_id` | The biometric user. |
| `timestamp` | When the punch happened (`YYYY-MM-DD HH:MM:SS`). |
| `status` | ZKTeco verification mode (fingerprint, card, password, …). |
| `punch` | ZKTeco punch state (check-in, check-out, …). |
| `synced` | `0` = still to send. `1` = delivered to ERP. |
| `sync_attempts` | How many times the pusher tried for this record. |
| `last_attempt_at` | When was the most recent attempt. |
| `last_error` | If the last attempt failed, what was the error message. |
| `created_at` | When the row first landed in the logbook. |

A `UNIQUE(device_id, user_id, timestamp, status, punch)` constraint makes the
table **idempotent**: the same punch can be offered to it 100 times and only
the first insertion creates a row.

### Table `sync_state`

A tiny key/value table for run-to-run state:

| Key | Meaning |
|---|---|
| `last_eod_date` | The most recent date the End-of-Day catch-up succeeded. |
| `last_boot_recovery_at` | Last time the daemon performed a boot-time recovery. |

---

## 7. The Watchdog

The daemon runs one capture worker per device. Devices and networks
sometimes silently drop connections. To stop a dropped connection from
turning into "we missed half a day of punches", two healing loops run:

* **Watchdog (every 30 seconds).** Checks every capture worker. If one has
  exited (crash, kill, segfault, timeout), it is immediately respawned, and a
  Telegram alert is sent (rate-limited).
* **Scheduled reconnect (every 15 minutes).** Even if everything *looks*
  alive, the daemon kills and respawns all capture workers from scratch.
  This proactively heals the rare case where a worker is technically
  running but the underlying TCP connection has gone stale.

Both are paranoid on purpose. They are cheap to run and they cover real-world
weirdness with ZKTeco firmware.

---

## 8. The `config.json` File

This is the **only file most schools ever touch**. Edit it once when setting
up; rarely touch it again. After any edit, restart the service.

```json
{
  "endpoint": "https://pmbs.paceeducation.com/erp-api/sync/empAttSync.php",
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
    "bot_token": "…",
    "chat_id": "…",
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

### Required fields

| Field | What it is | Example |
|---|---|---|
| `endpoint` | Full URL of the ERP API the daemon POSTs to. | `"https://…/empAttSync.php"` |
| `name` | A short label for this school. Used in Telegram and logs to identify which server is talking. | `"PMBS-attendance"` |
| `devices` | List of biometric devices on this school's network. One entry per device. | see below |
| `devices[].device_id` | A unique number you assign to that device. Goes into every record. | `1` |
| `devices[].ip_address` | The local IP of the device. | `"10.50.141.32"` |
| `devices[].port` | The port the device listens on. Almost always `4370`. | `4370` |
| `devices[].password` | The device's communication password (set on the device itself). Use `"0"` if no password. | `"4546"` |
| `buffer_limit` | If this many records pile up in the logbook, push immediately instead of waiting for the next interval. Older systems used this directly; new systems treat it as a "push sooner" hint. | `3` |
| `log_level` | `DEBUG` / `INFO` / `WARNING` / `ERROR`. Use `INFO` normally; `DEBUG` while troubleshooting. | `"INFO"` |
| `telegram.bot_token` | From `@BotFather` on Telegram. | `"123:ABC…"` |
| `telegram.chat_id` | The chat / group ID to send to. | `"-1003063465563"` |
| `telegram.enabled` | `true` / `false`. | `true` |
| `telegram.notifications.*` | Fine-grained on/off per category. | all `true` |

### Optional fields (for advanced tuning)

All optional. Defaults are good for typical schools. They let you tune the
durable queue without touching code:

```json
{
  "sync": {
    "db_path": "data/attendance_queue.db",
    "batch_size": 200,
    "push_interval_s": 15,
    "push_timeout_s": 60,
    "push_retries": 5,
    "purge_synced_after_days": 14,
    "watchdog_interval_s": 30,
    "reconnect_interval_min": 15,
    "eod_lookback_days": 1,
    "boot_recovery_days": 3,
    "boot_sync_days": 60
  }
}
```

| Key | Meaning |
|---|---|
| `db_path` | Where the durable logbook lives. |
| `batch_size` | How many records to push per HTTP request. |
| `push_interval_s` | How often the pusher polls the logbook. |
| `push_timeout_s` | HTTP timeout per push. |
| `push_retries` | How many times each push is retried before backing off. |
| `purge_synced_after_days` | Synced rows are deleted after this many days. |
| `watchdog_interval_s` | How often the watchdog checks capture workers. |
| `reconnect_interval_min` | Periodic full reconnect cycle. |
| `eod_lookback_days` | Daily 23:55 catch-up scans this many recent days. |
| `boot_recovery_days` | Daemon-start recovery scans this many recent days. |
| `boot_sync_days` | The boot-sync subprocess scans this many days. |

---

## 9. Installing on a school server

You can install in **three** ways. Pick the one that matches your school's
setup. (Linux/systemd is the recommended option for the 9 production
servers.)

### Option A — Linux server with systemd (recommended)

```bash
# 1. Get the code
git clone <your-repo-url> /opt/attendance-ztech
cd /opt/attendance-ztech

# 2. Create a Python virtual environment and install requirements
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Edit the school-specific settings
nano config.json

# 4. Test once in the foreground
python main.py
# (Ctrl+C to stop)

# 5. Install as an always-on service
sudo nano /etc/systemd/system/attendance-ztech.service
```

Paste:

```ini
[Unit]
Description=Attendance ZTech daemon
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

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now attendance-ztech
```

To also enable the boot-time historical sync, add the second unit described
in [`DOCUMENTATION.md` §5](DOCUMENTATION.md).

### Option B — Docker

```bash
docker build -t attendance-ztech .
docker run -d --name attendance-ztech --restart=always --network=host \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  attendance-ztech
```

> `--network=host` is required so the container can reach the biometric
> devices on the local network.
>
> The two volume mounts make the durable logbook (`data/`) and the rotating
> logs (`logs/`) survive container rebuilds.

### Option C — Windows service

Follow the steps in `DOCUMENTATION.md` if you must run on Windows. Linux is
strongly preferred for production.

---

## 10. A Typical Day, hour by hour

```
00:00  Daemon is running. Pusher idle (queue empty).
       Watchdog ticks every 30s.

07:30  First staff arrive. Punches start.
       Real-time capture hands them to the logbook in milliseconds.
       Pusher sees pending rows, sends them to the ERP within ~15 s.

07:30 – 17:00  All day, the same loop:
       capture → enqueue → push → mark synced.
       At most one Telegram "Sync OK" message per 10 minutes.

09:15  Brief school-wifi blip for ~30 s.
       Pusher's HTTP call fails. Records stay in the logbook.
       Pusher retries with exponential backoff.
       One Telegram "Sync Failed" alert.
       09:15:42  Network is back. Pusher succeeds.
       One Telegram "Sync Recovered" alert.

15:00  Periodic 15-min full reconnect cycle. All capture workers are
       restarted from scratch. Devices reconnect cleanly. No data lost.

23:55  End-of-Day catch-up starts.
       Daemon pulls each device's stored attendance and re-enqueues it.
       Anything already in the logbook is silently ignored.
       Anything missed during the day is now in the logbook.
       Pusher delivers the new rows.
       One Telegram "End-of-Day Complete" message.

00:00  New day starts. last_eod_date is updated.
       Cycle repeats.
```

---

## 11. Telegram alerts

The school's chat / group sees a **small number of high-signal messages**:

| Event | When |
|---|---|
| 🚀 **Started** | Daemon booted. |
| ✅ **Sync OK** | A batch was pushed (rate-limited to once per 10 minutes). |
| ❌ **Sync Failed** | A push failed (rate-limited to once per 5 minutes). |
| ✅ **Sync Recovered** | The first success after a string of failures. |
| ⚠️ **Worker Restarted** | The watchdog respawned a dead device worker (rate-limited per device). |
| 🧹 **End-of-Day Started / Complete** | Nightly catch-up cycle. |
| 🚀 **Boot Sync Started / Complete / Failed** | Power-on backfill. |
| 👋 **System Stopped** | Daemon was shut down. |
| 💥 **Fatal Error** | Truly unexpected — should be very rare. |

Because each kind of message has its own rate limit, the chat **never floods**
even on bad network days. Failures still show up; chatter does not.

To verify your Telegram setup once after install:

```bash
python test_telegram.py
```

---

## 12. Logs

Every run writes to several rotating files (10 MB each, 5 backups), so disks
never fill up on their own:

| Path | What's in it |
|---|---|
| `logs/attendance.log` | Full structured log. |
| `log.txt` | A convenience copy in the project folder. |
| `~/Desktop/AttendanceZTech Logs/attendance.log` | A copy on the Desktop, only if a Desktop folder exists. |
| `journalctl -u attendance-ztech` | If installed under systemd. |

Each line looks like:

```
2026-04-26 08:32:11 - INFO - 🕘 [dev 1] punch user=1042 @ 2026-04-26 08:32:11
2026-04-26 08:32:25 - INFO - ✅ Synced 14 records (pending after: 0)
```

Useful one-liners:

```bash
tail -f logs/attendance.log                           # follow in real time
grep -E "Sync (OK|Failed|Recovered)" logs/attendance.log | tail -50
grep "EoD" logs/attendance.log
```

---

## 13. Health checks

The system exposes its state in two places.

### A) The logbook itself

```bash
sqlite3 data/attendance_queue.db <<SQL
SELECT 'pending=' || COUNT(*) FROM attendance_queue WHERE synced=0;
SELECT 'synced='  || COUNT(*) FROM attendance_queue WHERE synced=1;
SELECT key, value, updated_at FROM sync_state;
SELECT id, device_id, user_id, timestamp, sync_attempts, last_error
  FROM attendance_queue
 WHERE synced = 0
 ORDER BY id DESC
 LIMIT 20;
SQL
```

### B) The Telegram chat

If `pending` keeps growing, you'll see periodic `❌ Sync Failed` messages
that include the current pending count.

If `pending` is 0 and the latest log line is recent, the system is healthy.

---

## 14. Failure scenarios

| What happens | What the system does, automatically |
|---|---|
| ERP server is down for 2 hours | Records pile up in the logbook. Pusher retries with backoff. As soon as the ERP is back, all records flow through. **No human action needed.** |
| Internet is down for half a day | Same as above. |
| Biometric device is unplugged briefly | The capture worker errors out, the watchdog respawns it, the worker retries to connect with exponential backoff. When the device returns, the worker reconnects. End-of-Day pulls anything that happened during the outage from the device's own memory. |
| The whole school server crashes / reboots | The logbook is on disk. When the server comes back, the daemon resumes from the same logbook state. `boot_sync_30d.py` also runs to backfill the last 60 days from each device. |
| Power outage right when a punch is happening | The device may or may not record the punch (that's a hardware property). If it did, the next reconnect / catch-up captures it. If it didn't, no software in the world can recover it. |
| A bug in the daemon throws an exception | The bad record's exception is logged, the surrounding loop continues, and systemd will restart the process if it ever exits. The daemon never silently dies. |
| Same punch arrives twice (e.g. real-time + end-of-day) | The logbook ignores the duplicate. The ERP sees it exactly once. |
| Pusher gets HTTP 500 from ERP | Records are NOT marked synced. They stay pending. Pusher retries with backoff. Telegram alert is sent. |
| Pusher gets HTTP 200 | Records ARE marked synced. Telegram "Sync OK" (rate-limited). |

---

## 15. Common operations

```bash
# Check service status and live logs (Linux + systemd)
sudo systemctl status attendance-ztech
sudo journalctl -u attendance-ztech -f

# Restart after editing config.json
sudo systemctl restart attendance-ztech

# Sync a specific historical range manually
cd /opt/attendance-ztech && source venv/bin/activate
python sync_all.py --from 2026-04-01 --to 2026-04-26

# Sync only one device
python sync_all.py --device-id 1 --from 2026-04-20 --to 2026-04-26

# Pull from devices but DO NOT push directly — let the daemon drain
python sync_all.py --no-push --from 2026-04-20 --to 2026-04-26

# Test that Telegram is wired up correctly
python test_telegram.py
```

---

## 16. Troubleshooting

### Nothing seems to be pushing

```bash
sqlite3 data/attendance_queue.db \
  "SELECT COUNT(*) FROM attendance_queue WHERE synced=0;"
```

* If `0` → there is nothing to send (no punches yet today).
* If a number that keeps growing → the ERP / network is failing. Check the
  log for `Sync failed`.

### "Failed to connect" to a device

```bash
ping 10.50.141.32          # is the device reachable?
nc -zv 10.50.141.32 4370   # is the port open?
```

* Verify `ip_address`, `port`, and `password` in `config.json`.
* Make sure the school server and the device are on the same network.

### Telegram messages stopped arriving

```bash
python test_telegram.py
```

If the test fails, fix `bot_token` / `chat_id` in `config.json`.

### Service won't start after reboot

```bash
sudo systemctl status attendance-ztech
sudo journalctl -u attendance-ztech --no-pager | tail -50
```

* `config.json` must be valid JSON.
* `data/` must be writable by the service user.

### The logbook is huge

The pusher purges synced records older than 14 days every hour
automatically. If you ever want to truncate by hand:

```bash
sqlite3 data/attendance_queue.db \
  "DELETE FROM attendance_queue WHERE synced = 1;"
sqlite3 data/attendance_queue.db "VACUUM;"
```

(Never delete rows where `synced = 0` — those have not been delivered yet.)

---

## 17. FAQ

**Q: How often does it push to the ERP?**
A: As soon as records appear, generally within 15 seconds (the
`push_interval_s`). If `buffer_limit` records pile up sooner, it pushes
immediately.

**Q: Are records ever lost?**
A: Only if the device itself fails to record the original punch. From the
moment a punch reaches this software, it is on disk and will be delivered.

**Q: Will the ERP get duplicates?**
A: The local logbook will not produce duplicates; each record has a unique
key. The ERP should still be safe against duplicates as defense in depth, but
the daemon does not generate them.

**Q: How many devices can one server handle?**
A: Each device is a small subprocess. A typical school server handles 5–10
devices comfortably with very low CPU and memory.

**Q: Does it need a database server like MySQL?**
A: No. The logbook is plain SQLite — a single file, no daemon, no
configuration. It survives reboots and works on any disk.

**Q: What if I want to add a new device?**
A: Edit `config.json`, add a new entry to `devices[]`, save, and restart the
service. The new device gets its own capture worker on the next start.

**Q: What if I want to disable Telegram?**
A: Set `"telegram": { "enabled": false }` in `config.json` and restart.

**Q: Where do I see "did the punch go through?"**
A: Two places:
1. The Telegram chat will say `✅ Sync OK` shortly after each batch.
2. The logbook query in [§13](#13-health-checks) shows pending count.

**Q: What is the difference between `main.py`, `sync_all.py`, and
`boot_sync_30d.py`?**

| Script | Runs when | Purpose |
|---|---|---|
| `main.py` | 24×7 | Real-time capture and ERP pushing. |
| `sync_all.py` | On demand | One-shot pull from devices into the logbook. |
| `boot_sync_30d.py` | Once at boot | Calls `sync_all.py` for the last 60 days. |

---

## 18. Architecture diagram

```
                     ┌──────────────────────────┐
                     │      ERP REST API        │
                     └─────────────▲────────────┘
                                   │ HTTPS
                                   │
                     ┌─────────────┴────────────┐
                     │   pusher thread          │
                     │   (in main.py)           │
                     └─────────────▲────────────┘
                                   │ fetch unsynced / mark synced
                                   │
                     ┌─────────────┴────────────┐
                     │    storage.py (SQLite)   │
                     │    data/attendance_queue.db
                     └─────────────▲────────────┘
                       │           │           │
                       │           │           │
              enqueue  │  enqueue  │  enqueue  │
                       │           │           │
                ┌──────┴──┐ ┌──────┴──┐ ┌──────┴──┐
                │ Capture │ │ Capture │ │ Capture │
                │  dev 1  │ │  dev 2  │ │  dev N  │
                └─────────┘ └─────────┘ └─────────┘
                     ▲           ▲           ▲
                     │           │           │
                     │  ZK proto │           │
                     │  (TCP     │           │
                     │   4370)   │           │
                ┌────┴────┐ ┌────┴────┐ ┌────┴────┐
                │ ZKTeco  │ │ ZKTeco  │ │ ZKTeco  │
                │ device  │ │ device  │ │ device  │
                └─────────┘ └─────────┘ └─────────┘

       Watchdog (in main.py)  →  every 30 s respawns dead capture workers
       End-of-Day  (in main.py) →  23:55-23:59 catch-up via get_attendance()
       Boot recovery (main.py) →  3-day catch-up at daemon start
       boot_sync_30d.py        →  60-day catch-up at machine boot
```

---

## 19. Quick reference card

| Action | Command |
|---|---|
| Start service | `sudo systemctl start attendance-ztech` |
| Stop service | `sudo systemctl stop attendance-ztech` |
| Restart service | `sudo systemctl restart attendance-ztech` |
| View live logs | `sudo journalctl -u attendance-ztech -f` |
| View file logs | `tail -f logs/attendance.log` |
| Pending records | `sqlite3 data/attendance_queue.db "SELECT COUNT(*) FROM attendance_queue WHERE synced=0;"` |
| Manual range sync | `python sync_all.py --from 2026-04-01 --to 2026-04-26` |
| Manual full backfill | `python boot_sync_30d.py` |
| Test Telegram | `python test_telegram.py` |
| Add a device | Edit `config.json` → restart service |
| Disable Telegram | `"telegram": { "enabled": false }` → restart service |
| Debug-level logs | `"log_level": "DEBUG"` in `config.json` → restart service |

---

*Built for Pace Education · 9 schools, 9 servers, one philosophy: never lose
a punch, never bother a human.*
