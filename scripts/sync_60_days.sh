#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# Manual "Sync last 60 days" tool for school IT admins.
#
# What it does (in plain language):
#   1. Asks the admin "are you sure?"
#   2. Connects to every biometric device listed in config.json
#   3. Pulls the last 60 days of attendance from each device
#   4. Stores them in the local logbook (data/attendance_queue.db)
#   5. The PM2 daemon then pushes them to the ERP, idempotently
#   6. Pauses at the end so the admin can read the result.
#
# Safe to run at any time, including while the PM2 service is running.
# Safe to run multiple times — duplicates are silently ignored.
# ----------------------------------------------------------------------------

set -u

# --- Auto-detection ---------------------------------------------------------
# The script lives inside the project folder under scripts/, so by default we
# resolve the project as the script's parent directory. This works no matter
# whether the project is at /opt/attendance-ztech, ~/Projects/attendance-ztech,
# or anywhere else. Override with ATTENDANCE_PROJECT_DIR if you ever need to.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_DIR="${ATTENDANCE_PROJECT_DIR:-$(dirname "$SCRIPT_DIR")}"
DAYS="${ATTENDANCE_BACKFILL_DAYS:-60}"
# ----------------------------------------------------------------------------

RED=$'\e[31m'
GREEN=$'\e[32m'
YELLOW=$'\e[33m'
BOLD=$'\e[1m'
RESET=$'\e[0m'
PM2_TAIL_PID=""

cleanup_background_jobs() {
    if [[ -n "${PM2_TAIL_PID:-}" ]]; then
        kill "$PM2_TAIL_PID" >/dev/null 2>&1 || true
        wait "$PM2_TAIL_PID" >/dev/null 2>&1 || true
        PM2_TAIL_PID=""
    fi
}

trap cleanup_background_jobs EXIT INT TERM

pause_and_exit() {
    local code="${1:-0}"
    echo
    echo "----------------------------------------------------------------"
    read -rp "Press ENTER to close this window..." _
    exit "$code"
}

banner() {
    clear
    echo "${BOLD}=============================================================${RESET}"
    echo "${BOLD}        Attendance — Sync last ${DAYS} days from devices${RESET}"
    echo "${BOLD}=============================================================${RESET}"
    echo
}

banner

if [[ ! -d "$PROJECT_DIR" ]]; then
    echo "${RED}ERROR:${RESET} project folder not found at: $PROJECT_DIR"
    echo "If the project is installed elsewhere, set ATTENDANCE_PROJECT_DIR."
    pause_and_exit 1
fi

cd "$PROJECT_DIR" || { echo "${RED}Cannot cd into $PROJECT_DIR${RESET}"; pause_and_exit 1; }

# Find a usable Python interpreter (prefer the project's venv)
PYTHON=""
for candidate in "$PROJECT_DIR/venv/bin/python" "$PROJECT_DIR/.venv/bin/python" "$(command -v python3)" "$(command -v python)"; do
    if [[ -x "$candidate" ]]; then PYTHON="$candidate"; break; fi
done

if [[ -z "$PYTHON" ]]; then
    echo "${RED}ERROR:${RESET} no Python interpreter found."
    pause_and_exit 1
fi

if [[ ! -f "$PROJECT_DIR/sync_all.py" ]]; then
    echo "${RED}ERROR:${RESET} sync_all.py not found in $PROJECT_DIR"
    pause_and_exit 1
fi

if [[ ! -f "$PROJECT_DIR/config.json" ]]; then
    echo "${RED}ERROR:${RESET} config.json not found in $PROJECT_DIR"
    pause_and_exit 1
fi

# Compute date range (portable across GNU and BSD date)
TODAY="$(date +%F)"
if date -v-1d +%F >/dev/null 2>&1; then
    FROM_DATE="$(date -v-${DAYS}d +%F)"   # macOS / BSD
else
    FROM_DATE="$(date -d "${DAYS} days ago" +%F)"  # GNU / Linux
fi

echo "Project folder : $PROJECT_DIR"
echo "Python         : $PYTHON"
echo "Date range     : ${BOLD}$FROM_DATE${RESET} → ${BOLD}$TODAY${RESET}"
echo "Mode           : enqueue only (PM2 daemon will push to ERP)"
echo

read -rp "Proceed with the ${DAYS}-day sync? [y/N] " ANSWER
case "$ANSWER" in
    y|Y|yes|YES) ;;
    *) echo "${YELLOW}Cancelled.${RESET}"; pause_and_exit 0 ;;
esac

echo
echo "${BOLD}Starting sync...${RESET}"
echo "(This can take a few minutes depending on how many devices you have.)"
echo "Progress will be shown below (device-by-device + heartbeat every 10s)."

if command -v pm2 >/dev/null 2>&1 && pm2 describe attendance-ztech >/dev/null 2>&1; then
    echo "Attaching live daemon push logs (attendance-ztech) ..."
    pm2 logs attendance-ztech --lines 0 2>/dev/null &
    PM2_TAIL_PID=$!
    sleep 1
    if ! kill -0 "$PM2_TAIL_PID" >/dev/null 2>&1; then
        PM2_TAIL_PID=""
        echo "(Could not attach PM2 logs; continuing with sync logs only.)"
    fi
else
    echo "(PM2 not found or attendance-ztech not running; showing sync logs only.)"
fi
echo

START_TS=$(date +%s)

# --no-push  → only write into the local logbook; let the running PM2 daemon
#              drain it to the ERP. This avoids racing with the daemon and
#              guarantees no duplicate POSTs.
PYTHONUNBUFFERED=1 "$PYTHON" "$PROJECT_DIR/sync_all.py" --no-push --from "$FROM_DATE" --to "$TODAY" &
SYNC_PID=$!

# Heartbeat so admins never stare at a blank terminal during long device pulls.
while kill -0 "$SYNC_PID" >/dev/null 2>&1; do
    NOW_TS=$(date +%s)
    RUN_FOR=$(( NOW_TS - START_TS ))
    printf '[%s] Still syncing... elapsed=%ss\n' "$(date '+%H:%M:%S')" "$RUN_FOR"
    sleep 10
done

wait "$SYNC_PID"
RC=$?

END_TS=$(date +%s)
ELAPSED=$(( END_TS - START_TS ))

echo
echo "${BOLD}-----------------------------------------------------------${RESET}"

# Show the durable-queue stats (works even without sqlite3 binary because it's
# just a plain query through Python).
"$PYTHON" - <<'PY' 2>/dev/null || true
import json, os, sqlite3, sys
cfg_path = os.path.join(os.environ.get("PROJECT_DIR", "."), "config.json")
try:
    with open(cfg_path) as f:
        cfg = json.load(f)
    db_path = cfg.get("sync", {}).get("db_path", "data/attendance_queue.db")
    if not os.path.isabs(db_path):
        db_path = os.path.join(os.environ.get("PROJECT_DIR", "."), db_path)
    if not os.path.exists(db_path):
        print(f"  (logbook not found at {db_path}, nothing to report)")
        sys.exit(0)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    pending = cur.execute("SELECT COUNT(*) FROM attendance_queue WHERE synced=0").fetchone()[0]
    synced  = cur.execute("SELECT COUNT(*) FROM attendance_queue WHERE synced=1").fetchone()[0]
    print(f"  Logbook:  pending={pending}   already-synced={synced}")
    conn.close()
except Exception as e:
    print(f"  (could not read logbook: {e})")
PY

PROJECT_DIR="$PROJECT_DIR" "$PYTHON" - <<'PY' 2>/dev/null || true
PY

echo "  Elapsed:  ${ELAPSED}s"
echo "${BOLD}-----------------------------------------------------------${RESET}"

if [[ $RC -eq 0 ]]; then
    echo "${GREEN}${BOLD}DONE — sync completed successfully.${RESET}"
    echo "Records are now in the local logbook. The running daemon will push"
    echo "any pending records to the ERP within the next minute."
elif [[ $RC -eq 2 ]]; then
    echo "${YELLOW}${BOLD}PARTIAL — some devices or batches failed.${RESET}"
    echo "Records that were captured are safely in the logbook."
    echo "Check the daemon logs (pm2 logs attendance-ztech) for details,"
    echo "or simply run this tool again — it is safe to retry."
else
    echo "${RED}${BOLD}FAILED — the sync did not complete (exit code $RC).${RESET}"
    echo "Most common reasons:"
    echo "  • a biometric device is unreachable (check network / power)"
    echo "  • config.json has wrong IP / port / password"
    echo "  • the daemon is not yet running"
    echo "Check pm2 logs attendance-ztech for the full error."
fi

pause_and_exit "$RC"
