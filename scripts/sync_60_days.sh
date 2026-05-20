#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# Manual backfill tool for school IT admins (default: last 60 days).
#
# For a 7-day window, use scripts/sync_7_days.sh (or set
# ATTENDANCE_BACKFILL_DAYS=7 before running this script).
#
# What it does (in plain language):
#   1. Asks the admin "are you sure?"
#   2. Connects to every biometric device listed in config.json
#   3. Pulls the last N days of attendance from each device (N=60 by default)
#   4. Stores them in the local logbook (data/attendance_queue.db)
#   5. The PM2 daemon then pushes them to the ERP, idempotently
#   6. Pauses at the end so the admin can read the result.
#
# Safe to run multiple times — duplicates are silently ignored.
# If the SQLite queue file is corrupt, this script exits before syncing
# (fix the queue first; see README / operator runbook).
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

# sync_all.py needs third-party packages (httpx, pyzk, …). Prefer venv;
# without it, system python3 often raises ModuleNotFoundError.
if ! "$PYTHON" -c "import httpx" 2>/dev/null; then
    echo "${RED}ERROR:${RESET} Python dependencies are missing for:"
    echo "  $PYTHON"
    echo
    echo "Create the project venv and install requirements, then run again"
    echo "(this script uses venv/bin/python automatically when present):"
    echo
    echo "  cd \"$PROJECT_DIR\""
    echo "  python3 -m venv venv"
    echo "  ./venv/bin/pip install -r requirements.txt"
    echo
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

echo "Checking SQLite queue database..."
if ! PROJECT_DIR="$PROJECT_DIR" "$PYTHON" -c '
import json, os, sys
sys.path.insert(0, os.environ["PROJECT_DIR"])
os.chdir(os.environ["PROJECT_DIR"])
from storage import DEFAULT_DB_PATH, verify_sqlite_queue_db
with open("config.json") as f:
    cfg = json.load(f)
db = str((cfg.get("sync") or {}).get("db_path", DEFAULT_DB_PATH))
if not os.path.isabs(db):
    db = os.path.join(os.environ["PROJECT_DIR"], db)
ok, msg = verify_sqlite_queue_db(db)
if not ok:
    print()
    print("FATAL: local attendance queue database is damaged:")
    print(" ", msg)
    print()
    print("Fix:")
    print("  1) pm2 stop attendance-sync   (or: pm2 stop all)")
    print("  2) Backup then remove the queue files, for example:")
    print("     ", db)
    print("     ", db + "-wal", "and", db + "-shm", "(if they exist)")
    print("  3) pm2 start …  then run this backfill sync again.")
    print()
    sys.exit(1)
'; then
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
echo "Live progress appears below — per device: read count, date filter,"
echo "new records, and logbook totals. After devices finish, ERP push"
echo "progress is shown if records are still pending."
echo

PM2_APP="${ATTENDANCE_PM2_APP:-}"
if [[ -z "$PM2_APP" ]] && command -v pm2 >/dev/null 2>&1; then
    for name in attendance-sync attendance-ztech; do
        if pm2 describe "$name" >/dev/null 2>&1; then
            PM2_APP="$name"
            break
        fi
    done
fi

queue_stats() {
    PROJECT_DIR="$PROJECT_DIR" "$PYTHON" -c '
import json, os, sys
sys.path.insert(0, os.environ["PROJECT_DIR"])
os.chdir(os.environ["PROJECT_DIR"])
from storage import DEFAULT_DB_PATH, AttendanceQueue
with open("config.json") as f:
    cfg = json.load(f)
db = str((cfg.get("sync") or {}).get("db_path", DEFAULT_DB_PATH))
if not os.path.isabs(db):
    db = os.path.join(os.environ["PROJECT_DIR"], db)
q = AttendanceQueue(db)
print(q.count_unsynced(), q.count_synced())
' 2>/dev/null
}

wait_for_erp_drain() {
    local pending_start="$1"
    local timeout_s="${ATTENDANCE_ERP_WAIT_S:-900}"
    local interval_s=5
    local waited=0
    local last_pending="$pending_start"
    local pushed_total=0

    if [[ "$pending_start" -le 0 ]]; then
        echo "No pending records — ERP is already up to date."
        return 0
    fi

    echo
    echo "${BOLD}Waiting for daemon to push records to ERP...${RESET}"
    if [[ -n "$PM2_APP" ]]; then
        echo "(PM2 app: $PM2_APP — pushes every ~15s in batches)"
    else
        echo "${YELLOW}Warning:${RESET} PM2 daemon not detected; records stay pending until it runs."
    fi
    echo "Pending at start: ${pending_start}"
    echo

    while [[ "$waited" -lt "$timeout_s" ]]; do
        read -r pending synced _ <<< "$(queue_stats || echo '-1 -1')"
        if [[ "$pending" == "-1" ]]; then
            echo "  (could not read logbook)"
            break
        fi

        if [[ "$pending" -le 0 ]]; then
            echo
            echo "${GREEN}All ${pending_start} pending record(s) pushed to ERP.${RESET}"
            return 0
        fi

        pushed_this_round=$(( last_pending - pending ))
        if [[ "$pushed_this_round" -lt 0 ]]; then
            pushed_this_round=0
        fi
        pushed_total=$(( pushed_total + pushed_this_round ))
        pct=0
        if [[ "$pending_start" -gt 0 ]]; then
            pct=$(( (pending_start - pending) * 100 / pending_start ))
        fi

        printf '[%s] ERP push: %3d%% done │ pending=%s │ pushed≈%s │ synced total=%s │ wait=%ss\n' \
            "$(date '+%H:%M:%S')" "$pct" "$pending" "$pushed_total" "$synced" "$waited"

        last_pending="$pending"
        sleep "$interval_s"
        waited=$(( waited + interval_s ))
    done

    read -r pending_final _ <<< "$(queue_stats || echo '-1 -1')"
    if [[ "$pending_final" -gt 0 ]]; then
        echo
        echo "${YELLOW}ERP push still in progress (${pending_final} pending after ${timeout_s}s).${RESET}"
        echo "The daemon will continue in the background."
        if [[ -n "$PM2_APP" ]]; then
            echo "Watch live: pm2 logs $PM2_APP"
        fi
        return 1
    fi
    return 0
}

read -r PENDING_BEFORE _ _ <<< "$(queue_stats || echo '0 0')"

START_TS=$(date +%s)

# --no-push → enqueue only; PM2 daemon drains to ERP (monitored below).
PYTHONUNBUFFERED=1 "$PYTHON" "$PROJECT_DIR/sync_all.py" --no-push --from "$FROM_DATE" --to "$TODAY"
RC=$?

END_TS=$(date +%s)
ELAPSED=$(( END_TS - START_TS ))

read -r PENDING_AFTER SYNCED_AFTER _ <<< "$(queue_stats || echo '0 0')"

ERP_DRAIN_RC=0
if [[ "$PENDING_AFTER" -gt 0 ]]; then
    wait_for_erp_drain "$PENDING_AFTER" || ERP_DRAIN_RC=$?
fi

read -r PENDING_FINAL SYNCED_FINAL _ <<< "$(queue_stats || echo '0 0')"

echo
echo "${BOLD}-----------------------------------------------------------${RESET}"
echo "  Pull phase elapsed     : ${ELAPSED}s"
echo "  Logbook pending (ERP)  : ${PENDING_FINAL}"
echo "  Logbook synced (ERP)   : ${SYNCED_FINAL}"
echo "${BOLD}-----------------------------------------------------------${RESET}"

if [[ $RC -eq 0 && "$ERP_DRAIN_RC" -eq 0 && "$PENDING_FINAL" -eq 0 ]]; then
    echo "${GREEN}${BOLD}DONE — devices synced and ERP is up to date.${RESET}"
elif [[ $RC -eq 0 ]]; then
    echo "${GREEN}${BOLD}DONE — device pull completed.${RESET}"
    if [[ "$PENDING_FINAL" -gt 0 ]]; then
        echo "${PENDING_FINAL} record(s) still pending ERP push (daemon will continue)."
    fi
elif [[ $RC -eq 2 ]]; then
    echo "${YELLOW}${BOLD}PARTIAL — some devices or batches failed.${RESET}"
    echo "Records that were captured are safely in the logbook."
    echo "Check the daemon logs (e.g. pm2 logs ${PM2_APP:-attendance-sync}) for details,"
    echo "or simply run this tool again — it is safe to retry."
else
    echo "${RED}${BOLD}FAILED — the sync did not complete (exit code $RC).${RESET}"
    echo "Most common reasons:"
    echo "  • a biometric device is unreachable (check network / power)"
    echo "  • config.json has wrong IP / port / password"
    echo "  • the daemon is not yet running"
    echo "Check pm2 logs (attendance-sync or attendance-ztech) for the full error."
fi

pause_and_exit "$RC"
