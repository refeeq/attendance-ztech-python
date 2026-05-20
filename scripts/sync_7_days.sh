#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# Manual "Sync last 7 days" tool for school IT admins.
#
# Same workflow as sync_60_days.sh, but pulls only the past week from devices.
# Records are enqueued locally; the PM2 daemon pushes them to the ERP.
# ----------------------------------------------------------------------------

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export ATTENDANCE_BACKFILL_DAYS=7
exec bash "$SCRIPT_DIR/sync_60_days.sh"
