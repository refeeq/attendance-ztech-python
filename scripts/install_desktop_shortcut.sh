#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# One-time installer: puts attendance backfill shortcuts on the user's
# Desktop and in the system applications menu.
#
#   • Sync Last 7 Days  — past week (quick catch-up)
#   • Sync Last 60 Days — two-month backfill
#
# Run this ONCE on each school server, as the user who logs into the GUI
# (NOT as root, unless that user is also the one who logs in).
#
#   bash scripts/install_desktop_shortcut.sh
# ----------------------------------------------------------------------------

set -euo pipefail

install_launcher() {
    local src_desktop="$1"
    local src_script="$2"
    local desktop_filename="$3"
    local apps_filename="$4"

    if [[ ! -f "$src_desktop" ]]; then
        echo "ERROR: $src_desktop not found." >&2
        exit 1
    fi
    if [[ ! -f "$src_script" ]]; then
        echo "ERROR: $src_script not found." >&2
        exit 1
    fi

    chmod +x "$src_script"

    local tmp_desktop
    tmp_desktop="$(mktemp)"
    sed "s|__PROJECT_DIR__|$PROJECT_DIR|g" "$src_desktop" > "$tmp_desktop"

    local desktop_target="$DESKTOP_DIR/$desktop_filename"
    local apps_target="$APPS_DIR/$apps_filename"

    install -m 0755 "$tmp_desktop" "$desktop_target"
    install -m 0644 "$tmp_desktop" "$apps_target"
    rm -f "$tmp_desktop"

    if command -v gio >/dev/null 2>&1; then
        gio set "$desktop_target" metadata::trusted true 2>/dev/null || true
    fi

    echo "  Desktop icon : $desktop_target"
    echo "  Apps menu    : $apps_target"
}

# Resolve the project folder from the location of this installer script.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_DIR="${ATTENDANCE_PROJECT_DIR:-$(dirname "$SCRIPT_DIR")}"

DESKTOP_DIR="$(xdg-user-dir DESKTOP 2>/dev/null || echo "$HOME/Desktop")"
mkdir -p "$DESKTOP_DIR"

APPS_DIR="$HOME/.local/share/applications"
mkdir -p "$APPS_DIR"

echo "Installing attendance backfill shortcuts..."
echo

install_launcher \
    "$PROJECT_DIR/scripts/Sync7Days.desktop" \
    "$PROJECT_DIR/scripts/sync_7_days.sh" \
    "Sync7Days.desktop" \
    "attendance-sync-7days.desktop"

install_launcher \
    "$PROJECT_DIR/scripts/Sync60Days.desktop" \
    "$PROJECT_DIR/scripts/sync_60_days.sh" \
    "Sync60Days.desktop" \
    "attendance-sync-60days.desktop"

if command -v update-desktop-database >/dev/null 2>&1; then
    update-desktop-database "$APPS_DIR" >/dev/null 2>&1 || true
fi

echo
echo "Project dir: $PROJECT_DIR"
echo
echo "On GNOME, you may need to right-click each icon → 'Allow Launching'"
echo "the very first time."
echo
echo "Done."
