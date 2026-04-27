#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# One-time installer: puts the "Sync Last 60 Days" shortcut on the user's
# Desktop and in the system applications menu.
#
# Run this ONCE on each school server, as the user who logs into the GUI
# (NOT as root, unless that user is also the one who logs in).
#
#   bash scripts/install_desktop_shortcut.sh
#
# After this runs:
#   • A double-clickable icon appears on the Desktop.
#   • The launcher also shows up in the apps menu under "Attendance".
# ----------------------------------------------------------------------------

set -euo pipefail

# Resolve the project folder from the location of this installer script.
# (scripts/install_desktop_shortcut.sh → project dir is the parent.)
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_DIR="${ATTENDANCE_PROJECT_DIR:-$(dirname "$SCRIPT_DIR")}"
SRC_DESKTOP="$PROJECT_DIR/scripts/Sync60Days.desktop"
SRC_SCRIPT="$PROJECT_DIR/scripts/sync_60_days.sh"

if [[ ! -f "$SRC_DESKTOP" ]]; then
    echo "ERROR: $SRC_DESKTOP not found." >&2
    exit 1
fi
if [[ ! -f "$SRC_SCRIPT" ]]; then
    echo "ERROR: $SRC_SCRIPT not found." >&2
    exit 1
fi

# Make the wrapper executable
chmod +x "$SRC_SCRIPT"

# Patch the __PROJECT_DIR__ placeholder in the .desktop file with the
# actual install location (could be /opt/..., ~/Projects/..., anywhere).
TMP_DESKTOP="$(mktemp)"
sed "s|__PROJECT_DIR__|$PROJECT_DIR|g" "$SRC_DESKTOP" > "$TMP_DESKTOP"

# Resolve the user's Desktop folder using XDG (falls back to ~/Desktop)
DESKTOP_DIR="$(xdg-user-dir DESKTOP 2>/dev/null || echo "$HOME/Desktop")"
mkdir -p "$DESKTOP_DIR"

# Apps menu location for the current user
APPS_DIR="$HOME/.local/share/applications"
mkdir -p "$APPS_DIR"

DESKTOP_TARGET="$DESKTOP_DIR/Sync60Days.desktop"
APPS_TARGET="$APPS_DIR/attendance-sync-60days.desktop"

install -m 0755 "$TMP_DESKTOP" "$DESKTOP_TARGET"
install -m 0644 "$TMP_DESKTOP" "$APPS_TARGET"
rm -f "$TMP_DESKTOP"

# On GNOME, files copied to the Desktop need to be marked "trusted" before
# they show their proper icon and run on double-click.
if command -v gio >/dev/null 2>&1; then
    gio set "$DESKTOP_TARGET" metadata::trusted true 2>/dev/null || true
fi

# Refresh the apps menu cache (best-effort)
if command -v update-desktop-database >/dev/null 2>&1; then
    update-desktop-database "$APPS_DIR" >/dev/null 2>&1 || true
fi

echo
echo "Installed:"
echo "  Desktop icon : $DESKTOP_TARGET"
echo "  Apps menu    : $APPS_TARGET"
echo "  Project dir  : $PROJECT_DIR"
echo
echo "On GNOME, you may need to right-click the icon → 'Allow Launching'"
echo "the very first time."
echo
echo "Done."
