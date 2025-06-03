#!/usr/bin/env python3

import os
import sys
import subprocess
import platform
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import plistlib
from common.env_utils import preprocess_env

# ──────────────── Constants & Configuration ───────────────── #
preprocess_env()

# Paths
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "disk_metrics_collector.py"
)
PYTHON_EXEC = Path(sys.executable)

# Comment/Label used to identify the job
CRON_COMMENT = "# hpc-disk-monitor collector"
PLIST_LABEL = (
    "com.hpc.diskmonitor"  # must match the filename (com.hpc.diskmonitor.plist)
)
LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
PLIST_PATH = LAUNCH_AGENTS_DIR / f"{PLIST_LABEL}.plist"

# Sampling frequency from env (default 5 minutes)
DISK_SAMPLING_MINUTES = int(os.environ.get("DISK_SAMPLING_MINUTES", "5"))

# Cron schedule (every X minutes)
CRON_SCHEDULE = f"*/{DISK_SAMPLING_MINUTES} * * * *"

# Inline environment variables (flattened) for both cron and launchd
ENV_EXPORTS = {
    "DISK_STATS_DB": os.environ.get("DISK_STATS_DB", ""),
    "FILESYSTEM_PATHS": os.environ.get("FILESYSTEM_PATHS", ""),
    "FILESYSTEM_LABELS": os.environ.get("FILESYSTEM_LABELS", ""),
}


# ──────────────── Helpers: Linux (crontab) ───────────────── #


def get_crontab_lines():
    """Return existing crontab lines (as a list). If none, return an empty list."""
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        return []
    return result.stdout.strip().splitlines()


def pretty_print_cron_entry(entry_line: str):
    parts = entry_line.split()
    schedule = " ".join(parts[:5])
    env_parts = [p for p in parts[5:] if "=" in p and not p.startswith("#")]
    command = " ".join([p for p in parts[5:] if p not in env_parts])
    print(f"Schedule: {schedule}")
    if env_parts:
        print("Environment:")
        for p in env_parts:
            print(f"  {p}")
    print("Command:")
    print(f"  {command} {CRON_COMMENT}")


def install_cron_job():
    current = get_crontab_lines()
    if any(CRON_COMMENT in line for line in current):
        print("⚠️  Cron job already exists.")
        return

    print("📋 Current crontab:")
    print("\n".join(current) or "(empty)")
    print("\n--- Adding job ---\n")

    # Build the single-line cron entry
    env_str = " ".join(f"{k}={v}" for k, v in ENV_EXPORTS.items())
    cron_entry = f"{CRON_SCHEDULE} {env_str} {PYTHON_EXEC} {SCRIPT_PATH} {CRON_COMMENT}"
    pretty_print_cron_entry(cron_entry)

    new_crontab = current + [cron_entry]
    _confirm_and_apply_cron(new_crontab)


def update_cron_job():
    current = get_crontab_lines()
    filtered = [line for line in current if CRON_COMMENT not in line]

    env_str = " ".join(f"{k}={v}" for k, v in ENV_EXPORTS.items())
    cron_entry = f"{CRON_SCHEDULE} {env_str} {PYTHON_EXEC} {SCRIPT_PATH} {CRON_COMMENT}"

    print("📋 Current crontab:")
    print("\n".join(current) or "(empty)")
    print("\n--- Updating job ---\n")
    pretty_print_cron_entry(cron_entry)

    new_crontab = filtered + [cron_entry]
    _confirm_and_apply_cron(new_crontab)


def remove_cron_job():
    current = get_crontab_lines()
    new_crontab = [line for line in current if CRON_COMMENT not in line]

    print("📋 Current crontab:")
    print("\n".join(current) or "(empty)")
    print("\n--- Removing job ---\n")
    print("\n".join(new_crontab) or "(empty)")
    _confirm_and_apply_cron(new_crontab)


def _confirm_and_apply_cron(lines):
    print("\nType 'yes' to confirm changes to crontab:")
    if input().strip().lower() == "yes":
        tmp = "/tmp/tmp_crontab.txt"
        with open(tmp, "w") as f:
            f.write("\n".join(lines) + "\n")
        subprocess.run(["crontab", tmp])
        os.remove(tmp)
        print("✅ Crontab updated.")
    else:
        print("❌ Aborted.")


# ──────────────── Helpers: macOS (launchd) ───────────────── #


def _make_launchd_plist_dict() -> dict:
    """
    Build a Python dict representing the plist content for launchd.
    - Runs every 300 seconds (5 minutes) via StartInterval.
    - Passes environment variables via 'EnvironmentVariables' key.
    """
    # Command plus arguments:
    program_args = [str(PYTHON_EXEC), str(SCRIPT_PATH)]

    # Only include environment variables that are non-empty:
    env_dict = {k: v for k, v in ENV_EXPORTS.items() if v}

    plist = {
        "Label": PLIST_LABEL,
        "ProgramArguments": program_args,
        # Run at specified interval (in seconds)
        "StartInterval": DISK_SAMPLING_MINUTES * 60,  # Convert minutes to seconds
        # Load at login (optional if you want it always active when user logs in)
        "RunAtLoad": True,
        "EnvironmentVariables": env_dict,
        # Redirect stdout/stderr into ~/Library/Logs/com.hpc.diskmonitor.log
        "StandardOutPath": str(
            Path.home() / "Library" / "Logs" / f"{PLIST_LABEL}.out.log"
        ),
        "StandardErrorPath": str(
            Path.home() / "Library" / "Logs" / f"{PLIST_LABEL}.err.log"
        ),
    }
    return plist


def install_launchd_job():
    LAUNCH_AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    if PLIST_PATH.exists():
        print("⚠️  LaunchAgent plist already exists.")
        return

    plist_dict = _make_launchd_plist_dict()
    print(f"📋 Will write LaunchAgent plist to:\n    {PLIST_PATH}\n")
    print("Contents:")
    print(plistlib.dumps(plist_dict).decode())

    print("\nType 'yes' to write and load this LaunchAgent:")
    if input().strip().lower() == "yes":
        with open(PLIST_PATH, "wb") as f:
            plistlib.dump(plist_dict, f)

        # Load it into launchd
        subprocess.run(["launchctl", "load", str(PLIST_PATH)])
        print("✅ LaunchAgent installed and loaded.")
    else:
        print("❌ Aborted.")


def update_launchd_job():
    if not PLIST_PATH.exists():
        print("⚠️  No existing plist to update. Please run 'install' first.")
        return

    plist_dict = _make_launchd_plist_dict()
    print(f"📋 Will overwrite LaunchAgent plist at:\n    {PLIST_PATH}\n")
    print("New Contents:")
    print(plistlib.dumps(plist_dict).decode())

    print("\nType 'yes' to overwrite and reload this LaunchAgent:")
    if input().strip().lower() == "yes":
        with open(PLIST_PATH, "wb") as f:
            plistlib.dump(plist_dict, f)

        # Unload and reload so launchd picks up changes
        subprocess.run(["launchctl", "unload", str(PLIST_PATH)])
        subprocess.run(["launchctl", "load", str(PLIST_PATH)])
        print("✅ LaunchAgent updated and reloaded.")
    else:
        print("❌ Aborted.")


def remove_launchd_job():
    if not PLIST_PATH.exists():
        print("⚠️  No existing plist to remove.")
        return

    print(f"📋 Found plist at {PLIST_PATH}.")
    print("After unloading, the file itself will be deleted.\n")
    print("Type 'yes' to unload and remove this LaunchAgent:")
    if input().strip().lower() == "yes":
        subprocess.run(["launchctl", "unload", str(PLIST_PATH)])
        PLIST_PATH.unlink()
        print("✅ LaunchAgent unloaded and plist removed.")
    else:
        print("❌ Aborted.")


# ──────────────── Platform Dispatcher & Usage ───────────────── #


def usage():
    print("Usage: python manage_cron.py [install|update|remove]")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)

    action = sys.argv[1].lower()
    is_darwin = platform.system() == "Darwin"

    if action == "install":
        if is_darwin:
            install_launchd_job()
        else:
            install_cron_job()

    elif action == "update":
        if is_darwin:
            update_launchd_job()
        else:
            update_cron_job()

    elif action == "remove":
        if is_darwin:
            remove_launchd_job()
        else:
            remove_cron_job()

    else:
        usage()
        sys.exit(1)
