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
COLLECTOR_SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "resource_metrics_collector.py"
)
API_COLLECTOR_SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "api_status_collector.py"
)
SUMMARY_SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "db_summary.py"
)
PYTHON_EXEC = Path(sys.executable)

# Comment/Label used to identify the jobs
COLLECTOR_CRON_COMMENT = "# hpc-resource-monitor collector"
API_COLLECTOR_CRON_COMMENT = "# hpc-resource-monitor api-collector"
SUMMARY_CRON_COMMENT = "# hpc-resource-monitor summary"

# LaunchAgent identifiers
COLLECTOR_PLIST_LABEL = "com.hpc.resourcemonitor"  # must match the filename
API_COLLECTOR_PLIST_LABEL = "com.hpc.resourcemonitor.api"
SUMMARY_PLIST_LABEL = "com.hpc.resourcemonitor.summary"
LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
COLLECTOR_PLIST_PATH = LAUNCH_AGENTS_DIR / f"{COLLECTOR_PLIST_LABEL}.plist"
API_COLLECTOR_PLIST_PATH = LAUNCH_AGENTS_DIR / f"{API_COLLECTOR_PLIST_LABEL}.plist"
SUMMARY_PLIST_PATH = LAUNCH_AGENTS_DIR / f"{SUMMARY_PLIST_LABEL}.plist"

# Schedule configuration from env
DISK_SAMPLING_MINUTES = int(os.environ.get("DISK_SAMPLING_MINUTES", "5"))
API_SAMPLING_MINUTES = int(os.environ.get("API_SAMPLING_MINUTES", "5"))
SUMMARY_INTERVAL_HOURS = 24  # Run summary every 24 hours

# Cron schedules
COLLECTOR_CRON_SCHEDULE = f"*/{DISK_SAMPLING_MINUTES} * * * *"  # Every X minutes
API_COLLECTOR_CRON_SCHEDULE = f"*/{API_SAMPLING_MINUTES} * * * *"  # Every X minutes
SUMMARY_CRON_SCHEDULE = f"0 */24 * * *"  # Every 24 hours at minute 0

# Inline environment variables (flattened) for both cron and launchd
ENV_EXPORTS = {
    # Filesystem configuration
    "FILESYSTEM_PATHS": os.environ.get("FILESYSTEM_PATHS", ""),
    "FILESYSTEM_LABELS": os.environ.get("FILESYSTEM_LABELS", ""),
    
    # API configuration
    "API_ENDPOINTS": os.environ.get("API_ENDPOINTS", ""),
    "API_NAMES": os.environ.get("API_NAMES", ""),
    "API_REQUEST_TIMEOUT": os.environ.get("API_REQUEST_TIMEOUT", "30"),
    
    # Database location (backward compatibility)
    "RESOURCE_STATS_DB": os.environ.get("RESOURCE_STATS_DB", ""),
    "DISK_STATS_DB": os.environ.get("DISK_STATS_DB", ""),
    
    # Data collection and display settings
    "DISK_SAMPLING_MINUTES": os.environ.get("DISK_SAMPLING_MINUTES", "5"),
    "API_SAMPLING_MINUTES": os.environ.get("API_SAMPLING_MINUTES", "5"),
    "DASH_REFRESH_SECONDS": os.environ.get("DASH_REFRESH_SECONDS", "5"),
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
    
    # Detect which comment is used in this entry
    if SUMMARY_CRON_COMMENT in entry_line:
        job_comment = SUMMARY_CRON_COMMENT
    elif API_COLLECTOR_CRON_COMMENT in entry_line:
        job_comment = API_COLLECTOR_CRON_COMMENT
    else:
        job_comment = COLLECTOR_CRON_COMMENT
        
    # Extract command without environment variables and comment
    command = " ".join([p for p in parts[5:] 
                      if p not in env_parts 
                      and job_comment not in p])
    
    print(f"Schedule: {schedule}")
    if env_parts:
        print("Environment:")
        for p in env_parts:
            print(f"  {p}")
    print("Command:")
    print(f"  {command} {job_comment}")


def install_cron_job():
    current = get_crontab_lines()
    collector_exists = any(COLLECTOR_CRON_COMMENT in line for line in current)
    api_collector_exists = any(API_COLLECTOR_CRON_COMMENT in line for line in current)
    summary_exists = any(SUMMARY_CRON_COMMENT in line for line in current)
    
    if collector_exists and api_collector_exists and summary_exists:
        print("⚠️  All cron jobs (disk collector, API collector, and summary) already exist.")
        return

    print("📋 Current crontab:")
    print("\n".join(current) or "(empty)")
    print("\n--- Adding jobs ---\n")

    new_crontab = current[:]
    env_str = " ".join(f"{k}={v}" for k, v in ENV_EXPORTS.items())
    
    # Add collector job if needed
    if not collector_exists:
        collector_entry = f"{COLLECTOR_CRON_SCHEDULE} {env_str} {PYTHON_EXEC} {COLLECTOR_SCRIPT_PATH} {COLLECTOR_CRON_COMMENT}"
        print("Adding disk collector job:")
        pretty_print_cron_entry(collector_entry)
        new_crontab.append(collector_entry)
    
    # Add API collector job if needed
    if not api_collector_exists:
        api_collector_entry = f"{API_COLLECTOR_CRON_SCHEDULE} {env_str} {PYTHON_EXEC} {API_COLLECTOR_SCRIPT_PATH} {API_COLLECTOR_CRON_COMMENT}"
        print("\nAdding API collector job:")
        pretty_print_cron_entry(api_collector_entry)
        new_crontab.append(api_collector_entry)
    
    # Add summary job if needed
    if not summary_exists:
        # Add --time-period 1d to generate daily summary
        summary_entry = f"{SUMMARY_CRON_SCHEDULE} {env_str} {PYTHON_EXEC} {SUMMARY_SCRIPT_PATH} --time-period 1d --recompute {SUMMARY_CRON_COMMENT}"
        print("\nAdding summary job:")
        pretty_print_cron_entry(summary_entry)
        new_crontab.append(summary_entry)
    
    _confirm_and_apply_cron(new_crontab)


def update_cron_job():
    current = get_crontab_lines()
    filtered = [line for line in current 
                if COLLECTOR_CRON_COMMENT not in line 
                and API_COLLECTOR_CRON_COMMENT not in line
                and SUMMARY_CRON_COMMENT not in line]

    env_str = " ".join(f"{k}={v}" for k, v in ENV_EXPORTS.items())
    
    # Create collector job entry
    collector_entry = f"{COLLECTOR_CRON_SCHEDULE} {env_str} {PYTHON_EXEC} {COLLECTOR_SCRIPT_PATH} {COLLECTOR_CRON_COMMENT}"
    
    # Create API collector job entry
    api_collector_entry = f"{API_COLLECTOR_CRON_SCHEDULE} {env_str} {PYTHON_EXEC} {API_COLLECTOR_SCRIPT_PATH} {API_COLLECTOR_CRON_COMMENT}"
    
    # Create summary job entry
    summary_entry = f"{SUMMARY_CRON_SCHEDULE} {env_str} {PYTHON_EXEC} {SUMMARY_SCRIPT_PATH} --time-period 1d --recompute {SUMMARY_CRON_COMMENT}"

    print("📋 Current crontab:")
    print("\n".join(current) or "(empty)")
    print("\n--- Updating jobs ---\n")
    
    print("Updating disk collector job:")
    pretty_print_cron_entry(collector_entry)
    
    print("\nUpdating API collector job:")
    pretty_print_cron_entry(api_collector_entry)
    
    print("\nUpdating summary job:")
    pretty_print_cron_entry(summary_entry)

    new_crontab = filtered + [collector_entry, api_collector_entry, summary_entry]
    _confirm_and_apply_cron(new_crontab)


def remove_cron_job():
    current = get_crontab_lines()
    new_crontab = [line for line in current 
                   if COLLECTOR_CRON_COMMENT not in line 
                   and API_COLLECTOR_CRON_COMMENT not in line
                   and SUMMARY_CRON_COMMENT not in line]

    print("📋 Current crontab:")
    print("\n".join(current) or "(empty)")
    print("\n--- Removing jobs ---\n")
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


def _make_collector_plist_dict() -> dict:
    """
    Build a Python dict representing the plist content for the collector LaunchAgent.
    - Runs every X minutes via StartInterval.
    - Passes environment variables via 'EnvironmentVariables' key.
    """
    # Command plus arguments:
    program_args = [str(PYTHON_EXEC), str(COLLECTOR_SCRIPT_PATH)]

    # Only include environment variables that are non-empty:
    env_dict = {k: v for k, v in ENV_EXPORTS.items() if v}

    plist = {
        "Label": COLLECTOR_PLIST_LABEL,
        "ProgramArguments": program_args,
        # Run at specified interval (in seconds)
        "StartInterval": DISK_SAMPLING_MINUTES * 60,  # Convert minutes to seconds
        # Load at login (optional if you want it always active when user logs in)
        "RunAtLoad": True,
        "EnvironmentVariables": env_dict,
        # Redirect stdout/stderr into ~/Library/Logs/com.hpc.resourcemonitor.log
        "StandardOutPath": str(
            Path.home() / "Library" / "Logs" / f"{COLLECTOR_PLIST_LABEL}.out.log"
        ),
        "StandardErrorPath": str(
            Path.home() / "Library" / "Logs" / f"{COLLECTOR_PLIST_LABEL}.err.log"
        ),
    }
    return plist

def _make_api_collector_plist_dict() -> dict:
    """
    Build a Python dict representing the plist content for the API collector LaunchAgent.
    - Runs every X minutes via StartInterval.
    - Passes environment variables via 'EnvironmentVariables' key.
    """
    # Command plus arguments:
    program_args = [str(PYTHON_EXEC), str(API_COLLECTOR_SCRIPT_PATH)]

    # Only include environment variables that are non-empty:
    env_dict = {k: v for k, v in ENV_EXPORTS.items() if v}

    plist = {
        "Label": API_COLLECTOR_PLIST_LABEL,
        "ProgramArguments": program_args,
        # Run at specified interval (in seconds)
        "StartInterval": API_SAMPLING_MINUTES * 60,  # Convert minutes to seconds
        # Load at login (optional if you want it always active when user logs in)
        "RunAtLoad": True,
        "EnvironmentVariables": env_dict,
        # Redirect stdout/stderr into ~/Library/Logs/com.hpc.resourcemonitor.api.log
        "StandardOutPath": str(
            Path.home() / "Library" / "Logs" / f"{API_COLLECTOR_PLIST_LABEL}.out.log"
        ),
        "StandardErrorPath": str(
            Path.home() / "Library" / "Logs" / f"{API_COLLECTOR_PLIST_LABEL}.err.log"
        ),
    }
    return plist

def _make_summary_plist_dict() -> dict:
    """
    Build a Python dict representing the plist content for the summary LaunchAgent.
    - Runs every 24 hours via StartInterval.
    - Passes environment variables via 'EnvironmentVariables' key.
    """
    # Command plus arguments (including --time-period and --recompute):
    program_args = [
        str(PYTHON_EXEC), 
        str(SUMMARY_SCRIPT_PATH),
        "--time-period", 
        "1d",
        "--recompute"
    ]

    # Only include environment variables that are non-empty:
    env_dict = {k: v for k, v in ENV_EXPORTS.items() if v}

    plist = {
        "Label": SUMMARY_PLIST_LABEL,
        "ProgramArguments": program_args,
        # Run every 24 hours (in seconds)
        "StartInterval": SUMMARY_INTERVAL_HOURS * 3600,  # Convert hours to seconds
        # Load at login (optional if you want it always active when user logs in)
        "RunAtLoad": True,
        "EnvironmentVariables": env_dict,
        # Redirect stdout/stderr into ~/Library/Logs/com.hpc.resourcemonitor.summary.log
        "StandardOutPath": str(
            Path.home() / "Library" / "Logs" / f"{SUMMARY_PLIST_LABEL}.out.log"
        ),
        "StandardErrorPath": str(
            Path.home() / "Library" / "Logs" / f"{SUMMARY_PLIST_LABEL}.err.log"
        ),
    }
    return plist


def install_launchd_job():
    LAUNCH_AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Check if plists already exist
    collector_exists = COLLECTOR_PLIST_PATH.exists()
    api_collector_exists = API_COLLECTOR_PLIST_PATH.exists()
    summary_exists = SUMMARY_PLIST_PATH.exists()
    
    if collector_exists and api_collector_exists and summary_exists:
        print("⚠️  All LaunchAgent plists already exist.")
        return

    # Install collector if needed
    if not collector_exists:
        collector_plist = _make_collector_plist_dict()
        print(f"📋 Will write Disk Collector LaunchAgent plist to:\n    {COLLECTOR_PLIST_PATH}\n")
        print("Contents:")
        print(plistlib.dumps(collector_plist).decode())
    
    # Install API collector if needed
    if not api_collector_exists:
        api_collector_plist = _make_api_collector_plist_dict()
        print(f"📋 Will write API Collector LaunchAgent plist to:\n    {API_COLLECTOR_PLIST_PATH}\n")
        print("Contents:")
        print(plistlib.dumps(api_collector_plist).decode())
    
    # Install summary if needed
    if not summary_exists:
        summary_plist = _make_summary_plist_dict()
        print(f"📋 Will write Summary LaunchAgent plist to:\n    {SUMMARY_PLIST_PATH}\n")
        print("Contents:")
        print(plistlib.dumps(summary_plist).decode())

    print("\nType 'yes' to write and load these LaunchAgents:")
    if input().strip().lower() == "yes":
        if not collector_exists:
            with open(COLLECTOR_PLIST_PATH, "wb") as f:
                plistlib.dump(collector_plist, f)
            # Load it into launchd
            subprocess.run(["launchctl", "load", str(COLLECTOR_PLIST_PATH)])
            print(f"✅ Disk Collector LaunchAgent installed and loaded at {COLLECTOR_PLIST_PATH}")
            
        if not api_collector_exists:
            with open(API_COLLECTOR_PLIST_PATH, "wb") as f:
                plistlib.dump(api_collector_plist, f)
            # Load it into launchd
            subprocess.run(["launchctl", "load", str(API_COLLECTOR_PLIST_PATH)])
            print(f"✅ API Collector LaunchAgent installed and loaded at {API_COLLECTOR_PLIST_PATH}")
            
        if not summary_exists:
            with open(SUMMARY_PLIST_PATH, "wb") as f:
                plistlib.dump(summary_plist, f)
            # Load it into launchd
            subprocess.run(["launchctl", "load", str(SUMMARY_PLIST_PATH)])
            print(f"✅ Summary LaunchAgent installed and loaded at {SUMMARY_PLIST_PATH}")
    else:
        print("❌ Aborted.")


def update_launchd_job():
    collector_exists = COLLECTOR_PLIST_PATH.exists()
    api_collector_exists = API_COLLECTOR_PLIST_PATH.exists()
    summary_exists = SUMMARY_PLIST_PATH.exists()
    
    if not collector_exists and not api_collector_exists and not summary_exists:
        print("⚠️  No existing plists to update. Please run 'install' first.")
        return

    # Create plist objects
    collector_plist = _make_collector_plist_dict()
    api_collector_plist = _make_api_collector_plist_dict()
    summary_plist = _make_summary_plist_dict()
    
    if collector_exists:
        print(f"📋 Will overwrite Disk Collector LaunchAgent plist at:\n    {COLLECTOR_PLIST_PATH}\n")
        print("New Contents:")
        print(plistlib.dumps(collector_plist).decode())
        
    if api_collector_exists:
        print(f"📋 Will overwrite API Collector LaunchAgent plist at:\n    {API_COLLECTOR_PLIST_PATH}\n")
        print("New Contents:")
        print(plistlib.dumps(api_collector_plist).decode())
        
    if summary_exists:
        print(f"📋 Will overwrite Summary LaunchAgent plist at:\n    {SUMMARY_PLIST_PATH}\n")
        print("New Contents:")
        print(plistlib.dumps(summary_plist).decode())

    print("\nType 'yes' to overwrite and reload these LaunchAgents:")
    if input().strip().lower() == "yes":
        if collector_exists:
            with open(COLLECTOR_PLIST_PATH, "wb") as f:
                plistlib.dump(collector_plist, f)
            # Unload and reload so launchd picks up changes
            subprocess.run(["launchctl", "unload", str(COLLECTOR_PLIST_PATH)])
            subprocess.run(["launchctl", "load", str(COLLECTOR_PLIST_PATH)])
            print("✅ Disk Collector LaunchAgent updated and reloaded.")
            
        if api_collector_exists:
            with open(API_COLLECTOR_PLIST_PATH, "wb") as f:
                plistlib.dump(api_collector_plist, f)
            # Unload and reload so launchd picks up changes
            subprocess.run(["launchctl", "unload", str(API_COLLECTOR_PLIST_PATH)])
            subprocess.run(["launchctl", "load", str(API_COLLECTOR_PLIST_PATH)])
            print("✅ API Collector LaunchAgent updated and reloaded.")
            
        if summary_exists:
            with open(SUMMARY_PLIST_PATH, "wb") as f:
                plistlib.dump(summary_plist, f)
            # Unload and reload so launchd picks up changes
            subprocess.run(["launchctl", "unload", str(SUMMARY_PLIST_PATH)])
            subprocess.run(["launchctl", "load", str(SUMMARY_PLIST_PATH)])
            print("✅ Summary LaunchAgent updated and reloaded.")
    else:
        print("❌ Aborted.")


def remove_launchd_job():
    collector_exists = COLLECTOR_PLIST_PATH.exists()
    api_collector_exists = API_COLLECTOR_PLIST_PATH.exists()
    summary_exists = SUMMARY_PLIST_PATH.exists()
    
    if not collector_exists and not api_collector_exists and not summary_exists:
        print("⚠️  No existing plists to remove.")
        return
    
    if collector_exists:
        print(f"📋 Found disk collector plist at {COLLECTOR_PLIST_PATH}.")
    if api_collector_exists:
        print(f"📋 Found API collector plist at {API_COLLECTOR_PLIST_PATH}.")
    if summary_exists:
        print(f"📋 Found summary plist at {SUMMARY_PLIST_PATH}.")
    
    print("After unloading, the files will be deleted.\n")
    print("Type 'yes' to unload and remove these LaunchAgents:")
    if input().strip().lower() == "yes":
        if collector_exists:
            subprocess.run(["launchctl", "unload", str(COLLECTOR_PLIST_PATH)])
            COLLECTOR_PLIST_PATH.unlink()
            print("✅ Disk Collector LaunchAgent unloaded and plist removed.")
            
        if api_collector_exists:
            subprocess.run(["launchctl", "unload", str(API_COLLECTOR_PLIST_PATH)])
            API_COLLECTOR_PLIST_PATH.unlink()
            print("✅ API Collector LaunchAgent unloaded and plist removed.")
            
        if summary_exists:
            subprocess.run(["launchctl", "unload", str(SUMMARY_PLIST_PATH)])
            SUMMARY_PLIST_PATH.unlink()
            print("✅ Summary LaunchAgent unloaded and plist removed.")
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
