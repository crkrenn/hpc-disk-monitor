#!/usr/bin/env python3

import os
import sys
import subprocess
from pathlib import Path

# Add project root to import path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from common.env_utils import preprocess_env

# Load .env values, using shell env as precedence
preprocess_env(use_shell_env=True)

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "disk_metrics_collector.py"
PYTHON_EXEC = Path(sys.executable)
CRON_COMMENT = "# hpc-disk-monitor collector"

# Prepare environment variables inline for crontab (flattened)
ENV_EXPORTS = [
    f"DISK_STATS_DB={os.environ.get('DISK_STATS_DB')}",
    f"FILESYSTEM_PATHS={os.environ.get('FILESYSTEM_PATHS')}",
    f"FILESYSTEM_LABELS={os.environ.get('FILESYSTEM_LABELS')}"
]

CRON_CMD = f"{' '.join(ENV_EXPORTS)} {PYTHON_EXEC} {SCRIPT_PATH} {CRON_COMMENT}"

def get_crontab():
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        return []
    return result.stdout.strip().splitlines()

def pretty_print_cron(cmd):
    parts = cmd.split()
    env_parts = [p for p in parts if '=' in p and not p.endswith(CRON_COMMENT)]
    command = ' '.join([p for p in parts if '=' not in p or p.endswith(CRON_COMMENT)])
    print("Environment:")
    for p in env_parts:
        print(f"  {p}")
    print("Command:")
    print(f"  {command}")

def install_cron_job():
    current = get_crontab()
    if any(CRON_COMMENT in line for line in current):
        print("⚠️  Cron job already exists.")
        return

    print("📋 Current crontab:")
    print("\n".join(current) or "(empty)")
    print("\n--- Adding job ---\n")
    pretty_print_cron(CRON_CMD)
    new_crontab = current + [CRON_CMD]
    confirm_and_apply(new_crontab)

def update_cron_job():
    current = get_crontab()
    filtered = [line for line in current if CRON_COMMENT not in line]
    new_crontab = filtered + [CRON_CMD]

    print("📋 Current crontab:")
    print("\n".join(current) or "(empty)")
    print("\n--- Updating job ---\n")
    pretty_print_cron(CRON_CMD)
    confirm_and_apply(new_crontab)

def remove_cron_job():
    current = get_crontab()
    new_crontab = [line for line in current if CRON_COMMENT not in line]

    print("📋 Current crontab:")
    print("\n".join(current) or "(empty)")
    print("\n--- Removing job ---\n")
    print("\n".join(new_crontab) or "(empty)")
    confirm_and_apply(new_crontab)

def confirm_and_apply(lines):
    print("\nType 'yes' to confirm changes to crontab:")
    if input().strip().lower() == "yes":
        temp = "/tmp/tmp_crontab"
        with open(temp, "w") as f:
            f.write("\n".join(lines) + "\n")
        subprocess.run(["crontab", temp])
        os.remove(temp)
        print("✅ Crontab updated.")
    else:
        print("❌ Aborted.")

def usage():
    print("Usage: python manage_cron.py [install|update|remove]")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
    elif sys.argv[1] == "install":
        install_cron_job()
    elif sys.argv[1] == "update":
        update_cron_job()
    elif sys.argv[1] == "remove":
        remove_cron_job()
    else:
        usage()
