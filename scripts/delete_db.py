#!/usr/bin/env python3

import os
import sys
from pathlib import Path

# Enable relative import of env_utils
sys.path.append(str(Path(__file__).resolve().parent.parent))
from common.env_utils import preprocess_env

# Load and preprocess .env
preprocess_env()

DB_FILE = os.getenv("DISK_STATS_DB", str(Path.home() / "hpc-disk-monitor/data/disk_stats.db"))

def main():
    print(f"⚠️ This will permanently delete the database file at:\n  {DB_FILE}\n")
    confirm = input("Type 'yes' to confirm: ").strip().lower()
    if confirm != "yes":
        print("❌ Aborted.")
        return

    try:
        Path(DB_FILE).unlink()
        print("✅ Database deleted.")
    except FileNotFoundError:
        print("⚠️ No database file found — nothing to delete.")
    except Exception as e:
        print(f"❌ Failed to delete database: {e}")

if __name__ == "__main__":
    main()
