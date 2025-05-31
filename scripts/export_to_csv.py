#!/usr/bin/env python3

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import os
import sqlite3
import pandas as pd
from common.env_utils import preprocess_env

# Load and process .env with template substitution
preprocess_env()

# Get database path
DB_FILE = os.getenv("DISK_STATS_DB", str(Path.home() / "hpc-disk-monitor/data/disk_stats.db"))

# Output directory
OUTPUT_DIR = Path("exports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EXPORTS = {
    "disk_stats": OUTPUT_DIR / "disk_stats.csv",
    "disk_stats_summary": OUTPUT_DIR / "disk_stats_summary.csv"
}

def export_table_to_csv(db_path, table_name, csv_path):
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        df.to_csv(csv_path, index=False)
        print(f"✅ Exported {table_name} → {csv_path} ({len(df)} rows)")

def main():
    for table, csv_path in EXPORTS.items():
        export_table_to_csv(DB_FILE, table, csv_path)

if __name__ == "__main__":
    main()

