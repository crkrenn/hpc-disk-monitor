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

# Get database path (support both new and legacy variable names)
DB_FILE = os.getenv("RESOURCE_STATS_DB", os.getenv("DISK_STATS_DB", str(Path.home() / "hpc-resource-monitor/data/resource_stats.db")))

# Output directory
OUTPUT_DIR = Path("exports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EXPORTS = {
    "disk_stats": OUTPUT_DIR / "disk_stats.csv",
    "disk_stats_summary": OUTPUT_DIR / "disk_stats_summary.csv",
    "api_stats": OUTPUT_DIR / "api_stats.csv", 
    "api_stats_summary": OUTPUT_DIR / "api_stats_summary.csv"
}

def export_table_to_csv(db_path, table_name, csv_path):
    """Export a table to CSV, handling missing tables gracefully."""
    try:
        with sqlite3.connect(db_path) as conn:
            # Check if table exists
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                print(f"⚠️  Table {table_name} not found, skipping export")
                return
                
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            df.to_csv(csv_path, index=False)
            print(f"✅ Exported {table_name} → {csv_path} ({len(df)} rows)")
    except sqlite3.Error as e:
        print(f"⚠️  Error exporting {table_name}: {e}")
    except Exception as e:
        print(f"⚠️  Unexpected error exporting {table_name}: {e}")

def main():
    """Export all available tables to CSV files."""
    print(f"📁 Exporting data from: {DB_FILE}")
    print(f"📂 Output directory: {OUTPUT_DIR}")
    print()
    
    # Check if database file exists
    if not Path(DB_FILE).exists():
        print(f"⚠️  Database file not found: {DB_FILE}")
        return 1
        
    # Export each table
    exported_count = 0
    for table, csv_path in EXPORTS.items():
        try:
            export_table_to_csv(DB_FILE, table, csv_path)
            if csv_path.exists():
                exported_count += 1
        except Exception as e:
            print(f"⚠️  Failed to export {table}: {e}")
    
    print()
    print(f"✅ Successfully exported {exported_count} of {len(EXPORTS)} tables")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

