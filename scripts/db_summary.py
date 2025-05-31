#!/usr/bin/env python3

import sqlite3
import os
from tabulate import tabulate
from dotenv import load_dotenv
from pathlib import Path

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.env_utils import preprocess_env

# Load .env and expand {{HOME}}/{{whoami}} if needed
preprocess_env(use_shell_env=True)
DB_FILE = os.getenv("DISK_STATS_DB", str(Path.home() / "hpc-disk-monitor/data/disk_stats.db"))

def connect_db():
    return sqlite3.connect(DB_FILE)

def get_time_bounds_and_count(conn):
    c = conn.cursor()
    c.execute("SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM disk_stats")
    return c.fetchone()

def get_latest_summary(conn):
    c = conn.cursor()
    c.execute("""
        SELECT label, metric, avg, min, max, stddev
        FROM disk_stats_summary
        WHERE timestamp = (SELECT MAX(timestamp) FROM disk_stats_summary)
        ORDER BY label, metric
    """)
    return c.fetchall()

def sci(x):
    return f"{x:.2e}"

def main():
    conn = connect_db()


    summary = get_latest_summary(conn)
    if not summary:
        print("⚠️  No summary data found.")
        return

    table = []
    for label, metric, avg, minv, maxv, std in summary:
        table.append([
            label, metric,
            sci(avg),
            sci(minv),
            sci(maxv),
            sci(std)
        ])

    print(tabulate(table, headers=["Label", "Metric", "Avg", "Min", "Max", "Stddev"], tablefmt="grid"))

    first_ts, last_ts, count = get_time_bounds_and_count(conn)
    print(f"📊 Total Samples: {count}")
    print(f"📅 First Sample: {first_ts}")
    print(f"📅 Last Sample : {last_ts}\n")


if __name__ == "__main__":
    main()
