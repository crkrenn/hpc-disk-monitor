#!/usr/bin/env python3

import sqlite3
import os
from tabulate import tabulate
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.env_utils import preprocess_env

# Load .env and expand {{HOME}}/{{whoami}} if needed
preprocess_env(use_shell_env=True)
DB_FILE = os.getenv("DISK_STATS_DB", str(Path.home() / "hpc-disk-monitor/data/disk_stats.db"))

# Get filesystem configuration from environment
FS_LABELS = os.getenv("FILESYSTEM_LABELS", "tmpfs").split(",")

def connect_db():
    return sqlite3.connect(DB_FILE)

def get_time_bounds_and_count(conn):
    c = conn.cursor()
    c.execute("SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM disk_stats")
    return c.fetchone()

def get_latest_summary_per_filesystem(conn):
    """Get the latest summary stats for each filesystem and metric, with only one record per metric"""
    c = conn.cursor()
    
    # Create label filter if filesystem labels are configured
    if FS_LABELS:
        placeholders = ', '.join(['?'] * len(FS_LABELS))
        label_filter = f"AND label IN ({placeholders})"
        params = FS_LABELS
    else:
        label_filter = ""
        params = []
    
    # Subquery to get latest timestamp for each filesystem
    query = f"""
        WITH latest_fs_timestamps AS (
            SELECT label, MAX(timestamp) as latest_timestamp
            FROM disk_stats_summary
            WHERE 1=1 {label_filter}
            GROUP BY label
        ),
        -- Pick one record per filesystem+metric from the latest timestamp
        latest_metrics AS (
            SELECT 
                s.label, 
                s.metric, 
                s.avg, 
                s.min, 
                s.max, 
                s.stddev, 
                s.timestamp,
                s.hostname,
                ROW_NUMBER() OVER (
                    PARTITION BY s.label, s.metric 
                    ORDER BY s.timestamp DESC, s.rowid DESC
                ) as row_num
            FROM disk_stats_summary s
            JOIN latest_fs_timestamps l ON s.label = l.label AND s.timestamp = l.latest_timestamp
        )
        -- Only return one record per filesystem+metric
        SELECT label, metric, avg, min, max, stddev, timestamp, hostname
        FROM latest_metrics
        WHERE row_num = 1
        ORDER BY label, metric
    """
    
    c.execute(query, params)
    return c.fetchall()

def get_all_filesystems(conn):
    c = conn.cursor()
    
    # If filesystem labels are configured, only return those that exist in the database
    if FS_LABELS:
        placeholders = ', '.join(['?'] * len(FS_LABELS))
        c.execute(f"SELECT DISTINCT label FROM disk_stats WHERE label IN ({placeholders}) ORDER BY label", FS_LABELS)
    else:
        c.execute("SELECT DISTINCT label FROM disk_stats ORDER BY label")
        
    return [row[0] for row in c.fetchall()]

def sci(x):
    return f"{x:.2e}"

def format_value(value, metric):
    """Format value based on the metric type"""
    if "lat" in metric.lower():
        # Latency values (typically small)
        return sci(value)
    elif "iops" in metric.lower():
        # IOPS values (typically large integers)
        return f"{value:.0f}"
    else:
        # Throughput values (typically floating point)
        return f"{value:.2f}"

def main():
    conn = connect_db()
    
    # Get all filesystems configured in .env that have data
    configured_filesystems = FS_LABELS
    filesystems = get_all_filesystems(conn)
    
    if not filesystems:
        print("⚠️  No filesystem data found in database.")
    else:
        # Find which configured filesystems have data
        found_fs = [fs for fs in configured_filesystems if fs in filesystems]
        not_found_fs = [fs for fs in configured_filesystems if fs not in filesystems]
        
        print(f"Found data for {len(found_fs)} of {len(configured_filesystems)} configured filesystems: {', '.join(found_fs)}")
        
        if not_found_fs:
            print(f"⚠️  No data found for these configured filesystems: {', '.join(not_found_fs)}")
        
        print()
    
    # Get latest summary data for all filesystems (one record per metric)
    summary = get_latest_summary_per_filesystem(conn)
    if not summary:
        print("⚠️  No summary data found.")
        return

    # Group data by filesystem for better presentation
    summary_by_fs = {}
    for row in summary:
        label, metric, avg, minv, maxv, std, timestamp, hostname = row
        if label not in summary_by_fs:
            summary_by_fs[label] = {
                'data': [],
                'timestamp': timestamp,  # Use the timestamp from the first metric for this filesystem
                'hostname': hostname
            }
        summary_by_fs[label]['data'].append([metric, avg, minv, maxv, std])

    # Display summary for each filesystem
    for label, fs_info in summary_by_fs.items():
        print(f"\n=== {label.upper()} ===")
        print(f"Last updated: {fs_info['timestamp']} on {fs_info['hostname']}")
        
        table = []
        for metric, avg, minv, maxv, std in fs_info['data']:
            # Format values based on metric type
            table.append([
                metric.replace('_', ' ').title(),
                format_value(avg, metric),
                format_value(minv, metric),
                format_value(maxv, metric),
                format_value(std, metric)
            ])
        
        print(tabulate(table, headers=["Metric", "Avg", "Min", "Max", "Stddev"], tablefmt="grid"))

    # Show overall stats
    first_ts, last_ts, count = get_time_bounds_and_count(conn)
    print("\n=== OVERALL STATISTICS ===")
    print(f"📊 Total Samples: {count}")
    print(f"📅 First Sample: {first_ts}")
    print(f"📅 Last Sample : {last_ts}\n")


if __name__ == "__main__":
    main()
