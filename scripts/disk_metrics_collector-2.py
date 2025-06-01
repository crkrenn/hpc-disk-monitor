#!/usr/bin/env python3
import os
import time
import statistics
import sqlite3
from datetime import datetime, timedelta
from dotenv import dotenv_values
import getpass
from pathlib import Path

# Preprocess .env manually to support {{whoami}} and {{HOME}}
def preprocess_env(path=".env"):
    raw_env = dotenv_values(path)
    username = getpass.getuser()
    home_dir = str(Path.home())

    processed = {
        key: value
        .replace("{{whoami}}", username)
        .replace("{{HOME}}", home_dir)
        if value else value
        for key, value in raw_env.items()
    }

    for key, value in processed.items():
        os.environ[key] = value

preprocess_env()

DB_FILE = os.getenv("DISK_STATS_DB", str(Path.home() / "hpc-disk-monitor/data/disk_stats.db"))


def connect_db():
    return sqlite3.connect(DB_FILE)


def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def compute_summary_from_db():
    conn = connect_db()
    one_hour_ago = datetime.now() - timedelta(hours=1)
    ts_threshold = one_hour_ago.strftime("%Y-%m-%d %H:%M")
    summary = {}
    with conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT label FROM disk_stats")
        labels = [row[0] for row in cursor.fetchall()]

        for label in labels:
            cursor.execute("""
                SELECT write_mbps, write_iops, write_lat_avg,
                       read_mbps, read_iops, read_lat_avg
                FROM disk_stats
                WHERE label = ? AND timestamp >= ?
            """, (label, ts_threshold))
            rows = cursor.fetchall()

            if not rows:
                continue

            metrics = list(zip(*rows))  # Transpose rows
            metric_names = [
                "write_mbps", "write_iops", "write_lat_avg",
                "read_mbps", "read_iops", "read_lat_avg"
            ]

            for i, name in enumerate(metric_names):
                values = metrics[i]
                summary.setdefault(label, {})[name] = {
                    "avg": sum(values) / len(values),
                    "min": min(values),
                    "max": max(values),
                    "stddev": statistics.stdev(values) if len(values) > 1 else 0
                }

    return summary


def insert_summary_stats_from_db():
    conn = connect_db()
    timestamp = current_timestamp()
    summary = compute_summary_from_db()

    with conn:
        c = conn.cursor()
        for label, metrics in summary.items():
            for metric, stats in metrics.items():
                c.execute('''
                    INSERT INTO disk_stats_summary (
                        timestamp, label, metric, avg, min, max, stddev
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (timestamp, label, metric,
                      stats["avg"], stats["min"], stats["max"], stats["stddev"]))
        conn.commit()


if __name__ == "__main__":
    insert_summary_stats_from_db()
    print("✅ Summary statistics from last hour inserted.")
