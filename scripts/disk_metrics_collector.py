#!/usr/bin/env python3
import os
import time
import statistics
import sqlite3
from datetime import datetime
from collections import deque
from pathlib import Path

import socket
HOSTNAME = socket.gethostname()

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.env_utils import preprocess_env
preprocess_env()


# Parse filesystem config
fs_paths = os.getenv("FILESYSTEM_PATHS", "/tmp").split(",")
fs_labels = os.getenv("FILESYSTEM_LABELS", "tmpfs").split(",")

if len(fs_paths) != len(fs_labels):
    raise ValueError("FILESYSTEM_PATHS and FILESYSTEM_LABELS must have the same length")

FILESYSTEM_CONFIG = dict(zip(fs_paths, fs_labels))
DB_FILE = os.getenv("DISK_STATS_DB", "/mnt/data/disk_stats.db")

# Parameters
DURATION = 3  # seconds per test cycle
CHUNK_SIZE = 4 * 1024  # 4 KB
ROLLING_WINDOW_MINUTES = 60

# Rolling stats store
rolling_stats = {
    label: deque(maxlen=ROLLING_WINDOW_MINUTES)
    for label in FILESYSTEM_CONFIG.values()
}

def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M")

def generate_data(size):
    return os.urandom(size)

def calculate_latency_stats(latencies):
    if not latencies:
        return {"min": 0, "max": 0, "avg": 0, "stdev": 0}
    return {
        "min": min(latencies),
        "max": max(latencies),
        "avg": sum(latencies) / len(latencies),
        "stdev": statistics.stdev(latencies) if len(latencies) > 1 else 0
    }

def test_io_speed(directory, mode='write'):
    file_path = os.path.join(directory, "test_speed.tmp")
    latencies = []
    total_bytes = 0
    ops = 0
    start = time.time()

    try:
        with open(file_path, 'wb' if mode == 'write' else 'rb') as f:
            while time.time() - start < DURATION:
                op_start = time.time()
                if mode == 'write':
                    f.write(generate_data(CHUNK_SIZE))
                    f.flush()
                    os.fsync(f.fileno())
                else:
                    data = f.read(CHUNK_SIZE)
                    if not data:
                        f.seek(0)
                        continue
                latency = time.time() - op_start
                latencies.append(latency)
                total_bytes += CHUNK_SIZE
                ops += 1
    except Exception as e:
        return {"error": str(e)}

    return {
        "mbps": total_bytes / (1024 * 1024) / DURATION,
        "iops": ops / DURATION,
        "latency": calculate_latency_stats(latencies)
    }

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS disk_stats (
                timestamp TEXT,
                label TEXT,
                write_mbps REAL,
                write_iops REAL,
                write_lat_avg REAL,
                read_mbps REAL,
                read_iops REAL,
                read_lat_avg REAL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS disk_stats_summary (
                timestamp TEXT,
                label TEXT,
                metric TEXT,
                avg REAL,
                min REAL,
                max REAL,
                stddev REAL
            )
        ''')
        conn.commit()

def insert_stat_record(record):
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute('''
            INSERT INTO disk_stats (
                timestamp, hostname, label,
                write_mbps, write_iops, write_lat_avg,
                read_mbps, read_iops, read_lat_avg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            record["timestamp"], HOSTNAME, record["label"],
            record["write_mbps"], record["write_iops"], record["write_lat_avg"],
            record["read_mbps"], record["read_iops"], record["read_lat_avg"]
        ))
        conn.commit()

def insert_summary_stats(label, summary):
    timestamp = current_timestamp()
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        for metric, stats in summary.items():
            c.execute('''
                INSERT INTO disk_stats_summary (
                    timestamp, hostname, label, metric, avg, min, max, stddev
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (timestamp, HOSTNAME, label, metric,
                  stats["avg"], stats["min"], stats["max"], stats["stddev"]))
        conn.commit()

def compute_and_store_summary(label):
    window = rolling_stats[label]
    if not window:
        return

    summary = {}
    for key in ["write_mbps", "write_iops", "write_lat_avg",
                "read_mbps", "read_iops", "read_lat_avg"]:
        values = [entry[key] for entry in window]
        if values:
            summary[key] = {
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / len(values),
                "stddev": statistics.stdev(values) if len(values) > 1 else 0
            }
    insert_summary_stats(label, summary)

def decimate_old_data():
    """ Retain all points from the last 1 day, decimate older ones """
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute('''
            DELETE FROM disk_stats
            WHERE timestamp < datetime('now', '-3 days')
              AND rowid % 60 != 0
        ''')
        c.execute('''
            DELETE FROM disk_stats
            WHERE timestamp < datetime('now', '-1 days')
              AND timestamp >= datetime('now', '-3 days')
              AND rowid % 6 != 0
        ''')
        conn.commit()

def run_once_and_record():
    timestamp = current_timestamp()
    for path, label in FILESYSTEM_CONFIG.items():
        write = test_io_speed(path, 'write')
        read = test_io_speed(path, 'read')
        try:
            os.remove(os.path.join(path, "test_speed.tmp"))
        except:
            pass
        if 'error' in write or 'error' in read:
            continue
        entry = {
            "timestamp": timestamp,
            "label": label,
            "write_mbps": write["mbps"],
            "write_iops": write["iops"],
            "write_lat_avg": write["latency"]["avg"],
            "read_mbps": read["mbps"],
            "read_iops": read["iops"],
            "read_lat_avg": read["latency"]["avg"]
        }
        rolling_stats[label].append(entry)
        insert_stat_record(entry)
        compute_and_store_summary(label)
    decimate_old_data()

# Initialize and run
init_db()
run_once_and_record()