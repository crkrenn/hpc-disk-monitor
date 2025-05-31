import sqlite3
import os

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.env_utils import preprocess_env
preprocess_env()

DB_FILE = os.getenv("DISK_STATS_DB", str(Path.home() / "hpc-disk-monitor/data/disk_stats.db"))

def connect_db():
    db_path = Path(DB_FILE)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_FILE)

def create_tables(conn):
    with conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS disk_stats (
                timestamp TEXT,
                hostname TEXT,
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
                hostname TEXT,
                label TEXT,
                metric TEXT,
                avg REAL,
                min REAL,
                max REAL,
                stddev REAL
            )
        ''')
