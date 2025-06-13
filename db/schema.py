import sqlite3
import os

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.env_utils import preprocess_env
preprocess_env()

# Use HOME directory for database path by default
DB_FILE = os.getenv("RESOURCE_STATS_DB", os.getenv("DISK_STATS_DB", str(Path.home() / "hpc-resource-monitor/data/resource_stats.db")))

def connect_db(fail_gracefully=True):
    """Connect to the database and create directory structure if needed.
    
    Args:
        fail_gracefully: If True, return None instead of raising exceptions for filesystem errors
        
    Returns:
        Connection object or None if error and fail_gracefully is True
    """
    try:
        db_path = Path(DB_FILE)
        try:
            db_path.parent.mkdir(parents=True, exist_ok=True)
        except (OSError, IOError) as e:
            if fail_gracefully:
                print(f"Error creating database directory: {e.__class__.__name__}: {e}")
                print(f"Cannot access {db_path.parent}")
                return None
            raise
            
        # Try to check if we can actually write to this location
        try:
            # Test write permissions by touching a file
            test_file = db_path.parent / ".db_write_test"
            test_file.touch()
            test_file.unlink(missing_ok=True)
        except (OSError, IOError) as e:
            if fail_gracefully:
                print(f"Error writing to database directory: {e.__class__.__name__}: {e}")
                print(f"Cannot write to {db_path.parent}")
                return None
            raise
            
        return sqlite3.connect(DB_FILE)
    except (sqlite3.Error, OSError, IOError) as e:
        if fail_gracefully:
            print(f"Database connection error: {e.__class__.__name__}: {e}")
            return None
        raise

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
        c.execute('''
            CREATE TABLE IF NOT EXISTS api_stats (
                timestamp TEXT,
                hostname TEXT,
                api_name TEXT,
                endpoint_url TEXT,
                response_time_ms REAL,
                status_code INTEGER,
                success BOOLEAN,
                error_message TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS api_stats_summary (
                timestamp TEXT,
                hostname TEXT,
                api_name TEXT,
                metric TEXT,
                avg REAL,
                min REAL,
                max REAL,
                stddev REAL,
                success_rate REAL
            )
        ''')
