#!/usr/bin/env python3
import os
import time
import statistics
import sqlite3
import argparse
from datetime import datetime, timedelta
from collections import deque
from pathlib import Path

import socket
HOSTNAME = socket.gethostname()

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.env_utils import preprocess_env
from db.schema import connect_db, create_tables
preprocess_env()

# Parse command line arguments
parser = argparse.ArgumentParser(description='Collect disk I/O metrics')
parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
args = parser.parse_args()


# Parse filesystem config
fs_paths = os.getenv("FILESYSTEM_PATHS", "/tmp").split(",")
fs_labels = os.getenv("FILESYSTEM_LABELS", "tmpfs").split(",")

if len(fs_paths) != len(fs_labels):
    raise ValueError("FILESYSTEM_PATHS and FILESYSTEM_LABELS must have the same length")

FILESYSTEM_CONFIG = dict(zip(fs_paths, fs_labels))

# Use DB_FILE from schema module
from db.schema import DB_FILE

# Parameters
DURATION = 3  # seconds per test cycle
CHUNK_SIZE = 4 * 1024  # 4 KB
ROLLING_WINDOW_MINUTES = 60

# We no longer need rolling_stats as we'll query the database directly

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
    """Initialize database and create tables if needed.
    
    Returns:
        bool: True if database was successfully initialized, False otherwise.
    """
    conn = connect_db(fail_gracefully=True)
    if conn is None:
        if args.verbose:
            print("⚠️  Could not connect to database. Check permissions and path.")
        return False
        
    try:
        create_tables(conn)
        
        if args.verbose:
            # Show table schemas
            with conn:
                c = conn.cursor()
                c.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = c.fetchall()
                for table in tables:
                    table_name = table[0]
                    print(f"Table: {table_name}")
                    c.execute(f"PRAGMA table_info({table_name})")
                    columns = c.fetchall()
                    for col in columns:
                        print(f"  {col[1]} ({col[2]})")
        return True
    except sqlite3.Error as e:
        if args.verbose:
            print(f"⚠️  Database error: {e}")
        return False
    finally:
        if conn:
            conn.close()

def insert_stat_record(record):
    """Insert a new record into the disk_stats table.
    
    Returns:
        bool: True if record was inserted successfully, False otherwise.
    """
    conn = connect_db(fail_gracefully=True)
    if conn is None:
        if args.verbose:
            print(f"⚠️  Could not connect to database to insert record for {record['label']}")
        return False
        
    try:
        with conn:
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
        return True
    except sqlite3.Error as e:
        if args.verbose:
            print(f"⚠️  Error inserting record for {record['label']}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def insert_summary_stats(label, summary):
    """Insert summary statistics into the disk_stats_summary table.
    
    Returns:
        bool: True if summary was inserted successfully, False otherwise.
    """
    timestamp = current_timestamp()
    conn = connect_db(fail_gracefully=True)
    if conn is None:
        if args.verbose:
            print(f"⚠️  Could not connect to database to insert summary for {label}")
        return False
        
    try:
        with conn:
            c = conn.cursor()
            for metric, stats in summary.items():
                c.execute('''
                    INSERT INTO disk_stats_summary (
                        timestamp, hostname, label, metric, avg, min, max, stddev
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (timestamp, HOSTNAME, label, metric,
                      stats["avg"], stats["min"], stats["max"], stats["stddev"]))
        return True
    except sqlite3.Error as e:
        if args.verbose:
            print(f"⚠️  Error inserting summary for {label}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def compute_and_store_summary(label):
    """Calculate summary statistics from the last hour of data in the database
    
    Returns:
        bool: True if summary was computed and stored successfully, False otherwise.
    """
    conn = connect_db(fail_gracefully=True)
    if conn is None:
        if args.verbose:
            print(f"⚠️  Could not connect to database to compute summary for {label}")
        return False
        
    try:
        one_hour_ago = datetime.now() - timedelta(hours=1)
        ts_threshold = one_hour_ago.strftime("%Y-%m-%d %H:%M")
        
        if args.verbose:
            print(f"Computing summary statistics for {label} since {ts_threshold}...")
        
        with conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT write_mbps, write_iops, write_lat_avg,
                       read_mbps, read_iops, read_lat_avg
                FROM disk_stats
                WHERE label = ? AND hostname = ? AND timestamp >= ?
            """, (label, HOSTNAME, ts_threshold))
            rows = cursor.fetchall()
        
        if not rows:
            if args.verbose:
                print(f"No data found for {label} in the last hour")
            return True  # This is not an error condition, just no data
        
        # Transpose rows to get columns
        metrics = list(zip(*rows))
        metric_names = [
            "write_mbps", "write_iops", "write_lat_avg",
            "read_mbps", "read_iops", "read_lat_avg"
        ]
        
        summary = {}
        for i, name in enumerate(metric_names):
            values = metrics[i]
            if values:
                summary[name] = {
                    "min": min(values),
                    "max": max(values),
                    "avg": sum(values) / len(values),
                    "stddev": statistics.stdev(values) if len(values) > 1 else 0
                }
                
        if args.verbose:
            print(f"Found {len(rows)} data points for {label} in the last hour")
                
        return insert_summary_stats(label, summary)
    except sqlite3.Error as e:
        if args.verbose:
            print(f"⚠️  Error computing summary for {label}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def decimate_old_data():
    """ Retain all points from the last 1 day, decimate older ones
    
    Returns:
        bool: True if decimation was successful, False otherwise.
    """
    conn = connect_db(fail_gracefully=True)
    if conn is None:
        if args.verbose:
            print("⚠️  Could not connect to database to decimate old data")
        return False
        
    try:
        with conn:
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
        return True
    except sqlite3.Error as e:
        if args.verbose:
            print(f"⚠️  Error decimating old data: {e}")
        return False
    finally:
        if conn:
            conn.close()

def run_once_and_record():
    """Run a single collection cycle for all configured filesystems.
    
    Returns:
        bool: True if at least one filesystem was successfully tested, False otherwise.
    """
    timestamp = current_timestamp()
    success_count = 0
    error_count = 0
    
    for path, label in FILESYSTEM_CONFIG.items():
        if args.verbose:
            print(f"Testing {label} ({path})...")
        
        # Check if path exists and is accessible
        try:
            if not os.path.isdir(path):
                if args.verbose:
                    print(f"⚠️  Path {path} for {label} does not exist or is not a directory")
                error_count += 1
                continue
                
            # Check if we can write to the path
            test_path = os.path.join(path, ".write_test")
            try:
                with open(test_path, 'w') as f:
                    f.write('test')
                os.remove(test_path)
            except (OSError, IOError) as e:
                if args.verbose:
                    print(f"⚠️  Cannot write to {path} for {label}: {e}")
                error_count += 1
                continue
        except Exception as e:
            if args.verbose:
                print(f"⚠️  Error checking {path} for {label}: {e}")
            error_count += 1
            continue
            
        # Run the actual tests
        try:
            write = test_io_speed(path, 'write')
            read = test_io_speed(path, 'read')
            
            # Clean up test file
            try:
                os.remove(os.path.join(path, "test_speed.tmp"))
            except:
                pass
            
            if 'error' in write or 'error' in read:
                if args.verbose:
                    print(f"Error testing {label}: {write.get('error', '')} {read.get('error', '')}")
                error_count += 1
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
            
            if args.verbose:
                print(f"{label} results:")
                print(f"  Write: {write['mbps']:.2f} MB/s, {write['iops']:.2f} IOPS, {write['latency']['avg']*1000:.2f} ms avg latency")
                print(f"  Read:  {read['mbps']:.2f} MB/s, {read['iops']:.2f} IOPS, {read['latency']['avg']*1000:.2f} ms avg latency")
            
            # Insert record and compute summary
            if insert_stat_record(entry) and compute_and_store_summary(label):
                success_count += 1
            else:
                error_count += 1
                
        except Exception as e:
            if args.verbose:
                print(f"⚠️  Unexpected error testing {label}: {e}")
            error_count += 1
    
    # Try to decimate old data but don't count it in success/error
    if args.verbose:
        print("Decimating old data...")
    decimate_success = decimate_old_data()
    
    if not decimate_success and args.verbose:
        print("⚠️  Failed to decimate old data")
    
    if args.verbose:
        print(f"Done. Successfully processed {success_count} of {len(FILESYSTEM_CONFIG)} filesystems.")
    
    return success_count > 0

def main():
    """Main entry point for the script."""
    try:
        # Validate environment and configuration
        if not FILESYSTEM_CONFIG:
            print("⚠️  No filesystems configured. Please check FILESYSTEM_PATHS and FILESYSTEM_LABELS in .env")
            return 1
            
        if args.verbose:
            print(f"Initializing DB at {DB_FILE}...")
            
        # Initialize database - continue even if this fails as we might be able to use memory mode
        db_init_success = init_db()
        if not db_init_success:
            if args.verbose:
                print("⚠️  Database initialization failed. Some features may not work.")
                
        if args.verbose:
            print(f"Starting disk metrics collection for: {', '.join(FILESYSTEM_CONFIG.values())}")
            
        # Run the collection process
        collection_success = run_once_and_record()
        
        # Return status code based on success
        if not collection_success:
            if args.verbose:
                print("⚠️  No filesystems were successfully processed.")
            return 1
            
        return 0
        
    except Exception as e:
        # Catch any unexpected exceptions to avoid traceback
        print(f"⚠️  An unexpected error occurred: {e.__class__.__name__}: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

# Only run if executed directly
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)