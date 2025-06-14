#!/usr/bin/env python3
import os
import time
import statistics
import sqlite3
import argparse
import requests
from datetime import datetime, timedelta
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
parser = argparse.ArgumentParser(description='Collect API status metrics')
parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
args = parser.parse_args()

# Parse API endpoints config
api_endpoints = os.getenv("API_ENDPOINTS", "").split(",")
api_names = os.getenv("API_NAMES", "").split(",")

# Filter out empty strings
api_endpoints = [url.strip() for url in api_endpoints if url.strip()]
api_names = [name.strip() for name in api_names if name.strip()]

if len(api_endpoints) != len(api_names):
    if api_endpoints and not api_names:
        # Generate default names from URLs
        api_names = [f"API-{i+1}" for i in range(len(api_endpoints))]
    elif len(api_endpoints) > 0 and len(api_names) > 0:
        raise ValueError("API_ENDPOINTS and API_NAMES must have the same length")

API_CONFIG = dict(zip(api_endpoints, api_names)) if api_endpoints else {}

# Use DB_FILE from schema module
from db.schema import DB_FILE

# Parameters
REQUEST_TIMEOUT = int(os.getenv("API_REQUEST_TIMEOUT", "30"))  # seconds
ROLLING_WINDOW_MINUTES = 60

def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M")

def test_api_endpoint(url, timeout=REQUEST_TIMEOUT):
    """Test an API endpoint and return response metrics."""
    start_time = time.time()
    result = {
        "response_time_ms": 0,
        "status_code": 0,
        "success": False,
        "error_message": None
    }
    
    try:
        response = requests.get(url, timeout=timeout)
        response_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        result.update({
            "response_time_ms": response_time,
            "status_code": response.status_code,
            "success": 200 <= response.status_code < 300,
            "error_message": None if 200 <= response.status_code < 300 else f"HTTP {response.status_code}"
        })
        
    except requests.exceptions.Timeout:
        result.update({
            "response_time_ms": timeout * 1000,
            "status_code": 0,
            "success": False,
            "error_message": "Request timeout"
        })
    except requests.exceptions.ConnectionError as e:
        result.update({
            "response_time_ms": (time.time() - start_time) * 1000,
            "status_code": 0,
            "success": False,
            "error_message": f"Connection error: {str(e)[:100]}"
        })
    except requests.exceptions.RequestException as e:
        result.update({
            "response_time_ms": (time.time() - start_time) * 1000,
            "status_code": 0,
            "success": False,
            "error_message": f"Request error: {str(e)[:100]}"
        })
    except Exception as e:
        result.update({
            "response_time_ms": (time.time() - start_time) * 1000,
            "status_code": 0,
            "success": False,
            "error_message": f"Unexpected error: {str(e)[:100]}"
        })
    
    return result

def init_db():
    """Initialize database and create tables if needed."""
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
                    if 'api' in table_name:
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

def insert_api_record(record):
    """Insert a new record into the api_stats table."""
    conn = connect_db(fail_gracefully=True)
    if conn is None:
        if args.verbose:
            print(f"⚠️  Could not connect to database to insert record for {record['api_name']}")
        return False
        
    try:
        with conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO api_stats (
                    timestamp, hostname, api_name, endpoint_url,
                    response_time_ms, status_code, success, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record["timestamp"], HOSTNAME, record["api_name"], record["endpoint_url"],
                record["response_time_ms"], record["status_code"], record["success"], record["error_message"]
            ))
        return True
    except sqlite3.Error as e:
        if args.verbose:
            print(f"⚠️  Error inserting record for {record['api_name']}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def insert_api_summary_stats(api_name, summary):
    """Insert summary statistics into the api_stats_summary table."""
    timestamp = current_timestamp()
    conn = connect_db(fail_gracefully=True)
    if conn is None:
        if args.verbose:
            print(f"⚠️  Could not connect to database to insert summary for {api_name}")
        return False
        
    try:
        with conn:
            c = conn.cursor()
            for metric, stats in summary.items():
                success_rate = stats.get("success_rate", 0.0)
                c.execute('''
                    INSERT INTO api_stats_summary (
                        timestamp, hostname, api_name, metric, avg, min, max, stddev, success_rate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (timestamp, HOSTNAME, api_name, metric,
                      stats["avg"], stats["min"], stats["max"], stats["stddev"], success_rate))
        return True
    except sqlite3.Error as e:
        if args.verbose:
            print(f"⚠️  Error inserting summary for {api_name}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def compute_and_store_api_summary(api_name):
    """Calculate summary statistics from the last hour of API data in the database."""
    conn = connect_db(fail_gracefully=True)
    if conn is None:
        if args.verbose:
            print(f"⚠️  Could not connect to database to compute summary for {api_name}")
        return False
        
    try:
        one_hour_ago = datetime.now() - timedelta(hours=1)
        ts_threshold = one_hour_ago.strftime("%Y-%m-%d %H:%M")
        
        if args.verbose:
            print(f"Computing API summary statistics for {api_name} since {ts_threshold}...")
        
        with conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT response_time_ms, success, status_code
                FROM api_stats
                WHERE api_name = ? AND hostname = ? AND timestamp >= ?
            """, (api_name, HOSTNAME, ts_threshold))
            rows = cursor.fetchall()
        
        if not rows:
            if args.verbose:
                print(f"No API data found for {api_name} in the last hour")
            return True
        
        response_times = [row[0] for row in rows]
        successes = [row[1] for row in rows]
        status_codes = [row[2] for row in rows]
        
        success_rate = sum(successes) / len(successes) if successes else 0.0
        
        summary = {
            "response_time_ms": {
                "min": min(response_times),
                "max": max(response_times),
                "avg": sum(response_times) / len(response_times),
                "stddev": statistics.stdev(response_times) if len(response_times) > 1 else 0,
                "success_rate": success_rate
            },
            "status_code": {
                "min": min(status_codes),
                "max": max(status_codes),
                "avg": sum(status_codes) / len(status_codes),
                "stddev": statistics.stdev(status_codes) if len(status_codes) > 1 else 0,
                "success_rate": success_rate
            }
        }
                
        if args.verbose:
            print(f"Found {len(rows)} API data points for {api_name} in the last hour")
            print(f"Success rate: {success_rate:.2%}")
                
        return insert_api_summary_stats(api_name, summary)
    except sqlite3.Error as e:
        if args.verbose:
            print(f"⚠️  Error computing API summary for {api_name}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def run_once_and_record():
    """Run a single collection cycle for all configured API endpoints."""
    if not API_CONFIG:
        if args.verbose:
            print("No API endpoints configured. Please set API_ENDPOINTS and optionally API_NAMES.")
        return True  # Not an error condition, just no APIs to monitor
    
    timestamp = current_timestamp()
    success_count = 0
    error_count = 0
    
    for endpoint_url, api_name in API_CONFIG.items():
        if args.verbose:
            print(f"Testing {api_name} ({endpoint_url})...")
        
        try:
            result = test_api_endpoint(endpoint_url)
            
            entry = {
                "timestamp": timestamp,
                "api_name": api_name,
                "endpoint_url": endpoint_url,
                "response_time_ms": result["response_time_ms"],
                "status_code": result["status_code"],
                "success": result["success"],
                "error_message": result["error_message"]
            }
            
            if args.verbose:
                status = "✅ SUCCESS" if result["success"] else "❌ FAILED"
                print(f"{api_name} results: {status}")
                print(f"  Response time: {result['response_time_ms']:.2f} ms")
                print(f"  Status code: {result['status_code']}")
                if result["error_message"]:
                    print(f"  Error: {result['error_message']}")
            
            # Insert record and compute summary
            if insert_api_record(entry) and compute_and_store_api_summary(api_name):
                success_count += 1
            else:
                error_count += 1
                
        except Exception as e:
            if args.verbose:
                print(f"⚠️  Unexpected error testing {api_name}: {e}")
            error_count += 1
    
    if args.verbose:
        print(f"Done. Successfully processed {success_count} of {len(API_CONFIG)} API endpoints.")
    
    return success_count > 0 or len(API_CONFIG) == 0

def main():
    """Main entry point for the script."""
    try:
        if args.verbose:
            print(f"Initializing DB at {DB_FILE}...")
            
        # Initialize database
        db_init_success = init_db()
        if not db_init_success:
            if args.verbose:
                print("⚠️  Database initialization failed. Some features may not work.")
                
        if API_CONFIG and args.verbose:
            print(f"Starting API status collection for: {', '.join(API_CONFIG.values())}")
        elif args.verbose:
            print("No API endpoints configured for monitoring.")
            
        # Run the collection process
        collection_success = run_once_and_record()
        
        # Return status code based on success
        if not collection_success:
            if args.verbose:
                print("⚠️  No API endpoints were successfully processed.")
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