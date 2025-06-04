#!/usr/bin/env python3

import sqlite3
import os
from tabulate import tabulate
from pathlib import Path
import sys
import argparse
from datetime import datetime, timedelta

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.env_utils import preprocess_env

# Define common time period options
TIME_PERIODS = {
    "1h": {"label": "Last Hour", "days": 0.0417},
    "6h": {"label": "Last 6 Hours", "days": 0.25},
    "12h": {"label": "Last 12 Hours", "days": 0.5},
    "1d": {"label": "Last 24 Hours", "days": 1},
    "7d": {"label": "Last 7 Days", "days": 7},
    "30d": {"label": "Last 30 Days", "days": 30},
    "90d": {"label": "Last 90 Days", "days": 90},
    "1y": {"label": "Last Year", "days": 365},
    "all": {"label": "All Time", "days": None}
}

# Parse command line arguments
parser = argparse.ArgumentParser(description='Display disk metrics summary')
parser.add_argument('--time-period', '-t', choices=TIME_PERIODS.keys(), default="1d",
                    help='Time period for statistics (default: 1d)')
parser.add_argument('--recompute', '-r', action='store_true',
                    help='Recompute summary statistics for the specified time period')
parser.add_argument('--start-date', type=str, 
                    help='Start date for custom period (format: YYYY-MM-DD [HH:MM])')
parser.add_argument('--end-date', type=str,
                    help='End date for custom period (format: YYYY-MM-DD [HH:MM], defaults to now)')
parser.add_argument('--verbose', '-v', action='store_true',
                    help='Show verbose output')
args = parser.parse_args()

# Load .env and expand {{HOME}}/{{whoami}} if needed
preprocess_env(use_shell_env=True)
DB_FILE = os.getenv("DISK_STATS_DB", str(Path.home() / "hpc-disk-monitor/data/disk_stats.db"))

# Get filesystem configuration from environment
FS_LABELS = os.getenv("FILESYSTEM_LABELS", "tmpfs").split(",")

def connect_db():
    """Connect to the database with error handling.
    
    Returns:
        Connection object or None if connection failed.
    """
    try:
        return sqlite3.connect(DB_FILE)
    except (sqlite3.Error, OSError, IOError) as e:
        print(f"⚠️  Database connection error: {e.__class__.__name__}: {e}")
        print(f"⚠️  Could not connect to database at {DB_FILE}")
        return None

def get_time_bounds_and_count(conn, start_time=None, end_time=None):
    """Get time bounds and count of records, optionally filtered by time period
    
    Args:
        conn (Connection): SQLite connection
        start_time (str): ISO format datetime string for start of period
        end_time (str): ISO format datetime string for end of period
        
    Returns:
        tuple: (first_timestamp, last_timestamp, count)
    """
    if conn is None:
        return None, None, 0
        
    try:
        c = conn.cursor()
        
        # Get time filter if specified
        time_filter, time_params = get_time_filter_params(start_time, end_time)
        
        # Execute query with optional time filter
        query = f"SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM disk_stats WHERE 1=1 {time_filter}"
        c.execute(query, time_params)
        
        return c.fetchone()
    except sqlite3.Error as e:
        print(f"⚠️  Error retrieving time bounds: {e}")
        return None, None, 0

def get_time_filter_params(start_time=None, end_time=None):
    """Get SQL filter for time period and corresponding parameters
    
    Args:
        start_time (str): ISO format datetime string for start of period
        end_time (str): ISO format datetime string for end of period
        
    Returns:
        tuple: (sql_filter, params)
    """
    filters = []
    params = []
    
    if start_time:
        filters.append("timestamp >= ?")
        params.append(start_time)
        
    if end_time:
        filters.append("timestamp <= ?")
        params.append(end_time)
        
    if filters:
        return " AND " + " AND ".join(filters), params
    else:
        return "", []

def compute_summary_stats(conn, start_time=None, end_time=None):
    """Recompute summary statistics for all filesystems within the time period
    
    Args:
        conn (Connection): SQLite connection
        start_time (str): ISO format datetime string for start of period
        end_time (str): ISO format datetime string for end of period
        
    Returns:
        bool: True if successful, False otherwise
    """
    if conn is None:
        print("⚠️  Cannot compute statistics: no database connection")
        return False
        
    try:
        # Get the time filter
        time_filter, time_params = get_time_filter_params(start_time, end_time)
        
        # Create label filter
        if FS_LABELS:
            placeholders = ', '.join(['?'] * len(FS_LABELS))
            label_filter = f"AND label IN ({placeholders})"
            label_params = FS_LABELS
        else:
            label_filter = ""
            label_params = []
        
        # Get all filesystems with data in the specified period
        cursor = conn.cursor()
        query = f"""
            SELECT DISTINCT label 
            FROM disk_stats
            WHERE 1=1 {time_filter} {label_filter}
            ORDER BY label
        """
        cursor.execute(query, time_params + label_params)
        labels = [row[0] for row in cursor.fetchall()]
        
        if not labels:
            print(f"⚠️  No data found for the specified time period")
            return False
            
        # Time period description for logging
        time_desc = ""
        if start_time:
            time_desc += f"from {start_time} "
        if end_time:
            time_desc += f"to {end_time}"
        if not time_desc:
            time_desc = "for all time"
            
        if args.verbose:
            print(f"Computing summary statistics {time_desc} for {len(labels)} filesystems:")
            for label in labels:
                print(f"  - {label}")
                
        # Current timestamp for all new summaries
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        hostname = os.uname().nodename
        
        # For each label, compute stats for each metric
        for label in labels:
            # Get all raw data for this label in the time period
            cursor.execute(f"""
                SELECT write_mbps, write_iops, write_lat_avg,
                       read_mbps, read_iops, read_lat_avg
                FROM disk_stats
                WHERE label = ? {time_filter}
            """, [label] + time_params)
            
            rows = cursor.fetchall()
            if not rows:
                if args.verbose:
                    print(f"  No data points found for {label} in the specified time period")
                continue
                
            # Transpose rows to get columns
            metrics = list(zip(*rows))
            metric_names = [
                "write_mbps", "write_iops", "write_lat_avg",
                "read_mbps", "read_iops", "read_lat_avg"
            ]
            
            # Calculate stats for each metric
            summary = {}
            for i, name in enumerate(metric_names):
                values = metrics[i]
                if values:
                    import statistics
                    summary[name] = {
                        "min": min(values),
                        "max": max(values),
                        "avg": sum(values) / len(values),
                        "stddev": statistics.stdev(values) if len(values) > 1 else 0
                    }
            
            if args.verbose:
                print(f"  Computed stats for {label} from {len(rows)} data points")
                
            # Insert into summary table
            for metric, stats in summary.items():
                cursor.execute('''
                    INSERT INTO disk_stats_summary (
                        timestamp, hostname, label, metric, avg, min, max, stddev
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (timestamp, hostname, label, metric,
                      stats["avg"], stats["min"], stats["max"], stats["stddev"]))
                      
        conn.commit()
        
        if args.verbose:
            print(f"Summary statistics computation complete for time period {time_desc}")
        
        return True
        
    except (sqlite3.Error, Exception) as e:
        print(f"⚠️  Error computing summary statistics: {e}")
        return False

def get_latest_summary_per_filesystem(conn, start_time=None, end_time=None):
    """Get the latest summary stats for each filesystem and metric, with only one record per metric
    
    Args:
        conn (Connection): SQLite connection
        start_time (str): ISO format datetime string for start of period
        end_time (str): ISO format datetime string for end of period
        
    Returns:
        list: Summary records
    """
    if conn is None:
        return []
        
    try:
        c = conn.cursor()
        
        # Create label filter if filesystem labels are configured
        if FS_LABELS:
            placeholders = ', '.join(['?'] * len(FS_LABELS))
            label_filter = f"AND label IN ({placeholders})"
            label_params = FS_LABELS
        else:
            label_filter = ""
            label_params = []
            
        # Add time filter if specified
        time_filter, time_params = get_time_filter_params(start_time, end_time)
        
        # Query for the latest summary records within time period
        if start_time or end_time:
            # Use the provided time range and get the latest record per label/metric in that range
            query = f"""
                WITH filtered_summaries AS (
                    SELECT 
                        label, 
                        metric, 
                        MAX(timestamp) as latest_timestamp
                    FROM disk_stats_summary
                    WHERE 1=1 {time_filter} {label_filter}
                    GROUP BY label, metric
                ),
                -- Add row numbers to pick just one record per label/metric combination
                numbered_results AS (
                    SELECT
                        dss.label,
                        dss.metric,
                        dss.avg,
                        dss.min,
                        dss.max,
                        dss.stddev,
                        dss.timestamp,
                        dss.hostname,
                        ROW_NUMBER() OVER (PARTITION BY dss.label, dss.metric ORDER BY dss.rowid DESC) as row_num
                    FROM disk_stats_summary dss
                    JOIN filtered_summaries f ON 
                        dss.label = f.label AND 
                        dss.metric = f.metric AND 
                        dss.timestamp = f.latest_timestamp
                )
                SELECT
                    label,
                    metric,
                    avg,
                    min,
                    max,
                    stddev,
                    timestamp,
                    hostname
                FROM numbered_results
                WHERE row_num = 1
                ORDER BY label, metric
            """
            params = time_params + label_params
        else:
            # Default behavior - just get the most recent record for each label/metric
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
                        dss.label, 
                        dss.metric, 
                        dss.avg, 
                        dss.min, 
                        dss.max, 
                        dss.stddev, 
                        dss.timestamp,
                        dss.hostname,
                        ROW_NUMBER() OVER (
                            PARTITION BY dss.label, dss.metric 
                            ORDER BY dss.timestamp DESC, dss.rowid DESC
                        ) as row_num
                    FROM disk_stats_summary dss
                    JOIN latest_fs_timestamps l ON dss.label = l.label AND dss.timestamp = l.latest_timestamp
                )
                -- Only return one record per filesystem+metric
                SELECT label, metric, avg, min, max, stddev, timestamp, hostname
                FROM latest_metrics
                WHERE row_num = 1
                ORDER BY label, metric
            """
            params = label_params
        
        c.execute(query, params)
        return c.fetchall()
        
    except sqlite3.Error as e:
        print(f"⚠️  Error retrieving summary data: {e}")
        return []

def get_all_filesystems(conn, start_time=None, end_time=None):
    """Get all filesystems with data, optionally filtered by time period
    
    Args:
        conn (Connection): SQLite connection
        start_time (str): ISO format datetime string for start of period
        end_time (str): ISO format datetime string for end of period
        
    Returns:
        list: Filesystem labels
    """
    if conn is None:
        return []
        
    try:
        c = conn.cursor()
        
        # Get time filter if specified
        time_filter, time_params = get_time_filter_params(start_time, end_time)
        
        # If filesystem labels are configured, only return those that exist in the database
        if FS_LABELS:
            placeholders = ', '.join(['?'] * len(FS_LABELS))
            query = f"SELECT DISTINCT label FROM disk_stats WHERE label IN ({placeholders}) {time_filter} ORDER BY label"
            c.execute(query, FS_LABELS + time_params)
        else:
            query = f"SELECT DISTINCT label FROM disk_stats WHERE 1=1 {time_filter} ORDER BY label"
            c.execute(query, time_params)
            
        return [row[0] for row in c.fetchall()]
    except sqlite3.Error as e:
        print(f"⚠️  Error retrieving filesystems: {e}")
        return []

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

def get_time_range_from_args():
    """Parse command line arguments and return time range parameters
    
    Returns:
        tuple: (start_time, end_time, time_description)
    """
    # Custom date range takes precedence
    if args.start_date:
        # Parse start date
        try:
            # Try parsing with time
            try:
                start_time = datetime.strptime(args.start_date, "%Y-%m-%d %H:%M")
            except ValueError:
                # Try parsing just date
                start_time = datetime.strptime(args.start_date, "%Y-%m-%d")
                
            start_str = start_time.strftime("%Y-%m-%d %H:%M")
        except ValueError as e:
            print(f"⚠️  Invalid start date format: {e}")
            print("    Expected format: YYYY-MM-DD [HH:MM]")
            return None, None, None
            
        # Parse end date if provided, otherwise use current time
        if args.end_date:
            try:
                # Try parsing with time
                try:
                    end_time = datetime.strptime(args.end_date, "%Y-%m-%d %H:%M")
                except ValueError:
                    # Try parsing just date
                    end_time = datetime.strptime(args.end_date, "%Y-%m-%d")
                    
                end_str = end_time.strftime("%Y-%m-%d %H:%M")
            except ValueError as e:
                print(f"⚠️  Invalid end date format: {e}")
                print("    Expected format: YYYY-MM-DD [HH:MM]")
                return None, None, None
        else:
            end_time = datetime.now()
            end_str = end_time.strftime("%Y-%m-%d %H:%M")
            
        time_desc = f"from {start_str} to {end_str}"
        return start_str, end_str, time_desc
        
    # If using a predefined time period
    if args.time_period:
        end_time = datetime.now()
        end_str = end_time.strftime("%Y-%m-%d %H:%M")
        
        # Special case for "all"
        if args.time_period == "all":
            return None, None, "all time"
            
        # Get days from time period
        days = TIME_PERIODS[args.time_period]["days"]
        label = TIME_PERIODS[args.time_period]["label"]
        
        # Calculate start time
        start_time = end_time - timedelta(days=days)
        start_str = start_time.strftime("%Y-%m-%d %H:%M")
        
        return start_str, end_str, label
        
    # Default to last 24 hours
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)
    return start_time.strftime("%Y-%m-%d %H:%M"), end_time.strftime("%Y-%m-%d %H:%M"), "Last 24 Hours"

def main():
    """Main entry point for the script."""
    try:
        # Connect to the database
        conn = connect_db()
        if conn is None:
            print("⚠️  Cannot continue without database connection.")
            return 1
        
        try:
            # Get time range from command line arguments
            start_time, end_time, time_desc = get_time_range_from_args()
            if start_time is None and args.start_date:
                # Error in date parsing
                return 1
                
            # Recompute statistics if requested
            if args.recompute:
                print(f"Recomputing summary statistics for {time_desc}...")
                if compute_summary_stats(conn, start_time, end_time):
                    print("✅ Summary statistics have been recomputed and saved to database.")
                else:
                    print("⚠️  Failed to recompute summary statistics.")
                    return 1
            
            # Get all filesystems configured in .env that have data for the time period
            configured_filesystems = FS_LABELS
            filesystems = get_all_filesystems(conn, start_time, end_time)
            
            if not filesystems:
                print(f"⚠️  No filesystem data found for {time_desc}.")
            else:
                # Find which configured filesystems have data
                found_fs = [fs for fs in configured_filesystems if fs in filesystems]
                not_found_fs = [fs for fs in configured_filesystems if fs not in filesystems]
                
                print(f"Found data for {len(found_fs)} of {len(configured_filesystems)} configured filesystems: {', '.join(found_fs)}")
                
                if not_found_fs:
                    print(f"⚠️  No data found for these configured filesystems during {time_desc}: {', '.join(not_found_fs)}")
                
                print()
                
            # Print time period information
            print(f"📊 Showing statistics for: {time_desc}")
            
            # Get summary data for the selected time period
            try:
                summary = get_latest_summary_per_filesystem(conn, start_time, end_time)
                if not summary:
                    print("⚠️  No summary data found for the specified time period.")
                    return 0
            except sqlite3.Error as e:
                print(f"⚠️  Error retrieving summary data: {e}")
                return 1
        
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
        
            # Show overall stats for both the filtered period and all time
            try:
                # Get stats for the filtered period
                period_first_ts, period_last_ts, period_count = get_time_bounds_and_count(conn, start_time, end_time)
                
                # Get stats for all time
                all_first_ts, all_last_ts, all_count = get_time_bounds_and_count(conn)
                
                print("\n=== STATISTICS FOR SELECTED PERIOD ===")
                if period_count > 0:
                    print(f"📊 Total Samples: {period_count}")
                    print(f"📅 First Sample: {period_first_ts}")
                    print(f"📅 Last Sample : {period_last_ts}")
                else:
                    print("⚠️  No samples in the selected time period")
                
                if time_desc != "all time":
                    print("\n=== OVERALL STATISTICS (ALL TIME) ===")
                    print(f"📊 Total Samples: {all_count}")
                    print(f"📅 First Sample: {all_first_ts}")
                    print(f"📅 Last Sample : {all_last_ts}")
                
                print()
            except sqlite3.Error as e:
                print(f"\n⚠️  Error retrieving statistics: {e}")
                
            return 0
        
        finally:
            if conn:
                conn.close()
                
    except Exception as e:
        # Catch any unexpected exceptions to avoid traceback
        print(f"⚠️  An unexpected error occurred: {e.__class__.__name__}: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
