# HPC Disk Monitor

A comprehensive system for monitoring disk I/O performance metrics across multiple filesystems. Designed for High-Performance Computing (HPC) environments, this tool collects, stores, and visualizes key disk performance metrics.

## Features

- Monitors multiple filesystems simultaneously
- Collects key metrics: read/write throughput (MB/s), IOPS, and latency
- Stores historical data with intelligent decimation for long-term storage
- Provides hourly summary statistics for each filesystem
- Interactive dashboard for visualization with time range selection
- Command-line summary tool for quick checks

## Quick Start

1. Copy `dot-env-example` to `.env` and customize for your environment:
   ```
   cp dot-env-example .env
   ```

2. Edit the `.env` file to configure your filesystems:
   ```
   # Filesystem configuration
   FILESYSTEM_PATHS={{HOME}},/tmp
   FILESYSTEM_LABELS=home,tmpfs
   
   # Database location
   DISK_STATS_DB={{HOME}}/hpc-disk-monitor/data/disk_stats.db
   ```

3. Create the database directory:
   ```
   mkdir -p ~/hpc-disk-monitor/data
   ```

4. Run the collector to gather metrics:
   ```
   python3 scripts/disk_metrics_collector.py --verbose
   ```

5. View the summary report:
   ```
   python3 scripts/db_summary.py
   ```

6. Launch the dashboard:
   ```
   python3 scripts/monitor_disk_metrics.py
   ```

## Scripts

- `disk_metrics_collector.py` - Collects disk I/O metrics and stores them in the database
- `db_summary.py` - Displays a summary of the latest metrics for all filesystems
- `monitor_disk_metrics.py` - Interactive dashboard for visualizing metrics
- `manage_cron.py` - Sets up scheduled data collection (via cron or launchd)
- `init_db.py` - Initializes the database
- `delete_db.py` - Deletes the database
- `export_to_csv.py` - Exports database contents to CSV files

## Setting Up Scheduled Collection

To set up automatic data collection every 5 minutes:

```
python3 scripts/manage_cron.py --install
```

## Credits

Written with the help of Claude AI and reviewed by humans.
