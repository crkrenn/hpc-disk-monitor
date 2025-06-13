# HPC Resource Monitor

A comprehensive system for monitoring resource performance metrics including disk I/O and API status across multiple systems. Designed for High-Performance Computing (HPC) environments, this tool collects, stores, and visualizes key performance metrics.

## Features

- **Disk I/O Monitoring**: Monitors multiple filesystems simultaneously
- **API Status Monitoring**: Monitors API endpoints for availability and response times
- Collects key metrics: read/write throughput (MB/s), IOPS, latency, and API response times
- Stores historical data with intelligent decimation for long-term storage
- Provides hourly summary statistics for each filesystem and API endpoint
- Interactive dashboard for visualization with time range selection
- Command-line summary tool for quick checks

## Quick Start

1. Copy `dot-env-example` to `.env` and customize for your environment:
   ```
   cp dot-env-example .env
   ```

2. Edit the `.env` file to configure your filesystems and APIs:
   ```
   # Filesystem configuration
   FILESYSTEM_PATHS={{HOME}},/tmp
   FILESYSTEM_LABELS=home,tmpfs
   
   # API configuration
   API_ENDPOINTS=https://api.example.com/health,https://services.hpc.local/status
   API_NAMES=Main API,HPC Services
   API_REQUEST_TIMEOUT=30
   
   # Database location
   RESOURCE_STATS_DB={{HOME}}/hpc-resource-monitor/data/resource_stats.db
   ```

3. Create the database directory:
   ```
   mkdir -p ~/hpc-resource-monitor/data
   ```

4. Run the collector to gather metrics:
   ```
   python3 scripts/resource_metrics_collector.py --verbose
   ```

5. View the summary report:
   ```
   python3 scripts/db_summary.py
   ```

6. Launch the dashboard:
   ```
   python3 scripts/monitor_resource_metrics.py
   ```

## Scripts

- `resource_metrics_collector.py` - Collects disk I/O metrics and stores them in the database
- `api_status_collector.py` - Collects API status metrics and stores them in the database  
- `db_summary.py` - Displays a summary of the latest metrics for all filesystems and APIs
- `monitor_resource_metrics.py` - Interactive dashboard for visualizing metrics
- `manage_cron.py` - Sets up scheduled data collection (via cron or launchd)
- `init_db.py` - Initializes the database
- `delete_db.py` - Deletes the database
- `export_to_csv.py` - Exports database contents to CSV files

## Setting Up Scheduled Collection

To set up automatic data collection every 5 minutes for both disk I/O and API status:

```
python3 scripts/manage_cron.py install
```

This will install:
- Disk metrics collector (every 5 minutes, configurable via `DISK_SAMPLING_MINUTES`)
- API status collector (every 5 minutes, configurable via `API_SAMPLING_MINUTES`)  
- Daily summary job (every 24 hours)

## Manual Collection

You can also run collectors manually:

```bash
# Collect disk metrics
python3 scripts/resource_metrics_collector.py --verbose

# Collect API status metrics
python3 scripts/api_status_collector.py --verbose
```

## Credits

Written with the help of Claude AI and reviewed by humans.
