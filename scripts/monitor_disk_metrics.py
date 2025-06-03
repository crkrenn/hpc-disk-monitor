#!/usr/bin/env python3
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objs as go
import sqlite3
import pandas as pd
import os
from pathlib import Path
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Import project modules
from common.env_utils import preprocess_env
from db.schema import DB_FILE, connect_db
import webbrowser
import threading
import time

# Initialize environment
preprocess_env()

# Get environment variables for dashboard settings
DASH_REFRESH_SECONDS = int(os.getenv("DASH_REFRESH_SECONDS", "5"))
DISK_SAMPLING_MINUTES = int(os.getenv("DISK_SAMPLING_MINUTES", "5"))

# Time range options for display
TIME_RANGES = {
    "1d": {"label": "Last 24 Hours", "days": 1},
    "1w": {"label": "Last Week", "days": 7},
    "1m": {"label": "Last Month", "days": 30},
    "1y": {"label": "Last Year", "days": 365},
    "max": {"label": "All Time", "days": None}
}

# Parse command line arguments
import argparse

parser = argparse.ArgumentParser(description='Run Disk Performance Monitor Dashboard')
parser.add_argument('--port', type=int, default=8050, help='Port to run the server on')
parser.add_argument('--host', type=str, default='127.0.0.1', help='Host to run the server on')
parser.add_argument('--debug', action='store_true', help='Run in debug mode')
parser.add_argument('--no-browser', action='store_true', help='Don\'t open browser automatically')
args = parser.parse_args()

# Initialize Dash app
app = Dash(__name__)
app.title = "Disk Performance Monitor"


# Load summary data from SQLite with optional time range filter
def fetch_summary_data(time_range_days=None):
    try:
        conn = connect_db()
        with conn:
            # Check if the table exists and has data
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='disk_stats_summary'")
            if not cursor.fetchone():
                print("Warning: disk_stats_summary table not found in database")
                return pd.DataFrame(columns=["timestamp", "hostname", "label", "metric", "avg", "min", "max", "stddev"])
            
            # Check if there's any data
            cursor.execute("SELECT COUNT(*) FROM disk_stats_summary")
            if cursor.fetchone()[0] == 0:
                print("Warning: No data found in disk_stats_summary table")
                return pd.DataFrame(columns=["timestamp", "hostname", "label", "metric", "avg", "min", "max", "stddev"])
            
            # If table exists and has data, fetch it with optional time filter
            if time_range_days is not None:
                # Apply time filter
                query = """
                    SELECT * FROM disk_stats_summary 
                    WHERE timestamp >= datetime('now', ?) 
                    ORDER BY timestamp
                """
                days_param = f"-{time_range_days} days"
                df = pd.read_sql_query(query, conn, params=(days_param,))
            else:
                # Get all data
                df = pd.read_sql_query("SELECT * FROM disk_stats_summary ORDER BY timestamp", conn)
            
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
        
    except (sqlite3.Error, FileNotFoundError) as e:
        print(f"Error accessing database: {e}")
        return pd.DataFrame(columns=["timestamp", "hostname", "label", "metric", "avg", "min", "max", "stddev"])


# Create a Plotly figure
def build_graph(df, metric, show_min, show_max, show_std, height=300):
    fig = go.Figure()
    
    # Handle empty dataframe case
    if df.empty:
        fig.update_layout(
            title=f"No data available for {metric.replace('_', ' ').title()}",
            annotations=[
                dict(
                    text="No data available. Please check that the collector script has been run.",
                    showarrow=False,
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5
                )
            ]
        )
        return fig
    
    # Process data for this metric
    try:
        # Convert column names to match the format we need
        metric_cols = [col for col in df.columns if col.endswith(('avg', 'min', 'max', 'stddev'))]
        
        if not metric_cols:
            fig.update_layout(
                title=f"No metrics found for {metric.replace('_', ' ').title()}",
                annotations=[dict(
                    text="No metrics available. Check database schema.",
                    showarrow=False,
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5
                )]
            )
            return fig
        
        # Reshape data if needed
        if f"{metric}_avg" not in df.columns and "metric" in df.columns:
            # Handle case where data is in long format with 'metric' column
            df = df[df["metric"] == metric].copy()
            if df.empty:
                fig.update_layout(
                    title=f"No data for {metric.replace('_', ' ').title()}",
                    annotations=[dict(
                        text=f"No data found for metric: {metric}",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5
                    )]
                )
                return fig
            
            # Rename columns for compatibility
            df = df.rename(columns={
                "avg": f"{metric}_avg",
                "min": f"{metric}_min",
                "max": f"{metric}_max",
                "stddev": f"{metric}_std"
            })
    
        # Plot data for each label
        labels = df["label"].unique()
        for label in labels:
            label_df = df[df["label"] == label]

            fig.add_trace(
                go.Scatter(
                    x=label_df["timestamp"],
                    y=label_df[f"{metric}_avg"],
                    mode="lines",
                    name=f"{label} avg",
                )
            )

            if show_max and f"{metric}_max" in label_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=label_df["timestamp"],
                        y=label_df[f"{metric}_max"],
                        mode="lines",
                        name=f"{label} max",
                        line=dict(dash="dash"),
                    )
                )
            if show_min and f"{metric}_min" in label_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=label_df["timestamp"],
                        y=label_df[f"{metric}_min"],
                        mode="lines",
                        name=f"{label} min",
                        line=dict(dash="dash"),
                    )
                )
            if show_std and f"{metric}_std" in label_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=label_df["timestamp"],
                        y=label_df[f"{metric}_avg"] + label_df[f"{metric}_std"],
                        mode="lines",
                        name=f"{label} avg +1σ",
                        line=dict(dash="dot"),
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=label_df["timestamp"],
                        y=label_df[f"{metric}_avg"] - label_df[f"{metric}_std"],
                        mode="lines",
                        name=f"{label} avg -1σ",
                        line=dict(dash="dot"),
                    )
                )
    except Exception as e:
        print(f"Error building graph: {e}")
        fig.update_layout(
            title=f"Error displaying {metric.replace('_', ' ').title()}",
            annotations=[dict(
                text=f"Error: {str(e)}",
                showarrow=False,
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5
            )]
        )
        return fig

    fig.update_layout(
        title=f"{metric.replace('_', ' ').title()} Over Time",
        xaxis_title="Time",
        yaxis_title=metric.replace('_', ' ').title(),
        hovermode="x unified",
        height=height,
        margin=dict(l=50, r=50, t=50, b=50)
    )
    return fig


# Define styles
HEADER_STYLE = {
    'text-align': 'center', 
    'margin': '20px 0', 
    'color': '#2c3e50'
}

CONTROL_STYLE = {
    'background': '#f8f9fa',
    'padding': '15px',
    'border-radius': '8px',
    'margin': '10px 0 25px 0',
    'box-shadow': '0 2px 5px rgba(0,0,0,0.1)'
}

SECTION_STYLE = {
    'margin-bottom': '30px', 
    'background': 'white', 
    'padding': '15px', 
    'border-radius': '8px',
    'box-shadow': '0 2px 5px rgba(0,0,0,0.05)'
}

# Dash layout
app.layout = html.Div(
    [
        # Header with title and last updated info
        html.Div([
            html.H1("Disk Performance Dashboard", style=HEADER_STYLE),
            html.Div(id="last-updated", style={'text-align': 'center', 'color': '#7f8c8d', 'margin-bottom': '10px'})
        ]),
        
        # Controls
        html.Div(
            [
                html.Div([
                    # First row of controls
                    html.Div([
                        html.Label("Time Range:", style={'font-weight': 'bold', 'margin-right': '10px'}),
                        dcc.Dropdown(
                            id="time-range-dropdown",
                            options=[
                                {"label": range_info["label"], "value": range_id}
                                for range_id, range_info in TIME_RANGES.items()
                            ],
                            value="1w",  # Default to last week
                            clearable=False,
                            style={'width': '180px'}
                        ),
                        
                        html.Label("Display Options:", style={'font-weight': 'bold', 'margin-left': '30px', 'margin-right': '10px'}),
                        dcc.Checklist(
                            id="detail-options",
                            options=[
                                {"label": "Show Min/Max", "value": "minmax"},
                                {"label": "Show ±1σ", "value": "std"},
                            ],
                            value=["minmax"],  # Default to showing min/max
                            inline=True,
                            labelStyle={'margin-right': '15px', 'font-size': '14px'}
                        ),
                    ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'})
                ])
            ],
            style=CONTROL_STYLE
        ),
        
        # System info
        html.Div(
            [
                html.Div([
                    html.Span("Disk metrics collected every: ", style={'font-weight': 'bold'}),
                    html.Span(f"{DISK_SAMPLING_MINUTES} minutes", style={'color': '#16a085'})
                ], style={'margin-right': '20px'}),
                
                html.Div([
                    html.Span("Dashboard refreshes every: ", style={'font-weight': 'bold'}),
                    html.Span(f"{DASH_REFRESH_SECONDS} seconds", style={'color': '#16a085'})
                ])
            ],
            style={'display': 'flex', 'justify-content': 'center', 'margin-bottom': '20px', 'font-size': '14px'}
        ),
        
        # Matrix layout for all graphs (3x2 grid)
        html.Div([
            # Row 1
            html.Div([
                # Column 1 - Write Throughput
                html.Div([
                    dcc.Graph(id="write-mbps-graph")
                ], style={'width': '33%', 'display': 'inline-block'}),
                
                # Column 2 - Write IOPS
                html.Div([
                    dcc.Graph(id="write-iops-graph")
                ], style={'width': '33%', 'display': 'inline-block'}),
                
                # Column 3 - Write Latency
                html.Div([
                    dcc.Graph(id="write-lat-graph")
                ], style={'width': '33%', 'display': 'inline-block'})
            ], style={'display': 'flex'}),
            
            # Row 2
            html.Div([
                # Column 1 - Read Throughput
                html.Div([
                    dcc.Graph(id="read-mbps-graph")
                ], style={'width': '33%', 'display': 'inline-block'}),
                
                # Column 2 - Read IOPS
                html.Div([
                    dcc.Graph(id="read-iops-graph")
                ], style={'width': '33%', 'display': 'inline-block'}),
                
                # Column 3 - Read Latency
                html.Div([
                    dcc.Graph(id="read-lat-graph")
                ], style={'width': '33%', 'display': 'inline-block'})
            ], style={'display': 'flex'})
        ], style={
            'background': 'white',
            'padding': '15px',
            'border-radius': '8px',
            'box-shadow': '0 2px 5px rgba(0,0,0,0.05)',
            'margin-bottom': '20px'
        }),
        
        # Footer
        html.Div(
            [
                html.P(f"Database path: {DB_FILE}", style={'color': '#7f8c8d', 'font-size': '12px', 'margin': '5px 0'}),
            ],
            style={'text-align': 'center', 'margin-top': '10px', 'padding': '10px', 'border-top': '1px solid #eee'}
        ),
        
        # Interval for auto-refresh
        dcc.Interval(
            id="refresh-interval",
            interval=DASH_REFRESH_SECONDS * 1000,  # Convert seconds to milliseconds
            n_intervals=0,
        ),
    ],
    style={
        'max-width': '1600px',  # Wider to accommodate 3-column layout
        'margin': '0 auto', 
        'padding': '20px',
        'font-family': 'Arial, sans-serif',
        'background': '#f5f6f7'
    }
)


# Separate function to generate a single graph
def generate_graph(df, metric, show_min_max, show_std, metric_settings):
    """Generate a single graph for a metric - separated for better error handling"""
    try:
        height = metric_settings.get(metric, {}).get("height", 300)
        fig = build_graph(df, metric, show_min_max, show_min_max, show_std, height=height)
        
        # Override title if provided in settings
        if metric in metric_settings and "title" in metric_settings[metric]:
            fig.update_layout(title=metric_settings[metric]["title"])
        
        return fig
    except Exception as e:
        print(f"Error generating {metric} graph: {e}")
        # Return a blank figure with error message
        fig = go.Figure()
        fig.update_layout(
            title=f"Error loading {metric} data",
            annotations=[dict(
                text=f"Error: Unable to display {metric} data. Please check logs.",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.5, y=0.5
            )],
            height=350
        )
        return fig

# Callback to update all graphs and last updated timestamp
@app.callback(
    [
        Output("write-mbps-graph", "figure"),
        Output("write-iops-graph", "figure"),
        Output("write-lat-graph", "figure"),
        Output("read-mbps-graph", "figure"),
        Output("read-iops-graph", "figure"),
        Output("read-lat-graph", "figure"),
        Output("last-updated", "children"),
    ],
    [
        Input("time-range-dropdown", "value"),
        Input("detail-options", "value"),
        Input("refresh-interval", "n_intervals"),
    ],
    prevent_initial_call=False,
)
def update_all_graphs(time_range=None, detail_opts=None, n_intervals=None):
    # Wrap everything in try-except to ensure we don't crash
    try:
        # Get current time for "last updated" display
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        last_updated_text = f"Last updated: {current_time}"
        
        # --- Handle time range ---
        # Default time range if none provided
        if time_range is None:
            time_range = "1w"
            print(f"Warning: No time range specified, using default: {time_range}")
            
        # Validate time_range - if invalid, use default "1w"
        if not isinstance(time_range, str) or time_range not in TIME_RANGES:
            time_range = "1w"
            print(f"Warning: Invalid time range specified, falling back to {time_range}")
        
        # Convert time range to days
        days = None
        if time_range != "max":
            days = TIME_RANGES[time_range]["days"]
        
        # --- Handle display options ---
        # Default display options if none provided
        if detail_opts is None or not isinstance(detail_opts, list):
            detail_opts = ["minmax"]
            print("Warning: Invalid detail options, using defaults")
            
        # Get display options
        show_min_max = "minmax" in detail_opts
        show_std = "std" in detail_opts
            
        # --- Fetch data ---
        try:
            df = fetch_summary_data(time_range_days=days)
        except Exception as e:
            print(f"Error fetching data: {e}")
            df = pd.DataFrame()  # Empty dataframe as fallback
        
        # --- Update status text ---
        try:
            range_label = TIME_RANGES[time_range]["label"]
            last_updated_text = f"Last updated: {current_time} | Showing: {range_label}"
        except:
            last_updated_text = f"Last updated: {current_time}"
        
        # --- Build all graphs ---
        metrics = [
            "write_mbps", "write_iops", "write_lat_avg",
            "read_mbps", "read_iops", "read_lat_avg"
        ]
        
        # Define metric-specific settings
        metric_settings = {
            "write_mbps": {"title": "Write Throughput (MB/s)", "height": 350},
            "write_iops": {"title": "Write IOPS", "height": 350},
            "write_lat_avg": {"title": "Write Latency (ms)", "height": 350},
            "read_mbps": {"title": "Read Throughput (MB/s)", "height": 350},
            "read_iops": {"title": "Read IOPS", "height": 350},
            "read_lat_avg": {"title": "Read Latency (ms)", "height": 350}
        }
        
        # Generate each graph with individual error handling
        figures = [
            generate_graph(df, metric, show_min_max, show_std, metric_settings)
            for metric in metrics
        ]
        
        # Return the tuple of figures and the last updated text
        return tuple(figures) + (last_updated_text,)
    
    except Exception as e:
        # Catch-all error handler to ensure the dashboard doesn't crash
        print(f"Critical error in update_all_graphs: {e}")
        
        # Generate blank figures for all outputs
        blank_figures = []
        for _ in range(6):
            fig = go.Figure()
            fig.update_layout(
                title="Error Loading Data",
                annotations=[dict(
                    text="An error occurred while updating the dashboard. Check server logs.",
                    showarrow=False, 
                    xref="paper", yref="paper",
                    x=0.5, y=0.5
                )],
                height=350
            )
            blank_figures.append(fig)
            
        error_text = f"Error: Dashboard update failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Return fallback values for all outputs
        return tuple(blank_figures) + (error_text,)


# Function to open browser after a short delay
def open_browser(host, port):
    """Open the browser after a short delay to ensure server is up"""
    time.sleep(1.5)  # Wait for server to start
    url = f"http://{host}:{port}"
    print(f"Opening browser at {url}")
    webbrowser.open(url)

# Run the app
if __name__ == "__main__":
    print(f"Starting Disk Performance Monitor on http://{args.host}:{args.port}")
    print(f"Database path: {DB_FILE}")
    
    # Open browser in a separate thread unless disabled
    if not args.no_browser:
        browser_thread = threading.Thread(
            target=open_browser,
            args=(args.host, args.port)
        )
        browser_thread.daemon = True
        browser_thread.start()
    
    # Start the server
    app.run(debug=args.debug, host=args.host, port=args.port)
