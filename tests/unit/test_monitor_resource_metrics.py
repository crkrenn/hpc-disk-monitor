#!/usr/bin/env python3
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
import os
import sys
from pathlib import Path

# Add parent directory to path so we can import modules
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

# Import the module after mocking argparse
with patch('argparse.ArgumentParser.parse_args') as mock_args:
    mock_args.return_value = MagicMock(port=8050, host='127.0.0.1', debug=False, no_browser=True)
    with patch.dict('os.environ', {"DASH_REFRESH_SECONDS": "10", "DISK_SAMPLING_MINUTES": "15"}):
        from scripts import monitor_resource_metrics


class TestMonitorResourceMetrics(unittest.TestCase):
    
    @patch('scripts.monitor_resource_metrics.connect_db')
    def test_fetch_summary_data_with_data(self, mock_connect_db):
        # Create mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock SQL query return
        mock_data = [
            ('2023-01-01 12:00', 'host1', 'fs1', 'write_mbps', 100.0, 90.0, 110.0, 5.0),
            ('2023-01-01 12:00', 'host1', 'fs1', 'read_mbps', 200.0, 190.0, 210.0, 5.0),
        ]
        mock_columns = ['timestamp', 'hostname', 'label', 'metric', 'avg', 'min', 'max', 'stddev']
        
        # Setup mock cursor responses
        mock_cursor.fetchone.side_effect = [(True,), (2,)]  # Table exists, has 2 rows
        
        # Setup pandas read_sql_query mock
        with patch('scripts.monitor_resource_metrics.pd.read_sql_query') as mock_read_sql:
            mock_df = pd.DataFrame(data=mock_data, columns=mock_columns)
            mock_read_sql.return_value = mock_df
            
            # Call the function
            result = monitor_resource_metrics.fetch_summary_data()
            
            # Check results
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 2)
            self.assertEqual(list(result.columns), mock_columns)
            
            # Verify timestamp was converted to datetime
            self.assertEqual(result['timestamp'].dtype, 'datetime64[ns]')
    
    @patch('scripts.monitor_resource_metrics.connect_db')
    def test_fetch_summary_data_empty_table(self, mock_connect_db):
        # Create mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Setup mock cursor responses for empty table
        mock_cursor.fetchone.side_effect = [(True,), (0,)]  # Table exists, but has 0 rows
        
        # Call the function with empty table
        result = monitor_resource_metrics.fetch_summary_data()
        
        # Check result is empty dataframe with expected columns
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)
        self.assertEqual(list(result.columns), 
                         ["timestamp", "hostname", "label", "metric", "avg", "min", "max", "stddev"])
    
    @patch('scripts.monitor_resource_metrics.connect_db')
    def test_fetch_summary_data_missing_table(self, mock_connect_db):
        # Create mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Setup mock cursor responses for missing table
        mock_cursor.fetchone.return_value = None  # Table doesn't exist
        
        # Call the function with missing table
        result = monitor_resource_metrics.fetch_summary_data()
        
        # Check result is empty dataframe with expected columns
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)
    
    def test_build_graph_with_data(self):
        # Create test data
        data = {
            'timestamp': pd.date_range(start='2023-01-01', periods=10),
            'hostname': ['host1'] * 10,
            'label': ['fs1'] * 10,
            'metric': ['write_mbps'] * 10,
            'write_mbps_avg': np.linspace(90, 110, 10),
            'write_mbps_min': np.linspace(80, 100, 10),
            'write_mbps_max': np.linspace(100, 120, 10),
            'write_mbps_std': [5.0] * 10
        }
        df = pd.DataFrame(data)
        
        # Call the function with different options
        fig_basic = monitor_resource_metrics.build_graph(df, 'write_mbps', False, False, False)
        fig_with_min_max = monitor_resource_metrics.build_graph(df, 'write_mbps', True, True, False)
        fig_with_std = monitor_resource_metrics.build_graph(df, 'write_mbps', False, False, True)
        
        # Check basic figure properties
        self.assertEqual(fig_basic.layout.title.text, "Write Mbps Over Time")
        self.assertEqual(fig_basic.layout.xaxis.title.text, "Time")
        self.assertEqual(fig_basic.layout.yaxis.title.text, "Write Mbps")
        
        # Check traces - basic should have 1 trace
        self.assertEqual(len(fig_basic.data), 1)
        
        # With min/max should have 3 traces
        self.assertEqual(len(fig_with_min_max.data), 3)
        
        # With stddev should have 3 traces
        self.assertEqual(len(fig_with_std.data), 3)
    
    def test_build_graph_with_empty_data(self):
        # Create empty dataframe
        df = pd.DataFrame(columns=[
            "timestamp", "hostname", "label", "metric", 
            "avg", "min", "max", "stddev"
        ])
        
        # Call the function
        fig = monitor_resource_metrics.build_graph(df, 'write_mbps', True, True, True)
        
        # Check that we get a figure with an annotation explaining no data
        self.assertTrue(fig.layout.annotations)
        self.assertIn("No data available", fig.layout.annotations[0].text)
        
        # Check title
        self.assertEqual(fig.layout.title.text, "No data available for Write Mbps")
    
    def test_generate_graph_with_error(self):
        """Test that generate_graph handles errors gracefully."""
        # Create empty dataframe
        df = pd.DataFrame(columns=["timestamp", "hostname", "label", "metric", "avg", "min", "max", "stddev"])
        
        # Mock the build_graph function to raise an exception
        with patch('scripts.monitor_resource_metrics.build_graph', side_effect=Exception("Test error")):
            # Call generate_graph
            fig = monitor_resource_metrics.generate_graph(df, "write_mbps", True, False, 
                                                    {"write_mbps": {"title": "Test", "height": 300}})
            
            # Should return an error figure, not raise an exception
            self.assertIsNotNone(fig)
            self.assertEqual(fig.layout.title.text, "Error loading write_mbps data")
            self.assertTrue(fig.layout.annotations)  # Should have an annotation explaining the error
    
    def test_callback_with_invalid_inputs(self):
        """Test the callback handles invalid inputs gracefully."""
        # Test with None values
        with patch('scripts.monitor_resource_metrics.fetch_summary_data') as mock_fetch:
            mock_fetch.return_value = pd.DataFrame()  # Return empty dataframe
            
            # Call with invalid time range
            result = monitor_resource_metrics.update_all_graphs("invalid_range", None, 1)
            
            # Should return 7 items (6 figures + 1 text element)
            self.assertEqual(len(result), 7)
            
            # All elements should be valid (not crash)
            for i in range(6):
                self.assertIsNotNone(result[i])  # Check each figure
            
            # Check last updated text 
            self.assertIn("Last updated", result[6])
            self.assertIn("Showing: Last Week", result[6])  # Should fall back to default
        
        # Test with empty detail_opts
        with patch('scripts.monitor_resource_metrics.fetch_summary_data') as mock_fetch:
            mock_fetch.return_value = pd.DataFrame()  # Return empty dataframe
            
            # Call with empty detail options
            result = monitor_resource_metrics.update_all_graphs("1d", [], 1)
            
            # Should still work without errors
            self.assertEqual(len(result), 7)
            
        # Test with severe exception that triggers the catch-all handler
        with patch('scripts.monitor_resource_metrics.fetch_summary_data') as mock_fetch:
            # Make it raise an exception when accessed
            mock_fetch.side_effect = Exception("Critical test error")
            
            # Also patch the generate_graph to ensure it also raises an error
            # This simulates a more severe error condition that should trigger the outer exception handler
            with patch('scripts.monitor_resource_metrics.generate_graph', side_effect=Exception("Critical graph error")):
                # The callback should handle this gracefully
                result = monitor_resource_metrics.update_all_graphs("1d", ["minmax"], 1)
                
                # Should return error figures
                self.assertEqual(len(result), 7)
                # The last item should be the error message
                self.assertIn("Error", result[6])


if __name__ == '__main__':
    unittest.main()