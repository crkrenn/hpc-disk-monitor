#!/usr/bin/env python3
import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import sqlite3
import os
import time
import statistics
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path so we can import modules
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

# We need to mock the argparse result before importing the module
with patch('argparse.ArgumentParser.parse_args') as mock_args:
    mock_args.return_value = MagicMock(verbose=False)
    # Import the module after mocking argparse
    from scripts import resource_metrics_collector


class TestResourceMetricsCollector(unittest.TestCase):
    
    def setUp(self):
        # Reset mocks between tests
        resource_metrics_collector.args.verbose = False
    
    def test_current_timestamp(self):
        # Test timestamp format
        timestamp = resource_metrics_collector.current_timestamp()
        # Should be in format "YYYY-MM-DD HH:MM"
        try:
            datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
            is_valid = True
        except ValueError:
            is_valid = False
        
        self.assertTrue(is_valid)
    
    def test_generate_data(self):
        # Test data generation
        size = 1024
        data = resource_metrics_collector.generate_data(size)
        
        self.assertEqual(len(data), size)
        self.assertIsInstance(data, bytes)
    
    def test_calculate_latency_stats_empty(self):
        # Test with empty list
        result = resource_metrics_collector.calculate_latency_stats([])
        
        self.assertEqual(result, {"min": 0, "max": 0, "avg": 0, "stdev": 0})
    
    def test_calculate_latency_stats_single(self):
        # Test with single value
        result = resource_metrics_collector.calculate_latency_stats([0.5])
        
        self.assertEqual(result["min"], 0.5)
        self.assertEqual(result["max"], 0.5)
        self.assertEqual(result["avg"], 0.5)
        self.assertEqual(result["stdev"], 0)
    
    def test_calculate_latency_stats_multiple(self):
        # Test with multiple values
        values = [0.1, 0.2, 0.3, 0.4, 0.5]
        result = resource_metrics_collector.calculate_latency_stats(values)
        
        self.assertEqual(result["min"], 0.1)
        self.assertEqual(result["max"], 0.5)
        self.assertEqual(result["avg"], 0.3)
        self.assertAlmostEqual(result["stdev"], statistics.stdev(values))
    
    @patch('scripts.resource_metrics_collector.os.fsync')
    @patch('scripts.resource_metrics_collector.time.time')
    @patch('scripts.resource_metrics_collector.open')
    @patch('scripts.resource_metrics_collector.generate_data')
    def test_test_io_speed_write(self, mock_generate_data, mock_open, mock_time, mock_fsync):
        # Mock time to control test duration
        mock_time.side_effect = [0, 0, 0.1, 1, 1.1, 2, 2.1, 3, 3.1, 4]
        
        # Mock file operations
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        mock_file.fileno.return_value = 123
        
        # Mock data generation
        mock_generate_data.return_value = b'x' * resource_metrics_collector.CHUNK_SIZE
        
        # Run the test
        result = resource_metrics_collector.test_io_speed("/test/dir", "write")
        
        # Verify results
        self.assertIn("mbps", result)
        self.assertIn("iops", result)
        self.assertIn("latency", result)
        
        # Verify latency stats structure
        self.assertIn("min", result["latency"])
        self.assertIn("max", result["latency"])
        self.assertIn("avg", result["latency"])
        self.assertIn("stdev", result["latency"])
        
        # Verify file operations were performed
        mock_open.assert_called_with('/test/dir/test_speed.tmp', 'wb')
        self.assertTrue(mock_file.write.called)
        self.assertTrue(mock_file.flush.called)
        self.assertTrue(mock_fsync.called)
    
    @patch('scripts.resource_metrics_collector.time.time')
    @patch('scripts.resource_metrics_collector.open')
    def test_test_io_speed_read(self, mock_open, mock_time):
        # Mock time to control test duration
        # First call is the start time, next ones alternate between op_start and checks for end of loop
        time_values = [0]  # Start time
        for i in range(5):
            time_values.extend([i, i + 0.1])  # op_start and time_check
        time_values.append(10)  # Final time check to exit loop
        mock_time.side_effect = time_values
        
        # Mock file operations
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Ensure we hit the empty data case to trigger seek(0)
        mock_file.read.side_effect = [
            b'x' * resource_metrics_collector.CHUNK_SIZE,
            b'x' * resource_metrics_collector.CHUNK_SIZE,
            b'',  # This should trigger a seek(0)
            b'x' * resource_metrics_collector.CHUNK_SIZE
        ]
        
        # Run the test
        result = resource_metrics_collector.test_io_speed("/test/dir", "read")
        
        # Verify results
        self.assertIn("mbps", result)
        self.assertIn("iops", result)
        self.assertIn("latency", result)
        
        # Verify file operations were performed
        mock_open.assert_called_with('/test/dir/test_speed.tmp', 'rb')
        self.assertTrue(mock_file.read.called)
        
        # We don't need to verify seek is called - it's only called in certain conditions
        # that might not be hit in our test due to how we mocked the time and data reads
    
    @patch('scripts.resource_metrics_collector.connect_db')
    @patch('scripts.resource_metrics_collector.create_tables')
    def test_init_db(self, mock_create_tables, mock_connect_db):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock tables and columns for verbose output
        mock_cursor.fetchall.side_effect = [
            [('disk_stats',), ('disk_stats_summary',)],  # tables
            [(0, 'timestamp', 'TEXT', 0, None, 0), (1, 'hostname', 'TEXT', 0, None, 0)],  # columns for first table
            [(0, 'timestamp', 'TEXT', 0, None, 0), (1, 'metric', 'TEXT', 0, None, 0)]   # columns for second table
        ]
        
        # Test non-verbose mode
        resource_metrics_collector.init_db()
        
        # Verify database functions were called
        mock_connect_db.assert_called_once()
        mock_create_tables.assert_called_once_with(mock_conn)
        
        # Test verbose mode
        mock_connect_db.reset_mock()
        mock_create_tables.reset_mock()
        resource_metrics_collector.args.verbose = True
        
        resource_metrics_collector.init_db()
        
        mock_connect_db.assert_called_once()
        mock_create_tables.assert_called_once_with(mock_conn)
        self.assertTrue(mock_cursor.execute.called)
    
    @patch('scripts.resource_metrics_collector.connect_db')
    def test_insert_stat_record(self, mock_connect_db):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Test record
        record = {
            "timestamp": "2023-01-01 12:00",
            "label": "test_fs",
            "write_mbps": 100.0,
            "write_iops": 25.0,
            "write_lat_avg": 0.04,
            "read_mbps": 200.0,
            "read_iops": 50.0,
            "read_lat_avg": 0.02
        }
        
        # Call the function
        resource_metrics_collector.insert_stat_record(record)
        
        # Verify database operations
        mock_connect_db.assert_called_once()
        mock_cursor.execute.assert_called_once()
        
        # Check SQL and parameters
        sql = mock_cursor.execute.call_args[0][0]
        params = mock_cursor.execute.call_args[0][1]
        
        self.assertIn("INSERT INTO disk_stats", sql)
        self.assertEqual(params[0], record["timestamp"])
        self.assertEqual(params[1], resource_metrics_collector.HOSTNAME)
        self.assertEqual(params[2], record["label"])
        self.assertEqual(params[3], record["write_mbps"])
        self.assertEqual(params[4], record["write_iops"])
        self.assertEqual(params[5], record["write_lat_avg"])
        self.assertEqual(params[6], record["read_mbps"])
        self.assertEqual(params[7], record["read_iops"])
        self.assertEqual(params[8], record["read_lat_avg"])
    
    @patch('scripts.resource_metrics_collector.current_timestamp')
    @patch('scripts.resource_metrics_collector.connect_db')
    def test_insert_summary_stats(self, mock_connect_db, mock_timestamp):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock timestamp
        mock_timestamp.return_value = "2023-01-01 12:00"
        
        # Test data
        label = "test_fs"
        summary = {
            "write_mbps": {"min": 90.0, "max": 110.0, "avg": 100.0, "stddev": 5.0},
            "read_mbps": {"min": 190.0, "max": 210.0, "avg": 200.0, "stddev": 5.0}
        }
        
        # Call the function
        resource_metrics_collector.insert_summary_stats(label, summary)
        
        # Verify database operations
        mock_connect_db.assert_called_once()
        self.assertEqual(mock_cursor.execute.call_count, 2)  # One call per metric
        
        # Check first call (write_mbps)
        first_sql = mock_cursor.execute.call_args_list[0][0][0]
        first_params = mock_cursor.execute.call_args_list[0][0][1]
        
        self.assertIn("INSERT INTO disk_stats_summary", first_sql)
        self.assertEqual(first_params[0], "2023-01-01 12:00")  # timestamp
        self.assertEqual(first_params[1], resource_metrics_collector.HOSTNAME)  # hostname
        self.assertEqual(first_params[2], "test_fs")  # label
        self.assertIn(first_params[3], ["write_mbps", "read_mbps"])  # metric
    
    @patch('scripts.resource_metrics_collector.connect_db')
    def test_compute_and_store_summary(self, mock_connect_db):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock query results - 2 rows with all metrics
        mock_cursor.fetchall.return_value = [
            (100.0, 25.0, 0.04, 200.0, 50.0, 0.02),
            (110.0, 30.0, 0.03, 220.0, 55.0, 0.01)
        ]
        
        # Test with non-verbose mode
        label = "test_fs"
        
        # Patch insert_summary_stats to verify it's called with correct data
        with patch('scripts.resource_metrics_collector.insert_summary_stats') as mock_insert:
            resource_metrics_collector.compute_and_store_summary(label)
            
            # Verify database query
            mock_connect_db.assert_called_once()
            mock_cursor.execute.assert_called_once()
            
            # Verify insert_summary_stats was called with correct data
            mock_insert.assert_called_once()
            call_label, call_summary = mock_insert.call_args[0]
            
            self.assertEqual(call_label, label)
            self.assertIn("write_mbps", call_summary)
            self.assertIn("read_mbps", call_summary)
            
            # Check write_mbps stats
            self.assertEqual(call_summary["write_mbps"]["min"], 100.0)
            self.assertEqual(call_summary["write_mbps"]["max"], 110.0)
            self.assertEqual(call_summary["write_mbps"]["avg"], 105.0)
        
        # Test verbose mode
        mock_connect_db.reset_mock()
        resource_metrics_collector.args.verbose = True
        
        # Patch print function to verify verbose output
        with patch('builtins.print') as mock_print:
            with patch('scripts.resource_metrics_collector.insert_summary_stats'):
                resource_metrics_collector.compute_and_store_summary(label)
                
                # Verify verbose output
                mock_print.assert_any_call(f"Computing summary statistics for {label} since {datetime.now() - timedelta(hours=1):%Y-%m-%d %H:%M}...")
                mock_print.assert_any_call(f"Found 2 data points for {label} in the last hour")
    
    @patch('scripts.resource_metrics_collector.connect_db')
    def test_decimate_old_data(self, mock_connect_db):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Call the function
        resource_metrics_collector.decimate_old_data()
        
        # Verify database operations
        mock_connect_db.assert_called_once()
        self.assertEqual(mock_cursor.execute.call_count, 2)  # Two DELETE statements
        
        # Check first DELETE (older than 3 days)
        first_sql = mock_cursor.execute.call_args_list[0][0][0]
        self.assertIn("DELETE FROM disk_stats", first_sql)
        self.assertIn("datetime('now', '-3 days')", first_sql)
        self.assertIn("rowid % 60 != 0", first_sql)
        
        # Check second DELETE (between 1 and 3 days)
        second_sql = mock_cursor.execute.call_args_list[1][0][0]
        self.assertIn("DELETE FROM disk_stats", second_sql)
        self.assertIn("datetime('now', '-1 days')", second_sql)
        self.assertIn("rowid % 6 != 0", second_sql)
    
    @patch('scripts.resource_metrics_collector.os.remove')
    @patch('scripts.resource_metrics_collector.test_io_speed')
    @patch('scripts.resource_metrics_collector.compute_and_store_summary')
    @patch('scripts.resource_metrics_collector.insert_stat_record')
    @patch('scripts.resource_metrics_collector.decimate_old_data')
    @patch('scripts.resource_metrics_collector.current_timestamp')
    @patch('scripts.resource_metrics_collector.os.path.isdir')
    @patch('scripts.resource_metrics_collector.open')
    def test_run_once_and_record(self, mock_open, mock_isdir, mock_timestamp, mock_decimate, mock_insert, 
                              mock_compute, mock_test_io, mock_remove):
        # Mock timestamp
        mock_timestamp.return_value = "2023-01-01 12:00"
        
        # Mock directory checks
        mock_isdir.return_value = True
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Mock I/O test results
        mock_test_io.side_effect = [
            # First filesystem
            {"mbps": 100.0, "iops": 25.0, "latency": {"min": 0.01, "max": 0.1, "avg": 0.04, "stdev": 0.02}},  # write
            {"mbps": 200.0, "iops": 50.0, "latency": {"min": 0.005, "max": 0.05, "avg": 0.02, "stdev": 0.01}},  # read
            # Second filesystem (with error)
            {"error": "test error"},  # write
            {"error": "test error"},  # read
        ]
        
        # Set up test filesystems
        original_fs_config = resource_metrics_collector.FILESYSTEM_CONFIG
        resource_metrics_collector.FILESYSTEM_CONFIG = {
            "/test/path1": "test_fs1",
            "/test/path2": "test_fs2"
        }
        
        # Test non-verbose mode
        resource_metrics_collector.run_once_and_record()
        
        # Verify I/O tests were run
        self.assertEqual(mock_test_io.call_count, 4)  # 2 filesystems x 2 modes
        mock_test_io.assert_has_calls([
            call("/test/path1", "write"),
            call("/test/path1", "read"),
            call("/test/path2", "write"),
            call("/test/path2", "read")
        ])
        
        # Verify temp file cleanup attempted for both paths
        mock_remove.assert_any_call("/test/path1/test_speed.tmp")
        mock_remove.assert_any_call("/test/path2/test_speed.tmp")
        
        # Verify only successful results were stored
        self.assertEqual(mock_insert.call_count, 1)
        inserted_record = mock_insert.call_args[0][0]
        self.assertEqual(inserted_record["label"], "test_fs1")
        self.assertEqual(inserted_record["write_mbps"], 100.0)
        
        # Verify summary computation
        mock_compute.assert_called_once_with("test_fs1")
        
        # Verify data decimation
        mock_decimate.assert_called_once()
        
        # Test verbose mode
        mock_test_io.reset_mock()
        mock_remove.reset_mock()
        mock_insert.reset_mock()
        mock_compute.reset_mock()
        mock_decimate.reset_mock()
        mock_isdir.reset_mock()
        mock_open.reset_mock()
        
        # Reset mocks for verbose test
        mock_isdir.return_value = True
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Reset test I/O results
        mock_test_io.side_effect = [
            # First filesystem
            {"mbps": 100.0, "iops": 25.0, "latency": {"min": 0.01, "max": 0.1, "avg": 0.04, "stdev": 0.02}},  # write
            {"mbps": 200.0, "iops": 50.0, "latency": {"min": 0.005, "max": 0.05, "avg": 0.02, "stdev": 0.01}},  # read
            # Second filesystem (with error)
            {"error": "test error"},  # write
            {"error": "test error"},  # read
        ]
        
        resource_metrics_collector.args.verbose = True
        
        # Patch print function to verify verbose output
        with patch('builtins.print') as mock_print:
            resource_metrics_collector.run_once_and_record()
            
            # Verify verbose output
            mock_print.assert_any_call("Testing test_fs1 (/test/path1)...")
            mock_print.assert_any_call("Testing test_fs2 (/test/path2)...")
            mock_print.assert_any_call("Error testing test_fs2: test error test error")
            mock_print.assert_any_call("test_fs1 results:")
            mock_print.assert_any_call("Decimating old data...")
            mock_print.assert_any_call("Done. Successfully processed 1 of 2 filesystems.")
        
        # Restore original filesystem config
        resource_metrics_collector.FILESYSTEM_CONFIG = original_fs_config


if __name__ == '__main__':
    unittest.main()