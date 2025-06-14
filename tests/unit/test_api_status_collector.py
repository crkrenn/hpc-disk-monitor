#!/usr/bin/env python3
import unittest
from unittest.mock import patch, MagicMock, Mock, call
import sqlite3
import time
import statistics
from datetime import datetime, timedelta
from pathlib import Path
import requests

# Add parent directory to path so we can import modules
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

# We need to mock the argparse result before importing the module
with patch('argparse.ArgumentParser.parse_args') as mock_args:
    mock_args.return_value = MagicMock(verbose=False)
    # Import the module after mocking argparse
    from scripts import api_status_collector


class TestApiStatusCollector(unittest.TestCase):
    
    def setUp(self):
        # Reset mocks between tests
        api_status_collector.args.verbose = False
        # Save original config and restore in tearDown
        self.original_api_config = api_status_collector.API_CONFIG
    
    def tearDown(self):
        # Restore original config
        api_status_collector.API_CONFIG = self.original_api_config
    
    def test_current_timestamp(self):
        # Test timestamp format
        timestamp = api_status_collector.current_timestamp()
        # Should be in format "YYYY-MM-DD HH:MM"
        try:
            datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
            is_valid = True
        except ValueError:
            is_valid = False
        
        self.assertTrue(is_valid)
    
    @patch('scripts.api_status_collector.time.time')
    @patch('scripts.api_status_collector.requests.get')
    def test_test_api_endpoint_success(self, mock_get, mock_time):
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        # Mock time for response calculation
        mock_time.side_effect = [0.0, 0.1]  # start_time, end_time
        
        # Test successful API call
        result = api_status_collector.test_api_endpoint("https://api.example.com/health")
        
        self.assertEqual(result["response_time_ms"], 100.0)  # 0.1 seconds = 100ms
        self.assertEqual(result["status_code"], 200)
        self.assertTrue(result["success"])
        self.assertIsNone(result["error_message"])
        
        mock_get.assert_called_once_with("https://api.example.com/health", timeout=30)
    
    @patch('scripts.api_status_collector.time.time')
    @patch('scripts.api_status_collector.requests.get')
    def test_test_api_endpoint_client_error(self, mock_get, mock_time):
        # Mock client error response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        # Mock time for response calculation
        mock_time.side_effect = [0.0, 0.05]  # start_time, end_time
        
        # Test client error API call
        result = api_status_collector.test_api_endpoint("https://api.example.com/notfound")
        
        self.assertEqual(result["response_time_ms"], 50.0)  # 0.05 seconds = 50ms
        self.assertEqual(result["status_code"], 404)
        self.assertFalse(result["success"])
        self.assertEqual(result["error_message"], "HTTP 404")
    
    @patch('scripts.api_status_collector.time.time')
    @patch('scripts.api_status_collector.requests.get')
    def test_test_api_endpoint_timeout(self, mock_get, mock_time):
        # Mock timeout exception
        mock_get.side_effect = requests.exceptions.Timeout()
        
        # Mock time for timeout detection
        mock_time.side_effect = [0.0, 30.0]  # start_time, timeout_time
        
        # Test timeout
        result = api_status_collector.test_api_endpoint("https://api.example.com/slow", timeout=30)
        
        self.assertEqual(result["response_time_ms"], 30000.0)  # 30 seconds = 30000ms
        self.assertEqual(result["status_code"], 0)
        self.assertFalse(result["success"])
        self.assertEqual(result["error_message"], "Request timeout")
    
    @patch('scripts.api_status_collector.time.time')
    @patch('scripts.api_status_collector.requests.get')
    def test_test_api_endpoint_connection_error(self, mock_get, mock_time):
        # Mock connection error
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")
        
        # Mock time for response calculation
        mock_time.side_effect = [0.0, 0.02]  # start_time, error_time
        
        # Test connection error
        result = api_status_collector.test_api_endpoint("https://nonexistent.api.com/health")
        
        self.assertEqual(result["response_time_ms"], 20.0)  # 0.02 seconds = 20ms
        self.assertEqual(result["status_code"], 0)
        self.assertFalse(result["success"])
        self.assertTrue(result["error_message"].startswith("Connection error:"))
    
    @patch('scripts.api_status_collector.time.time')
    @patch('scripts.api_status_collector.requests.get')
    def test_test_api_endpoint_generic_exception(self, mock_get, mock_time):
        # Mock generic exception
        mock_get.side_effect = Exception("Something went wrong")
        
        # Mock time for response calculation
        mock_time.side_effect = [0.0, 0.01]  # start_time, error_time
        
        # Test generic exception
        result = api_status_collector.test_api_endpoint("https://api.example.com/health")
        
        self.assertEqual(result["response_time_ms"], 10.0)  # 0.01 seconds = 10ms
        self.assertEqual(result["status_code"], 0)
        self.assertFalse(result["success"])
        self.assertTrue(result["error_message"].startswith("Unexpected error:"))
    
    @patch('scripts.api_status_collector.connect_db')
    @patch('scripts.api_status_collector.create_tables')
    def test_init_db(self, mock_create_tables, mock_connect_db):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock tables and columns for verbose output
        mock_cursor.fetchall.side_effect = [
            [('api_stats',), ('api_stats_summary',)],  # tables
            [(0, 'timestamp', 'TEXT', 0, None, 0), (1, 'hostname', 'TEXT', 0, None, 0)],  # columns for first table
            [(0, 'timestamp', 'TEXT', 0, None, 0), (1, 'api_name', 'TEXT', 0, None, 0)]   # columns for second table
        ]
        
        # Test non-verbose mode
        result = api_status_collector.init_db()
        
        # Verify database functions were called
        mock_connect_db.assert_called_once()
        mock_create_tables.assert_called_once_with(mock_conn)
        self.assertTrue(result)
        
        # Test verbose mode
        mock_connect_db.reset_mock()
        mock_create_tables.reset_mock()
        api_status_collector.args.verbose = True
        
        result = api_status_collector.init_db()
        
        mock_connect_db.assert_called_once()
        mock_create_tables.assert_called_once_with(mock_conn)
        self.assertTrue(result)
        self.assertTrue(mock_cursor.execute.called)
    
    @patch('scripts.api_status_collector.connect_db')
    def test_insert_api_record(self, mock_connect_db):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Test record
        record = {
            "timestamp": "2023-01-01 12:00",
            "api_name": "Test API",
            "endpoint_url": "https://api.example.com/health",
            "response_time_ms": 150.5,
            "status_code": 200,
            "success": True,
            "error_message": None
        }
        
        # Call the function
        result = api_status_collector.insert_api_record(record)
        
        # Verify database operations
        mock_connect_db.assert_called_once()
        mock_cursor.execute.assert_called_once()
        self.assertTrue(result)
        
        # Check SQL and parameters
        sql = mock_cursor.execute.call_args[0][0]
        params = mock_cursor.execute.call_args[0][1]
        
        self.assertIn("INSERT INTO api_stats", sql)
        self.assertEqual(params[0], record["timestamp"])
        self.assertEqual(params[1], api_status_collector.HOSTNAME)
        self.assertEqual(params[2], record["api_name"])
        self.assertEqual(params[3], record["endpoint_url"])
        self.assertEqual(params[4], record["response_time_ms"])
        self.assertEqual(params[5], record["status_code"])
        self.assertEqual(params[6], record["success"])
        self.assertEqual(params[7], record["error_message"])
    
    @patch('scripts.api_status_collector.current_timestamp')
    @patch('scripts.api_status_collector.connect_db')
    def test_insert_api_summary_stats(self, mock_connect_db, mock_timestamp):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock timestamp
        mock_timestamp.return_value = "2023-01-01 12:00"
        
        # Test data
        api_name = "Test API"
        summary = {
            "response_time_ms": {
                "min": 100.0, 
                "max": 200.0, 
                "avg": 150.0, 
                "stddev": 25.0,
                "success_rate": 0.95
            }
        }
        
        # Call the function
        result = api_status_collector.insert_api_summary_stats(api_name, summary)
        
        # Verify database operations
        mock_connect_db.assert_called_once()
        mock_cursor.execute.assert_called_once()
        self.assertTrue(result)
        
        # Check first call (response_time_ms)
        sql = mock_cursor.execute.call_args[0][0]
        params = mock_cursor.execute.call_args[0][1]
        
        self.assertIn("INSERT INTO api_stats_summary", sql)
        self.assertEqual(params[0], "2023-01-01 12:00")  # timestamp
        self.assertEqual(params[1], api_status_collector.HOSTNAME)  # hostname
        self.assertEqual(params[2], "Test API")  # api_name
        self.assertEqual(params[3], "response_time_ms")  # metric
        self.assertEqual(params[4], 150.0)  # avg
        self.assertEqual(params[5], 100.0)  # min
        self.assertEqual(params[6], 200.0)  # max
        self.assertEqual(params[7], 25.0)  # stddev
        self.assertEqual(params[8], 0.95)  # success_rate
    
    @patch('scripts.api_status_collector.connect_db')
    def test_compute_and_store_api_summary(self, mock_connect_db):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock query results - 3 rows: 2 successful, 1 failed
        # Format: (response_time_ms, success, status_code)
        mock_cursor.fetchall.return_value = [
            (100.0, True, 200),
            (150.0, True, 200),
            (500.0, False, 500)
        ]
        
        # Test with non-verbose mode
        api_name = "Test API"
        
        # Patch insert_api_summary_stats to verify it's called with correct data
        with patch('scripts.api_status_collector.insert_api_summary_stats') as mock_insert:
            result = api_status_collector.compute_and_store_api_summary(api_name)
            
            # Verify database query
            mock_connect_db.assert_called_once()
            mock_cursor.execute.assert_called_once()
            self.assertTrue(result)
            
            # Verify insert_api_summary_stats was called with correct data
            mock_insert.assert_called_once()
            call_api_name, call_summary = mock_insert.call_args[0]
            
            self.assertEqual(call_api_name, api_name)
            self.assertIn("response_time_ms", call_summary)
            self.assertIn("status_code", call_summary)
            
            # Check response_time_ms stats
            self.assertEqual(call_summary["response_time_ms"]["min"], 100.0)
            self.assertEqual(call_summary["response_time_ms"]["max"], 500.0)
            self.assertEqual(call_summary["response_time_ms"]["avg"], 250.0)  # (100+150+500)/3
            self.assertAlmostEqual(call_summary["response_time_ms"]["success_rate"], 2/3)  # 2 successes out of 3
            
            # Check status_code stats
            self.assertEqual(call_summary["status_code"]["min"], 200)
            self.assertEqual(call_summary["status_code"]["max"], 500)
            self.assertEqual(call_summary["status_code"]["avg"], (200+200+500)/3)  # (200+200+500)/3
            self.assertAlmostEqual(call_summary["status_code"]["success_rate"], 2/3)  # 2 successes out of 3
        
        # Test verbose mode
        mock_connect_db.reset_mock()
        api_status_collector.args.verbose = True
        
        # Patch print function to verify verbose output
        with patch('builtins.print') as mock_print:
            with patch('scripts.api_status_collector.insert_api_summary_stats'):
                api_status_collector.compute_and_store_api_summary(api_name)
                
                # Verify verbose output includes success rate
                mock_print.assert_any_call(f"Found 3 API data points for {api_name} in the last hour")
                # Check that success rate is printed
                success_rate_calls = [call for call in mock_print.call_args_list 
                                    if "Success rate:" in str(call)]
                self.assertTrue(len(success_rate_calls) > 0)
    
    @patch('scripts.api_status_collector.test_api_endpoint')
    @patch('scripts.api_status_collector.insert_api_record')
    @patch('scripts.api_status_collector.compute_and_store_api_summary')
    @patch('scripts.api_status_collector.current_timestamp')
    def test_run_once_and_record(self, mock_timestamp, mock_compute, mock_insert, mock_test_api):
        # Mock timestamp
        mock_timestamp.return_value = "2023-01-01 12:00"
        
        # Mock API test results
        mock_test_api.side_effect = [
            # First API
            {"response_time_ms": 120.0, "status_code": 200, "success": True, "error_message": None},
            # Second API (with error)
            {"response_time_ms": 30000.0, "status_code": 0, "success": False, "error_message": "Request timeout"},
        ]
        
        # Mock insert and compute functions to return success
        mock_insert.return_value = True
        mock_compute.return_value = True
        
        # Set up test API config
        api_status_collector.API_CONFIG = {
            "https://api.example.com/health": "Test API 1",
            "https://slow.api.com/health": "Test API 2"
        }
        
        # Test non-verbose mode
        result = api_status_collector.run_once_and_record()
        
        # Verify API tests were run
        self.assertEqual(mock_test_api.call_count, 2)
        mock_test_api.assert_has_calls([
            call("https://api.example.com/health"),
            call("https://slow.api.com/health")
        ])
        
        # Verify both results were stored (success and failure)
        self.assertEqual(mock_insert.call_count, 2)
        
        # Check first successful record
        first_record = mock_insert.call_args_list[0][0][0]
        self.assertEqual(first_record["api_name"], "Test API 1")
        self.assertEqual(first_record["response_time_ms"], 120.0)
        self.assertTrue(first_record["success"])
        
        # Check second failed record
        second_record = mock_insert.call_args_list[1][0][0]
        self.assertEqual(second_record["api_name"], "Test API 2")
        self.assertEqual(second_record["response_time_ms"], 30000.0)
        self.assertFalse(second_record["success"])
        
        # Verify summary computation for both APIs
        self.assertEqual(mock_compute.call_count, 2)
        mock_compute.assert_any_call("Test API 1")
        mock_compute.assert_any_call("Test API 2")
        
        self.assertTrue(result)
        
        # Test verbose mode
        mock_test_api.reset_mock()
        mock_insert.reset_mock()
        mock_compute.reset_mock()
        
        # Reset test API results
        mock_test_api.side_effect = [
            {"response_time_ms": 120.0, "status_code": 200, "success": True, "error_message": None},
            {"response_time_ms": 30000.0, "status_code": 0, "success": False, "error_message": "Request timeout"},
        ]
        
        api_status_collector.args.verbose = True
        
        # Patch print function to verify verbose output
        with patch('builtins.print') as mock_print:
            api_status_collector.run_once_and_record()
            
            # Verify verbose output
            mock_print.assert_any_call("Testing Test API 1 (https://api.example.com/health)...")
            mock_print.assert_any_call("Testing Test API 2 (https://slow.api.com/health)...")
            
            # Check for success/failure indicators
            success_calls = [call for call in mock_print.call_args_list 
                           if "✅ SUCCESS" in str(call)]
            failure_calls = [call for call in mock_print.call_args_list 
                           if "❌ FAILED" in str(call)]
            
            self.assertTrue(len(success_calls) > 0)
            self.assertTrue(len(failure_calls) > 0)
    
    @patch('scripts.api_status_collector.test_api_endpoint')
    @patch('scripts.api_status_collector.current_timestamp')
    def test_run_once_and_record_no_apis(self, mock_timestamp, mock_test_api):
        # Mock timestamp
        mock_timestamp.return_value = "2023-01-01 12:00"
        
        # Set up empty API config
        api_status_collector.API_CONFIG = {}
        
        # Test with no APIs configured
        result = api_status_collector.run_once_and_record()
        
        # Verify no API tests were run
        mock_test_api.assert_not_called()
        
        # Should return True (not an error condition)
        self.assertTrue(result)
        
        # Test verbose mode
        api_status_collector.args.verbose = True
        
        # Patch print function to verify verbose output
        with patch('builtins.print') as mock_print:
            api_status_collector.run_once_and_record()
            
            # Verify verbose output about no APIs
            mock_print.assert_any_call("No API endpoints configured. Please set API_ENDPOINTS and optionally API_NAMES.")


if __name__ == '__main__':
    unittest.main()