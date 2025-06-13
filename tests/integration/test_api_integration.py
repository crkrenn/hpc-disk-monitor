#!/usr/bin/env python3
import unittest
import os
import sqlite3
import tempfile
import shutil
import time
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch
import http.server
import socketserver
import threading

# Add parent directory to path so we can import modules
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from db.schema import create_tables


class MockAPIHandler(http.server.BaseHTTPRequestHandler):
    """Mock API server for testing"""
    
    def do_GET(self):
        if '/health' in self.path:
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'status': 'healthy', 'timestamp': int(time.time())}
            self.wfile.write(json.dumps(response).encode())
        elif '/slow' in self.path:
            # Simulate a slow endpoint
            time.sleep(2)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'status': 'slow but healthy'}
            self.wfile.write(json.dumps(response).encode())
        elif '/error' in self.path:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'error': 'Internal server error'}
            self.wfile.write(json.dumps(response).encode())
        elif '/notfound' in self.path:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'error': 'Not found'}
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Suppress log messages during testing
        pass


class TestAPIIntegration(unittest.TestCase):
    
    def setUp(self):
        # Create temporary directories for testing
        self.temp_dir = tempfile.mkdtemp()
        
        # Create a temporary database
        self.db_path = os.path.join(self.temp_dir, "test_api_stats.db")
        self.conn = sqlite3.connect(self.db_path)
        create_tables(self.conn)
        
        # Start mock API server
        self.port = 8765  # Use a fixed port for testing
        self.server = socketserver.TCPServer(("localhost", self.port), MockAPIHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
        
        # Give the server a moment to start
        time.sleep(0.1)
    
    def tearDown(self):
        # Stop mock API server
        self.server.shutdown()
        self.server_thread.join(timeout=1)
        
        # Close database connection
        self.conn.close()
        
        # Clean up temporary directories
        shutil.rmtree(self.temp_dir)
    
    def test_end_to_end_api_collection(self):
        """Test that the API collector script can run and store data in the database."""
        # Set up environment variables for the test
        env = os.environ.copy()
        env["API_ENDPOINTS"] = f"http://localhost:{self.port}/health,http://localhost:{self.port}/slow"
        env["API_NAMES"] = "Health API,Slow API"
        env["API_REQUEST_TIMEOUT"] = "5"  # Set timeout to 5 seconds
        env["RESOURCE_STATS_DB"] = self.db_path
        
        # Run the API collector script
        script_path = os.path.join(
            Path(__file__).resolve().parent.parent.parent,
            "scripts",
            "api_status_collector.py"
        )
        
        result = subprocess.run(
            [sys.executable, script_path, "--verbose"],
            env=env,
            capture_output=True,
            text=True
        )
        
        # Check that the script executed successfully
        self.assertEqual(result.returncode, 0, 
                         f"Script failed with error: {result.stderr}")
        
        # Verify data was written to the database
        cursor = self.conn.cursor()
        
        # Check api_stats table
        cursor.execute("SELECT COUNT(*) FROM api_stats")
        count = cursor.fetchone()[0]
        self.assertGreater(count, 0, "No data was written to api_stats table")
        
        # Check api_stats_summary table
        cursor.execute("SELECT COUNT(*) FROM api_stats_summary")
        count = cursor.fetchone()[0]
        self.assertGreater(count, 0, "No data was written to api_stats_summary table")
        
        # Verify metrics were collected for Health API
        cursor.execute("""
            SELECT response_time_ms, status_code, success, error_message
            FROM api_stats 
            WHERE api_name = 'Health API'
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
        response_time, status_code, success, error_message = row
        
        # Health API should be successful
        self.assertGreater(response_time, 0)  # Should have some response time
        self.assertEqual(status_code, 200)
        self.assertTrue(success)
        self.assertIsNone(error_message)
        
        # Verify metrics were collected for Slow API
        cursor.execute("""
            SELECT response_time_ms, status_code, success, error_message
            FROM api_stats 
            WHERE api_name = 'Slow API'
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
        response_time, status_code, success, error_message = row
        
        # Slow API should also be successful but take longer
        self.assertGreater(response_time, 1000)  # Should take at least 1 second (1000ms)
        self.assertEqual(status_code, 200)
        self.assertTrue(success)
        self.assertIsNone(error_message)
    
    def test_api_error_handling(self):
        """Test that API errors are properly recorded."""
        # Set up environment variables for error endpoints
        env = os.environ.copy()
        env["API_ENDPOINTS"] = f"http://localhost:{self.port}/error,http://localhost:{self.port}/notfound"
        env["API_NAMES"] = "Error API,NotFound API"
        env["API_REQUEST_TIMEOUT"] = "5"
        env["RESOURCE_STATS_DB"] = self.db_path
        
        # Run the API collector script
        script_path = os.path.join(
            Path(__file__).resolve().parent.parent.parent,
            "scripts",
            "api_status_collector.py"
        )
        
        result = subprocess.run(
            [sys.executable, script_path, "--verbose"],
            env=env,
            capture_output=True,
            text=True
        )
        
        # Check that the script executed successfully (errors are recorded, not fatal)
        self.assertEqual(result.returncode, 0, 
                         f"Script failed with error: {result.stderr}")
        
        # Verify error data was written to the database
        cursor = self.conn.cursor()
        
        # Check Error API (500 status)
        cursor.execute("""
            SELECT response_time_ms, status_code, success, error_message
            FROM api_stats 
            WHERE api_name = 'Error API'
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
        response_time, status_code, success, error_message = row
        
        self.assertGreater(response_time, 0)
        self.assertEqual(status_code, 500)
        self.assertFalse(success)
        self.assertEqual(error_message, "HTTP 500")
        
        # Check NotFound API (404 status)
        cursor.execute("""
            SELECT response_time_ms, status_code, success, error_message
            FROM api_stats 
            WHERE api_name = 'NotFound API'
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
        response_time, status_code, success, error_message = row
        
        self.assertGreater(response_time, 0)
        self.assertEqual(status_code, 404)
        self.assertFalse(success)
        self.assertEqual(error_message, "HTTP 404")
    
    def test_api_timeout_handling(self):
        """Test that API timeouts are properly handled."""
        # Set up environment variables with very short timeout
        env = os.environ.copy()
        env["API_ENDPOINTS"] = f"http://localhost:{self.port}/slow"
        env["API_NAMES"] = "Timeout API"
        env["API_REQUEST_TIMEOUT"] = "1"  # 1 second timeout, but endpoint takes 2 seconds
        env["RESOURCE_STATS_DB"] = self.db_path
        
        # Run the API collector script
        script_path = os.path.join(
            Path(__file__).resolve().parent.parent.parent,
            "scripts",
            "api_status_collector.py"
        )
        
        result = subprocess.run(
            [sys.executable, script_path, "--verbose"],
            env=env,
            capture_output=True,
            text=True
        )
        
        # Check that the script executed successfully (timeouts are recorded, not fatal)
        self.assertEqual(result.returncode, 0, 
                         f"Script failed with error: {result.stderr}")
        
        # Verify timeout data was written to the database
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT response_time_ms, status_code, success, error_message
            FROM api_stats 
            WHERE api_name = 'Timeout API'
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
        response_time, status_code, success, error_message = row
        
        # Response time should be approximately the timeout value (1000ms)
        self.assertGreaterEqual(response_time, 1000)
        self.assertEqual(status_code, 0)  # No status code for timeout
        self.assertFalse(success)
        self.assertEqual(error_message, "Request timeout")
    
    def test_no_apis_configured(self):
        """Test that the script handles the case where no APIs are configured."""
        # Set up environment variables with no APIs
        env = os.environ.copy()
        env["API_ENDPOINTS"] = ""
        env["API_NAMES"] = ""
        env["RESOURCE_STATS_DB"] = self.db_path
        
        # Run the API collector script
        script_path = os.path.join(
            Path(__file__).resolve().parent.parent.parent,
            "scripts",
            "api_status_collector.py"
        )
        
        result = subprocess.run(
            [sys.executable, script_path, "--verbose"],
            env=env,
            capture_output=True,
            text=True
        )
        
        # Check that the script executed successfully
        self.assertEqual(result.returncode, 0, 
                         f"Script failed with error: {result.stderr}")
        
        # Verify no data was written to the database
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM api_stats")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 0, "Data was written despite no APIs configured")
        
        # Verify verbose output mentions no APIs
        self.assertIn("No API endpoints configured", result.stdout)


if __name__ == '__main__':
    unittest.main()