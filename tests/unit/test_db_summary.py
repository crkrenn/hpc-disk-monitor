#!/usr/bin/env python3

import unittest
import tempfile
import sqlite3
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, call
from datetime import datetime

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from scripts import db_summary


class TestDbSummary(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_db = os.path.join(self.temp_dir, "test.db")
        
        # Create test database with sample data
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE disk_stats (
                timestamp TEXT,
                hostname TEXT,
                label TEXT,
                write_mbps REAL,
                write_iops INTEGER,
                write_lat_avg REAL,
                read_mbps REAL,
                read_iops INTEGER,
                read_lat_avg REAL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE disk_stats_summary (
                timestamp TEXT,
                hostname TEXT,
                label TEXT,
                metric TEXT,
                avg REAL,
                min REAL,
                max REAL,
                stddev REAL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE api_stats (
                timestamp TEXT,
                hostname TEXT,
                api_name TEXT,
                endpoint_url TEXT,
                response_time_ms REAL,
                status_code INTEGER,
                success BOOLEAN,
                error_message TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE api_stats_summary (
                timestamp TEXT,
                hostname TEXT,
                api_name TEXT,
                metric TEXT,
                avg REAL,
                min REAL,
                max REAL,
                stddev REAL
            )
        ''')
        
        # Insert sample data
        cursor.execute('''
            INSERT INTO disk_stats VALUES 
            ('2023-01-01 12:00', 'testhost', 'tmpfs', 100.0, 500, 0.001, 200.0, 1000, 0.002),
            ('2023-01-01 13:00', 'testhost', 'tmpfs', 110.0, 550, 0.0015, 210.0, 1100, 0.0025)
        ''')
        
        cursor.execute('''
            INSERT INTO disk_stats_summary VALUES 
            ('2023-01-01 13:00', 'testhost', 'tmpfs', 'write_mbps', 105.0, 100.0, 110.0, 5.0),
            ('2023-01-01 13:00', 'testhost', 'tmpfs', 'read_mbps', 205.0, 200.0, 210.0, 5.0)
        ''')
        
        cursor.execute('''
            INSERT INTO api_stats VALUES 
            ('2023-01-01 12:00', 'testhost', 'Test API', 'http://test.com', 150.5, 200, 1, NULL),
            ('2023-01-01 13:00', 'testhost', 'Test API', 'http://test.com', 160.0, 200, 1, NULL)
        ''')
        
        cursor.execute('''
            INSERT INTO api_stats_summary VALUES 
            ('2023-01-01 13:00', 'testhost', 'Test API', 'response_time_ms', 155.25, 150.5, 160.0, 4.75),
            ('2023-01-01 13:00', 'testhost', 'Test API', 'success_rate', 1.0, 1.0, 1.0, 0.0)
        ''')
        
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_connect_db_success(self):
        """Test successful database connection."""
        with patch('scripts.db_summary.DB_FILE', self.test_db):
            conn = db_summary.connect_db()
            self.assertIsNotNone(conn)
            conn.close()
    
    def test_connect_db_failure(self):
        """Test database connection failure."""
        with patch('scripts.db_summary.DB_FILE', '/nonexistent/path.db'):
            conn = db_summary.connect_db()
            self.assertIsNone(conn)
    
    def test_get_time_bounds_and_count(self):
        """Test getting time bounds and count from database."""
        conn = sqlite3.connect(self.test_db)
        try:
            result = db_summary.get_time_bounds_and_count(conn)
            self.assertEqual(len(result), 3)
            first_ts, last_ts, count = result
            self.assertEqual(first_ts, '2023-01-01 12:00')
            self.assertEqual(last_ts, '2023-01-01 13:00')
            self.assertEqual(count, 4)  # 2 disk_stats + 2 api_stats
        finally:
            conn.close()
    
    def test_get_time_bounds_and_count_with_time_filter(self):
        """Test getting time bounds with time filter."""
        conn = sqlite3.connect(self.test_db)
        try:
            result = db_summary.get_time_bounds_and_count(
                conn, 
                start_time='2023-01-01 12:30', 
                end_time='2023-01-01 13:30'
            )
            first_ts, last_ts, count = result
            self.assertEqual(count, 2)  # Only 13:00 records
        finally:
            conn.close()
    
    def test_get_latest_summary_per_filesystem(self):
        """Test getting latest summary data for both disk and API resources."""
        conn = sqlite3.connect(self.test_db)
        try:
            with patch('scripts.db_summary.FS_LABELS', ['tmpfs']):
                summaries = db_summary.get_latest_summary_per_filesystem(conn)
                
                # Should have both disk and API summaries
                self.assertGreater(len(summaries), 0)
                
                # Check that we have both disk and API data
                data_types = set()
                labels = set()
                for summary in summaries:
                    label, metric, avg, minv, maxv, std, timestamp, hostname, data_type = summary
                    data_types.add(data_type)
                    labels.add(label)
                
                self.assertIn('disk', data_types)
                self.assertIn('api', data_types)
                self.assertIn('tmpfs', labels)
                self.assertIn('Test API', labels)
        finally:
            conn.close()
    
    def test_get_all_resources(self):
        """Test getting all available resources."""
        conn = sqlite3.connect(self.test_db)
        try:
            with patch('scripts.db_summary.FS_LABELS', ['tmpfs']):
                resources = db_summary.get_all_resources(conn)
                
                self.assertIn('disk', resources)
                self.assertIn('api', resources)
                self.assertIn('tmpfs', resources['disk'])
                self.assertIn('Test API', resources['api'])
        finally:
            conn.close()
    
    def test_format_value_latency(self):
        """Test formatting latency values."""
        result = db_summary.format_value(0.001234, 'write_lat_avg')
        self.assertEqual(result, '1.23e-03')
    
    def test_format_value_iops(self):
        """Test formatting IOPS values."""
        result = db_summary.format_value(1500.7, 'write_iops')
        self.assertEqual(result, '1501')
    
    def test_format_value_throughput(self):
        """Test formatting throughput values."""
        result = db_summary.format_value(123.456, 'write_mbps')
        self.assertEqual(result, '123.46')
    
    def test_get_time_range_from_args_predefined_period(self):
        """Test parsing predefined time periods."""
        with patch('scripts.db_summary.args') as mock_args:
            mock_args.time_period = '1d'
            mock_args.start_date = None
            mock_args.end_date = None
            
            start_time, end_time, desc = db_summary.get_time_range_from_args()
            
            self.assertIsNotNone(start_time)
            self.assertIsNotNone(end_time)
            self.assertEqual(desc, 'Last 24 Hours')
    
    def test_get_time_range_from_args_custom_dates(self):
        """Test parsing custom date ranges."""
        with patch('scripts.db_summary.args') as mock_args:
            mock_args.time_period = '1d'
            mock_args.start_date = '2023-01-01'
            mock_args.end_date = '2023-01-02'
            
            start_time, end_time, desc = db_summary.get_time_range_from_args()
            
            self.assertEqual(start_time, '2023-01-01 00:00')
            self.assertEqual(end_time, '2023-01-02 00:00')
            self.assertIn('from 2023-01-01 00:00 to 2023-01-02 00:00', desc)
    
    def test_get_time_range_from_args_all_time(self):
        """Test parsing 'all time' period."""
        with patch('scripts.db_summary.args') as mock_args:
            mock_args.time_period = 'all'
            mock_args.start_date = None
            mock_args.end_date = None
            
            start_time, end_time, desc = db_summary.get_time_range_from_args()
            
            self.assertIsNone(start_time)
            self.assertIsNone(end_time)
            self.assertEqual(desc, 'all time')
    
    @patch('scripts.db_summary.connect_db')
    @patch('scripts.db_summary.get_time_range_from_args')
    @patch('scripts.db_summary.get_latest_summary_per_filesystem')
    @patch('scripts.db_summary.get_all_resources')
    @patch('scripts.db_summary.get_time_bounds_and_count')
    @patch('builtins.print')
    def test_main_success(self, mock_print, mock_get_bounds, mock_get_resources, 
                         mock_get_summary, mock_get_time_range, mock_connect):
        """Test successful main execution with both disk and API data."""
        # Mock setup
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_get_time_range.return_value = (None, None, 'all time')
        mock_get_resources.return_value = {'disk': ['tmpfs'], 'api': ['Test API']}
        mock_get_summary.return_value = [
            ('tmpfs', 'write_mbps', 105.0, 100.0, 110.0, 5.0, '2023-01-01 13:00', 'testhost', 'disk'),
            ('Test API', 'response_time_ms', 155.25, 150.5, 160.0, 4.75, '2023-01-01 13:00', 'testhost', 'api')
        ]
        mock_get_bounds.side_effect = [
            ('2023-01-01 12:00', '2023-01-01 13:00', 4),  # For filtered period
            ('2023-01-01 12:00', '2023-01-01 13:00', 4)   # For all time
        ]
        
        with patch('scripts.db_summary.FS_LABELS', ['tmpfs']):
            with patch('scripts.db_summary.API_CONFIG', {'http://test.com': 'Test API'}):
                with patch('scripts.db_summary.args') as mock_args:
                    mock_args.recompute = False
                    mock_args.start_date = None
                    
                    result = db_summary.main()
        
        self.assertEqual(result, 0)
        mock_connect.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('scripts.db_summary.connect_db')
    @patch('builtins.print')
    def test_main_connection_failure(self, mock_print, mock_connect):
        """Test main execution with database connection failure."""
        mock_connect.return_value = None
        
        result = db_summary.main()
        
        self.assertEqual(result, 1)
        mock_print.assert_any_call("⚠️  Cannot continue without database connection.")


if __name__ == '__main__':
    unittest.main()