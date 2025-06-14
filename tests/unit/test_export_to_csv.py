#!/usr/bin/env python3

import unittest
import tempfile
import sqlite3
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from scripts import export_to_csv


class TestExportToCsv(unittest.TestCase):
    
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
            ('2023-01-01 12:00', 'testhost', 'tmpfs', 100.0, 500, 0.001, 200.0, 1000, 0.002)
        ''')
        
        cursor.execute('''
            INSERT INTO disk_stats_summary VALUES 
            ('2023-01-01 12:00', 'testhost', 'tmpfs', 'write_mbps', 100.0, 90.0, 110.0, 5.0)
        ''')
        
        cursor.execute('''
            INSERT INTO api_stats VALUES 
            ('2023-01-01 12:00', 'testhost', 'Test API', 'http://test.com', 150.5, 200, 1, NULL)
        ''')
        
        cursor.execute('''
            INSERT INTO api_stats_summary VALUES 
            ('2023-01-01 12:00', 'testhost', 'Test API', 'response_time_ms', 150.5, 100.0, 200.0, 25.0)
        ''')
        
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    @patch('scripts.export_to_csv.DB_FILE')
    @patch('scripts.export_to_csv.OUTPUT_DIR')
    def test_export_table_to_csv_disk_stats(self, mock_output_dir, mock_db_file):
        """Test exporting disk_stats table to CSV."""
        mock_db_file.return_value = self.test_db
        mock_output_dir.return_value = Path(self.temp_dir)
        
        csv_path = Path(self.temp_dir) / "disk_stats.csv"
        export_to_csv.export_table_to_csv(self.test_db, "disk_stats", csv_path)
        
        # Verify CSV was created and contains expected data
        self.assertTrue(csv_path.exists())
        df = pd.read_csv(csv_path)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['label'], 'tmpfs')
        self.assertEqual(df.iloc[0]['write_mbps'], 100.0)
    
    @patch('scripts.export_to_csv.DB_FILE')
    @patch('scripts.export_to_csv.OUTPUT_DIR')
    def test_export_table_to_csv_api_stats(self, mock_output_dir, mock_db_file):
        """Test exporting api_stats table to CSV."""
        mock_db_file.return_value = self.test_db
        mock_output_dir.return_value = Path(self.temp_dir)
        
        csv_path = Path(self.temp_dir) / "api_stats.csv"
        export_to_csv.export_table_to_csv(self.test_db, "api_stats", csv_path)
        
        # Verify CSV was created and contains expected data
        self.assertTrue(csv_path.exists())
        df = pd.read_csv(csv_path)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['api_name'], 'Test API')
        self.assertEqual(df.iloc[0]['response_time_ms'], 150.5)
    
    @patch('scripts.export_to_csv.DB_FILE')
    @patch('scripts.export_to_csv.OUTPUT_DIR')
    def test_export_table_to_csv_missing_table(self, mock_output_dir, mock_db_file):
        """Test handling of missing table gracefully."""
        mock_db_file.return_value = self.test_db
        mock_output_dir.return_value = Path(self.temp_dir)
        
        csv_path = Path(self.temp_dir) / "nonexistent.csv"
        
        # Should not raise an exception
        export_to_csv.export_table_to_csv(self.test_db, "nonexistent_table", csv_path)
        
        # CSV should not be created
        self.assertFalse(csv_path.exists())
    
    @patch('builtins.print')
    def test_main_exports_all_tables(self, mock_print):
        """Test that main() exports all available tables."""
        # Create expected EXPORTS mapping with temp directory
        temp_output_dir = Path(self.temp_dir)
        temp_exports = {
            "disk_stats": temp_output_dir / "disk_stats.csv",
            "disk_stats_summary": temp_output_dir / "disk_stats_summary.csv",
            "api_stats": temp_output_dir / "api_stats.csv",
            "api_stats_summary": temp_output_dir / "api_stats_summary.csv"
        }
        
        # Patch module-level variables
        with patch('scripts.export_to_csv.DB_FILE', self.test_db), \
             patch('scripts.export_to_csv.OUTPUT_DIR', temp_output_dir), \
             patch('scripts.export_to_csv.EXPORTS', temp_exports), \
             patch.object(Path, 'exists', return_value=True):
            
            result = export_to_csv.main()
        
        # Should return 0 on success
        self.assertEqual(result, 0)
        
        # Check that CSV files were created for tables that exist
        self.assertTrue(temp_exports["disk_stats"].exists())
        self.assertTrue(temp_exports["api_stats"].exists())
        self.assertTrue(temp_exports["disk_stats_summary"].exists())
        self.assertTrue(temp_exports["api_stats_summary"].exists())
    
    @patch('builtins.print')
    def test_main_missing_database(self, mock_print):
        """Test handling of missing database file."""
        nonexistent_db = '/nonexistent/path/db.sqlite'
        
        with patch('scripts.export_to_csv.DB_FILE', nonexistent_db), \
             patch('scripts.export_to_csv.OUTPUT_DIR', Path(self.temp_dir)), \
             patch.object(Path, 'exists', return_value=False):
            
            result = export_to_csv.main()
        
        # Should return 1 on error
        self.assertEqual(result, 1)
        
        # Should print warning about missing database
        mock_print.assert_any_call(f"⚠️  Database file not found: {nonexistent_db}")


if __name__ == '__main__':
    unittest.main()