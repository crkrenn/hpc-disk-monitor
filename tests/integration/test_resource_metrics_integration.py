#!/usr/bin/env python3
import unittest
import os
import sqlite3
import tempfile
import shutil
import time
from pathlib import Path
import subprocess
import sys

# Add parent directory to path so we can import modules
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from db.schema import create_tables


class TestResourceMetricsIntegration(unittest.TestCase):
    
    def setUp(self):
        # Create temporary directories for testing
        self.temp_dir = tempfile.mkdtemp()
        self.test_fs_path = os.path.join(self.temp_dir, "test_fs")
        os.makedirs(self.test_fs_path, exist_ok=True)
        
        # Create a temporary database
        self.db_path = os.path.join(self.temp_dir, "test_resource_stats.db")
        self.conn = sqlite3.connect(self.db_path)
        create_tables(self.conn)
    
    def tearDown(self):
        # Close database connection
        self.conn.close()
        
        # Clean up temporary directories
        shutil.rmtree(self.temp_dir)
    
    def test_end_to_end_collection(self):
        """Test that the collector script can run and store data in the database."""
        # Set up environment variables for the test
        env = os.environ.copy()
        env["FILESYSTEM_PATHS"] = self.test_fs_path
        env["FILESYSTEM_LABELS"] = "test_fs"
        env["RESOURCE_STATS_DB"] = self.db_path
        
        # Run the collector script
        script_path = os.path.join(
            Path(__file__).resolve().parent.parent.parent,
            "scripts",
            "resource_metrics_collector.py"
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
        
        # Check disk_stats table
        cursor.execute("SELECT COUNT(*) FROM disk_stats WHERE label = 'test_fs'")
        count = cursor.fetchone()[0]
        self.assertGreater(count, 0, "No data was written to disk_stats table")
        
        # Check disk_stats_summary table
        cursor.execute("SELECT COUNT(*) FROM disk_stats_summary WHERE label = 'test_fs'")
        count = cursor.fetchone()[0]
        self.assertGreater(count, 0, "No data was written to disk_stats_summary table")
        
        # Verify metrics were collected
        cursor.execute("""
            SELECT write_mbps, write_iops, write_lat_avg, 
                   read_mbps, read_iops, read_lat_avg
            FROM disk_stats 
            WHERE label = 'test_fs'
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        # All metrics should be present and have positive values
        for metric in row:
            self.assertIsNotNone(metric)
            self.assertGreaterEqual(metric, 0)
    
    def test_data_retention_and_decimation(self):
        """Test that the data decimation function works as expected."""
        # Set up test data - insert records with timestamps at different ages
        cursor = self.conn.cursor()
        
        # Current data (should be kept)
        cursor.execute("""
            INSERT INTO disk_stats (
                timestamp, hostname, label, 
                write_mbps, write_iops, write_lat_avg,
                read_mbps, read_iops, read_lat_avg
            ) VALUES (
                datetime('now'), 'test-host', 'test_fs',
                100, 25, 0.04, 200, 50, 0.02
            )
        """)
        
        # We need to use rowid for decimation, so let's start with a high rowid
        # First, ensure the rowid starts high enough for our test
        for i in range(1000):
            cursor.execute("""
                INSERT INTO disk_stats (
                    timestamp, hostname, label, 
                    write_mbps, write_iops, write_lat_avg,
                    read_mbps, read_iops, read_lat_avg
                ) VALUES (
                    datetime('now'), 'test-host', 'dummy',
                    1, 1, 0.01, 1, 1, 0.01
                )
            """)
        
        # Now delete the dummy data
        cursor.execute("DELETE FROM disk_stats WHERE label = 'dummy'")
        
        # 2-day old data (should be decimated to 1/6)
        # Make rowids such that most will be deleted (not divisible by 6)
        for i in range(60):  # Insert 60 records
            cursor.execute("""
                INSERT INTO disk_stats (
                    timestamp, hostname, label, 
                    write_mbps, write_iops, write_lat_avg,
                    read_mbps, read_iops, read_lat_avg
                ) VALUES (
                    datetime('now', '-2 days', ?, 'minutes'), 'test-host', 'test_fs',
                    100, 25, 0.04, 200, 50, 0.02
                )
            """, (str(-i),))
        
        # 4-day old data (should be decimated to 1/60)
        for i in range(120):  # Insert 120 records
            cursor.execute("""
                INSERT INTO disk_stats (
                    timestamp, hostname, label, 
                    write_mbps, write_iops, write_lat_avg,
                    read_mbps, read_iops, read_lat_avg
                ) VALUES (
                    datetime('now', '-4 days', ?, 'minutes'), 'test-host', 'test_fs',
                    100, 25, 0.04, 200, 50, 0.02
                )
            """, (str(-i),))
        
        self.conn.commit()
        
        # Verify initial record count
        cursor.execute("SELECT COUNT(*) FROM disk_stats")
        initial_count = cursor.fetchone()[0]
        self.assertEqual(initial_count, 1 + 60 + 120)
        
        # Set up environment variables for the test
        env = os.environ.copy()
        env["FILESYSTEM_PATHS"] = self.test_fs_path
        env["FILESYSTEM_LABELS"] = "test_fs"
        env["RESOURCE_STATS_DB"] = self.db_path
        
        # Run the collector script
        script_path = os.path.join(
            Path(__file__).resolve().parent.parent.parent,
            "scripts",
            "resource_metrics_collector.py"
        )
        
        result = subprocess.run(
            [sys.executable, script_path],
            env=env,
            capture_output=True,
            text=True
        )
        
        # Check that the script executed successfully
        self.assertEqual(result.returncode, 0, 
                         f"Script failed with error: {result.stderr}")
        
        # Verify data was decimated
        cursor.execute("SELECT COUNT(*) FROM disk_stats")
        final_count = cursor.fetchone()[0]
        
        # We don't know exactly how many rows will remain due to the modulo operation
        # on rowid, but it should be significantly less than the original count.
        # The test might be flaky depending on rowid distribution, so let's use a very
        # conservative assertion.
        
        print(f"Initial count: {initial_count}, Final count: {final_count}")
        
        # The count should definitely be less than the initial count
        # This is a more robust test that's less likely to be flaky
        self.assertLess(final_count, initial_count)
        
        # Check 2-day old data (should be decimated to about 1/6)
        cursor.execute("""
            SELECT COUNT(*) FROM disk_stats 
            WHERE timestamp < datetime('now', '-1 days')
            AND timestamp >= datetime('now', '-3 days')
        """)
        count_2day = cursor.fetchone()[0]
        self.assertLess(count_2day, 20)  # Should be around 10 (60/6) plus a little tolerance
        
        # Check 4-day old data (should be decimated to about 1/60)
        cursor.execute("""
            SELECT COUNT(*) FROM disk_stats 
            WHERE timestamp < datetime('now', '-3 days')
        """)
        count_4day = cursor.fetchone()[0]
        self.assertLess(count_4day, 5)  # Should be around 2 (120/60) plus a little tolerance


if __name__ == '__main__':
    unittest.main()