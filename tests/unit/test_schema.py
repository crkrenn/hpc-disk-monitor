#!/usr/bin/env python3
import unittest
from unittest.mock import patch, MagicMock, mock_open
import sqlite3
import os
from pathlib import Path

# Add parent directory to path so we can import modules
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from db.schema import connect_db, create_tables, DB_FILE


class TestSchema(unittest.TestCase):
    
    @patch('db.schema.Path')
    @patch('db.schema.sqlite3.connect')
    def test_connect_db_creates_directories(self, mock_connect, mock_path):
        # Setup path mock
        path_instance = MagicMock()
        mock_path.return_value = path_instance
        parent_mock = MagicMock()
        path_instance.parent = parent_mock
        
        # Setup sqlite3 mock
        db_conn_mock = MagicMock()
        mock_connect.return_value = db_conn_mock
        
        # Call function
        result = connect_db()
        
        # Verify directory creation and connection
        parent_mock.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_connect.assert_called_once_with(DB_FILE)
        self.assertEqual(result, db_conn_mock)
    
    def test_create_tables(self):
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Call the function
        create_tables(mock_conn)
        
        # Verify that execute was called twice (once for each table)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        
        # Check that the first call contains disk_stats table creation
        first_call = mock_cursor.execute.call_args_list[0][0][0]
        self.assertIn('CREATE TABLE IF NOT EXISTS disk_stats', first_call)
        self.assertIn('timestamp TEXT', first_call)
        self.assertIn('hostname TEXT', first_call)
        self.assertIn('label TEXT', first_call)
        self.assertIn('write_mbps REAL', first_call)
        self.assertIn('read_mbps REAL', first_call)
        
        # Check that the second call contains disk_stats_summary table creation
        second_call = mock_cursor.execute.call_args_list[1][0][0]
        self.assertIn('CREATE TABLE IF NOT EXISTS disk_stats_summary', second_call)
        self.assertIn('metric TEXT', second_call)
        self.assertIn('avg REAL', second_call)
        self.assertIn('min REAL', second_call)
        self.assertIn('max REAL', second_call)
        self.assertIn('stddev REAL', second_call)


if __name__ == '__main__':
    unittest.main()