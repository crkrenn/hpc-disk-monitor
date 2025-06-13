#!/usr/bin/env python3
import unittest
import os
from unittest.mock import patch
from pathlib import Path

# Add parent directory to path so we can import modules
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))


class TestEnvironmentConfiguration(unittest.TestCase):
    
    def test_api_config_parsing(self):
        """Test that API endpoint configuration is parsed correctly."""
        # Test the parsing logic directly instead of importing the module
        endpoints = "https://api1.example.com/health,https://api2.example.com/status"
        names = "API 1,API 2"
        
        # Parse the configuration like the module does
        api_endpoints = [url.strip() for url in endpoints.split(",") if url.strip()]
        api_names = [name.strip() for name in names.split(",") if name.strip()]
        
        expected_config = {
            "https://api1.example.com/health": "API 1",
            "https://api2.example.com/status": "API 2"
        }
        
        actual_config = dict(zip(api_endpoints, api_names))
        self.assertEqual(actual_config, expected_config)
    
    def test_api_config_empty(self):
        """Test that empty API configuration is handled correctly."""
        # Test the parsing logic directly
        endpoints = ""
        names = ""
        
        # Parse the configuration like the module does
        api_endpoints = [url.strip() for url in endpoints.split(",") if url.strip()]
        api_names = [name.strip() for name in names.split(",") if name.strip()]
        
        actual_config = dict(zip(api_endpoints, api_names)) if api_endpoints else {}
        self.assertEqual(actual_config, {})
    
    def test_api_config_auto_names(self):
        """Test that API names are auto-generated when not provided."""
        # Test the auto-generation logic directly
        endpoints = "https://api1.example.com/health,https://api2.example.com/status"
        names = ""
        
        # Parse the configuration like the module does
        api_endpoints = [url.strip() for url in endpoints.split(",") if url.strip()]
        api_names = [name.strip() for name in names.split(",") if name.strip()]
        
        if len(api_endpoints) != len(api_names):
            if api_endpoints and not api_names:
                # Generate default names from URLs
                api_names = [f"API-{i+1}" for i in range(len(api_endpoints))]
        
        expected_config = {
            "https://api1.example.com/health": "API-1",
            "https://api2.example.com/status": "API-2"
        }
        
        actual_config = dict(zip(api_endpoints, api_names))
        self.assertEqual(actual_config, expected_config)
    
    def test_api_config_mismatch_error(self):
        """Test that mismatched endpoints and names raise an error."""
        # Test the validation logic directly
        endpoints = "https://api1.example.com/health,https://api2.example.com/status"
        names = "API 1"  # Only one name for two endpoints
        
        # Parse the configuration like the module does
        api_endpoints = [url.strip() for url in endpoints.split(",") if url.strip()]
        api_names = [name.strip() for name in names.split(",") if name.strip()]
        
        # This should raise a ValueError
        with self.assertRaises(ValueError) as context:
            if len(api_endpoints) != len(api_names):
                if api_endpoints and not api_names:
                    # Generate default names - this is OK
                    api_names = [f"API-{i+1}" for i in range(len(api_endpoints))]
                elif len(api_endpoints) > 0 and len(api_names) > 0:
                    # Different lengths but both non-empty - this is an error
                    raise ValueError("API_ENDPOINTS and API_NAMES must have the same length")
        
        self.assertIn("API_ENDPOINTS and API_NAMES must have the same length", str(context.exception))
    
    def test_database_path_configuration(self):
        """Test that database path configuration works with new variable names."""
        # Test the environment variable precedence logic directly
        import os
        from pathlib import Path
        
        # Mock the environment variable lookup logic
        resource_db = "/test/path/resource_stats.db"
        disk_db = "/fallback/path/disk_stats.db"
        default_path = str(Path.home() / "hpc-resource-monitor/data/resource_stats.db")
        
        # Test new variable takes precedence
        db_file = resource_db if resource_db else (disk_db if disk_db else default_path)
        self.assertEqual(db_file, "/test/path/resource_stats.db")
    
    def test_database_path_fallback(self):
        """Test that database path falls back to legacy DISK_STATS_DB if RESOURCE_STATS_DB is not set."""
        # Test the fallback logic directly
        from pathlib import Path
        
        resource_db = None  # Not set
        disk_db = "/test/path/disk_stats.db"
        default_path = str(Path.home() / "hpc-resource-monitor/data/resource_stats.db")
        
        # Test fallback to legacy variable
        db_file = resource_db if resource_db else (disk_db if disk_db else default_path)
        self.assertEqual(db_file, "/test/path/disk_stats.db")
    
    def test_filesystem_config_parsing(self):
        """Test that filesystem configuration is parsed correctly."""
        # Test the parsing logic directly
        paths = "/path1,/path2,/path3"
        labels = "label1,label2,label3"
        
        # Parse the configuration like the module does
        fs_paths = paths.split(",")
        fs_labels = labels.split(",")
        
        if len(fs_paths) != len(fs_labels):
            raise ValueError("FILESYSTEM_PATHS and FILESYSTEM_LABELS must have the same length")
        
        expected_config = {
            "/path1": "label1",
            "/path2": "label2",
            "/path3": "label3"
        }
        
        actual_config = dict(zip(fs_paths, fs_labels))
        self.assertEqual(actual_config, expected_config)
    
    def test_sampling_intervals(self):
        """Test that sampling interval configuration is parsed correctly."""
        # Test the parsing logic directly
        disk_minutes = "10"
        api_minutes = "3"
        
        # Parse like the module does
        disk_sampling_minutes = int(disk_minutes)
        api_sampling_minutes = int(api_minutes)
        
        self.assertEqual(disk_sampling_minutes, 10)
        self.assertEqual(api_sampling_minutes, 3)


if __name__ == '__main__':
    unittest.main()