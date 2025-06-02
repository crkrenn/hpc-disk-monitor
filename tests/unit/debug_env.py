#!/usr/bin/env python3
import unittest
from unittest.mock import patch, mock_open
import os
import socket
import getpass
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from common.env_utils import preprocess_env

class TestEnvDebug(unittest.TestCase):
    
    @patch('common.env_utils.dotenv_values')
    @patch('common.env_utils.socket.gethostname')
    @patch('common.env_utils.getpass.getuser')
    @patch('common.env_utils.Path.home')
    def test_debug_templates(self, mock_home, mock_getuser, mock_hostname, mock_dotenv):
        # Setup mocks
        mock_home.return_value = Path('/home/testuser')
        mock_getuser.return_value = 'testuser'
        mock_hostname.return_value = 'testhost'
        
        test_cases = [
            ('{{HOME}}/test', '/home/testuser/test'),
            ('/{{HOME}}/test', '//home/testuser/test'),  # This is the issue - double slash
            ('{{whoami}}', 'testuser'),
            ('{{hostname}}', 'testhost'),
            ('prefix_{{HOME}}_suffix', 'prefix_/home/testuser_suffix')
        ]
        
        for template, expected in test_cases:
            # Create a new dotenv values for each test
            mock_dotenv.return_value = {'TEST_VAR': template}
            
            # Clear environment for each test
            with patch.dict(os.environ, {}, clear=True):
                preprocess_env()
                
                # Print debug information
                print(f"Template: '{template}'")
                print(f"Expected: '{expected}'")
                print(f"Actual  : '{os.environ['TEST_VAR']}'")
                print(f"Match   : {os.environ['TEST_VAR'] == expected}")
                print("---")
        
        # This test is informational only, always passes
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()