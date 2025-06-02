#!/usr/bin/env python3
import os
import unittest
from unittest.mock import patch, mock_open
import socket
import getpass
from pathlib import Path

# Add parent directory to path so we can import modules
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from common.env_utils import preprocess_env


class TestEnvUtils(unittest.TestCase):
    
    @patch('common.env_utils.dotenv_values')
    @patch('common.env_utils.socket.gethostname')
    @patch('common.env_utils.getpass.getuser')
    @patch('common.env_utils.Path.home')
    def test_preprocess_env_with_templates(self, mock_home, mock_getuser, mock_hostname, mock_dotenv):
        # Setup mocks
        mock_home.return_value = Path('/home/testuser')
        mock_getuser.return_value = 'testuser'
        mock_hostname.return_value = 'testhost'
        # The issue seems to be with the template replacement
        # Let's check what's actually happening by capturing it
        mock_dotenv.return_value = {
            'PATH_VAR': '{{HOME}}/test',  # Changed to remove leading slash
            'USER_VAR': '{{whoami}}',
            'HOST_VAR': '{{hostname}}',
            'EMPTY_VAR': '',
            'NORMAL_VAR': 'normal_value'
        }
        
        # Execute with environment clear
        with patch.dict(os.environ, {}, clear=True):
            preprocess_env()
            
            # The template replacement includes the leading slash in Path.home()
            self.assertEqual(os.environ['PATH_VAR'], '/home/testuser/test')
            self.assertEqual(os.environ['USER_VAR'], 'testuser')
            self.assertEqual(os.environ['HOST_VAR'], 'testhost')
            self.assertEqual(os.environ['EMPTY_VAR'], '')
            self.assertEqual(os.environ['NORMAL_VAR'], 'normal_value')
    
    @patch('common.env_utils.dotenv_values')
    def test_preprocess_env_with_shell_env(self, mock_dotenv):
        # Setup mock for dotenv
        mock_dotenv.return_value = {'TEST_VAR': 'dotenv_value', 'NEW_VAR': 'new_value'}
        
        # Setup test environment
        test_env = {'SHELL_VAR': 'shell_value', 'TEST_VAR': 'shell_value'}
        
        # Execute with environment variables
        with patch.dict(os.environ, test_env, clear=True):
            preprocess_env(use_shell_env=True)
            
            # Verify environment was preserved
            self.assertEqual(os.environ['SHELL_VAR'], 'shell_value')
            # Verify shell value is kept (not overridden by dotenv)
            self.assertEqual(os.environ['TEST_VAR'], 'shell_value')
            # Verify new variables are still added
            self.assertEqual(os.environ['NEW_VAR'], 'new_value')
    
    @patch('common.env_utils.dotenv_values')
    def test_preprocess_env_nonexistent_file(self, mock_dotenv):
        # Simulate file not found
        mock_dotenv.return_value = {}
        
        # Should not raise exception
        with patch.dict(os.environ, {}, clear=True):
            preprocess_env(path="nonexistent.env")
            # Test passes if no exception is raised


if __name__ == '__main__':
    unittest.main()