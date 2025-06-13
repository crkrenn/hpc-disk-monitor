#!/usr/bin/env python3
"""
Test runner for HPC Resource Monitor project.
Runs all unit and integration tests.
"""

import unittest
import sys
import os
from pathlib import Path

# Add the project root to sys.path to ensure imports work correctly
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

def run_tests():
    """Discover and run all tests in the project."""
    print("Running HPC Resource Monitor tests...")
    
    # Create test suites
    test_loader = unittest.TestLoader()
    
    # Unit tests
    unit_tests = test_loader.discover(
        start_dir=os.path.join(project_root, 'tests', 'unit'),
        pattern='test_*.py'
    )
    
    # Integration tests (only simplified ones)
    integration_tests = test_loader.discover(
        start_dir=os.path.join(project_root, 'tests', 'integration'),
        pattern='test_*simplified.py'
    )
    
    # Combine test suites
    all_tests = unittest.TestSuite([unit_tests, integration_tests])
    
    # Run tests
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(all_tests)
    
    print("\nTest Summary:")
    print(f"  Ran {result.testsRun} tests")
    print(f"  Errors: {len(result.errors)}")
    print(f"  Failures: {len(result.failures)}")
    
    # Return appropriate exit code
    return 0 if result.wasSuccessful() else 1

if __name__ == '__main__':
    sys.exit(run_tests())