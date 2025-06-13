# HPC Resource Monitor Tests

This directory contains tests for the HPC Resource Monitor project.

## Test Structure

- `unit/`: Unit tests for individual components
- `integration/`: Integration tests that verify multiple components working together

## Running Tests

### Using unittest

```bash
# Run all tests
python run_tests.py

# Run only unit tests
python -m unittest discover -s tests/unit

# Run only integration tests
python -m unittest discover -s tests/integration

# Run a specific test file
python -m tests.unit.test_env_utils

# Run API status collector tests
python -m tests.unit.test_api_status_collector

# Run resource metrics collector tests  
python -m tests.unit.test_resource_metrics_collector
```

### Using pytest

```bash
# Run all tests with coverage report
pytest --cov=. tests/

# Run specific test directory
pytest tests/unit/

# Run specific test file
pytest tests/unit/test_env_utils.py

# Run API-related tests
pytest tests/unit/test_api_status_collector.py tests/integration/test_api_integration.py

# Run disk metrics tests
pytest tests/unit/test_resource_metrics_collector.py tests/integration/test_resource_metrics_integration.py
```

## Writing New Tests

1. Create a new test file in the appropriate directory (unit/ or integration/)
2. Name your file with the prefix `test_`
3. Ensure your test classes inherit from `unittest.TestCase`
4. Ensure your test methods start with `test_`

Example:

```python
import unittest

class TestMyFeature(unittest.TestCase):
    
    def test_some_functionality(self):
        # Test code here
        self.assertTrue(True)
```

## Test Coverage

Test coverage is tracked using pytest-cov. To generate a coverage report:

```bash
pytest --cov=. --cov-report=html tests/
```

This will create an HTML report in the `htmlcov/` directory that you can open in a browser.