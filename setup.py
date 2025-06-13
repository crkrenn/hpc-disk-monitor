#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name="hpc-resource-monitor",
    version="0.1.0",
    description="HPC Resource Monitoring Tool",
    author="crkrenn",
    packages=find_packages(),
    install_requires=[
        "python-dotenv",
        "tabulate",
        "pandas",
        "requests",
    ],
    python_requires=">=3.6",
    entry_points={
        "console_scripts": [
            "resource-monitor=scripts.resource_metrics_collector:main",
            "resource-summary=scripts.db_summary:main",
        ],
    },
    test_suite="tests",
)