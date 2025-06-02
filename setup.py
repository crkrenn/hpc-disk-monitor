#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name="hpc-disk-monitor",
    version="0.1.0",
    description="HPC Disk I/O Monitoring Tool",
    author="crkrenn",
    packages=find_packages(),
    install_requires=[
        "python-dotenv",
        "tabulate",
        "pandas",
    ],
    python_requires=">=3.6",
    entry_points={
        "console_scripts": [
            "disk-monitor=scripts.disk_metrics_collector:main",
            "disk-summary=scripts.db_summary:main",
        ],
    },
    test_suite="tests",
)