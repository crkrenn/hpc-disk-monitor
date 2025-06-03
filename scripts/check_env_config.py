#!/usr/bin/env python3
"""
Script to check if all required environment variables are being properly included
in both cron and launchd configurations.
"""
import os
import sys
from pathlib import Path
import plistlib
import subprocess
import tempfile

# Add the parent directory to the path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from scripts.manage_cron import ENV_EXPORTS, CRON_SCHEDULE, DISK_SAMPLING_MINUTES
from common.env_utils import preprocess_env

def main():
    # Initialize environment
    preprocess_env()
    
    print("=== Environment Variable Configuration Checker ===")
    
    # Check if all .env variables are in ENV_EXPORTS
    required_variables = [
        "FILESYSTEM_PATHS", "FILESYSTEM_LABELS", "DISK_STATS_DB",
        "DISK_SAMPLING_MINUTES", "DASH_REFRESH_SECONDS"
    ]
    
    print("\n1. Checking if all required variables are exported:")
    all_exported = True
    for var in required_variables:
        if var in ENV_EXPORTS:
            print(f"  ✅ {var} is included in exports")
        else:
            print(f"  ❌ {var} is missing from exports")
            all_exported = False
    
    if all_exported:
        print("\n✅ All required variables are included in exports!")
    else:
        print("\n❌ Some required variables are missing!")
    
    # Check cron string
    print("\n2. Checking cron schedule:")
    expected_schedule = f"*/{DISK_SAMPLING_MINUTES} * * * *"
    if CRON_SCHEDULE == expected_schedule:
        print(f"  ✅ Cron schedule is correct: {CRON_SCHEDULE}")
    else:
        print(f"  ❌ Cron schedule mismatch!")
        print(f"     Expected: {expected_schedule}")
        print(f"     Actual:   {CRON_SCHEDULE}")
    
    # Generate cron entry and check if all variables are included
    print("\n3. Generating cron entry for testing:")
    env_str = " ".join(f"{k}={v}" for k, v in ENV_EXPORTS.items())
    cron_entry = f"{CRON_SCHEDULE} {env_str} python script.py"
    print(f"  {cron_entry}")
    
    for var in required_variables:
        if f"{var}=" in cron_entry:
            print(f"  ✅ {var} is in cron entry")
        else:
            print(f"  ❌ {var} is missing from cron entry")
    
    # Generate launchd plist and check if all variables are included
    print("\n4. Generating launchd plist for testing:")
    try:
        # Import the function from manage_cron
        from scripts.manage_cron import _make_launchd_plist_dict
        
        # Generate plist dict
        plist_dict = _make_launchd_plist_dict()
        
        # Check if all variables are included
        env_vars_in_plist = plist_dict.get('EnvironmentVariables', {})
        for var in required_variables:
            if var in env_vars_in_plist:
                print(f"  ✅ {var} is in launchd plist")
            else:
                print(f"  ❌ {var} is missing from launchd plist")
        
        # Check StartInterval
        expected_interval = DISK_SAMPLING_MINUTES * 60
        if plist_dict.get('StartInterval') == expected_interval:
            print(f"  ✅ StartInterval is correct: {plist_dict.get('StartInterval')} seconds")
        else:
            print(f"  ❌ StartInterval mismatch!")
            print(f"     Expected: {expected_interval} seconds")
            print(f"     Actual:   {plist_dict.get('StartInterval')} seconds")
        
        # Write plist to a temp file for inspection
        with tempfile.NamedTemporaryFile(suffix='.plist', delete=False) as tmp:
            plistlib.dump(plist_dict, tmp)
            tmp_path = tmp.name
        
        print(f"\nTest plist written to: {tmp_path}")
        print("You can inspect this file to verify all settings.")
    
    except Exception as e:
        print(f"Error generating plist: {e}")

if __name__ == "__main__":
    main()