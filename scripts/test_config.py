#!/usr/bin/env python3
"""
Test configuration setup for Cold Email Pipeline
Run this to verify all credentials and settings are properly configured
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config

def test_configuration():
    """Test that all required configuration is present and valid"""
    print("Cold Email Pipeline Configuration Test")
    print("=" * 50)
    
    # Check configuration validation
    validation = config.validate()
    
    print("\nConfiguration Status:")
    for key, is_valid in validation.items():
        status = "✓" if is_valid else "✗"
        print(f"  {status} {key}")
    
    print("\nConfiguration Details:")
    print(f"  Project ID: {config.gcp_project_id or 'NOT SET'}")
    print(f"  BigQuery Dataset: {config.bigquery_dataset}")
    print(f"  BigQuery Location: {config.bigquery_location}")
    print(f"  Instantly API URL: {config.instantly_base_url}")
    print(f"  Batch Size: {config.batch_size}")
    print(f"  Sync Interval: {config.sync_interval_hours} hours")
    
    # Check for API key presence (don't print the actual key)
    print(f"\n  Instantly API Key: {'SET' if config.instantly_api_key else 'NOT SET'}")
    
    # Overall status
    all_valid = all(validation.values())
    print("\n" + "=" * 50)
    if all_valid:
        print("✓ All configuration checks passed!")
        print("\nNext steps:")
        print("1. Run 'python scripts/create_bigquery_tables.py' to set up BigQuery")
        print("2. Run 'python scripts/test_instantly_api.py' to test Instantly connection")
    else:
        print("✗ Configuration incomplete. Please check the items marked with ✗ above.")
        print("\nRefer to config/secrets/README.md for setup instructions.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(test_configuration())