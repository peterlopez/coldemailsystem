#!/usr/bin/env python3
"""
Quick BigQuery Permissions Checker

Lightweight script to quickly check if BigQuery access is working
without running the full diagnostics suite.
"""

import os
import sys

def quick_check():
    """Quick check of BigQuery access."""
    print("🔍 Quick BigQuery Access Check")
    print("=" * 40)
    
    try:
        # Check credentials file
        creds_path = 'config/secrets/bigquery-credentials.json'
        if not os.path.exists(creds_path):
            print(f"❌ Credentials file missing: {creds_path}")
            return False
        
        print(f"✅ Credentials file found: {creds_path}")
        
        # Set environment
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
        
        # Test import and client
        from google.cloud import bigquery
        client = bigquery.Client()
        
        project_id = client.project
        print(f"✅ BigQuery client initialized")
        print(f"📋 Project: {project_id}")
        
        # Quick query test
        query = "SELECT 1 as test"
        job = client.query(query)
        results = list(job.result())
        
        if results and results[0].test == 1:
            print("✅ Basic query execution works")
        
        # Test dataset access
        dataset_ref = f"{project_id}.email_analytics"
        try:
            dataset = client.get_dataset(dataset_ref)
            print(f"✅ Can access dataset: {dataset_ref}")
        except Exception as e:
            print(f"❌ Cannot access dataset: {e}")
            return False
        
        # Test table listing
        try:
            tables = list(client.list_tables(dataset_ref))
            print(f"✅ Can list tables: {len(tables)} found")
        except Exception as e:
            print(f"❌ Cannot list tables: {e}")
            return False
        
        print()
        print("🎯 Quick check PASSED - BigQuery access appears to be working")
        return True
        
    except Exception as e:
        print(f"❌ Quick check FAILED: {e}")
        print(f"Error type: {type(e).__name__}")
        return False

if __name__ == "__main__":
    success = quick_check()
    sys.exit(0 if success else 1)