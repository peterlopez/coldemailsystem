#!/usr/bin/env python3
"""
Add last_drain_check column to ops_inst_state table for time-based drain filtering.
Run this once to update the database schema.
"""

import os
from google.cloud import bigquery
from google.oauth2 import service_account

def add_drain_timestamp_column():
    """Add last_drain_check column to ops_inst_state table."""
    
    # Setup BigQuery credentials
    credentials_path = "config/secrets/bigquery-credentials.json"
    if os.path.exists(credentials_path):
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    else:
        print("❌ BigQuery credentials not found. Make sure config/secrets/bigquery-credentials.json exists.")
        return False
    
    # Define the ALTER TABLE query
    query = """
    ALTER TABLE `instant-ground-394115.email_analytics.ops_inst_state`
    ADD COLUMN IF NOT EXISTS last_drain_check TIMESTAMP
    """
    
    try:
        print("🔧 Adding last_drain_check column to ops_inst_state table...")
        
        # Execute the query
        query_job = client.query(query)
        query_job.result()  # Wait for completion
        
        print("✅ Successfully added last_drain_check column")
        
        # Verify the column was added
        verify_query = """
        SELECT column_name, data_type 
        FROM `instant-ground-394115.email_analytics.INFORMATION_SCHEMA.COLUMNS` 
        WHERE table_name = 'ops_inst_state' AND column_name = 'last_drain_check'
        """
        
        verify_job = client.query(verify_query)
        results = list(verify_job.result())
        
        if results:
            print(f"✅ Column verified: {results[0].column_name} ({results[0].data_type})")
            return True
        else:
            print("⚠️ Column may not have been created - verification failed")
            return False
            
    except Exception as e:
        print(f"❌ Failed to add column: {e}")
        return False

if __name__ == "__main__":
    print("📊 BigQuery Schema Update: Adding Drain Timestamp Column")
    print("=" * 60)
    
    success = add_drain_timestamp_column()
    
    if success:
        print("\n🚀 Schema update complete! The drain system can now use time-based filtering.")
    else:
        print("\n❌ Schema update failed. Please check the error messages above.")