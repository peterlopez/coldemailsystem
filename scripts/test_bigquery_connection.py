#!/usr/bin/env python3
"""
Test BigQuery connection and check for existing tables
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
from google.oauth2 import service_account
from config import config

def test_bigquery_connection():
    """Test connection to BigQuery and check for specific table"""
    print("Testing BigQuery Connection")
    print("=" * 50)
    
    try:
        # Load credentials
        credentials = service_account.Credentials.from_service_account_file(
            config.google_credentials_path
        )
        
        # Initialize BigQuery client
        client = bigquery.Client(
            project=config.gcp_project_id,
            credentials=credentials
        )
        
        print(f"✓ Connected to project: {config.gcp_project_id}")
        
        # Test connection by listing datasets
        print("\nListing available datasets:")
        datasets = list(client.list_datasets())
        for dataset in datasets:
            print(f"  - {dataset.dataset_id}")
        
        # Check for specific table
        table_id = "instant-ground-394115.email_analytics.storeleads"
        print(f"\nChecking for table: {table_id}")
        
        try:
            table = client.get_table(table_id)
            print(f"✓ Table found: {table_id}")
            print(f"  - Total rows: {table.num_rows}")
            print(f"  - Created: {table.created}")
            print(f"  - Schema fields: {len(table.schema)}")
            
            # Show schema
            print("\n  Schema:")
            for field in table.schema[:10]:  # Show first 10 fields
                print(f"    - {field.name} ({field.field_type})")
            if len(table.schema) > 10:
                print(f"    ... and {len(table.schema) - 10} more fields")
                
        except Exception as e:
            print(f"✗ Table not found: {table_id}")
            print(f"  Error: {str(e)}")
            
    except Exception as e:
        print(f"✗ Connection error: {str(e)}")
        return 1
    
    print("\n" + "=" * 50)
    print("✓ BigQuery connection test completed!")
    return 0

if __name__ == "__main__":
    sys.exit(test_bigquery_connection())