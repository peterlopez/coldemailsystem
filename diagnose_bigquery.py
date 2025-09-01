#!/usr/bin/env python3
"""
BigQuery Diagnostics Script

Comprehensive diagnostics to check BigQuery setup before running schema updates.
Verifies permissions, tables, views, and data availability.

Usage:
    python diagnose_bigquery.py [--verbose]
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_bigquery_import():
    """Test if BigQuery library can be imported."""
    try:
        from google.cloud import bigquery
        logger.info("✅ Google Cloud BigQuery library imported successfully")
        return True, bigquery
    except ImportError as e:
        logger.error(f"❌ Cannot import BigQuery library: {e}")
        return False, None

def test_credentials():
    """Test BigQuery credentials and client initialization."""
    try:
        # Check if credentials file exists
        creds_path = 'config/secrets/bigquery-credentials.json'
        if not os.path.exists(creds_path):
            logger.error(f"❌ Credentials file not found: {creds_path}")
            return False, None
            
        logger.info(f"✅ Credentials file exists: {creds_path}")
        
        # Set environment variable
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
        
        # Test client initialization
        from google.cloud import bigquery
        client = bigquery.Client()
        
        # Test basic API call
        project_id = client.project
        logger.info(f"✅ BigQuery client initialized successfully")
        logger.info(f"📋 Project ID: {project_id}")
        
        return True, client
        
    except Exception as e:
        logger.error(f"❌ BigQuery client initialization failed: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        return False, None

def check_dataset_access(client, project_id: str, dataset_id: str):
    """Check if we can access the specified dataset."""
    try:
        dataset_ref = f"{project_id}.{dataset_id}"
        dataset = client.get_dataset(dataset_ref)
        
        logger.info(f"✅ Dataset accessible: {dataset_ref}")
        logger.info(f"📊 Created: {dataset.created}")
        logger.info(f"📊 Location: {dataset.location}")
        logger.info(f"📊 Description: {dataset.description or 'None'}")
        
        return True, dataset
        
    except Exception as e:
        logger.error(f"❌ Cannot access dataset {project_id}.{dataset_id}: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        return False, None

def list_tables_in_dataset(client, project_id: str, dataset_id: str):
    """List all tables and views in the dataset."""
    try:
        dataset_ref = f"{project_id}.{dataset_id}"
        tables = list(client.list_tables(dataset_ref))
        
        if not tables:
            logger.warning(f"⚠️ No tables found in dataset {dataset_ref}")
            return []
        
        logger.info(f"📋 Found {len(tables)} tables/views in {dataset_ref}:")
        
        table_info = []
        for table in tables:
            table_type = "VIEW" if table.table_type == "VIEW" else "TABLE"
            logger.info(f"   • {table.table_id} ({table_type})")
            table_info.append({
                "table_id": table.table_id,
                "table_type": table_type,
                "full_table_id": table.full_table_id
            })
        
        return table_info
        
    except Exception as e:
        logger.error(f"❌ Cannot list tables in {project_id}.{dataset_id}: {e}")
        return []

def check_specific_table(client, project_id: str, dataset_id: str, table_id: str):
    """Check if a specific table/view exists and get its schema."""
    try:
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        table = client.get_table(table_ref)
        
        table_type = "VIEW" if table.table_type == "VIEW" else "TABLE"
        logger.info(f"✅ {table_type} exists: {table_id}")
        logger.info(f"📊 Rows: {table.num_rows if table.num_rows is not None else 'N/A (VIEW)'}")
        logger.info(f"📊 Schema fields: {len(table.schema)}")
        
        # Show schema for important tables
        if table_id in ['ops_inst_state', 'v_ready_for_instantly']:
            logger.info(f"📋 Schema for {table_id}:")
            for field in table.schema[:10]:  # Show first 10 fields
                mode = f" ({field.mode})" if field.mode != "NULLABLE" else ""
                description = f" - {field.description}" if field.description else ""
                logger.info(f"   • {field.name}: {field.field_type}{mode}{description}")
            
            if len(table.schema) > 10:
                logger.info(f"   ... and {len(table.schema) - 10} more fields")
        
        return True, table
        
    except Exception as e:
        logger.error(f"❌ Table/view {table_id} not accessible: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        return False, None

def test_query_permissions(client, project_id: str, dataset_id: str):
    """Test if we can run queries on the dataset."""
    try:
        # Simple test query
        test_query = f"""
        SELECT 
            table_name,
            table_type,
            creation_time
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        WHERE table_schema = '{dataset_id}'
        ORDER BY creation_time DESC
        LIMIT 5
        """
        
        logger.info("🔍 Testing query permissions with INFORMATION_SCHEMA...")
        query_job = client.query(test_query)
        results = list(query_job.result())
        
        logger.info(f"✅ Query executed successfully, found {len(results)} tables")
        for row in results:
            logger.info(f"   • {row.table_name} ({row.table_type}) - {row.creation_time}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Query permission test failed: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        return False

def check_ready_for_instantly_view(client, project_id: str, dataset_id: str):
    """Check the critical v_ready_for_instantly view."""
    try:
        view_ref = f"{project_id}.{dataset_id}.v_ready_for_instantly"
        
        # Test if view exists
        exists, view = check_specific_table(client, project_id, dataset_id, "v_ready_for_instantly")
        if not exists:
            return False
        
        # Test a simple query on the view
        test_query = f"""
        SELECT COUNT(*) as eligible_count
        FROM `{view_ref}`
        LIMIT 1
        """
        
        logger.info("🔍 Testing v_ready_for_instantly view query...")
        query_job = client.query(test_query)
        results = list(query_job.result())
        
        if results:
            count = results[0].eligible_count
            logger.info(f"✅ v_ready_for_instantly query successful: {count:,} eligible leads")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ v_ready_for_instantly view test failed: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        return False

def check_ops_tables(client, project_id: str, dataset_id: str):
    """Check operational tables that the system needs."""
    required_tables = [
        "ops_inst_state",
        "ops_lead_history", 
        "ops_dead_letters",
        "config"
    ]
    
    logger.info("🔍 Checking required operational tables...")
    
    results = {}
    for table_id in required_tables:
        exists, table = check_specific_table(client, project_id, dataset_id, table_id)
        results[table_id] = exists
        
        if exists and table_id == "ops_inst_state":
            # Check for verification fields
            schema_fields = [field.name for field in table.schema]
            verification_fields = [
                "verification_status",
                "verification_catch_all", 
                "verification_credits_used",
                "verified_at"
            ]
            
            logger.info("🔍 Checking for async verification fields in ops_inst_state:")
            for field in verification_fields:
                if field in schema_fields:
                    logger.info(f"   ✅ {field} - exists")
                else:
                    logger.warning(f"   ❌ {field} - missing (will need schema update)")
    
    return results

def check_service_account_info(client):
    """Try to get information about the service account being used."""
    try:
        # This is a bit tricky since the client doesn't directly expose SA info
        # We'll try to infer it from a simple query
        query = "SELECT @@version as version"
        query_job = client.query(query)
        results = list(query_job.result())
        
        if results:
            logger.info(f"✅ Service account can execute queries")
            logger.info(f"📊 BigQuery version: {results[0].version}")
        
        # Try to get job statistics which might reveal more info
        logger.info(f"📊 Last query job ID: {query_job.job_id}")
        logger.info(f"📊 Query project: {query_job.project}")
        
        return True
        
    except Exception as e:
        logger.warning(f"⚠️ Cannot get service account details: {e}")
        return False

def main():
    """Main diagnostic function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Diagnose BigQuery setup')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    print("🔍 BIGQUERY DIAGNOSTICS")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    
    # Configuration
    project_id = "instant-ground-394115"
    dataset_id = "email_analytics"
    
    print(f"📋 Target Configuration:")
    print(f"   Project ID: {project_id}")
    print(f"   Dataset ID: {dataset_id}")
    print()
    
    # Test 1: BigQuery library import
    print("1️⃣ Testing BigQuery Library Import")
    success, bigquery_module = test_bigquery_import()
    if not success:
        print("❌ Cannot continue without BigQuery library")
        return 1
    print()
    
    # Test 2: Credentials and client
    print("2️⃣ Testing Credentials and Client")
    success, client = test_credentials()
    if not success:
        print("❌ Cannot continue without valid credentials")
        return 1
    print()
    
    # Test 3: Dataset access
    print("3️⃣ Testing Dataset Access")
    success, dataset = check_dataset_access(client, project_id, dataset_id)
    if not success:
        print("❌ Cannot access target dataset")
        return 1
    print()
    
    # Test 4: List tables
    print("4️⃣ Listing Tables and Views")
    tables = list_tables_in_dataset(client, project_id, dataset_id)
    print()
    
    # Test 5: Query permissions
    print("5️⃣ Testing Query Permissions")
    success = test_query_permissions(client, project_id, dataset_id)
    if not success:
        print("⚠️ Limited query permissions detected")
    print()
    
    # Test 6: Check critical view
    print("6️⃣ Testing v_ready_for_instantly View")
    success = check_ready_for_instantly_view(client, project_id, dataset_id)
    if not success:
        print("⚠️ Critical view not accessible")
    print()
    
    # Test 7: Check operational tables
    print("7️⃣ Checking Operational Tables")
    ops_results = check_ops_tables(client, project_id, dataset_id)
    print()
    
    # Test 8: Service account info
    print("8️⃣ Service Account Information")
    check_service_account_info(client)
    print()
    
    # Summary
    print("📊 DIAGNOSTIC SUMMARY")
    print("=" * 30)
    
    critical_checks = [
        ("BigQuery library", True),  # We got this far
        ("Credentials", True),       # We got this far  
        ("Dataset access", True),    # We got this far
        ("Query permissions", success),
        ("v_ready_for_instantly view", check_ready_for_instantly_view(client, project_id, dataset_id)),
    ]
    
    all_passed = True
    for check_name, passed in critical_checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} {check_name}")
        if not passed:
            all_passed = False
    
    print()
    
    if all_passed:
        print("🎯 All critical checks passed! System should work correctly.")
        return 0
    else:
        print("⚠️ Some checks failed. Review the output above for details.")
        print("💡 You may need to run schema updates or fix permissions.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)