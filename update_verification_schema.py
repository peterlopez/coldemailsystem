#!/usr/bin/env python3
"""
Update BigQuery Schema for Async Email Verification

This script adds the necessary fields to support async email verification tracking.
Ensures that the ops_inst_state table has all required verification fields.

New fields added:
- verification_status: The final verification result (valid, invalid, risky, accept_all, pending)
- verification_catch_all: Boolean indicating if domain is catch-all
- verification_credits_used: Number of credits consumed for verification  
- verified_at: Timestamp when verification was completed
"""

import os
import json
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_bigquery_client():
    """Get authenticated BigQuery client."""
    credentials_path = 'config/secrets/bigquery-credentials.json'
    if os.path.exists(credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    return bigquery.Client()

def update_ops_inst_state_schema():
    """Add verification fields to ops_inst_state table if they don't exist."""
    
    client = get_bigquery_client()
    table_id = "instant-ground-394115.email_analytics.ops_inst_state"
    
    try:
        # Get current table schema
        table = client.get_table(table_id)
        current_schema = table.schema
        
        logger.info(f"📋 Current table schema has {len(current_schema)} fields")
        
        # Define new fields to add
        new_fields = [
            bigquery.SchemaField("verification_status", "STRING", mode="NULLABLE",
                                description="Email verification status from Instantly (valid, invalid, risky, accept_all, pending)"),
            bigquery.SchemaField("verification_catch_all", "BOOLEAN", mode="NULLABLE", 
                                description="Whether domain is catch-all according to verification"),
            bigquery.SchemaField("verification_credits_used", "INTEGER", mode="NULLABLE",
                                description="Number of verification credits consumed"),
            bigquery.SchemaField("verified_at", "TIMESTAMP", mode="NULLABLE",
                                description="When email verification was completed"),
        ]
        
        # Check which fields already exist
        existing_field_names = {field.name for field in current_schema}
        fields_to_add = []
        
        for field in new_fields:
            if field.name not in existing_field_names:
                fields_to_add.append(field)
                logger.info(f"➕ Will add field: {field.name} ({field.field_type})")
            else:
                logger.info(f"✅ Field already exists: {field.name}")
        
        if not fields_to_add:
            logger.info("✅ All verification fields already exist in the table")
            return True
        
        # Add new fields to schema
        updated_schema = list(current_schema) + fields_to_add
        table.schema = updated_schema
        
        # Update table
        updated_table = client.update_table(table, ["schema"])
        
        logger.info(f"✅ Successfully added {len(fields_to_add)} verification fields to ops_inst_state")
        logger.info(f"📊 Table now has {len(updated_table.schema)} total fields")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating table schema: {e}")
        return False

def create_verification_tracking_view():
    """Create a view for verification tracking and analytics."""
    
    client = get_bigquery_client()
    view_id = "instant-ground-394115.email_analytics.v_verification_tracking"
    
    view_query = """
    SELECT 
        email,
        campaign_id,
        status,
        verification_status,
        verification_catch_all,
        verification_credits_used,
        added_at,
        verified_at,
        updated_at,
        
        -- Verification timing analysis
        DATETIME_DIFF(verified_at, added_at, MINUTE) as verification_time_minutes,
        
        -- Verification success rate calculation  
        CASE 
            WHEN verification_status IN ('valid', 'accept_all') THEN 'sendable'
            WHEN verification_status IN ('invalid', 'risky') THEN 'not_sendable'
            WHEN verification_status = 'pending' THEN 'pending'
            WHEN verification_status IS NULL THEN 'not_verified'
            ELSE 'unknown'
        END as sendable_category,
        
        -- Time since verification
        DATETIME_DIFF(CURRENT_DATETIME(), verified_at, HOUR) as hours_since_verified,
        
        -- Cost analysis
        COALESCE(verification_credits_used, 0) * 0.01 as estimated_verification_cost_usd
        
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE added_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)  -- Last 30 days
    ORDER BY added_at DESC
    """
    
    try:
        # Create or replace the view
        view = bigquery.Table(view_id)
        view.view_query = view_query
        
        # Try to get existing view first
        try:
            existing_view = client.get_table(view_id)
            logger.info("📊 Updating existing verification tracking view...")
            view = client.update_table(view, ["view_query"])
        except NotFound:
            logger.info("📊 Creating new verification tracking view...")
            view = client.create_table(view)
        
        logger.info(f"✅ Verification tracking view created/updated: {view_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating verification tracking view: {e}")
        return False

def test_verification_queries():
    """Test the new verification fields with sample queries."""
    
    client = get_bigquery_client()
    
    test_queries = [
        {
            "name": "Verification Status Breakdown",
            "query": """
            SELECT 
                verification_status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM `instant-ground-394115.email_analytics.ops_inst_state`
            WHERE added_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                AND verification_status IS NOT NULL
            GROUP BY verification_status
            ORDER BY count DESC
            """
        },
        {
            "name": "Verification Credits Usage",
            "query": """
            SELECT 
                DATE(verified_at) as verification_date,
                COUNT(*) as leads_verified,
                SUM(COALESCE(verification_credits_used, 0)) as total_credits,
                ROUND(AVG(verification_credits_used), 2) as avg_credits_per_lead
            FROM `instant-ground-394115.email_analytics.ops_inst_state`
            WHERE verified_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                AND verification_credits_used IS NOT NULL
            GROUP BY verification_date
            ORDER BY verification_date DESC
            """
        },
        {
            "name": "Pending Verifications",
            "query": """
            SELECT 
                COUNT(*) as pending_count,
                MIN(added_at) as oldest_pending,
                MAX(added_at) as newest_pending,
                DATETIME_DIFF(CURRENT_DATETIME(), MIN(added_at), HOUR) as oldest_pending_hours
            FROM `instant-ground-394115.email_analytics.ops_inst_state`
            WHERE verification_status = 'pending' 
                OR (verification_status IS NULL AND added_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
            """
        }
    ]
    
    logger.info("🧪 Testing verification queries...")
    
    for test in test_queries:
        try:
            logger.info(f"\n📊 {test['name']}:")
            query_job = client.query(test['query'])
            results = query_job.result()
            
            for row in results:
                row_data = dict(row)
                logger.info(f"   {row_data}")
                
        except Exception as e:
            logger.error(f"❌ Query '{test['name']}' failed: {e}")

def main():
    """Main function to update schema and create verification tracking."""
    
    print("🔄 UPDATING BIGQUERY SCHEMA FOR ASYNC VERIFICATION")
    print("=" * 60)
    
    # Step 1: Update ops_inst_state table schema
    print("\n1️⃣ Updating ops_inst_state table schema...")
    schema_success = update_ops_inst_state_schema()
    
    if not schema_success:
        print("❌ Schema update failed. Exiting.")
        return False
    
    # Step 2: Create verification tracking view
    print("\n2️⃣ Creating verification tracking view...")
    view_success = create_verification_tracking_view()
    
    if not view_success:
        print("⚠️ View creation failed, but schema was updated successfully.")
    
    # Step 3: Test the new fields
    print("\n3️⃣ Testing verification queries...")
    test_verification_queries()
    
    print("\n✅ SCHEMA UPDATE COMPLETE")
    print("\nNew verification fields added:")
    print("  • verification_status (STRING) - Final verification result")  
    print("  • verification_catch_all (BOOLEAN) - Catch-all domain flag")
    print("  • verification_credits_used (INTEGER) - Credits consumed")
    print("  • verified_at (TIMESTAMP) - Completion timestamp")
    
    print("\nNew verification tracking view:")
    print("  • v_verification_tracking - Analytics and reporting view")
    
    return True

if __name__ == "__main__":
    main()