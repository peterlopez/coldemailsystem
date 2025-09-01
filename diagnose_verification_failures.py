#!/usr/bin/env python3
"""
Diagnose email verification failures and inventory issues
"""

import os
import sys
from google.cloud import bigquery
from datetime import datetime, timedelta
import json

# Set up BigQuery client
PROJECT_ID = "instant-ground-394115"
DATASET_ID = "email_analytics"

# Use credentials file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
bq_client = bigquery.Client(project=PROJECT_ID)

def check_verification_results():
    """Check recent email verification results in BigQuery"""
    print("📧 CHECKING EMAIL VERIFICATION RESULTS")
    print("=" * 80)
    
    # Check verification status distribution (last 24 hours)
    query = """
    SELECT 
        verification_status,
        COUNT(*) as count,
        MIN(verified_at) as earliest,
        MAX(verified_at) as latest
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE verified_at IS NOT NULL 
        AND verified_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    GROUP BY verification_status
    ORDER BY count DESC
    """
    
    print("\nVerification Status Distribution (Last 24h):")
    print("-" * 60)
    
    results = bq_client.query(query).result()
    total_verified = 0
    for row in results:
        total_verified += row.count
        print(f"  Status: {row.verification_status:<20} Count: {row.count:>6}")
        print(f"    Earliest: {row.earliest}")
        print(f"    Latest: {row.latest}")
        print()
    
    print(f"Total Verified: {total_verified}")
    
    # Check some specific failed verifications
    print("\n📋 SAMPLE FAILED VERIFICATIONS:")
    print("-" * 60)
    
    failed_query = """
    SELECT 
        email,
        verification_status,
        verified_at
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE verification_status NOT IN ('valid', 'accept_all')
        AND verified_at IS NOT NULL
        AND verified_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    ORDER BY verified_at DESC
    LIMIT 10
    """
    
    results = bq_client.query(failed_query).result()
    for row in results:
        print(f"  {row.email:<40} Status: {row.verification_status:<15} Time: {row.verified_at}")

def check_failed_lead_destination():
    """Check what happens to leads that fail verification"""
    print("\n\n🔍 CHECKING FAILED LEAD DESTINATION")
    print("=" * 80)
    
    # Check if failed leads remain in ready view
    query = """
    WITH recent_failures AS (
        SELECT DISTINCT email
        FROM `instant-ground-394115.email_analytics.ops_inst_state`
        WHERE verification_status NOT IN ('valid', 'accept_all', '')
            AND verification_status IS NOT NULL
            AND verified_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    )
    SELECT COUNT(*) as still_eligible_count
    FROM `instant-ground-394115.email_analytics.v_ready_for_instantly` r
    JOIN recent_failures f ON r.email = f.email
    """
    
    result = bq_client.query(query).result()
    still_eligible = next(result).still_eligible_count
    
    print(f"Failed verification leads still showing as eligible: {still_eligible}")
    
    # Check the ops_inst_state for failed verifications
    print("\n📊 OPS_INST_STATE Status for Failed Verifications:")
    state_query = """
    SELECT 
        status,
        COUNT(*) as count
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE verification_status NOT IN ('valid', 'accept_all', '')
        AND verification_status IS NOT NULL
    GROUP BY status
    ORDER BY count DESC
    """
    
    results = bq_client.query(state_query).result()
    for row in results:
        print(f"  Status: {row.status:<20} Count: {row.count:>6}")

def check_inventory_accuracy():
    """Compare BigQuery tracked inventory vs actual Instantly inventory"""
    print("\n\n📦 CHECKING INVENTORY ACCURACY")
    print("=" * 80)
    
    # BigQuery tracked inventory
    bq_query = """
    SELECT 
        status,
        COUNT(*) as count
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    GROUP BY status
    ORDER BY count DESC
    """
    
    print("BigQuery Tracked Status Distribution:")
    print("-" * 60)
    results = bq_client.query(bq_query).result()
    bq_active = 0
    for row in results:
        print(f"  {row.status:<20}: {row.count:>8}")
        if row.status == 'active':
            bq_active = row.count
    
    print(f"\nBigQuery 'active' count: {bq_active}")
    print("\n⚠️  This is tracking leads added but NOT reflecting actual Instantly inventory!")
    print("   The drain process may not be updating statuses properly.")
    
    # Check for leads that should have been drained
    drain_check_query = """
    SELECT COUNT(*) as should_be_drained
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE status = 'active'
        AND added_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    """
    
    result = bq_client.query(drain_check_query).result()
    should_drain = next(result).should_be_drained
    print(f"\nLeads marked 'active' for >7 days (likely stale): {should_drain}")

def check_api_responses():
    """Check recent API errors in dead letters"""
    print("\n\n❌ CHECKING RECENT API ERRORS")
    print("=" * 80)
    
    error_query = """
    SELECT 
        phase,
        error_text,
        COUNT(*) as count,
        MAX(occurred_at) as latest
    FROM `instant-ground-394115.email_analytics.ops_dead_letters`
    WHERE occurred_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    GROUP BY phase, error_text
    ORDER BY count DESC
    LIMIT 10
    """
    
    results = bq_client.query(error_query).result()
    found_errors = False
    for row in results:
        found_errors = True
        print(f"\nPhase: {row.phase}")
        print(f"Error: {row.error_text[:100]}...")
        print(f"Count: {row.count}")
        print(f"Latest: {row.latest}")
    
    if not found_errors:
        print("No recent errors found in dead letters")

def check_verification_api_response():
    """Test the verification API directly"""
    print("\n\n🧪 TESTING VERIFICATION API")
    print("=" * 80)
    
    # Import the sync module to test verification
    try:
        from sync_once import verify_email, INSTANTLY_API_KEY
        
        # Test with a known good email
        test_email = "test@example.com"
        print(f"Testing verification for: {test_email}")
        
        result = verify_email(test_email)
        print(f"Result: {json.dumps(result, indent=2)}")
        
    except Exception as e:
        print(f"Failed to test verification: {e}")

if __name__ == "__main__":
    check_verification_results()
    check_failed_lead_destination()
    check_inventory_accuracy()
    check_api_responses()
    # check_verification_api_response()  # Commented out to avoid API calls