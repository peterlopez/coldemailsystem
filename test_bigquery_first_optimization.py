#!/usr/bin/env python3
"""
Test the BigQuery-first optimization that might be filtering out our completed leads.
This could be the root cause - if BigQuery shows them as 'active' but Instantly shows them as 'completed',
the BigQuery-first optimization might be working correctly but not finding the right leads.
"""

import os
import sys
from google.cloud import bigquery
from datetime import datetime, timezone
import json

# Add shared directory to path
sys.path.append('shared')

def get_bigquery_client():
    """Initialize BigQuery client."""
    credentials_path = './config/secrets/bigquery-credentials.json'
    project_id = "instant-ground-394115"
    
    if os.path.exists(credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        client = bigquery.Client(project=project_id)
        return client
    else:
        raise FileNotFoundError(f"BigQuery credentials not found at {credentials_path}")

def test_bigquery_first_optimization():
    """Test the exact BigQuery query used by get_leads_needing_drain_from_bigquery."""
    
    print("🔍 TESTING BIGQUERY-FIRST OPTIMIZATION")
    print("=" * 60)
    
    client = get_bigquery_client()
    
    SMB_CAMPAIGN_ID = "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"
    MIDSIZE_CAMPAIGN_ID = "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"
    PROJECT_ID = "instant-ground-394115"
    DATASET_ID = "email_analytics"
    
    # This is the EXACT query from get_leads_needing_drain_from_bigquery
    query = f"""
    SELECT 
        instantly_lead_id,
        campaign_id,
        email,
        status
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE (
        last_drain_check IS NULL 
        OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) >= 24
    )
    AND status IN ('active', 'pending')  -- Only check leads that might still be in campaigns
    AND campaign_id IN ('{SMB_CAMPAIGN_ID}', '{MIDSIZE_CAMPAIGN_ID}')
    ORDER BY 
        COALESCE(last_drain_check, TIMESTAMP('1970-01-01')) ASC,  -- Oldest checks first
        email ASC  -- Deterministic ordering
    LIMIT 1000  -- Reasonable limit to avoid overwhelming the system
    """
    
    print("🔍 Running BigQuery optimization query...")
    print("📋 This query looks for leads that:")
    print("   • Haven't been drain-checked in 24+ hours")  
    print("   • Have status 'active' or 'pending' in BigQuery")
    print("   • Are in our target campaigns")
    print()
    
    results = client.query(query).result()
    
    # Our target completed leads
    target_emails = [
        "care@nuropod.com",
        "info@luxxformen.com", 
        "contact.pufii.ro@gmail.com",
        "info@orchard-house.jp",
        "info@ladesignconcepts.com",
        "bagandwag@outlook.com",
        "ciaobellahair@gmail.com",
        "ventas@masx.cl",
        "hello@millesimebaby.com",
        "contact@yves-jardin.com"
    ]
    
    found_targets = []
    total_results = 0
    smb_count = 0
    midsize_count = 0
    
    for row in results:
        total_results += 1
        
        if row.campaign_id == SMB_CAMPAIGN_ID:
            smb_count += 1
        else:
            midsize_count += 1
            
        if row.email in target_emails:
            found_targets.append({
                'email': row.email,
                'lead_id': row.instantly_lead_id,
                'campaign_id': row.campaign_id,
                'bigquery_status': row.status
            })
            print(f"✅ FOUND TARGET: {row.email}")
            print(f"   Campaign: {'SMB' if row.campaign_id == SMB_CAMPAIGN_ID else 'Midsize'}")
            print(f"   BigQuery Status: {row.status}")
            print(f"   Lead ID: {row.instantly_lead_id}")
            print()
    
    print("=" * 60)
    print("📊 BIGQUERY OPTIMIZATION RESULTS:")
    print("=" * 60)
    print(f"📧 Total leads found by optimization: {total_results}")
    print(f"   • SMB campaign: {smb_count} leads")
    print(f"   • Midsize campaign: {midsize_count} leads")
    print(f"🎯 Target completed leads found: {len(found_targets)}/{len(target_emails)}")
    
    if len(found_targets) == len(target_emails):
        print(f"✅ SUCCESS: All target leads found by BigQuery optimization!")
        print(f"   This means the BigQuery-first optimization should work correctly.")
        print(f"   The issue must be elsewhere in the drain pipeline.")
    elif len(found_targets) > 0:
        print(f"⚠️  PARTIAL: Only {len(found_targets)} out of {len(target_emails)} target leads found.")
        print(f"   Missing targets may have different BigQuery status.")
    else:
        print(f"❌ FAILURE: No target leads found by BigQuery optimization!")
        print(f"   This could be the root cause - BigQuery filter may be too restrictive.")
        
        # Check what status the missing leads have
        print(f"\n🔍 CHECKING STATUS OF MISSING LEADS:")
        missing_emails = [email for email in target_emails if email not in [t['email'] for t in found_targets]]
        
        if missing_emails:
            email_list = "', '".join(missing_emails)
            status_query = f"""
            SELECT email, status, campaign_id, last_drain_check
            FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
            WHERE email IN ('{email_list}')
            """
            
            status_results = client.query(status_query).result()
            for row in status_results:
                print(f"   📧 {row.email}: status='{row.status}', campaign='{row.campaign_id}'")
                if row.status not in ['active', 'pending']:
                    print(f"      ❌ FILTERED OUT: Status '{row.status}' not in ('active', 'pending')")
    
    return found_targets, total_results

def test_alternative_optimization_query():
    """Test a modified query that might catch completed leads."""
    
    print(f"\n" + "="*60)
    print("🔧 TESTING ALTERNATIVE OPTIMIZATION QUERY")
    print("="*60)
    
    client = get_bigquery_client()
    
    SMB_CAMPAIGN_ID = "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"
    MIDSIZE_CAMPAIGN_ID = "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"
    PROJECT_ID = "instant-ground-394115"
    DATASET_ID = "email_analytics"
    
    # Modified query that doesn't filter by status
    query = f"""
    SELECT 
        instantly_lead_id,
        campaign_id,
        email,
        status
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE (
        last_drain_check IS NULL 
        OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) >= 24
    )
    AND campaign_id IN ('{SMB_CAMPAIGN_ID}', '{MIDSIZE_CAMPAIGN_ID}')
    -- REMOVED: AND status IN ('active', 'pending') filter
    ORDER BY 
        COALESCE(last_drain_check, TIMESTAMP('1970-01-01')) ASC,
        email ASC
    LIMIT 1000
    """
    
    print("🔍 Running modified optimization query (no status filter)...")
    print("📋 This query removes the status filter to catch all leads needing evaluation")
    print()
    
    results = client.query(query).result()
    
    target_emails = [
        "care@nuropod.com",
        "info@luxxformen.com", 
        "contact.pufii.ro@gmail.com"  # Just test first 3
    ]
    
    found_targets = []
    for row in results:
        if row.email in target_emails:
            found_targets.append({
                'email': row.email,
                'bigquery_status': row.status
            })
            print(f"✅ FOUND: {row.email} (status: {row.status})")
    
    print(f"\n📊 Modified query found {len(found_targets)}/{len(target_emails)} target leads")
    
    if len(found_targets) > 0:
        print(f"💡 INSIGHT: Removing status filter helps find the completed leads!")
        print(f"   This suggests BigQuery status is out of sync with Instantly status.")

def main():
    try:
        found_targets, total_results = test_bigquery_first_optimization()
        
        if len(found_targets) < 10:  # If we didn't find all targets
            test_alternative_optimization_query()
        
        print(f"\n" + "="*60)
        print("🎯 FINAL DIAGNOSIS:")
        print("="*60)
        
        if len(found_targets) == 10:
            print("✅ BigQuery optimization should work - issue is elsewhere in drain pipeline")
            print("💡 Check if process_bigquery_first_drain is working correctly")
        else:
            print("❌ BigQuery optimization is filtering out completed leads")
            print("💡 Root cause: BigQuery status filter AND status IN ('active', 'pending')")
            print("💡 These leads show 'active' in BigQuery but 'completed' in Instantly")
            print("💡 Solution: Remove or modify the status filter in BigQuery optimization")
        
    except Exception as e:
        print(f"❌ Error during investigation: {e}")

if __name__ == "__main__":
    main()