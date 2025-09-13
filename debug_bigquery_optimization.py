#!/usr/bin/env python3
"""
Debug why the BigQuery optimization isn't finding our leads even though they should qualify.
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

def debug_specific_leads():
    """Debug why our specific completed leads aren't found by the BigQuery optimization."""
    
    print("🐛 DEBUGGING BIGQUERY OPTIMIZATION FOR SPECIFIC LEADS")
    print("=" * 60)
    
    client = get_bigquery_client()
    
    # Test with just one lead first
    test_emails = ["care@nuropod.com", "info@luxxformen.com", "contact.pufii.ro@gmail.com"]
    
    SMB_CAMPAIGN_ID = "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"
    MIDSIZE_CAMPAIGN_ID = "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"
    PROJECT_ID = "instant-ground-394115"
    DATASET_ID = "email_analytics"
    
    for email in test_emails:
        print(f"\n🔍 DEBUGGING: {email}")
        print("-" * 40)
        
        # Check all conditions individually
        debug_query = f"""
        SELECT 
            email,
            instantly_lead_id,
            campaign_id,
            status,
            last_drain_check,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) as hours_since_check,
            -- Check each condition
            CASE WHEN last_drain_check IS NULL THEN true ELSE false END as never_checked,
            CASE WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) >= 24 THEN true ELSE false END as needs_check_24h,
            CASE WHEN status IN ('active', 'pending') THEN true ELSE false END as status_filter,
            CASE WHEN campaign_id IN ('{SMB_CAMPAIGN_ID}', '{MIDSIZE_CAMPAIGN_ID}') THEN true ELSE false END as campaign_filter,
            -- Overall qualification
            CASE WHEN (
                (last_drain_check IS NULL OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) >= 24)
                AND status IN ('active', 'pending')
                AND campaign_id IN ('{SMB_CAMPAIGN_ID}', '{MIDSIZE_CAMPAIGN_ID}')
            ) THEN true ELSE false END as qualifies_for_optimization
        FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
        WHERE email = '{email}'
        """
        
        results = client.query(debug_query).result()
        
        found = False
        for row in results:
            found = True
            print(f"   📧 Email: {row.email}")
            print(f"   📋 Campaign ID: {row.campaign_id}")
            print(f"   📊 Status: {row.status}")
            print(f"   ⏰ Last drain check: {row.last_drain_check}")
            print(f"   🕐 Hours since check: {row.hours_since_check}")
            print(f"   ✅ Never checked: {row.never_checked}")
            print(f"   ✅ Needs 24h+ check: {row.needs_check_24h}")
            print(f"   ✅ Status filter pass: {row.status_filter}")
            print(f"   ✅ Campaign filter pass: {row.campaign_filter}")
            print(f"   🎯 QUALIFIES FOR OPTIMIZATION: {row.qualifies_for_optimization}")
            
            if not row.qualifies_for_optimization:
                print(f"   ❌ DOESN'T QUALIFY - checking why...")
                if not (row.never_checked or row.needs_check_24h):
                    print(f"      • Time filter failed: checked {row.hours_since_check}h ago")
                if not row.status_filter:
                    print(f"      • Status filter failed: '{row.status}' not in ('active', 'pending')")
                if not row.campaign_filter:
                    print(f"      • Campaign filter failed: '{row.campaign_id}' not in target campaigns")
        
        if not found:
            print(f"   ❌ EMAIL NOT FOUND IN ops_inst_state TABLE!")
            print(f"      This lead may not exist in BigQuery tracking")

def check_optimization_query_ordering():
    """Check if our leads are outside the LIMIT 1000 due to ordering."""
    
    print(f"\n" + "="*60)
    print("🔍 CHECKING QUERY ORDERING AND LIMITS")
    print("="*60)
    
    client = get_bigquery_client()
    
    SMB_CAMPAIGN_ID = "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"
    MIDSIZE_CAMPAIGN_ID = "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"
    PROJECT_ID = "instant-ground-394115"
    DATASET_ID = "email_analytics"
    
    # Check where our leads rank in the ordering
    rank_query = f"""
    WITH ranked_leads AS (
        SELECT 
            email,
            last_drain_check,
            COALESCE(last_drain_check, TIMESTAMP('1970-01-01')) as sort_time,
            ROW_NUMBER() OVER (
                ORDER BY 
                    COALESCE(last_drain_check, TIMESTAMP('1970-01-01')) ASC,
                    email ASC
            ) as rank_position
        FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
        WHERE (
            last_drain_check IS NULL 
            OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) >= 24
        )
        AND status IN ('active', 'pending')
        AND campaign_id IN ('{SMB_CAMPAIGN_ID}', '{MIDSIZE_CAMPAIGN_ID}')
    )
    SELECT * FROM ranked_leads 
    WHERE email IN ('care@nuropod.com', 'info@luxxformen.com', 'contact.pufii.ro@gmail.com')
    ORDER BY rank_position
    """
    
    results = client.query(rank_query).result()
    
    for row in results:
        print(f"📧 {row.email}")
        print(f"   Rank position: {row.rank_position}")
        print(f"   Sort time: {row.sort_time}")
        if row.rank_position > 1000:
            print(f"   ❌ OUTSIDE LIMIT: Rank {row.rank_position} > 1000 limit")
        else:
            print(f"   ✅ WITHIN LIMIT: Should be included in optimization")
        print()

def main():
    try:
        debug_specific_leads()
        check_optimization_query_ordering() 
        
        print(f"\n" + "="*60)
        print("🎯 DEBUG CONCLUSIONS:")
        print("="*60)
        print("If leads qualify but aren't found, check:")
        print("1. Are they outside the LIMIT 1000 due to ordering?")
        print("2. Is there a bug in the BigQuery optimization implementation?")
        print("3. Are the campaign IDs correct in the query?")
        
    except Exception as e:
        print(f"❌ Error during debugging: {e}")

if __name__ == "__main__":
    main()