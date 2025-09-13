#!/usr/bin/env python3
"""
Test the 24-hour filtering logic that may be preventing completed leads from being evaluated.
This script will check the specific BigQuery logic that determines if leads need drain evaluation.
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

def simulate_batch_check_leads_for_drain():
    """Simulate the exact logic used by batch_check_leads_for_drain for our completed leads."""
    
    print("🔍 TESTING 24-HOUR FILTER LOGIC")
    print("=" * 60)
    
    client = get_bigquery_client()
    
    # Get the instantly_lead_ids for our completed leads (we know they exist from our earlier test)
    sample_emails = [
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
    
    print(f"📧 Testing 24-hour filter for {len(sample_emails)} completed leads...")
    
    # First, get the instantly_lead_ids for these emails
    email_list = "', '".join(sample_emails)
    
    query = f"""
    SELECT 
        email,
        instantly_lead_id,
        last_drain_check,
        updated_at,
        CASE 
            WHEN last_drain_check IS NULL THEN 'NEVER_CHECKED'
            WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) >= 24 THEN 'NEEDS_CHECK'  
            ELSE 'RECENT_CHECK'
        END as check_status,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) as hours_since_last_drain_check,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated_at, HOUR) as hours_since_updated
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE email IN ('{email_list}')
    ORDER BY email
    """
    
    print("\n📋 CHECKING BIGQUERY DRAIN CHECK STATUS:")
    print("-" * 50)
    
    results = client.query(query).result()
    
    needs_check_count = 0
    recent_check_count = 0
    never_checked_count = 0
    
    for row in results:
        print(f"📧 {row.email}")
        print(f"   Lead ID: {row.instantly_lead_id}")
        print(f"   Last drain check: {row.last_drain_check}")
        print(f"   Hours since drain check: {row.hours_since_last_drain_check}")  
        print(f"   Status: {row.check_status}")
        
        if row.check_status == 'NEEDS_CHECK':
            needs_check_count += 1
            print(f"   ✅ WOULD BE EVALUATED (>24h or never checked)")
        elif row.check_status == 'RECENT_CHECK':
            recent_check_count += 1  
            print(f"   ❌ WOULD BE SKIPPED (checked within 24h)")
        else:
            never_checked_count += 1
            print(f"   ✅ WOULD BE EVALUATED (never checked)")
        print()
    
    print("=" * 60)
    print("📊 24-HOUR FILTER RESULTS:")
    print("=" * 60)
    print(f"✅ Would be EVALUATED: {needs_check_count + never_checked_count}")
    print(f"   - Never checked: {never_checked_count}")
    print(f"   - >24h since last check: {needs_check_count}")
    print(f"❌ Would be SKIPPED: {recent_check_count}")
    print(f"   - Checked within 24h: {recent_check_count}")
    
    if recent_check_count > 0:
        print(f"\n❌ ROOT CAUSE FOUND: {recent_check_count} leads are being filtered out by 24-hour rule!")
        print(f"   The drain system thinks these leads were checked recently, but they weren't actually processed.")
        print(f"   This suggests a mismatch between when drain checks happen and when statuses are updated.")
    
    if needs_check_count + never_checked_count == 0:
        print(f"\n❌ CRITICAL ISSUE: ALL leads are being filtered out by the 24-hour rule!")
        print(f"   This explains why no completed leads are being evaluated by the drain system.")
        print(f"   The system thinks they were all checked recently, but they clearly weren't drained.")
        
        # Check if this is a timestamp update issue
        print(f"\n🔍 INVESTIGATING TIMESTAMP UPDATE LOGIC:")
        print(f"   The system may be updating 'last_drain_check' without actually processing the leads.")
        print(f"   This would cause completed leads to be skipped indefinitely.")

def check_lead_ids_for_api_lookup():
    """Get the instantly_lead_ids we need for the API lookup."""
    
    print("\n🔍 GETTING INSTANTLY LEAD IDS FOR API DRAIN CHECK")
    print("=" * 60)
    
    client = get_bigquery_client()
    
    sample_emails = [
        "care@nuropod.com",
        "info@luxxformen.com", 
        "contact.pufii.ro@gmail.com",
        "info@orchard-house.jp",
        "info@ladesignconcepts.com"  # Just first 5 for focused test
    ]
    
    email_list = "', '".join(sample_emails)
    
    query = f"""
    SELECT 
        email,
        instantly_lead_id,
        campaign_id,
        status as bigquery_status,
        last_drain_check
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE email IN ('{email_list}')
        AND instantly_lead_id IS NOT NULL
    ORDER BY email
    """
    
    results = client.query(query).result()
    
    lead_ids = []
    print("📋 LEAD IDs FOR API CHECK:")
    for row in results:
        lead_ids.append(row.instantly_lead_id)
        print(f"   {row.email} → {row.instantly_lead_id} (BQ status: {row.bigquery_status})")
    
    print(f"\n💡 These {len(lead_ids)} lead IDs should be used to test the actual batch_check_leads_for_drain logic")
    print(f"   Lead IDs: {lead_ids}")
    
    return lead_ids

def main():
    try:
        simulate_batch_check_leads_for_drain()
        lead_ids = check_lead_ids_for_api_lookup()
        
        print(f"\n" + "="*60)
        print("🎯 INVESTIGATION CONCLUSIONS")  
        print("="*60)
        print("The 24-hour filter is the most likely culprit.")
        print("Next steps:")
        print("1. Check if last_drain_check timestamps are being updated incorrectly")
        print("2. Verify the batch_check_leads_for_drain logic with actual lead IDs")
        print("3. Look for a bug where timestamps are updated but leads aren't processed")
        
    except Exception as e:
        print(f"❌ Error during investigation: {e}")

if __name__ == "__main__":
    main()