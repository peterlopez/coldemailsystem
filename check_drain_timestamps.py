#!/usr/bin/env python3
"""
Check if the completed leads have been evaluated by the drain process recently.
This will help determine if the drain workflow is running or if there's a timing issue.
"""

import os
import sys
from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
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

def check_drain_evaluation_timestamps():
    """Check when these leads were last evaluated by the drain process."""
    
    # Sample emails from our investigation
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
    
    client = get_bigquery_client()
    
    # Check ops_inst_state table for these leads
    email_list = "', '".join(sample_emails)
    
    query = f"""
    SELECT 
        email,
        status,
        added_at,
        updated_at,
        verification_status,
        DATETIME_DIFF(CURRENT_DATETIME(), DATETIME(updated_at), HOUR) as hours_since_update,
        DATETIME_DIFF(CURRENT_DATETIME(), DATETIME(added_at), DAY) as days_since_added
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE email IN ('{email_list}')
    ORDER BY updated_at DESC
    """
    
    print("🔍 CHECKING BIGQUERY STATE FOR COMPLETED LEADS")
    print("=" * 60)
    print(f"📧 Checking {len(sample_emails)} leads in ops_inst_state table...")
    print()
    
    results = client.query(query).result()
    
    found_in_bq = []
    for row in results:
        found_in_bq.append(dict(row))
        print(f"📧 {row.email}")
        print(f"   Status: {row.status}")
        print(f"   Added: {row.added_at} ({row.days_since_added} days ago)")
        print(f"   Updated: {row.updated_at} ({row.hours_since_update} hours ago)")
        print(f"   Verification: {row.verification_status}")
        print()
    
    not_in_bq = [email for email in sample_emails if email not in [r['email'] for r in found_in_bq]]
    
    print("=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    print(f"✅ Found in BigQuery: {len(found_in_bq)} leads")
    print(f"❌ NOT in BigQuery: {len(not_in_bq)} leads")
    
    if not_in_bq:
        print("\n❌ Leads NOT in BigQuery (never added to system):")
        for email in not_in_bq:
            print(f"   • {email}")
    
    if found_in_bq:
        print(f"\n📅 Update timing analysis:")
        current_time = datetime.now(timezone.utc)
        
        # Check how recent the updates are
        recent_updates = [r for r in found_in_bq if r['hours_since_update'] < 24]
        old_updates = [r for r in found_in_bq if r['hours_since_update'] >= 24]
        
        print(f"   • Updated in last 24h: {len(recent_updates)} leads")
        print(f"   • Updated >24h ago: {len(old_updates)} leads")
        
        if old_updates:
            print(f"\n⚠️ Leads with old drain check timestamps (>24h):")
            for lead in old_updates:
                print(f"   • {lead['email']}: {lead['hours_since_update']} hours ago")
            print(f"\n   This suggests drain workflow may not be checking these leads recently!")
    
    # Check if drain workflows have been running
    print(f"\n🔄 CHECKING RECENT DRAIN ACTIVITY")
    print("-" * 40)
    
    # Check for recent drain activity across all leads
    recent_activity_query = """
    SELECT 
        DATE(updated_at) as update_date,
        COUNT(*) as leads_updated,
        COUNT(DISTINCT status) as status_types,
        STRING_AGG(DISTINCT status ORDER BY status) as statuses_updated
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE updated_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY DATE(updated_at)
    ORDER BY update_date DESC
    """
    
    recent_results = client.query(recent_activity_query).result()
    
    activity_found = False
    for row in recent_results:
        activity_found = True
        print(f"📅 {row.update_date}: {row.leads_updated} leads updated (statuses: {row.statuses_updated})")
    
    if not activity_found:
        print("❌ No recent drain activity found in last 7 days!")
        print("   This suggests the drain workflow may not be running.")
    
    return found_in_bq, not_in_bq

def main():
    try:
        found_in_bq, not_in_bq = check_drain_evaluation_timestamps()
        
        print("\n" + "=" * 60)
        print("🎯 DIAGNOSIS CONCLUSIONS")
        print("=" * 60)
        
        if not_in_bq:
            print("❌ ISSUE 1: Some completed leads are not tracked in BigQuery")
            print("   → These leads were likely added before the tracking system")
            print("   → Or the sync process didn't properly track them")
            print()
        
        old_tracked_leads = [r for r in found_in_bq if r['hours_since_update'] >= 24]
        if old_tracked_leads:
            print("❌ ISSUE 2: Tracked leads have old drain check timestamps")  
            print("   → The drain workflow may not be running frequently enough")
            print("   → Or there's a filtering issue preventing recent evaluation")
            print()
        
        print("💡 NEXT STEPS:")
        print("1. Check if drain workflow is actually running (GitHub Actions)")
        print("2. Check drain workflow logs for errors or filtering issues") 
        print("3. Consider if leads are being filtered out by the 24-hour check logic")
        print("4. Verify the drain process is reaching these specific campaigns")
        
    except Exception as e:
        print(f"❌ Error during investigation: {e}")

if __name__ == "__main__":
    main()