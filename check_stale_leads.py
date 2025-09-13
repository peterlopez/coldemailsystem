#!/usr/bin/env python3
"""
Check for other leads that might have similar issues (stale active leads)
"""

import os
from google.cloud import bigquery
from shared_config import PROJECT_ID, DATASET_ID

def main():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/secrets/bigquery-credentials.json'
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    print("🔍 Checking for potentially stale leads...")
    print("=" * 60)
    
    # Check for leads that haven't been updated recently but are still active
    query = f"""
    SELECT 
        email,
        status,
        campaign_id,
        added_at,
        updated_at,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated_at, HOUR) as hours_stale,
        CASE 
            WHEN campaign_id = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6' THEN 'SMB'
            WHEN campaign_id = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df' THEN 'Midsize'
            ELSE 'Other'
        END as campaign_type
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE status = 'active'
        AND updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    ORDER BY updated_at ASC
    LIMIT 20
    """
    
    result = bq_client.query(query).result()
    rows = list(result)
    
    if rows:
        print(f"\n⚠️  Found {len(rows)} leads that haven't been updated in 24+ hours:")
        print()
        for idx, row in enumerate(rows[:10], 1):  # Show top 10
            print(f"{idx:2d}. {row.email}")
            print(f"    Campaign: {row.campaign_type}")
            print(f"    Hours stale: {row.hours_stale}")
            print(f"    Added: {row.added_at}")
            print(f"    Last updated: {row.updated_at}")
            print()
        
        if len(rows) > 10:
            print(f"... and {len(rows) - 10} more")
        
        print("\n💡 These leads might have similar issues:")
        print("   - Replies not detected")
        print("   - Sequences completed but not drained") 
        print("   - Drain workflow not running properly")
        
    else:
        print("\n✅ No stale active leads found (all updated within 24 hours)")
    
    # Check last drain activity
    print("\n" + "=" * 60)
    print("📈 Recent drain activity:")
    
    query = f"""
    SELECT 
        status,
        COUNT(*) as count,
        MIN(updated_at) as first_update,
        MAX(updated_at) as last_update
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE updated_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        AND status IN ('replied', 'completed', 'unsubscribed', 'bounced_hard')
    GROUP BY status
    ORDER BY last_update DESC
    """
    
    result = bq_client.query(query).result()
    drain_rows = list(result)
    
    if drain_rows:
        for row in drain_rows:
            print(f"\n{row.status.upper()}: {row.count} leads")
            print(f"  First: {row.first_update}")
            print(f"  Latest: {row.last_update}")
    else:
        print("\n❌ NO drain activity in the last 7 days!")
        print("The drain workflow appears to be completely broken.")

if __name__ == "__main__":
    main()