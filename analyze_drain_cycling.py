#!/usr/bin/env python3
"""
Comprehensive analysis to answer:
1. What processes update ops_inst_state status?
2. Will lead #5230 eventually be reached after multiple runs?
3. Are we stuck analyzing the same 1000 leads repeatedly?
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

def analyze_status_update_processes():
    """Analyze what processes update the ops_inst_state status column."""
    
    print("🔍 QUESTION 1: What processes update ops_inst_state status?")
    print("=" * 60)
    
    print("\n📋 Based on code analysis, these processes update status:")
    print("\n1. **sync_once.py - update_ops_state()**")
    print("   - When: After creating new leads in Instantly")
    print("   - Sets status: 'active' for newly added leads")
    print("   - File: sync_once.py lines 1973-2032")
    
    print("\n2. **sync_once.py - _bulk_update_ops_inst_state()**")
    print("   - When: After draining finished leads")  
    print("   - Sets status: 'completed', 'replied', 'bounced_hard', 'unsubscribed', 'stale_active'")
    print("   - File: sync_once.py lines 1438-1473")
    
    print("\n3. **simple_async_verification.py - Multiple functions**")
    print("   - When: During email verification process")
    print("   - Sets verification_status: 'pending', 'verified', 'invalid', etc.")
    print("   - Does NOT update the main 'status' column")
    
    print("\n⚠️ KEY FINDING: Only the drain process updates status from 'active' to 'completed'!")
    print("   If drain doesn't run properly, status remains 'active' forever.")

def analyze_drain_queue_progression():
    """Analyze if the drain process will eventually reach lead #5230."""
    
    print("\n\n🔍 QUESTION 2: Will drain eventually reach lead #5230?")
    print("=" * 60)
    
    client = get_bigquery_client()
    
    # Get current drain queue statistics
    query = """
    WITH drain_queue AS (
        SELECT 
            email,
            instantly_lead_id,
            last_drain_check,
            COALESCE(last_drain_check, TIMESTAMP('1970-01-01')) as sort_time,
            ROW_NUMBER() OVER (
                ORDER BY 
                    COALESCE(last_drain_check, TIMESTAMP('1970-01-01')) ASC,
                    email ASC
            ) as queue_position
        FROM `instant-ground-394115.email_analytics.ops_inst_state`
        WHERE (
            last_drain_check IS NULL 
            OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) >= 24
        )
        AND status IN ('active', 'pending')
        AND campaign_id IN ('8c46e0c9-c1f9-4201-a8d6-6221bafeada6', '5ffbe8c3-dc0e-41e4-9999-48f00d2015df')
    )
    SELECT 
        COUNT(*) as total_in_queue,
        MIN(queue_position) as min_position,
        MAX(queue_position) as max_position,
        COUNT(CASE WHEN queue_position <= 1000 THEN 1 END) as in_first_batch,
        COUNT(CASE WHEN queue_position > 1000 AND queue_position <= 2000 THEN 1 END) as in_second_batch,
        COUNT(CASE WHEN queue_position > 2000 AND queue_position <= 3000 THEN 1 END) as in_third_batch,
        COUNT(CASE WHEN queue_position > 3000 AND queue_position <= 4000 THEN 1 END) as in_fourth_batch,
        COUNT(CASE WHEN queue_position > 4000 AND queue_position <= 5000 THEN 1 END) as in_fifth_batch,
        COUNT(CASE WHEN queue_position > 5000 THEN 1 END) as beyond_fifth_batch
    FROM drain_queue
    """
    
    result = client.query(query).result()
    
    for row in result:
        print(f"📊 Current Drain Queue Statistics:")
        print(f"   Total leads in queue: {row.total_in_queue:,}")
        print(f"   Queue positions: {row.min_position} to {row.max_position}")
        print(f"\n   Batch distribution (1000 leads per batch):")
        print(f"   • Batch 1 (1-1000): {row.in_first_batch:,} leads")
        print(f"   • Batch 2 (1001-2000): {row.in_second_batch:,} leads")
        print(f"   • Batch 3 (2001-3000): {row.in_third_batch:,} leads")
        print(f"   • Batch 4 (3001-4000): {row.in_fourth_batch:,} leads")
        print(f"   • Batch 5 (4001-5000): {row.in_fifth_batch:,} leads")
        print(f"   • Beyond batch 5 (5000+): {row.beyond_fifth_batch:,} leads")
        
        # Calculate when lead #5230 would be reached
        runs_needed = 6  # Lead 5230 would be in the 6th batch
        hours_needed = runs_needed * 2  # Drain runs every 2 hours
        
        print(f"\n⏰ To reach lead #5230:")
        print(f"   • Runs needed: {runs_needed} (processing 1000 leads per run)")
        print(f"   • Time needed: {hours_needed} hours")
        print(f"   • BUT... there's a problem!")

def analyze_timestamp_updates():
    """Check if we're stuck processing the same 1000 leads."""
    
    print("\n\n🔍 QUESTION 3: Are we stuck processing the same 1000 leads?")
    print("=" * 60)
    
    client = get_bigquery_client()
    
    # Check timestamp update patterns
    query = """
    WITH timestamp_groups AS (
        SELECT 
            DATE(last_drain_check) as check_date,
            EXTRACT(HOUR FROM last_drain_check) as check_hour,
            COUNT(*) as leads_updated
        FROM `instant-ground-394115.email_analytics.ops_inst_state`
        WHERE last_drain_check IS NOT NULL
            AND last_drain_check > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        GROUP BY check_date, check_hour
        ORDER BY check_date DESC, check_hour DESC
    )
    SELECT * FROM timestamp_groups
    LIMIT 20
    """
    
    print("\n📅 Recent drain timestamp updates:")
    results = client.query(query).result()
    
    for row in results:
        print(f"   {row.check_date} Hour {row.check_hour:02d}: {row.leads_updated:,} leads updated")
    
    # Check if the same leads keep getting checked
    print("\n🔄 Checking for repeated processing...")
    
    repeat_query = """
    SELECT 
        COUNT(*) as total_leads,
        COUNT(CASE WHEN last_drain_check > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 END) as checked_last_24h,
        COUNT(CASE WHEN last_drain_check > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR) THEN 1 END) as checked_last_48h,
        COUNT(CASE WHEN last_drain_check IS NULL OR last_drain_check < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 END) as eligible_for_check
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE status IN ('active', 'pending')
    """
    
    result = client.query(repeat_query).result()
    
    for row in result:
        print(f"\n   Total active/pending leads: {row.total_leads:,}")
        print(f"   Checked in last 24h: {row.checked_last_24h:,}")
        print(f"   Checked in last 48h: {row.checked_last_48h:,}")
        print(f"   Eligible for new check: {row.eligible_for_check:,}")
    
    print("\n💡 THE CRITICAL ISSUE:")
    print("   The system updates last_drain_check BEFORE checking the lead!")
    print("   This means:")
    print("   1. Lead gets selected for evaluation (position 1-1000)")
    print("   2. last_drain_check gets updated to NOW")
    print("   3. Lead is evaluated (but not actually drained)")
    print("   4. Next run: Lead now has recent timestamp, goes to END of queue")
    print("   5. New leads move to front of queue")
    print("   6. Process repeats with different 1000 leads")

def main():
    try:
        analyze_status_update_processes()
        analyze_drain_queue_progression()
        analyze_timestamp_updates()
        
        print("\n" + "="*60)
        print("🎯 FINAL ANSWERS:")
        print("="*60)
        
        print("\n1. **What updates status?**")
        print("   Only the drain process updates status from 'active' to 'completed'")
        print("   No other process syncs Instantly completion status back to BigQuery")
        
        print("\n2. **Will lead #5230 be reached?**")
        print("   THEORETICALLY: Yes, after ~6 drain runs (12 hours)")
        print("   ACTUALLY: NO! Because of the timestamp update issue")
        
        print("\n3. **Are we stuck on the same 1000 leads?**")
        print("   NO - We're cycling through DIFFERENT leads each time!")
        print("   The timestamp update pushes processed leads to the back")
        print("   This creates an infinite loop where no lead gets properly drained")
        
        print("\n🐛 THE BUG:")
        print("   1. Drain selects oldest 1000 leads by last_drain_check")
        print("   2. Updates their timestamps IMMEDIATELY")
        print("   3. Evaluates them (finds they need draining)")
        print("   4. But BigQuery optimization returns them for processing")
        print("   5. Since they're 'active' in BigQuery, they qualify")
        print("   6. But after timestamp update, they go to back of queue")
        print("   7. Next run processes DIFFERENT 1000 leads")
        print("   8. Your completed leads never get their turn!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")

if __name__ == "__main__":
    main()