#!/usr/bin/env python3
"""
Analyze why BigQuery inventory count differs from actual Instantly inventory
"""

import os
import json
from datetime import datetime, timedelta
from google.cloud import bigquery

# Set up BigQuery client
PROJECT_ID = "instant-ground-394115"
DATASET_ID = "email_analytics"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
bq_client = bigquery.Client(project=PROJECT_ID)

def analyze_ops_state():
    """Analyze the ops_inst_state table to understand tracking issues"""
    print("🔍 ANALYZING OPS_INST_STATE TABLE")
    print("=" * 80)
    
    # 1. Overall status distribution
    query = """
    SELECT 
        status,
        COUNT(*) as count,
        MIN(added_at) as earliest_added,
        MAX(updated_at) as last_updated
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    GROUP BY status
    ORDER BY count DESC
    """
    
    print("\n📊 Status Distribution in ops_inst_state:")
    print("-" * 60)
    results = bq_client.query(query).result()
    total_records = 0
    active_count = 0
    
    for row in results:
        total_records += row.count
        if row.status == 'active':
            active_count = row.count
        print(f"  {row.status:<25} {row.count:>8} records")
        print(f"    Earliest: {row.earliest_added}")
        print(f"    Latest:   {row.last_updated}")
        print()
    
    print(f"Total records: {total_records}")
    print(f"Active records (BigQuery inventory): {active_count}")
    
    # 2. Check for stale active records
    print("\n⏰ Age Analysis of 'active' Records:")
    print("-" * 60)
    
    age_query = """
    SELECT 
        CASE 
            WHEN added_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) THEN '< 1 day'
            WHEN added_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) THEN '1-7 days'
            WHEN added_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN '7-30 days'
            WHEN added_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY) THEN '30-90 days'
            ELSE '> 90 days'
        END as age_bucket,
        COUNT(*) as count
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE status = 'active'
    GROUP BY age_bucket
    ORDER BY 
        CASE age_bucket
            WHEN '< 1 day' THEN 1
            WHEN '1-7 days' THEN 2
            WHEN '7-30 days' THEN 3
            WHEN '30-90 days' THEN 4
            ELSE 5
        END
    """
    
    results = bq_client.query(age_query).result()
    for row in results:
        print(f"  {row.age_bucket:<15} {row.count:>8} active leads")
    
    # 3. Check campaign distribution
    print("\n🎯 Campaign Distribution of Active Leads:")
    print("-" * 60)
    
    campaign_query = """
    SELECT 
        campaign_id,
        COUNT(*) as count
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE status = 'active'
    GROUP BY campaign_id
    """
    
    results = bq_client.query(campaign_query).result()
    for row in results:
        campaign_type = "SMB" if "8c46e0c9" in row.campaign_id else "Midsize"
        print(f"  {campaign_type:<10} ({row.campaign_id}): {row.count:>5} leads")
    
    # 4. Sample old active records
    print("\n📋 Sample of Oldest 'Active' Records:")
    print("-" * 60)
    
    old_records_query = """
    SELECT 
        email,
        campaign_id,
        added_at,
        updated_at,
        instantly_lead_id
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE status = 'active'
    ORDER BY added_at
    LIMIT 10
    """
    
    results = bq_client.query(old_records_query).result()
    for row in results:
        days_old = (datetime.now().replace(tzinfo=row.added_at.tzinfo) - row.added_at).days
        print(f"  {row.email:<35} Added: {days_old} days ago")
        print(f"    Lead ID: {row.instantly_lead_id}")
        print(f"    Last Updated: {row.updated_at}")
        print()

def check_drain_history():
    """Check if drain process has been updating statuses properly"""
    print("\n\n🚰 DRAIN PROCESS HISTORY")
    print("=" * 80)
    
    # Check recent status changes
    drain_query = """
    SELECT 
        DATE(updated_at) as update_date,
        status,
        COUNT(*) as count
    FROM `instant-ground-394115.email_analytics.ops_inst_state`
    WHERE updated_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        AND status != 'active'
    GROUP BY update_date, status
    ORDER BY update_date DESC, count DESC
    """
    
    print("\nRecent Status Changes (Last 30 days):")
    print("-" * 60)
    results = bq_client.query(drain_query).result()
    
    current_date = None
    for row in results:
        if current_date != row.update_date:
            current_date = row.update_date
            print(f"\n{row.update_date}:")
        print(f"  {row.status:<20} {row.count:>6} leads")
    
    # Check if there are leads that should have been drained
    missing_drain_query = """
    WITH active_leads AS (
        SELECT 
            email,
            added_at,
            instantly_lead_id
        FROM `instant-ground-394115.email_analytics.ops_inst_state`
        WHERE status = 'active'
    )
    SELECT 
        al.email,
        al.added_at,
        h.status_final,
        h.completed_at
    FROM active_leads al
    LEFT JOIN `instant-ground-394115.email_analytics.ops_lead_history` h
        ON al.email = h.email
    WHERE h.completed_at IS NOT NULL
    ORDER BY h.completed_at DESC
    LIMIT 10
    """
    
    print("\n\n⚠️  Active Leads That Appear in History as Completed:")
    print("-" * 60)
    results = bq_client.query(missing_drain_query).result()
    found_issues = False
    
    for row in results:
        found_issues = True
        print(f"  {row.email}")
        print(f"    Added to Instantly: {row.added_at}")
        print(f"    Completed in History: {row.completed_at} as '{row.status_final}'")
        print(f"    ❌ Should have been drained!")
        print()
    
    if not found_issues:
        print("  ✅ No obvious drain mismatches found")

def check_manual_deletions():
    """Check for leads that might have been manually deleted from Instantly"""
    print("\n\n🔍 CHECKING FOR MANUAL DELETIONS")
    print("=" * 80)
    
    # Look for patterns in dead letters that might indicate deletions
    deletion_query = """
    SELECT 
        phase,
        http_status,
        error_text,
        COUNT(*) as count,
        MAX(occurred_at) as latest
    FROM `instant-ground-394115.email_analytics.ops_dead_letters`
    WHERE phase = 'drain'
        AND occurred_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY phase, http_status, error_text
    ORDER BY count DESC
    LIMIT 10
    """
    
    print("\nRecent Drain Errors (might indicate missing leads):")
    print("-" * 60)
    results = bq_client.query(deletion_query).result()
    
    found_errors = False
    for row in results:
        found_errors = True
        print(f"\nHTTP {row.http_status}: {row.count} occurrences")
        print(f"Error: {row.error_text[:100]}...")
        print(f"Latest: {row.latest}")
    
    if not found_errors:
        print("  No recent drain errors found")

def analyze_lead_lifecycle():
    """Analyze typical lead lifecycle to understand patterns"""
    print("\n\n📈 LEAD LIFECYCLE ANALYSIS")
    print("=" * 80)
    
    lifecycle_query = """
    WITH lead_durations AS (
        SELECT 
            email,
            status,
            added_at,
            updated_at,
            TIMESTAMP_DIFF(updated_at, added_at, DAY) as days_to_update
        FROM `instant-ground-394115.email_analytics.ops_inst_state`
        WHERE status != 'active'
    )
    SELECT 
        status,
        COUNT(*) as count,
        AVG(days_to_update) as avg_days_to_completion,
        MIN(days_to_update) as min_days,
        MAX(days_to_update) as max_days
    FROM lead_durations
    GROUP BY status
    ORDER BY count DESC
    """
    
    print("\nAverage Time from Addition to Status Change:")
    print("-" * 60)
    results = bq_client.query(lifecycle_query).result()
    
    for row in results:
        print(f"\n{row.status}:")
        print(f"  Count: {row.count}")
        print(f"  Avg days to completion: {row.avg_days_to_completion:.1f}")
        print(f"  Range: {row.min_days} - {row.max_days} days")

def get_recommendations():
    """Provide recommendations based on analysis"""
    print("\n\n💡 RECOMMENDATIONS")
    print("=" * 80)
    
    print("\n1. **Stale Active Leads**: Many 'active' leads may have completed their sequences")
    print("   - Run a reconciliation process to sync BigQuery with Instantly")
    print("   - The drain process may not have been running regularly")
    print("\n2. **Missing Status Updates**: Leads marked 'active' for >30 days are likely stale")
    print("   - These should be checked against Instantly's actual status")
    print("   - Consider implementing a daily reconciliation job")
    print("\n3. **Manual Interventions**: If leads were deleted manually in Instantly,")
    print("   - BigQuery wouldn't know about it")
    print("   - Need webhook or regular sync to catch manual changes")
    print("\n4. **Drain Process**: Check if drain has been running successfully")
    print("   - Look at GitHub Actions history for drain workflow")
    print("   - May need to run a one-time cleanup")

if __name__ == "__main__":
    analyze_ops_state()
    check_drain_history()
    check_manual_deletions()
    analyze_lead_lifecycle()
    get_recommendations()