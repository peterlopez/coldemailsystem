#!/usr/bin/env python3
"""
Analyze lead processing history to understand why we only have 99 leads
"""

import os
from google.cloud import bigquery
from datetime import datetime, timedelta

# Set up BigQuery client
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/secrets/bigquery-credentials.json'
client = bigquery.Client(project='instant-ground-394115')

PROJECT_ID = "instant-ground-394115"
DATASET_ID = "email_analytics"

def analyze_lead_history():
    print("🔍 ANALYZING LEAD PROCESSING HISTORY")
    print("=" * 60)
    
    # 1. Check total leads by campaign
    print("\n📊 Total Leads in BigQuery Tracking:")
    query1 = f"""
    SELECT 
        campaign_id,
        COUNT(*) as total_leads,
        MIN(added_at) as first_added,
        MAX(added_at) as last_added
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    GROUP BY campaign_id
    """
    
    try:
        for row in client.query(query1).result():
            if '8c46e0c9' in row.campaign_id:
                campaign = 'SMB'
            elif '5ffbe8c3' in row.campaign_id:
                campaign = 'Midsize'
            else:
                campaign = 'Unknown'
            
            print(f"\n{campaign} Campaign:")
            print(f"  Total leads tracked: {row.total_leads}")
            print(f"  First added: {row.first_added}")
            print(f"  Last added: {row.last_added}")
    except Exception as e:
        print(f"Error querying campaign totals: {e}")
    
    # 2. Check daily lead additions
    print("\n📅 Daily Lead Additions (Last 14 Days):")
    query2 = f"""
    SELECT 
        DATE(added_at) as add_date,
        campaign_id,
        COUNT(*) as leads_added
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE added_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
    GROUP BY add_date, campaign_id
    ORDER BY add_date DESC
    """
    
    try:
        daily_totals = {}
        for row in client.query(query2).result():
            date_str = str(row.add_date)
            if date_str not in daily_totals:
                daily_totals[date_str] = {'SMB': 0, 'Midsize': 0}
            
            if '8c46e0c9' in row.campaign_id:
                daily_totals[date_str]['SMB'] = row.leads_added
            elif '5ffbe8c3' in row.campaign_id:
                daily_totals[date_str]['Midsize'] = row.leads_added
        
        for date, counts in sorted(daily_totals.items(), reverse=True):
            print(f"  {date}: SMB={counts['SMB']}, Midsize={counts['Midsize']}, Total={counts['SMB'] + counts['Midsize']}")
    except Exception as e:
        print(f"Error querying daily additions: {e}")
    
    # 3. Check hourly pattern (to see if scheduled runs are working)
    print("\n⏰ Hourly Lead Additions (Last 7 Days):")
    query3 = f"""
    SELECT 
        EXTRACT(HOUR FROM added_at) as hour,
        COUNT(*) as leads_added
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE added_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY hour
    ORDER BY hour
    """
    
    try:
        for row in client.query(query3).result():
            print(f"  Hour {row.hour:02d}:00 - {row.leads_added} leads")
    except Exception as e:
        print(f"Error querying hourly pattern: {e}")
    
    # 4. Check for any errors
    print("\n❌ Recent Errors (Last 7 Days):")
    query4 = f"""
    SELECT 
        DATE(occurred_at) as error_date,
        phase,
        COUNT(*) as error_count,
        MAX(error_text) as sample_error
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_dead_letters`
    WHERE occurred_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY error_date, phase
    ORDER BY error_date DESC
    LIMIT 10
    """
    
    try:
        error_count = 0
        for row in client.query(query4).result():
            print(f"  {row.error_date} - {row.phase}: {row.error_count} errors")
            print(f"    Sample: {row.sample_error[:100]}...")
            error_count += row.error_count
        
        if error_count == 0:
            print("  ✅ No errors found!")
    except Exception as e:
        print(f"  No error data available or error querying: {e}")
    
    # 5. Check GitHub Actions run frequency
    print("\n🤖 GitHub Actions Analysis:")
    print("  To see actual GitHub Actions history:")
    print("  1. Go to: https://github.com/peterlopez/coldemailsystem/actions")
    print("  2. Click on 'Cold Email Sync' workflow")
    print("  3. Check run history and parameters")
    
    # 6. Summary and recommendations
    print("\n📊 ANALYSIS SUMMARY:")
    print("=" * 60)
    
    # Calculate expected vs actual
    current_inventory_query = f"""
    SELECT COUNT(*) as total
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE campaign_id = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
    """
    
    try:
        result = client.query(current_inventory_query).result()
        tracked_smb_leads = next(result).total
        
        print(f"\n🎯 Key Findings:")
        print(f"  - BigQuery tracks {tracked_smb_leads} SMB leads")
        print(f"  - You see 99 leads in Instantly dashboard")
        
        if tracked_smb_leads != 99:
            print(f"  - ⚠️ Mismatch: BigQuery has {tracked_smb_leads}, Instantly shows 99")
            print(f"  - This suggests some leads were deleted or not properly synced")
    except Exception as e:
        print(f"Error getting current inventory: {e}")
    
    print("\n🔍 Likely Reasons for Low Lead Count:")
    print("  1. Campaign assignment bug - many early leads not assigned")
    print("  2. Manual deletions during testing")
    print("  3. Dry run mode for many executions")
    print("  4. Small batch sizes (10-25 instead of 100)")
    print("  5. GitHub Actions not running on schedule")
    
    print("\n🔧 Recommendations:")
    print("  1. Check GitHub Actions history for actual run count")
    print("  2. Ensure future runs use dry_run=false")
    print("  3. Increase target_leads to 100 for normal operations")
    print("  4. Monitor next few scheduled runs via dpaste logs")

if __name__ == "__main__":
    analyze_lead_history()