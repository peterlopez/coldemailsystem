#!/usr/bin/env python3
"""
Check if a lead exists in the source data and why it might not be eligible
"""

import os
import sys
from google.cloud import bigquery
from shared_config import config, PROJECT_ID, DATASET_ID

def setup_bigquery():
    """Initialize BigQuery client"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/secrets/bigquery-credentials.json'
    return bigquery.Client(project=PROJECT_ID)

def check_lead_in_source(bq_client, email):
    """Check if lead exists in the v_ready_for_instantly view"""
    query = f"""
    SELECT 
        email,
        merchant_name,
        estimated_sales_yearly,
        sequence_target,
        klaviyo_installed_at,
        klaviyo_priority
    FROM `{PROJECT_ID}.{DATASET_ID}.v_ready_for_instantly`
    WHERE email = @email
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email)
        ]
    )
    
    result = bq_client.query(query, job_config=job_config).result()
    rows = list(result)
    
    if rows:
        print("\n✅ FOUND in v_ready_for_instantly (eligible leads view):")
        for row in rows:
            print(f"  Email: {row.email}")
            print(f"  Merchant: {row.merchant_name}")
            print(f"  Annual Sales: ${row.estimated_sales_yearly:,.0f}")
            print(f"  Segment: {row.sequence_target}")
            print(f"  Klaviyo Installed: {row.klaviyo_installed_at}")
            print(f"  Priority: {row.klaviyo_priority}")
            print("\n  This lead IS eligible but hasn't been synced yet!")
        return True
    else:
        print(f"\n❌ NOT found in v_ready_for_instantly")
        return False

def check_raw_data(bq_client, email):
    """Check if lead exists in raw data but might be filtered out"""
    # First check shopify_traffic_and_segment
    query = f"""
    SELECT 
        email,
        merchant_name,
        estimated_sales_yearly,
        has_email,
        klaviyo_installed_at,
        DATE_DIFF(CURRENT_DATE(), DATE(klaviyo_installed_at), DAY) as days_since_klaviyo,
        CASE 
            WHEN estimated_sales_yearly < 1000000 THEN 'SMB'
            ELSE 'Midsize'
        END as would_be_segment
    FROM `{PROJECT_ID}.{DATASET_ID}.shopify_traffic_and_segment`
    WHERE email = @email
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email)
        ]
    )
    
    result = bq_client.query(query, job_config=job_config).result()
    rows = list(result)
    
    if rows:
        print("\n📊 FOUND in raw shopify_traffic_and_segment table:")
        for row in rows:
            print(f"  Email: {row.email}")
            print(f"  Merchant: {row.merchant_name}")
            print(f"  Annual Sales: ${row.estimated_sales_yearly:,.0f}")
            print(f"  Has Email: {row.has_email}")
            print(f"  Would be segment: {row.would_be_segment}")
            print(f"  Klaviyo Installed: {row.klaviyo_installed_at}")
            if row.klaviyo_installed_at:
                print(f"  Days Since Klaviyo: {row.days_since_klaviyo}")
            
            # Analyze why it might be filtered
            print("\n  🔍 Filtering Analysis:")
            if not row.has_email:
                print("    ❌ FILTERED: has_email is FALSE")
            if row.klaviyo_installed_at is None:
                print("    ❌ FILTERED: No Klaviyo installation date")
            elif row.days_since_klaviyo and row.days_since_klaviyo > 365:
                print(f"    ⚠️  NOTE: Klaviyo installed {row.days_since_klaviyo} days ago (low priority)")
            
            return True
    else:
        print(f"\n❌ NOT found in shopify_traffic_and_segment")
        return False

def check_dnc_and_history(bq_client, email):
    """Check if email is blocked by DNC or history"""
    # Check DNC
    query = f"""
    SELECT COUNT(*) as dnc_count
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_do_not_contact`
    WHERE email = @email
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email)
        ]
    )
    
    try:
        result = bq_client.query(query, job_config=job_config).result()
        dnc_count = list(result)[0].dnc_count
        if dnc_count > 0:
            print(f"\n⛔ BLOCKED: Email is in Do Not Contact list!")
    except:
        # Table might not exist with that name
        pass
    
    # Check 90-day cooldown
    query = f"""
    SELECT 
        MAX(completed_at) as last_completed,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(completed_at)), DAY) as days_ago
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_lead_history`
    WHERE email = @email
        AND completed_at IS NOT NULL
    """
    
    result = bq_client.query(query, job_config=job_config).result()
    rows = list(result)
    
    if rows and rows[0].last_completed:
        days_ago = rows[0].days_ago
        print(f"\n⏰ COOLDOWN CHECK: Last completed {days_ago} days ago")
        if days_ago < 90:
            print(f"  ❌ BLOCKED: Still in 90-day cooldown period")
        else:
            print(f"  ✅ OK: Outside 90-day cooldown period")

def main():
    email = "kelly@gullmeadowfarms.com"
    
    print(f"🔍 Checking lead eligibility for: {email}")
    print("=" * 60)
    
    # Setup BigQuery
    bq_client = setup_bigquery()
    
    # Check if in eligible view
    in_eligible = check_lead_in_source(bq_client, email)
    
    # If not eligible, check raw data to understand why
    if not in_eligible:
        in_raw = check_raw_data(bq_client, email)
        
        if not in_raw:
            print("\n🚫 CONCLUSION: This email doesn't exist in the source data at all")
            print("   It may be from a different data source or manually entered")
        else:
            print("\n⚠️  CONCLUSION: Email exists in raw data but is filtered out")
            print("   Check the filtering criteria above")
    
    # Always check blocklists
    check_dnc_and_history(bq_client, email)
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()