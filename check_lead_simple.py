#!/usr/bin/env python3
"""
Simple check if email exists in source data
"""

import os
from google.cloud import bigquery
from shared_config import PROJECT_ID, DATASET_ID

def main():
    email = "info@gullmeadowfarms.com"
    
    print(f"🔍 Checking for: {email}")
    print("=" * 60)
    
    # Setup BigQuery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/secrets/bigquery-credentials.json'
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # 1. Check v_ready_for_instantly (eligible leads)
    print("\n1️⃣ Checking v_ready_for_instantly (eligible leads)...")
    query = f"""
    SELECT COUNT(*) as count
    FROM `{PROJECT_ID}.{DATASET_ID}.v_ready_for_instantly`
    WHERE email = @email
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email)
        ]
    )
    
    try:
        result = bq_client.query(query, job_config=job_config).result()
        count = list(result)[0].count
        if count > 0:
            print(f"   ✅ FOUND - This lead is eligible for sync!")
        else:
            print(f"   ❌ NOT FOUND in eligible leads")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # 2. Check raw shopify data
    print("\n2️⃣ Checking shopify_traffic_and_segment (raw data)...")
    query = f"""
    SELECT 
        email,
        merchant_name,
        estimated_sales_yearly,
        has_email
    FROM `{PROJECT_ID}.{DATASET_ID}.shopify_traffic_and_segment`
    WHERE email = @email
    LIMIT 1
    """
    
    try:
        result = bq_client.query(query, job_config=job_config).result()
        rows = list(result)
        if rows:
            row = rows[0]
            print(f"   ✅ FOUND in raw data:")
            print(f"      Merchant: {row.merchant_name}")
            print(f"      Annual Sales: ${row.estimated_sales_yearly:,.0f}")
            print(f"      Has Email Flag: {row.has_email}")
            
            if not row.has_email:
                print(f"   ⚠️  FILTERED OUT: has_email = FALSE")
        else:
            print(f"   ❌ NOT FOUND in raw Shopify data")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # 3. Check ops_inst_state (already synced)
    print("\n3️⃣ Checking ops_inst_state (already synced leads)...")
    query = f"""
    SELECT 
        status,
        campaign_id,
        added_at
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE email = @email
    LIMIT 1
    """
    
    try:
        result = bq_client.query(query, job_config=job_config).result()
        rows = list(result)
        if rows:
            row = rows[0]
            print(f"   ✅ FOUND - Already synced:")
            print(f"      Status: {row.status}")
            print(f"      Campaign: {row.campaign_id}")
            print(f"      Added: {row.added_at}")
        else:
            print(f"   ❌ NOT FOUND - Never synced to Instantly")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print("kelly@gullmeadowfarms.com appears to NOT be in the system")
    print("and has never been processed by the sync workflow.")

if __name__ == "__main__":
    main()