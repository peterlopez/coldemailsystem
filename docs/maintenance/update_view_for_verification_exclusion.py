#!/usr/bin/env python3
"""
Update v_ready_for_instantly view to exclude failed verifications
"""

import os
from google.cloud import bigquery

# Configuration
PROJECT_ID = "instant-ground-394115"
DATASET_ID = "email_analytics"

# Initialize BigQuery client
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
client = bigquery.Client(project=PROJECT_ID)

def update_view():
    """Update the view to exclude failed verifications"""
    
    view_id = f"{PROJECT_ID}.{DATASET_ID}.v_ready_for_instantly"
    
    # Updated view that excludes verification_failed leads
    view_query = f"""
    WITH eligible_shopify_leads AS (
        SELECT 
            domain,
            contact_email,
            shopify_verified,
            first_order_date,
            status,
            location,
            country_code,
            annual_revenue,
            platform_detected,
            niche,
            growth_trend,
            CASE 
                WHEN COALESCE(annual_revenue, 0) < 1000000 THEN 'SMB'
                ELSE 'Midsize'
            END AS sequence_target
        FROM `{PROJECT_ID}.{DATASET_ID}.shopify_merchants`
        WHERE shopify_verified = TRUE
            AND contact_email IS NOT NULL
            AND contact_email != ''
    )
    SELECT DISTINCT
        sl.domain,
        sl.contact_email AS email,
        sl.sequence_target,
        sl.annual_revenue,
        sl.location,
        sl.country_code
    FROM eligible_shopify_leads sl
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` state
        ON sl.contact_email = state.email
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.ops_lead_history` history
        ON sl.contact_email = history.email
        AND history.completed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    WHERE 
        -- Not already in Instantly or failed verification
        (state.email IS NULL OR state.status = 'verification_failed')
        -- Not in DNC list
        AND NOT EXISTS (
            SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.ops_dnc_list` dnc
            WHERE dnc.email = sl.contact_email
        )
        -- Not completed in last 90 days
        AND history.email IS NULL
    """
    
    # Create or replace the view
    view = bigquery.Table(view_id)
    view.view_query = view_query
    
    try:
        # Try to delete existing view first
        client.delete_table(view_id, not_found_ok=True)
        print(f"Deleted existing view: {view_id}")
    except Exception as e:
        print(f"Note: Could not delete existing view: {e}")
    
    # Create the new view
    view = client.create_table(view)
    print(f"✅ Updated view: {view_id}")
    print(f"   - Now excludes leads with status='verification_failed'")
    print(f"   - Failed verifications can be retried after fixing email quality issues")
    
    # Check current counts
    count_query = f"SELECT COUNT(*) as eligible_count FROM `{view_id}`"
    result = list(client.query(count_query))[0]
    print(f"\n📊 Current eligible leads: {result.eligible_count:,}")
    
    # Check failed verifications
    failed_query = f"""
    SELECT COUNT(*) as failed_count 
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE status = 'verification_failed'
    """
    failed_result = list(client.query(failed_query))[0]
    print(f"📊 Failed verification leads: {failed_result.failed_count:,}")

if __name__ == "__main__":
    print("🔄 Updating v_ready_for_instantly view...")
    update_view()