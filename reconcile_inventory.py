#!/usr/bin/env python3
"""
Reconcile BigQuery inventory with actual Instantly inventory
This will update ops_inst_state to reflect reality
"""

import os
import sys
import json
import time
from datetime import datetime
from google.cloud import bigquery

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import from sync_once
from sync_once import (
    call_instantly_api, 
    SMB_CAMPAIGN_ID, 
    MIDSIZE_CAMPAIGN_ID,
    PROJECT_ID,
    DATASET_ID,
    logger
)

# Initialize BigQuery client
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
bq_client = bigquery.Client(project=PROJECT_ID)

def get_all_instantly_leads():
    """Get ALL leads from Instantly (all statuses)"""
    all_leads = []
    
    for campaign_name, campaign_id in [("SMB", SMB_CAMPAIGN_ID), ("Midsize", MIDSIZE_CAMPAIGN_ID)]:
        print(f"\n📥 Fetching all leads from {campaign_name} campaign...")
        
        page = 1
        campaign_leads = []
        
        while True:
            try:
                # Get ALL leads, not just active
                data = {
                    'campaign_id': campaign_id,
                    'page': page,
                    'per_page': 100
                }
                
                response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
                
                if not response or not response.get('items'):
                    break
                
                items = response.get('items', [])
                for item in items:
                    lead_data = {
                        'email': item.get('email'),
                        'campaign_id': campaign_id,
                        'instantly_lead_id': item.get('id'),
                        'status': item.get('status', 1),
                        'created_at': item.get('created_at'),
                        'email_reply_count': item.get('email_reply_count', 0)
                    }
                    campaign_leads.append(lead_data)
                
                print(f"  Page {page}: {len(items)} leads")
                
                if len(items) < 100:
                    break
                
                page += 1
                time.sleep(0.5)  # Rate limiting
                
                if page > 100:  # Safety limit
                    break
                    
            except Exception as e:
                print(f"  ❌ Error on page {page}: {e}")
                break
        
        print(f"  Total {campaign_name} leads: {len(campaign_leads)}")
        all_leads.extend(campaign_leads)
    
    return all_leads

def get_bigquery_tracked_leads():
    """Get all leads tracked in BigQuery"""
    query = f"""
    SELECT 
        email,
        campaign_id,
        status,
        instantly_lead_id,
        added_at,
        updated_at
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    """
    
    results = bq_client.query(query).result()
    bq_leads = {}
    
    for row in results:
        key = f"{row.email}:{row.campaign_id}"
        bq_leads[key] = {
            'email': row.email,
            'campaign_id': row.campaign_id,
            'status': row.status,
            'instantly_lead_id': row.instantly_lead_id,
            'added_at': row.added_at,
            'updated_at': row.updated_at
        }
    
    return bq_leads

def map_instantly_status(status_code):
    """Map Instantly status codes to our status strings"""
    status_map = {
        1: 'active',      # Not started
        2: 'active',      # In progress  
        3: 'completed',   # Finished
        4: 'unsubscribed',
        5: 'bounced_hard',
        6: 'active',      # Paused (still in system)
        7: 'replied'      # Replied
    }
    return status_map.get(status_code, f'unknown_{status_code}')

def reconcile_inventories(dry_run=True):
    """Compare and reconcile the inventories"""
    print("\n🔄 STARTING INVENTORY RECONCILIATION")
    print("=" * 80)
    
    # Get data from both sources
    print("\n1️⃣ Fetching Instantly inventory...")
    instantly_leads = get_all_instantly_leads()
    print(f"   Total Instantly leads: {len(instantly_leads)}")
    
    print("\n2️⃣ Fetching BigQuery tracked inventory...")
    bq_leads = get_bigquery_tracked_leads()
    print(f"   Total BigQuery records: {len(bq_leads)}")
    
    # Analyze discrepancies
    instantly_keys = set()
    for lead in instantly_leads:
        key = f"{lead['email']}:{lead['campaign_id']}"
        instantly_keys.add(key)
    
    bq_keys = set(bq_leads.keys())
    
    # Find differences
    in_instantly_not_bq = instantly_keys - bq_keys
    in_bq_not_instantly = bq_keys - instantly_keys
    in_both = instantly_keys & bq_keys
    
    print(f"\n📊 ANALYSIS:")
    print(f"   In Instantly but not BigQuery: {len(in_instantly_not_bq)}")
    print(f"   In BigQuery but not Instantly: {len(in_bq_not_instantly)}")
    print(f"   In both systems: {len(in_both)}")
    
    # Check status mismatches
    status_mismatches = []
    for lead in instantly_leads:
        key = f"{lead['email']}:{lead['campaign_id']}"
        if key in bq_leads:
            instantly_status = map_instantly_status(lead['status'])
            bq_status = bq_leads[key]['status']
            
            if instantly_status != bq_status:
                status_mismatches.append({
                    'email': lead['email'],
                    'campaign_id': lead['campaign_id'],
                    'instantly_status': instantly_status,
                    'bq_status': bq_status
                })
    
    print(f"   Status mismatches: {len(status_mismatches)}")
    
    # Show samples
    if status_mismatches:
        print("\n📋 Sample Status Mismatches:")
        for mismatch in status_mismatches[:5]:
            print(f"   {mismatch['email']}")
            print(f"     Instantly: {mismatch['instantly_status']}")
            print(f"     BigQuery: {mismatch['bq_status']}")
    
    if in_bq_not_instantly:
        print("\n📋 Sample Leads in BigQuery but NOT in Instantly (possibly deleted):")
        for key in list(in_bq_not_instantly)[:5]:
            lead = bq_leads[key]
            print(f"   {lead['email']} - Status: {lead['status']}")
    
    # Perform reconciliation
    if not dry_run:
        print("\n🔧 PERFORMING RECONCILIATION...")
        
        # Update status mismatches
        if status_mismatches:
            print(f"   Updating {len(status_mismatches)} status mismatches...")
            for mismatch in status_mismatches:
                try:
                    query = f"""
                    UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
                    SET status = @new_status,
                        updated_at = CURRENT_TIMESTAMP()
                    WHERE email = @email AND campaign_id = @campaign_id
                    """
                    
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("new_status", "STRING", mismatch['instantly_status']),
                            bigquery.ScalarQueryParameter("email", "STRING", mismatch['email']),
                            bigquery.ScalarQueryParameter("campaign_id", "STRING", mismatch['campaign_id']),
                        ]
                    )
                    
                    bq_client.query(query, job_config=job_config).result()
                except Exception as e:
                    print(f"     ❌ Failed to update {mismatch['email']}: {e}")
        
        # Mark deleted leads
        if in_bq_not_instantly:
            print(f"   Marking {len(in_bq_not_instantly)} deleted leads...")
            for key in in_bq_not_instantly:
                lead = bq_leads[key]
                try:
                    query = f"""
                    UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
                    SET status = 'deleted_from_instantly',
                        updated_at = CURRENT_TIMESTAMP()
                    WHERE email = @email AND campaign_id = @campaign_id
                    """
                    
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("email", "STRING", lead['email']),
                            bigquery.ScalarQueryParameter("campaign_id", "STRING", lead['campaign_id']),
                        ]
                    )
                    
                    bq_client.query(query, job_config=job_config).result()
                except Exception as e:
                    print(f"     ❌ Failed to mark deleted: {lead['email']}: {e}")
        
        print("\n✅ Reconciliation complete!")
    else:
        print("\n⚠️  DRY RUN - No changes made")
        print("   Run with --no-dry-run to apply changes")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-dry-run', action='store_true', help='Actually perform reconciliation')
    args = parser.parse_args()
    
    reconcile_inventories(dry_run=not args.no_dry_run)