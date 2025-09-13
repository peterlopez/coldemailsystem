#!/usr/bin/env python3
"""
Manual fix for lead that replied but wasn't detected
Provides options to update status and add to DNC
"""

import os
import sys
import json
import logging
import requests
from datetime import datetime, timezone
from google.cloud import bigquery
from shared_config import config, PROJECT_ID, DATASET_ID, DRY_RUN

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_bigquery():
    """Initialize BigQuery client"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/secrets/bigquery-credentials.json'
    return bigquery.Client(project=PROJECT_ID)

def call_instantly_api(endpoint: str, method: str = 'DELETE'):
    """Call Instantly API"""
    api_key = config.api.instantly_api_key
    url = f"https://api.instantly.ai{endpoint}"
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    try:
        if method == 'DELETE':
            response = requests.delete(url, headers=headers, timeout=30)
        
        response.raise_for_status()
        return {'success': True}
    except Exception as e:
        logger.error(f"API call failed: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.text}")
        return None

def update_lead_status_to_replied(bq_client, email):
    """Update lead status to 'replied' in BigQuery"""
    logger.info(f"\n📝 Updating lead status to 'replied' in BigQuery...")
    
    query = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    SET 
        status = 'replied',
        updated_at = CURRENT_TIMESTAMP()
    WHERE email = @email
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email)
        ]
    )
    
    if DRY_RUN:
        logger.info("  🔄 DRY RUN: Would update status to 'replied'")
        return True
    
    try:
        job = bq_client.query(query, job_config=job_config)
        job.result()
        logger.info(f"  ✅ Successfully updated status to 'replied' for {email}")
        return True
    except Exception as e:
        logger.error(f"  ❌ Failed to update status: {e}")
        return False

def add_to_dnc_list(bq_client, email, reason="Not interested - manual reply"):
    """Add email to Do Not Contact list"""
    logger.info(f"\n🚫 Adding to Do Not Contact list...")
    
    # Insert into DNC table
    query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_do_not_contact` 
    (email, reason, added_at, source)
    VALUES (@email, @reason, CURRENT_TIMESTAMP(), 'manual_fix')
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email),
            bigquery.ScalarQueryParameter("reason", "STRING", reason)
        ]
    )
    
    if DRY_RUN:
        logger.info("  🔄 DRY RUN: Would add to DNC list")
        return True
    
    try:
        job = bq_client.query(query, job_config=job_config)
        job.result()
        logger.info(f"  ✅ Successfully added {email} to DNC list")
        return True
    except Exception as e:
        if "Already Exists" in str(e):
            logger.info(f"  ℹ️  Email already in DNC list")
            return True
        else:
            logger.error(f"  ❌ Failed to add to DNC: {e}")
            return False

def delete_from_instantly(lead_id):
    """Delete lead from Instantly"""
    logger.info(f"\n🗑️  Deleting lead from Instantly...")
    
    if DRY_RUN:
        logger.info(f"  🔄 DRY RUN: Would delete lead {lead_id}")
        return True
    
    result = call_instantly_api(f'/api/v2/leads/{lead_id}', method='DELETE')
    
    if result:
        logger.info(f"  ✅ Successfully deleted lead from Instantly")
        return True
    else:
        logger.info(f"  ❌ Failed to delete from Instantly (may already be deleted)")
        return False

def add_to_lead_history(bq_client, email, campaign_id):
    """Add entry to lead history for tracking"""
    logger.info(f"\n📊 Adding to lead history...")
    
    query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_lead_history`
    (email, campaign_id, sequence_name, status_final, completed_at, attempt_num)
    VALUES (
        @email,
        @campaign_id,
        'Midsize Campaign',
        'replied',
        CURRENT_TIMESTAMP(),
        1
    )
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email),
            bigquery.ScalarQueryParameter("campaign_id", "STRING", campaign_id)
        ]
    )
    
    if DRY_RUN:
        logger.info("  🔄 DRY RUN: Would add to lead history")
        return True
    
    try:
        job = bq_client.query(query, job_config=job_config)
        job.result()
        logger.info(f"  ✅ Successfully added to lead history")
        return True
    except Exception as e:
        logger.error(f"  ❌ Failed to add to history: {e}")
        return False

def main():
    email = "info@gullmeadowfarms.com"
    campaign_id = "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"
    lead_id = "84e8a190-9c6c-435f-9317-04ef38e709f5"
    
    logger.info("🔧 Manual Lead Fix Tool")
    logger.info(f"Email: {email}")
    logger.info(f"Campaign: Midsize ({campaign_id})")
    logger.info(f"Lead ID: {lead_id}")
    logger.info(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    logger.info("="*60)
    
    # Confirm action
    logger.info("\nThis script will:")
    logger.info("1. Update BigQuery status to 'replied'")
    logger.info("2. Add email to Do Not Contact list")
    logger.info("3. Delete lead from Instantly campaign")
    logger.info("4. Add entry to lead history for tracking")
    
    if not DRY_RUN:
        response = input("\n⚠️  Continue with LIVE updates? (y/n): ")
        if response.lower() != 'y':
            logger.info("Cancelled.")
            return
    
    # Setup BigQuery
    bq_client = setup_bigquery()
    
    # Execute fixes
    success_count = 0
    
    if update_lead_status_to_replied(bq_client, email):
        success_count += 1
    
    if add_to_dnc_list(bq_client, email):
        success_count += 1
    
    if delete_from_instantly(lead_id):
        success_count += 1
    
    if add_to_lead_history(bq_client, email, campaign_id):
        success_count += 1
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info(f"✅ Completed {success_count}/4 actions successfully")
    
    if success_count == 4:
        logger.info("\n🎉 All fixes applied successfully!")
        logger.info("The lead has been properly marked as replied and removed from the campaign.")
    else:
        logger.warning("\n⚠️  Some actions failed. Check the logs above.")
    
    # Additional recommendations
    logger.info("\n📋 NEXT STEPS:")
    logger.info("1. Check why the drain workflow isn't running (GitHub Actions)")
    logger.info("2. Investigate why reply detection failed in Instantly")
    logger.info("3. Consider checking other leads that might have the same issue")

if __name__ == "__main__":
    main()