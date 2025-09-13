#!/usr/bin/env python3
"""
Investigate specific lead status in BigQuery and Instantly
Usage: python investigate_lead_status.py <email>
"""

import os
import sys
import json
import logging
from datetime import datetime
from google.cloud import bigquery
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import shared config
from shared_config import config, PROJECT_ID, DATASET_ID

def setup_bigquery():
    """Initialize BigQuery client"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/secrets/bigquery-credentials.json'
    return bigquery.Client(project=PROJECT_ID)

def call_instantly_api(endpoint: str, method: str = 'GET', data: dict = None):
    """Call Instantly API"""
    api_key = config.api.instantly_api_key
    url = f"https://api.instantly.ai{endpoint}"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=data, timeout=30)
        
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"API call failed: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.text}")
        return None

def check_ops_inst_state(bq_client, email):
    """Check current state in ops_inst_state table"""
    query = f"""
    SELECT 
        email,
        campaign_id,
        status,
        instantly_lead_id,
        added_at,
        updated_at,
        verification_status,
        verification_catch_all,
        verification_credits_used,
        verified_at
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
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
        logger.info(f"\n=== Current State in ops_inst_state ===")
        for row in rows:
            logger.info(f"Email: {row.email}")
            logger.info(f"Campaign ID: {row.campaign_id}")
            logger.info(f"Status: {row.status}")
            logger.info(f"Instantly Lead ID: {row.instantly_lead_id}")
            logger.info(f"Added: {row.added_at}")
            logger.info(f"Updated: {row.updated_at}")
            logger.info(f"Verification Status: {row.verification_status}")
            logger.info(f"Verification Catch All: {row.verification_catch_all}")
            logger.info(f"Verification Credits Used: {row.verification_credits_used}")
            logger.info(f"Verified At: {row.verified_at}")
        return rows[0].instantly_lead_id if rows[0].instantly_lead_id else None
    else:
        logger.info(f"\n❌ No record found in ops_inst_state for {email}")
        return None

def check_lead_history(bq_client, email):
    """Check historical data in ops_lead_history table"""
    query = f"""
    SELECT 
        email,
        campaign_id,
        sequence_name,
        status_final,
        completed_at,
        attempt_num
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_lead_history`
    WHERE email = @email
    ORDER BY completed_at DESC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email)
        ]
    )
    
    result = bq_client.query(query, job_config=job_config).result()
    rows = list(result)
    
    if rows:
        logger.info(f"\n=== Lead History (ops_lead_history) ===")
        for idx, row in enumerate(rows):
            logger.info(f"\nHistory Entry #{idx + 1}:")
            logger.info(f"  Campaign ID: {row.campaign_id}")
            logger.info(f"  Sequence: {row.sequence_name}")
            logger.info(f"  Final Status: {row.status_final}")
            logger.info(f"  Completed: {row.completed_at}")
            logger.info(f"  Attempt #: {row.attempt_num}")
    else:
        logger.info(f"\n❌ No history found in ops_lead_history for {email}")

def check_dead_letters(bq_client, email):
    """Check for any errors in ops_dead_letters table"""
    query = f"""
    SELECT 
        occurred_at,
        phase,
        http_status,
        error_text,
        retry_count
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_dead_letters`
    WHERE email = @email
    ORDER BY occurred_at DESC
    LIMIT 10
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email)
        ]
    )
    
    result = bq_client.query(query, job_config=job_config).result()
    rows = list(result)
    
    if rows:
        logger.info(f"\n=== Errors Found (ops_dead_letters) ===")
        for idx, row in enumerate(rows):
            logger.info(f"\nError #{idx + 1}:")
            logger.info(f"  Occurred: {row.occurred_at}")
            logger.info(f"  Phase: {row.phase}")
            logger.info(f"  HTTP Status: {row.http_status}")
            logger.info(f"  Error: {row.error_text[:200]}...")
            logger.info(f"  Retry Count: {row.retry_count}")
    else:
        logger.info(f"\n✅ No errors found in ops_dead_letters for {email}")

def check_dnc_list(bq_client, email):
    """Check if email is in DNC list"""
    query = f"""
    SELECT 
        email,
        added_at,
        reason
    FROM `{PROJECT_ID}.{DATASET_ID}.dnc_list`
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
        logger.info(f"\n⚠️ Email found in DNC list!")
        for row in rows:
            logger.info(f"  Added: {row.added_at}")
            logger.info(f"  Reason: {row.reason}")
    else:
        logger.info(f"\n✅ Email NOT in DNC list")

def check_instantly_lead(lead_id):
    """Check lead status directly in Instantly API"""
    if not lead_id:
        logger.info("\n❌ No Instantly lead ID found, cannot check API")
        return
    
    logger.info(f"\n=== Checking Instantly API ===")
    logger.info(f"Looking up lead ID: {lead_id}")
    
    # Try to get lead details
    lead_data = call_instantly_api(f'/api/v2/leads/{lead_id}')
    
    if lead_data:
        logger.info("\n✅ Lead found in Instantly:")
        logger.info(f"  Email: {lead_data.get('email')}")
        logger.info(f"  Campaign: {lead_data.get('campaign')}")
        logger.info(f"  Status: {lead_data.get('status')}")
        logger.info(f"  Created: {lead_data.get('created_at')}")
        logger.info(f"  Updated: {lead_data.get('updated_at')}")
        
        # Additional fields that might be present
        if 'replied' in lead_data:
            logger.info(f"  Replied: {lead_data.get('replied')}")
        if 'opened' in lead_data:
            logger.info(f"  Opened: {lead_data.get('opened')}")
        if 'clicked' in lead_data:
            logger.info(f"  Clicked: {lead_data.get('clicked')}")
        if 'unsubscribed' in lead_data:
            logger.info(f"  Unsubscribed: {lead_data.get('unsubscribed')}")
        if 'bounced' in lead_data:
            logger.info(f"  Bounced: {lead_data.get('bounced')}")
    else:
        logger.info("\n❌ Lead not found in Instantly (may have been deleted)")

def search_instantly_by_email(email):
    """Search for lead in Instantly by email across all campaigns"""
    logger.info(f"\n=== Searching Instantly for {email} ===")
    
    # Get both campaign IDs from config
    campaigns = [
        config.campaigns.smb_campaign_id,
        config.campaigns.midsize_campaign_id
    ]
    
    found = False
    for campaign_id in campaigns:
        logger.info(f"\nChecking campaign: {campaign_id}")
        
        # Use the leads list endpoint with search
        data = {
            "campaign": campaign_id,
            "search": email,
            "limit": 10
        }
        
        response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
        
        if response and 'leads' in response:
            leads = response['leads']
            matching_leads = [l for l in leads if l.get('email') == email]
            
            if matching_leads:
                found = True
                for lead in matching_leads:
                    logger.info(f"\n✅ Found in campaign {campaign_id}:")
                    logger.info(f"  Lead ID: {lead.get('id')}")
                    logger.info(f"  Status: {lead.get('status')}")
                    logger.info(f"  Created: {lead.get('created_at')}")
                    logger.info(f"  Updated: {lead.get('updated_at')}")
                    if 'replied' in lead:
                        logger.info(f"  Replied: {lead.get('replied')}")
                    if 'opened' in lead:
                        logger.info(f"  Opened: {lead.get('opened')}")
    
    if not found:
        logger.info("\n❌ Email not found in any Instantly campaigns")

def main():
    if len(sys.argv) < 2:
        email = "info@gullmeadowfarms.com"  # Default for this investigation
    else:
        email = sys.argv[1]
    
    logger.info(f"🔍 Investigating lead: {email}")
    logger.info("=" * 60)
    
    # Setup BigQuery
    bq_client = setup_bigquery()
    
    # Check BigQuery tables
    instantly_lead_id = check_ops_inst_state(bq_client, email)
    check_lead_history(bq_client, email)
    check_dead_letters(bq_client, email)
    check_dnc_list(bq_client, email)
    
    # Check Instantly API
    if instantly_lead_id:
        check_instantly_lead(instantly_lead_id)
    
    # Also search by email in case lead ID is missing
    search_instantly_by_email(email)
    
    logger.info("\n" + "=" * 60)
    logger.info("Investigation complete!")

if __name__ == "__main__":
    main()