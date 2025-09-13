#!/usr/bin/env python3
"""
Investigate why a reply isn't being detected for info@gullmeadowfarms.com
"""

import os
import sys
import json
import logging
import requests
from datetime import datetime, timezone
from google.cloud import bigquery
from shared_config import config, PROJECT_ID, DATASET_ID

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def check_lead_with_list_api(email, campaign_id):
    """Use the list API to get more detailed lead info"""
    logger.info(f"\n=== Checking lead using /api/v2/leads/list endpoint ===")
    
    data = {
        "campaign": campaign_id,
        "search": email,
        "limit": 1
    }
    
    response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
    
    if response and 'leads' in response:
        leads = response.get('leads', [])
        if leads:
            lead = leads[0]
            logger.info("\n📋 Detailed Lead Information:")
            logger.info(f"  ID: {lead.get('id')}")
            logger.info(f"  Email: {lead.get('email')}")
            logger.info(f"  Status: {lead.get('status')}")
            
            # Check all reply-related fields
            logger.info("\n💬 Reply Status:")
            logger.info(f"  replied: {lead.get('replied', 'N/A')}")
            logger.info(f"  reply_count: {lead.get('reply_count', 'N/A')}")
            logger.info(f"  last_replied: {lead.get('last_replied', 'N/A')}")
            logger.info(f"  first_reply_date: {lead.get('first_reply_date', 'N/A')}")
            
            # Check sequence status
            logger.info("\n📧 Sequence Status:")
            logger.info(f"  sequence_status: {lead.get('sequence_status', 'N/A')}")
            logger.info(f"  sequence_step: {lead.get('sequence_step', 'N/A')}")
            logger.info(f"  last_sent: {lead.get('last_sent', 'N/A')}")
            logger.info(f"  next_step: {lead.get('next_step', 'N/A')}")
            
            # Check other engagement
            logger.info("\n📊 Other Engagement:")
            logger.info(f"  opened: {lead.get('opened', 'N/A')}")
            logger.info(f"  open_count: {lead.get('open_count', 'N/A')}")
            logger.info(f"  clicked: {lead.get('clicked', 'N/A')}")
            logger.info(f"  click_count: {lead.get('click_count', 'N/A')}")
            logger.info(f"  bounced: {lead.get('bounced', 'N/A')}")
            logger.info(f"  unsubscribed: {lead.get('unsubscribed', 'N/A')}")
            
            # Check timestamps
            logger.info("\n⏰ Timestamps:")
            logger.info(f"  created_at: {lead.get('created_at', 'N/A')}")
            logger.info(f"  updated_at: {lead.get('updated_at', 'N/A')}")
            logger.info(f"  last_activity: {lead.get('last_activity', 'N/A')}")
            
            return lead
        else:
            logger.error("❌ Lead not found in list API response")
    else:
        logger.error("❌ Failed to get lead from list API")
    
    return None

def check_campaign_settings(campaign_id):
    """Check campaign settings for reply detection"""
    logger.info(f"\n=== Checking Campaign Settings ===")
    
    campaign_data = call_instantly_api(f'/api/v2/campaigns/{campaign_id}')
    
    if campaign_data:
        logger.info("\n⚙️ Reply Detection Settings:")
        logger.info(f"  stop_on_reply: {campaign_data.get('stop_on_reply', 'N/A')}")
        logger.info(f"  stop_on_auto_reply: {campaign_data.get('stop_on_auto_reply', 'N/A')}")
        logger.info(f"  reply_to_email: {campaign_data.get('reply_to_email', 'N/A')}")
        
        # Check mailbox settings
        if 'sending_accounts' in campaign_data:
            logger.info(f"\n📮 Mailboxes: {len(campaign_data['sending_accounts'])} configured")
            for idx, account in enumerate(campaign_data['sending_accounts'][:3]):  # Show first 3
                logger.info(f"  Mailbox {idx+1}: {account.get('email', 'N/A')}")
        
        return campaign_data
    else:
        logger.error("❌ Failed to get campaign settings")
        return None

def check_drain_history(bq_client, email):
    """Check when drain workflow last ran and if it processed this lead"""
    logger.info(f"\n=== Checking Drain Workflow History ===")
    
    # Check last update time for this lead
    query = f"""
    SELECT 
        email,
        status,
        updated_at,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated_at, HOUR) as hours_since_update
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
        row = rows[0]
        logger.info(f"\n📊 Lead Update Status:")
        logger.info(f"  Last updated: {row.updated_at}")
        logger.info(f"  Hours since update: {row.hours_since_update}")
        logger.info(f"  Current status: {row.status}")
        
        if row.hours_since_update > 2:
            logger.warning(f"  ⚠️  Lead hasn't been processed in {row.hours_since_update} hours")
            logger.warning(f"  The drain workflow runs every 2 hours, so it should have run by now")
    
    # Check recent drain activity
    query = """
    SELECT 
        status,
        COUNT(*) as count,
        MAX(updated_at) as last_update
    FROM `{}.{}.ops_inst_state`
    WHERE updated_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        AND status IN ('replied', 'completed', 'unsubscribed', 'bounced_hard')
    GROUP BY status
    ORDER BY last_update DESC
    """.format(PROJECT_ID, DATASET_ID)
    
    result = bq_client.query(query).result()
    drain_activity = list(result)
    
    if drain_activity:
        logger.info("\n📈 Recent Drain Activity (last 24h):")
        for row in drain_activity:
            logger.info(f"  {row.status}: {row.count} leads, last at {row.last_update}")
    else:
        logger.warning("\n⚠️  No drain activity in the last 24 hours!")

def provide_recommendations(lead_data):
    """Provide recommendations based on findings"""
    logger.info("\n" + "="*60)
    logger.info("🔍 ANALYSIS & RECOMMENDATIONS")
    logger.info("="*60)
    
    if lead_data:
        replied = lead_data.get('replied', False)
        reply_count = lead_data.get('reply_count', 0)
        
        if not replied or reply_count == 0:
            logger.info("\n❌ PROBLEM CONFIRMED: Reply not detected in Instantly")
            logger.info("\nPossible causes:")
            logger.info("1. Reply went to a different email address than the sending mailbox")
            logger.info("2. Reply tracking is not properly configured in the campaign")
            logger.info("3. Reply went to spam/junk folder")
            logger.info("4. Reply was sent to a forwarding address")
            logger.info("5. Instantly's reply detection webhook might be failing")
            
            logger.info("\n🔧 RECOMMENDED ACTIONS:")
            logger.info("\n1. Check Reply Configuration:")
            logger.info("   - Verify reply-to email matches sending mailbox")
            logger.info("   - Check if 'stop_on_reply' is enabled in campaign")
            logger.info("   - Verify mailbox IMAP/SMTP settings are correct")
            
            logger.info("\n2. Manual Verification:")
            logger.info("   - Check the actual mailbox for the reply")
            logger.info("   - Look in spam/junk folders")
            logger.info("   - Check if reply went to a different address")
            
            logger.info("\n3. Manual Lead Update:")
            logger.info("   - You can manually mark the lead as replied in Instantly")
            logger.info("   - Or update the BigQuery status to trigger drain")
            
            logger.info("\n4. Add to DNC List:")
            logger.info("   - Since they're not interested, add to Do Not Contact list")
            logger.info("   - This prevents future emails to this address")
    else:
        logger.error("\n❌ Could not retrieve lead data to analyze")

def main():
    email = "info@gullmeadowfarms.com"
    campaign_id = "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"  # Midsize campaign
    
    logger.info(f"🔍 Investigating Reply Detection Issue")
    logger.info(f"Email: {email}")
    logger.info(f"Campaign: {campaign_id}")
    logger.info("="*60)
    
    # Check lead with detailed API
    lead_data = check_lead_with_list_api(email, campaign_id)
    
    # Check campaign settings
    campaign_data = check_campaign_settings(campaign_id)
    
    # Check drain history
    bq_client = setup_bigquery()
    check_drain_history(bq_client, email)
    
    # Provide recommendations
    provide_recommendations(lead_data)
    
    # Offer to create manual fix script
    logger.info("\n" + "="*60)
    logger.info("Would you like me to create a script to:")
    logger.info("1. Manually mark this lead as 'replied' in BigQuery?")
    logger.info("2. Add this email to the Do Not Contact list?")
    logger.info("3. Force a drain operation for this specific lead?")

if __name__ == "__main__":
    main()