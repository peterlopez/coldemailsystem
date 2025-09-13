#!/usr/bin/env python3
"""
Check lead status directly by ID in Instantly API
"""

import os
import sys
import json
import logging
import requests
from shared_config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def call_instantly_api(endpoint: str, method: str = 'GET'):
    """Call Instantly API"""
    api_key = config.api.instantly_api_key
    url = f"https://api.instantly.ai{endpoint}"
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"API call failed: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.text}")
        return None

def check_lead_by_id(lead_id):
    """Check lead details by ID"""
    logger.info(f"Checking lead ID: {lead_id}")
    
    lead_data = call_instantly_api(f'/api/v2/leads/{lead_id}')
    
    if lead_data:
        logger.info("\n✅ Lead found in Instantly:")
        logger.info(f"  Email: {lead_data.get('email')}")
        logger.info(f"  Campaign: {lead_data.get('campaign')}")
        logger.info(f"  Status: {lead_data.get('status')}")
        logger.info(f"  Created: {lead_data.get('created_at')}")
        logger.info(f"  Updated: {lead_data.get('updated_at')}")
        
        # Check engagement
        logger.info("\n📊 Engagement Metrics:")
        logger.info(f"  Replied: {lead_data.get('replied', False)}")
        logger.info(f"  Reply Count: {lead_data.get('reply_count', 0)}")
        logger.info(f"  Opened: {lead_data.get('opened', False)}")
        logger.info(f"  Open Count: {lead_data.get('open_count', 0)}")
        logger.info(f"  Clicked: {lead_data.get('clicked', False)}")
        logger.info(f"  Click Count: {lead_data.get('click_count', 0)}")
        logger.info(f"  Unsubscribed: {lead_data.get('unsubscribed', False)}")
        logger.info(f"  Bounced: {lead_data.get('bounced', False)}")
        
        # Additional status info
        if 'sequence_status' in lead_data:
            logger.info(f"\n  Sequence Status: {lead_data.get('sequence_status')}")
        if 'last_activity' in lead_data:
            logger.info(f"  Last Activity: {lead_data.get('last_activity')}")
            
        # Custom variables
        if 'custom_variables' in lead_data:
            logger.info("\n📝 Custom Variables:")
            for k, v in lead_data.get('custom_variables', {}).items():
                logger.info(f"  {k}: {v}")
                
        return lead_data
    else:
        logger.info("\n❌ Lead not found (may have been deleted)")
        return None

def main():
    # Lead ID from BigQuery for info@gullmeadowfarms.com
    lead_id = "84e8a190-9c6c-435f-9317-04ef38e709f5"
    email = "info@gullmeadowfarms.com"
    
    logger.info(f"🔍 Checking Instantly API for: {email}")
    logger.info(f"Using lead ID from BigQuery: {lead_id}")
    logger.info("=" * 60)
    
    lead_data = check_lead_by_id(lead_id)
    
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY FROM INVESTIGATION:")
    logger.info("\n📊 BigQuery Status:")
    logger.info("  ✅ FOUND in ops_inst_state")
    logger.info("  - Campaign: 5ffbe8c3-dc0e-41e4-9999-48f00d2015df (Midsize)")
    logger.info("  - Status: active")
    logger.info("  - Added: 2025-08-28 19:25:54 UTC")
    logger.info("  - Last Updated: 2025-09-05 21:41:26 UTC")
    logger.info("  - Email Verified: Yes (0.25 credits used)")
    logger.info("  - NO history in ops_lead_history (sequence not completed)")
    logger.info("  - NO errors in ops_dead_letters")
    
    logger.info("\n🔍 Instantly Status:")
    if lead_data:
        status = lead_data.get('status', 'unknown')
        logger.info(f"  ✅ FOUND - Current status: {status}")
        if lead_data.get('replied'):
            logger.info("  ⚠️  Lead has REPLIED - should be drained soon")
        elif status == 'completed':
            logger.info("  ⚠️  Sequence COMPLETED - should be drained soon")
        else:
            logger.info("  📧 Lead is still active in the campaign")
    else:
        logger.info("  ❌ NOT FOUND - Lead may have been deleted")

if __name__ == "__main__":
    main()