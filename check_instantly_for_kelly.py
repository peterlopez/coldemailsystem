#!/usr/bin/env python3
"""
Quick script to check kelly@gullmeadowfarms.com in Instantly API
"""

import os
import sys
import json
import logging
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import shared config
from shared_config import config

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

def search_instantly_by_email(email):
    """Search for lead in Instantly by email across all campaigns"""
    logger.info(f"=== Searching Instantly for {email} ===")
    
    # Get both campaign IDs from config
    campaigns = [
        ('SMB', config.campaigns.smb_campaign_id),
        ('Midsize', config.campaigns.midsize_campaign_id)
    ]
    
    found = False
    for campaign_name, campaign_id in campaigns:
        logger.info(f"\nChecking {campaign_name} campaign: {campaign_id}")
        
        # Use the leads list endpoint with search
        data = {
            "campaign": campaign_id,
            "search": email,
            "limit": 10
        }
        
        response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
        
        if response and 'leads' in response:
            leads = response.get('leads', [])
            total = response.get('total', 0)
            
            logger.info(f"Total leads matching search: {total}")
            
            matching_leads = [l for l in leads if l.get('email', '').lower() == email.lower()]
            
            if matching_leads:
                found = True
                for lead in matching_leads:
                    logger.info(f"\n✅ FOUND in {campaign_name} campaign!")
                    logger.info(f"  Lead ID: {lead.get('id')}")
                    logger.info(f"  Email: {lead.get('email')}")
                    logger.info(f"  Status: {lead.get('status')}")
                    logger.info(f"  Created: {lead.get('created_at')}")
                    logger.info(f"  Updated: {lead.get('updated_at')}")
                    
                    # Check engagement metrics
                    if 'reply_count' in lead:
                        logger.info(f"  Reply Count: {lead.get('reply_count')}")
                    if 'opened_count' in lead:
                        logger.info(f"  Opened Count: {lead.get('opened_count')}")
                    if 'clicked_count' in lead:
                        logger.info(f"  Clicked Count: {lead.get('clicked_count')}")
                    if 'bounced' in lead:
                        logger.info(f"  Bounced: {lead.get('bounced')}")
                    if 'unsubscribed' in lead:
                        logger.info(f"  Unsubscribed: {lead.get('unsubscribed')}")
                    
                    # Additional info
                    if 'sequence_status' in lead:
                        logger.info(f"  Sequence Status: {lead.get('sequence_status')}")
                    if 'last_activity' in lead:
                        logger.info(f"  Last Activity: {lead.get('last_activity')}")
            else:
                logger.info(f"  Not found in {campaign_name} campaign")
        else:
            logger.info(f"  ❌ Failed to search {campaign_name} campaign")
    
    if not found:
        logger.info("\n❌ Email not found in any Instantly campaigns")
    
    return found

def main():
    email = "info@gullmeadowfarms.com"
    
    logger.info(f"🔍 Checking Instantly API for: {email}")
    logger.info("=" * 60)
    
    # Search Instantly
    found = search_instantly_by_email(email)
    
    logger.info("\n" + "=" * 60)
    
    # Summary from BigQuery results
    logger.info("\n📊 BIGQUERY SUMMARY (from previous run):")
    logger.info("  - NOT in ops_inst_state (never synced to Instantly)")
    logger.info("  - NOT in ops_lead_history (no completed sequences)")
    logger.info("  - NOT in ops_dead_letters (no sync errors)")
    
    logger.info("\n🔍 INSTANTLY SUMMARY:")
    if found:
        logger.info("  - FOUND in Instantly campaign(s)")
        logger.info("  - This lead was likely added outside of the sync system")
    else:
        logger.info("  - NOT found in any Instantly campaigns")
        logger.info("  - This lead has never been added to Instantly")
    
    logger.info("\n✨ CONCLUSION:")
    if not found:
        logger.info("kelly@gullmeadowfarms.com is NOT in the system and has never been processed")
    else:
        logger.info("kelly@gullmeadowfarms.com exists in Instantly but was not added via the sync system")

if __name__ == "__main__":
    main()