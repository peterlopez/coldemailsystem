"""
Instantly API client functions.
Contains all API communication logic - no BigQuery dependencies.
"""

import os
import json
import time
import logging
from typing import Dict, Optional, List
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .models import InstantlyLead

logger = logging.getLogger(__name__)

# Configuration
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
INSTANTLY_BASE_URL = 'https://api.instantly.ai'

# Load API key
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
if not INSTANTLY_API_KEY:
    # Try loading from config file as fallback
    try:
        config_path = './config/secrets/instantly-config.json'
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = json.load(f)
                INSTANTLY_API_KEY = config.get('api_key')
                logger.info("Loaded API key from config file")
    except Exception as e:
        logger.warning(f"Failed to load API key from config: {e}")

if not INSTANTLY_API_KEY:
    logger.error("❌ INSTANTLY_API_KEY is not configured!")
    logger.error("Set INSTANTLY_API_KEY environment variable or add to config file")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=60))
def call_instantly_api(endpoint: str, method: str = 'GET', data: Optional[Dict] = None) -> Dict:
    """Call Instantly API with automatic retry and backoff."""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    if DRY_RUN:
        logger.info(f"DRY RUN: Would call {method} {url} with data: {data}")
        return {'success': True, 'dry_run': True}
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=data, timeout=30)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, timeout=30)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        response.raise_for_status()
        # Handle empty response for DELETE operations
        if method == 'DELETE' and not response.content:
            return {'success': True}
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed: {e}")
        # Don't log to dead letters here to avoid BigQuery dependency
        raise

def delete_lead_from_instantly(lead: InstantlyLead) -> bool:
    """
    Delete a lead from Instantly using the official V2 DELETE endpoint.
    Follows API best practices: treats 404 as idempotent success.
    Simplified to avoid retry wrapper conflicts.
    """
    if DRY_RUN:
        logger.info(f"🧪 DRY RUN: Would delete lead {lead.email} (ID: {lead.id})")
        return True
    
    try:
        logger.debug(f"🔄 Deleting lead {lead.email} via DELETE /api/v2/leads/{lead.id}")
        
        # Use direct requests to avoid any wrapper issues
        headers = {
            'Authorization': f'Bearer {INSTANTLY_API_KEY}',
            'Accept': 'application/json'
        }
        
        response = requests.delete(
            f"{INSTANTLY_BASE_URL}/api/v2/leads/{lead.id}",
            headers=headers,
            timeout=30
        )
        
        if response.status_code in [200, 204]:
            logger.info(f"✅ Successfully deleted {lead.email} from Instantly")
            return True
        elif response.status_code == 404:
            # 404 means lead is already gone - treat as successful idempotent operation
            logger.info(f"✅ Lead {lead.email} already deleted (404 = idempotent success)")
            return True
        elif response.status_code == 429:
            # 429 means rate limited - this is a recoverable error
            logger.warning(f"⚠️ Rate limited deleting {lead.email} - API suggests slowing down")
            return False
        else:
            # Other HTTP errors are actual failures
            logger.error(f"❌ HTTP error deleting {lead.email}: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Exception deleting lead {lead.email}: {type(e).__name__}: {e}")
        return False

def get_finished_leads() -> List[InstantlyLead]:
    """
    Get leads that are finished (completed, replied, bounced, unsubscribed).
    Uses the working POST /api/v2/leads/list endpoint.
    """
    
    all_finished_leads = []
    campaigns = [
        ("SMB", SMB_CAMPAIGN_ID),
        ("Midsize", MIDSIZE_CAMPAIGN_ID)
    ]
    
    # Use optimized pagination to get all leads once, then filter by campaign
    from .pagination_utils import fetch_all_leads
    
    logger.info(f"🔍 Fetching all leads to check for finished leads across campaigns...")
    
    # Fetch all leads using optimized cursor pagination
    all_leads, pagination_stats = fetch_all_leads(
        api_call_func=call_instantly_api,
        campaign_filter=None,  # Get all leads, filter client-side
        batch_size=200,        # Larger batches for better performance
        use_cache=False        # Don't cache drain operations (need fresh data)
    )
    
    logger.info(f"📊 Drain analysis: {pagination_stats.total_items} leads fetched in {pagination_stats.total_pages} pages ({pagination_stats.duration_seconds:.1f}s)")
    
    # Process leads for each campaign
    for campaign_name, campaign_id in campaigns:
        logger.info(f"🔍 Analyzing {campaign_name} campaign for finished leads...")
        
        campaign_finished = []
        campaign_lead_count = 0
        
        # Filter and analyze leads for this campaign
        for item in all_leads:
            # Filter by campaign field (client-side filtering)
            lead_campaign = item.get('campaign')
            if lead_campaign != campaign_id:
                continue  # Skip leads not in this campaign
            
            campaign_lead_count += 1
            
            lead = InstantlyLead(
                id=item.get('id'),
                email=item.get('email'),
                status=item.get('status', 1),
                campaign_id=campaign_id,
                email_reply_count=item.get('email_reply_count', 0),
                created_at=item.get('created_at')
            )
            
            # Check if this lead should be drained
            decision = should_drain_lead(lead, item)
            if decision['should_drain']:
                campaign_finished.append(lead)
                logger.debug(f"  ✅ Will drain {lead.email}: {decision['drain_reason']}")
        
        logger.info(f"  📊 {campaign_name}: {campaign_lead_count} total leads, {len(campaign_finished)} to be drained")
        all_finished_leads.extend(campaign_finished)
    
    logger.info(f"📋 Total finished leads across campaigns: {len(all_finished_leads)}")
    return all_finished_leads

def should_drain_lead(lead: InstantlyLead, raw_data: dict) -> dict:
    """
    Determine if a lead should be drained based on its status and other factors.
    This is a simplified version focusing on API data only.
    """
    status = lead.status
    email = lead.email
    email_reply_count = lead.email_reply_count
    
    # Status mapping from Instantly:
    # 1 = Not started (active)
    # 2 = In progress (active) 
    # 3 = Finished
    # 4 = Unsubscribed
    # 5 = Bounced
    # 6 = Paused
    # 7 = Replied (but may still be active in campaign)
    
    if status == 3:  # Finished
        if email_reply_count > 0:
            return {
                'should_drain': True,
                'drain_reason': 'replied',
                'details': f'Status 3 (finished) with {email_reply_count} replies'
            }
        else:
            return {
                'should_drain': True,
                'drain_reason': 'completed',
                'details': 'Status 3 (finished) with no replies - sequence completed'
            }
    
    elif status == 4:  # Unsubscribed
        return {
            'should_drain': True,
            'drain_reason': 'unsubscribed',
            'details': 'Status 4 - lead unsubscribed'
        }
    
    elif status == 5:  # Bounced
        return {
            'should_drain': True,
            'drain_reason': 'bounced_hard',
            'details': 'Status 5 - email bounced'
        }
    
    else:  # Status 1, 2, 6, 7 (active states)
        return {
            'should_drain': False,
            'keep_reason': f'Status {status} - lead still active in campaign'
        }

# Campaign IDs - centralized here  
SMB_CAMPAIGN_ID = "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"
MIDSIZE_CAMPAIGN_ID = "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"