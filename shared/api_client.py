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
        # Include response status/body when available for faster diagnostics
        status = getattr(getattr(e, 'response', None), 'status_code', None)
        body = getattr(getattr(e, 'response', None), 'text', '')
        if status:
            logger.error(f"API call failed: {e} (status={status}, body={body[:800]})")
        else:
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

# Campaign IDs - centralized here  
SMB_CAMPAIGN_ID = "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"
MIDSIZE_CAMPAIGN_ID = "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"
