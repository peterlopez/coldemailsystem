#!/usr/bin/env python3
"""
Shared functions used by both sync and drain processes.
This separates common functionality to avoid circular dependencies.
"""

import os
import json
import logging
import time
from typing import List, Dict, Optional
from dataclasses import dataclass
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# Configuration
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')

if not INSTANTLY_API_KEY:
    # Try loading from config file as fallback
    try:
        with open('config/secrets/instantly-config.json', 'r') as f:
            config = json.load(f)
            INSTANTLY_API_KEY = config.get('api_key')
    except:
        pass

@dataclass
class InstantlyLead:
    """Represents a lead from Instantly API."""
    id: str
    email: str
    status: int
    campaign_id: str
    email_reply_count: int = 0
    created_at: Optional[str] = None
    
    def __post_init__(self):
        # Store original status for drain tracking
        self.status = self.status  # Keep as string for classification

def call_instantly_api(endpoint: str, method: str = 'GET', 
                     data: Optional[dict] = None, 
                     lead_id: Optional[str] = None) -> Optional[dict]:
    """Make API call to Instantly with retry logic."""
    
    if DRY_RUN and method in ['POST', 'DELETE', 'PUT', 'PATCH']:
        logger.info(f"DRY RUN: Would call {method} {endpoint}")
        if method == 'DELETE':
            return {'success': True}
        return {'id': 'dry-run-id', 'email': data.get('email', 'test@example.com')}
    
    base_url = 'https://app.instantly.ai'
    url = f"{base_url}{endpoint}"
    
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Log the request
    logger.debug(f"API Request: {method} {endpoint}")
    if data and method != 'GET':
        logger.debug(f"Request data: {json.dumps(data)}")
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=data, timeout=30)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, timeout=30)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        # Log response status
        logger.debug(f"Response status: {response.status_code}")
        
        # Check for errors
        if response.status_code == 404:
            logger.warning(f"Resource not found: {endpoint}")
            return None
        elif response.status_code == 401:
            logger.error("Authentication failed - check API key")
            raise Exception("Invalid API key")
        elif response.status_code >= 400:
            logger.error(f"API error {response.status_code}: {response.text}")
            response.raise_for_status()
        
        # Return JSON response
        if response.text:
            result = response.json()
            logger.debug(f"Response data: {json.dumps(result)[:500]}...")
            return result
        return None
        
    except requests.exceptions.Timeout:
        logger.error(f"Request timeout for {endpoint}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {endpoint}: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def delete_lead_from_instantly(lead: InstantlyLead) -> bool:
    """Delete a single lead from Instantly with retry logic."""
    try:
        endpoint = f'/api/v2/leads/{lead.id}'
        result = call_instantly_api(endpoint, method='DELETE', lead_id=lead.id)
        
        if result is not None:
            logger.debug(f"Successfully deleted lead {lead.email} ({lead.id})")
            return True
        else:
            logger.warning(f"Delete returned None for lead {lead.email} ({lead.id})")
            return False
            
    except Exception as e:
        logger.error(f"Failed to delete lead {lead.email} ({lead.id}): {e}")
        # Re-raise to trigger retry
        raise