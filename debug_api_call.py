#!/usr/bin/env python3
"""
Debug what's being sent to the Instantly API
"""

import os
import sys
import logging
from shared_config import config
import requests

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_api_call():
    """Test direct API call to see what's happening"""
    api_key = config.api.instantly_api_key
    from shared.api_client import SMB_CAMPAIGN_ID
    
    url = "https://api.instantly.ai/api/v2/leads/list"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    # Test the exact payload our pagination utility sends
    data = {
        'campaign_id': SMB_CAMPAIGN_ID,
        'limit': 100  # Max allowed by Instantly API
    }
    
    logger.info(f"🔍 Testing API call:")
    logger.info(f"   URL: {url}")
    logger.info(f"   Headers: {{'Authorization': 'Bearer [REDACTED]', 'Content-Type': 'application/json'}}")
    logger.info(f"   Data: {data}")
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        logger.info(f"   Status: {response.status_code}")
        logger.info(f"   Response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"   ✅ Success! Got {len(result.get('items', []))} leads")
            return True
        else:
            logger.error(f"   ❌ Failed: {response.status_code}")
            logger.error(f"   Response text: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"   ❌ Exception: {e}")
        return False

def main():
    logger.info("🔍 DEBUG API CALL")
    logger.info("=" * 30)
    
    return test_api_call()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)