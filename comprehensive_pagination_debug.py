#!/usr/bin/env python3
"""
Comprehensive debugging script to identify pagination issues
"""

import os
import sys
import logging
import requests
from shared_config import config
from shared.api_client import SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_api_directly():
    """Test API calls directly to understand what's working"""
    api_key = config.api.instantly_api_key
    base_url = "https://api.instantly.ai"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    logger.info("🔍 TESTING DIRECT API CALLS")
    logger.info("=" * 50)
    
    # Test 1: Basic API connectivity
    logger.info("\n1️⃣ Testing workspace endpoint...")
    try:
        response = requests.get(f"{base_url}/api/v2/workspaces/current", headers=headers, timeout=10)
        if response.status_code == 200:
            logger.info(f"   ✅ Workspace API OK: {response.json().get('name', 'Unknown')}")
        else:
            logger.error(f"   ❌ Workspace API failed: {response.status_code}")
    except Exception as e:
        logger.error(f"   ❌ Workspace API error: {e}")
    
    # Test 2: Test leads/list WITHOUT campaign_id (this might be the issue!)
    logger.info("\n2️⃣ Testing leads/list WITHOUT campaign_id...")
    try:
        data = {'limit': 10}
        logger.info(f"   Request: POST {base_url}/api/v2/leads/list")
        logger.info(f"   Data: {data}")
        
        response = requests.post(f"{base_url}/api/v2/leads/list", headers=headers, json=data, timeout=10)
        logger.info(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"   ✅ SUCCESS: Got {len(result.get('items', []))} leads")
        else:
            logger.error(f"   ❌ FAILED: {response.text}")
    except Exception as e:
        logger.error(f"   ❌ Exception: {e}")
    
    # Test 3: Test leads/list WITH campaign_id (known working)
    logger.info("\n3️⃣ Testing leads/list WITH campaign_id...")
    try:
        data = {'campaign_id': SMB_CAMPAIGN_ID, 'limit': 10}
        logger.info(f"   Request: POST {base_url}/api/v2/leads/list")
        logger.info(f"   Data: {data}")
        
        response = requests.post(f"{base_url}/api/v2/leads/list", headers=headers, json=data, timeout=10)
        logger.info(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"   ✅ SUCCESS: Got {len(result.get('items', []))} leads")
        else:
            logger.error(f"   ❌ FAILED: {response.text}")
    except Exception as e:
        logger.error(f"   ❌ Exception: {e}")
    
    # Test 4: Test other possible endpoints for all leads
    logger.info("\n4️⃣ Testing alternative endpoints...")
    
    endpoints_to_try = [
        {'endpoint': '/api/v2/leads', 'method': 'GET', 'data': None},
        {'endpoint': '/api/v2/leads', 'method': 'POST', 'data': {'limit': 10}},
    ]
    
    for test in endpoints_to_try:
        try:
            logger.info(f"   Testing {test['method']} {test['endpoint']}...")
            if test['method'] == 'GET':
                response = requests.get(f"{base_url}{test['endpoint']}", headers=headers, timeout=10)
            else:
                response = requests.post(f"{base_url}{test['endpoint']}", headers=headers, json=test['data'], timeout=10)
            
            logger.info(f"   Status: {response.status_code}")
            if response.status_code == 200:
                result = response.json()
                if 'items' in result:
                    logger.info(f"   ✅ Got {len(result['items'])} leads")
                else:
                    logger.info(f"   ✅ Got response: {str(result)[:100]}...")
            else:
                logger.error(f"   ❌ Failed: {response.text[:100]}...")
        except Exception as e:
            logger.error(f"   ❌ Exception: {e}")

def test_pagination_utility():
    """Test our pagination utility to see what's being sent"""
    logger.info("\n🔧 TESTING PAGINATION UTILITY")
    logger.info("=" * 50)
    
    try:
        from shared.pagination_utils import fetch_all_leads
        from shared.api_client import call_instantly_api
        
        # Test the utility that's failing
        logger.info("\n5️⃣ Testing pagination utility (the failing one)...")
        
        # Monkey patch the API call to see exactly what's being sent
        original_call = call_instantly_api
        
        def debug_call_instantly_api(endpoint, method='GET', data=None):
            logger.info(f"   🔍 API CALL INTERCEPTED:")
            logger.info(f"      Endpoint: {method} {endpoint}")
            logger.info(f"      Data: {data}")
            
            # Make the actual call
            try:
                result = original_call(endpoint, method, data)
                logger.info(f"      Result: Success ({len(str(result))} chars)")
                return result
            except Exception as e:
                logger.error(f"      Result: FAILED - {e}")
                raise
        
        # Replace the function temporarily
        import shared.api_client
        shared.api_client.call_instantly_api = debug_call_instantly_api
        
        # Now test the pagination utility
        all_leads, stats = fetch_all_leads(
            api_call_func=debug_call_instantly_api,
            campaign_filter=None,  # This is what's failing
            batch_size=10,  # Small batch for testing
            use_cache=False
        )
        
        logger.info(f"   Result: {len(all_leads)} leads, {stats.total_pages} pages")
        
        # Restore original function
        shared.api_client.call_instantly_api = original_call
        
    except Exception as e:
        logger.error(f"   ❌ Pagination utility failed: {e}")

def main():
    logger.info("🚨 COMPREHENSIVE PAGINATION DEBUG")
    logger.info("=" * 60)
    
    test_api_directly()
    test_pagination_utility()
    
    logger.info("\n📋 SUMMARY")
    logger.info("=" * 30)
    logger.info("Check the logs above to identify:")
    logger.info("1. Which API calls are working vs failing")
    logger.info("2. What exact parameters are being sent")
    logger.info("3. Whether the issue is in our pagination utility or API requirements")

if __name__ == "__main__":
    main()