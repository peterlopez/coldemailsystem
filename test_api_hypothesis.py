#!/usr/bin/env python3
"""
Test the core hypothesis: API doesn't support leads/list without campaign_id
"""

import requests
from shared_config import config
from shared.api_client import SMB_CAMPAIGN_ID

def test_hypothesis():
    api_key = config.api.instantly_api_key
    headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
    url = "https://api.instantly.ai/api/v2/leads/list"
    
    print("🔍 TESTING API HYPOTHESIS")
    print("=" * 40)
    
    # Test 1: WITHOUT campaign_id (what pagination utility tries)
    print("\n1️⃣ Testing WITHOUT campaign_id (current pagination approach):")
    data1 = {'limit': 10}
    print(f"   Data: {data1}")
    
    try:
        response = requests.post(url, headers=headers, json=data1, timeout=10)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"   ✅ SUCCESS: {len(result.get('items', []))} leads")
        else:
            print(f"   ❌ FAILED: {response.text}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    # Test 2: WITH campaign_id (what works everywhere else)
    print("\n2️⃣ Testing WITH campaign_id (working approach):")
    data2 = {'campaign_id': SMB_CAMPAIGN_ID, 'limit': 10}
    print(f"   Data: {data2}")
    
    try:
        response = requests.post(url, headers=headers, json=data2, timeout=10)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"   ✅ SUCCESS: {len(result.get('items', []))} leads")
        else:
            print(f"   ❌ FAILED: {response.text}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")

if __name__ == "__main__":
    test_hypothesis()