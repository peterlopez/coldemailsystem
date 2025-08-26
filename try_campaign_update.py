#!/usr/bin/env python3
"""
Try different approaches to update campaign settings via API
"""

import os
import requests
import json

# Get API key
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
if not INSTANTLY_API_KEY:
    try:
        from config.config import Config
        config = Config()
        INSTANTLY_API_KEY = config.instantly_api_key
    except:
        print("❌ Could not load API key")
        exit(1)

INSTANTLY_BASE_URL = 'https://api.instantly.ai'
SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'

def call_api(endpoint, method='GET', data=None):
    """Call Instantly API"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=data, timeout=30)
        elif method == 'PUT':
            response = requests.put(url, headers=headers, json=data, timeout=30)
        elif method == 'PATCH':
            response = requests.patch(url, headers=headers, json=data, timeout=30)
        
        print(f"\n{method} {endpoint}")
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Success! Keys in response: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
            return result
        else:
            print(f"Response: {response.text}")
            return None
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def main():
    print("🔍 EXPLORING CAMPAIGN API ENDPOINTS")
    print("=" * 50)
    
    # Try to get campaign info to see structure
    endpoints_to_try = [
        f'/api/v2/campaign/{SMB_CAMPAIGN_ID}',
        f'/api/v1/campaign/{SMB_CAMPAIGN_ID}',
        f'/api/v2/campaigns/{SMB_CAMPAIGN_ID}',
        f'/api/v1/campaigns/{SMB_CAMPAIGN_ID}',
    ]
    
    campaign_data = None
    for endpoint in endpoints_to_try:
        result = call_api(endpoint)
        if result:
            campaign_data = result
            print(f"✅ Found working endpoint: {endpoint}")
            break
    
    if not campaign_data:
        print("❌ No working campaign endpoint found")
        return
    
    # Show campaign structure
    print(f"\n📊 Campaign data structure:")
    for key, value in campaign_data.items():
        if isinstance(value, (str, int, bool)):
            print(f"  {key}: {value}")
        else:
            print(f"  {key}: {type(value).__name__} ({len(value) if hasattr(value, '__len__') else 'N/A'})")
    
    # Look for mailbox/email related fields
    mailbox_fields = []
    for key in campaign_data.keys():
        if any(term in key.lower() for term in ['email', 'mailbox', 'account', 'inbox', 'sender']):
            mailbox_fields.append(key)
    
    if mailbox_fields:
        print(f"\n📧 Potential mailbox fields found:")
        for field in mailbox_fields:
            value = campaign_data[field]
            print(f"  {field}: {value}")
    else:
        print(f"\n⚠️ No obvious mailbox fields found in campaign data")
    
    # Try some update approaches
    print(f"\n🔄 Testing update endpoints...")
    
    # Test data - small change first
    test_updates = [
        {'name': campaign_data.get('name', 'Test Campaign')},  # Safe update
        {'emails': ['rohan.s@getrippleaiagency.com']},  # Test email assignment
        {'accounts': ['rohan.s@getrippleaiagency.com']},  # Test account assignment
        {'mailboxes': ['rohan.s@getrippleaiagency.com']},  # Test mailbox assignment
    ]
    
    update_endpoints = [
        f'/api/v2/campaign/{SMB_CAMPAIGN_ID}',
        f'/api/v1/campaign/{SMB_CAMPAIGN_ID}',
    ]
    
    methods = ['PUT', 'PATCH', 'POST']
    
    for test_data in test_updates:
        print(f"\n🧪 Testing update with: {test_data}")
        for endpoint in update_endpoints:
            for method in methods:
                result = call_api(endpoint, method, test_data)
                if result:
                    print(f"✅ SUCCESS with {method} {endpoint}")
                    return
    
    print(f"\n❌ No successful update methods found")
    print(f"💡 You may need to update mailboxes manually in the Instantly dashboard")

if __name__ == "__main__":
    main()