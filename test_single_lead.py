#!/usr/bin/env python3
"""
Test creating a single lead to debug the issue
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

SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'

def create_test_lead():
    """Create a single test lead"""
    url = 'https://api.instantly.ai/api/v2/leads'
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    data = {
        'email': 'test@example.com',
        'first_name': 'Test',
        'last_name': 'User',
        'company_name': 'Test Company',
        'campaign_id': SMB_CAMPAIGN_ID,
        'custom_variables': {
            'company': 'Test Company',
            'domain': 'test.myshopify.com',
            'location': 'California',
            'country': 'US'
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        print(f"Status Code: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
        print(f"Response Text: {response.text}")
        
        if response.status_code == 200 or response.status_code == 201:
            result = response.json()
            print(f"✅ Success! Response: {result}")
            return result
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")
            return None
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    print("🧪 Testing single lead creation...")
    result = create_test_lead()