#!/usr/bin/env python3
"""
Test different campaign assignment parameter formats
"""

import os
import requests
import json
import time

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

def test_campaign_assignment_format(test_name, data):
    """Test a specific campaign assignment format"""
    url = 'https://api.instantly.ai/api/v2/leads'
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        print(f"\n🧪 {test_name}")
        print(f"Data sent: {json.dumps(data, indent=2)}")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code in [200, 201]:
            result = response.json()
            print(f"✅ SUCCESS!")
            print(f"Lead ID: {result.get('id')}")
            print(f"Campaign in response: {result.get('campaign', 'NOT FOUND')}")
            return result.get('id')
        else:
            print(f"❌ FAILED: {response.text}")
            return None
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return None

def main():
    print("🔍 TESTING CAMPAIGN ASSIGNMENT FORMATS")
    print("=" * 50)
    
    # Test 1: Using "campaign_id" (current approach)
    test1_data = {
        'email': 'test1@campaigntest.com',
        'first_name': 'Test',
        'last_name': 'User1',
        'company_name': 'Test Company 1',
        'campaign_id': SMB_CAMPAIGN_ID,
        'custom_variables': {
            'company': 'Test Company 1',
            'domain': 'test1.myshopify.com'
        }
    }
    
    # Test 2: Using "campaign" (based on API docs)
    test2_data = {
        'email': 'test2@campaigntest.com',
        'first_name': 'Test',
        'last_name': 'User2', 
        'company_name': 'Test Company 2',
        'campaign': SMB_CAMPAIGN_ID,
        'custom_variables': {
            'company': 'Test Company 2',
            'domain': 'test2.myshopify.com'
        }
    }
    
    # Test 3: Using "campaignId" (camelCase)
    test3_data = {
        'email': 'test3@campaigntest.com',
        'first_name': 'Test',
        'last_name': 'User3',
        'company_name': 'Test Company 3', 
        'campaignId': SMB_CAMPAIGN_ID,
        'custom_variables': {
            'company': 'Test Company 3',
            'domain': 'test3.myshopify.com'
        }
    }
    
    # Run tests
    lead1_id = test_campaign_assignment_format("Test 1: campaign_id parameter", test1_data)
    time.sleep(1)
    
    lead2_id = test_campaign_assignment_format("Test 2: campaign parameter", test2_data)
    time.sleep(1)
    
    lead3_id = test_campaign_assignment_format("Test 3: campaignId parameter", test3_data)
    
    print("\n" + "=" * 50)
    print("🎯 RESULTS SUMMARY:")
    print("Now check your Instantly dashboard to see which leads")
    print("were assigned to the SMB campaign:")
    print(f"- Test 1 (campaign_id): test1@campaigntest.com")
    print(f"- Test 2 (campaign): test2@campaigntest.com") 
    print(f"- Test 3 (campaignId): test3@campaigntest.com")
    print(f"\nSMB Campaign ID: {SMB_CAMPAIGN_ID}")

if __name__ == "__main__":
    main()