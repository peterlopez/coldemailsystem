#!/usr/bin/env python3
"""
Discover available Instantly API endpoints for lead status tracking
"""

import os
import requests
import json
from datetime import datetime

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
        
        print(f"  {method} {endpoint} -> Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            return result
        else:
            print(f"    Error: {response.text}")
            return None
    except Exception as e:
        print(f"    Exception: {e}")
        return None

def test_known_endpoints():
    """Test endpoints we know work from existing code"""
    
    print("🔍 TESTING KNOWN WORKING ENDPOINTS")
    print("=" * 50)
    
    # From sync_once.py and other scripts, these should work:
    working_endpoints = [
        '/api/v1/account/emails',    # get_mailbox_capacity uses this
        '/api/v2/campaigns/8c46e0c9-c1f9-4201-a8d6-6221bafeada6',  # check_campaign_status uses this  
        '/api/v1/campaigns',         # debug_leads.py uses this
    ]
    
    for endpoint in working_endpoints:
        print(f"\n📊 Testing: {endpoint}")
        result = call_api(endpoint)
        
        if result:
            print(f"  ✅ Success!")
            if isinstance(result, dict):
                print(f"  Keys: {list(result.keys())}")
                
                # Look for leads-related data
                for key, value in result.items():
                    if 'lead' in key.lower():
                        print(f"  Lead-related field '{key}': {type(value)}")
                        if isinstance(value, (int, str)):
                            print(f"    Value: {value}")
            elif isinstance(result, list):
                print(f"  List with {len(result)} items")
                if result:
                    print(f"  Sample item keys: {list(result[0].keys()) if isinstance(result[0], dict) else 'Not dict'}")

def discover_lead_endpoints():
    """Try to discover how to get leads with status"""
    
    print(f"\n🔍 DISCOVERING LEAD STATUS ENDPOINTS")
    print("=" * 50)
    
    # Try different endpoint patterns
    endpoint_patterns = [
        # V1 patterns
        f'/api/v1/campaigns/{SMB_CAMPAIGN_ID}',
        f'/api/v1/campaigns/{SMB_CAMPAIGN_ID}/leads', 
        f'/api/v1/campaign/{SMB_CAMPAIGN_ID}',
        f'/api/v1/campaign/{SMB_CAMPAIGN_ID}/leads',
        f'/api/v1/leads',
        f'/api/v1/leads/{SMB_CAMPAIGN_ID}',
        
        # V2 patterns  
        f'/api/v2/campaigns/{SMB_CAMPAIGN_ID}',
        f'/api/v2/campaigns/{SMB_CAMPAIGN_ID}/leads',
        f'/api/v2/campaign/{SMB_CAMPAIGN_ID}',
        f'/api/v2/leads',
        
        # Other common patterns
        f'/api/v1/analytics/{SMB_CAMPAIGN_ID}',
        f'/api/v1/stats/{SMB_CAMPAIGN_ID}',
        f'/api/v1/reports/{SMB_CAMPAIGN_ID}',
    ]
    
    successful_endpoints = []
    
    for endpoint in endpoint_patterns:
        print(f"\n📊 Testing: {endpoint}")
        result = call_api(endpoint)
        
        if result:
            successful_endpoints.append((endpoint, result))
            print(f"  ✅ SUCCESS!")
            
            # Analyze the response
            if isinstance(result, dict):
                keys = list(result.keys())
                print(f"  Response keys: {keys}")
                
                # Look for status-related fields
                for key in keys:
                    if any(word in key.lower() for word in ['status', 'state', 'lead', 'reply', 'bounce']):
                        value = result[key]
                        print(f"  🎯 Relevant field '{key}': {type(value)}")
                        if isinstance(value, (str, int, bool)):
                            print(f"      Value: {value}")
                        elif isinstance(value, list) and value:
                            print(f"      List of {len(value)} items")
                            if isinstance(value[0], dict):
                                print(f"      Sample item keys: {list(value[0].keys())}")
            
def check_campaign_details():
    """Get detailed campaign info to understand lead structure"""
    
    print(f"\n📋 CAMPAIGN DETAILS ANALYSIS")
    print("=" * 50)
    
    # Get campaign info
    campaign_info = call_api(f'/api/v2/campaigns/{SMB_CAMPAIGN_ID}')
    
    if campaign_info:
        print("✅ Campaign info retrieved")
        
        # Look for lead-related fields
        for key, value in campaign_info.items():
            if 'lead' in key.lower() or 'count' in key.lower() or 'total' in key.lower():
                print(f"  📊 {key}: {value}")
        
        # Check if there's a leads array or reference
        if 'leads' in campaign_info:
            leads = campaign_info['leads']
            print(f"  🎯 Found 'leads' field with {len(leads) if isinstance(leads, list) else type(leads)}")
            
            if isinstance(leads, list) and leads:
                sample_lead = leads[0]
                print(f"  Sample lead structure:")
                for key, value in sample_lead.items():
                    print(f"    - {key}: {type(value)} = {value}")
        
        # Look for other relevant data
        important_fields = ['status', 'statistics', 'analytics', 'metrics']
        for field in important_fields:
            if field in campaign_info:
                print(f"  🎯 {field}: {campaign_info[field]}")

def main():
    print("🚀 INSTANTLY API ENDPOINT DISCOVERY")
    print(f"Timestamp: {datetime.now()}")
    
    # Test known working endpoints first
    test_known_endpoints()
    
    # Try to discover lead endpoints
    discover_lead_endpoints()
    
    # Get detailed campaign info
    check_campaign_details()
    
    print(f"\n📋 SUMMARY")
    print("=" * 50)
    print("After discovery, we need to:")
    print("1. Find the correct endpoint to get leads with status")
    print("2. Understand the lead object structure")
    print("3. Identify how to filter by status (replied, completed, etc.)")
    print("4. Implement proper OOO detection logic")
    print("5. Update get_finished_leads() function with working API call")

if __name__ == "__main__":
    main()