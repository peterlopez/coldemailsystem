#!/usr/bin/env python3
"""
Test script to understand how Instantly API handles lead statuses
and identify the OOO/automated response problem.
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
MIDSIZE_CAMPAIGN_ID = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'

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
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ API Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Exception: {e}")
        return None

def test_campaign_leads_endpoint():
    """Test different ways to get leads with status information"""
    
    print("🔍 TESTING INSTANTLY API LEAD STATUS ENDPOINTS")
    print("=" * 60)
    
    # Test 1: Get campaign leads (V1)
    print("\n📊 Test 1: Get SMB Campaign Leads (V1 API)")
    print("-" * 40)
    
    leads_v1 = call_api(f'/api/v1/campaign/{SMB_CAMPAIGN_ID}/leads')
    if leads_v1:
        if 'leads' in leads_v1:
            leads = leads_v1['leads']
            print(f"✅ Found {len(leads)} leads")
            
            # Analyze lead statuses
            status_counts = {}
            replied_leads = []
            sample_leads = []
            
            for lead in leads:
                status = lead.get('status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
                
                # Collect leads with replies
                if 'replied' in status.lower() or 'reply' in status.lower():
                    replied_leads.append(lead)
                
                # Sample first few leads
                if len(sample_leads) < 3:
                    sample_leads.append(lead)
            
            print(f"\n📈 Status Distribution:")
            for status, count in status_counts.items():
                print(f"  - {status}: {count} leads")
            
            print(f"\n🔍 Sample Lead Data:")
            for i, lead in enumerate(sample_leads):
                print(f"  Lead {i+1}:")
                print(f"    Email: {lead.get('email', 'Unknown')}")
                print(f"    Status: {lead.get('status', 'Unknown')}")
                print(f"    Created: {lead.get('created_at', 'Unknown')}")
                
                # Look for additional status fields
                for key, value in lead.items():
                    if 'status' in key.lower() or 'reply' in key.lower():
                        print(f"    {key}: {value}")
            
            if replied_leads:
                print(f"\n💬 REPLIED LEADS ANALYSIS ({len(replied_leads)} found):")
                for lead in replied_leads[:5]:  # Show first 5 replied leads
                    print(f"  - {lead.get('email')}: {lead.get('status')}")
                    
                    # Look for reply content or timestamps
                    for key, value in lead.items():
                        if 'reply' in key.lower() or 'message' in key.lower():
                            print(f"    {key}: {str(value)[:100]}")
        else:
            print("❌ No 'leads' field in response")
            print(f"Response structure: {list(leads_v1.keys())}")
    else:
        print("❌ Could not get campaign leads")
    
    # Test 2: Try V2 API
    print(f"\n📊 Test 2: V2 API Endpoints")
    print("-" * 40)
    
    # Try to get leads via V2
    v2_endpoints = [
        f'/api/v2/campaigns/{SMB_CAMPAIGN_ID}/leads',
        f'/api/v2/leads?campaign_id={SMB_CAMPAIGN_ID}',
        '/api/v2/leads'
    ]
    
    for endpoint in v2_endpoints:
        print(f"Testing: {endpoint}")
        result = call_api(endpoint)
        if result:
            print(f"  ✅ Success: {type(result)} with keys: {list(result.keys()) if isinstance(result, dict) else 'List'}")
            
            # If it's a successful response with leads, analyze it
            if isinstance(result, dict) and 'leads' in result:
                print(f"  Found {len(result['leads'])} leads")
            elif isinstance(result, list):
                print(f"  Found {len(result)} items")
        else:
            print(f"  ❌ Failed")
    
    # Test 3: Try to get leads by status
    print(f"\n📊 Test 3: Filter by Status")
    print("-" * 40)
    
    status_filters = ['replied', 'completed', 'bounced', 'unsubscribed', 'active']
    
    for status in status_filters:
        endpoint = f'/api/v1/campaign/{SMB_CAMPAIGN_ID}/leads?status={status}'
        print(f"Testing status filter: {status}")
        result = call_api(endpoint)
        
        if result and 'leads' in result:
            count = len(result['leads'])
            print(f"  ✅ {count} leads with status '{status}'")
        else:
            print(f"  ❌ No leads or failed")

def analyze_reply_types():
    """Analyze different types of replies to identify OOO messages"""
    
    print("\n🤖 AUTOMATED REPLY DETECTION ANALYSIS")
    print("=" * 60)
    
    # Get all replied leads
    leads_response = call_api(f'/api/v1/campaign/{SMB_CAMPAIGN_ID}/leads')
    if not leads_response or 'leads' not in leads_response:
        print("❌ Cannot get leads for analysis")
        return
    
    replied_leads = [lead for lead in leads_response['leads'] 
                    if 'replied' in str(lead.get('status', '')).lower()]
    
    print(f"📧 Found {len(replied_leads)} leads with replied status")
    
    if not replied_leads:
        print("⚠️ No replied leads found to analyze")
        return
    
    # Common OOO/automated response patterns
    ooo_patterns = [
        'out of office', 'ooo', 'auto-reply', 'automatic reply',
        'vacation', 'holiday', 'away from office', 'temporarily away',
        'not available', 'will not be', 'returning on', 'back on',
        'limited email access', 'auto response', 'automated message',
        'sick leave', 'maternity leave', 'parental leave',
        'conference', 'travel', 'unavailable until'
    ]
    
    print(f"\n🔍 Analyzing reply patterns...")
    print(f"Looking for OOO indicators: {', '.join(ooo_patterns[:5])}...")
    
    # This is where we'd analyze reply content if available in the API
    # For now, just show the structure
    for i, lead in enumerate(replied_leads[:3]):
        print(f"\n📨 Reply Example {i+1}:")
        print(f"  Email: {lead.get('email')}")
        print(f"  Status: {lead.get('status')}")
        print(f"  Reply Time: {lead.get('replied_at', 'Unknown')}")
        
        # Look for any reply content fields
        reply_fields = [key for key in lead.keys() 
                       if 'reply' in key.lower() or 'message' in key.lower()]
        
        if reply_fields:
            print(f"  Available reply fields: {reply_fields}")
            for field in reply_fields:
                content = str(lead.get(field, ''))[:200]
                print(f"  {field}: {content}")
        else:
            print("  ⚠️ No reply content fields available in API response")

def main():
    print("🚀 INSTANTLY API LEAD STATUS ANALYSIS")
    print(f"Timestamp: {datetime.now()}")
    print(f"Testing campaigns: SMB ({SMB_CAMPAIGN_ID})")
    
    # Test the API endpoints
    test_campaign_leads_endpoint()
    
    # Analyze reply types
    analyze_reply_types()
    
    print(f"\n📋 KEY FINDINGS & RECOMMENDATIONS:")
    print("=" * 60)
    print("1. Check what lead status values are actually returned")
    print("2. Determine if reply content is accessible via API")
    print("3. Identify pattern matching needed for OOO detection")
    print("4. Understand current drain logic in get_finished_leads()")
    print("5. Implement smart reply filtering to avoid removing genuine replies")
    
    print(f"\n🔧 NEXT STEPS:")
    print("- Update get_finished_leads() to actually fetch from API")
    print("- Add OOO pattern detection before marking as 'replied'")
    print("- Implement proper status tracking and filtering")
    print("- Test with actual campaign data")

if __name__ == "__main__":
    main()