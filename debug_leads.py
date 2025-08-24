#!/usr/bin/env python3
"""
Debug script to check why leads aren't appearing in Instantly
"""

import os
import requests
import json
from datetime import datetime, timezone

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

def call_api(endpoint, method='GET'):
    """Call Instantly API"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, timeout=30)
        else:
            response = requests.post(url, headers=headers, timeout=30)
        
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ API Error for {endpoint}: {e}")
        return None

def main():
    print("🔍 DEBUGGING INSTANTLY LEAD VISIBILITY")
    print("=" * 50)
    
    # 1. Check API key works - try multiple endpoints
    print("\n1. Testing API Authentication...")
    
    # Try different account endpoints
    endpoints_to_try = [
        '/api/v1/account',
        '/api/v2/account', 
        '/api/v1/account/info',
        '/api/v1/campaigns'  # This should work if API key is valid
    ]
    
    api_working = False
    for endpoint in endpoints_to_try:
        result = call_api(endpoint)
        if result:
            print(f"✅ API Key working - {endpoint} returned data")
            api_working = True
            break
    
    if not api_working:
        print("❌ API Authentication failed on all endpoints")
        print("Continuing debug anyway...")
    
    # 2. Check campaign status
    print("\n2. Checking Campaign Status...")
    for campaign_name, campaign_id in [("SMB", SMB_CAMPAIGN_ID), ("Midsize", MIDSIZE_CAMPAIGN_ID)]:
        campaign_info = call_api(f'/api/v1/campaign/{campaign_id}')
        if campaign_info:
            status_map = {1: "Active", 2: "Paused", 3: "Stopped"}
            status = status_map.get(campaign_info.get('status'), 'Unknown')
            print(f"  {campaign_name} Campaign: {status} (status: {campaign_info.get('status')})")
            print(f"    Name: {campaign_info.get('name', 'Unknown')}")
            print(f"    Leads: {campaign_info.get('leads_count', 0)}")
        else:
            print(f"  ❌ Could not get {campaign_name} campaign info")
    
    # 3. Check recent leads in SMB campaign
    print(f"\n3. Checking Recent Leads in SMB Campaign...")
    leads_info = call_api(f'/api/v1/campaign/{SMB_CAMPAIGN_ID}/leads')
    if leads_info and 'leads' in leads_info:
        leads = leads_info['leads']
        print(f"  Total leads in campaign: {len(leads)}")
        
        # Look for our recent test leads
        test_emails = [
            'info@estrella-boutique.com',
            'trendifystore689@gmail.com', 
            'contact@rituelleboutique.com'
        ]
        
        recent_leads = []
        for lead in leads[-20:]:  # Check last 20 leads
            if lead.get('email') in test_emails:
                recent_leads.append(lead)
            
        if recent_leads:
            print("  ✅ Found our test leads:")
            for lead in recent_leads:
                print(f"    - {lead.get('email')} (Status: {lead.get('status', 'Unknown')})")
        else:
            print("  ⚠️ Our test leads not found in campaign")
            print("  Last 5 leads in campaign:")
            for lead in leads[-5:]:
                print(f"    - {lead.get('email', 'Unknown')} (Added: {lead.get('created_at', 'Unknown')})")
    else:
        print("  ❌ Could not get campaign leads")
    
    # 4. Check if leads are in a different status
    print(f"\n4. Searching All Campaigns for Test Leads...")
    campaigns_info = call_api('/api/v1/campaigns')
    if campaigns_info and 'campaigns' in campaigns_info:
        test_emails = ['info@estrella-boutique.com', 'trendifystore689@gmail.com']
        
        for campaign in campaigns_info['campaigns']:
            campaign_leads = call_api(f'/api/v1/campaign/{campaign["id"]}/leads')
            if campaign_leads and 'leads' in campaign_leads:
                for lead in campaign_leads['leads']:
                    if lead.get('email') in test_emails:
                        print(f"  ✅ Found {lead['email']} in campaign: {campaign['name']} (ID: {campaign['id']})")
    
    # 5. Try to get lead by email directly
    print(f"\n5. Direct Lead Search...")
    test_email = 'info@estrella-boutique.com'
    lead_search = call_api(f'/api/v1/leads?email={test_email}')
    if lead_search:
        print(f"  Direct search result for {test_email}: {lead_search}")
    else:
        print(f"  ❌ Could not find {test_email} via direct search")
    
    print("\n" + "=" * 50)
    print("🔍 DEBUG COMPLETE")
    print("\nIf leads are not found, check:")
    print("1. Campaign has active mailboxes assigned")
    print("2. Campaign status is 'Active' (not Paused)")  
    print("3. Wait 5-10 minutes for dashboard sync")
    print("4. Check different browser/clear cache")

if __name__ == "__main__":
    main()