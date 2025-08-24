#!/usr/bin/env python3
"""
Debug script to check campaign and mailbox status in Instantly.ai
"""

import os
import sys
import requests
import json
from typing import Dict, Any

# Load API key from environment or config
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')

if not INSTANTLY_API_KEY:
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from config.config import Config
        config = Config()
        INSTANTLY_API_KEY = config.instantly_api_key
        print("✅ Loaded API key from config file")
    except Exception as e:
        print(f"❌ Failed to load API key: {e}")
        sys.exit(1)

INSTANTLY_BASE_URL = 'https://api.instantly.ai'
HEADERS = {
    'Authorization': f'Bearer {INSTANTLY_API_KEY}',
    'Content-Type': 'application/json'
}

# Campaign IDs to check
SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
MIDSIZE_CAMPAIGN_ID = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'

def call_api(endpoint: str, method: str = 'GET') -> Dict[Any, Any]:
    """Make API call with error handling"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    try:
        if method == 'GET':
            response = requests.get(url, headers=HEADERS, timeout=30)
        else:
            response = requests.post(url, headers=HEADERS, timeout=30)
        
        print(f"API Call: {method} {endpoint}")
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error Response: {response.text}")
            return {"error": f"HTTP {response.status_code}", "details": response.text}
    except Exception as e:
        print(f"Exception: {e}")
        return {"error": str(e)}

def main():
    print("🔍 DEBUGGING INSTANTLY CAMPAIGN STATUS")
    print("=" * 50)
    
    # Check campaigns list
    print("\n📋 1. CHECKING CAMPAIGNS LIST")
    campaigns_data = call_api('/api/v2/campaigns')
    
    if 'error' not in campaigns_data:
        campaigns = campaigns_data.get('items', campaigns_data)
        print(f"Found {len(campaigns)} campaigns")
        
        for campaign in campaigns:
            campaign_id = campaign.get('id')
            name = campaign.get('name', 'Unknown')
            status = campaign.get('status', 'Unknown')
            
            if campaign_id in [SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID]:
                print(f"\n🎯 TARGET CAMPAIGN: {name}")
                print(f"   ID: {campaign_id}")
                print(f"   Status: {status}")
                print(f"   Full details: {json.dumps(campaign, indent=2)}")
    else:
        print(f"❌ Failed to get campaigns: {campaigns_data}")
    
    # Check specific campaign details
    for camp_name, camp_id in [("SMB", SMB_CAMPAIGN_ID), ("Midsize", MIDSIZE_CAMPAIGN_ID)]:
        print(f"\n📊 2. CHECKING {camp_name} CAMPAIGN DETAILS")
        campaign_detail = call_api(f'/api/v2/campaigns/{camp_id}')
        
        if 'error' not in campaign_detail:
            print(f"✅ {camp_name} Campaign found:")
            print(f"   Status: {campaign_detail.get('status')}")
            print(f"   Name: {campaign_detail.get('name')}")
            print(f"   Leads count: {campaign_detail.get('leads_count', 'N/A')}")
            print(f"   Full details: {json.dumps(campaign_detail, indent=2)}")
        else:
            print(f"❌ Failed to get {camp_name} campaign: {campaign_detail}")
    
    # Check leads in campaigns
    for camp_name, camp_id in [("SMB", SMB_CAMPAIGN_ID), ("Midsize", MIDSIZE_CAMPAIGN_ID)]:
        print(f"\n👥 3. CHECKING LEADS IN {camp_name} CAMPAIGN")
        leads_data = call_api(f'/api/v2/campaigns/{camp_id}/leads')
        
        if 'error' not in leads_data:
            leads = leads_data.get('items', leads_data)
            print(f"✅ Found {len(leads)} leads in {camp_name}")
            
            # Show recent leads
            for i, lead in enumerate(leads[:5]):  # Show first 5
                email = lead.get('email', 'N/A')
                status = lead.get('status', 'N/A')
                created = lead.get('created_at', 'N/A')
                print(f"   Lead {i+1}: {email} | Status: {status} | Created: {created}")
        else:
            print(f"❌ Failed to get leads for {camp_name}: {leads_data}")
    
    # Check mailboxes (try both v1 and v2)
    print(f"\n📧 4. CHECKING MAILBOXES")
    
    # Try V2 first
    mailboxes_v2 = call_api('/api/v2/account/emails')
    if 'error' not in mailboxes_v2:
        emails = mailboxes_v2.get('emails', mailboxes_v2)
        print(f"✅ V2 API: Found {len(emails)} mailboxes")
        for email in emails[:3]:  # Show first 3
            email_addr = email.get('email', 'N/A')
            status = email.get('status', 'N/A') 
            print(f"   {email_addr} | Status: {status}")
    else:
        print(f"❌ V2 Mailboxes failed: {mailboxes_v2}")
        
        # Try V1 as fallback
        mailboxes_v1 = call_api('/api/v1/account/emails')
        if 'error' not in mailboxes_v1:
            emails = mailboxes_v1.get('emails', mailboxes_v1)
            print(f"✅ V1 API: Found {len(emails)} mailboxes")
        else:
            print(f"❌ V1 Mailboxes also failed: {mailboxes_v1}")
    
    # Check campaign mailbox assignments
    for camp_name, camp_id in [("SMB", SMB_CAMPAIGN_ID), ("Midsize", MIDSIZE_CAMPAIGN_ID)]:
        print(f"\n📫 5. CHECKING {camp_name} CAMPAIGN MAILBOX ASSIGNMENTS")
        campaign_emails = call_api(f'/api/v2/campaigns/{camp_id}/emails')
        
        if 'error' not in campaign_emails:
            emails = campaign_emails.get('emails', campaign_emails)
            print(f"✅ {camp_name} has {len(emails)} assigned mailboxes")
            
            if len(emails) == 0:
                print(f"⚠️ WARNING: {camp_name} campaign has NO assigned mailboxes!")
                print("   This is likely why leads aren't processing!")
                
            for email in emails[:3]:  # Show first 3
                email_addr = email.get('email', 'N/A')
                status = email.get('status', 'N/A')
                print(f"   {email_addr} | Status: {status}")
        else:
            print(f"❌ Failed to get {camp_name} campaign emails: {campaign_emails}")
    
    print("\n" + "=" * 50)
    print("🏁 DEBUG COMPLETE")
    print("\nKey things to check:")
    print("1. Campaign status should be 1 (active)")
    print("2. Campaigns should have assigned mailboxes")  
    print("3. Assigned mailboxes should have active status")
    print("4. Recent leads should appear in campaign lead lists")

if __name__ == "__main__":
    main()