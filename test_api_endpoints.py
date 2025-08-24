#!/usr/bin/env python3
"""
Test script to verify Instantly.ai API V2 endpoints and lead creation
"""

import os
import sys
import requests
import json
import time
from typing import Dict, Any, Optional

# Load API key
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

SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'

def call_api(endpoint: str, method: str = 'GET', data: Optional[Dict] = None) -> Dict[Any, Any]:
    """Make API call with detailed logging"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    
    print(f"\n🔗 API Call: {method} {endpoint}")
    if data:
        print(f"📤 Payload: {json.dumps(data, indent=2)}")
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=HEADERS, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=HEADERS, json=data, timeout=30)
        elif method == 'DELETE':
            response = requests.delete(url, headers=HEADERS, timeout=30)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        print(f"📥 Status: {response.status_code}")
        print(f"📥 Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            try:
                result = response.json()
                print(f"📥 Response: {json.dumps(result, indent=2)}")
                return result
            except json.JSONDecodeError:
                print(f"📥 Raw Response: {response.text}")
                return {"error": "Invalid JSON response", "raw": response.text}
        else:
            print(f"❌ Error Response: {response.text}")
            return {"error": f"HTTP {response.status_code}", "details": response.text}
            
    except Exception as e:
        print(f"💥 Exception: {e}")
        return {"error": str(e)}

def test_lead_creation():
    """Test the exact lead creation process"""
    
    print("🧪 TESTING LEAD CREATION PROCESS")
    print("=" * 50)
    
    # Test data - using a test email
    test_lead_data = {
        'email': 'test-debug-lead@example.com',
        'first_name': 'Debug',
        'last_name': 'Test',
        'company_name': 'Test Company',
        'campaign_id': SMB_CAMPAIGN_ID,
        'custom_variables': {
            'company': 'Test Company',
            'domain': 'test.myshopify.com',
            'location': 'CA',
            'country': 'US'
        }
    }
    
    print("📝 1. Testing V2 Lead Creation")
    result = call_api('/api/v2/leads', method='POST', data=test_lead_data)
    
    if 'error' not in result:
        lead_id = result.get('id')
        if lead_id:
            print(f"✅ Lead created successfully with ID: {lead_id}")
            
            # Wait a moment then check if it appears in campaign
            print("\n⏳ Waiting 5 seconds before checking campaign...")
            time.sleep(5)
            
            print(f"📋 2. Checking if lead appears in campaign {SMB_CAMPAIGN_ID}")
            campaign_leads = call_api(f'/api/v2/campaigns/{SMB_CAMPAIGN_ID}/leads')
            
            if 'error' not in campaign_leads:
                leads = campaign_leads.get('items', campaign_leads)
                found_lead = None
                for lead in leads:
                    if lead.get('email') == test_lead_data['email']:
                        found_lead = lead
                        break
                
                if found_lead:
                    print(f"✅ Lead found in campaign!")
                    print(f"   Status: {found_lead.get('status')}")
                    print(f"   Created: {found_lead.get('created_at')}")
                else:
                    print(f"❌ Lead NOT found in campaign leads list")
                    print(f"   Campaign has {len(leads)} total leads")
            else:
                print(f"❌ Failed to get campaign leads: {campaign_leads}")
            
            # Clean up - delete the test lead
            print(f"\n🧹 3. Cleaning up test lead")
            delete_result = call_api(f'/api/v2/leads/{lead_id}', method='DELETE')
            
            if 'error' not in delete_result:
                print("✅ Test lead deleted successfully")
            else:
                print(f"⚠️ Failed to delete test lead: {delete_result}")
        else:
            print(f"❌ Lead creation response missing 'id' field: {result}")
    else:
        print(f"❌ Lead creation failed: {result}")

def test_campaign_status():
    """Check current campaign status and configuration"""
    
    print("\n🎯 TESTING CAMPAIGN STATUS")
    print("=" * 50)
    
    # Get campaign details
    print(f"📊 Checking SMB Campaign {SMB_CAMPAIGN_ID}")
    campaign = call_api(f'/api/v2/campaigns/{SMB_CAMPAIGN_ID}')
    
    if 'error' not in campaign:
        status = campaign.get('status')
        print(f"Campaign Status: {status}")
        
        status_meanings = {
            1: "Active",
            2: "Paused", 
            3: "Completed",
            4: "Draft"
        }
        
        status_text = status_meanings.get(status, f"Unknown ({status})")
        print(f"Status Meaning: {status_text}")
        
        if status == 2:
            print("⚠️ WARNING: Campaign is PAUSED - this explains why leads aren't processing!")
            print("   Solution: Reactivate the campaign in Instantly dashboard")
        elif status == 1:
            print("✅ Campaign is ACTIVE")
        
        # Check other important fields
        print(f"Name: {campaign.get('name')}")
        print(f"Leads Count: {campaign.get('leads_count', 'N/A')}")
        print(f"Created: {campaign.get('created_at', 'N/A')}")
    else:
        print(f"❌ Failed to get campaign: {campaign}")

def main():
    print("🔍 INSTANTLY.AI API V2 ENDPOINT TESTING")
    print("=" * 60)
    
    # Test basic connectivity
    print("🌐 1. Testing API Connectivity")
    campaigns = call_api('/api/v2/campaigns')
    if 'error' not in campaigns:
        print("✅ API connectivity OK")
    else:
        print(f"❌ API connectivity failed: {campaigns}")
        return
    
    # Test campaign status
    test_campaign_status()
    
    # Test lead creation process
    test_lead_creation()
    
    print("\n" + "=" * 60)
    print("🏁 TESTING COMPLETE")
    print("\nIf leads are being created but not appearing:")
    print("1. Campaign might be paused (Status 2)")
    print("2. Campaign might have no assigned mailboxes")
    print("3. There might be a delay in dashboard updates")
    print("4. API v2 might have different behavior than expected")

if __name__ == "__main__":
    main()