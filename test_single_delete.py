#!/usr/bin/env python3
"""
Test single lead deletion with Instantly API to debug 400 errors.
"""

import os
import requests
import json

# Load API key
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
if not INSTANTLY_API_KEY:
    try:
        with open('config/secrets/instantly-config.json', 'r') as f:
            config = json.load(f)
            INSTANTLY_API_KEY = config['api_key']
    except:
        print("❌ No API key found")
        exit(1)

BASE_URL = "https://api.instantly.ai"

def test_lead_lookup_and_delete(email):
    """Test the complete flow: lookup lead ID, verify, delete, verify deletion"""
    
    print(f"\n🔍 Testing delete flow for: {email}")
    
    # Step 1: Find lead by email using global search
    print("1️⃣ Finding canonical lead ID...")
    
    search_payload = {
        "search": email
    }
    
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v2/leads/list",
            headers=headers,
            json=search_payload,
            timeout=30
        )
        
        print(f"   Search response: {response.status_code}")
        
        if response.status_code != 200:
            print(f"   ❌ Search failed: {response.text}")
            return False
            
        search_data = response.json()
        leads = search_data.get('items', [])
        
        if not leads:
            print(f"   ⚠️ No leads found for {email}")
            return False
            
        lead = leads[0]
        lead_id = lead['id']
        print(f"   ✅ Found lead ID: {lead_id}")
        
        # Show lead info
        print(f"   📋 Lead info: {lead.get('email')} - Status: {lead.get('status')} - Campaign: {lead.get('campaign_id', 'unknown')}")
        
    except Exception as e:
        print(f"   ❌ Search error: {e}")
        return False
    
    # Step 2: Verify lead exists with GET
    print("2️⃣ Verifying lead exists...")
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/v2/leads/{lead_id}",
            headers=headers,
            timeout=30
        )
        
        print(f"   GET response: {response.status_code}")
        
        if response.status_code == 200:
            print(f"   ✅ Lead exists and is accessible")
        elif response.status_code == 404:
            print(f"   ⚠️ Lead not found (404) - may already be deleted")
            return True  # This is actually success
        else:
            print(f"   ❌ GET failed: {response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ GET error: {e}")
        return False
    
    # Step 3: Attempt DELETE
    print("3️⃣ Attempting DELETE...")
    
    try:
        # Ensure no body is sent
        response = requests.delete(
            f"{BASE_URL}/api/v2/leads/{lead_id}",
            headers={
                'Authorization': f'Bearer {INSTANTLY_API_KEY}',
                'Accept': 'application/json'
            },  # No Content-Type for DELETE
            timeout=30
        )
        
        print(f"   DELETE response: {response.status_code}")
        print(f"   DELETE response body: {response.text}")
        
        if response.status_code in [200, 204]:
            print(f"   ✅ DELETE successful")
        elif response.status_code == 404:
            print(f"   ✅ DELETE returned 404 (already deleted) - treating as success")
        elif response.status_code == 400:
            print(f"   ❌ DELETE 400 error: {response.text}")
            return False
        else:
            print(f"   ❌ DELETE failed with {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ DELETE error: {e}")
        return False
    
    # Step 4: Verify deletion
    print("4️⃣ Verifying deletion...")
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v2/leads/list",
            headers=headers,
            json=search_payload,
            timeout=30
        )
        
        if response.status_code == 200:
            verify_data = response.json()
            remaining_leads = verify_data.get('items', [])
            
            if not remaining_leads:
                print(f"   ✅ Verification successful - lead deleted")
                return True
            else:
                print(f"   ⚠️ Lead still exists after delete: {len(remaining_leads)} found")
                return False
        else:
            print(f"   ❌ Verification failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ Verification error: {e}")
        return False

if __name__ == "__main__":
    # Test with a known email from the logs
    test_emails = [
        "support@sleepava.com",  # This one appeared in both campaigns
    ]
    
    print(f"🧪 Testing Instantly API Delete Operation")
    print(f"📡 Base URL: {BASE_URL}")
    print(f"🔑 API Key present: {bool(INSTANTLY_API_KEY)}")
    
    for email in test_emails:
        success = test_lead_lookup_and_delete(email)
        print(f"🎯 Result for {email}: {'✅ SUCCESS' if success else '❌ FAILED'}")