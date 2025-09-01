#!/usr/bin/env python3
"""
Delete specific test leads we created during testing
"""

import os
import requests
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

def delete_lead_by_id(lead_id, email):
    """Delete a lead by ID"""
    url = f'https://api.instantly.ai/api/v2/leads/{lead_id}'
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.delete(url, headers=headers, json={}, timeout=30)
        if response.status_code in [200, 204]:
            print(f"✅ Deleted: {email}")
            return True
        else:
            print(f"❌ Failed to delete {email}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error deleting {email}: {e}")
        return False

def main():
    print("🗑️  DELETING TEST LEADS")
    print("=" * 30)
    
    # List of test leads we created (with their IDs from our tests)
    test_leads = [
        # From our testing sessions
        {'id': 'f9ca06b7-cb81-4faf-89c2-8a7b02482063', 'email': 'test@example.com'},
        {'id': '37955ea1-deb9-4fb7-82fc-28b4293f442f', 'email': 'test1@campaigntest.com'},
        {'id': '626246c7-f9a2-45f7-afee-d1bf3ce83218', 'email': 'test2@campaigntest.com'},
        {'id': '2d16733c-cbbe-4653-95c5-bc4335b99688', 'email': 'test3@campaigntest.com'},
        {'id': 'ee282705-0aab-492e-b050-c616fe756d8f', 'email': 'sales@sprocketzone.co.uk'},
    ]
    
    print(f"Attempting to delete {len(test_leads)} test leads...")
    
    deleted_count = 0
    for lead in test_leads:
        if delete_lead_by_id(lead['id'], lead['email']):
            deleted_count += 1
        time.sleep(0.5)  # Rate limiting
    
    print(f"\n✅ Successfully deleted {deleted_count}/{len(test_leads)} test leads")
    
    # Also try to delete any leads with test email patterns
    print("\n🔍 For any remaining test leads, manually check your Instantly dashboard for:")
    print("  - Any emails containing 'test'")
    print("  - Any emails containing 'campaigntest'")  
    print("  - Any emails from our recent testing")
    
    print("\n🧹 System ready for clean testing!")

if __name__ == "__main__":
    main()