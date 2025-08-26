#!/usr/bin/env python3
"""
Diagnose pagination issues - why are we seeing double the leads?
"""
import os
import requests
import json
import time

# Load API key
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
if not INSTANTLY_API_KEY:
    with open('config/secrets/instantly-config.json', 'r') as f:
        config = json.load(f)
        INSTANTLY_API_KEY = config['api_key']

headers = {
    'Authorization': f'Bearer {INSTANTLY_API_KEY}',
    'Content-Type': 'application/json'
}

# Check both campaigns
campaigns = [
    ("SMB", "8c46e0c9-c1f9-4201-a8d6-6221bafeada6", 1401),
    ("Midsize", "5ffbe8c3-dc0e-41e4-9999-48f00d2015df", 487)
]

print("PAGINATION DIAGNOSTIC")
print("=" * 60)
print(f"Expected total: 1,888 leads (1,401 SMB + 487 Midsize)")
print()

for name, campaign_id, expected_count in campaigns:
    print(f"\n{name} Campaign Analysis:")
    print(f"Expected: {expected_count} leads")
    
    # Track all lead IDs we see
    all_lead_ids = []
    unique_lead_ids = set()
    unique_emails = set()
    
    # Get leads page by page
    starting_after = None
    page_count = 0
    
    while page_count < 5:  # Just check first 5 pages
        payload = {
            "campaign_id": campaign_id,
            "limit": 50
        }
        
        if starting_after:
            payload["starting_after"] = starting_after
            
        if page_count > 0:
            time.sleep(1)  # Rate limiting
            
        response = requests.post(
            "https://api.instantly.ai/api/v2/leads/list",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            leads = data.get('items', [])
            
            if not leads:
                break
                
            page_count += 1
            
            # Track lead IDs
            for lead in leads:
                lead_id = lead.get('id')
                email = lead.get('email')
                
                if lead_id:
                    all_lead_ids.append(lead_id)
                    unique_lead_ids.add(lead_id)
                    
                if email:
                    unique_emails.add(email)
            
            print(f"\nPage {page_count}:")
            print(f"  Leads in page: {len(leads)}")
            print(f"  Total lead IDs seen: {len(all_lead_ids)}")
            print(f"  Unique lead IDs: {len(unique_lead_ids)}")
            print(f"  Unique emails: {len(unique_emails)}")
            
            # Check for duplicates
            if len(all_lead_ids) > len(unique_lead_ids):
                print(f"  ⚠️ DUPLICATES DETECTED: {len(all_lead_ids) - len(unique_lead_ids)} duplicate IDs")
                
            # Get next cursor
            starting_after = data.get('next_starting_after')
            if not starting_after:
                print(f"\n✅ Pagination complete after {page_count} pages")
                break
        else:
            print(f"❌ API Error: {response.status_code}")
            break
    
    # Final analysis
    print(f"\nFinal counts for {name}:")
    print(f"  Total API calls: {page_count}")
    print(f"  Total leads processed: {len(all_lead_ids)}")
    print(f"  Unique lead IDs: {len(unique_lead_ids)}")
    print(f"  Unique emails: {len(unique_emails)}")
    print(f"  Expected: {expected_count}")
    
    if len(unique_lead_ids) != expected_count:
        diff = len(unique_lead_ids) - expected_count
        print(f"  ⚠️ MISMATCH: {abs(diff)} {'more' if diff > 0 else 'fewer'} than expected")

print("\n" + "=" * 60)
print("HYPOTHESIS: The API might be returning leads multiple times")
print("or pagination cursor is not working correctly.")