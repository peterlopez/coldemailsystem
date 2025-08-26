#!/usr/bin/env python3
"""
Count all pages in each campaign to get accurate totals
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

campaigns = [
    ("SMB", "8c46e0c9-c1f9-4201-a8d6-6221bafeada6", 1401),
    ("Midsize", "5ffbe8c3-dc0e-41e4-9999-48f00d2015df", 487)
]

print("COMPLETE PAGINATION COUNT")
print("=" * 60)

total_across_campaigns = 0

for name, campaign_id, expected_count in campaigns:
    print(f"\n{name} Campaign - Full Count:")
    
    unique_lead_ids = set()
    unique_emails = set()
    starting_after = None
    page_count = 0
    
    while True:
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
                print(f"  ✅ End of pagination - no more leads")
                break
                
            page_count += 1
            
            # Track unique IDs and emails
            for lead in leads:
                lead_id = lead.get('id')
                email = lead.get('email')
                
                if lead_id:
                    unique_lead_ids.add(lead_id)
                    
                if email:
                    unique_emails.add(email)
            
            if page_count % 5 == 0:  # Progress every 5 pages
                print(f"  Page {page_count}: {len(unique_lead_ids)} unique leads so far")
                
            # Get next cursor
            starting_after = data.get('next_starting_after')
            if not starting_after:
                print(f"  ✅ Pagination complete - no next cursor")
                break
                
        else:
            print(f"  ❌ API Error on page {page_count + 1}: {response.status_code}")
            break
    
    actual_count = len(unique_lead_ids)
    total_across_campaigns += actual_count
    
    print(f"\n  RESULTS:")
    print(f"    Total pages: {page_count}")
    print(f"    Unique leads found: {actual_count}")
    print(f"    Expected from UI: {expected_count}")
    
    if actual_count == expected_count:
        print(f"    ✅ MATCH - API count matches UI")
    else:
        diff = actual_count - expected_count
        print(f"    ⚠️ DIFFERENCE: {abs(diff)} {'more' if diff > 0 else 'fewer'} than UI shows")

print(f"\n" + "=" * 60)
print(f"TOTAL ACROSS ALL CAMPAIGNS:")
print(f"  API Count: {total_across_campaigns}")
print(f"  Expected (UI): 1,888 (1,401 + 487)")
print(f"  Previous wrong count: 3,782")

if total_across_campaigns == 1888:
    print("  ✅ API matches UI - our earlier count was wrong")
elif total_across_campaigns > 1888:
    print("  ⚠️ API shows more leads than UI")
else:
    print("  ⚠️ API shows fewer leads than UI")