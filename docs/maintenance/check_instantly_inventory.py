#\!/usr/bin/env python3
import os
import requests
import json

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
    ("SMB", "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"),
    ("Midsize", "5ffbe8c3-dc0e-41e4-9999-48f00d2015df")
]

total_leads = 0

for name, campaign_id in campaigns:
    print(f"\nChecking {name} campaign...")
    
    # Get first page to see total
    response = requests.post(
        "https://api.instantly.ai/api/v2/leads/list",
        headers=headers,
        json={"campaign_id": campaign_id, "limit": 50},
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        leads_in_page = len(data.get('items', []))
        
        # Count all pages
        page_count = 1
        campaign_total = leads_in_page
        
        while data.get('next_starting_after'):
            response = requests.post(
                "https://api.instantly.ai/api/v2/leads/list",
                headers=headers,
                json={
                    "campaign_id": campaign_id, 
                    "limit": 50,
                    "starting_after": data['next_starting_after']
                },
                timeout=30
            )
            if response.status_code == 200:
                data = response.json()
                page_count += 1
                campaign_total += len(data.get('items', []))
            else:
                break
                
        print(f"  Pages: {page_count}")
        print(f"  Leads: {campaign_total}")
        total_leads += campaign_total

print(f"\nTOTAL LEADS IN INSTANTLY: {total_leads}")
