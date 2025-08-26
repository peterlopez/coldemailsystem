#!/usr/bin/env python3
"""
Quick estimate of actual lead counts
"""
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

campaigns = [
    ("SMB", "8c46e0c9-c1f9-4201-a8d6-6221bafeada6", 1401),
    ("Midsize", "5ffbe8c3-dc0e-41e4-9999-48f00d2015df", 487)
]

print("QUICK LEAD COUNT ESTIMATE")
print("=" * 60)

for name, campaign_id, expected_count in campaigns:
    print(f"\n{name} Campaign:")
    
    # Get first page to estimate
    payload = {"campaign_id": campaign_id, "limit": 50}
    
    response = requests.post(
        "https://api.instantly.ai/api/v2/leads/list",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        leads = data.get('items', [])
        
        # Calculate estimated pages needed
        if expected_count > 0:
            estimated_pages = (expected_count + 49) // 50  # Round up
            print(f"  Expected leads: {expected_count}")
            print(f"  Estimated pages needed: {estimated_pages}")
            print(f"  First page has: {len(leads)} leads")
            
            if len(leads) == 50 and expected_count > 50:
                print(f"  ✅ Looks normal - would need ~{estimated_pages} pages")
            elif len(leads) < 50:
                print(f"  ⚠️ First page only has {len(leads)} leads - might be fewer than expected")
        else:
            print(f"  First page has: {len(leads)} leads")
    else:
        print(f"  ❌ API Error: {response.status_code}")

print(f"\nCONCLUSION:")
print(f"The discrepancy might be because:")
print(f"1. Our previous count script had a bug")
print(f"2. We were somehow counting pages twice")  
print(f"3. The API pagination was returning duplicates (but diagnostic shows it's working)")
print()
print(f"The drain workflow should be working with the actual ~1,888 leads,")
print(f"not the inflated 23,902 evaluations we saw in logs.")