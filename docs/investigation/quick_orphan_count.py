#!/usr/bin/env python3
"""
Quick Orphaned Lead Count - Fast check with limited scanning
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

INSTANTLY_BASE_URL = 'https://api.instantly.ai'
HEADERS = {
    'Authorization': f'Bearer {INSTANTLY_API_KEY}',
    'Content-Type': 'application/json'
}

SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
MIDSIZE_CAMPAIGN_ID = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'
TARGET_CAMPAIGNS = {SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID}

def call_api(endpoint, method='GET', data=None):
    """Make API call"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    
    try:
        if method == 'POST':
            response = requests.post(url, headers=HEADERS, json=data, timeout=30)
        else:
            response = requests.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {'error': response.status_code, 'text': response.text}
    except Exception as e:
        return {'error': str(e)}

def quick_orphan_count():
    """Quick count of orphaned leads - first 5 pages only"""
    print("⚡ QUICK ORPHANED LEAD COUNT")
    print("=" * 40)
    
    orphaned_count = 0
    total_scanned = 0
    starting_after = None
    
    # Only scan first 5 pages for quick count
    for page in range(5):
        payload = {'limit': 100}
        if starting_after:
            payload['starting_after'] = starting_after
        
        response = call_api('/api/v2/leads/list', 'POST', payload)
        
        if 'error' in response:
            print(f"❌ Error: {response}")
            break
        
        leads = response.get('items', [])
        if not leads:
            break
        
        total_scanned += len(leads)
        page_orphans = 0
        
        for lead in leads:
            campaign_id = lead.get('campaign_id') or lead.get('campaign')
            if not campaign_id or campaign_id not in TARGET_CAMPAIGNS:
                orphaned_count += 1
                page_orphans += 1
        
        print(f"Page {page + 1}: {page_orphans} orphaned of {len(leads)} leads")
        
        starting_after = response.get('next_starting_after')
        if not starting_after:
            print("✅ Reached end of leads")
            break
    
    print(f"\n📊 QUICK SAMPLE RESULTS:")
    print(f"   Orphaned leads found: {orphaned_count}")
    print(f"   Total leads scanned: {total_scanned}")
    
    if orphaned_count > 0:
        print(f"   📈 Estimated remaining orphaned leads: {orphaned_count * 4}")
        print(f"   ✅ More orphaned leads to assign!")
    else:
        print(f"   🎯 No orphaned leads found in sample!")
    
    return orphaned_count

if __name__ == "__main__":
    quick_orphan_count()