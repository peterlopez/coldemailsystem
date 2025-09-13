#!/usr/bin/env python3
"""
Very simple test - just check if we can get any leads with campaign_id
"""

from shared_config import config
from shared.api_client import SMB_CAMPAIGN_ID
import requests

def test_basic_pagination():
    api_key = config.api.instantly_api_key
    
    data = {'campaign_id': SMB_CAMPAIGN_ID, 'limit': 10}
    headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
    
    response = requests.post('https://api.instantly.ai/api/v2/leads/list', 
                           headers=headers, json=data, timeout=10)
    
    if response.status_code == 200:
        result = response.json()
        lead_count = len(result.get('items', []))
        print(f"✅ SUCCESS: Got {lead_count} leads from SMB campaign")
        return True
    else:
        print(f"❌ FAILED: {response.status_code} - {response.text}")
        return False

if __name__ == "__main__":
    test_basic_pagination()