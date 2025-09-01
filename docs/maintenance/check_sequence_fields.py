#!/usr/bin/env python3
"""
Check for sequence control fields in campaign data
"""

import os
import requests
import json

# Get API key
try:
    from config.config import Config
    config = Config()
    api_key = config.instantly_api_key
except:
    print("❌ Could not load API key")
    exit(1)

SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

print('🔍 CHECKING FOR SEQUENCE CONTROL FIELDS')
print('=' * 50)

try:
    response = requests.get(
        f'https://api.instantly.ai/api/v2/campaigns/{SMB_CAMPAIGN_ID}',
        headers=headers,
        timeout=30
    )
    
    if response.status_code == 200:
        campaign = response.json()
        
        # Look for any other campaign settings
        print('🔍 ALL CAMPAIGN FIELDS (looking for hidden controls):')
        for key, value in sorted(campaign.items()):
            if key not in ['sequences', 'email_list']:  # Skip big arrays
                if isinstance(value, (dict, list)):
                    print(f'  {key}: {type(value).__name__}({len(value) if hasattr(value, "__len__") else "N/A"}) items')
                else:
                    print(f'  {key}: {value}')
                    
        # Specifically look for fields that might indicate if campaign is actually running
        print('\n🎯 KEY INDICATORS:')
        print(f'  Campaign Status: {campaign.get("status", "Unknown")}')
        print(f'  Daily Limit: {campaign.get("daily_limit", "Unknown")}')
        print(f'  Email Gap: {campaign.get("email_gap", "Unknown")}')
        
        # Check sequences
        sequences = campaign.get('sequences', [])
        if sequences:
            seq = sequences[0] 
            print('\n🎯 SEQUENCE FIELDS:')
            for key, value in seq.items():
                if key != 'steps': 
                    print(f'  {key}: {value}')
        
except Exception as e:
    print(f'❌ Error: {e}')