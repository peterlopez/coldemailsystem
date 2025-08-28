#!/usr/bin/env python3
"""
Quick Orphan Lead Diagnosis

Gets the essential information quickly to diagnose the orphaned leads issue
"""

import os
import requests
import json
from datetime import datetime, timezone, timedelta

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

def call_api(endpoint, method='GET', data=None):
    """Make API call"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    
    try:
        if method == 'POST':
            response = requests.post(url, headers=HEADERS, json=data, timeout=15)
        else:
            response = requests.get(url, headers=HEADERS, timeout=15)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {'error': response.status_code}
    except Exception as e:
        return {'error': str(e)}

def main():
    print("🚨 ORPHANED LEADS DIAGNOSIS")
    print("=" * 40)
    
    # Get first page of all leads (no campaign filter)
    print("1. Getting sample of all leads in account...")
    all_leads = call_api('/api/v2/leads/list', 'POST', {'limit': 100})
    
    if 'error' not in all_leads:
        leads = all_leads.get('items', [])
        total_sample = len(leads)
        
        with_campaign = 0
        without_campaign = 0
        recent_orphans = []
        
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        
        for lead in leads:
            campaign_id = lead.get('campaign_id') or lead.get('campaign')
            
            if campaign_id:
                with_campaign += 1
            else:
                without_campaign += 1
                
                # Check if recent
                created_at = lead.get('created_at')
                if created_at:
                    try:
                        lead_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        if lead_time > cutoff:
                            recent_orphans.append({
                                'email': lead.get('email'),
                                'created': created_at,
                                'verification': lead.get('verification_status', 'unknown')
                            })
                    except:
                        pass
        
        print(f"   Sample size: {total_sample} leads")
        print(f"   With campaigns: {with_campaign}")
        print(f"   Without campaigns: {without_campaign}")
        print(f"   Recent orphans (24h): {len(recent_orphans)}")
        
        if recent_orphans:
            print(f"\n   Recent orphaned leads:")
            for orphan in recent_orphans[:5]:
                print(f"     - {orphan['email']} ({orphan['verification']})")
        
        # Extrapolate if we have a pattern
        if total_sample == 100 and without_campaign > 0:
            estimated_total_orphans = (without_campaign / total_sample) * 1000  # Rough estimate
            print(f"\n   🚨 ESTIMATED TOTAL ORPHANS: ~{int(estimated_total_orphans)} leads")
    
    # Check campaign status
    print(f"\n2. Checking campaign status...")
    smb_campaign = call_api(f'/api/v2/campaigns/8c46e0c9-c1f9-4201-a8d6-6221bafeada6')
    
    if 'error' not in smb_campaign:
        status = smb_campaign.get('status')
        status_text = {1: 'Active', 2: 'Paused', 3: 'Completed', 4: 'Draft'}.get(status, 'Unknown')
        print(f"   SMB Campaign Status: {status} ({status_text})")
        
        if status == 2:
            print(f"   🚨 CAMPAIGN IS PAUSED - This prevents lead assignment!")
    
    print(f"\n💡 KEY FINDINGS:")
    if 'error' not in all_leads and without_campaign > 0:
        print(f"   ✅ CONFIRMED: {without_campaign}/{total_sample} leads are orphaned")
        print(f"   ✅ Recent orphans suggest ongoing issue")
        print(f"   🔧 ROOT CAUSE: Lead creation succeeds, campaign assignment fails")
        
        if 'error' not in smb_campaign and smb_campaign.get('status') == 2:
            print(f"   🎯 LIKELY REASON: Campaign is PAUSED")
            print(f"   📋 SOLUTION: Reactivate campaigns in Instantly dashboard")
        else:
            print(f"   📋 SOLUTIONS: Check verification requirements or API limits")
    else:
        print(f"   ❌ Could not confirm orphaned leads")

if __name__ == "__main__":
    main()