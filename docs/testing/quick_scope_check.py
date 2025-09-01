#!/usr/bin/env python3
"""
Quick Scope Check for Orphaned Leads

Fast assessment to understand the true scale of orphaned leads
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

INSTANTLY_BASE_URL = 'https://api.instantly.ai'
HEADERS = {
    'Authorization': f'Bearer {INSTANTLY_API_KEY}',
    'Content-Type': 'application/json'
}

SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
MIDSIZE_CAMPAIGN_ID = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'

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

def count_leads_in_campaign(campaign_id, campaign_name, max_pages=10):
    """Count leads in a specific campaign with pagination limit"""
    print(f"📊 Counting leads in {campaign_name} campaign...")
    
    total_count = 0
    starting_after = None
    page = 0
    
    while page < max_pages:
        payload = {'campaign_id': campaign_id, 'limit': 100}
        if starting_after:
            payload['starting_after'] = starting_after
        
        response = call_api('/api/v2/leads/list', 'POST', payload)
        
        if 'error' in response:
            print(f"   ❌ Error: {response}")
            break
        
        leads = response.get('items', [])
        if not leads:
            break
        
        total_count += len(leads)
        page += 1
        
        starting_after = response.get('next_starting_after')
        if not starting_after:
            break
        
        if page % 5 == 0:
            print(f"   📄 Page {page}: {total_count} leads so far...")
        
        time.sleep(0.3)
    
    if page >= max_pages:
        print(f"   ⚠️ Stopped at page limit ({max_pages}) - count may be incomplete")
    
    print(f"   ✅ {campaign_name}: {total_count} leads")
    return total_count

def estimate_total_leads(max_pages=5):
    """Estimate total leads across all campaigns"""
    print(f"🌐 Estimating total leads in account...")
    
    total_count = 0
    starting_after = None
    page = 0
    
    while page < max_pages:
        payload = {'limit': 100}
        if starting_after:
            payload['starting_after'] = starting_after
        
        response = call_api('/api/v2/leads/list', 'POST', payload)
        
        if 'error' in response:
            print(f"   ❌ Error: {response}")
            break
        
        leads = response.get('items', [])
        if not leads:
            break
        
        total_count += len(leads)
        page += 1
        
        starting_after = response.get('next_starting_after')
        if not starting_after:
            print(f"   ✅ Found all leads: {total_count}")
            return total_count, False  # Complete count
        
        time.sleep(0.3)
    
    # Estimate based on what we found
    if page >= max_pages and total_count > 0:
        estimated_total = total_count * 2  # Conservative estimate
        print(f"   ⚠️ Estimated total (incomplete): ~{estimated_total}")
        return estimated_total, True  # Estimated
    
    return total_count, False

def main():
    print("⚡ QUICK SCOPE CHECK FOR ORPHANED LEADS")
    print("=" * 50)
    
    # Count leads in our target campaigns
    smb_count = count_leads_in_campaign(SMB_CAMPAIGN_ID, "SMB")
    midsize_count = count_leads_in_campaign(MIDSIZE_CAMPAIGN_ID, "Midsize")
    campaign_total = smb_count + midsize_count
    
    print(f"\n📊 CAMPAIGN TOTALS:")
    print(f"   SMB Campaign: {smb_count}")
    print(f"   Midsize Campaign: {midsize_count}")
    print(f"   Total in target campaigns: {campaign_total}")
    
    # Estimate total leads
    estimated_total, is_estimate = estimate_total_leads()
    
    print(f"\n🎯 ORPHANED LEADS CALCULATION:")
    print(f"   Total leads (estimate): {estimated_total}")
    print(f"   Leads in target campaigns: {campaign_total}")
    
    if estimated_total > campaign_total:
        orphaned_estimate = estimated_total - campaign_total
        print(f"   🚨 ESTIMATED ORPHANED LEADS: {orphaned_estimate}")
        
        if is_estimate:
            print(f"   ⚠️ This is a rough estimate - actual number may be higher")
        
        if orphaned_estimate > 500:
            print(f"\n💡 NEXT STEPS:")
            print(f"   1. This confirms a significant number of orphaned leads")
            print(f"   2. Need to use a different identification method")
            print(f"   3. Consider checking leads that are NOT in either target campaign")
    else:
        print(f"   ✅ No significant orphaned leads detected")
    
    # Try a different approach - check for leads without campaigns
    print(f"\n🔍 ALTERNATIVE CHECK: Leads without any campaign assignment")
    
    payload = {'limit': 200}
    response = call_api('/api/v2/leads/list', 'POST', payload)
    
    if 'error' not in response:
        leads = response.get('items', [])
        no_campaign = 0
        different_campaign = 0
        
        for lead in leads:
            campaign_id = lead.get('campaign_id') or lead.get('campaign')
            if not campaign_id:
                no_campaign += 1
            elif campaign_id not in [SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID]:
                different_campaign += 1
        
        print(f"   Sample of {len(leads)} leads:")
        print(f"   - No campaign: {no_campaign}")
        print(f"   - Different campaign: {different_campaign}")
        print(f"   - In target campaigns: {len(leads) - no_campaign - different_campaign}")

if __name__ == "__main__":
    main()