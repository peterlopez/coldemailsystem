#!/usr/bin/env python3
"""
Manage Orphaned Leads

This script helps you:
1. Find all orphaned leads (created but not assigned to campaigns)
2. Attempt to assign them to appropriate campaigns
3. Delete orphaned leads that can't be assigned (optional)
"""

import os
import requests
import json
import time
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

SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
MIDSIZE_CAMPAIGN_ID = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'

def call_api(endpoint, method='GET', data=None):
    """Make API call with error handling"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    
    try:
        if method == 'POST':
            response = requests.post(url, headers=HEADERS, json=data, timeout=30)
        elif method == 'DELETE':
            response = requests.delete(url, headers=HEADERS, timeout=30)
        else:
            response = requests.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {'error': response.status_code, 'message': response.text}
    except Exception as e:
        return {'error': 'exception', 'message': str(e)}

def find_orphaned_leads():
    """Find all orphaned leads in the account"""
    print("🔍 Finding orphaned leads...")
    
    orphaned_leads = []
    starting_after = None
    page = 0
    
    while page < 20:  # Limit to prevent infinite loops
        payload = {'limit': 50}
        if starting_after:
            payload['starting_after'] = starting_after
            
        response = call_api('/api/v2/leads/list', 'POST', payload)
        
        if 'error' in response:
            print(f"❌ Error fetching leads: {response}")
            break
            
        leads = response.get('items', [])
        if not leads:
            break
            
        # Filter for orphaned leads
        for lead in leads:
            campaign_id = lead.get('campaign_id') or lead.get('campaign')
            if not campaign_id:
                orphaned_leads.append(lead)
        
        page += 1
        starting_after = response.get('next_starting_after')
        if not starting_after:
            break
            
        time.sleep(0.5)  # Rate limiting
    
    print(f"✅ Found {len(orphaned_leads)} orphaned leads")
    return orphaned_leads

def classify_lead_for_campaign(lead):
    """
    Classify lead for campaign assignment based on company revenue
    This mimics the logic from your sync script
    """
    # Try to determine company size - this is a simplified version
    # In your real script, this comes from BigQuery data
    
    company_name = lead.get('company_name', '').lower()
    
    # Simple heuristics - in practice you'd need revenue data
    # For now, just assign based on company name patterns
    small_indicators = ['shop', 'boutique', 'local', 'small']
    large_indicators = ['corp', 'inc', 'llc', 'limited', 'enterprise']
    
    if any(indicator in company_name for indicator in large_indicators):
        return MIDSIZE_CAMPAIGN_ID, "Midsize"
    else:
        return SMB_CAMPAIGN_ID, "SMB"

def attempt_lead_assignment(lead, dry_run=True):
    """Attempt to assign orphaned lead to appropriate campaign"""
    
    lead_id = lead.get('id')
    email = lead.get('email')
    
    if not lead_id:
        return {'success': False, 'reason': 'No lead ID'}
    
    # Classify for campaign
    campaign_id, campaign_name = classify_lead_for_campaign(lead)
    
    print(f"   Assigning {email} to {campaign_name} campaign...")
    
    if dry_run:
        print(f"   [DRY RUN] Would assign {lead_id} to {campaign_id}")
        return {'success': True, 'campaign': campaign_name, 'dry_run': True}
    
    # Attempt assignment
    move_data = {
        'ids': [lead_id],
        'to_campaign_id': campaign_id
    }
    
    result = call_api('/api/v2/leads/move', 'POST', move_data)
    
    if 'error' not in result:
        status = result.get('status', 'unknown')
        return {'success': True, 'campaign': campaign_name, 'status': status}
    else:
        return {'success': False, 'reason': result.get('message', 'Move failed')}

def delete_orphaned_lead(lead, dry_run=True):
    """Delete an orphaned lead that can't be assigned"""
    
    lead_id = lead.get('id')
    email = lead.get('email')
    
    if not lead_id:
        return {'success': False, 'reason': 'No lead ID'}
    
    print(f"   Deleting orphaned lead {email}...")
    
    if dry_run:
        print(f"   [DRY RUN] Would delete {lead_id}")
        return {'success': True, 'dry_run': True}
    
    result = call_api(f'/api/v2/leads/{lead_id}', 'DELETE')
    
    if 'error' not in result:
        return {'success': True}
    else:
        return {'success': False, 'reason': result.get('message', 'Delete failed')}

def main():
    import sys
    
    print("🔧 ORPHANED LEAD MANAGEMENT")
    print("=" * 50)
    
    # Check arguments
    dry_run = '--live' not in sys.argv
    delete_mode = '--delete' in sys.argv
    
    if dry_run:
        print("🚨 DRY RUN MODE - No actual changes will be made")
        print("   Use --live to make actual changes")
    else:
        print("⚠️ LIVE MODE - Changes will be made!")
        
    if delete_mode:
        print("🗑️ DELETE MODE - Orphaned leads will be deleted")
    else:
        print("📋 ASSIGN MODE - Will attempt to assign orphaned leads to campaigns")
    
    # Find orphaned leads
    orphaned_leads = find_orphaned_leads()
    
    if not orphaned_leads:
        print("✅ No orphaned leads found!")
        return
    
    print(f"\n📊 Processing {len(orphaned_leads)} orphaned leads...")
    
    success_count = 0
    failed_count = 0
    
    for i, lead in enumerate(orphaned_leads):
        email = lead.get('email', 'unknown')
        verification = lead.get('verification_status', 'unknown')
        created = lead.get('created_at', 'unknown')
        
        print(f"\n{i+1}/{len(orphaned_leads)}: {email}")
        print(f"   Created: {created}")
        print(f"   Verification: {verification}")
        
        if delete_mode:
            result = delete_orphaned_lead(lead, dry_run)
        else:
            result = attempt_lead_assignment(lead, dry_run)
        
        if result['success']:
            success_count += 1
            if not result.get('dry_run'):
                if delete_mode:
                    print(f"   ✅ Deleted")
                else:
                    campaign = result.get('campaign', 'unknown')
                    status = result.get('status', 'pending')
                    print(f"   ✅ Assigned to {campaign} (Status: {status})")
        else:
            failed_count += 1
            reason = result.get('reason', 'unknown')
            print(f"   ❌ Failed: {reason}")
        
        # Rate limiting
        if not dry_run:
            time.sleep(1)
    
    print(f"\n📈 SUMMARY:")
    print(f"   Total processed: {len(orphaned_leads)}")
    print(f"   Successful: {success_count}")
    print(f"   Failed: {failed_count}")
    
    if dry_run:
        print(f"\n💡 To make actual changes:")
        print(f"   python manage_orphaned_leads.py --live")
        if not delete_mode:
            print(f"   python manage_orphaned_leads.py --live --delete  (to delete instead of assign)")

if __name__ == "__main__":
    main()