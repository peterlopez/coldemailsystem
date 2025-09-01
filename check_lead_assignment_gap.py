#!/usr/bin/env python3
"""
Quick Check for Lead Assignment Gap

Based on the sync_once.py implementation, this script specifically checks for the most likely
scenario causing orphaned leads:

1. Lead creation succeeds (Step 1/2) 
2. Campaign assignment fails (Step 2/2)

The sync script creates leads first, then uses /api/v2/leads/move to assign to campaigns.
If the move operation fails silently, leads become orphaned.
"""

import os
import sys
import requests
import json
from datetime import datetime, timezone, timedelta

# Load API key
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
if not INSTANTLY_API_KEY:
    try:
        with open('config/secrets/instantly-config.json', 'r') as f:
            config = json.load(f)
            INSTANTLY_API_KEY = config['api_key']
    except Exception as e:
        print(f"❌ Failed to load API key: {e}")
        sys.exit(1)

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
        if method == 'GET':
            response = requests.get(url, headers=HEADERS, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=HEADERS, json=data, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {'error': response.status_code, 'message': response.text}
    except Exception as e:
        return {'error': 'exception', 'message': str(e)}

def count_campaign_leads():
    """Count leads currently in campaigns"""
    print("🔢 Counting leads in campaigns...")
    
    # CORRECTED: Can't filter by campaign_id in API - get all leads and count client-side
    smb_data = call_api('/api/v2/leads/list', 'POST', {
        'limit': 100  # Get more leads to count properly
    })
    
    midsize_data = call_api('/api/v2/leads/list', 'POST', {
        'limit': 100  # Get more leads to count properly  
    })
    
    smb_count = 0
    midsize_count = 0
    
    # Count SMB leads by filtering client-side using 'campaign' field
    if 'error' not in smb_data:
        for lead in smb_data.get('items', []):
            if lead.get('campaign') == SMB_CAMPAIGN_ID:
                smb_count += 1
    
    # Count Midsize leads by filtering client-side using 'campaign' field  
    if 'error' not in midsize_data:
        for lead in midsize_data.get('items', []):
            if lead.get('campaign') == MIDSIZE_CAMPAIGN_ID:
                midsize_count += 1
    
    print(f"   SMB Campaign: {smb_count} leads")
    print(f"   Midsize Campaign: {midsize_count} leads") 
    print(f"   Total in campaigns: {smb_count + midsize_count}")
    
    return smb_count + midsize_count

def check_for_unassigned_leads():
    """
    Check for leads that exist but aren't in campaigns
    
    Key insight: Your sync script does:
    1. POST /api/v2/leads (create lead) 
    2. POST /api/v2/leads/move (assign to campaign)
    
    If step 2 fails, lead exists but isn't in any campaign.
    """
    print("\n🔍 Checking for unassigned leads...")
    
    # Method 1: Try to get ALL leads without campaign filter
    all_leads_data = call_api('/api/v2/leads/list', 'POST', {'limit': 100})
    
    if 'error' in all_leads_data:
        print("❌ Could not fetch all leads")
        return 0
    
    all_leads = all_leads_data.get('items', [])
    total_leads = len(all_leads)
    
    # Check how many have campaign assignments
    with_campaign = 0
    without_campaign = 0
    recent_without_campaign = []
    
    # Look for leads created in last 24 hours
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
    
    for lead in all_leads:
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
                    if lead_time > cutoff_time:
                        recent_without_campaign.append(lead)
                except:
                    pass
    
    print(f"   Total leads found: {total_leads}")
    print(f"   With campaign assignment: {with_campaign}")
    print(f"   Without campaign assignment: {without_campaign}")
    
    if recent_without_campaign:
        print(f"   🚨 Recently created unassigned leads: {len(recent_without_campaign)}")
        print("\n   Recent unassigned leads:")
        for lead in recent_without_campaign[:5]:
            verification = lead.get('verification_status', 'unknown')
            print(f"     - {lead.get('email')} (verified: {verification})")
    
    return without_campaign

def test_lead_creation_and_assignment():
    """
    Test the exact process your sync script uses to identify where it might fail
    """
    print("\n🧪 Testing lead creation and assignment process...")
    
    test_email = f"test-orphan-check-{int(datetime.now().timestamp())}@example.com"
    
    # Step 1: Create lead (like sync_once.py does)
    print("   Step 1: Creating test lead...")
    create_data = {
        'email': test_email,
        'first_name': 'Test',
        'last_name': 'Orphan',
        'company_name': 'Test Company'
    }
    
    create_result = call_api('/api/v2/leads', 'POST', create_data)
    
    if 'error' in create_result:
        print(f"   ❌ Lead creation failed: {create_result}")
        return
    
    lead_id = create_result.get('id')
    if not lead_id:
        print(f"   ❌ No lead ID returned: {create_result}")
        return
        
    print(f"   ✅ Lead created with ID: {lead_id}")
    
    # Step 2: Try to assign to campaign (like sync_once.py does)
    print("   Step 2: Assigning to SMB campaign...")
    move_data = {
        'ids': [lead_id],
        'to_campaign_id': SMB_CAMPAIGN_ID
    }
    
    move_result = call_api('/api/v2/leads/move', 'POST', move_data)
    
    print(f"   Move result: {move_result}")
    
    if 'error' in move_result:
        print(f"   ❌ Campaign assignment FAILED: {move_result}")
        print("   🚨 This explains orphaned leads - assignment step is failing!")
    else:
        print(f"   ✅ Campaign assignment succeeded")
    
    # Step 3: Clean up
    print("   Step 3: Cleaning up test lead...")
    delete_result = call_api(f'/api/v2/leads/{lead_id}', 'DELETE')
    
    if 'error' not in delete_result:
        print("   ✅ Test lead deleted")
    else:
        print(f"   ⚠️ Could not delete test lead: {delete_result}")

def main():
    print("🔍 QUICK CHECK FOR LEAD ASSIGNMENT GAP")
    print("=" * 50)
    print("This checks the most likely cause of orphaned leads:")
    print("Lead creation succeeds but campaign assignment fails")
    
    # Count current campaign leads
    campaign_count = count_campaign_leads()
    
    # Check for unassigned leads
    unassigned_count = check_for_unassigned_leads()
    
    # Test the assignment process
    test_lead_creation_and_assignment()
    
    print(f"\n📊 SUMMARY:")
    print(f"   Leads in campaigns: {campaign_count}")
    print(f"   Unassigned leads: {unassigned_count}")
    
    if unassigned_count > 0:
        print(f"\n💡 LIKELY CAUSE:")
        print(f"   The /api/v2/leads/move endpoint is failing or")
        print(f"   campaigns are not accepting lead assignments")
        print(f"\n🔧 SOLUTIONS:")
        print(f"   1. Check if campaigns are active (not paused)")
        print(f"   2. Verify campaign IDs are correct")
        print(f"   3. Check if leads need verification before assignment")
        print(f"   4. Add better error handling for move operation")

if __name__ == "__main__":
    main()