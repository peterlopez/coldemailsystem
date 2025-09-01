#!/usr/bin/env python3
"""
Investigate Orphaned Leads in Instantly.ai

This script helps identify leads that were created but not properly assigned to campaigns.
Based on the issue where sync reported processing 100 leads but campaigns only show 20.

Key Investigation Areas:
1. Total lead count across all campaigns vs. account total
2. Leads that exist but aren't in any campaign
3. Recently created leads without campaign assignment
4. Verification status of unassigned leads

API Endpoints Used:
- POST /api/v2/leads/list (with and without campaign_id filter)
- GET /api/v2/leads/{id} (for individual lead details)
- GET /api/v2/campaigns (to get all campaigns)
"""

import os
import sys
import requests
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Set

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

# Known campaign IDs
SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
MIDSIZE_CAMPAIGN_ID = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'

def call_instantly_api(endpoint: str, method: str = 'GET', data: Optional[Dict] = None) -> Optional[Dict]:
    """Make API call with error handling"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=HEADERS, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=HEADERS, json=data, timeout=30)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ API Error {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"💥 API Exception: {e}")
        return None

def get_all_leads_in_campaign(campaign_id: str, campaign_name: str) -> List[Dict]:
    """Get all leads in a specific campaign using pagination"""
    print(f"\n🔍 Fetching all leads in {campaign_name} campaign...")
    
    all_leads = []
    starting_after = None
    page_count = 0
    
    while True:
        payload = {
            "campaign_id": campaign_id,
            "limit": 50
        }
        
        if starting_after:
            payload["starting_after"] = starting_after
        
        response = call_instantly_api('/api/v2/leads/list', method='POST', data=payload)
        
        if not response:
            print(f"❌ Failed to get leads for {campaign_name}")
            break
            
        leads = response.get('items', [])
        
        if not leads:
            print(f"✅ {campaign_name}: Pagination complete")
            break
            
        all_leads.extend(leads)
        page_count += 1
        
        if page_count % 10 == 0:
            print(f"  📄 {campaign_name}: Page {page_count}, {len(all_leads)} leads so far...")
        
        starting_after = response.get('next_starting_after')
        if not starting_after:
            break
            
        time.sleep(0.5)  # Rate limiting
    
    print(f"✅ {campaign_name}: Found {len(all_leads)} total leads")
    return all_leads

def get_all_leads_without_campaign_filter() -> List[Dict]:
    """
    Try to get ALL leads in the account without campaign filter
    This might reveal orphaned leads that aren't assigned to any campaign
    """
    print(f"\n🌐 Attempting to fetch ALL leads in account (no campaign filter)...")
    
    all_leads = []
    starting_after = None
    page_count = 0
    
    # Try different approaches to get all leads
    endpoints_to_try = [
        # Method 1: leads/list without campaign_id
        {'endpoint': '/api/v2/leads/list', 'payload': {'limit': 50}},
        # Method 2: just leads endpoint
        {'endpoint': '/api/v2/leads', 'payload': None},
    ]
    
    for approach in endpoints_to_try:
        print(f"\n🔬 Trying approach: {approach['endpoint']}")
        
        if approach['payload'] is None:
            # GET request
            response = call_instantly_api(approach['endpoint'], method='GET')
            if response and response.get('items'):
                print(f"✅ Found {len(response['items'])} leads via GET {approach['endpoint']}")
                return response['items']
        else:
            # POST request with pagination
            starting_after = None
            page_count = 0
            approach_leads = []
            
            while page_count < 5:  # Limit to prevent infinite loop
                payload = approach['payload'].copy()
                if starting_after:
                    payload["starting_after"] = starting_after
                
                response = call_instantly_api(approach['endpoint'], method='POST', data=payload)
                
                if not response:
                    print(f"❌ No response from {approach['endpoint']}")
                    break
                    
                leads = response.get('items', [])
                
                if not leads:
                    print(f"✅ No more leads from {approach['endpoint']}")
                    break
                    
                approach_leads.extend(leads)
                page_count += 1
                
                print(f"  📄 Page {page_count}: {len(approach_leads)} leads so far...")
                
                starting_after = response.get('next_starting_after')
                if not starting_after:
                    break
                    
                time.sleep(0.5)
            
            if approach_leads:
                print(f"✅ Found {len(approach_leads)} leads via {approach['endpoint']}")
                return approach_leads
    
    print("❌ Could not retrieve all leads - API might not support this")
    return []

def analyze_recent_leads(all_account_leads: List[Dict], hours_back: int = 24) -> Dict:
    """Analyze recently created leads"""
    print(f"\n📅 Analyzing leads created in last {hours_back} hours...")
    
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    recent_leads = []
    
    for lead in all_account_leads:
        created_at = lead.get('created_at')
        if created_at:
            try:
                # Parse ISO timestamp
                lead_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                if lead_time > cutoff_time:
                    recent_leads.append(lead)
            except Exception as e:
                print(f"⚠️ Could not parse timestamp {created_at}: {e}")
    
    analysis = {
        'total_recent': len(recent_leads),
        'with_campaign': [],
        'without_campaign': [],
        'verification_status': {},
    }
    
    for lead in recent_leads:
        campaign_id = lead.get('campaign_id') or lead.get('campaign')
        
        if campaign_id:
            analysis['with_campaign'].append(lead)
        else:
            analysis['without_campaign'].append(lead)
        
        # Track verification status
        verification = lead.get('verification_status', 'unknown')
        analysis['verification_status'][verification] = analysis['verification_status'].get(verification, 0) + 1
    
    print(f"📊 Recent Lead Analysis:")
    print(f"   Total recent leads: {analysis['total_recent']}")
    print(f"   With campaigns: {len(analysis['with_campaign'])}")
    print(f"   Without campaigns: {len(analysis['without_campaign'])}")
    print(f"   Verification status: {analysis['verification_status']}")
    
    return analysis

def investigate_lead_details(lead_id: str) -> Dict:
    """Get detailed information about a specific lead"""
    response = call_instantly_api(f'/api/v2/leads/{lead_id}', method='GET')
    return response or {}

def main():
    print("🕵️ INVESTIGATING ORPHANED LEADS IN INSTANTLY.AI")
    print("=" * 60)
    print("This script will help identify leads that were created but not assigned to campaigns")
    
    # Step 1: Get leads in known campaigns
    print("\n📊 STEP 1: Count leads in known campaigns")
    
    smb_leads = get_all_leads_in_campaign(SMB_CAMPAIGN_ID, "SMB")
    midsize_leads = get_all_leads_in_campaign(MIDSIZE_CAMPAIGN_ID, "Midsize")
    
    campaign_total = len(smb_leads) + len(midsize_leads)
    
    # Step 2: Try to get ALL leads in account
    print("\n🌐 STEP 2: Attempt to get ALL leads in account")
    
    all_account_leads = get_all_leads_without_campaign_filter()
    
    if all_account_leads:
        account_total = len(all_account_leads)
        print(f"\n📈 COMPARISON:")
        print(f"   Leads in campaigns: {campaign_total}")
        print(f"   Total in account: {account_total}")
        
        if account_total > campaign_total:
            orphaned_count = account_total - campaign_total
            print(f"   🚨 POTENTIAL ORPHANED LEADS: {orphaned_count}")
            
            # Step 3: Analyze recent leads
            recent_analysis = analyze_recent_leads(all_account_leads, hours_back=24)
            
            if recent_analysis['without_campaign']:
                print(f"\n🔍 STEP 3: Investigating {len(recent_analysis['without_campaign'])} recent unassigned leads")
                
                for i, lead in enumerate(recent_analysis['without_campaign'][:5]):  # Show first 5
                    print(f"\n   Lead {i+1}:")
                    print(f"     Email: {lead.get('email')}")
                    print(f"     Created: {lead.get('created_at')}")
                    print(f"     Verification: {lead.get('verification_status', 'unknown')}")
                    print(f"     Campaign: {lead.get('campaign_id', 'NONE')}")
                    
                    # Get detailed info
                    if lead.get('id'):
                        details = investigate_lead_details(lead['id'])
                        if details:
                            print(f"     Status: {details.get('status', 'unknown')}")
                            print(f"     Company: {details.get('company_name', 'N/A')}")
            
        elif account_total == campaign_total:
            print("   ✅ NO ORPHANED LEADS: All leads are properly assigned")
        else:
            print("   ⚠️ INCONSISTENCY: Fewer leads in account than campaigns report")
    else:
        print("❌ Could not retrieve total account leads for comparison")
    
    # Step 4: Recommendations
    print(f"\n💡 STEP 4: Recommendations based on findings")
    
    if all_account_leads and len(all_account_leads) > campaign_total:
        print("\n🔧 POTENTIAL SOLUTIONS:")
        print("1. Check if lead creation succeeded but campaign assignment failed")
        print("2. Verify that leads pass email verification before assignment")
        print("3. Check if the move operation (/api/v2/leads/move) has async delays")
        print("4. Look for leads with verification_status != 'valid' that got stuck")
        
        print("\n🐛 DEBUGGING STEPS:")
        print("1. Run your sync script with more detailed logging on lead creation vs assignment")
        print("2. Check if leads exist by searching for emails manually in Instantly dashboard")
        print("3. Look at the /api/v2/leads/move endpoint response - it might be failing silently")
        print("4. Consider adding a verification step after each lead creation")
        
    print(f"\n🏁 INVESTIGATION COMPLETE")
    print("Check the output above for any orphaned leads that need attention")

if __name__ == "__main__":
    main()