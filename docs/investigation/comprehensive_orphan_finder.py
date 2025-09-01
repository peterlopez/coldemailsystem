#!/usr/bin/env python3
"""
Comprehensive Orphaned Lead Finder

Complete assessment of all orphaned leads including:
1. Leads with no campaign assignment
2. Leads in wrong/unexpected campaigns  
3. Proper campaign discovery and analysis
"""

import os
import requests
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Set

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
    """Make API call with better error handling"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    
    try:
        if method == 'POST':
            response = requests.post(url, headers=HEADERS, json=data, timeout=60)
        else:
            response = requests.get(url, headers=HEADERS, timeout=60)
        
        print(f"   API Call: {method} {endpoint} -> Status {response.status_code}")
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"   ❌ Error {response.status_code}: {response.text[:200]}")
            return {'error': response.status_code, 'text': response.text}
    except Exception as e:
        print(f"   💥 Exception: {e}")
        return {'error': str(e)}

def discover_all_campaigns():
    """Discover all campaigns using multiple API approaches"""
    print("🔍 Discovering all campaigns...")
    
    campaigns = []
    
    # Try different endpoints for campaign discovery
    endpoints_to_try = [
        '/api/v2/campaigns',
        '/api/v1/campaigns',
        '/campaigns'
    ]
    
    for endpoint in endpoints_to_try:
        print(f"\n   Trying {endpoint}...")
        response = call_api(endpoint)
        
        if 'error' not in response:
            # Handle different response formats
            if 'campaigns' in response:
                campaigns = response['campaigns']
                print(f"   ✅ Found campaigns via {endpoint}")
                break
            elif isinstance(response, list):
                campaigns = response
                print(f"   ✅ Found campaigns via {endpoint}")
                break
            elif 'items' in response:
                campaigns = response['items']
                print(f"   ✅ Found campaigns via {endpoint}")
                break
    
    if not campaigns:
        print(f"   ⚠️ Could not discover campaigns via API")
        # Use known campaigns as fallback
        campaigns = [
            {'id': SMB_CAMPAIGN_ID, 'name': 'SMB (Known)', 'status': 1},
            {'id': MIDSIZE_CAMPAIGN_ID, 'name': 'Midsize (Known)', 'status': 1}
        ]
        print(f"   📋 Using known campaigns as fallback")
    
    print(f"\n   📊 Campaign Discovery Results:")
    for campaign in campaigns:
        campaign_id = campaign.get('id')
        name = campaign.get('name', 'Unnamed')
        status = campaign.get('status', 'Unknown')
        status_text = {1: 'Active', 2: 'Paused', 3: 'Completed', 4: 'Draft'}.get(status, f'Status {status}')
        
        if campaign_id in TARGET_CAMPAIGNS:
            print(f"      🎯 {name} ({campaign_id}): {status_text}")
        else:
            print(f"      ❓ {name} ({campaign_id}): {status_text} - OTHER CAMPAIGN")
    
    return campaigns

def count_all_orphaned_leads():
    """Get comprehensive count of ALL orphaned leads"""
    print(f"\n🔢 COMPREHENSIVE ORPHANED LEAD COUNT")
    print("=" * 50)
    
    orphaned_leads = []
    starting_after = None
    page = 0
    total_processed = 0
    
    # Process more pages to get closer to the real total
    while page < 100:  # Increased limit to catch more leads
        payload = {'limit': 100}
        if starting_after:
            payload['starting_after'] = starting_after
        
        print(f"   📄 Processing page {page + 1}...")
        response = call_api('/api/v2/leads/list', 'POST', payload)
        
        if 'error' in response:
            print(f"   ❌ Error on page {page + 1}: {response}")
            break
        
        leads = response.get('items', [])
        if not leads:
            print(f"   ✅ No more leads found - completed full scan")
            break
        
        total_processed += len(leads)
        
        # Classify each lead
        page_no_campaign = 0
        page_wrong_campaign = 0
        page_correct_campaign = 0
        
        for lead in leads:
            campaign_id = lead.get('campaign_id') or lead.get('campaign')
            
            if not campaign_id:
                # No campaign at all
                orphaned_leads.append({
                    'lead': lead,
                    'type': 'no_campaign',
                    'reason': 'No campaign assigned'
                })
                page_no_campaign += 1
            elif campaign_id not in TARGET_CAMPAIGNS:
                # Wrong campaign
                orphaned_leads.append({
                    'lead': lead,
                    'type': 'wrong_campaign', 
                    'reason': f'In campaign {campaign_id} (not SMB/Midsize)',
                    'current_campaign': campaign_id
                })
                page_wrong_campaign += 1
            else:
                # Correct campaign
                page_correct_campaign += 1
        
        page += 1
        
        if page % 10 == 0:
            print(f"   📊 Progress: {total_processed} leads processed, {len(orphaned_leads)} orphaned")
        
        starting_after = response.get('next_starting_after')
        if not starting_after:
            break
        
        time.sleep(0.3)  # Rate limiting
    
    print(f"\n   ✅ SCAN COMPLETE:")
    print(f"      Total leads processed: {total_processed}")
    print(f"      Orphaned leads found: {len(orphaned_leads)}")
    
    # Analyze orphaned types
    no_campaign = sum(1 for x in orphaned_leads if x['type'] == 'no_campaign')
    wrong_campaign = sum(1 for x in orphaned_leads if x['type'] == 'wrong_campaign')
    
    print(f"      - No campaign: {no_campaign}")
    print(f"      - Wrong campaign: {wrong_campaign}")
    
    if page >= 100:
        print(f"   ⚠️ Hit page limit - there may be more orphaned leads beyond what we found")
    
    return orphaned_leads, total_processed

def analyze_orphaned_types(orphaned_leads):
    """Analyze the different types of orphaned leads"""
    print(f"\n📊 ORPHANED LEAD TYPE ANALYSIS")
    print("=" * 50)
    
    type_breakdown = {}
    campaign_breakdown = {}
    verification_breakdown = {}
    recent_breakdown = {'recent': 0, 'old': 0}
    
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=7)
    
    for orphan in orphaned_leads:
        lead = orphan['lead']
        orphan_type = orphan['type']
        
        # Type analysis
        type_breakdown[orphan_type] = type_breakdown.get(orphan_type, 0) + 1
        
        # Campaign analysis (for wrong campaign types)
        if orphan_type == 'wrong_campaign':
            current_campaign = orphan.get('current_campaign', 'unknown')
            campaign_breakdown[current_campaign] = campaign_breakdown.get(current_campaign, 0) + 1
        
        # Verification analysis
        verification = lead.get('verification_status', 'unknown')
        verification_breakdown[verification] = verification_breakdown.get(verification, 0) + 1
        
        # Recency analysis
        created_at = lead.get('created_at')
        if created_at:
            try:
                lead_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                if lead_time > cutoff_time:
                    recent_breakdown['recent'] += 1
                else:
                    recent_breakdown['old'] += 1
            except:
                recent_breakdown['old'] += 1
        else:
            recent_breakdown['old'] += 1
    
    print(f"🔢 ORPHAN TYPES:")
    for orphan_type, count in type_breakdown.items():
        percentage = count / len(orphaned_leads) * 100
        print(f"   {orphan_type}: {count} ({percentage:.1f}%)")
    
    if campaign_breakdown:
        print(f"\n📋 WRONG CAMPAIGNS:")
        for campaign_id, count in sorted(campaign_breakdown.items(), key=lambda x: x[1], reverse=True):
            percentage = count / len(orphaned_leads) * 100
            short_id = campaign_id[:8] + '...' if len(campaign_id) > 12 else campaign_id
            print(f"   {short_id}: {count} ({percentage:.1f}%)")
    
    print(f"\n🔍 VERIFICATION STATUS:")
    for status, count in sorted(verification_breakdown.items(), key=lambda x: x[1], reverse=True):
        percentage = count / len(orphaned_leads) * 100
        print(f"   {status}: {count} ({percentage:.1f}%)")
    
    print(f"\n📅 AGE ANALYSIS:")
    total_with_dates = recent_breakdown['recent'] + recent_breakdown['old'] 
    if total_with_dates > 0:
        recent_pct = recent_breakdown['recent'] / total_with_dates * 100
        old_pct = recent_breakdown['old'] / total_with_dates * 100
        print(f"   Recent (last 7 days): {recent_breakdown['recent']} ({recent_pct:.1f}%)")
        print(f"   Older: {recent_breakdown['old']} ({old_pct:.1f}%)")

def create_assignment_preview(orphaned_leads, max_preview=20):
    """Create a preview of what assignment would look like"""
    print(f"\n🎯 ASSIGNMENT PREVIEW (First {max_preview} leads)")
    print("=" * 60)
    
    preview_count = 0
    assignable_count = 0
    delete_count = 0
    
    for orphan in orphaned_leads[:max_preview]:
        lead = orphan['lead']
        email = lead.get('email', 'unknown')
        company = lead.get('company_name', 'Unknown')
        verification = lead.get('verification_status', 'unknown')
        
        preview_count += 1
        
        # Simple classification logic for preview
        if verification in ['valid', 'verified']:
            # Would be assignable - use simple heuristic for campaign
            if 'corp' in company.lower() or 'inc' in company.lower():
                target_campaign = 'Midsize'
            else:
                target_campaign = 'SMB'
            
            print(f"   {preview_count}. ASSIGN → {target_campaign}: {email} ({company}) - {verification}")
            assignable_count += 1
        else:
            print(f"   {preview_count}. DELETE: {email} ({company}) - {verification}")
            delete_count += 1
    
    print(f"\n   Preview Summary:")
    print(f"      Would assign: {assignable_count}")
    print(f"      Would delete: {delete_count}")
    
    # Extrapolate to full dataset
    if preview_count > 0:
        assign_rate = assignable_count / preview_count
        delete_rate = delete_count / preview_count
        
        total_would_assign = int(len(orphaned_leads) * assign_rate)
        total_would_delete = int(len(orphaned_leads) * delete_rate)
        
        print(f"\n   📊 Extrapolated to all {len(orphaned_leads)} orphaned leads:")
        print(f"      Estimated assignments: {total_would_assign}")
        print(f"      Estimated deletions: {total_would_delete}")

def main():
    print("🎯 COMPREHENSIVE ORPHANED LEAD FINDER")
    print("=" * 60)
    print("Complete analysis of ALL orphaned leads\n")
    
    # Step 1: Discover campaigns
    campaigns = discover_all_campaigns()
    
    # Step 2: Get comprehensive count of orphaned leads
    orphaned_leads, total_processed = count_all_orphaned_leads()
    
    # Step 3: Analyze types of orphaned leads
    if orphaned_leads:
        analyze_orphaned_types(orphaned_leads)
        
        # Step 4: Preview assignment strategy
        create_assignment_preview(orphaned_leads)
    
    # Summary
    print(f"\n🎯 FINAL SUMMARY")
    print("=" * 30)
    print(f"Total leads scanned: {total_processed}")
    print(f"Orphaned leads found: {len(orphaned_leads)}")
    
    if len(orphaned_leads) > 500:
        print(f"\n✅ CONFIRMED: Major orphaned leads issue - {len(orphaned_leads)} leads need attention")
        print(f"💡 This aligns with your estimate of ~700 orphaned leads")
    
    print(f"\n📋 PHASE 2 COMPLETE - Enhanced classification strategy ready")
    print(f"Next: Create BigQuery-enhanced assignment script")

if __name__ == "__main__":
    main()