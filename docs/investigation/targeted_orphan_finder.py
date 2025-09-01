#!/usr/bin/env python3
"""
Targeted Orphaned Lead Finder

Instead of trying to process all leads, this script specifically looks for:
1. Leads that are NOT in either target campaign
2. Recently created leads that might be orphaned
3. Leads in unexpected campaigns or states
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
    """Make API call"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    
    try:
        if method == 'POST':
            response = requests.post(url, headers=HEADERS, json=data, timeout=60)
        else:
            response = requests.get(url, headers=HEADERS, timeout=60)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {'error': response.status_code, 'text': response.text}
    except Exception as e:
        return {'error': str(e)}

def get_all_campaigns():
    """Get all campaigns to see what else exists"""
    print("🔍 Getting all campaigns...")
    
    response = call_api('/api/v2/campaigns')
    
    if 'error' in response:
        print(f"   ❌ Error getting campaigns: {response}")
        return []
    
    campaigns = response.get('campaigns', [])
    print(f"   ✅ Found {len(campaigns)} campaigns")
    
    for campaign in campaigns:
        campaign_id = campaign.get('id')
        name = campaign.get('name', 'Unnamed')
        status = campaign.get('status', 'Unknown')
        status_text = {1: 'Active', 2: 'Paused', 3: 'Completed', 4: 'Draft'}.get(status, f'Status {status}')
        
        if campaign_id in TARGET_CAMPAIGNS:
            print(f"   🎯 {name} ({campaign_id}): {status_text}")
        else:
            print(f"   ❓ {name} ({campaign_id}): {status_text} - UNEXPECTED CAMPAIGN")
    
    return campaigns

def find_leads_in_unexpected_campaigns(campaigns, max_leads_per_campaign=100):
    """Find leads in campaigns other than our target ones"""
    print(f"\n🔍 Checking for leads in unexpected campaigns...")
    
    unexpected_leads = []
    
    for campaign in campaigns:
        campaign_id = campaign.get('id')
        campaign_name = campaign.get('name', 'Unnamed')
        
        # Skip our target campaigns
        if campaign_id in TARGET_CAMPAIGNS:
            continue
        
        print(f"   📊 Checking {campaign_name} ({campaign_id})...")
        
        payload = {'campaign_id': campaign_id, 'limit': max_leads_per_campaign}
        response = call_api('/api/v2/leads/list', 'POST', payload)
        
        if 'error' not in response:
            leads = response.get('items', [])
            if leads:
                print(f"      🚨 Found {len(leads)} leads in unexpected campaign!")
                for lead in leads:
                    lead['unexpected_campaign_name'] = campaign_name
                    unexpected_leads.extend(leads)
            else:
                print(f"      ✅ No leads found")
        else:
            print(f"      ❌ Error: {response}")
        
        time.sleep(0.5)
    
    return unexpected_leads

def find_leads_without_campaigns(max_pages=20):
    """Find leads that have no campaign assignment"""
    print(f"\n🔍 Looking for leads without campaign assignments...")
    
    orphaned_leads = []
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
            print(f"   ✅ No more leads found")
            break
        
        # Filter for leads without campaigns
        page_orphans = []
        for lead in leads:
            campaign_id = lead.get('campaign_id') or lead.get('campaign')
            if not campaign_id:
                page_orphans.append(lead)
        
        orphaned_leads.extend(page_orphans)
        page += 1
        
        if page_orphans:
            print(f"   📄 Page {page}: Found {len(page_orphans)} orphaned leads ({len(orphaned_leads)} total)")
        
        starting_after = response.get('next_starting_after')
        if not starting_after:
            print(f"   ✅ Searched all pages")
            break
        
        time.sleep(0.5)
    
    if page >= max_pages:
        print(f"   ⚠️ Stopped at page limit - there may be more orphaned leads")
    
    return orphaned_leads

def analyze_recent_activity(days_back=7):
    """Look for recently created leads that might be orphaned"""
    print(f"\n📅 Analyzing recent lead activity (last {days_back} days)...")
    
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
    recent_orphans = []
    
    # Sample recent leads
    payload = {'limit': 200}
    response = call_api('/api/v2/leads/list', 'POST', payload)
    
    if 'error' not in response:
        leads = response.get('items', [])
        
        for lead in leads:
            created_at = lead.get('created_at')
            if created_at:
                try:
                    lead_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    if lead_time > cutoff_time:
                        campaign_id = lead.get('campaign_id') or lead.get('campaign')
                        if not campaign_id or campaign_id not in TARGET_CAMPAIGNS:
                            recent_orphans.append(lead)
                except:
                    pass
        
        print(f"   📊 Found {len(recent_orphans)} recent orphaned leads in sample")
        
        if recent_orphans:
            print(f"   📋 Recent orphan examples:")
            for i, lead in enumerate(recent_orphans[:5]):
                email = lead.get('email', 'unknown')
                created = lead.get('created_at', '')
                verification = lead.get('verification_status', 'unknown')
                campaign = lead.get('campaign_id') or 'NONE'
                print(f"      {i+1}. {email} - {created[:10]} - {verification} - Campaign: {campaign}")
    
    return recent_orphans

def create_targeted_sample(max_sample=50):
    """Create a targeted sample of orphaned leads for analysis"""
    print(f"\n🎯 Creating targeted sample of orphaned leads...")
    
    sample_leads = []
    
    # Strategy 1: Get a small sample and filter
    payload = {'limit': 500}  # Get larger sample
    response = call_api('/api/v2/leads/list', 'POST', payload)
    
    if 'error' not in response:
        leads = response.get('items', [])
        
        orphaned_count = 0
        for lead in leads:
            campaign_id = lead.get('campaign_id') or lead.get('campaign')
            
            # Consider orphaned if:
            # 1. No campaign at all, OR
            # 2. In a campaign that's not one of our targets
            if not campaign_id or campaign_id not in TARGET_CAMPAIGNS:
                sample_leads.append(lead)
                orphaned_count += 1
                
                if len(sample_leads) >= max_sample:
                    break
        
        print(f"   ✅ Found {orphaned_count} orphaned leads in sample of {len(leads)}")
        print(f"   📊 Orphaned rate: {orphaned_count/len(leads)*100:.1f}%")
        
        # Extrapolate to estimate total
        if orphaned_count > 0:
            estimated_total_orphaned = (orphaned_count / len(leads)) * 3000  # Rough estimate of total leads
            print(f"   🚨 ESTIMATED TOTAL ORPHANED: ~{int(estimated_total_orphaned)} leads")
    
    return sample_leads

def analyze_sample(sample_leads):
    """Analyze the sample of orphaned leads"""
    print(f"\n📊 ANALYZING SAMPLE OF {len(sample_leads)} ORPHANED LEADS")
    print("=" * 60)
    
    campaign_analysis = {}
    verification_analysis = {}
    recent_count = 0
    
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=7)
    
    for lead in sample_leads:
        # Campaign analysis
        campaign_id = lead.get('campaign_id') or 'NONE'
        campaign_analysis[campaign_id] = campaign_analysis.get(campaign_id, 0) + 1
        
        # Verification analysis
        verification = lead.get('verification_status', 'unknown')
        verification_analysis[verification] = verification_analysis.get(verification, 0) + 1
        
        # Recent analysis
        created_at = lead.get('created_at')
        if created_at:
            try:
                lead_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                if lead_time > cutoff_time:
                    recent_count += 1
            except:
                pass
    
    print(f"📋 CAMPAIGN DISTRIBUTION:")
    for campaign_id, count in sorted(campaign_analysis.items(), key=lambda x: x[1], reverse=True):
        percentage = count / len(sample_leads) * 100
        if campaign_id == 'NONE':
            print(f"   No Campaign: {count} ({percentage:.1f}%)")
        elif campaign_id in TARGET_CAMPAIGNS:
            name = "SMB" if campaign_id == SMB_CAMPAIGN_ID else "Midsize"
            print(f"   {name} (unexpected): {count} ({percentage:.1f}%)")
        else:
            print(f"   Other Campaign ({campaign_id[:8]}...): {count} ({percentage:.1f}%)")
    
    print(f"\n🔍 VERIFICATION STATUS:")
    for status, count in sorted(verification_analysis.items(), key=lambda x: x[1], reverse=True):
        percentage = count / len(sample_leads) * 100
        print(f"   {status}: {count} ({percentage:.1f}%)")
    
    print(f"\n📅 RECENT ACTIVITY:")
    print(f"   Created in last 7 days: {recent_count} ({recent_count/len(sample_leads)*100:.1f}%)")
    
    return {
        'campaign_analysis': campaign_analysis,
        'verification_analysis': verification_analysis,
        'recent_count': recent_count
    }

def main():
    print("🎯 TARGETED ORPHANED LEAD FINDER")
    print("=" * 50)
    print("Looking for leads that are NOT in target campaigns")
    
    # Step 1: See what campaigns exist
    campaigns = get_all_campaigns()
    
    # Step 2: Check for leads in unexpected campaigns
    unexpected_leads = find_leads_in_unexpected_campaigns(campaigns)
    
    # Step 3: Look for leads without any campaign
    no_campaign_leads = find_leads_without_campaigns()
    
    # Step 4: Create targeted sample
    sample_leads = create_targeted_sample()
    
    # Step 5: Analyze recent activity
    recent_orphans = analyze_recent_activity()
    
    # Step 6: Comprehensive analysis
    if sample_leads:
        analysis = analyze_sample(sample_leads)
    
    print(f"\n🎯 SUMMARY FINDINGS:")
    print(f"   Leads in unexpected campaigns: {len(unexpected_leads)}")
    print(f"   Leads with no campaign: {len(no_campaign_leads)}")
    print(f"   Sample orphaned leads: {len(sample_leads)}")
    print(f"   Recent orphans (7 days): {len(recent_orphans)}")
    
    total_identified = len(unexpected_leads) + len(no_campaign_leads)
    print(f"\n🚨 TOTAL ORPHANED LEADS IDENTIFIED: {total_identified}")
    
    if total_identified > 500:
        print(f"\n✅ CONFIRMED: Significant orphaned leads issue exists")
        print(f"💡 This matches your estimate of ~700 orphaned leads")
    
    print(f"\n📋 NEXT STEPS:")
    print(f"1. Review the findings above")
    print(f"2. Focus on the specific types of orphaned leads found")
    print(f"3. Prepare assignment strategy for each type")

if __name__ == "__main__":
    main()