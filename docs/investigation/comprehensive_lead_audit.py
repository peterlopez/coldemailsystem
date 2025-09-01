#!/usr/bin/env python3
"""
Comprehensive Lead Audit - Thorough scan of ALL leads in Instantly

This script performs a complete audit of all leads in the system to:
1. Count total leads
2. Identify any orphaned leads (without campaign assignment)
3. Provide breakdown by campaign
4. Verify data integrity
"""

import os
import sys
import requests
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Set

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

# Campaign IDs
SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
MIDSIZE_CAMPAIGN_ID = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'
TARGET_CAMPAIGNS = {
    SMB_CAMPAIGN_ID: 'SMB Campaign',
    MIDSIZE_CAMPAIGN_ID: 'Midsize Campaign'
}

def call_api(endpoint, method='GET', data=None):
    """Make API call with error handling"""
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

def scan_all_leads():
    """Scan ALL leads in the system - no limits"""
    print("🔍 COMPREHENSIVE LEAD AUDIT - SCANNING ALL LEADS")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Initialize counters
    total_leads = 0
    orphaned_leads = []
    campaign_breakdown = {
        SMB_CAMPAIGN_ID: {'count': 0, 'name': 'SMB Campaign'},
        MIDSIZE_CAMPAIGN_ID: {'count': 0, 'name': 'Midsize Campaign'},
        'other_campaigns': {},
        'no_campaign': 0
    }
    
    # Track unique emails to detect duplicates
    unique_emails = set()
    duplicate_emails = []
    
    # Pagination
    starting_after = None
    page = 0
    
    print("📊 Scanning leads page by page...")
    print("-" * 40)
    
    while True:
        payload = {'limit': 100}
        if starting_after:
            payload['starting_after'] = starting_after
        
        response = call_api('/api/v2/leads/list', 'POST', payload)
        
        if 'error' in response:
            print(f"❌ Error on page {page + 1}: {response}")
            break
        
        leads = response.get('items', [])
        if not leads:
            print(f"\n✅ Scan complete - no more leads found")
            break
        
        page += 1
        page_total = len(leads)
        total_leads += page_total
        
        # Analyze each lead
        page_orphans = 0
        for lead in leads:
            email = lead.get('email', 'unknown')
            campaign_id = lead.get('campaign_id') or lead.get('campaign')
            
            # Check for duplicates
            if email in unique_emails:
                duplicate_emails.append(email)
            else:
                unique_emails.add(email)
            
            # Categorize by campaign
            if not campaign_id:
                # Orphaned lead - no campaign
                orphaned_leads.append({
                    'email': email,
                    'company': lead.get('company_name', 'Unknown'),
                    'id': lead.get('id'),
                    'verification_status': lead.get('verification_status', 'unknown'),
                    'created_at': lead.get('created_at', 'unknown')
                })
                campaign_breakdown['no_campaign'] += 1
                page_orphans += 1
            elif campaign_id == SMB_CAMPAIGN_ID:
                campaign_breakdown[SMB_CAMPAIGN_ID]['count'] += 1
            elif campaign_id == MIDSIZE_CAMPAIGN_ID:
                campaign_breakdown[MIDSIZE_CAMPAIGN_ID]['count'] += 1
            else:
                # Other campaign (unexpected)
                if campaign_id not in campaign_breakdown['other_campaigns']:
                    campaign_breakdown['other_campaigns'][campaign_id] = 0
                campaign_breakdown['other_campaigns'][campaign_id] += 1
        
        # Progress update every 10 pages
        if page % 10 == 0:
            print(f"Progress: Page {page} - Scanned {total_leads} total leads - {len(orphaned_leads)} orphaned")
        elif page_orphans > 0:
            print(f"⚠️  Page {page}: Found {page_orphans} orphaned leads!")
        
        starting_after = response.get('next_starting_after')
        if not starting_after:
            print(f"\n✅ Reached end of pagination")
            break
        
        # Rate limiting
        time.sleep(0.5)
    
    print("\n" + "=" * 60)
    print("📊 COMPREHENSIVE AUDIT RESULTS")
    print("=" * 60)
    
    # Overall Statistics
    print(f"\n🔢 TOTAL LEAD COUNT: {total_leads:,}")
    print(f"   Unique emails: {len(unique_emails):,}")
    print(f"   Duplicate emails: {len(duplicate_emails):,}")
    print(f"   Pages scanned: {page}")
    
    # Campaign Breakdown
    print(f"\n🎯 CAMPAIGN ASSIGNMENT BREAKDOWN:")
    print(f"   ✅ SMB Campaign: {campaign_breakdown[SMB_CAMPAIGN_ID]['count']:,} leads")
    print(f"   ✅ Midsize Campaign: {campaign_breakdown[MIDSIZE_CAMPAIGN_ID]['count']:,} leads")
    
    if campaign_breakdown['other_campaigns']:
        print(f"\n   ❓ OTHER CAMPAIGNS:")
        for campaign_id, count in campaign_breakdown['other_campaigns'].items():
            print(f"      {campaign_id}: {count:,} leads")
    
    print(f"\n   🚨 ORPHANED LEADS (No Campaign): {campaign_breakdown['no_campaign']:,}")
    
    # Orphaned Lead Details
    if orphaned_leads:
        print(f"\n⚠️  ORPHANED LEAD DETAILS (First 20):")
        print("-" * 40)
        for i, orphan in enumerate(orphaned_leads[:20]):
            print(f"{i+1}. {orphan['email']} - {orphan['company']} (Status: {orphan['verification_status']})")
        
        if len(orphaned_leads) > 20:
            print(f"... and {len(orphaned_leads) - 20} more orphaned leads")
    else:
        print(f"\n✅ NO ORPHANED LEADS FOUND! All leads are properly assigned to campaigns.")
    
    # Data Quality Check
    print(f"\n📋 DATA QUALITY METRICS:")
    assigned_total = campaign_breakdown[SMB_CAMPAIGN_ID]['count'] + campaign_breakdown[MIDSIZE_CAMPAIGN_ID]['count']
    other_total = sum(campaign_breakdown['other_campaigns'].values())
    
    print(f"   Leads in target campaigns: {assigned_total:,} ({assigned_total/total_leads*100:.1f}%)")
    print(f"   Leads in other campaigns: {other_total:,} ({other_total/total_leads*100:.1f}%)")
    print(f"   Orphaned leads: {campaign_breakdown['no_campaign']:,} ({campaign_breakdown['no_campaign']/total_leads*100:.1f}%)")
    
    # Final Summary
    print(f"\n📊 FINAL VERIFICATION:")
    if campaign_breakdown['no_campaign'] == 0:
        print(f"   ✅ SUCCESS: All {total_leads:,} leads are properly assigned to campaigns!")
        print(f"   ✅ NO ORPHANED LEADS in the system!")
    else:
        print(f"   ⚠️  WARNING: Found {campaign_breakdown['no_campaign']:,} orphaned leads that need assignment!")
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return {
        'total_leads': total_leads,
        'orphaned_count': campaign_breakdown['no_campaign'],
        'smb_count': campaign_breakdown[SMB_CAMPAIGN_ID]['count'],
        'midsize_count': campaign_breakdown[MIDSIZE_CAMPAIGN_ID]['count'],
        'orphaned_leads': orphaned_leads
    }

if __name__ == "__main__":
    results = scan_all_leads()