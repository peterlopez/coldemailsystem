#!/usr/bin/env python3
"""
CORRECTED V2 API Inventory Count
Uses the correct field name "campaign" instead of "campaign_id"
"""

import os
import sys
from datetime import datetime
from shared_config import config

# Import the functions we need from sync_once
sys.path.append('.')
from sync_once import call_instantly_api, SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID

def correct_inventory():
    """Get accurate V2 API inventory count using correct field names."""
    print("🔍 CORRECTED Instantly V2 API Inventory Count")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    
    # Step 1: Verify V2 API Authentication
    print("1️⃣ Testing V2 API Authentication...")
    try:
        workspace_response = call_instantly_api('/api/v2/workspaces/current', method='GET')
        if workspace_response and workspace_response.get('id'):
            print(f"✅ V2 Auth SUCCESS!")
            print(f"   Workspace: {workspace_response.get('name', 'Unknown')}")
        else:
            print(f"❌ V2 Auth FAILED")
            return
    except Exception as e:
        print(f"❌ V2 Auth ERROR: {e}")
        return
    
    print()
    
    # Step 2: Get all leads and filter by campaign
    print("2️⃣ Getting All Leads and Filtering by Campaign...")
    print()
    
    campaigns = [
        ("SMB", SMB_CAMPAIGN_ID),
        ("Midsize", MIDSIZE_CAMPAIGN_ID)
    ]
    
    grand_total_inventory = 0
    all_status_breakdown = {}
    unassigned_count = 0
    
    # Get all leads first
    all_leads = []
    starting_after = None
    page_count = 0
    
    print("📄 Fetching all leads...")
    
    while True:
        data = {'limit': 100}
        
        if starting_after:
            data['starting_after'] = starting_after
        
        response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
        
        if not response or not response.get('items'):
            break
        
        items = response.get('items', [])
        page_count += 1
        all_leads.extend(items)
        
        # Check for next page
        starting_after = response.get('next_starting_after')
        if not starting_after:
            break
        
        # Safety limit
        if page_count > 50:
            print(f"   ⚠️ Hit page limit at {page_count} pages")
            break
    
    print(f"   Total leads fetched: {len(all_leads)}")
    print()
    
    # Now filter by each campaign
    for campaign_name, campaign_id in campaigns:
        print(f"📊 {campaign_name} Campaign ({campaign_id}):")
        
        campaign_leads = []
        campaign_total = 0
        status_breakdown = {}
        
        # Filter leads for this campaign
        for lead in all_leads:
            lead_campaign = lead.get('campaign')  # Use 'campaign' not 'campaign_id'
            
            if lead_campaign == campaign_id:
                campaign_leads.append(lead)
                
                status = lead.get('status', 0)
                status_name = {
                    1: 'active',
                    2: 'paused', 
                    3: 'completed',
                    -1: 'bounced',
                    -2: 'unsubscribed',
                    -3: 'skipped'
                }.get(status, f'unknown_{status}')
                
                status_breakdown[status_name] = status_breakdown.get(status_name, 0) + 1
                
                # Count only Active (1) and Paused (2) as inventory
                if status in [1, 2]:
                    campaign_total += 1
        
        # Display results for this campaign
        print(f"   Total Leads in Campaign: {len(campaign_leads)}")
        print(f"   Total Inventory (Active + Paused): {campaign_total:,} leads")
        
        if status_breakdown:
            print(f"   Status Breakdown:")
            for status, count in sorted(status_breakdown.items()):
                is_inventory = " ✓ COUNTED" if status in ['active', 'paused'] else ""
                print(f"     - {status}: {count:,}{is_inventory}")
        
        # Add to grand totals
        grand_total_inventory += campaign_total
        for status, count in status_breakdown.items():
            all_status_breakdown[status] = all_status_breakdown.get(status, 0) + count
        
        print()
    
    # Count unassigned leads
    for lead in all_leads:
        if not lead.get('campaign'):
            unassigned_count += 1
    
    # Step 3: Show grand totals
    print("3️⃣ GRAND TOTALS:")
    print("=" * 40)
    print(f"🎯 Total Inventory (Active + Paused): {grand_total_inventory:,} leads")
    print(f"📋 Total Assigned Leads: {len(all_leads) - unassigned_count:,}")
    print(f"❓ Unassigned Leads: {unassigned_count:,}")
    print(f"📄 Total Leads in System: {len(all_leads):,}")
    print()
    
    if all_status_breakdown:
        print("📊 Campaign Status Breakdown:")
        for status, count in sorted(all_status_breakdown.items()):
            is_inventory = " ✓ COUNTED IN INVENTORY" if status in ['active', 'paused'] else ""
            print(f"   {status}: {count:,}{is_inventory}")
    
    print()
    
    # Step 4: Capacity calculation
    print("4️⃣ Capacity Calculation:")
    mailboxes = 68
    emails_per_day = 10
    daily_capacity = mailboxes * emails_per_day
    
    safe_limit_default = int(daily_capacity * 3.5)
    utilization_default = (grand_total_inventory / safe_limit_default * 100) if safe_limit_default > 0 else 0
    
    safe_limit_new = int(daily_capacity * 10)
    utilization_new = (grand_total_inventory / safe_limit_new * 100) if safe_limit_new > 0 else 0
    
    print(f"   Daily capacity: {daily_capacity} emails/day")
    print(f"   With multiplier 3.5: {safe_limit_default:,} safe limit ({utilization_default:.1f}% used)")
    print(f"   With multiplier 10: {safe_limit_new:,} safe limit ({utilization_new:.1f}% used)")

if __name__ == "__main__":
    correct_inventory()