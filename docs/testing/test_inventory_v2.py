#!/usr/bin/env python3
"""
Test Instantly V2 API Inventory Count
Shows real-time inventory using only V2 API endpoints
"""

import os
import sys
from datetime import datetime
from shared_config import config

# Import the functions we need from sync_once
sys.path.append('.')
from sync_once import call_instantly_api, SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID

def test_inventory():
    """Test V2 API inventory count with detailed breakdown."""
    print("🔍 Testing Instantly V2 API Inventory Count")
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
            print(f"   ID: {workspace_response.get('id')}")
            print(f"   Plan: {workspace_response.get('plan', 'Unknown')}")
        else:
            print(f"❌ V2 Auth FAILED - Response: {workspace_response}")
            return
    except Exception as e:
        print(f"❌ V2 Auth ERROR: {e}")
        return
    
    print()
    
    # Step 2: Count inventory for each campaign
    print("2️⃣ Counting Lead Inventory by Campaign...")
    print()
    
    campaigns = [
        ("SMB", SMB_CAMPAIGN_ID),
        ("Midsize", MIDSIZE_CAMPAIGN_ID)
    ]
    
    grand_total_inventory = 0
    all_status_breakdown = {}
    
    for campaign_name, campaign_id in campaigns:
        print(f"📊 {campaign_name} Campaign ({campaign_id}):")
        
        try:
            campaign_total = 0
            status_breakdown = {}
            starting_after = None
            page_count = 0
            
            while True:
                # V2 API: POST /api/v2/leads/list with cursor pagination
                # NOTE: This test file shows the broken approach for comparison
                # The campaign_id filter doesn't work - keeping for documentation
                data = {
                    'campaign_id': campaign_id,  # ❌ BROKEN - API ignores this
                    'limit': 100
                }
                
                if starting_after:
                    data['starting_after'] = starting_after
                
                response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
                
                if not response or not response.get('items'):
                    break
                
                items = response.get('items', [])
                page_count += 1
                
                # Count by status
                for item in items:
                    status = item.get('status', 0)
                    # V2 API Status codes
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
                
                # Check for next page
                starting_after = response.get('next_starting_after')
                if not starting_after:
                    break
                    
                # Safety limit
                if page_count > 50:
                    print(f"   ⚠️ Hit page limit at {page_count} pages")
                    break
            
            # Display results for this campaign
            print(f"   Total Inventory (Active + Paused): {campaign_total:,} leads")
            print(f"   Pages processed: {page_count}")
            
            if status_breakdown:
                print(f"   Status Breakdown:")
                for status, count in sorted(status_breakdown.items()):
                    is_inventory = " ✓ COUNTED" if status in ['active', 'paused'] else ""
                    print(f"     - {status}: {count:,}{is_inventory}")
            
            # Add to grand totals
            grand_total_inventory += campaign_total
            for status, count in status_breakdown.items():
                all_status_breakdown[status] = all_status_breakdown.get(status, 0) + count
            
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
        
        print()
    
    # Step 3: Show grand totals
    print("3️⃣ GRAND TOTALS:")
    print("=" * 40)
    print(f"🎯 Total Inventory (Active + Paused): {grand_total_inventory:,} leads")
    print()
    
    if all_status_breakdown:
        print("📊 Combined Status Breakdown:")
        for status, count in sorted(all_status_breakdown.items()):
            is_inventory = " ✓ COUNTED IN INVENTORY" if status in ['active', 'paused'] else ""
            print(f"   {status}: {count:,}{is_inventory}")
    
    print()
    
    # Step 4: Capacity calculation with new multiplier
    print("4️⃣ Capacity Calculation:")
    mailboxes = 68
    emails_per_day = 10
    daily_capacity = mailboxes * emails_per_day
    
    # With default multiplier
    safe_limit_default = int(daily_capacity * 3.5)
    utilization_default = (grand_total_inventory / safe_limit_default * 100) if safe_limit_default > 0 else 0
    
    # With new multiplier
    safe_limit_new = int(daily_capacity * 10)
    utilization_new = (grand_total_inventory / safe_limit_new * 100) if safe_limit_new > 0 else 0
    
    print(f"   Mailboxes: {mailboxes}")
    print(f"   Daily capacity: {daily_capacity} emails/day")
    print()
    print(f"   With multiplier 3.5:")
    print(f"     Safe limit: {safe_limit_default:,} leads")
    print(f"     Utilization: {utilization_default:.1f}%")
    print()
    print(f"   With multiplier 10:")
    print(f"     Safe limit: {safe_limit_new:,} leads")
    print(f"     Utilization: {utilization_new:.1f}%")

if __name__ == "__main__":
    test_inventory()