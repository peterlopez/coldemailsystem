#!/usr/bin/env python3
"""
Test getting all leads without campaign filter
"""

import os
import sys
from datetime import datetime
from shared_config import config

# Import the functions we need from sync_once
sys.path.append('.')
from sync_once import call_instantly_api

def test_all_leads():
    """Get all leads to see total count and structure."""
    print("🔍 Testing All Leads (No Campaign Filter)")
    print("=" * 60)
    
    # Test without campaign_id filter
    try:
        total_leads = 0
        status_breakdown = {}
        page_count = 0
        starting_after = None
        
        while True:
            data = {
                'limit': 100
            }
            
            if starting_after:
                data['starting_after'] = starting_after
            
            response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
            
            if not response or not response.get('items'):
                break
            
            items = response.get('items', [])
            page_count += 1
            total_leads += len(items)
            
            print(f"Page {page_count}: {len(items)} leads")
            
            # Count by status
            for item in items:
                status = item.get('status', 0)
                status_name = {
                    1: 'active',
                    2: 'paused', 
                    3: 'completed',
                    -1: 'bounced',
                    -2: 'unsubscribed',
                    -3: 'skipped'
                }.get(status, f'unknown_{status}')
                
                status_breakdown[status_name] = status_breakdown.get(status_name, 0) + 1
            
            # Check for next page
            starting_after = response.get('next_starting_after')
            if not starting_after:
                break
            
            # Safety limit
            if page_count > 50:
                print(f"Hit page limit at {page_count} pages")
                break
        
        print()
        print(f"🎯 Total Leads Found: {total_leads}")
        print(f"📄 Pages processed: {page_count}")
        print()
        print("📊 Status Breakdown:")
        active_and_paused = 0
        for status, count in sorted(status_breakdown.items()):
            is_inventory = ""
            if status in ['active', 'paused']:
                is_inventory = " ✓ INVENTORY"
                active_and_paused += count
            print(f"   {status}: {count:,}{is_inventory}")
        
        print()
        print(f"🎯 Total Active + Paused (Inventory): {active_and_paused:,}")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    test_all_leads()