#!/usr/bin/env python3
"""
Debug Instantly V2 API Inventory Count
Shows detailed information about what the API is returning
"""

import os
import sys
from datetime import datetime
from shared_config import config

# Import the functions we need from sync_once
sys.path.append('.')
from sync_once import call_instantly_api, SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID

def debug_inventory():
    """Debug V2 API inventory count with detailed breakdown."""
    print("🔍 DEBUG: Instantly V2 API Inventory Count")
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
        else:
            print(f"❌ V2 Auth FAILED - Response: {workspace_response}")
            return
    except Exception as e:
        print(f"❌ V2 Auth ERROR: {e}")
        return
    
    print()
    
    # Step 2: Debug each campaign separately
    campaigns = [
        ("SMB", SMB_CAMPAIGN_ID),
        ("Midsize", MIDSIZE_CAMPAIGN_ID)
    ]
    
    for campaign_name, campaign_id in campaigns:
        print(f"📊 DEBUG {campaign_name} Campaign ({campaign_id}):")
        print()
        
        try:
            # First API call - let's see what we get
            # NOTE: This debug file shows the broken approach for comparison
            data = {
                'campaign_id': campaign_id,  # ❌ BROKEN - API ignores this
                'limit': 10  # Small limit for debugging
            }
            
            response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
            
            print(f"   Raw API Response Structure:")
            if response:
                print(f"     Response keys: {list(response.keys())}")
                print(f"     Items count: {len(response.get('items', []))}")
                print(f"     Has next_starting_after: {'next_starting_after' in response}")
                print(f"     Next cursor: {response.get('next_starting_after', 'None')}")
                
                # Show first few items
                items = response.get('items', [])
                print(f"   First few leads:")
                for i, item in enumerate(items[:3]):
                    print(f"     Lead {i+1}:")
                    print(f"       Email: {item.get('email', 'N/A')}")
                    print(f"       Status: {item.get('status', 'N/A')}")
                    print(f"       Campaign ID: {item.get('campaign_id', 'N/A')}")
                    
                    # Check if campaign_id matches
                    if item.get('campaign_id') != campaign_id:
                        print(f"       ⚠️ MISMATCH: Expected {campaign_id}, got {item.get('campaign_id')}")
                
            else:
                print(f"     ❌ No response or empty response")
            
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
        
        print()
        print("-" * 40)
        print()

if __name__ == "__main__":
    debug_inventory()