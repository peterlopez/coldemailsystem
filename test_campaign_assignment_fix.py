#!/usr/bin/env python3
"""
Test Campaign Assignment Fix - FINAL VERIFICATION

Tests the new 1-step campaign assignment process to ensure:
1. Leads are created with direct campaign assignment
2. Campaign assignment is immediately verified  
3. No orphaned/unassigned leads are created

This replaces the broken 2-step process (create → move) with 
the correct 1-step process (create with campaign field).
"""

import os
import sys
import json
import time
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from sync_once import call_instantly_api, SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID, logger
    from sync_once import Lead  # Import Lead dataclass
except ImportError as e:
    print(f"❌ Failed to import from sync_once: {e}")
    sys.exit(1)

def test_campaign_assignment_fix():
    """Test the corrected 1-step campaign assignment process."""
    print("🧪 Testing Campaign Assignment Fix")
    print("=" * 60)
    print("Testing new 1-step process: create with 'campaign' field + verify")
    print()
    
    # Create test email with timestamp
    timestamp = int(time.time())
    test_email = f"test-campaign-fix-{timestamp}@example.com"
    
    print(f"📧 Test email: {test_email}")
    print(f"🎯 Target campaign: SMB ({SMB_CAMPAIGN_ID})")
    print()
    
    # Test Step 1: Create lead with direct campaign assignment
    print("1️⃣ Testing Lead Creation with Direct Campaign Assignment")
    payload = {
        'email': test_email,
        'first_name': 'Test',
        'last_name': 'Fix',
        'company_name': 'Campaign Fix Test Company',
        'campaign': SMB_CAMPAIGN_ID,  # ✅ Using correct 'campaign' field
        'custom_variables': {
            'company': 'Campaign Fix Test Company',
            'domain': 'test-fix.example.com',
            'location': 'California',
            'country': 'US'
        }
    }
    
    print(f"   📋 Payload uses 'campaign' field: {SMB_CAMPAIGN_ID}")
    
    try:
        response = call_instantly_api('/api/v2/leads', method='POST', data=payload)
        
        if not response:
            print("   ❌ Creation failed - no response")
            return False
            
        lead_id = response.get('id')
        if not lead_id:
            print(f"   ❌ Creation failed - no lead ID in response: {response}")
            return False
            
        print(f"   ✅ Lead created successfully with ID: {lead_id}")
        
    except Exception as e:
        print(f"   ❌ Creation failed with exception: {e}")
        return False
    
    print()
    
    # Test Step 2: Immediate verification via GET
    print("2️⃣ Testing Immediate Campaign Assignment Verification")
    
    try:
        verify_response = call_instantly_api(f'/api/v2/leads/{lead_id}', method='GET')
        
        if not verify_response:
            print("   ❌ Verification failed - no response from GET")
            cleanup_test_lead(lead_id)
            return False
        
        actual_campaign = verify_response.get('campaign')
        print(f"   📋 Expected campaign: {SMB_CAMPAIGN_ID}")
        print(f"   📋 Actual campaign: {actual_campaign}")
        
        if actual_campaign != SMB_CAMPAIGN_ID:
            print("   ❌ ASSIGNMENT FAILED - campaign mismatch!")
            print("   🚨 This indicates the fix didn't work")
            cleanup_test_lead(lead_id)
            return False
        
        print("   ✅ Campaign assignment verified successfully!")
        
    except Exception as e:
        print(f"   ❌ Verification failed with exception: {e}")
        cleanup_test_lead(lead_id)
        return False
    
    print()
    
    # Test Step 3: Confirm lead appears in campaign (not unassigned)
    print("3️⃣ Testing Campaign Inventory Count")
    
    try:
        # Get leads in SMB campaign to confirm our test lead appears
        list_response = call_instantly_api('/api/v2/leads/list', method='POST', data={
            'limit': 50  # Small limit for test
        })
        
        if not list_response:
            print("   ❌ Could not fetch campaign leads")
            cleanup_test_lead(lead_id)
            return False
        
        # Check if our test lead appears in the results
        test_lead_found = False
        unassigned_count = 0
        
        for lead in list_response.get('items', []):
            if lead.get('email') == test_email:
                lead_campaign = lead.get('campaign')
                if lead_campaign == SMB_CAMPAIGN_ID:
                    test_lead_found = True
                    print(f"   ✅ Test lead found in SMB campaign")
                elif not lead_campaign:
                    print(f"   ❌ Test lead found but UNASSIGNED!")
                    cleanup_test_lead(lead_id)
                    return False
                else:
                    print(f"   ❌ Test lead found in wrong campaign: {lead_campaign}")
                    cleanup_test_lead(lead_id)
                    return False
            
            # Count unassigned leads for reference
            if not lead.get('campaign'):
                unassigned_count += 1
        
        if not test_lead_found:
            print(f"   ⚠️ Test lead not found in first 50 leads (may be on later page)")
            print(f"   📊 Unassigned leads in first 50: {unassigned_count}")
        else:
            print(f"   📊 Unassigned leads in first 50: {unassigned_count}")
        
    except Exception as e:
        print(f"   ❌ Campaign inventory check failed: {e}")
        cleanup_test_lead(lead_id)
        return False
    
    print()
    
    # Cleanup
    print("4️⃣ Cleanup")
    success = cleanup_test_lead(lead_id)
    
    print()
    print("📊 FINAL RESULT")
    print("=" * 30)
    
    if success:
        print("✅ CAMPAIGN ASSIGNMENT FIX WORKING!")
        print("   • Lead created with direct campaign assignment")
        print("   • Campaign assignment immediately verified")  
        print("   • No orphaned/unassigned leads created")
        print("   • Ready for production deployment")
        return True
    else:
        print("❌ CAMPAIGN ASSIGNMENT FIX FAILED")
        print("   • Manual investigation required")
        return False

def cleanup_test_lead(lead_id: str) -> bool:
    """Clean up test lead"""
    print(f"   🧹 Cleaning up test lead {lead_id}...")
    
    try:
        delete_response = call_instantly_api(f'/api/v2/leads/{lead_id}', method='DELETE')
        print(f"   ✅ Test lead deleted successfully")
        return True
    except Exception as e:
        print(f"   ⚠️ Could not delete test lead: {e}")
        print(f"   📝 Manual cleanup required for lead ID: {lead_id}")
        return False

if __name__ == "__main__":
    print(f"🕐 Test started at: {datetime.now().isoformat()}")
    print()
    
    success = test_campaign_assignment_fix()
    
    print()
    print(f"🕐 Test completed at: {datetime.now().isoformat()}")
    
    if success:
        print("🎉 All tests passed - fix is working correctly!")
        sys.exit(0)
    else:
        print("💥 Tests failed - fix needs investigation")
        sys.exit(1)