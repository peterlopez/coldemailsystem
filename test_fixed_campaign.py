#!/usr/bin/env python3
"""
Test the fixed campaign assignment with actual lead creation logic
"""

import os
import sys

# Set up environment for testing
os.environ['DRY_RUN'] = 'false'
os.environ['TARGET_NEW_LEADS_PER_RUN'] = '2'  # Small test
os.environ['LEAD_INVENTORY_MULTIPLIER'] = '3.5'

# Test the fixed sync logic
import sync_once

try:
    print("🧪 Testing fixed campaign assignment...")
    print("Getting 2 leads for testing...")
    
    # Get 2 leads
    leads = sync_once.get_eligible_leads(2)
    
    if not leads:
        print("❌ No leads found")
        exit(1)
    
    print(f"✅ Got {len(leads)} leads")
    
    # Test creating one lead in SMB campaign
    test_lead = leads[0]
    print(f"🧪 Testing with: {test_lead.email}")
    
    lead_id = sync_once.create_lead_in_instantly(test_lead, sync_once.SMB_CAMPAIGN_ID)
    
    if lead_id:
        print(f"✅ SUCCESS! Lead created with ID: {lead_id}")
        print(f"📧 Email: {test_lead.email}")
        print(f"🏢 Company: {test_lead.merchant_name}")
        print(f"🎯 Campaign: {sync_once.SMB_CAMPAIGN_ID}")
        print("\n🔍 Check your Instantly dashboard - this lead should appear in the SMB campaign!")
    else:
        print("❌ FAILED - No lead ID returned")

except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()