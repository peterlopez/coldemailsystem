#!/usr/bin/env python3
"""
Test the verification and inventory fixes
"""

import os
import sys

# Set environment variables for testing
os.environ['DRY_RUN'] = 'false'
os.environ['PYTHONPATH'] = '.'

try:
    from sync_once import (
        get_current_instantly_inventory, 
        verify_email,
        logger,
        VERIFICATION_VALID_STATUSES
    )
    
    print("✅ Successfully imported sync_once modules")
    
    # Test 1: Real inventory count
    print("\n📦 TEST 1: Getting real inventory from Instantly API...")
    try:
        inventory = get_current_instantly_inventory()
        print(f"✅ Real inventory count: {inventory} leads")
    except Exception as e:
        print(f"❌ Failed to get inventory: {e}")
    
    # Test 2: Email verification with detailed logging
    print("\n📧 TEST 2: Testing email verification...")
    
    # Test emails - replace with real test emails if needed
    test_emails = [
        "test@example.com",
        "info@validcompany.com",
        "noreply@test.com"
    ]
    
    print(f"Valid statuses accepted: {VERIFICATION_VALID_STATUSES}")
    
    for email in test_emails[:1]:  # Only test first email to save credits
        print(f"\nTesting: {email}")
        try:
            result = verify_email(email)
            print(f"Result: {result}")
            
            if result['status'] in VERIFICATION_VALID_STATUSES:
                print(f"✅ Would be accepted")
            else:
                print(f"❌ Would be rejected")
                
        except Exception as e:
            print(f"❌ Verification error: {e}")
    
    print("\n✅ Tests completed!")
    print("\nNOTE: Check the logs for detailed verification responses")
    print("If all emails are failing, check:")
    print("1. API response format - maybe 'verification_status' field name changed")
    print("2. Valid statuses - maybe the API returns different status values")
    print("3. API endpoint - maybe the verification endpoint changed")
    
except Exception as e:
    print(f"❌ Failed to run tests: {e}")
    import traceback
    traceback.print_exc()