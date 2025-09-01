#!/usr/bin/env python3
"""
Simple Verification Test
Test email verification with known good/bad emails to check if the API is working correctly.
"""

import os
import sys
import json

# Set DRY_RUN to false to test real verification
os.environ['DRY_RUN'] = 'false'

print("🔍 Starting Simple Email Verification Test")
print("=" * 50)

try:
    from sync_once import verify_email, VERIFICATION_VALID_STATUSES
    print("✅ Successfully imported verification functions")
    print(f"📋 Accepted statuses: {VERIFICATION_VALID_STATUSES}")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

def test_email(email: str, expected_result: str = None) -> dict:
    """Test a single email verification."""
    print(f"\n🔍 Testing: {email}")
    try:
        result = verify_email(email)
        
        status = result.get('status', 'unknown')
        disposable = result.get('disposable', False)
        role_based = result.get('role_based', False)
        mx_records = result.get('mx_records', False)
        credits_used = result.get('credits_used', 0)
        
        print(f"   📧 Status: {status}")
        print(f"   🏷️  Disposable: {disposable}")
        print(f"   👤 Role-based: {role_based}")
        print(f"   📮 MX Records: {mx_records}")
        print(f"   💰 Credits: {credits_used}")
        
        would_pass = status in VERIFICATION_VALID_STATUSES
        result_icon = "✅" if would_pass else "❌"
        print(f"   {result_icon} Would Pass: {would_pass}")
        
        if expected_result:
            if expected_result.lower() == 'pass' and would_pass:
                print(f"   ✅ Expected PASS - Got PASS")
            elif expected_result.lower() == 'fail' and not would_pass:
                print(f"   ✅ Expected FAIL - Got FAIL")
            else:
                print(f"   ⚠️  Expected {expected_result} - Got {'PASS' if would_pass else 'FAIL'}")
        
        return result
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return {'email': email, 'error': str(e)}

def main():
    """Run email verification tests."""
    
    # Test 1: Known good corporate emails (should pass)
    print("\n🏢 Testing Known Good Corporate Emails:")
    good_emails = [
        "info@shopify.com",
        "hello@stripe.com", 
        "contact@github.com"
    ]
    
    for email in good_emails:
        test_email(email, expected_result='pass')
    
    # Test 2: Obviously bad emails (should fail)
    print("\n💩 Testing Obviously Bad Emails:")
    bad_emails = [
        "invalid@nonexistentdomain12345.com",
        "test@fake-domain-that-does-not-exist.xyz"
    ]
    
    for email in bad_emails:
        test_email(email, expected_result='fail')
    
    # Test 3: Sample emails from different categories
    print("\n🎯 Testing Mixed Email Types:")
    mixed_emails = [
        "admin@example.com",  # Common role-based
        "noreply@test.com",   # No-reply (role-based)
        "info@gmail.com"      # Free email provider
    ]
    
    for email in mixed_emails:
        test_email(email)
    
    print("\n" + "=" * 50)
    print("🏁 Verification Test Complete")
    print("\n💡 Analysis:")
    print("   - If known good emails are failing, there may be an API issue")
    print("   - If all emails are failing, check API key or configuration")
    print("   - If bad emails are passing, verification is too lenient")

if __name__ == "__main__":
    main()