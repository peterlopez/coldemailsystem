#!/usr/bin/env python3
"""
Test script to validate the new drain functionality works correctly.
This script will test the classify_lead_for_drain logic and get_finished_leads API call.
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_classify_lead_logic():
    """Test the lead classification logic with various scenarios."""
    print("🧪 TESTING LEAD CLASSIFICATION LOGIC")
    print("=" * 60)
    
    # Import the function from sync_once
    from sync_once import classify_lead_for_drain
    
    # Test scenarios based on our approved drain logic
    test_cases = [
        {
            "name": "Replied Lead (Status 3 with replies)",
            "lead": {
                "email": "replied@example.com",
                "status": 3,
                "email_reply_count": 2,
                "created_at": (datetime.now() - timedelta(days=5)).isoformat() + "Z"
            },
            "expected_drain": True,
            "expected_reason": "replied"
        },
        {
            "name": "Completed Sequence (Status 3, no replies)",
            "lead": {
                "email": "completed@example.com", 
                "status": 3,
                "email_reply_count": 0,
                "created_at": (datetime.now() - timedelta(days=10)).isoformat() + "Z"
            },
            "expected_drain": True,
            "expected_reason": "completed"
        },
        {
            "name": "Hard Bounce after grace period",
            "lead": {
                "email": "hardbounce@example.com",
                "status": 1,
                "esp_code": 550,
                "email_reply_count": 0,
                "created_at": (datetime.now() - timedelta(days=8)).isoformat() + "Z"
            },
            "expected_drain": True,
            "expected_reason": "bounced_hard"
        },
        {
            "name": "Recent Hard Bounce (within grace period)",
            "lead": {
                "email": "recentbounce@example.com",
                "status": 1,
                "esp_code": 550,
                "email_reply_count": 0,
                "created_at": (datetime.now() - timedelta(days=3)).isoformat() + "Z"
            },
            "expected_drain": False,
            "expected_reason": None
        },
        {
            "name": "Soft Bounce (keep for retry)",
            "lead": {
                "email": "softbounce@example.com",
                "status": 1,
                "esp_code": 421,
                "email_reply_count": 0,
                "created_at": (datetime.now() - timedelta(days=10)).isoformat() + "Z"
            },
            "expected_drain": False,
            "expected_reason": None
        },
        {
            "name": "Stale Active Lead (90+ days)",
            "lead": {
                "email": "stale@example.com",
                "status": 1,
                "email_reply_count": 0,
                "created_at": (datetime.now() - timedelta(days=95)).isoformat() + "Z"
            },
            "expected_drain": True,
            "expected_reason": "stale_active"
        },
        {
            "name": "Normal Active Lead",
            "lead": {
                "email": "active@example.com",
                "status": 1,
                "email_reply_count": 0,
                "created_at": (datetime.now() - timedelta(days=5)).isoformat() + "Z"
            },
            "expected_drain": False,
            "expected_reason": None
        },
        {
            "name": "Unsubscribed Lead",
            "lead": {
                "email": "unsub@example.com",
                "status": 1,
                "status_text": "unsubscribed",
                "created_at": (datetime.now() - timedelta(days=2)).isoformat() + "Z"
            },
            "expected_drain": True,
            "expected_reason": "unsubscribed"
        }
    ]
    
    passed = 0
    failed = 0
    
    for test_case in test_cases:
        print(f"\n🔍 Testing: {test_case['name']}")
        
        try:
            result = classify_lead_for_drain(test_case['lead'], "SMB")
            
            should_drain = result['should_drain']
            drain_reason = result.get('drain_reason')
            
            # Check if result matches expected
            drain_match = should_drain == test_case['expected_drain']
            reason_match = True
            
            if test_case['expected_drain']:
                reason_match = drain_reason == test_case['expected_reason']
            
            if drain_match and reason_match:
                print(f"  ✅ PASS - Should drain: {should_drain}, Reason: {drain_reason}")
                passed += 1
            else:
                print(f"  ❌ FAIL - Expected drain: {test_case['expected_drain']}, got: {should_drain}")
                print(f"         Expected reason: {test_case['expected_reason']}, got: {drain_reason}")
                print(f"         Full result: {result}")
                failed += 1
                
        except Exception as e:
            print(f"  ❌ ERROR - {e}")
            failed += 1
    
    print(f"\n📊 Classification Test Results: {passed} passed, {failed} failed")
    return failed == 0

def test_api_connection():
    """Test the API connection and lead data structure."""
    print("\n🌐 TESTING INSTANTLY API CONNECTION")
    print("=" * 60)
    
    # Get API key
    INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
    if not INSTANTLY_API_KEY:
        try:
            from config.config import Config
            config = Config()
            INSTANTLY_API_KEY = config.instantly_api_key
        except:
            print("❌ Could not load API key - skipping API tests")
            return False
    
    INSTANTLY_BASE_URL = 'https://api.instantly.ai'
    SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
    
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Test the POST /api/v2/leads/list endpoint
    url = f"{INSTANTLY_BASE_URL}/api/v2/leads/list"
    payload = {
        "campaign_id": SMB_CAMPAIGN_ID,
        "offset": 0,
        "limit": 5  # Just get a few leads for testing
    }
    
    try:
        print(f"📡 Testing API endpoint: POST {url}")
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            leads = data.get('data', [])
            
            print(f"✅ API Response successful - found {len(leads)} leads")
            
            if leads:
                sample_lead = leads[0]
                print(f"\n🔍 Sample lead structure:")
                
                # Show key fields we use in classification
                key_fields = ['email', 'status', 'esp_code', 'email_reply_count', 'created_at', 'status_text']
                
                for field in key_fields:
                    value = sample_lead.get(field, 'NOT_FOUND')
                    print(f"  - {field}: {value}")
                
                print(f"\n📋 All available fields:")
                for key in sorted(sample_lead.keys()):
                    print(f"  - {key}")
                
                # Test classification on real lead
                print(f"\n🧪 Testing classification on real lead:")
                from sync_once import classify_lead_for_drain
                classification = classify_lead_for_drain(sample_lead, "SMB")
                print(f"  Result: {classification}")
                
                return True
            else:
                print("⚠️ No leads found in campaign for testing")
                return True  # Still successful API call
        else:
            print(f"❌ API Error {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ API Connection failed: {e}")
        return False

def test_dry_run_functionality():
    """Test the complete drain functionality in dry run mode."""
    print("\n🏃 TESTING COMPLETE DRAIN FUNCTIONALITY (DRY RUN)")
    print("=" * 60)
    
    # Set dry run mode
    os.environ['DRY_RUN'] = 'true'
    
    try:
        from sync_once import get_finished_leads
        
        print("📡 Calling get_finished_leads() in dry run mode...")
        finished_leads = get_finished_leads()
        
        print(f"✅ Function completed successfully")
        print(f"📊 Found {len(finished_leads)} leads marked for draining")
        
        if finished_leads:
            print(f"\n🗑️ Leads that would be drained:")
            
            drain_reasons = {}
            for lead in finished_leads:
                reason = lead.status
                drain_reasons[reason] = drain_reasons.get(reason, 0) + 1
                print(f"  - {lead.email}: {reason}")
            
            print(f"\n📈 Drain reason summary:")
            for reason, count in drain_reasons.items():
                print(f"  - {reason}: {count} leads")
        
        return True
        
    except Exception as e:
        print(f"❌ Drain functionality test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all drain functionality tests."""
    print("🚀 DRAIN FUNCTIONALITY TEST SUITE")
    print(f"Timestamp: {datetime.now()}")
    print("=" * 80)
    
    # Run all tests
    tests_passed = 0
    total_tests = 3
    
    print("Test 1/3: Lead Classification Logic")
    if test_classify_lead_logic():
        tests_passed += 1
        print("✅ Classification logic tests PASSED")
    else:
        print("❌ Classification logic tests FAILED")
    
    print("\nTest 2/3: API Connection and Data Structure")
    if test_api_connection():
        tests_passed += 1
        print("✅ API connection tests PASSED")
    else:
        print("❌ API connection tests FAILED")
    
    print("\nTest 3/3: Complete Drain Functionality")
    if test_dry_run_functionality():
        tests_passed += 1
        print("✅ Drain functionality tests PASSED")
    else:
        print("❌ Drain functionality tests FAILED")
    
    print("\n" + "=" * 80)
    print(f"🏁 TEST SUITE COMPLETE")
    print(f"📊 Results: {tests_passed}/{total_tests} test groups passed")
    
    if tests_passed == total_tests:
        print("🎉 ALL TESTS PASSED - Drain functionality is ready!")
        return True
    else:
        print("⚠️ Some tests failed - review issues before production use")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)