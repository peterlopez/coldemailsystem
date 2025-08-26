#!/usr/bin/env python3
"""
Verify auto-reply logic by examining active leads with replies.
If stop_on_auto_reply=False works correctly, we should see:
- Status 1 (active) leads with replies = auto-replies (sequence continues)
- Status 3 (finished) leads with replies = genuine replies (sequence stopped)
"""

import os
import requests
import json
from datetime import datetime

# Load API key
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
if not INSTANTLY_API_KEY:
    try:
        with open('config/secrets/instantly-config.json', 'r') as f:
            config = json.load(f)
            INSTANTLY_API_KEY = config['api_key']
    except:
        print("❌ No API key found")
        exit(1)

BASE_URL = "https://api.instantly.ai"

def analyze_reply_patterns():
    """Analyze patterns between status and replies to understand auto-reply handling."""
    
    print("🔍 VERIFYING AUTO-REPLY LOGIC")
    print("=" * 60)
    print("Theory: stop_on_auto_reply=False means:")
    print("- Status 1 + replies = auto-replies (sequence continues)")  
    print("- Status 3 + replies = genuine replies (sequence stopped)")
    print()
    
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # Get a larger sample to find reply patterns
    payload = {
        "limit": 100
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v2/leads/list",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ API Error: {response.status_code} - {response.text}")
            return
            
        data = response.json()
        leads = data.get('items', [])
        
        # Categorize leads
        categories = {
            'status_1_with_replies': [],  # Active + replies = likely auto-replies
            'status_1_no_replies': [],    # Active + no replies = normal
            'status_3_with_replies': [],  # Finished + replies = likely genuine
            'status_3_no_replies': [],    # Finished + no replies = completed
            'other_status': []
        }
        
        for lead in leads:
            status = lead.get('status', 0)
            reply_count = lead.get('email_reply_count', 0)
            email = lead.get('email')
            
            if status == 1 and reply_count > 0:
                categories['status_1_with_replies'].append((email, reply_count, lead.get('timestamp_last_contact')))
            elif status == 1 and reply_count == 0:
                categories['status_1_no_replies'].append((email, reply_count, lead.get('timestamp_last_contact')))
            elif status == 3 and reply_count > 0:
                categories['status_3_with_replies'].append((email, reply_count, lead.get('timestamp_last_contact')))
            elif status == 3 and reply_count == 0:
                categories['status_3_no_replies'].append((email, reply_count, lead.get('timestamp_last_contact')))
            else:
                categories['other_status'].append((email, status, reply_count))
        
        print(f"📊 ANALYSIS RESULTS from {len(leads)} leads:")
        print()
        
        print(f"🔴 Status 1 + Replies (likely AUTO-REPLIES): {len(categories['status_1_with_replies'])}")
        if categories['status_1_with_replies']:
            print("   💡 These leads replied but sequence is still active")
            print("   💡 Indicates auto-replies that don't stop sequences")
            for email, replies, last_contact in categories['status_1_with_replies'][:3]:
                print(f"      {email}: {replies} replies, last contact: {last_contact}")
            if len(categories['status_1_with_replies']) > 3:
                print(f"      ... and {len(categories['status_1_with_replies']) - 3} more")
        print()
        
        print(f"🟢 Status 3 + Replies (likely GENUINE REPLIES): {len(categories['status_3_with_replies'])}")
        if categories['status_3_with_replies']:
            print("   💡 These leads replied and sequence stopped")
            print("   💡 Indicates genuine replies that stopped sequences")
            for email, replies, last_contact in categories['status_3_with_replies'][:3]:
                print(f"      {email}: {replies} replies, last contact: {last_contact}")
            if len(categories['status_3_with_replies']) > 3:
                print(f"      ... and {len(categories['status_3_with_replies']) - 3} more")
        print()
        
        print(f"⚪ Status 1 + No Replies (normal active): {len(categories['status_1_no_replies'])}")
        print(f"⚫ Status 3 + No Replies (completed): {len(categories['status_3_no_replies'])}")
        print(f"🔵 Other Status Combinations: {len(categories['other_status'])}")
        print()
        
        print("🎯 LOGIC VALIDATION:")
        
        if len(categories['status_1_with_replies']) > 0:
            print("✅ Found active leads with replies - suggests auto-reply detection working")
            print("   💡 These should NOT be drained as 'replied'")
            print("   💡 Current drain logic would INCORRECTLY keep these (good)")
        else:
            print("⚠️ No active leads with replies found")
            print("   💡 Either no auto-replies, or all auto-replies already processed")
        
        if len(categories['status_3_with_replies']) > 0:
            print("✅ Found finished leads with replies - genuine engagement")
            print("   💡 These should be drained as 'replied'")
            print("   💡 Current drain logic would CORRECTLY drain these")
        else:
            print("⚠️ No finished leads with replies found")
        
        print()
        print("🔧 DRAIN LOGIC RECOMMENDATION:")
        
        if len(categories['status_1_with_replies']) > 0 and len(categories['status_3_with_replies']) > 0:
            print("✅ CURRENT LOGIC IS CORRECT:")
            print("   - Only drain Status 3 + replies (genuine engagement)")
            print("   - Keep Status 1 + replies (auto-replies, sequence continuing)")
        elif len(categories['status_1_with_replies']) == 0:
            print("⚠️ NEED MORE DATA:")
            print("   - No active leads with replies to validate auto-reply handling")
            print("   - Current logic may be too aggressive")
            print("   - Consider additional validation")
        else:
            print("🔍 FURTHER INVESTIGATION NEEDED")
    
    except Exception as e:
        print(f"❌ Analysis error: {e}")

if __name__ == "__main__":
    analyze_reply_patterns()
    
    print("\n" + "=" * 80)
    print("💡 SUMMARY:")
    print("=" * 80)
    print("If Instantly's auto-reply detection works correctly:")
    print("- Auto-replies don't stop sequences (stay Status 1)")
    print("- Genuine replies stop sequences (become Status 3)")
    print("- Our drain logic: Status 3 + replies = drain as 'replied'")
    print("- This should correctly identify genuine engagement only")