#!/usr/bin/env python3
"""
Enhanced auto-reply classification for drain logic.
Addresses the gap where Status 3 + replies might include auto-replies.
"""

import os
import requests
import json
import re
from datetime import datetime, timedelta

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

# Common auto-reply patterns
AUTO_REPLY_PATTERNS = [
    # Out of office
    r'out of office',
    r'away from office',
    r'not in the office',
    r'currently out of office',
    r'away until',
    r'will be out',
    r'temporary away',
    
    # Vacation/leave
    r'on vacation',
    r'on leave',
    r'on holiday',
    r'maternity leave',
    r'paternity leave',
    r'sick leave',
    
    # Auto-response indicators
    r'automatic reply',
    r'auto.?response',
    r'auto.?reply',
    r'automated message',
    r'this is an automated',
    r'thank you for your email',
    r'we have received your',
    r'your message has been received',
    
    # Delivery confirmations
    r'delivery receipt',
    r'read receipt',
    r'message delivered',
    
    # System messages
    r'system generated',
    r'do not reply',
    r'noreply',
    r'no-reply',
]

def classify_lead_with_auto_reply_detection(lead, campaign_name):
    """
    Enhanced classification that accounts for potential auto-reply misclassification.
    """
    try:
        email = lead.get('email', 'unknown')
        status = lead.get('status', 0)  
        esp_code = lead.get('esp_code', 0)  
        reply_count = lead.get('email_reply_count', 0)
        created_at = lead.get('timestamp_created')
        last_reply_at = lead.get('timestamp_last_reply')
        
        # Parse creation date for time-based decisions
        days_since_created = 0
        if created_at:
            try:
                created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                days_since_created = (datetime.now().astimezone() - created_date).days
            except:
                days_since_created = 0
        
        print(f"📊 ENHANCED CLASSIFICATION for {email}:")
        print(f"   Status: {status}, ESP: {esp_code}, Replies: {reply_count}")
        print(f"   Created: {days_since_created} days ago")
        
        # Status 3 with replies needs special handling
        if status == 3 and reply_count > 0:
            print(f"   🔍 Status 3 + replies detected - investigating...")
            
            # Check timing patterns that suggest auto-replies
            auto_reply_score = 0
            auto_reply_reasons = []
            
            # 1. Very quick reply (within minutes of first email)
            if last_reply_at and created_at:
                try:
                    created_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    reply_time = datetime.fromisoformat(last_reply_at.replace('Z', '+00:00'))
                    reply_delay = (reply_time - created_time).total_seconds() / 60  # minutes
                    
                    if reply_delay < 5:  # Replied within 5 minutes
                        auto_reply_score += 2
                        auto_reply_reasons.append(f"Very quick reply ({reply_delay:.1f} minutes)")
                    elif reply_delay < 60:  # Within 1 hour
                        auto_reply_score += 1
                        auto_reply_reasons.append(f"Quick reply ({reply_delay:.1f} minutes)")
                        
                except Exception as e:
                    print(f"   ⚠️ Could not parse reply timing: {e}")
            
            # 2. Multiple replies from same lead (unusual for genuine interest)
            if reply_count > 2:
                auto_reply_score += 1
                auto_reply_reasons.append(f"Multiple replies ({reply_count}) - unusual for genuine engagement")
            
            # 3. Campaign configuration suggests auto-reply handling issue
            if days_since_created == 0:  # Same day creation and finish
                auto_reply_score += 1
                auto_reply_reasons.append("Same-day creation and completion")
            
            print(f"   🤖 Auto-reply score: {auto_reply_score}/5")
            if auto_reply_reasons:
                for reason in auto_reply_reasons:
                    print(f"      - {reason}")
            
            # Classification based on score
            if auto_reply_score >= 3:
                print(f"   ❌ LIKELY AUTO-REPLY (score {auto_reply_score}/5)")
                print(f"   📋 Action: Keep active, do not drain as 'replied'")
                return {
                    'should_drain': False,
                    'keep_reason': f'Likely auto-reply (score {auto_reply_score}/5): {", ".join(auto_reply_reasons)}'
                }
            elif auto_reply_score >= 1:
                print(f"   ⚠️ POSSIBLE AUTO-REPLY (score {auto_reply_score}/5)")
                print(f"   📋 Action: Manual review recommended, drain conservatively")
                # For now, treat as genuine but flag for review
                return {
                    'should_drain': True,
                    'drain_reason': 'replied_suspicious',
                    'details': f'Possible auto-reply (score {auto_reply_score}/5) but draining conservatively: {", ".join(auto_reply_reasons)}'
                }
            else:
                print(f"   ✅ LIKELY GENUINE REPLY (score {auto_reply_score}/5)")
                print(f"   📋 Action: Drain as 'replied' - good engagement")
                return {
                    'should_drain': True,
                    'drain_reason': 'replied',
                    'details': f'Status 3 with {reply_count} replies - genuine engagement'
                }
        
        # Original logic for other cases
        elif status == 3 and reply_count == 0:
            return {
                'should_drain': True,
                'drain_reason': 'completed',
                'details': 'Sequence completed without replies'
            }
        elif esp_code in [550, 551, 553] and days_since_created >= 7:
            return {
                'should_drain': True,
                'drain_reason': 'bounced_hard',
                'details': f'Hard bounce after {days_since_created} days'
            }
        elif esp_code in [421, 450, 451]:
            return {
                'should_drain': False,
                'keep_reason': f'Soft bounce - temporary issue'
            }
        elif status == 1 and days_since_created >= 90:
            return {
                'should_drain': True,
                'drain_reason': 'stale_active',
                'details': f'Active lead stuck for {days_since_created} days'
            }
        else:
            return {
                'should_drain': False,
                'keep_reason': f'Active lead (Status {status}) - {days_since_created} days old'
            }
        
    except Exception as e:
        print(f"   ❌ Classification error: {e}")
        return {
            'should_drain': False,
            'keep_reason': f'Classification error - keeping safely: {str(e)}'
        }

def test_enhanced_classification(email):
    """Test enhanced classification on a specific lead."""
    
    print(f"\n🔍 ENHANCED AUTO-REPLY TEST: {email}")
    print("=" * 60)
    
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # Search for the lead
    search_payload = {"search": email}
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v2/leads/list",
            headers=headers,
            json=search_payload,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ Search failed: {response.status_code}")
            return
            
        search_data = response.json()
        leads = search_data.get('items', [])
        
        if not leads:
            print(f"❌ Lead not found: {email}")
            return
            
        lead = leads[0]
        campaign_name = "SMB" if lead.get('campaign') == "8c46e0c9-c1f9-4201-a8d6-6221bafeada6" else "Midsize"
        
        # Apply enhanced classification
        result = classify_lead_with_auto_reply_detection(lead, campaign_name)
        
        print(f"\n🎯 ENHANCED CLASSIFICATION RESULT:")
        if result['should_drain']:
            print(f"   ✅ DRAIN as '{result['drain_reason']}'")
            print(f"   📋 Details: {result['details']}")
        else:
            print(f"   ⏸️ KEEP in campaign")
            print(f"   📋 Reason: {result['keep_reason']}")
            
        # Compare with original logic
        original_would_drain = lead.get('status') == 3 and lead.get('email_reply_count', 0) > 0
        enhanced_drains = result['should_drain']
        
        print(f"\n🔄 LOGIC COMPARISON:")
        print(f"   Original logic: {'DRAIN' if original_would_drain else 'KEEP'}")
        print(f"   Enhanced logic: {'DRAIN' if enhanced_drains else 'KEEP'}")
        
        if original_would_drain != enhanced_drains:
            print(f"   ⚠️ DIFFERENT RESULT - Enhanced logic provides better classification")
        else:
            print(f"   ✅ SAME RESULT - Both logics agree")
        
    except Exception as e:
        print(f"❌ Test error: {e}")

if __name__ == "__main__":
    # Test on the lead you identified as auto-reply
    test_enhanced_classification("info@travall.com")
    
    print("\n" + "=" * 80)
    print("💡 ENHANCED AUTO-REPLY DETECTION:")
    print("=" * 80)
    print("Score-based classification considers:")
    print("- Reply timing (very quick = likely auto-reply)")
    print("- Reply count (multiple = suspicious)")  
    print("- Creation pattern (same-day finish = potential auto-reply)")
    print()
    print("Scoring:")
    print("- 3+ points = Likely auto-reply (don't drain)")
    print("- 1-2 points = Suspicious (flag for review)")
    print("- 0 points = Genuine reply (drain normally)")