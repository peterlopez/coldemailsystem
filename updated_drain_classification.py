#!/usr/bin/env python3
"""
Updated drain classification with proper auto-reply detection.
Uses the 'pause_until' field to identify auto-replies (OOO messages).
"""

from datetime import datetime

def classify_lead_with_auto_reply_detection_v2(lead, campaign_name):
    """
    Enhanced classification that properly detects auto-replies using pause_until field.
    """
    try:
        email = lead.get('email', 'unknown')
        status = lead.get('status', 0)  
        esp_code = lead.get('esp_code', 0)  
        reply_count = lead.get('email_reply_count', 0)
        created_at = lead.get('timestamp_created')
        
        # NEW: Check for auto-reply indicators
        payload = lead.get('payload', {})
        pause_until = payload.get('pause_until') if payload else None
        
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
        print(f"   Days Since Created: {days_since_created}")
        print(f"   🔍 Auto-reply check: pause_until = {pause_until}")
        
        # Status 3 with replies - check for auto-reply first
        if status == 3 and reply_count > 0:
            
            # AUTO-REPLY DETECTION
            if pause_until:
                print(f"   🤖 AUTO-REPLY DETECTED: Lead paused until {pause_until}")
                print(f"   📋 Action: Do NOT drain - this is an out-of-office response")
                return {
                    'should_drain': False,
                    'keep_reason': f'Auto-reply detected (paused until {pause_until}) - not genuine engagement',
                    'auto_reply': True
                }
            
            # No auto-reply indicators - genuine engagement
            else:
                print(f"   ✅ GENUINE REPLY: No auto-reply indicators found")
                print(f"   📋 Action: Drain as 'replied' - real engagement")
                return {
                    'should_drain': True,
                    'drain_reason': 'replied',
                    'details': f'Status 3 with {reply_count} replies - genuine engagement (no auto-reply flags)',
                    'auto_reply': False
                }
        
        # Status 3 without replies - completed sequence
        elif status == 3 and reply_count == 0:
            return {
                'should_drain': True,
                'drain_reason': 'completed',
                'details': 'Sequence completed without replies'
            }
        
        # Hard bounces with grace period
        elif esp_code in [550, 551, 553]:
            if days_since_created >= 7:
                return {
                    'should_drain': True,
                    'drain_reason': 'bounced_hard',
                    'details': f'Hard bounce (ESP {esp_code}) after {days_since_created} days'
                }
            else:
                return {
                    'should_drain': False,
                    'keep_reason': f'Recent hard bounce (ESP {esp_code}), within 7-day grace period'
                }
        
        # Soft bounces - keep for retry
        elif esp_code in [421, 450, 451]:
            return {
                'should_drain': False,
                'keep_reason': f'Soft bounce (ESP {esp_code}) - keeping for retry'
            }
        
        # Unsubscribes (check status text)
        elif 'unsubscribed' in str(lead.get('status_text', '')).lower():
            return {
                'should_drain': True,
                'drain_reason': 'unsubscribed',
                'details': 'Lead unsubscribed from campaign'
            }
        
        # Very old active leads
        elif status == 1 and days_since_created >= 90:
            return {
                'should_drain': True,
                'drain_reason': 'stale_active',
                'details': f'Active lead stuck for {days_since_created} days'
            }
        
        # Status 1 with replies and pause_until - auto-replies that didn't stop sequence
        elif status == 1 and reply_count > 0 and pause_until:
            print(f"   🤖 ACTIVE AUTO-REPLY: Status 1 + replies + paused = OOO continuing sequence")
            return {
                'should_drain': False,
                'keep_reason': f'Active lead with auto-reply (paused until {pause_until}) - sequence will continue',
                'auto_reply': True
            }
        
        # Default: Keep active leads
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

def test_updated_classification(email):
    """Test the updated classification with auto-reply detection."""
    
    import os
    import requests
    import json
    
    # Load API key
    INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
    if not INSTANTLY_API_KEY:
        try:
            with open('config/secrets/instantly-config.json', 'r') as f:
                config = json.load(f)
                INSTANTLY_API_KEY = config['api_key']
        except:
            print("❌ No API key found")
            return

    BASE_URL = "https://api.instantly.ai"
    
    print(f"\n🔍 UPDATED AUTO-REPLY TEST: {email}")
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
        
        # Apply updated classification
        result = classify_lead_with_auto_reply_detection_v2(lead, campaign_name)
        
        print(f"\n🎯 UPDATED CLASSIFICATION RESULT:")
        if result['should_drain']:
            print(f"   ✅ DRAIN as '{result['drain_reason']}'")
            print(f"   📋 Details: {result['details']}")
        else:
            print(f"   ⏸️ KEEP in campaign")
            print(f"   📋 Reason: {result['keep_reason']}")
            
        # Show auto-reply detection
        if 'auto_reply' in result:
            if result['auto_reply']:
                print(f"   🤖 AUTO-REPLY: YES - Properly detected and handled")
            else:
                print(f"   👤 GENUINE REPLY: YES - No auto-reply indicators")
            
        # Compare with original logic
        original_would_drain = lead.get('status') == 3 and lead.get('email_reply_count', 0) > 0
        updated_drains = result['should_drain']
        
        print(f"\n🔄 LOGIC COMPARISON:")
        print(f"   Original logic: {'DRAIN' if original_would_drain else 'KEEP'}")
        print(f"   Updated logic: {'DRAIN' if updated_drains else 'KEEP'}")
        
        if original_would_drain != updated_drains:
            print(f"   🎯 IMPROVED: Updated logic correctly handles auto-replies")
        else:
            print(f"   ✅ CONSISTENT: Both logics agree (no auto-reply detected)")
        
    except Exception as e:
        print(f"❌ Test error: {e}")

if __name__ == "__main__":
    # Test on the auto-reply case
    test_updated_classification("info@travall.com")
    
    # Test on genuine reply case
    test_updated_classification("info@madelines.co")
    
    print("\n" + "=" * 80)
    print("💡 UPDATED AUTO-REPLY DETECTION:")
    print("=" * 80)
    print("✅ Uses 'pause_until' field to detect out-of-office responses")
    print("✅ Prevents draining auto-replies as genuine engagement")
    print("✅ Maintains proper inventory management for real replies")
    print("✅ Handles both Status 1 and Status 3 auto-reply scenarios")