#!/usr/bin/env python3
"""
Spot check specific leads in Instantly to analyze their status and drain treatment.
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

def classify_lead_for_drain_analysis(lead, campaign_name):
    """
    Same logic as our drain classification but with detailed analysis output.
    """
    try:
        email = lead.get('email', 'unknown')
        status = lead.get('status', 0)  
        esp_code = lead.get('esp_code', 0)  
        email_reply_count = lead.get('email_reply_count', 0)
        created_at = lead.get('timestamp_created')
        
        # Parse creation date for time-based decisions
        days_since_created = 0
        if created_at:
            try:
                created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                days_since_created = (datetime.now().astimezone() - created_date).days
            except:
                days_since_created = 0
        
        print(f"📊 CLASSIFICATION ANALYSIS for {email}:")
        print(f"   Status: {status}")
        print(f"   ESP Code: {esp_code}")  
        print(f"   Reply Count: {email_reply_count}")
        print(f"   Days Since Created: {days_since_created}")
        print()
        
        # Apply our drain logic step by step
        print("🤖 DRAIN LOGIC EVALUATION:")
        
        # 1. Status 3 = Processed/Finished leads
        if status == 3:
            if email_reply_count > 0:
                result = {
                    'should_drain': True,
                    'drain_reason': 'replied',
                    'details': f'Status 3 with {email_reply_count} replies - genuine engagement'
                }
                print("   ✅ DRAIN: Status 3 + replies > 0 = REPLIED")
                print("   📋 Action: Remove from campaign, add to 90-day cooldown, potential sales lead")
                return result
            else:
                result = {
                    'should_drain': True,
                    'drain_reason': 'completed', 
                    'details': 'Sequence completed without replies'
                }
                print("   ✅ DRAIN: Status 3 + no replies = COMPLETED")
                print("   📋 Action: Remove from campaign, add to 90-day cooldown, free inventory space")
                return result
        
        # 2. ESP Code analysis for email delivery issues
        elif esp_code in [550, 551, 553]:  # Hard bounces
            if days_since_created >= 7:  # 7-day grace period
                result = {
                    'should_drain': True,
                    'drain_reason': 'bounced_hard',
                    'details': f'Hard bounce (ESP {esp_code}) after {days_since_created} days'
                }
                print(f"   ✅ DRAIN: Hard bounce (ESP {esp_code}) after grace period")
                print("   📋 Action: Remove from campaign, email likely invalid")
                return result
            else:
                result = {
                    'should_drain': False,
                    'keep_reason': f'Recent hard bounce (ESP {esp_code}), within 7-day grace period'
                }
                print(f"   ⏸️ KEEP: Hard bounce (ESP {esp_code}) but within 7-day grace period")
                print("   📋 Action: Keep for now, may resolve or be drained after grace period")
                return result
        
        elif esp_code in [421, 450, 451]:  # Soft bounces
            result = {
                'should_drain': False,
                'keep_reason': f'Soft bounce (ESP {esp_code}) - keeping for retry'
            }
            print(f"   ⏸️ KEEP: Soft bounce (ESP {esp_code}) - temporary issue")
            print("   📋 Action: Keep for retry, likely temporary delivery issue")
            return result
        
        # 3. Unsubscribes
        elif 'unsubscribed' in str(lead.get('status_text', '')).lower():
            result = {
                'should_drain': True,
                'drain_reason': 'unsubscribed',
                'details': 'Lead unsubscribed from campaign'
            }
            print("   ✅ DRAIN: Lead unsubscribed")
            print("   📋 Action: Remove from campaign, add to permanent DNC list")
            return result
        
        # 4. Very old active leads (90+ days)
        elif status == 1 and days_since_created >= 90:
            result = {
                'should_drain': True,
                'drain_reason': 'stale_active',
                'details': f'Active lead stuck for {days_since_created} days'
            }
            print(f"   ✅ DRAIN: Stale active lead ({days_since_created} days old)")
            print("   📋 Action: Remove likely stuck lead, free inventory space")
            return result
        
        # DEFAULT: Keep active leads
        else:
            result = {
                'should_drain': False,
                'keep_reason': f'Active lead (Status {status}) - {days_since_created} days old'
            }
            print(f"   ⏸️ KEEP: Active lead (Status {status})")
            if status == 1:
                print("   📋 Action: Keep in campaign, still receiving emails")
            elif status == 2:
                print("   📋 Action: Keep in campaign, currently paused but may resume")
            else:
                print(f"   📋 Action: Keep in campaign, status {status} analysis needed")
            return result
        
    except Exception as e:
        print(f"   ❌ Classification error: {e}")
        return {
            'should_drain': False,
            'keep_reason': f'Classification error - keeping safely: {str(e)}'
        }

def spot_check_lead(email):
    """Spot check a specific lead and analyze how it would be treated."""
    
    print(f"\n🔍 SPOT CHECK: {email}")
    print("=" * 60)
    
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # Search for the lead
    search_payload = {
        "search": email
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v2/leads/list",
            headers=headers,
            json=search_payload,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ Search failed: {response.status_code} - {response.text}")
            return
            
        search_data = response.json()
        leads = search_data.get('items', [])
        
        if not leads:
            print(f"❌ Lead not found: {email}")
            print("   This lead may have been:")
            print("   - Already deleted from campaigns")
            print("   - Never added to Instantly")
            print("   - In a different campaign not searched")
            return
        
        for i, lead in enumerate(leads):
            if len(leads) > 1:
                print(f"\n📋 LEAD {i+1}/{len(leads)} (Multiple instances found)")
            
            # Display lead details
            print(f"📧 Email: {lead.get('email')}")
            print(f"👤 Name: {lead.get('first_name', '')} {lead.get('last_name', '')}")
            print(f"🏢 Company: {lead.get('company_name', 'N/A')}")
            print(f"🎯 Campaign: {lead.get('campaign', 'unknown')}")
            print(f"📅 Created: {lead.get('timestamp_created', 'unknown')}")
            print(f"📅 Updated: {lead.get('timestamp_updated', 'unknown')}")
            print()
            
            print(f"📊 ENGAGEMENT METRICS:")
            print(f"   📧 Email Opens: {lead.get('email_open_count', 0)}")
            print(f"   💬 Email Replies: {lead.get('email_reply_count', 0)}")  
            print(f"   🔗 Email Clicks: {lead.get('email_click_count', 0)}")
            print()
            
            print(f"🔧 TECHNICAL STATUS:")
            print(f"   📊 Status Code: {lead.get('status', 0)}")
            print(f"   📨 ESP Code: {lead.get('esp_code', 0)}")
            print(f"   📋 Status Summary: {lead.get('status_summary', {})}")
            
            # Look for Instantly status information that might contain "out of office"
            print(f"   🔍 Detailed Status Analysis:")
            for key, value in lead.items():
                if 'status' in key.lower():
                    print(f"      {key}: {value}")
                    # Check if value contains out of office indicators
                    if isinstance(value, str) and any(phrase in value.lower() for phrase in ['out of office', 'ooo', 'away', 'vacation']):
                        print(f"         🚨 AUTO-REPLY DETECTED: '{phrase}' found in {key}")
                    elif isinstance(value, dict):
                        # Check nested status objects
                        for nested_key, nested_value in value.items():
                            if isinstance(nested_value, str) and any(phrase in nested_value.lower() for phrase in ['out of office', 'ooo', 'away', 'vacation']):
                                print(f"         🚨 AUTO-REPLY DETECTED: '{phrase}' found in {key}.{nested_key}")
                    elif isinstance(value, list):
                        # Check list of statuses
                        for i, item in enumerate(value):
                            if isinstance(item, str) and any(phrase in item.lower() for phrase in ['out of office', 'ooo', 'away', 'vacation']):
                                print(f"         🚨 AUTO-REPLY DETECTED: '{phrase}' found in {key}[{i}]")
                            elif isinstance(item, dict):
                                for nested_key, nested_value in item.items():
                                    if isinstance(nested_value, str) and any(phrase in nested_value.lower() for phrase in ['out of office', 'ooo', 'away', 'vacation']):
                                        print(f"         🚨 AUTO-REPLY DETECTED: '{phrase}' found in {key}[{i}].{nested_key}")
            print()
            
            print(f"🤖 AUTO-REPLY ANALYSIS:")
            
            # Detailed timing analysis
            created = lead.get('timestamp_created')
            updated = lead.get('timestamp_updated')
            last_reply = lead.get('timestamp_last_reply')
            last_contact = lead.get('timestamp_last_contact')
            
            print(f"   ⏰ Timing Analysis:")
            print(f"      Created: {created}")
            print(f"      Updated: {updated}")
            print(f"      Last Reply: {last_reply}")
            print(f"      Last Contact: {last_contact}")
            
            if created and last_reply:
                try:
                    created_dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
                    reply_dt = datetime.fromisoformat(last_reply.replace('Z', '+00:00'))
                    reply_delay_minutes = (reply_dt - created_dt).total_seconds() / 60
                    reply_delay_hours = reply_delay_minutes / 60
                    
                    print(f"      ⚡ Reply delay: {reply_delay_minutes:.1f} minutes ({reply_delay_hours:.1f} hours)")
                    
                    if reply_delay_minutes < 5:
                        print(f"      🚨 VERY QUICK REPLY (<5 min) - Likely auto-reply!")
                    elif reply_delay_minutes < 60:
                        print(f"      ⚠️ Quick reply (<1 hour) - Possibly auto-reply")
                    elif reply_delay_hours < 24:
                        print(f"      ✅ Normal timing (same day)")
                    else:
                        print(f"      ✅ Normal timing (multiple days)")
                        
                except Exception as e:
                    print(f"      ❌ Timing calculation error: {e}")
            
            # Check for auto-reply related fields
            print(f"   🔍 All reply-related fields:")
            for key, value in lead.items():
                if 'auto' in key.lower() or 'reply' in key.lower() or 'ooo' in key.lower() or 'out_of_office' in key.lower():
                    print(f"      {key}: {value}")
            print()
            
            # Apply our drain classification
            campaign_name = "SMB" if lead.get('campaign') == "8c46e0c9-c1f9-4201-a8d6-6221bafeada6" else "Midsize"
            classification = classify_lead_for_drain_analysis(lead, campaign_name)
            
            print()
            print("🎯 FINAL RECOMMENDATION:")
            if classification['should_drain']:
                print(f"   ✅ DRAIN as '{classification['drain_reason']}'")
                print(f"   📋 Reason: {classification['details']}")
            else:
                print(f"   ⏸️ KEEP in campaign")
                print(f"   📋 Reason: {classification['keep_reason']}")
            print()
        
    except Exception as e:
        print(f"❌ Spot check error: {e}")

if __name__ == "__main__":
    # Test the specific lead requested
    import sys
    if len(sys.argv) > 1:
        spot_check_lead(sys.argv[1])
    else:
        spot_check_lead("support@giftyusa.com")
    
    print("\n" + "=" * 80)
    print("💡 INTERPRETATION GUIDE:")
    print("=" * 80)
    print("Status 1: ACTIVE - Lead is receiving emails in sequence")
    print("Status 2: PAUSED - Lead sequence temporarily stopped") 
    print("Status 3: FINISHED - Lead sequence completed or stopped")
    print()
    print("ESP Codes:")
    print("1000: Success/Normal")
    print("550/551/553: Hard bounces (permanent delivery failure)")
    print("421/450/451: Soft bounces (temporary delivery issues)")
    print()
    print("Our Drain Logic:")
    print("✅ DRAIN: Status 3 (finished), Hard bounces >7 days, Unsubscribes, Stale active >90 days")  
    print("⏸️ KEEP: Status 1/2 (active/paused), Recent hard bounces, Soft bounces")