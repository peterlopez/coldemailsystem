#!/usr/bin/env python3
"""
Check campaign status and why emails might not be sending
"""

import os
import requests
import json
from datetime import datetime

# Get API key
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
if not INSTANTLY_API_KEY:
    try:
        from config.config import Config
        config = Config()
        INSTANTLY_API_KEY = config.instantly_api_key
    except:
        print("❌ Could not load API key")
        exit(1)

INSTANTLY_BASE_URL = 'https://api.instantly.ai'
SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
MIDSIZE_CAMPAIGN_ID = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'

def call_api(endpoint, method='GET'):
    """Call Instantly API"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def check_campaign_status(campaign_id, campaign_name):
    """Check detailed campaign status"""
    print(f"\n📊 {campaign_name} Campaign Analysis:")
    print("=" * 40)
    
    # Get campaign info
    campaign_data = call_api(f'/api/v2/campaigns/{campaign_id}')
    
    if not campaign_data:
        print("❌ Could not get campaign data")
        return
    
    # Status mapping
    status_map = {1: "✅ ACTIVE", 2: "⏸️ PAUSED", 3: "🛑 STOPPED"}
    status = campaign_data.get('status', 'Unknown')
    status_text = status_map.get(status, f"Unknown ({status})")
    
    print(f"Status: {status_text}")
    
    if status == 2:
        print("⚠️  Campaign is PAUSED - this is why emails aren't sending!")
        print("   → Go to Instantly dashboard and click 'Resume' or 'Activate'")
    
    # Check mailboxes
    email_list = campaign_data.get('email_list', [])
    print(f"Mailboxes assigned: {len(email_list)}")
    
    if len(email_list) == 0:
        print("❌ NO MAILBOXES ASSIGNED - emails cannot send!")
    
    # Check sequences
    sequences = campaign_data.get('sequences', [])
    print(f"Sequences configured: {len(sequences)}")
    
    if len(sequences) == 0:
        print("❌ NO SEQUENCES CONFIGURED - nothing to send!")
    else:
        for i, seq in enumerate(sequences):
            print(f"  Sequence {i+1}: {seq.get('name', 'Unnamed')}")
            # Check if sequence is active
            if not seq.get('active', True):
                print(f"    ⚠️ Sequence is INACTIVE")
    
    # Check schedule
    schedule = campaign_data.get('campaign_schedule', {})
    print(f"\nSchedule Settings:")
    
    # Daily schedule
    days = schedule.get('days', {})
    active_days = [day for day, active in days.items() if active]
    print(f"  Active days: {', '.join(active_days) if active_days else 'NONE'}")
    
    # Time windows
    time_from = schedule.get('time_from', 'Not set')
    time_to = schedule.get('time_to', 'Not set')
    timezone = schedule.get('timezone', 'Not set')
    print(f"  Sending hours: {time_from} - {time_to} ({timezone})")
    
    # Check if we're currently in sending window
    current_hour = datetime.now().hour
    current_day = datetime.now().strftime('%A').lower()
    
    if current_day not in active_days:
        print(f"  ⚠️ Today ({current_day}) is not an active sending day!")
    
    # Other settings
    print(f"\nOther Settings:")
    print(f"  Daily limit: {campaign_data.get('daily_limit', 'Not set')}")
    print(f"  Email gap: {campaign_data.get('email_gap', 'Not set')} minutes between emails")
    print(f"  Stop on reply: {campaign_data.get('stop_on_reply', False)}")
    print(f"  Prioritize new leads: {campaign_data.get('prioritize_new_leads', False)}")
    
    # Get lead count (approximate from BigQuery)
    print(f"\nLead Status:")
    # This would need to query BigQuery for exact counts
    print(f"  Check Instantly dashboard for lead counts and status")

def main():
    print("🔍 INSTANTLY CAMPAIGN STATUS CHECK")
    print("=" * 50)
    print(f"Timestamp: {datetime.now()}")
    
    check_campaign_status(SMB_CAMPAIGN_ID, "SMB")
    check_campaign_status(MIDSIZE_CAMPAIGN_ID, "Midsize")
    
    print("\n" + "=" * 50)
    print("\n🚨 COMMON ISSUES PREVENTING EMAILS:")
    print("1. Campaign Status = PAUSED (most common)")
    print("2. No sequences created or all sequences inactive")
    print("3. Schedule settings prevent sending (wrong days/hours)")
    print("4. Daily limit reached")
    print("5. All leads already in 'completed' state")
    print("6. Mailboxes not warmed up or paused")
    
    print("\n🔧 QUICK FIXES:")
    print("1. Activate/Resume campaigns in Instantly dashboard")
    print("2. Check sequence is active and has email steps")
    print("3. Verify schedule allows sending now")
    print("4. Check mailbox warmup status")

if __name__ == "__main__":
    main()