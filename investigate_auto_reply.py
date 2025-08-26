#!/usr/bin/env python3
"""
Investigate auto-reply detection in Instantly API.
Understand what data is available to distinguish auto-replies from genuine replies.
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

def investigate_auto_reply_data():
    """Get comprehensive lead data to understand auto-reply detection."""
    
    print("🔍 INVESTIGATING AUTO-REPLY DETECTION")
    print("=" * 60)
    
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # Get leads with replies to understand data structure
    payload = {
        "limit": 10  # Small sample
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
        
        print(f"📊 Analyzing {len(leads)} leads for auto-reply data")
        print()
        
        # Find leads with replies
        replied_leads = [lead for lead in leads if lead.get('email_reply_count', 0) > 0]
        
        if not replied_leads:
            print("⚠️ No leads with replies found in sample")
            print("Let's examine all available fields instead...")
            
            # Show all available fields from any lead
            if leads:
                sample_lead = leads[0]
                print("📋 ALL AVAILABLE FIELDS:")
                for key in sorted(sample_lead.keys()):
                    print(f"   {key}: {sample_lead.get(key)}")
            return
        
        print(f"📧 Found {len(replied_leads)} leads with replies")
        print()
        
        for i, lead in enumerate(replied_leads):
            email = lead.get('email', 'unknown')
            reply_count = lead.get('email_reply_count', 0)
            status = lead.get('status', 0)
            
            print(f"📋 LEAD {i+1}: {email}")
            print(f"   Replies: {reply_count}, Status: {status}")
            
            # Look for auto-reply indicators
            auto_reply_fields = {}
            potential_fields = [
                'auto_reply', 'is_auto_reply', 'auto_response', 'is_auto_response',
                'ooo', 'out_of_office', 'stop_on_auto_reply', 'auto_reply_detected',
                'reply_type', 'message_type', 'automated_response', 'bounce_type'
            ]
            
            for field in potential_fields:
                if field in lead:
                    auto_reply_fields[field] = lead[field]
            
            if auto_reply_fields:
                print("   🎯 Auto-reply related fields:")
                for field, value in auto_reply_fields.items():
                    print(f"      {field}: {value}")
            else:
                print("   ⚠️ No obvious auto-reply fields found")
            
            # Show all fields with 'reply' in name
            reply_related = {}
            for key, value in lead.items():
                if 'reply' in key.lower():
                    reply_related[key] = value
            
            if reply_related:
                print("   📬 All reply-related fields:")
                for field, value in reply_related.items():
                    print(f"      {field}: {value}")
            
            print()
    
    except Exception as e:
        print(f"❌ Investigation error: {e}")

def check_campaign_settings():
    """Check campaign settings for auto-reply configuration."""
    
    print("\n🔧 CHECKING CAMPAIGN AUTO-REPLY SETTINGS")
    print("=" * 60)
    
    campaigns = [
        ("SMB", "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"),
        ("Midsize", "5ffbe8c3-dc0e-41e4-9999-48f00d2015df")
    ]
    
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Accept': 'application/json'
    }
    
    for name, campaign_id in campaigns:
        print(f"\n📊 {name} Campaign: {campaign_id}")
        try:
            response = requests.get(
                f"{BASE_URL}/api/v2/campaigns/{campaign_id}",
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                campaign_data = response.json()
                
                # Look for auto-reply settings
                auto_reply_settings = {}
                for key, value in campaign_data.items():
                    if 'auto' in key.lower() or 'reply' in key.lower() or 'ooo' in key.lower():
                        auto_reply_settings[key] = value
                
                if auto_reply_settings:
                    print("   🎯 Auto-reply settings:")
                    for setting, value in auto_reply_settings.items():
                        print(f"      {setting}: {value}")
                else:
                    print("   ⚠️ No obvious auto-reply settings found")
                    print("   📋 Available settings keys:")
                    for key in sorted(campaign_data.keys())[:10]:  # Show first 10
                        print(f"      {key}")
            else:
                print(f"   ❌ Failed to get campaign: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Campaign check error: {e}")

if __name__ == "__main__":
    investigate_auto_reply_data()
    check_campaign_settings()
    
    print("\n" + "=" * 80)
    print("💡 NEXT STEPS:")
    print("=" * 80)
    print("1. Based on findings, update drain logic to handle auto-replies")
    print("2. If no auto-reply detection found, implement manual detection")
    print("3. Update classification logic to treat auto-replies differently")
    print("4. Test with known auto-reply cases")