#!/usr/bin/env python3
"""
Find where Instantly stores detailed status information like "out of office".
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

def deep_search_all_fields(email):
    """Search all fields comprehensively for out of office or status information."""
    
    print(f"🔍 DEEP STATUS SEARCH: {email}")
    print("=" * 60)
    
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # Get lead data
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
        lead_id = lead['id']
        
        print(f"📋 COMPLETE FIELD ANALYSIS:")
        print(f"Lead ID: {lead_id}")
        print()
        
        # Show ALL fields with their values
        ooo_keywords = ['out of office', 'ooo', 'away', 'vacation', 'auto', 'reply', 'bounce', 'unsubscribe']
        
        print("🔍 ALL FIELDS (searching for status indicators):")
        for key in sorted(lead.keys()):
            value = lead[key]
            print(f"   {key}: {value}")
            
            # Check if this field contains relevant keywords
            value_str = str(value).lower()
            found_keywords = [kw for kw in ooo_keywords if kw in value_str]
            if found_keywords:
                print(f"      🎯 Contains keywords: {found_keywords}")
        
        print()
        print("🔍 SEARCHING FOR NESTED STATUS DATA...")
        
        # Try to get more detailed lead information if available
        try:
            detail_response = requests.get(
                f"{BASE_URL}/api/v2/leads/{lead_id}",
                headers=headers,
                timeout=30
            )
            
            if detail_response.status_code == 200:
                detail_data = detail_response.json()
                print("📊 DETAILED LEAD DATA:")
                
                for key in sorted(detail_data.keys()):
                    value = detail_data[key]
                    if key not in lead:  # Show only new fields
                        print(f"   NEW: {key}: {value}")
                        
                        value_str = str(value).lower()
                        found_keywords = [kw for kw in ooo_keywords if kw in value_str]
                        if found_keywords:
                            print(f"      🎯 Contains keywords: {found_keywords}")
            else:
                print(f"⚠️ Could not get detailed data: {detail_response.status_code}")
        
        except Exception as e:
            print(f"⚠️ Detail request error: {e}")
        
        print()
        print("🔍 CHECKING FOR CAMPAIGN ACTIVITY/EVENTS...")
        
        # Check if there are activity/event endpoints that might show status changes
        try:
            # Try different endpoints that might contain status history
            endpoints_to_try = [
                f"/api/v2/leads/{lead_id}/activities",
                f"/api/v2/leads/{lead_id}/events",
                f"/api/v2/leads/{lead_id}/history",
                f"/api/v2/leads/{lead_id}/status",
                f"/api/v2/campaigns/{lead.get('campaign')}/leads/{lead_id}",
            ]
            
            for endpoint in endpoints_to_try:
                try:
                    test_response = requests.get(
                        f"{BASE_URL}{endpoint}",
                        headers=headers,
                        timeout=10
                    )
                    
                    print(f"   {endpoint}: {test_response.status_code}")
                    
                    if test_response.status_code == 200:
                        test_data = test_response.json()
                        print(f"      ✅ SUCCESS - Data available:")
                        
                        # Search this data for status information
                        test_str = json.dumps(test_data).lower()
                        found_keywords = [kw for kw in ooo_keywords if kw in test_str]
                        if found_keywords:
                            print(f"         🎯 Contains keywords: {found_keywords}")
                            print(f"         📋 Raw data: {test_data}")
                        else:
                            # Show structure without overwhelming output
                            if isinstance(test_data, dict):
                                print(f"         📋 Keys: {list(test_data.keys())}")
                            elif isinstance(test_data, list):
                                print(f"         📋 List with {len(test_data)} items")
                            
                except requests.exceptions.Timeout:
                    print(f"   {endpoint}: TIMEOUT")
                except Exception as e:
                    print(f"   {endpoint}: ERROR - {e}")
        
        except Exception as e:
            print(f"⚠️ Activity search error: {e}")
        
        print()
        print("💡 RECOMMENDATIONS:")
        print("1. Check Instantly dashboard to see where 'out of office' status appears")
        print("2. Look for webhook/event data that might capture status changes")
        print("3. Check if there are additional API endpoints for status history")
        print("4. Consider that status might be in campaign-specific data")
        
    except Exception as e:
        print(f"❌ Search error: {e}")

if __name__ == "__main__":
    deep_search_all_fields("info@travall.com")