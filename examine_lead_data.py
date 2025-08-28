#!/usr/bin/env python3
"""
Examine the structure of lead data from Instantly V2 API
"""

import os
import sys
import json
from datetime import datetime
from shared_config import config

# Import the functions we need from sync_once
sys.path.append('.')
from sync_once import call_instantly_api

def examine_leads():
    """Look at the detailed structure of lead data."""
    print("🔍 Examining Lead Data Structure")
    print("=" * 60)
    
    try:
        # Get first page of leads
        data = {'limit': 5}  # Just get a few for examination
        
        response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
        
        if response and response.get('items'):
            items = response.get('items', [])
            
            print(f"Found {len(items)} leads to examine")
            print()
            
            for i, lead in enumerate(items):
                print(f"Lead {i+1}:")
                print(f"  All available fields: {list(lead.keys())}")
                print(f"  Email: {lead.get('email', 'N/A')}")
                print(f"  Status: {lead.get('status', 'N/A')}")
                print(f"  Campaign ID: {lead.get('campaign_id', 'N/A')}")
                
                # Look for other campaign-related fields
                campaign_fields = [k for k in lead.keys() if 'campaign' in k.lower()]
                if campaign_fields:
                    print(f"  Campaign-related fields: {campaign_fields}")
                    for field in campaign_fields:
                        print(f"    {field}: {lead.get(field)}")
                
                # Show all fields for first lead
                if i == 0:
                    print(f"  Full lead data:")
                    for key, value in lead.items():
                        print(f"    {key}: {value}")
                
                print()
                
        else:
            print("❌ No leads found or API error")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")

def test_campaign_endpoints():
    """Test different ways to get campaign information."""
    print("🔍 Testing Campaign Endpoints")
    print("=" * 60)
    
    try:
        # Try to get campaigns list
        print("Testing GET /api/v2/campaigns...")
        campaigns = call_instantly_api('/api/v2/campaigns', method='GET')
        
        if campaigns:
            print(f"Campaigns response: {campaigns}")
            print()
            
            # If we get campaigns, show their structure
            if isinstance(campaigns, list):
                for i, campaign in enumerate(campaigns):
                    print(f"Campaign {i+1}:")
                    print(f"  ID: {campaign.get('id', 'N/A')}")
                    print(f"  Name: {campaign.get('name', 'N/A')}")
                    print(f"  Status: {campaign.get('status', 'N/A')}")
                    if hasattr(campaign, 'keys'):
                        print(f"  All fields: {list(campaign.keys())}")
                    print()
        else:
            print("No campaigns found or endpoint doesn't exist")
            
    except Exception as e:
        print(f"❌ Campaigns endpoint error: {e}")

if __name__ == "__main__":
    examine_leads()
    print()
    test_campaign_endpoints()