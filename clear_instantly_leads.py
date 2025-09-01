#!/usr/bin/env python3
"""
Clear all leads from Instantly campaigns for clean testing
"""

import os
import requests
import json
import time

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

def call_api(endpoint, method='GET', data=None):
    """Call Instantly API"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=data, timeout=30)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, timeout=30)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error for {endpoint}: {e} - {response.text}")
        return None
    except Exception as e:
        print(f"❌ API Error for {endpoint}: {e}")
        return None

def get_all_leads():
    """Get all leads from both campaigns"""
    print("🔍 Getting all leads from Instantly...")
    
    all_leads = []
    
    # Try to get leads from all campaigns first
    campaigns_result = call_api('/api/v1/campaigns')
    if campaigns_result and 'campaigns' in campaigns_result:
        print(f"Found {len(campaigns_result['campaigns'])} campaigns")
        
        for campaign in campaigns_result['campaigns']:
            campaign_id = campaign['id']
            campaign_name = campaign.get('name', 'Unknown')
            
            # Get leads from this campaign  
            leads_result = call_api(f'/api/v1/campaign/{campaign_id}/leads')
            if leads_result and 'leads' in leads_result:
                campaign_leads = leads_result['leads']
                print(f"  {campaign_name}: {len(campaign_leads)} leads")
                
                for lead in campaign_leads:
                    lead['campaign_name'] = campaign_name
                    lead['campaign_id'] = campaign_id
                    all_leads.append(lead)
    
    # Also try getting leads directly (if API supports it)
    try:
        direct_leads = call_api('/api/v2/leads')
        if direct_leads:
            print(f"Found additional leads via direct API")
            if isinstance(direct_leads, list):
                all_leads.extend(direct_leads)
            elif isinstance(direct_leads, dict) and 'leads' in direct_leads:
                all_leads.extend(direct_leads['leads'])
    except:
        pass  # Direct leads API might not exist
    
    # Remove duplicates based on email
    seen_emails = set()
    unique_leads = []
    for lead in all_leads:
        email = lead.get('email', '').lower()
        if email and email not in seen_emails:
            seen_emails.add(email)
            unique_leads.append(lead)
    
    return unique_leads

def delete_lead(lead_id, lead_email):
    """Delete a single lead"""
    try:
        # Try v2 API first
        result = call_api(f'/api/v2/leads/{lead_id}', method='DELETE')
        if result is not None:
            print(f"✅ Deleted (v2): {lead_email}")
            return True
        
        # Try v1 API as fallback
        result = call_api(f'/api/v1/leads/{lead_id}', method='DELETE')
        if result is not None:
            print(f"✅ Deleted (v1): {lead_email}")
            return True
        
        print(f"❌ Failed to delete: {lead_email}")
        return False
        
    except Exception as e:
        print(f"❌ Error deleting {lead_email}: {e}")
        return False

def main():
    print("🧹 CLEARING ALL INSTANTLY LEADS")
    print("=" * 50)
    
    # Get all leads
    leads = get_all_leads()
    
    if not leads:
        print("✅ No leads found - campaigns are already clean!")
        return
    
    print(f"\n📊 Found {len(leads)} total leads to delete")
    
    # Show breakdown by campaign
    campaign_counts = {}
    for lead in leads:
        campaign_name = lead.get('campaign_name', 'Unknown')
        campaign_counts[campaign_name] = campaign_counts.get(campaign_name, 0) + 1
    
    for campaign_name, count in campaign_counts.items():
        print(f"  {campaign_name}: {count} leads")
    
    # Confirm deletion
    print(f"\n⚠️  This will delete ALL {len(leads)} leads from Instantly!")
    confirm = input("Type 'DELETE' to confirm: ")
    
    if confirm != 'DELETE':
        print("❌ Deletion cancelled")
        return
    
    print(f"\n🗑️  Deleting {len(leads)} leads...")
    
    # Delete leads with rate limiting
    deleted_count = 0
    failed_count = 0
    
    for i, lead in enumerate(leads, 1):
        lead_id = lead.get('id')
        lead_email = lead.get('email', 'Unknown')
        
        if not lead_id:
            print(f"❌ No ID for lead: {lead_email}")
            failed_count += 1
            continue
        
        print(f"[{i}/{len(leads)}] Deleting: {lead_email}")
        
        if delete_lead(lead_id, lead_email):
            deleted_count += 1
        else:
            failed_count += 1
        
        # Rate limiting
        time.sleep(0.5)
    
    print("\n" + "=" * 50)
    print(f"🎯 DELETION COMPLETE:")
    print(f"  ✅ Successfully deleted: {deleted_count} leads")
    print(f"  ❌ Failed to delete: {failed_count} leads")
    print(f"  📊 Total processed: {len(leads)} leads")
    
    if deleted_count > 0:
        print("\n🧹 Instantly campaigns should now be clean!")
        print("Wait 1-2 minutes then check your dashboard to confirm.")

if __name__ == "__main__":
    main()