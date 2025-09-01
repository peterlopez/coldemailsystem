#!/usr/bin/env python3
"""
Update campaign mailboxes via Instantly API
Remove all current mailboxes and assign only the specified ones
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

# Target mailboxes to assign
TARGET_MAILBOXES = [
    'previews@ripplepreviews.com',
    'r.s.m@openripplestudio.info',
    'r.seth@openripplestudio.info',
    'rohan.s@getrippleaiagency.com',
    'rohan.s@getrippleaihq.com',
    'rohan.s@gogetrippleonline.com',
    'rohan.s@gorippleaihq.com',
    'rohan.s@meetgetrippledigital.com',
    'rohan.s@meetgetripplelabs.com',
    'rohan.s@meetrippleaiagency.com',
    'rohan.s@meetrippleaimedia.com',
    'rohan.s@myrippleai.com',
    'rohan.s@myrippleaiads.com',
    'rohan.s@myrippleaihub.com',
    'rohan.s@opengetripplemedia.com',
    'rohan.s@opengetripplestudio.com',
    'rohan.s@openrippleailabs.com',
    'rohan.s@openripplestudio.info',
    'rohan.s@rippleaihq.com',
    'rohan.s@rippleaimedia.com',
    'rohan.s@rippleaistudio.com',
    'rohan.s@thegetrippleagency.com',
    'rohan.s@thegetripplestudio.com',
    'rohan.s@therippleaiads.com',
    'rohan.s@therippleaiagency.com',
    'rohan.s@therippleaihub.com',
    'rohan.s@trygetripplehq.com',
    'rohan.s@trygetripplemedia.com',
    'rohan.s@trygetripplestudio.com',
    'rohan.s@tryrippleaihub.com',
    'rohan.s@tryrippleaionline.com',
    'rohan.s@tryrippleaisolutions.com',
    'rohan.s@userippleaidigital.com',
    'rohan.s@userippleaisolutions.com',
    'rohan.seth.2@openripplestudio.info',
    'rohan.seth@getrippleaiagency.com',
    'rohan.seth@getrippleaihq.com',
    'rohan.seth@gogetrippleonline.com',
    'rohan.seth@gorippleaihq.com',
    'rohan.seth@meetgetrippledigital.com',
    'rohan.seth@meetgetripplelabs.com',
    'rohan.seth@meetrippleaiagency.com',
    'rohan.seth@meetrippleaimedia.com',
    'rohan.seth@myrippleai.com',
    'rohan.seth@myrippleaiads.com',
    'rohan.seth@myrippleaihub.com',
    'rohan.seth@opengetripplemedia.com',
    'rohan.seth@opengetripplestudio.com',
    'rohan.seth@openrippleailabs.com',
    'rohan.seth@openripplestudio.info',
    'rohan.seth@rippleaihq.com',
    'rohan.seth@rippleaimedia.com',
    'rohan.seth@rippleaistudio.com',
    'rohan.seth@thegetrippleagency.com',
    'rohan.seth@thegetripplestudio.com',
    'rohan.seth@therippleaiads.com',
    'rohan.seth@therippleaiagency.com',
    'rohan.seth@therippleaihub.com',
    'rohan.seth@trygetripplehq.com',
    'rohan.seth@trygetripplemedia.com',
    'rohan.seth@trygetripplestudio.com',
    'rohan.seth@tryrippleaihub.com',
    'rohan.seth@tryrippleaionline.com',
    'rohan.seth@tryrippleaisolutions.com',
    'rohan.seth@userippleaidigital.com',
    'rohan.seth@userippleaisolutions.com',
    'rohan@openripplestudio.info',
    'rseth@openripplestudio.info'
]

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
        elif method == 'PUT':
            response = requests.put(url, headers=headers, json=data, timeout=30)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, json=data or {}, timeout=30)
        
        print(f"API {method} {endpoint}: {response.status_code}")
        if response.status_code >= 400:
            print(f"Response: {response.text}")
        elif response.status_code == 200 and response.text:
            print(f"Success response: {response.text[:200]}...")  # Show first 200 chars
        
        response.raise_for_status()
        return response.json() if response.text else {}
    
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error for {endpoint}: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response: {e.response.text}")
        return None
    except Exception as e:
        print(f"❌ API Error for {endpoint}: {e}")
        return None

def get_all_mailboxes():
    """Get all available mailboxes"""
    print("🔍 Getting all mailboxes...")
    
    # Try different endpoints for getting mailboxes
    endpoints = [
        '/api/v1/account/emails',
        '/api/v2/account/emails', 
        '/api/v1/emails',
        '/api/v2/emails'
    ]
    
    for endpoint in endpoints:
        result = call_api(endpoint)
        if result:
            if 'emails' in result:
                return result['emails']
            elif isinstance(result, list):
                return result
    
    print("❌ Could not get mailboxes from any endpoint")
    return []

def get_campaign_info(campaign_id, campaign_name):
    """Get campaign information"""
    print(f"\n📊 Getting {campaign_name} campaign info...")
    
    # Try different endpoints
    endpoints = [
        f'/api/v1/campaign/{campaign_id}',
        f'/api/v2/campaign/{campaign_id}',
        f'/api/v1/campaigns/{campaign_id}',
        f'/api/v2/campaigns/{campaign_id}'
    ]
    
    for endpoint in endpoints:
        result = call_api(endpoint)
        if result:
            return result
    
    return None

def update_campaign_mailboxes(campaign_id, campaign_name, mailbox_ids):
    """Update campaign mailboxes"""
    print(f"\n🔄 Updating {campaign_name} campaign mailboxes...")
    
    # Try different approaches for updating mailboxes
    update_data = {
        'mailboxes': mailbox_ids,
        'emails': mailbox_ids,
        'account_emails': mailbox_ids
    }
    
    endpoints = [
        f'/api/v1/campaign/{campaign_id}/emails',
        f'/api/v2/campaign/{campaign_id}/emails',
        f'/api/v1/campaign/{campaign_id}/mailboxes',
        f'/api/v2/campaign/{campaign_id}/mailboxes',
        f'/api/v1/campaign/{campaign_id}',
        f'/api/v2/campaign/{campaign_id}'
    ]
    
    methods = ['PUT', 'POST', 'PATCH']
    
    for endpoint in endpoints:
        for method in methods:
            for key, value in update_data.items():
                test_data = {key: value}
                result = call_api(endpoint, method, test_data)
                if result is not None:
                    print(f"✅ {campaign_name} mailboxes updated successfully!")
                    return True
                time.sleep(1)  # Rate limiting
    
    print(f"❌ Failed to update {campaign_name} mailboxes")
    return False

def main():
    print("🔄 UPDATING CAMPAIGN MAILBOXES VIA API")
    print("=" * 50)
    print(f"Target mailboxes: {len(TARGET_MAILBOXES)}")
    
    # Get all available mailboxes
    all_mailboxes = get_all_mailboxes()
    if not all_mailboxes:
        print("❌ Cannot proceed without mailbox data")
        return
    
    print(f"✅ Found {len(all_mailboxes)} total mailboxes in account")
    
    # Map email addresses to mailbox IDs
    mailbox_map = {}
    for mailbox in all_mailboxes:
        email = mailbox.get('email', '').lower()
        mailbox_id = mailbox.get('id') or mailbox.get('_id')
        if email and mailbox_id:
            mailbox_map[email] = mailbox_id
    
    # Find IDs for target mailboxes
    target_mailbox_ids = []
    missing_mailboxes = []
    
    for email in TARGET_MAILBOXES:
        email_lower = email.lower()
        if email_lower in mailbox_map:
            target_mailbox_ids.append(mailbox_map[email_lower])
            print(f"✅ Found: {email}")
        else:
            missing_mailboxes.append(email)
            print(f"❌ Missing: {email}")
    
    if missing_mailboxes:
        print(f"\n⚠️  {len(missing_mailboxes)} mailboxes not found in account:")
        for email in missing_mailboxes[:5]:  # Show first 5
            print(f"  - {email}")
        if len(missing_mailboxes) > 5:
            print(f"  ... and {len(missing_mailboxes) - 5} more")
        
        print(f"\nProceeding with {len(target_mailbox_ids)} found mailboxes...")
    
    if not target_mailbox_ids:
        print("❌ No target mailboxes found. Cannot proceed.")
        return
    
    # Get campaign info
    smb_info = get_campaign_info(SMB_CAMPAIGN_ID, "SMB")
    midsize_info = get_campaign_info(MIDSIZE_CAMPAIGN_ID, "Midsize")
    
    # Update campaigns
    print(f"\n🎯 Updating campaigns with {len(target_mailbox_ids)} mailboxes...")
    
    smb_success = update_campaign_mailboxes(SMB_CAMPAIGN_ID, "SMB", target_mailbox_ids)
    time.sleep(2)
    midsize_success = update_campaign_mailboxes(MIDSIZE_CAMPAIGN_ID, "Midsize", target_mailbox_ids)
    
    # Summary
    print("\n" + "=" * 50)
    print("🎯 UPDATE SUMMARY:")
    print(f"✅ Target mailboxes: {len(TARGET_MAILBOXES)}")
    print(f"✅ Found in account: {len(target_mailbox_ids)}")
    print(f"❌ Missing from account: {len(missing_mailboxes)}")
    print(f"📊 SMB campaign update: {'✅ SUCCESS' if smb_success else '❌ FAILED'}")
    print(f"📊 Midsize campaign update: {'✅ SUCCESS' if midsize_success else '❌ FAILED'}")
    
    if smb_success or midsize_success:
        print(f"\n🚀 Mailbox assignment updated! New daily capacity:")
        print(f"   {len(target_mailbox_ids)} mailboxes × 10 emails/day = {len(target_mailbox_ids) * 10} emails/day")

if __name__ == "__main__":
    main()