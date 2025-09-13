#!/usr/bin/env python3
"""
Diagnostic script to investigate why "Completed" leads in Instantly UI aren't being drained.
This script queries the Instantly API to get the actual numeric status codes for leads
that show as "Completed" in the UI to understand the status mapping issue.
"""

import os
import sys
import json
import requests
from datetime import datetime
from typing import List, Dict, Any

# Add shared directory to path
sys.path.append('shared')

def get_instantly_headers():
    """Get headers for Instantly API requests."""
    api_key = os.getenv('INSTANTLY_API_KEY')
    if not api_key:
        # Try loading from config file
        try:
            with open('config/secrets/instantly-config.json', 'r') as f:
                config = json.load(f)
                api_key = config.get('api_key')
        except:
            pass
    
    if not api_key:
        raise ValueError("INSTANTLY_API_KEY not found in environment or config file")
    
    return {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

def search_leads_by_email(emails: List[str], campaign_ids: List[str]) -> Dict[str, Any]:
    """Search for specific leads by email across campaigns."""
    print(f"🔍 Searching for {len(emails)} leads across campaigns...")
    
    base_url = "https://api.instantly.ai"
    found_leads = {}
    
    for campaign_id in campaign_ids:
        print(f"\n📋 Checking campaign {campaign_id}...")
        
        # Use the POST endpoint to get leads from campaign
        url = f"{base_url}/api/v2/leads/list"
        payload = {
            "campaign_id": campaign_id,
            "limit": 100  # Get more leads per page for better chance of finding our targets
        }
        
        starting_after = None
        page_count = 0
        max_pages = 10  # Limit search to prevent long runs
        
        while page_count < max_pages:
            if starting_after:
                payload["starting_after"] = starting_after
            
            try:
                response = requests.post(
                    url,
                    headers=get_instantly_headers(),
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    leads = data.get('items', [])
                    
                    if not leads:
                        print(f"   No more leads found in campaign")
                        break
                    
                    page_count += 1
                    print(f"   📄 Page {page_count}: checking {len(leads)} leads...")
                    
                    # Check if any of our target emails are in this batch
                    for lead in leads:
                        email = lead.get('email', '').lower()
                        if email in [e.lower() for e in emails]:
                            found_leads[email] = lead
                            print(f"   ✅ Found target lead: {email}")
                    
                    # Get next page cursor
                    starting_after = data.get('next_starting_after')
                    if not starting_after:
                        break
                        
                else:
                    print(f"   ❌ API error: {response.status_code} - {response.text}")
                    break
                    
            except Exception as e:
                print(f"   ❌ Exception: {e}")
                break
        
        print(f"   📊 Campaign search complete: {len([k for k, v in found_leads.items() if v.get('campaign_id') == campaign_id])} target leads found")
    
    return found_leads

def analyze_lead_status(lead: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze a lead's status and related fields."""
    email = lead.get('email', 'unknown')
    status = lead.get('status', 'N/A')
    status_text = lead.get('status_text', 'N/A')
    esp_code = lead.get('esp_code', 'N/A')
    email_reply_count = lead.get('email_reply_count', 0)
    created_at = lead.get('timestamp_created', 'N/A')
    updated_at = lead.get('updated_at', 'N/A')
    
    # Check payload for auto-reply indicators
    payload = lead.get('payload', {})
    pause_until = payload.get('pause_until') if payload else None
    
    # Check what our current drain logic would do
    from sync_once import classify_lead_for_drain
    classification = classify_lead_for_drain(lead, "Test Campaign")
    
    analysis = {
        'email': email,
        'numeric_status': status,
        'status_text': status_text,
        'esp_code': esp_code,
        'email_reply_count': email_reply_count,
        'pause_until': pause_until,
        'created_at': created_at,
        'updated_at': updated_at,
        'would_drain': classification.get('should_drain', False),
        'drain_reason': classification.get('drain_reason', 'N/A'),
        'keep_reason': classification.get('keep_reason', 'N/A'),
        'classification_details': classification.get('details', 'N/A')
    }
    
    return analysis

def main():
    """Main investigation function."""
    print("🔍 INVESTIGATING COMPLETED STATUS MAPPING")
    print("=" * 60)
    
    # Sample emails from the user's list (first 10 for focused investigation)
    sample_emails = [
        "care@nuropod.com",
        "info@luxxformen.com", 
        "contact.pufii.ro@gmail.com",
        "info@orchard-house.jp",
        "info@ladesignconcepts.com",
        "bagandwag@outlook.com",
        "ciaobellahair@gmail.com",
        "ventas@masx.cl",
        "hello@millesimebaby.com",
        "contact@yves-jardin.com"
    ]
    
    # Campaign IDs from the CLAUDE.md
    campaign_ids = [
        "8c46e0c9-c1f9-4201-a8d6-6221bafeada6",  # SMB
        "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"   # Midsize
    ]
    
    print(f"📧 Target emails: {len(sample_emails)}")
    print(f"📋 Campaigns to search: {len(campaign_ids)}")
    print()
    
    # Search for the leads
    found_leads = search_leads_by_email(sample_emails, campaign_ids)
    
    if not found_leads:
        print("❌ No target leads found in the API search")
        print("\nPossible reasons:")
        print("1. Leads may have been deleted/drained already")
        print("2. Campaign IDs may have changed") 
        print("3. Search didn't go deep enough into pagination")
        print("4. API access issues")
        return
    
    print(f"\n✅ Found {len(found_leads)} target leads in API")
    print("\n" + "="*60)
    print("📊 DETAILED ANALYSIS")
    print("="*60)
    
    # Analyze each found lead
    status_mapping = {}
    for email, lead in found_leads.items():
        print(f"\n📧 {email}")
        print("-" * 40)
        
        analysis = analyze_lead_status(lead)
        
        # Track status mapping
        numeric_status = analysis['numeric_status']
        if numeric_status not in status_mapping:
            status_mapping[numeric_status] = []
        status_mapping[numeric_status].append(email)
        
        # Print analysis
        for key, value in analysis.items():
            if key != 'email':
                print(f"   {key}: {value}")
    
    # Summary
    print("\n" + "="*60)
    print("📈 SUMMARY & FINDINGS")
    print("="*60)
    
    print(f"\n🎯 Status Code Distribution:")
    for status_code, emails in status_mapping.items():
        print(f"   Status {status_code}: {len(emails)} leads")
        for email in emails[:3]:  # Show first 3 examples
            print(f"      • {email}")
        if len(emails) > 3:
            print(f"      • ... and {len(emails)-3} more")
    
    # Check drain logic effectiveness
    would_drain = sum(1 for lead in found_leads.values() if analyze_lead_status(lead)['would_drain'])
    would_keep = len(found_leads) - would_drain
    
    print(f"\n⚖️ Current Drain Logic Results:")
    print(f"   Would DRAIN: {would_drain} leads")
    print(f"   Would KEEP: {would_keep} leads")
    
    if would_keep > 0:
        print(f"\n❌ ISSUE CONFIRMED: {would_keep} 'Completed' leads are NOT being drained")
        print("   This explains why they remain in campaigns!")
    else:
        print(f"\n✅ No drain logic issues found - all leads would be processed correctly")
    
    # Save detailed results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"status_investigation_results_{timestamp}.json"
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'sample_emails': sample_emails,
        'found_count': len(found_leads),
        'status_mapping': status_mapping,
        'detailed_analysis': {email: analyze_lead_status(lead) for email, lead in found_leads.items()},
        'summary': {
            'would_drain': would_drain,
            'would_keep': would_keep,
            'total_analyzed': len(found_leads)
        }
    }
    
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Detailed results saved to: {results_file}")
    print("\n🔚 Investigation complete!")

if __name__ == "__main__":
    main()