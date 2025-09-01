#!/usr/bin/env python3
"""
Enhanced Orphaned Leads Assessment

Phase 1: Detailed diagnosis with BigQuery integration
Phase 2: Enhanced classification strategy using real revenue data

This script prepares everything for safe assignment without making changes to Instantly.
"""

import os
import sys
import requests
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Set
from google.cloud import bigquery

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shared_config import PROJECT_ID, DATASET_ID

# Load API key
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
if not INSTANTLY_API_KEY:
    try:
        with open('config/secrets/instantly-config.json', 'r') as f:
            config = json.load(f)
            INSTANTLY_API_KEY = config['api_key']
    except Exception as e:
        print(f"❌ Failed to load API key: {e}")
        sys.exit(1)

INSTANTLY_BASE_URL = 'https://api.instantly.ai'
HEADERS = {
    'Authorization': f'Bearer {INSTANTLY_API_KEY}',
    'Content-Type': 'application/json'
}

# Campaign IDs
SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
MIDSIZE_CAMPAIGN_ID = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'

# Initialize BigQuery client
try:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
    bq_client = bigquery.Client(project=PROJECT_ID)
except Exception as e:
    print(f"❌ Failed to initialize BigQuery client: {e}")
    bq_client = None

def call_api(endpoint, method='GET', data=None):
    """Make API call with error handling"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    
    try:
        if method == 'POST':
            response = requests.post(url, headers=HEADERS, json=data, timeout=30)
        elif method == 'DELETE':
            response = requests.delete(url, headers=HEADERS, timeout=30)
        else:
            response = requests.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {'error': response.status_code, 'message': response.text}
    except Exception as e:
        return {'error': 'exception', 'message': str(e)}

def get_all_orphaned_leads():
    """Get all orphaned leads in the account"""
    print("🔍 Finding ALL orphaned leads...")
    
    orphaned_leads = []
    starting_after = None
    page = 0
    
    while page < 50:  # Safety limit
        payload = {'limit': 100}
        if starting_after:
            payload['starting_after'] = starting_after
            
        response = call_api('/api/v2/leads/list', 'POST', payload)
        
        if 'error' in response:
            print(f"❌ Error fetching leads: {response}")
            break
            
        leads = response.get('items', [])
        if not leads:
            break
            
        # Filter for orphaned leads
        page_orphans = 0
        for lead in leads:
            campaign_id = lead.get('campaign_id') or lead.get('campaign')
            if not campaign_id:
                orphaned_leads.append(lead)
                page_orphans += 1
        
        page += 1
        print(f"   Page {page}: Found {page_orphans} orphans (total: {len(orphaned_leads)})")
        
        starting_after = response.get('next_starting_after')
        if not starting_after:
            break
            
        time.sleep(0.5)  # Rate limiting
    
    print(f"✅ Found {len(orphaned_leads)} total orphaned leads")
    return orphaned_leads

def get_bigquery_lead_data(emails: List[str]) -> Dict[str, Dict]:
    """Get lead data from BigQuery for classification"""
    if not bq_client:
        print("❌ BigQuery client not available")
        return {}
    
    if not emails:
        return {}
    
    print(f"📊 Querying BigQuery for {len(emails)} emails...")
    
    # Convert emails to SQL-safe format
    email_list = "', '".join(emails)
    
    query = f"""
    SELECT 
        email,
        company_name,
        annual_revenue,
        sequence_target,
        domain,
        country,
        location
    FROM `{PROJECT_ID}.{DATASET_ID}.v_ready_for_instantly`
    WHERE email IN ('{email_list}')
    """
    
    try:
        results = bq_client.query(query).result()
        
        lead_data = {}
        for row in results:
            lead_data[row.email] = {
                'company_name': row.company_name,
                'annual_revenue': row.annual_revenue,
                'sequence_target': row.sequence_target,
                'domain': row.domain,
                'country': row.country,
                'location': row.location
            }
        
        print(f"✅ Found BigQuery data for {len(lead_data)} leads")
        return lead_data
        
    except Exception as e:
        print(f"❌ BigQuery query failed: {e}")
        return {}

def classify_lead_with_bigquery(email: str, instantly_lead: Dict, bq_data: Dict) -> Dict:
    """Enhanced lead classification using BigQuery data"""
    
    # Check if we have BigQuery data for this email
    if email in bq_data:
        bq_lead = bq_data[email]
        
        # Use BigQuery sequence_target if available
        sequence_target = bq_lead.get('sequence_target')
        annual_revenue = bq_lead.get('annual_revenue', 0)
        
        if sequence_target == 'SMB' or (annual_revenue and annual_revenue < 1000000):
            return {
                'campaign_id': SMB_CAMPAIGN_ID,
                'campaign_name': 'SMB',
                'classification_source': 'bigquery',
                'annual_revenue': annual_revenue,
                'company_name': bq_lead.get('company_name', ''),
                'assignable': True,
                'reason': f'Revenue: ${annual_revenue:,}' if annual_revenue else 'Sequence target: SMB'
            }
        elif sequence_target == 'Midsize' or (annual_revenue and annual_revenue >= 1000000):
            return {
                'campaign_id': MIDSIZE_CAMPAIGN_ID,
                'campaign_name': 'Midsize',
                'classification_source': 'bigquery',
                'annual_revenue': annual_revenue,
                'company_name': bq_lead.get('company_name', ''),
                'assignable': True,
                'reason': f'Revenue: ${annual_revenue:,}' if annual_revenue else 'Sequence target: Midsize'
            }
    
    # Fallback: Not in BigQuery or unclear classification
    return {
        'campaign_id': None,
        'campaign_name': None,
        'classification_source': 'none',
        'annual_revenue': None,
        'company_name': instantly_lead.get('company_name', ''),
        'assignable': False,
        'reason': 'Not found in BigQuery eligible leads'
    }

def analyze_orphaned_leads(orphaned_leads: List[Dict]) -> Dict:
    """Comprehensive analysis of orphaned leads"""
    print(f"\n📊 ANALYZING {len(orphaned_leads)} ORPHANED LEADS")
    print("=" * 50)
    
    # Extract emails for BigQuery lookup
    emails = [lead.get('email') for lead in orphaned_leads if lead.get('email')]
    
    # Get BigQuery data
    bq_data = get_bigquery_lead_data(emails)
    
    # Analyze each lead
    analysis = {
        'total_orphans': len(orphaned_leads),
        'assignable_to_smb': [],
        'assignable_to_midsize': [],
        'not_assignable': [],
        'verification_status': {},
        'creation_timeline': {},
        'summary': {}
    }
    
    for lead in orphaned_leads:
        email = lead.get('email', 'unknown')
        verification_status = lead.get('verification_status', 'unknown')
        created_at = lead.get('created_at', '')
        
        # Track verification status
        analysis['verification_status'][verification_status] = \
            analysis['verification_status'].get(verification_status, 0) + 1
        
        # Track creation timeline
        try:
            if created_at:
                created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00')).date()
                date_str = created_date.strftime('%Y-%m-%d')
                analysis['creation_timeline'][date_str] = \
                    analysis['creation_timeline'].get(date_str, 0) + 1
        except:
            pass
        
        # Classify lead
        classification = classify_lead_with_bigquery(email, lead, bq_data)
        lead['classification'] = classification
        
        if classification['assignable']:
            if classification['campaign_name'] == 'SMB':
                analysis['assignable_to_smb'].append(lead)
            else:
                analysis['assignable_to_midsize'].append(lead)
        else:
            analysis['not_assignable'].append(lead)
    
    # Generate summary
    analysis['summary'] = {
        'assignable_total': len(analysis['assignable_to_smb']) + len(analysis['assignable_to_midsize']),
        'smb_count': len(analysis['assignable_to_smb']),
        'midsize_count': len(analysis['assignable_to_midsize']),
        'not_assignable_count': len(analysis['not_assignable']),
        'assignable_percentage': round((len(analysis['assignable_to_smb']) + len(analysis['assignable_to_midsize'])) / len(orphaned_leads) * 100, 1)
    }
    
    return analysis

def print_analysis_report(analysis: Dict):
    """Print comprehensive analysis report"""
    summary = analysis['summary']
    
    print(f"\n📊 ORPHANED LEADS ANALYSIS REPORT")
    print("=" * 50)
    
    print(f"\n🔢 OVERALL SUMMARY:")
    print(f"   Total orphaned leads: {analysis['total_orphans']}")
    print(f"   Assignable to campaigns: {summary['assignable_total']} ({summary['assignable_percentage']}%)")
    print(f"   Not assignable: {summary['not_assignable_count']}")
    
    print(f"\n🎯 CAMPAIGN ASSIGNMENT BREAKDOWN:")
    print(f"   → SMB Campaign: {summary['smb_count']} leads")
    print(f"   → Midsize Campaign: {summary['midsize_count']} leads")
    print(f"   → Cannot assign: {summary['not_assignable_count']} leads")
    
    print(f"\n📧 VERIFICATION STATUS:")
    for status, count in sorted(analysis['verification_status'].items()):
        percentage = round(count / analysis['total_orphans'] * 100, 1)
        print(f"   {status}: {count} ({percentage}%)")
    
    print(f"\n📅 CREATION TIMELINE (Recent dates):")
    sorted_dates = sorted(analysis['creation_timeline'].items(), reverse=True)
    for date_str, count in sorted_dates[:7]:  # Show last 7 days
        print(f"   {date_str}: {count} leads")
    
    print(f"\n🚨 NOT ASSIGNABLE LEADS (First 10):")
    for i, lead in enumerate(analysis['not_assignable'][:10]):
        email = lead.get('email', 'unknown')
        reason = lead['classification']['reason']
        verification = lead.get('verification_status', 'unknown')
        print(f"   {i+1}. {email} - {reason} (verification: {verification})")
    
    if len(analysis['not_assignable']) > 10:
        remaining = len(analysis['not_assignable']) - 10
        print(f"   ... and {remaining} more")

def generate_assignment_plan(analysis: Dict) -> Dict:
    """Generate detailed assignment plan"""
    plan = {
        'smb_assignments': [],
        'midsize_assignments': [],
        'deletions': [],
        'total_operations': 0
    }
    
    # SMB assignments
    for lead in analysis['assignable_to_smb']:
        plan['smb_assignments'].append({
            'lead_id': lead.get('id'),
            'email': lead.get('email'),
            'company': lead['classification']['company_name'],
            'revenue': lead['classification']['annual_revenue'],
            'verification': lead.get('verification_status')
        })
    
    # Midsize assignments
    for lead in analysis['assignable_to_midsize']:
        plan['midsize_assignments'].append({
            'lead_id': lead.get('id'),
            'email': lead.get('email'),
            'company': lead['classification']['company_name'],
            'revenue': lead['classification']['annual_revenue'],
            'verification': lead.get('verification_status')
        })
    
    # Deletions (leads not in BigQuery)
    for lead in analysis['not_assignable']:
        if lead['classification']['reason'] == 'Not found in BigQuery eligible leads':
            plan['deletions'].append({
                'lead_id': lead.get('id'),
                'email': lead.get('email'),
                'company': lead.get('company_name', ''),
                'verification': lead.get('verification_status'),
                'reason': 'Not in BigQuery eligible leads'
            })
    
    plan['total_operations'] = len(plan['smb_assignments']) + len(plan['midsize_assignments']) + len(plan['deletions'])
    
    return plan

def print_assignment_plan(plan: Dict):
    """Print detailed assignment plan"""
    print(f"\n📋 ASSIGNMENT EXECUTION PLAN")
    print("=" * 50)
    
    print(f"\n🎯 PLANNED OPERATIONS:")
    print(f"   SMB assignments: {len(plan['smb_assignments'])}")
    print(f"   Midsize assignments: {len(plan['midsize_assignments'])}")
    print(f"   Deletions: {len(plan['deletions'])}")
    print(f"   Total operations: {plan['total_operations']}")
    
    if plan['smb_assignments']:
        print(f"\n🏢 SMB CAMPAIGN ASSIGNMENTS (Sample):")
        for i, assignment in enumerate(plan['smb_assignments'][:5]):
            revenue_str = f"${assignment['revenue']:,}" if assignment['revenue'] else "N/A"
            print(f"   {i+1}. {assignment['email']} - {assignment['company']} ({revenue_str})")
        if len(plan['smb_assignments']) > 5:
            print(f"   ... and {len(plan['smb_assignments']) - 5} more")
    
    if plan['midsize_assignments']:
        print(f"\n🏭 MIDSIZE CAMPAIGN ASSIGNMENTS (Sample):")
        for i, assignment in enumerate(plan['midsize_assignments'][:5]):
            revenue_str = f"${assignment['revenue']:,}" if assignment['revenue'] else "N/A"
            print(f"   {i+1}. {assignment['email']} - {assignment['company']} ({revenue_str})")
        if len(plan['midsize_assignments']) > 5:
            print(f"   ... and {len(plan['midsize_assignments']) - 5} more")
    
    if plan['deletions']:
        print(f"\n🗑️ PLANNED DELETIONS (Sample):")
        for i, deletion in enumerate(plan['deletions'][:5]):
            print(f"   {i+1}. {deletion['email']} - {deletion['reason']}")
        if len(plan['deletions']) > 5:
            print(f"   ... and {len(plan['deletions']) - 5} more")

def save_analysis_to_file(analysis: Dict, plan: Dict):
    """Save analysis and plan to files for record keeping"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save detailed analysis
    analysis_file = f"orphan_analysis_{timestamp}.json"
    with open(analysis_file, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'analysis': analysis,
            'plan': plan
        }, f, indent=2, default=str)
    
    print(f"\n💾 Analysis saved to: {analysis_file}")
    return analysis_file

def main():
    print("🕵️ ENHANCED ORPHANED LEADS ASSESSMENT")
    print("=" * 60)
    print("Phase 1: Detailed diagnosis with BigQuery integration")
    print("Phase 2: Enhanced classification strategy")
    print("\n⚠️ READ-ONLY MODE: No changes will be made to Instantly")
    
    # Phase 1: Get all orphaned leads
    orphaned_leads = get_all_orphaned_leads()
    
    if not orphaned_leads:
        print("✅ No orphaned leads found!")
        return
    
    # Phase 2: Analyze with BigQuery integration
    analysis = analyze_orphaned_leads(orphaned_leads)
    
    # Print comprehensive report
    print_analysis_report(analysis)
    
    # Generate assignment plan
    plan = generate_assignment_plan(analysis)
    print_assignment_plan(plan)
    
    # Save everything to file
    analysis_file = save_analysis_to_file(analysis, plan)
    
    print(f"\n✅ ASSESSMENT COMPLETE")
    print(f"\n💡 NEXT STEPS:")
    print(f"   1. Review the analysis above and the saved file: {analysis_file}")
    print(f"   2. Verify the assignment plan looks correct")
    print(f"   3. When ready, run the execution script to make actual changes")
    print(f"   4. Consider testing with a small batch first (--limit 10)")

if __name__ == "__main__":
    main()