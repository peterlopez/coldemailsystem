#!/usr/bin/env python3
"""
Batch Orphaned Lead Cleanup Script

Efficiently processes orphaned leads across all pages of the Instantly API.
Uses the same BigQuery-integrated classification as enhanced_orphan_assignment.py
but with improved pagination handling.

Usage:
    python batch_orphan_cleanup.py --dry-run --limit 50      # Preview mode
    python batch_orphan_cleanup.py --live --limit 100        # Delete mode
"""

import os
import sys
import requests
import json
import time
import argparse
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
TARGET_CAMPAIGNS = {SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID}

# Initialize BigQuery client
try:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
    bq_client = bigquery.Client(project=PROJECT_ID)
except Exception as e:
    print(f"❌ Failed to initialize BigQuery client: {e}")
    bq_client = None
    sys.exit(1)

def call_api(endpoint, method='GET', data=None):
    """Make API call with comprehensive error handling"""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    
    try:
        if method == 'POST':
            response = requests.post(url, headers=HEADERS, json=data, timeout=60)
        elif method == 'DELETE':
            # For DELETE requests, don't send JSON body unless data is provided
            if data:
                response = requests.delete(url, headers=HEADERS, json=data, timeout=60)
            else:
                # Remove content-type header for empty DELETE requests
                delete_headers = {k: v for k, v in HEADERS.items() if k != 'Content-Type'}
                response = requests.delete(url, headers=delete_headers, timeout=60)
        else:
            response = requests.get(url, headers=HEADERS, timeout=60)
        
        return {
            'success': response.status_code == 200,
            'status_code': response.status_code,
            'data': response.json() if response.status_code == 200 else None,
            'error': response.text if response.status_code != 200 else None
        }
    except Exception as e:
        return {
            'success': False,
            'status_code': 0,
            'data': None,
            'error': str(e)
        }

def find_orphaned_leads_across_pages(target_count, max_pages=50):
    """Find orphaned leads by scanning across multiple API pages"""
    print(f"🔍 Scanning for {target_count} orphaned leads across multiple pages...")
    
    orphaned_leads = []
    starting_after = None
    page = 0
    total_scanned = 0
    
    while len(orphaned_leads) < target_count and page < max_pages:
        payload = {'limit': 100}
        if starting_after:
            payload['starting_after'] = starting_after
        
        response = call_api('/api/v2/leads/list', 'POST', payload)
        
        if not response['success']:
            print(f"   ❌ API Error on page {page + 1}: {response['error']}")
            break
        
        leads = response['data'].get('items', [])
        if not leads:
            print(f"   ✅ No more leads found after scanning {total_scanned} leads")
            break
        
        total_scanned += len(leads)
        page_orphans = 0
        
        # Filter for orphaned leads on this page
        for lead in leads:
            if len(orphaned_leads) >= target_count:
                break
                
            campaign_id = lead.get('campaign_id') or lead.get('campaign')
            if not campaign_id or campaign_id not in TARGET_CAMPAIGNS:
                orphaned_leads.append(lead)
                page_orphans += 1
        
        page += 1
        
        if page_orphans > 0:
            print(f"   📄 Page {page}: Found {page_orphans} orphans (total: {len(orphaned_leads)}/{target_count})")
        elif page % 10 == 0:
            print(f"   📄 Page {page}: Scanned {total_scanned} leads, found {len(orphaned_leads)} orphans")
        
        starting_after = response['data'].get('next_starting_after')
        if not starting_after:
            print(f"   ✅ Reached end of leads after scanning {total_scanned} total leads")
            break
        
        time.sleep(0.3)  # Rate limiting
    
    if page >= max_pages:
        print(f"   ⚠️ Hit page limit ({max_pages}) - there may be more orphaned leads")
    
    print(f"✅ Found {len(orphaned_leads)} orphaned leads after scanning {total_scanned} total leads")
    return orphaned_leads

def get_bigquery_classification_data(emails: List[str]) -> Dict[str, Dict]:
    """Get classification data from BigQuery for the emails"""
    if not emails:
        return {}
    
    print(f"   📊 Querying BigQuery for {len(emails)} emails...")
    
    # Convert emails to SQL-safe format
    email_list = "', '".join([email.replace("'", "''") for email in emails])  # Escape single quotes
    
    query = f"""
    SELECT 
        email,
        merchant_name as company_name,
        estimated_sales_yearly as annual_revenue,
        sequence_target,
        platform_domain as domain,
        country_code as country,
        state as location,
        -- Additional fields for better classification
        employee_count as employees,
        product_count,
        avg_price
    FROM `{PROJECT_ID}.{DATASET_ID}.v_ready_for_instantly`
    WHERE email IN ('{email_list}')
    """
    
    try:
        results = bq_client.query(query).result()
        
        classification_data = {}
        for row in results:
            classification_data[row.email] = {
                'company_name': row.company_name,
                'annual_revenue': row.annual_revenue,
                'sequence_target': row.sequence_target,
                'domain': row.domain,
                'country': row.country,
                'location': row.location,
                'employees': getattr(row, 'employees', None),
                'product_count': getattr(row, 'product_count', None),
                'avg_price': getattr(row, 'avg_price', None)
            }
        
        print(f"   ✅ Found BigQuery data for {len(classification_data)}/{len(emails)} emails")
        return classification_data
        
    except Exception as e:
        print(f"   ❌ BigQuery query failed: {e}")
        return {}

def classify_lead_for_assignment(email: str, instantly_lead: Dict, bq_data: Dict) -> Dict:
    """Classify lead for campaign assignment - assign ALL orphaned leads to SMB campaign"""
    
    # Simple approach: assign ALL orphaned leads to SMB campaign
    company = instantly_lead.get('company_name', 'Unknown Company')
    
    return {
        'assignable': True,
        'action': 'assign',
        'reason': f'Orphaned lead from {company} - assigning to SMB campaign',
        'campaign_id': SMB_CAMPAIGN_ID,
        'campaign_name': 'SMB',
        'confidence': 'high'
    }

def delete_orphaned_lead(lead: Dict, dry_run: bool = True) -> Dict:
    """Delete an orphaned lead that can't be assigned"""
    lead_id = lead.get('id')
    email = lead.get('email')
    
    if not lead_id:
        return {'success': False, 'error': 'No lead ID'}
    
    if dry_run:
        return {'success': True, 'dry_run': True}
    
    response = call_api(f'/api/v2/leads/{lead_id}', 'DELETE')
    
    if response['success']:
        print(f"   ✅ Deleted {email}")
        return {'success': True}
    else:
        print(f"   ❌ Failed to delete {email}: {response['error']}")
        return {'success': False, 'error': response['error']}

def assign_lead_to_campaign(lead: Dict, campaign_id: str, dry_run: bool = True) -> Dict:
    """Assign a lead to a specific campaign"""
    lead_id = lead.get('id')
    email = lead.get('email')
    
    if not lead_id:
        return {'success': False, 'error': 'No lead ID'}
    
    if dry_run:
        return {'success': True, 'dry_run': True}
    
    # Use the move leads API
    move_data = {
        'ids': [lead_id],
        'to_campaign_id': campaign_id
    }
    
    response = call_api('/api/v2/leads/move', 'POST', move_data)
    
    if response['success']:
        print(f"   ✅ Assigned {email} to campaign")
        return {'success': True, 'response': response['data']}
    else:
        print(f"   ❌ Failed to assign {email}: {response['error']}")
        return {'success': False, 'error': response['error']}

def process_orphaned_leads_batch(orphaned_leads, dry_run=True):
    """Process a batch of orphaned leads"""
    print(f"\n🔍 PROCESSING {len(orphaned_leads)} ORPHANED LEADS")
    print("=" * 60)
    
    results = {
        'total_processed': 0,
        'smb_assignments': 0,
        'midsize_assignments': 0, 
        'deletions': 0,
        'errors': 0,
        'skipped': 0
    }
    
    if not orphaned_leads:
        print("✅ No orphaned leads to process")
        return results
    
    # Get BigQuery data for classification
    emails = [lead.get('email') for lead in orphaned_leads if lead.get('email')]
    bq_data = get_bigquery_classification_data(emails)
    
    # Process each lead
    for i, lead in enumerate(orphaned_leads):
        email = lead.get('email', 'unknown')
        lead_id = lead.get('id')
        company = lead.get('company_name', 'Unknown')
        
        print(f"\n📧 {i+1}/{len(orphaned_leads)}: {email}")
        print(f"   Company: {company}")
        
        # Classify the lead
        classification = classify_lead_for_assignment(email, lead, bq_data)
        print(f"   Classification: {classification['action']} - {classification['reason']}")
        
        results['total_processed'] += 1
        
        # Take action based on classification
        if classification['action'] == 'assign':
            if dry_run:
                print(f"   [DRY RUN] Would assign to {classification['campaign_name']} campaign")
                if classification['campaign_name'] == 'SMB':
                    results['smb_assignments'] += 1
                else:
                    results['midsize_assignments'] += 1
            else:
                # Perform assignment
                assign_result = assign_lead_to_campaign(lead, classification['campaign_id'], dry_run=False)
                if assign_result['success']:
                    if classification['campaign_name'] == 'SMB':
                        results['smb_assignments'] += 1
                    else:
                        results['midsize_assignments'] += 1
                else:
                    results['errors'] += 1
                
                # Rate limiting
                time.sleep(1)
        
        elif classification['action'] == 'delete':
            # This shouldn't happen anymore since we assign all leads to SMB
            print(f"   ⚠️ Unexpected delete action - skipping")
            results['skipped'] += 1
        else:
            print(f"   ⏭️ Skipped (no action)")
            results['skipped'] += 1
    
    return results

def print_results_summary(results: Dict, dry_run: bool):
    """Print summary of processing results"""
    print(f"\n📊 PROCESSING SUMMARY")
    print("=" * 30)
    print(f"   Total processed: {results['total_processed']}")
    print(f"   SMB assignments: {results['smb_assignments']}")
    print(f"   Midsize assignments: {results['midsize_assignments']}")
    print(f"   Deletions: {results['deletions']}")
    print(f"   Skipped: {results['skipped']}")
    print(f"   Errors: {results['errors']}")
    
    total_actions = results['smb_assignments'] + results['midsize_assignments'] + results['deletions']
    
    if dry_run:
        print(f"\n💡 DRY RUN COMPLETE")
        print(f"   To execute these changes, run with --live")
        if total_actions > 0:
            print(f"   {total_actions} leads would be processed")
    else:
        print(f"\n✅ LIVE EXECUTION COMPLETE")
        if total_actions > 0:
            print(f"   {total_actions} leads successfully processed")
        if results['errors'] > 0:
            print(f"   ⚠️ {results['errors']} errors occurred")

def main():
    parser = argparse.ArgumentParser(
        description='Batch Orphaned Lead Cleanup with Multi-Page Scanning',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Preview 25 orphaned leads
    python batch_orphan_cleanup.py --dry-run --limit 25
    
    # Delete 100 unqualified orphaned leads
    python batch_orphan_cleanup.py --live --limit 100
    
    # Process up to 200 orphaned leads
    python batch_orphan_cleanup.py --live --limit 200
        """
    )
    
    parser.add_argument('--dry-run', action='store_true', default=False,
                       help='Preview mode - show what would be done without making changes')
    parser.add_argument('--live', action='store_true',
                       help='Live mode - actually make changes in Instantly')
    parser.add_argument('--limit', type=int, default=50,
                       help='Maximum number of orphaned leads to process (default: 50)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.live and args.dry_run:
        print("❌ Cannot use both --live and --dry-run")
        sys.exit(1)
    
    # Default to dry run if neither specified
    if not args.live and not args.dry_run:
        dry_run = True  # Default to dry run if no mode specified
    else:
        dry_run = args.dry_run
    
    print("🎯 BATCH ORPHANED LEAD CLEANUP")
    print("=" * 50)
    print("Multi-page scanning with BigQuery integration\n")
    
    if dry_run:
        print("🚨 DRY RUN MODE - No changes will be made")
    else:
        print("⚠️ LIVE MODE - Changes will be made in Instantly")
    
    print(f"📊 Target orphaned leads: {args.limit}")
    
    # Step 1: Find orphaned leads across multiple pages
    orphaned_leads = find_orphaned_leads_across_pages(args.limit)
    
    if not orphaned_leads:
        print("✅ No orphaned leads found!")
        return 0
    
    # Step 2: Process the batch
    results = process_orphaned_leads_batch(orphaned_leads, dry_run)
    
    # Step 3: Print summary
    print_results_summary(results, dry_run)
    
    # Recommendations
    if dry_run and results['total_processed'] > 0:
        print(f"\n💡 NEXT STEPS:")
        assignable = results['smb_assignments'] + results['midsize_assignments']
        if assignable > 0:
            print(f"   1. Test with small batch: --live --limit 10")
            print(f"   2. Scale up gradually: --live --limit 25, 50, 100")
        if results['deletions'] > 0:
            print(f"   3. Clean up unqualified: --live --limit {min(100, results['deletions'])}")
    
    return 0 if results['errors'] == 0 else 1

if __name__ == "__main__":
    sys.exit(main())