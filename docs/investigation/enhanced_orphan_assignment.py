#!/usr/bin/env python3
"""
Enhanced Orphaned Lead Assignment Script - Phase 3

BigQuery-integrated assignment of orphaned leads using real revenue data.
Safe execution with dry-run mode and batch processing capabilities.

Usage:
    python enhanced_orphan_assignment.py --dry-run                    # Preview what would be done
    python enhanced_orphan_assignment.py --live --limit 50            # Process 50 leads
    python enhanced_orphan_assignment.py --live --delete-unmatched    # Delete leads not in BigQuery
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

def get_orphaned_leads_batch(limit=100, starting_after=None):
    """Get a batch of orphaned leads"""
    payload = {'limit': limit}
    if starting_after:
        payload['starting_after'] = starting_after
    
    response = call_api('/api/v2/leads/list', 'POST', payload)
    
    if not response['success']:
        return {'leads': [], 'next_starting_after': None, 'error': response['error']}
    
    all_leads = response['data'].get('items', [])
    orphaned_leads = []
    
    # Filter for leads without campaigns
    for lead in all_leads:
        campaign_id = lead.get('campaign_id') or lead.get('campaign')
        if not campaign_id:
            orphaned_leads.append(lead)
    
    next_starting_after = response['data'].get('next_starting_after')
    
    return {
        'leads': orphaned_leads,
        'next_starting_after': next_starting_after,
        'total_processed': len(all_leads),
        'error': None
    }

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
                'industry': getattr(row, 'industry', None)
            }
        
        print(f"   ✅ Found BigQuery data for {len(classification_data)}/{len(emails)} emails")
        return classification_data
        
    except Exception as e:
        print(f"   ❌ BigQuery query failed: {e}")
        return {}

def classify_lead_for_assignment(email: str, instantly_lead: Dict, bq_data: Dict) -> Dict:
    """Classify lead for campaign assignment using BigQuery data"""
    
    if email not in bq_data:
        return {
            'assignable': False,
            'action': 'delete',
            'reason': 'Not found in BigQuery - not qualified for campaigns',
            'campaign_id': None,
            'campaign_name': None,
            'confidence': 'high'
        }
    
    bq_lead = bq_data[email]
    annual_revenue = bq_lead.get('annual_revenue', 0)
    sequence_target = bq_lead.get('sequence_target')
    
    # Use BigQuery sequence_target as primary classification
    if sequence_target == 'SMB' or (annual_revenue and annual_revenue < 1000000):
        return {
            'assignable': True,
            'action': 'assign',
            'reason': f'Revenue: ${annual_revenue:,}' if annual_revenue else 'Sequence target: SMB',
            'campaign_id': SMB_CAMPAIGN_ID,
            'campaign_name': 'SMB',
            'confidence': 'high' if sequence_target else 'medium',
            'bq_data': bq_lead
        }
    elif sequence_target == 'Midsize' or (annual_revenue and annual_revenue >= 1000000):
        return {
            'assignable': True,
            'action': 'assign', 
            'reason': f'Revenue: ${annual_revenue:,}' if annual_revenue else 'Sequence target: Midsize',
            'campaign_id': MIDSIZE_CAMPAIGN_ID,
            'campaign_name': 'Midsize',
            'confidence': 'high' if sequence_target else 'medium',
            'bq_data': bq_lead
        }
    else:
        return {
            'assignable': False,
            'action': 'delete',
            'reason': f'Unclear classification - Revenue: ${annual_revenue:,}' if annual_revenue else 'No clear size indicator',
            'campaign_id': None,
            'campaign_name': None,
            'confidence': 'low'
        }

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

def update_bigquery_tracking(email: str, lead_id: str, campaign_id: str, action: str):
    """Update BigQuery ops_inst_state table to reflect the assignment"""
    if not bq_client:
        return
    
    try:
        now = datetime.now(timezone.utc)
        
        if action == 'assign':
            # Insert or update tracking record
            query = f"""
            MERGE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` AS target
            USING (
                SELECT @email as email, @lead_id as instantly_lead_id
            ) AS source
            ON target.email = source.email
            WHEN MATCHED THEN
                UPDATE SET
                    instantly_lead_id = @lead_id,
                    campaign_id = @campaign_id,
                    status = 'active',
                    updated_at = @now,
                    added_at = COALESCE(added_at, @now)
            WHEN NOT MATCHED THEN
                INSERT (email, instantly_lead_id, campaign_id, status, added_at, updated_at)
                VALUES (@email, @lead_id, @campaign_id, 'active', @now, @now)
            """
        else:  # delete
            # Mark as deleted
            query = f"""
            MERGE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` AS target
            USING (
                SELECT @email as email
            ) AS source
            ON target.email = source.email
            WHEN MATCHED THEN
                UPDATE SET
                    status = 'deleted',
                    updated_at = @now
            WHEN NOT MATCHED THEN
                INSERT (email, instantly_lead_id, campaign_id, status, added_at, updated_at)
                VALUES (@email, @lead_id, '', 'deleted', @now, @now)
            """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("lead_id", "STRING", lead_id),
                bigquery.ScalarQueryParameter("campaign_id", "STRING", campaign_id or ''),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now)
            ]
        )
        
        bq_client.query(query, job_config=job_config).result()
        
    except Exception as e:
        print(f"   ⚠️ BigQuery tracking update failed for {email}: {e}")

def process_orphaned_leads_batch(limit=50, delete_unmatched=False, dry_run=True):
    """Process a batch of orphaned leads"""
    print(f"\n🔍 PROCESSING ORPHANED LEADS (Limit: {limit})")
    print("=" * 60)
    
    results = {
        'total_processed': 0,
        'smb_assignments': 0,
        'midsize_assignments': 0, 
        'deletions': 0,
        'errors': 0,
        'skipped': 0
    }
    
    # Get batch of orphaned leads
    batch = get_orphaned_leads_batch(limit=min(100, limit * 2))  # Get extra to account for filtering, max 100
    
    if batch['error']:
        print(f"❌ Failed to get orphaned leads: {batch['error']}")
        return results
    
    orphaned_leads = batch['leads'][:limit]  # Limit to requested amount
    
    if not orphaned_leads:
        print("✅ No orphaned leads found")
        return results
    
    print(f"📊 Found {len(orphaned_leads)} orphaned leads to process")
    
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
                    
                    # Update BigQuery tracking
                    update_bigquery_tracking(email, lead_id, classification['campaign_id'], 'assign')
                else:
                    results['errors'] += 1
                
                # Rate limiting
                time.sleep(1)
        
        elif classification['action'] == 'delete' and delete_unmatched:
            if dry_run:
                print(f"   [DRY RUN] Would delete (not in BigQuery)")
                results['deletions'] += 1
            else:
                # Perform deletion
                delete_result = delete_orphaned_lead(lead, dry_run=False)
                if delete_result['success']:
                    results['deletions'] += 1
                    # Update BigQuery tracking
                    update_bigquery_tracking(email, lead_id, None, 'delete')
                else:
                    results['errors'] += 1
                
                # Rate limiting
                time.sleep(1)
        else:
            print(f"   ⏭️ Skipped (no action requested)")
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
        description='Enhanced Orphaned Lead Assignment with BigQuery Integration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Preview what would be done
    python enhanced_orphan_assignment.py --dry-run --limit 10
    
    # Assign 50 leads to campaigns (safe start)
    python enhanced_orphan_assignment.py --live --limit 50
    
    # Delete leads not in BigQuery (cleanup)
    python enhanced_orphan_assignment.py --live --delete-unmatched --limit 25
    
    # Process larger batch
    python enhanced_orphan_assignment.py --live --limit 100
        """
    )
    
    parser.add_argument('--dry-run', action='store_true', default=False,
                       help='Preview mode - show what would be done without making changes')
    parser.add_argument('--live', action='store_true',
                       help='Live mode - actually make changes in Instantly')
    parser.add_argument('--limit', type=int, default=50,
                       help='Maximum number of leads to process per run (default: 50)')
    parser.add_argument('--delete-unmatched', action='store_true',
                       help='Delete leads that are not found in BigQuery')
    
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
    
    print("🎯 ENHANCED ORPHANED LEAD ASSIGNMENT")
    print("=" * 60)
    print("Phase 3: BigQuery-integrated lead assignment\n")
    
    if dry_run:
        print("🚨 DRY RUN MODE - No changes will be made")
    else:
        print("⚠️ LIVE MODE - Changes will be made in Instantly")
    
    print(f"📊 Processing limit: {args.limit} leads")
    print(f"🗑️ Delete unmatched: {'Yes' if args.delete_unmatched else 'No'}")
    
    # Process the batch
    results = process_orphaned_leads_batch(
        limit=args.limit,
        delete_unmatched=args.delete_unmatched,
        dry_run=dry_run
    )
    
    # Print summary
    print_results_summary(results, dry_run)
    
    # Recommendations
    if dry_run and results['total_processed'] > 0:
        print(f"\n💡 NEXT STEPS:")
        assignable = results['smb_assignments'] + results['midsize_assignments']
        if assignable > 0:
            print(f"   1. Test with small batch: --live --limit 10")
            print(f"   2. Scale up gradually: --live --limit 50, 100, etc.")
        if results['deletions'] > 0 and args.delete_unmatched:
            print(f"   3. Clean up unmatched: --live --delete-unmatched --limit 25")
    
    return 0 if results['errors'] == 0 else 1

if __name__ == "__main__":
    sys.exit(main())