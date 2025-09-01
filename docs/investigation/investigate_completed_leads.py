#!/usr/bin/env python3
"""
Investigate "Completed" Leads - Why weren't they caught by regular drain?
Deep dive into the leads marked as "completed" to understand why they weren't processed by regular drain.
"""

import os
import sys
import logging
import json
from datetime import datetime
from typing import List, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Import the existing drain logic
try:
    from sync_once import (
        SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID, INSTANTLY_BASE_URL,
        get_instantly_headers, adaptive_rate_limiter, bq_client, PROJECT_ID, DATASET_ID
    )
    import requests
    IMPORTS_AVAILABLE = True
    logger.info("✅ Successfully imported from sync_once")
except ImportError as e:
    logger.error(f"❌ Failed to import: {e}")
    IMPORTS_AVAILABLE = False
    sys.exit(1)

def get_sample_completed_leads(campaign_id: str, campaign_name: str, limit: int = 20) -> List[dict]:
    """Get a sample of leads with Status 3 for detailed analysis."""
    logger.info(f"🔍 Getting sample of Status 3 leads from {campaign_name} campaign...")
    
    completed_leads = []
    starting_after = None
    page_count = 0
    
    while len(completed_leads) < limit and page_count < 10:  # Max 10 pages to find samples
        try:
            url = f"{INSTANTLY_BASE_URL}/api/v2/leads/list"
            payload = {
                "campaign_id": campaign_id,
                "limit": 50
            }
            
            if starting_after:
                payload["starting_after"] = starting_after
            
            if page_count > 0:
                adaptive_rate_limiter.wait()
            
            response = requests.post(
                url,
                headers=get_instantly_headers(),
                json=payload,
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"❌ API error {response.status_code}")
                break
                
            data = response.json()
            leads = data.get('items', [])
            
            if not leads:
                break
            
            page_count += 1
            
            # Find Status 3 leads
            for lead in leads:
                if lead.get('status') == 3:  # Status 3 = Finished/Completed
                    completed_leads.append(lead)
                    if len(completed_leads) >= limit:
                        break
            
            logger.info(f"📄 Page {page_count}: Found {len([l for l in leads if l.get('status') == 3])} Status 3 leads ({len(completed_leads)} total collected)")
            
            starting_after = data.get('next_starting_after')
            if not starting_after:
                break
                
        except Exception as e:
            logger.error(f"❌ Error getting completed leads: {e}")
            break
    
    logger.info(f"✅ Collected {len(completed_leads)} Status 3 leads for analysis")
    return completed_leads

def analyze_completed_lead(lead: dict, campaign_name: str) -> Dict:
    """Deep analysis of a single completed lead."""
    email = lead.get('email', 'unknown')
    lead_id = lead.get('id', 'unknown')
    
    analysis = {
        'email': email,
        'lead_id': lead_id,
        'campaign': campaign_name,
        'basic_fields': {
            'status': lead.get('status'),
            'esp_code': lead.get('esp_code'),
            'email_reply_count': lead.get('email_reply_count', 0),
            'timestamp_created': lead.get('timestamp_created'),
            'updated_at': lead.get('updated_at'),
            'status_text': lead.get('status_text', ''),
        },
        'payload_info': {},
        'diagnostic_flags': [],
        'bigquery_status': None
    }
    
    # Analyze payload for additional info
    payload = lead.get('payload', {})
    if payload:
        analysis['payload_info'] = {
            'pause_until': payload.get('pause_until'),
            'bounce_reason': payload.get('bounce_reason'),
            'last_email_sent_at': payload.get('last_email_sent_at'),
            'emails_sent': payload.get('emails_sent', 0),
            'sequence_step': payload.get('sequence_step', 0),
        }
    
    # Check diagnostic flags
    esp_code = lead.get('esp_code', 0)
    if esp_code != 0:
        analysis['diagnostic_flags'].append(f"ESP_CODE_{esp_code}")
    
    emails_sent = analysis['payload_info'].get('emails_sent', 0)
    if emails_sent == 0:
        analysis['diagnostic_flags'].append("ZERO_EMAILS_SENT")
    
    last_sent = analysis['payload_info'].get('last_email_sent_at')
    if not last_sent:
        analysis['diagnostic_flags'].append("NO_LAST_EMAIL_TIMESTAMP")
    
    # Calculate days since creation
    created_at = lead.get('timestamp_created')
    if created_at:
        try:
            created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            days_since_created = (datetime.now().astimezone() - created_date).days
            analysis['days_since_created'] = days_since_created
        except:
            analysis['days_since_created'] = 'unknown'
    
    return analysis

def check_bigquery_status(lead_ids: List[str]) -> Dict[str, dict]:
    """Check what BigQuery knows about these leads."""
    if not lead_ids:
        return {}
    
    logger.info(f"📊 Checking BigQuery status for {len(lead_ids)} leads...")
    
    try:
        # Query BigQuery for these specific leads
        query = f"""
        SELECT 
            instantly_lead_id,
            status,
            last_drain_check,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) as hours_since_last_check,
            added_at,
            updated_at,
            verification_status,
            email
        FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
        WHERE instantly_lead_id IN UNNEST(@lead_ids)
        """
        
        from google.cloud import bigquery
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("lead_ids", "STRING", lead_ids),
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job.result(timeout=60))
        
        bq_status = {}
        for row in results:
            bq_status[row.instantly_lead_id] = {
                'bq_status': row.status,
                'last_drain_check': str(row.last_drain_check) if row.last_drain_check else None,
                'hours_since_last_check': row.hours_since_last_check,
                'added_at': str(row.added_at) if row.added_at else None,
                'updated_at': str(row.updated_at) if row.updated_at else None,
                'verification_status': row.verification_status,
                'bq_email': row.email
            }
        
        logger.info(f"📊 Found {len(bq_status)} leads in BigQuery")
        return bq_status
        
    except Exception as e:
        logger.error(f"❌ Error checking BigQuery: {e}")
        return {}

def main():
    """Investigate completed leads to understand why they weren't drained."""
    logger.info("🕵️ INVESTIGATING COMPLETED LEADS")
    logger.info("=" * 60)
    logger.info("Why weren't these Status 3 leads caught by regular drain?")
    logger.info("=" * 60)
    
    if not IMPORTS_AVAILABLE:
        logger.error("❌ Required imports not available")
        return
    
    campaigns_to_investigate = [
        (SMB_CAMPAIGN_ID, "SMB"),
        (MIDSIZE_CAMPAIGN_ID, "Midsize")
    ]
    
    all_analyses = []
    
    # Get samples from each campaign
    for campaign_id, campaign_name in campaigns_to_investigate:
        logger.info(f"🔍 Investigating {campaign_name} campaign...")
        
        # Get sample of completed leads
        sample_leads = get_sample_completed_leads(campaign_id, campaign_name, limit=15)
        
        if not sample_leads:
            logger.warning(f"⚠️ No Status 3 leads found in {campaign_name} campaign")
            continue
        
        # Analyze each lead
        campaign_analyses = []
        for lead in sample_leads:
            analysis = analyze_completed_lead(lead, campaign_name)
            campaign_analyses.append(analysis)
        
        # Check BigQuery status for this batch
        lead_ids = [analysis['lead_id'] for analysis in campaign_analyses]
        bq_status = check_bigquery_status(lead_ids)
        
        # Merge BigQuery info
        for analysis in campaign_analyses:
            lead_id = analysis['lead_id']
            if lead_id in bq_status:
                analysis['bigquery_status'] = bq_status[lead_id]
        
        all_analyses.extend(campaign_analyses)
        
        # Quick summary for this campaign
        logger.info(f"📋 {campaign_name.upper()} COMPLETED LEADS ANALYSIS:")
        
        # Count diagnostic patterns
        patterns = {}
        for analysis in campaign_analyses:
            flags = analysis['diagnostic_flags']
            if not flags:
                flags = ['NO_FLAGS']
            
            pattern_key = ' + '.join(sorted(flags))
            patterns[pattern_key] = patterns.get(pattern_key, 0) + 1
        
        logger.info(f"  • Sample size: {len(campaign_analyses)} leads")
        logger.info(f"  • Common patterns:")
        for pattern, count in sorted(patterns.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"    - {pattern}: {count} leads")
        
        logger.info("")
    
    # Overall analysis
    logger.info("🕵️ DETAILED INVESTIGATION RESULTS:")
    logger.info("=" * 60)
    
    # Pattern analysis across all campaigns
    all_patterns = {}
    never_drained_count = 0
    recently_drained_count = 0
    zero_emails_sent = 0
    invalid_email_indicators = 0
    
    for analysis in all_analyses:
        email = analysis['email']
        flags = analysis['diagnostic_flags']
        bq_info = analysis.get('bigquery_status', {})
        
        # Count specific patterns
        if 'ZERO_EMAILS_SENT' in flags:
            zero_emails_sent += 1
        
        if analysis['basic_fields']['esp_code'] in [550, 551, 553]:  # Hard bounce codes
            invalid_email_indicators += 1
        
        # Check drain history
        last_drain_check = bq_info.get('last_drain_check') if bq_info else None
        if not last_drain_check or last_drain_check == 'None':
            never_drained_count += 1
        else:
            hours_since = bq_info.get('hours_since_last_check', 0) if bq_info else 999
            if hours_since < 24:
                recently_drained_count += 1
        
        # Overall pattern
        pattern_key = ' + '.join(sorted(flags)) if flags else 'NO_FLAGS'
        all_patterns[pattern_key] = all_patterns.get(pattern_key, 0) + 1
        
        # Show detailed info for first few examples
        if len([a for a in all_analyses if a['email'] == email]) <= 5:  # First 5 unique emails
            logger.info(f"📧 EXAMPLE: {email}")
            logger.info(f"   Status: {analysis['basic_fields']['status']} | ESP: {analysis['basic_fields']['esp_code']}")
            logger.info(f"   Emails sent: {analysis['payload_info'].get('emails_sent', 'unknown')}")
            logger.info(f"   Created: {analysis.get('days_since_created', 'unknown')} days ago")
            logger.info(f"   Last drain check: {bq_info.get('last_drain_check', 'NEVER') if bq_info else 'NOT_IN_BQ'}")
            logger.info(f"   Flags: {' | '.join(flags) if flags else 'None'}")
            logger.info("")
    
    # Summary statistics
    total_analyzed = len(all_analyses)
    logger.info("📊 KEY FINDINGS:")
    logger.info(f"  • Total analyzed: {total_analyzed} completed leads")
    logger.info(f"  • Never been drain-checked: {never_drained_count} ({never_drained_count/max(total_analyzed,1)*100:.1f}%)")
    logger.info(f"  • Recently drain-checked (<24h): {recently_drained_count} ({recently_drained_count/max(total_analyzed,1)*100:.1f}%)")
    logger.info(f"  • Zero emails actually sent: {zero_emails_sent} ({zero_emails_sent/max(total_analyzed,1)*100:.1f}%)")
    logger.info(f"  • Hard bounce ESP codes: {invalid_email_indicators} ({invalid_email_indicators/max(total_analyzed,1)*100:.1f}%)")
    
    logger.info("\n📋 COMMON PATTERNS:")
    for pattern, count in sorted(all_patterns.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / max(total_analyzed, 1)) * 100
        logger.info(f"  • {pattern}: {count} ({percentage:.1f}%)")
    
    # Root cause hypothesis
    logger.info("\n🎯 LIKELY ROOT CAUSES:")
    if never_drained_count > total_analyzed * 0.8:
        logger.info(f"  ✅ PRIMARY: Most leads ({never_drained_count}) were NEVER drain-checked")
        logger.info(f"     → Regular drain uses 24h time filter, these leads were added but never evaluated")
    
    if zero_emails_sent > total_analyzed * 0.5:
        logger.info(f"  ✅ EMAIL ISSUE: {zero_emails_sent} leads had zero emails sent")
        logger.info(f"     → These are likely invalid emails that failed immediately")
    
    if invalid_email_indicators > 0:
        logger.info(f"  ✅ BOUNCE CODES: {invalid_email_indicators} leads have hard bounce ESP codes")
        logger.info(f"     → These should have been cleaned up as bounces")
    
    logger.info("\n💡 RECOMMENDATION:")
    logger.info("  The 'completed' leads are likely invalid emails that:")
    logger.info("  1. Were added to campaigns but never had emails successfully sent")
    logger.info("  2. Were marked as Status 3 (completed) due to immediate failures")
    logger.info("  3. Never went through drain evaluation due to 24h time filtering")
    logger.info("  4. Should be cleaned up as they're taking up inventory space")
    
    logger.info("🏁 Investigation complete")

if __name__ == "__main__":
    main()