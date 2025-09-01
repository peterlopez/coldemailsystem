#!/usr/bin/env python3
"""
One-Time Drain Audit Script
Comprehensive scan of ALL leads in Instantly campaigns to identify cleanup candidates.
Uses the same drain logic but bypasses 24-hour time filtering for complete visibility.
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('one_time_drain_audit.log')
    ]
)
logger = logging.getLogger(__name__)

# Import the existing drain logic
try:
    from sync_once import (
        get_finished_leads, classify_lead_for_drain, InstantlyLead,
        SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID, INSTANTLY_BASE_URL,
        get_instantly_headers, adaptive_rate_limiter
    )
    import requests
    IMPORTS_AVAILABLE = True
    logger.info("✅ Successfully imported drain logic from sync_once")
except ImportError as e:
    logger.error(f"❌ Failed to import drain logic: {e}")
    IMPORTS_AVAILABLE = False
    sys.exit(1)

def scan_all_leads_in_campaign(campaign_id: str, campaign_name: str) -> List[dict]:
    """
    Comprehensive scan of ALL leads in a campaign, bypassing all time limitations.
    
    Returns:
        List of all lead dictionaries from Instantly API
    """
    logger.info(f"🔍 COMPREHENSIVE SCAN: Starting full scan of {campaign_name} campaign...")
    
    all_leads = []
    starting_after = None
    page_count = 0
    seen_lead_ids = set()
    consecutive_empty_pages = 0
    
    while True:
        try:
            url = f"{INSTANTLY_BASE_URL}/api/v2/leads/list"
            payload = {
                "campaign_id": campaign_id,
                "limit": 50
            }
            
            if starting_after:
                payload["starting_after"] = starting_after
            
            # Rate limiting
            if page_count > 0:
                adaptive_rate_limiter.wait()
            
            response = requests.post(
                url,
                headers=get_instantly_headers(),
                json=payload,
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"❌ API error {response.status_code} for {campaign_name}: {response.text}")
                break
                
            data = response.json()
            leads = data.get('items', [])
            
            if not leads:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    logger.info(f"📄 No more leads found for {campaign_name} after {page_count} pages")
                    break
                continue
            else:
                consecutive_empty_pages = 0
            
            page_count += 1
            
            # Deduplication check
            page_lead_ids = {lead.get('id') for lead in leads if lead.get('id')}
            new_leads = [lead for lead in leads if lead.get('id') not in seen_lead_ids]
            
            if not new_leads:
                logger.warning(f"⚠️ Page {page_count} contained only duplicate leads")
                if page_count > 100:  # Safety limit for duplicates
                    logger.error(f"❌ Too many duplicate pages, stopping scan")
                    break
            else:
                seen_lead_ids.update(page_lead_ids)
                all_leads.extend(new_leads)
                
                logger.info(f"📄 Page {page_count}: {len(new_leads)} new leads ({len(all_leads)} total)")
            
            # Get cursor for next page
            starting_after = data.get('next_starting_after')
            if not starting_after:
                logger.info(f"✅ Reached natural end of {campaign_name} campaign after {page_count} pages")
                break
            
            # Safety limit to prevent infinite loops - but allow more for comprehensive audit
            if page_count >= 100:
                logger.warning(f"⚠️ Reached safety limit of 100 pages for {campaign_name} ({len(all_leads)} leads found)")
                break
                
        except Exception as e:
            logger.error(f"❌ Error scanning {campaign_name} page {page_count + 1}: {e}")
            break
    
    logger.info(f"🏁 {campaign_name} scan complete: {len(all_leads)} total leads found in {page_count} pages")
    return all_leads

def audit_campaign_for_cleanup(campaign_id: str, campaign_name: str) -> Dict:
    """
    Audit a single campaign for cleanup candidates using existing drain logic.
    
    Returns:
        Dictionary with audit results
    """
    logger.info(f"🔍 AUDIT: Starting comprehensive audit of {campaign_name} campaign...")
    
    # Get all leads in campaign
    all_leads = scan_all_leads_in_campaign(campaign_id, campaign_name)
    
    if not all_leads:
        return {
            'campaign_name': campaign_name,
            'total_leads': 0,
            'leads_to_drain': [],
            'classification_breakdown': {},
            'error': 'No leads found'
        }
    
    # Classify each lead using existing logic
    leads_to_drain = []
    classification_breakdown = {
        'replied': 0,
        'completed': 0,
        'bounced_hard': 0,
        'unsubscribed': 0,
        'stale_active': 0,
        'auto_reply_detected': 0,
        'kept_active': 0,
        'kept_paused': 0,
        'kept_other': 0
    }
    
    lead_details = []  # Store detailed info for reporting
    
    for lead in all_leads:
        try:
            lead_id = lead.get('id', '')
            email = lead.get('email', '')
            
            if not lead_id or not email:
                logger.debug(f"⚠️ Skipping lead with missing ID or email")
                continue
            
            # Use existing classification logic
            classification = classify_lead_for_drain(lead, campaign_name)
            
            lead_info = {
                'id': lead_id,
                'email': email,
                'status': lead.get('status', 0),
                'classification': classification
            }
            
            if classification['should_drain']:
                drain_reason = classification.get('drain_reason', 'unknown')
                classification_breakdown[drain_reason] += 1
                
                instantly_lead = InstantlyLead(
                    id=lead_id,
                    email=email,
                    campaign_id=campaign_id,
                    status=drain_reason
                )
                leads_to_drain.append(instantly_lead)
                
                details = classification.get('details', '')
                logger.info(f"🗑️ CLEANUP CANDIDATE: {email} → {drain_reason} | {details}")
                
                lead_info['action'] = 'DRAIN'
                lead_info['drain_reason'] = drain_reason
                lead_info['details'] = details
            else:
                keep_reason = str(classification.get('keep_reason', 'unknown reason'))
                status = lead.get('status', 0)
                
                is_auto_reply = ('auto-reply' in keep_reason.lower() if isinstance(keep_reason, str) else False) or \
                               classification.get('auto_reply', False) == True
                
                if is_auto_reply:
                    classification_breakdown['auto_reply_detected'] += 1
                    keep_category = 'auto_reply_detected'
                elif status == 1:
                    classification_breakdown['kept_active'] += 1
                    keep_category = 'kept_active'
                elif status == 2:
                    classification_breakdown['kept_paused'] += 1
                    keep_category = 'kept_paused'
                else:
                    classification_breakdown['kept_other'] += 1
                    keep_category = 'kept_other'
                
                logger.debug(f"✅ KEEP: {email} → {keep_category} | {keep_reason}")
                
                lead_info['action'] = 'KEEP'
                lead_info['keep_reason'] = keep_reason
                lead_info['keep_category'] = keep_category
            
            lead_details.append(lead_info)
            
        except Exception as e:
            logger.error(f"❌ Error classifying lead {lead.get('email', 'unknown')}: {e}")
            continue
    
    return {
        'campaign_name': campaign_name,
        'campaign_id': campaign_id,
        'total_leads': len(all_leads),
        'leads_to_drain': leads_to_drain,
        'classification_breakdown': classification_breakdown,
        'lead_details': lead_details
    }

def main():
    """Run comprehensive drain audit on all campaigns."""
    logger.info("🧹 STARTING COMPREHENSIVE DRAIN AUDIT")
    logger.info("=" * 80)
    logger.info(f"⚠️  AUDIT MODE: This script will identify cleanup candidates but NOT delete anything")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    if not IMPORTS_AVAILABLE:
        logger.error("❌ Required imports not available, cannot proceed")
        return
    
    campaigns_to_audit = [
        (SMB_CAMPAIGN_ID, "SMB"),
        (MIDSIZE_CAMPAIGN_ID, "Midsize")
    ]
    
    audit_results = []
    total_cleanup_candidates = 0
    
    # Audit each campaign
    for campaign_id, campaign_name in campaigns_to_audit:
        try:
            result = audit_campaign_for_cleanup(campaign_id, campaign_name)
            audit_results.append(result)
            total_cleanup_candidates += len(result.get('leads_to_drain', []))
            
        except Exception as e:
            logger.error(f"❌ Failed to audit {campaign_name} campaign: {e}")
            continue
    
    # Generate comprehensive report
    logger.info("=" * 80)
    logger.info("📊 COMPREHENSIVE DRAIN AUDIT RESULTS")
    logger.info("=" * 80)
    
    grand_total_leads = 0
    grand_total_candidates = 0
    
    for result in audit_results:
        campaign_name = result['campaign_name']
        total_leads = result['total_leads']
        leads_to_drain = result.get('leads_to_drain', [])
        classification = result.get('classification_breakdown', {})
        
        grand_total_leads += total_leads
        grand_total_candidates += len(leads_to_drain)
        
        logger.info(f"📋 {campaign_name.upper()} CAMPAIGN AUDIT:")
        logger.info(f"  • Total leads in campaign: {total_leads}")
        logger.info(f"  • Cleanup candidates identified: {len(leads_to_drain)}")
        if total_leads > 0:
            cleanup_rate = (len(leads_to_drain) / total_leads) * 100
            logger.info(f"  • Cleanup rate: {cleanup_rate:.1f}%")
        
        # Detailed breakdown
        if classification:
            logger.info(f"  • Classification breakdown:")
            for reason, count in sorted(classification.items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    percentage = (count / max(total_leads, 1)) * 100
                    logger.info(f"    - {reason}: {count} ({percentage:.1f}%)")
        
        # List cleanup candidates
        if leads_to_drain:
            logger.info(f"  • Cleanup candidates:")
            for lead in leads_to_drain[:10]:  # Show first 10
                logger.info(f"    - {lead.email} → {lead.status}")
            if len(leads_to_drain) > 10:
                logger.info(f"    - ... and {len(leads_to_drain) - 10} more")
        
        logger.info("")
    
    # Grand summary
    logger.info("🎯 GRAND SUMMARY:")
    logger.info(f"  • Total leads across all campaigns: {grand_total_leads}")
    logger.info(f"  • Total cleanup candidates identified: {grand_total_candidates}")
    if grand_total_leads > 0:
        overall_cleanup_rate = (grand_total_candidates / grand_total_leads) * 100
        logger.info(f"  • Overall cleanup rate: {overall_cleanup_rate:.1f}%")
    
    logger.info("=" * 80)
    
    # Save detailed results to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"drain_audit_results_{timestamp}.json"
    
    try:
        import json
        with open(results_file, 'w') as f:
            # Convert InstantlyLead objects to dict for JSON serialization
            serializable_results = []
            for result in audit_results:
                serializable_result = result.copy()
                serializable_result['leads_to_drain'] = [
                    {
                        'id': lead.id,
                        'email': lead.email,
                        'campaign_id': lead.campaign_id,
                        'status': lead.status
                    }
                    for lead in result.get('leads_to_drain', [])
                ]
                serializable_results.append(serializable_result)
            
            json.dump(serializable_results, f, indent=2)
        
        logger.info(f"💾 Detailed results saved to: {results_file}")
    except Exception as e:
        logger.warning(f"⚠️ Could not save detailed results: {e}")
    
    # Final recommendation
    if grand_total_candidates > 0:
        logger.info("🎯 RECOMMENDATION:")
        logger.info(f"   Found {grand_total_candidates} leads that should be cleaned up from Instantly")
        logger.info(f"   Review the detailed breakdown above and the saved file: {results_file}")
        logger.info(f"   If the classifications look correct, these leads can be safely removed")
    else:
        logger.info("✅ RESULT: No cleanup candidates found - all leads are properly classified")
    
    logger.info("🏁 Comprehensive drain audit complete")
    return audit_results

if __name__ == "__main__":
    main()