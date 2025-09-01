#!/usr/bin/env python3
"""
Quick Audit Sample Script
Fast sampling approach to estimate cleanup candidates by analyzing a representative sample.
"""

import os
import sys
import logging
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
        classify_lead_for_drain, InstantlyLead,
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

def quick_sample_campaign(campaign_id: str, campaign_name: str, max_pages: int = 10) -> Dict:
    """
    Quick sample of a campaign to estimate cleanup rates.
    
    Args:
        campaign_id: Campaign ID to sample
        campaign_name: Campaign name for logging
        max_pages: Number of pages to sample (default 10 = 500 leads)
    
    Returns:
        Dictionary with sample results and projections
    """
    logger.info(f"🎯 QUICK SAMPLE: Analyzing first {max_pages} pages of {campaign_name} campaign...")
    
    sample_leads = []
    page_count = 0
    starting_after = None
    
    # Get sample pages
    while page_count < max_pages:
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
                logger.error(f"❌ API error {response.status_code} for {campaign_name}")
                break
                
            data = response.json()
            leads = data.get('items', [])
            
            if not leads:
                logger.info(f"📄 No more leads after {page_count} pages")
                break
            
            page_count += 1
            sample_leads.extend(leads)
            logger.info(f"📄 Sample page {page_count}: {len(leads)} leads ({len(sample_leads)} total)")
            
            starting_after = data.get('next_starting_after')
            if not starting_after:
                logger.info(f"✅ Reached end of {campaign_name} after {page_count} pages")
                break
                
        except Exception as e:
            logger.error(f"❌ Error sampling {campaign_name}: {e}")
            break
    
    # Analyze sample for cleanup candidates
    cleanup_candidates = 0
    classification_breakdown = {
        'replied': 0, 'completed': 0, 'bounced_hard': 0, 'unsubscribed': 0,
        'stale_active': 0, 'auto_reply_detected': 0, 'kept_active': 0,
        'kept_paused': 0, 'kept_other': 0
    }
    
    for lead in sample_leads:
        try:
            lead_id = lead.get('id', '')
            email = lead.get('email', '')
            
            if not lead_id or not email:
                continue
            
            classification = classify_lead_for_drain(lead, campaign_name)
            
            if classification['should_drain']:
                cleanup_candidates += 1
                drain_reason = classification.get('drain_reason', 'unknown')
                classification_breakdown[drain_reason] += 1
                logger.debug(f"🗑️ SAMPLE CLEANUP: {email} → {drain_reason}")
            else:
                keep_reason = str(classification.get('keep_reason', 'unknown'))
                status = lead.get('status', 0)
                
                is_auto_reply = ('auto-reply' in keep_reason.lower() if isinstance(keep_reason, str) else False)
                
                if is_auto_reply:
                    classification_breakdown['auto_reply_detected'] += 1
                elif status == 1:
                    classification_breakdown['kept_active'] += 1
                elif status == 2:
                    classification_breakdown['kept_paused'] += 1
                else:
                    classification_breakdown['kept_other'] += 1
                
        except Exception as e:
            logger.debug(f"Error classifying sample lead: {e}")
            continue
    
    # Calculate rates and projections
    sample_size = len(sample_leads)
    cleanup_rate = (cleanup_candidates / max(sample_size, 1)) * 100
    
    # Estimate total campaign size (rough estimate based on 50 leads/page)
    estimated_total_leads = page_count * 50
    if page_count < max_pages:
        # We hit the end, so this is accurate
        estimated_total_leads = sample_size
    else:
        # Extrapolate - assume we're seeing early pages
        # For a more accurate estimate, we'd need to scan more
        estimated_total_leads = max(sample_size * 2, 3000)  # Conservative estimate
    
    projected_cleanup = int((cleanup_rate / 100) * estimated_total_leads)
    
    return {
        'campaign_name': campaign_name,
        'campaign_id': campaign_id,
        'sample_size': sample_size,
        'sample_cleanup_candidates': cleanup_candidates,
        'sample_cleanup_rate': cleanup_rate,
        'classification_breakdown': classification_breakdown,
        'estimated_total_leads': estimated_total_leads,
        'projected_total_cleanup': projected_cleanup
    }

def main():
    """Run quick audit sample on both campaigns."""
    logger.info("🚀 STARTING QUICK AUDIT SAMPLE")
    logger.info("=" * 60)
    logger.info("📊 This will analyze ~10 pages per campaign for quick estimates")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    if not IMPORTS_AVAILABLE:
        logger.error("❌ Required imports not available")
        return
    
    campaigns_to_sample = [
        (SMB_CAMPAIGN_ID, "SMB"),
        (MIDSIZE_CAMPAIGN_ID, "Midsize")
    ]
    
    sample_results = []
    
    # Sample each campaign
    for campaign_id, campaign_name in campaigns_to_sample:
        try:
            result = quick_sample_campaign(campaign_id, campaign_name, max_pages=10)
            sample_results.append(result)
        except Exception as e:
            logger.error(f"❌ Failed to sample {campaign_name}: {e}")
            continue
    
    # Generate quick report
    logger.info("=" * 60)
    logger.info("📊 QUICK AUDIT SAMPLE RESULTS")
    logger.info("=" * 60)
    
    total_projected_cleanup = 0
    total_estimated_leads = 0
    
    for result in sample_results:
        campaign_name = result['campaign_name']
        sample_size = result['sample_size']
        cleanup_candidates = result['sample_cleanup_candidates']
        cleanup_rate = result['sample_cleanup_rate']
        estimated_total = result['estimated_total_leads']
        projected_cleanup = result['projected_total_cleanup']
        classification = result['classification_breakdown']
        
        total_projected_cleanup += projected_cleanup
        total_estimated_leads += estimated_total
        
        logger.info(f"📋 {campaign_name.upper()} CAMPAIGN SAMPLE:")
        logger.info(f"  • Sample analyzed: {sample_size} leads")
        logger.info(f"  • Sample cleanup candidates: {cleanup_candidates}")
        logger.info(f"  • Sample cleanup rate: {cleanup_rate:.1f}%")
        logger.info(f"  • Estimated total leads in campaign: {estimated_total:,}")
        logger.info(f"  • Projected total cleanup needed: {projected_cleanup:,}")
        
        # Show top classification reasons
        if classification:
            top_reasons = sorted(classification.items(), key=lambda x: x[1], reverse=True)[:5]
            logger.info(f"  • Top classifications:")
            for reason, count in top_reasons:
                if count > 0:
                    percentage = (count / max(sample_size, 1)) * 100
                    logger.info(f"    - {reason}: {count} ({percentage:.1f}%)")
        
        logger.info("")
    
    # Grand summary with projections
    overall_cleanup_rate = (total_projected_cleanup / max(total_estimated_leads, 1)) * 100
    
    logger.info("🎯 PROJECTED TOTALS (Based on Sample):")
    logger.info(f"  • Estimated total leads across campaigns: {total_estimated_leads:,}")
    logger.info(f"  • Projected cleanup candidates: {total_projected_cleanup:,}")
    logger.info(f"  • Projected cleanup rate: {overall_cleanup_rate:.1f}%")
    logger.info("")
    logger.info("⚠️  NOTE: These are estimates based on sampling first ~500 leads per campaign")
    logger.info("📊 For exact counts, run the full one_time_drain_audit.py script")
    logger.info("=" * 60)
    
    return sample_results

if __name__ == "__main__":
    main()