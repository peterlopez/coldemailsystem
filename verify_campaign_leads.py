#!/usr/bin/env python3
"""
Post-Campaign Email Verification
Verifies leads after they've been added to Instantly campaigns.
This runs after sync_once.py to verify newly added leads.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import shared configuration and utilities
try:
    from shared_config import config
    from sync_once import (
        get_bigquery_client,
        call_instantly_api,
        PROJECT_ID,
        DATASET_ID
    )
    logger.info("✅ Successfully imported verification functions")
except ImportError as e:
    logger.error(f"❌ Import failed: {e}")
    sys.exit(1)

@dataclass
class CampaignLead:
    """Represents a lead in a campaign that needs verification."""
    email: str
    campaign_id: str
    instantly_lead_id: Optional[str]
    added_at: datetime

def get_unverified_campaign_leads(hours_back: int = 2, limit: int = 200) -> List[CampaignLead]:
    """Get leads added to campaigns in last N hours that haven't been verified."""
    try:
        client = get_bigquery_client()
        
        query = f"""
        SELECT 
            email,
            campaign_id, 
            instantly_lead_id,
            added_at
        FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
        WHERE added_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
          AND (verification_status IS NULL OR verification_status = 'pending' OR verification_status = 'error')
          AND status = 'active'  -- Only verify active leads
        ORDER BY added_at DESC
        LIMIT {limit}
        """
        
        results = client.query(query).result()
        
        leads = []
        for row in results:
            leads.append(CampaignLead(
                email=row.email,
                campaign_id=row.campaign_id,
                instantly_lead_id=row.instantly_lead_id,
                added_at=row.added_at
            ))
        
        logger.info(f"📋 Found {len(leads)} unverified campaign leads from last {hours_back} hours")
        return leads
        
    except Exception as e:
        logger.error(f"❌ Failed to get unverified leads: {e}")
        return []

def update_lead_verification_status(lead: CampaignLead, verification_result: Dict[str, Any]) -> bool:
    """Update verification status for a single lead in BigQuery."""
    try:
        client = get_bigquery_client()
        
        query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
        SET 
            verification_status = @status,
            verification_catch_all = @catch_all,
            verification_credits_used = @credits_used,
            verified_at = CURRENT_TIMESTAMP()
        WHERE email = @email AND campaign_id = @campaign_id
        """
        
        from google.cloud import bigquery
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("status", "STRING", verification_result.get('status', 'unknown')),
                bigquery.ScalarQueryParameter("catch_all", "BOOLEAN", verification_result.get('catch_all', False)),
                bigquery.ScalarQueryParameter("credits_used", "INTEGER", verification_result.get('credits_used', 1)),
                bigquery.ScalarQueryParameter("email", "STRING", lead.email),
                bigquery.ScalarQueryParameter("campaign_id", "STRING", lead.campaign_id),
            ]
        )
        
        client.query(query, job_config=job_config).result()
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to update verification for {lead.email}: {e}")
        return False

def bulk_update_verification_results(results: List[tuple]) -> None:
    """Bulk update verification results in BigQuery."""
    try:
        if not results:
            return
            
        client = get_bigquery_client()
        
        # Prepare bulk update data
        values_clauses = []
        for lead, verification in results:
            status = verification.get('status', 'unknown').replace("'", "\\'")
            catch_all = 'TRUE' if verification.get('catch_all', False) else 'FALSE'
            credits = verification.get('credits_used', 1)
            email = lead.email.replace("'", "\\'")
            campaign_id = lead.campaign_id.replace("'", "\\'")
            
            values_clauses.append(f"('{email}', '{campaign_id}', '{status}', {catch_all}, {credits})")
        
        values_clause = ",\\n    ".join(values_clauses)
        
        query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` AS target
        SET 
            verification_status = source.status,
            verification_catch_all = source.catch_all, 
            verification_credits_used = source.credits_used,
            verified_at = CURRENT_TIMESTAMP()
        FROM (
          SELECT * FROM UNNEST([
            {values_clause}
          ]) AS t(email, campaign_id, status, catch_all, credits_used)
        ) AS source
        WHERE target.email = source.email 
          AND target.campaign_id = source.campaign_id
        """
        
        client.query(query).result()
        logger.info(f"✅ Bulk updated {len(results)} verification results")
        
    except Exception as e:
        logger.error(f"❌ Bulk verification update failed: {e}")
        # Fall back to individual updates
        for lead, verification in results:
            update_lead_verification_status(lead, verification)

def verify_campaign_leads_batch(leads: List[CampaignLead]) -> Dict[str, Any]:
    """Verify a batch of campaign leads and return statistics."""
    if not leads:
        logger.info("📭 No leads to verify")
        return {'total': 0, 'verified': 0, 'failed': 0, 'credits_used': 0}
    
    logger.info(f"🔍 Starting verification of {len(leads)} campaign leads...")
    
    verification_results = []
    failed_verifications = []
    total_credits = 0
    verified_count = 0
    failed_count = 0
    
    for i, lead in enumerate(leads, 1):
        try:
            logger.info(f"📧 [{i}/{len(leads)}] Verifying: {lead.email}")
            
            # Verify the email using Instantly API
            verification = verify_email(lead.email)
            
            # Track results
            verification_results.append((lead, verification))
            credits_used = verification.get('credits_used', 1)
            total_credits += credits_used
            
            if verification.get('status') == 'error':
                failed_verifications.append((lead, verification))
                failed_count += 1
                logger.warning(f"❌ Verification failed for {lead.email}: {verification.get('error', 'Unknown error')}")
            else:
                verified_count += 1
                status = verification.get('status', 'unknown')
                logger.info(f"✅ {lead.email} → {status} (Credits: {credits_used})")
            
            # Rate limiting between verification calls
            if i < len(leads):  # Don't sleep after last verification
                time.sleep(config.rate_limits.verification_delay)
                
        except Exception as e:
            logger.error(f"❌ Verification error for {lead.email}: {e}")
            failed_verifications.append((lead, {'email': lead.email, 'status': 'error', 'error': str(e)}))
            failed_count += 1
    
    # Bulk update results in BigQuery
    if verification_results:
        bulk_update_verification_results(verification_results)
    
    # Track failed verifications for debugging
    if failed_verifications:
        try:
            failed_leads_data = [
                {
                    'email': lead.email,
                    'campaign_id': lead.campaign_id,
                    'error': verification.get('error', 'Verification failed'),
                    'status': verification.get('status', 'error')
                }
                for lead, verification in failed_verifications
            ]
            _bulk_track_verification_failures(failed_leads_data, 'post_campaign_verification')
        except Exception as e:
            logger.error(f"❌ Failed to track verification failures: {e}")
    
    stats = {
        'total': len(leads),
        'verified': verified_count,
        'failed': failed_count,
        'credits_used': total_credits,
        'verification_results': verification_results
    }
    
    logger.info(f"📊 Verification complete: {verified_count}/{len(leads)} verified, {total_credits} credits used")
    return stats

def main():
    """Main post-campaign verification process."""
    start_time = time.time()
    
    logger.info("🔍 Starting Post-Campaign Email Verification")
    logger.info("=" * 60)
    
    # Configuration check
    if config.verification.enabled:
        logger.warning("⚠️ Pre-campaign verification is still enabled - this may cause double verification!")
    
    logger.info(f"🔧 Configuration:")
    logger.info(f"   DRY_RUN: {config.dry_run}")
    logger.info(f"   Rate Limit: {config.rate_limits.verification_delay}s between calls")
    
    try:
        # Get unverified leads from recent campaign additions
        hours_back = int(os.getenv('VERIFICATION_HOURS_BACK', '2'))
        max_leads = int(os.getenv('VERIFICATION_MAX_LEADS', '200'))
        
        leads = get_unverified_campaign_leads(hours_back, max_leads)
        
        if not leads:
            logger.info("✅ No unverified campaign leads found - all caught up!")
            return
        
        # Verify the leads
        stats = verify_campaign_leads_batch(leads)
        
        # Summary
        duration = time.time() - start_time
        rate = (stats['total'] / duration * 60) if duration > 0 else 0
        
        logger.info("\n" + "=" * 60)
        logger.info("📊 POST-CAMPAIGN VERIFICATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"✅ Leads Verified: {stats['verified']}/{stats['total']}")
        logger.info(f"❌ Verification Failures: {stats['failed']}")
        logger.info(f"💰 Credits Used: {stats['credits_used']}")
        logger.info(f"⏱️  Duration: {duration:.1f}s")
        logger.info(f"📈 Rate: {rate:.1f} verifications/minute")
        
        # Success rate analysis
        if stats['total'] > 0:
            success_rate = (stats['verified'] / stats['total']) * 100
            logger.info(f"📊 Success Rate: {success_rate:.1f}%")
            
            if success_rate < 50:
                logger.warning("⚠️ Low verification success rate - check lead quality or API issues")
            elif success_rate > 90:
                logger.info("🎉 Excellent verification success rate!")
        
    except Exception as e:
        logger.error(f"❌ Post-campaign verification failed: {e}")
        raise

if __name__ == "__main__":
    main()