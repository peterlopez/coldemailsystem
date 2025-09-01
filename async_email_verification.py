#!/usr/bin/env python3
"""
Async Email Verification System for Instantly.ai

This module implements asynchronous email verification with proper pending status handling.
Based on research showing that Instantly returns "pending" status initially, then processes
verification in the background.

Key Features:
- Triggers bulk verification for new leads after campaign assignment
- Polls verification results without blocking lead creation
- Handles "pending" status as legitimate intermediate state
- Updates BigQuery with final verification results
- Tracks verification credits usage

Usage:
1. Create leads and assign to campaigns (sync_once.py)
2. Trigger verification for newly assigned leads
3. Poll verification results periodically
4. Update BigQuery with final statuses
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from google.cloud import bigquery

from shared_config import InstantlyConfig, BigQueryConfig
from sync_once import call_instantly_api, get_bigquery_client

logger = logging.getLogger(__name__)

@dataclass
class VerificationJob:
    """Represents a verification job for tracking purposes."""
    lead_id: str
    email: str
    campaign_id: str
    submitted_at: datetime
    status: str = "submitted"  # submitted, pending, completed, failed
    verification_status: Optional[str] = None  # valid, invalid, risky, accept_all
    credits_used: Optional[int] = None
    last_checked: Optional[datetime] = None
    attempts: int = 0

class AsyncEmailVerification:
    """Handles async email verification with Instantly.ai"""
    
    def __init__(self):
        self.bq_client = get_bigquery_client()
        self.instantly_config = InstantlyConfig()
        self.bq_config = BigQueryConfig()
        
    def trigger_bulk_verification(self, lead_emails: List[str]) -> Dict[str, Any]:
        """
        Trigger bulk verification for a list of email addresses.
        
        Args:
            lead_emails: List of email addresses to verify
            
        Returns:
            Dict with submission results and job tracking info
        """
        if not lead_emails:
            return {"success": True, "submitted": 0, "message": "No emails to verify"}
        
        logger.info(f"🔍 Triggering verification for {len(lead_emails)} emails...")
        
        submitted_count = 0
        failed_submissions = []
        
        # Submit verification requests (in batches if needed)
        for email in lead_emails:
            try:
                # Submit individual verification request
                verification_data = {
                    "email": email,
                    "verify_on_import": True  # This triggers immediate verification
                }
                
                response = call_instantly_api('/api/v2/email-verification', 'POST', verification_data)
                
                if response and 'error' not in response:
                    submitted_count += 1
                    logger.debug(f"✅ Verification submitted for {email}")
                else:
                    failed_submissions.append({
                        "email": email, 
                        "error": response.get('message', 'Unknown error')
                    })
                    logger.warning(f"❌ Failed to submit verification for {email}: {response}")
                
                # Rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                failed_submissions.append({"email": email, "error": str(e)})
                logger.error(f"Exception submitting verification for {email}: {e}")
        
        logger.info(f"📊 Verification submission complete: {submitted_count} submitted, {len(failed_submissions)} failed")
        
        return {
            "success": submitted_count > 0,
            "submitted": submitted_count,
            "failed": len(failed_submissions),
            "failed_details": failed_submissions
        }
    
    def poll_verification_results(self, max_leads: int = 500) -> Dict[str, Any]:
        """
        Poll verification results for leads with pending verification.
        
        Args:
            max_leads: Maximum number of leads to check per poll
            
        Returns:
            Dict with polling results
        """
        logger.info(f"🔍 Polling verification results (max {max_leads} leads)...")
        
        # Get leads that need verification status updates
        pending_leads = self._get_pending_verification_leads(max_leads)
        
        if not pending_leads:
            logger.info("✅ No leads pending verification check")
            return {"success": True, "checked": 0, "updated": 0}
        
        logger.info(f"📋 Found {len(pending_leads)} leads to check")
        
        checked_count = 0
        updated_count = 0
        still_pending = 0
        
        for lead in pending_leads:
            try:
                # Get current lead status from Instantly
                lead_id = lead['instantly_lead_id']
                response = call_instantly_api(f'/api/v2/leads/{lead_id}')
                
                if response and 'error' not in response:
                    verification_status = response.get('verification_status')
                    verification_catch_all = response.get('verification_catch_all')
                    verification_credits = response.get('verification_credits_used')
                    
                    checked_count += 1
                    
                    # Check if verification is complete (not pending)
                    if verification_status and verification_status != 'pending':
                        # Update BigQuery with final verification results
                        self._update_verification_status(
                            lead['email'],
                            verification_status,
                            verification_catch_all,
                            verification_credits
                        )
                        updated_count += 1
                        logger.debug(f"✅ Updated verification for {lead['email']}: {verification_status}")
                    else:
                        still_pending += 1
                        logger.debug(f"⏳ Still pending: {lead['email']}")
                
                # Rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error checking verification for {lead['email']}: {e}")
        
        logger.info(f"📊 Verification poll complete: {checked_count} checked, {updated_count} updated, {still_pending} still pending")
        
        return {
            "success": True,
            "checked": checked_count,
            "updated": updated_count,
            "still_pending": still_pending
        }
    
    def _get_pending_verification_leads(self, limit: int = 500) -> List[Dict]:
        """Get leads that need verification status updates."""
        
        query = f"""
        SELECT 
            email,
            instantly_lead_id,
            campaign_id,
            verification_status,
            verified_at
        FROM `{self.bq_config.project_id}.{self.bq_config.dataset_id}.ops_inst_state`
        WHERE instantly_lead_id IS NOT NULL
            AND (
                verification_status IS NULL 
                OR verification_status = 'pending'
                OR (verification_status = 'pending' AND verified_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
            )
            AND added_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)  -- Only check recent leads
        ORDER BY added_at DESC
        LIMIT {limit}
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            leads = []
            for row in results:
                leads.append({
                    'email': row.email,
                    'instantly_lead_id': row.instantly_lead_id,
                    'campaign_id': row.campaign_id,
                    'verification_status': row.verification_status,
                    'verified_at': row.verified_at
                })
            
            return leads
            
        except Exception as e:
            logger.error(f"Error querying pending verification leads: {e}")
            return []
    
    def _update_verification_status(self, email: str, verification_status: str, 
                                  verification_catch_all: bool, verification_credits: int):
        """Update verification status in BigQuery."""
        
        query = f"""
        UPDATE `{self.bq_config.project_id}.{self.bq_config.dataset_id}.ops_inst_state`
        SET 
            verification_status = @verification_status,
            verification_catch_all = @verification_catch_all,
            verification_credits_used = @verification_credits,
            verified_at = CURRENT_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP()
        WHERE email = @email
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("verification_status", "STRING", verification_status),
                bigquery.ScalarQueryParameter("verification_catch_all", "BOOL", verification_catch_all),
                bigquery.ScalarQueryParameter("verification_credits", "INTEGER", verification_credits),
                bigquery.ScalarQueryParameter("email", "STRING", email),
            ]
        )
        
        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            query_job.result()  # Wait for completion
            logger.debug(f"✅ Updated verification status for {email}: {verification_status}")
            
        except Exception as e:
            logger.error(f"Error updating verification status for {email}: {e}")
    
    def get_verification_stats(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get verification statistics for reporting."""
        
        query = f"""
        SELECT 
            verification_status,
            COUNT(*) as count,
            SUM(COALESCE(verification_credits_used, 0)) as total_credits
        FROM `{self.bq_config.project_id}.{self.bq_config.dataset_id}.ops_inst_state`
        WHERE verified_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
            AND verification_status IS NOT NULL
        GROUP BY verification_status
        ORDER BY count DESC
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            stats = {}
            total_verified = 0
            total_credits = 0
            
            for row in results:
                stats[row.verification_status] = {
                    'count': row.count,
                    'credits': row.total_credits or 0
                }
                total_verified += row.count
                total_credits += row.total_credits or 0
            
            # Add summary
            stats['summary'] = {
                'total_verified': total_verified,
                'total_credits_used': total_credits,
                'hours_back': hours_back
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting verification stats: {e}")
            return {}

def trigger_verification_for_new_leads(lead_emails: List[str]) -> bool:
    """
    Convenience function to trigger verification for newly created leads.
    
    Args:
        lead_emails: List of email addresses to verify
        
    Returns:
        bool: True if verification was successfully triggered
    """
    if not lead_emails:
        return True
    
    verifier = AsyncEmailVerification()
    result = verifier.trigger_bulk_verification(lead_emails)
    
    return result.get('success', False)

def poll_pending_verifications() -> Dict[str, Any]:
    """
    Convenience function to poll pending verification results.
    
    Returns:
        Dict with polling results
    """
    verifier = AsyncEmailVerification()
    return verifier.poll_verification_results()

def main():
    """Main function for standalone verification polling."""
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    verifier = AsyncEmailVerification()
    
    if '--stats' in sys.argv:
        # Show verification statistics
        print("📊 VERIFICATION STATISTICS")
        print("=" * 50)
        
        hours = 24
        if '--hours' in sys.argv:
            try:
                idx = sys.argv.index('--hours')
                hours = int(sys.argv[idx + 1])
            except (IndexError, ValueError):
                hours = 24
        
        stats = verifier.get_verification_stats(hours)
        
        if stats:
            print(f"\nVerification results (last {hours} hours):")
            for status, data in stats.items():
                if status != 'summary':
                    print(f"  {status}: {data['count']} leads ({data['credits']} credits)")
            
            summary = stats.get('summary', {})
            print(f"\nSummary:")
            print(f"  Total verified: {summary.get('total_verified', 0)}")
            print(f"  Total credits used: {summary.get('total_credits_used', 0)}")
        else:
            print("No verification data found")
    
    else:
        # Poll for verification results
        print("🔍 POLLING VERIFICATION RESULTS")
        print("=" * 50)
        
        result = verifier.poll_verification_results()
        
        print(f"\nResults:")
        print(f"  Leads checked: {result.get('checked', 0)}")
        print(f"  Statuses updated: {result.get('updated', 0)}")
        print(f"  Still pending: {result.get('still_pending', 0)}")

if __name__ == "__main__":
    main()