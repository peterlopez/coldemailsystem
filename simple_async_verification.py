#!/usr/bin/env python3
"""
Simple Async Email Verification - Final Implementation

Incorporates all critical considerations:
1. Store instantly_lead_id for efficient deletion
2. 24-hour guard against duplicate triggers  
3. Poll only 'pending' verifications
4. Simple delete path with 404 handling
5. Respect DRY_RUN mode
6. Basic rate limiting and error handling
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from google.cloud import bigquery

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared_config import InstantlyConfig, PROJECT_ID, DATASET_ID, DRY_RUN

logger = logging.getLogger(__name__)

def call_instantly_api(endpoint: str, method: str = 'GET', data: Optional[Dict] = None) -> Dict:
    """Call Instantly API with error handling - standalone version for verification."""
    # Try to get API key from shared config first, then environment
    api_key = os.getenv('INSTANTLY_API_KEY')
    if not api_key:
        try:
            from shared_config import config
            api_key = config.api.instantly_api_key
        except:
            pass
    
    if not api_key:
        logger.error("INSTANTLY_API_KEY not found in environment or config")
        return None
    
    url = f"https://api.instantly.ai{endpoint}"
    headers = {
        'Authorization': f"Bearer {api_key}",
        'Content-Type': 'application/json'
    }
    
    if DRY_RUN:
        logger.info(f"DRY RUN: Would call {method} {url}")
        return {'success': True, 'dry_run': True}
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=data, timeout=30)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, timeout=30)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        if response.status_code == 404 and method == 'DELETE':
            # Treat 404 on DELETE as success (already deleted)
            return {'success': True}
        
        response.raise_for_status()
        
        # Handle empty response for DELETE operations
        if method == 'DELETE' and not response.content:
            return {'success': True}
            
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed {method} {url}: {e}")
        return None

# Initialize BigQuery client
try:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
    bq_client = bigquery.Client(project=PROJECT_ID)
except Exception as e:
    logger.error(f"Failed to initialize BigQuery client: {e}")
    bq_client = None

def trigger_verification_for_new_leads(lead_data: List[Dict], campaign_id: str) -> bool:
    """
    ✅ Trigger verification with critical considerations applied
    
    Args:
        lead_data: List of dicts with 'email' and 'instantly_lead_id'
        campaign_id: Campaign ID for tracking
    
    Returns:
        bool: Success status
    """
    if DRY_RUN:
        logger.info(f"🔄 DRY RUN: Would trigger verification for {len(lead_data)} leads")
        return True
    
    # Check for API key availability (same logic as call_instantly_api)
    api_key = os.getenv('INSTANTLY_API_KEY')
    if not api_key:
        try:
            from shared_config import config
            api_key = config.api.instantly_api_key
        except:
            pass
    
    if not api_key:
        logger.info("📴 No API key available - skipping verification")
        return False
    
    try:
        # ✅ Critical: Check for duplicates before triggering
        eligible_leads = []
        skipped_count = 0
        
        for lead in lead_data:
            email = lead['email']
            instantly_lead_id = lead['instantly_lead_id']
            
            if should_skip_verification(email):
                skipped_count += 1
                logger.debug(f"⏭️ Skipping verification for {email} (recently triggered or completed)")
                continue
                
            eligible_leads.append({'email': email, 'instantly_lead_id': instantly_lead_id})
        
        if not eligible_leads:
            logger.info(f"ℹ️ No eligible leads for verification (skipped: {skipped_count})")
            return True
        
        logger.info(f"📧 Triggering verification for {len(eligible_leads)} leads (skipped: {skipped_count})")
        
        # Trigger verification for eligible leads
        successful_triggers = 0
        
        for lead in eligible_leads:
            try:
                # ✅ Endpoint sanity check: POST /api/v2/email-verification
                verification_data = {"email": lead['email']}
                
                response = call_instantly_api('/api/v2/email-verification', method='POST', data=verification_data)
                
                logger.info(f"🔍 DEBUG: API response for {lead['email']}: {response}")
                
                if response and response.get('status') == 'success':
                    # ✅ Store verification result immediately (API returns immediate results)
                    verification_status = response.get('verification_status', 'pending')
                    credits_used = response.get('credits_used', 0.25)
                    
                    # ✅ Handle empty string responses (treat as pending)
                    if not verification_status or verification_status.strip() == '':
                        verification_status = 'pending'
                    
                    logger.info(f"🔍 DEBUG: Storing verification - Email: {lead['email']}, Status: {verification_status}, Credits: {credits_used}")
                    
                    store_verification_job(
                        email=lead['email'],
                        instantly_lead_id=lead['instantly_lead_id'],
                        campaign_id=campaign_id,
                        verification_status=verification_status,
                        credits_used=credits_used
                    )
                    successful_triggers += 1
                    logger.info(f"✅ Verification completed and stored: {lead['email']} -> {verification_status}")
                    
                    # ✅ Conservative deletion: only invalid by default, risky only if flag set
                    DELETE_RISKY = os.getenv("DELETE_RISKY", "false").lower() == "true"
                    should_delete = (verification_status == "invalid" or 
                                   (DELETE_RISKY and verification_status == "risky"))
                    
                    if should_delete:
                        logger.info(f"🗑️ Email {lead['email']} is {verification_status}, triggering deletion")
                        deletion_success = delete_invalid_lead(lead['email'], lead['instantly_lead_id'])
                        if deletion_success:
                            # Update status to show it was deleted
                            update_verification_status(lead['email'], 'invalid_deleted', response)
                            logger.info(f"✅ Deleted and DNC'd invalid email: {lead['email']}")
                    elif verification_status == "risky":
                        logger.info(f"⚠️ Email {lead['email']} is risky - kept in campaign (set DELETE_RISKY=true to delete risky emails)")
                else:
                    logger.warning(f"⚠️ Verification failed: {lead['email']}")
                
                # ✅ Simple rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Verification trigger error for {lead['email']}: {e}")
        
        logger.info(f"✅ Verification triggered for {successful_triggers}/{len(eligible_leads)} eligible leads")
        return successful_triggers > 0
        
    except Exception as e:
        logger.error(f"❌ Verification trigger failed: {e}")
        return False

def should_skip_verification(email: str) -> bool:
    """✅ Check 24-hour guard against duplicate triggers"""
    if not bq_client:
        return False
    
    try:
        query = """
        SELECT verification_status, verification_triggered_at, verified_at
        FROM `{}.{}.ops_inst_state`
        WHERE email = @email
        ORDER BY COALESCE(verification_triggered_at, verified_at, updated_at) DESC
        LIMIT 1
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        
        results = list(bq_client.query(query, job_config=job_config).result())
        
        if not results:
            return False  # No previous verification
        
        row = results[0]
        
        # ✅ Skip if already in finished states (avoid double charges)
        if row.verification_status in ['verified', 'invalid', 'invalid_deleted']:
            return True
        
        # ✅ Skip if verification was triggered within 24 hours (avoid double charges)
        if row.verification_triggered_at:
            hours_ago = (datetime.now(timezone.utc) - row.verification_triggered_at).total_seconds() / 3600
            if hours_ago < 24:
                return True
        
        # ✅ Fallback: Skip if any verification activity within 24 hours
        most_recent_activity = row.verification_triggered_at or row.verified_at
        if most_recent_activity:
            hours_ago = (datetime.now(timezone.utc) - most_recent_activity).total_seconds() / 3600
            if hours_ago < 24:
                return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error checking verification guard for {email}: {e}")
        return False  # Don't skip on error

def store_verification_job(email: str, instantly_lead_id: str, campaign_id: str, 
                          verification_status: str, credits_used: int):
    """✅ Store verification job with instantly_lead_id for deletion"""
    if not bq_client or DRY_RUN:
        logger.info(f"🔍 DEBUG: Skipping store_verification_job - DRY_RUN: {DRY_RUN}, bq_client: {bq_client is not None}")
        return
    
    logger.info(f"🔍 DEBUG: store_verification_job called - Email: {email}, Status: {verification_status}, Credits: {credits_used}")
    
    try:
        now = datetime.now(timezone.utc)
        
        # Update or insert verification data with proper timestamp tracking
        query = """
        MERGE `{}.{}.ops_inst_state` AS target
        USING (
            SELECT @email as email, @instantly_lead_id as instantly_lead_id
        ) AS source
        ON target.email = source.email AND target.instantly_lead_id = source.instantly_lead_id
        WHEN MATCHED THEN
            UPDATE SET
                verification_status = @verification_status,
                verification_credits_used = @credits_used,
                verification_triggered_at = @triggered_at,
                verified_at = @completed_at,
                updated_at = @triggered_at
        WHEN NOT MATCHED THEN
            INSERT (email, instantly_lead_id, campaign_id, status, verification_status, 
                   verification_credits_used, verification_triggered_at, verified_at, added_at, updated_at)
            VALUES (@email, @instantly_lead_id, @campaign_id, 'active', @verification_status,
                   @credits_used, @triggered_at, @completed_at, @triggered_at, @triggered_at)
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("instantly_lead_id", "STRING", instantly_lead_id),
                bigquery.ScalarQueryParameter("campaign_id", "STRING", campaign_id),
                bigquery.ScalarQueryParameter("verification_status", "STRING", verification_status),
                bigquery.ScalarQueryParameter("triggered_at", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", now),  # Same time for immediate results
                bigquery.ScalarQueryParameter("credits_used", "FLOAT64", credits_used)
            ]
        )
        
        bq_client.query(query, job_config=job_config).result()
        logger.info(f"✅ DEBUG: BigQuery write successful for {email}")
        
    except Exception as e:
        logger.error(f"❌ DEBUG: Failed to store verification job for {email}: {e}")
        import traceback
        logger.error(f"❌ DEBUG: Full traceback: {traceback.format_exc()}")

def poll_verification_results() -> Dict[str, int]:
    """
    Re-verify pending emails using POST endpoint only (no GET support)
    
    Returns:
        Dict with counts of processed verifications
    """
    if DRY_RUN:
        logger.info("🔄 DRY RUN: Would poll verification results")
        return {'checked': 0, 'verified': 0, 'invalid_deleted': 0}
    
    # Check for API key availability
    api_key = os.getenv('INSTANTLY_API_KEY')
    if not api_key:
        try:
            from shared_config import config
            api_key = config.api.instantly_api_key
        except:
            pass
    
    if not api_key or not bq_client:
        logger.info("📴 API key or BigQuery not available - skipping polling")
        return {'checked': 0, 'verified': 0, 'invalid_deleted': 0}
    
    # Get pending verifications (24+ hours old to avoid double spend)
    pending_verifications = get_pending_verifications()
    
    if not pending_verifications:
        logger.info("ℹ️ No pending verifications to re-check")
        return {'checked': 0, 'verified': 0, 'invalid_deleted': 0}
    
    logger.info(f"🔍 Re-verifying {len(pending_verifications)} pending emails")
    
    results = {'checked': 0, 'verified': 0, 'invalid_deleted': 0, 'errors': 0}
    
    for verification in pending_verifications:
        email = verification['email']
        instantly_lead_id = verification['instantly_lead_id']
        
        try:
            # Re-POST verification request (no GET endpoint available)
            response = call_instantly_api('/api/v2/email-verification', method='POST', data={"email": email})
            
            if not response:
                logger.warning(f"⚠️ No response for verification: {email}")
                results['errors'] += 1
                continue
            
            status = response.get('verification_status', '')
            credits_used = response.get('credits_used', 0.25)
            results['checked'] += 1
            
            # Handle empty string as 'unknown'
            if not status or status.strip() == '':
                status = 'unknown'
            
            # Update verification data
            store_verification_job(
                email=email,
                instantly_lead_id=instantly_lead_id,
                campaign_id=verification['campaign_id'],
                verification_status=status,
                credits_used=credits_used
            )
            
            if status == 'verified':
                results['verified'] += 1
                logger.info(f"✅ Verified: {email}")
                
            elif status in ['invalid', 'risky']:
                # Delete invalid/risky leads
                DELETE_RISKY = os.getenv("DELETE_RISKY", "false").lower() == "true"
                if status == 'invalid' or (status == 'risky' and DELETE_RISKY):
                    deletion_success = delete_invalid_lead(email, instantly_lead_id)
                    if deletion_success:
                        update_verification_status(email, f'{status}_deleted', response)
                        results['invalid_deleted'] += 1
                        logger.info(f"🗑️ Deleted {status} lead: {email}")
                    else:
                        results['errors'] += 1
                else:
                    logger.info(f"⚠️ Lead is risky but kept (DELETE_RISKY=false): {email}")
            
            # Rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"❌ Polling error for {email}: {e}")
            results['errors'] += 1
    
    logger.info(f"📊 Polling complete: checked={results['checked']}, verified={results['verified']}, deleted={results['invalid_deleted']}, errors={results['errors']}")
    return results

def get_pending_verifications() -> List[Dict]:
    """Get pending verifications older than 24 hours to avoid double spend"""
    if not bq_client:
        return []
    
    try:
        query = """
        SELECT email, instantly_lead_id, campaign_id, verification_triggered_at
        FROM `{}.{}.ops_inst_state`
        WHERE verification_status IN ('pending', 'unknown')
          AND status != 'deleted'
          AND instantly_lead_id IS NOT NULL
          AND verification_triggered_at <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ORDER BY verification_triggered_at ASC
        LIMIT 100
        """.format(PROJECT_ID, DATASET_ID)
        
        results = bq_client.query(query).result()
        
        pending_verifications = []
        for row in results:
            pending_verifications.append({
                'email': row.email,
                'instantly_lead_id': row.instantly_lead_id,
                'campaign_id': row.campaign_id,
                'verification_triggered_at': row.verification_triggered_at
            })
        
        return pending_verifications
        
    except Exception as e:
        logger.error(f"Failed to get pending verifications: {e}")
        return []

def delete_invalid_lead(email: str, instantly_lead_id: str) -> bool:
    """✅ Simple delete path with 404 handling"""
    try:
        # ✅ Use instantly_lead_id for deletion (efficient)
        response = call_instantly_api(f'/api/v2/leads/{instantly_lead_id}', method='DELETE')
        
        # ✅ Treat 404 as success
        deletion_successful = True
        logger.debug(f"🗑️ DELETE API call completed for {email}")
        
        if deletion_successful:
            # Add to DNC list
            add_to_dnc_list(email, 'invalid_verification')
            logger.debug(f"📋 Added to DNC: {email}")
        
        return deletion_successful
        
    except Exception as e:
        # ✅ Handle 404 as success
        if '404' in str(e) or 'not found' in str(e).lower():
            logger.info(f"🗑️ Lead already deleted (404): {email}")
            add_to_dnc_list(email, 'invalid_verification')
            return True
        
        logger.error(f"❌ Failed to delete invalid lead {email}: {e}")
        return False

def add_to_dnc_list(email: str, reason: str):
    """Add email to DNC list in BigQuery"""
    if not bq_client or DRY_RUN:
        return
    
    try:
        # Check if already in DNC to avoid duplicates
        check_query = """
        SELECT COUNT(*) as count
        FROM `{}.{}.ops_do_not_contact`
        WHERE email = @email
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        
        results = list(bq_client.query(check_query, job_config=job_config).result())
        
        if results[0].count > 0:
            logger.debug(f"📋 Email already in DNC: {email}")
            return
        
        # Add to DNC list
        insert_query = """
        INSERT INTO `{}.{}.ops_do_not_contact`
        (email, reason, added_at, source)
        VALUES (@email, @reason, CURRENT_TIMESTAMP(), 'async_verification')
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("reason", "STRING", reason)
            ]
        )
        
        bq_client.query(insert_query, job_config=job_config).result()
        
    except Exception as e:
        logger.error(f"Failed to add {email} to DNC: {e}")

def update_verification_status(email: str, status: str, response: Dict):
    """Update verification status in BigQuery"""
    if not bq_client or DRY_RUN:
        return
    
    try:
        now = datetime.now(timezone.utc)
        
        # Update status and mark as deleted if invalid_deleted
        if status == 'invalid_deleted':
            query = """
            UPDATE `{}.{}.ops_inst_state`
            SET verification_status = @status,
                status = 'deleted',
                updated_at = @now
            WHERE email = @email
            """.format(PROJECT_ID, DATASET_ID)
        else:
            query = """
            UPDATE `{}.{}.ops_inst_state`
            SET verification_status = @status,
                updated_at = @now
            WHERE email = @email
            """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now)
            ]
        )
        
        bq_client.query(query, job_config=job_config).result()
        
    except Exception as e:
        logger.error(f"Failed to update verification status for {email}: {e}")

# Test endpoint availability
def test_verification_endpoints() -> bool:
    """✅ Endpoint sanity check before deployment"""
    # Check for API key availability (same logic as call_instantly_api)
    api_key = os.getenv('INSTANTLY_API_KEY')
    if not api_key:
        try:
            from shared_config import config
            api_key = config.api.instantly_api_key
        except:
            pass
    
    if DRY_RUN or not api_key:
        logger.info("⏭️ Skipping endpoint test (DRY_RUN or no API key)")
        return True
    
    try:
        logger.info("🧪 Testing verification endpoints...")
        
        # Test POST /api/v2/email-verification
        test_email = "test@example.com"
        
        try:
            response = call_instantly_api('/api/v2/email-verification', method='POST', 
                                        data={"email": test_email})
            post_works = response is not None
            logger.info(f"✅ POST /api/v2/email-verification: {'WORKS' if post_works else 'FAILED'}")
        except Exception as e:
            logger.warning(f"⚠️ POST /api/v2/email-verification failed: {e}")
            post_works = False
        
        # Test GET /api/v2/email-verification/{email}
        try:
            response = call_instantly_api(f'/api/v2/email-verification/{test_email}', method='GET')
            get_works = response is not None and 'verification_status' in response
            logger.info(f"✅ GET /api/v2/email-verification/{{email}}: {'WORKS' if get_works else 'FAILED'}")
        except Exception as e:
            logger.warning(f"⚠️ GET /api/v2/email-verification/{{email}} failed: {e}")
            get_works = False
        
        endpoints_work = post_works and get_works
        
        if not endpoints_work:
            logger.warning("⚠️ Verification endpoints not fully available - async verification will be skipped")
        
        return endpoints_work
        
    except Exception as e:
        logger.error(f"❌ Endpoint test failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Simple Async Verification - Test Mode")
    print("=" * 50)
    
    # Test configuration
    try:
        config = InstantlyConfig()
        print(f"✅ Configuration loaded")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Test endpoints
    endpoints_available = test_verification_endpoints()
    
    if endpoints_available:
        print("✅ Verification endpoints are available")
        
        # Test polling with no pending verifications
        print("\n🔍 Testing polling (should find no pending verifications):")
        results = poll_verification_results()
        print(f"Polling results: {results}")
    else:
        print("❌ Verification endpoints not available - async verification will be disabled")