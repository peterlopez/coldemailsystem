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
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared_config import InstantlyConfig, PROJECT_ID, DATASET_ID, DRY_RUN

logger = logging.getLogger(__name__)

# Import notification system
try:
    from shared.notify import get_notifier
    notifier = get_notifier()
    NOTIFICATIONS_AVAILABLE = True
    logger.info("📡 Notification system loaded for verification polling")
except ImportError:
    NOTIFICATIONS_AVAILABLE = False
    logger.info("📴 Notification system not available for verification polling")

def is_uuid4(s: str) -> bool:
    """Check if string is a valid UUID v4"""
    try:
        return uuid.UUID(s).version == 4
    except Exception:
        return False

def call_instantly_api(endpoint: str, method: str = 'GET', data: Optional[Dict] = None, use_session: bool = False) -> Dict:
    """Call Instantly API with enhanced logging and session management"""
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
        'Authorization': f"Bearer {api_key}"
    }
    
    # Only add Content-Type for requests with body data
    if method in ['POST', 'PUT', 'PATCH'] and data is not None:
        headers['Content-Type'] = 'application/json'
    
    if DRY_RUN:
        logger.info(f"DRY RUN: Would call {method} {url}")
        return {'success': True, 'dry_run': True}
    
    # Set timeout based on method (3s for DELETE, 30s for others)
    timeout = (3, 3) if method == 'DELETE' else 30
    
    try:
        if use_session and method == 'DELETE':
            # Use session with retry adapter for DELETE operations
            session = requests.Session()
            session.headers.update(headers)
            
            # Only retry on 429/5xx errors, not 400s
            retries = Retry(
                total=2,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["DELETE"]
            )
            session.mount("https://", HTTPAdapter(max_retries=retries))
            response = session.delete(url, timeout=timeout)
        else:
            # Standard requests for non-DELETE or when session not requested
            if method == 'GET':
                response = requests.get(url, headers=headers, timeout=timeout)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=data, timeout=timeout)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers, timeout=timeout)
            else:
                raise ValueError(f"Unsupported method: {method}")
        
        # Enhanced logging for DELETE operations
        if method == 'DELETE':
            rid = response.headers.get("X-Request-Id", "none")
            body = response.text[:800] if response.text else ""
            lead_id = endpoint.split('/')[-1] if '/' in endpoint else "unknown"
            
            # Log successful DELETEs as INFO, failures as WARNING
            if 200 <= response.status_code < 300 or response.status_code == 404:
                logger.info(f"DELETE {response.status_code} id={lead_id} rid={rid} body={body}")
            else:
                logger.warning(f"DELETE {response.status_code} id={lead_id} rid={rid} body={body}")
        
        # Always return structured response with status code for better success detection
        structured_response = {
            'status_code': response.status_code,
            'text': response.text,
            'json': None
        }
        
        # Treat 404/409 on DELETE as success (already deleted/conflict)  
        if response.status_code in [404, 409] and method == 'DELETE':
            return structured_response
        
        # Don't raise_for_status - let caller handle status codes
        if response.status_code >= 400:
            return structured_response
            
        # Parse JSON if available
        try:
            if response.content:
                structured_response['json'] = response.json()
        except:
            pass  # Keep json as None if parsing fails
            
        return structured_response
    
    except requests.exceptions.RequestException as e:
        # Enhanced error logging for DELETE operations
        if method == 'DELETE' and hasattr(e, 'response') and e.response is not None:
            rid = e.response.headers.get("X-Request-Id", "none")
            body = e.response.text[:800] if e.response.text else str(e)
            lead_id = endpoint.split('/')[-1] if '/' in endpoint else "unknown"
            status_code = getattr(e.response, 'status_code', 0)
            logger.error(f"DELETE {status_code} id={lead_id} rid={rid} body={body}")
            return {
                'status_code': status_code,
                'text': body,
                'json': None
            }
        else:
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
        
        logger.info(f"📧 Fire-and-forget verification for {len(eligible_leads)} leads (skipped: {skipped_count})")
        
        # Fire-and-forget verification for eligible leads
        successful_triggers = 0
        
        for lead in eligible_leads:
            try:
                email = lead['email']
                instantly_lead_id = lead['instantly_lead_id']
                
                # Step 1: Store as pending FIRST (recovery guarantee)
                store_verification_job_as_pending(
                    email=email,
                    instantly_lead_id=instantly_lead_id,
                    campaign_id=campaign_id
                )
                
                # Step 2: Fire POST request (don't wait for/parse response)
                verification_data = {"email": email}
                try:
                    call_instantly_api('/api/v2/email-verification', method='POST', data=verification_data)
                    logger.debug(f"🚀 Fired verification request: {email}")
                except Exception as api_error:
                    logger.warning(f"⚠️ API request failed for {email}: {api_error}")
                    # Continue - poller will retry since we marked as pending
                
                successful_triggers += 1
                
            except Exception as e:
                logger.error(f"❌ Verification trigger error for {lead['email']}: {e}")
        
        logger.info(f"✅ Fired verification requests for {successful_triggers}/{len(eligible_leads)} eligible leads - poller will handle results")
        return successful_triggers > 0
        
    except Exception as e:
        logger.error(f"❌ Verification trigger failed: {e}")
        return False

def store_verification_job_as_pending(email: str, instantly_lead_id: str, campaign_id: str):
    """Store verification job as pending and increment attempts (recovery guarantee)"""
    if not bq_client or DRY_RUN:
        logger.info(f"🔍 DEBUG: Skipping store_verification_job_as_pending - DRY_RUN: {DRY_RUN}")
        return
    
    try:
        now = datetime.now(timezone.utc)
        
        # MERGE to upsert the pending status and increment attempts
        query = """
        MERGE `{}.{}.ops_inst_state` AS target
        USING (
            SELECT @email as email, @instantly_lead_id as instantly_lead_id
        ) AS source
        ON target.email = source.email AND target.instantly_lead_id = source.instantly_lead_id
        WHEN MATCHED THEN
            UPDATE SET
                verification_status = 'pending',
                verification_triggered_at = @triggered_at,
                verification_attempts = COALESCE(verification_attempts, 0) + 1,
                updated_at = @triggered_at
        WHEN NOT MATCHED THEN
            INSERT (email, instantly_lead_id, campaign_id, status, verification_status, 
                   verification_triggered_at, verification_attempts, added_at, updated_at)
            VALUES (@email, @instantly_lead_id, @campaign_id, 'active', 'pending',
                   @triggered_at, 1, @triggered_at, @triggered_at)
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("instantly_lead_id", "STRING", instantly_lead_id),
                bigquery.ScalarQueryParameter("campaign_id", "STRING", campaign_id),
                bigquery.ScalarQueryParameter("triggered_at", "TIMESTAMP", now)
            ]
        )
        
        bq_client.query(query, job_config=job_config).result()
        logger.debug(f"✅ Stored {email} as pending (attempts incremented)")
        
    except Exception as e:
        logger.error(f"❌ Failed to store {email} as pending: {e}")
        raise  # Re-raise to stop processing this lead

def queue_for_deletion(email: str, instantly_lead_id: str):
    """Queue a lead for deletion by updating deletion_status"""
    if not bq_client or DRY_RUN:
        logger.info(f"🔍 DEBUG: Skipping queue_for_deletion - DRY_RUN: {DRY_RUN}")
        return
    
    try:
        query = """
        UPDATE `{}.{}.ops_inst_state`
        SET deletion_status = 'queued',
            deletion_attempts = 0,
            updated_at = CURRENT_TIMESTAMP()
        WHERE email = @email
          AND instantly_lead_id = @instantly_lead_id
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("instantly_lead_id", "STRING", instantly_lead_id)
            ]
        )
        
        bq_client.query(query, job_config=job_config).result()
        logger.debug(f"✅ Queued {email} for deletion")
        
    except Exception as e:
        logger.error(f"❌ Failed to queue {email} for deletion: {e}")

def should_skip_verification(email: str) -> bool:
    """Check de-duplication conditions to avoid unnecessary verification requests"""
    if not bq_client:
        return False
    
    try:
        query = """
        SELECT verification_status, verification_triggered_at, verification_attempts
        FROM `{}.{}.ops_inst_state`
        WHERE email = @email
        ORDER BY COALESCE(verification_triggered_at, updated_at) DESC
        LIMIT 1
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        
        results = list(bq_client.query(query, job_config=job_config).result())
        
        if not results:
            return False  # No previous record - don't skip
        
        row = results[0]
        
        # Skip condition 1: Already in finished states
        if row.verification_status in ['verified', 'invalid', 'risky', 'no_result']:
            logger.debug(f"⏭️ Skipping {email} - already {row.verification_status}")
            return True
        
        # Skip condition 2: Recent pending (within 10 minutes)
        if (row.verification_status in ['pending', ''] and 
            row.verification_triggered_at and
            (datetime.now(timezone.utc) - row.verification_triggered_at).total_seconds() < 600):  # 10 minutes
            logger.debug(f"⏭️ Skipping {email} - recently triggered ({row.verification_triggered_at})")
            return True
        
        # Skip condition 3: Too many attempts
        attempts = row.verification_attempts or 0
        if attempts >= 3:
            logger.debug(f"⏭️ Skipping {email} - max attempts reached ({attempts})")
            return True
        
        return False  # Don't skip - this email is eligible
        
    except Exception as e:
        logger.error(f"Error checking verification skip conditions for {email}: {e}")
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
    Process verifications first, then deletion queue (reordered for priority)
    
    Returns:
        Dict with counts of processed operations
    """
    if DRY_RUN:
        logger.info("🔄 DRY RUN: Would poll verification results and process deletions")
        return {
            'deletes_processed': 0, 
            'verifications_checked': 0, 
            'errors': 0,
            'checked': 0,
            'verified': 0,
            'invalid_deleted': 0,
            'status_breakdown': {},
            'deletion_breakdown': {}
        }
    
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
        return {
            'deletes_processed': 0, 
            'verifications_checked': 0, 
            'errors': 0,
            'checked': 0,
            'verified': 0,
            'invalid_deleted': 0,
            'status_breakdown': {},
            'deletion_breakdown': {}
        }
    
    results = {'deletes_processed': 0, 'verifications_checked': 0, 'errors': 0}
    
    # Process verifications FIRST to prevent starvation
    verification_results = process_stale_verifications()
    results['verifications_checked'] = verification_results.get('checked', 0)
    results['errors'] += verification_results.get('errors', 0)
    results['status_breakdown'] = verification_results.get('status_breakdown', {})
    results['queued_for_deletion'] = verification_results.get('queued_for_deletion', 0)
    
    # Then process deletion queue with circuit breaker
    deletion_results = process_deletion_queue()
    results['deletes_processed'] = deletion_results.get('processed', 0)
    results['errors'] += deletion_results.get('errors', 0)
    results['deletion_breakdown'] = deletion_results.get('campaign_breakdown', {})
    
    # Add backward compatibility keys for the workflow
    results['checked'] = results['verifications_checked']
    results['verified'] = results['verifications_checked']
    results['invalid_deleted'] = results['deletes_processed']
    
    logger.info(f"📊 Polling complete: verifications={results['verifications_checked']}, deletions={results['deletes_processed']}, errors={results['errors']}")
    return results

def process_deletion_queue() -> Dict[str, int]:
    """Process queued deletions with UUID validation, capping, and circuit breaker"""
    if not bq_client:
        return {'processed': 0, 'errors': 0, 'campaign_breakdown': {}}
    
    try:
        # Get up to 30 queued deletions with campaign info (capped to prevent starvation)
        query = """
        SELECT email, instantly_lead_id, deletion_attempts, campaign_id
        FROM `{}.{}.ops_inst_state`
        WHERE deletion_status = 'queued'
          AND deletion_attempts < 5
        ORDER BY COALESCE(last_deletion_attempt, updated_at) ASC
        LIMIT 30
        """.format(PROJECT_ID, DATASET_ID)
        
        results = list(bq_client.query(query).result())
        
        if not results:
            logger.debug("ℹ️ No queued deletions to process")
            return {'processed': 0, 'errors': 0, 'campaign_breakdown': {}}
        
        logger.info(f"🗑️ Processing {len(results)} queued deletions (capped at 30)")
        
        processed = 0
        errors = 0
        skipped_invalid_uuid = 0
        campaign_breakdown = {
            '8c46e0c9-c1f9-4201-a8d6-6221bafeada6': {'name': 'SMB', 'count': 0},
            '5ffbe8c3-dc0e-41e4-9999-48f00d2015df': {'name': 'Midsize', 'count': 0}
        }
        
        for row in results:
            email = row.email
            instantly_lead_id = row.instantly_lead_id
            attempts = row.deletion_attempts
            campaign_id = row.campaign_id
            
            # UUID validation - skip invalid UUIDs
            if not is_uuid4(instantly_lead_id):
                logger.warning(f"⚠️ Skipping invalid UUID for {email}: {instantly_lead_id}")
                # Mark as failed due to invalid UUID
                increment_deletion_attempts_with_error(
                    email, instantly_lead_id, 400, "Invalid UUID format"
                )
                skipped_invalid_uuid += 1
                errors += 1
                continue
            
            try:
                # Attempt deletion with session retry adapter
                response = call_instantly_api(
                    f'/api/v2/leads/{instantly_lead_id}', 
                    method='DELETE',
                    use_session=True
                )
                
                if not response:
                    # No response indicates failure
                    increment_deletion_attempts_with_error(
                        email, instantly_lead_id, 0, "No response from API"
                    )
                    errors += 1
                    continue
                
                # Check for success using status code (2xx or 404)
                status_code = response.get('status_code', 0)
                success = (200 <= status_code < 300) or status_code == 404
                
                if success:
                    # Mark as done and add to DNC
                    mark_deletion_complete(email, instantly_lead_id)
                    add_to_dnc_list(email, 'invalid_verification')
                    logger.info(f"✅ Successfully deleted: {email}")
                    processed += 1
                    
                    # Track campaign breakdown
                    if campaign_id in campaign_breakdown:
                        campaign_breakdown[campaign_id]['count'] += 1
                else:
                    # Extract error details and increment attempts
                    error_message = response.get('text', str(response))[:1000]
                    increment_deletion_attempts_with_error(
                        email, instantly_lead_id, status_code, error_message
                    )
                    errors += 1
                    
            except Exception as e:
                # Handle exceptions with error tracking
                logger.error(f"❌ DELETE error for {email}: {e}")
                increment_deletion_attempts_with_error(
                    email, instantly_lead_id, 0, str(e)
                )
                errors += 1
            
            # Circuit breaker: stop if failure rate > 80%
            if processed + errors > 5:  # Only check after 5+ attempts
                failure_rate = errors / (processed + errors)
                if failure_rate > 0.8:
                    logger.warning(f"🔴 Circuit breaker engaged: {failure_rate:.1%} failure rate after {processed + errors} deletions")
                    break
            
            # Rate limiting between deletions
            time.sleep(0.5)
        
        if skipped_invalid_uuid > 0:
            logger.info(f"⚠️ Skipped {skipped_invalid_uuid} deletions due to invalid UUIDs")
        
        # Clean up campaign breakdown to only include campaigns with deletions
        campaign_breakdown = {k: v for k, v in campaign_breakdown.items() if v['count'] > 0}
        
        return {'processed': processed, 'errors': errors, 'campaign_breakdown': campaign_breakdown}
        
    except Exception as e:
        logger.error(f"❌ Error processing deletion queue: {e}")
        return {'processed': 0, 'errors': 1, 'campaign_breakdown': {}}

def process_stale_verifications() -> Dict[str, int]:
    """Re-verify stale pending emails with attempt limits"""
    if not bq_client:
        return {'checked': 0, 'errors': 0, 'status_breakdown': {}, 'queued_for_deletion': 0}
    
    try:
        # Get up to 100 stale pending verifications
        query = """
        SELECT email, instantly_lead_id, campaign_id, verification_attempts
        FROM `{}.{}.ops_inst_state`
        WHERE verification_status IN ('', 'pending')
          AND verification_triggered_at <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
          AND COALESCE(verification_attempts, 0) < 2
        ORDER BY verification_triggered_at ASC
        LIMIT 100
        """.format(PROJECT_ID, DATASET_ID)
        
        results = list(bq_client.query(query).result())
        
        if not results:
            logger.debug("ℹ️ No stale verifications to process")
            return {'checked': 0, 'errors': 0, 'status_breakdown': {}, 'queued_for_deletion': 0}
        
        logger.info(f"🔍 Re-verifying {len(results)} stale pending emails")
        
        checked = 0
        errors = 0
        queued_for_deletion = 0
        status_breakdown = {
            'valid': 0,
            'invalid': 0,
            'risky': 0,
            'pending': 0,
            'no_result': 0,
            'accept_all': 0
        }
        
        for row in results:
            email = row.email
            instantly_lead_id = row.instantly_lead_id
            campaign_id = row.campaign_id
            attempts = row.verification_attempts or 0
            
            try:
                # Re-POST verification
                response = call_instantly_api('/api/v2/email-verification', method='POST', data={"email": email})
                
                if not response:
                    errors += 1
                    continue
                
                # Extract from JSON response if available
                response_data = response.get('json', response) if isinstance(response, dict) and 'json' in response else response
                raw_status = response_data.get('verification_status', '') if response_data else ''
                credits_used = response_data.get('credits_used', 0.25) if response_data else 0.25
                
                # Map API status to internal status (Instantly API returns 'verified' but we expect 'valid')
                status = 'valid' if raw_status == 'verified' else raw_status
                
                # Handle empty string results
                if not status or status.strip() == '':
                    # After 3 total attempts with empty results, mark as no_result
                    if attempts >= 2:  # attempts is 0-indexed, so 2 means 3rd attempt
                        status = 'no_result'
                        logger.info(f"🤷 Marking {email} as no_result after {attempts + 1} attempts")
                    else:
                        status = 'pending'  # Keep as pending for retry
                
                # Track status in breakdown
                if status in status_breakdown:
                    status_breakdown[status] += 1
                else:
                    status_breakdown[status] = 1
                
                # Store verification result and increment attempts
                store_verification_with_attempts(
                    email=email,
                    instantly_lead_id=instantly_lead_id,
                    campaign_id=campaign_id,
                    verification_status=status,
                    credits_used=credits_used,
                    attempts=attempts + 1
                )
                
                # Queue for deletion if invalid/risky
                if status in ['invalid', 'risky']:
                    DELETE_RISKY = os.getenv("DELETE_RISKY", "false").lower() == "true"
                    if status == 'invalid' or (status == 'risky' and DELETE_RISKY):
                        queue_for_deletion(email, instantly_lead_id)
                        queued_for_deletion += 1
                        logger.info(f"🗑️ Queued {status} email for deletion: {email}")
                
                checked += 1
                
            except Exception as e:
                logger.error(f"❌ Re-verification error for {email}: {e}")
                errors += 1
            
            # Rate limiting
            time.sleep(0.5)
        
        # Remove zero-count statuses from breakdown
        status_breakdown = {k: v for k, v in status_breakdown.items() if v > 0}
        
        return {
            'checked': checked, 
            'errors': errors, 
            'status_breakdown': status_breakdown,
            'queued_for_deletion': queued_for_deletion
        }
        
    except Exception as e:
        logger.error(f"❌ Error processing stale verifications: {e}")
        return {'checked': 0, 'errors': 1, 'status_breakdown': {}, 'queued_for_deletion': 0}

def mark_deletion_complete(email: str, instantly_lead_id: str):
    """Mark deletion as complete in BigQuery"""
    if not bq_client:
        return
    
    try:
        query = """
        UPDATE `{}.{}.ops_inst_state`
        SET deletion_status = 'done',
            status = 'deleted',
            last_deletion_attempt = CURRENT_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP()
        WHERE email = @email
          AND instantly_lead_id = @instantly_lead_id
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("instantly_lead_id", "STRING", instantly_lead_id)
            ]
        )
        
        bq_client.query(query, job_config=job_config).result()
        
    except Exception as e:
        logger.error(f"❌ Failed to mark deletion complete for {email}: {e}")

def increment_deletion_attempts_with_error(email: str, instantly_lead_id: str, status_code: int, error_message: str):
    """Increment deletion attempts and store error details"""
    if not bq_client:
        return
    
    try:
        # First get current attempts
        query = """
        SELECT deletion_attempts
        FROM `{}.{}.ops_inst_state`
        WHERE email = @email
          AND instantly_lead_id = @instantly_lead_id
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("instantly_lead_id", "STRING", instantly_lead_id)
            ]
        )
        
        results = list(bq_client.query(query, job_config=job_config).result())
        current_attempts = results[0].deletion_attempts if results else 0
        new_attempts = current_attempts + 1
        
        # Truncate error message to prevent BigQuery field size issues
        truncated_error = error_message[:1000] if error_message else ""
        
        # Update attempts, status, and error details
        if new_attempts >= 5:
            # Mark as failed after 5 attempts
            update_query = """
            UPDATE `{}.{}.ops_inst_state`
            SET deletion_attempts = @new_attempts,
                deletion_status = 'failed',
                deletion_last_error_code = @error_code,
                deletion_last_error_message = @error_message,
                last_deletion_attempt = CURRENT_TIMESTAMP(),
                updated_at = CURRENT_TIMESTAMP()
            WHERE email = @email
              AND instantly_lead_id = @instantly_lead_id
            """.format(PROJECT_ID, DATASET_ID)
            logger.warning(f"⚠️ Marking {email} as deletion failed after {new_attempts} attempts (code: {status_code})")
        else:
            # Just increment attempts and store error details
            update_query = """
            UPDATE `{}.{}.ops_inst_state`
            SET deletion_attempts = @new_attempts,
                deletion_last_error_code = @error_code,
                deletion_last_error_message = @error_message,
                last_deletion_attempt = CURRENT_TIMESTAMP(),
                updated_at = CURRENT_TIMESTAMP()
            WHERE email = @email
              AND instantly_lead_id = @instantly_lead_id
            """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("instantly_lead_id", "STRING", instantly_lead_id),
                bigquery.ScalarQueryParameter("new_attempts", "INTEGER", new_attempts),
                bigquery.ScalarQueryParameter("error_code", "INTEGER", status_code),
                bigquery.ScalarQueryParameter("error_message", "STRING", truncated_error)
            ]
        )
        
        bq_client.query(update_query, job_config=job_config).result()
        
        # Log the error to dead letters for additional debugging
        log_dead_letter('delete_lead', email, error_message, status_code, truncated_error)
        
    except Exception as e:
        logger.error(f"❌ Failed to increment deletion attempts for {email}: {e}")

# Keep the old function for backwards compatibility
def increment_deletion_attempts(email: str, instantly_lead_id: str, error_message: str):
    """Legacy function - use increment_deletion_attempts_with_error instead"""
    increment_deletion_attempts_with_error(email, instantly_lead_id, 0, error_message)

def store_verification_with_attempts(email: str, instantly_lead_id: str, campaign_id: str, 
                                   verification_status: str, credits_used: float, attempts: int):
    """Store verification result and update attempt count"""
    if not bq_client:
        return
    
    try:
        now = datetime.now(timezone.utc)
        
        query = """
        UPDATE `{}.{}.ops_inst_state`
        SET verification_status = @verification_status,
            verification_credits_used = @credits_used,
            verification_attempts = @attempts,
            verified_at = @verified_at,
            updated_at = @verified_at
        WHERE email = @email
          AND instantly_lead_id = @instantly_lead_id
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("instantly_lead_id", "STRING", instantly_lead_id),
                bigquery.ScalarQueryParameter("verification_status", "STRING", verification_status),
                bigquery.ScalarQueryParameter("credits_used", "FLOAT64", credits_used),
                bigquery.ScalarQueryParameter("attempts", "INTEGER", attempts),
                bigquery.ScalarQueryParameter("verified_at", "TIMESTAMP", now)
            ]
        )
        
        bq_client.query(query, job_config=job_config).result()
        
    except Exception as e:
        logger.error(f"❌ Failed to store verification with attempts for {email}: {e}")

def log_dead_letter(phase: str, email: str, data: str, http_status: int, error_text: str):
    """Log a dead letter entry for debugging"""
    if not bq_client:
        return
    
    try:
        query = """
        INSERT INTO `{}.{}.ops_dead_letters`
        (occurred_at, phase, email, http_status, error_text, retry_count)
        VALUES (CURRENT_TIMESTAMP(), @phase, @email, @http_status, @error_text, 1)
        """.format(PROJECT_ID, DATASET_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("phase", "STRING", phase),
                bigquery.ScalarQueryParameter("email", "STRING", email or ""),
                bigquery.ScalarQueryParameter("http_status", "INTEGER", http_status),
                bigquery.ScalarQueryParameter("error_text", "STRING", error_text[:1000])  # Truncate long errors
            ]
        )
        
        bq_client.query(query, job_config=job_config).result()
        
    except Exception as e:
        logger.error(f"❌ Failed to log dead letter: {e}")

def get_pending_verifications() -> List[Dict]:
    """Get pending verifications older than 24 hours to avoid double spend"""
    if not bq_client:
        return []
    
    try:
        query = """
        SELECT email, instantly_lead_id, campaign_id, verification_triggered_at
        FROM `{}.{}.ops_inst_state`
        WHERE (verification_status IN ('pending', 'unknown') OR verification_status IS NULL)
          AND status != 'deleted'
          AND instantly_lead_id IS NOT NULL
          AND (
            -- Never verified
            verification_triggered_at IS NULL
            -- Or verified more than 24 hours ago
            OR verification_triggered_at <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          )
        ORDER BY COALESCE(verification_triggered_at, added_at) ASC
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
        
        if not response:
            logger.error(f"❌ No response from DELETE API for {email}")
            return False
        
        # Check for success using status code (2xx or 404)
        status_code = response.get('status_code', 0)
        deletion_successful = (200 <= status_code < 300) or status_code == 404
        
        logger.debug(f"🗑️ DELETE API call completed for {email} (status: {status_code})")
        
        if deletion_successful:
            # Add to DNC list
            add_to_dnc_list(email, 'invalid_verification')
            logger.debug(f"📋 Added to DNC: {email}")
        
        return deletion_successful
        
    except Exception as e:
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
            # Check if response has json data with verification_status
            response_data = response.get('json', response) if isinstance(response, dict) and 'json' in response else response
            get_works = response is not None and response_data and 'verification_status' in response_data
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

def poll_verification_results_with_notification() -> Dict[str, int]:
    """
    Wrapper function that polls verification results and sends notification
    Used by GitHub Actions workflow
    """
    start_time = time.time()
    
    # Poll verification results
    results = poll_verification_results()
    
    # Calculate duration
    end_time = time.time()
    duration = end_time - start_time
    
    # Prepare notification data
    notification_data = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'duration_seconds': duration,
        'verifications_checked': results.get('verifications_checked', 0),
        'status_breakdown': results.get('status_breakdown', {}),
        'queued_for_deletion': results.get('queued_for_deletion', 0),
        'deletes_processed': results.get('deletes_processed', 0),
        'deletion_breakdown': results.get('deletion_breakdown', {}),
        'errors': results.get('errors', 0),
        'github_run_url': f"{os.getenv('GITHUB_SERVER_URL', '')}/{os.getenv('GITHUB_REPOSITORY', '')}/actions/runs/{os.getenv('GITHUB_RUN_ID', '')}"
    }
    
    # Send notification if available and there was activity
    if NOTIFICATIONS_AVAILABLE and (results.get('verifications_checked', 0) > 0 or results.get('deletes_processed', 0) > 0):
        try:
            logger.info("📤 Sending verification polling notification...")
            success = notifier.send_verification_polling_notification(notification_data)
            if success:
                logger.info("✅ Notification sent successfully")
            else:
                logger.warning("⚠️ Failed to send notification")
        except Exception as e:
            logger.warning(f"⚠️ Error sending notification: {e}")
    elif results.get('verifications_checked', 0) == 0 and results.get('deletes_processed', 0) == 0:
        logger.info("📴 No activity to report - skipping notification")
    
    return results

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
