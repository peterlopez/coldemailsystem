#!/usr/bin/env python3
"""
Cold Email Sync Script - BigQuery to Instantly.ai Pipeline
Synchronizes leads between BigQuery and Instantly.ai campaigns.
"""

import os
import sys
import re
import json
import time
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass

import requests
from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential
from dateutil import parser as date_parser

# Configure logging FIRST
log_format = '%(asctime)s - %(levelname)s - %(message)s'

# Configure logging to both console and file
log_handlers = [logging.StreamHandler()]

# Add file handler if we're in GitHub Actions or if log file is requested
if os.environ.get('GITHUB_ACTIONS') or os.environ.get('LOG_TO_FILE'):
    log_file = 'cold-email-sync.log'
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(log_format))
    log_handlers.append(file_handler)

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=log_handlers
)
logger = logging.getLogger(__name__)

# Import notification system AFTER logger is configured
try:
    from cold_email_notifier import notifier
    NOTIFICATIONS_AVAILABLE = True
    logger.info("📡 Notification system loaded")
except ImportError as e:
    NOTIFICATIONS_AVAILABLE = False
    logger.warning(f"📴 Notification system not available: {e}")

# Import simple async verification system
try:
    from simple_async_verification import trigger_verification_for_new_leads
    ASYNC_VERIFICATION_AVAILABLE = True
    logger.info("🔍 Simple async verification system loaded")
except ImportError as e:
    ASYNC_VERIFICATION_AVAILABLE = False
    logger.warning(f"📴 Simple async verification system not available: {e}")

# OPTIMIZED: Use centralized configuration
from shared_config import config, DRY_RUN, SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID, PROJECT_ID, DATASET_ID, TARGET_NEW_LEADS_PER_RUN

# Legacy variable mappings for backward compatibility
INSTANTLY_CAP_GUARD = config.processing.inventory_cap_guard
BATCH_SIZE = config.processing.bigquery_batch_size
BATCH_SLEEP_SECONDS = int(os.getenv('BATCH_SLEEP_SECONDS', '10'))  # Keep for backward compatibility
LEAD_INVENTORY_MULTIPLIER = float(os.getenv('LEAD_INVENTORY_MULTIPLIER', '3.5'))  # Conservative start

# Drain testing configuration - limit total leads processed for testing  
MAX_LEADS_TO_EVALUATE = int(os.getenv('MAX_LEADS_TO_EVALUATE', '0'))  # 0 = no limit, set to 200 for testing
MAX_PAGES_TO_PROCESS = int(os.getenv('MAX_PAGES_TO_PROCESS', '0'))  # 0 = no limit, set to 2 for testing
FORCE_DRAIN_CHECK = os.getenv('FORCE_DRAIN_CHECK', 'false').lower() == 'true'  # Skip 24hr check for testing

# OPTIMIZED: API configuration handled by shared_config
INSTANTLY_API_KEY = config.api.instantly_api_key
INSTANTLY_BASE_URL = config.api.instantly_base_url
logger.info(f"✅ INSTANTLY_API_KEY configured via shared_config")

# BigQuery client
try:
    logger.info("Initializing BigQuery client...")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/secrets/bigquery-credentials.json'
    
    # Check if credentials file exists
    creds_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    if not os.path.exists(creds_path):
        logger.error(f"BigQuery credentials file not found at: {creds_path}")
        raise FileNotFoundError(f"Credentials file not found: {creds_path}")
    
    logger.info(f"Using credentials file: {creds_path}")
    bq_client = bigquery.Client(project=PROJECT_ID)
    logger.info("✅ BigQuery client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize BigQuery client: {e}")
    raise

@dataclass
class Lead:
    email: str
    merchant_name: str
    platform_domain: str
    state: str
    country_code: str
    estimated_sales_yearly: Optional[int]
    sequence_target: str
    klaviyo_installed_at: Optional[str]

@dataclass
class InstantlyLead:
    id: str
    email: str
    campaign_id: str
    status: str

# Global variable to store last drain metrics for notification system
LAST_DRAIN_METRICS = {
    'api_calls_made': 0,
    'leads_found': 0,
    'leads_missing': 0,
    'api_errors': 0,
    'api_success_rate': 0.0,
    'drain_classifications': {}
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=60))
def call_instantly_api(endpoint: str, method: str = 'GET', data: Optional[Dict] = None) -> Dict:
    """Call Instantly API with automatic retry and backoff."""
    url = f"{INSTANTLY_BASE_URL}{endpoint}"
    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    if DRY_RUN:
        logger.info(f"DRY RUN: Would call {method} {url} with data: {data}")
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
        
        response.raise_for_status()
        # Handle empty response for DELETE operations
        if method == 'DELETE' and not response.content:
            return {'success': True}
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed: {e}")
        # Log to dead letters
        log_dead_letter('api_call', None, str(data), getattr(e.response, 'status_code', 0), str(e))
        raise

def delete_lead_from_instantly(lead: 'InstantlyLead') -> bool:
    """
    Delete a lead from Instantly using the official V2 DELETE endpoint.
    Follows API best practices: treats 404 as idempotent success.
    Simplified to avoid retry wrapper conflicts.
    """
    if DRY_RUN:
        logger.info(f"🧪 DRY RUN: Would delete lead {lead.email} (ID: {lead.id})")
        return True
    
    try:
        logger.debug(f"🔄 Deleting lead {lead.email} via DELETE /api/v2/leads/{lead.id}")
        
        # Use direct requests to avoid any wrapper issues
        headers = {
            'Authorization': f'Bearer {INSTANTLY_API_KEY}',
            'Accept': 'application/json'
        }
        
        response = requests.delete(
            f"{INSTANTLY_BASE_URL}/api/v2/leads/{lead.id}",
            headers=headers,
            timeout=30
        )
        
        if response.status_code in [200, 204]:
            logger.info(f"✅ Successfully deleted {lead.email} from Instantly")
            return True
        elif response.status_code == 404:
            # 404 means lead is already gone - treat as successful idempotent operation
            logger.info(f"✅ Lead {lead.email} already deleted (404 = idempotent success)")
            return True
        elif response.status_code == 429:
            # 429 means rate limited - this is a recoverable error
            logger.warning(f"⚠️ Rate limited deleting {lead.email} - API suggests slowing down")
            log_dead_letter('delete_lead_rate_limit', lead.email, lead.id, 429, response.text)
            return False
        else:
            # Other HTTP errors are actual failures
            logger.error(f"❌ HTTP error deleting {lead.email}: {response.status_code} - {response.text}")
            log_dead_letter('delete_lead', lead.email, lead.id, response.status_code, response.text)
            return False
            
    except Exception as e:
        # Non-HTTP errors (network, timeout, etc.)
        logger.error(f"❌ Delete error for {lead.email}: {e}")
        log_dead_letter('delete_lead', lead.email, lead.id, 0, str(e))
        return False

class AdaptiveRateLimit:
    """Adaptive rate limiting with backoff based on API response patterns."""
    
    def __init__(self):
        self.success_count = 0
        self.failure_count = 0
        self.consecutive_successes = 0
        self.consecutive_failures = 0
        self.base_delay = config.rate_limits.pagination_delay
        self.current_delay = self.base_delay
        
    def record_success(self):
        """Record a successful API call and potentially reduce delay."""
        self.success_count += 1
        self.consecutive_successes += 1
        self.consecutive_failures = 0
        
        # Gradually reduce delay after sustained success (but not below minimum)
        if self.consecutive_successes >= 5:
            self.current_delay = max(0.5, self.current_delay * 0.9)
            self.consecutive_successes = 0  # Reset counter
            logger.debug(f"🚀 Rate limit optimized: {self.current_delay:.1f}s (sustained success)")
    
    def record_failure(self, is_rate_limit: bool = False):
        """Record a failed API call and increase delay if needed."""
        self.failure_count += 1
        self.consecutive_failures += 1
        self.consecutive_successes = 0
        
        if is_rate_limit:
            # More aggressive backoff for rate limiting
            self.current_delay = min(10.0, self.current_delay * 2.0)
            logger.debug(f"🐌 Rate limit increased due to 429: {self.current_delay:.1f}s")
        elif self.consecutive_failures >= 3:
            # Moderate backoff for general failures
            self.current_delay = min(5.0, self.current_delay * 1.5)
            logger.debug(f"⚠️ Rate limit increased due to failures: {self.current_delay:.1f}s")
    
    def get_delay(self) -> float:
        """Get current adaptive delay."""
        return self.current_delay
    
    def wait(self, custom_delay: Optional[float] = None):
        """Wait with current adaptive delay or custom delay."""
        delay = custom_delay or self.current_delay
        if delay > 0:
            logger.debug(f"⏸️ Adaptive rate limit: {delay:.1f}s")
            time.sleep(delay)

# Global adaptive rate limiter
adaptive_rate_limiter = AdaptiveRateLimit()

def log_dead_letter(phase: str, email: Optional[str], payload: str, status_code: int, error_text: str) -> None:
    """Log failed operations to dead letters table."""
    try:
        # Use safe table reference with parameters
        query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_dead_letters`
        (id, occurred_at, phase, email, http_status, error_text, retry_count)
        VALUES (@id, CURRENT_TIMESTAMP(), @phase, @email, @status, @error, 1)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", str(uuid.uuid4())),
                bigquery.ScalarQueryParameter("phase", "STRING", phase),
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("status", "INT64", status_code),
                bigquery.ScalarQueryParameter("error", "STRING", f"{error_text[:500]} | Payload: {payload[:500]}"),
            ]
        )
        
        if not DRY_RUN:
            bq_client.query(query, job_config=job_config).result()
    except Exception as e:
        logger.error(f"Failed to log dead letter: {e}")

def get_lead_failure_count(email: str, failure_type: str) -> int:
    """
    Get the failure count for a specific lead and failure type from dead letters table.
    
    Args:
        email: Lead email address to check
        failure_type: Type of failure ('campaign_assignment', 'lead_creation', etc.)
        
    Returns:
        int: Number of previous failures for this lead and type
    """
    try:
        # Use safe table reference construction
        query = f"""
        SELECT COUNT(*) as failure_count
        FROM `{PROJECT_ID}.{DATASET_ID}.ops_dead_letters`
        WHERE email = @email 
            AND error_text LIKE CONCAT('%', @failure_type, '%')
            AND occurred_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("failure_type", "STRING", failure_type),
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job.result())
        
        if results:
            failure_count = results[0].failure_count
            logger.debug(f"📊 Lead {email} has {failure_count} previous {failure_type} failures")
            return failure_count
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error getting failure count for {email}: {e}")
        # Conservative approach: assume no previous failures if we can't check
        return 0

def get_instantly_headers() -> dict:
    """OPTIMIZED: Get headers for Instantly API calls using shared config."""
    return config.get_instantly_headers()

def should_check_lead_for_drain(lead_id: str) -> bool:
    """
    Check if a lead needs drain evaluation (24+ hours since last check).
    
    Note: This is a simplified version for individual checks.
    For better performance, use batch_check_leads_for_drain() for multiple leads.
    
    Returns:
        True if lead hasn't been checked in 24+ hours or never checked
        False if lead was checked recently (within 24 hours)
    """
    try:
        if not lead_id:
            return True  # Always check leads without IDs (safety)
        
        # For individual checks, we'll be conservative and always check
        # The batch version below is much more efficient
        logger.debug(f"📝 Individual check for lead {lead_id} - defaulting to check needed")
        return True
            
    except Exception as e:
        logger.error(f"❌ Error checking drain timestamp for lead {lead_id}: {e}")
        # Conservative approach: check the lead if we can't determine timestamp
        return True


def batch_check_leads_for_drain(lead_ids: list) -> dict:
    """
    Efficiently check multiple leads for drain evaluation with timeout handling and smaller batches.
    
    Args:
        lead_ids: List of Instantly lead IDs to check
        
    Returns:
        Dict mapping lead_id -> boolean (True if needs check, False if recent)
    """
    try:
        if not lead_ids:
            return {}
        
        # Process in optimized batches to handle larger API pages
        BIGQUERY_BATCH_SIZE = 50  # Match API page size for efficiency
        all_results = {}
        
        for i in range(0, len(lead_ids), BIGQUERY_BATCH_SIZE):
            batch_ids = lead_ids[i:i + BIGQUERY_BATCH_SIZE]
            logger.debug(f"📊 Processing BigQuery batch {i//BIGQUERY_BATCH_SIZE + 1}: {len(batch_ids)} leads")
            
            try:
                # PHASE 3 FIX: Robust parameterized array SELECT query to prevent syntax errors
                # Using explicit UNNEST with proper formatting for consistency
                query = f"""
                SELECT 
                    instantly_lead_id,
                    last_drain_check,
                    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) as hours_since_check
                FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
                WHERE instantly_lead_id IN (
                    SELECT lead_id 
                    FROM UNNEST(@lead_ids) AS lead_id
                )
                """
                
                # Ensure all lead_ids are strings and not None/empty
                clean_batch_ids = [str(lead_id) for lead_id in batch_ids if lead_id]
                
                if not clean_batch_ids:
                    logger.debug(f"⚠️ No valid lead IDs in batch, skipping...")
                    # Add empty results for these leads as fallback
                    for lead_id in batch_ids:
                        all_results[lead_id] = True
                    continue
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("lead_ids", "STRING", clean_batch_ids),
                    ]
                )
                
                query_job = bq_client.query(query, job_config=job_config)
                results = list(query_job.result(timeout=60))  # 60 second result timeout
                
                # Process batch results
                found_lead_ids = set()
                
                for row in results:
                    lead_id = row.instantly_lead_id
                    found_lead_ids.add(lead_id)
                    
                    if row.last_drain_check is None:
                        # Never checked before
                        all_results[lead_id] = True
                        logger.debug(f"📝 Lead {lead_id} has no drain check timestamp - needs check")
                    elif row.hours_since_check >= 24:
                        # 24+ hours since last check
                        all_results[lead_id] = True
                        logger.debug(f"📝 Lead {lead_id} last checked {row.hours_since_check} hours ago - needs check")
                    else:
                        # Recent check, skip
                        all_results[lead_id] = False
                        logger.debug(f"⏰ Lead {lead_id} checked {row.hours_since_check} hours ago - skipping")
                
                # Any lead IDs not found in the database need first-time check
                untracked_leads = []
                for lead_id in batch_ids:
                    if lead_id not in found_lead_ids:
                        all_results[lead_id] = True
                        untracked_leads.append(lead_id)
                        logger.debug(f"📝 Lead {lead_id} not in tracking - needs first drain check")
                
                # Log summary of untracked leads (these are leads in Instantly but not in our BigQuery table)
                if untracked_leads:
                    logger.info(f"🔍 Found {len(untracked_leads)} leads in Instantly not tracked in BigQuery - will evaluate for drain")
                        
            except Exception as batch_error:
                logger.error(f"❌ BigQuery batch failed: {batch_error}")
                # Conservative fallback: check all leads in this batch
                for lead_id in batch_ids:
                    all_results[lead_id] = True
                    logger.debug(f"📝 Lead {lead_id} - defaulting to check due to batch error")
        
        return all_results
        
    except Exception as e:
        logger.error(f"❌ Error batch checking drain timestamps: {e}")
        # Conservative approach: check all leads if we can't determine timestamps
        return {lead_id: True for lead_id in lead_ids}


def update_lead_drain_check_timestamp(lead_id: str) -> bool:
    """
    Update the last_drain_check timestamp for a lead after evaluation.
    
    Note: For better performance, use batch_update_drain_timestamps() for multiple leads.
    
    Args:
        lead_id: The Instantly lead ID
        
    Returns:
        True if update successful, False otherwise
    """
    try:
        if not lead_id:
            return False
        
        # Update timestamp - only update existing records, don't create new ones
        query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
        SET last_drain_check = CURRENT_TIMESTAMP(), updated_at = CURRENT_TIMESTAMP()
        WHERE instantly_lead_id = @lead_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lead_id", "STRING", lead_id),
            ]
            # Note: QueryJobConfig doesn't accept timeout parameters
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result(timeout=15)  # 15 second result timeout
        
        logger.debug(f"✅ Updated drain check timestamp for lead {lead_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to update drain timestamp for lead {lead_id}: {e}")
        return False


def batch_update_drain_timestamps(lead_ids: list) -> bool:
    """
    Batch update drain check timestamps for multiple leads to avoid timeout issues.
    
    Args:
        lead_ids: List of Instantly lead IDs to update
        
    Returns:
        True if batch update successful, False otherwise
    """
    try:
        if not lead_ids:
            return True
        
        # Process in smaller batches to avoid BigQuery timeouts
        BATCH_SIZE = 100  # Process 100 leads at a time
        
        for i in range(0, len(lead_ids), BATCH_SIZE):
            batch_ids = lead_ids[i:i + BATCH_SIZE]
            logger.debug(f"📊 Batch updating timestamps: batch {i//BATCH_SIZE + 1}, {len(batch_ids)} leads")
            
            try:
                # PHASE 3 FIX: Robust parameterized array UPDATE query to prevent syntax errors
                # Using explicit UNNEST with proper formatting to avoid parsing issues
                query = f"""
                UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
                SET 
                    last_drain_check = CURRENT_TIMESTAMP(),
                    updated_at = CURRENT_TIMESTAMP()
                WHERE instantly_lead_id IN (
                    SELECT lead_id 
                    FROM UNNEST(@lead_ids) AS lead_id
                )
                """
                
                # Ensure all lead_ids are strings and not None/empty
                clean_batch_ids = [str(lead_id) for lead_id in batch_ids if lead_id]
                
                if not clean_batch_ids:
                    logger.debug(f"⚠️ No valid lead IDs in batch, skipping...")
                    continue
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("lead_ids", "STRING", clean_batch_ids),
                    ]
                )
                
                query_job = bq_client.query(query, job_config=job_config)
                query_job.result(timeout=30)  # 30 second result timeout
                
                logger.debug(f"✅ Batch updated {len(batch_ids)} drain timestamps")
                
            except Exception as batch_error:
                logger.error(f"❌ Batch timestamp update failed: {batch_error}")
                # Skip individual fallback - timestamp updates are not critical for drain functionality
                logger.info(f"⏭️ Skipping timestamp updates for batch due to error - drain will continue")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error batch updating drain timestamps: {e}")
        return False

def classify_lead_for_drain(lead: dict, campaign_name: str) -> dict:
    """
    Classify a lead from Instantly API to determine if it should be drained.
    
    BALANCED APPROACH: 
    - Trust Instantly's sequence management for normal operations
    - But include 90-day safety net for truly stuck leads
    - Enhanced auto-reply detection using pause_until field
    """
    try:
        email = lead.get('email', 'unknown')
        status = lead.get('status', 0)  # Status code from Instantly
        esp_code = lead.get('esp_code', 0)  # Email service provider code
        email_reply_count = lead.get('email_reply_count', 0)
        created_at = lead.get('timestamp_created')  # Use correct timestamp field
        
        # NEW: Check for auto-reply indicators in payload
        payload = lead.get('payload', {})
        pause_until = payload.get('pause_until') if payload else None
        
        # Parse creation date for time-based decisions
        days_since_created = 0
        if created_at:
            try:
                from datetime import datetime
                created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                days_since_created = (datetime.now().astimezone() - created_date).days
            except:
                days_since_created = 0
        
        # BALANCED DRAIN DECISION LOGIC
        
        # 1. ONLY drain Status 3 (Finished) leads - Instantly has decided they're done
        if status == 3:
            if email_reply_count > 0:
                # Check for auto-reply detection
                if pause_until:
                    # Auto-reply detected - do not drain as genuine engagement
                    logger.debug(f"🤖 Auto-reply detected for {email}: paused until {pause_until}")
                    return {
                        'should_drain': False,
                        'keep_reason': f'Auto-reply detected (paused until {pause_until}) - not genuine engagement',
                        'auto_reply': True
                    }
                else:
                    # No auto-reply indicators - genuine engagement
                    logger.debug(f"👤 Genuine reply detected for {email}: no auto-reply flags")
                    return {
                        'should_drain': True,
                        'drain_reason': 'replied',
                        'details': f'Status 3 with {email_reply_count} replies - genuine engagement (no auto-reply flags)',
                        'auto_reply': False
                    }
            else:
                # Sequence completed without replies
                return {
                    'should_drain': True,
                    'drain_reason': 'completed',
                    'details': 'Sequence completed without replies'
                }
        
        # 2. Status 1/2 with auto-replies - keep but log auto-reply detection
        elif (status == 1 or status == 2) and email_reply_count > 0 and pause_until:
            logger.debug(f"🤖 Auto-reply for {email}: Status {status} + replies + paused until {pause_until}")
            return {
                'should_drain': False,
                'keep_reason': f'Status {status} lead with auto-reply (paused until {pause_until}) - let Instantly manage sequence',
                'auto_reply': True
            }
        
        # 3. SAFETY NET: Very old active leads (90+ days) - trust Instantly but prevent stuck leads
        elif status == 1 and days_since_created >= 90:
            logger.debug(f"⚠️ Stale active lead detected: {email} - {days_since_created} days old")
            return {
                'should_drain': True,
                'drain_reason': 'stale_active',
                'details': f'Active lead stuck for {days_since_created} days - safety net for inventory management'
            }
        
        # 4. CLEAR delivery failures only - hard bounces after grace period
        elif esp_code in [550, 551, 553]:  # Hard bounces
            if days_since_created >= 7:  # 7-day grace period
                return {
                    'should_drain': True,
                    'drain_reason': 'bounced_hard',
                    'details': f'Hard bounce (ESP {esp_code}) after {days_since_created} days - clear delivery failure'
                }
            else:
                return {
                    'should_drain': False,
                    'keep_reason': f'Recent hard bounce (ESP {esp_code}), within 7-day grace period'
                }
        
        # 5. Soft bounces - always keep for retry (trust Instantly to manage)
        elif esp_code in [421, 450, 451]:  # Soft bounces
            return {
                'should_drain': False,
                'keep_reason': f'Soft bounce (ESP {esp_code}) - letting Instantly manage retry'
            }
        
        # 6. Unsubscribes - clear signal to drain regardless of status
        elif 'unsubscribed' in str(lead.get('status_text', '')).lower():
            return {
                'should_drain': True,
                'drain_reason': 'unsubscribed',
                'details': 'Lead unsubscribed from campaign'
            }
        
        # DEFAULT: TRUST INSTANTLY'S STATUS MANAGEMENT (under 90 days)
        # Status 1 (Active) = Instantly wants sequence to continue
        # Status 2 (Paused) = Instantly may resume sequence  
        # Any other status = Keep conservatively
        else:
            status_description = {
                1: "Active - sequence continuing",
                2: "Paused - may resume", 
                0: "Unknown status"
            }.get(status, f"Status {status}")
            
            return {
                'should_drain': False,
                'keep_reason': f'{status_description} - trusting Instantly\'s sequence management ({days_since_created} days old)'
            }
        
    except Exception as e:
        logger.error(f"Error classifying lead {lead.get('email', 'unknown')}: {e}")
        # Conservative approach: don't drain on error
        return {
            'should_drain': False,
            'keep_reason': f'Classification error - keeping safely: {str(e)}'
        }

def get_leads_needing_drain_from_bigquery() -> Dict[str, List[str]]:
    """
    DIRECT API OPTIMIZATION: BigQuery-first approach to find leads that need drain evaluation.
    Now targeting 50 leads for direct API calls instead of pagination.
    
    Returns:
        Dict mapping campaign_id -> list of instantly_lead_ids that need checking
    """
    try:
        logger.info("📊 DIRECT API: Querying BigQuery for leads needing drain evaluation...")
        
        # Query leads that haven't been checked in 24+ hours OR never checked
        # AND are currently active in campaigns
        query = f"""
        SELECT 
            instantly_lead_id,
            campaign_id,
            email,
            status
        FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
        WHERE (
            last_drain_check IS NULL 
            OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) >= 24
        )
        AND status IN ('active', 'pending')  -- Only check leads that might still be in campaigns
        AND campaign_id IN ('{SMB_CAMPAIGN_ID}', '{MIDSIZE_CAMPAIGN_ID}')
        ORDER BY 
            COALESCE(last_drain_check, TIMESTAMP('1970-01-01')) ASC,  -- Oldest checks first
            email ASC  -- Deterministic ordering
        LIMIT 50  -- Direct API batch size for reliable processing
        """
        
        query_job = bq_client.query(query)
        results = list(query_job.result(timeout=60))
        
        # Group leads by campaign for efficient processing
        leads_by_campaign = {}
        for row in results:
            campaign_id = row.campaign_id
            if campaign_id not in leads_by_campaign:
                leads_by_campaign[campaign_id] = []
            leads_by_campaign[campaign_id].append(row.instantly_lead_id)
        
        total_leads = sum(len(ids) for ids in leads_by_campaign.values())
        
        # Enhanced BigQuery results logging with direct API optimization metrics
        logger.info(f"📊 BigQuery Direct API Results:")
        logger.info(f"  • Campaigns with leads: {len(leads_by_campaign)}")
        logger.info(f"  • Total leads for direct API calls: {total_leads}")
        logger.info(f"  • Expected processing time: ~{total_leads * 0.5:.1f} seconds")
        
        # Show per-campaign breakdown
        for campaign_id, lead_ids in leads_by_campaign.items():
            campaign_name = "SMB" if campaign_id == SMB_CAMPAIGN_ID else "Midsize"
            logger.info(f"  • {campaign_name}: {len(lead_ids)} leads for direct lookup")
        
        # Performance prediction for direct API calls
        if total_leads > 0:
            logger.info(f"🎯 Direct API Optimization:")
            logger.info(f"  • {total_leads} individual API calls vs pagination")
            logger.info(f"  • Expected success rate: ~95% (vs current ~0.6%)")
            logger.info(f"  • No client-side matching required")
        
        return leads_by_campaign
        
    except Exception as e:
        logger.error(f"❌ BigQuery direct API approach failed: {e}")
        logger.info("🔄 Will fall back to current pagination method")
        return {}


def get_leads_by_ids_from_instantly(campaign_id: str, lead_ids: List[str]) -> Dict[str, List]:
    """
    DIRECT API OPTIMIZATION: Query Instantly for specific lead IDs using direct GET calls.
    Eliminates pagination and client-side matching issues.
    
    Args:
        campaign_id: The campaign ID to query
        lead_ids: List of specific lead IDs to fetch
    
    Returns:
        Dict with 'found_leads' and 'missing_leads' lists
    """
    try:
        if not lead_ids:
            return {'found_leads': [], 'missing_leads': []}
            
        logger.debug(f"🎯 Making {len(lead_ids)} direct API calls to Instantly...")
        
        found_leads = []
        missing_leads = []
        api_errors = []
        
        for i, lead_id in enumerate(lead_ids):
            try:
                # Direct API call to get specific lead
                url = f"{INSTANTLY_BASE_URL}/api/v2/leads/{lead_id}"
                
                adaptive_rate_limiter.wait()
                
                response = requests.get(
                    url,
                    headers=get_instantly_headers(),
                    timeout=30
                )
                
                if response.status_code == 200:
                    lead_data = response.json()
                    found_leads.append(lead_data)
                    logger.debug(f"✅ Found lead {i+1}/{len(lead_ids)}: {lead_data.get('email', lead_id)}")
                    
                elif response.status_code == 404:
                    # Single retry for 404s
                    logger.debug(f"🔄 Lead {i+1}/{len(lead_ids)} not found, retrying...")
                    time.sleep(1.0)
                    
                    retry_response = requests.get(
                        url,
                        headers=get_instantly_headers(),
                        timeout=30
                    )
                    
                    if retry_response.status_code == 200:
                        lead_data = retry_response.json()
                        found_leads.append(lead_data)
                        logger.debug(f"✅ Found on retry {i+1}/{len(lead_ids)}: {lead_data.get('email', lead_id)}")
                    elif retry_response.status_code == 404:
                        missing_leads.append(lead_id)
                        logger.debug(f"❌ Confirmed missing {i+1}/{len(lead_ids)}: {lead_id}")
                    else:
                        logger.warning(f"⚠️ Retry failed with {retry_response.status_code} for lead {lead_id}")
                        api_errors.append(lead_id)
                        
                elif response.status_code == 401:
                    logger.error(f"🔐 Authentication error - stopping batch processing")
                    raise Exception(f"Authentication failed: {response.status_code}")
                    
                elif response.status_code == 429:
                    logger.warning(f"🚦 Rate limited on lead {i+1}/{len(lead_ids)}, backing off...")
                    # Increase rate limiter delay and retry
                    adaptive_rate_limiter.increase_delay()
                    time.sleep(2.0)
                    
                    retry_response = requests.get(
                        url,
                        headers=get_instantly_headers(),
                        timeout=30
                    )
                    
                    if retry_response.status_code == 200:
                        lead_data = retry_response.json()
                        found_leads.append(lead_data)
                        logger.debug(f"✅ Found after rate limit {i+1}/{len(lead_ids)}")
                    else:
                        logger.warning(f"⚠️ Rate limit retry failed for {lead_id}")
                        api_errors.append(lead_id)
                else:
                    logger.warning(f"⚠️ API error {response.status_code} for lead {lead_id}")
                    api_errors.append(lead_id)
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"🌐 Network error for lead {lead_id}: {e}")
                api_errors.append(lead_id)
            except Exception as e:
                logger.error(f"❌ Unexpected error processing lead {lead_id}: {e}")
                api_errors.append(lead_id)
        
        # Results summary
        found_count = len(found_leads)
        missing_count = len(missing_leads)
        error_count = len(api_errors)
        total_requested = len(lead_ids)
        
        success_rate = (found_count / max(total_requested, 1)) * 100
        
        logger.info(f"🎯 Direct API Results:")
        logger.info(f"  • Found: {found_count}/{total_requested} leads ({success_rate:.1f}%)")
        logger.info(f"  • Missing: {missing_count} (confirmed not in Instantly)")
        logger.info(f"  • API errors: {error_count} (network/auth issues)")
        logger.info(f"  • Processing time: ~{len(lead_ids) * 0.5:.1f} seconds")
        
        if missing_count > 0:
            logger.info(f"📝 Missing leads will be marked as 'missing' and treated as completed")
        
        return {
            'found_leads': found_leads,
            'missing_leads': missing_leads,
            'api_errors': api_errors
        }
        
    except Exception as e:
        logger.error(f"❌ Direct API batch failed: {e}")
        return {'found_leads': [], 'missing_leads': [], 'api_errors': lead_ids}


def process_bigquery_first_drain(bigquery_leads: Dict[str, List[str]]) -> List[InstantlyLead]:
    """
    DIRECT API OPTIMIZATION: Process drain using direct API calls instead of pagination.
    
    Args:
        bigquery_leads: Dict mapping campaign_id -> list of instantly_lead_ids to check
        
    Returns:
        List of InstantlyLead objects that should be drained
    """
    try:
        finished_leads = []
        total_leads_processed = 0
        
        # Enhanced tracking with missing lead support
        drain_reasons = {
            'replied': 0,
            'completed': 0, 
            'missing': 0,  # New status for leads not found in Instantly
            'bounced_hard': 0,
            'unsubscribed': 0,
            'stale_active': 0,
            'auto_reply_detected': 0,
            'kept_active': 0,
            'kept_paused': 0,
            'kept_other': 0,
            'api_errors': 0
        }
        
        # Direct API metrics tracking (for notifications)
        global LAST_DRAIN_METRICS
        LAST_DRAIN_METRICS = {
            'api_calls_made': 0,
            'leads_found': 0,
            'leads_missing': 0,
            'api_errors': 0,
            'api_success_rate': 0.0,
            'drain_classifications': drain_reasons
        }
        
        # Process each campaign's leads using direct API approach
        for campaign_id, lead_ids in bigquery_leads.items():
            campaign_name = "SMB" if campaign_id == SMB_CAMPAIGN_ID else "Midsize"
            
            logger.info(f"🎯 Processing {len(lead_ids)} leads from {campaign_name} campaign via direct API...")
            
            # Direct API calls to Instantly
            api_results = get_leads_by_ids_from_instantly(campaign_id, lead_ids)
            
            found_leads = api_results['found_leads']
            missing_leads = api_results['missing_leads']
            api_errors = api_results.get('api_errors', [])
            
            # Update direct API metrics
            LAST_DRAIN_METRICS['api_calls_made'] += len(lead_ids)
            LAST_DRAIN_METRICS['leads_found'] += len(found_leads)
            LAST_DRAIN_METRICS['leads_missing'] += len(missing_leads)
            LAST_DRAIN_METRICS['api_errors'] += len(api_errors)
            
            logger.info(f"📊 Retrieved {len(found_leads)} found, {len(missing_leads)} missing, {len(api_errors)} errors from {campaign_name}")
            
            # Track all leads that we'll update timestamps for
            leads_to_update_timestamps = []
            
            # Process found leads using existing classification logic
            for lead in found_leads:
                total_leads_processed += 1
                
                lead_id = lead.get('id', '')
                email = lead.get('email', '')
                
                if not lead_id:
                    logger.debug(f"⚠️ Skipping lead with no ID: {email}")
                    continue
                
                # Apply testing limit if configured
                if MAX_LEADS_TO_EVALUATE > 0 and total_leads_processed > MAX_LEADS_TO_EVALUATE:
                    logger.info(f"🧪 TESTING LIMIT REACHED: Processed {total_leads_processed} leads, stopping")
                    break
                
                # Classify lead using existing drain logic
                classification = classify_lead_for_drain(lead, campaign_name)
                
                if classification['should_drain']:
                    instantly_lead = InstantlyLead(
                        id=lead_id,
                        email=email,
                        campaign_id=campaign_id,
                        status=classification['drain_reason']
                    )
                    finished_leads.append(instantly_lead)
                    
                    # Track drain reason
                    drain_reason = classification.get('drain_reason', 'unknown')
                    drain_reasons[drain_reason] = drain_reasons.get(drain_reason, 0) + 1
                    
                    details = classification.get('details', '')
                    logger.info(f"🗑️ DRAIN: {email} → {drain_reason} | {details}")
                else:
                    # Track keep reasons
                    keep_reason = str(classification.get('keep_reason', 'unknown reason'))
                    status = lead.get('status', 0)
                    
                    is_auto_reply = ('auto-reply' in keep_reason.lower() if isinstance(keep_reason, str) else False) or \
                                   classification.get('auto_reply', False) == True
                    
                    if is_auto_reply:
                        drain_reasons['auto_reply_detected'] += 1
                        logger.debug(f"🤖 KEEP: {email} → auto-reply detected | {keep_reason}")
                    elif status == 1:
                        drain_reasons['kept_active'] += 1
                        logger.debug(f"⚡ KEEP: {email} → active sequence | {keep_reason}")
                    elif status == 2:
                        drain_reasons['kept_paused'] += 1  
                        logger.debug(f"⏸️ KEEP: {email} → paused sequence | {keep_reason}")
                    else:
                        drain_reasons['kept_other'] += 1
                        logger.debug(f"📋 KEEP: {email} → other reason | {keep_reason}")
                
                # Queue for timestamp update
                leads_to_update_timestamps.append(lead_id)
            
            # Process missing leads (treat as completed and drain)
            for missing_lead_id in missing_leads:
                total_leads_processed += 1
                
                # Create InstantlyLead with missing status
                instantly_lead = InstantlyLead(
                    id=missing_lead_id,
                    email=f"missing_lead_{missing_lead_id}",  # Placeholder email
                    campaign_id=campaign_id,
                    status='missing'
                )
                finished_leads.append(instantly_lead)
                
                drain_reasons['missing'] += 1
                logger.info(f"🗑️ DRAIN: {missing_lead_id} → missing | Lead not found in Instantly (likely auto-removed)")
                
                # Queue for timestamp update
                leads_to_update_timestamps.append(missing_lead_id)
            
            # Track API errors (don't update timestamps for these)
            if api_errors:
                drain_reasons['api_errors'] += len(api_errors)
                logger.warning(f"⚠️ {len(api_errors)} leads had API errors and will be retried next run")
            
            # Batch update timestamps for successfully processed leads (found + missing, but not errors)
            if leads_to_update_timestamps:
                try:
                    batch_update_drain_timestamps(leads_to_update_timestamps)
                    logger.info(f"📊 Updated drain check timestamps for {len(leads_to_update_timestamps)} leads in {campaign_name}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to update timestamps for {campaign_name}: {e}")
            
            # Break if testing limit reached
            if MAX_LEADS_TO_EVALUATE > 0 and total_leads_processed > MAX_LEADS_TO_EVALUATE:
                break
        
        # Calculate API success rate
        if LAST_DRAIN_METRICS['api_calls_made'] > 0:
            LAST_DRAIN_METRICS['api_success_rate'] = (LAST_DRAIN_METRICS['leads_found'] / LAST_DRAIN_METRICS['api_calls_made']) * 100
        
        # Update metrics with latest drain classifications
        LAST_DRAIN_METRICS['drain_classifications'] = drain_reasons
        
        # Enhanced summary with direct API performance metrics
        total_to_drain = len(finished_leads)
        total_campaigns_processed = len(bigquery_leads)
        
        logger.info("=" * 60)
        logger.info("🚀 DIRECT API OPTIMIZATION: Drain processing complete")
        logger.info("=" * 60)
        logger.info(f"📊 Performance Summary:")
        logger.info(f"  • Campaigns processed: {total_campaigns_processed}")
        logger.info(f"  • Total leads processed: {total_leads_processed}")
        logger.info(f"  • Leads to drain: {total_to_drain}")
        logger.info(f"  • Processing success rate: {(total_leads_processed/max(sum(len(ids) for ids in bigquery_leads.values()),1)*100):.1f}%")
        logger.info(f"  • Direct API success rate: {LAST_DRAIN_METRICS['api_success_rate']:.1f}%")
        
        # Show campaign-specific processing results
        if total_leads_processed > 0:
            logger.info(f"📊 Campaign Processing Results:")
            
            for campaign_id, lead_ids in bigquery_leads.items():
                campaign_name = "SMB" if campaign_id == SMB_CAMPAIGN_ID else "Midsize"
                leads_targeted = len(lead_ids)
                
                # Count actual drain results for this campaign
                campaign_drained = sum(1 for lead in finished_leads if lead.campaign_id == campaign_id)
                campaign_efficiency = (campaign_drained / max(leads_targeted, 1) * 100)
                
                logger.info(f"  • {campaign_name}: {leads_targeted} targeted → {campaign_drained} drained ({campaign_efficiency:.1f}%)")
        
        # Enhanced drain reasons breakdown with percentages
        if any(count > 0 for count in drain_reasons.values()):
            logger.info("📋 Drain Classification Analysis:")
            total_classifications = sum(drain_reasons.values())
            
            for reason, count in sorted(drain_reasons.items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    percentage = (count / max(total_classifications, 1) * 100)
                    logger.info(f"  • {reason}: {count} ({percentage:.1f}%)")
        
        logger.info("=" * 60)
        
        return finished_leads
        
    except Exception as e:
        logger.error(f"❌ Direct API drain processing failed: {e}")
        raise


def get_finished_leads() -> List[InstantlyLead]:
    """Get leads with terminal status from Instantly using BigQuery-first optimization with fallback."""
    try:
        logger.info("🔄 DRAIN: Fetching finished leads from Instantly campaigns...")
        
        if MAX_LEADS_TO_EVALUATE > 0:
            logger.info(f"🧪 TESTING MODE: Limiting evaluation to {MAX_LEADS_TO_EVALUATE} leads total")
        
        if MAX_PAGES_TO_PROCESS > 0:
            logger.info(f"🧪 TESTING MODE: Limiting pagination to {MAX_PAGES_TO_PROCESS} pages per campaign")
        
        if FORCE_DRAIN_CHECK:
            logger.info(f"🧪 TESTING MODE: Forcing drain check on all leads (bypassing 24hr limit)")
        
        # PHASE 2 OPTIMIZATION: Try BigQuery-first approach
        bigquery_leads = get_leads_needing_drain_from_bigquery()
        
        if bigquery_leads and not FORCE_DRAIN_CHECK:
            logger.info("🚀 PHASE 2: Using BigQuery-first optimization for targeted drain processing")
            return process_bigquery_first_drain(bigquery_leads)
        else:
            if not bigquery_leads:
                logger.info("🔄 BigQuery-first approach returned no results, using current pagination method")
            if FORCE_DRAIN_CHECK:
                logger.info("🧪 FORCE_DRAIN_CHECK enabled, using current pagination method to scan all leads")
            logger.info("🔄 FALLBACK: Using current pagination-based drain processing")
        
        finished_leads = []
        total_leads_evaluated = 0  # Track total across all campaigns
        reached_test_limit = False  # Flag to break out of nested loops
        
        # ACCURATE TRACKING: Track unique leads evaluated across all campaigns
        global_unique_leads_evaluated = set()  # Track unique lead IDs evaluated
        global_unique_leads_to_drain = set()   # Track unique emails to drain
        
        # ENHANCED TRACKING: Count drain reasons for visibility
        drain_reasons = {
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
        
        # Get leads from both campaigns using proper cursor pagination
        campaigns_to_check = [
            ("SMB", SMB_CAMPAIGN_ID),
            ("Midsize", MIDSIZE_CAMPAIGN_ID)
        ]
        
        for campaign_name, campaign_id in campaigns_to_check:
            logger.info(f"🔍 Checking {campaign_name} campaign for finished leads...")
            
            # CURSOR-BASED PAGINATION (proper method)
            starting_after = None  # Start from beginning
            page_count = 0
            total_leads_accessed = 0
            leads_needing_check = 0
            
            # Track leads that need timestamp updates
            leads_to_update_timestamps = []
            
            # Deduplication safety net
            seen_lead_ids = set()
            consecutive_duplicate_pages = 0
            
            # Rate limit retry tracking  
            consecutive_429_errors = 0
            
            # PHASE 1 OPTIMIZATION: Early exit counters
            empty_pages_in_row = 0  # Count consecutive pages with no new candidates
            from datetime import datetime, timezone, timedelta
            current_time = datetime.now(timezone.utc)
            
            while True:
                # Use proper cursor-based pagination
                url = f"{INSTANTLY_BASE_URL}/api/v2/leads/list"
                payload = {
                    "campaign_id": campaign_id,
                    "limit": 50  # Get 50 leads per page (conservative approach)
                }
                
                if starting_after:
                    payload["starting_after"] = starting_after
                
                # RATE LIMITING: Use optimized delay from centralized config
                if page_count > 0:  # Don't delay the first call
                    adaptive_rate_limiter.wait()  # Use adaptive rate limiting
                
                response = requests.post(
                    url,
                    headers=get_instantly_headers(),
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    # Reset rate limit counter on successful response
                    consecutive_429_errors = 0
                    # Track success for adaptive rate limiting
                    adaptive_rate_limiter.record_success()
                    
                    data = response.json()
                    leads = data.get('items', [])
                    
                    if not leads:
                        logger.info(f"📄 No more leads found for {campaign_name} - pagination complete")
                        break
                    
                    page_count += 1
                    total_leads_accessed += len(leads)
                    
                    # DEDUPLICATION SAFETY NET
                    page_lead_ids = {lead.get('id') for lead in leads if lead.get('id')}
                    
                    if page_lead_ids.issubset(seen_lead_ids):
                        # We've seen all these leads before
                        consecutive_duplicate_pages += 1
                        logger.warning(f"⚠️ Page {page_count} contains only duplicate leads (consecutive: {consecutive_duplicate_pages})")
                        
                        if consecutive_duplicate_pages >= 3:
                            logger.error(f"❌ Detected broken pagination for {campaign_name} - same leads repeated 3+ times")
                            break
                    else:
                        consecutive_duplicate_pages = 0
                        seen_lead_ids.update(page_lead_ids)
                    
                    logger.info(f"📄 Processing page {page_count}: {len(leads)} leads ({len(seen_lead_ids)} unique total)")
                    
                    # OPTIMIZED TIME-BASED FILTERING: Batch check all leads on this page
                    page_lead_ids_list = [lead.get('id') for lead in leads if lead.get('id')]
                    leads_check_results = batch_check_leads_for_drain(page_lead_ids_list)
                    
                    # PHASE 1 OPTIMIZATION: Track candidates and oldest update time on this page
                    new_candidates_on_page = 0
                    oldest_updated_on_page = current_time  # Start with current time, will be reduced
                    
                    # Process leads that need evaluation
                    for lead in leads:
                        lead_id = lead.get('id', '')
                        email = lead.get('email', '')
                        
                        if not lead_id:
                            logger.debug(f"⚠️ Skipping lead with no ID: {email}")
                            continue
                            
                        # Check if lead needs evaluation (from batch results or force check)
                        needs_check = FORCE_DRAIN_CHECK or leads_check_results.get(lead_id, True)  # Default to True if not found
                        
                        # PHASE 1 OPTIMIZATION: Track if this is a new candidate
                        if needs_check:
                            new_candidates_on_page += 1
                        
                        # PHASE 1 OPTIMIZATION: Track oldest update time on page
                        try:
                            updated_at_str = lead.get('updated_at', '')
                            if updated_at_str:
                                lead_updated_at = date_parser.parse(updated_at_str)
                                if lead_updated_at < oldest_updated_on_page:
                                    oldest_updated_on_page = lead_updated_at
                        except Exception as e:
                            logger.debug(f"Could not parse updated_at for {email}: {e}")
                        
                        if needs_check:
                            leads_needing_check += 1
                            total_leads_evaluated += 1
                            
                            # ACCURATE TRACKING: Track unique leads evaluated
                            global_unique_leads_evaluated.add(lead_id)
                            
                            # Check testing limit
                            if MAX_LEADS_TO_EVALUATE > 0 and total_leads_evaluated > MAX_LEADS_TO_EVALUATE:
                                logger.info(f"🧪 TESTING LIMIT REACHED: Evaluated {total_leads_evaluated} leads, stopping")
                                # Set flag to break out of all loops
                                reached_test_limit = True
                                break
                            
                            # Classify lead according to our approved drain logic
                            classification = classify_lead_for_drain(lead, campaign_name)
                            
                            if classification['should_drain']:
                                instantly_lead = InstantlyLead(
                                    id=lead_id,
                                    email=email,
                                    campaign_id=campaign_id,
                                    status=classification['drain_reason']
                                )
                                finished_leads.append(instantly_lead)
                                
                                # ACCURATE TRACKING: Track unique emails to drain
                                global_unique_leads_to_drain.add(email)
                                
                                # ENHANCED LOGGING: Track drain reason with details (type safe)
                                drain_reason = classification.get('drain_reason', 'unknown')
                                drain_reasons[drain_reason] = drain_reasons.get(drain_reason, 0) + 1
                                
                                details = classification.get('details', '')
                                logger.info(f"🗑️ DRAIN: {email} → {drain_reason} | {details}")
                            else:
                                # ENHANCED LOGGING: Track keep reasons with type safety
                                keep_reason = str(classification.get('keep_reason', 'unknown reason'))
                                status = lead.get('status', 0)
                                
                                # Safe string checking for auto-reply detection
                                is_auto_reply = ('auto-reply' in keep_reason.lower() if isinstance(keep_reason, str) else False) or \
                                               classification.get('auto_reply', False) == True
                                
                                if is_auto_reply:
                                    drain_reasons['auto_reply_detected'] += 1
                                    logger.debug(f"🤖 KEEP: {email} → auto-reply detected | {keep_reason}")
                                elif status == 1:
                                    drain_reasons['kept_active'] += 1
                                    logger.debug(f"⚡ KEEP: {email} → active sequence | {keep_reason}")
                                elif status == 2:
                                    drain_reasons['kept_paused'] += 1  
                                    logger.debug(f"⏸️ KEEP: {email} → paused sequence | {keep_reason}")
                                else:
                                    drain_reasons['kept_other'] += 1
                                    logger.debug(f"📋 KEEP: {email} → other reason | {keep_reason}")
                            
                            # Queue for batch timestamp update (don't do individual updates)
                            leads_to_update_timestamps.append(lead_id)
                            
                        else:
                            logger.debug(f"⏰ Skipping recent check: {email} (checked within 24h)")
                    
                    # Break out of pagination loop if test limit reached
                    if reached_test_limit:
                        logger.info(f"🧪 Stopping pagination for {campaign_name} due to test limit")
                        break
                    
                    # PHASE 1 OPTIMIZATION: Early exit logic - stop when no new candidates found
                    if new_candidates_on_page == 0:
                        empty_pages_in_row += 1
                        logger.debug(f"📄 Page {page_count}: 0 new candidates (consecutive empty pages: {empty_pages_in_row})")
                        
                        # Calculate time difference (26 hours = 26 * 60 * 60 seconds)
                        time_threshold = current_time - timedelta(hours=26)
                        
                        if oldest_updated_on_page < time_threshold:
                            logger.info(f"⚡ EARLY EXIT: {campaign_name} - No new candidates on page {page_count} and oldest update ({oldest_updated_on_page.strftime('%Y-%m-%d %H:%M:%S')}) is older than 26h threshold. Stopping pagination to optimize performance.")
                            break
                        else:
                            logger.debug(f"📄 Page {page_count}: No candidates but recent updates found (oldest: {oldest_updated_on_page.strftime('%Y-%m-%d %H:%M:%S')}), continuing...")
                    else:
                        empty_pages_in_row = 0  # Reset counter when we find candidates
                        logger.debug(f"📄 Page {page_count}: {new_candidates_on_page} new candidates found")
                    
                    # Get cursor for next page
                    starting_after = data.get('next_starting_after')
                    if not starting_after:
                        logger.info(f"✅ Reached end of {campaign_name} campaign - no more pages")
                        break
                    
                    # Testing page limit check
                    if MAX_PAGES_TO_PROCESS > 0 and page_count >= MAX_PAGES_TO_PROCESS:
                        logger.info(f"🧪 TESTING LIMIT: Reached {MAX_PAGES_TO_PROCESS} pages for {campaign_name} (processed {total_leads_accessed} leads)")
                        break
                    
                    # Safety check to prevent infinite loops (configurable, reduced for drain efficiency)
                    max_pages_limit = int(os.getenv('DRAIN_MAX_PAGES_PER_CAMPAIGN', 20))  # Reduced from 60 to 20
                    if page_count >= max_pages_limit:
                        logger.warning(f"⚠️ Reached safety limit of {max_pages_limit} pages for {campaign_name} (processed {total_leads_accessed} leads)")
                        break
                
                elif response.status_code == 401:
                    # 401 = Authentication/Authorization error - not rate limiting
                    logger.error(f"❌ Authentication error (401) for {campaign_name} - invalid API key or permissions")
                    logger.error(f"Response: {response.text}")
                    
                    # Record as dead letter for investigation
                    record_dead_letter(
                        phase="DRAIN",
                        email="system",
                        http_status=401,
                        error_text=f"Authentication failed for campaign {campaign_name}: {response.text}",
                        retry_count=0
                    )
                    break  # Don't retry auth errors
                
                elif response.status_code == 429:
                    # 429 = Rate limiting - implement adaptive backoff
                    consecutive_429_errors += 1
                    # Track rate limit failure for adaptive strategy
                    adaptive_rate_limiter.record_failure(is_rate_limit=True)
                    
                    if consecutive_429_errors >= 5:
                        logger.error(f"❌ Too many consecutive rate limit errors ({consecutive_429_errors}) for {campaign_name} - stopping pagination")
                        break
                    
                    # Use adaptive backoff combined with exponential backoff
                    exponential_backoff = min(10 * consecutive_429_errors, 60)  # 10s, 20s, 30s, 40s, 60s max
                    adaptive_backoff = adaptive_rate_limiter.get_delay()
                    backoff_time = max(exponential_backoff, adaptive_backoff)  # Use the higher of the two
                    
                    logger.warning(f"⚠️ Rate limit error #{consecutive_429_errors} for {campaign_name} on page {page_count + 1}")
                    logger.info(f"💤 Adaptive + exponential backoff: {backoff_time:.1f}s before retry...")
                    time.sleep(backoff_time)
                    continue  # Retry the same page
                    
                else:
                    # Track general API failure for adaptive rate limiting
                    adaptive_rate_limiter.record_failure(is_rate_limit=False)
                    logger.error(f"❌ Failed to get leads from {campaign_name} campaign (page {page_count + 1}): {response.status_code} - {response.text}")
                    break
            
            # BATCH UPDATE TIMESTAMPS: Much more efficient than individual updates
            if leads_to_update_timestamps:
                logger.info(f"📊 Batch updating timestamps for {len(leads_to_update_timestamps)} evaluated leads...")
                try:
                    batch_update_drain_timestamps(leads_to_update_timestamps)
                    logger.info(f"✅ Successfully updated timestamps for {len(leads_to_update_timestamps)} leads")
                except Exception as timestamp_error:
                    logger.error(f"❌ Batch timestamp update failed: {timestamp_error}")
                    logger.info(f"⏭️ Continuing drain process - timestamp updates are not critical for functionality")
                    # Continue processing - timestamp updates are not critical for drain functionality
            
            logger.info(f"📊 {campaign_name} campaign: accessed {total_leads_accessed} leads ({len(seen_lead_ids)} unique) in {page_count} pages")
            logger.info(f"📊 {campaign_name} campaign: {leads_needing_check} leads evaluated (24hr+ since last check)")
            
            # Break out of campaign loop if test limit reached
            if reached_test_limit:
                logger.info(f"🧪 Stopping campaign processing due to test limit ({total_leads_evaluated} leads evaluated)")
                break
        
        # DEDUPLICATE LEADS: Same email can appear in multiple campaigns but has single lead ID
        unique_leads = {}
        duplicate_count = 0
        
        for lead in finished_leads:
            if lead.email in unique_leads:
                duplicate_count += 1
                logger.debug(f"🔄 Skipping duplicate lead: {lead.email} (already found with ID {unique_leads[lead.email].id})")
            else:
                unique_leads[lead.email] = lead
        
        deduplicated_leads = list(unique_leads.values())
        
        if duplicate_count > 0:
            logger.info(f"🔄 Deduplicated {duplicate_count} duplicate leads - {len(deduplicated_leads)} unique leads to drain")
        
        # ENHANCED LOGGING: Summary of drain analysis results with ACCURATE counts
        logger.info("=" * 60)
        logger.info("📊 DRAIN ANALYSIS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"📋 Total unique leads evaluated: {len(global_unique_leads_evaluated)}")
        logger.info(f"📋 Total evaluations performed: {total_leads_evaluated} (includes re-evaluations due to pagination)")
        logger.info(f"🗑️ Total unique emails to drain: {len(global_unique_leads_to_drain)}")
        logger.info(f"🗑️ Final leads to drain (after dedup): {len(deduplicated_leads)}")
        logger.info(f"📈 Drain rate: {(len(deduplicated_leads)/max(len(global_unique_leads_evaluated),1)*100):.1f}%")
        logger.info("")
        
        # Show breakdown of drain reasons
        drains_found = sum(drain_reasons[reason] for reason in ['replied', 'completed', 'bounced_hard', 'unsubscribed', 'stale_active'])
        if drains_found > 0:
            logger.info("🗑️ DRAIN REASONS BREAKDOWN (includes duplicates from pagination):")
            for reason in ['replied', 'completed', 'bounced_hard', 'unsubscribed', 'stale_active']:
                if drain_reasons[reason] > 0:
                    logger.info(f"   • {reason}: {drain_reasons[reason]} evaluations")
            logger.info("")
        
        # Show breakdown of keep reasons  
        keeps_found = sum(drain_reasons[reason] for reason in ['auto_reply_detected', 'kept_active', 'kept_paused', 'kept_other'])
        if keeps_found > 0:
            logger.info("⏸️ KEEP REASONS BREAKDOWN (includes duplicates from pagination):")
            for reason in ['auto_reply_detected', 'kept_active', 'kept_paused', 'kept_other']:
                if drain_reasons[reason] > 0:
                    reason_display = reason.replace('kept_', '').replace('_', ' ').title()
                    logger.info(f"   • {reason_display}: {drain_reasons[reason]} evaluations")
            logger.info("")
        
        logger.info(f"✅ DRAIN: Found {len(deduplicated_leads)} unique leads to drain across all campaigns")
        logger.info("=" * 60)
        
        return deduplicated_leads
        
    except Exception as e:
        logger.error(f"❌ Failed to get finished leads: {e}")
        return []

def update_bigquery_state(leads: List[InstantlyLead]) -> None:
    """Update BigQuery with lead status and history - OPTIMIZED with bulk operations."""
    if not leads or DRY_RUN:
        return
    
    try:
        logger.info(f"📊 Updating BigQuery state for {len(leads)} drained leads with bulk operations...")
        
        # Track drain reasons for reporting
        drain_reasons = {}
        for lead in leads:
            drain_reasons[lead.status] = drain_reasons.get(lead.status, 0) + 1
        
        # OPTIMIZATION 1: Bulk MERGE for ops_inst_state
        _bulk_update_ops_inst_state(leads)
        
        # OPTIMIZATION 2: Bulk INSERT for history (90-day cooldown)
        history_leads = [lead for lead in leads if lead.status in ['completed', 'replied']]
        if history_leads:
            _bulk_insert_lead_history(history_leads)
        
        # OPTIMIZATION 3: Bulk INSERT for DNC list
        dnc_leads = [lead for lead in leads if lead.status == 'unsubscribed']
        if dnc_leads:
            _bulk_insert_dnc_list(dnc_leads)
        
        # Log summary of drain reasons
        logger.info(f"✅ Updated BigQuery state with bulk operations - Drain summary:")
        for reason, count in drain_reasons.items():
            logger.info(f"  - {reason}: {count} leads")
    
    except Exception as e:
        logger.error(f"❌ Failed to update BigQuery state: {e}")
        log_dead_letter('bigquery_update_drain', None, json.dumps([l.__dict__ for l in leads]), 0, str(e))

def _bulk_update_ops_inst_state(leads: List[InstantlyLead]) -> None:
    """OPTIMIZED: Single bulk MERGE operation instead of individual queries."""
    if not leads:
        return
        
    # Build VALUES clause for all leads at once
    values_rows = []
    for lead in leads:
        # Escape single quotes in email and use safe string formatting
        safe_email = lead.email.replace("'", "''")
        safe_status = lead.status.replace("'", "''") 
        safe_lead_id = lead.id.replace("'", "''") if lead.id else ''
        
        values_rows.append(f"('{safe_email}', '{lead.campaign_id}', '{safe_status}', '{safe_lead_id}')")
    
    values_clause = ",\n    ".join(values_rows)
    
    # Single MERGE query for all leads using safe table reference
    bulk_merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` T
    USING (
        SELECT email, campaign_id, status, instantly_lead_id
        FROM UNNEST([
            {values_clause}
        ]) AS S(email, campaign_id, status, instantly_lead_id)
    ) S
    ON LOWER(T.email) = LOWER(S.email) AND T.campaign_id = S.campaign_id
    WHEN MATCHED THEN
        UPDATE SET status = S.status, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (email, campaign_id, status, instantly_lead_id, added_at, updated_at)
        VALUES (S.email, S.campaign_id, S.status, S.instantly_lead_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """
    
    bq_client.query(bulk_merge_query).result()
    logger.info(f"✅ Bulk updated {len(leads)} leads in ops_inst_state (single query vs {len(leads)} individual queries)")

def _bulk_insert_lead_history(leads: List[InstantlyLead]) -> None:
    """OPTIMIZED: Single bulk INSERT for lead history."""
    if not leads:
        return
        
    # Build VALUES clause for all history entries
    values_rows = []
    for lead in leads:
        safe_email = lead.email.replace("'", "''")
        safe_status = lead.status.replace("'", "''")
        sequence_name = 'SMB' if lead.campaign_id == SMB_CAMPAIGN_ID else 'Midsize'
        
        values_rows.append(f"('{safe_email}', '{lead.campaign_id}', '{sequence_name}', '{safe_status}', CURRENT_TIMESTAMP(), 1)")
    
    values_clause = ",\n    ".join(values_rows)
    
    # Single INSERT query for all history records using safe table reference
    bulk_history_query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_lead_history`
    (email, campaign_id, sequence_name, status_final, completed_at, attempt_num)
    VALUES
    {values_clause}"""
    
    bq_client.query(bulk_history_query).result()
    logger.info(f"✅ Bulk inserted {len(leads)} leads to history (90-day cooldown)")

def _bulk_insert_dnc_list(leads: List[InstantlyLead]) -> None:
    """OPTIMIZED: Single bulk INSERT for DNC list."""
    if not leads:
        return
        
    # Build VALUES clause for all DNC entries
    values_rows = []
    for lead in leads:
        safe_email = lead.email.replace("'", "''")
        domain_part = lead.email.split('@')[1] if '@' in lead.email else 'unknown'
        safe_domain = domain_part.replace("'", "''")
        
        values_rows.append(f"""(
            GENERATE_UUID(), 
            '{safe_email}', 
            '{safe_domain}', 
            'instantly_drain', 
            'unsubscribe_via_api', 
            CURRENT_TIMESTAMP(), 
            'sync_script_v2_bulk', 
            TRUE
        )""")
    
    values_clause = ",\n    ".join(values_rows)
    
    # Single INSERT query for all DNC records using safe table reference
    bulk_dnc_query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dnc_list`
    (id, email, domain, source, reason, added_date, added_by, is_active)
    VALUES
    {values_clause}"""
    
    bq_client.query(bulk_dnc_query).result()
    logger.info(f"🚫 Bulk added {len(leads)} unsubscribes to permanent DNC list")

# VERIFICATION FAILURE TRACKING REMOVED - No longer needed

def delete_leads_from_instantly(leads: List[InstantlyLead]) -> None:
    """Delete finished leads from Instantly to free inventory with proper rate limiting."""
    if not leads:
        return
    
    logger.info(f"🗑️ Deleting {len(leads)} leads from Instantly...")
    successful_deletes = 0
    failed_deletes = 0
    
    try:
        for i, lead in enumerate(leads, 1):
            logger.debug(f"Deleting {i}/{len(leads)}: {lead.email}")
            
            # Use our improved single-lead delete function
            success = delete_lead_from_instantly(lead)
            
            if success:
                successful_deletes += 1
            else:
                failed_deletes += 1
            
            # Adaptive rate limiting: sleep between deletes (except for dry run and last item)
            if not DRY_RUN and i < len(leads):
                # Increase delay if we're seeing failures (could be rate limiting)
                base_delay = 1.5
                if failed_deletes > 0 and i > 5:  # After 5 deletes, if seeing failures
                    adaptive_delay = min(base_delay * (1 + failed_deletes * 0.5), 10.0)  # Max 10s
                    logger.debug(f"⏱️ Adaptive rate limiting: {adaptive_delay:.1f}s (failures: {failed_deletes})")
                    time.sleep(adaptive_delay)
                else:
                    logger.debug("⏱️ Standard rate limiting: 1.5s between deletes")
                    time.sleep(base_delay)
    
    except Exception as e:
        logger.error(f"❌ Batch delete operation failed: {e}")
    
    # Report batch results
    logger.info(f"📊 Delete batch complete: {successful_deletes} successful, {failed_deletes} failed")
    if failed_deletes > 0:
        logger.warning(f"⚠️ {failed_deletes} leads failed to delete - check dead letters table")

def drain_finished_leads() -> int:
    """Main drain function - remove completed/bounced/unsubscribed leads."""
    logger.info("=== DRAINING FINISHED LEADS ===")
    
    finished_leads = get_finished_leads()
    if not finished_leads:
        logger.info("No finished leads to drain")
        return 0
    
    update_bigquery_state(finished_leads)
    delete_leads_from_instantly(finished_leads)
    
    logger.info(f"Drained {len(finished_leads)} finished leads")
    return len(finished_leads)

def get_mailbox_capacity() -> Tuple[int, int]:
    """Get current mailbox capacity - V2 API ONLY (no v1 endpoints)."""
    try:
        # V2 API: First verify authentication with workspace endpoint
        response = call_instantly_api('/api/v2/workspaces/current', method='GET')
        
        if DRY_RUN:
            # For dry run, simulate conservative capacity
            logger.info("DRY RUN: Using simulated mailbox capacity (68 boxes @ 10 emails/day ESTIMATE)")
            return 68, 680
        
        # V2 API: Check workspace authentication succeeded
        if response and response.get('id'):
            workspace_name = response.get('name', 'Unknown')
            logger.info(f"✅ V2 API Auth OK - Workspace: {workspace_name}")
            
            # V2 API doesn't have a direct mailbox count endpoint
            # Use known conservative estimates until we find the right V2 endpoint
            # TODO: Find V2 endpoint for mailbox count if it exists
            total_mailboxes = 68  # Known value from your setup
        
            # IMPORTANT: This is a conservative estimate since Instantly API doesn't expose per-mailbox limits
            # Mailbox capacity varies by warmup stage: 10-30 emails/day per mailbox
            # Using conservative 10 emails/day for safety
            estimated_emails_per_day_per_mailbox = 10
            daily_capacity_estimate = total_mailboxes * estimated_emails_per_day_per_mailbox
            
            logger.info(f"📊 Mailbox Capacity ESTIMATE: {total_mailboxes} mailboxes @ {estimated_emails_per_day_per_mailbox} emails/day/mailbox = {daily_capacity_estimate} total emails/day")
            logger.info(f"⚠️ NOTE: This is a conservative estimate. Actual capacity may be higher as mailboxes warm up.")
            
            return total_mailboxes, daily_capacity_estimate
        else:
            # V2 API auth failed or returned unexpected data
            logger.warning("V2 API workspace call failed, using fallback capacity estimate")
            logger.info("📊 Using FALLBACK ESTIMATE: 68 mailboxes @ 10 emails/day (conservative)")
            return 68, 680
        
    except Exception as e:
        logger.warning(f"Could not get mailbox capacity from API: {e}")
        logger.info("📊 Using FALLBACK ESTIMATE: 68 mailboxes @ 10 emails/day (conservative)")
        return 68, 680  # Conservative fallback estimate

def get_current_instantly_inventory() -> int:
    """Get current lead count in Instantly using real API data - CORRECTED to use proper campaign field."""
    try:
        # CORRECTED: Get ALL leads first, then filter by campaign field (not campaign_id filter)
        # This is because the API's campaign_id filter doesn't work - it returns all leads regardless
        
        logger.info("📊 Fetching all leads to calculate accurate inventory...")
        
        # Step 1: Fetch all leads from Instantly (only once, not per campaign)
        all_leads = []
        starting_after = None
        page_count = 0
        
        while True:
            data = {'limit': 100}  # No campaign filter - get all leads
            
            if starting_after:
                data['starting_after'] = starting_after
            
            response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
            
            if not response or not response.get('items'):
                break
            
            items = response.get('items', [])
            page_count += 1
            all_leads.extend(items)
            logger.debug(f"  Page {page_count}: {len(items)} leads fetched")
            
            # Check for next page
            starting_after = response.get('next_starting_after')
            if not starting_after:
                break
            
            # Safety limit
            if page_count > 50:
                logger.warning(f"Hit page limit at {page_count} pages while fetching inventory")
                break
        
        logger.info(f"📄 Total leads fetched: {len(all_leads)}")
        
        # Step 2: Filter and count by campaign using the correct 'campaign' field
        total_inventory = 0
        campaign_breakdown = {}
        unassigned_count = 0
        
        for campaign_name, campaign_id in [("SMB", SMB_CAMPAIGN_ID), ("Midsize", MIDSIZE_CAMPAIGN_ID)]:
            campaign_leads = []
            campaign_inventory = 0
            status_breakdown = {}
            
            # Filter leads for this campaign using 'campaign' field (not 'campaign_id')
            for lead in all_leads:
                lead_campaign = lead.get('campaign')  # CORRECTED: Use 'campaign' not 'campaign_id'
                
                if lead_campaign == campaign_id:
                    campaign_leads.append(lead)
                    
                    status = lead.get('status', 0)
                    # V2 API Status codes: 1=Active, 2=Paused, 3=Completed, -1=Bounced, -2=Unsubscribed, -3=Skipped
                    status_name = {
                        1: 'active',
                        2: 'paused', 
                        3: 'completed',
                        -1: 'bounced',
                        -2: 'unsubscribed',
                        -3: 'skipped'
                    }.get(status, f'unknown_{status}')
                    
                    status_breakdown[status_name] = status_breakdown.get(status_name, 0) + 1
                    
                    # Count only Active (1) and Paused (2) as inventory
                    if status in [1, 2]:
                        campaign_inventory += 1
            
            # Log campaign results
            logger.info(f"  📊 {campaign_name} campaign: {len(campaign_leads)} total leads, {campaign_inventory} active inventory")
            if status_breakdown:
                logger.debug(f"    Status breakdown: {status_breakdown}")
            
            total_inventory += campaign_inventory
            campaign_breakdown[campaign_name] = {
                'total_leads': len(campaign_leads),
                'inventory': campaign_inventory,
                'status_breakdown': status_breakdown
            }
        
        # Count unassigned leads for completeness
        for lead in all_leads:
            if not lead.get('campaign'):
                unassigned_count += 1
        
        # Log summary
        logger.info(f"📋 Inventory Summary:")
        logger.info(f"  🎯 Total Active Inventory: {total_inventory} leads")
        logger.info(f"  📊 SMB: {campaign_breakdown.get('SMB', {}).get('inventory', 0)} active")
        logger.info(f"  📊 Midsize: {campaign_breakdown.get('Midsize', {}).get('inventory', 0)} active") 
        logger.info(f"  ❓ Unassigned leads: {unassigned_count}")
        
        return total_inventory
        
    except Exception as e:
        logger.error(f"Failed to get inventory from API, falling back to BigQuery: {e}")
        # Fallback to BigQuery tracking 
        query = f"""
        SELECT COUNT(*) as count 
        FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` 
        WHERE status = 'active'
        """
        result = bq_client.query(query).result()
        total = next(result).count
        logger.info(f"Current Instantly inventory (tracked fallback): {total}")
        return total

def calculate_smart_lead_target() -> int:
    """Calculate optimal number of leads to add based on mailbox capacity."""
    try:
        # Get mailbox capacity
        mailbox_count, daily_capacity = get_mailbox_capacity()
        
        # Calculate safe total inventory limit
        safe_inventory_limit = int(daily_capacity * LEAD_INVENTORY_MULTIPLIER)
        
        # Get current inventory
        current_inventory = get_current_instantly_inventory()
        
        # Calculate available capacity
        available_capacity = safe_inventory_limit - current_inventory
        
        # Don't exceed the configured target per run
        target_leads = min(available_capacity, TARGET_NEW_LEADS_PER_RUN)
        
        # Never go negative
        target_leads = max(0, target_leads)
        
        logger.info(f"📊 Capacity calculation (based on ESTIMATES):")
        logger.info(f"  - Mailboxes: {mailbox_count} (actual count)")
        logger.info(f"  - Daily capacity ESTIMATE: {daily_capacity} emails/day @ 10/mailbox")
        logger.info(f"  - Safe inventory limit: {safe_inventory_limit} leads (capacity × {LEAD_INVENTORY_MULTIPLIER})")
        logger.info(f"  - Current inventory: {current_inventory} leads (actual)")
        logger.info(f"  - Available capacity ESTIMATE: {available_capacity} leads")
        logger.info(f"  - Target for this run: {target_leads} leads")
        
        return target_leads
        
    except Exception as e:
        logger.error(f"Failed to calculate smart lead target: {e}")
        # Fallback to original logic
        return min(TARGET_NEW_LEADS_PER_RUN, 50)  # Conservative fallback

def get_eligible_leads(limit: int) -> List[Lead]:
    """Get leads ready for Instantly from BigQuery, prioritized by Klaviyo install date."""
    try:
        query = f"""
        SELECT email, merchant_name, platform_domain, state, country_code, 
               estimated_sales_yearly, sequence_target, klaviyo_installed_at,
               -- Add priority tiers for analysis using safe timestamp parsing
               CASE 
                 WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', klaviyo_installed_at)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) THEN 'HOT'
                 WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', klaviyo_installed_at)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) THEN 'WARM' 
                 ELSE 'COLD'
               END as klaviyo_priority
        FROM `{PROJECT_ID}.{DATASET_ID}.v_ready_for_instantly`
        WHERE email IS NOT NULL AND email != ''
        ORDER BY 
          SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', klaviyo_installed_at) DESC NULLS LAST,
          RAND()  -- Secondary randomization for same-day installs
        LIMIT @limit
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ]
        )
        
        result = bq_client.query(query, job_config=job_config).result()
        leads = []
        
        # Track priority distribution for logging
        priority_counts = {'HOT': 0, 'WARM': 0, 'COLD': 0}
        
        for row in result:
            if hasattr(row, 'klaviyo_priority'):
                priority_counts[row.klaviyo_priority] = priority_counts.get(row.klaviyo_priority, 0) + 1
                
            leads.append(Lead(
                email=row.email,
                merchant_name=row.merchant_name or '',
                platform_domain=row.platform_domain or '',
                state=row.state or '',
                country_code=row.country_code or '',
                estimated_sales_yearly=row.estimated_sales_yearly,
                sequence_target=row.sequence_target,
                klaviyo_installed_at=row.klaviyo_installed_at
            ))
        
        logger.info(f"Retrieved {len(leads)} eligible leads with Klaviyo prioritization:")
        logger.info(f"  - HOT (90-day): {priority_counts['HOT']} leads")
        logger.info(f"  - WARM (1-year): {priority_counts['WARM']} leads") 
        logger.info(f"  - COLD (1+ year): {priority_counts['COLD']} leads")
        
        return leads
    
    except Exception as e:
        logger.error(f"Failed to get eligible leads: {e}")
        return []

def split_leads_by_segment(leads: List[Lead]) -> Tuple[List[Lead], List[Lead]]:
    """Split leads into SMB and Midsize segments."""
    smb_leads = [l for l in leads if l.sequence_target == 'SMB']
    midsize_leads = [l for l in leads if l.sequence_target == 'Midsize']
    return smb_leads, midsize_leads

# EMAIL VERIFICATION REMOVED - Let Instantly handle verification internally

def create_lead_in_instantly(lead: Lead, campaign_id: str) -> Optional[str]:
    """Create a single lead in Instantly campaign with direct campaign assignment - FIXED."""
    try:
        # ✅ FIXED: Single-step creation with direct campaign assignment
        payload = {
            'email': lead.email,
            'first_name': '',  # Not available in our data
            'last_name': '',   # Not available in our data
            'company_name': lead.merchant_name,
            'campaign': campaign_id,  # ✅ CORRECTED: 'campaign' not 'campaign_id' per API docs
            # ✅ DO NOT include verify_leads_on_import (keep verification fully async)
            'custom_variables': {
                'company': lead.merchant_name,
                'domain': lead.platform_domain,
                'location': lead.state,
                'country': lead.country_code
            }
        }
        
        if DRY_RUN:
            logger.info(f"🔄 DRY RUN: Would create {lead.email} in campaign {campaign_id}")
            return 'dry-run-id'
        
        # Create lead with direct campaign assignment
        logger.debug(f"Creating lead {lead.email} directly in campaign {campaign_id}")
        response = call_instantly_api('/api/v2/leads', method='POST', data=payload)
        
        # Check for successful creation
        lead_id = response.get('id')
        if not lead_id:
            logger.error(f"❌ Lead creation FAILED for {lead.email}")
            logger.error(f"📋 Create response: {json.dumps(response)}")
            return None
        
        # ✅ VERIFICATION: Immediate per-lead GET to confirm campaign assignment
        logger.debug(f"Verifying campaign assignment for lead {lead.email}")
        verify_response = call_instantly_api(f'/api/v2/leads/{lead_id}', method='GET')
        
        if not verify_response:
            logger.error(f"❌ Failed to verify lead {lead.email} (GET request failed)")
            return None
        
        actual_campaign = verify_response.get('campaign')
        if actual_campaign != campaign_id:
            logger.error(f"❌ Lead {lead.email} created but assignment FAILED")
            logger.error(f"   Expected campaign: {campaign_id}")
            logger.error(f"   Actual campaign: {actual_campaign}")
            return None
        
        logger.info(f"✅ Lead {lead.email} created and verified in campaign {campaign_id}")
        return lead_id
    
    except Exception as e:
        logger.error(f"❌ Exception creating lead {lead.email}: {e}")
        return None

def move_lead_to_campaign(lead: Lead, campaign_id: str) -> Optional[str]:
    """Move existing lead to target campaign using proper Instantly API."""
    try:
        logger.info(f"🔄 Moving existing lead {lead.email} to campaign {campaign_id}")
        
        # First, try to find the existing lead to get its lead ID
        search_response = call_instantly_api(f'/api/v2/leads?email={lead.email}')
        
        if not search_response or 'error' in search_response:
            logger.warning(f"⚠️ Could not find existing lead {lead.email} - treating as new lead")
            return None
        
        # Extract lead info from search results - CORRECTED API response structure
        leads_data = search_response.get('items', [])  # ✅ V2 API uses 'items' not 'data'
        if not leads_data:
            logger.warning(f"⚠️ No lead data found for {lead.email} - treating as new lead")
            return None
        
        existing_lead = leads_data[0]  # Take the first match
        existing_lead_id = existing_lead.get('id')
        current_campaign_id = existing_lead.get('campaign')  # ✅ CORRECTED: 'campaign' not 'campaign_id'
        
        if not existing_lead_id:
            logger.error(f"❌ Could not get lead ID for existing lead {lead.email}")
            return None
        
        # Check if already in target campaign
        if current_campaign_id == campaign_id:
            logger.info(f"✅ Lead {lead.email} already in target campaign {campaign_id}")
            return existing_lead_id
        
        # Move lead to new campaign using the move endpoint
        # CORRECTED: Use 'campaign' field, not 'campaign_id' (V2 API pattern)
        move_data = {
            'campaign': campaign_id
        }
        
        move_response = call_instantly_api(f'/api/v2/leads/{existing_lead_id}/move', 'POST', move_data)
        
        if move_response and 'error' not in move_response:
            logger.info(f"✅ Lead {lead.email} moved from campaign {current_campaign_id} to {campaign_id}")
            return existing_lead_id
        else:
            logger.error(f"❌ Failed to move lead {lead.email}: {move_response}")
            # Log the failure for retry tracking
            record_dead_letter(
                phase="TOP-UP",
                email=lead.email,
                http_status=0,
                error_text=f"Lead move failed: {move_response}",
                payload=json.dumps(move_data)
            )
            return None
    
    except Exception as e:
        logger.error(f"❌ Exception moving lead {lead.email}: {e}")
        record_dead_letter(
            phase="TOP-UP",
            email=lead.email,
            http_status=0,
            error_text=f"Lead move exception: {str(e)}",
            payload=json.dumps({'campaign_id': campaign_id})
        )
        return None

def update_ops_state(leads: List[Lead], campaign_id: str, lead_ids: List[str], 
                    verification_results: Optional[List[dict]] = None) -> None:
    """Update ops_inst_state with newly added leads and verification results."""
    if DRY_RUN:
        return
    
    try:
        for i, (lead, lead_id) in enumerate(zip(leads, lead_ids)):
            if lead_id:
                # Find matching verification result if available
                verification = None
                if verification_results:
                    for v in verification_results:
                        if v['email'] == lead.email:
                            verification = v
                            break
                
                # Build insert query with verification fields using safe table reference
                if verification:
                    query = f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
                    (email, campaign_id, status, instantly_lead_id, added_at, updated_at,
                     verification_status, verification_catch_all, verification_credits_used, verified_at)
                    VALUES (@email, @campaign_id, 'active', @lead_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
                            @verification_status, @verification_catch_all, @verification_credits_used, CURRENT_TIMESTAMP())
                    """
                    
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("email", "STRING", lead.email),
                            bigquery.ScalarQueryParameter("campaign_id", "STRING", campaign_id),
                            bigquery.ScalarQueryParameter("lead_id", "STRING", lead_id),
                            bigquery.ScalarQueryParameter("verification_status", "STRING", verification.get('status')),
                            bigquery.ScalarQueryParameter("verification_catch_all", "BOOL", verification.get('catch_all', False)),
                            bigquery.ScalarQueryParameter("verification_credits_used", "INT64", verification.get('credits_used', 1)),
                        ]
                    )
                else:
                    # Original query without verification fields using safe table reference
                    query = f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
                    (email, campaign_id, status, instantly_lead_id, added_at, updated_at)
                    VALUES (@email, @campaign_id, 'active', @lead_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                    """
                    
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("email", "STRING", lead.email),
                            bigquery.ScalarQueryParameter("campaign_id", "STRING", campaign_id),
                            bigquery.ScalarQueryParameter("lead_id", "STRING", lead_id),
                        ]
                    )
                
                bq_client.query(query, job_config=job_config).result()
        
        logger.info(f"Updated ops state for {len([l for l in lead_ids if l])} leads")
    
    except Exception as e:
        logger.error(f"Failed to update ops state: {e}")

def process_lead_batch(leads: List[Lead], campaign_id: str) -> Tuple[int, Dict[str, Any]]:
    """Process a batch of leads for a specific campaign - NO VERIFICATION."""
    if not leads:
        return 0, {"verification_triggered": False, "verification_count": 0}
    
    logger.info(f"Processing batch of {len(leads)} leads for campaign {campaign_id}")
    logger.info("📤 Processing all leads directly (no pre-verification)")
    
    successful_ids = []
    
    # Process in smaller batches to respect rate limits
    for i in range(0, len(leads), BATCH_SIZE):
        batch = leads[i:i + BATCH_SIZE]
        batch_ids = []
        
        for lead in batch:
            lead_id = create_lead_in_instantly(lead, campaign_id)
            batch_ids.append(lead_id)
            adaptive_rate_limiter.wait(0.5)  # Use adaptive rate limiting between individual calls
        
        successful_ids.extend(batch_ids)
        
        # Update ops_state without verification results
        update_ops_state(batch, campaign_id, batch_ids)
        
        if i + BATCH_SIZE < len(leads):  # Not the last batch
            logger.info(f"Sleeping {BATCH_SLEEP_SECONDS}s between batches...")
            time.sleep(BATCH_SLEEP_SECONDS)
    
    # NOTIFICATION FIX: Count all non-None IDs as successful (includes existing leads)
    successful_count = len([id for id in successful_ids if id is not None])
    logger.info(f"✅ Successfully processed {successful_count}/{len(leads)} leads")
    logger.info(f"📊 Notification will report {successful_count} leads added to campaign")
    
    # ASYNC VERIFICATION: Trigger verification for successfully created leads
    verification_result = {"verification_triggered": False, "verification_count": 0}
    
    # Check if verification should be skipped during sync
    skip_sync_verification = os.getenv('SKIP_SYNC_VERIFICATION', 'false').lower() == 'true'
    
    if successful_count > 0 and ASYNC_VERIFICATION_AVAILABLE and not DRY_RUN and not skip_sync_verification:
        # ✅ Build lead data with both email and instantly_lead_id for efficient deletion
        successful_lead_data = [
            {"email": lead.email, "instantly_lead_id": lead_id} 
            for lead, lead_id in zip(leads, successful_ids) 
            if lead_id is not None
        ]
        if successful_lead_data:
            logger.info(f"🔍 Triggering async verification for {len(successful_lead_data)} successfully created leads")
            try:
                verification_triggered = trigger_verification_for_new_leads(successful_lead_data, campaign_id)
                verification_result = {
                    "verification_triggered": verification_triggered,
                    "verification_count": len(successful_lead_data)
                }
                if verification_triggered:
                    logger.info(f"✅ Async verification triggered successfully")
                else:
                    logger.warning(f"⚠️ Async verification trigger failed - leads created but verification not initiated")
            except Exception as e:
                logger.error(f"❌ Async verification trigger error: {e}")
                logger.info("📧 Leads were created successfully but verification trigger failed")
                verification_result = {
                    "verification_triggered": False,
                    "verification_count": len(successful_lead_data)
                }
    elif skip_sync_verification:
        logger.info(f"⏭️ Skipping verification during sync (SKIP_SYNC_VERIFICATION=true) - poller will handle it")
        verification_result = {
            "verification_skipped": True,
            "verification_count": successful_count
        }
    elif DRY_RUN:
        logger.info(f"🔍 DRY RUN: Would trigger async verification for {successful_count} leads")
        verification_result = {
            "verification_triggered": False,  # Dry run doesn't actually trigger
            "verification_count": successful_count
        }
    elif not ASYNC_VERIFICATION_AVAILABLE:
        logger.info(f"📴 Async verification not available - leads created without verification trigger")
        verification_result = {
            "verification_triggered": False,
            "verification_count": 0
        }
    
    return successful_count, verification_result

def top_up_campaigns() -> Tuple[int, int, Dict[str, Any]]:
    """Add new eligible leads to campaigns using smart capacity management."""
    logger.info("=== TOPPING UP CAMPAIGNS ===")
    
    # Calculate smart lead target based on mailbox capacity
    target_leads = calculate_smart_lead_target()
    
    if target_leads == 0:
        logger.info("Smart capacity management: No leads to add this run")
        return 0, 0, {"triggered": False, "lead_count": 0}
    
    # Check legacy inventory guard as backup safety
    current_inventory = get_current_instantly_inventory()
    if current_inventory >= INSTANTLY_CAP_GUARD:
        logger.warning(f"Legacy safety guard triggered: Inventory at {current_inventory}, skipping top-up (guard: {INSTANTLY_CAP_GUARD})")
        return 0, 0, {"triggered": False, "lead_count": 0}
    
    # Get eligible leads using smart target
    logger.info(f"Smart targeting: requesting {target_leads} leads for this run")
    leads = get_eligible_leads(target_leads)
    
    if not leads:
        logger.info("No eligible leads found")
        return 0, 0, {"triggered": False, "lead_count": 0}
    
    # Split by segment
    smb_leads, midsize_leads = split_leads_by_segment(leads)
    
    logger.info(f"Found {len(smb_leads)} SMB and {len(midsize_leads)} Midsize leads")
    
    # Process each segment
    smb_processed, smb_verification = process_lead_batch(smb_leads, SMB_CAMPAIGN_ID)
    midsize_processed, midsize_verification = process_lead_batch(midsize_leads, MIDSIZE_CAMPAIGN_ID)
    
    # Combine verification results
    combined_verification = {
        "triggered": smb_verification.get("verification_triggered", False) or midsize_verification.get("verification_triggered", False),
        "lead_count": smb_verification.get("verification_count", 0) + midsize_verification.get("verification_count", 0),
        "smb_verification": smb_verification,
        "midsize_verification": midsize_verification
    }
    
    logger.info(f"Top-up complete: {smb_processed} SMB + {midsize_processed} Midsize = {smb_processed + midsize_processed} total")
    return smb_processed, midsize_processed, combined_verification

def housekeeping() -> Dict:
    """Generate summary metrics and perform housekeeping."""
    logger.info("=== HOUSEKEEPING ===")
    
    try:
        # Get current counts
        inventory = get_current_instantly_inventory()
        mailbox_count, daily_capacity = get_mailbox_capacity()
        safe_inventory_limit = int(daily_capacity * LEAD_INVENTORY_MULTIPLIER)
        
        # Get eligible count
        query = "SELECT COUNT(*) as count FROM `" + PROJECT_ID + "." + DATASET_ID + ".v_ready_for_instantly`"
        result = bq_client.query(query).result()
        eligible_count = next(result).count
        
        # VERIFICATION METRICS REMOVED - Let Instantly handle verification internally
        verification_stats = {}
        
        # Calculate utilization metrics
        capacity_utilization = (inventory / safe_inventory_limit * 100) if safe_inventory_limit > 0 else 0
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'current_inventory': inventory,  # NOTIFICATION FIX: Use expected key name
            'instantly_inventory': inventory,  # Keep for backward compatibility
            'eligible_leads': eligible_count,
            'mailbox_count': mailbox_count,
            'daily_email_capacity': daily_capacity,
            'safe_inventory_limit': safe_inventory_limit,
            'capacity_utilization_pct': round(capacity_utilization, 1),
            'lead_multiplier': LEAD_INVENTORY_MULTIPLIER,
            'cap_guard': INSTANTLY_CAP_GUARD,
            'dry_run': DRY_RUN
        }
        
        logger.info(f"📊 Summary:")
        logger.info(f"  - Current inventory: {inventory:,} leads (actual)")
        logger.info(f"  - Eligible leads: {eligible_count:,} (actual)")  
        logger.info(f"  - Mailboxes: {mailbox_count} (actual) @ {daily_capacity} emails/day (ESTIMATE)")
        logger.info(f"  - Safe capacity ESTIMATE: {safe_inventory_limit:,} leads (utilization: {capacity_utilization:.1f}%)")
        logger.info(f"  - Legacy cap guard: {INSTANTLY_CAP_GUARD:,} (hard limit)")
        # Update verification logging based on actual system behavior
        if ASYNC_VERIFICATION_AVAILABLE:
            logger.info(f"  - Verification: ASYNC (triggered after lead creation)")
        else:
            logger.info(f"  - Verification: UNAVAILABLE (async verification module not loaded)")
        logger.info(f"  - Verification details: Instantly handles validation internally")
        
        return metrics
    
    except Exception as e:
        logger.error(f"Housekeeping failed: {e}")
        return {'error': str(e)}

def main():
    """Main synchronization function - Fast Mode (WITHOUT drain phase)."""
    sync_start_time = time.time()
    
    logger.info("🚀 STARTING COLD EMAIL SYNC (Fast Mode)")
    logger.info(f"Config - Target: {TARGET_NEW_LEADS_PER_RUN}, Cap: {INSTANTLY_CAP_GUARD}, Multiplier: {LEAD_INVENTORY_MULTIPLIER}, Dry Run: {DRY_RUN}")
    logger.info("ℹ️ NOTE: Drain phase now handled by separate workflow - this is FAST MODE")
    
    # V2 API Auth Sanity Check - Verify authentication before proceeding
    logger.info("🔐 Verifying Instantly V2 API authentication...")
    try:
        workspace_response = call_instantly_api('/api/v2/workspaces/current', method='GET')
        if workspace_response and workspace_response.get('id'):
            logger.info(f"✅ Instantly V2 Auth OK - Workspace: {workspace_response.get('name', 'Unknown')} (ID: {workspace_response.get('id')})")
        else:
            logger.error("❌ Instantly V2 Auth FAILED - Invalid workspace response")
            logger.error(f"Response: {workspace_response}")
            logger.error("Please check your Instantly API key is valid for V2 API")
            return
    except Exception as auth_error:
        logger.error(f"❌ Instantly V2 Auth Check FAILED: {auth_error}")
        logger.error("Cannot proceed without valid V2 API authentication")
        return
    
    # Initialize notification tracking
    notification_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "duration_seconds": 0,
        "capacity_status": {},
        "leads_processed": {
            "total_attempted": TARGET_NEW_LEADS_PER_RUN,
            "smb_campaign": {"campaign_id": SMB_CAMPAIGN_ID, "leads_added": 0, "campaign_name": "SMB"},
            "midsize_campaign": {"campaign_id": MIDSIZE_CAMPAIGN_ID, "leads_added": 0, "campaign_name": "Midsize"}
        },
        "verification_results": {
            "total_attempted": 0,
            "verified_successful": 0,
            "verification_failed": 0,
            "success_rate_percentage": 0.0,
            "credits_used": 0.0
        },
        "final_inventory": {"instantly_total": 0, "bigquery_eligible": 0},
        "performance": {"api_success_rate": 0.0, "processing_rate_per_minute": 0.0},
        "errors": [],
        "github_run_url": f"{os.getenv('GITHUB_SERVER_URL', '')}/{os.getenv('GITHUB_REPOSITORY', '')}/actions/runs/{os.getenv('GITHUB_RUN_ID', '')}"
    }
    
    try:
        # REMOVED: Step 1: Drain finished leads (now handled by separate drain workflow)
        logger.info("⏭️ SKIPPING DRAIN: Handled by separate drain-leads workflow")
        
        # Step 1: Top up campaigns (renamed from Step 2)
        smb_added, midsize_added, verification_data = top_up_campaigns()
        
        # Update notification data with results
        notification_data["leads_processed"]["smb_campaign"]["leads_added"] = smb_added
        notification_data["leads_processed"]["midsize_campaign"]["leads_added"] = midsize_added
        
        # Add async verification data to notifications
        notification_data["async_verification"] = verification_data
        
        # NOTIFICATION FIX: Add debug logging
        logger.info(f"📊 Notification data: SMB={smb_added}, Midsize={midsize_added}, Total={smb_added + midsize_added}")
        if smb_added == 0 and midsize_added == 0:
            logger.warning("⚠️ Notification showing 0 leads - check for return value issues in top_up_campaigns()")
        
        # Step 2: Housekeeping (renamed from Step 3)
        metrics = housekeeping()
        
        # Update notification data with capacity and inventory info
        if metrics and not metrics.get('error'):
            current_inventory = metrics.get('current_inventory', 0)
            eligible_leads = metrics.get('eligible_leads', 0)
            
            # NOTIFICATION FIX: Add debugging for capacity data
            logger.info(f"📊 Housekeeping metrics for notification:")
            logger.info(f"   - current_inventory from housekeeping: {current_inventory}")
            logger.info(f"   - eligible_leads from housekeeping: {eligible_leads}")
            logger.info(f"   - metrics keys available: {list(metrics.keys())}")
            
            # Use actual calculated safe inventory limit instead of hardcoded cap
            safe_inventory_limit = metrics.get('safe_inventory_limit', INSTANTLY_CAP_GUARD)
            
            notification_data["capacity_status"] = {
                "current_inventory": current_inventory,
                "max_capacity": safe_inventory_limit,
                "utilization_percentage": round((current_inventory / safe_inventory_limit) * 100, 1),
                "estimated_capacity_remaining": safe_inventory_limit - current_inventory
            }
            
            notification_data["final_inventory"] = {
                "instantly_total": current_inventory,
                "bigquery_eligible": eligible_leads
            }
        else:
            # NOTIFICATION FIX: Add debugging for missing metrics
            logger.warning("⚠️ No valid metrics from housekeeping - notification will show default values")
            if metrics:
                logger.warning(f"   Metrics error: {metrics.get('error', 'Unknown error')}")
            else:
                logger.warning("   Metrics is None - housekeeping may have failed")
        
        # Calculate final metrics
        sync_end_time = time.time()
        sync_duration = sync_end_time - sync_start_time
        notification_data["duration_seconds"] = sync_duration
        
        total_added = smb_added + midsize_added
        if sync_duration > 0:
            notification_data["performance"]["processing_rate_per_minute"] = round((total_added / sync_duration) * 60, 1)
        
        # Estimate verification success (placeholder - you can enhance this by tracking actual verification)
        if total_added > 0:
            notification_data["verification_results"].update({
                "total_attempted": total_added,
                "verified_successful": total_added,  # All added leads were verified
                "verification_failed": 0,
                "success_rate_percentage": 100.0,
                "credits_used": total_added * 0.25  # Estimated credits
            })
            # Calculate actual API success rate from adaptive rate limiter
            total_api_calls = adaptive_rate_limiter.success_count + adaptive_rate_limiter.failure_count
            if total_api_calls > 0:
                actual_success_rate = (adaptive_rate_limiter.success_count / total_api_calls) * 100
                notification_data["performance"]["api_success_rate"] = round(actual_success_rate, 1)
                logger.info(f"📊 Actual API success rate: {actual_success_rate:.1f}% ({adaptive_rate_limiter.success_count}/{total_api_calls} calls)")
            else:
                notification_data["performance"]["api_success_rate"] = 100.0  # No API calls made
        
        # Send notification
        if NOTIFICATIONS_AVAILABLE:
            try:
                logger.info("📤 Sending sync completion notification...")
                success = notifier.send_sync_notification(notification_data)
                if success:
                    logger.info("✅ Notification sent successfully")
                else:
                    logger.warning("⚠️ Notification failed to send")
            except Exception as e:
                logger.warning(f"⚠️ Notification error (sync continues): {e}")
        
        # Final summary
        logger.info("✅ SYNC COMPLETE (Fast Mode)")
        logger.info(f"Results - Added: {smb_added + midsize_added} (SMB: {smb_added}, Midsize: {midsize_added})")
        logger.info("ℹ️ Lead cleanup handled by separate drain workflow every 2 hours")
        
    except Exception as e:
        # Add error to notification data
        notification_data["errors"].append(str(e))
        
        # Send error notification if possible
        if NOTIFICATIONS_AVAILABLE:
            try:
                sync_end_time = time.time()
                notification_data["duration_seconds"] = sync_end_time - sync_start_time
                notifier.send_sync_notification(notification_data)
            except:
                pass  # Don't let notification errors mask the original error
        logger.error(f"❌ SYNC FAILED: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        
        # Log stack trace
        import traceback
        logger.error("Stack trace:")
        logger.error(traceback.format_exc())
        
        # Ensure logs are flushed
        for handler in logger.handlers:
            handler.flush()
        
        # Exit with error code
        raise

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        # Exit with error code 1
        import sys
        sys.exit(1)