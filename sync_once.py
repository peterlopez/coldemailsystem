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
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

import requests
from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
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

# Configuration from environment
PROJECT_ID = "instant-ground-394115"
DATASET_ID = "email_analytics"
TARGET_NEW_LEADS_PER_RUN = int(os.getenv('TARGET_NEW_LEADS_PER_RUN', '100'))
INSTANTLY_CAP_GUARD = int(os.getenv('INSTANTLY_CAP_GUARD', '24000'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
BATCH_SLEEP_SECONDS = int(os.getenv('BATCH_SLEEP_SECONDS', '10'))
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'

# Campaign configuration
SMB_CAMPAIGN_ID = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
MIDSIZE_CAMPAIGN_ID = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'

# Mailbox capacity management
LEAD_INVENTORY_MULTIPLIER = float(os.getenv('LEAD_INVENTORY_MULTIPLIER', '3.5'))  # Conservative start

# Email verification settings
VERIFY_EMAILS_BEFORE_CREATION = os.getenv('VERIFY_EMAILS_BEFORE_CREATION', 'true').lower() == 'true'
VERIFICATION_VALID_STATUSES = ['valid', 'accept_all']  # Configurable valid statuses
VERIFICATION_TIMEOUT = int(os.getenv('VERIFICATION_TIMEOUT', '10'))  # Max wait for pending

# Instantly API configuration
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
logger.info(f"Environment INSTANTLY_API_KEY present: {bool(INSTANTLY_API_KEY)}")

if not INSTANTLY_API_KEY:
    # Fallback to config file if environment variable not set (local development)
    logger.info("INSTANTLY_API_KEY not found in environment, attempting to load from config file")
    try:
        # Add current directory to Python path to help with imports
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from config.config import Config
        config = Config()
        INSTANTLY_API_KEY = config.instantly_api_key
        logger.info(f"Loaded INSTANTLY_API_KEY from config file: {'[PRESENT]' if INSTANTLY_API_KEY else '[EMPTY/NONE]'}")
    except ImportError as e:
        logger.warning(f"Could not import config module: {e}")
        logger.info("This is expected in GitHub Actions where config module is not needed")
    except Exception as e:
        logger.error(f"Failed to load API key from config: {e}")
        logger.error("INSTANTLY_API_KEY must be set as environment variable or in config file")
        
if not INSTANTLY_API_KEY:
    logger.error("❌ INSTANTLY_API_KEY is not configured!")
    raise RuntimeError("INSTANTLY_API_KEY not configured")


INSTANTLY_BASE_URL = 'https://api.instantly.ai'

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
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed: {e}")
        # Log to dead letters
        log_dead_letter('api_call', None, str(data), getattr(e.response, 'status_code', 0), str(e))
        raise

def log_dead_letter(phase: str, email: Optional[str], payload: str, status_code: int, error_text: str) -> None:
    """Log failed operations to dead letters table."""
    try:
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

def get_instantly_headers() -> dict:
    """Get standard Instantly API headers."""
    return {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }

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
                # Create parameterized query for batch
                query = """
                SELECT 
                    instantly_lead_id,
                    last_drain_check,
                    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) as hours_since_check
                FROM `instant-ground-394115.email_analytics.ops_inst_state`
                WHERE instantly_lead_id IN UNNEST(@lead_ids)
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("lead_ids", "STRING", batch_ids),
                    ]
                    # Note: QueryJobConfig doesn't accept timeout parameters - use result() timeout instead
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
                for lead_id in batch_ids:
                    if lead_id not in found_lead_ids:
                        all_results[lead_id] = True
                        logger.debug(f"📝 Lead {lead_id} not in tracking - needs first drain check")
                        
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
        
        # Update timestamp using MERGE to handle both insert and update cases with timeout
        query = """
        MERGE `instant-ground-394115.email_analytics.ops_inst_state` AS target
        USING (
            SELECT @lead_id as instantly_lead_id, CURRENT_TIMESTAMP() as check_time
        ) AS source
        ON target.instantly_lead_id = source.instantly_lead_id
        WHEN MATCHED THEN
            UPDATE SET last_drain_check = source.check_time, updated_at = source.check_time
        WHEN NOT MATCHED THEN
            INSERT (instantly_lead_id, last_drain_check, added_at, updated_at)
            VALUES (source.instantly_lead_id, source.check_time, source.check_time, source.check_time)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lead_id", "STRING", lead_id),
            ],
            job_timeout=15  # 15 second timeout
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
                # Create batch update query
                query = """
                MERGE `instant-ground-394115.email_analytics.ops_inst_state` AS target
                USING (
                    SELECT lead_id as instantly_lead_id, CURRENT_TIMESTAMP() as check_time
                    FROM UNNEST(@lead_ids) AS lead_id
                ) AS source
                ON target.instantly_lead_id = source.instantly_lead_id
                WHEN MATCHED THEN
                    UPDATE SET last_drain_check = source.check_time, updated_at = source.check_time
                WHEN NOT MATCHED THEN
                    INSERT (instantly_lead_id, last_drain_check, added_at, updated_at)
                    VALUES (source.instantly_lead_id, source.check_time, source.check_time, source.check_time)
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("lead_ids", "STRING", batch_ids),
                    ],
                    job_timeout=30  # 30 second timeout per batch
                )
                
                query_job = bq_client.query(query, job_config=job_config)
                query_job.result(timeout=30)  # 30 second result timeout
                
                logger.debug(f"✅ Batch updated {len(batch_ids)} drain timestamps")
                
            except Exception as batch_error:
                logger.error(f"❌ Batch timestamp update failed: {batch_error}")
                # Fall back to individual updates for this batch
                for lead_id in batch_ids:
                    update_lead_drain_check_timestamp(lead_id)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error batch updating drain timestamps: {e}")
        return False

def classify_lead_for_drain(lead: dict, campaign_name: str) -> dict:
    """
    Classify a lead from Instantly API to determine if it should be drained.
    
    Based on approved drain logic:
    - Trust Instantly's OOO detection (stop_on_auto_reply=false)
    - Use status codes to differentiate replies 
    - 7-day grace period for delivery issues
    - Allow bounced emails to retry later
    """
    try:
        email = lead.get('email', 'unknown')
        status = lead.get('status', 0)  # Status code from Instantly
        esp_code = lead.get('esp_code', 0)  # Email service provider code
        email_reply_count = lead.get('email_reply_count', 0)
        created_at = lead.get('created_at')
        
        # Parse creation date for time-based decisions
        days_since_created = 0
        if created_at:
            try:
                from datetime import datetime
                created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                days_since_created = (datetime.now().astimezone() - created_date).days
            except:
                days_since_created = 0
        
        # DRAIN DECISION LOGIC (based on approved plan)
        
        # 1. Status 3 = Processed/Finished leads
        if status == 3:
            if email_reply_count > 0:
                # Trust Instantly's reply detection (they handle OOO filtering)
                return {
                    'should_drain': True,
                    'drain_reason': 'replied',
                    'details': f'Status 3 with {email_reply_count} replies - genuine engagement'
                }
            else:
                # Sequence completed without replies
                return {
                    'should_drain': True,
                    'drain_reason': 'completed',
                    'details': 'Sequence completed without replies'
                }
        
        # 2. ESP Code analysis for email delivery issues
        if esp_code in [550, 551, 553]:  # Hard bounces
            if days_since_created >= 7:  # 7-day grace period
                return {
                    'should_drain': True,
                    'drain_reason': 'bounced_hard',
                    'details': f'Hard bounce (ESP {esp_code}) after {days_since_created} days'
                }
            else:
                return {
                    'should_drain': False,
                    'keep_reason': f'Recent hard bounce (ESP {esp_code}), within 7-day grace period'
                }
        
        if esp_code in [421, 450, 451]:  # Soft bounces
            if days_since_created >= 7:  # Allow retry period
                return {
                    'should_drain': False,
                    'keep_reason': f'Soft bounce (ESP {esp_code}) - keeping for retry'
                }
        
        # 3. Unsubscribes (if available in API data)
        if 'unsubscribed' in str(lead.get('status_text', '')).lower():
            return {
                'should_drain': True,
                'drain_reason': 'unsubscribed',
                'details': 'Lead unsubscribed from campaign'
            }
        
        # 4. Very old active leads (90+ days) - potential stuck leads
        if status == 1 and days_since_created >= 90:
            return {
                'should_drain': True,
                'drain_reason': 'stale_active',
                'details': f'Active lead stuck for {days_since_created} days'
            }
        
        # DEFAULT: Keep active leads (Status 1) and recent leads
        return {
            'should_drain': False,
            'keep_reason': f'Active lead (Status {status}) - {days_since_created} days old'
        }
        
    except Exception as e:
        logger.error(f"Error classifying lead {lead.get('email', 'unknown')}: {e}")
        # Conservative approach: don't drain on error
        return {
            'should_drain': False,
            'keep_reason': f'Classification error - keeping safely: {str(e)}'
        }

def get_finished_leads() -> List[InstantlyLead]:
    """Get leads with terminal status from Instantly using proper cursor-based pagination and time filtering."""
    try:
        logger.info("🔄 DRAIN: Fetching finished leads from Instantly campaigns...")
        
        finished_leads = []
        
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
            consecutive_401_errors = 0
            
            while True:
                # Use proper cursor-based pagination
                url = f"{INSTANTLY_BASE_URL}/api/v2/leads/list"
                payload = {
                    "campaign_id": campaign_id,
                    "limit": 50  # Get 50 leads per page (conservative approach)
                }
                
                if starting_after:
                    payload["starting_after"] = starting_after
                
                # RATE LIMITING: Add delay between API calls to prevent 401 errors
                if page_count > 0:  # Don't delay the first call
                    logger.debug(f"⏸️ Rate limiting: waiting 3 seconds before next API call...")
                    time.sleep(3.0)  # 3 second delay between pagination calls (increased for larger pages)
                
                response = requests.post(
                    url,
                    headers=get_instantly_headers(),
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    # Reset 401 counter on successful response
                    consecutive_401_errors = 0
                    
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
                    
                    # Process leads that need evaluation
                    for lead in leads:
                        lead_id = lead.get('id', '')
                        email = lead.get('email', '')
                        
                        if not lead_id:
                            logger.debug(f"⚠️ Skipping lead with no ID: {email}")
                            continue
                            
                        # Check if lead needs evaluation (from batch results)
                        if leads_check_results.get(lead_id, True):  # Default to True if not found
                            leads_needing_check += 1
                            
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
                                
                                logger.info(f"🗑️ Marking for drain: {email} - {classification['drain_reason']}")
                            else:
                                logger.debug(f"⏸️ Keeping: {email} - {classification['keep_reason']}")
                            
                            # Queue for batch timestamp update (don't do individual updates)
                            leads_to_update_timestamps.append(lead_id)
                            
                        else:
                            logger.debug(f"⏰ Skipping recent check: {email} (checked within 24h)")
                    
                    # Get cursor for next page
                    starting_after = data.get('next_starting_after')
                    if not starting_after:
                        logger.info(f"✅ Reached end of {campaign_name} campaign - no more pages")
                        break
                    
                    # Safety check to prevent infinite loops (now with proper limit)
                    if page_count >= 60:  # Max 60 pages (3,000 leads per campaign with 50/page)
                        logger.warning(f"⚠️ Reached safety limit of 60 pages for {campaign_name} (processed {total_leads_accessed} leads)")
                        break
                
                elif response.status_code == 401:
                    # Rate limiting likely cause of 401 errors - be more aggressive with backoff
                    consecutive_401_errors += 1
                    
                    if consecutive_401_errors >= 5:
                        logger.error(f"❌ Too many consecutive 401 errors ({consecutive_401_errors}) for {campaign_name} - stopping pagination")
                        break
                    
                    backoff_time = min(10 * consecutive_401_errors, 60)  # 10s, 20s, 30s, 40s, 60s max
                    logger.warning(f"⚠️ Got 401 error #{consecutive_401_errors} (likely rate limiting) for {campaign_name} on page {page_count + 1}")
                    logger.info(f"💤 Backing off for {backoff_time} seconds before retry...")
                    time.sleep(backoff_time)
                    continue  # Retry the same page
                    
                else:
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
                    # Continue processing - timestamp updates are not critical for drain functionality
            
            logger.info(f"📊 {campaign_name} campaign: accessed {total_leads_accessed} leads ({len(seen_lead_ids)} unique) in {page_count} pages")
            logger.info(f"📊 {campaign_name} campaign: {leads_needing_check} leads evaluated (24hr+ since last check)")
        
        logger.info(f"✅ DRAIN: Found {len(finished_leads)} leads to drain across all campaigns")
        return finished_leads
        
    except Exception as e:
        logger.error(f"❌ Failed to get finished leads: {e}")
        return []

def update_bigquery_state(leads: List[InstantlyLead]) -> None:
    """Update BigQuery with lead status and history - enhanced for new drain logic."""
    if not leads or DRY_RUN:
        return
    
    try:
        logger.info(f"📊 Updating BigQuery state for {len(leads)} drained leads...")
        
        # Track drain reasons for reporting
        drain_reasons = {}
        
        for lead in leads:
            # Count drain reasons for summary
            drain_reasons[lead.status] = drain_reasons.get(lead.status, 0) + 1
            
            # Update ops_inst_state with new status
            query = f"""
            MERGE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` T
            USING (SELECT @email as email, @campaign_id as campaign_id, @status as status) S
            ON LOWER(T.email) = LOWER(S.email) AND T.campaign_id = S.campaign_id
            WHEN MATCHED THEN
              UPDATE SET status = S.status, updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT (email, campaign_id, status, instantly_lead_id, added_at, updated_at)
              VALUES (S.email, S.campaign_id, S.status, @lead_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", lead.email),
                    bigquery.ScalarQueryParameter("campaign_id", "STRING", lead.campaign_id),
                    bigquery.ScalarQueryParameter("status", "STRING", lead.status),
                    bigquery.ScalarQueryParameter("lead_id", "STRING", lead.id),
                ]
            )
            
            bq_client.query(query, job_config=job_config).result()
            
            # Add to history for completed/replied leads (90-day cooldown)
            if lead.status in ['completed', 'replied']:
                history_query = f"""
                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_lead_history`
                (email, campaign_id, sequence_name, status_final, completed_at, attempt_num)
                VALUES (@email, @campaign_id, @sequence_name, @status, CURRENT_TIMESTAMP(), 1)
                """
                
                sequence_name = 'SMB' if lead.campaign_id == SMB_CAMPAIGN_ID else 'Midsize'
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("email", "STRING", lead.email),
                        bigquery.ScalarQueryParameter("campaign_id", "STRING", lead.campaign_id),
                        bigquery.ScalarQueryParameter("sequence_name", "STRING", sequence_name),
                        bigquery.ScalarQueryParameter("status", "STRING", lead.status),
                    ]
                )
                
                bq_client.query(history_query, job_config=job_config).result()
                logger.debug(f"📝 Added {lead.email} to 90-day cooldown history")
            
            # Add unsubscribes to DNC list (permanent block)
            if lead.status == 'unsubscribed':
                dnc_query = f"""
                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dnc_list`
                (id, email, domain, source, reason, added_date, added_by, is_active)
                VALUES (
                    GENERATE_UUID(), 
                    @email, 
                    SPLIT(@email, '@')[OFFSET(1)], 
                    'instantly_drain', 
                    'unsubscribe_via_api', 
                    CURRENT_TIMESTAMP(), 
                    'sync_script_v2', 
                    TRUE
                )
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("email", "STRING", lead.email),
                    ]
                )
                
                bq_client.query(dnc_query, job_config=job_config).result()
                logger.info(f"🚫 Added {lead.email} to permanent DNC list")
        
        # Log summary of drain reasons
        logger.info(f"✅ Updated BigQuery state - Drain summary:")
        for reason, count in drain_reasons.items():
            logger.info(f"  - {reason}: {count} leads")
    
    except Exception as e:
        logger.error(f"❌ Failed to update BigQuery state: {e}")
        log_dead_letter('bigquery_update_drain', None, json.dumps([l.__dict__ for l in leads]), 0, str(e))

def delete_leads_from_instantly(leads: List[InstantlyLead]) -> None:
    """Delete finished leads from Instantly to free inventory."""
    if not leads:
        return
    
    try:
        for lead in leads:
            if DRY_RUN:
                logger.info(f"DRY RUN: Would delete lead {lead.email} from Instantly")
                continue
                
            try:
                call_instantly_api(f'/api/v2/leads/{lead.id}', method='DELETE')
                logger.info(f"Deleted lead {lead.email} from Instantly")
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logger.error(f"Failed to delete lead {lead.email}: {e}")
                log_dead_letter('delete_lead', lead.email, lead.id, 0, str(e))
    
    except Exception as e:
        logger.error(f"Failed to delete leads: {e}")

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
    """Get current mailbox capacity from Instantly API."""
    try:
        # Get mailbox information from Instantly
        response = call_instantly_api('/api/v1/account/emails', method='GET')
        
        if DRY_RUN:
            # For dry run, simulate 68 mailboxes at 10 emails/day each
            logger.info("DRY RUN: Simulating 68 mailboxes at 10 emails/day capacity")
            return 68, 680
        
        if not response or 'emails' not in response:
            logger.warning("Could not get mailbox data from API, using fallback estimate")
            # Fallback: assume 68 mailboxes at 10 emails/day (early warmup)
            return 68, 680
        
        mailboxes = response['emails']
        total_mailboxes = len(mailboxes)
        
        # Calculate total daily capacity
        # Each mailbox: assume 10 emails/day initially (warmup), scaling to 30
        # For now, use conservative estimate of 10 emails/day per mailbox
        daily_capacity = total_mailboxes * 10  # Will enhance this to read actual limits
        
        logger.info(f"Mailbox capacity: {total_mailboxes} mailboxes, {daily_capacity} emails/day")
        return total_mailboxes, daily_capacity
        
    except Exception as e:
        logger.error(f"Failed to get mailbox capacity: {e}")
        logger.info("Using fallback capacity estimate")
        return 68, 680  # Fallback estimate

def get_current_instantly_inventory() -> int:
    """Get current lead count in Instantly (both campaigns)."""
    try:
        # For now, use our BigQuery tracking instead of Instantly API
        query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` WHERE status = 'active'"
        result = bq_client.query(query).result()
        total = next(result).count
        
        logger.info(f"Current Instantly inventory (tracked): {total}")
        return total
    except Exception as e:
        logger.error(f"Failed to get inventory: {e}")
        return 0

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
        
        logger.info(f"Capacity calculation:")
        logger.info(f"  - Mailboxes: {mailbox_count}")
        logger.info(f"  - Daily capacity: {daily_capacity} emails")
        logger.info(f"  - Safe inventory limit: {safe_inventory_limit} leads (multiplier: {LEAD_INVENTORY_MULTIPLIER})")
        logger.info(f"  - Current inventory: {current_inventory} leads")
        logger.info(f"  - Available capacity: {available_capacity} leads")
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
               -- Add priority tiers for analysis
               CASE 
                 WHEN DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', klaviyo_installed_at)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) THEN 'HOT'
                 WHEN DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', klaviyo_installed_at)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) THEN 'WARM' 
                 ELSE 'COLD'
               END as klaviyo_priority
        FROM `{PROJECT_ID}.{DATASET_ID}.v_ready_for_instantly`
        WHERE email IS NOT NULL AND email != ''
        ORDER BY 
          PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', klaviyo_installed_at) DESC NULLS LAST,
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

def verify_email(email: str) -> dict:
    """Verify email using Instantly.ai verification API."""
    try:
        # Skip verification in dry run mode
        if DRY_RUN:
            return {
                'email': email,
                'status': 'valid',
                'catch_all': False,
                'credits_used': 0
            }
        
        data = {'email': email}
        response = call_instantly_api('/api/v2/email-verification', 
                                    method='POST', 
                                    data=data)
        
        # Handle pending status with polling
        if response.get('verification_status') == 'pending':
            logger.info(f"Verification pending for {email}, waiting...")
            time.sleep(2)  # Wait before checking status
            response = call_instantly_api(f'/api/v2/email-verification/{email}', 
                                        method='GET')
        
        return {
            'email': email,
            'status': response.get('verification_status', 'unknown'),
            'catch_all': response.get('catch_all', False),
            'credits_used': response.get('credits_used', 1)
        }
    except Exception as e:
        logger.error(f"Email verification failed for {email}: {e}")
        log_dead_letter('verification', email, None, None, None, str(e))
        return {'email': email, 'status': 'error', 'error': str(e)}

def create_lead_in_instantly(lead: Lead, campaign_id: str) -> Optional[str]:
    """Create a single lead in Instantly campaign with proper campaign assignment."""
    try:
        # Step 1: Create the lead with basic data (no campaign assignment in creation)
        basic_data = {
            'email': lead.email,
            'first_name': '',  # Not available in our data
            'last_name': '',   # Not available in our data
            'company_name': lead.merchant_name,
            'custom_variables': {
                'company': lead.merchant_name,
                'domain': lead.platform_domain,
                'location': lead.state,
                'country': lead.country_code
            }
        }
        
        # Create lead without campaign assignment first
        logger.debug(f"Creating lead {lead.email} (Step 1/2)")
        response = call_instantly_api('/api/v2/leads', method='POST', data=basic_data)
        
        if DRY_RUN:
            return 'dry-run-id'
        
        # Check for successful creation
        lead_id = response.get('id')
        if not lead_id:
            logger.error(f"Failed to create lead {lead.email}: {response}")
            return None
        
        logger.info(f"✅ Created lead {lead.email} with ID {lead_id}")
        
        # Step 2: Move lead to the specified campaign
        logger.debug(f"Assigning lead {lead.email} to campaign (Step 2/2)")
        move_data = {
            'ids': [lead_id],
            'to_campaign_id': campaign_id
        }
        
        move_response = call_instantly_api('/api/v2/leads/move', method='POST', data=move_data)
        
        if move_response.get('status') == 'pending':
            logger.info(f"🎯 Lead {lead.email} assignment to campaign queued (async operation)")
            return lead_id
        else:
            logger.warning(f"Campaign assignment may have failed for {lead.email}: {move_response}")
            # Still return the lead_id as the lead was created successfully
            return lead_id
    
    except Exception as e:
        if '409' in str(e) or 'already exists' in str(e).lower():
            # Lead already exists, try to move it
            logger.info(f"Lead {lead.email} already exists, attempting move")
            return move_lead_to_campaign(lead, campaign_id)
        else:
            logger.error(f"Failed to create lead {lead.email}: {e}")
            return None

def move_lead_to_campaign(lead: Lead, campaign_id: str) -> Optional[str]:
    """Move existing lead to different campaign using correct API endpoint."""
    try:
        # First, try to find the existing lead by email
        # Since we don't have a direct email search, we'll need the lead ID
        # This function is called when a lead already exists (409 error)
        # So we need to handle this differently
        
        logger.warning(f"Lead {lead.email} already exists, but we need lead ID to move it")
        logger.warning("Current move_lead_to_campaign needs enhancement to find existing lead ID")
        
        # For now, return None to indicate we couldn't move it
        # The calling function will handle this appropriately
        return None
    
    except Exception as e:
        logger.error(f"Failed to move lead {lead.email}: {e}")
        log_dead_letter('move_lead', lead.email, json.dumps({'campaign_id': campaign_id}), 0, str(e))
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
                
                # Build insert query with verification fields
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
                    # Original query without verification fields
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

def process_lead_batch(leads: List[Lead], campaign_id: str) -> int:
    """Process a batch of leads for a specific campaign with email verification."""
    if not leads:
        return 0
    
    logger.info(f"Processing batch of {len(leads)} leads for campaign {campaign_id}")
    
    # Verification phase
    verified_leads = []
    verification_results = []
    
    if VERIFY_EMAILS_BEFORE_CREATION:
        logger.info(f"Verifying {len(leads)} email addresses...")
        for lead in leads:
            verification = verify_email(lead.email)
            verification_results.append(verification)
            
            if verification['status'] in VERIFICATION_VALID_STATUSES:
                verified_leads.append(lead)
                logger.debug(f"✅ {lead.email} verified as {verification['status']}")
            else:
                logger.info(f"❌ Skipping {lead.email}: {verification['status']}")
        
        logger.info(f"Verified {len(verified_leads)}/{len(leads)} leads as valid")
    else:
        # Skip verification if disabled
        verified_leads = leads
        logger.info("Email verification disabled, processing all leads")
    
    successful_ids = []
    
    # Process in smaller batches to respect rate limits
    for i in range(0, len(verified_leads), BATCH_SIZE):
        batch = verified_leads[i:i + BATCH_SIZE]
        batch_ids = []
        
        for lead in batch:
            lead_id = create_lead_in_instantly(lead, campaign_id)
            batch_ids.append(lead_id)
            time.sleep(0.5)  # Rate limiting between individual calls
        
        successful_ids.extend(batch_ids)
        
        # Update ops_state with verification results
        if verification_results:
            # Only pass verification results for the current batch
            batch_verifications = []
            for lead in batch:
                # Find matching verification result
                for v in verification_results:
                    if v['email'] == lead.email:
                        batch_verifications.append(v)
                        break
            update_ops_state(batch, campaign_id, batch_ids, batch_verifications)
        else:
            update_ops_state(batch, campaign_id, batch_ids)
        
        if i + BATCH_SIZE < len(verified_leads):  # Not the last batch
            logger.info(f"Sleeping {BATCH_SLEEP_SECONDS}s between batches...")
            time.sleep(BATCH_SLEEP_SECONDS)
    
    successful_count = len([id for id in successful_ids if id])
    logger.info(f"Successfully processed {successful_count}/{len(verified_leads)} verified leads")
    return successful_count

def top_up_campaigns() -> Tuple[int, int]:
    """Add new eligible leads to campaigns using smart capacity management."""
    logger.info("=== TOPPING UP CAMPAIGNS ===")
    
    # Calculate smart lead target based on mailbox capacity
    target_leads = calculate_smart_lead_target()
    
    if target_leads == 0:
        logger.info("Smart capacity management: No leads to add this run")
        return 0, 0
    
    # Check legacy inventory guard as backup safety
    current_inventory = get_current_instantly_inventory()
    if current_inventory >= INSTANTLY_CAP_GUARD:
        logger.warning(f"Legacy safety guard triggered: Inventory at {current_inventory}, skipping top-up (guard: {INSTANTLY_CAP_GUARD})")
        return 0, 0
    
    # Get eligible leads using smart target
    logger.info(f"Smart targeting: requesting {target_leads} leads for this run")
    leads = get_eligible_leads(target_leads)
    
    if not leads:
        logger.info("No eligible leads found")
        return 0, 0
    
    # Split by segment
    smb_leads, midsize_leads = split_leads_by_segment(leads)
    
    logger.info(f"Found {len(smb_leads)} SMB and {len(midsize_leads)} Midsize leads")
    
    # Process each segment
    smb_processed = process_lead_batch(smb_leads, SMB_CAMPAIGN_ID)
    midsize_processed = process_lead_batch(midsize_leads, MIDSIZE_CAMPAIGN_ID)
    
    logger.info(f"Top-up complete: {smb_processed} SMB + {midsize_processed} Midsize = {smb_processed + midsize_processed} total")
    return smb_processed, midsize_processed

def housekeeping() -> Dict:
    """Generate summary metrics and perform housekeeping."""
    logger.info("=== HOUSEKEEPING ===")
    
    try:
        # Get current counts
        inventory = get_current_instantly_inventory()
        mailbox_count, daily_capacity = get_mailbox_capacity()
        safe_inventory_limit = int(daily_capacity * LEAD_INVENTORY_MULTIPLIER)
        
        # Get eligible count
        query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.v_ready_for_instantly`"
        result = bq_client.query(query).result()
        eligible_count = next(result).count
        
        # Get verification metrics if enabled
        verification_stats = {}
        if VERIFY_EMAILS_BEFORE_CREATION:
            try:
                verification_query = f'''
                SELECT 
                    verification_status,
                    COUNT(*) as count,
                    COALESCE(SUM(verification_credits_used), 0) as total_credits
                FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
                WHERE verified_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
                  AND verification_status IS NOT NULL
                GROUP BY verification_status
                '''
                
                verification_result = bq_client.query(verification_query).result()
                
                logger.info("📊 Verification Stats (Last 24h):")
                total_verified = 0
                total_credits = 0
                
                for row in verification_result:
                    status = row.verification_status
                    count = row.count
                    credits = row.total_credits
                    
                    verification_stats[status] = {
                        'count': count,
                        'credits': credits
                    }
                    
                    total_verified += count
                    total_credits += credits
                    
                    logger.info(f"  - {status}: {count} emails, {credits} credits")
                
                if total_verified > 0:
                    valid_count = sum(stats['count'] for status, stats in verification_stats.items() 
                                    if status in VERIFICATION_VALID_STATUSES)
                    valid_rate = (valid_count / total_verified * 100) if total_verified > 0 else 0
                    logger.info(f"  - Total verified: {total_verified}, Valid rate: {valid_rate:.1f}%")
                    logger.info(f"  - Credits used: {total_credits}")
                
            except Exception as e:
                logger.warning(f"Could not get verification metrics: {e}")
        
        # Calculate utilization metrics
        capacity_utilization = (inventory / safe_inventory_limit * 100) if safe_inventory_limit > 0 else 0
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'instantly_inventory': inventory,
            'eligible_leads': eligible_count,
            'mailbox_count': mailbox_count,
            'daily_email_capacity': daily_capacity,
            'safe_inventory_limit': safe_inventory_limit,
            'capacity_utilization_pct': round(capacity_utilization, 1),
            'lead_multiplier': LEAD_INVENTORY_MULTIPLIER,
            'cap_guard': INSTANTLY_CAP_GUARD,
            'dry_run': DRY_RUN,
            'verification_enabled': VERIFY_EMAILS_BEFORE_CREATION,
            'verification_stats': verification_stats
        }
        
        logger.info(f"Summary:")
        logger.info(f"  - Current inventory: {inventory:,} leads")
        logger.info(f"  - Eligible leads: {eligible_count:,}")  
        logger.info(f"  - Mailboxes: {mailbox_count} ({daily_capacity} emails/day)")
        logger.info(f"  - Safe capacity: {safe_inventory_limit:,} leads (utilization: {capacity_utilization:.1f}%)")
        logger.info(f"  - Legacy cap guard: {INSTANTLY_CAP_GUARD:,}")
        logger.info(f"  - Verification: {'ENABLED' if VERIFY_EMAILS_BEFORE_CREATION else 'DISABLED'}")
        
        return metrics
    
    except Exception as e:
        logger.error(f"Housekeeping failed: {e}")
        return {'error': str(e)}

def main():
    """Main synchronization function - Fast Mode (WITHOUT drain phase)."""
    logger.info("🚀 STARTING COLD EMAIL SYNC (Fast Mode)")
    logger.info(f"Config - Target: {TARGET_NEW_LEADS_PER_RUN}, Cap: {INSTANTLY_CAP_GUARD}, Multiplier: {LEAD_INVENTORY_MULTIPLIER}, Dry Run: {DRY_RUN}")
    logger.info("ℹ️ NOTE: Drain phase now handled by separate workflow - this is FAST MODE")
    
    try:
        # REMOVED: Step 1: Drain finished leads (now handled by separate drain workflow)
        logger.info("⏭️ SKIPPING DRAIN: Handled by separate drain-leads workflow")
        
        # Step 1: Top up campaigns (renamed from Step 2)
        smb_added, midsize_added = top_up_campaigns()
        
        # Step 2: Housekeeping (renamed from Step 3)
        metrics = housekeeping()
        
        # Final summary
        logger.info("✅ SYNC COMPLETE (Fast Mode)")
        logger.info(f"Results - Added: {smb_added + midsize_added} (SMB: {smb_added}, Midsize: {midsize_added})")
        logger.info("ℹ️ Lead cleanup handled by separate drain workflow every 2 hours")
        
    except Exception as e:
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