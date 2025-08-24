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
        logger.info("Loaded INSTANTLY_API_KEY from config file")
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

def get_finished_leads() -> List[InstantlyLead]:
    """Get leads with terminal status from Instantly."""
    try:
        # For now, return empty list since we need to implement lead status tracking differently
        # The API doesn't have a direct "get leads by campaign" endpoint
        # We'll track status through our own BigQuery tables
        logger.info("Skipping drain - will be implemented with lead status tracking")
        return []
    
    except Exception as e:
        logger.error(f"Failed to get finished leads: {e}")
        return []

def update_bigquery_state(leads: List[InstantlyLead]) -> None:
    """Update BigQuery with lead status and history."""
    if not leads or DRY_RUN:
        return
    
    try:
        # Update ops_inst_state
        for lead in leads:
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
            
            # Add to history if completed
            if lead.status == 'completed':
                history_query = f"""
                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_lead_history`
                (email, campaign_id, sequence_name, status_final, completed_at)
                VALUES (@email, @campaign_id, @sequence_name, 'completed', CURRENT_TIMESTAMP())
                """
                
                sequence_name = 'SMB' if lead.campaign_id == SMB_CAMPAIGN_ID else 'Midsize'
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("email", "STRING", lead.email),
                        bigquery.ScalarQueryParameter("campaign_id", "STRING", lead.campaign_id),
                        bigquery.ScalarQueryParameter("sequence_name", "STRING", sequence_name),
                    ]
                )
                
                bq_client.query(history_query, job_config=job_config).result()
            
            # Add unsubscribes to DNC list
            if lead.status == 'unsubscribed':
                dnc_query = f"""
                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dnc_list`
                (id, email, domain, source, reason, added_date, added_by, is_active)
                VALUES (
                    GENERATE_UUID(), 
                    @email, 
                    SPLIT(@email, '@')[OFFSET(1)], 
                    'instantly', 
                    'unsubscribe', 
                    CURRENT_TIMESTAMP(), 
                    'sync_script', 
                    TRUE
                )
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("email", "STRING", lead.email),
                    ]
                )
                
                bq_client.query(dnc_query, job_config=job_config).result()
        
        logger.info(f"Updated BigQuery state for {len(leads)} leads")
    
    except Exception as e:
        logger.error(f"Failed to update BigQuery state: {e}")
        log_dead_letter('bigquery_update', None, json.dumps([l.__dict__ for l in leads]), 0, str(e))

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

def create_lead_in_instantly(lead: Lead, campaign_id: str) -> Optional[str]:
    """Create a single lead in Instantly campaign."""
    try:
        data = {
            'email': lead.email,
            'first_name': '',  # Not available in our data
            'last_name': '',   # Not available in our data
            'company_name': lead.merchant_name,
            'campaign_id': campaign_id,  # Specify which campaign to add to
            'custom_variables': {
                'company': lead.merchant_name,
                'domain': lead.platform_domain,
                'location': lead.state,
                'country': lead.country_code
            }
        }
        
        # Use the correct V2 endpoint
        response = call_instantly_api('/api/v2/leads', method='POST', data=data)
        
        if DRY_RUN:
            return 'dry-run-id'
        
        # Check for success in the response
        if response.get('id'):  # V2 API returns the lead ID directly
            logger.info(f"✅ Created lead {lead.email} with ID {response['id']}")
            return response['id']
        else:
            logger.error(f"Failed to create lead {lead.email}: {response}")
            return None
    
    except Exception as e:
        if '409' in str(e) or 'already exists' in str(e).lower():
            # Lead already exists, try to move it
            logger.info(f"Lead {lead.email} already exists, attempting move")
            return move_lead_to_campaign(lead, campaign_id)
        else:
            logger.error(f"Failed to create lead {lead.email}: {e}")
            return None

def move_lead_to_campaign(lead: Lead, campaign_id: str) -> Optional[str]:
    """Move existing lead to different campaign."""
    try:
        data = {
            'email': lead.email,
            'to_campaign_id': campaign_id
        }
        
        response = call_instantly_api('/api/v2/leads/move', method='POST', data=data)
        
        if DRY_RUN:
            return 'dry-run-move-id'
        
        if response.get('success'):
            return response.get('lead_id')
        else:
            logger.error(f"Failed to move lead {lead.email}: {response}")
            return None
    
    except Exception as e:
        logger.error(f"Failed to move lead {lead.email}: {e}")
        log_dead_letter('move_lead', lead.email, json.dumps(data), 0, str(e))
        return None

def update_ops_state(leads: List[Lead], campaign_id: str, lead_ids: List[str]) -> None:
    """Update ops_inst_state with newly added leads."""
    if DRY_RUN:
        return
    
    try:
        for lead, lead_id in zip(leads, lead_ids):
            if lead_id:
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
    """Process a batch of leads for a specific campaign."""
    if not leads:
        return 0
    
    logger.info(f"Processing batch of {len(leads)} leads for campaign {campaign_id}")
    
    successful_ids = []
    
    # Process in smaller batches to respect rate limits
    for i in range(0, len(leads), BATCH_SIZE):
        batch = leads[i:i + BATCH_SIZE]
        batch_ids = []
        
        for lead in batch:
            lead_id = create_lead_in_instantly(lead, campaign_id)
            batch_ids.append(lead_id)
            time.sleep(0.5)  # Rate limiting between individual calls
        
        successful_ids.extend(batch_ids)
        update_ops_state(batch, campaign_id, batch_ids)
        
        if i + BATCH_SIZE < len(leads):  # Not the last batch
            logger.info(f"Sleeping {BATCH_SLEEP_SECONDS}s between batches...")
            time.sleep(BATCH_SLEEP_SECONDS)
    
    successful_count = len([id for id in successful_ids if id])
    logger.info(f"Successfully processed {successful_count}/{len(leads)} leads")
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
            'dry_run': DRY_RUN
        }
        
        logger.info(f"Summary:")
        logger.info(f"  - Current inventory: {inventory:,} leads")
        logger.info(f"  - Eligible leads: {eligible_count:,}")  
        logger.info(f"  - Mailboxes: {mailbox_count} ({daily_capacity} emails/day)")
        logger.info(f"  - Safe capacity: {safe_inventory_limit:,} leads (utilization: {capacity_utilization:.1f}%)")
        logger.info(f"  - Legacy cap guard: {INSTANTLY_CAP_GUARD:,}")
        
        return metrics
    
    except Exception as e:
        logger.error(f"Housekeeping failed: {e}")
        return {'error': str(e)}

def main():
    """Main synchronization function."""
    logger.info("🚀 STARTING COLD EMAIL SYNC")
    logger.info(f"Config - Target: {TARGET_NEW_LEADS_PER_RUN}, Cap: {INSTANTLY_CAP_GUARD}, Multiplier: {LEAD_INVENTORY_MULTIPLIER}, Dry Run: {DRY_RUN}")
    
    try:
        # Step 1: Drain finished leads first
        drained = drain_finished_leads()
        
        # Step 2: Top up campaigns
        smb_added, midsize_added = top_up_campaigns()
        
        # Step 3: Housekeeping
        metrics = housekeeping()
        
        # Final summary
        logger.info("✅ SYNC COMPLETE")
        logger.info(f"Results - Drained: {drained}, Added: {smb_added + midsize_added} (SMB: {smb_added}, Midsize: {midsize_added})")
        
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