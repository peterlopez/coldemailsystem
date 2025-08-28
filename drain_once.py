#!/usr/bin/env python3
"""
Dedicated Lead Drain Script - Enhanced Rate Limiting
Processes finished leads from Instantly campaigns with aggressive rate limiting for DELETE operations.
Runs independently from main sync to prevent rate limiting issues from blocking new lead acquisition.
"""

import os
import sys
import time
import logging
import re
from collections import deque
from datetime import datetime
from typing import List, Tuple, Optional

# OPTIMIZED: Use shared configuration
try:
    from shared_config import config
    print("✅ Loaded shared configuration")
except ImportError:
    print("⚠️ Shared config not available, using environment fallback")
    config = None

# Import core functions from sync_once (except the broken HTTP helper)
try:
    from sync_once import (
        get_finished_leads, update_bigquery_state, 
        DRY_RUN, InstantlyLead,
        log_dead_letter
    )
    print("✅ Successfully imported from sync_once")
    IMPORTS_AVAILABLE = True
except ImportError as e:
    print(f"❌ Failed to import from sync_once: {e}")
    # Set minimal defaults to prevent total failure
    DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
    IMPORTS_AVAILABLE = False

# Import the FIXED HTTP helper from verification poller (not the broken sync_once version)
try:
    from simple_async_verification import call_instantly_api
    print("✅ Successfully imported FIXED call_instantly_api from verification poller")
    HTTP_HELPER_AVAILABLE = True
except ImportError as e:
    print(f"❌ Failed to import fixed HTTP helper: {e}")
    HTTP_HELPER_AVAILABLE = False
    
    # Fallback HTTP helper (minimal implementation)
    def call_instantly_api(endpoint: str, method: str = 'GET', data: Optional[dict] = None) -> dict:
        print("⚠️ Using fallback HTTP helper - imports not available")
        return {"status_code": 200, "text": "fallback", "success": False}
    
    # Create minimal stubs to prevent crashes
    class InstantlyLead:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
    
    def get_finished_leads():
        return []
    
    def delete_lead_from_instantly(lead):
        return True
    
    def update_bigquery_state(leads):
        pass
    
    def log_dead_letter(*args):
        pass
    
    print("⚠️ Using minimal stubs - workflow will run but won't do much")

# Import notification system
try:
    from cold_email_notifier import notifier
    NOTIFICATIONS_AVAILABLE = True
    print("📡 Drain notification system loaded")
except ImportError as e:
    NOTIFICATIONS_AVAILABLE = False
    print(f"📴 Drain notification system not available: {e}")

# SEPARATE LOGGING CONFIGURATION FOR DRAIN WORKFLOW
# Note: This creates a separate logger but imported functions still use sync logger
log_format = '%(asctime)s - %(levelname)s - %(message)s'

# Create separate logger for drain workflow  
drain_logger = logging.getLogger('drain_workflow')
drain_logger.setLevel(logging.INFO)

# Remove any existing handlers to avoid conflicts
drain_logger.handlers.clear()

# Add console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter(log_format))
drain_logger.addHandler(console_handler)

# Add separate file handler for drain operations
drain_file_handler = logging.FileHandler('cold-email-drain.log')
drain_file_handler.setFormatter(logging.Formatter(log_format))
drain_logger.addHandler(drain_file_handler)

# Use this logger for drain-specific operations
logger = drain_logger

# INFO: Functions imported from sync_once.py will still use their original logger
# This means get_finished_leads() logs will go to sync log, but our drain-specific
# logs will go to drain log. This is actually a good separation of concerns.

def should_retry_status(status_code: int) -> bool:
    """Determine if a status code should be retried - matches poller logic."""
    if status_code in (400, 401, 403, 404, 422):  # Never retry these
        return False
    return status_code == 429 or status_code >= 500  # Only retry rate limits and server errors

def classify_status_code(status_code: int) -> str:
    """Classify status code for circuit breaker - matches poller logic."""
    if 200 <= status_code < 300:
        return 'ok'
    elif status_code == 404:
        return '404'
    elif status_code == 429:
        return '429'
    elif 400 <= status_code < 500:
        return '4xx'
    else:
        return '5xx'

def is_success_status(status_code: int) -> bool:
    """Check if status code represents success - matches poller logic."""
    return (200 <= status_code < 300) or (status_code == 404)

def extract_request_id(response_body: str) -> str:
    """Extract request ID from response body if available."""
    if not response_body:
        return "unknown"
    
    # Look for common request ID patterns
    patterns = [
        r'"request[_-]?id":\s*"([^"]+)"',
        r'"rid":\s*"([^"]+)"',
        r'request[_-]?id[=:]\s*([a-zA-Z0-9-]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, response_body, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return "not-found"

def delete_single_lead_with_retry(lead: InstantlyLead, max_retries: int = 2) -> Tuple[bool, str]:
    """Delete a single lead with smart retry logic - matches poller behavior.
    
    Returns:
        Tuple of (success, status_code) where status_code is 'ok', '404', '429', '5xx', or '4xx'
    """
    
    if DRY_RUN:
        logger.info(f"🧪 DRY RUN: Would delete lead {lead.email} (ID: {lead.id})")
        return True, 'ok'
    
    last_status_code = 0
    last_response_body = ""
    
    for attempt in range(max_retries + 1):
        try:
            # ✅ Use the FIXED HTTP helper with no Content-Type for DELETE
            logger.debug(f"🔄 Deleting lead {lead.email} via DELETE /api/v2/leads/{lead.id} (attempt {attempt + 1})")
            
            # ✅ Ensure no JSON body is passed (this was causing 400 errors)
            response = call_instantly_api(f'/api/v2/leads/{lead.id}', method='DELETE', data=None)
            
            if not response:
                logger.error(f"❌ No response from DELETE API for {lead.email}")
                log_dead_letter("drain", lead.email, f"DELETE {lead.id}", 0, "No response from API")
                return False, '5xx'
            
            # ✅ Extract status and body for rich logging
            status_code = response.get('status_code', 0)
            response_body = response.get('text', '')
            request_id = extract_request_id(response_body)
            
            last_status_code = status_code
            last_response_body = response_body
            
            # ✅ Rich body logging like the poller
            status_label = classify_status_code(status_code)
            if is_success_status(status_code):
                logger.info(f"✅ DELETE {status_code} {lead.email} rid={request_id} body={response_body}")
            else:
                logger.error(f"❌ DELETE {status_code} {lead.email} rid={request_id} body={response_body}")
            
            # ✅ Success logic: 2xx or 404 = success (matches poller exactly)
            if is_success_status(status_code):
                return True, status_label
            
            # ✅ Smart retry logic: only retry 429 and 5xx
            if not should_retry_status(status_code):
                # Don't retry 400/401/403/422 - log and fail immediately
                log_dead_letter("drain", lead.email, f"DELETE {lead.id}", status_code, 
                              f"Non-retriable error: {response_body}")
                return False, status_label
            
            # Retriable error - log and continue to next attempt
            if attempt < max_retries:
                sleep_time = 0.5 * (2 ** attempt)  # Exponential backoff: 0.5s, 1s
                logger.warning(f"⏳ Retriable error {status_code} for {lead.email}, retrying in {sleep_time}s (attempt {attempt + 1}/{max_retries + 1})")
                time.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"❌ Delete exception for {lead.email} (attempt {attempt + 1}): {e}")
            if attempt == max_retries:
                log_dead_letter("drain", lead.email, f"DELETE {lead.id}", 0, str(e))
                return False, '5xx'
    
    # All retries exhausted
    final_status = classify_status_code(last_status_code)
    log_dead_letter("drain", lead.email, f"DELETE {lead.id}", last_status_code, 
                   f"All retries exhausted. Last response: {last_response_body}")
    return False, final_status

def delete_leads_from_instantly_enhanced(leads: List[InstantlyLead]) -> Tuple[int, List[InstantlyLead], dict]:
    """Delete leads with enhanced rate limiting designed for DELETE operations.
    
    Returns:
        Tuple of (successful_count, successfully_deleted_leads, circuit_breaker_info)
    """
    if not leads:
        return 0, [], {"activated": False, "reason": None, "leads_not_processed": 0}
    
    logger.info(f"🗑️ Starting enhanced deletion of {len(leads)} leads...")
    
    # ENHANCED LOGGING: Track deletion reasons
    deletion_reasons = {}
    for lead in leads:
        reason = lead.status if hasattr(lead, 'status') and lead.status else 'unknown'
        deletion_reasons[reason] = deletion_reasons.get(reason, 0) + 1
    
    # Show what we're about to delete
    logger.info("🗑️ DELETION BREAKDOWN:")
    for reason, count in deletion_reasons.items():
        logger.info(f"   • {reason}: {count} leads")
    logger.info("")
    
    DELETE_DELAY = config.rate_limits.delete_delay if config else 1.0  # OPTIMIZED: Use centralized config
    successful_deletions = 0
    failed_deletions = 0
    successfully_deleted_leads = []  # Track successful deletions for BigQuery updates
    
    # Circuit breaker implementation
    recent_results = deque(maxlen=10)  # Store True/False for last 10 attempts
    rate_limited_streak = 0
    circuit_breaker_activated = False
    circuit_breaker_reason = None
    
    for i, lead in enumerate(leads):
        reason = lead.status if hasattr(lead, 'status') and lead.status else 'unknown'
        logger.info(f"🗑️ Deleting {i+1}/{len(leads)}: {lead.email} → {reason}")
        
        # Rate limiting delay (except for first lead)
        if i > 0:
            logger.debug(f"⏸️ Enhanced DELETE rate limiting: waiting {DELETE_DELAY}s...")
            time.sleep(DELETE_DELAY)
        
        # Attempt deletion with retry and circuit breaker monitoring
        success, status_code = delete_single_lead_with_retry(lead)
        recent_results.append(success)
        
        if success:
            successful_deletions += 1
            successfully_deleted_leads.append(lead)  # Track successful deletion
            logger.info(f"   ✅ SUCCESS: {lead.email} deleted (status: {status_code})")
            # Reset rate limit streak on success
            if status_code != '429':
                rate_limited_streak = 0
        else:
            failed_deletions += 1
            logger.warning(f"   ❌ FAILED: {lead.email} deletion failed (status: {status_code})")
        
        # Circuit breaker logic
        if status_code == '429':
            rate_limited_streak += 1
        
        # Check circuit breaker conditions
        if len(recent_results) == 10:
            failure_rate = sum(1 for result in recent_results if not result) / 10.0
            if failure_rate > 0.8:
                circuit_breaker_reason = f">80% recent DELETE failures (failure rate: {failure_rate:.1%})"
                logger.warning(f"🔴 Circuit breaker activated: {circuit_breaker_reason} — pausing drain.")
                circuit_breaker_activated = True
                break
        
        if rate_limited_streak >= 3:
            circuit_breaker_reason = f"{rate_limited_streak} consecutive 429 rate limits"
            logger.warning(f"🔴 Circuit breaker activated: {circuit_breaker_reason} — pausing drain.")
            circuit_breaker_activated = True
            break
    
    # ENHANCED LOGGING: Summary of deletion results
    logger.info("=" * 60)
    logger.info("🗑️ DELETION RESULTS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"📋 Total leads processed: {i+1}/{len(leads)}")
    logger.info(f"✅ Successful deletions: {successful_deletions}")
    logger.info(f"❌ Failed deletions: {failed_deletions}")
    logger.info(f"📈 Success rate: {(successful_deletions/max(i+1,1)*100):.1f}%")
    if circuit_breaker_activated:
        logger.warning("🔴 Circuit breaker activated - drain stopped early to prevent cascade failures")
        remaining_leads = len(leads) - (i + 1)
        if remaining_leads > 0:
            logger.info(f"⏸️ {remaining_leads} leads not processed due to circuit breaker activation")
    logger.info("=" * 60)
    
    # Prepare circuit breaker info
    leads_not_processed = len(leads) - (i + 1) if circuit_breaker_activated else 0
    circuit_breaker_info = {
        "activated": circuit_breaker_activated,
        "reason": circuit_breaker_reason,
        "leads_not_processed": leads_not_processed
    }
    
    return successful_deletions, successfully_deleted_leads, circuit_breaker_info

def drain_finished_leads_enhanced(finished_leads: List[InstantlyLead] = None) -> Tuple[int, dict]:
    """Enhanced drain with aggressive rate limiting for DELETE operations.
    
    Returns:
        Tuple of (total_drained, circuit_breaker_info)
    """
    logger.info("=== ENHANCED DRAIN MODE ===")
    
    # Use provided leads or fetch if not provided (backward compatibility)
    if finished_leads is None:
        logger.debug("No leads provided, fetching...")
        finished_leads = get_finished_leads()
    else:
        logger.debug(f"Using provided leads list with {len(finished_leads)} leads")
    
    if not finished_leads:
        logger.info("✅ No finished leads to drain")
        return 0
    
    logger.info(f"📋 Found {len(finished_leads)} leads to drain")
    
    # Process in smaller batches with longer delays for DELETE operations
    DRAIN_BATCH_SIZE = 5   # Smaller batches for DELETE (vs 50-100 for CREATE)
    DRAIN_BATCH_DELAY = config.rate_limits.delete_batch_delay if config else 5.0  # OPTIMIZED: Use centralized config
    
    total_drained = 0
    batch_count = (len(finished_leads) + DRAIN_BATCH_SIZE - 1) // DRAIN_BATCH_SIZE
    circuit_breaker_info = {"activated": False, "reason": None, "leads_not_processed": 0}
    
    logger.info(f"📦 Processing in {batch_count} batches of {DRAIN_BATCH_SIZE} leads each")
    
    for i in range(0, len(finished_leads), DRAIN_BATCH_SIZE):
        batch = finished_leads[i:i+DRAIN_BATCH_SIZE]
        batch_num = i // DRAIN_BATCH_SIZE + 1
        
        logger.info(f"🔄 Processing drain batch {batch_num}/{batch_count} ({len(batch)} leads)")
        
        # Enhanced rate limiting between batches
        if i > 0:
            logger.info(f"⏸️ Enhanced batch rate limiting: waiting {DRAIN_BATCH_DELAY}s between delete batches...")
            time.sleep(DRAIN_BATCH_DELAY)
        
        # Delete batch with individual delays and retries
        successful_in_batch, successfully_deleted_batch, batch_circuit_breaker_info = delete_leads_from_instantly_enhanced(batch)
        
        # Update BigQuery tracking ONLY for successful deletions (improved accuracy)
        if successfully_deleted_batch:
            try:
                update_bigquery_state(successfully_deleted_batch)
                logger.info(f"📊 Updated BigQuery tracking for {len(successfully_deleted_batch)} successfully deleted leads in batch {batch_num}")
            except Exception as e:
                logger.error(f"❌ Failed to update BigQuery for batch {batch_num}: {e}")
                # Continue processing - don't fail entire batch for BigQuery issues
        else:
            logger.info(f"📊 No successful deletions in batch {batch_num}, skipping BigQuery update")
        
        total_drained += successful_in_batch
        logger.info(f"✅ Batch {batch_num} complete: {successful_in_batch}/{len(batch)} deleted, {total_drained}/{len(finished_leads)} total processed")
        
        # Check if circuit breaker was activated in this batch
        if batch_circuit_breaker_info["activated"]:
            circuit_breaker_info.update(batch_circuit_breaker_info)
            # Calculate remaining leads across all remaining batches
            remaining_leads_total = len(finished_leads) - (i + len(batch))
            circuit_breaker_info["leads_not_processed"] += remaining_leads_total
            logger.warning(f"🔴 Circuit breaker activated - stopping drain process. {remaining_leads_total} leads across remaining batches not processed.")
            break
    
    logger.info(f"🏁 Enhanced drain complete: {total_drained}/{len(finished_leads)} leads successfully deleted")
    return total_drained, circuit_breaker_info

def main():
    """Dedicated drain execution with enhanced rate limiting."""
    drain_start_time = time.time()
    
    logger.info("🧹 STARTING LEAD DRAIN PROCESS (Enhanced Mode)")
    logger.info(f"Config - Dry Run: {DRY_RUN}")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Environment diagnostics
    logger.info("🔍 Environment Diagnostics:")
    logger.info(f"   Python path: {sys.executable}")
    logger.info(f"   Working directory: {os.getcwd()}")
    logger.info(f"   PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")
    logger.info(f"   Shared imports available: {IMPORTS_AVAILABLE}")
    logger.info(f"   Notifications available: {NOTIFICATIONS_AVAILABLE}")
    
    # Initialize notification tracking
    notification_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "duration_seconds": 0,
        "analysis_summary": {
            "total_leads_analyzed": 0,
            "leads_skipped_24hr": 0,  # Placeholder - your logic may track this
            "leads_eligible_for_drain": 0
        },
        "drain_classifications": {
            "completed": 0,
            "replied": 0,
            "bounced_hard": 0,
            "unsubscribed": 0,
            "stale_active": 0,
            "total_identified": 0
        },
        "deletion_results": {
            "attempted_deletions": 0,
            "successful_deletions": 0,
            "failed_deletions": 0,
            "success_rate_percentage": 0.0
        },
        "dnc_updates": {
            "new_unsubscribes": 0,  # Placeholder - enhance if you track this
            "total_dnc_list": 11726  # Your known DNC count
        },
        "inventory_impact": {
            "leads_removed": 0,
            "new_inventory_total": 0  # Will be calculated
        },
        "performance": {
            "classification_accuracy": 99.0,  # Estimated
            "processing_rate_per_minute": 0.0
        },
        "circuit_breaker": {
            "activated": False,
            "reason": None,
            "leads_not_processed": 0
        },
        "errors": [],
        "github_run_url": f"{os.getenv('GITHUB_SERVER_URL', '')}/{os.getenv('GITHUB_REPOSITORY', '')}/actions/runs/{os.getenv('GITHUB_RUN_ID', '')}"
    }
    
    try:
        # Get leads to drain for analysis
        finished_leads = get_finished_leads()
        total_leads_found = len(finished_leads) if finished_leads else 0
        
        # Update notification data
        notification_data["analysis_summary"]["total_leads_analyzed"] = total_leads_found
        notification_data["analysis_summary"]["leads_eligible_for_drain"] = total_leads_found
        notification_data["drain_classifications"]["total_identified"] = total_leads_found
        
        # For now, classify all as "completed" - you can enhance this with actual classification logic
        notification_data["drain_classifications"]["completed"] = total_leads_found
        
        # Enhanced drain with aggressive rate limiting (pass fetched leads to avoid double-fetch)
        drained, circuit_breaker_info = drain_finished_leads_enhanced(finished_leads)
        
        # Update notification data with results
        notification_data["deletion_results"]["attempted_deletions"] = total_leads_found
        notification_data["deletion_results"]["successful_deletions"] = drained
        notification_data["deletion_results"]["failed_deletions"] = total_leads_found - drained
        
        if total_leads_found > 0:
            notification_data["deletion_results"]["success_rate_percentage"] = round((drained / total_leads_found) * 100, 1)
        
        notification_data["inventory_impact"]["leads_removed"] = drained
        
        # Update circuit breaker information
        notification_data["circuit_breaker"] = circuit_breaker_info
        
        # Calculate timing and performance
        end_time = time.time()
        duration = end_time - drain_start_time
        notification_data["duration_seconds"] = duration
        
        if duration > 0:
            notification_data["performance"]["processing_rate_per_minute"] = round((total_leads_found / duration) * 60, 1)
        
        # Send notification
        if NOTIFICATIONS_AVAILABLE:
            try:
                logger.info("📤 Sending drain completion notification...")
                success = notifier.send_drain_notification(notification_data)
                if success:
                    logger.info("✅ Notification sent successfully")
                else:
                    logger.warning("⚠️ Notification failed to send")
            except Exception as e:
                logger.warning(f"⚠️ Notification error (drain continues): {e}")
        
        logger.info("="*60)
        logger.info("✅ DRAIN PROCESS COMPLETE")
        logger.info(f"📊 Results: {drained} leads processed")
        logger.info(f"⏱️ Duration: {duration:.1f} seconds")
        logger.info(f"📈 Rate: {drained/max(duration/60, 0.1):.1f} leads/minute")
        logger.info("="*60)
        
        return drained
        
    except Exception as e:
        # Add error to notification data
        notification_data["errors"].append(str(e))
        
        # Send error notification if possible
        if NOTIFICATIONS_AVAILABLE:
            try:
                end_time = time.time()
                notification_data["duration_seconds"] = end_time - drain_start_time
                notifier.send_drain_notification(notification_data)
            except:
                pass  # Don't let notification errors mask the original error
        logger.error(f"❌ DRAIN PROCESS FAILED: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        
        # Log stack trace for debugging
        import traceback
        logger.error("Stack trace:")
        logger.error(traceback.format_exc())
        
        # Ensure logs are flushed
        for handler in logger.handlers:
            handler.flush()
        
        raise

if __name__ == "__main__":
    main()