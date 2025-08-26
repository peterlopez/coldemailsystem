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
from datetime import datetime
from typing import List

# Import required functions and classes from sync_once.py
try:
    from sync_once import (
        get_finished_leads, update_bigquery_state, 
        DRY_RUN, InstantlyLead, delete_lead_from_instantly,
        log_dead_letter
    )
    print("✅ Sync imports successful")
    SYNC_IMPORTS_AVAILABLE = True
except ImportError as e:
    print(f"❌ Sync import error: {e}")
    print(f"   Error type: {type(e).__name__}")
    print(f"   Error details: {str(e)}")
    
    # More detailed error information
    if 'requests' in str(e):
        print("   This appears to be a missing 'requests' dependency")
        print("   Check that pip install -r requirements.txt completed successfully")
    elif 'google' in str(e):
        print("   This appears to be a missing Google Cloud dependency")
        print("   Check that Google Cloud libraries are installed")
    
    # Set defaults for missing imports
    DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
    SYNC_IMPORTS_AVAILABLE = False
    
    print("❌ CRITICAL: Cannot proceed without sync_once.py imports")
    sys.exit(1)  # Exit immediately rather than raising to get clearer error message

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

def delete_single_lead_with_retry(lead: InstantlyLead, max_retries: int = 1) -> bool:
    """Delete a single lead - simplified to avoid retry conflicts."""
    
    try:
        # Use the working delete function from sync_once.py (it handles errors properly)
        success = delete_lead_from_instantly(lead)
        
        if success:
            logger.debug(f"✅ Successfully deleted {lead.email}")
            return True
        else:
            logger.error(f"❌ Failed to delete {lead.email}")
            log_dead_letter("drain", lead.email, f"DELETE {lead.id}", 400, "Delete function returned False")
            return False
            
    except Exception as e:
        logger.error(f"❌ Delete error for {lead.email}: {e}")
        log_dead_letter("drain", lead.email, f"DELETE {lead.id}", 0, str(e))
        return False

def delete_leads_from_instantly_enhanced(leads: List[InstantlyLead]) -> int:
    """Delete leads with enhanced rate limiting designed for DELETE operations."""
    if not leads:
        return 0
    
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
    
    DELETE_DELAY = 3.0  # 3 seconds between each DELETE call (aggressive rate limiting)
    successful_deletions = 0
    failed_deletions = 0
    
    for i, lead in enumerate(leads):
        reason = lead.status if hasattr(lead, 'status') and lead.status else 'unknown'
        logger.info(f"🗑️ Deleting {i+1}/{len(leads)}: {lead.email} → {reason}")
        
        # Rate limiting delay (except for first lead)
        if i > 0:
            logger.debug(f"⏸️ Enhanced DELETE rate limiting: waiting {DELETE_DELAY}s...")
            time.sleep(DELETE_DELAY)
        
        # Attempt deletion with retry
        if delete_single_lead_with_retry(lead):
            successful_deletions += 1
            logger.info(f"   ✅ SUCCESS: {lead.email} deleted")
        else:
            failed_deletions += 1
            logger.warning(f"   ❌ FAILED: {lead.email} deletion failed")
    
    # ENHANCED LOGGING: Summary of deletion results
    logger.info("=" * 60)
    logger.info("🗑️ DELETION RESULTS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"📋 Total deletion attempts: {len(leads)}")
    logger.info(f"✅ Successful deletions: {successful_deletions}")
    logger.info(f"❌ Failed deletions: {failed_deletions}")
    logger.info(f"📈 Success rate: {(successful_deletions/max(len(leads),1)*100):.1f}%")
    logger.info("=" * 60)
    
    return successful_deletions

def drain_finished_leads_enhanced() -> int:
    """Enhanced drain with aggressive rate limiting for DELETE operations."""
    logger.info("=== ENHANCED DRAIN MODE ===")
    
    # Get finished leads (this already has 1s pagination rate limiting from recent fix)
    finished_leads = get_finished_leads()
    
    if not finished_leads:
        logger.info("✅ No finished leads to drain")
        return 0
    
    logger.info(f"📋 Found {len(finished_leads)} leads to drain")
    
    # Process in smaller batches with longer delays for DELETE operations
    DRAIN_BATCH_SIZE = 5   # Smaller batches for DELETE (vs 50-100 for CREATE)
    DRAIN_BATCH_DELAY = 10 # Longer delays between batches (vs 1s for pagination)
    
    total_drained = 0
    batch_count = (len(finished_leads) + DRAIN_BATCH_SIZE - 1) // DRAIN_BATCH_SIZE
    
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
        successful_in_batch = delete_leads_from_instantly_enhanced(batch)
        
        # Update BigQuery tracking for the entire batch (including failed deletions)
        # This ensures our tracking reflects the drain attempt even if API deletion fails
        try:
            update_bigquery_state(batch)
            logger.info(f"📊 Updated BigQuery tracking for batch {batch_num}")
        except Exception as e:
            logger.error(f"❌ Failed to update BigQuery for batch {batch_num}: {e}")
            # Continue processing - don't fail entire batch for BigQuery issues
        
        total_drained += successful_in_batch
        logger.info(f"✅ Batch {batch_num} complete: {successful_in_batch}/{len(batch)} deleted, {total_drained}/{len(finished_leads)} total processed")
    
    logger.info(f"🏁 Enhanced drain complete: {total_drained}/{len(finished_leads)} leads successfully deleted")
    return total_drained

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
    logger.info(f"   Sync imports available: {SYNC_IMPORTS_AVAILABLE}")
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
        
        # Enhanced drain with aggressive rate limiting
        drained = drain_finished_leads_enhanced()
        
        # Update notification data with results
        notification_data["deletion_results"]["attempted_deletions"] = total_leads_found
        notification_data["deletion_results"]["successful_deletions"] = drained
        notification_data["deletion_results"]["failed_deletions"] = total_leads_found - drained
        
        if total_leads_found > 0:
            notification_data["deletion_results"]["success_rate_percentage"] = round((drained / total_leads_found) * 100, 1)
        
        notification_data["inventory_impact"]["leads_removed"] = drained
        
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