#!/usr/bin/env python3
"""
Test drain notification independently
"""
import os
from datetime import datetime

# Test notification import
try:
    from cold_email_notifier import notifier
    print("✅ Notification system imported successfully")
    
    # Test drain notification
    test_drain_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "duration_seconds": 45.2,
        "analysis_summary": {
            "total_leads_analyzed": 150,
            "leads_skipped_24hr": 120,
            "leads_eligible_for_drain": 30
        },
        "drain_classifications": {
            "completed": 18,
            "replied": 7,
            "bounced_hard": 3,
            "unsubscribed": 2,
            "stale_active": 0,
            "total_identified": 30
        },
        "deletion_results": {
            "attempted_deletions": 30,
            "successful_deletions": 28,
            "failed_deletions": 2,
            "success_rate_percentage": 93.3
        },
        "dnc_updates": {
            "new_unsubscribes": 2,
            "total_dnc_list": 11728
        },
        "inventory_impact": {
            "leads_removed": 28,
            "new_inventory_total": 1200
        },
        "performance": {
            "classification_accuracy": 98.5,
            "processing_rate_per_minute": 20.0
        },
        "errors": [],
        "github_run_url": "https://github.com/peterlopez/coldemailsystem/actions/runs/test"
    }
    
    print("📤 Testing drain notification...")
    success = notifier.send_drain_notification(test_drain_data)
    print(f"{'✅ Success' if success else '❌ Failed'}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()