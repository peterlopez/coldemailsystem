#!/usr/bin/env python3
"""
Reset BigQuery lead tracking tables to start fresh
This will clear our internal tracking so we can start with a clean slate
"""

import os
from google.cloud import bigquery

# Set up BigQuery client
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/secrets/bigquery-credentials.json'
client = bigquery.Client(project='instant-ground-394115')

PROJECT_ID = "instant-ground-394115"
DATASET_ID = "email_analytics"

def reset_tracking_tables():
    print("🔄 RESETTING BIGQUERY LEAD TRACKING")
    print("=" * 50)
    
    # 1. Clear ops_inst_state (current active leads)
    print("\n1. Clearing ops_inst_state table...")
    query1 = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` WHERE TRUE"
    
    try:
        result = client.query(query1).result()
        print("✅ ops_inst_state table cleared")
    except Exception as e:
        print(f"❌ Error clearing ops_inst_state: {e}")
    
    # 2. Clear ops_lead_history (lead history)
    print("\n2. Clearing ops_lead_history table...")
    query2 = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ops_lead_history` WHERE TRUE"
    
    try:
        result = client.query(query2).result()
        print("✅ ops_lead_history table cleared")
    except Exception as e:
        print(f"❌ Error clearing ops_lead_history: {e}")
    
    # 3. Clear ops_dead_letters (error logs) - optional
    print("\n3. Clearing ops_dead_letters table...")
    query3 = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ops_dead_letters` WHERE TRUE"
    
    try:
        result = client.query(query3).result()
        print("✅ ops_dead_letters table cleared")
    except Exception as e:
        print(f"❌ Error clearing ops_dead_letters: {e}")
    
    # 4. Check current eligible leads count
    print("\n4. Checking eligible leads count...")
    query4 = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.v_ready_for_instantly`"
    
    try:
        result = client.query(query4).result()
        count = next(result).count
        print(f"✅ Eligible leads available: {count:,}")
    except Exception as e:
        print(f"❌ Error checking eligible leads: {e}")
    
    print("\n" + "=" * 50)
    print("🎯 RESET COMPLETE!")
    print("")
    print("📊 What this means:")
    print("  • All previous lead tracking cleared")
    print("  • No leads marked as 'active' in system")
    print("  • Fresh start for new lead processing")
    print("  • All eligible leads available for selection")
    print("")
    print("🚀 The system will now treat Instantly as empty")
    print("   and can start fresh lead processing!")

def main():
    print("⚠️  This will reset all BigQuery lead tracking tables.")
    print("   This gives us a clean slate for testing but doesn't")
    print("   delete leads from Instantly dashboard directly.")
    print("")
    
    confirm = input("Type 'RESET' to confirm: ")
    
    if confirm != 'RESET':
        print("❌ Reset cancelled")
        return
    
    reset_tracking_tables()

if __name__ == "__main__":
    main()