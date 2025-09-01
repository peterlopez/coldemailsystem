#!/usr/bin/env python3
"""
Debug script to identify sync_once.py issues in GitHub Actions
"""

import sys
import os
import traceback

# Create log file immediately
log_file = open('cold-email-sync.log', 'w')

def log(message):
    """Log to both stdout and file"""
    print(message)
    log_file.write(message + '\n')
    log_file.flush()

log("=== DEBUG SYNC SCRIPT STARTED ===")
log(f"Python version: {sys.version}")
log(f"Working directory: {os.getcwd()}")
log(f"Environment INSTANTLY_API_KEY present: {bool(os.getenv('INSTANTLY_API_KEY'))}")

try:
    log("\n1. Testing imports...")
    
    log("   Importing standard libraries...")
    import json
    import time
    import uuid
    import logging
    from datetime import datetime
    from typing import List, Dict, Optional, Tuple
    from dataclasses import dataclass
    log("   ✅ Standard libraries OK")
    
    log("   Importing requests...")
    import requests
    log("   ✅ requests OK")
    
    log("   Importing google.cloud.bigquery...")
    from google.cloud import bigquery
    log("   ✅ google.cloud.bigquery OK")
    
    log("   Importing tenacity...")
    from tenacity import retry, stop_after_attempt, wait_exponential
    log("   ✅ tenacity OK")
    
    log("\n2. Checking credentials file...")
    creds_path = './config/secrets/bigquery-credentials.json'
    if os.path.exists(creds_path):
        log(f"   ✅ Credentials file exists: {creds_path}")
        log(f"   File size: {os.path.getsize(creds_path)} bytes")
    else:
        log(f"   ❌ Credentials file NOT FOUND: {creds_path}")
    
    log("\n3. Attempting to import sync_once...")
    import sync_once
    log("   ✅ sync_once imported successfully")
    
    log("\n4. Running sync_once.main()...")
    sync_once.main()
    log("   ✅ sync_once.main() completed")
    
except Exception as e:
    log(f"\n❌ ERROR: {type(e).__name__}: {str(e)}")
    log("\nStack trace:")
    log(traceback.format_exc())
    sys.exit(1)
finally:
    log("\n=== DEBUG SYNC SCRIPT ENDED ===")
    log_file.close()