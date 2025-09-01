#!/usr/bin/env python3
"""
Test script to diagnose drain import issues in GitHub Actions
"""

import os
import sys

print("=" * 80)
print("DRAIN IMPORT DIAGNOSTICS")
print("=" * 80)

# Environment info
print("\n1. ENVIRONMENT:")
print(f"   Python: {sys.executable}")
print(f"   Version: {sys.version}")
print(f"   Platform: {sys.platform}")
print(f"   Working Dir: {os.getcwd()}")
print(f"   PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")

# Check sys.path
print("\n2. PYTHON PATH:")
for i, path in enumerate(sys.path):
    print(f"   [{i}] {path}")

# Check if files exist
print("\n3. FILE CHECKS:")
files_to_check = ['sync_once.py', 'drain_once.py', 'requirements.txt', 
                  'config/secrets/bigquery-credentials.json']
for file in files_to_check:
    exists = os.path.exists(file)
    print(f"   {file}: {'✅ EXISTS' if exists else '❌ NOT FOUND'}")

# Test imports one by one
print("\n4. IMPORT TESTS:")

# Standard library
try:
    import os
    print("   ✅ os")
except Exception as e:
    print(f"   ❌ os: {e}")

try:
    import sys
    print("   ✅ sys")
except Exception as e:
    print(f"   ❌ sys: {e}")

try:
    import logging
    print("   ✅ logging")
except Exception as e:
    print(f"   ❌ logging: {e}")

# Third party
try:
    import requests
    print(f"   ✅ requests (version {requests.__version__})")
except Exception as e:
    print(f"   ❌ requests: {e}")

try:
    import google
    print(f"   ✅ google")
except Exception as e:
    print(f"   ❌ google: {e}")

try:
    from google.cloud import bigquery
    print(f"   ✅ google.cloud.bigquery")
except Exception as e:
    print(f"   ❌ google.cloud.bigquery: {e}")

try:
    import tenacity
    print(f"   ✅ tenacity")
except Exception as e:
    print(f"   ❌ tenacity: {e}")

# Test sync_once import with detailed error
print("\n5. SYNC_ONCE IMPORT TEST:")
try:
    # Set environment variable if not set
    if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
        print("   Set GOOGLE_APPLICATION_CREDENTIALS")
    
    import sync_once
    print("   ✅ sync_once module imported")
    
    # Test specific imports
    from sync_once import get_finished_leads, DRY_RUN
    print("   ✅ get_finished_leads imported")
    print(f"   ✅ DRY_RUN = {DRY_RUN}")
    
except Exception as e:
    print(f"   ❌ sync_once import failed: {type(e).__name__}: {e}")
    
    # Try to find the specific line causing the issue
    import traceback
    print("\n   TRACEBACK:")
    traceback.print_exc()

print("\n" + "=" * 80)