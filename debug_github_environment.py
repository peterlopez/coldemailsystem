#!/usr/bin/env python3
"""
Debug script specifically for GitHub Actions environment
"""

import os
import sys
import subprocess

print("🔍 GITHUB ACTIONS ENVIRONMENT DEBUG")
print("=" * 80)

# Basic environment
print("\n📍 BASIC ENVIRONMENT:")
print(f"   Python executable: {sys.executable}")
print(f"   Python version: {sys.version}")
print(f"   Working directory: {os.getcwd()}")
print(f"   HOME: {os.environ.get('HOME', 'Not set')}")
print(f"   USER: {os.environ.get('USER', 'Not set')}")

# Check if we're in GitHub Actions
print(f"   GITHUB_ACTIONS: {os.environ.get('GITHUB_ACTIONS', 'Not set')}")
print(f"   GITHUB_WORKSPACE: {os.environ.get('GITHUB_WORKSPACE', 'Not set')}")

# Python path
print(f"\n🐍 PYTHON PATH:")
print(f"   PYTHONPATH env: {os.environ.get('PYTHONPATH', 'Not set')}")
print(f"   sys.path entries:")
for i, path in enumerate(sys.path[:10]):  # Show first 10 entries
    print(f"      [{i}] {path}")

# Check file structure
print(f"\n📁 FILE STRUCTURE:")
files_to_check = [
    'sync_once.py',
    'drain_once.py', 
    'shared/',
    'shared/__init__.py',
    'shared/models.py',
    'shared/api_client.py',
    'shared/bigquery_utils.py',
    'requirements.txt',
    'config/secrets/bigquery-credentials.json'
]

for file_path in files_to_check:
    exists = os.path.exists(file_path)
    if exists:
        if os.path.isdir(file_path):
            try:
                contents = os.listdir(file_path)
                print(f"   ✅ {file_path}/ (contains: {', '.join(contents[:5])})")
            except:
                print(f"   ✅ {file_path}/ (directory)")
        else:
            size = os.path.getsize(file_path)
            print(f"   ✅ {file_path} ({size} bytes)")
    else:
        print(f"   ❌ {file_path} (not found)")

# Check installed packages
print(f"\n📦 PACKAGE INSTALLATION:")
packages_to_check = ['requests', 'google-cloud-bigquery', 'tenacity']

for package in packages_to_check:
    try:
        result = subprocess.run([sys.executable, '-c', f'import {package}; print({package}.__version__)'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print(f"   ✅ {package}: version {result.stdout.strip()}")
        else:
            print(f"   ❌ {package}: import failed")
            print(f"      Error: {result.stderr.strip()}")
    except Exception as e:
        print(f"   ❌ {package}: check failed ({e})")

# Test imports step by step
print(f"\n🧪 STEP-BY-STEP IMPORT TESTING:")

# Step 1: Basic modules
basic_modules = ['os', 'sys', 'logging', 'datetime', 'typing']
for module in basic_modules:
    try:
        __import__(module)
        print(f"   ✅ {module}")
    except ImportError as e:
        print(f"   ❌ {module}: {e}")

# Step 2: Third party modules  
third_party = ['requests', 'google.cloud.bigquery', 'tenacity']
for module in third_party:
    try:
        __import__(module)
        print(f"   ✅ {module}")
    except ImportError as e:
        print(f"   ❌ {module}: {e}")

# Step 3: Our modules
print(f"\n🏠 OUR MODULE IMPORTS:")

# Test shared package
try:
    import shared
    print(f"   ✅ shared package")
    
    from shared.models import InstantlyLead
    print(f"   ✅ shared.models.InstantlyLead")
    
    from shared.api_client import DRY_RUN
    print(f"   ✅ shared.api_client.DRY_RUN = {DRY_RUN}")
    
except Exception as e:
    print(f"   ❌ shared package failed: {e}")
    import traceback
    print("   Traceback:")
    for line in traceback.format_exc().split('\n'):
        print(f"      {line}")

# Test sync_once
try:
    import sync_once
    print(f"   ✅ sync_once module")
    
    from sync_once import DRY_RUN, InstantlyLead
    print(f"   ✅ sync_once imports: DRY_RUN = {DRY_RUN}")
    
except Exception as e:
    print(f"   ❌ sync_once failed: {e}")
    import traceback
    print("   Traceback:")
    for line in traceback.format_exc().split('\n'):
        print(f"      {line}")

print(f"\n" + "=" * 80)
print("🎯 CONCLUSION: Check the specific error above to understand the issue")
print("=" * 80)