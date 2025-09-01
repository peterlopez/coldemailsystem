#!/usr/bin/env python3
"""
Simple test to check if basic drain imports work
"""

import os
import sys

print("SIMPLE DRAIN IMPORT TEST")
print("=" * 50)

# Test the original approach first
print("\n1. Testing original sync_once import:")
try:
    # Test just the basic import without calling anything
    import sync_once
    print("   ✅ sync_once module imports")
    
    # Test specific imports
    from sync_once import DRY_RUN, InstantlyLead
    print(f"   ✅ Basic imports work, DRY_RUN = {DRY_RUN}")
    
    # Test if we can access the classes/functions
    print(f"   ✅ InstantlyLead class available: {InstantlyLead}")
    
except Exception as e:
    print(f"   ❌ sync_once import failed: {e}")
    import traceback
    traceback.print_exc()

print("\n2. Testing shared module import:")
try:
    from shared.models import InstantlyLead
    from shared.api_client import DRY_RUN
    print("   ✅ Shared modules import successfully")
    
except Exception as e:
    print(f"   ❌ Shared modules failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 50)
print("If sync_once works but shared fails, we'll use fallback approach")