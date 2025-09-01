#!/usr/bin/env python3
"""
Test script to verify the refactor works correctly.
This tests the new shared module structure.
"""

import os
import sys

print("=" * 80)
print("TESTING REFACTORED SHARED MODULES")
print("=" * 80)

# Test 1: Import shared modules individually
print("\n1. TESTING INDIVIDUAL SHARED MODULE IMPORTS:")

try:
    from shared.models import InstantlyLead, Lead
    print("   ✅ shared.models imported successfully")
    
    # Test data class creation
    test_lead = InstantlyLead(
        id="test-123",
        email="test@example.com", 
        status=1,
        campaign_id="test-campaign"
    )
    print(f"   ✅ InstantlyLead created: {test_lead.email}")
except Exception as e:
    print(f"   ❌ shared.models failed: {e}")

try:
    from shared.api_client import call_instantly_api, delete_lead_from_instantly, DRY_RUN
    print("   ✅ shared.api_client imported successfully")
    print(f"   ✅ DRY_RUN = {DRY_RUN}")
except Exception as e:
    print(f"   ❌ shared.api_client failed: {e}")

# Test 2: Test drain_once imports (the critical one)
print("\n2. TESTING DRAIN_ONCE IMPORT PATTERN:")

try:
    # This is exactly what drain_once.py does
    from shared.api_client import get_finished_leads, delete_lead_from_instantly, DRY_RUN
    from shared.models import InstantlyLead
    # Note: We intentionally don't import bigquery_utils to test API-only functionality
    
    print("   ✅ Drain-critical imports successful (API client only)")
    print("   ✅ No BigQuery dependency required for basic drain functions")
    
except Exception as e:
    print(f"   ❌ Drain imports failed: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Test BigQuery imports separately
print("\n3. TESTING BIGQUERY MODULE (OPTIONAL FOR DRAIN):")

try:
    from shared.bigquery_utils import get_bigquery_client, update_bigquery_state, log_dead_letter
    print("   ✅ shared.bigquery_utils imported successfully")
    print("   ✅ BigQuery functions available when needed")
except Exception as e:
    print(f"   ⚠️ BigQuery utils not available: {e}")
    print("   ✅ This is OK - drain can work without BigQuery for API operations")

# Test 4: Test the __init__.py imports
print("\n4. TESTING SHARED PACKAGE __init__.py:")

try:
    from shared import InstantlyLead, call_instantly_api, delete_lead_from_instantly
    print("   ✅ Shared package __init__.py imports work")
except Exception as e:
    print(f"   ❌ Shared package imports failed: {e}")

print("\n" + "=" * 80)
print("REFACTOR TEST COMPLETE")

# Summary
print("\n📊 SUMMARY:")
print("   If all ✅ above, the refactor should fix GitHub Actions import issues")
print("   Drain workflow will only import what it needs (API client)")
print("   BigQuery imports happen separately and won't block the API client")
print("=" * 80)