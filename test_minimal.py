#!/usr/bin/env python3
"""Minimal test to identify GitHub Actions failure"""

print("TEST: Script started")

# Test 1: Basic Python
try:
    import sys
    print(f"TEST: Python {sys.version}")
except Exception as e:
    print(f"FAIL: Basic Python - {e}")
    sys.exit(1)

# Test 2: Create log file
try:
    with open('test.log', 'w') as f:
        f.write("Test log file created\n")
    print("TEST: Log file created")
except Exception as e:
    print(f"FAIL: Create log file - {e}")
    sys.exit(1)

# Test 3: Import sync_once
try:
    import sync_once
    print("TEST: sync_once imported")
except Exception as e:
    print(f"FAIL: Import sync_once - {e}")
    with open('test.log', 'a') as f:
        f.write(f"Import error: {e}\n")
    sys.exit(1)

print("TEST: All tests passed")