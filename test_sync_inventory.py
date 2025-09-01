#!/usr/bin/env python3
"""
Test the updated inventory counting in sync_once.py
"""

import os
import sys
from shared_config import config

# Import the function from sync_once
sys.path.append('.')
from sync_once import get_current_instantly_inventory

def test_sync_inventory():
    """Test the corrected inventory counting function from sync_once.py."""
    print("🔍 Testing Updated sync_once.py Inventory Function")
    print("=" * 60)
    
    try:
        # This should now use the corrected logic
        inventory_count = get_current_instantly_inventory()
        
        print(f"✅ Function completed successfully")
        print(f"🎯 Total inventory returned: {inventory_count} leads")
        
        # Validate that the count makes sense
        if 1700 <= inventory_count <= 2000:
            print(f"✅ Inventory count is in expected range (1700-2000)")
        else:
            print(f"⚠️ Inventory count outside expected range")
            
    except Exception as e:
        print(f"❌ Function failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_sync_inventory()