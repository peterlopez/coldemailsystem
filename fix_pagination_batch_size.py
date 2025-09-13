#!/usr/bin/env python3
"""
Fix the batch_size issue in sync_once.py
"""

def fix_batch_size():
    print("🔧 FIXING BATCH SIZE IN SYNC_ONCE.PY")
    print("=" * 50)
    
    file_path = "/Users/peterlopez/Documents/Cold Email System/sync_once.py"
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find the problematic line
    old_line = "batch_size=200,  # Increased batch size for better performance"
    new_line = "batch_size=100,  # API maximum limit (fixed from 200)"
    
    if old_line in content:
        print(f"✅ Found problematic line: {old_line}")
        content = content.replace(old_line, new_line)
        
        with open(file_path, 'w') as f:
            f.write(content)
        
        print(f"✅ Fixed: Changed batch_size from 200 to 100")
        print("📝 This should resolve the 400 Bad Request errors")
        return True
    else:
        print(f"❌ Could not find the problematic line in {file_path}")
        print("   The line might have already been changed or the format is different")
        return False

if __name__ == "__main__":
    success = fix_batch_size()
    if success:
        print("\n🎉 FIX APPLIED SUCCESSFULLY")
        print("The pagination should now work correctly!")
    else:
        print("\n⚠️ MANUAL FIX REQUIRED")
        print("Please manually change batch_size=200 to batch_size=100 in sync_once.py line ~1750")