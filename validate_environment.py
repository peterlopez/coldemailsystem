#!/usr/bin/env python3
"""
Validate environment for GitHub Actions
"""

import os
import sys
from pathlib import Path

def validate_environment():
    """Validate all requirements are met before running sync."""
    
    print("🔍 VALIDATING GITHUB ACTIONS ENVIRONMENT")
    print("=" * 50)
    
    errors = []
    
    # Check environment variables
    print("1. Environment Variables:")
    required_env_vars = ['INSTANTLY_API_KEY']
    
    for var in required_env_vars:
        value = os.getenv(var)
        if value:
            print(f"   ✅ {var}: Present ({len(value)} chars)")
        else:
            print(f"   ❌ {var}: MISSING")
            errors.append(f"Missing environment variable: {var}")
    
    # Check files
    print("\n2. Required Files:")
    required_files = [
        'sync_once.py',
        'config/secrets/bigquery-credentials.json'
    ]
    
    for file_path in required_files:
        if Path(file_path).exists():
            size = Path(file_path).stat().st_size
            print(f"   ✅ {file_path}: Present ({size} bytes)")
        else:
            print(f"   ❌ {file_path}: MISSING")
            errors.append(f"Missing required file: {file_path}")
    
    # Check Python imports
    print("\n3. Python Dependencies:")
    required_modules = [
        'google.cloud.bigquery',
        'requests', 
        'tenacity'
    ]
    
    for module in required_modules:
        try:
            __import__(module)
            print(f"   ✅ {module}: Available")
        except ImportError:
            print(f"   ❌ {module}: MISSING")
            errors.append(f"Missing Python module: {module}")
    
    # Test BigQuery credentials
    print("\n4. BigQuery Credentials:")
    creds_path = 'config/secrets/bigquery-credentials.json'
    if Path(creds_path).exists():
        try:
            import json
            with open(creds_path, 'r') as f:
                creds = json.load(f)
            
            required_keys = ['type', 'project_id', 'private_key', 'client_email']
            missing = [k for k in required_keys if k not in creds]
            
            if missing:
                print(f"   ⚠️  Missing keys in credentials: {missing}")
                errors.append(f"Invalid credentials file: missing {missing}")
            else:
                print(f"   ✅ Credentials file valid")
                print(f"   📋 Project: {creds.get('project_id')}")
                
        except Exception as e:
            print(f"   ❌ Credentials file error: {e}")
            errors.append(f"Credentials file error: {e}")
    
    # Summary
    print("\n" + "=" * 50)
    if errors:
        print("❌ VALIDATION FAILED:")
        for error in errors:
            print(f"   • {error}")
        print("\n💡 Fix these issues before running sync_once.py")
        return False
    else:
        print("✅ ALL VALIDATIONS PASSED!")
        print("🚀 Environment is ready for sync_once.py")
        return True

if __name__ == "__main__":
    success = validate_environment()
    sys.exit(0 if success else 1)