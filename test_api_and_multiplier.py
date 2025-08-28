#!/usr/bin/env python3
"""
Quick test script to:
1. Test Instantly API authentication
2. Show how to increase the inventory multiplier
"""

import os
import requests
from shared_config import config

print("🔍 Testing Instantly API Authentication and Configuration")
print("=" * 60)

# Show current configuration
print("\n📋 Current Configuration:")
print(f"API Base URL: {config.api.instantly_base_url}")
print(f"API Key (first 10 chars): {config.api.instantly_api_key[:10]}...")
print(f"Current Inventory Multiplier: {os.getenv('LEAD_INVENTORY_MULTIPLIER', '3.5')}")

# Test API authentication
print("\n🔐 Testing API Authentication:")
url = f"{config.api.instantly_base_url}/api/v1/account/info"
headers = {
    'Authorization': f'Bearer {config.api.instantly_api_key}',
    'Content-Type': 'application/json'
}

try:
    response = requests.get(url, headers=headers, timeout=10)
    print(f"Response Status: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ Authentication SUCCESSFUL!")
        data = response.json()
        print(f"Account Email: {data.get('email', 'N/A')}")
        print(f"Credits Available: {data.get('credits', 'N/A')}")
    elif response.status_code == 401:
        print("❌ Authentication FAILED - Invalid API Key")
        print(f"Response: {response.text}")
        print("\n💡 To fix authentication:")
        print("1. Get your API key from Instantly.ai dashboard")
        print("2. Set it in GitHub Secrets as INSTANTLY_API_KEY")
        print("3. Or create config/secrets/instantly-config.json with:")
        print('   {"api_key": "your-api-key-here"}')
    else:
        print(f"❌ Unexpected response: {response.status_code}")
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"❌ API call failed: {e}")

print("\n📈 To Increase Inventory Multiplier to 10:")
print("=" * 40)
print("Option 1 - Set environment variable before running:")
print("  export LEAD_INVENTORY_MULTIPLIER=10")
print("  python sync_once.py")
print("")
print("Option 2 - Run with environment variable inline:")
print("  LEAD_INVENTORY_MULTIPLIER=10 python sync_once.py")
print("")
print("Option 3 - For GitHub Actions, add to workflow:")
print("  env:")
print("    LEAD_INVENTORY_MULTIPLIER: 10")
print("")
print("With multiplier=10 and 680 daily capacity:")
print(f"  New safe limit would be: 680 × 10 = 6,800 leads")
print(f"  Current inventory (from BigQuery): 4,784 leads")
print(f"  This would change utilization from 201% to 70%")