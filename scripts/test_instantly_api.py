#!/usr/bin/env python3
"""
Test Instantly API connection and basic operations
"""
import sys
import os
import requests
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config

def test_instantly_connection():
    """Test connection to Instantly API"""
    print("Testing Instantly API Connection")
    print("=" * 50)
    
    if not config.instantly_api_key:
        print("✗ No Instantly API key found!")
        print("Please add your API key to config/secrets/instantly-config.json")
        return 1
    
    # Test API connection with a simple request
    headers = {
        "Authorization": f"Bearer {config.instantly_api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        # Test with campaign list endpoint
        url = f"{config.instantly_base_url}/api/v2/campaigns"
        print(f"\nTesting API endpoint: {url}")
        
        response = requests.get(url, headers=headers)
        
        print(f"\nResponse Status: {response.status_code}")
        
        if response.status_code == 200:
            print("✓ Successfully connected to Instantly API!")
            
            # Try to parse response
            try:
                data = response.json()
                if isinstance(data, list):
                    print(f"\nFound {len(data)} campaigns")
                else:
                    print("\nResponse data:", json.dumps(data, indent=2)[:500])
            except:
                print("\nRaw response:", response.text[:500])
                
        elif response.status_code == 401:
            print("✗ Authentication failed - please check your API key")
            return 1
        else:
            print(f"✗ Unexpected response: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return 1
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Connection error: {e}")
        return 1
    
    print("\n" + "=" * 50)
    print("✓ Instantly API test completed successfully!")
    print("\nNext steps:")
    print("1. Add your BigQuery credentials to config/secrets/")
    print("2. Run 'python scripts/test_config.py' to verify all configuration")
    print("3. Run 'python scripts/create_bigquery_tables.py' to set up BigQuery")
    
    return 0

if __name__ == "__main__":
    sys.exit(test_instantly_connection())