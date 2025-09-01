#!/usr/bin/env python3
"""
Test script to verify GitHub Actions secrets configuration.
This simulates how the GitHub Actions workflow will access secrets.
"""

import os
import json
import tempfile
from pathlib import Path

def test_github_secrets_simulation():
    """Simulate GitHub Actions environment with secrets."""
    print("🧪 TESTING GITHUB SECRETS SIMULATION")
    print("=" * 50)
    
    # Get local credentials to simulate secrets
    from config.config import Config
    config = Config()
    
    # Read BigQuery credentials JSON
    with open(config.google_credentials_path, 'r') as f:
        bigquery_json = f.read()
    
    # Simulate GitHub Actions environment
    simulated_secrets = {
        'INSTANTLY_API_KEY': config.instantly_api_key,
        'BIGQUERY_CREDENTIALS_JSON': bigquery_json
    }
    
    print("1. Simulating GitHub Actions environment...")
    
    # Test 1: Create credentials file like GitHub Actions does
    print("2. Creating temporary credentials file...")
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(simulated_secrets['BIGQUERY_CREDENTIALS_JSON'])
        temp_creds_path = f.name
    
    # Test 2: Set environment variables like GitHub Actions
    print("3. Setting environment variables...")
    test_env = {
        'INSTANTLY_API_KEY': simulated_secrets['INSTANTLY_API_KEY'],
        'GOOGLE_APPLICATION_CREDENTIALS': temp_creds_path,
        'TARGET_NEW_LEADS_PER_RUN': '5',  # Small test batch
        'DRY_RUN': 'true'  # Safe testing
    }
    
    # Update environment
    for key, value in test_env.items():
        os.environ[key] = value
    
    print("4. Testing BigQuery connection with simulated secrets...")
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project='instant-ground-394115')
        
        # Test query
        query = "SELECT COUNT(*) as count FROM `instant-ground-394115.email_analytics.v_ready_for_instantly` LIMIT 1"
        result = client.query(query).result()
        count = next(result).count
        
        print(f"   ✅ BigQuery connection successful!")
        print(f"   ✅ Found {count:,} leads ready for Instantly")
        
    except Exception as e:
        print(f"   ❌ BigQuery test failed: {e}")
        return False
    
    print("5. Testing Instantly API with simulated secrets...")
    try:
        import requests
        
        headers = {
            'Authorization': f'Bearer {os.environ["INSTANTLY_API_KEY"]}',
            'Content-Type': 'application/json'
        }
        
        # Test API call
        response = requests.get('https://api.instantly.ai/api/v2/campaigns', headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            campaigns = data.get('items', [])
            print(f"   ✅ Instantly API connection successful!")
            print(f"   ✅ Found {len(campaigns)} campaigns")
            
            # Check our specific campaigns
            campaign_ids = [c.get('id') for c in campaigns]
            smb_found = '8c46e0c9-c1f9-4201-a8d6-6221bafeada6' in campaign_ids
            midsize_found = '5ffbe8c3-dc0e-41e4-9999-48f00d2015df' in campaign_ids
            
            print(f"   {'✅' if smb_found else '❌'} SMB campaign found")
            print(f"   {'✅' if midsize_found else '❌'} Midsize campaign found")
            
        else:
            print(f"   ❌ Instantly API test failed: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"   ❌ Instantly API test failed: {e}")
        return False
    
    print("6. Testing sync_once.py with simulated environment...")
    try:
        # Import main sync function
        import sys
        sys.path.append('.')
        
        # This should work without errors
        from sync_once import get_eligible_leads, get_current_instantly_inventory
        
        print("   ✅ sync_once.py imports successfully")
        
        # Test key functions
        leads = get_eligible_leads(3)  # Very small test
        print(f"   ✅ get_eligible_leads() returned {len(leads)} leads")
        
        inventory = get_current_instantly_inventory()
        print(f"   ✅ get_current_instantly_inventory() returned {inventory}")
        
    except Exception as e:
        print(f"   ❌ sync_once.py test failed: {e}")
        return False
    
    # Cleanup
    try:
        os.unlink(temp_creds_path)
    except:
        pass
    
    print("\n" + "=" * 50)
    print("✅ ALL TESTS PASSED!")
    print("\n🎯 GITHUB SECRETS SETUP INSTRUCTIONS:")
    print("\n1. Go to: https://github.com/peterlopez/coldemailsystem/settings/secrets/actions")
    print("\n2. Add these secrets:")
    print(f"   Name: INSTANTLY_API_KEY")
    print(f"   Value: {config.instantly_api_key}")
    print(f"\n   Name: BIGQUERY_CREDENTIALS_JSON")
    print(f"   Value: [Copy entire contents of {config.google_credentials_path}]")
    print("\n3. Enable GitHub Actions if not already enabled")
    print("\n4. Workflow will automatically run on schedule or manual trigger")
    print("\n✅ System is ready for production deployment!")
    
    return True

if __name__ == "__main__":
    test_github_secrets_simulation()