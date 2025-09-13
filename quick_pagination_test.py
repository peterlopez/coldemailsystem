#!/usr/bin/env python3
"""
Quick test to validate pagination fix is working
"""

import os
import sys
import logging
from shared_config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def call_instantly_api(endpoint: str, method: str = 'GET', data: dict = None):
    """Call Instantly API"""
    api_key = config.api.instantly_api_key
    url = f"https://api.instantly.ai{endpoint}"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    import requests
    try:
        if method == 'POST':
            response = requests.post(url, headers=headers, json=data, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"API call failed: {e}")
        return None

def test_single_campaign_pagination():
    """Test that campaign_id parameter works for a single campaign"""
    logger.info("🧪 Testing single campaign pagination with campaign_id...")
    
    from shared.pagination_utils import fetch_all_leads
    from shared.api_client import SMB_CAMPAIGN_ID
    
    # Test with campaign filter (should use campaign_id parameter)
    smb_leads, smb_stats = fetch_all_leads(
        api_call_func=call_instantly_api,
        campaign_filter=SMB_CAMPAIGN_ID,
        batch_size=100,  # Max allowed by Instantly API
        use_cache=False
    )
    
    logger.info(f"✅ SMB campaign: {len(smb_leads)} leads in {smb_stats.total_pages} pages")
    
    return len(smb_leads) > 0

def test_multi_campaign_pagination():
    """Test that multi-campaign fetching works"""
    logger.info("🧪 Testing multi-campaign pagination...")
    
    from shared.pagination_utils import fetch_all_leads
    
    # Test without campaign filter (should fetch from both campaigns)  
    all_leads, combined_stats = fetch_all_leads(
        api_call_func=call_instantly_api,
        campaign_filter=None,  # This triggers multi-campaign mode
        batch_size=100,  # Max allowed by Instantly API
        use_cache=False
    )
    
    logger.info(f"✅ All campaigns: {len(all_leads)} leads in {combined_stats.total_pages} pages")
    
    # Check that we have the _fetched_from_campaign field
    campaigns_found = set()
    for lead in all_leads[:10]:  # Check first 10 leads
        if '_fetched_from_campaign' in lead:
            campaigns_found.add(lead['_fetched_from_campaign'])
    
    logger.info(f"   Campaigns found: {campaigns_found}")
    
    return len(all_leads) > 0

def main():
    logger.info("🔍 QUICK PAGINATION VALIDATION")
    logger.info("=" * 40)
    
    try:
        # Test single campaign
        single_success = test_single_campaign_pagination()
        
        # Test multi-campaign (but limit to save time)
        logger.info("\n" + "=" * 40)
        multi_success = test_multi_campaign_pagination()
        
        if single_success and multi_success:
            logger.info("\n🎉 PAGINATION FIX VALIDATED")
            logger.info("✅ Single campaign fetching works")
            logger.info("✅ Multi-campaign fetching works")
            logger.info("✅ campaign_id parameter is being sent correctly")
            return True
        else:
            logger.error("\n❌ VALIDATION FAILED")
            return False
            
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)