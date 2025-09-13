#!/usr/bin/env python3
"""
Simple validation test to confirm pagination improvements are working
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

def test_pagination_fix():
    """Test that we can now fetch more than 5,100 leads"""
    logger.info("🧪 Testing cursor-based pagination fix...")
    
    # Test the old way (simulate the 50-page limit)
    logger.info("\n1️⃣ Simulating OLD SYSTEM (50-page limit):")
    starting_after = None
    page_count = 0
    old_system_leads = []
    
    while page_count < 50:  # Old limit
        data = {'limit': 100}
        if starting_after:
            data['starting_after'] = starting_after
        
        response = call_instantly_api('/api/v2/leads/list', method='POST', data=data)
        if not response or not response.get('items'):
            break
        
        items = response.get('items', [])
        old_system_leads.extend(items)
        page_count += 1
        
        starting_after = response.get('next_starting_after')
        if not starting_after:
            break
    
    logger.info(f"   Old system result: {len(old_system_leads)} leads in {page_count} pages")
    
    # Test the new way (no limit)
    logger.info("\n2️⃣ Testing NEW SYSTEM (no limit):")
    from shared.pagination_utils import fetch_all_leads
    
    all_leads, stats = fetch_all_leads(
        api_call_func=call_instantly_api,
        batch_size=100,
        use_cache=False
    )
    
    logger.info(f"   New system result: {len(all_leads)} leads in {stats.total_pages} pages")
    
    # Compare results
    improvement = len(all_leads) - len(old_system_leads)
    percentage_improvement = (improvement / len(old_system_leads)) * 100 if len(old_system_leads) > 0 else 0
    
    logger.info(f"\n📊 IMPROVEMENT SUMMARY:")
    logger.info(f"   Old system: {len(old_system_leads):,} leads")
    logger.info(f"   New system: {len(all_leads):,} leads")
    logger.info(f"   Improvement: +{improvement:,} leads ({percentage_improvement:.1f}% more)")
    
    if len(all_leads) > len(old_system_leads):
        logger.info("✅ PAGINATION FIX SUCCESSFUL - No more artificial limits!")
        return True
    else:
        logger.warning("⚠️ Results are the same - may need further investigation")
        return False

def main():
    logger.info("🔍 CURSOR-BASED PAGINATION VALIDATION")
    logger.info("=" * 50)
    
    success = test_pagination_fix()
    
    if success:
        logger.info("\n🎉 VALIDATION PASSED")
        logger.info("The pagination improvements are working correctly!")
        logger.info("The system can now handle inventories of any size.")
    else:
        logger.warning("\n⚠️ VALIDATION INCONCLUSIVE")
        logger.warning("The improvements may need further testing.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)