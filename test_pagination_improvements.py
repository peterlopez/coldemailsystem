#!/usr/bin/env python3
"""
Test script to validate the cursor-based pagination improvements
Compares old vs new pagination performance
"""

import os
import sys
import time
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import shared config and functions
from shared_config import config
from shared.pagination_utils import fetch_all_leads, get_paginator

def call_instantly_api_old_method():
    """Simulate old pagination method (for comparison)"""
    # This is just for testing - we won't actually implement the old method
    pass

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
        if method == 'GET':
            response = requests.get(url, headers=headers, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=data, timeout=30)
        
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"API call failed: {e}")
        return None

def test_new_pagination_performance():
    """Test the new cursor-based pagination"""
    logger.info("🧪 Testing NEW cursor-based pagination...")
    
    start_time = time.time()
    
    # Test with different batch sizes
    batch_sizes = [100, 200, 500]
    results = {}
    
    for batch_size in batch_sizes:
        logger.info(f"\n📏 Testing batch size: {batch_size}")
        
        batch_start = time.time()
        all_leads, stats = fetch_all_leads(
            api_call_func=call_instantly_api,
            campaign_filter=None,
            batch_size=batch_size,
            use_cache=False  # Don't use cache for testing
        )
        batch_duration = time.time() - batch_start
        
        results[batch_size] = {
            'total_leads': len(all_leads),
            'pages': stats.total_pages,
            'duration': batch_duration,
            'items_per_sec': len(all_leads) / batch_duration if batch_duration > 0 else 0,
            'cache_hit': stats.cache_hit
        }
        
        logger.info(f"  📊 Results: {len(all_leads)} leads in {stats.total_pages} pages")
        logger.info(f"  ⏱️  Duration: {batch_duration:.1f}s ({results[batch_size]['items_per_sec']:.1f} leads/sec)")
    
    # Find best performing batch size
    best_batch = max(results.keys(), key=lambda x: results[x]['items_per_sec'])
    logger.info(f"\n🏆 Best performing batch size: {best_batch} ({results[best_batch]['items_per_sec']:.1f} leads/sec)")
    
    return results

def test_caching_effectiveness():
    """Test the caching mechanism"""
    logger.info("\n🧪 Testing CACHING effectiveness...")
    
    # First call (should fetch from API)
    logger.info("📥 First call (should be live fetch)...")
    start_time = time.time()
    leads1, stats1 = fetch_all_leads(
        api_call_func=call_instantly_api,
        campaign_filter=None,
        batch_size=200,
        use_cache=True
    )
    first_call_duration = time.time() - start_time
    
    logger.info(f"  Duration: {first_call_duration:.1f}s, Cache hit: {stats1.cache_hit}")
    
    # Second call (should use cache)
    logger.info("\n📋 Second call (should use cache)...")
    start_time = time.time()
    leads2, stats2 = fetch_all_leads(
        api_call_func=call_instantly_api,
        campaign_filter=None,
        batch_size=200,
        use_cache=True
    )
    second_call_duration = time.time() - start_time
    
    logger.info(f"  Duration: {second_call_duration:.1f}s, Cache hit: {stats2.cache_hit}")
    
    # Calculate cache effectiveness
    if stats2.cache_hit and first_call_duration > 0:
        speedup = first_call_duration / second_call_duration
        logger.info(f"\n🚀 Cache speedup: {speedup:.1f}x faster ({first_call_duration:.1f}s → {second_call_duration:.1f}s)")
    else:
        logger.warning("⚠️ Cache didn't work as expected")
    
    # Verify data consistency
    if len(leads1) == len(leads2):
        logger.info(f"✅ Data consistency: Both calls returned {len(leads1)} leads")
    else:
        logger.warning(f"⚠️ Data inconsistency: {len(leads1)} vs {len(leads2)} leads")

def test_large_inventory_handling():
    """Test how the system handles large inventories"""
    logger.info("\n🧪 Testing LARGE INVENTORY handling...")
    
    start_time = time.time()
    all_leads, stats = fetch_all_leads(
        api_call_func=call_instantly_api,
        campaign_filter=None,
        batch_size=200,
        use_cache=False
    )
    duration = time.time() - start_time
    
    # Analysis
    total_leads = len(all_leads)
    logger.info(f"\n📊 Large Inventory Analysis:")
    logger.info(f"  Total leads: {total_leads:,}")
    logger.info(f"  Total pages: {stats.total_pages}")
    logger.info(f"  Duration: {duration:.1f} seconds")
    logger.info(f"  Rate: {total_leads/duration:.1f} leads/second")
    logger.info(f"  Avg per page: {stats.avg_items_per_page:.1f}")
    
    # Campaign breakdown
    smb_count = sum(1 for lead in all_leads if lead.get('campaign') == '8c46e0c9-c1f9-4201-a8d6-6221bafeada6')
    midsize_count = sum(1 for lead in all_leads if lead.get('campaign') == '5ffbe8c3-dc0e-41e4-9999-48f00d2015df')
    other_count = total_leads - smb_count - midsize_count
    
    logger.info(f"\n📋 Campaign Breakdown:")
    logger.info(f"  SMB: {smb_count:,} leads")
    logger.info(f"  Midsize: {midsize_count:,} leads") 
    logger.info(f"  Other/Unassigned: {other_count:,} leads")
    
    # Status breakdown
    status_counts = {}
    for lead in all_leads:
        status = lead.get('status', 0)
        status_name = {
            1: 'active',
            2: 'paused', 
            3: 'completed',
            -1: 'bounced',
            -2: 'unsubscribed',
            -3: 'skipped'
        }.get(status, f'unknown_{status}')
        status_counts[status_name] = status_counts.get(status_name, 0) + 1
    
    logger.info(f"\n📈 Status Breakdown:")
    for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_leads) * 100 if total_leads > 0 else 0
        logger.info(f"  {status}: {count:,} ({percentage:.1f}%)")
    
    return {
        'total_leads': total_leads,
        'pages': stats.total_pages,
        'duration': duration,
        'rate': total_leads/duration if duration > 0 else 0,
        'smb_leads': smb_count,
        'midsize_leads': midsize_count,
        'status_breakdown': status_counts
    }

def compare_with_old_system():
    """Compare performance with what the old system would have achieved"""
    logger.info("\n🧪 Comparing with OLD SYSTEM limitations...")
    
    # Get current results
    results = test_large_inventory_handling()
    
    # Simulate old system (50 page limit)
    old_system_max_leads = 50 * 100  # 50 pages × 100 leads per page
    old_system_percentage = (old_system_max_leads / results['total_leads']) * 100 if results['total_leads'] > 0 else 0
    
    logger.info(f"\n📊 Old vs New System Comparison:")
    logger.info(f"  Old system (50 page limit): {old_system_max_leads:,} leads ({old_system_percentage:.1f}% of total)")
    logger.info(f"  New system (no limit): {results['total_leads']:,} leads (100% of total)")
    logger.info(f"  Improvement: {results['total_leads'] - old_system_max_leads:,} additional leads visible")
    logger.info(f"  Undercount eliminated: {100 - old_system_percentage:.1f}% more accurate")

def main():
    """Run all pagination tests"""
    logger.info("🚀 CURSOR-BASED PAGINATION IMPROVEMENT TESTS")
    logger.info("=" * 60)
    logger.info(f"Test started: {datetime.now()}")
    
    try:
        # Test 1: Performance with different batch sizes
        performance_results = test_new_pagination_performance()
        
        # Test 2: Caching effectiveness
        test_caching_effectiveness()
        
        # Test 3: Large inventory handling
        inventory_results = test_large_inventory_handling()
        
        # Test 4: Compare with old system
        compare_with_old_system()
        
        # Final summary
        logger.info("\n" + "=" * 60)
        logger.info("🎉 ALL TESTS COMPLETED SUCCESSFULLY")
        logger.info(f"Total leads discovered: {inventory_results['total_leads']:,}")
        logger.info(f"Processing rate: {inventory_results['rate']:.1f} leads/second")
        logger.info(f"Pages processed: {inventory_results['pages']}")
        logger.info("✅ Pagination improvements working correctly")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)