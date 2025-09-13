"""
Cursor-based pagination utilities for Instantly API
Provides standardized, efficient pagination with caching and monitoring
"""

import time
import logging
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class PaginationStats:
    """Statistics for pagination performance monitoring"""
    total_pages: int = 0
    total_items: int = 0
    duration_seconds: float = 0.0
    avg_items_per_page: float = 0.0
    cache_hit: bool = False
    
    def __post_init__(self):
        if self.total_pages > 0:
            self.avg_items_per_page = self.total_items / self.total_pages

@dataclass
class CachedResult:
    """Cached pagination result with timestamp"""
    data: List[Dict[str, Any]]
    timestamp: datetime
    stats: PaginationStats
    
    def is_expired(self, cache_ttl_minutes: int = 5) -> bool:
        """Check if cached result is expired"""
        return datetime.utcnow() - self.timestamp > timedelta(minutes=cache_ttl_minutes)

class CursorPaginator:
    """
    High-performance cursor-based pagination for Instantly API
    Features:
    - Cursor-based pagination (no artificial limits)
    - Performance monitoring
    - Error handling with retry logic
    - Optional caching
    - Progress logging for large datasets
    """
    
    def __init__(self, api_call_func: Callable, cache_ttl_minutes: int = 5):
        """
        Initialize paginator
        
        Args:
            api_call_func: Function to make API calls (e.g., call_instantly_api)
            cache_ttl_minutes: How long to cache results (0 = no caching)
        """
        self.api_call_func = api_call_func
        self.cache_ttl_minutes = cache_ttl_minutes
        self._cache: Dict[str, CachedResult] = {}
    
    def fetch_all(
        self,
        endpoint: str,
        base_params: Dict[str, Any],
        batch_size: int = 100,
        max_retries: int = 3,
        progress_interval: int = 20,
        max_safety_pages: int = 1000
    ) -> tuple[List[Dict[str, Any]], PaginationStats]:
        """
        Fetch all items using cursor-based pagination
        
        Args:
            endpoint: API endpoint to call
            base_params: Base parameters for the API call
            batch_size: Items per page (100-500 recommended)
            max_retries: Retries per failed request
            progress_interval: Log progress every N pages
            max_safety_pages: Safety limit to prevent infinite loops
            
        Returns:
            Tuple of (all_items, pagination_stats)
        """
        # Generate cache key
        cache_key = self._generate_cache_key(endpoint, base_params, batch_size)
        
        # Check cache first
        if self.cache_ttl_minutes > 0 and cache_key in self._cache:
            cached = self._cache[cache_key]
            if not cached.is_expired(self.cache_ttl_minutes):
                logger.debug(f"📋 Using cached results: {len(cached.data)} items")
                cached.stats.cache_hit = True
                return cached.data, cached.stats
            else:
                # Remove expired cache
                del self._cache[cache_key]
        
        # Perform pagination
        start_time = time.time()
        all_items = []
        starting_after = None
        page_count = 0
        consecutive_failures = 0
        
        while True:
            # Prepare request parameters
            params = base_params.copy()
            params['limit'] = batch_size
            
            if starting_after:
                params['starting_after'] = starting_after
            
            # Make API call with retry logic
            try:
                response = self.api_call_func(endpoint, method='POST', data=params)
                
                if not response or not response.get('items'):
                    logger.debug("🔚 No more items returned, ending pagination")
                    break
                
                items = response.get('items', [])
                page_count += 1
                all_items.extend(items)
                consecutive_failures = 0  # Reset failure count on success
                
                logger.debug(f"  Page {page_count}: {len(items)} items fetched")
                
                # Progress logging for large datasets
                if progress_interval > 0 and page_count % progress_interval == 0:
                    elapsed = time.time() - start_time
                    rate = len(all_items) / elapsed if elapsed > 0 else 0
                    logger.info(f"📄 Progress: {page_count} pages, {len(all_items)} items ({rate:.1f} items/sec)")
                
                # Performance warnings
                if page_count == 100:
                    logger.warning(f"⚠️ Large dataset: {page_count} pages, {len(all_items)} items. Consider caching.")
                elif page_count == 200:
                    logger.warning(f"🐌 Very large dataset: {page_count} pages, {len(all_items)} items.")
                
                # Check for next page
                starting_after = response.get('next_starting_after')
                if not starting_after:
                    logger.debug("🔚 No next_starting_after, pagination complete")
                    break
                
                # Safety limit to prevent infinite loops
                if page_count >= max_safety_pages:
                    logger.error(f"❌ Safety limit reached: {page_count} pages. Possible pagination corruption.")
                    break
                
            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"⚠️ Pagination API error (attempt {consecutive_failures}/{max_retries}): {e}")
                
                if consecutive_failures >= max_retries:
                    logger.error(f"❌ Max retries reached for pagination. Returning partial results: {len(all_items)} items")
                    break
                
                # Wait before retry
                time.sleep(0.5 * consecutive_failures)
        
        # Calculate statistics
        duration = time.time() - start_time
        stats = PaginationStats(
            total_pages=page_count,
            total_items=len(all_items),
            duration_seconds=duration,
            cache_hit=False
        )
        
        logger.info(f"📊 Pagination complete: {len(all_items)} items in {page_count} pages ({duration:.1f}s)")
        
        # Cache results if caching enabled
        if self.cache_ttl_minutes > 0:
            self._cache[cache_key] = CachedResult(
                data=all_items.copy(),
                timestamp=datetime.utcnow(),
                stats=stats
            )
            logger.debug(f"💾 Results cached for {self.cache_ttl_minutes} minutes")
        
        return all_items, stats
    
    def _generate_cache_key(self, endpoint: str, params: Dict[str, Any], batch_size: int) -> str:
        """Generate cache key from request parameters"""
        # Create a simple cache key from endpoint and key parameters
        key_parts = [endpoint, str(batch_size)]
        
        # Include relevant parameters (exclude pagination-specific ones)
        for key, value in sorted(params.items()):
            if key not in ['starting_after', 'limit', 'page', 'per_page']:
                key_parts.append(f"{key}:{value}")
        
        return "|".join(key_parts)
    
    def clear_cache(self):
        """Clear all cached results"""
        cache_count = len(self._cache)
        self._cache.clear()
        logger.info(f"🗑️ Cleared {cache_count} cached pagination results")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_items = sum(result.data.__len__() for result in self._cache.values())
        return {
            'cached_queries': len(self._cache),
            'total_cached_items': total_items,
            'oldest_cache': min((r.timestamp for r in self._cache.values()), default=None),
            'newest_cache': max((r.timestamp for r in self._cache.values()), default=None)
        }

# Global paginator instance for easy reuse
_global_paginator = None

def get_paginator(api_call_func: Callable, cache_ttl_minutes: int = 5) -> CursorPaginator:
    """Get or create global paginator instance"""
    global _global_paginator
    
    if _global_paginator is None:
        _global_paginator = CursorPaginator(api_call_func, cache_ttl_minutes)
    
    return _global_paginator

def fetch_all_leads(api_call_func: Callable, campaign_filter: Optional[str] = None, 
                   batch_size: int = 100, use_cache: bool = True) -> tuple[List[Dict[str, Any]], PaginationStats]:
    """
    Convenience function to fetch all leads with optimized settings
    
    Args:
        api_call_func: API calling function
        campaign_filter: Optional campaign ID to filter by (if None, fetches from all campaigns)
        batch_size: Items per page (100 max for Instantly API)
        use_cache: Whether to use caching
        
    Returns:
        Tuple of (leads, stats)
    """
    # Import campaign IDs here to avoid circular imports
    from shared.api_client import SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID
    
    cache_ttl = 5 if use_cache else 0
    paginator = get_paginator(api_call_func, cache_ttl)
    
    # The Instantly API requires campaign_id for the /api/v2/leads/list endpoint
    if campaign_filter:
        # Single campaign mode
        params = {'campaign_id': campaign_filter}  # Note: 'campaign_id' not 'campaign' for list endpoint
        return paginator.fetch_all(
            endpoint='/api/v2/leads/list',
            base_params=params,
            batch_size=batch_size,
            progress_interval=25,
            max_safety_pages=2000
        )
    else:
        # Multi-campaign mode - fetch from both campaigns and combine
        logger.info("📊 Fetching leads from all campaigns...")
        all_leads = []
        total_duration = 0.0
        total_pages = 0
        cache_hits = 0
        
        campaigns = [
            ("SMB", SMB_CAMPAIGN_ID),
            ("Midsize", MIDSIZE_CAMPAIGN_ID)
        ]
        
        for campaign_name, campaign_id in campaigns:
            logger.info(f"🔍 Fetching {campaign_name} campaign leads...")
            params = {'campaign_id': campaign_id}
            
            campaign_leads, campaign_stats = paginator.fetch_all(
                endpoint='/api/v2/leads/list',
                base_params=params,
                batch_size=batch_size,
                progress_interval=25,
                max_safety_pages=2000
            )
            
            # Add campaign field to each lead for identification
            for lead in campaign_leads:
                lead['_fetched_from_campaign'] = campaign_id
            
            all_leads.extend(campaign_leads)
            total_duration += campaign_stats.duration_seconds
            total_pages += campaign_stats.total_pages
            if campaign_stats.cache_hit:
                cache_hits += 1
            
            logger.info(f"   ✅ {campaign_name}: {len(campaign_leads)} leads in {campaign_stats.total_pages} pages")
        
        # Create combined stats
        combined_stats = PaginationStats(
            total_pages=total_pages,
            total_items=len(all_leads),
            duration_seconds=total_duration,
            cache_hit=(cache_hits == len(campaigns))  # All campaigns were cached
        )
        
        logger.info(f"📊 Combined: {len(all_leads)} total leads from {len(campaigns)} campaigns")
        
        return all_leads, combined_stats