#!/usr/bin/env python3
"""
Centralized Configuration for Cold Email Sync System
Reduces duplication and provides single source of truth for system settings.
"""

import os
import json
import logging
from typing import List, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ApiConfig:
    """Centralized API configuration."""
    instantly_api_key: str
    instantly_base_url: str = 'https://api.instantly.ai'
    timeout_seconds: int = 30
    max_retries: int = 3
    
    @classmethod
    def load(cls) -> 'ApiConfig':
        """Load API configuration with fallback logic."""
        api_key = os.getenv('INSTANTLY_API_KEY')
        
        # Fallback to config file if environment variable not set
        if not api_key:
            try:
                config_path = './config/secrets/instantly-config.json'
                if os.path.exists(config_path):
                    with open(config_path, 'r') as f:
                        config = json.load(f)
                        api_key = config.get('api_key')
                        logger.info("✅ Loaded API key from config file")
            except Exception as e:
                logger.warning(f"⚠️ Failed to load API key from config: {e}")
        
        if not api_key:
            raise ValueError("INSTANTLY_API_KEY not found in environment or config file")
        
        return cls(instantly_api_key=api_key)

@dataclass 
class CampaignConfig:
    """Campaign IDs and settings."""
    smb_campaign_id: str = "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"
    midsize_campaign_id: str = "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"
    revenue_threshold: int = 1_000_000  # SMB vs Midsize threshold
    
    def get_campaign_id_for_revenue(self, revenue: int) -> str:
        """Get appropriate campaign ID based on revenue."""
        return self.smb_campaign_id if revenue < self.revenue_threshold else self.midsize_campaign_id
    
    def get_sequence_name(self, campaign_id: str) -> str:
        """Get sequence name for campaign ID."""
        return 'SMB' if campaign_id == self.smb_campaign_id else 'Midsize'

@dataclass
class BigQueryConfig:
    """BigQuery configuration."""
    project_id: str = "instant-ground-394115"
    dataset_id: str = "email_analytics"
    credentials_path: str = "./config/secrets/bigquery-credentials.json"
    
    def get_table_name(self, table: str) -> str:
        """Get fully qualified table name."""
        return f"`{self.project_id}.{self.dataset_id}.{table}`"

@dataclass
class RateLimitConfig:
    """Rate limiting configuration - OPTIMIZED values."""
    # API pagination delays
    pagination_delay: float = 1.0  # OPTIMIZED: Reduced from 3.0s
    
    # Lead creation delays
    lead_creation_delay: float = 0.5  # Between individual lead creations
    
    # DELETE operation delays  
    delete_delay: float = 1.0  # OPTIMIZED: Reduced from 3.0s
    delete_batch_delay: float = 5.0  # OPTIMIZED: Reduced from 10.0s

@dataclass
class ProcessingConfig:
    """Processing limits and batch sizes."""
    # Batch sizes
    api_page_size: int = 50  # Leads per API page
    bigquery_batch_size: int = 100  # Bulk operations batch size
    drain_batch_size: int = 5  # DELETE operations batch size
    
    # Processing limits
    max_pages_per_campaign: int = 60  # Safety limit
    inventory_cap_guard: int = 24_000  # Total inventory limit
    
    # Lead selection  
    target_new_leads_per_run: int = 100  # RESTORED: No verification filtering, so back to original target
    
    # Timestamps and cooldowns
    stale_lead_days: int = 90  # Days before lead considered stale
    bounce_grace_days: int = 7  # Grace period for hard bounces

# InstantlyConfig class for async verification compatibility
class InstantlyConfig:
    """Configuration class for Instantly API access - needed for async verification"""
    
    def __init__(self):
        self.api_key = config.api.instantly_api_key
        self.base_url = config.api.instantly_base_url
        self.timeout = config.api.timeout_seconds

class SystemConfig:
    """Main configuration class that aggregates all config sections."""
    
    def __init__(self):
        self.api = ApiConfig.load()
        self.campaigns = CampaignConfig()
        self.bigquery = BigQueryConfig()
        self.rate_limits = RateLimitConfig()
        self.processing = ProcessingConfig()
        
        # System settings
        self.dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
        
        # Load dynamic values from environment
        self._load_environment_overrides()
    
    def _load_environment_overrides(self):
        """Load configuration overrides from environment variables."""
        # Processing overrides
        if os.getenv('TARGET_NEW_LEADS_PER_RUN'):
            self.processing.target_new_leads_per_run = int(os.getenv('TARGET_NEW_LEADS_PER_RUN'))
        
        if os.getenv('MAX_PAGES_TO_PROCESS'):
            max_pages = int(os.getenv('MAX_PAGES_TO_PROCESS'))
            if max_pages > 0:  # 0 means unlimited
                self.processing.max_pages_per_campaign = max_pages
        
        # Rate limit overrides for testing
        if os.getenv('PAGINATION_DELAY'):
            self.rate_limits.pagination_delay = float(os.getenv('PAGINATION_DELAY'))
    
    def get_instantly_headers(self) -> Dict[str, str]:
        """Get standard Instantly API headers."""
        return {
            'Authorization': f'Bearer {self.api.instantly_api_key}',
            'Content-Type': 'application/json'
        }
    
    def log_config_summary(self):
        """Log current configuration summary."""
        logger.info("🔧 System Configuration:")
        logger.info(f"   Dry Run: {self.dry_run}")
        logger.info(f"   Target Leads/Run: {self.processing.target_new_leads_per_run}")
        logger.info(f"   Pagination Delay: {self.rate_limits.pagination_delay}s")
        logger.info(f"   DELETE Delay: {self.rate_limits.delete_delay}s")

# Global configuration instance
config = SystemConfig()

# Backward compatibility exports (for existing code)
DRY_RUN = config.dry_run
SMB_CAMPAIGN_ID = config.campaigns.smb_campaign_id
MIDSIZE_CAMPAIGN_ID = config.campaigns.midsize_campaign_id
PROJECT_ID = config.bigquery.project_id
DATASET_ID = config.bigquery.dataset_id
TARGET_NEW_LEADS_PER_RUN = config.processing.target_new_leads_per_run
# VERIFICATION_VALID_STATUSES removed - no longer needed

def get_instantly_headers() -> Dict[str, str]:
    """Backward compatibility function."""
    return config.get_instantly_headers()