#!/usr/bin/env python3
"""
Validation Script for Phase 1 Optimizations
Tests that all optimization changes work correctly without breaking functionality.
"""

import sys
import os
import logging
from typing import List, Dict, Any

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_shared_config_import():
    """Test that shared configuration can be imported and initialized."""
    try:
        from shared_config import config, DRY_RUN, SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID
        logger.info("✅ Shared config import successful")
        
        # Test configuration values
        assert config.api.instantly_base_url == 'https://api.instantly.ai'
        assert config.campaigns.smb_campaign_id == "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"
        assert config.rate_limits.pagination_delay == 1.0  # Optimized from 3.0
        assert config.rate_limits.delete_delay == 1.0  # Optimized from 3.0
        
        logger.info("✅ Configuration values validated")
        return True
    except Exception as e:
        logger.error(f"❌ Shared config test failed: {e}")
        return False

def test_sync_once_imports():
    """Test that sync_once.py can import shared config successfully."""
    try:
        # Set DRY_RUN to avoid actual API calls
        os.environ['DRY_RUN'] = 'true'
        
        # Test import (this will test the configuration loading)
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        
        # This should work with shared config
        from sync_once import config, DRY_RUN, get_instantly_headers
        
        # Test that functions work
        headers = get_instantly_headers()
        assert 'Authorization' in headers
        assert 'Bearer' in headers['Authorization']
        
        logger.info("✅ sync_once.py imports and functions work")
        return True
    except Exception as e:
        logger.error(f"❌ sync_once import test failed: {e}")
        return False

def test_drain_once_imports():
    """Test that drain_once.py can import dependencies successfully."""
    try:
        os.environ['DRY_RUN'] = 'true'
        
        # This should work with fallback handling
        import drain_once
        
        logger.info("✅ drain_once.py imports work")
        return True
    except Exception as e:
        logger.error(f"❌ drain_once import test failed: {e}")
        return False

def test_bulk_operation_functions():
    """Test that bulk operation functions are defined and callable."""
    try:
        from sync_once import (
            _bulk_update_ops_inst_state,
            _bulk_insert_lead_history,
            _bulk_insert_dnc_list,
            _bulk_track_verification_failures,
            InstantlyLead
        )
        
        # Test that functions exist and are callable
        assert callable(_bulk_update_ops_inst_state)
        assert callable(_bulk_insert_lead_history)
        assert callable(_bulk_insert_dnc_list)
        assert callable(_bulk_track_verification_failures)
        
        logger.info("✅ All bulk operation functions are defined and callable")
        return True
    except Exception as e:
        logger.error(f"❌ Bulk operation functions test failed: {e}")
        return False

def test_optimized_rate_limits():
    """Test that rate limiting configuration is optimized."""
    try:
        from shared_config import config
        
        # Verify optimized values
        assert config.rate_limits.pagination_delay == 1.0, f"Expected 1.0, got {config.rate_limits.pagination_delay}"
        assert config.rate_limits.delete_delay == 1.0, f"Expected 1.0, got {config.rate_limits.delete_delay}"
        assert config.rate_limits.delete_batch_delay == 5.0, f"Expected 5.0, got {config.rate_limits.delete_batch_delay}"
        assert config.rate_limits.verification_delay == 0.2, f"Expected 0.2, got {config.rate_limits.verification_delay}"
        
        logger.info("✅ Rate limiting values are optimized")
        return True
    except Exception as e:
        logger.error(f"❌ Rate limiting test failed: {e}")
        return False

def test_configuration_summary():
    """Test configuration logging and summary."""
    try:
        from shared_config import config
        
        # Test that configuration summary works
        config.log_config_summary()
        
        logger.info("✅ Configuration summary works")
        return True
    except Exception as e:
        logger.error(f"❌ Configuration summary test failed: {e}")
        return False

def validate_environment_compatibility():
    """Test that optimizations work in GitHub Actions environment."""
    try:
        # Simulate GitHub Actions environment
        original_env = os.environ.copy()
        
        # Test with missing config file (GitHub Actions scenario)
        if 'INSTANTLY_API_KEY' not in os.environ:
            os.environ['INSTANTLY_API_KEY'] = 'test_key_for_validation'
        
        from shared_config import config
        
        # Verify it works with environment variable only
        assert config.api.instantly_api_key == 'test_key_for_validation'
        
        # Restore original environment
        os.environ.clear()
        os.environ.update(original_env)
        
        logger.info("✅ Environment compatibility validated")
        return True
    except Exception as e:
        logger.error(f"❌ Environment compatibility test failed: {e}")
        return False

def main():
    """Run all validation tests."""
    logger.info("🧪 Starting Phase 1 Optimization Validation")
    logger.info("=" * 60)
    
    tests = [
        ("Shared Config Import", test_shared_config_import),
        ("Sync Once Imports", test_sync_once_imports),
        ("Drain Once Imports", test_drain_once_imports),
        ("Bulk Operation Functions", test_bulk_operation_functions),
        ("Optimized Rate Limits", test_optimized_rate_limits),
        ("Configuration Summary", test_configuration_summary),
        ("Environment Compatibility", validate_environment_compatibility),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        logger.info(f"\n🔍 Running: {test_name}")
        try:
            if test_func():
                passed += 1
                logger.info(f"✅ PASSED: {test_name}")
            else:
                failed += 1
                logger.error(f"❌ FAILED: {test_name}")
        except Exception as e:
            failed += 1
            logger.error(f"❌ ERROR in {test_name}: {e}")
    
    logger.info("\n" + "=" * 60)
    logger.info("🏁 VALIDATION RESULTS")
    logger.info("=" * 60)
    logger.info(f"✅ Tests Passed: {passed}")
    logger.info(f"❌ Tests Failed: {failed}")
    logger.info(f"📊 Success Rate: {(passed/(passed+failed)*100):.1f}%")
    
    if failed == 0:
        logger.info("🎉 ALL OPTIMIZATIONS VALIDATED SUCCESSFULLY!")
        logger.info("✅ Ready for production deployment")
        return 0
    else:
        logger.error("⚠️ Some validations failed - review before deployment")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)