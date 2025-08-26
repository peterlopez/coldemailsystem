#!/usr/bin/env python3
"""
Test script for email verification functionality.
Run this to verify the implementation is working correctly.
"""

import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set test environment
os.environ['DRY_RUN'] = 'true'
os.environ['VERIFY_EMAILS_BEFORE_CREATION'] = 'true'
os.environ['TARGET_NEW_LEADS_PER_RUN'] = '5'  # Small batch for testing

def main():
    """Test the email verification implementation."""
    logger.info("🧪 Testing Email Verification Implementation")
    logger.info("=" * 50)
    
    try:
        # Import after setting environment variables
        from sync_once import (
            verify_email, 
            VERIFY_EMAILS_BEFORE_CREATION,
            VERIFICATION_VALID_STATUSES,
            logger as sync_logger
        )
        
        # Test 1: Check configuration
        logger.info("\n📋 Test 1: Configuration Check")
        logger.info(f"VERIFY_EMAILS_BEFORE_CREATION: {VERIFY_EMAILS_BEFORE_CREATION}")
        logger.info(f"VERIFICATION_VALID_STATUSES: {VERIFICATION_VALID_STATUSES}")
        logger.info(f"DRY_RUN mode: {os.getenv('DRY_RUN')}")
        
        # Test 2: Test verify_email function in dry run mode
        logger.info("\n📋 Test 2: Email Verification Function (Dry Run)")
        test_emails = [
            "valid@example.com",
            "test@shopify.com",
            "invalid@nonexistent123456.com"
        ]
        
        for email in test_emails:
            result = verify_email(email)
            logger.info(f"Verified {email}: {result}")
            assert result['status'] == 'valid', f"Dry run should return 'valid' for all emails"
            assert result['credits_used'] == 0, f"Dry run should use 0 credits"
        
        # Test 3: Run a small sync to test the full pipeline
        logger.info("\n📋 Test 3: Full Pipeline Test (Dry Run)")
        logger.info("Running sync_once.main() in dry run mode...")
        
        # Import and run main
        from sync_once import main as sync_main
        
        # Capture original log level
        original_level = sync_logger.level
        
        try:
            # Run the sync
            sync_main()
            logger.info("✅ Full pipeline test completed successfully")
        except Exception as e:
            logger.error(f"❌ Pipeline test failed: {e}")
            raise
        finally:
            # Restore log level
            sync_logger.setLevel(original_level)
        
        # Test 4: Verification disabled test
        logger.info("\n📋 Test 4: Verification Disabled Test")
        os.environ['VERIFY_EMAILS_BEFORE_CREATION'] = 'false'
        
        # Need to reload the module to pick up new environment variable
        import importlib
        import sync_once
        importlib.reload(sync_once)
        
        logger.info(f"VERIFY_EMAILS_BEFORE_CREATION after reload: {sync_once.VERIFY_EMAILS_BEFORE_CREATION}")
        
        logger.info("\n✅ All tests passed!")
        logger.info("\n📊 Next Steps:")
        logger.info("1. Run `python update_schema_for_verification.py` to update BigQuery schema")
        logger.info("2. Test with dry_run=false and a small batch")
        logger.info("3. Monitor verification metrics in logs")
        logger.info("4. Check BigQuery for verification data")
        
    except ImportError as e:
        logger.error(f"❌ Import error: {e}")
        logger.error("Make sure you're in the correct directory")
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()