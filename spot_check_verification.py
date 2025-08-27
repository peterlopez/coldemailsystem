#!/usr/bin/env python3
"""
Spot Check Email Verification
Manually test a sample of recent leads to investigate verification issues.
"""

import os
import sys
import json
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
from google.cloud import bigquery

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set DRY_RUN to false to test real verification
os.environ['DRY_RUN'] = 'false'

try:
    from sync_once import (
        verify_email, 
        call_instantly_api,
        get_bigquery_client,
        VERIFICATION_VALID_STATUSES,
        PROJECT_ID,
        DATASET_ID
    )
    logger.info("✅ Successfully imported verification functions")
except ImportError as e:
    logger.error(f"❌ Import failed: {e}")
    sys.exit(1)

def get_recent_verification_failures() -> List[Dict[str, Any]]:
    """Get sample of leads that failed verification in last run."""
    try:
        client = get_bigquery_client()
        
        # Query to get recent failed verifications with lead details
        query = f"""
        WITH recent_failures AS (
            SELECT 
                email,
                verification_status,
                verification_catch_all,
                added_at,
                verification_credits_used
            FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
            WHERE verification_status = 'invalid'
                AND verified_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
            ORDER BY verified_at DESC
            LIMIT 10
        )
        SELECT * FROM recent_failures
        """
        
        results = client.query(query).result()
        
        failed_leads = []
        for row in results:
            failed_leads.append({
                'email': row.email,
                'verification_status': row.verification_status,
                'verification_catch_all': row.verification_catch_all,
                'added_at': row.added_at,
                'credits_used': row.verification_credits_used
            })
        
        logger.info(f"📋 Found {len(failed_leads)} recent verification failures")
        return failed_leads
        
    except Exception as e:
        logger.error(f"❌ Failed to get recent failures: {e}")
        return []

def manual_verification_check(email: str) -> Dict[str, Any]:
    """Manually verify an email and return detailed results."""
    try:
        logger.info(f"🔍 Manually verifying: {email}")
        
        # Call verification directly
        result = verify_email(email)
        
        logger.info(f"📧 Manual verification result for {email}:")
        logger.info(f"   Status: {result.get('status', 'unknown')}")
        logger.info(f"   Catch-all: {result.get('catch_all', False)}")
        logger.info(f"   Disposable: {result.get('disposable', False)}")
        logger.info(f"   Role-based: {result.get('role_based', False)}")
        logger.info(f"   MX Records: {result.get('mx_records', False)}")
        logger.info(f"   Free Email: {result.get('free_email', False)}")
        logger.info(f"   Credits Used: {result.get('credits_used', 0)}")
        
        # Check if it would be accepted
        would_pass = result.get('status') in VERIFICATION_VALID_STATUSES
        logger.info(f"   Would Pass Filter: {'✅ YES' if would_pass else '❌ NO'}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Manual verification failed for {email}: {e}")
        return {'email': email, 'error': str(e)}

def test_known_good_emails() -> None:
    """Test some known good emails to check if verification is working."""
    known_good_emails = [
        "info@shopify.com",
        "contact@stripe.com", 
        "hello@notion.so",
        "support@github.com"
    ]
    
    logger.info(f"\n🧪 Testing {len(known_good_emails)} known good emails...")
    
    for email in known_good_emails:
        try:
            result = manual_verification_check(email)
            status = result.get('status', 'unknown')
            
            if status in VERIFICATION_VALID_STATUSES:
                logger.info(f"✅ {email} -> {status} (WOULD PASS)")
            else:
                logger.warning(f"⚠️ {email} -> {status} (WOULD FAIL)")
                
        except Exception as e:
            logger.error(f"❌ Error testing {email}: {e}")

def analyze_verification_api_directly() -> None:
    """Test the Instantly verification API directly to check for issues."""
    logger.info(f"\n🔧 Testing Instantly API directly...")
    
    test_email = "test@example.com"
    
    try:
        # Call API directly
        data = {'email': test_email}
        response = call_instantly_api('/api/v2/email-verification', 
                                    method='POST', 
                                    data=data)
        
        logger.info(f"📡 Direct API Response: {json.dumps(response, indent=2)}")
        
        # Check response structure
        if 'verification_status' in response:
            logger.info(f"✅ API returning expected 'verification_status' field")
        else:
            logger.warning(f"⚠️ API not returning 'verification_status' field")
            logger.warning(f"Available fields: {list(response.keys())}")
            
    except Exception as e:
        logger.error(f"❌ Direct API test failed: {e}")

def main():
    """Run comprehensive verification spot check."""
    logger.info("🔍 Starting Email Verification Spot Check")
    logger.info("=" * 60)
    
    # Check configuration
    logger.info(f"📋 Current Configuration:")
    logger.info(f"   Accepted Statuses: {VERIFICATION_VALID_STATUSES}")
    logger.info(f"   DRY_RUN: {os.getenv('DRY_RUN')}")
    
    # Test 1: Known good emails
    test_known_good_emails()
    
    # Test 2: Direct API test
    analyze_verification_api_directly()
    
    # Test 3: Recent failed leads
    logger.info(f"\n📊 Analyzing Recent Failed Leads...")
    recent_failures = get_recent_verification_failures()
    
    if recent_failures:
        logger.info(f"Found {len(recent_failures)} recent failures to investigate:")
        
        # Spot check first 3 failed leads
        for i, lead in enumerate(recent_failures[:3]):
            email = lead['email']
            old_status = lead['verification_status']
            
            logger.info(f"\n🔍 Spot Check #{i+1}: {email}")
            logger.info(f"   Previous Status: {old_status}")
            logger.info(f"   Previous Credits: {lead['credits_used']}")
            
            # Re-verify manually
            new_result = manual_verification_check(email)
            new_status = new_result.get('status', 'unknown')
            
            if new_status != old_status:
                logger.warning(f"⚠️ STATUS CHANGED: {old_status} -> {new_status}")
            else:
                logger.info(f"✅ Status consistent: {new_status}")
    else:
        logger.warning("⚠️ No recent verification failures found in BigQuery")
    
    logger.info("\n" + "=" * 60)
    logger.info("🏁 Spot Check Complete")
    
    # Recommendations
    logger.info(f"\n💡 Next Steps:")
    logger.info(f"   1. Review the verification results above")
    logger.info(f"   2. Check if known good emails are failing")
    logger.info(f"   3. Look for patterns in API responses")
    logger.info(f"   4. Consider if Instantly API behavior has changed")

if __name__ == "__main__":
    main()