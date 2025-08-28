#!/usr/bin/env python3
"""
Poll Verification Results Script

This script polls Instantly.ai for pending email verification results and updates
BigQuery with the final verification statuses. Designed to run separately from
the main sync process to handle async verification without blocking lead creation.

Usage:
    python poll_verification_results.py [--max-leads N] [--stats] [--hours N]

Options:
    --max-leads N    Maximum number of leads to check per run (default: 500)
    --stats         Show verification statistics instead of polling
    --hours N       Hours back for statistics (default: 24)
"""

import os
import sys
import logging
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(
        description='Poll verification results from Instantly.ai',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--max-leads', type=int, default=500,
                       help='Maximum number of leads to check per run')
    parser.add_argument('--stats', action='store_true',
                       help='Show verification statistics instead of polling')
    parser.add_argument('--hours', type=int, default=24,
                       help='Hours back for statistics (when using --stats)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without making changes')
    
    args = parser.parse_args()
    
    try:
        from async_email_verification import AsyncEmailVerification
    except ImportError as e:
        logger.error(f"❌ Cannot import async verification module: {e}")
        logger.error("Make sure async_email_verification.py is in the current directory")
        return 1
    
    logger.info("🔍 ASYNC EMAIL VERIFICATION POLLING")
    logger.info("=" * 50)
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    
    if args.dry_run:
        logger.info("🚨 DRY RUN MODE - No changes will be made")
    
    try:
        verifier = AsyncEmailVerification()
        
        if args.stats:
            # Show verification statistics
            logger.info(f"📊 SHOWING VERIFICATION STATISTICS (last {args.hours} hours)")
            print()
            
            stats = verifier.get_verification_stats(args.hours)
            
            if stats:
                summary = stats.get('summary', {})
                total_verified = summary.get('total_verified', 0)
                total_credits = summary.get('total_credits_used', 0)
                
                print(f"📋 Verification Summary (last {args.hours} hours):")
                print(f"   Total leads verified: {total_verified:,}")
                print(f"   Total credits used: {total_credits:,}")
                print(f"   Average cost per lead: ${(total_credits * 0.01 / max(total_verified, 1)):.4f}")
                print()
                
                print("📊 Status Breakdown:")
                for status, data in stats.items():
                    if status != 'summary':
                        count = data.get('count', 0)
                        credits = data.get('credits', 0)
                        percentage = (count / max(total_verified, 1)) * 100
                        print(f"   {status}: {count:,} leads ({percentage:.1f}%) - {credits:,} credits")
                
                print()
                
                # Show cost analysis
                if total_credits > 0:
                    print("💰 Cost Analysis:")
                    print(f"   Credits consumed: {total_credits:,}")
                    print(f"   Estimated cost: ${total_credits * 0.01:.2f}")
                    if total_verified > 0:
                        print(f"   Cost per verified lead: ${(total_credits * 0.01 / total_verified):.4f}")
                    print()
                
            else:
                print("No verification data found for the specified time period")
                
        else:
            # Poll for verification results
            logger.info(f"🔍 POLLING VERIFICATION RESULTS (max {args.max_leads} leads)")
            print()
            
            if args.dry_run:
                logger.info("🚨 DRY RUN: Would poll verification results")
                return 0
                
            result = verifier.poll_verification_results(args.max_leads)
            
            success = result.get('success', False)
            checked = result.get('checked', 0)
            updated = result.get('updated', 0)
            still_pending = result.get('still_pending', 0)
            
            print("📊 Polling Results:")
            print(f"   Leads checked: {checked:,}")
            print(f"   Statuses updated: {updated:,}")
            print(f"   Still pending: {still_pending:,}")
            print()
            
            if success:
                logger.info("✅ Verification polling completed successfully")
                
                if updated > 0:
                    update_rate = (updated / max(checked, 1)) * 100
                    logger.info(f"📈 Update rate: {update_rate:.1f}% ({updated}/{checked})")
                    
                if still_pending > 0:
                    logger.info(f"⏳ {still_pending} leads still have pending verification")
                    logger.info("💡 Consider running this script again later to check for updates")
                    
                if checked == 0:
                    logger.info("✅ No leads found pending verification check")
                    
            else:
                logger.error("❌ Verification polling failed")
                return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error during verification polling: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)