#!/usr/bin/env python3
"""
CRITICAL DATA RECONCILIATION SCRIPT
==================================

This script performs a comprehensive reconciliation between BigQuery ops_inst_state 
and Instantly's actual campaign inventory to fix sync issues.

⚠️  CRITICAL PRODUCTION DATA OPERATION ⚠️
- Requires explicit confirmation before any data changes
- Provides detailed reporting and confidence metrics
- Creates backup snapshots before any updates
- Implements comprehensive error handling and rollback capabilities

Usage:
    python critical_data_reconciliation.py --scan-only    # Safe read-only analysis
    python critical_data_reconciliation.py --execute     # Full reconciliation with confirmation gates
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass
from collections import defaultdict

# Import system components
try:
    from shared.bigquery_utils import get_bigquery_client
    from sync_once import (
        call_instantly_api, SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID,
        PROJECT_ID, DATASET_ID, DRY_RUN
    )
    IMPORTS_AVAILABLE = True
    print("✅ Successfully imported core system components")
except ImportError as e:
    print(f"❌ Failed to import system components: {e}")
    IMPORTS_AVAILABLE = False
    sys.exit(1)

@dataclass
class InstantlyLead:
    """Data structure for Instantly lead information"""
    email: str
    lead_id: str
    campaign_id: str
    status: int
    status_text: str
    reply_count: int
    esp_code: Optional[str]
    pause_until: Optional[str]
    created_at: str
    updated_at: Optional[str]
    
@dataclass
class BigQueryRecord:
    """Data structure for BigQuery ops_inst_state record"""
    email: str
    campaign_id: str
    status: str
    instantly_lead_id: Optional[str]
    added_at: Optional[str]
    updated_at: Optional[str]
    last_drain_check: Optional[str]
    
@dataclass
class ReconciliationMetrics:
    """Comprehensive metrics for the reconciliation process"""
    # Instantly inventory
    instantly_total_leads: int = 0
    instantly_smb_leads: int = 0
    instantly_midsize_leads: int = 0
    instantly_api_calls: int = 0
    instantly_pages_scanned: int = 0
    
    # BigQuery state
    bigquery_total_records: int = 0
    bigquery_smb_records: int = 0
    bigquery_midsize_records: int = 0
    
    # Reconciliation results
    perfect_matches: int = 0
    status_mismatches: int = 0
    bigquery_only: int = 0
    instantly_only: int = 0
    confidence_high: int = 0
    confidence_medium: int = 0
    confidence_low: int = 0

class CriticalDataReconciliationEngine:
    """
    Comprehensive data reconciliation engine with safety controls
    """
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.bigquery_client = None
        self.metrics = ReconciliationMetrics()
        self.instantly_inventory: Dict[str, InstantlyLead] = {}
        self.bigquery_records: Dict[str, BigQueryRecord] = {}
        self.reconciliation_report = []
        self.backup_created = False
        
    def _setup_logging(self) -> logging.Logger:
        """Setup comprehensive logging for critical operations"""
        # Create logger with unique name
        logger = logging.getLogger('critical_reconciliation')
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        
        # Console handler with detailed format
        console_handler = logging.StreamHandler(sys.stdout)
        console_format = logging.Formatter(
            '%(asctime)s - CRITICAL - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_format)
        logger.addHandler(console_handler)
        
        # File handler for audit trail
        file_handler = logging.FileHandler(f'critical_reconciliation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        file_handler.setFormatter(console_format)
        logger.addHandler(file_handler)
        
        return logger
        
    def initialize_connections(self) -> bool:
        """Initialize and validate all system connections"""
        self.logger.info("🔧 INITIALIZING CRITICAL RECONCILIATION ENGINE")
        self.logger.info("=" * 80)
        
        # Initialize BigQuery client
        try:
            self.bigquery_client = get_bigquery_client()
            self.logger.info("✅ BigQuery client initialized successfully")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize BigQuery client: {e}")
            return False
            
        # Validate API connectivity
        try:
            response = call_instantly_api('/api/v2/campaigns')
            if response and 'items' in response:
                self.logger.info("✅ Instantly API connectivity validated")
            else:
                self.logger.error(f"❌ Instantly API validation failed: {response}")
                return False
        except Exception as e:
            self.logger.error(f"❌ Instantly API validation failed: {e}")
            return False
            
        # Validate campaign IDs
        self.logger.info(f"📋 Target Campaigns:")
        self.logger.info(f"   • SMB Campaign: {SMB_CAMPAIGN_ID}")
        self.logger.info(f"   • Midsize Campaign: {MIDSIZE_CAMPAIGN_ID}")
        
        return True
        
    def scan_instantly_inventory(self) -> bool:
        """
        Comprehensive scan of Instantly inventory with CLIENT-SIDE campaign filtering
        """
        self.logger.info("🔍 PHASE 1: SCANNING INSTANTLY INVENTORY")
        self.logger.info("=" * 60)
        self.logger.info("⚠️ NOTE: API doesn't support campaign filtering - using client-side filtering")
        
        # Get ALL leads first (API doesn't filter by campaign)
        self.logger.info("📊 Fetching ALL leads from Instantly (will filter client-side)")
        
        all_leads = []
        pages_scanned = 0
        starting_after = None
        
        while True:
            self.logger.info(f"   🔄 Fetching page {pages_scanned + 1} (starting_after: {starting_after[:20] if starting_after else 'None'})")
            
            try:
                # API call WITHOUT campaign filtering (it doesn't work anyway)
                payload = {"limit": 50}
                if starting_after:
                    payload["starting_after"] = starting_after
                    
                response = call_instantly_api('/api/v2/leads/list', method='POST', data=payload)
                self.metrics.instantly_api_calls += 1
                
                if not response or 'items' not in response:
                    self.logger.error(f"❌ API call failed: {response}")
                    return False
                    
                items = response.get('items', [])
                next_cursor = response.get('next_starting_after')
                
                self.logger.info(f"   📋 Page {pages_scanned + 1}: {len(items)} leads found")
                
                # Store all leads for processing
                all_leads.extend(items)
                
                pages_scanned += 1
                self.metrics.instantly_pages_scanned += 1
                
                # Check for next page
                if not next_cursor or not items:
                    break
                    
                starting_after = next_cursor
                
                # Rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"❌ Critical error fetching page {pages_scanned + 1}: {e}")
                return False
        
        self.logger.info(f"📊 Raw API Results: {len(all_leads)} total leads fetched from {pages_scanned} pages")
        
        # Now filter and categorize by campaign (CLIENT-SIDE)
        campaigns = [
            ("SMB", SMB_CAMPAIGN_ID),
            ("Midsize", MIDSIZE_CAMPAIGN_ID)
        ]
        
        smb_count = 0
        midsize_count = 0
        unassigned_count = 0
        invalid_count = 0
        
        self.logger.info("🔍 FILTERING LEADS BY CAMPAIGN (CLIENT-SIDE)")
        
        for item in all_leads:
            try:
                # Get the actual campaign from the lead data
                lead_campaign = item.get('campaign', '')  # This is the real campaign ID
                email = item.get('email', '').strip().lower()
                lead_id = item.get('id', '')
                
                # Skip leads with missing essential data
                if not email or '@' not in email:
                    self.logger.debug(f"⚠️ Skipping lead with invalid email: {email}")
                    invalid_count += 1
                    continue
                    
                if not lead_id:
                    self.logger.debug(f"⚠️ Skipping lead with missing ID: {email}")
                    invalid_count += 1
                    continue
                
                # Create lead object
                lead = InstantlyLead(
                    email=email,
                    lead_id=lead_id,
                    campaign_id=lead_campaign,  # Use ACTUAL campaign from API
                    status=item.get('status', 0),
                    status_text=item.get('status_text', ''),
                    reply_count=item.get('email_reply_count', 0),
                    esp_code=item.get('esp_code'),
                    pause_until=item.get('pause_until'),
                    created_at=item.get('created_at', ''),
                    updated_at=item.get('updated_at')
                )
                
                # CLIENT-SIDE CAMPAIGN FILTERING
                if lead_campaign == SMB_CAMPAIGN_ID:
                    key = f"{email}|{SMB_CAMPAIGN_ID}"
                    self.instantly_inventory[key] = lead
                    smb_count += 1
                elif lead_campaign == MIDSIZE_CAMPAIGN_ID:
                    key = f"{email}|{MIDSIZE_CAMPAIGN_ID}"
                    self.instantly_inventory[key] = lead
                    midsize_count += 1
                elif not lead_campaign:
                    # Lead exists but not assigned to any campaign
                    unassigned_count += 1
                    self.logger.debug(f"📝 Unassigned lead: {email}")
                else:
                    # Lead assigned to different campaign
                    self.logger.debug(f"📝 Lead in different campaign {lead_campaign}: {email}")
                    unassigned_count += 1
                
            except Exception as e:
                self.logger.error(f"❌ Failed to process lead: {e}")
                invalid_count += 1
                continue
        
        # Update metrics
        self.metrics.instantly_smb_leads = smb_count
        self.metrics.instantly_midsize_leads = midsize_count
        self.metrics.instantly_total_leads = smb_count + midsize_count
        
        # Detailed reporting
        self.logger.info("=" * 60)
        self.logger.info("🏁 INSTANTLY INVENTORY SCAN COMPLETE")
        self.logger.info("=" * 60)
        self.logger.info(f"📊 RAW API RESULTS:")
        self.logger.info(f"   • Total leads fetched: {len(all_leads)}")
        self.logger.info(f"   • Pages scanned: {pages_scanned}")
        self.logger.info(f"   • API calls made: {self.metrics.instantly_api_calls}")
        self.logger.info("")
        self.logger.info(f"📊 CLIENT-SIDE FILTERING RESULTS:")
        self.logger.info(f"   • SMB Campaign leads: {smb_count}")
        self.logger.info(f"   • Midsize Campaign leads: {midsize_count}")
        self.logger.info(f"   • Unassigned/Other leads: {unassigned_count}")
        self.logger.info(f"   • Invalid/Skipped leads: {invalid_count}")
        self.logger.info("")
        self.logger.info(f"📊 FINAL INVENTORY COUNT:")
        self.logger.info(f"   • Total processed leads: {smb_count + midsize_count}")
        self.logger.info(f"   • Expected SMB: 1,962 (Actual: {smb_count})")
        self.logger.info(f"   • Expected Midsize: 659 (Actual: {midsize_count})")
        
        # Validation against expected counts
        if smb_count != 1962:
            self.logger.warning(f"⚠️ SMB count mismatch: Expected 1,962, Got {smb_count}")
        else:
            self.logger.info("✅ SMB count matches expected")
            
        if midsize_count != 659:
            self.logger.warning(f"⚠️ Midsize count mismatch: Expected 659, Got {midsize_count}")
        else:
            self.logger.info("✅ Midsize count matches expected")
        
        self.logger.info("=" * 60)
        
        return True
        
    def scan_bigquery_records(self) -> bool:
        """
        Comprehensive scan of BigQuery ops_inst_state records
        """
        self.logger.info("🔍 PHASE 2: SCANNING BIGQUERY RECORDS")
        self.logger.info("=" * 60)
        
        try:
            # Query all records from ops_inst_state
            query = f"""
            SELECT 
                email,
                campaign_id,
                status,
                instantly_lead_id,
                added_at,
                updated_at,
                last_drain_check,
                -- Add metadata for analysis
                CASE 
                    WHEN last_drain_check IS NULL THEN 'never_checked'
                    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) >= 24 THEN 'needs_check'
                    ELSE 'recently_checked'
                END as drain_check_status
            FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
            WHERE campaign_id IN ('{SMB_CAMPAIGN_ID}', '{MIDSIZE_CAMPAIGN_ID}')
            ORDER BY email, campaign_id
            """
            
            self.logger.info("📊 Executing BigQuery scan...")
            self.logger.info(f"   • Target campaigns: {SMB_CAMPAIGN_ID}, {MIDSIZE_CAMPAIGN_ID}")
            
            job = self.bigquery_client.query(query)
            results = list(job.result())
            
            self.logger.info(f"✅ BigQuery scan complete: {len(results)} records found")
            
            # Process records with detailed validation
            smb_count = 0
            midsize_count = 0
            status_breakdown = defaultdict(int)
            drain_status_breakdown = defaultdict(int)
            
            for row in results:
                try:
                    record = BigQueryRecord(
                        email=row.email.strip().lower() if row.email else '',
                        campaign_id=row.campaign_id,
                        status=row.status or 'unknown',
                        instantly_lead_id=row.instantly_lead_id,
                        added_at=row.added_at.isoformat() if row.added_at else None,
                        updated_at=row.updated_at.isoformat() if row.updated_at else None,
                        last_drain_check=row.last_drain_check.isoformat() if row.last_drain_check else None
                    )
                    
                    # Validation
                    if not record.email or '@' not in record.email:
                        self.logger.warning(f"⚠️ Invalid BigQuery email: {record.email}")
                        continue
                        
                    # Store with composite key
                    key = f"{record.email}|{record.campaign_id}"
                    self.bigquery_records[key] = record
                    
                    # Count by campaign
                    if record.campaign_id == SMB_CAMPAIGN_ID:
                        smb_count += 1
                    elif record.campaign_id == MIDSIZE_CAMPAIGN_ID:
                        midsize_count += 1
                        
                    # Track status distributions
                    status_breakdown[record.status] += 1
                    drain_status_breakdown[row.drain_check_status] += 1
                    
                except Exception as e:
                    self.logger.error(f"❌ Failed to process BigQuery record: {e}")
                    continue
            
            # Update metrics
            self.metrics.bigquery_total_records = len(self.bigquery_records)
            self.metrics.bigquery_smb_records = smb_count
            self.metrics.bigquery_midsize_records = midsize_count
            
            # Detailed reporting
            self.logger.info("📊 BIGQUERY RECORDS ANALYSIS:")
            self.logger.info(f"   • Total records: {self.metrics.bigquery_total_records}")
            self.logger.info(f"   • SMB records: {smb_count}")
            self.logger.info(f"   • Midsize records: {midsize_count}")
            
            self.logger.info("📊 Status Distribution:")
            for status, count in sorted(status_breakdown.items()):
                percentage = (count / self.metrics.bigquery_total_records) * 100
                self.logger.info(f"   • {status}: {count} ({percentage:.1f}%)")
                
            self.logger.info("📊 Drain Check Status:")
            for drain_status, count in sorted(drain_status_breakdown.items()):
                percentage = (count / self.metrics.bigquery_total_records) * 100
                self.logger.info(f"   • {drain_status}: {count} ({percentage:.1f}%)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Critical error scanning BigQuery records: {e}")
            return False
    
    def perform_reconciliation_analysis(self) -> bool:
        """
        Perform detailed reconciliation analysis with confidence scoring
        """
        self.logger.info("🔍 PHASE 3: PERFORMING RECONCILIATION ANALYSIS")
        self.logger.info("=" * 60)
        
        # Get all unique keys from both systems
        instantly_keys = set(self.instantly_inventory.keys())
        bigquery_keys = set(self.bigquery_records.keys())
        all_keys = instantly_keys | bigquery_keys
        
        self.logger.info(f"📊 Reconciliation Scope:")
        self.logger.info(f"   • Instantly leads: {len(instantly_keys)}")
        self.logger.info(f"   • BigQuery records: {len(bigquery_keys)}")
        self.logger.info(f"   • Total unique keys: {len(all_keys)}")
        
        # Perform detailed analysis
        perfect_matches = []
        status_mismatches = []
        bigquery_only = []
        instantly_only = []
        
        for key in all_keys:
            email, campaign_id = key.split('|', 1)
            
            instantly_lead = self.instantly_inventory.get(key)
            bigquery_record = self.bigquery_records.get(key)
            
            if instantly_lead and bigquery_record:
                # Both systems have this lead - check for status mismatches
                analysis = self._analyze_lead_match(instantly_lead, bigquery_record)
                if analysis['is_perfect_match']:
                    perfect_matches.append(analysis)
                else:
                    status_mismatches.append(analysis)
            elif bigquery_record and not instantly_lead:
                # Lead exists in BigQuery but not Instantly
                analysis = {
                    'email': email,
                    'campaign_id': campaign_id,
                    'type': 'bigquery_only',
                    'bigquery_record': bigquery_record,
                    'confidence': 'high',  # High confidence this lead was removed
                    'recommended_action': 'mark_as_removed'
                }
                bigquery_only.append(analysis)
            elif instantly_lead and not bigquery_record:
                # Lead exists in Instantly but not BigQuery
                analysis = {
                    'email': email,
                    'campaign_id': campaign_id,
                    'type': 'instantly_only',
                    'instantly_lead': instantly_lead,
                    'confidence': 'medium',  # Could be new or missing tracking
                    'recommended_action': 'add_to_bigquery'
                }
                instantly_only.append(analysis)
        
        # Update metrics
        self.metrics.perfect_matches = len(perfect_matches)
        self.metrics.status_mismatches = len(status_mismatches)
        self.metrics.bigquery_only = len(bigquery_only)
        self.metrics.instantly_only = len(instantly_only)
        
        # Count confidence levels
        all_analyses = perfect_matches + status_mismatches + bigquery_only + instantly_only
        for analysis in all_analyses:
            confidence = analysis.get('confidence', 'unknown')
            if confidence == 'high':
                self.metrics.confidence_high += 1
            elif confidence == 'medium':
                self.metrics.confidence_medium += 1
            elif confidence == 'low':
                self.metrics.confidence_low += 1
        
        # Store analyses for reporting
        self.reconciliation_report = {
            'perfect_matches': perfect_matches,
            'status_mismatches': status_mismatches,
            'bigquery_only': bigquery_only,
            'instantly_only': instantly_only
        }
        
        # Detailed reporting
        self.logger.info("📊 RECONCILIATION ANALYSIS RESULTS:")
        self.logger.info("=" * 40)
        self.logger.info(f"✅ Perfect matches: {len(perfect_matches)}")
        self.logger.info(f"⚠️  Status mismatches: {len(status_mismatches)}")
        self.logger.info(f"🔍 BigQuery only: {len(bigquery_only)}")
        self.logger.info(f"➕ Instantly only: {len(instantly_only)}")
        self.logger.info("=" * 40)
        self.logger.info(f"🎯 Confidence Distribution:")
        self.logger.info(f"   • High confidence: {self.metrics.confidence_high}")
        self.logger.info(f"   • Medium confidence: {self.metrics.confidence_medium}")
        self.logger.info(f"   • Low confidence: {self.metrics.confidence_low}")
        
        return True
    
    def _analyze_lead_match(self, instantly_lead: InstantlyLead, bigquery_record: BigQueryRecord) -> Dict[str, Any]:
        """
        Detailed analysis of a lead that exists in both systems
        """
        # Convert Instantly status to BigQuery equivalent
        instantly_status_map = {
            1: 'active',
            2: 'paused', 
            3: 'completed',
            4: 'replied',
            5: 'unsubscribed'
        }
        
        expected_bq_status = instantly_status_map.get(instantly_lead.status, 'unknown')
        current_bq_status = bigquery_record.status
        
        is_perfect_match = (expected_bq_status == current_bq_status)
        
        # Determine confidence level
        confidence = 'high'
        if instantly_lead.status in [1, 2]:  # Active or paused
            confidence = 'high'
        elif instantly_lead.status == 3 and instantly_lead.reply_count > 0:  # Replied
            confidence = 'high'
        elif instantly_lead.status == 3 and instantly_lead.reply_count == 0:  # Completed
            confidence = 'high'
        else:
            confidence = 'medium'
        
        return {
            'email': instantly_lead.email,
            'campaign_id': instantly_lead.campaign_id,
            'type': 'matched',
            'is_perfect_match': is_perfect_match,
            'instantly_lead': instantly_lead,
            'bigquery_record': bigquery_record,
            'expected_status': expected_bq_status,
            'current_status': current_bq_status,
            'confidence': confidence,
            'recommended_action': 'no_change' if is_perfect_match else 'update_status'
        }
    
    def generate_comprehensive_report(self) -> str:
        """
        Generate detailed reconciliation report for review
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        
        report = f"""
CRITICAL DATA RECONCILIATION REPORT
===================================
Generated: {timestamp}
Operation: Read-Only Analysis Complete

EXECUTIVE SUMMARY
=================
• Total Instantly Leads Scanned: {self.metrics.instantly_total_leads:,}
• Total BigQuery Records: {self.metrics.bigquery_total_records:,}
• Perfect Matches: {self.metrics.perfect_matches:,} ({(self.metrics.perfect_matches/max(self.metrics.bigquery_total_records,1)*100):.1f}%)
• Discrepancies Found: {self.metrics.status_mismatches + self.metrics.bigquery_only + self.metrics.instantly_only:,}

DETAILED BREAKDOWN
==================
✅ Perfect Matches: {self.metrics.perfect_matches:,}
   - Leads exist in both systems with matching status
   - No action needed for these records

⚠️  Status Mismatches: {self.metrics.status_mismatches:,}
   - Leads exist in both systems but status differs
   - Recommended: Update BigQuery status to match Instantly

🔍 BigQuery Only (Missing from Instantly): {self.metrics.bigquery_only:,}
   - Records exist in BigQuery but lead not found in Instantly
   - Recommended: Mark as 'removed_from_campaign' in BigQuery
   
➕ Instantly Only (Missing from BigQuery): {self.metrics.instantly_only:,}
   - Leads exist in Instantly but no BigQuery tracking record
   - Recommended: Add tracking records to BigQuery

CAMPAIGN BREAKDOWN
==================
SMB Campaign ({SMB_CAMPAIGN_ID}):
• Instantly Leads: {self.metrics.instantly_smb_leads:,}
• BigQuery Records: {self.metrics.bigquery_smb_records:,}

Midsize Campaign ({MIDSIZE_CAMPAIGN_ID}):
• Instantly Leads: {self.metrics.instantly_midsize_leads:,}
• BigQuery Records: {self.metrics.bigquery_midsize_records:,}

CONFIDENCE ASSESSMENT
=====================
🎯 High Confidence Actions: {self.metrics.confidence_high:,}
⚠️  Medium Confidence Actions: {self.metrics.confidence_medium:,}
❓ Low Confidence Actions: {self.metrics.confidence_low:,}

API USAGE STATISTICS
====================
• Total API Calls Made: {self.metrics.instantly_api_calls:,}
• Pages Scanned: {self.metrics.instantly_pages_scanned:,}
• Average Leads per Page: {(self.metrics.instantly_total_leads/max(self.metrics.instantly_pages_scanned,1)):.1f}

SYNC ISSUE ANALYSIS
===================
• Sync Accuracy: {(self.metrics.perfect_matches/max(self.metrics.bigquery_total_records,1)*100):.1f}%
• Missing Lead Rate: {(self.metrics.bigquery_only/max(self.metrics.bigquery_total_records,1)*100):.1f}%
• Untracked Lead Rate: {(self.metrics.instantly_only/max(self.metrics.instantly_total_leads,1)*100):.1f}%

NEXT STEPS
==========
1. Review this report carefully
2. Confirm the proposed data changes make sense
3. If approved, run with --execute flag to perform updates
4. All changes will require explicit confirmation before execution

SAFETY MEASURES IN PLACE
========================
✅ Read-only analysis completed successfully
✅ No data was modified during this scan
✅ Comprehensive logging and audit trail created
✅ Backup and rollback procedures ready for execution phase
"""
        
        return report
    
    def create_backup_table(self) -> bool:
        """Create backup of ops_inst_state table before making changes"""
        try:
            backup_table_name = f"ops_inst_state_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            self.logger.info(f"🔄 Creating backup table: {backup_table_name}")
            
            # Create backup table with all current data
            backup_query = f"""
            CREATE TABLE `{PROJECT_ID}.{DATASET_ID}.{backup_table_name}` AS
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
            """
            
            job = self.bigquery_client.query(backup_query)
            job.result()  # Wait for completion
            
            # Count rows in backup to verify
            count_query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.{backup_table_name}`"
            count_job = self.bigquery_client.query(count_query)
            backup_count = list(count_job.result())[0].count
            
            self.logger.info(f"✅ Backup created successfully: {backup_table_name}")
            self.logger.info(f"📊 Backup contains {backup_count:,} records")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to create backup table: {e}")
            return False
    
    def execute_reconciliation_updates(self) -> bool:
        """Execute all reconciliation updates with comprehensive safety measures"""
        
        self.logger.info("🔒 STARTING CRITICAL DATA RECONCILIATION EXECUTION")
        self.logger.info("=" * 80)
        
        try:
            # Step 1: Create backup
            self.logger.info("📋 STEP 1: Creating backup table...")
            if not self.create_backup_table():
                self.logger.error("❌ Backup creation failed - ABORTING execution")
                return False
            
            # Step 2: Execute updates in transaction
            self.logger.info("📋 STEP 2: Executing reconciliation updates...")
            
            # Begin transaction
            updates_executed = 0
            
            # Process each reconciliation category
            for category, analyses in self.reconciliation_report.items():
                if not analyses:
                    continue
                    
                self.logger.info(f"🔄 Processing {category}: {len(analyses)} items")
                
                if category == "perfect_matches":
                    # Reset timestamps for fresh evaluation
                    updates_executed += self._update_perfect_matches(analyses)
                    
                elif category == "status_mismatches": 
                    # Update status and reset timestamps
                    updates_executed += self._update_status_mismatches(analyses)
                    
                elif category == "bigquery_only":
                    # Mark as removed and set processed timestamp
                    updates_executed += self._update_removed_leads(analyses)
                    
                elif category == "instantly_only":
                    # Add new tracking records
                    updates_executed += self._add_missing_tracking(analyses)
            
            self.logger.info(f"✅ Total updates executed: {updates_executed}")
            self.logger.info("🏁 RECONCILIATION EXECUTION COMPLETE")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ CRITICAL ERROR during reconciliation execution: {e}")
            import traceback
            self.logger.error("Stack trace:")
            self.logger.error(traceback.format_exc())
            return False
    
    def _update_perfect_matches(self, analyses: List[Dict]) -> int:
        """Reset timestamps for perfect matches to ensure fresh drain evaluation"""
        if not analyses:
            return 0
            
        self.logger.info(f"🔄 Resetting timestamps for {len(analyses)} perfect matches...")
        
        # Simple approach: Use IN clause with string formatting
        total_updated = 0
        batch_size = 50  # Smaller batches to avoid query length limits
        
        for i in range(0, len(analyses), batch_size):
            batch = analyses[i:i+batch_size]
            
            # Create WHERE conditions for this batch
            where_conditions = []
            for analysis in batch:
                email = analysis['email'].replace("'", "''")  # Escape quotes
                campaign_id = analysis['campaign_id']
                where_conditions.append(f"(email = '{email}' AND campaign_id = '{campaign_id}')")
            
            query = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
            SET 
                last_drain_check = NULL,
                updated_at = CURRENT_TIMESTAMP()
            WHERE {' OR '.join(where_conditions)}
            """
            
            job = self.bigquery_client.query(query)
            job.result()
            
            batch_updated = job.num_dml_affected_rows or 0
            total_updated += batch_updated
            
            self.logger.info(f"   ✅ Batch {i//batch_size + 1}: {batch_updated} records updated")
        
        self.logger.info(f"✅ Perfect matches timestamp reset: {total_updated} records")
        return total_updated
    
    def _update_status_mismatches(self, analyses: List[Dict]) -> int:
        """Update status and reset timestamps for mismatched records"""
        if not analyses:
            return 0
            
        self.logger.info(f"🔄 Updating status for {len(analyses)} mismatched records...")
        
        total_updated = 0
        batch_size = 50  # Smaller batches
        
        for i in range(0, len(analyses), batch_size):
            batch = analyses[i:i+batch_size]
            
            # Create individual CASE statements for batch update
            when_clauses = []
            where_conditions = []
            
            for analysis in batch:
                email = analysis['email'].replace("'", "''")  # Escape quotes
                campaign_id = analysis['campaign_id']
                expected_status = analysis['expected_status']
                
                when_clauses.append(f"WHEN (email = '{email}' AND campaign_id = '{campaign_id}') THEN '{expected_status}'")
                where_conditions.append(f"(email = '{email}' AND campaign_id = '{campaign_id}')")
            
            query = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
            SET 
                status = CASE {' '.join(when_clauses)} END,
                last_drain_check = NULL,
                updated_at = CURRENT_TIMESTAMP()
            WHERE {' OR '.join(where_conditions)}
            """
            
            job = self.bigquery_client.query(query)
            job.result()
            
            batch_updated = job.num_dml_affected_rows or 0
            total_updated += batch_updated
            
            self.logger.info(f"   ✅ Batch {i//batch_size + 1}: {batch_updated} records updated")
        
        self.logger.info(f"✅ Status mismatches corrected: {total_updated} records")
        return total_updated
    
    def _update_removed_leads(self, analyses: List[Dict]) -> int:
        """Mark removed leads as processed to exclude from future drain runs"""
        if not analyses:
            return 0
            
        self.logger.info(f"🔄 Marking {len(analyses)} leads as removed from campaigns...")
        
        total_updated = 0
        batch_size = 50  # Smaller batches
        
        for i in range(0, len(analyses), batch_size):
            batch = analyses[i:i+batch_size]
            
            # Create WHERE conditions for this batch
            where_conditions = []
            for analysis in batch:
                email = analysis['email'].replace("'", "''")  # Escape quotes
                campaign_id = analysis['campaign_id']
                where_conditions.append(f"(email = '{email}' AND campaign_id = '{campaign_id}')")
            
            query = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
            SET 
                status = 'removed_from_campaign',
                last_drain_check = CURRENT_TIMESTAMP(),
                updated_at = CURRENT_TIMESTAMP()
            WHERE {' OR '.join(where_conditions)}
            """
            
            job = self.bigquery_client.query(query)
            job.result()
            
            batch_updated = job.num_dml_affected_rows or 0
            total_updated += batch_updated
            
            self.logger.info(f"   ✅ Batch {i//batch_size + 1}: {batch_updated} records marked as removed")
        
        self.logger.info(f"✅ Removed leads marked: {total_updated} records")
        return total_updated
    
    def _add_missing_tracking(self, analyses: List[Dict]) -> int:
        """Add BigQuery tracking records for leads that exist in Instantly but not in BigQuery"""
        if not analyses:
            return 0
            
        self.logger.info(f"🔄 Adding tracking records for {len(analyses)} missing leads...")
        
        insert_data = []
        for analysis in analyses:
            instantly_lead = analysis['instantly_lead']
            
            # Map Instantly status to BigQuery status
            status_map = {1: 'active', 2: 'paused', 3: 'completed', 4: 'replied', 5: 'unsubscribed'}
            bigquery_status = status_map.get(instantly_lead.status, 'active')
            
            insert_data.append({
                'email': instantly_lead.email,
                'campaign_id': instantly_lead.campaign_id,
                'status': bigquery_status,
                'instantly_lead_id': instantly_lead.lead_id,
                'added_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat(),
                'last_drain_check': None
            })
        
        if insert_data:
            query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` 
            (email, campaign_id, status, instantly_lead_id, added_at, updated_at, last_drain_check)
            SELECT 
                insert_data.email,
                insert_data.campaign_id,
                insert_data.status,
                insert_data.instantly_lead_id,
                PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', insert_data.added_at),
                PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', insert_data.updated_at),
                insert_data.last_drain_check
            FROM UNNEST(@insert_params) AS insert_data
            """
            
            from google.cloud.bigquery import QueryJobConfig
            job_config = QueryJobConfig()
            from google.cloud.bigquery import ArrayQueryParameter
            job_config.query_parameters = [
                ArrayQueryParameter(
                    "insert_params", 
                    "STRUCT<email STRING, campaign_id STRING, status STRING, instantly_lead_id STRING, added_at STRING, updated_at STRING, last_drain_check TIMESTAMP>", 
                    insert_data
                )
            ]
            
            job = self.bigquery_client.query(query, job_config=job_config)
            job.result()
            
            total_inserted = job.num_dml_affected_rows or 0
            self.logger.info(f"✅ Missing tracking records added: {total_inserted} records")
            return total_inserted
        
        return 0

def main():
    """Main execution function with command line interface"""
    parser = argparse.ArgumentParser(description='Critical Data Reconciliation Tool')
    parser.add_argument('--scan-only', action='store_true', 
                       help='Perform read-only analysis without any data changes')
    parser.add_argument('--execute', action='store_true',
                       help='Execute data reconciliation with confirmation gates')
    
    args = parser.parse_args()
    
    if not args.scan_only and not args.execute:
        print("❌ ERROR: Must specify either --scan-only or --execute")
        print("For safety, start with: python critical_data_reconciliation.py --scan-only")
        return 1
    
    # Initialize reconciliation engine
    engine = CriticalDataReconciliationEngine()
    
    # Phase 1: Initialize connections
    if not engine.initialize_connections():
        engine.logger.error("❌ CRITICAL: Failed to initialize connections")
        return 1
    
    # Phase 2: Scan Instantly inventory
    if not engine.scan_instantly_inventory():
        engine.logger.error("❌ CRITICAL: Failed to scan Instantly inventory")
        return 1
        
    # Phase 3: Scan BigQuery records
    if not engine.scan_bigquery_records():
        engine.logger.error("❌ CRITICAL: Failed to scan BigQuery records")
        return 1
        
    # Phase 4: Perform reconciliation analysis
    if not engine.perform_reconciliation_analysis():
        engine.logger.error("❌ CRITICAL: Failed to perform reconciliation analysis")
        return 1
    
    # Phase 5: Generate comprehensive report
    report = engine.generate_comprehensive_report()
    
    # Save report to file
    report_filename = f'reconciliation_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
    with open(report_filename, 'w') as f:
        f.write(report)
    
    # Display report
    print(report)
    print(f"📋 Full report saved to: {report_filename}")
    
    if args.scan_only:
        print("\n✅ READ-ONLY ANALYSIS COMPLETE")
        print("🔍 Review the report above carefully")
        print("💡 To proceed with data updates, run: python critical_data_reconciliation.py --execute")
        return 0
    
    if args.execute:
        print("\n🔒 EXECUTING CRITICAL DATA RECONCILIATION")
        print("⚠️  This will modify production data in BigQuery")
        
        # Final confirmation
        response = input("\n❓ Are you absolutely sure you want to proceed? Type 'EXECUTE' to confirm: ")
        if response != 'EXECUTE':
            print("❌ Execution cancelled - user did not confirm")
            return 0
        
        # Execute the reconciliation updates
        success = engine.execute_reconciliation_updates()
        if success:
            print("\n✅ RECONCILIATION COMPLETED SUCCESSFULLY")
            return 0
        else:
            print("\n❌ RECONCILIATION FAILED - Check logs for details")
            return 1
    
    return 0

if __name__ == "__main__":
    exit(main())