# Cold Email System - File Organization

## 🏭 PRODUCTION FILES (Active in GitHub Actions)

### Core Production Scripts
- **`sync_once.py`** (1,991 lines) - Main sync script that handles lead creation and housekeeping
- **`simple_async_verification.py`** (1,066 lines) - Email verification system using Instantly API  
- **`drain_once.py`** - Dedicated script for removing finished leads from campaigns
- **`cold_email_notifier.py`** - Slack notification system via Echo API
- **`validate_environment.py`** - Pre-flight environment validation

### Shared Production Components
- **`shared/api_client.py`** - Centralized Instantly API operations
- **`shared/bigquery_utils.py`** - BigQuery connection and query management
- **`shared/models.py`** - Data models and type definitions
- **`shared_config.py`** - Centralized configuration management

### GitHub Actions Workflows (4 Total)
- **`.github/workflows/cold-email-sync.yml`** - Main sync every hour during business hours
- **`.github/workflows/drain-leads.yml`** - Lead removal every 2 hours
- **`.github/workflows/async-verification-poller.yml`** - Verification polling every hour
- **`.github/workflows/bigquery-diagnostics.yml`** - Manual troubleshooting workflow

### Configuration & Setup
- **`setup.py`** - Initial BigQuery table creation
- **`requirements.txt`** - Python dependencies
- **`config/config.py`** - Configuration management
- **`config/feature_flags.py`** - Feature flag management
- **`.env.example`** - Environment variable template

## 🧪 TEST & DEVELOPMENT FILES

### API Testing Scripts
- `test_instantly_api.py` - Instantly API endpoint testing
- `test_bigquery_connection.py` - BigQuery connectivity testing
- `test_api_endpoints.py` - Comprehensive API endpoint testing
- `test_lead_status_api.py` - Lead status API testing
- `test_verification.py` - Email verification testing
- `test_single_lead.py` - Individual lead processing testing
- `test_campaign_assignment.py` - Campaign assignment logic testing

### System Testing Scripts
- `test_drain_functionality.py` - Drain system testing
- `test_sync_inventory.py` - Inventory management testing
- `simple_drain_test.py` - Simple drain functionality test
- `test_github_secrets.py` - GitHub Actions secrets validation
- `debug_github_environment.py` - GitHub Actions environment debugging

### Verification System Testing
- `simple_verification_test.py` - Basic verification system test
- `spot_check_verification.py` - Verification spot checking
- `poll_verification_results.py` - Verification result polling test
- `test_verification_fixes.py` - Verification system fixes testing

## 🔍 INVESTIGATION & ANALYSIS FILES

### Data Reconciliation & Auditing
- `comprehensive_lead_audit.py` - Complete lead data audit
- `critical_data_reconciliation.py` - Data consistency checking
- `reconcile_inventory.py` - Inventory reconciliation
- `reconciliation_report_*.txt` - Generated reconciliation reports

### Orphaned Leads Analysis
- `comprehensive_orphan_finder.py` - Find orphaned leads
- `targeted_orphan_finder.py` - Targeted orphan detection
- `enhanced_orphan_assessment.py` - Advanced orphan analysis
- `enhanced_orphan_assignment.py` - Orphan assignment logic
- `manage_orphaned_leads.py` - Orphan management utilities

### System Analysis Scripts
- `analyze_drain_issue.py` - Drain functionality analysis
- `analyze_drain_logic.py` - Drain logic analysis
- `analyze_inventory_discrepancy.py` - Inventory issue analysis
- `analyze_lead_timing.py` - Lead timing analysis
- `investigate_completed_leads.py` - Completed leads investigation
- `investigate_orphaned_leads.py` - Orphaned leads investigation
- `investigate_auto_reply.py` - Auto-reply detection investigation

### Quick Diagnostic Scripts
- `quick_orphan_count.py` - Fast orphan counting
- `quick_audit_sample.py` - Sample auditing
- `quick_scope_check.py` - Scope validation
- `quick_count.py` - General counting utilities
- `quick_orphan_diagnosis.py` - Fast orphan diagnosis

## 🛠️ UTILITY & MAINTENANCE FILES

### Database Schema Updates
- `update_schema_for_verification.py` - Email verification schema updates
- `update_verification_schema.py` - Verification table schema updates
- `fix_credits_schema.py` - Credits schema fixes
- `add_drain_timestamp_column.py` - Drain timestamp column addition
- `add_deletion_fields.py` - Deletion tracking fields

### Data Management Scripts
- `create_dnc_table.py` - Do Not Contact table creation
- `reset_lead_tracking.py` - Lead tracking reset utilities
- `clear_instantly_leads.py` - Clear Instantly inventory
- `delete_test_leads.py` - Test data cleanup
- `batch_orphan_cleanup.py` - Bulk orphan cleanup

### System Maintenance
- `check_bigquery_permissions.py` - Permission validation
- `check_instantly_inventory.py` - Inventory checking
- `check_campaign_status.py` - Campaign status monitoring
- `check_lead_status.py` - Lead status checking
- `diagnose_bigquery.py` - BigQuery diagnostics
- `debug_sync.py` - Sync debugging utilities

### Manual Checks & Spot Testing
- `spot_check_lead.py` - Individual lead checking
- `manual_email_check.py` - Manual email validation
- `examine_lead_data.py` - Lead data examination
- `final_diagnosis.py` - Final system diagnosis

## 📊 MONITORING & LOGGING FILES

### Log Analysis
- `send_logs.py` - Log transmission for main sync
- `send_logs_drain.py` - Log transmission for drain operations
- `analyze_log_counting.py` - Log analysis utilities

### Performance Analysis
- `count_all_pages.py` - Page counting utilities
- `validate_optimizations.py` - Optimization validation
- `run_sync_with_high_multiplier.sh` - High-performance sync testing

## 📋 DOCUMENTATION FILES

### System Documentation
- **`CLAUDE.md`** - Main project documentation (ACTIVE)
- `README.md` - Project overview
- `DEPLOYMENT.md` - Deployment instructions
- `DEPLOYMENT_SUMMARY.md` - Deployment summary

### Implementation Documentation
- `ASYNC_VERIFICATION_IMPLEMENTATION.md` - Verification system docs
- `VERIFICATION_IMPLEMENTATION_SUMMARY.md` - Verification summary
- `DRAIN_FUNCTIONALITY_IMPLEMENTED.md` - Drain system docs
- `PHASE_1_OPTIMIZATION_SUMMARY.md` - Optimization summary

### Analysis & Planning Documents
- `COLD_EMAIL_SYNC_ANALYSIS.md` - Sync system analysis
- `ANALYSIS_REPORT_OOO_PROBLEM.md` - Out-of-office issue analysis
- `SYSTEM_FLOW_DOCUMENTATION.md` - System flow documentation
- `COLD_EMAIL_AUTOMATION_PLAN.md` - Original automation plan

### Notification System Documentation
- `COLD_EMAIL_NOTIFICATION_SYSTEM_SPEC.md` - Notification system spec
- `NOTIFICATION_SYSTEM_SPEC.md` - General notification spec
- `ECHO_EXISTING_API_NOTIFICATION_PLAN.md` - Echo API integration plan
- `SLACK_NOTIFICATION_APPROACH.md` - Slack integration approach

### Technical Specifications
- `cold_email_pipeline_spec.md` - Pipeline specification
- `EMAIL_VERIFICATION_PLAN.md` - Verification planning

## 🧹 DEPRECATED/UNUSED FILES

### Legacy Scripts (Not in Active Use)
- `async_email_verification.py` - Old async verification (replaced by simple_async_verification.py)
- `email_validation.py` - Old email validation
- `simple_email_validation.py` - Old simple validation
- `notification_handler.py` - Old notification handler
- `shared_functions.py` - Legacy shared functions

### Old Testing Files
- `test_all_leads.py` - Legacy comprehensive testing
- `test_refactor.py` - Refactoring tests
- `test_fixed_campaign.py` - Campaign fix testing
- `test_api_and_multiplier.py` - API multiplier testing

### Archived Analysis
- `debug_inventory_v2.py` - Old inventory debugging
- `test_inventory_v2.py` - Old inventory testing
- `correct_inventory_v2.py` - Old inventory correction
- `discover_api_endpoints.py` - API endpoint discovery
- `enhanced_auto_reply_classification.py` - Enhanced auto-reply logic
- `updated_drain_classification.py` - Updated drain classification

### Backup Files
- `view_backup_v_ready_for_instantly_*.sql` - SQL view backups
- Various `.py` files with investigation or debugging purposes

## 📁 DIRECTORY STRUCTURE

```
/
├── .github/workflows/          # GitHub Actions (4 production workflows)
├── shared/                     # Shared production components
├── config/                     # Configuration management
├── scripts/                    # Utility scripts
├── test_env/                   # Test environment files
├── .serena/                    # Serena AI memory and cache
└── [Root files]               # Main scripts and documentation
```

## 🎯 KEY TAKEAWAYS

### Production System (11 files actively used)
1. **4 GitHub Actions workflows** running on schedule
2. **3 core Python scripts** (sync, verification, drain)
3. **3 shared components** for modularity
4. **1 notification system** for Slack integration

### Development & Testing (40+ files)
- Comprehensive test coverage for all major components
- API endpoint testing and validation
- System integration testing
- Performance and stress testing

### Investigation & Analysis (30+ files)  
- Data reconciliation and audit scripts
- Orphaned lead detection and management
- System performance analysis
- Issue investigation utilities

### Maintenance & Utilities (20+ files)
- Database schema management
- System monitoring and diagnostics
- Data cleanup and maintenance
- Manual verification tools

**Total:** 111+ tracked files with clear separation between production, testing, and investigation purposes.