# Cold Email System - Codebase Overview

## System Status: PRODUCTION READY ✅
- **292,561 eligible leads** ready for processing
- **4 automated GitHub Actions workflows** running
- **Email verification system** active and filtering invalid emails
- **Dual workflow architecture** prevents API conflicts
- **Real-time Slack notifications** via Echo API integration

## Core Architecture

### Main Production Files (Root Directory)
1. **`sync_once.py`** (1,991 lines) - Main sync engine with dual workflow support
2. **`simple_async_verification.py`** (1,066 lines) - Email verification system
3. **`drain_once.py`** - Lead removal system with smart classification
4. **`cold_email_notifier.py`** - Slack notification integration

### Shared Components (`/shared/`)
- **`api_client.py`** - Centralized Instantly API operations
- **`bigquery_utils.py`** - BigQuery connection and query management
- **`models.py`** - Data models (Lead, InstantlyLead classes)

### GitHub Actions Workflows (`.github/workflows/`)
1. **`cold-email-sync.yml`** - Main sync (every hour, business hours)
2. **`drain-leads.yml`** - Lead removal (every 2 hours)
3. **`async-verification-poller.yml`** - Email verification polling (every 5 min)
4. **`bigquery-diagnostics.yml`** - Manual system diagnostics

## Key System Features

### Dual-Workflow Architecture
- **Main Sync Workflow**: TOP-UP (adds leads) + HOUSEKEEPING (updates tracking)
- **Dedicated Drain Workflow**: Removes finished leads using smart classification
- **Async Verification**: Independent email validation with DNC management

### Campaign Configuration
- **SMB Campaign**: `8c46e0c9-c1f9-4201-a8d6-6221bafeada6` (< $1M revenue)
- **Midsize Campaign**: `5ffbe8c3-dc0e-41e4-9999-48f00d2015df` (≥ $1M revenue)
- **Status**: Both campaigns ACTIVE but need sending schedule configuration

### Data Flow
1. **BigQuery** → eligible leads query (v_ready_for_instantly view)
2. **Lead Creation** → Instantly API with immediate campaign assignment
3. **Async Verification** → Email validation using Instantly API
4. **Drain Process** → Smart lead classification and removal
5. **Notifications** → Real-time Slack updates via Echo API

## Critical Tables (BigQuery)
- **`ops_inst_state`** - Current campaign state with verification data
- **`ops_lead_history`** - 90-day cooldown tracking
- **`ops_dead_letters`** - Error logging with retry attempts
- **`config`** - Dynamic system configuration
- **`v_ready_for_instantly`** - Smart filtering view

## System Safeguards
- **90-day cooldown** prevents lead fatigue
- **11,726 DNC entries** for compliance
- **24-hour verification guard** prevents duplicates
- **Graduated failure handling** (3-attempt retry before deletion)
- **Adaptive rate limiting** adjusts based on API performance
- **Parameterized SQL queries** prevent injection attacks

## Recent Enhancements (Phase 1-4)
- **Enhanced notifications** with detailed breakdowns
- **Fixed BigQuery bulk UPDATE syntax** with parameterized arrays
- **BigQuery-first drain discovery** with fallback
- **Smart batch size logging** for better visibility
- **Comprehensive error classification** and handling

## Verification System
- **Non-blocking architecture** - leads created immediately
- **Async polling** every 5 minutes during business hours
- **Credit tracking** and cost monitoring
- **DNC integration** for invalid emails
- **Idempotent deletion** using instantly_lead_id

## Testing & Validation
- **DRY_RUN mode** for safe testing
- **Environment validation** via validate_environment.py
- **Local testing scripts** available
- **Manual workflow triggers** with custom parameters
- **Comprehensive diagnostic queries** in CLAUDE.md

## Next Steps Required
⚠️ **CRITICAL**: Configure sending schedule in Instantly dashboard
- Set active days (Monday-Friday recommended)
- Set sending hours (9 AM - 6 PM EST)  
- Set timezone (America/New_York)

## Key Metrics
- **Processing capacity**: ~2,400 leads/day potential
- **Current inventory**: 6 leads (from testing)
- **Batch size**: 50 leads per API call
- **Rate limiting**: 0.5s-10s adaptive delays
- **API success rate**: Monitored via dead letters table