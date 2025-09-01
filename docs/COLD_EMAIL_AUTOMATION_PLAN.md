# Cold Email Automation - Final Implementation Plan

## Executive Summary

This plan implements a reliable, scalable cold email automation system that synchronizes leads between BigQuery and Instantly.ai. The approach prioritizes simplicity and reliability for a small startup team while maintaining the ability to scale.

### Key Outcomes
- **Volume**: ~2,000 sends/day once warmed up (68 inboxes × ~30/day)
- **Inventory**: Keep Instantly working set <25,000 leads
- **Deduplication**: Zero duplicate emailing within 90 days (email-level dedupe)
- **State Management**: Clear, idempotent state in BigQuery
- **Architecture**: Minimal moving parts in v1.0; add real-time updates in v1.1

## Phased Implementation

### v1.0 - Single Cron Reconciler (Ship First)
**Timeline: Week 1-2**

One GitHub Action runs `sync_once.py` on a schedule (every 30-60 minutes):

1. **Drain**: Pull Completed/Unsubscribed/Bounced from Instantly → Update BigQuery → Delete from Instantly
2. **Top-up**: Select eligible leads → Create/move into campaigns (SMB/Midsize)
3. **Housekeeping**: Enforce caps, rate-limit batches, send Slack/email summary

**Why this wins**: Smallest surface area, deterministic, easy to debug and rollback.

### v1.1 - Add Real-Time Status Updates
**Timeline: Week 3-4**

Add webhook processing while keeping cron as safety net:
- **Webhook path**: Instantly → Cloud Run (HTTP) → Pub/Sub → Subscriber updates BigQuery
- Provides low-latency updates while v1.0 cron handles reconciliation/drift

### v1.2 - Optional Future Enhancements
- Move top-up to Pub/Sub batches for parallelism
- Add weekly deep reconciliation (full Instantly export vs BigQuery)

## Data Architecture

### Existing BigQuery Tables

#### 1. Master Lead Source
**Table**: `instant-ground-394115.email_analytics.storeleads`

Key fields used:
- `email` (STRING) - Primary contact email
- `platform_domain` (STRING) - Domain for deduplication
- `merchant_name` (STRING) - Company name
- `estimated_sales_yearly` (INTEGER) - For SMB/Midsize segmentation
- `klaviyo_active` (BOOLEAN) - Must be TRUE for eligibility
- `state`, `country_code` - Location data
- `employee_count`, `product_count`, `avg_price` - Additional enrichment

#### 2. Do Not Contact (DNC) List
**Table**: `instant-ground-394115.email_analytics.dnc_list`

Key fields:
- `email` (STRING) - Email to suppress
- `domain` (STRING) - Domain to suppress
- `source` (STRING REQUIRED) - Where suppression came from
- `reason` (STRING) - Why suppressed
- `is_active` (BOOLEAN) - Whether suppression is active
- `added_date` (TIMESTAMP)

#### 3. Pre-filtered Eligible Leads
**View**: `instant-ground-394115.email_analytics.eligible_leads`

Current definition:
```sql
SELECT s.* 
FROM instant-ground-394115.email_analytics.storeleads s
LEFT JOIN instant-ground-394115.email_analytics.active_dnc d 
  ON (s.email = d.email OR s.platform_domain = d.domain)
WHERE d.email IS NULL 
  AND s.email IS NOT NULL 
  AND s.klaviyo_active = TRUE
```

### New Tables to Create

```sql
-- 1. Current state in Instantly (tracks what's actively in Instantly)
CREATE TABLE instant-ground-394115.email_analytics.ops_inst_state (
  email STRING NOT NULL,
  instantly_lead_id STRING,
  campaign_id STRING,
  status STRING, -- 'active', 'completed', 'bounced', 'unsubscribed'
  status_updated_at TIMESTAMP,
  first_added_at TIMESTAMP,
  last_seen_at TIMESTAMP,
  PRIMARY KEY (email) NOT ENFORCED
);

-- 2. Historical tracking (for 90-day cooldown logic)
CREATE TABLE instant-ground-394115.email_analytics.ops_lead_history (
  email STRING NOT NULL,
  sequence_name STRING,
  campaign_id STRING,
  status_final STRING,
  first_sent_at TIMESTAMP,
  last_sent_at TIMESTAMP,
  last_sequence_completed_at TIMESTAMP,
  attempt_num INT64
);
```

### Required Views and Tables

#### 1. Active DNC View (normalize and handle NULLs)
```sql
CREATE OR REPLACE VIEW `instant-ground-394115.email_analytics.active_dnc` AS
SELECT
  LOWER(email) AS email,
  LOWER(domain) AS domain,
  source,
  reason,
  added_date,
  COALESCE(is_active, TRUE) AS is_active  -- Treat NULL as active (safer)
FROM `instant-ground-394115.email_analytics.dnc_list`
WHERE COALESCE(is_active, TRUE);
```

#### 2. Configuration Table (for dynamic thresholds)
```sql
CREATE TABLE IF NOT EXISTS `instant-ground-394115.email_analytics.config` (
  key STRING NOT NULL,
  value_int INT64,
  value_string STRING,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (key) NOT ENFORCED
);

-- Seed with SMB threshold (confirmed: $1M cutoff)
INSERT INTO `instant-ground-394115.email_analytics.config` (key, value_int)
VALUES ('smb_sales_threshold', 1000000)
ON CONFLICT (key) DO NOTHING;
```

#### 3. Enhanced Eligibility View (fully self-contained)
```sql
CREATE OR REPLACE VIEW `instant-ground-394115.email_analytics.v_eligible_for_instantly` AS
WITH 
config AS (
  SELECT COALESCE(value_int, 1000000) AS smb_threshold
  FROM `instant-ground-394115.email_analytics.config`
  WHERE key = 'smb_sales_threshold'
  LIMIT 1
),
cooled AS (
  -- Leads that completed a sequence within 90 days
  SELECT LOWER(email) AS email
  FROM `instant-ground-394115.email_analytics.ops_lead_history`
  WHERE last_sequence_completed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
),
active AS (
  -- Leads currently active in Instantly
  SELECT LOWER(email) AS email
  FROM `instant-ground-394115.email_analytics.ops_inst_state`
  WHERE status = 'active'
),
base AS (
  SELECT
    LOWER(e.email) AS email,
    e.merchant_name,
    LOWER(e.platform_domain) AS platform_domain,
    e.state,
    e.country_code,
    e.estimated_sales_yearly,
    e.employee_count,
    e.product_count,
    e.avg_price
  FROM `instant-ground-394115.email_analytics.eligible_leads` e
  WHERE e.email IS NOT NULL
    AND e.email != ''
    -- Basic email validity check
    AND REGEXP_CONTAINS(LOWER(e.email), r'^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$')
    -- Exclude role accounts
    AND NOT REGEXP_CONTAINS(LOWER(e.email), r'^(info|support|help|sales|hello|admin|contact|team|noreply|no-reply)@')
)
SELECT
  b.*,
  CASE 
    WHEN b.estimated_sales_yearly IS NOT NULL 
         AND b.estimated_sales_yearly < c.smb_threshold
    THEN 'SMB' 
    ELSE 'Midsize' 
  END AS sequence_target
FROM base b
CROSS JOIN config c
LEFT JOIN cooled cool ON b.email = cool.email
LEFT JOIN active a ON b.email = a.email
WHERE cool.email IS NULL  -- Not in cooldown
  AND a.email IS NULL;    -- Not currently active
```

#### 4. Dead Letters Table (for failed operations)
```sql
CREATE TABLE IF NOT EXISTS `instant-ground-394115.email_analytics.ops_dead_letters` (
  id STRING NOT NULL DEFAULT GENERATE_UUID(),
  occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  phase STRING,  -- 'drain', 'topup', 'delete', 'move', 'create'
  email STRING,
  campaign_id STRING,
  payload JSON,
  http_status INT64,
  error_text STRING,
  retry_count INT64 DEFAULT 0
) PARTITION BY DATE(occurred_at);
```

#### 5. Idempotent History Updates
```sql
-- Example MERGE for updating history without duplicates
MERGE `instant-ground-394115.email_analytics.ops_lead_history` T
USING (
  SELECT 
    @email AS email, 
    @sequence AS sequence_name, 
    @campaign AS campaign_id,
    @final AS status_final, 
    @first AS first_sent_at, 
    @last AS last_sent_at,
    @completed AS last_sequence_completed_at, 
    @attempt AS attempt_num
) S
ON T.email = S.email
   AND T.sequence_name = S.sequence_name
   AND T.campaign_id = S.campaign_id
   AND ((T.last_sequence_completed_at IS NULL AND S.last_sequence_completed_at IS NULL)
        OR T.last_sequence_completed_at = S.last_sequence_completed_at)
WHEN NOT MATCHED THEN
  INSERT (email, sequence_name, campaign_id, status_final, first_sent_at, 
          last_sent_at, last_sequence_completed_at, attempt_num)
  VALUES (email, sequence_name, campaign_id, status_final, first_sent_at, 
          last_sent_at, last_sequence_completed_at, attempt_num);
```

## v1.0 Implementation Details

### Control Loop (`sync_once.py`)

```python
import re
import time
from tenacity import retry, stop_after_attempt, wait_exponential

# Core configuration from environment
TARGET_NEW_LEADS_PER_RUN = int(os.getenv('TARGET_NEW_LEADS_PER_RUN', '300'))
INSTANTLY_CAP_GUARD = int(os.getenv('INSTANTLY_CAP_GUARD', '24000'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
BATCH_SLEEP_SECONDS = int(os.getenv('BATCH_SLEEP_SECONDS', '10'))
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'

# BigQuery configuration
PROJECT_ID = "instant-ground-394115"
DATASET_ID = "email_analytics"

# Role account filter
ROLE_RE = re.compile(r'^(info|support|help|sales|hello|admin|contact|team|noreply|no-reply)@', re.I)

def sync_once():
    """Main orchestration function - always drain first"""
    drain_finished_leads()
    top_up_campaigns()
    housekeeping_and_alerts()

def drain_finished():
    """Remove completed/bounced/unsubscribed leads"""
    # 1. List leads with terminal status from Instantly
    # 2. Update ops_inst_state and ops_lead_history (using MERGE)
    # 3. For unsubscribed: INSERT into dnc_list
    # 4. Delete from Instantly to free inventory
    # 5. Log failures to ops_dead_letters

def top_up_campaigns():
    """Add new eligible leads to campaigns"""
    # 1. Check current inventory against cap
    # 2. Query v_eligible_for_instantly with limit
    # 3. Split proportionally between SMB/Midsize
    # 4. Batch create/move with exponential backoff
    # 5. Poll background jobs for moves
    # 6. Update ops_inst_state only after success

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=60))
def call_instantly_api(endpoint, method='GET', data=None):
    """Call Instantly API with automatic retry and backoff"""
    # Implementation with proper error handling
    pass

def write_unsubscribe_to_dnc(email):
    """Automatically add unsubscribes to DNC list"""
    query = """
    INSERT INTO `instant-ground-394115.email_analytics.dnc_list`
    (id, email, domain, source, reason, added_date, is_active)
    VALUES (
        GENERATE_UUID(), 
        @email, 
        SPLIT(@email, '@')[OFFSET(1)], 
        'instantly', 
        'unsubscribe', 
        CURRENT_TIMESTAMP(), 
        TRUE
    )
    """
    # Execute with BigQuery client
```

### GitHub Action Schedule (Hardened)

```yaml
name: Cold Email Sync
on:
  schedule:
    - cron: '*/30 * * * *'  # Every 30 minutes
  workflow_dispatch: {}      # Manual trigger

permissions:
  id-token: write           # For Workload Identity Federation
  contents: read

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      # GCP keyless auth via Workload Identity Federation
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.GCP_WIF_PROVIDER }}
          service_account: ${{ secrets.GCP_SA_EMAIL }}
      
      - uses: google-github-actions/setup-gcloud@v2
      
      - name: Install dependencies
        run: pip install google-cloud-bigquery requests tenacity
      
      - name: Run sync
        env:
          PROJECT_ID: instant-ground-394115
          DATASET_ID: email_analytics
          INSTANTLY_API_KEY: ${{ secrets.INSTANTLY_API_KEY }}
          SMB_CAMPAIGN_ID: '8c46e0c9-c1f9-4201-a8d6-6221bafeada6'
          MIDSIZE_CAMPAIGN_ID: '5ffbe8c3-dc0e-41e4-9999-48f00d2015df'
          TARGET_NEW_LEADS_PER_RUN: '300'
          INSTANTLY_CAP_GUARD: '24000'
          BATCH_SIZE: '100'
          BATCH_SLEEP_SECONDS: '10'
          DRY_RUN: 'false'  # Set to 'true' for first run
        run: python sync_once.py
```

### Safe Defaults & Configuration

- **Campaign flags in Instantly**:
  - `insert_unsubscribe_header`: true (Gmail/Yahoo compliance)
  - `stop_on_reply`: true
  - `stop_for_company`: false (revisit if needed)

- **Rate limits**:
  - 100 operations per batch
  - 10 second sleep between batches
  - Exponential backoff on 429 errors

## v1.1 Webhook Implementation

### Architecture
1. **Cloud Run Service**: Receives webhooks, validates, publishes to Pub/Sub
2. **Pub/Sub Topic**: `instantly.events` for durable message delivery
3. **Subscriber Function**: Processes events, updates BigQuery, deletes from Instantly
4. **Dead Letter Queue**: Handles poison messages

### Security
- Shared secret validation on webhook endpoint
- Workload Identity Federation for GitHub → GCP auth
- No long-lived credentials

## Monitoring & Alerts

### Key Metrics
- **Bounce rate**: Warn at 3%, critical at 5% (last 24h)
- **Inventory**: Warn at 24,500 leads (approaching cap)
- **Pipeline health**: Alert if no leads created in 24h
- **Webhook health** (v1.1): DLQ depth, 4xx/5xx rates

### Run Summary (Every Execution)
```
[2024-01-15 14:30] Sync complete: 
Created: 300 | Moved: 12 | Drained: 287 | Errors: 0 | 
Inventory: 18,432/25,000 | Bounce: 1.2%
```

## Pre-Launch Testing

1. **Dry run**: 200 leads end-to-end
2. **Conflict handling**: Test duplicate prevention
3. **Cap guard**: Verify stops at 24,000 leads
4. **Suppression**: Confirm exclusion logic works
5. **Cooldown**: Test 90-day re-eligibility
6. **Rate limiting**: Force 429 errors, verify backoff
7. **Webhook flow** (v1.1): Synthetic events → state updates

## Implementation Checklist

### Milestone 1: v1.0 Foundation (Week 1)
- [ ] Create new BigQuery tables:
  - [ ] ops_inst_state (current Instantly state)
  - [ ] ops_lead_history (90-day cooldown tracking)
  - [ ] ops_dead_letters (failed operations)
  - [ ] config (for dynamic thresholds)
- [ ] Create/update views:
  - [ ] active_dnc (normalized with NULL handling)
  - [ ] v_eligible_for_instantly (with all filters)
- [ ] Set up GitHub Actions:
  - [ ] Configure Workload Identity Federation
  - [ ] Add secrets: GCP_WIF_PROVIDER, GCP_SA_EMAIL, INSTANTLY_API_KEY
  - [ ] Add vars: SMB_CAMPAIGN_ID, MIDSIZE_CAMPAIGN_ID
- [ ] Configure Instantly campaigns:
  - [ ] Enable insert_unsubscribe_header=true
  - [ ] Enable stop_on_reply=true
  - [ ] Set stop_for_company=false
- [ ] Deploy `sync_once.py` with DRY_RUN=true
- [ ] Test with 50 leads in dry-run mode
- [ ] Verify email normalization and role filtering

### Milestone 2: Scale Up (Week 2)
- [ ] Increase quota as warming progresses
- [ ] Add Slack/email notifications
- [ ] Monitor and tune performance

### Milestone 3: v1.1 Webhooks (Week 3-4)
- [ ] Deploy Cloud Run webhook handler
- [ ] Configure Pub/Sub and subscriber
- [ ] Enable Instantly webhooks
- [ ] Verify event flow

## Critical Success Factors

1. **Always drain first**: Prevents inventory buildup
2. **Keep the cron**: Even with webhooks, reconciliation is essential
3. **Delete on completion**: Keeps Instantly light and billing predictable
4. **Auto-update DNC**: Unsubscribes automatically added to suppression
5. **Normalize everything**: Always LOWER() emails and domains
6. **Idempotent operations**: Use MERGE for history, handle retries gracefully
7. **Dead letter tracking**: Never lose failed operations
8. **Compliance first**: List-unsubscribe headers from day one
9. **Role account filtering**: Built into eligibility view
10. **Configurable thresholds**: SMB cutoff in config table, not hardcoded

## Configuration Summary (CONFIRMED)

✅ **Campaign IDs**:
- SMB Campaign: `8c46e0c9-c1f9-4201-a8d6-6221bafeada6`
- Midsize Campaign: `5ffbe8c3-dc0e-41e4-9999-48f00d2015df`

✅ **SMB Threshold**: $1,000,000 (confirmed cutoff)

✅ **Alerts**: No alerts for v1.0 (confirmed)

✅ **Warmup Capacity**: 68 inboxes × 20 sends/day while warming

✅ **Data Source**: `instant-ground-394115.email_analytics.eligible_leads`

✅ **Deduplication**: Email-only (no domain-level blocking)

## Final Questions Before Implementation

### 1. **GitHub ↔ GCP Authentication** (REQUIRED)
Do you have Workload Identity Federation set up between your GitHub repo and GCP?
- [ ] **Yes, WIF is configured** (provide WIF provider + service account email)
- [ ] **No, please help me set this up** (I'll provide exact gcloud commands)
- [ ] **Use service account JSON key instead** (less secure but faster to start)

### 2. **DNC List Semantics** (CLARIFICATION)
How should we handle NULL values in `dnc_list.is_active`?
- [ ] **Treat NULL as active** (safer - exclude these emails)
- [ ] **Only is_active = TRUE counts as active** (stricter)

### 3. **First Run Safety Check**
Before we deploy, should we:
- [ ] **Start with DRY_RUN=true** to verify mappings (recommended)
- [ ] **Go live immediately** with small batch (10-20 leads)

### 4. **Current Eligible Lead Count** (OPTIONAL)
Run this to see our potential volume:
```sql
SELECT COUNT(*) as total_eligible_leads 
FROM `instant-ground-394115.email_analytics.eligible_leads`
```
Result: `_____________` (helps us validate initial batch sizes)

## Ready to Deploy Checklist

Once you answer the 3 required questions above, we have everything needed to:

1. ✅ Create BigQuery tables and views (5 mins)
2. ✅ Deploy sync_once.py with confirmed campaign IDs (10 mins) 
3. ✅ Set up GitHub Action with your auth method (15 mins)
4. ✅ Test with dry run or small live batch (5 mins)
5. ✅ Scale to full 300 leads/run capacity

**The plan is complete and ready for implementation.** Just need your authentication preference and DNC handling preference!

---

*This plan prioritizes reliability and simplicity while maintaining flexibility for future enhancements. Start with v1.0 to ship quickly, then layer on real-time capabilities as needed.*