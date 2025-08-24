# Deployment Guide - Cold Email System

## 🚀 Quick Start Deployment

### Prerequisites
1. BigQuery credentials file at `config/secrets/bigquery-credentials.json`
2. Instantly API key
3. GitHub repository access

### Step 1: Initial Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Run setup to create BigQuery tables
python setup.py
```

### Step 2: Configure GitHub Secrets
In your GitHub repository settings > Secrets and variables > Actions, add:

**Required Secrets:**
- `INSTANTLY_API_KEY`: Your Instantly.ai API key
- `BIGQUERY_CREDENTIALS_JSON`: Contents of your BigQuery service account JSON file

### Step 3: Test Locally (Recommended)
```bash
# Test with dry run first
DRY_RUN=true python sync_once.py

# Test with small batch
TARGET_NEW_LEADS_PER_RUN=10 python sync_once.py
```

### Step 4: Enable GitHub Actions
The workflow is pre-configured to run:
- **Weekdays**: Every 30 minutes during business hours (9 AM - 6 PM EST)
- **Weekends**: Every 2 hours

Manual triggers available with options for:
- Dry run mode
- Custom lead count

## 🔧 Configuration

### Campaign IDs (Pre-configured)
- **SMB**: `8c46e0c9-c1f9-4201-a8d6-6221bafeada6`
- **Midsize**: `5ffbe8c3-dc0e-41e4-9999-48f00d2015df`

### Key Settings
- **SMB Threshold**: $1,000,000 (configurable in BigQuery)
- **Batch Size**: 50 leads per API batch (conservative)
- **Inventory Cap**: 24,000 leads max in Instantly
- **Cooldown Period**: 90 days between sequences

### Environment Variables
- `TARGET_NEW_LEADS_PER_RUN`: Default 100
- `INSTANTLY_CAP_GUARD`: Default 24,000  
- `BATCH_SIZE`: Default 50
- `BATCH_SLEEP_SECONDS`: Default 10
- `DRY_RUN`: Default false

## 📊 Monitoring

### BigQuery Tables Created
1. **ops_inst_state**: Current Instantly campaign status
2. **ops_lead_history**: 90-day cooldown tracking  
3. **ops_dead_letters**: Failed operation logs
4. **config**: Dynamic configuration values

### Views Created
- **v_ready_for_instantly**: Final filtered lead list

### GitHub Actions Logs
- Check Actions tab for execution logs
- Failed runs upload artifacts with detailed logs
- Timeout set to 15 minutes per run

## 🔍 Troubleshooting

### Common Issues

**1. Authentication Errors**
- Verify BigQuery credentials JSON is valid
- Check Instantly API key is correct and has proper permissions

**2. No Leads Being Processed**
- Check if `v_ready_for_instantly` view has data
- Verify leads aren't in 90-day cooldown
- Check inventory hasn't hit the 24K cap

**3. API Rate Limits**
- Script includes automatic backoff
- Batch size is conservative (50)
- 10-second sleep between batches

**4. Workflow Not Running**
- Check GitHub Actions are enabled
- Verify secrets are properly set
- Check cron schedule syntax

### Debug Queries

```sql
-- Check ready leads count
SELECT COUNT(*) FROM `instant-ground-394115.email_analytics.v_ready_for_instantly`;

-- Check current Instantly state  
SELECT status, COUNT(*) FROM `instant-ground-394115.email_analytics.ops_inst_state` 
GROUP BY status;

-- Check recent errors
SELECT * FROM `instant-ground-394115.email_analytics.ops_dead_letters` 
WHERE occurred_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
ORDER BY occurred_at DESC;

-- Check segmentation breakdown
SELECT sequence_target, COUNT(*) FROM `instant-ground-394115.email_analytics.v_ready_for_instantly`
GROUP BY sequence_target;
```

## 🔄 Operational Notes

### Normal Operation Flow
1. **Drain**: Remove completed/bounced/unsubscribed leads from Instantly
2. **Top-up**: Add new eligible leads to appropriate campaigns  
3. **Housekeeping**: Update metrics and perform cleanup

### Expected Behavior
- Unsubscribes automatically added to DNC list
- 90-day cooldown enforced automatically
- Inventory stays under 24K limit
- Equal distribution between SMB/Midsize based on $1M threshold

### Manual Operations
```bash
# Manual sync with custom settings
TARGET_NEW_LEADS_PER_RUN=200 python sync_once.py

# Dry run to check what would happen
DRY_RUN=true TARGET_NEW_LEADS_PER_RUN=50 python sync_once.py

# Check system status
python -c "from setup import *; setup_bigquery_tables()"
```

## ⚠️ Safety Features

- **DRY_RUN mode**: Test without making changes
- **Inventory cap**: Prevents exceeding Instantly limits  
- **Dead letter logging**: Captures all failures
- **Automatic retries**: Built-in exponential backoff
- **Rate limiting**: Respects API constraints

## 📈 Scaling

Current configuration handles:
- **~2,400 leads/day** (100 every 30 min × 24 runs)
- **~16,800 leads/week** during business hours
- **Safe margin** below 24K Instantly limit

To increase volume:
1. Increase `TARGET_NEW_LEADS_PER_RUN`
2. Decrease cron frequency 
3. Monitor rate limits and dead letters
4. Consider upgrading Instantly plan if needed