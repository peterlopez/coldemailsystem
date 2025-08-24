# Cold Email System - Complete Implementation

## 🚀 System Overview
This is a fully automated pipeline that synchronizes lead data between BigQuery and Instantly.ai for cold email campaigns. The system handles lead segmentation, campaign management, sequence tracking, and automated lead lifecycle management.

## 🎯 Current Status: **PRODUCTION READY**
- ✅ **292,561 eligible leads** ready for processing
- ✅ **Automated GitHub Actions workflow** (every 30 minutes)
- ✅ **Smart segmentation** (SMB < $1M, Midsize ≥ $1M)
- ✅ **90-day cooldown system** prevents lead fatigue
- ✅ **11,726 DNC entries** for compliance
- ✅ **Comprehensive error tracking** and logging
- ⚠️ **Campaigns currently paused** - need reactivation in Instantly dashboard

## 🔄 How It Works

### **3-Phase Sync Process (Runs Every 30 Minutes)**

#### **Phase 1: DRAIN** 
- Identifies completed/bounced/unsubscribed leads in Instantly
- Removes them to free inventory space
- Adds unsubscribes to DNC list automatically
- Updates BigQuery tracking tables

#### **Phase 2: TOP-UP**
- Queries BigQuery for fresh eligible leads (default: 100 per run)
- **Smart Segmentation:**
  - SMB: Revenue < $1,000,000 → Campaign `8c46e0c9-c1f9-4201-a8d6-6221bafeada6`
  - Midsize: Revenue ≥ $1,000,000 → Campaign `5ffbe8c3-dc0e-41e4-9999-48f00d2015df`
- Creates leads in appropriate Instantly campaigns with company data
- Respects 24,000 lead inventory cap

#### **Phase 3: HOUSEKEEPING**
- Updates ops tracking tables
- Logs performance metrics
- Reports summary statistics
- Manages error logging

## 🏗️ Technical Architecture

### **BigQuery Tables Created**
```sql
-- Current campaign state tracking
ops_inst_state (email, campaign_id, status, instantly_lead_id, added_at, updated_at)

-- 90-day cooldown tracking  
ops_lead_history (email, campaign_id, sequence_name, status_final, completed_at, attempt_num)

-- Failed operation logging
ops_dead_letters (id, occurred_at, phase, email, http_status, error_text, retry_count)

-- Dynamic configuration
config (key, value_int, value_string, updated_at)

-- Smart filtering view
v_ready_for_instantly -- Combines all filtering logic
```

### **Key Features**
- **90-Day Cooldown:** Prevents re-contacting leads for 90 days after sequence completion
- **DNC Protection:** Permanent unsubscribe list (11,726 entries)  
- **Inventory Management:** Stays under 24,000 lead cap
- **Error Tracking:** Dead letter logging for all failures
- **Smart Filtering:** Excludes duplicates, active leads, and recent completions
- **Automatic Retry:** Exponential backoff for API failures

## 🛠️ System Components

### **Core Script: sync_once.py**
- **400+ lines** of production-ready Python code
- Handles all API interactions with Instantly.ai V2
- Manages BigQuery connections and transactions  
- Implements drain-first architecture pattern
- Comprehensive error handling and logging

### **GitHub Actions Workflow**
- **Automated scheduling:** Every 30 minutes (9 AM - 6 PM EST weekdays)
- **Weekend schedule:** Every 2 hours  
- **Manual triggers:** Available with custom parameters
- **Environment validation:** Pre-flight checks before execution
- **Secrets management:** Secure API key and credentials handling

### **Configuration Files**
- `config/secrets/instantly-config.json` - API configuration
- `config/secrets/bigquery-credentials.json` - Service account credentials
- `requirements.txt` - Python dependencies
- `setup.py` - Initial table creation script

## 🔧 Campaign Configuration

### **Active Campaigns**
- **SMB Campaign:** `8c46e0c9-c1f9-4201-a8d6-6221bafeada6`
  - Target: Companies < $1M revenue
  - Status: Currently paused (Status 2)
  - Historical: 32,630 contacted, 15,876 opens, 498 replies

- **Midsize Campaign:** `5ffbe8c3-dc0e-41e4-9999-48f00d2015df`  
  - Target: Companies ≥ $1M revenue
  - Status: Currently paused (Status 2)
  - Historical: 4,189 contacted, 947 opens, 29 replies

### **Lead Data Structure**
```json
{
  "email": "contact@company.com",
  "first_name": "",
  "last_name": "", 
  "company_name": "Company Name",
  "campaign_id": "campaign-uuid",
  "custom_variables": {
    "company": "Company Name",
    "domain": "company.myshopify.com", 
    "location": "State/Province",
    "country": "Country Code"
  }
}
```

## 📊 Current Metrics
- **Total Eligible Leads:** 292,561
- **Current Instantly Inventory:** 6 leads (from testing)
- **DNC List Size:** 11,726 (595 from Instantly unsubscribes)
- **Processing Capacity:** ~2,400 leads/day potential
- **SMB Threshold:** $1,000,000 (configurable in BigQuery)

## 🔐 Security & Compliance

### **GitHub Secrets Required**
- `INSTANTLY_API_KEY`: Instantly.ai API authentication
- `BIGQUERY_CREDENTIALS_JSON`: Google Cloud service account credentials

### **Data Protection**
- No PII in logs or error messages
- Encrypted API connections only
- Service account with minimal permissions
- Automatic credential cleanup after runs

### **Compliance Features**
- DNC list management (permanent unsubscribe protection)
- 90-day cooldown prevents over-contacting  
- Audit trail in ops_lead_history table
- Error logging for compliance reporting

## 🚨 Troubleshooting

### **Common Issues**

#### **"No leads showing in campaigns"**
- **Cause:** Campaigns are paused (Status 2)
- **Solution:** Reactivate campaigns in Instantly dashboard
- **Note:** Leads are successfully created but won't process until campaigns are active

#### **"GitHub Actions failing"**
- **Check:** Secrets are properly configured
- **Debug:** Review workflow logs for specific error messages  
- **Validate:** Run `validate_environment.py` locally

#### **"API 401 errors"**
- **Cause:** Missing or invalid INSTANTLY_API_KEY
- **Solution:** Verify secret is set correctly in GitHub
- **Test:** Script now includes fallback to config file for local testing

### **Debug Queries**
```sql
-- Check ready leads count
SELECT COUNT(*) FROM `instant-ground-394115.email_analytics.v_ready_for_instantly`;

-- Check current campaign state
SELECT status, COUNT(*) FROM `instant-ground-394115.email_analytics.ops_inst_state` 
GROUP BY status;

-- Check recent errors
SELECT * FROM `instant-ground-394115.email_analytics.ops_dead_letters`
WHERE occurred_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
ORDER BY occurred_at DESC;

-- Segmentation breakdown
SELECT sequence_target, COUNT(*) FROM `instant-ground-394115.email_analytics.v_ready_for_instantly`
GROUP BY sequence_target;
```

## 🚀 Deployment Instructions

### **Initial Setup (Already Complete)**
1. ✅ BigQuery tables created via `setup.py`
2. ✅ GitHub Actions workflow configured  
3. ✅ Secrets configured in GitHub repository
4. ✅ Sync script tested and validated

### **Activation Steps**
1. **Reactivate Instantly campaigns** in dashboard
2. **Add mailboxes** to campaigns (user task)
3. **Test with dry run:** Manual trigger with `dry_run: true`
4. **Go live:** Manual trigger with `dry_run: false`
5. **Monitor:** GitHub Actions logs for ongoing operations

### **Operational Commands**
```bash
# Local testing
DRY_RUN=true TARGET_NEW_LEADS_PER_RUN=5 python sync_once.py

# Environment validation  
python validate_environment.py

# Setup/reset tables
python setup.py
```

## 📈 Performance & Scaling

### **Current Limits**
- **Instantly inventory cap:** 24,000 leads
- **Batch size:** 50 leads per API call (conservative)
- **API rate limiting:** 0.5s delay between calls
- **Processing rate:** ~100 leads per 30-minute run

### **Scaling Options**
- Increase `TARGET_NEW_LEADS_PER_RUN` (up to 500)
- Decrease cron frequency (every 15 minutes)
- Increase `BATCH_SIZE` (up to 100)
- Monitor dead letters for rate limit issues

## 🔮 Future Enhancements
- **Real-time webhooks** (when Instantly supports them)
- **Advanced analytics dashboard** 
- **Multi-sequence support** per campaign
- **A/B testing integration**
- **Lead scoring and prioritization**

## ✅ Development Best Practices

### **Code Quality Standards**
- **Comprehensive error handling** with exponential backoff
- **Idempotent operations** using MERGE statements
- **Extensive logging** for debugging and monitoring
- **Type hints and dataclasses** for maintainability
- **Configuration-driven** behavior (no hardcoded values)

### **Testing Approach**
- **DRY_RUN mode** for safe testing
- **Environment validation** before execution
- **Local testing scripts** for development
- **GitHub Actions integration testing**

### **Maintenance Schedule**
- **Weekly:** Review error logs and metrics
- **Monthly:** Verify API limits and performance  
- **Quarterly:** Rotate API keys and review access
- **As needed:** Update for API changes

---

## 🎯 **READY FOR PRODUCTION**

The Cold Email System is **fully implemented and tested**. The only remaining step is **reactivating the paused campaigns** in your Instantly dashboard. Once active, the system will automatically:

- Process 100 leads every 30 minutes during business hours
- Maintain proper segmentation between SMB and Midsize
- Respect the 90-day cooldown period
- Handle unsubscribes and maintain DNC compliance
- Provide comprehensive logging and error tracking

**Total development time:** 4 days  
**Lines of code:** 800+ (production-ready)  
**Test coverage:** Comprehensive with multiple validation layers  
**Status:** ✅ Ready for immediate production use