# 🔄 **Cold Email System: Complete Lead Journey**

Let me trace exactly how **one lead** flows through our entire automated system:

## **📍 Starting Point: BigQuery Lead Pool**
Our lead starts in the `storeleads` table in BigQuery:
```sql
-- Lead example: jane@techstartup.com
email: 'jane@techstartup.com'
company: 'Tech Startup Inc'
domain: 'techstartup.myshopify.com'
revenue: 750000  -- This determines SMB vs Midsize
klaviyo_installed_at: '2025-08-20'  -- Recent = higher priority
```

## **🎯 Phase 1: Lead Selection (Every 30 Minutes)**

### **Step 1: Eligibility Check via `v_ready_for_instantly` View**
Our GitHub Actions workflow runs every 30 minutes and queries this smart view:
```sql
SELECT email, company, domain, revenue, klaviyo_installed_at
FROM v_ready_for_instantly  
WHERE email NOT IN (
  -- ❌ Exclude: Currently active in campaigns
  SELECT email FROM ops_inst_state WHERE status IN ('active', 'in_progress')
)
AND email NOT IN (
  -- ❌ Exclude: 90-day cooldown (completed sequence recently)  
  SELECT email FROM ops_lead_history 
  WHERE completed_at > CURRENT_TIMESTAMP() - INTERVAL 90 DAY
)
AND email NOT IN (
  -- ❌ Exclude: DNC list (11,726 permanent unsubscribes)
  SELECT email FROM dnc_list
)
ORDER BY klaviyo_installed_at DESC  -- 🔥 Most recent Klaviyo installs first
LIMIT 100  -- Target leads per run
```

**✅ Our lead qualifies:** Recent Klaviyo install, not in DNC, not recently contacted.

### **Step 2: Smart Segmentation**
```python
if lead.revenue < 1000000:
    campaign_id = "8c46e0c9-c1f9-4201-a8d6-6221bafeada6"  # SMB Campaign
    sequence_target = "SMB"
else:
    campaign_id = "5ffbe8c3-dc0e-41e4-9999-48f00d2015df"  # Midsize Campaign  
    sequence_target = "Midsize"
```

**🎯 Our lead:** Revenue = $750K → **SMB Campaign**

### **Step 3: Email Verification (NEW)**
Before creating the lead in Instantly, we verify the email address:

```python
# Call Instantly's verification API
verification_response = POST https://api.instantly.ai/api/v2/email-verification
{
    'email': 'jane@techstartup.com'
}

# Response example
{
    'email': 'jane@techstartup.com',
    'verification_status': 'valid',  # ✅ PASSED
    'catch_all': false,
    'credits_used': 0.25,
    'credits': 60187.50
}
```

**🔍 Verification Logic:**
- ✅ **VALID emails:** Proceed to campaign creation
- ✅ **ACCEPT_ALL emails:** Domain accepts all emails (risky but allowed)  
- ❌ **INVALID emails:** Skip and log (protects sender reputation)
- ❌ **UNKNOWN emails:** Skip and log (cannot determine validity)

**🎯 Our lead:** `verification_status: 'valid'` → **PROCEEDS TO CAMPAIGN** ✅

## **🚀 Phase 2: Lead Creation in Instantly.ai**

### **Step 4: API Call to Instantly** (Only for Verified Emails)
```python
instantly_payload = {
    'email': 'jane@techstartup.com',
    'campaign': '8c46e0c9-c1f9-4201-a8d6-6221bafeada6',  # SMB Campaign
    'first_name': '',
    'last_name': '',
    'company_name': 'Tech Startup Inc',
    'custom_variables': {
        'company': 'Tech Startup Inc',
        'domain': 'techstartup.myshopify.com', 
        'location': 'California',
        'country': 'US'
    }
}

POST https://api.instantly.ai/api/v2/leads
```

**✅ Result:** Lead created in Instantly with unique `instantly_lead_id`

### **Step 5: BigQuery Tracking with Verification Data (ops_inst_state)**
```sql
INSERT INTO ops_inst_state (
  email, campaign_id, status, instantly_lead_id, added_at, updated_at,
  verification_status, verification_catch_all, verification_credits_used, verified_at
) VALUES (
  'jane@techstartup.com',
  '8c46e0c9-c1f9-4201-a8d6-6221bafeada6',
  'active',
  'inst_lead_xyz123',  -- Instantly's internal ID
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  'valid',           -- ✅ Verification result
  false,             -- Not catch-all domain  
  0.25,              -- Credits consumed
  CURRENT_TIMESTAMP() -- When verified
)
```

**📊 Current State:**
- `ops_inst_state` table now tracks our lead as **"active"** with full verification data
- ✅ **Email verified as valid** - protecting sender reputation  
- 💰 **0.25 credits used** - cost tracking for verification
- Lead is live in SMB campaign with 68 mailboxes ready to send

## **📧 Phase 3: Email Sequence Execution (Instantly.ai)**

### **Step 6: Instantly Takes Over**
Our lead sits in the SMB campaign queue. Instantly's algorithm:

**Campaign Schedule Check:**
- ✅ Monday 9:12 AM ET = Within sending window (9 AM - 6 PM)
- ✅ Active days: Monday-Sunday
- ✅ 68 mailboxes available
- ✅ Daily limit: 4,000 emails

**Mailbox Selection:**
Instantly picks one of our 68 mailboxes (e.g., `rohan.s@getrippleaiagency.com`)

**Email Composition:**
```
From: rohan.s@getrippleaiagency.com
To: jane@techstartup.com
Subject: [Sequence Step 1 Subject]

Hi {{first_name}},

I noticed {{company}} is using Shopify at {{domain}}...

[Sequence content with custom variables filled in]

Best,
Rohan
```

### **Step 7: Send + Response Tracking**
- **Email sent** → Status in Instantly becomes "contacted"
- **Email opened** → Tracking pixel fires
- **Link clicked** → Click tracking
- **Reply received** → Status becomes "replied" 
- **Unsubscribe clicked** → Status becomes "unsubscribed"

## **🔄 Phase 4: Status Updates (Every 30 Minutes)**

### **Step 8: DRAIN Phase - Intelligent Lead Classification**
Every 30 minutes, our system queries Instantly for status updates using the **working API endpoint**:

```python
# DISCOVERED ENDPOINT: POST /api/v2/leads/list (pagination-enabled)
response = POST https://api.instantly.ai/api/v2/leads/list
{
    "campaign_id": "8c46e0c9-c1f9-4201-a8d6-6221bafeada6",
    "offset": 0,
    "limit": 100  # Process in batches
}

# SMART CLASSIFICATION LOGIC (New Implementation)
for lead in response['data']:
    classification = classify_lead_for_drain(lead)
    
    # Status 3 = Processed/Finished leads
    if lead.status == 3:
        if lead.email_reply_count > 0:
            # ✅ DRAIN: Replied (trusts Instantly's OOO filtering)
            drain_reason = 'replied'
        else:
            # ✅ DRAIN: Sequence completed without replies
            drain_reason = 'completed'
    
    # ESP Code analysis for bounces
    elif lead.esp_code in [550, 551, 553]:  # Hard bounces
        if days_since_created >= 7:  # 7-day grace period
            # ✅ DRAIN: Hard bounce after grace period
            drain_reason = 'bounced_hard'
        else:
            # ⏸️ KEEP: Recent hard bounce, within grace period
            continue
    
    elif lead.esp_code in [421, 450, 451]:  # Soft bounces
        # ⏸️ KEEP: Soft bounces for retry
        continue
    
    # Unsubscribe detection
    elif 'unsubscribed' in str(lead.status_text).lower():
        # ✅ DRAIN: Unsubscribed
        drain_reason = 'unsubscribed'
    
    # Stale active leads (90+ days old)
    elif lead.status == 1 and days_since_created >= 90:
        # ✅ DRAIN: Stuck active lead
        drain_reason = 'stale_active'
    
    else:
        # ⏸️ KEEP: Active leads and recent leads
        continue
    
    # EXECUTE DRAIN
    if drain_reason:
        # Remove from Instantly to free inventory
        DELETE /api/v2/leads/{lead.id}
        
        # Enhanced BigQuery tracking with drain reasons
        UPDATE ops_inst_state 
        SET status = drain_reason, updated_at = CURRENT_TIMESTAMP()
        WHERE email = lead.email
        
        # 90-day cooldown for completed/replied leads
        if drain_reason in ['completed', 'replied']:
            INSERT INTO ops_lead_history (
                email, campaign_id, sequence_name, status_final,
                completed_at, attempt_num
            ) VALUES (
                lead.email, lead.campaign_id, 'SMB', drain_reason,
                CURRENT_TIMESTAMP(), 1
            )
        
        # Permanent DNC for unsubscribes
        if drain_reason == 'unsubscribed':
            INSERT INTO dnc_list (
                email, domain, source, reason, added_date, is_active
            ) VALUES (
                lead.email, SPLIT(lead.email, '@')[1], 
                'instantly_drain', 'unsubscribe_via_api',
                CURRENT_TIMESTAMP(), TRUE
            )
```

### **Key Improvements in New Drain System:**
- ✅ **Working API Integration:** Uses discovered POST `/api/v2/leads/list` endpoint
- 🧠 **Smart Classification:** 8 different lead scenarios with approved logic
- 🛡️ **OOO Handling:** Trusts Instantly's built-in detection (stop_on_auto_reply=false)
- ⏰ **Grace Periods:** 7-day grace for hard bounces, keeps soft bounces for retry
- 📄 **Pagination:** Handles campaigns with 100+ leads efficiently
- 🔒 **Conservative Errors:** Keeps leads safe when classification fails

## **📊 Phase 5: Lead Lifecycle Outcomes (Enhanced Classification)**

### **Scenario A: Lead Replies ✅**
```sql
-- DETECTED: Status 3 + email_reply_count > 0
-- ops_inst_state: status = 'replied' 
-- ops_lead_history: Records successful engagement + 90-day cooldown
-- dnc_list: No addition (positive response)
-- Result: Lead drained from Instantly, QUALIFIED LEAD for sales team
-- Note: Trusts Instantly's OOO filtering (stop_on_auto_reply=false)
```

### **Scenario B: Lead Unsubscribes ❌**  
```sql
-- DETECTED: 'unsubscribed' in status_text
-- ops_inst_state: status = 'unsubscribed'
-- ops_lead_history: No cooldown record (permanent removal)
-- dnc_list: PERMANENT addition with source='instantly_drain'
-- Result: Lead drained, never contacted again (compliance)
```

### **Scenario C: Sequence Completes (No Response) 📤**
```sql  
-- DETECTED: Status 3 + email_reply_count = 0
-- ops_inst_state: status = 'completed'
-- ops_lead_history: Records sequence completion + 90-day cooldown
-- Result: Lead drained from Instantly, eligible for re-contact after 90 days
```

### **Scenario D: Hard Bounce (After Grace Period) ⚠️**
```sql
-- DETECTED: ESP codes 550/551/553 + >7 days old
-- ops_inst_state: status = 'bounced_hard'
-- Result: Lead drained to free inventory, no DNC (may retry later)
-- Grace Period: Gives 7 days for temporary delivery issues
```

### **Scenario E: Soft Bounce (Retry Allowed) 🔄**
```sql
-- DETECTED: ESP codes 421/450/451
-- ops_inst_state: status remains 'active'
-- Result: Lead KEPT in campaign for retry (not drained)
-- Logic: Soft bounces are temporary, lead should retry delivery
```

### **Scenario F: Stale Active Lead (90+ Days) 🕐**
```sql
-- DETECTED: Status 1 (active) + created_at > 90 days ago
-- ops_inst_state: status = 'stale_active'
-- Result: Lead drained (likely stuck in system)
-- Purpose: Prevents old leads from consuming inventory indefinitely
```

## **🔄 Phase 6: Continuous Cycle**

### **TOP-UP Phase (Same 30-min cycle)**
With lead inventory freed by DRAIN:
```python
current_inventory = count_active_leads_in_instantly()
capacity_needed = (68_mailboxes * 10_emails_per_day * 3.5_multiplier) - current_inventory

if capacity_needed > 0:
    new_leads = get_next_eligible_leads(limit=min(100, capacity_needed))
    create_leads_in_instantly(new_leads)
    update_bigquery_tracking(new_leads)
```

## **📈 System Intelligence**

### **90-Day Cooldown Protection:**
```sql
-- Lead can only be re-contacted after 90 days
WHERE completed_at < CURRENT_TIMESTAMP() - INTERVAL 90 DAY
```

### **DNC Compliance:**
```sql  
-- 11,726 permanent unsubscribes protected
AND email NOT IN (SELECT email FROM dnc_list)
```

### **Smart Capacity Management:**
```python
# Accounts for sequence timing gaps with 3.5x multiplier
daily_capacity = 68_mailboxes * 10_emails * 3.5_multiplier = ~2,380 leads
```

### **Priority Queue:**
```sql
ORDER BY klaviyo_installed_at DESC  -- Most recent Klaviyo installs first
```

### **Email Verification Quality Gate:**
```python
# Only verified emails enter campaigns (NEW)
if verification_status in ['valid', 'accept_all']:
    create_lead_in_instantly(lead)
    credit_cost += 0.25  # Track verification expense
else:
    skip_lead_and_log(lead, verification_status)
```

---

## **🎯 Current State of Our Lead**

Right now, `jane@techstartup.com` is sitting in the SMB campaign as **"active"** status with **verified email**, waiting for Instantly to send the first email in her sequence. Her email passed verification (cost: 0.25 credits), ensuring deliverability and protecting our sender reputation. Within the next few hours, she should receive her first outreach email, and her journey through our automated pipeline will continue based on her response (or lack thereof).

**The entire system runs automatically every 30 minutes, processing ~100 leads per cycle, with full compliance and tracking at every step.**

## **📋 System Tables Reference**

### **BigQuery Tables:**
- **`storeleads`** - Master lead database with company data
- **`ops_inst_state`** - Real-time campaign status tracking **+ email verification data**
  - New columns: `verification_status`, `verification_catch_all`, `verification_credits_used`, `verified_at`
- **`ops_lead_history`** - Historical archive for 90-day cooldown
- **`ops_dead_letters`** - Error logging and debugging (includes verification errors)
- **`dnc_list`** - Permanent unsubscribe protection (11,726 entries)
- **`config`** - Dynamic system configuration
- **`v_ready_for_instantly`** - Smart filtering view combining all logic

### **Instantly.ai Campaigns:**
- **SMB Campaign:** `8c46e0c9-c1f9-4201-a8d6-6221bafeada6` (Revenue < $1M)
- **Midsize Campaign:** `5ffbe8c3-dc0e-41e4-9999-48f00d2015df` (Revenue ≥ $1M)

### **GitHub Actions Workflow:**
- **Schedule:** Every 30 minutes (9 AM - 6 PM EST weekdays), Every 2 hours weekends
- **Manual triggers:** Available with custom parameters
- **Automatic logging:** Via dpaste.org for remote access

---

## **🔥 Email Verification Impact**

### **Quality Improvement (NEW):**
- **✅ Only verified emails** enter campaigns (85-95% pass rate expected)
- **❌ Invalid emails filtered** before wasting campaign slots
- **💰 Cost:** ~$0.025 per 100 leads verified
- **🛡️ Reputation protection:** Prevents bounces and spam flags

### **Expected Results:**
- **Bounce rate:** Down from ~5-10% to <2%
- **Deliverability:** Improved sender reputation
- **Efficiency:** Campaign slots used only for valid emails
- **ROI:** Better engagement rates from quality filtering

**Email verification is now ACTIVE and protecting your campaigns on every run!** 🚀