# Email Verification Integration Plan - Instantly API

## 📋 Executive Summary

This document outlines the plan to integrate Instantly's email verification API into the existing cold email system. The verification step will be added to the lead processing pipeline to ensure only valid email addresses enter campaigns, improving deliverability and reducing bounce rates.

## 🎯 Objectives

1. **Reduce bounce rates** by verifying emails before campaign creation
2. **Improve sender reputation** by only sending to valid addresses
3. **Save resources** by not wasting sequences on invalid emails
4. **Track verification metrics** for optimization

## 🔄 Integration Overview

### Current Flow
```
Lead → Create in Campaign → Send Sequences → Handle Bounces
```

### New Flow with Verification
```
Lead → Verify Email → Valid? → Create in Campaign → Send Sequences
                    ↓
                  Invalid → Skip & Log
```

## 📡 Instantly Email Verification API

### Endpoints
- **POST** `/api/v2/email-verification` - Verify email address
- **GET** `/api/v2/email-verification/{email}` - Check verification status

### Request Format
```bash
curl -X POST https://api.instantly.ai/api/v2/email-verification \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com"}'
```

### Response Format
```json
{
  "status": "success",
  "email": "test@example.com",
  "verification_status": "valid",  // Use this field
  "catch_all": false,
  "credits": 999,
  "credits_used": 1
}
```

### Verification Statuses
- `valid` - Email is deliverable
- `invalid` - Email is not deliverable
- `accept_all` - Catch-all domain (risky but usually acceptable)
- `pending` - Still processing (check status endpoint)
- `unknown` - Could not determine validity

## 🏗️ Implementation Plan

### Phase 1: Database Schema Updates

Add verification tracking columns to existing `ops_inst_state` table:

```sql
ALTER TABLE `instant-ground-394115.email_analytics.ops_inst_state`
ADD COLUMN IF NOT EXISTS verification_status STRING,
ADD COLUMN IF NOT EXISTS verification_catch_all BOOLEAN,
ADD COLUMN IF NOT EXISTS verification_credits_used INT64,
ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP;
```

### Phase 2: Code Implementation

#### 1. Add Verification Function (sync_once.py)

```python
def verify_email(email: str) -> dict:
    """Verify email using Instantly.ai verification API."""
    try:
        # Skip verification in dry run mode
        if DRY_RUN:
            return {
                'email': email,
                'status': 'valid',
                'catch_all': False,
                'credits_used': 0
            }
        
        data = {'email': email}
        response = call_instantly_api('/api/v2/email-verification', 
                                    method='POST', 
                                    data=data)
        
        # Handle pending status with polling
        if response.get('verification_status') == 'pending':
            time.sleep(2)  # Wait before checking status
            response = call_instantly_api(f'/api/v2/email-verification/{email}', 
                                        method='GET')
        
        return {
            'email': email,
            'status': response.get('verification_status', 'unknown'),
            'catch_all': response.get('catch_all', False),
            'credits_used': response.get('credits_used', 1)
        }
    except Exception as e:
        logger.error(f"Email verification failed for {email}: {e}")
        log_dead_letter('verification', email, None, None, None, str(e))
        return {'email': email, 'status': 'error', 'error': str(e)}
```

#### 2. Modify Lead Processing Function

```python
def process_lead_batch(leads: List[Lead], campaign_id: str) -> int:
    """Process a batch of leads for a specific campaign with email verification."""
    if not leads:
        return 0
    
    logger.info(f"Processing batch of {len(leads)} leads for campaign {campaign_id}")
    
    # Verification phase
    verified_leads = []
    verification_results = []
    
    if VERIFY_EMAILS_BEFORE_CREATION:
        logger.info(f"Verifying {len(leads)} email addresses...")
        for lead in leads:
            verification = verify_email(lead.email)
            verification_results.append(verification)
            
            if verification['status'] in VERIFICATION_VALID_STATUSES:
                verified_leads.append(lead)
                logger.debug(f"✅ {lead.email} verified as {verification['status']}")
            else:
                logger.info(f"❌ Skipping {lead.email}: {verification['status']}")
        
        logger.info(f"Verified {len(verified_leads)}/{len(leads)} leads as valid")
    else:
        # Skip verification if disabled
        verified_leads = leads
    
    # Continue with existing lead creation process
    successful_ids = []
    
    # Process in smaller batches to respect rate limits
    for i in range(0, len(verified_leads), BATCH_SIZE):
        batch = verified_leads[i:i + BATCH_SIZE]
        batch_ids = []
        
        for lead in batch:
            lead_id = create_lead_in_instantly(lead, campaign_id)
            batch_ids.append(lead_id)
            time.sleep(0.5)  # Rate limiting between individual calls
        
        successful_ids.extend(batch_ids)
        
        # Update ops_state with verification results
        update_ops_state(batch, campaign_id, batch_ids, verification_results)
        
        if i + BATCH_SIZE < len(verified_leads):
            logger.info(f"Sleeping {BATCH_SLEEP_SECONDS}s between batches...")
            time.sleep(BATCH_SLEEP_SECONDS)
    
    successful_count = len([id for id in successful_ids if id])
    logger.info(f"Successfully processed {successful_count}/{len(verified_leads)} verified leads")
    return successful_count
```

#### 3. Update ops_state Function

```python
def update_ops_state(leads: List[Lead], campaign_id: str, lead_ids: List[str], 
                    verification_results: Optional[List[dict]] = None):
    """Update ops_inst_state with lead data and verification results."""
    
    rows_to_insert = []
    for i, lead in enumerate(leads):
        row = {
            'email': lead.email.lower(),
            'campaign_id': campaign_id,
            'status': 'added' if lead_ids[i] else 'failed',
            'instantly_lead_id': lead_ids[i] or None,
            'added_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }
        
        # Add verification data if available
        if verification_results:
            # Find matching verification result
            verification = next((v for v in verification_results 
                               if v['email'] == lead.email), None)
            if verification:
                row.update({
                    'verification_status': verification.get('status'),
                    'verification_catch_all': verification.get('catch_all'),
                    'verification_credits_used': verification.get('credits_used'),
                    'verified_at': datetime.utcnow().isoformat()
                })
        
        rows_to_insert.append(row)
    
    # Rest of existing function remains the same...
```

### Phase 3: Configuration Updates

Add new environment variables and constants:

```python
# Email verification settings
VERIFY_EMAILS_BEFORE_CREATION = os.getenv('VERIFY_EMAILS_BEFORE_CREATION', 'true').lower() == 'true'
VERIFICATION_VALID_STATUSES = ['valid', 'accept_all']  # Configurable valid statuses
VERIFICATION_TIMEOUT = int(os.getenv('VERIFICATION_TIMEOUT', '10'))  # Max wait for pending
```

### Phase 4: Monitoring & Reporting

Add verification metrics to housekeeping function:

```python
def housekeeping():
    """Generate summary report with verification metrics."""
    # Existing housekeeping code...
    
    # Add verification metrics
    verification_query = f'''
    SELECT 
        verification_status,
        COUNT(*) as count,
        SUM(verification_credits_used) as total_credits
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE verified_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    GROUP BY verification_status
    '''
    
    verification_stats = bq_client.query(verification_query).to_dataframe()
    
    logger.info("📊 Verification Stats (Last 24h):")
    for _, row in verification_stats.iterrows():
        logger.info(f"  - {row['verification_status']}: {row['count']} emails, {row['total_credits']} credits")
```

## 📊 Expected Impact & Metrics

### Performance Impact
- **Additional API calls**: 2x (verification + creation)
- **Processing time**: +0.5-2s per lead
- **Current run**: 100 leads in ~5 minutes
- **With verification**: 100 leads in ~7-8 minutes

### Quality Improvements
- **Expected invalid rate**: 5-15% of leads
- **Bounce reduction**: 80-90% fewer bounces
- **Campaign efficiency**: Higher engagement rates

### Cost Considerations
- **Verification cost**: ~$0.10-0.50 per 1000 emails
- **ROI**: Saved sequences and improved reputation
- **Monthly estimate**: ~$5-10 for 100 leads/30min

## 🚀 Rollout Plan

### Week 1: Development & Testing
- [ ] Update BigQuery schema
- [ ] Implement verification function
- [ ] Modify lead processing logic
- [ ] Test in dry-run mode

### Week 2: Pilot Testing
- [ ] Enable for 10% of leads
- [ ] Monitor verification results
- [ ] Track API usage and costs
- [ ] Validate improved bounce rates

### Week 3: Full Deployment
- [ ] Enable for all leads
- [ ] Update documentation
- [ ] Set up monitoring alerts
- [ ] Train team on new metrics

## ⚙️ Configuration Options

### Environment Variables
```bash
# Enable/disable verification
VERIFY_EMAILS_BEFORE_CREATION=true

# Accepted verification statuses
VERIFICATION_VALID_STATUSES=valid,accept_all

# Verification timeout (seconds)
VERIFICATION_TIMEOUT=10

# Skip verification for SMB (optional)
VERIFY_SMB_LEADS=true
VERIFY_MIDSIZE_LEADS=true
```

### GitHub Actions Workflow
No changes needed - verification is integrated into existing sync process.

## 🔍 Monitoring Queries

### Verification Success Rate
```sql
SELECT 
    DATE(verified_at) as date,
    verification_status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY DATE(verified_at)), 2) as percentage
FROM `instant-ground-394115.email_analytics.ops_inst_state`
WHERE verified_at IS NOT NULL
GROUP BY date, verification_status
ORDER BY date DESC, count DESC;
```

### Verification Cost Tracking
```sql
SELECT 
    DATE(verified_at) as date,
    SUM(verification_credits_used) as total_credits,
    COUNT(DISTINCT email) as emails_verified,
    ROUND(SUM(verification_credits_used) / COUNT(DISTINCT email), 2) as credits_per_email
FROM `instant-ground-394115.email_analytics.ops_inst_state`
WHERE verified_at IS NOT NULL
GROUP BY date
ORDER BY date DESC;
```

### Campaign Performance Comparison
```sql
-- Compare bounce rates before/after verification
SELECT 
    campaign_id,
    verification_status IS NOT NULL as has_verification,
    COUNT(*) as total_leads,
    SUM(CASE WHEN status = 'email_bounced' THEN 1 ELSE 0 END) as bounced,
    ROUND(SUM(CASE WHEN status = 'email_bounced' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as bounce_rate
FROM `instant-ground-394115.email_analytics.ops_inst_state`
GROUP BY campaign_id, has_verification;
```

## ✅ Success Criteria

1. **Bounce rate reduction**: < 2% (from current ~5-10%)
2. **Verification success**: > 85% of emails pass verification
3. **Performance impact**: < 100% increase in processing time
4. **Cost efficiency**: < $0.01 per verified lead
5. **System stability**: No increase in error rates

## 🚨 Rollback Plan

If issues arise, disable verification without code changes:

```bash
# Set environment variable
VERIFY_EMAILS_BEFORE_CREATION=false

# Or update GitHub Actions secret
# The system will immediately skip verification
```

---

## 📝 Summary

This email verification integration will significantly improve the quality of leads entering your Instantly campaigns. By verifying emails before creation, we prevent invalid addresses from consuming valuable sequence slots and protect sender reputation.

The implementation is designed to be:
- **Non-invasive**: Minimal changes to existing code
- **Configurable**: Easy to enable/disable/tune
- **Monitored**: Comprehensive metrics and logging
- **Cost-effective**: Only verify what's necessary

**Total implementation time**: 2-3 days  
**Expected bounce rate reduction**: 80-90%  
**Monthly cost estimate**: $5-10  
**ROI**: Improved deliverability and reputation