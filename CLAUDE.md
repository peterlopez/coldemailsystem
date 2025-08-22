# Cold Email Pipeline Implementation Guide

## Overview
This pipeline synchronizes lead data between BigQuery and Instantly.ai for automated cold email campaigns. The system handles lead import, campaign sequence triggering, completion tracking, and status updates.

## Core Functions
1. **Send leads from BigQuery to Instantly**
2. **Automatically trigger campaign sequences for each lead**
3. **Remove leads from Instantly when sequences complete**
4. **Track lead status in BigQuery (cold email sent: yes/no)**

## Technical Architecture

### API Integration
- **Instantly API Version**: V2 (required - V1 deprecating in 2025)
- **Authentication**: Bearer token
- **Key Endpoints**:
  - `POST /api/v2/leads` - Add leads to campaigns
  - `POST /api/v2/leads/subsequence/remove` - Remove from sequences
  - `GET /api/v2/campaigns/analytics` - Track email status

### BigQuery Schema

```sql
-- Main leads table
CREATE TABLE leads (
  lead_id STRING NOT NULL,
  email STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  company_name STRING,
  campaign_id STRING,
  added_to_instantly BOOLEAN DEFAULT FALSE,
  added_date TIMESTAMP,
  sequence_completed BOOLEAN DEFAULT FALSE,
  sequence_completed_date TIMESTAMP,
  last_email_status STRING,  -- 'sent', 'opened', 'clicked', 'replied'
  cold_email_sent BOOLEAN DEFAULT FALSE,
  updated_at TIMESTAMP
);

-- Campaign tracking table
CREATE TABLE campaign_status (
  campaign_id STRING NOT NULL,
  campaign_name STRING,
  total_leads INT64,
  leads_completed INT64,
  last_sync TIMESTAMP
);

-- Error logging table
CREATE TABLE pipeline_errors (
  error_id STRING,
  error_timestamp TIMESTAMP,
  error_type STRING,
  lead_id STRING,
  error_message STRING,
  retry_count INT64
);
```

### Pipeline Components

#### 1. Lead Export Function
- **Trigger**: Daily schedule or manual trigger
- **Process**:
  1. Query BigQuery for leads where `added_to_instantly = FALSE`
  2. Batch leads (max 100 per API call)
  3. Call Instantly API to add leads to campaign
  4. Update BigQuery with `added_to_instantly = TRUE` and timestamp

#### 2. Status Sync Function
- **Trigger**: Every 4 hours
- **Process**:
  1. Call Instantly analytics API for campaign data
  2. Match email addresses to BigQuery leads
  3. Update `last_email_status` and `cold_email_sent` fields
  4. Identify completed sequences

#### 3. Sequence Completion Handler
- **Trigger**: Daily
- **Process**:
  1. Query for leads with completed sequences
  2. Remove leads from Instantly using API
  3. Update BigQuery: `sequence_completed = TRUE` with timestamp

## Implementation Details

### Google Cloud Services
- **Cloud Functions**: For API integration logic
- **Cloud Scheduler**: For automated triggers
- **Secret Manager**: Store Instantly API key
- **Cloud Logging**: For monitoring and debugging

### Error Handling
- Implement exponential backoff for API retries
- Log all errors to `pipeline_errors` table
- Alert on repeated failures (>3 retries)

### Rate Limiting
- Instantly API limits: Check current limits in API docs
- Implement request queuing if needed
- Add delays between batch operations

## Development Phases

### Phase 1: Basic Integration (Week 1)
- Set up Cloud Function for lead export
- Implement Instantly API authentication
- Test with small batch of leads
- Basic error logging

### Phase 2: Status Tracking (Week 2)
- Implement analytics sync function
- Update BigQuery with email status
- Add completion detection logic

### Phase 3: Automation (Week 3)
- Set up Cloud Scheduler jobs
- Implement sequence removal
- Add monitoring and alerts

### Phase 4: Testing & Optimization (Week 4)
- Full pipeline testing
- Performance optimization
- Documentation updates

## Monitoring & Alerts

### Key Metrics
- Daily leads processed
- API success/failure rates
- Average time to sequence completion
- Error rates by type

### Alerts
- API authentication failures
- No leads processed in 24 hours
- Error rate > 5%
- Sequence completion sync failures

## Security Considerations
- API key rotation schedule (quarterly)
- Minimal IAM permissions for service accounts
- No PII in logs
- Encrypted connections only

## Maintenance Tasks
- Weekly: Review error logs
- Monthly: Check API usage against limits
- Quarterly: Update API key
- As needed: Update for Instantly API changes

## Known Limitations
- Instantly API V2 documentation may have gaps
- Webhook support unclear - using polling approach
- Batch size limits may require adjustment

## Future Enhancements (Not in MVP)
- Real-time webhook integration (if available)
- Advanced analytics dashboard
- Multi-campaign support
- A/B testing integration