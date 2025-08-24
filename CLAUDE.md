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

## Development Best Practices - Using Serena MCP

### Objective
Prefer Serena's symbolic tools (via MCP) for finding/editing code. Avoid reading entire files or regex/grep when a symbol-aware action exists. Keep context small, do edits safely, and verify with tests.

### Golden Rules

#### Always use Serena tools first
- `get_symbols_overview` → quick map of classes/functions in a file
- `find_symbol` → locate definitions by (partial) name
- `find_referencing_symbols` → find callsites/usages
- `insert_after_symbol`, `insert_before_symbol`, `replace_symbol_body` → edit precisely
- Do NOT read whole files unless needed. Ask for `get_symbols_overview` first, then read only targeted snippets

#### Stay project-aware
- If tools aren't responding, activate the project and (if first time) onboard
- Use the repo's venv interpreter for Python (so the LSP is alive)

#### Be frugal with tokens
- Don't dump large file bodies; prefer symbol lists and small diffs
- Summarize when you must review multiple files

#### Validate changes
- After edits, run tests/lint via shell commands
- Always run `black .` and `flake8 .` before committing

### Tooling Cheatsheet (MCP)

#### Activation & Onboarding (first time per repo)
- "Check if onboarding was performed; if not, perform it."

#### Discover & Navigate
- "Give me a symbols overview of path/to/file.py."
- "Find symbols containing EmailSender (any kind)."
- "Find all references to send_message."
- "List files matching *email* under src/ (don't read bodies)."

#### Edit Safely
- "Insert the following after the definition of EmailSender: …code…"
- "Replace the full body of function build_payload with: …code…"
- "Insert before the definition of main a new import block: …code…"

#### Verify
- "Run tests with pytest -q and show the summary."
- "Show me the diff for files you changed."

### Policy & Style for Edits
- **Symbol-first editing**: Prefer `replace_symbol_body` / `insert_*_symbol` over line-based edits
- **Small, reversible steps**: Make one logically complete change, then verify
- **No blind refactors**: When renaming/moving symbols, gather references first
- **Respect excludes**: Don't traverse node_modules, .git, build dirs, or .serena/cache

### When Things Break
- **Tools missing?**: "Activate the project at <ABSOLUTE_PATH>."
- **LSP can't find Python?**: Use Python at `/Users/peterlopez/Documents/Cold Email System/venv/bin/python` for the LSP
- **Too much context?**: "Summarize changes so far." → then continue in a fresh turn

### Examples

#### Add a parameter to a function and update callsites
1. `find_symbol "EmailSender"`
2. `find_symbol "send"` (limit to class EmailSender)
3. `replace_symbol_body "EmailSender.send"` with updated signature & logic
4. `find_referencing_symbols "EmailSender.send"` and show exact callsites to adjust
5. Provide targeted patches for each callsite (no full-file reads)
6. Run tests

#### Safely inject logging before main
1. `find_symbol "main"`
2. `insert_before_symbol "main"` with:
   ```python
   import logging
   logging.basicConfig(level=logging.INFO)
   ```
3. Provide a snippet of the exact surrounding code to verify placement

### Do/Don't Quicklist
- ✅ Do: `get_symbols_overview` → `find_symbol` → precise edit
- ✅ Do: confirm placement with a 5–20 line context excerpt (not the whole file)
- ✅ Do: summarize reasoning & next steps after each change
- ❌ Don't: open large files in full
- ❌ Don't: use grep-like scans when a symbol tool exists
- ❌ Don't: perform multi-file edits without listing affected symbols/callsites first