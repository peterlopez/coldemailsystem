# Cold Email System - Project Overview

## Purpose
This is a cold email pipeline implementation that synchronizes lead data between Google BigQuery and Instantly.ai for automated cold email campaigns.

## Core Functionality
1. Send leads from BigQuery to Instantly.ai
2. Automatically trigger campaign sequences for each lead
3. Remove leads from Instantly when sequences complete
4. Track lead status in BigQuery (cold email sent: yes/no)

## Tech Stack
- **Language**: Python 3.13.5
- **Cloud Platform**: Google Cloud Platform (GCP)
- **Database**: BigQuery
- **Email Platform**: Instantly.ai API v2
- **Infrastructure**: Cloud Functions, Cloud Scheduler, Secret Manager

## Project Structure
- `/config/` - Configuration management and secrets
  - `config.py` - Main configuration module
  - `/secrets/` - API credentials (instantly-config.json, bigquery-credentials.json)
- `/functions/` - Cloud Functions for pipeline components
- `/scripts/` - Utility scripts for testing
  - `test_config.py` - Configuration testing
  - `test_instantly_api.py` - Instantly API testing
- `/tests/` - Test suite
- `requirements.txt` - Python dependencies
- `CLAUDE.md` - Detailed implementation guide
- `cold_email_pipeline_spec.md` - Pipeline specification

## Key Components
1. **Lead Export Function** - Exports leads from BigQuery to Instantly
2. **Status Sync Function** - Syncs email status back to BigQuery
3. **Sequence Completion Handler** - Removes completed leads from Instantly

## Development Status
Initial project setup completed with configuration framework in place. Ready for pipeline implementation.