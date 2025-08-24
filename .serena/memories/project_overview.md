# Cold Email System - Project Overview

## Purpose
This is an automated pipeline system that synchronizes lead data between Google BigQuery and Instantly.ai for cold email campaigns. The system handles:
- Lead export from BigQuery to Instantly
- Automatic campaign sequence triggering
- Completion tracking and lead removal
- Status updates back to BigQuery

## Tech Stack
- **Language**: Python 3.x
- **Cloud Platform**: Google Cloud Platform (GCP)
  - Google Cloud Functions (for serverless execution)
  - BigQuery (data warehouse)
  - Cloud Scheduler (automated triggers)
  - Secret Manager (credential storage)
  - Cloud Logging (monitoring)
- **APIs**: Instantly.ai V2 API
- **Libraries**:
  - google-cloud-bigquery (3.13.0)
  - google-cloud-secret-manager (2.18.1)
  - google-cloud-logging (3.8.0)
  - requests (2.31.0)
  - aiohttp (3.9.1)
  - python-dotenv (1.0.0)
  - pydantic (2.5.3)
  - functions-framework (3.5.0)

## Development Tools
- pytest (7.4.3) - Testing framework
- pytest-asyncio (0.21.1) - Async test support
- black (23.12.1) - Code formatter
- flake8 (6.1.0) - Linter

## Architecture
The system is designed to run as serverless functions on Google Cloud, with three main components:
1. Lead Export Function - Sends new leads from BigQuery to Instantly
2. Status Sync Function - Updates lead status from Instantly back to BigQuery
3. Sequence Completion Handler - Removes completed leads from Instantly

## Key Features
- Batch processing (100 leads per API call)
- Error handling with exponential backoff
- Comprehensive logging and monitoring
- Security through GCP Secret Manager
- Automated scheduling (daily exports, 4-hour status syncs)