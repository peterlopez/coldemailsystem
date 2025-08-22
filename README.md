# Cold Email Pipeline

Automated pipeline for syncing leads between BigQuery and Instantly.ai for cold email campaigns.

## Setup Instructions

### 1. Clone the repository
```bash
git clone [your-repo-url]
cd "Cold Email System"
```

### 2. Set up credentials

#### BigQuery Service Account:
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create or select your project
3. Go to IAM & Admin > Service Accounts
4. Create a new service account with BigQuery Admin role
5. Download the JSON key file
6. Save it as `config/secrets/bigquery-credentials.json`

#### Instantly API Key:
1. Log in to your Instantly account
2. Go to Settings > API
3. Generate a new API key
4. Create `config/secrets/instantly-config.json`:
```json
{
  "api_key": "your-api-key-here",
  "base_url": "https://api.instantly.ai"
}
```

### 3. Configure environment
```bash
cp .env.example .env
# Edit .env with your project details
```

### 4. Install dependencies
```bash
pip install -r requirements.txt
```

### 5. Test configuration
```bash
python scripts/test_config.py
```

## Project Structure
```
Cold Email System/
├── config/
│   ├── __init__.py
│   ├── config.py          # Configuration manager
│   └── secrets/           # API keys and credentials (git-ignored)
├── functions/             # Cloud Functions code
├── scripts/               # Utility scripts
├── tests/                 # Test files
├── .env.example           # Environment template
├── .gitignore            # Git ignore rules
├── CLAUDE.md             # Implementation guide
└── README.md             # This file
```

## Quick Start

Once credentials are configured, the pipeline will:
1. Export leads from BigQuery to Instantly
2. Track email campaign progress
3. Remove completed leads from sequences
4. Update BigQuery with results

See [CLAUDE.md](CLAUDE.md) for detailed implementation guide.