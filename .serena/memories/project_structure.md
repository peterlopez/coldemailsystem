# Cold Email System - Project Structure

## Directory Layout
```
Cold Email System/
├── config/                 # Configuration management
│   ├── __init__.py        # Package initializer
│   ├── config.py          # Main Config class with environment/secret loading
│   └── secrets/           # Git-ignored directory for credentials
│       ├── bigquery-credentials.json
│       └── instantly-config.json
├── functions/             # Cloud Functions (currently empty, to be implemented)
├── scripts/               # Utility and test scripts
│   ├── test_config.py     # Tests overall configuration
│   ├── test_bigquery_connection.py  # Tests BigQuery connectivity
│   └── test_instantly_api.py        # Tests Instantly API connection
├── tests/                 # Unit tests directory (currently empty)
├── .serena/              # Serena tool memories
├── .env                  # Environment variables (git-ignored)
├── .env.example          # Template for environment setup
├── .gitignore           # Git ignore rules
├── activate.sh          # Script for environment activation
├── CLAUDE.md            # Detailed implementation guide
├── README.md            # Project setup and overview
├── cold_email_pipeline_spec.md  # Original specification document
└── requirements.txt     # Python dependencies
```

## Key Components

### Configuration (`config/`)
- Centralized configuration management
- Loads from environment variables and JSON files
- Handles both GCP and Instantly credentials
- Validates configuration on startup

### Scripts (`scripts/`)
- Standalone validation scripts
- Test individual components (BigQuery, Instantly)
- Useful for debugging and initial setup

### Functions (`functions/`)
- Will contain Cloud Functions code
- Three main functions planned:
  1. Lead export function
  2. Status sync function  
  3. Sequence completion handler

### Environment Files
- `.env.example`: Template with all required variables
- `.env`: Actual configuration (not committed)
- Contains project IDs, dataset names, API endpoints

## Data Flow
1. Leads stored in BigQuery dataset
2. Cloud Functions read from BigQuery
3. API calls to Instantly for lead management
4. Status updates written back to BigQuery
5. Logging and error tracking throughout