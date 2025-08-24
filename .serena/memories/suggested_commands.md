# Suggested Commands for Cold Email System

## Setup and Configuration
```bash
# Clone and navigate to project
git clone [repo-url]
cd "Cold Email System"

# Set up environment
cp .env.example .env
pip install -r requirements.txt
```

## Testing and Validation
```bash
# Test overall configuration
python scripts/test_config.py

# Test BigQuery connection
python scripts/test_bigquery_connection.py

# Test Instantly API connection
python scripts/test_instantly_api.py
```

## Code Quality Commands
```bash
# Format code with Black
black .

# Run linter
flake8 .

# Run tests (when implemented)
pytest
pytest -v  # verbose output
pytest --asyncio-mode=auto  # for async tests
```

## Development Commands
```bash
# Install dependencies
pip install -r requirements.txt

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On macOS/Linux

# Git commands
git status
git add .
git commit -m "message"
git push origin main
```

## Google Cloud Commands (when deployed)
```bash
# Deploy cloud function
gcloud functions deploy [function-name] --runtime python39

# View logs
gcloud functions logs read [function-name]

# Test function locally
functions-framework --target [function-name]
```

## macOS Specific Utils
```bash
# File operations
ls -la  # List all files with details
find . -name "*.py"  # Find Python files
grep -r "pattern" .  # Search in files

# Process management
ps aux | grep python  # Find Python processes
kill -9 [PID]  # Force kill process
```

## Environment Management
```bash
# View environment variables
env | grep GCP
env | grep INSTANTLY

# Edit configuration files
nano .env  # or vim, code, etc.
```