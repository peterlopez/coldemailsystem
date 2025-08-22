"""
Configuration module for Cold Email Pipeline
Loads configuration from environment variables and secret files
"""
import os
import json
from pathlib import Path
from typing import Dict, Any

class Config:
    """Configuration manager for the Cold Email Pipeline"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent
        self.secrets_dir = self.base_dir / "config" / "secrets"
        self._load_env()
        
    def _load_env(self):
        """Load environment variables from .env file if it exists"""
        env_file = self.base_dir / ".env"
        if env_file.exists():
            with open(env_file) as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        os.environ[key] = value
    
    @property
    def google_credentials_path(self) -> str:
        """Path to Google Cloud credentials JSON file"""
        return os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 
                         str(self.secrets_dir / 'bigquery-credentials.json'))
    
    @property
    def gcp_project_id(self) -> str:
        """Google Cloud Project ID"""
        return os.getenv('GCP_PROJECT_ID', '')
    
    @property
    def bigquery_dataset(self) -> str:
        """BigQuery dataset name"""
        return os.getenv('BIGQUERY_DATASET', 'cold_email_pipeline')
    
    @property
    def bigquery_location(self) -> str:
        """BigQuery dataset location"""
        return os.getenv('BIGQUERY_LOCATION', 'US')
    
    @property
    def instantly_api_key(self) -> str:
        """Instantly API key - tries multiple sources"""
        # First try environment variable
        api_key = os.getenv('INSTANTLY_API_KEY')
        if api_key:
            return api_key
        
        # Then try secrets file
        config_file = self.secrets_dir / 'instantly-config.json'
        if config_file.exists():
            with open(config_file) as f:
                config = json.load(f)
                return config.get('api_key', '')
        
        return ''
    
    @property
    def instantly_base_url(self) -> str:
        """Instantly API base URL"""
        return os.getenv('INSTANTLY_BASE_URL', 'https://api.instantly.ai')
    
    @property
    def batch_size(self) -> int:
        """Batch size for API operations"""
        return int(os.getenv('BATCH_SIZE', '100'))
    
    @property
    def sync_interval_hours(self) -> int:
        """Hours between sync operations"""
        return int(os.getenv('SYNC_INTERVAL_HOURS', '4'))
    
    def validate(self) -> Dict[str, bool]:
        """Validate that all required configuration is present"""
        return {
            'google_credentials': os.path.exists(self.google_credentials_path),
            'gcp_project_id': bool(self.gcp_project_id),
            'instantly_api_key': bool(self.instantly_api_key),
            'bigquery_dataset': bool(self.bigquery_dataset)
        }
    
    def __repr__(self) -> str:
        validation = self.validate()
        return f"Config(validated={all(validation.values())}, checks={validation})"

# Global config instance
config = Config()