"""
BigQuery utility functions.
Contains all BigQuery operations - separate from API client.
"""

import os
import logging
from typing import List, Optional
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "instant-ground-394115"
DATASET_ID = "email_analytics"

# BigQuery client - initialized on first use
_bq_client = None

def get_bigquery_client():
    """Get or create BigQuery client."""
    global _bq_client
    
    if _bq_client is None:
        try:
            logger.info("Initializing BigQuery client...")
            
            # Set credentials path
            creds_path = './config/secrets/bigquery-credentials.json'
            if not os.path.exists(creds_path):
                logger.error(f"BigQuery credentials file not found at: {creds_path}")
                raise FileNotFoundError(f"Credentials file not found: {creds_path}")
            
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
            logger.info(f"Using credentials file: {creds_path}")
            
            _bq_client = bigquery.Client(project=PROJECT_ID)
            logger.info("✅ BigQuery client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise
    
    return _bq_client

def update_bigquery_state(leads: List, status: str = 'completed') -> None:
    """Update the status of leads in BigQuery ops_inst_state table."""
    if not leads:
        return
    
    try:
        client = get_bigquery_client()
        
        # Update each lead's status
        for lead in leads:
            query = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
            SET status = @status,
                updated_at = CURRENT_TIMESTAMP()
            WHERE email = @email AND campaign_id = @campaign_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("status", "STRING", status),
                    bigquery.ScalarQueryParameter("email", "STRING", lead.email),
                    bigquery.ScalarQueryParameter("campaign_id", "STRING", lead.campaign_id),
                ]
            )
            
            client.query(query, job_config=job_config).result()
        
        logger.info(f"Updated BigQuery status for {len(leads)} leads to '{status}'")
        
    except Exception as e:
        logger.error(f"Failed to update BigQuery state: {e}")
        raise

def log_dead_letter(phase: str, email: Optional[str], payload: str, 
                   http_status: Optional[int], error_text: str) -> None:
    """Log failed operations to the dead letter table."""
    try:
        client = get_bigquery_client()
        
        query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_dead_letters`
        (occurred_at, phase, email, http_status, error_text, payload)
        VALUES (CURRENT_TIMESTAMP(), @phase, @email, @http_status, @error_text, @payload)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("phase", "STRING", phase),
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("http_status", "INT64", http_status),
                bigquery.ScalarQueryParameter("error_text", "STRING", str(error_text)[:1000]),  # Truncate long errors
                bigquery.ScalarQueryParameter("payload", "STRING", str(payload)[:500] if payload else None),
            ]
        )
        
        client.query(query, job_config=job_config).result()
        logger.debug(f"Logged dead letter: {phase} - {email} - {error_text[:100]}")
        
    except Exception as e:
        logger.error(f"Failed to log dead letter: {e}")
        # Don't raise - logging failures shouldn't break the main process