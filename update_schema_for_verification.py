#!/usr/bin/env python3
"""
Update BigQuery schema to add email verification tracking columns.
This script adds verification columns to the existing ops_inst_state table.
"""

import os
import sys
import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "instant-ground-394115"
DATASET_ID = "email_analytics"

def main():
    """Update BigQuery schema to add verification columns."""
    try:
        # Initialize BigQuery client
        logger.info("Initializing BigQuery client...")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/secrets/bigquery-credentials.json'
        
        # Check if credentials file exists
        creds_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        if not os.path.exists(creds_path):
            logger.error(f"BigQuery credentials file not found at: {creds_path}")
            raise FileNotFoundError(f"Credentials file not found: {creds_path}")
        
        client = bigquery.Client(project=PROJECT_ID)
        logger.info("✅ BigQuery client initialized successfully")
        
        # Check if table exists
        table_id = f"{PROJECT_ID}.{DATASET_ID}.ops_inst_state"
        try:
            table = client.get_table(table_id)
            logger.info(f"✅ Table {table_id} exists")
        except Exception as e:
            logger.error(f"❌ Table {table_id} does not exist. Run setup.py first.")
            return
        
        # Get current schema
        current_schema = [field.name for field in table.schema]
        logger.info(f"Current columns: {', '.join(current_schema)}")
        
        # Check which columns need to be added
        columns_to_add = []
        
        if 'verification_status' not in current_schema:
            columns_to_add.append("ADD COLUMN IF NOT EXISTS verification_status STRING")
            
        if 'verification_catch_all' not in current_schema:
            columns_to_add.append("ADD COLUMN IF NOT EXISTS verification_catch_all BOOLEAN")
            
        if 'verification_credits_used' not in current_schema:
            columns_to_add.append("ADD COLUMN IF NOT EXISTS verification_credits_used INT64")
            
        if 'verified_at' not in current_schema:
            columns_to_add.append("ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP")
        
        if not columns_to_add:
            logger.info("✅ All verification columns already exist!")
            return
        
        # Build and execute ALTER TABLE query
        logger.info(f"Adding {len(columns_to_add)} new columns...")
        
        for column_def in columns_to_add:
            query = f"ALTER TABLE `{table_id}` {column_def}"
            logger.info(f"Executing: {query}")
            
            try:
                client.query(query).result()
                logger.info(f"✅ Added column: {column_def.split()[-2]}")
            except Exception as e:
                logger.warning(f"Column might already exist or error occurred: {e}")
        
        # Verify the schema update
        logger.info("\nVerifying schema update...")
        table = client.get_table(table_id)  # Refresh table metadata
        final_schema = [field.name for field in table.schema]
        
        verification_columns = ['verification_status', 'verification_catch_all', 
                              'verification_credits_used', 'verified_at']
        
        all_present = all(col in final_schema for col in verification_columns)
        
        if all_present:
            logger.info("✅ Schema update successful! All verification columns are present.")
            logger.info(f"Final columns: {', '.join(final_schema)}")
        else:
            missing = [col for col in verification_columns if col not in final_schema]
            logger.error(f"❌ Missing columns: {', '.join(missing)}")
        
        # Show sample verification query
        logger.info("\n📊 Sample verification metrics query:")
        sample_query = f'''
SELECT 
    verification_status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
WHERE verified_at IS NOT NULL
GROUP BY verification_status
ORDER BY count DESC;
'''
        logger.info(sample_query)
        
    except Exception as e:
        logger.error(f"❌ Schema update failed: {e}")
        raise

if __name__ == "__main__":
    main()