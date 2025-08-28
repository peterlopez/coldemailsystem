#!/usr/bin/env python3
"""Create missing ops_do_not_contact table in BigQuery"""

import os
from google.cloud import bigquery

def create_dnc_table():
    """Create the ops_do_not_contact table if it doesn't exist"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
    client = bigquery.Client(project='instant-ground-394115')
    
    table_id = 'instant-ground-394115.email_analytics.ops_do_not_contact'
    
    schema = [
        bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("reason", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("added_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table {table.project}.{table.dataset_id}.{table.table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"ℹ️ Table {table_id} already exists")
        else:
            print(f"❌ Error creating table: {e}")
            raise

if __name__ == "__main__":
    create_dnc_table()