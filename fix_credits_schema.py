#!/usr/bin/env python3
"""
Fix BigQuery schema for verification_credits_used field to accept decimal values.
Instantly API returns 0.25 credits but our field is INTEGER, causing storage failures.
"""

import os
from google.cloud import bigquery

def fix_credits_schema():
    """Change verification_credits_used from INTEGER to FLOAT"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
    client = bigquery.Client(project='instant-ground-394115')
    
    table_id = 'instant-ground-394115.email_analytics.ops_inst_state'
    table = client.get_table(table_id)
    
    print("Current schema:")
    for field in table.schema:
        if 'credit' in field.name:
            print(f"  {field.name}: {field.field_type}")
    
    # Create new schema with FLOAT instead of INTEGER for credits
    new_schema = []
    for field in table.schema:
        if field.name == 'verification_credits_used':
            # Change INTEGER to FLOAT
            new_field = bigquery.SchemaField(
                field.name, 
                'FLOAT',  # Changed from INTEGER to FLOAT
                mode=field.mode,
                description=field.description
            )
            new_schema.append(new_field)
            print(f"✅ Changing {field.name} from INTEGER to FLOAT")
        else:
            new_schema.append(field)
    
    # Update the table schema
    table.schema = new_schema
    updated_table = client.update_table(table, ["schema"])
    
    print("\n✅ Schema updated successfully!")
    print("New schema:")
    for field in updated_table.schema:
        if 'credit' in field.name:
            print(f"  {field.name}: {field.field_type}")

if __name__ == "__main__":
    fix_credits_schema()