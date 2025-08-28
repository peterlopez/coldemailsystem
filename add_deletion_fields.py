#!/usr/bin/env python3
"""Add deletion_status and deletion_attempts fields to ops_inst_state table"""

import os
from google.cloud import bigquery

def add_deletion_fields():
    """Add deletion tracking fields to the ops_inst_state table"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
    client = bigquery.Client(project='instant-ground-394115')
    
    table_id = 'instant-ground-394115.email_analytics.ops_inst_state'
    table = client.get_table(table_id)
    
    print("Current schema:")
    for field in table.schema:
        print(f"  {field.name}: {field.field_type}")
    
    # Check if fields already exist
    existing_fields = {field.name for field in table.schema}
    fields_to_add = []
    
    if 'deletion_status' not in existing_fields:
        fields_to_add.append(bigquery.SchemaField(
            'deletion_status', 
            'STRING',  # BigQuery doesn't have ENUM, use STRING
            mode='NULLABLE',
            description='Deletion queue status: queued, done, or failed'
        ))
        print("✅ Will add deletion_status field")
    else:
        print("ℹ️ deletion_status already exists")
    
    if 'deletion_attempts' not in existing_fields:
        fields_to_add.append(bigquery.SchemaField(
            'deletion_attempts',
            'INTEGER',
            mode='NULLABLE',
            default_value_expression='0',
            description='Number of deletion attempts made'
        ))
        print("✅ Will add deletion_attempts field")
    else:
        print("ℹ️ deletion_attempts already exists")
    
    if 'verification_attempts' not in existing_fields:
        fields_to_add.append(bigquery.SchemaField(
            'verification_attempts',
            'INTEGER',
            mode='NULLABLE',
            default_value_expression='0',
            description='Number of verification attempts made'
        ))
        print("✅ Will add verification_attempts field")
    else:
        print("ℹ️ verification_attempts already exists")
    
    if fields_to_add:
        # Use ALTER TABLE statements for each field
        for field in fields_to_add:
            try:
                # Add the column
                alter_query = f"""
                ALTER TABLE `{table_id}`
                ADD COLUMN IF NOT EXISTS {field.name} {field.field_type}
                """
                client.query(alter_query).result()
                print(f"✅ Added {field.name} column")
                
                # Set default value if it's an INTEGER field
                if field.field_type == 'INTEGER':
                    default_query = f"""
                    UPDATE `{table_id}`
                    SET {field.name} = 0
                    WHERE {field.name} IS NULL
                    """
                    client.query(default_query).result()
                    print(f"✅ Set default value 0 omatically for {field.name}")
                    
            except Exception as e:
                print(f"⚠️ Error adding {field.name}: {e}")
        
        print(f"\n✅ Finished updating {table_id}")
    else:
        print("\n✅ All required fields already exist")

if __name__ == "__main__":
    add_deletion_fields()