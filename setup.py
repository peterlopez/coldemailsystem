#!/usr/bin/env python3
"""
Setup script for Cold Email System
Creates necessary BigQuery tables and views if they don't exist.
"""

import os
from google.cloud import bigquery

def setup_bigquery_tables():
    """Create all required BigQuery tables and views."""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/secrets/bigquery-credentials.json'
    client = bigquery.Client(project='instant-ground-394115')
    
    print("🚀 Setting up BigQuery tables for Cold Email System")
    
    # Check if tables already exist
    existing_tables = []
    try:
        tables = client.list_tables('instant-ground-394115.email_analytics')
        existing_tables = [table.table_id for table in tables]
    except Exception as e:
        print(f"Warning: Could not list existing tables: {e}")
    
    # Create ops_inst_state table
    if 'ops_inst_state' not in existing_tables:
        print("Creating ops_inst_state table...")
        query = '''
        CREATE TABLE `instant-ground-394115.email_analytics.ops_inst_state` (
          email STRING NOT NULL,
          campaign_id STRING,
          status STRING,
          instantly_lead_id STRING,
          added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        '''
        client.query(query).result()
        print("✅ ops_inst_state table created")
    else:
        print("✅ ops_inst_state table already exists")
    
    # Create ops_lead_history table
    if 'ops_lead_history' not in existing_tables:
        print("Creating ops_lead_history table...")
        query = '''
        CREATE TABLE `instant-ground-394115.email_analytics.ops_lead_history` (
          email STRING NOT NULL,
          campaign_id STRING,
          sequence_name STRING,
          status_final STRING,
          completed_at TIMESTAMP,
          attempt_num INT64 DEFAULT 1,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        '''
        client.query(query).result()
        print("✅ ops_lead_history table created")
    else:
        print("✅ ops_lead_history table already exists")
    
    # Create config table
    if 'config' not in existing_tables:
        print("Creating config table...")
        query = '''
        CREATE TABLE `instant-ground-394115.email_analytics.config` (
          key STRING NOT NULL,
          value_int INT64,
          value_string STRING,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        '''
        client.query(query).result()
        print("✅ config table created")
    else:
        print("✅ config table already exists")
    
    # Create dead letters table
    if 'ops_dead_letters' not in existing_tables:
        print("Creating ops_dead_letters table...")
        query = '''
        CREATE TABLE `instant-ground-394115.email_analytics.ops_dead_letters` (
          id STRING,
          occurred_at TIMESTAMP,
          phase STRING,
          email STRING,
          campaign_id STRING,
          payload JSON,
          http_status INT64,
          error_text STRING,
          retry_count INT64
        ) PARTITION BY DATE(occurred_at)
        '''
        client.query(query).result()
        print("✅ ops_dead_letters table created")
    else:
        print("✅ ops_dead_letters table already exists")
    
    # Set SMB threshold
    print("Setting SMB threshold configuration...")
    query = '''
    MERGE `instant-ground-394115.email_analytics.config` T
    USING (SELECT 'smb_sales_threshold' as key, 1000000 as value_int) S
    ON T.key = S.key
    WHEN NOT MATCHED THEN
      INSERT (key, value_int) VALUES (key, value_int)
    '''
    client.query(query).result()
    print("✅ SMB threshold set to $1,000,000")
    
    # Create ready-for-instantly view
    print("Creating v_ready_for_instantly view...")
    query = '''
    CREATE OR REPLACE VIEW `instant-ground-394115.email_analytics.v_ready_for_instantly` AS
    WITH 
    config AS (
      SELECT COALESCE(value_int, 1000000) AS smb_threshold
      FROM `instant-ground-394115.email_analytics.config`
      WHERE key = 'smb_sales_threshold'
      LIMIT 1
    ),
    active_in_instantly AS (
      SELECT LOWER(email) AS email 
      FROM `instant-ground-394115.email_analytics.ops_inst_state` 
      WHERE status = 'active'
    ),
    recently_completed AS (
      SELECT LOWER(email) AS email
      FROM `instant-ground-394115.email_analytics.ops_lead_history`
      WHERE completed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    )
    SELECT 
      LOWER(e.email) AS email,
      e.merchant_name,
      LOWER(e.platform_domain) AS platform_domain,
      e.state,
      e.country_code,
      e.estimated_sales_yearly,
      e.employee_count,
      e.product_count,
      e.avg_price,
      e.klaviyo_installed_at,
      CASE 
        WHEN e.estimated_sales_yearly < c.smb_threshold THEN 'SMB' 
        ELSE 'Midsize' 
      END AS sequence_target
    FROM `instant-ground-394115.email_analytics.eligible_leads` e
    CROSS JOIN config c
    LEFT JOIN active_in_instantly a ON LOWER(e.email) = a.email
    LEFT JOIN recently_completed r ON LOWER(e.email) = r.email
    WHERE a.email IS NULL AND r.email IS NULL
    '''
    client.query(query).result()
    print("✅ v_ready_for_instantly view created")
    
    # Check final counts
    print("\n📊 Final Status Check:")
    
    query = "SELECT COUNT(*) as count FROM `instant-ground-394115.email_analytics.eligible_leads`"
    result = client.query(query).result()
    eligible_count = next(result).count
    print(f"   Total eligible leads: {eligible_count:,}")
    
    query = "SELECT COUNT(*) as count FROM `instant-ground-394115.email_analytics.v_ready_for_instantly`"
    result = client.query(query).result() 
    ready_count = next(result).count
    print(f"   Ready for Instantly: {ready_count:,}")
    
    query = """
    SELECT 
      CASE 
        WHEN estimated_sales_yearly < 1000000 THEN 'SMB' 
        ELSE 'Midsize' 
      END as segment,
      COUNT(*) as count
    FROM `instant-ground-394115.email_analytics.v_ready_for_instantly`
    WHERE estimated_sales_yearly IS NOT NULL
    GROUP BY segment
    ORDER BY segment
    """
    result = client.query(query).result()
    print("   Segmentation:")
    for row in result:
        print(f"     {row.segment}: {row.count:,}")
    
    print("\n✅ Cold Email System setup complete!")
    print("\nNext steps:")
    print("1. Set up GitHub Actions secrets:")
    print("   - INSTANTLY_API_KEY")
    print("   - BIGQUERY_CREDENTIALS_JSON") 
    print("2. Test with: python sync_once.py (set DRY_RUN=true first)")
    print("3. Enable GitHub Actions workflow")

if __name__ == "__main__":
    setup_bigquery_tables()