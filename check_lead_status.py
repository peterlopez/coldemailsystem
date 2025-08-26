#\!/usr/bin/env python3
import os
from google.cloud import bigquery

# Setup BigQuery client
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
bq_client = bigquery.Client(project='instant-ground-394115')

# Check specific lead
email = "support@giftyusa.com"

query = f"""
SELECT 
    email,
    campaign_id,
    status,
    instantly_lead_id,
    added_at,
    updated_at,
    last_drain_check
FROM `instant-ground-394115.email_analytics.ops_inst_state`
WHERE email = '{email}'
"""

print(f"Checking BigQuery for {email}...\n")
results = list(bq_client.query(query))

if results:
    for row in results:
        print(f"Found in BigQuery:")
        print(f"  Email: {row.email}")
        print(f"  Campaign: {row.campaign_id}")
        print(f"  Status: {row.status}")
        print(f"  Lead ID: {row.instantly_lead_id}")
        print(f"  Added: {row.added_at}")
        print(f"  Updated: {row.updated_at}")
        print(f"  Last Drain Check: {row.last_drain_check}")
else:
    print(f"❌ Lead {email} NOT found in BigQuery ops_inst_state table")
    print("This could mean:")
    print("  1. Lead was never processed by our sync")
    print("  2. Lead was deleted from our tracking")
    print("  3. Lead exists only in Instantly, not in our system")
