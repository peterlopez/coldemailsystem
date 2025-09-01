#\!/usr/bin/env python3
import os
import requests
import json
from google.cloud import bigquery

# Setup
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
bq_client = bigquery.Client(project='instant-ground-394115')

# Load API key
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')
if not INSTANTLY_API_KEY:
    with open('config/secrets/instantly-config.json', 'r') as f:
        config = json.load(f)
        INSTANTLY_API_KEY = config['api_key']

# Check a sample of leads marked as "active" in BigQuery
query = """
SELECT email, instantly_lead_id, campaign_id, status, last_drain_check
FROM `instant-ground-394115.email_analytics.ops_inst_state`
WHERE status = 'active'
AND last_drain_check > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
LIMIT 10
"""

print("Checking 10 'active' leads that were checked in last 24 hours...\n")
results = list(bq_client.query(query))

mismatches = 0
headers = {
    'Authorization': f'Bearer {INSTANTLY_API_KEY}',
    'Content-Type': 'application/json'
}

for row in results:
    # Search for this lead in Instantly
    response = requests.post(
        "https://api.instantly.ai/api/v2/leads/list",
        headers=headers,
        json={"search": row.email},
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        leads = data.get('items', [])
        if leads:
            instantly_status = leads[0].get('status', 'unknown')
            if instantly_status == 3:
                mismatches += 1
                print(f"❌ MISMATCH: {row.email}")
                print(f"   BigQuery: 'active' | Instantly: Status {instantly_status} (finished)")
        else:
            print(f"⚠️ NOT FOUND: {row.email} not in Instantly")

print(f"\nFound {mismatches} mismatches out of 10 checked")
