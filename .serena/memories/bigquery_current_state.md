# BigQuery Current State - Cold Email System

## Connection Details
- **Project**: instant-ground-394115  
- **Dataset**: email_analytics
- **Credentials**: Available at config/secrets/bigquery-credentials.json
- **Connection**: ✅ Working

## Existing Tables

### Core Data Tables
1. **storeleads** (TABLE) - 390,052 rows
   - Main lead source with 25 fields
   - Contains: store_id, platform_domain, merchant_name, estimated_sales_yearly, klaviyo_active, etc.

2. **dnc_list** (TABLE) - 11,726 rows  
   - Suppression list with fields:
     - id (STRING, REQUIRED)
     - email (STRING, NULLABLE)  
     - domain (STRING, NULLABLE)
     - source (STRING, REQUIRED)
     - reason (STRING, NULLABLE)
     - added_date (TIMESTAMP, NULLABLE)
     - added_by (STRING, NULLABLE)
     - notes (STRING, NULLABLE)
     - is_active (BOOLEAN, NULLABLE)

### Existing Views
1. **active_dnc** (VIEW)
   - Filters dnc_list WHERE is_active = TRUE
   - Groups by email/domain with aggregated sources/reasons
   
2. **eligible_leads** (VIEW)  
   - Joins storeleads LEFT JOIN active_dnc
   - Filters: d.email IS NULL AND s.email IS NOT NULL AND s.klaviyo_active = TRUE
   - **Current count: 292,574 eligible leads**

## Segmentation Analysis (using $1M cutoff)
- **SMB** (< $1M): 105,701 leads (36.2%)
- **Midsize** (≥ $1M): 184,304 leads (63.8%)

## What We Need to Add
- **ops_inst_state**: Track current Instantly campaign state
- **ops_lead_history**: Track completed sequences for 90-day cooldown
- **config**: Store dynamic thresholds (SMB cutoff, etc.)

## Key Insights
- DNC list uses is_active column (treat TRUE as active suppression)
- Current active_dnc view only shows is_active = TRUE records
- Large volume of eligible leads (292k+) - need careful batch management
- Existing eligible_leads view already handles DNC filtering properly