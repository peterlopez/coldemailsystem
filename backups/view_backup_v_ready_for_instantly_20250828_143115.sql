-- Backup of v_ready_for_instantly view
-- Created: 2025-08-28 14:31:15.166072

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