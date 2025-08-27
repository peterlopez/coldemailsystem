-- Check recent verification results
SELECT 
    verification_status,
    COUNT(*) as count,
    MIN(verified_at) as earliest,
    MAX(verified_at) as latest
FROM `instant-ground-394115.email_analytics.ops_inst_state`
WHERE verified_at IS NOT NULL 
    AND verified_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY verification_status
ORDER BY count DESC;

-- Sample of failed verifications
SELECT 
    email,
    verification_status,
    verified_at
FROM `instant-ground-394115.email_analytics.ops_inst_state`  
WHERE verification_status NOT IN ('valid', 'accept_all')
    AND verified_at IS NOT NULL
    AND verified_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY verified_at DESC
LIMIT 10;

-- Check inventory accuracy
SELECT 
    status,
    COUNT(*) as count
FROM `instant-ground-394115.email_analytics.ops_inst_state`
GROUP BY status
ORDER BY count DESC;

-- Check if failed verifications are tracked
SELECT 
    verification_status,
    status,
    COUNT(*) as count
FROM `instant-ground-394115.email_analytics.ops_inst_state`
WHERE verification_status IS NOT NULL
GROUP BY verification_status, status
ORDER BY verification_status, count DESC;