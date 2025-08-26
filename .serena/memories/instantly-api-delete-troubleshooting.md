# Instantly.ai V2 API Delete Operation - Complete Guide

## Correct Delete Endpoint
- **Method**: DELETE (not POST)
- **URL**: `https://api.instantly.ai/api/v2/leads/{id}` (plural "leads")
- **Auth**: Authorization: Bearer {API_KEY}
- **Body**: None (empty body)
- **Scope Required**: `leads:delete` or broader (`leads:all`, `all:delete`, `all:all`)

## Common 400 Bad Request Causes

### 1. Wrong API Key Scope
- Even with valid key, 400 can occur if lacking `leads:delete` scope
- Need to re-issue API key with proper scopes
- Current permission: `all:all` (should be sufficient)

### 2. Wrong Lead ID Source
- Only use lead IDs from `POST /api/v2/leads/list` 
- Don't use IDs from campaign lists or other endpoints
- Each lead has one canonical ID regardless of campaign membership

### 3. Duplicate Lead Handling
- Same email can appear in multiple campaigns but has single lead ID
- Must dedupe by email before delete to avoid trying to delete same lead twice
- Use email → lead_id mapping from `/leads/list`

### 4. HTTP Request Issues
- Ensure no request body on DELETE
- No trailing whitespace in URL
- Proper headers: Authorization + Accept: application/json

### 5. Already Deleted Leads
- 404 should be treated as success (idempotent)
- Some 400s might be for non-existent leads

### 6. Mock vs Production Host
- Production: `https://api.instantly.ai/api/v2`
- Mock: `https://developer.instantly.ai/_mock/api/v2`

## Debugging Steps

### Step 1: Get Canonical Lead ID
```python
# Use POST /api/v2/leads/list to get proper lead ID
POST /api/v2/leads/list
{
    "search": "email@domain.com"
}
# Use items[0].id for delete operation
```

### Step 2: Verify Lead Exists
```python
GET /api/v2/leads/{id}
# Should return 200 with lead JSON
```

### Step 3: Delete Lead
```python
DELETE /api/v2/leads/{id}
# Headers: Authorization: Bearer {key}
# Body: empty
```

### Step 4: Verify Deletion
```python
# Re-run POST /api/v2/leads/list with same email
# Should return empty items[] array
```

## Current Issue Analysis
Based on logs showing 400 errors:
- Using correct endpoint format: `/api/v2/leads/{id}`
- Valid UUID format: `007af0ca-21e2-4c21-aa09-437b2bf65b12`
- Same email appearing in both SMB and Midsize campaigns
- Likely cause: Trying to delete duplicate lead IDs for same email

## Recommended Fix
1. **Dedupe leads by email before delete**
2. **Use single canonical lead ID per email from `/leads/list`**
3. **Verify API key has proper scopes**
4. **Test with single lead first**