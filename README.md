# Cold Email System - Quick Navigation

## 🏭 PRODUCTION FILES (Root Directory)
**These files are actively used in GitHub Actions:**

### Core Scripts
- `sync_once.py` (1,991 lines) - Main sync engine
- `simple_async_verification.py` (1,066 lines) - Email verification
- `drain_once.py` - Lead removal system  
- `cold_email_notifier.py` - Slack notifications
- `validate_environment.py` - Environment validation

### Configuration
- `setup.py` - Database table creation
- `requirements.txt` - Dependencies
- `shared/` - Shared components
- `config/` - Configuration files
- `.github/workflows/` - 4 GitHub Actions workflows

## 📁 ORGANIZED DIRECTORIES

### `docs/`
- `FILE_ORGANIZATION.md` - Complete file categorization
- `production/` - Production documentation (CLAUDE.md, deployment docs)
- `testing/` - Test scripts and debugging tools
- `investigation/` - Analysis and audit scripts  
- `maintenance/` - Database schema and system maintenance
- `deprecated/` - Legacy/unused files

### Key Production Workflows
1. **Cold Email Sync** - Every hour (business hours)
2. **Drain Leads** - Every 2 hours
3. **Async Verification Poller** - Every hour
4. **BigQuery Diagnostics** - Manual trigger

## 🎯 Status: PRODUCTION READY
- ✅ 292,561 eligible leads ready
- ✅ 4 automated GitHub Actions workflows
- ✅ Email verification system active
- ⚠️ **Need to configure sending schedule in Instantly dashboard**

See `docs/production/CLAUDE.md` for complete system documentation.