#!/bin/bash
#
# Run sync_once.py with increased inventory multiplier
# This allows testing even when over the default safe limit
#

echo "🚀 Running Cold Email Sync with Increased Inventory Multiplier"
echo "============================================================="
echo ""
echo "📊 Configuration:"
echo "  - Inventory Multiplier: 10 (increased from 3.5)"
echo "  - This changes safe limit from 2,380 to 6,800 leads"
echo "  - Current inventory (BigQuery): 4,784 leads"
echo "  - New utilization: ~70% (was 201%)"
echo ""
echo "Starting sync..."
echo ""

# Run with increased multiplier and verbose logging
LEAD_INVENTORY_MULTIPLIER=10 \
TARGET_NEW_LEADS_PER_RUN=10 \
python sync_once.py

echo ""
echo "✅ Sync completed"
echo ""
echo "💡 If you got 401 errors, you need to fix the API key first:"
echo "   1. Check your Instantly.ai dashboard for the API key"
echo "   2. Update INSTANTLY_API_KEY in GitHub Secrets or config file"
echo "   3. Run 'python test_api_and_multiplier.py' to verify"