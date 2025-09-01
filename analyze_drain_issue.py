# The issue is that our drain process:
# 1. Fetches leads from Instantly API
# 2. Classifies them (drain vs keep)
# 3. Only updates BigQuery for leads that are DRAINED

# But if a lead changes from Status 1 to Status 3 between syncs,
# and we check it within 24 hours of the last check, 
# the timestamp filter might skip it\!

# Let's verify this theory
print("HYPOTHESIS: The lead was skipped due to 24-hour timestamp filtering")
print()
print("Timeline:")
print("1. Lead added as 'active' to BigQuery: 2025-08-24 22:14:53")
print("2. Lead changed to Status 3 in Instantly: 2025-08-25 14:21:58") 
print("3. Our drain check timestamp: 2025-08-26 13:35:12")
print()
print("The issue:")
print("- Lead WAS checked on 2025-08-26 13:35:12")
print("- But if it was checked earlier (within 24h), it might have been skipped")
print("- BigQuery shows 'active' because we only update on DRAIN, not on KEEP")
print()
print("This is a DESIGN FLAW: We need to update BigQuery status for ALL checked leads,")
print("not just the ones we drain\!")
