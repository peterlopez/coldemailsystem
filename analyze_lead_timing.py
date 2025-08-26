from datetime import datetime, timezone

# Lead data from Instantly
created = "2025-08-24T22:12:21.592Z"
updated_instantly = "2025-08-25T14:21:58.098Z"  # When Instantly marked as Status 3

# Our drain check time
last_drain_check = "2025-08-26 13:35:12.613873+00:00"

# Parse timestamps
created_dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
updated_dt = datetime.fromisoformat(updated_instantly.replace('Z', '+00:00'))
drain_check_dt = datetime.fromisoformat(last_drain_check)

print("Timeline Analysis for support@giftyusa.com:")
print(f"1. Created in Instantly: {created_dt}")
print(f"2. Status changed to 3: {updated_dt}")
print(f"3. Our drain check: {drain_check_dt}")
print()
print(f"Status changed {(drain_check_dt - updated_dt).total_seconds() / 3600:.1f} hours BEFORE our drain check")
print()
print("This means the lead WAS Status 3 when we checked it, so it should have been drained\!")
