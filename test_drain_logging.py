#!/usr/bin/env python3
"""
Test the separated logging configuration for drain workflow
"""

print("Testing drain logging configuration...")

# This should create the drain logger
from drain_once import logger as drain_logger

# This should be the sync logger
from sync_once import logger as sync_logger

print(f"Drain logger name: {drain_logger.name}")
print(f"Sync logger name: {sync_logger.name}")

print(f"Drain logger handlers: {len(drain_logger.handlers)}")
print(f"Sync logger handlers: {len(sync_logger.handlers)}")

# Test logging to both
drain_logger.info("🧹 This is a DRAIN workflow message - should go to cold-email-drain.log")
sync_logger.info("🔄 This is a SYNC workflow message - should go to cold-email-sync.log")

print("✅ Test complete. Check the log files:")
print("  - cold-email-drain.log should have drain message")
print("  - cold-email-sync.log should have sync message")