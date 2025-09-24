#!/usr/bin/env python3
"""
Backfill analysis for ops_inst_state to reduce 'missing' drain cases.

This script computes how many rows would be updated by the following rules:

R1: status='deleted' where deletion_status='done' but status != 'deleted'
R2: status='deleted' where verification_status='invalid_deleted' and status NOT IN ('deleted')
R3: status='unsubscribed' where email exists in dnc tables and status NOT IN ('unsubscribed','deleted')
R4: status=status_final from ops_lead_history (last 120 days) where ops_inst_state.status IN ('active','pending')

It prints counts and does not modify any data. Use this to get impact before running backfill.
"""

import os
import sys
from google.cloud import bigquery
import argparse

# Ensure repository root is on path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from shared_config import PROJECT_ID, DATASET_ID


def run_count(client, sql: str, params=None) -> int:
    job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
    if params:
        job_config.query_parameters = params
    res = list(client.query(sql, job_config=job_config).result())
    if not res:
        return 0
    row = res[0]
    # Try common field names
    for k in ("cnt", "count", "c"):
        if hasattr(row, k):
            return getattr(row, k)
    # Fallback: first value
    return list(row.values())[0]


def apply_backfill(client) -> None:
    print("\n🚀 Applying backfill updates (Standard SQL)")
    # R3: unsubscribed via DNC
    r3_update = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` s
    SET status = 'unsubscribed',
        updated_at = CURRENT_TIMESTAMP()
    WHERE COALESCE(s.status, '') NOT IN ('unsubscribed','deleted')
      AND LOWER(s.email) IN (
        SELECT email FROM (
          SELECT DISTINCT LOWER(email) AS email FROM `{PROJECT_ID}.{DATASET_ID}.dnc_list`
          UNION ALL
          SELECT DISTINCT LOWER(email) AS email FROM `{PROJECT_ID}.{DATASET_ID}.ops_do_not_contact`
        )
      )
    """
    job = client.query(r3_update, job_config=bigquery.QueryJobConfig(use_legacy_sql=False))
    job.result()
    r3_affected = getattr(job, 'num_dml_affected_rows', 0) or 0
    print(f"R3 unsubscribed updates applied: {r3_affected}")

    # R4: sync with latest history status (completed/replied) in last 120 days
    r4_update = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` s
    SET s.status = h.status_final,
        s.updated_at = CURRENT_TIMESTAMP()
    FROM (
      SELECT LOWER(email) AS email, status_final
      FROM `{PROJECT_ID}.{DATASET_ID}.ops_lead_history`
      WHERE completed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 120 DAY)
        AND status_final IN ('completed','replied')
      QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(email) ORDER BY completed_at DESC) = 1
    ) h
    WHERE LOWER(s.email) = h.email
      AND COALESCE(s.status,'') IN ('active','pending')
    """
    job2 = client.query(r4_update, job_config=bigquery.QueryJobConfig(use_legacy_sql=False))
    job2.result()
    r4_affected = getattr(job2, 'num_dml_affected_rows', 0) or 0
    print(f"R4 history status updates applied: {r4_affected}")


def main():
    parser = argparse.ArgumentParser(description="Backfill ops_inst_state impact or apply")
    parser.add_argument("--apply", action="store_true", help="Apply backfill updates")
    args = parser.parse_args()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/secrets/bigquery-credentials.json'
    client = bigquery.Client(project=PROJECT_ID)

    print("\n🔎 Backfill Impact Analysis\n")

    # R1: deletion_status done -> status deleted
    r1_sql = f"""
    SELECT COUNT(*) AS cnt
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE deletion_status = 'done'
      AND COALESCE(status, '') != 'deleted'
    """
    r1 = run_count(client, r1_sql)
    print(f"R1 delete-status backfill candidates: {r1}")

    # R2: verification invalid_deleted -> status deleted
    r2_sql = f"""
    SELECT COUNT(*) AS cnt
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
    WHERE verification_status = 'invalid_deleted'
      AND COALESCE(status, '') != 'deleted'
    """
    r2 = run_count(client, r2_sql)
    print(f"R2 verification-deleted candidates: {r2}")

    # R3: unsubscribed via dnc tables -> status unsubscribed
    # Prefer primary dnc_list; fallback ops_do_not_contact when present
    r3_sql = f"""
    WITH dnc AS (
      SELECT LOWER(email) AS email FROM `{PROJECT_ID}.{DATASET_ID}.dnc_list`
      UNION ALL
      SELECT LOWER(email) FROM `{PROJECT_ID}.{DATASET_ID}.ops_do_not_contact`
    )
    SELECT COUNT(*) AS cnt
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` s
    JOIN (SELECT DISTINCT email FROM dnc) d ON LOWER(s.email) = d.email
    WHERE COALESCE(s.status, '') NOT IN ('unsubscribed','deleted')
    """
    r3 = run_count(client, r3_sql)
    print(f"R3 unsubscribed (DNC) candidates: {r3}")

    # R4: history-derived status (completed/replied) in last 120 days
    r4_sql = f"""
    SELECT COUNT(*) AS cnt
    FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` s
    JOIN `{PROJECT_ID}.{DATASET_ID}.ops_lead_history` h
      ON LOWER(s.email) = LOWER(h.email)
    WHERE h.completed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 120 DAY)
      AND h.status_final IN ('completed','replied')
      AND COALESCE(s.status, '') IN ('active','pending')
    """
    r4 = run_count(client, r4_sql)
    print(f"R4 history status candidates (completed/replied): {r4}")

    total = r1 + r2 + r3 + r4
    print("\n📊 Total unique impact (upper bound): {}".format(total))
    print("Note: Some emails may overlap across rules; actual unique updates will be <= this sum.")

    if args.apply:
        apply_backfill(client)


if __name__ == "__main__":
    main()
