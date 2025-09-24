"""
Shared BigQuery helpers facade.

Phase 1/2: Re-export existing functions to decouple modules. Later phases
can move implementations here without changing callers.
"""

from typing import Optional, List
import json


def _sync_module():
    import sync_once  # lazy import to avoid cycles

    return sync_once


def log_dead_letter(phase: str, email: Optional[str], payload: str, status_code: int, error_text: str) -> None:
    """Proxy to current implementation in sync_once to avoid duplication now."""
    from sync_once import log_dead_letter as _impl  # lazy import

    return _impl(phase, email, payload, status_code, error_text)


def update_bigquery_state(leads: List[object]) -> None:
    """Bulk update ops_inst_state, history, and DNC list for drained leads.

    Behavior mirrors the original implementation in sync_once.
    """
    sync = _sync_module()
    DRY_RUN = getattr(sync, "DRY_RUN")
    logger = getattr(sync, "logger")
    bq_client = getattr(sync, "bq_client")

    if not leads or DRY_RUN:
        return

    try:
        logger.info(f"📊 Updating BigQuery state for {len(leads)} drained leads with bulk operations...")

        # Track drain reasons for reporting
        drain_reasons = {}
        for lead in leads:
            drain_reasons[getattr(lead, "status")] = drain_reasons.get(getattr(lead, "status"), 0) + 1

        _bulk_update_ops_inst_state(leads)

        history_leads = [l for l in leads if getattr(l, "status") in ["completed", "replied"]]
        if history_leads:
            _bulk_insert_lead_history(history_leads)

        dnc_leads = [l for l in leads if getattr(l, "status") == "unsubscribed"]
        if dnc_leads:
            _bulk_insert_dnc_list(dnc_leads)

        logger.info("✅ Updated BigQuery state with bulk operations - Drain summary:")
        for reason, count in drain_reasons.items():
            logger.info(f"  - {reason}: {count} leads")

    except Exception as e:
        logger.error(f"❌ Failed to update BigQuery state: {e}")
        # Use original dead-letter implementation to record
        try:
            payload = json.dumps([getattr(l, "__dict__", {}) for l in leads])
        except Exception:
            payload = "[]"
        log_dead_letter("bigquery_update_drain", None, payload, 0, str(e))


def _bulk_update_ops_inst_state(leads: List[object]) -> None:
    """Robust bulk MERGE using parameterized arrays (no string interpolation)."""
    if not leads:
        return
    sync = _sync_module()
    bq_client = getattr(sync, "bq_client")
    PROJECT_ID = getattr(sync, "PROJECT_ID")
    DATASET_ID = getattr(sync, "DATASET_ID")
    logger = getattr(sync, "logger")

    # Prepare arrays for parameterized UNNEST zip by OFFSET
    emails: List[str] = []
    campaigns: List[str] = []
    statuses: List[str] = []
    lead_ids: List[str] = []
    for l in leads:
        emails.append((getattr(l, "email", "") or ""))
        campaigns.append((getattr(l, "campaign_id", "") or ""))
        statuses.append((getattr(l, "status", "") or ""))
        lead_ids.append((getattr(l, "id", "") or ""))

    sql = f"""
    WITH emails AS (
      SELECT value AS email, OFFSET FROM UNNEST(@emails) WITH OFFSET
    ), campaigns AS (
      SELECT value AS campaign_id, OFFSET FROM UNNEST(@campaign_ids) WITH OFFSET
    ), statuses AS (
      SELECT value AS status, OFFSET FROM UNNEST(@statuses) WITH OFFSET
    ), ids AS (
      SELECT value AS instantly_lead_id, OFFSET FROM UNNEST(@lead_ids) WITH OFFSET
    ), src AS (
      SELECT emails.email, campaigns.campaign_id, statuses.status, ids.instantly_lead_id
      FROM emails
      JOIN campaigns USING (OFFSET)
      JOIN statuses USING (OFFSET)
      JOIN ids USING (OFFSET)
    )
    MERGE `{PROJECT_ID}.{DATASET_ID}.ops_inst_state` T
    USING src S
    ON LOWER(T.email) = LOWER(S.email) AND T.campaign_id = S.campaign_id
    WHEN MATCHED THEN
      UPDATE SET status = S.status, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (email, campaign_id, status, instantly_lead_id, added_at, updated_at)
      VALUES (S.email, S.campaign_id, S.status, S.instantly_lead_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """

    # Build query parameters
    from google.cloud import bigquery  # lazy import
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("emails", "STRING", emails),
            bigquery.ArrayQueryParameter("campaign_ids", "STRING", campaigns),
            bigquery.ArrayQueryParameter("statuses", "STRING", statuses),
            bigquery.ArrayQueryParameter("lead_ids", "STRING", lead_ids),
        ]
    )

    bq_client.query(sql, job_config=job_config).result()
    logger.info(f"✅ Bulk updated {len(leads)} leads in ops_inst_state (parameterized arrays)")


def _bulk_insert_lead_history(leads: List[object]) -> None:
    if not leads:
        return
    sync = _sync_module()
    bq_client = getattr(sync, "bq_client")
    PROJECT_ID = getattr(sync, "PROJECT_ID")
    DATASET_ID = getattr(sync, "DATASET_ID")
    SMB_CAMPAIGN_ID = getattr(sync, "SMB_CAMPAIGN_ID")
    logger = getattr(sync, "logger")

    rows = []
    for lead in leads:
        safe_email = getattr(lead, "email").replace("'", "''")
        safe_status = getattr(lead, "status").replace("'", "''")
        seq = "SMB" if getattr(lead, "campaign_id") == SMB_CAMPAIGN_ID else "Midsize"
        rows.append(f"('{safe_email}', '{getattr(lead, 'campaign_id')}', '{seq}', '{safe_status}', CURRENT_TIMESTAMP(), 1)")

    values_clause = ",\n    ".join(rows)
    sql = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ops_lead_history`
    (email, campaign_id, sequence_name, status_final, completed_at, attempt_num)
    VALUES
    {values_clause}
    """
    bq_client.query(sql).result()
    logger.info(f"✅ Bulk inserted {len(leads)} leads to history (90-day cooldown)")


def _bulk_insert_dnc_list(leads: List[object]) -> None:
    if not leads:
        return
    sync = _sync_module()
    bq_client = getattr(sync, "bq_client")
    PROJECT_ID = getattr(sync, "PROJECT_ID")
    DATASET_ID = getattr(sync, "DATASET_ID")
    logger = getattr(sync, "logger")

    rows = []
    for lead in leads:
        safe_email = getattr(lead, "email").replace("'", "''")
        domain_part = getattr(lead, "email").split("@")[1] if "@" in getattr(lead, "email") else "unknown"
        safe_domain = domain_part.replace("'", "''")
        rows.append(
            "("
            "GENERATE_UUID(), "
            f"'{safe_email}', "
            f"'{safe_domain}', "
            "'instantly_drain', "
            "'unsubscribe_via_api', "
            "CURRENT_TIMESTAMP(), "
            "'sync_script_v2_bulk', "
            "TRUE)"
        )

    values_clause = ",\n    ".join(rows)
    sql = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dnc_list`
    (id, email, domain, source, reason, added_date, added_by, is_active)
    VALUES
    {values_clause}
    """
    bq_client.query(sql).result()
    logger.info(f"🚫 Bulk added {len(leads)} unsubscribes to permanent DNC list")
