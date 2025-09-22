"""
Drain service facade.

Phase 2a: Provide a stable service interface for the drain workflow while
delegating to existing implementations in sync_once. This removes the
direct dependency from drain_once.py to sync_once.py and lets us migrate
the implementation here incrementally without breaking production.
"""

from typing import List
import logging
from datetime import datetime
from typing import Dict

# Export DRY_RUN and InstantlyLead for compatibility
from shared_config import DRY_RUN  # type: ignore


def _sync():
    """Lazy import sync_once to avoid import cycles during test collection."""
    import sync_once  # noqa: WPS433

    return sync_once


# Re-export InstantlyLead for type compatibility in drain_once
InstantlyLead = _sync().InstantlyLead  # type: ignore

# Use shared BigQuery facade for updates
from shared.bq import update_bigquery_state as _update_bq_state  # type: ignore


def get_finished_leads() -> List[InstantlyLead]:  # type: ignore
    """Get leads with terminal status using BigQuery-first with pagination fallback.

    Mirrors sync_once.get_finished_leads logic while using shared HTTP client
    and existing sync helpers for timestamp checks and batching.
    """
    sync = _sync()
    logger = getattr(sync, "logger")
    try:
        logger.info("🔄 DRAIN: Fetching finished leads from Instantly campaigns...")

        MAX_LEADS_TO_EVALUATE = getattr(sync, "MAX_LEADS_TO_EVALUATE", 0)
        MAX_PAGES_TO_PROCESS = getattr(sync, "MAX_PAGES_TO_PROCESS", 0)
        FORCE_DRAIN_CHECK = getattr(sync, "FORCE_DRAIN_CHECK", False)

        if MAX_LEADS_TO_EVALUATE > 0:
            logger.info(f"🧪 TESTING MODE: Limiting evaluation to {MAX_LEADS_TO_EVALUATE} leads total")
        if MAX_PAGES_TO_PROCESS > 0:
            logger.info(f"🧪 TESTING MODE: Limiting pagination to {MAX_PAGES_TO_PROCESS} pages per campaign")
        if FORCE_DRAIN_CHECK:
            logger.info("🧪 TESTING MODE: Forcing drain check on all leads (bypassing 24hr limit)")

        bigquery_leads = get_leads_needing_drain_from_bigquery()
        if bigquery_leads and not FORCE_DRAIN_CHECK:
            logger.info("🚀 PHASE 2: Using BigQuery-first optimization for targeted drain processing")
            return process_bigquery_first_drain(bigquery_leads)
        else:
            if not bigquery_leads:
                logger.info("🔄 BigQuery-first approach returned no results, using current pagination method")
            if FORCE_DRAIN_CHECK:
                logger.info("🧪 FORCE_DRAIN_CHECK enabled, using current pagination method to scan all leads")
            logger.info("🔄 FALLBACK: Using current pagination-based drain processing")

        # Fallback to original pagination loop in sync_once
        return sync.get_finished_leads()
    except Exception as e:
        logger.error(f"❌ Direct API drain processing failed: {e}")
        raise


def get_leads_needing_drain_from_bigquery() -> Dict[str, List[str]]:
    """BigQuery-first: find leads needing drain evaluation (24h+ or never checked)."""
    sync = _sync()
    logger = getattr(sync, "logger")
    bq_client = getattr(sync, "bq_client")
    PROJECT_ID = getattr(sync, "PROJECT_ID")
    DATASET_ID = getattr(sync, "DATASET_ID")
    SMB_CAMPAIGN_ID = getattr(sync, "SMB_CAMPAIGN_ID")
    MIDSIZE_CAMPAIGN_ID = getattr(sync, "MIDSIZE_CAMPAIGN_ID")
    DRAIN_BATCH_SIZE = getattr(sync, "DRAIN_BATCH_SIZE", 50)
    try:
        logger.info(f"📊 DIRECT API: Querying BigQuery for leads needing drain evaluation (batch size: {DRAIN_BATCH_SIZE})...")
        query = f"""
        SELECT instantly_lead_id, campaign_id, email, status
        FROM `{PROJECT_ID}.{DATASET_ID}.ops_inst_state`
        WHERE (
            last_drain_check IS NULL
            OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_drain_check, HOUR) >= 24
        )
        AND status IN ('active', 'pending')
        AND campaign_id IN ('{SMB_CAMPAIGN_ID}', '{MIDSIZE_CAMPAIGN_ID}')
        ORDER BY COALESCE(last_drain_check, TIMESTAMP('1970-01-01')) ASC, email ASC
        LIMIT {DRAIN_BATCH_SIZE}
        """
        results = list(bq_client.query(query).result())
        leads_by_campaign: Dict[str, List[str]] = {}
        for row in results:
            leads_by_campaign.setdefault(row.campaign_id, []).append(row.instantly_lead_id)

        total_leads = sum(len(v) for v in leads_by_campaign.values())
        if total_leads > 0:
            logger.info("📊 BigQuery Direct API Results:")
            logger.info(f"  • Batch size configured: {DRAIN_BATCH_SIZE}")
            logger.info(f"  • Campaigns with leads: {len(leads_by_campaign)}")
            logger.info(f"  • Total leads for direct API calls: {total_leads}")
            logger.info("🎯 Direct API Optimization:")
            logger.info("  • {0} individual API calls vs pagination".format(total_leads))
            logger.info("  • Expected success rate: ~95% (vs current ~0.6%)")
            logger.info("  • No client-side matching required")
            if DRAIN_BATCH_SIZE != 50:
                logger.info(f"  • Custom batch size: {DRAIN_BATCH_SIZE} (default: 50)")
        return leads_by_campaign
    except Exception as e:
        logger.error(f"❌ BigQuery direct API approach failed: {e}")
        logger.info("🔄 Will fall back to current pagination method")
        return {}


def get_leads_by_ids_from_instantly(campaign_id: str, lead_ids: List[str]) -> Dict[str, List]:
    """Direct API: GET each lead id from Instantly; return found/missing/errors lists."""
    sync = _sync()
    logger = getattr(sync, "logger")
    INSTANTLY_BASE_URL = getattr(sync, "INSTANTLY_BASE_URL")
    adaptive_rate_limiter = getattr(sync, "adaptive_rate_limiter")
    try:
        if not lead_ids:
            return {'found_leads': [], 'missing_leads': [], 'api_errors': []}

        from shared.http import call_instantly_api
        found_leads = []
        missing_leads = []
        api_errors = []
        for i, lead_id in enumerate(lead_ids):
            try:
                adaptive_rate_limiter.wait()
                resp = call_instantly_api(f"/api/v2/leads/{lead_id}", method="GET")
                if isinstance(resp, dict) and resp.get('id'):
                    found_leads.append(resp)
                    logger.debug(f"✅ Found lead {i+1}/{len(lead_ids)}: {resp.get('email', lead_id)}")
                else:
                    # If structured client returns no json, fallback to missing retry behavior
                    api_errors.append(lead_id)
            except Exception as e:
                logger.warning(f"🌐 Network/API error for lead {lead_id}: {e}")
                api_errors.append(lead_id)
        # Log summary
        found_count = len(found_leads)
        missing_count = len(missing_leads)
        error_count = len(api_errors)
        success_rate = (found_count / max(len(lead_ids), 1)) * 100
        logger.info("🎯 Direct API Results:")
        logger.info(f"  • Found: {found_count}/{len(lead_ids)} leads ({success_rate:.1f}%)")
        logger.info(f"  • Missing: {missing_count} (confirmed not in Instantly)")
        logger.info(f"  • API errors: {error_count} (network/auth issues)")
        logger.info(f"  • Processing time: ~{len(lead_ids) * 0.5:.1f} seconds")
        return {'found_leads': found_leads, 'missing_leads': missing_leads, 'api_errors': api_errors}
    except Exception as e:
        logger.error(f"❌ Direct API batch failed: {e}")
        return {'found_leads': [], 'missing_leads': [], 'api_errors': lead_ids}


def process_bigquery_first_drain(bigquery_leads: Dict[str, List[str]]) -> List[InstantlyLead]:
    """Process drain using direct API; update metrics and timestamps; return finished leads."""
    sync = _sync()
    logger = getattr(sync, "logger")
    SMB_CAMPAIGN_ID = getattr(sync, "SMB_CAMPAIGN_ID")
    batch_update_drain_timestamps = getattr(sync, "batch_update_drain_timestamps")
    MAX_LEADS_TO_EVALUATE = getattr(sync, "MAX_LEADS_TO_EVALUATE", 0)
    # Initialize LAST_DRAIN_METRICS in sync_once for notifications compatibility
    sync.LAST_DRAIN_METRICS = {
        'api_calls_made': 0,
        'leads_found': 0,
        'leads_missing': 0,
        'api_errors': 0,
        'api_success_rate': 0.0,
        'drain_classifications': {
            'replied': 0, 'completed': 0, 'missing': 0, 'bounced_hard': 0,
            'unsubscribed': 0, 'stale_active': 0, 'auto_reply_detected': 0,
            'kept_active': 0, 'kept_paused': 0, 'kept_other': 0, 'api_errors': 0,
        }
    }

    finished_leads: List[InstantlyLead] = []
    total_leads_processed = 0
    for campaign_id, lead_ids in bigquery_leads.items():
        campaign_name = "SMB" if campaign_id == SMB_CAMPAIGN_ID else "Midsize"
        logger.info(f"🎯 Processing {len(lead_ids)} leads from {campaign_name} campaign via direct API...")
        api_results = get_leads_by_ids_from_instantly(campaign_id, lead_ids)
        found_leads = api_results['found_leads']
        missing_leads = api_results['missing_leads']
        api_errors = api_results.get('api_errors', [])
        sync.LAST_DRAIN_METRICS['api_calls_made'] += len(lead_ids)
        sync.LAST_DRAIN_METRICS['leads_found'] += len(found_leads)
        sync.LAST_DRAIN_METRICS['leads_missing'] += len(missing_leads)
        sync.LAST_DRAIN_METRICS['api_errors'] += len(api_errors)

        leads_to_update_timestamps: List[str] = []
        for lead in found_leads:
            total_leads_processed += 1
            lead_id = lead.get('id', '')
            email = lead.get('email', '')
            if not lead_id:
                logger.debug(f"⚠️ Skipping lead with no ID: {email}")
                continue
            if MAX_LEADS_TO_EVALUATE > 0 and total_leads_processed > MAX_LEADS_TO_EVALUATE:
                logger.info(f"🧪 TESTING LIMIT REACHED: Processed {total_leads_processed} leads, stopping")
                break
            classification = classify_lead_for_drain(lead, campaign_name)
            if classification['should_drain']:
                instantly_lead = sync.InstantlyLead(
                    id=lead_id,
                    email=email,
                    campaign_id=campaign_id,
                    status=classification['drain_reason']
                )
                finished_leads.append(instantly_lead)
                reason = classification.get('drain_reason', 'unknown')
                sync.LAST_DRAIN_METRICS['drain_classifications'][reason] = sync.LAST_DRAIN_METRICS['drain_classifications'].get(reason, 0) + 1
                details = classification.get('details', '')
                logger.info(f"🗑️ DRAIN: {email} → {reason} | {details}")
            else:
                keep_reason = str(classification.get('keep_reason', 'unknown reason'))
                status = lead.get('status', 0)
                is_auto_reply = ('auto-reply' in keep_reason.lower()) or (classification.get('auto_reply', False) is True)
                if is_auto_reply:
                    sync.LAST_DRAIN_METRICS['drain_classifications']['auto_reply_detected'] += 1
                    logger.debug(f"🤖 KEEP: {email} → auto-reply detected | {keep_reason}")
                elif status == 1:
                    sync.LAST_DRAIN_METRICS['drain_classifications']['kept_active'] += 1
                    logger.debug(f"⚡ KEEP: {email} → active sequence | {keep_reason}")
                elif status == 2:
                    sync.LAST_DRAIN_METRICS['drain_classifications']['kept_paused'] += 1
                    logger.debug(f"⏸️ KEEP: {email} → paused sequence | {keep_reason}")
                else:
                    sync.LAST_DRAIN_METRICS['drain_classifications']['kept_other'] += 1
                    logger.debug(f"📋 KEEP: {email} → other reason | {keep_reason}")
            leads_to_update_timestamps.append(lead_id)

        # Mark missing leads
        for missing_lead_id in missing_leads:
            total_leads_processed += 1
            instantly_lead = sync.InstantlyLead(id=missing_lead_id, email=f"missing_lead_{missing_lead_id}", campaign_id=campaign_id, status='missing')
            finished_leads.append(instantly_lead)
            sync.LAST_DRAIN_METRICS['drain_classifications']['missing'] += 1
            logger.info(f"🗑️ DRAIN: {missing_lead_id} → missing | Lead not found in Instantly (likely auto-removed)")
            leads_to_update_timestamps.append(missing_lead_id)

        if api_errors:
            sync.LAST_DRAIN_METRICS['drain_classifications']['api_errors'] += len(api_errors)
            logger.warning(f"⚠️ {len(api_errors)} leads had API errors and will be retried next run")

        if leads_to_update_timestamps:
            try:
                batch_update_drain_timestamps(leads_to_update_timestamps)
                logger.info(f"📊 Updated drain check timestamps for {len(leads_to_update_timestamps)} leads in {campaign_name}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to update timestamps for {campaign_name}: {e}")

        if MAX_LEADS_TO_EVALUATE > 0 and total_leads_processed > MAX_LEADS_TO_EVALUATE:
            break

    if sync.LAST_DRAIN_METRICS['api_calls_made'] > 0:
        sync.LAST_DRAIN_METRICS['api_success_rate'] = (sync.LAST_DRAIN_METRICS['leads_found'] / sync.LAST_DRAIN_METRICS['api_calls_made']) * 100

    logger.info("=" * 60)
    logger.info("🚀 DIRECT API OPTIMIZATION: Drain processing complete")
    logger.info("=" * 60)
    logger.info("📊 Performance Summary:")
    total_to_drain = len(finished_leads)
    logger.info(f"  • Campaigns processed: {len(bigquery_leads)}")
    logger.info(f"  • Total leads processed: {total_leads_processed}")
    logger.info(f"  • Leads to drain: {total_to_drain}")
    logger.info(f"  • Processing success rate: {(total_leads_processed/max(sum(len(ids) for ids in bigquery_leads.values()),1)*100):.1f}%")
    logger.info(f"  • Direct API success rate: {sync.LAST_DRAIN_METRICS['api_success_rate']:.1f}%")
    if total_leads_processed > 0:
        logger.info("📊 Campaign Processing Results:")
        for campaign_id, lead_ids in bigquery_leads.items():
            campaign_name = "SMB" if campaign_id == SMB_CAMPAIGN_ID else "Midsize"
            leads_targeted = len(lead_ids)
            campaign_drained = sum(1 for lead in finished_leads if lead.campaign_id == campaign_id)
            campaign_efficiency = (campaign_drained / max(leads_targeted, 1) * 100)
            logger.info(f"  • {campaign_name}: {leads_targeted} targeted → {campaign_drained} drained ({campaign_efficiency:.1f}%)")
    if any(v > 0 for v in sync.LAST_DRAIN_METRICS['drain_classifications'].values()):
        logger.info("📋 Drain Classification Analysis:")
        total_cls = sum(sync.LAST_DRAIN_METRICS['drain_classifications'].values())
        for reason, count in sorted(sync.LAST_DRAIN_METRICS['drain_classifications'].items(), key=lambda x: x[1], reverse=True):
            if count > 0:
                pct = (count / max(total_cls, 1) * 100)
                logger.info(f"  • {reason}: {count} ({pct:.1f}%)")
    logger.info("=" * 60)
    return finished_leads


def classify_lead_for_drain(lead: dict, campaign_name: str) -> dict:
    """Classify a lead from Instantly API to determine drain action.

    Function body mirrors sync_once.classify_lead_for_drain to preserve behavior.
    """
    logger = getattr(_sync(), "logger")  # reuse original logger
    try:
        email = lead.get('email', 'unknown')
        status = lead.get('status', 0)
        esp_code = lead.get('esp_code', 0)
        email_reply_count = lead.get('email_reply_count', 0)
        created_at = lead.get('timestamp_created')

        payload = lead.get('payload', {})
        pause_until = payload.get('pause_until') if payload else None

        days_since_created = 0
        if created_at:
            try:
                created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                days_since_created = (datetime.now().astimezone() - created_date).days
            except Exception:
                days_since_created = 0

        if status == 3:
            if email_reply_count > 0:
                if pause_until:
                    logger.debug(f"🤖 Auto-reply detected for {email}: paused until {pause_until}")
                    return {
                        'should_drain': False,
                        'keep_reason': f'Auto-reply detected (paused until {pause_until}) - not genuine engagement',
                        'auto_reply': True
                    }
                else:
                    logger.debug(f"👤 Genuine reply detected for {email}: no auto-reply flags")
                    return {
                        'should_drain': True,
                        'drain_reason': 'replied',
                        'details': f'Status 3 with {email_reply_count} replies - genuine engagement (no auto-reply flags)',
                        'auto_reply': False
                    }
            else:
                return {
                    'should_drain': True,
                    'drain_reason': 'completed',
                    'details': 'Sequence completed without replies'
                }

        elif (status == 1 or status == 2) and email_reply_count > 0 and pause_until:
            logger.debug(f"🤖 Auto-reply for {email}: Status {status} + replies + paused until {pause_until}")
            return {
                'should_drain': False,
                'keep_reason': f'Status {status} lead with auto-reply (paused until {pause_until}) - let Instantly manage sequence',
                'auto_reply': True
            }

        elif status == 1 and days_since_created >= 90:
            logger.debug(f"⚠️ Stale active lead detected: {email} - {days_since_created} days old")
            return {
                'should_drain': True,
                'drain_reason': 'stale_active',
                'details': f'Active lead stuck for {days_since_created} days - safety net for inventory management'
            }

        elif esp_code in [550, 551, 553]:
            if days_since_created >= 7:
                return {
                    'should_drain': True,
                    'drain_reason': 'bounced_hard',
                    'details': f'Hard bounce (ESP {esp_code}) after {days_since_created} days - clear delivery failure'
                }
            else:
                return {
                    'should_drain': False,
                    'keep_reason': f'Recent hard bounce (ESP {esp_code}), within 7-day grace period'
                }

        elif esp_code in [421, 450, 451]:
            return {
                'should_drain': False,
                'keep_reason': f'Soft bounce (ESP {esp_code}) - letting Instantly manage retry'
            }

        elif 'unsubscribed' in str(lead.get('status_text', '')).lower():
            return {
                'should_drain': True,
                'drain_reason': 'unsubscribed',
                'details': 'Lead unsubscribed from campaign'
            }

        else:
            status_description = {
                1: "Active - sequence continuing",
                2: "Paused - may resume",
                0: "Unknown status",
            }.get(status, f"Status {status}")

            return {
                'should_drain': False,
                'keep_reason': f"{status_description} - trusting Instantly's sequence management ({days_since_created} days old)"
            }

    except Exception as e:
        logger.error(f"Error classifying lead {lead.get('email', 'unknown')}: {e}")
        return {
            'should_drain': False,
            'keep_reason': f'Classification error - keeping safely: {str(e)}'
        }


def update_bigquery_state(leads: List[InstantlyLead]) -> None:  # type: ignore
    """Use shared facade for bulk BigQuery updates (state/history/DNC)."""
    return _update_bq_state(leads)


def delete_leads_from_instantly(leads: List[InstantlyLead]) -> None:  # type: ignore
    """Delegate to existing Instantly delete batch implementation."""
    return _sync().delete_leads_from_instantly(leads)


def log_dead_letter(phase: str, email: str, payload: str, status_code: int, error_text: str) -> None:
    """Delegate to existing dead letter logger to preserve behavior."""
    return _sync().log_dead_letter(phase, email, payload, status_code, error_text)


def drain_finished_leads() -> int:
    """Run full drain: fetch → update BigQuery → delete in Instantly."""
    sync = _sync()
    finished_leads = sync.get_finished_leads()
    if not finished_leads:
        return 0
    sync.update_bigquery_state(finished_leads)
    sync.delete_leads_from_instantly(finished_leads)
    return len(finished_leads)


def get_direct_api_metrics() -> dict:
    """Expose direct API drain metrics without importing sync_once in callers.

    Returns a dict with keys:
      - api_calls_made, api_success_rate, leads_found, leads_missing, api_errors
      - drain_classifications (dict)
    Safe defaults are returned if metrics are unavailable.
    """
    try:
        sync = _sync()
        metrics = getattr(sync, "LAST_DRAIN_METRICS", None)
        if not isinstance(metrics, dict) or not metrics:
            return {
                "api_calls_made": 0,
                "api_success_rate": 0.0,
                "leads_found": 0,
                "leads_missing": 0,
                "api_errors": 0,
                "drain_classifications": {},
            }
        # Normalize and return a shallow copy to avoid mutation
        return {
            "api_calls_made": int(metrics.get("api_calls_made", 0) or 0),
            "api_success_rate": float(metrics.get("api_success_rate", 0.0) or 0.0),
            "leads_found": int(metrics.get("leads_found", 0) or 0),
            "leads_missing": int(metrics.get("leads_missing", 0) or 0),
            "api_errors": int(metrics.get("api_errors", 0) or 0),
            "drain_classifications": dict(metrics.get("drain_classifications", {}) or {}),
        }
    except Exception:
        return {
            "api_calls_made": 0,
            "api_success_rate": 0.0,
            "leads_found": 0,
            "leads_missing": 0,
            "api_errors": 0,
            "drain_classifications": {},
        }
