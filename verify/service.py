"""
Verify service facade.

Phase 2: Provide a stable interface for verification operations while
delegating to existing implementations in simple_async_verification.
This avoids coupling sync_once.py directly to verification internals and
prevents circular imports.
"""

from typing import Dict, List


def trigger_verification(leads: List[Dict], campaign_id: str) -> bool:
    """Trigger async verification for newly created leads.

    Delegates to simple_async_verification.trigger_verification_for_new_leads
    to preserve current behavior.
    """
    from simple_async_verification import (
        trigger_verification_for_new_leads as _trigger,
    )

    return _trigger(leads, campaign_id)


def poll() -> Dict[str, int]:
    """Poll verification results and process queued deletions.

    Delegates to simple_async_verification.poll_verification_results to
    preserve current behavior. The GitHub workflow continues to call the
    existing module directly; this service is for sync orchestration.
    """
    from simple_async_verification import (
        poll_verification_results as _poll,
    )

    return _poll()

