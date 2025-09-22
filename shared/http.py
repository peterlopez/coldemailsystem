"""
Shared HTTP client facade for Instantly API.

Phase 1: Re-export existing implementation to decouple modules safely.
Later phases can replace the underlying implementation here without
touching callers in drain/verify/sync.
"""

from typing import Dict, Optional


def call_instantly_api(endpoint: str, method: str = "GET", data: Optional[Dict] = None, use_session: bool = False) -> Dict:
    """Proxy to the current robust implementation.

    Delegates to simple_async_verification.call_instantly_api to preserve
    exact behavior and logging during the first step of refactor.
    """
    from simple_async_verification import call_instantly_api as _impl  # lazy import

    return _impl(endpoint, method=method, data=data, use_session=use_session)


def delete_lead(lead_id: str) -> Dict:
    """Convenience wrapper for DELETE /api/v2/leads/{id} returning structured response.
    The underlying implementation treats 2xx/404/409 as idempotent success.
    """
    return call_instantly_api(f"/api/v2/leads/{lead_id}", method="DELETE")

