from typing import Dict, Any

# Re-export the existing notifier instance to avoid changing call sites
try:
    from cold_email_notifier import notifier as _notifier
except Exception:
    _notifier = None


def get_notifier():
    return _notifier


def send_drain_notification(payload: Dict[str, Any]) -> bool:
    if _notifier is None:
        return True  # treat as no-op success
    return _notifier.send_drain_notification(payload)


def send_sync_notification(payload: Dict[str, Any]) -> bool:
    if _notifier is None:
        return True
    return _notifier.send_sync_notification(payload)


def send_verification_polling_notification(payload: Dict[str, Any]) -> bool:
    if _notifier is None:
        return True
    return _notifier.send_verification_polling_notification(payload)

