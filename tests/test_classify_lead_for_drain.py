import os
import pytest


def _make_lead(
    email="test@example.com",
    status=1,
    email_reply_count=0,
    esp_code=0,
    timestamp_created="2025-01-01T00:00:00Z",
    payload=None,
    status_text=None,
):
    lead = {
        "email": email,
        "status": status,
        "email_reply_count": email_reply_count,
        "esp_code": esp_code,
        "timestamp_created": timestamp_created,
        "payload": payload or {},
    }
    if status_text is not None:
        lead["status_text"] = status_text
    return lead


def test_classify_completed_without_reply():
    from drain.service import classify_lead_for_drain

    lead = _make_lead(status=3, email_reply_count=0)
    out = classify_lead_for_drain(lead, "Midsize")
    assert out["should_drain"] is True
    assert out["drain_reason"] == "completed"


def test_classify_replied_genuine():
    from drain.service import classify_lead_for_drain

    lead = _make_lead(status=3, email_reply_count=1, payload={})
    out = classify_lead_for_drain(lead, "Midsize")
    assert out["should_drain"] is True
    assert out["drain_reason"] == "replied"


def test_classify_auto_reply_detected_kept():
    from drain.service import classify_lead_for_drain

    lead = _make_lead(
        status=3,
        email_reply_count=1,
        payload={"pause_until": "2025-01-03T00:00:00Z"},
    )
    out = classify_lead_for_drain(lead, "Midsize")
    assert out["should_drain"] is False
    assert out.get("auto_reply") is True


def test_classify_unsubscribed_via_status_text():
    """Current implementation keys off status_text containing 'unsubscribed'."""
    from drain.service import classify_lead_for_drain
    from datetime import datetime, timezone

    recent = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    lead = _make_lead(
        status=1,
        status_text="User unsubscribed from campaign",
        timestamp_created=recent,
    )
    out = classify_lead_for_drain(lead, "Midsize")
    assert out["should_drain"] is True
    assert out["drain_reason"] == "unsubscribed"


@pytest.mark.skip(reason="Enable after refactor to use status_summary['unsubscribed']")
def test_classify_unsubscribed_via_status_summary_future():
    import sync_once

    lead = _make_lead(status=3)
    lead["status_summary"] = {"unsubscribed": 1}
    out = sync_once.classify_lead_for_drain(lead, "Midsize")
    assert out["should_drain"] is True
    assert out["drain_reason"] == "unsubscribed"
