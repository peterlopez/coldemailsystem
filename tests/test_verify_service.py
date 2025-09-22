def test_verify_trigger_delegates(monkeypatch):
    called = {"args": None}

    def fake_trigger(leads, campaign_id):
        called["args"] = (leads, campaign_id)
        return True

    monkeypatch.setattr(
        "simple_async_verification.trigger_verification_for_new_leads",
        fake_trigger,
        raising=True,
    )

    from verify.service import trigger_verification

    leads = [{"email": "a@b.com", "instantly_lead_id": "id1"}]
    assert trigger_verification(leads, "camp") is True
    assert called["args"][0] == leads
    assert called["args"][1] == "camp"


def test_verify_poll_delegates(monkeypatch):
    def fake_poll():
        return {"checked": 1, "verified": 1, "invalid_deleted": 0, "errors": 0}

    monkeypatch.setattr(
        "simple_async_verification.poll_verification_results",
        fake_poll,
        raising=True,
    )

    from verify.service import poll

    out = poll()
    assert out.get("checked") == 1
