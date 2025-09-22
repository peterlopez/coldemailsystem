class DummyLead:
    def __init__(self, id, email):
        self.id = id
        self.email = email


def test_sync_delete_success_200(monkeypatch):
    import sync_once

    def fake_call(endpoint, method="GET", data=None, use_session=False):
        assert method == "DELETE"
        return {"status_code": 200, "text": ""}

    monkeypatch.setattr("shared.http.call_instantly_api", fake_call, raising=True)
    lead = DummyLead("abc", "user@example.com")
    assert sync_once.delete_lead_from_instantly(lead) is True


def test_sync_delete_success_404(monkeypatch):
    import sync_once

    def fake_call(endpoint, method="GET", data=None, use_session=False):
        return {"status_code": 404, "text": "not found"}

    monkeypatch.setattr("shared.http.call_instantly_api", fake_call, raising=True)
    lead = DummyLead("abc", "user@example.com")
    assert sync_once.delete_lead_from_instantly(lead) is True


def test_sync_delete_rate_limited(monkeypatch):
    import sync_once

    def fake_call(endpoint, method="GET", data=None, use_session=False):
        return {"status_code": 429, "text": "rate limited"}

    monkeypatch.setattr("shared.http.call_instantly_api", fake_call, raising=True)
    lead = DummyLead("abc", "user@example.com")
    assert sync_once.delete_lead_from_instantly(lead) is False

