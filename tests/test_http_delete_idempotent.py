import types


class _FakeResponse:
    def __init__(self, status_code=200, text="", json_data=None, headers=None):
        self.status_code = status_code
        self.text = text
        self._json = json_data
        self.headers = headers or {}
        self.content = (text or "").encode()

    def json(self):
        if self._json is None:
            raise ValueError("No JSON")
        return self._json


def test_call_instantly_api_delete_404_is_structured(monkeypatch):
    from simple_async_verification import call_instantly_api

    def fake_delete(url, headers=None, timeout=None):
        return _FakeResponse(status_code=404, text="not found", json_data=None)

    monkeypatch.setattr("requests.delete", fake_delete)
    out = call_instantly_api("/api/v2/leads/abc", method="DELETE")
    assert isinstance(out, dict)
    assert out.get("status_code") == 404
    assert "text" in out


def test_call_instantly_api_delete_204_is_structured(monkeypatch):
    from simple_async_verification import call_instantly_api

    def fake_delete(url, headers=None, timeout=None):
        return _FakeResponse(status_code=204, text="")

    monkeypatch.setattr("requests.delete", fake_delete)
    out = call_instantly_api("/api/v2/leads/abc", method="DELETE")
    assert isinstance(out, dict)
    assert out.get("status_code") == 204

