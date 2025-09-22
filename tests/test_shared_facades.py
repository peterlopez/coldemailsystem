def test_shared_http_reuses_underlying_impl(monkeypatch):
    calls = {}

    def fake_impl(endpoint, method="GET", data=None, use_session=False):
        calls["endpoint"] = endpoint
        calls["method"] = method
        calls["data"] = data
        calls["use_session"] = use_session
        return {"status_code": 200, "json": {"ok": True}, "text": ""}

    # Patch the underlying implementation referenced by the facade
    monkeypatch.setattr(
        "simple_async_verification.call_instantly_api", fake_impl, raising=True
    )

    from shared.http import call_instantly_api

    out = call_instantly_api("/api/v2/ping", method="GET")
    assert out["status_code"] == 200
    assert calls["endpoint"] == "/api/v2/ping"
    assert calls["method"] == "GET"


def test_shared_bq_log_dead_letter_passthrough(monkeypatch):
    captured = {}

    def fake_log(phase, email, payload, status_code, error_text):
        captured.update(
            phase=phase,
            email=email,
            payload=payload,
            status=status_code,
            error=error_text,
        )

    monkeypatch.setattr("sync_once.log_dead_letter", fake_log, raising=True)

    from shared.bq import log_dead_letter

    log_dead_letter("phaseX", "user@example.com", "{}", 400, "oops")
    assert captured["phase"] == "phaseX"
    assert captured["email"] == "user@example.com"
    assert captured["status"] == 400

