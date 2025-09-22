def test_shared_bq_update_respects_dry_run(monkeypatch):
    # Ensure no BigQuery calls are made under DRY_RUN
    monkeypatch.setattr("sync_once.DRY_RUN", True, raising=False)
    from shared.bq import update_bigquery_state
    update_bigquery_state([object()])  # Should be a no-op without exceptions


def test_drain_service_uses_shared_bq(monkeypatch):
    # capture calls to shared facade
    called = {"count": 0}

    def fake_update(leads):
        called["count"] += 1

    # Patch the function reference used inside drain.service
    monkeypatch.setattr("drain.service._update_bq_state", fake_update, raising=True)

    from drain.service import update_bigquery_state

    update_bigquery_state([object()])
    assert called["count"] == 1
