import io
import os


def _read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def test_drain_imports_from_service_not_sync():
    src = _read_file("drain_once.py")
    assert "from drain.service import (" in src or "from drain.service import" in src
    assert "from sync_once import" not in src


def test_drain_imports_http_via_shared_facade():
    src = _read_file("drain_once.py")
    assert "from shared.http import call_instantly_api" in src
