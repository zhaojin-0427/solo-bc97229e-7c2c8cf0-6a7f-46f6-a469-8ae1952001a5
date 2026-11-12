"""Pytest fixtures: isolated temp lake per test, TestClient with worker control."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

# Default state paths (overridden per test below).
_TMP = tempfile.mkdtemp(prefix="scan-lake-test-")
os.environ.setdefault("LAKE_DB_PATH", str(Path(_TMP) / "db" / "scan_lake.db"))
os.environ.setdefault("LAKE_ROOT", str(Path(_TMP) / "lake"))
os.environ["LAKE_CHUNK_SIZE"] = "3"  # small chunks exercise multi-part repartition
os.environ["LAKE_WORKER_INTERVAL"] = "0.05"

from fastapi.testclient import TestClient  # noqa: E402

from app import service  # noqa: E402
from app.database import db  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(autouse=True)
def _isolated_lake(tmp_path, monkeypatch):
    """Give every test its own DB + lake directory."""
    db_dir = tmp_path / "db"
    lake_dir = tmp_path / "lake"
    db_dir.mkdir()
    lake_dir.mkdir()
    monkeypatch.setenv("LAKE_DB_PATH", str(db_dir / "scan_lake.db"))
    monkeypatch.setenv("LAKE_ROOT", str(lake_dir))
    # The TestClient lifespan owns the connection; service-only tests connect
    # explicitly and rely on their own cleanup.
    assert db._conn is None, "previous test left the database connected"
    service._locks.clear()
    service.worker_gate.set()
    yield


@pytest.fixture
def client():
    # Tests drive the repartition worker explicitly via /debug/worker/tick so
    # assertions about intermediate states are deterministic.
    service.worker_gate.clear()
    with TestClient(app) as c:
        service.worker_gate.clear()
        yield c


def make_scan(sid: str, utc_ts: str, **extra):
    rec = {"id": sid, "utc_ts": utc_ts}
    rec.update(extra)
    return rec
