"""End-to-end tests for the scan lakehouse ingestion service."""

from __future__ import annotations

import io
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from app import config
from tests.conftest import make_scan


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def register(client, code="JFK", tz="America/New_York"):
    r = client.post("/airports", json={"code": code, "timezone": tz})
    assert r.status_code == 201, r.text
    return r.json()


def ingest(client, code, records):
    return client.post("/ingest", json={"airport": code, "records": records})


def tick(client):
    r = client.post("/debug/worker/tick")
    assert r.status_code == 200, r.text
    return r.json()["job_processed"]


def run_job(client, code):
    """Advance the worker until no active job remains."""
    for _ in range(200):
        progress = client.get(f"/airports/{code}/progress").json()
        if progress["active_job"] is None:
            return
        tick(client)
    raise AssertionError("repartition job did not finish")


def tick_until(client, state):
    r = client.post(f"/debug/worker/tick?until_state={state}")
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------------------
# Airport registration
# ---------------------------------------------------------------------------


def test_register_airport_and_duplicate_registration(client):
    body = register(client)
    assert body["code"] == "JFK"
    assert body["timezone"] == "America/New_York"
    assert body["current_version"] == 1

    # Same timezone -> idempotent 200-style response.
    r = client.post("/airports", json={"code": "JFK", "timezone": "America/New_York"})
    assert r.status_code == 201
    assert r.json()["current_version"] == 1

    # Different timezone on register -> 409 pointing at the PATCH endpoint.
    r = client.post("/airports", json={"code": "JFK", "timezone": "UTC"})
    assert r.status_code == 409
    assert r.json()["error"] == "airport_exists"

    # Unknown IANA timezone -> 422.
    r = client.post("/airports", json={"code": "XYZ", "timezone": "Mars/Olympus"})
    assert r.status_code == 422


def test_ingest_unknown_airport_404(client):
    r = ingest(client, "NOPE", [make_scan("a", "2024-01-01T00:00:00Z")])
    assert r.status_code == 404
    assert r.json()["error"] == "airport_not_found"


# ---------------------------------------------------------------------------
# Ingestion: partitioning, manifest, synchronous visibility
# ---------------------------------------------------------------------------


def test_ingest_writes_partitioned_parquet_and_publishes_manifest(client):
    register(client)
    # 06:30Z and 07:00Z -> 2024-11-03 in New York; 03:15Z next day -> 2024-11-03 too (22:15 local).
    records = [
        make_scan("S1", "2024-11-03T06:30:00Z", event_type="DEP", location="G17"),
        make_scan("S2", "2024-11-04T03:15:00Z", event_type="ARR"),
        make_scan("S3", "2024-11-04T05:00:00Z"),  # 00:00 local 2024-11-04
    ]
    r = ingest(client, "JFK", records)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "accepted"
    assert body["inserted"] == 3
    assert body["duplicates"] == 0
    assert body["version"] == 1
    assert body["timezone"] == "America/New_York"

    manifest = body["manifest"]
    assert manifest["status"] == "current"
    assert manifest["row_count"] == 3
    dates = {f["local_date"] for f in manifest["files"]}
    assert dates == {"2024-11-03", "2024-11-04"}
    # Every file lives under a local_date partition and carries a sha256.
    for f in manifest["files"]:
        assert f"local_date={f['local_date']}" in f["relative_path"]
        assert len(f["sha256"]) == 64
        assert f["timezone"] == "America/New_York"

    # Downstream can immediately read the current manifest.
    cur = client.get("/airports/JFK/manifests/current").json()
    assert cur["manifest_id"] == manifest["manifest_id"]

    # Parquet content checks: UTC preserved, local_date column correct.
    root = config.lake_root()
    total = 0
    for f in manifest["files"]:
        table = pq.read_table(root / f["relative_path"])
        total += table.num_rows
        tz = table.schema.field("utc_ts").type
        assert str(tz) == "timestamp[us, tz=UTC]"
        col = table.column("local_date").to_pylist()
        assert all(str(d) == f["local_date"] for d in col)
    assert total == 3


def test_local_date_uses_airport_timezone_not_utc_date(client):
    # UTC 2024-11-03T02:00Z is 2024-11-02 22:00 in New York.
    register(client)
    r = ingest(client, "JFK", [make_scan("N1", "2024-11-03T02:00:00Z")])
    assert r.status_code == 200
    files = r.json()["manifest"]["files"]
    assert [f["local_date"] for f in files] == ["2024-11-02"]


# ---------------------------------------------------------------------------
# Idempotency / whole-batch rejection
# ---------------------------------------------------------------------------


def test_same_content_retransmit_is_idempotent(client):
    register(client)
    records = [make_scan("S1", "2024-11-03T06:30:00Z"), make_scan("S2", "2024-11-03T07:30:00Z")]
    first = ingest(client, "JFK", records)
    assert first.status_code == 200
    first_manifest = first.json()["manifest"]["manifest_id"]
    first_files = {f["relative_path"] for f in first.json()["manifest"]["files"]}

    second = ingest(client, "JFK", records)
    assert second.status_code == 200
    body = second.json()
    assert body["status"] == "duplicate"
    assert body["inserted"] == 0
    assert body["duplicates"] == 2
    # No new manifest revision, no new files.
    assert body["manifest"]["manifest_id"] == first_manifest
    assert {f["relative_path"] for f in body["manifest"]["files"]} == first_files


def test_partial_overlap_mixed_new_and_duplicate(client):
    register(client)
    ingest(client, "JFK", [make_scan("S1", "2024-11-03T06:30:00Z")])
    r = ingest(
        client,
        "JFK",
        [
            make_scan("S1", "2024-11-03T06:30:00Z"),  # identical -> dup
            make_scan("S2", "2024-11-03T08:30:00Z"),  # new
        ],
    )
    assert r.status_code == 200
    body = r.json()
    assert body["inserted"] == 1
    assert body["duplicates"] == 1
    assert body["manifest"]["row_count"] == 2


def test_different_content_same_id_rejects_whole_batch(client):
    register(client)
    r1 = ingest(client, "JFK", [make_scan("S1", "2024-11-03T06:30:00Z", event_type="DEP")])
    assert r1.status_code == 200

    # Same id, different utc_ts -> whole batch rejected, including the unrelated S9.
    r2 = ingest(
        client,
        "JFK",
        [
            make_scan("S1", "2024-11-03T09:99:00Z".replace("99", "59"), event_type="DEP"),
            make_scan("S9", "2024-11-03T10:00:00Z"),
        ],
    )
    assert r2.status_code == 409
    err = r2.json()
    assert err["error"] == "content_conflict"
    assert err["details"]["conflicts"][0]["id"] == "S1"

    # Nothing from the rejected batch landed.
    cur = client.get("/airports/JFK/manifests/current").json()
    assert cur["row_count"] == 1
    progress = client.get("/airports/JFK/progress").json()
    assert progress["total_scans"] == 1


def test_duplicate_id_within_batch_422(client):
    register(client)
    r = ingest(
        client,
        "JFK",
        [make_scan("S1", "2024-11-03T06:30:00Z"), make_scan("S1", "2024-11-03T07:30:00Z")],
    )
    assert r.status_code == 422


def test_batch_size_limit_200(client):
    register(client)
    ok = [make_scan(f"S{i}", "2024-11-03T06:30:00Z") for i in range(200)]
    assert ingest(client, "JFK", ok).status_code == 200
    too_many = [make_scan(f"T{i}", "2024-11-03T06:30:00Z") for i in range(201)]
    assert ingest(client, "JFK", too_many).status_code == 422


def test_naive_utc_ts_rejected_422(client):
    register(client)
    r = ingest(client, "JFK", [{"id": "S1", "utc_ts": "2024-11-03T06:30:00"}])
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Timezone change: repartition, exactly-once, old manifest readable
# ---------------------------------------------------------------------------


def test_timezone_change_repartitions_all_history_and_switches(client):
    register(client)
    # 10 scans spread across days; chunk size is 3 in tests.
    records = [
        make_scan(f"S{i:02d}", f"2024-11-0{i % 4 + 1}T0{1 + i % 9}:30:00Z")
        for i in range(10)
    ]
    r = ingest(client, "JFK", records)
    assert r.status_code == 200
    old_manifest = client.get("/airports/JFK/manifests/current").json()
    assert old_manifest["version"] == 1

    r = client.patch("/airports/JFK/timezone", json={"timezone": "America/Chicago"})
    assert r.status_code == 200, r.text
    job = r.json()
    assert job["state"] == "pending"
    assert job["new_version"] == 2
    assert job["from_timezone"] == "America/New_York"
    assert job["to_timezone"] == "America/Chicago"

    # While pending/running, the old manifest is still the readable current one.
    mid = client.get("/airports/JFK/manifests/current").json()
    assert mid["version"] == 1

    run_job(client, "JFK")

    progress = client.get("/airports/JFK/progress").json()
    assert progress["active_job"] is None
    assert progress["airport"]["timezone"] == "America/Chicago"
    assert progress["airport"]["current_version"] == 2

    new_manifest = client.get("/airports/JFK/manifests/current").json()
    assert new_manifest["version"] == 2
    assert new_manifest["timezone"] == "America/Chicago"
    assert new_manifest["row_count"] == 10
    assert all(f["timezone"] == "America/Chicago" for f in new_manifest["files"])

    # Every scan appears exactly once in the new version.
    root = config.lake_root()
    ids = set()
    for f in new_manifest["files"]:
        ids |= set(pq.read_table(root / f["relative_path"], columns=["row_id"]).column("row_id").to_pylist())
    assert len(ids) == 10

    # Re-partitioned local dates really follow Chicago (UTC-6 vs New York UTC-5).
    expected = {}
    for rec in records:
        dt = datetime.fromisoformat(rec["utc_ts"].replace("Z", "+00:00"))
        expected[rec["id"]] = dt.astimezone(ZoneInfo("America/Chicago")).date().isoformat()
    found = {}
    for f in new_manifest["files"]:
        t = pq.read_table(root / f["relative_path"], columns=["scan_id", "local_date"])
        for sid, d in zip(t.column("scan_id").to_pylist(), t.column("local_date").to_pylist()):
            found[sid] = str(d)
    assert found == expected

    # Old version stays available and immutable (superseded, same row count).
    old_listed = client.get("/airports/JFK/manifests").json()
    versions = {m["version"]: m for m in old_listed}
    assert set(versions) == {1, 2}
    assert versions[1]["status"] == "superseded"
    assert versions[1]["row_count"] == 10


def test_scans_ingested_during_repartition_are_included_exactly_once(client):
    register(client)
    ingest(client, "JFK", [make_scan(f"H{i}", "2024-11-03T06:30:00Z") for i in range(7)])

    r = client.patch("/airports/JFK/timezone", json={"timezone": "America/Chicago"})
    assert r.status_code == 200
    jid = r.json()["job_id"]

    # Freeze the pipeline inside the bulk (running) phase.
    info = tick_until(client, "running")
    assert info["state"] == "running"
    job = client.get("/airports/JFK/jobs").json()[0]
    assert 0 < job["processed_rows"] < 7

    # Ingestion keeps working during repartition, landing in the OLD version.
    during = [make_scan(f"D{i}", "2024-11-04T02:00:00Z") for i in range(5)]
    r = ingest(client, "JFK", during)
    assert r.status_code == 200
    assert r.json()["version"] == 1
    assert r.json()["manifest"]["row_count"] == 12

    run_job(client, "JFK")

    job = next(j for j in client.get("/airports/JFK/jobs").json() if j["job_id"] == jid)
    assert job["state"] == "done"

    manifest = client.get("/airports/JFK/manifests/current").json()
    assert manifest["version"] == 2
    assert manifest["row_count"] == 12

    root = config.lake_root()
    ids, scan_ids = set(), set()
    for f in manifest["files"]:
        t = pq.read_table(root / f["relative_path"], columns=["row_id", "scan_id"])
        chunk = t.column("row_id").to_pylist()
        assert not (ids & set(chunk))
        ids |= set(chunk)
        scan_ids |= set(t.column("scan_id").to_pylist())
    assert len(ids) == 12
    assert {s for s in scan_ids if s.startswith("D")} == {f"D{i}" for i in range(5)}


def test_timezone_change_rejected_while_job_active(client):
    register(client)
    ingest(client, "JFK", [make_scan("S1", "2024-11-03T06:30:00Z")])
    r = client.patch("/airports/JFK/timezone", json={"timezone": "America/Chicago"})
    assert r.status_code == 200

    again = client.patch("/airports/JFK/timezone", json={"timezone": "Asia/Shanghai"})
    assert again.status_code == 409
    assert again.json()["error"] == "repartition_in_progress"

    # Same timezone as the airport already has -> 409 too.
    same = client.patch("/airports/JFK/timezone", json={"timezone": "America/Chicago"})
    assert same.status_code == 409


def test_empty_airport_timezone_change_completes(client):
    register(client)
    r = client.patch("/airports/JFK/timezone", json={"timezone": "UTC"})
    assert r.status_code == 200
    run_job(client, "JFK")
    progress = client.get("/airports/JFK/progress").json()
    assert progress["airport"]["current_version"] == 2
    # Ingestion after the switch writes directly into the new version.
    r = ingest(client, "JFK", [make_scan("S1", "2024-11-03T00:30:00Z")])
    assert r.status_code == 200
    assert r.json()["version"] == 2
    assert r.json()["manifest"]["files"][0]["local_date"] == "2024-11-03"


# ---------------------------------------------------------------------------
# Downloads: preserve vs normalize
# ---------------------------------------------------------------------------


def test_download_single_file_preserve(client):
    register(client)
    ingest(client, "JFK", [make_scan("S1", "2024-11-03T06:30:00Z")])
    r = client.get("/airports/JFK/download", follow_redirects=False)
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/vnd.apache.parquet"
    table = pq.read_table(io.BytesIO(r.content))
    assert table.num_rows == 1


def test_download_zip_preserve_and_normalize_layouts(client):
    register(client)
    records = [
        make_scan("S1", "2024-11-03T06:30:00Z"),
        make_scan("S2", "2024-11-04T05:00:00Z"),
    ]
    ingest(client, "JFK", records)

    r = client.get("/airports/JFK/download?layout=preserve")
    assert r.status_code == 200
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    names = zf.namelist()
    assert all(n.startswith("airports/JFK/versions/v1/") for n in names)
    assert any("local_date=2024-11-03" in n for n in names)

    r = client.get("/airports/JFK/download?layout=normalize")
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    names = zf.namelist()
    assert all(n.startswith("local_date=") for n in names)
    assert len(names) == 2


def test_download_historical_version(client):
    register(client)
    ingest(client, "JFK", [make_scan("S1", "2024-11-03T06:30:00Z")])
    client.patch("/airports/JFK/timezone", json={"timezone": "America/Chicago"})
    run_job(client, "JFK")

    r = client.get("/airports/JFK/download?version=1")
    assert r.status_code == 200
    table = pq.read_table(io.BytesIO(r.content))
    assert table.num_rows == 1


def test_file_download_endpoint_with_jail(client):
    register(client)
    body = ingest(client, "JFK", [make_scan("S1", "2024-11-03T06:30:00Z")]).json()
    rel = body["manifest"]["files"][0]["relative_path"]
    r = client.get("/files/download", params={"path": rel})
    assert r.status_code == 200

    bad = client.get("/files/download", params={"path": "../../etc/passwd"})
    assert bad.status_code in (403, 404)


# ---------------------------------------------------------------------------
# Crash recovery
# ---------------------------------------------------------------------------


def test_recovery_resumes_interrupted_repartition_without_loss_or_dupes(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from app.main import app as fastapi_app

    # The autouse fixture already created and pointed the env at these dirs.
    db_dir = tmp_path / "db"
    lake_dir = tmp_path / "lake"

    with TestClient(fastapi_app) as c1:
        c1.post("/debug/worker/pause")
        register(c1)
        c1.post(
            "/ingest",
            json={
                "airport": "JFK",
                "records": [
                    make_scan(f"S{i:02d}", f"2024-11-0{(i % 4) + 1}T12:00:00Z") for i in range(10)
                ],
            },
        )
        c1.patch("/airports/JFK/timezone", json={"timezone": "America/Chicago"})
        for _ in range(2):
            c1.post("/debug/worker/tick")
        partial = c1.get("/airports/JFK/progress").json()["active_job"]
        assert partial["processed_rows"] > 0
        assert partial["state"] == "running"

    # Process died here. A brand-new client performs startup recovery and the
    # worker resumes the interrupted job to completion.
    with TestClient(fastapi_app) as c2:
        c2.post("/debug/worker/pause")
        run_job(c2, "JFK")
        manifest = c2.get("/airports/JFK/manifests/current").json()
        assert manifest["version"] == 2
        assert manifest["row_count"] == 10
        root = config.lake_root()
        ids = set()
        for f in manifest["files"]:
            ids |= set(
                pq.read_table(root / f["relative_path"], columns=["row_id"])
                .column("row_id")
                .to_pylist()
            )
        assert len(ids) == 10
        assert list(config.lake_root().rglob("*.tmp")) == []


def test_recovery_resumes_interrupted_ingest_batch(tmp_path, monkeypatch):
    """A batch whose raw rows committed but whose file publish crashed."""
    from fastapi.testclient import TestClient
    from app.main import app as fastapi_app

    # The autouse fixture already created and pointed the env at these dirs.
    db_dir = tmp_path / "db"
    lake_dir = tmp_path / "lake"

    with TestClient(fastapi_app) as c1:
        c1.post("/airports", json={"code": "JFK", "timezone": "America/New_York"})
        r = c1.post(
            "/debug/crash/ingest",
            json={"airport": "JFK", "scan_id": "X1", "utc_ts": "2024-11-03T06:30:00Z"},
        )
        assert r.status_code == 200, r.text
        # Raw scan is in SQLite, but no lake files were published.
        progress = c1.get("/airports/JFK/progress").json()
        assert progress["total_scans"] == 1
        assert progress["current_manifest"]["row_count"] == 0

    # Restart: startup recovery resumes the stuck batch and publishes files.
    with TestClient(fastapi_app) as c2:
        manifest = c2.get("/airports/JFK/manifests/current").json()
        assert manifest["row_count"] == 1
        progress = c2.get("/airports/JFK/progress").json()
        assert progress["total_scans"] == 1


# ---------------------------------------------------------------------------
# Independent airports
# ---------------------------------------------------------------------------


def test_concurrent_ingest_while_background_worker_runs(tmp_path, monkeypatch):
    """Ingestion overlapping an active job is included in the switch exactly once.

    The worker runs for real; we freeze the bulk phase, insert a deterministic
    set of "during-switch" scans while the job is active (they land in the old
    version), then resume the worker and assert the new version contains every
    history + during-switch scan exactly once.
    """
    from fastapi.testclient import TestClient
    from app.main import app as fastapi_app

    with TestClient(fastapi_app) as c:
        c.post("/debug/worker/pause")
        register(c, "JFK")
        c.post(
            "/ingest",
            json={
                "airport": "JFK",
                "records": [
                    make_scan(f"H{i}", "2024-11-03T06:30:00Z") for i in range(8)
                ],
            },
        )
        c.patch("/airports/JFK/timezone", json={"timezone": "America/Chicago"})

        # Freeze inside the bulk phase.
        tick_until(c, "running")
        tick(c)
        active = c.get("/airports/JFK/progress").json()["active_job"]
        assert active is not None and active["state"] == "running"

        # These scans arrive while repartition is in progress -> old version.
        during_n = 4
        r = c.post(
            "/ingest",
            json={
                "airport": "JFK",
                "records": [
                    make_scan(f"LIVE-{i}", "2024-11-04T02:00:00Z")
                    for i in range(during_n)
                ],
            },
        )
        assert r.status_code == 200
        assert r.json()["version"] == 1
        # Old manifest stays readable throughout.
        assert c.get("/airports/JFK/manifests/current").json()["version"] == 1

        # Let the real background worker finish the job.
        c.post("/debug/worker/resume")
        deadline_loops = 200
        while deadline_loops:
            if c.get("/airports/JFK/progress").json()["active_job"] is None:
                break
            deadline_loops -= 1
        assert c.get("/airports/JFK/progress").json()["active_job"] is None

        manifest = c.get("/airports/JFK/manifests/current").json()
        assert manifest["version"] == 2
        assert manifest["row_count"] == 8 + during_n
        root = config.lake_root()
        ids, scan_ids = set(), set()
        for f in manifest["files"]:
            t = pq.read_table(root / f["relative_path"], columns=["row_id", "scan_id"])
            ids |= set(t.column("row_id").to_pylist())
            scan_ids |= set(t.column("scan_id").to_pylist())
        assert len(ids) == 8 + during_n  # exactly once
        assert {f"LIVE-{i}" for i in range(during_n)} <= scan_ids

        # Ingestion after the switch writes into the new version.
        after = c.post(
            "/ingest",
            json={"airport": "JFK", "records": [make_scan("AFTER", "2024-11-05T02:00:00Z")]},
        )
        assert after.status_code == 200
        assert after.json()["version"] == 2
        assert c.get("/airports/JFK/manifests/current").json()["row_count"] == 9 + during_n


def test_recovery_after_crash_between_manifest_and_pointer_switch():
    """Crash after manifest publish but before the airport pointer flips.

    Emulated with a separate SQLite connection: the job is moved back to
    'publishing' and the current version pointer is rolled back, while the new
    version's manifest and files stay on disk. Recovery must finish the switch
    without publishing a duplicate manifest revision.
    """
    import sqlite3

    from fastapi.testclient import TestClient
    from app.main import app as fastapi_app

    with TestClient(fastapi_app) as c1:
        c1.post("/debug/worker/pause")
        register(c1)
        c1.post(
            "/ingest",
            json={
                "airport": "JFK",
                "records": [make_scan(f"S{i}", "2024-11-03T12:00:00Z") for i in range(4)],
            },
        )
        c1.patch("/airports/JFK/timezone", json={"timezone": "Asia/Shanghai"})
        run_job(c1, "JFK")
        assert c1.get("/airports/JFK/progress").json()["airport"]["current_version"] == 2

    # Reopen the system of record with an independent connection and recreate
    # the exact crash window: manifest published, pointer not switched.
    conn = sqlite3.connect(str(config.db_path()))
    conn.execute("UPDATE repartition_jobs SET state='publishing' WHERE airport='JFK'")
    conn.execute("UPDATE airports SET current_version=1 WHERE code='JFK'")
    conn.commit()
    conn.close()

    with TestClient(fastapi_app) as c2:
        c2.post("/debug/worker/pause")
        # Old pointer still readable immediately after the simulated restart.
        assert c2.get("/airports/JFK/manifests/current").json()["version"] == 1
        tick(c2)  # resumes the publishing job

        progress = c2.get("/airports/JFK/progress").json()
        assert progress["active_job"] is None
        assert progress["airport"]["current_version"] == 2
        manifests = c2.get("/airports/JFK/manifests").json()
        v2 = next(m for m in manifests if m["version"] == 2)
        assert v2["revision"] == 1  # no duplicate publish
        assert v2["status"] == "current"
        assert v2["row_count"] == 4


def test_airports_are_independent(client):
    register(client, "JFK", "America/New_York")
    register(client, "LAX", "America/Los_Angeles")
    ingest(client, "JFK", [make_scan("J1", "2024-11-03T06:30:00Z")])
    ingest(client, "LAX", [make_scan("L1", "2024-11-03T06:30:00Z")])

    jfk = client.get("/airports/JFK/manifests/current").json()
    lax = client.get("/airports/LAX/manifests/current").json()
    assert jfk["files"][0]["local_date"] == "2024-11-03"  # 01:30 NY
    assert lax["files"][0]["local_date"] == "2024-11-02"  # 22:30 LA previous day

    airports = client.get("/airports").json()
    assert {a["code"] for a in airports} == {"JFK", "LAX"}
