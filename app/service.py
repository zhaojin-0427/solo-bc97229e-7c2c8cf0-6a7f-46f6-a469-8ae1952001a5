"""Core lakehouse logic: ingestion, idempotent dedup, repartition state machine.

Concurrency model
-----------------
The service runs a single uvicorn worker (see Dockerfile). Per-airport
``asyncio.Lock`` objects serialize ingestion and the repartition drain/publish
phases for the same airport; different airports stay fully independent.

Durability model
----------------
SQLite (WAL, synchronous=FULL) is the system of record. Every Parquet file is
written atomically (temp file + fsync + rename) *before* its row is inserted,
and a manifest is only marked current once every file it lists exists. Any
crash therefore leaves one of: file without DB row (orphan, cleaned on
startup), or DB row without manifest (resumed on startup) - never a published
file that is incomplete, and never a published manifest missing a file.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from . import config, storage
from .database import db
from .schemas import (
    AirportOut,
    IngestRequest,
    IngestResponse,
    LakeFileOut,
    ManifestOut,
    RepartitionJobOut,
)

UTC = timezone.utc


class ServiceError(Exception):
    def __init__(self, status: int, error: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.status = status
        self.error = error
        self.message = message
        self.details = details or {}


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# Per-airport async locks (single process -> in-memory map is sufficient).
_locks: dict[str, asyncio.Lock] = {}
# Test gate: when False the background worker pauses between iterations.
# A plain boolean (not asyncio.Event) so it can be flipped from any loop or
# from synchronous test code; the worker observes it at each iteration.
worker_paused: bool = False


class _GateProxy:
    """Event-like proxy backed by the loop-independent ``worker_paused`` flag."""

    def set(self) -> None:
        global worker_paused
        worker_paused = False

    def clear(self) -> None:
        global worker_paused
        worker_paused = True

    async def wait(self) -> None:
        while worker_paused:
            await asyncio.sleep(config.worker_interval_s())

    def is_set(self) -> bool:
        return not worker_paused


worker_gate = _GateProxy()


def airport_lock(code: str) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = _locks.get(code)
    if lock is None or lock._loop is not loop:
        lock = asyncio.Lock()
        _locks[code] = lock
    return lock


# ---------------------------------------------------------------------------
# Airport registration / timezone change
# ---------------------------------------------------------------------------


async def register_airport(code: str, tz_name: str) -> AirportOut:
    now = _iso(_now())
    async with db.transaction() as conn:
        row = await conn.execute(
            "SELECT * FROM airports WHERE code=?", (code,)
        )
        existing = await row.fetchone()
        if existing is not None:
            if existing["timezone"] != tz_name:
                raise ServiceError(
                    409,
                    "airport_exists",
                    f"airport {code} already registered with timezone "
                    f"{existing['timezone']!r}; use PATCH /airports/{code}/timezone to change it",
                    {"existing_timezone": existing["timezone"]},
                )
            return _airport_out(existing)
        await conn.execute(
            "INSERT INTO airports(code, timezone, current_version, created_at, updated_at)"
            " VALUES(?,?,?,?,?)",
            (code, tz_name, 1, now, now),
        )
        await conn.execute(
            "INSERT INTO versions(airport, version, timezone, state, created_at)"
            " VALUES(?,1,?,'active',?)",
            (code, tz_name, now),
        )
        row = await conn.execute("SELECT * FROM airports WHERE code=?", (code,))
        created = await row.fetchone()
    return _airport_out(created)


async def change_timezone(code: str, new_tz: str) -> RepartitionJobOut:
    now = _iso(_now())
    async with airport_lock(code):
        airport = await _get_airport_row(code)
        if airport["timezone"] == new_tz:
            raise ServiceError(
                409,
                "timezone_unchanged",
                f"airport {code} already uses timezone {new_tz!r}",
            )
        active = await db.query_all(
            "SELECT * FROM repartition_jobs WHERE airport=? AND state IN "
            "('pending','running','draining','publishing')",
            (code,),
        )
        if active:
            raise ServiceError(
                409,
                "repartition_in_progress",
                f"airport {code} already has an active repartition job "
                f"(id={active[0]['id']}, state={active[0]['state']}); "
                "another timezone change is rejected until it finishes",
                {"active_job_id": active[0]["id"], "state": active[0]["state"]},
            )
        new_version = airport["current_version"] + 1
        cur = await db.conn.execute(
            "INSERT INTO repartition_jobs(airport, from_timezone, to_timezone, new_version,"
            " state, created_at) VALUES(?,?,?,?,'pending',?)",
            (code, airport["timezone"], new_tz, new_version, now),
        )
        job_id = cur.lastrowid
        await db.conn.execute(
            "INSERT INTO versions(airport, version, timezone, state, created_at)"
            " VALUES(?,?,?,'building',?)",
            (code, new_version, new_tz, now),
        )
        await db.conn.execute(
            "UPDATE airports SET timezone=?, updated_at=? WHERE code=?",
            (new_tz, now, code),
        )
        await db.commit()
        row = await db.query_one(
            "SELECT * FROM repartition_jobs WHERE id=?", (job_id,)
        )
    return _job_out(row)


async def _get_airport_row(code: str) -> dict:
    row = await db.query_one("SELECT * FROM airports WHERE code=?", (code,))
    if row is None:
        raise ServiceError(404, "airport_not_found", f"airport {code!r} is not registered")
    return row


# ---------------------------------------------------------------------------
# Batch ingestion
# ---------------------------------------------------------------------------


async def ingest_batch(req: IngestRequest) -> IngestResponse:
    airport = await _get_airport_row(req.airport)
    code = airport["code"]
    payload_hash = _batch_payload_hash(req)
    async with airport_lock(code):
        # Idempotent retransmit of the exact same batch.
        dup_batch = await db.query_one(
            "SELECT * FROM batches WHERE airport=? AND payload_hash=?",
            (code, payload_hash),
        )
        if dup_batch is not None and dup_batch["state"] == "landed":
            manifest = await get_current_manifest(code)
            return IngestResponse(
                status="duplicate",
                batch_id=dup_batch["id"],
                airport=code,
                version=manifest.version,
                timezone=manifest.timezone,
                received=len(req.records),
                inserted=0,
                duplicates=dup_batch["record_count"],
                manifest=manifest,
            )

        # Whole-batch rejection: same (airport, scan_id) with different content.
        conflicts = await _find_content_conflicts(code, req)
        if conflicts:
            raise ServiceError(
                409,
                "content_conflict",
                "one or more scan ids already exist with different content; "
                "the whole batch is rejected and nothing is stored",
                {"conflicts": conflicts},
            )

        now = _now()
        now_s = _iso(now)
        if dup_batch is not None:
            # A previous attempt crashed mid-flight: resume it.
            batch_id = dup_batch["id"]
            await db.execute(
                "UPDATE batches SET state='received', error=NULL WHERE id=?",
                (batch_id,),
            )
        else:
            cur = await db.execute(
                "INSERT INTO batches(airport, payload_hash, state, record_count, created_at)"
                " VALUES(? ,?,'received',?,?)",
                (code, payload_hash, len(req.records), now_s),
            )
            batch_id = cur.lastrowid
        await db.commit()

        # Insert raw rows (SQLite UNIQUE(airport, scan_id) is the hard guard).
        inserted_rows, duplicate_count = await _insert_raw_scans(code, req, batch_id, now_s)

        version = airport["current_version"]
        tz_name = await _version_timezone(code, version)
        if inserted_rows:
            await _publish_ingest_files(code, version, tz_name, batch_id, inserted_rows, now_s)
        await db.execute(
            "UPDATE batches SET state='landed', inserted=?, duplicates=?, landed_at=? WHERE id=?",
            (len(inserted_rows), duplicate_count, now_s, batch_id),
        )
        await db.commit()

    manifest = await get_current_manifest(code)
    return IngestResponse(
        status="accepted",
        batch_id=batch_id,
        airport=code,
        version=manifest.version,
        timezone=manifest.timezone,
        received=len(req.records),
        inserted=len(inserted_rows),
        duplicates=duplicate_count,
        manifest=manifest,
    )


def _batch_payload_hash(req: IngestRequest) -> str:
    import hashlib

    payload = {
        "airport": req.airport,
        "records": [r.canonical() for r in req.records],
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(blob).hexdigest()


async def _find_content_conflicts(code: str, req: IngestRequest) -> list[dict]:
    ids = [r.id for r in req.records]
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    existing = await db.query_all(
        f"SELECT scan_id, content_hash FROM raw_scans WHERE airport=? AND scan_id IN ({placeholders})",
        (code, *ids),
    )
    by_id = {r["scan_id"]: r["content_hash"] for r in existing}
    conflicts = []
    for rec in req.records:
        if rec.id in by_id and by_id[rec.id] != rec.content_hash():
            conflicts.append({"id": rec.id, "stored_hash": by_id[rec.id], "incoming_hash": rec.content_hash()})
    return conflicts


async def _insert_raw_scans(
    code: str, req: IngestRequest, batch_id: int, now_s: str
) -> tuple[list[dict], int]:
    inserted: list[dict] = []
    duplicates = 0
    for rec in req.records:
        try:
            cur = await db.execute(
                "INSERT INTO raw_scans(airport, scan_id, content_hash, utc_ts, event_type,"
                " location, attributes, batch_id, ingested_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    code,
                    rec.id,
                    rec.content_hash(),
                    _iso(rec.utc_ts.astimezone(UTC)),
                    rec.event_type,
                    rec.location,
                    json.dumps(rec.attributes, sort_keys=True, ensure_ascii=False),
                    batch_id,
                    now_s,
                ),
            )
            await db.commit()
        except Exception as exc:  # sqlite3.IntegrityError wrapped by aiosqlite
            if _is_unique_violation(exc):
                duplicates += 1
                continue
            raise
        inserted.append(
            {
                "row_id": cur.lastrowid,
                "scan_id": rec.id,
                "airport": code,
                "utc_ts": _iso(rec.utc_ts.astimezone(UTC)),
                "event_type": rec.event_type,
                "location": rec.location,
                "attributes": rec.attributes,
                "ingested_at": now_s,
            }
        )
    return inserted, duplicates


def _is_unique_violation(exc: Exception) -> bool:
    import sqlite3

    inner = exc
    while inner is not None:
        if isinstance(inner, sqlite3.IntegrityError):
            return True
        inner = getattr(inner, "__cause__", None)
    return False


async def _version_timezone(code: str, version: int) -> str:
    row = await db.query_one(
        "SELECT timezone FROM versions WHERE airport=? AND version=?", (code, version)
    )
    assert row is not None
    return row["timezone"]


async def _publish_ingest_files(
    code: str,
    version: int,
    tz_name: str,
    batch_id: int,
    rows: list[dict],
    now_s: str,
) -> None:
    """Write one Parquet file per local date, then publish a new manifest revision."""
    by_date: dict[str, list[dict]] = {}
    for r in rows:
        d = storage.local_date_for(_utc_us(r["utc_ts"]), tz_name).isoformat()
        by_date.setdefault(d, []).append(r)

    seq = await _next_ingest_seq(code, version, batch_id)
    new_files: list[dict] = []
    for local_date in sorted(by_date):
        rel = (
            f"airports/{code}/versions/v{version}/ingest/"
            f"local_date={local_date}/ingest-b{batch_id}-s{seq}.parquet"
        )
        dest = config.lake_root() / rel
        row_count, sha, size = await asyncio.to_thread(
            storage.write_parquet_atomic, by_date[local_date], tz_name, dest
        )
        new_files.append(
            {
                "local_date": local_date,
                "relative_path": rel,
                "row_count": row_count,
                "size_bytes": size,
                "sha256": sha,
            }
        )
        seq += 1
    await _publish_manifest(code, version, tz_name, new_files, source="ingest", now_s=now_s)


async def _next_ingest_seq(code: str, version: int, batch_id: int) -> int:
    row = await db.query_one(
        "SELECT COUNT(*) AS n FROM lake_files WHERE airport=? AND version=? AND source='ingest'",
        (code, version),
    )
    return (row["n"] if row else 0) + 1


# ---------------------------------------------------------------------------
# Manifest materialization (single timezone per manifest, by construction)
# ---------------------------------------------------------------------------


async def _publish_manifest(
    code: str,
    version: int,
    tz_name: str,
    new_files: list[dict],
    source: str,
    now_s: str,
    attach_existing: bool = False,
) -> int:
    """Publish a new current manifest once every listed file is durable.

    Two modes:
    * ``attach_existing=False`` (ingest): the Parquet files were just written
      and their rows are inserted here as part of the manifest transaction;
    * ``attach_existing=True`` (repartition): files were pre-registered with
      ``manifest_id=0`` and are re-attached to this manifest instead.

    Files always hit disk (temp + fsync + atomic rename) *before* the manifest
    transaction commits, so a manifest can never reference a missing file.
    """
    await db.ensure_idle()
    rev_row = await db.query_one(
        "SELECT COALESCE(MAX(revision),0) AS r FROM manifests WHERE airport=? AND version=?",
        (code, version),
    )
    existing_rev = rev_row["r"] if rev_row else 0
    async with db.transaction() as conn:
        if attach_existing and existing_rev > 0:
            # Crash recovery: this version's manifest was already published
            # before the pointer switch. Re-attach files and return it instead
            # of creating a duplicate revision.
            row = await conn.execute(
                "SELECT id FROM manifests WHERE airport=? AND version=? "
                "ORDER BY revision DESC LIMIT 1",
                (code, version),
            )
            manifest_id = (await row.fetchone())["id"]
            await conn.execute(
                "UPDATE lake_files SET manifest_id=? WHERE airport=? AND version=?"
                " AND source='repartition'",
                (manifest_id, code, version),
            )
            revision = existing_rev
        else:
            revision = existing_rev + 1
            cur = await conn.execute(
                "INSERT INTO manifests(airport, version, revision, is_current, file_count,"
                " row_count, published_at) VALUES(?,?,?,0,0,0,?)",
                (code, version, revision, now_s),
            )
            manifest_id = cur.lastrowid
            if attach_existing:
                await conn.execute(
                    "UPDATE lake_files SET manifest_id=? WHERE airport=? AND version=?"
                    " AND source='repartition'",
                    (manifest_id, code, version),
                )
            else:
                for f in new_files:
                    await conn.execute(
                        "INSERT INTO lake_files(airport, version, manifest_id, timezone,"
                        " local_date, relative_path, row_count, size_bytes, sha256,"
                        " source, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            code,
                            version,
                            manifest_id,
                            tz_name,
                            f["local_date"],
                            f["relative_path"],
                            f["row_count"],
                            f["size_bytes"],
                            f["sha256"],
                            source,
                            now_s,
                        ),
                    )
        totals_row = await conn.execute(
            "SELECT COUNT(*) AS fc, COALESCE(SUM(row_count),0) AS rc FROM lake_files "
            "WHERE airport=? AND version=?",
            (code, version),
        )
        totals = await totals_row.fetchone()
        await conn.execute(
            "UPDATE manifests SET file_count=?, row_count=? WHERE id=?",
            (totals["fc"], totals["rc"], manifest_id),
        )
        # Only one current manifest per airport; all files of a version share one tz.
        await conn.execute(
            "UPDATE manifests SET is_current=0 WHERE airport=? AND is_current=1", (code,)
        )
        await conn.execute("UPDATE manifests SET is_current=1 WHERE id=?", (manifest_id,))

    all_files = await db.query_all(
        "SELECT * FROM lake_files WHERE airport=? AND version=? ORDER BY local_date, id",
        (code, version),
    )
    manifest_payload = {
        "airport": code,
        "version": version,
        "timezone": tz_name,
        "revision": revision,
        "published_at": now_s,
        "file_count": totals["fc"],
        "row_count": totals["rc"],
        "files": [
            {
                "relative_path": f["relative_path"],
                "local_date": f["local_date"],
                "row_count": f["row_count"],
                "size_bytes": f["size_bytes"],
                "sha256": f["sha256"],
                "source": f["source"],
            }
            for f in all_files
        ],
    }
    await asyncio.to_thread(
        storage.write_manifest_atomic, manifest_payload, code, version, revision
    )
    return manifest_id


async def get_current_manifest(code: str) -> ManifestOut:
    airport = await _get_airport_row(code)
    return await _load_manifest(code, airport["current_version"])


async def get_manifest(code: str, version: int) -> ManifestOut:
    await _get_airport_row(code)
    v = await db.query_one(
        "SELECT 1 FROM versions WHERE airport=? AND version=?", (code, version)
    )
    if v is None:
        raise ServiceError(404, "version_not_found", f"version {version} of airport {code} not found")
    return await _load_manifest(code, version)


async def _load_manifest(code: str, version: int) -> ManifestOut:
    m = await db.query_one(
        "SELECT * FROM manifests WHERE airport=? AND version=? ORDER BY revision DESC LIMIT 1",
        (code, version),
    )
    if m is None:
        # Empty airport: no manifest has been published yet.
        tz_row = await db.query_one(
            "SELECT timezone FROM versions WHERE airport=? AND version=?", (code, version)
        )
        return ManifestOut(
            manifest_id=0,
            airport=code,
            version=version,
            timezone=tz_row["timezone"] if tz_row else "",
            revision=0,
            status="current",
            file_count=0,
            row_count=0,
            files=[],
            published_at=_now(),
        )
    files = await db.query_all(
        "SELECT * FROM lake_files WHERE manifest_id=? ORDER BY local_date, id", (m["id"],)
    )
    return ManifestOut(
        manifest_id=m["id"],
        airport=code,
        version=version,
        timezone=await _version_timezone(code, version),
        revision=m["revision"],
        status="current" if m["is_current"] else "superseded",
        file_count=m["file_count"],
        row_count=m["row_count"],
        files=[_file_out(f) for f in files],
        published_at=_parse_iso(m["published_at"]),
    )


async def list_manifests(code: str) -> list[ManifestOut]:
    await _get_airport_row(code)
    rows = await db.query_all(
        "SELECT DISTINCT version FROM manifests WHERE airport=? ORDER BY version", (code,)
    )
    out: list[ManifestOut] = []
    for r in rows:
        out.append(await _load_manifest(code, r["version"]))
    return out


# ---------------------------------------------------------------------------
# Repartition state machine (background worker)
# ---------------------------------------------------------------------------


async def recover_on_startup() -> dict:
    """Reconcile disk and DB after a crash; resume every unfinished unit of work."""
    removed = await asyncio.to_thread(storage.cleanup_orphan_tmp)
    removed += await _cleanup_orphan_files()

    # Batches that had raw rows but never published their files.
    stuck = await db.query_all("SELECT * FROM batches WHERE state='received'")
    for b in stuck:
        await _resume_batch(b)

    # Jobs interrupted mid-flight go back to running; their files are idempotent
    # because parts are named after the covered row-id ranges and local dates.
    jobs = await db.query_all(
        "SELECT * FROM repartition_jobs WHERE state IN "
        "('pending','running','draining','publishing') ORDER BY id"
    )
    for j in jobs:
        if j["state"] != "pending":
            await db.execute(
                "UPDATE repartition_jobs SET state='running' WHERE id=?", (j["id"],)
            )
            await db.commit()
        await _rebuild_job_progress(j["id"])
    return {
        "orphan_tmp_removed": removed,
        "resumed_batches": len(stuck),
        "resumed_jobs": len(jobs),
    }


async def _cleanup_orphan_files() -> list[str]:
    """Delete on-disk Parquet files that have no lake_files row (crash after rename)."""
    root = config.lake_root()
    if not root.exists():
        return []
    registered = {
        r["relative_path"]
        for r in await db.query_all("SELECT relative_path FROM lake_files")
    }
    removed: list[str] = []
    for p in root.rglob("*.parquet"):
        rel = str(p.relative_to(root))
        if rel not in registered:
            try:
                p.unlink()
                removed.append(rel)
            except FileNotFoundError:
                pass
    return removed


async def _resume_batch(batch_row) -> None:
    code = batch_row["airport"]
    batch_id = batch_row["id"]
    rows_q = await db.query_all(
        "SELECT * FROM raw_scans WHERE batch_id=? ORDER BY id", (batch_id,)
    )
    if not rows_q:
        return
    airport = await _get_airport_row(code)
    version = airport["current_version"]
    tz_name = await _version_timezone(code, version)
    already = await db.query_all(
        "SELECT relative_path FROM lake_files WHERE source='ingest' "
        "AND airport=? AND relative_path LIKE ?",
        (code, f"airports/{code}/versions/v%/ingest/%/ingest-b{batch_id}-%.parquet"),
    )
    if not already:
        # Crash happened before any file was registered: write them now.
        rows = [_raw_row_to_file_row(r) for r in rows_q]
        await _publish_ingest_files(
            code, version, tz_name, batch_id, rows, _iso(_now())
        )
    dup = batch_row["record_count"] - len(rows_q)
    await db.execute(
        "UPDATE batches SET state='landed', inserted=?, duplicates=?, landed_at=? WHERE id=?",
        (len(rows_q), max(dup, 0), _iso(_now()), batch_id),
    )
    await db.commit()


async def _rebuild_job_progress(job_id: int) -> None:
    job = await db.query_one("SELECT * FROM repartition_jobs WHERE id=?", (job_id,))
    files = await db.query_all(
        "SELECT relative_path, COALESCE(row_count,0) AS row_count FROM lake_files "
        "WHERE source='repartition' AND relative_path LIKE ?",
        (
            f"airports/{job['airport']}/versions/v{job['new_version']}/repartition/"
            f"job-{job_id}/%",
        ),
    )
    chunks = len(files)
    rows = sum(f["row_count"] for f in files)
    max_id = 0
    for f in files:
        # Names end with -<last_row_id>.parquet.
        token = Path(f["relative_path"]).stem.rsplit("-", 1)[-1]
        if token.isdigit():
            max_id = max(max_id, int(token))
    await db.execute(
        "UPDATE repartition_jobs SET chunks_done=?, processed_rows=?, drained_rows=?,"
        " last_processed_id=? WHERE id=?",
        (chunks, rows, rows, max_id, job_id),
    )
    await db.commit()


async def worker_loop() -> None:
    while True:
        await worker_gate.wait()
        try:
            await _process_one_job(single_step=False)
        except asyncio.CancelledError:
            raise
        except Exception:  # keep the loop alive; failed jobs carry their error
            import traceback

            traceback.print_exc()
        await asyncio.sleep(config.worker_interval_s())


async def process_one_job() -> bool:
    """Public wrapper used by the test/debug tick endpoint.

    One call advances a job by a single phase boundary (one bulk chunk, or the
    drain+publish transition), so callers can observe intermediate states.
    """
    return await _process_one_job(single_step=True)


async def _process_one_job(single_step: bool = False) -> bool:
    job = await db.query_one(
        "SELECT * FROM repartition_jobs WHERE state IN ('pending','running','draining','publishing')"
        " ORDER BY id LIMIT 1"
    )
    if job is None:
        return False
    # Block until the airport lock is free. Ingestion and the drain+publish
    # transition therefore never overlap: once the worker starts the final
    # drain, every scan committed up to that point (and every scan that was
    # already waiting on this lock) is observed before the pointer switch.
    async with airport_lock(job["airport"]):
        job = await db.query_one("SELECT * FROM repartition_jobs WHERE id=?", (job["id"],))
        if job is None or job["state"] in ("done", "failed"):
            return False
        try:
            if job["state"] in ("pending", "running"):
                bulk_finished = await _run_repartition(job, single_step=single_step)
                if single_step and not bulk_finished:
                    # One bulk chunk per tick: release the lock having made
                    # progress, so ingestion can interleave between chunks.
                    return True
                job = await db.query_one("SELECT * FROM repartition_jobs WHERE id=?", (job["id"],))
            # Bulk completed within this same lock holding (or we resume in
            # draining/publishing): the drain check, verification and pointer
            # switch run atomically, so no ingestion can sneak in between the
            # drain and the switch and every in-flight scan is covered once.
            await _drain_and_publish(job)
        except Exception as exc:
            import traceback

            traceback.print_exc()
            # Roll back any half-open transaction so the connection stays usable.
            try:
                await db.conn.rollback()
            except Exception:
                pass
            await db.execute(
                "UPDATE repartition_jobs SET state='failed', error=?, finished_at=? WHERE id=?",
                (repr(exc), _iso(_now()), job["id"]),
            )
            await db.commit()
        return True


async def _run_repartition(job, single_step: bool = False) -> bool:
    """Bulk phase: rewrite every scan that existed when the job was created.

    Rows inserted afterwards (during the run) are caught by the drain phase,
    guaranteeing scans received while repartitioning land in the new version
    exactly once. ``single_step`` processes at most one chunk per call so the
    worker (and tests) can interleave ingestion and observe progress.

    Returns True once the bulk snapshot is fully covered.
    """
    jid = job["id"]
    code = job["airport"]
    new_version = job["new_version"]
    tz_name = job["to_timezone"]

    if job["state"] == "pending":
        total = await db.query_one(
            "SELECT MAX(id) AS max_id, COUNT(*) AS n FROM raw_scans WHERE airport=?",
            (code,),
        )
        await db.execute(
            "UPDATE repartition_jobs SET state='running', started_at=?, total_rows=?,"
            " bulk_high_id=? WHERE id=?",
            (_iso(_now()), total["n"], total["max_id"] or 0, jid),
        )
        await db.commit()
        job = await db.query_one("SELECT * FROM repartition_jobs WHERE id=?", (jid,))

    if job["bulk_high_id"] <= job["last_processed_id"]:
        return True  # bulk phase already complete (resumed after a crash)

    while True:
        job = await db.query_one("SELECT * FROM repartition_jobs WHERE id=?", (jid,))
        rows_q = await db.query_all(
            "SELECT * FROM raw_scans WHERE airport=? AND id>? ORDER BY id LIMIT ?",
            (code, job["last_processed_id"], config.chunk_size()),
        )
        if not rows_q:
            return True
        rows = [_raw_row_to_file_row(r) for r in rows_q]
        await _write_job_files(job, rows, tz_name, phase="bulk")
        job = await db.query_one("SELECT * FROM repartition_jobs WHERE id=?", (jid,))
        if job["last_processed_id"] >= job["bulk_high_id"]:
            return True
        if single_step:
            return False


async def _drain_and_publish(job) -> None:
    """Drain scans ingested while the bulk phase ran, then atomically switch.

    Handles all three entry states:
    * ``running``/``pending`` - perform the drain, verify, publish and switch;
    * ``draining`` - resume from an interrupted drain;
    * ``publishing`` - a crash happened after manifest publication but before
      the pointer switch; the manifest already exists, so only flip pointers.
    """
    jid = job["id"]
    code = job["airport"]
    new_version = job["new_version"]
    tz_name = job["to_timezone"]

    if job["state"] != "publishing":
        if job["state"] in ("running", "pending"):
            await db.execute(
                "UPDATE repartition_jobs SET state='draining' WHERE id=?", (jid,)
            )
            await db.commit()
        elif job["state"] != "draining":
            raise RuntimeError(f"job {jid} entered publish in unexpected state {job['state']!r}")

        # Single drain pass while holding the airport lock: no ingestion can
        # commit concurrently, so when this query returns empty the raw table
        # is fully covered. The row_id set verification below is the safety net.
        while True:
            job = await db.query_one("SELECT * FROM repartition_jobs WHERE id=?", (jid,))
            rows_q = await db.query_all(
                "SELECT * FROM raw_scans WHERE airport=? AND id>? ORDER BY id LIMIT ?",
                (code, job["last_processed_id"], config.chunk_size()),
            )
            if not rows_q:
                break
            rows = [_raw_row_to_file_row(r) for r in rows_q]
            await _write_job_files(job, rows, tz_name, phase="drain")

        # Verification: the new version must contain every raw scan exactly once.
        total = await db.query_one(
            "SELECT COUNT(*) AS n FROM raw_scans WHERE airport=?", (code,)
        )
        covered = await db.query_one(
            "SELECT COALESCE(SUM(row_count),0) AS n FROM lake_files "
            "WHERE airport=? AND version=? AND source='repartition'",
            (code, new_version),
        )
        if total["n"] != covered["n"]:
            raise RuntimeError(
                f"repartition row coverage mismatch for {code}: raw={total['n']} "
                f"files={covered['n']}"
            )
        await _verify_exactly_once(code, new_version, total["n"])

        await db.execute(
            "UPDATE repartition_jobs SET state='publishing' WHERE id=?", (jid,)
        )
        await db.commit()

        new_files = await db.query_all(
            "SELECT * FROM lake_files WHERE airport=? AND version=? AND source='repartition'"
            " ORDER BY local_date, id",
            (code, new_version),
        )
        now_s = _iso(_now())
        # Publish manifest first (files already durable), then flip pointers.
        await _publish_manifest(
            code,
            new_version,
            tz_name,
            [
                {
                    "local_date": f["local_date"],
                    "relative_path": f["relative_path"],
                    "row_count": f["row_count"],
                    "size_bytes": f["size_bytes"],
                    "sha256": f["sha256"],
                }
                for f in new_files
            ],
            source="repartition",
            now_s=now_s,
            attach_existing=True,
        )

    # Pointer switch is idempotent: safe whether this is the normal path or a
    # restart that found the job stuck in 'publishing' (manifest already out).
    now_s = _iso(_now())
    async with db.transaction() as conn:
        await conn.execute(
            "UPDATE airports SET current_version=?, updated_at=? WHERE code=?",
            (new_version, now_s, code),
        )
        await conn.execute(
            "UPDATE versions SET state='active' WHERE airport=? AND version=?",
            (code, new_version),
        )
        await conn.execute(
            "UPDATE versions SET state='abandoned' WHERE airport=? AND version<>?",
            (code, new_version),
        )
        await conn.execute(
            "UPDATE repartition_jobs SET state='done', finished_at=? WHERE id=?",
            (now_s, jid),
        )


async def _verify_exactly_once(code: str, version: int, expected: int) -> None:
    import pyarrow.parquet as pq

    files = await db.query_all(
        "SELECT relative_path FROM lake_files WHERE airport=? AND version=? ORDER BY id",
        (code, version),
    )
    ids: set[int] = set()
    for f in files:
        table = await asyncio.to_thread(
            pq.read_table, config.lake_root() / f["relative_path"], columns=["row_id"]
        )
        chunk = set(table.column("row_id").to_pylist())
        if ids & chunk:
            raise RuntimeError("overlapping row_id ranges in repartition output")
        ids |= chunk
    if len(ids) != expected:
        raise RuntimeError(
            f"exactly-once check failed: unique rows={len(ids)} expected={expected}"
        )


async def _write_job_files(job, rows: list[dict], tz_name: str, phase: str) -> None:
    """Write one Parquet part per local date and register all parts atomically.

    File names embed the phase, part sequence and the highest covered raw row
    id, so re-running a chunk after a crash either skips a registered file or
    overwrites an unregistered one - never duplicates a row.
    """
    jid = job["id"]
    code = job["airport"]
    new_version = job["new_version"]
    last_id = rows[-1]["row_id"]
    by_date: dict[str, list[dict]] = {}
    for r in rows:
        d = storage.local_date_for(_utc_us(r["utc_ts"]), tz_name).isoformat()
        by_date.setdefault(d, []).append(r)

    seq = job["chunks_done"]
    written: list[dict] = []
    for local_date in sorted(by_date):
        seq += 1
        folder = (
            f"airports/{code}/versions/v{new_version}/repartition/job-{jid}"
            + ("/drain" if phase == "drain" else "")
        )
        prefix = "drain" if phase == "drain" else "part"
        rel = (
            f"{folder}/local_date={local_date}/{prefix}-{seq}-{last_id}.parquet"
        )
        if await _file_registered(rel):
            continue
        dest = config.lake_root() / rel
        row_count, sha, size = await asyncio.to_thread(
            storage.write_parquet_atomic, by_date[local_date], tz_name, dest
        )
        written.append(
            {
                "local_date": local_date,
                "relative_path": rel,
                "row_count": row_count,
                "size_bytes": size,
                "sha256": sha,
            }
        )

    now_s = _iso(_now())
    added_rows = 0
    async with db.transaction() as conn:
        for f in written:
            await conn.execute(
                "INSERT INTO lake_files(airport, version, manifest_id, timezone, local_date,"
                " relative_path, row_count, size_bytes, sha256, source, created_at)"
                " VALUES(?,?,0,?,?,?,?,?,?, 'repartition', ?)",
                (
                    code,
                    new_version,
                    tz_name,
                    f["local_date"],
                    f["relative_path"],
                    f["row_count"],
                    f["size_bytes"],
                    f["sha256"],
                    now_s,
                ),
            )
            added_rows += f["row_count"]
        await conn.execute(
            "UPDATE repartition_jobs SET last_processed_id=?, chunks_done=chunks_done+?,"
            " processed_rows=processed_rows+?, drained_rows=drained_rows+? WHERE id=?",
            (last_id, len(written), added_rows, added_rows, jid),
        )


async def _file_registered(rel: str) -> bool:
    row = await db.query_one(
        "SELECT 1 FROM lake_files WHERE relative_path=?", (rel,)
    )
    return row is not None


# ---------------------------------------------------------------------------
# Progress / queries
# ---------------------------------------------------------------------------


async def get_progress(code: str) -> dict:
    airport_row = await _get_airport_row(code)
    job = await db.query_one(
        "SELECT * FROM repartition_jobs WHERE airport=? AND state IN "
        "('pending','running','draining','publishing') ORDER BY id DESC LIMIT 1",
        (code,),
    )
    total = await db.query_one(
        "SELECT COUNT(*) AS n FROM raw_scans WHERE airport=?", (code,)
    )
    manifest = await get_current_manifest(code)
    return {
        "airport": _airport_out(airport_row),
        "active_job": _job_out(job) if job else None,
        "total_scans": total["n"],
        "current_manifest": manifest,
    }


async def list_jobs(code: str) -> list[RepartitionJobOut]:
    await _get_airport_row(code)
    rows = await db.query_all(
        "SELECT * FROM repartition_jobs WHERE airport=? ORDER BY id", (code,)
    )
    return [_job_out(r) for r in rows]


async def list_jobs_for_debug() -> list[dict]:
    rows = await db.query_all(
        "SELECT airport, state FROM repartition_jobs ORDER BY id DESC"
    )
    return [{"airport": r["airport"], "state": r["state"]} for r in rows]


async def inject_crashed_ingest(code: str, scan_id: str, utc_iso: str) -> dict:
    """Test aid: leave a 'received' batch + raw scan but no Parquet files."""
    await _get_airport_row(code)
    now_s = _iso(_now())
    cur = await db.execute(
        "INSERT INTO batches(airport, payload_hash, state, record_count, created_at)"
        " VALUES(?, 'crashed-payload', 'received', 1, ?)",
        (code, now_s),
    )
    await db.execute(
        "INSERT INTO raw_scans(airport, scan_id, content_hash, utc_ts, event_type,"
        " location, attributes, batch_id, ingested_at) VALUES(?,?,?,?,?,?,?,?,?)",
        (code, scan_id, "crashed-hash", utc_iso, "SCAN", None, "{}", cur.lastrowid, now_s),
    )
    await db.commit()
    return {"injected": True, "batch_id": cur.lastrowid, "scan_id": scan_id}


async def list_airports() -> list[AirportOut]:
    rows = await db.query_all("SELECT * FROM airports ORDER BY code")
    return [_airport_out(r) for r in rows]


# ---------------------------------------------------------------------------
# Row mapping helpers
# ---------------------------------------------------------------------------


def _utc_us(iso_s: str) -> int:
    dt = datetime.fromisoformat(iso_s.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000)


def _raw_row_to_file_row(r) -> dict:
    return {
        "row_id": r["id"],
        "scan_id": r["scan_id"],
        "airport": r["airport"],
        "utc_ts": r["utc_ts"],
        "event_type": r["event_type"],
        "location": r["location"],
        "ingested_at": r["ingested_at"],
        "attributes": json.loads(r["attributes"]),
    }


def _airport_out(r) -> AirportOut:
    return AirportOut(
        code=r["code"],
        timezone=r["timezone"],
        current_version=r["current_version"],
        created_at=_parse_iso(r["created_at"]),
        updated_at=_parse_iso(r["updated_at"]),
    )


def _job_out(r) -> RepartitionJobOut:
    return RepartitionJobOut(
        job_id=r["id"],
        airport=r["airport"],
        from_timezone=r["from_timezone"],
        to_timezone=r["to_timezone"],
        new_version=r["new_version"],
        state=r["state"],
        total_rows=r["total_rows"],
        processed_rows=r["processed_rows"],
        chunks_done=r["chunks_done"],
        drained_rows=r["drained_rows"],
        error=r["error"],
        created_at=_parse_iso(r["created_at"]),
        started_at=_parse_iso(r["started_at"]) if r["started_at"] else None,
        finished_at=_parse_iso(r["finished_at"]) if r["finished_at"] else None,
    )


def _file_out(r) -> LakeFileOut:
    return LakeFileOut(
        file_id=r["id"],
        version=r["version"],
        timezone=r["timezone"],
        local_date=r["local_date"],
        relative_path=r["relative_path"],
        row_count=r["row_count"],
        size_bytes=r["size_bytes"],
        sha256=r["sha256"],
        source=r["source"],
        created_at=_parse_iso(r["created_at"]),
    )
