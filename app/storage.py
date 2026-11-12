"""Physical Parquet storage: paths, atomic file writes and downloads.

Layout under ``$LAKE_ROOT``::

    airports/<CODE>/versions/v<N>/manifest-latest.json
    airports/<CODE>/versions/v<N>/manifest/m<REV>.json
    airports/<CODE>/versions/v<N>/ingest/ingest-<BATCH>-<SEQ>.parquet
    airports/<CODE>/versions/v<N>/repartition/<JOB>/part-<K>-<lastrowid>.parquet
    airports/<CODE>/versions/v<N>/repartition/<JOB>/drain/drain-<SEQ>-<lastrowid>.parquet

Every Parquet file is first written to a ``.tmp`` sibling and atomically
renamed into place. A file is therefore either complete or absent - a crashed
process can never leave a published half-written file.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import zipfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from . import config

# Parquet schema shared by every file in the lake.
SCHEMA = pa.schema(
    [
        pa.field("row_id", pa.int64(), nullable=False),
        pa.field("scan_id", pa.string(), nullable=False),
        pa.field("airport", pa.string(), nullable=False),
        pa.field("utc_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("location", pa.string(), nullable=True),
        pa.field("attributes", pa.string(), nullable=True),  # JSON-encoded
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("local_date", pa.date32(), nullable=False),
        pa.field("timezone", pa.string(), nullable=False),
    ]
)

_SAFE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def airport_dir(code: str) -> Path:
    return config.lake_root() / "airports" / code


def version_dir(code: str, version: int) -> Path:
    return airport_dir(code) / "versions" / f"v{version}"


def ingest_dir(code: str, version: int) -> Path:
    return version_dir(code, version) / "ingest"


def job_dir(code: str, version: int, job_id: int) -> Path:
    return version_dir(code, version) / "repartition" / f"job-{job_id}"


def drain_dir(code: str, version: int, job_id: int) -> Path:
    return job_dir(code, version, job_id) / "drain"


def manifest_dir(code: str, version: int) -> Path:
    return version_dir(code, version) / "manifest"


def local_date_for(utc_ts_us: int, tz_name: str) -> datetime.date:
    """The airport-local calendar date of a UTC microsecond timestamp."""
    ts = datetime.fromtimestamp(utc_ts_us / 1_000_000, tz=ZoneInfo("UTC"))
    return ts.astimezone(ZoneInfo(tz_name)).date()


def _encode_rows(rows: list[dict], tz_name: str) -> pa.Table:
    tz = ZoneInfo(tz_name)
    encoded = []
    for r in rows:
        utc_dt = datetime.fromisoformat(r["utc_ts"]) if isinstance(r["utc_ts"], str) else r["utc_ts"]
        if utc_dt.tzinfo is None:
            raise ValueError("utc_ts must be timezone-aware")
        utc_dt = utc_dt.astimezone(ZoneInfo("UTC"))
        ingested = (
            datetime.fromisoformat(r["ingested_at"])
            if isinstance(r["ingested_at"], str)
            else r["ingested_at"]
        )
        if ingested.tzinfo is None:
            ingested = ingested.replace(tzinfo=ZoneInfo("UTC"))
        else:
            ingested = ingested.astimezone(ZoneInfo("UTC"))
        encoded.append(
            {
                "row_id": r["row_id"],
                "scan_id": r["scan_id"],
                "airport": r["airport"],
                "utc_ts": utc_dt,
                "event_type": r["event_type"],
                "location": r["location"],
                "attributes": json.dumps(r["attributes"], sort_keys=True, ensure_ascii=False),
                "ingested_at": ingested,
                "local_date": utc_dt.astimezone(tz).date(),
                "timezone": tz_name,
            }
        )
    return pa.Table.from_pylist(encoded, schema=SCHEMA)


def write_parquet_atomic(rows: list[dict], tz_name: str, dest: Path) -> tuple[int, str, int]:
    """Write rows to ``dest`` via temp-file + fsync + atomic rename.

    Returns (row_count, sha256, size_bytes) of the finished file.
    """
    _ensure_dir(dest.parent)
    table = _encode_rows(rows, tz_name)
    tmp = dest.with_name(dest.name + ".tmp")
    # A stale .tmp from a previous crash must never be renamed over.
    if tmp.exists():
        tmp.unlink()
    pq.write_table(table, tmp, compression="snappy", version="2.6")
    h = hashlib.sha256()
    with open(tmp, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    fd = os.open(tmp, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, dest)
    # fsync the directory so the rename itself is durable.
    dfd = os.open(dest.parent, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
    return table.num_rows, h.hexdigest(), dest.stat().st_size


def write_manifest_atomic(manifest: dict, code: str, version: int, revision: int) -> Path:
    """Materialize an immutable manifest JSON and refresh the latest pointer."""
    mdir = manifest_dir(code, version)
    _ensure_dir(mdir)
    dest = mdir / f"m{revision}.json"
    payload = json.dumps(manifest, sort_keys=True, ensure_ascii=False, indent=2).encode("utf-8")
    tmp = dest.with_name(dest.name + ".tmp")
    with open(tmp, "wb") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, dest)
    latest = version_dir(code, version) / "manifest-latest.json"
    tmp_latest = latest.with_name(latest.name + ".tmp")
    with open(tmp_latest, "wb") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_latest, latest)
    dfd = os.open(mdir, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
    return dest


def cleanup_orphan_tmp(root: Path | None = None) -> list[str]:
    """Remove leftover *.tmp files after a crash; returns removed paths."""
    base = root or config.lake_root()
    removed: list[str] = []
    if not base.exists():
        return removed
    for tmp in base.rglob("*.tmp"):
        try:
            tmp.unlink()
            removed.append(str(tmp))
        except FileNotFoundError:
            pass
    return removed


def resolve_file(relative_path: str) -> Path:
    """Resolve a lake-relative path and jail it inside the lake root."""
    root = config.lake_root().resolve()
    p = (root / relative_path).resolve()
    if root not in p.parents and p != root:
        raise PermissionError("path escapes the lake root")
    if not p.is_file():
        raise FileNotFoundError(relative_path)
    return p


def normalized_name(relative_path: str) -> str:
    """Flatten a stored path into the canonical ``local_date=<date>/<file>.parquet`` layout."""
    p = Path(relative_path)
    local_date = p.parent.name
    if local_date.startswith("local_date="):
        local_date = local_date.split("=", 1)[1]
    return f"local_date={local_date}/{p.name}"


def build_zip(files: list[tuple[str, str]], layout: str) -> bytes:
    """Zip (relative_path, arc_prefix) pairs into an in-memory archive.

    layout=preserve keeps the full versioned path; layout=normalize flattens
    every file to ``local_date=<date>/<name>.parquet``.
    """
    buf = io.BytesIO()
    seen: set[str] = set()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel, _prefix in files:
            src = resolve_file(rel)
            if layout == "normalize":
                arc = normalized_name(rel)
            else:
                arc = rel
            if arc in seen:  # pragma: no cover - names are unique by construction
                raise ValueError(f"duplicate archive entry: {arc}")
            seen.add(arc)
            zf.write(src, arc)
    return buf.getvalue()
