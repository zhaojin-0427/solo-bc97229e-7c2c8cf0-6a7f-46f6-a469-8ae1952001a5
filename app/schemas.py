"""Request/response models for the scan lakehouse ingestion API."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, Field, field_validator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

MAX_BATCH_RECORDS = 200


class AirportRegister(BaseModel):
    """Register an airport with its IANA timezone."""

    code: str = Field(min_length=1, max_length=16, pattern=r"^[A-Za-z0-9_]+$", examples=["JFK"])
    timezone: str = Field(
        min_length=1, max_length=64,
        examples=["America/New_York"],
        description="IANA timezone name, e.g. America/New_York.",
    )

    @field_validator("code")
    @classmethod
    def _upper_code(cls, v: str) -> str:
        return v.upper()

    @field_validator("timezone")
    @classmethod
    def _known_iana_name(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
            raise ValueError(f"unknown IANA timezone: {v!r}") from exc
        return v


class ScanIn(BaseModel):
    """One carrier-confirmed scan event: unique id + UTC instant + payload."""

    id: str = Field(min_length=1, max_length=128, examples=["AWB-7842-1"])
    utc_ts: AwareDatetime = Field(
        examples=["2024-11-03T06:30:00Z"],
        description="Carrier-confirmed UTC instant; a timezone offset (Z or +hh:mm) is required.",
    )
    event_type: str = Field(default="SCAN", min_length=1, max_length=32, examples=["DEP"])
    location: str | None = Field(default=None, max_length=64, examples=["JFK-T4-G17"])
    attributes: dict[str, Any] = Field(default_factory=dict)

    def canonical(self) -> dict[str, Any]:
        """Stable JSON representation used for content-hash / idempotency checks."""
        return {
            "id": self.id,
            "utc_ts": self.utc_ts.astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "event_type": self.event_type,
            "location": self.location,
            "attributes": self.attributes,
        }

    def content_hash(self) -> str:
        blob = json.dumps(
            self.canonical(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()


class IngestRequest(BaseModel):
    """A batch of at most 200 scans for one airport."""

    airport: str = Field(min_length=1, max_length=16, examples=["JFK"])
    records: list[ScanIn] = Field(min_length=1, max_length=MAX_BATCH_RECORDS)

    @field_validator("airport")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()

    @field_validator("records")
    @classmethod
    def _unique_ids_within_batch(cls, v: list[ScanIn]) -> list[ScanIn]:
        ids = [r.id for r in v]
        if len(set(ids)) != len(ids):
            dupes = sorted({i for i in ids if ids.count(i) > 1})
            raise ValueError(f"duplicate scan id(s) within batch: {', '.join(dupes)}")
        return v


class AirportOut(BaseModel):
    code: str
    timezone: str
    current_version: int
    created_at: datetime
    updated_at: datetime


class LakeFileOut(BaseModel):
    file_id: int
    version: int
    timezone: str
    local_date: str
    relative_path: str
    row_count: int
    size_bytes: int
    sha256: str
    source: Literal["ingest", "repartition"]
    created_at: datetime


class ManifestOut(BaseModel):
    manifest_id: int
    airport: str
    version: int
    timezone: str
    revision: int
    status: Literal["current", "superseded"]
    file_count: int
    row_count: int
    files: list[LakeFileOut]
    published_at: datetime


class IngestResponse(BaseModel):
    status: Literal["accepted", "duplicate"]
    batch_id: int
    airport: str
    version: int
    timezone: str
    received: int
    inserted: int
    duplicates: int
    manifest: ManifestOut


class RepartitionJobOut(BaseModel):
    job_id: int
    airport: str
    from_timezone: str
    to_timezone: str
    new_version: int
    state: Literal["pending", "running", "draining", "publishing", "done", "failed"]
    total_rows: int
    processed_rows: int
    chunks_done: int
    drained_rows: int
    error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class ProgressOut(BaseModel):
    airport: AirportOut
    active_job: RepartitionJobOut | None
    total_scans: int
    current_manifest: ManifestOut


class ErrorResponse(BaseModel):
    error: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
