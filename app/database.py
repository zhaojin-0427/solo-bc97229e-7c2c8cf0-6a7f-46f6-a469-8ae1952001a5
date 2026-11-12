"""SQLite persistence: schema bootstrap and the async connection wrapper."""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite

from . import config

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS airports (
    code            TEXT PRIMARY KEY,
    timezone        TEXT NOT NULL,
    current_version INTEGER NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS versions (
    airport      TEXT NOT NULL,
    version      INTEGER NOT NULL,
    timezone     TEXT NOT NULL,
    state        TEXT NOT NULL DEFAULT 'active',  -- active | building | abandoned
    created_at   TEXT NOT NULL,
    PRIMARY KEY (airport, version)
);

CREATE TABLE IF NOT EXISTS batches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    airport      TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    state        TEXT NOT NULL,            -- received | landed
    record_count INTEGER NOT NULL,
    inserted     INTEGER NOT NULL DEFAULT 0,
    duplicates   INTEGER NOT NULL DEFAULT 0,
    error        TEXT,
    created_at   TEXT NOT NULL,
    landed_at    TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_batches_airport_payload
    ON batches(airport, payload_hash);

CREATE TABLE IF NOT EXISTS raw_scans (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    airport       TEXT NOT NULL,
    scan_id       TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    utc_ts        TEXT NOT NULL,           -- ISO-8601 UTC
    event_type    TEXT NOT NULL,
    location      TEXT,
    attributes    TEXT NOT NULL,           -- JSON
    batch_id      INTEGER NOT NULL,
    ingested_at   TEXT NOT NULL,
    UNIQUE(airport, scan_id)
);
CREATE INDEX IF NOT EXISTS ix_raw_scans_airport ON raw_scans(airport, id);

CREATE TABLE IF NOT EXISTS manifests (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    airport      TEXT NOT NULL,
    version      INTEGER NOT NULL,
    revision     INTEGER NOT NULL,
    is_current   INTEGER NOT NULL,
    file_count   INTEGER NOT NULL,
    row_count    INTEGER NOT NULL,
    published_at TEXT NOT NULL,
    UNIQUE(airport, version, revision)
);

CREATE TABLE IF NOT EXISTS lake_files (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    airport       TEXT NOT NULL,
    version       INTEGER NOT NULL,
    manifest_id   INTEGER NOT NULL,
    timezone      TEXT NOT NULL,
    local_date    TEXT NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    row_count     INTEGER NOT NULL,
    size_bytes    INTEGER NOT NULL,
    sha256        TEXT NOT NULL,
    source        TEXT NOT NULL,           -- ingest | repartition
    created_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_lake_files_version ON lake_files(airport, version);

CREATE TABLE IF NOT EXISTS repartition_jobs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    airport            TEXT NOT NULL,
    from_timezone      TEXT NOT NULL,
    to_timezone        TEXT NOT NULL,
    new_version        INTEGER NOT NULL,
    state              TEXT NOT NULL,      -- pending|running|draining|publishing|done|failed
    bulk_high_id       INTEGER NOT NULL DEFAULT 0,
    total_rows         INTEGER NOT NULL DEFAULT 0,
    last_processed_id  INTEGER NOT NULL DEFAULT 0,
    chunks_done        INTEGER NOT NULL DEFAULT 0,
    processed_rows     INTEGER NOT NULL DEFAULT 0,
    drained_rows       INTEGER NOT NULL DEFAULT 0,
    error              TEXT,
    created_at         TEXT NOT NULL,
    started_at         TEXT,
    finished_at        TEXT
);
CREATE INDEX IF NOT EXISTS ix_jobs_airport_state ON repartition_jobs(airport, state);
"""


class Database:
    """Thin async wrapper keeping one connection for the whole process."""

    def __init__(self) -> None:
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        path = config.db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(str(path))
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=FULL")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute("PRAGMA busy_timeout=10000")
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        self._conn = conn

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        assert self._conn is not None, "database not connected"
        return self._conn

    async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> aiosqlite.Cursor:
        return await self.conn.execute(sql, params)

    async def executemany(self, sql: str, seq: list[tuple[Any, ...]]) -> None:
        await self.conn.executemany(sql, seq)

    async def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        cur = await self.conn.execute(sql, params)
        return await cur.fetchone()

    async def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        cur = await self.conn.execute(sql, params)
        return await cur.fetchall()

    async def commit(self) -> None:
        await self.conn.commit()

    @property
    def in_transaction(self) -> bool:
        return self.conn._connection.in_transaction

    async def ensure_idle(self) -> None:
        """Roll back any dangling transaction before starting a new unit of work."""
        if self.in_transaction:
            await self.conn.rollback()

    @asynccontextmanager
    async def transaction(self):
        conn = self.conn
        try:
            await conn.execute("BEGIN IMMEDIATE")
            yield conn
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


db = Database()
