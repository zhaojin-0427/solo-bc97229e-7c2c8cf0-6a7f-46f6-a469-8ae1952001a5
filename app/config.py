"""Runtime configuration, all overridable through environment variables."""

from __future__ import annotations

import os
from pathlib import Path


def db_path() -> Path:
    return Path(os.environ.get("LAKE_DB_PATH", "./data/db/scan_lake.db"))


def lake_root() -> Path:
    return Path(os.environ.get("LAKE_ROOT", "./data/lake"))


def chunk_size() -> int:
    """Rows per Parquet part written by a repartition job."""
    return int(os.environ.get("LAKE_CHUNK_SIZE", "1000"))


def worker_interval_s() -> float:
    """Background worker polling interval in seconds."""
    return float(os.environ.get("LAKE_WORKER_INTERVAL", "0.2"))
