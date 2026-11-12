"""FastAPI entrypoint for the airport scan lakehouse ingestion service."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Annotated, Literal

from fastapi import Body, FastAPI, HTTPException, Path, Query
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from . import service, storage
from .database import db
from .schemas import (
    AirportOut,
    AirportRegister,
    ErrorResponse,
    IngestRequest,
    IngestResponse,
    ManifestOut,
    ProgressOut,
    RepartitionJobOut,
)

DESCRIPTION = """
航空货运扫描入湖服务（Scan Lakehouse）

承运商已确认 **UTC** 的扫描事件按机场 IANA 时区换算成本地日，写成
**按 `local_date` 分区的 Parquet**，供下游装载扫描事实表。

关键保证
--------
* 每批最多 **200 条**扫描，含唯一 ID 与 UTC 时刻；原始记录先落 **SQLite**；
* 同机场同 ID **同内容**重传 → 幂等，不重复落湖；**异内容** → 整批拒绝（409），一条不写；
* 接收成功即同步发布新清单，下游马上能从**当前文件清单**读到整批数据；
* 调整机场时区会创建后台**重分区作业**：重写全部历史扫描到新版本，期间
  - 仍可正常接收扫描（写入旧版本，旧清单继续可读）；
  - 切换前 drain 阶段把期间接收的扫描也纳入新版本，**每条恰好一次**（Parquet row_id 校验）；
* **文件写完并 fsync 后才登记、才发布清单**；一次清单只包含同一时区口径的文件；
* 写文件 / 切换时进程被杀死，重启后自动恢复，不漏数、不发布残缺文件；
* 同机场重分区进行中再次调整时区 → **409 拒绝**；
* 提供作业进度、清单查询、文件下载（`preserve` 保留版本路径 / `normalize` 平铺为
  `local_date=…` 规范分区布局）。
"""

DEMO_INGEST = {
    "airport": "JFK",
    "records": [
        {
            "id": "AWB-001-DEP",
            "utc_ts": "2024-11-03T06:30:00Z",
            "event_type": "DEP",
            "location": "JFK-T4-G17",
            "attributes": {"flight": "CA982", "weight_kg": 1240.5},
        },
        {
            # 05:00Z -> 00:00 New York local on 2024-11-04 (next local day).
            "id": "AWB-001-ARR",
            "utc_ts": "2024-11-04T05:00:00Z",
            "event_type": "ARR",
            "location": "PVG-P2",
            "attributes": {"flight": "CA982"},
        },
        {
            "id": "AWB-002-XFR",
            "utc_ts": "2024-11-03T11:00:00.500000+00:00",
            "event_type": "XFR",
            "attributes": {},
        },
    ],
}

DEMO_TZ_CHANGE = {"timezone": "America/Chicago"}


class TimezoneChange(BaseModel):
    timezone: str = Field(
        min_length=1, max_length=64,
        examples=["America/Chicago"],
        description="New IANA timezone name.",
    )

    def validated(self) -> str:
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
            raise HTTPException(
                status_code=422, detail=f"unknown IANA timezone: {self.timezone!r}"
            ) from exc
        return self.timezone


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    recovery = await service.recover_on_startup()
    app.state.recovery = recovery
    worker = asyncio.create_task(service.worker_loop())
    try:
        yield
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        await db.close()


app = FastAPI(
    title="Airport Scan Lakehouse",
    description=DESCRIPTION,
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root() -> dict:
    return {
        "service": "scan-lakehouse",
        "docs": "/docs",
        "health": "/healthz",
        "endpoints": {
            "register_airport": "POST /airports",
            "ingest": "POST /ingest",
            "change_timezone": "PATCH /airports/{code}/timezone",
            "progress": "GET /airports/{code}/progress",
            "manifests": "GET /airports/{code}/manifests",
            "download": "GET /airports/{code}/download",
        },
    }


@app.get("/healthz", tags=["meta"])
async def healthz() -> dict:
    row = await db.query_one("SELECT COUNT(*) AS n FROM airports")
    return {"status": "up", "airports": row["n"] if row else 0}


# ---------------------------------------------------------------------------
# Airports
# ---------------------------------------------------------------------------


@app.post(
    "/airports",
    response_model=AirportOut,
    status_code=201,
    tags=["airports"],
    responses={409: {"model": ErrorResponse}},
    summary="登记机场及 IANA 时区",
)
async def register_airport(payload: AirportRegister) -> AirportOut:
    return await service.register_airport(payload.code, payload.timezone)


@app.get("/airports", response_model=list[AirportOut], tags=["airports"], summary="列出全部机场")
async def list_airports() -> list[AirportOut]:
    return await service.list_airports()


@app.patch(
    "/airports/{code}/timezone",
    response_model=RepartitionJobOut,
    tags=["airports"],
    responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
    summary="调整机场时区（后台重分区全部历史扫描）",
)
async def change_timezone(
    payload: TimezoneChange = Body(
        openapi_examples={
            "to_chicago": {
                "summary": "JFK 从 America/New_York 调整为 America/Chicago",
                "value": DEMO_TZ_CHANGE,
            }
        }
    ),
    code: str = Path(..., min_length=1, max_length=16),
) -> RepartitionJobOut:
    return await service.change_timezone(code.upper(), payload.validated())


@app.get(
    "/airports/{code}/progress",
    response_model=ProgressOut,
    tags=["airports"],
    responses={404: {"model": ErrorResponse}},
    summary="机场总览：当前清单、扫描总数、重分区作业进度",
)
async def progress(code: str) -> dict:
    return await service.get_progress(code.upper())


@app.get(
    "/airports/{code}/jobs",
    response_model=list[RepartitionJobOut],
    tags=["airports"],
    summary="该机场全部重分区作业（含历史）",
)
async def list_jobs(code: str) -> list[RepartitionJobOut]:
    return await service.list_jobs(code.upper())


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


@app.post(
    "/ingest",
    response_model=IngestResponse,
    tags=["ingest"],
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
    summary="接收一批扫描（最多 200 条），原始记录入 SQLite 并发布 Parquet 清单",
)
async def ingest(
    req: Annotated[
        IngestRequest,
        Body(
            openapi_examples={
                "jfk_batch": {
                    "summary": "JFK 一批 3 条 UTC 扫描（跨两个本地日）",
                    "description": "成功后返回新版本号、时区、插入/去重计数和当前清单（含文件路径与校验和）。",
                    "value": DEMO_INGEST,
                },
                "retransmit_same_batch": {
                    "summary": "同内容重传 → status=duplicate，不重复落湖",
                    "value": DEMO_INGEST,
                },
            }
        ),
    ],
) -> IngestResponse:
    return await service.ingest_batch(req)


# ---------------------------------------------------------------------------
# Manifests & downloads
# ---------------------------------------------------------------------------


@app.get(
    "/airports/{code}/manifests",
    response_model=list[ManifestOut],
    tags=["manifest"],
    summary="列出该机场各时区版本的清单（当前 / 已废弃）",
)
async def list_manifests(code: str) -> list[ManifestOut]:
    return await service.list_manifests(code.upper())


@app.get(
    "/airports/{code}/manifests/current",
    response_model=ManifestOut,
    tags=["manifest"],
    responses={404: {"model": ErrorResponse}},
    summary="当前文件清单（下游装载事实表的入口）",
)
async def current_manifest(code: str) -> ManifestOut:
    return await service.get_current_manifest(code.upper())


@app.get(
    "/airports/{code}/download",
    tags=["download"],
    responses={404: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
    summary="下载当前清单的 Parquet：单文件直出，多文件打 zip（preserve / normalize）",
)
async def download(
    code: str,
    layout: Literal["preserve", "normalize"] = Query(
        "preserve",
        description="preserve=保留版本化目录结构；normalize=平铺为 local_date=YYYY-MM-DD 规范 Hive 布局",
    ),
    version: int | None = Query(None, ge=1, description="下载指定历史版本；默认当前版本"),
) -> Response:
    code = code.upper()
    manifest = (
        await service.get_current_manifest(code)
        if version is None
        else await service.get_manifest(code, version)
    )
    if not manifest.files:
        raise HTTPException(status_code=404, detail="manifest has no files to download")
    if len(manifest.files) == 1:
        f = manifest.files[0]
        try:
            path = storage.resolve_file(f.relative_path)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return FileResponse(
            path,
            media_type="application/vnd.apache.parquet",
            filename=path.name,
        )
    try:
        pairs = [(f.relative_path, "") for f in manifest.files]
        blob = storage.build_zip(pairs, layout=layout)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    suffix = "normalized" if layout == "normalize" else "preserved"
    fname = f"{code.lower()}-v{manifest.version}-{suffix}.zip"
    return Response(
        content=blob,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get(
    "/files/download",
    tags=["download"],
    responses={404: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
    summary="按清单中的 relative_path 下载单个 Parquet 文件",
)
async def download_one(path: str = Query(..., description="清单文件中的 relative_path")) -> Response:
    try:
        p = storage.resolve_file(path)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(p, media_type="application/vnd.apache.parquet", filename=p.name)


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


@app.exception_handler(service.ServiceError)
async def service_error_handler(_request, exc: service.ServiceError) -> Response:
    return JSONResponse(
        status_code=exc.status,
        content=ErrorResponse(
            error=exc.error, message=exc.message, details=exc.details
        ).model_dump(mode="json"),
    )


# ---------------------------------------------------------------------------
# Test-only worker control (documented, harmless in production)
# ---------------------------------------------------------------------------


@app.post("/debug/worker/{action}", include_in_schema=True, tags=["debug"])
async def worker_control(
    action: Literal["pause", "resume", "tick"],
    until_state: Literal["pending", "running", "draining", "publishing", "done"] | None = None,
) -> dict:
    """Pause/resume the background repartition worker, or run iterations now.

    ``until_state`` runs ticks until the newest job reaches the given state
    (used by tests to freeze the pipeline at a deterministic phase).
    """
    if action == "pause":
        service.worker_gate.clear()
        return {"worker": "paused"}
    if action == "resume":
        service.worker_gate.set()
        return {"worker": "resumed"}
    ran = False
    iterations = 0
    while True:
        ran_one = await service.process_one_job()
        ran = ran or ran_one
        iterations += 1
        if until_state is None:
            break
        jobs = await service.list_jobs_for_debug()
        state = jobs[0]["state"] if jobs else None
        if state == until_state or state in ("done", "failed") or iterations > 500:
            return {"worker": "tick", "job_processed": ran, "state": state, "iterations": iterations}
        if not ran_one:
            return {"worker": "tick", "job_processed": ran, "state": state, "iterations": iterations}
    return {"worker": "tick", "job_processed": ran, "iterations": iterations}


class CrashIngest(BaseModel):
    airport: str
    scan_id: str = "X1"
    utc_ts: str = "2024-11-03T06:30:00Z"


@app.post("/debug/crash/ingest", include_in_schema=True, tags=["debug"])
async def crash_ingest(payload: CrashIngest) -> dict:
    """Test aid: commit a raw scan + 'received' batch with no lake files.

    Simulates a process killed after the SQLite commit but before the Parquet
    manifest was published, so startup recovery can be exercised.
    """
    return await service.inject_crashed_ingest(
        payload.airport.upper(), payload.scan_id, payload.utc_ts
    )
