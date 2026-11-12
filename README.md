# Scan Lakehouse — 航空货运扫描入湖服务

把承运商已确认 **UTC** 的扫描事件，按机场 **IANA 时区**换算成本地日，写成
**按 `local_date` 分区的 Parquet**，供下游装载扫描事实表。原始记录持久化在
**SQLite**，所有数据文件与清单保存在挂载卷 `/data` 下，进程重启后自动恢复。

## 核心保证

| 需求 | 实现方式 |
| --- | --- |
| 每批最多 200 条，含唯一 ID + UTC 时刻 | Pydantic 校验（`min/max_length`、AwareDatetime、批内 ID 唯一） |
| 原始记录存 SQLite | `raw_scans` 表，`UNIQUE(airport, scan_id)` 硬约束 |
| 同机场同 ID **同内容**重传不重复落湖 | 内容 SHA-256 比对 + 批次 payload 哈希幂等；命中即返回 `status=duplicate` |
| 同机场同 ID **异内容**整批拒绝 | 先全量冲突检查，任一冲突返回 **409**，一条都不写 |
| 接收成功下游即可读到整批 | 入湖同步写 Parquet 并发布**新清单修订**，响应即含当前清单 |
| 改时区后台重分区全部历史扫描 | 异步重分区作业（bulk 快照 + drain 追赶），期间不停接收 |
| 期间接收的扫描纳入切换、每条恰好一次 | 机场锁串行化 drain/发布；切换前 row_id 集合做 exactly-once 校验 |
| 旧口径切换前持续可读 | 新版本独立目录构建，指针切换前当前清单始终指向旧版本 |
| 文件写完才发布清单 | 临时文件 + fsync + 原子 rename 落盘后才登记，再发布清单 |
| 一次清单只含同一时区口径 | 每个版本绑定单一 timezone，清单只列该版本文件 |
| 写文件/切换时进程被杀可恢复 | 启动时清理 `.tmp`/孤儿文件、续跑 `received` 批次与未完成作业 |
| 重分区期间再次调整时区拒绝 | 存在活动作业时 PATCH 返回 **409** `repartition_in_progress` |
| 进度 / 清单 / 下载 | 进度端点、当前与历史清单、单文件直出与 zip（preserve/normalize） |

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

服务启动后：

- 交互式 API 文档（Swagger UI，内置可一键试用示例）：<http://localhost:8000/docs>
- 健康检查：`GET /healthz`
- OpenAPI：`/openapi.json`

所有状态写入命名卷 `scan_data`（容器内 `/data`），重启不丢数。

## 30 秒演示（curl）

```bash
# 1. 登记机场（JFK，纽约时区）
curl -s -X POST localhost:8000/airports -H 'Content-Type: application/json' \
  -d '{"code":"JFK","timezone":"America/New_York"}'

# 2. 入湖一批 UTC 扫描（最多 200 条）
curl -s -X POST localhost:8000/ingest -H 'Content-Type: application/json' -d '{
  "airport":"JFK",
  "records":[
    {"id":"AWB-1-DEP","utc_ts":"2024-11-03T06:30:00Z","event_type":"DEP"},
    {"id":"AWB-1-ARR","utc_ts":"2024-11-04T05:00:00Z","event_type":"ARR"}
  ]}'

# 3. 查看当前清单（下游入口；文件按 local_date 分区，带 sha256）
curl -s localhost:8000/airports/JFK/manifests/current

# 4. 同内容重传 -> status=duplicate，不新增文件
#    （重复执行第 2 步的命令即可）

# 5. 调整时区 -> 返回后台重分区作业，立刻可查进度
curl -s -X PATCH localhost:8000/airports/JFK/timezone \
  -H 'Content-Type: application/json' -d '{"timezone":"Asia/Shanghai"}'
curl -s localhost:8000/airports/JFK/progress
curl -s localhost:8000/airports/JFK/jobs

# 6. 下载：preserve 保留版本路径；normalize 平铺为 local_date= 规范布局
curl -OJ "localhost:8000/airports/JFK/download?layout=normalize"
```

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/airports` | 登记机场及 IANA 时区（重复登记同时区幂等，异时区 409） |
| GET | `/airports` | 列出全部机场 |
| PATCH | `/airports/{code}/timezone` | 调整时区，创建后台重分区作业（活动作业存在时 409） |
| POST | `/ingest` | 接收一批扫描（1–200 条），落 SQLite + 写 Parquet + 发布清单 |
| GET | `/airports/{code}/progress` | 当前清单、扫描总数、活动作业进度 |
| GET | `/airports/{code}/jobs` | 该机场全部重分区作业（含历史与失败原因） |
| GET | `/airports/{code}/manifests` | 各时区版本清单（current / superseded） |
| GET | `/airports/{code}/manifests/current` | 当前文件清单（下游装载入口） |
| GET | `/airports/{code}/download` | 单文件直出 Parquet；多文件 zip，支持 `layout=preserve|normalize` 与 `version` |
| GET | `/files/download?path=...` | 按清单 relative_path 下载单个文件（带目录越权防护） |

错误响应统一为 `{"error", "message", "details"}`：404 机场/版本不存在、409 内容冲突或
重分区冲突、422 请求体不合法。

## 存储布局

```
/data/
  db/scan_lake.db                 # SQLite：机场/批次/原始扫描/清单/作业
  lake/airports/<CODE>/versions/
    v<N>/manifest-latest.json     # 该版本最新清单（原子刷新）
    v<N>/manifest/m<REV>.json     # 不可变清单快照
    v<N>/ingest/local_date=YYYY-MM-DD/ingest-b<batch>-s<seq>.parquet
    v<N>/repartition/job-<J>/local_date=.../part-<seq>-<lastrowid>.parquet
    v<N>/repartition/job-<J>/drain/local_date=.../drain-<seq>-<lastrowid>.parquet
```

Parquet 字段：`row_id, scan_id, airport, utc_ts(UTC), event_type, location,
attributes(JSON), ingested_at, local_date, timezone`。

## 重分区状态机与崩溃恢复

作业状态：`pending → running → draining → publishing → done`（异常置 `failed` 并记录原因）。

1. **bulk**：按 `row_id` 分块重写作业创建时已存在的全部扫描到新版本（文件名含覆盖的最大
   row_id，天然支持断点续跑）。
2. **drain**：在机场锁内追赶 bulk 期间新入湖的扫描。
3. **校验**：新版本文件的 `row_id` 集合必须与原始扫描总数一致且无重叠，否则失败不切换。
4. **发布**：先确认所有 Parquet 已持久化，再写清单，最后单事务翻转机场当前版本指针。

启动恢复（`recover_on_startup`）：删除残留 `.tmp` 与无 DB 记录的孤儿 Parquet；续跑
`received` 状态的批次；把中断的作业拉回 `running` 并按已登记文件重建进度；卡在
`publishing` 的作业只补做幂等的指针切换，不重复发清单。

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest httpx
uvicorn app.main:app --reload
python -m pytest tests/ -q          # 23 个端到端用例
```

测试用临时目录隔离 DB 与湖，覆盖：登记/校验、分区与清单、同内容幂等/异内容整批拒绝、
200 条上限、时区切换 exactly-once、切换期间并发入湖、活动作业拒绝二次调整、preserve/normalize
下载、目录越权防护，以及入湖中断与发布中断两类崩溃恢复。

> `/debug/worker/*` 与 `/debug/crash/ingest` 是为可测试性提供的 worker 控制与故障注入端点，
> 生产环境可在网关层屏蔽；它们不改变任何正常业务语义。
