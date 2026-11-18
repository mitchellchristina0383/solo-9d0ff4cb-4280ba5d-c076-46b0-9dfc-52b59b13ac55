# HTTP 入口网关（灰度发布 / 优雅排空 / 健康探测 / 轮询转发）

滚动发布场景下的入口网关，支持两种接流方式：

* **灰度关闭**：在健康且未排空的节点间轮询（原始行为）；
* **灰度开启**：按请求头 `X-Rollout-Key` 做稳定分组，0～100% 比例放量到
  node2（canary 新版），其余终端固定接 node1（baseline 旧版）。

同时保留原有能力：运维人员可先排空节点，使新请求不再进入，已在途长请求
完整取回原响应，全部结束后才报告排空完成。

技术栈：**FastAPI + httpx**，由 **Docker Compose** 启动 1 个网关 + 2 个
模拟慢请求 / 故障的上游，上游响应体中带节点名可直接辨别落点。

## 目录结构

```
.
├── docker-compose.yml      # 编排：gateway + upstream1 + upstream2 + 策略持久化卷
├── gateway/
│   ├── app.py              # 网关：灰度选路、轮询、探测、排空/恢复/状态
│   ├── rollout.py          # 灰度策略：不可变快照、稳定分组(SHA-256)、原子落盘
│   ├── requirements.txt
│   └── Dockerfile
├── upstream/
│   ├── app.py              # 模拟上游：slow / flaky / 故障模式控制
│   ├── requirements.txt
│   └── Dockerfile
└── tests/
    ├── self_test.py        # 原有端到端自测（15 组场景，52 项断言）
    └── rollout_test.py     # 灰度端到端自测（13 组场景，52 项断言）
```

## 一键启动

```bash
docker compose up --build
# 网关:        http://localhost:18080
# 上游 node1:  http://localhost:18081
# 上游 node2:  http://localhost:18082
# 交互文档:    http://localhost:18080/docs   （可直接调比例、发起 GET）
```

网关环境变量（见 `docker-compose.yml`）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `UPSTREAMS` | `node1=http://upstream1:8000,node2=http://upstream2:8000` | 上游列表 `name=url` |
| `ROLLOUT_BASELINE_NODE` | `node1` | 灰度基线（旧版）节点名 |
| `ROLLOUT_CANARY_NODE` | `node2` | 灰度候选（新版）节点名 |
| `ROLLOUT_STATE_DIR` | `/data` | 策略持久化目录（compose 挂载命名卷） |
| `PROBE_INTERVAL` | `2.0` | 周期探测间隔（秒） |
| `PROBE_TIMEOUT` | `1.5` | 探测超时（秒） |
| `REQUEST_TIMEOUT` | `15.0` | 转发请求超时（秒） |
| `DRAIN_DEFAULT_TIMEOUT` | `30.0` | 排空 API 默认等待上限（秒） |

## 灰度语义（需求对照）

1. **开关 + 0～100% 比例**：`POST /admin/rollout`，body
   `{"enabled": true, "percent": 30}`；`enabled=false` 即关闭灰度。
2. **稳定分组**：`bucket = SHA256(X-Rollout-Key) % 100`，纯函数，与进程、
   时间、当前比例均无关。灰度开启时 `bucket < percent` 走 node2，否则走
   node1。比例不变时**重启网关也不换组**（测试 R3/R9 验证）。
3. **比例单调语义**：分组号固定、只移动阈值，因此上调比例只把更多组纳入
   node2 候选，已在 node2 的 key 不会被改投；下调按新比例把超出部分收回
   node1，调回原比例时分组集合完全复原（测试 R4 验证）。
4. **无标识请求**：不带 `X-Rollout-Key`（或值为空白）的请求在灰度开启时
   **只走 node1**；灰度关闭后所有流量恢复原轮询。
5. **选路与在途登记同一份策略**：策略是不可变快照（frozen dataclass），
   选路和 `inflight += 1` 在同一把锁内基于同一快照完成。调比例 / 关灰度
   只替换引用：已接流的请求手持旧快照，仍从原节点取响应；新请求按新策略
   选路（测试 R8 验证）。
6. **故障 / 排空不跨版改投**：灰度目标节点不健康或排空中时直接返回
   **503**（`reason=node_unhealthy|node_draining`），绝不改投另一版本节点
   （测试 R6/R7/R13 验证）。
7. **设置失败沿用旧策略**：先原子落盘（临时文件 + fsync + `os.replace`）
   再切换内存引用；落盘失败返回 500，内存与旧文件均不变。
8. **重启沿用最后成功策略**：启动时从 `ROLLOUT_STATE_DIR/rollout_policy.json`
   加载；文件缺失则默认关闭，文件损坏则安全回退到关闭态并在
   `/admin/rollout` 的 `last_load_error` 留痕（测试 R9/R10 验证）。

## API 一览（均可在 /docs 直接操作）

转发（仅 GET）：

```
GET /proxy/{path}      # 带 X-Rollout-Key 时按灰度分组，否则灰度开启走 node1
                       # 灰度关闭时轮询；目标不可用 -> 503（不跨版改投）
                       # 上游超时 -> 504；连不上 -> 502；非 GET -> 405
                       # 响应头 X-Upstream-Node 标明实际节点，
                       # X-Rollout-Route / X-Rollout-Bucket 标明灰度路由与分组
```

灰度管理：

```
GET  /admin/rollout                       # 当前生效策略、revision、持久化留痕
POST /admin/rollout                       # body: {"enabled": true, "percent": 30}
                                          # 先落盘后生效；失败 500 且沿用旧策略
GET  /admin/rollout/classify?key=xxx      # 预览某 key 的 bucket 与目标节点
```

节点管理：

```
GET  /admin/status                     # 节点健康/排空/在途/计数 + 当前灰度策略
POST /admin/nodes/{name}/drain?timeout=# 排空：立即停接新流，阻塞到在途清零
                                       #   在途归零 -> 200 drained=true
                                       #   超时仍有在途 -> 202 drained=false
POST /admin/nodes/{name}/resume        # 恢复：清排空，立即探测，由探测决定能否接流
POST /admin/probe                      # 手动触发一轮探测
```

上游模拟控制（演练用，直连上游端口，不经过网关）：

```
GET  /control/state            # 查看模式与在途数
POST /control/mode?mode=       # healthy | slow | error | hang
GET  /slow?seconds=N           # 慢请求（验证在途不被截断）
GET  /flaky                    # 按模式返回 200 / 500 / 挂起
GET  /                         # 健康端点（error->503, hang->挂起）
```

## 典型演练

```bash
# 1. 放量 30%：带固定标识的终端稳定分组，可反复请求验证不换节点
curl -X POST http://localhost:18080/admin/rollout \
  -H 'Content-Type: application/json' -d '{"enabled": true, "percent": 30}'
curl -H 'X-Rollout-Key: terminal-001' http://localhost:18080/proxy/echo
# 响应体 "node" 与响应头 X-Upstream-Node 标明落点；X-Rollout-Bucket 标明分组

# 2. 预览某个 key 会不会进新版（不发真实流量）
curl 'http://localhost:18080/admin/rollout/classify?key=terminal-001'

# 3. 出问题收回流量：下调比例或直接关灰度（在途请求不受影响）
curl -X POST http://localhost:18080/admin/rollout \
  -H 'Content-Type: application/json' -d '{"enabled": false, "percent": 0}'

# 4. 排空 node2：灰度新请求立即 503（不跨版改投），在途慢请求等回原响应
curl 'http://localhost:18080/proxy/slow?seconds=10' -H 'X-Rollout-Key: k' &
curl -X POST 'http://localhost:18080/admin/nodes/node2/drain?timeout=30'

# 5. 重启网关后策略仍在（docker compose restart gateway，卷内策略文件保留）
docker compose restart gateway
curl http://localhost:18080/admin/rollout
```

## 关键设计与并发正确性

- **不可变策略快照**：`RolloutPolicy` 为 frozen dataclass。请求在选路时只
  持有当次快照引用；策略变更只替换 `RolloutManager.policy`，已在途请求
  始终按旧快照、从原节点取回响应。
- **选路 + 登记同一把锁**：`pick_node` 在 `asyncio.Condition` 锁内完成
  「取快照 → 分组决策 → 健康/排空校验 → inflight 登记」，不存在按新策略
  选路却登记到旧状态的窗口。
- **稳定分组纯函数**：分组只依赖 key 的 SHA-256，不依赖比例与内存状态，
  从机制上保证比例不变时重启不换组，以及比例调整的单调集合语义。
- **禁止跨版改投**：灰度模式下节点由分组唯一指定，目标不可用即 503；
  轮询兜底逻辑只在灰度关闭时启用。
- **原子持久化**：`mkstemp + fsync + os.replace`，写入失败不触碰旧文件；
  内存引用仅在落盘成功后切换，保证「设置失败沿用旧策略」。
- **两个独立标志位**：`healthy` 只由探测（含请求失败降级）写入；`draining`
  只由人工 drain/resume 写入，探测成功永不解除排空。
- **恢复不绕过故障探测**：`resume` 先清排空并保守置 `healthy=false`，再做
  一次真实探测；探测通过前该节点不会被选中。
- **在途计数可靠**：转发用 `try/finally` 归还计数，成功、5xx、超时、连接
  错误、客户端取消都会准确递减；排空等待在条件变量上，请求一结束即唤醒。
- **不虚报成功**：5xx 如实透传并即时降级；超时 504、连接错误 502，绝不
  返回虚假 200。

## 自测

```bash
# 无需 docker；需本地具备 fastapi/uvicorn/httpx（可 pip install 到独立目录）
python3 tests/self_test.py       # 原有能力：15 组场景 / 52 项断言
python3 tests/rollout_test.py    # 灰度能力：13 组场景 / 52 项断言
```

`rollout_test.py` 覆盖：默认轮询、100%/0% 放量、稳定分组与观测头、
比例上调只增候选 / 下调收回 / 回调复原、canary 排空 503 与在途完整、
canary 故障 503 不跨版改投、**在途期间调比例/关灰度的交错**、
**重启后策略持久化且不换组**、**持久化失败沿用旧策略**、关闭后恢复轮询、
参数校验（422）、baseline 排空时无标识与 baseline 组请求 503。
